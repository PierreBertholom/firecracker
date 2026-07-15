// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Portions Copyright 2017 The Chromium OS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the THIRD-PARTY file.

use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::{self, JoinHandle};
use event_manager::{EventOps, Events, MutEventSubscriber, SubscriberOps};
use crate::EventManager;
use crate::devices::virtio::block::virtio::device::BlockResources;
use crate::devices::virtio::block::virtio::metrics::BlockDeviceMetrics;
use crate::devices::virtio::block::virtio::{FinishedRequest, IoErr, ProcessingResult, Request, VirtioBlockError};
use crate::devices::virtio::device::ActiveState;
use crate::rate_limiter::BucketUpdate;
use crate::seccomp::{BpfProgram, apply_filter};
use vmm_sys_util::epoll::EventSet;
use vmm_sys_util::eventfd::EventFd;
use crate::devices::virtio::ActivateError;
use crate::devices::virtio::block::CacheType;
use crate::devices::virtio::block::virtio::io::{async_io, FileEngine};
use crate::devices::virtio::block::virtio::persist::FileEngineTypeState;
use crate::devices::virtio::persist::QueueState;
use crate::devices::virtio::queue::{InvalidAvailIdx, Queue, QueueError};
use crate::devices::virtio::transport::VirtioInterruptType;
use crate::logger::{IncMetric, error, warn};
use crate::rate_limiter::persist::RateLimiterState;
use crate::snapshot::Persist;
use crate::vmm_config::drive::FileEngineType;
use crate::vstate::memory::GuestMemoryMmap;

/// Worker block device data
#[derive(Debug)]
pub(crate) struct BlockWorker {
    pub(crate) resources: BlockResources,
    pub(crate) active_state: ActiveState,
    pub(crate) metrics: Arc<BlockDeviceMetrics>,
}

#[derive(Debug)]
pub(crate) struct ThreadedWorker {
    state: WorkerState,
    control_evt: EventFd,
    control_rx: Receiver<ControlMsg>,
    response_tx: Sender<ControlResponse>,
}

pub(crate) struct SavedState {
    pub queue_state: Vec<QueueState>,
    pub rate_limiter_state: RateLimiterState,
    pub disk_path: String,
    pub file_engine_type: FileEngineTypeState,
}

#[derive(Debug)]
enum WorkerState {
    Parked,
    Running(BlockWorker),
    Paused(BlockWorker),
    Finished,
}

pub(crate) enum FlushMode {
    Drain,
    DrainAndFlush,
}

#[allow(clippy::large_enum_variant)]
enum ControlMsg {
    Start(BlockWorker),
    UpdateDiskImage(String, bool),
    UpdateRateLimiter(BucketUpdate, BucketUpdate),
    Kick,
    Pause,
    GetSavedState,
    MarkQMemDirty,
    Reset,
    Finish(FlushMode),
}

#[allow(clippy::large_enum_variant)]
enum ControlResponse {
    DiskUpdated(Result<u64, VirtioBlockError>), // nsectors / error
    Paused, // ACK
    SaveReady(SavedState),
    QMemDirty(Result<(), QueueError>),
    Reset(Option<BlockResources>),
}

#[derive(Debug)]
pub(crate) struct WorkerHandle {
    control_tx: Sender<ControlMsg>,
    receiver_rx: Receiver<ControlResponse>,
    control_evt: EventFd,
    join: JoinHandle<()>,
    queue_evts: Vec<EventFd>,
}
macro_rules! unwrap_async_file_engine_or_return {
    ($file_engine: expr) => {
        match $file_engine {
            FileEngine::Async(engine) => engine,
            FileEngine::Sync(_) => {
                error!("The block device doesn't use an async IO engine");
                return;
            }
        }
    };
}

impl WorkerHandle {
    pub(crate) fn spawn(
        seccomp_filter: Arc<BpfProgram>,
        queue_evts: Vec<EventFd>,
    ) -> Result<WorkerHandle, std::io::Error> {
        // One kick eventfd, shared between worker (registers on its epoll) and shell (writes to
        // wake it). Both refer to the same kernel object via try_clone.
        let control_evt = EventFd::new(libc::EFD_NONBLOCK)?;
        let handle_evt = control_evt.try_clone()?;

        let (control_tx, control_rx) = channel::<ControlMsg>();
        let (response_tx, receiver_rx) = channel::<ControlResponse>();

        let join = thread::Builder::new()
            .name("fc_blk_worker".to_owned())
            .spawn(move || {
                // epoll first (not in seccomp), then register, then lock down.
                let event_manager = EventManager::new()
                    .expect("Failed to create block worker EventManager");

                if let Err(err) = apply_filter(&seccomp_filter) {
                    panic!("Failed to apply seccomp filter on block worker: {err}");
                }
                run_worker_loop(event_manager, control_evt, control_rx, response_tx)
            })?;

        Ok(WorkerHandle {
            control_tx,
            receiver_rx,
            control_evt: handle_evt,
            join,
            queue_evts,
        })
    }

    pub(crate) fn queue_events(&self) -> &[EventFd] { &self.queue_evts }

    pub(crate) fn start(&self, worker: BlockWorker) {
        if let Err(e) = self.control_tx.send(ControlMsg::Start(worker)) {
            error!("Failed to send start message: {:?}", e);
        }

        if let Err(e) = self.control_evt.write(1) {
            error!("Failed to notify worker: {:?}", e);
        }
    }

    pub(crate) fn finish(self, flush_mode: FlushMode) {
        if let Err(e) = self.control_tx.send(ControlMsg::Finish(flush_mode)) {
            error!("Block receiver already dropped on teardown: {:?}", e);
        }

        if let Err(e) = self.control_evt.write(1) {
            error!("Block control event is closed on teardown: {:?}", e);
        }

        self.join.join().unwrap_or_else(|e| {
            error!("Block worker thread panicking during teardown: {:?}", e);
        });
    }

    pub(crate) fn pause(&self) {
        if let Err(e) = self.control_tx.send(ControlMsg::Pause) {
            error!("Block receiver already dropped on pause: {:?}", e);
        }

        if let Err(e) = self.control_evt.write(1) {
            error!("Block control event is closed on pause: {:?}", e);
        }

        loop {
            match self.receiver_rx.recv() {
                Ok(ControlResponse::Paused) => return,
                Ok(ControlResponse::DiskUpdated(_)) => {
                    warn!("Ignoring disk update response while waiting for pause");
                }
                Ok(ControlResponse::SaveReady(_)) => {
                    warn!("Ignoring saved state response while waiting for pause");
                }
                Ok(ControlResponse::Reset(_)) => {
                    warn!("Ignoring reset response while waiting for pause");
                }
                Ok(ControlResponse::QMemDirty(_)) => {
                    warn!("Ignoring marking queue memory dirty response while waiting for disk update");
                }
                Err(e) => {
                    error!("Block worker failed to acknowledge pause: {:?}", e);
                }
            }
        }
    }

    pub(crate) fn get_saved_state(&self) -> SavedState {
        if let Err(e) = self.control_tx.send(ControlMsg::GetSavedState) {
            error!("Block receiver already dropped on pause: {:?}", e);
        }

        if let Err(e) = self.control_evt.write(1) {
            error!("Block control event is closed on pause: {:?}", e);
        }

        loop {
            match self.receiver_rx.recv() {
                Ok(ControlResponse::SaveReady(st)) => return st,
                Ok(ControlResponse::DiskUpdated(_)) => {
                    warn!("Ignoring disk update response while waiting for saved state");
                }
                Ok(ControlResponse::Paused) => {
                    warn!("Ignoring pause response while waiting for saved state");
                }
                Ok(ControlResponse::Reset(_)) => {
                    warn!("Ignoring reset response while waiting for saved state");
                }
                Ok(ControlResponse::QMemDirty(_)) => {
                    warn!("Ignoring marking queue memory dirty response while waiting for disk update");
                }
                Err(e) => {
                    error!("Block worker failed to acknowledge saved state: {:?}", e);
                }
            }
        }
    }
    pub(crate) fn mark_queue_memory_dirty(&self, mem: &GuestMemoryMmap) -> Result<(), QueueError> {
        if let Err(e) = self.control_tx.send(ControlMsg::MarkQMemDirty) {
            error!("Block receiver already dropped on pause: {:?}", e);
        }

        if let Err(e) = self.control_evt.write(1) {
            error!("Block control event is closed on pause: {:?}", e);
        }

        loop {
            match self.receiver_rx.recv() {
                Ok(ControlResponse::QMemDirty(r)) => return r,
                Ok(ControlResponse::SaveReady(_)) => {
                    warn!("Ignoring saved state response while waiting for pause");
                },
                Ok(ControlResponse::DiskUpdated(_)) => {
                    warn!("Ignoring disk update response while waiting for saved state");
                }
                Ok(ControlResponse::Paused) => {
                    warn!("Ignoring pause response while waiting for saved state");
                }
                Ok(ControlResponse::Reset(_)) => {
                    warn!("Ignoring reset response while waiting for saved state");
                }
                Err(e) => {
                    error!("Block worker failed to acknowledge saved state: {:?}", e);
                }
            }
        }
    }

    pub(crate) fn reset(&self) -> Option<BlockResources> {
        if let Err(e) = self.control_tx.send(ControlMsg::Reset) {
            error!("Block receiver already dropped on reset: {:?}", e);
            return None;
        }

        if let Err(e) = self.control_evt.write(1) {
            error!("Block control event is closed on reset: {:?}", e);
            return None;
        }

        loop {
            match self.receiver_rx.recv() {
                Ok(ControlResponse::Reset(resources)) => return resources,
                Ok(ControlResponse::DiskUpdated(_)) => {
                    warn!("Ignoring disk update response while waiting for reset");
                }
                Ok(ControlResponse::Paused) => {
                    warn!("Ignoring pause response while waiting for reset");
                }
                Ok(ControlResponse::SaveReady(_)) => {
                    warn!("Ignoring saved state response while waiting for reset");
                }
                Ok(ControlResponse::QMemDirty(_)) => {
                    warn!("Ignoring marking queue memory dirty response while waiting for disk update");
                }
                Err(e) => {
                    error!("Block worker failed to acknowledge reset: {:?}", e);
                    return None;
                }
            }
        }
    }

    pub(crate) fn update_disk_image(&self, disk_image_path: String, read_only: bool) -> Result<u64, VirtioBlockError> {
        if let Err(e) = self.control_tx.send(ControlMsg::UpdateDiskImage(disk_image_path, read_only)) {
            error!("Failed to send disk update message: {:?}", e);
            return Err(VirtioBlockError::WorkerControl(format!(
                "failed to send disk update request: {e}"
            )));
        }

        if let Err(e) = self.control_evt.write(1) {
            error!("Block control event is closed on disk update: {:?}", e);
            return Err(VirtioBlockError::WorkerControl(format!(
                "failed to notify block worker for disk update: {e}"
            )));
        }

        loop {
            match self.receiver_rx.recv() {
                Ok(ControlResponse::DiskUpdated(result)) => return result,
                Ok(ControlResponse::Paused) => {
                    warn!("Ignoring pause response while waiting for disk update");
                }
                Ok(ControlResponse::SaveReady(_)) => {
                    warn!("Ignoring saved state response while waiting for disk update");
                }
                Ok(ControlResponse::Reset(_)) => {
                    warn!("Ignoring reset response while waiting for disk update");
                }
                Ok(ControlResponse::QMemDirty(_)) => {
                    warn!("Ignoring marking queue memory dirty response while waiting for disk update");
                }
                Err(e) => {
                    error!("Block worker failed to acknowledge disk update: {:?}", e);
                    return Err(VirtioBlockError::WorkerControl(format!(
                        "failed to receive disk update response: {e}"
                    )));
                }
            }
        }
    }

    pub(crate) fn kick(&self) {
        if let Err(e) = self.control_tx.send(ControlMsg::Kick) {
            error!("Block receiver dropped on kick: {:?}", e);
        }

        if let Err(e) = self.control_evt.write(1) {
            error!("Block control event is closed on kick: {:?}", e);
        }
    }

    pub(crate) fn update_rate_limiter(&self, bytes: BucketUpdate, ops_update: BucketUpdate) {
        if let Err(e) = self.control_tx.send(ControlMsg::UpdateRateLimiter(bytes, ops_update)) {
            error!("Block receiver dropped on rate limiter update: {:?}", e);
        }

        if let Err(e) = self.control_evt.write(1) {
            error!("Block control event is closed on rate limiter update: {:?}", e);
        }
    }
}

impl BlockResources {
    pub(crate) fn reset_for_reactivation(&mut self) {
        self.is_io_engine_throttled = false;
        for queue in self.queues.iter_mut() {
            queue.reset();
        }
    }
}

impl BlockWorker {

    /// Process a single event in the Virtio queue.
    ///
    /// This function is called by the event manager when the guest notifies us
    /// about new buffers in the queue.
    pub(crate) fn process_queue_event(&mut self) {
        self.metrics.queue_event_count.inc();
        if let Err(err) = self.resources.queue_evts[0].read() {
            error!("Failed to get queue event: {:?}", err);
            self.metrics.event_fails.inc();
        } else if self.resources.rate_limiter.is_blocked() {
            self.metrics.rate_limiter_throttled_events.inc();
        } else if self.resources.is_io_engine_throttled {
            self.metrics.io_engine_throttled_events.inc();
        } else {
            self.process_virtio_queues().unwrap()
        }
    }

    /// Process device virtio queue(s).
    pub(crate) fn process_virtio_queues(&mut self) -> Result<(), InvalidAvailIdx> {
        self.process_queue(0)
    }

    pub(crate) fn process_rate_limiter_event(&mut self) {
        self.metrics.rate_limiter_event_count.inc();
        // Upon rate limiter event, call the rate limiter handler
        // and restart processing the queue.
        if self.resources.rate_limiter.event_handler().is_ok() {
            self.process_queue(0).unwrap()
        }
    }

    /// Device specific function for peaking inside a queue and processing descriptors.
    fn process_queue(&mut self, queue_index: usize) -> Result<(), InvalidAvailIdx> {
        let queue = &mut self.resources.queues[queue_index];
        let mut used_any = false;

        while let Some(head) = queue.pop_or_enable_notification()? {
            self.metrics.remaining_reqs_count.add(queue.len().into());
            let processing_result =
                match Request::parse(&head, &self.active_state.mem, self.resources.disk.nsectors) {
                    Ok(request) => {
                        if request.rate_limit(&mut self.resources.rate_limiter) {
                            // Stop processing the queue and return this descriptor chain to the
                            // avail ring, for later processing.
                            queue.undo_pop();
                            self.metrics.rate_limiter_throttled_events.inc();
                            break;
                        }

                        request.process(
                            &mut self.resources.disk,
                            head.index,
                            &self.active_state.mem,
                            &self.metrics,
                        )
                    }
                    Err(err) => {
                        error!("Failed to parse available descriptor chain: {:?}", err);
                        self.metrics.execute_fails.inc();
                        ProcessingResult::Executed(FinishedRequest {
                            num_bytes_to_mem: 0,
                            desc_idx: head.index,
                        })
                    }
                };

            match processing_result {
                ProcessingResult::Submitted => {}
                ProcessingResult::Throttled => {
                    queue.undo_pop();
                    self.resources.is_io_engine_throttled = true;
                    break;
                }
                ProcessingResult::Executed(finished) => {
                    used_any = true;
                    queue
                        .add_used(head.index, finished.num_bytes_to_mem)
                        .unwrap_or_else(|err| {
                            error!(
                                "Failed to add available descriptor head {}: {}",
                                head.index, err
                            )
                        });
                }
            }
        }
        queue.advance_used_ring_idx();

        if used_any && queue.prepare_kick() {
            self.active_state
                .interrupt
                .trigger(VirtioInterruptType::Queue(0))
                .unwrap_or_else(|_| {
                    self.metrics.event_fails.inc();
                });
        }

        if let FileEngine::Async(ref mut engine) = self.resources.disk.file_engine
            && let Err(err) = engine.kick_submission_queue()
        {
            error!("BlockError submitting pending block requests: {:?}", err);
        }

        if !used_any {
            self.metrics.no_avail_buffer.inc();
        }

        Ok(())
    }

    fn process_async_completion_queue(&mut self) {
        let engine = unwrap_async_file_engine_or_return!(&mut self.resources.disk.file_engine);
        let queue = &mut self.resources.queues[0];

        loop {
            match engine.pop(&self.active_state.mem) {
                Err(error) => {
                    error!("Failed to read completed io_uring entry: {:?}", error);
                    break;
                }
                Ok(None) => break,
                Ok(Some(cqe)) => {
                    let res = cqe.result();
                    let user_data = cqe.user_data();

                    let (pending, res) = match res {
                        Ok(count) => (user_data, Ok(count)),
                        Err(error) => (
                            user_data,
                            Err(IoErr::FileEngine(crate::devices::virtio::block::virtio::io::BlockIoError::Async(
                                async_io::AsyncIoError::IO(error),
                            ))),
                        ),
                    };
                    let finished = pending.finish(&self.active_state.mem, res, &self.metrics);
                    queue
                        .add_used(finished.desc_idx, finished.num_bytes_to_mem)
                        .unwrap_or_else(|err| {
                            error!(
                                "Failed to add available descriptor head {}: {}",
                                finished.desc_idx, err
                            )
                        });
                }
            }
        }
        queue.advance_used_ring_idx();

        if queue.prepare_kick() {
            self.active_state
                .interrupt
                .trigger(VirtioInterruptType::Queue(0))
                .unwrap_or_else(|_| {
                    self.metrics.event_fails.inc();
                });
        }
    }

    pub(crate) fn process_async_completion_event(&mut self) {
        let engine = unwrap_async_file_engine_or_return!(&mut self.resources.disk.file_engine);

        if let Err(err) = engine.completion_evt().read() {
            error!("Failed to get async completion event: {:?}", err);
        } else {
            self.process_async_completion_queue();

            if self.resources.is_io_engine_throttled {
                self.resources.is_io_engine_throttled = false;
                self.process_queue(0).unwrap()
            }
        }
    }

    pub(crate) fn drain_and_flush(&mut self, discard: bool) {
        if let Err(err) = self.resources.disk.file_engine.drain_and_flush(discard) {
            error!("Failed to drain ops and flush block data: {:?}", err);
        }
    }

    pub(crate) fn drain(&mut self, discard: bool) {
        if let Err(err) = self.resources.disk.file_engine.drain(discard) {
            error!("Failed to drain ops: {:?}", err);
        }
    }
    /// Prepare device for being snapshotted.
    pub fn prepare_save(&mut self) {
        self.drain_and_flush(false);
        if let FileEngine::Async(ref _engine) = self.resources.disk.file_engine {
            self.process_async_completion_queue();
        }
    }

    /// Update the backing file and return the new sector count.
    pub fn update_disk_image(&mut self, disk_image_path: String, read_only: bool) -> Result<u64, VirtioBlockError> {
        self.resources.disk.update(disk_image_path, read_only)?;
        Ok(self.resources.disk.nsectors)
    }

    /// Updates the parameters for the rate limiter
    pub fn update_rate_limiter(&mut self, bytes: BucketUpdate, ops: BucketUpdate) {
        self.resources.rate_limiter.update_buckets(bytes, ops);
    }
}

impl ThreadedWorker {
    const PROCESS_QUEUE: u32 = 0;
    const PROCESS_RATE_LIMITER: u32 = 1;
    const PROCESS_ASYNC_COMPLETION: u32 = 2;
    const PROCESS_CONTROL: u32 = 3;

    fn register_control_event(&self, ops: &mut EventOps) {
        if let Err(err) = ops.add(Events::with_data(
            &self.control_evt,
            Self::PROCESS_CONTROL,
            EventSet::IN,
        )) {
            error!("Failed to register control event: {}", err);
        }
    }

    fn register_runtime_events(resources: &BlockResources, ops: &mut EventOps) {
        if let Err(err) = ops.add(Events::with_data(
            &resources.queue_evts[0],
            Self::PROCESS_QUEUE,
            EventSet::IN,
        )) {
            error!("Failed to register queue event: {}", err);
        }
        if let Err(err) = ops.add(Events::with_data(
            &resources.rate_limiter,
            Self::PROCESS_RATE_LIMITER,
            EventSet::IN,
        )) {
            error!("Failed to register ratelimiter event: {}", err);
        }
        if let FileEngine::Async(ref engine) = resources.disk.file_engine
            && let Err(err) = ops.add(Events::with_data(
                engine.completion_evt(),
                Self::PROCESS_ASYNC_COMPLETION,
                EventSet::IN,
            ))
        {
            error!("Failed to register IO engine completion event: {}", err);
        }
    }

    fn unregister_runtime_events(resources: &BlockResources, ops: &mut EventOps) {
        if let Err(err) = ops.remove(Events::with_data(
            &resources.queue_evts[0],
            Self::PROCESS_QUEUE,
            EventSet::IN,
        )) {
            error!("Failed to unregister queue event: {}", err);
        }
        if let Err(err) = ops.remove(Events::with_data(
            &resources.rate_limiter,
            Self::PROCESS_RATE_LIMITER,
            EventSet::IN,
        )) {
            error!("Failed to unregister ratelimiter event: {}", err);
        }
        if let FileEngine::Async(ref engine) = resources.disk.file_engine
            && let Err(err) = ops.remove(Events::with_data(
                engine.completion_evt(),
                Self::PROCESS_ASYNC_COMPLETION,
                EventSet::IN,
            ))
        {
            error!("Failed to unregister IO engine completion event: {}", err);
        }
    }

    fn process_control_event(&mut self, ops: &mut EventOps) {
        if let Err(err) = self.control_evt.read() {
            error!("Failed to get control event: {:?}", err);
            if let Some(worker) = self.worker_mut() {
                worker.metrics.event_fails.inc();
            }
            return;
        }

        while let Ok(msg) = self.control_rx.try_recv() {
            self.process_control_msg(ops, msg);
            if self.is_finished() {
                break;
            }
        }
    }

    fn process_control_msg(&mut self, ops: &mut EventOps, msg: ControlMsg) {
        match msg {
            ControlMsg::Start(worker) => self.start_worker(worker, ops),
            ControlMsg::UpdateDiskImage(path, read_only) => {
                let result = if let Some(worker) = self.worker_mut() {
                    worker.update_disk_image(path, read_only)
                } else {
                    warn!("Disk image update requested while block worker is parked");
                    Err(VirtioBlockError::WorkerControl(
                        "disk image update requested while block worker is not running".to_string(),
                    ))
                };

                if let Err(err) = self.response_tx.send(ControlResponse::DiskUpdated(result)) {
                    error!("Failed to send DiskUpdated ACK: {:?}", err);
                }
            }
            ControlMsg::UpdateRateLimiter(bytes, ops_update) => {
                if let Some(worker) = self.worker_mut() {
                    worker.update_rate_limiter(bytes, ops_update);
                } else {
                    warn!("Rate limiter update requested while block worker is parked");
                }
            }
            ControlMsg::Pause => self.pause_worker(ops),
            ControlMsg::Reset => self.reset_worker(ops),
            ControlMsg::Kick => {
                if matches!(self.state, WorkerState::Paused(_)) {
                self.resume_worker(ops);
                }
                // process directly instead of going through epoll (regular kick)
                if let WorkerState::Running(worker) = &mut self.state {
                    worker
                        .process_virtio_queues()
                        .unwrap_or_else(|e| error!("Kick queue processing failed: {:?}", e));
                }
            }
            ControlMsg::Finish(flush_mode) => self.finish_worker(flush_mode, ops),
            ControlMsg::GetSavedState => {
                if let WorkerState::Paused(worker) | WorkerState::Running(worker) = &self.state {
                    let saved_state = SavedState {
                        queue_state: worker.resources.queues.iter().map(Persist::save).collect(),
                        rate_limiter_state: worker.resources.rate_limiter.save(),
                        disk_path: worker.resources.disk.file_path.clone(),
                        file_engine_type: match worker.resources.disk.file_engine {
                            FileEngine::Async(_) => FileEngineTypeState::Async,
                            FileEngine::Sync(_) => FileEngineTypeState::Sync,
                        },
                    };
                    if let Err(err) = self.response_tx.send(ControlResponse::SaveReady(saved_state)) {
                        error!("Failed to send DiskUpdated ACK: {:?}", err);
                    }
                }
            }
            ControlMsg::MarkQMemDirty => {
                let mut result = Ok(());
                if let WorkerState::Paused(worker) = &mut self.state {
                    let mem = worker.active_state.mem.clone();
                    for queue in worker.resources.queues.iter_mut() {
                        // mark them dirty for next snapshot
                        if let Err(e) = queue.initialize(&mem) {
                            result = Err(e);
                            break;
                        }
                    }
                }
                let _ = self.response_tx.send(ControlResponse::QMemDirty(result));
            }
        }
    }

    fn start_worker(&mut self, worker: BlockWorker, ops: &mut EventOps) {
        if !matches!(self.state, WorkerState::Parked) {
            warn!("Start requested while block worker is not parked");
            return;
        }

        Self::register_runtime_events(&worker.resources, ops);
        self.state = WorkerState::Running(worker);
    }

    fn pause_worker(&mut self, ops: &mut EventOps) {
        match std::mem::replace(&mut self.state, WorkerState::Parked) {
            WorkerState::Running(mut worker) => {
                Self::unregister_runtime_events(&worker.resources, ops);
                worker.prepare_save();
                if let Err(err) = self.response_tx.send(ControlResponse::Paused)
                { error!("Failed to send Paused ACK: {:?}", err); }
                self.state = WorkerState::Paused(worker);
            }
            WorkerState::Paused(worker) => {
                if let Err(err) = self.response_tx.send(ControlResponse::Paused)
                { error!("Failed to send Paused ACK: {:?}", err); }
                self.state = WorkerState::Paused(worker);
            }
            other => {
                warn!("Pause requested while block worker is not running");
                self.state = other;
            }
        }
    }

    fn resume_worker(&mut self, ops: &mut EventOps) {
        match std::mem::replace(&mut self.state, WorkerState::Parked) {
            WorkerState::Paused(worker) => {
                Self::register_runtime_events(&worker.resources, ops);
                self.state = WorkerState::Running(worker);
            }
            other => {
                warn!("Resume requested while block worker is not paused");
                self.state = other;
            }
        }
    }

    fn reset_worker(&mut self, ops: &mut EventOps) {
        match std::mem::replace(&mut self.state, WorkerState::Parked) {
            WorkerState::Running(mut worker) => {
                Self::unregister_runtime_events(&worker.resources, ops);
                worker.drain(true);
                worker.resources.reset_for_reactivation();
                if let Err(err) = self
                    .response_tx
                    .send(ControlResponse::Reset(Some(worker.resources)))
                { error!("Failed to send Reset ACK: {:?}", err); }
            }
            WorkerState::Paused(mut worker) => {
                worker.drain(true);
                worker.resources.reset_for_reactivation();
                if let Err(err) = self
                    .response_tx
                    .send(ControlResponse::Reset(Some(worker.resources)))
                { error!("Failed to send Reset ACK: {:?}", err); }
            }
            WorkerState::Parked => {
                warn!("Reset requested while block worker is parked");
                if let Err(err) = self.response_tx.send(ControlResponse::Reset(None))
                { error!("Failed to send Reset ACK: {:?}", err); }
            }
            WorkerState::Finished => {
                self.state = WorkerState::Finished;
            }
        }
    }

    fn finish_worker(&mut self, flush_mode: FlushMode, ops: &mut EventOps) {
        match std::mem::replace(&mut self.state, WorkerState::Parked) {
            WorkerState::Running(mut worker) => {
                Self::unregister_runtime_events(&worker.resources, ops);
                Self::flush_worker(&mut worker, flush_mode);
            }
            WorkerState::Paused(mut worker) => {
                Self::flush_worker(&mut worker, flush_mode);
            }
            WorkerState::Parked | WorkerState::Finished => {}
        }
        self.state = WorkerState::Finished;
    }

    fn flush_worker(worker: &mut BlockWorker, flush_mode: FlushMode) {
        match flush_mode {
            FlushMode::Drain => worker.drain(true),
            FlushMode::DrainAndFlush => worker.drain_and_flush(true),
        }
        worker.resources.is_io_engine_throttled = false;
    }

    fn worker_mut(&mut self) -> Option<&mut BlockWorker> {
        match &mut self.state {
            WorkerState::Running(worker) | WorkerState::Paused(worker) => Some(worker),
            WorkerState::Parked | WorkerState::Finished => None,
        }
    }

    fn is_finished(&self) -> bool {
        matches!(self.state, WorkerState::Finished)
    }
}

fn run_worker_loop(
    mut event_manager: EventManager,
    control_evt: EventFd,
    control_rx: Receiver<ControlMsg>,
    response_tx: Sender<ControlResponse>,
) {
    let worker = Arc::new(Mutex::new(ThreadedWorker {
        state: WorkerState::Parked,
        control_evt,
        control_rx,
        response_tx,
    }));
    let subscriber: Arc<Mutex<dyn MutEventSubscriber>> = worker.clone();
    event_manager.add_subscriber(subscriber);

    loop {
        if let Err(err) = event_manager.run() {
            error!("Block worker event loop error: {:?}", err);
        }
        if worker.lock().expect("Poisoned block worker lock").is_finished() {
            break;
        }
    }

    // drop the EventManager FIRST to release the subscriber clone
    // it holds, so worker_arc reaches strong_count == 1 and try_unwrap succeeds.
    drop(event_manager);
    let worker = Arc::try_unwrap(worker)
        .expect("Block worker refs outlived event loop")
        .into_inner()
        .expect("Poisoned lock");
    assert!(matches!(worker.state, WorkerState::Finished));
}

impl MutEventSubscriber for ThreadedWorker {
    fn process(&mut self, event: Events, ops: &mut EventOps) {
        let source = event.data();
        let event_set = event.event_set();

        // TODO: also check for errors. Pending high level discussions on how we want
        // to handle errors in devices.
        let supported_events = EventSet::IN;
        if !supported_events.contains(event_set) {
            warn!(
                "Block: Received unknown event: {:?} from source: {:?}",
                event_set, source
            );
            return;
        }

        if let WorkerState::Running(worker) = &mut self.state {
            match source {
                Self::PROCESS_QUEUE => worker.process_queue_event(),
                Self::PROCESS_RATE_LIMITER => worker.process_rate_limiter_event(),
                Self::PROCESS_ASYNC_COMPLETION => worker.process_async_completion_event(),
                Self::PROCESS_CONTROL => self.process_control_event(ops),
                _ => warn!("Block: Spurious event received: {:?}", source),
            }
        } else {
            match source {
                Self::PROCESS_CONTROL => self.process_control_event(ops),
                _ => warn!("Block: The device worker is not yet activated. Spurious event received: {:?}",source),
            }
        }

    }

    fn init(&mut self, ops: &mut EventOps) {
        self.register_control_event(ops);
    }
}
