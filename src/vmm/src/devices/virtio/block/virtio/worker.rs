// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Portions Copyright 2017 The Chromium OS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the THIRD-PARTY file.

use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread::{JoinHandle, Thread};
use event_manager::{EventOps, Events, MutEventSubscriber};
use crate::devices::virtio::block::virtio::device::BlockResources;
use crate::devices::virtio::block::virtio::metrics::BlockDeviceMetrics;
use crate::devices::virtio::block::virtio::{FinishedRequest, IoErr, ProcessingResult, Request, VirtioBlockError};
use crate::devices::virtio::device::ActiveState;
use crate::rate_limiter::BucketUpdate;
use vmm_sys_util::epoll::EventSet;
use vmm_sys_util::eventfd::EventFd;
use crate::devices::virtio::block::virtio::io::{async_io, FileEngine};
use crate::devices::virtio::queue::InvalidAvailIdx;
use crate::devices::virtio::transport::VirtioInterruptType;
use crate::logger::{IncMetric, error, warn};

/// Worker block device data
#[derive(Debug)]
pub(crate) struct BlockWorker {
    pub(crate) resources: BlockResources,
    pub(crate) active_state: ActiveState,
    pub(crate) metrics: Arc<BlockDeviceMetrics>,
}

/// Wrapper for threaded mode
pub(crate) struct ThreadedWorker {
    worker: BlockWorker,
    worker_state: WorkerState,
    control_rx: Receiver<ControlMsg>,
    response_tx: Sender<ControlResponse>,
    control_evt: EventFd,
}

enum WorkerState {
    Running,
    Paused,
    Finished,
}
enum ControlMsg {
    UpdateDiskImage(String),
    UpdateRateLimiter(BucketUpdate, BucketUpdate),
    Pause,
    Resume,
    Finish,
}

enum ControlResponse {
    DiskUpdated(Result<u64, VirtioBlockError>), // nsectors / error
    Paused, // ACK
}

#[derive(Debug)]
pub(crate) struct WorkerHandle {
    control_tx: Sender<ControlMsg>,
    receiver_rx: Receiver<ControlResponse>,
    control_evt: EventFd,
    join: JoinHandle<()>,
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
    /*
    /// Update the backing file and the config space of the block device.
    pub fn update_disk_image(&mut self, disk_image_path: String) -> Result<(), VirtioBlockError> {
        let read_only = self.read_only;
        self.resources_mut().disk.update(disk_image_path, read_only)?; // TRW: use channel instead
        self.config_space.capacity = self.disk().nsectors.to_le(); // virtio_block_config_space();

        // Kick the driver to pick up the changes. (But only if the device is already activated).
        if self.is_activated() {
            self.interrupt_trigger()
                .trigger(VirtioInterruptType::Config)
                .unwrap();
        }

        self.metrics.update_count.inc();
        Ok(())
    }*/

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

    fn register_runtime_events(&self, ops: &mut EventOps) {
        if let Err(err) = ops.add(Events::with_data(
            &self.worker.resources.queue_evts[0],
            Self::PROCESS_QUEUE,
            EventSet::IN,
        )) {
            error!("Failed to register queue event: {}", err);
        }
        if let Err(err) = ops.add(Events::with_data(
            &self.worker.resources.rate_limiter,
            Self::PROCESS_RATE_LIMITER,
            EventSet::IN,
        )) {
            error!("Failed to register ratelimiter event: {}", err);
        }
        if let FileEngine::Async(ref engine) = self.worker.resources.disk.file_engine
            && let Err(err) = ops.add(Events::with_data(
            engine.completion_evt(),
            Self::PROCESS_ASYNC_COMPLETION,
            EventSet::IN,
        ))
        {
            error!("Failed to register IO engine completion event: {}", err);
        }
        if let Err(err) = ops.add(Events::with_data(
            &self.control_evt,
            Self::PROCESS_CONTROL,
            EventSet::IN,
        )) {
            error!("Failed to register control event: {}", err);
        }
    }

    fn process_control_event(&mut self) {
        if let Err(err) = self.control_evt.read() {
            error!("Failed to get control event: {:?}", err);
            self.worker.metrics.event_fails.inc();
        }

        while let Ok(msg) = self.control_rx.try_recv() {
            match msg {
                ControlMsg::UpdateDiskImage(path) => todo!("step 4: control plane"),
                ControlMsg::UpdateRateLimiter(bytes, ops_update) => {
                    self.worker.update_rate_limiter(bytes, ops_update);
                }
                ControlMsg::Pause => {
                    self.worker.prepare_save();
                    if let Err(err) = self.response_tx.send(ControlResponse::Paused) {
                        error!("Failed to send Paused ACK: {:?}", err);
                    }
                    self.worker_state = WorkerState::Paused;
                }
                ControlMsg::Resume => {
                    self.worker_state = WorkerState::Running;
                }
                ControlMsg::Finish => {
                    todo!();
                }
            }
        }
    }
}

/*
    UpdateDiskImage(String),
    UpdateRateLimiter(BucketUpdate, BucketUpdate),
    Pause,
    Resume,
    Finish,
*/

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

        match source {
            Self::PROCESS_QUEUE => self.worker.process_queue_event(),
            Self::PROCESS_RATE_LIMITER => self.worker.process_rate_limiter_event(),
            Self::PROCESS_ASYNC_COMPLETION => self.worker.process_async_completion_event(),
            Self::PROCESS_CONTROL => self.process_control_event(),
            _ => warn!("Block: Spurious event received: {:?}", source),
        }

    }

    fn init(&mut self, ops: &mut EventOps) {
        self.register_runtime_events(ops);
    }
}
