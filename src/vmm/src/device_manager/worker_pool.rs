// Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! A fixed-size pool of device-emulation worker threads.
//!
//! This is a **benchmark prototype** (see
//! `DESIGN_DOCUMENTS/block-thread-pool-prototype-prd.md`). Its purpose is to measure the
//! performance of an N:M thread-pool model against the thread-per-queue-group and
//! thread-per-device models — not to ship.
//!
//! # Model
//!
//! At microVM boot the [`WorkerPool`] pre-spawns a fixed number (`pool_size`) of worker
//! threads. Each worker runs its **own** [`EventManager`] (epoll loop) and is parked in
//! `epoll_wait` until woken. Virtio devices are assigned to workers by deterministic
//! round-robin as they register; the device's `Arc<Mutex<dyn VirtioDevice>>` is handed to the
//! chosen worker, which adds it to its own `EventManager`. From then on, all of that device's
//! event processing (queue kicks, async completions, rate-limiter) runs on the worker thread
//! instead of the shared VMM thread.
//!
//! `pool_size == 0` means *no pool*: devices stay on the shared VMM `EventManager` exactly as
//! before (legacy path, untouched). The pool is therefore opt-in and off by default.
//!
//! # Handoff
//!
//! Because a worker is parked in `epoll_wait`, assignment is **push-then-kick**: the assigning
//! thread pushes a [`ControlMsg`] onto the worker's channel, then writes the worker's control
//! `EventFd` (which the worker registered on its own epoll via [`ControlSubscriber`]). The
//! worker wakes, drains the channel, and calls `add_subscriber` from its own thread.
//!
//! Assignment is **fire-and-forget**: the caller does not wait for the worker to confirm
//! registration. This is race-free because the device's queue-notify `EventFd` is a latched
//! counter — if the guest kicks the queue before the worker registers it, epoll reports it
//! readable as soon as the worker adds the subscriber. No notification is lost and the
//! assigning (vCPU) thread is never stalled.
//!
//! # Shutdown
//!
//! Teardown is explicit via [`WorkerPool::shutdown`], called from `Vmm::stop` while guest
//! memory and the MMIO bus are still alive. Each worker is sent [`ControlMsg::Shutdown`] +
//! kicked, then joined. Joining **before** any other teardown is the key invariant: a worker
//! must never touch freed guest memory or a dropped MMIO bus mid-batch. [`Drop`] is a
//! best-effort backstop in case `shutdown` was never called.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use event_manager::{EventOps, Events, MutEventSubscriber, SubscriberOps};
use vmm_sys_util::epoll::EventSet;
use vmm_sys_util::eventfd::EventFd;

use crate::EventManager;
use crate::devices::virtio::device::VirtioDevice;
use crate::logger::{error, info};
use crate::seccomp::BpfProgram;

/// Messages sent from the assigning thread to a worker over its control channel.
enum ControlMsg {
    /// Add a virtio device to this worker's `EventManager`. The worker calls `add_subscriber`
    /// from its own thread, so the device's event sources land on the worker's epoll.
    AddSubscriber(Arc<Mutex<dyn VirtioDevice>>),
    /// Terminate the worker's event loop so the thread can be joined.
    Shutdown,
}

/// A subscriber whose only job is to own the worker's control `EventFd` so that epoll wakes the
/// worker when the assigning thread kicks it. It carries no logic: the actual channel draining
/// happens in the worker loop after `EventManager::run` returns.
#[derive(Debug)]
struct ControlSubscriber {
    control_evt: EventFd,
}

impl MutEventSubscriber for ControlSubscriber {
    fn process(&mut self, _event: Events, _ops: &mut EventOps) {
        // Drain the counter so epoll (level-triggered) stops reporting the fd as ready. The
        // count is irrelevant — pending `ControlMsg`s are drained from the channel by the
        // worker loop once `run` returns.
        if let Err(err) = self.control_evt.read() {
            error!("WorkerPool: failed to read control eventfd: {err}");
        }
    }

    fn init(&mut self, ops: &mut EventOps) {
        if let Err(err) = ops.add(Events::new(&self.control_evt, EventSet::IN)) {
            error!("WorkerPool: failed to register control eventfd: {err}");
        }
    }
}

/// The assigning-thread-side handle to a single worker thread.
#[derive(Debug)]
struct Worker {
    /// Sends control messages to the worker.
    control_tx: Sender<ControlMsg>,
    /// A clone of the worker's control `EventFd`; writing it wakes the worker out of
    /// `epoll_wait`.
    control_evt: EventFd,
    /// The worker thread handle. `None` once joined.
    join: Option<JoinHandle<()>>,
}

/// A fixed-size pool of device-emulation worker threads. See the module docs.
#[derive(Debug)]
pub struct WorkerPool {
    workers: Vec<Worker>,
    /// Round-robin cursor. Atomic so assignment can take `&self` even though the pool is
    /// reachable behind shared borrows during device registration.
    next: AtomicUsize,
}

impl WorkerPool {
    /// Pre-spawn `pool_size` worker threads, each running its own `EventManager`. Each worker
    /// applies `seccomp_filter` as its first act (same filter as the VMM thread for the
    /// prototype, so the perf comparison is fair).
    ///
    /// `pool_size` must be non-zero; the caller is responsible for treating `0` as "no pool".
    pub fn new(pool_size: usize, seccomp_filter: Arc<BpfProgram>) -> WorkerPool {
        assert!(pool_size > 0, "WorkerPool::new called with pool_size == 0");

        let mut workers = Vec::with_capacity(pool_size);
        for id in 0..pool_size {
            // The control eventfd is shared between the assigning thread (writes to kick) and
            // the worker (registers on its epoll). Both refer to the same kernel object.
            let control_evt =
                EventFd::new(libc::EFD_NONBLOCK).expect("Failed to create worker control eventfd");
            let worker_evt = control_evt
                .try_clone()
                .expect("Failed to clone worker control eventfd");

            let (control_tx, control_rx) = channel::<ControlMsg>();
            let filter = seccomp_filter.clone();

            let join = thread::Builder::new()
                .name(format!("fc_dev_worker {id}"))
                .spawn(move || worker_loop(id, worker_evt, control_rx, filter))
                .expect("Failed to spawn device worker thread");

            workers.push(Worker {
                control_tx,
                control_evt,
                join: Some(join),
            });
        }

        info!("WorkerPool: spawned {pool_size} device worker thread(s)");
        WorkerPool {
            workers,
            next: AtomicUsize::new(0),
        }
    }

    /// Assign a virtio device to the next worker (round-robin) and wake it. Fire-and-forget:
    /// does not wait for the worker to register the device (see module docs for why this is
    /// race-free).
    pub fn assign(&self, device: Arc<Mutex<dyn VirtioDevice>>) {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        let worker = &self.workers[idx];

        if worker.control_tx.send(ControlMsg::AddSubscriber(device)).is_err() {
            error!("WorkerPool: worker {idx} channel closed; device not assigned");
            return;
        }
        if let Err(err) = worker.control_evt.write(1) {
            error!("WorkerPool: failed to kick worker {idx}: {err}");
        }
    }

    /// Signal every worker to stop and join all worker threads. Must be called while guest
    /// memory and the MMIO bus are still alive, so a worker can never touch freed state.
    /// Idempotent: workers already joined are skipped.
    pub fn shutdown(&mut self) {
        // Signal + kick every worker first, so they wind down concurrently...
        for (idx, worker) in self.workers.iter().enumerate() {
            if worker.join.is_none() {
                continue;
            }
            let _ = worker.control_tx.send(ControlMsg::Shutdown);
            if let Err(err) = worker.control_evt.write(1) {
                error!("WorkerPool: failed to kick worker {idx} for shutdown: {err}");
            }
        }
        // ...then join.
        for (idx, worker) in self.workers.iter_mut().enumerate() {
            if let Some(join) = worker.join.take()
                && join.join().is_err()
            {
                error!("WorkerPool: worker {idx} panicked during shutdown");
            }
        }
        info!("WorkerPool: all device worker threads joined");
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        // Best-effort backstop. In normal operation `Vmm::stop` calls `shutdown` explicitly
        // (before guest memory / MMIO bus teardown); by the time we get here that has usually
        // already happened and this is a no-op.
        self.shutdown();
    }
}

/// The body of a worker thread. Applies seccomp, then runs an event loop hosting any devices
/// assigned to this worker, until told to stop.
fn worker_loop(
    id: usize,
    control_evt: EventFd,
    control_rx: Receiver<ControlMsg>,
    seccomp_filter: Arc<BpfProgram>,
) {
    // Create the EventManager (and register the control eventfd) BEFORE applying seccomp.
    // `EventManager::new` calls `epoll_create1`, which the "vmm" filter does not permit — the
    // VMM thread likewise creates its epoll at boot and only applies the filter right before
    // its run loop. So we mirror that ordering: set up epoll first, lock down second.
    let mut event_manager = EventManager::new().expect("Failed to create worker EventManager");

    // Register the control eventfd so a kick wakes us out of `epoll_wait`.
    let control_sub = Arc::new(Mutex::new(ControlSubscriber { control_evt }));
    event_manager.add_subscriber(control_sub);

    // Seccomp is per-thread, so it must be applied here, inside the worker, not by whoever
    // spawned us. Empty filters (e.g. `--no-seccomp`) are a no-op in `apply_filter`.
    if let Err(err) = crate::seccomp::apply_filter(&seccomp_filter) {
        panic!("Failed to apply seccomp filter on device worker {id}: {err}");
    }

    loop {
        // Blocks until a device event or a control kick.
        if let Err(err) = event_manager.run() {
            error!("Device worker {id} EventManager error: {err:?}");
        }

        // A kick may carry several queued messages; drain them all.
        let mut stop = false;
        while let Ok(msg) = control_rx.try_recv() {
            match msg {
                ControlMsg::AddSubscriber(device) => {
                    // `add_subscriber` runs `device.init`, registering the device's event
                    // sources on *this* worker's epoll — exactly what we want.
                    event_manager.add_subscriber(device);
                }
                ControlMsg::Shutdown => stop = true,
            }
        }
        if stop {
            break;
        }
    }

    info!("Device worker {id} exiting");
}
