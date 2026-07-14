// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Portions Copyright 2017 The Chromium OS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the THIRD-PARTY file.

use std::cmp;
use std::convert::From;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::ops::Deref;
use std::os::linux::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::Arc;
use block_io::FileEngine;
use event_manager::{EventOps, Events};
use serde::{Deserialize, Serialize};
use vm_memory::ByteValued;
use vmm_sys_util::epoll::EventSet;
use vmm_sys_util::eventfd::EventFd;

use super::io::async_io;
use super::request::*;
use super::{BLOCK_QUEUE_SIZES, SECTOR_SHIFT, SECTOR_SIZE, VirtioBlockError, io as block_io};
use crate::devices::virtio::ActivateError;
use crate::devices::virtio::block::CacheType;
use crate::devices::virtio::block::device::Block;
use crate::devices::virtio::block::device::Block::Virtio;
use crate::devices::virtio::block::virtio::metrics::{BlockDeviceMetrics, BlockMetricsPerDevice};
use crate::devices::virtio::block::virtio::worker::{BlockWorker, FlushMode, WorkerHandle};
use crate::devices::virtio::device::{ActiveState, DeviceState, VirtioDevice, VirtioDeviceType};
use crate::devices::virtio::generated::virtio_blk::{
    VIRTIO_BLK_F_FLUSH, VIRTIO_BLK_F_RO, VIRTIO_BLK_ID_BYTES,
};
use crate::devices::virtio::generated::virtio_config::VIRTIO_F_VERSION_1;
use crate::devices::virtio::generated::virtio_ring::VIRTIO_RING_F_EVENT_IDX;
use crate::devices::virtio::queue::{InvalidAvailIdx, Queue, QueueError};
use crate::devices::virtio::transport::{VirtioInterrupt, VirtioInterruptType};
use crate::impl_device_type;
use crate::logger::{IncMetric, error, warn};
use crate::rate_limiter::{BucketUpdate, RateLimiter};
use crate::seccomp::BpfProgram;
use crate::utils::u64_to_usize;
use crate::vmm_config::RateLimiterConfig;
use crate::vmm_config::drive::BlockDeviceConfig;
use crate::vstate::memory::GuestMemoryMmap;

/// The engine file type, either Sync or Async (through io_uring).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum FileEngineType {
    /// Use an Async engine, based on io_uring.
    Async,
    /// Use a Sync engine, based on blocking system calls.
    #[default]
    Sync,
}

/// Helper object for setting up all `Block` fields derived from its backing file.
#[derive(Debug)]
pub struct DiskProperties {
    pub file_path: String,
    pub file_engine: FileEngine,
    pub nsectors: u64,
    pub image_id: [u8; VIRTIO_BLK_ID_BYTES as usize],
}

impl DiskProperties {
    // Helper function that opens the file with the proper access permissions
    fn open_file(disk_image_path: &str, is_disk_read_only: bool) -> Result<File, VirtioBlockError> {
        OpenOptions::new()
            .read(true)
            .write(!is_disk_read_only)
            .open(PathBuf::from(&disk_image_path))
            .map_err(|x| VirtioBlockError::BackingFile(x, disk_image_path.to_string()))
    }

    // Helper function that gets the size of the file
    fn file_size(disk_image_path: &str, disk_image: &mut File) -> Result<u64, VirtioBlockError> {
        let disk_size = disk_image
            .seek(SeekFrom::End(0))
            .map_err(|x| VirtioBlockError::BackingFile(x, disk_image_path.to_string()))?;

        // We only support disk size, which uses the first two words of the configuration space.
        // If the image is not a multiple of the sector size, the tail bits are not exposed.
        if disk_size % u64::from(SECTOR_SIZE) != 0 {
            warn!(
                "Disk size {} is not a multiple of sector size {}; the remainder will not be \
                 visible to the guest.",
                disk_size, SECTOR_SIZE
            );
        }

        Ok(disk_size)
    }

    /// Create a new file for the block device using a FileEngine
    pub fn new(
        disk_image_path: String,
        is_disk_read_only: bool,
        file_engine_type: FileEngineType,
    ) -> Result<Self, VirtioBlockError> {
        let mut disk_image = Self::open_file(&disk_image_path, is_disk_read_only)?;
        let disk_size = Self::file_size(&disk_image_path, &mut disk_image)?;
        let image_id = Self::build_disk_image_id(&disk_image);

        Ok(Self {
            file_path: disk_image_path,
            file_engine: FileEngine::from_file(disk_image, file_engine_type)
                .map_err(VirtioBlockError::FileEngine)?,
            nsectors: disk_size >> SECTOR_SHIFT,
            image_id,
        })
    }

    /// Update the path to the file backing the block device
    pub fn update(
        &mut self,
        disk_image_path: String,
        is_disk_read_only: bool,
    ) -> Result<(), VirtioBlockError> {
        let mut disk_image = Self::open_file(&disk_image_path, is_disk_read_only)?;
        let disk_size = Self::file_size(&disk_image_path, &mut disk_image)?;

        self.image_id = Self::build_disk_image_id(&disk_image);
        self.file_engine
            .update_file_path(disk_image)
            .map_err(VirtioBlockError::FileEngine)?;
        self.nsectors = disk_size >> SECTOR_SHIFT;
        self.file_path = disk_image_path;

        Ok(())
    }

    fn build_device_id(disk_file: &File) -> Result<String, VirtioBlockError> {
        let blk_metadata = disk_file
            .metadata()
            .map_err(VirtioBlockError::GetFileMetadata)?;
        // This is how kvmtool does it.
        let device_id = format!(
            "{}{}{}",
            blk_metadata.st_dev(),
            blk_metadata.st_rdev(),
            blk_metadata.st_ino()
        );
        Ok(device_id)
    }

    fn build_disk_image_id(disk_file: &File) -> [u8; VIRTIO_BLK_ID_BYTES as usize] {
        let mut default_id = [0; VIRTIO_BLK_ID_BYTES as usize];
        match Self::build_device_id(disk_file) {
            Err(_) => {
                warn!("Could not generate device id. We'll use a default.");
            }
            Ok(disk_id_string) => {
                // The kernel only knows to read a maximum of VIRTIO_BLK_ID_BYTES.
                // This will also zero out any leftover bytes.
                let disk_id = disk_id_string.as_bytes();
                let bytes_to_copy = cmp::min(disk_id.len(), VIRTIO_BLK_ID_BYTES as usize);
                default_id[..bytes_to_copy].copy_from_slice(&disk_id[..bytes_to_copy]);
            }
        }
        default_id
    }
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
#[repr(C)]
pub struct ConfigSpace {
    pub capacity: u64,
}

// SAFETY: `ConfigSpace` contains only PODs in `repr(C)` or `repr(transparent)`, without padding.
unsafe impl ByteValued for ConfigSpace {}

/// Use this structure to set up the Block Device before booting the kernel.
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VirtioBlockConfig {
    /// Unique identifier of the drive.
    pub drive_id: String,
    /// Part-UUID. Represents the unique id of the boot partition of this device. It is
    /// optional and it will be used only if the `is_root_device` field is true.
    pub partuuid: Option<String>,
    /// If set to true, it makes the current device the root block device.
    /// Setting this flag to true will mount the block device in the
    /// guest under /dev/vda unless the partuuid is present.
    pub is_root_device: bool,
    /// If set to true, the drive will ignore flush requests coming from
    /// the guest driver.
    #[serde(default)]
    pub cache_type: CacheType,

    /// If set to true, the drive is opened in read-only mode. Otherwise, the
    /// drive is opened as read-write.
    pub is_read_only: bool,
    /// Toggle for multithreaded device
    // default to false
    #[serde(default)]
    pub threaded: bool,
    /// Path of the backing file on the host
    pub path_on_host: String,
    /// Rate Limiter for I/O operations.
    pub rate_limiter: Option<RateLimiterConfig>,
    /// The type of IO engine used by the device.
    #[serde(default)]
    #[serde(rename = "io_engine")]
    pub file_engine_type: FileEngineType,
}

impl TryFrom<&BlockDeviceConfig> for VirtioBlockConfig {
    type Error = VirtioBlockError;

    fn try_from(value: &BlockDeviceConfig) -> Result<Self, Self::Error> {
        if let (Some(path_on_host), None) = (&value.path_on_host, &value.socket) {
            Ok(Self {
                drive_id: value.drive_id.clone(),
                partuuid: value.partuuid.clone(),
                is_root_device: value.is_root_device,
                cache_type: value.cache_type,

                is_read_only: value.is_read_only.unwrap_or(false),
                threaded: value.threaded,
                path_on_host: path_on_host.clone(),
                rate_limiter: value.rate_limiter,
                file_engine_type: value.file_engine_type.unwrap_or_default(),
            })
        } else {
            Err(VirtioBlockError::Config)
        }
    }
}

impl From<VirtioBlockConfig> for BlockDeviceConfig {
    fn from(value: VirtioBlockConfig) -> Self {
        Self {
            drive_id: value.drive_id,
            partuuid: value.partuuid,
            is_root_device: value.is_root_device,
            cache_type: value.cache_type,

            is_read_only: Some(value.is_read_only),
            threaded: value.threaded,
            path_on_host: Some(value.path_on_host),
            rate_limiter: value.rate_limiter,
            file_engine_type: Some(value.file_engine_type),

            socket: None,
        }
    }
}

/// Shared config/control surface of a block device, data-path live in BlockState
#[derive(Debug)]
pub struct VirtioBlock {
    // Virtio fields.
    pub avail_features: u64,
    pub acked_features: u64,
    pub config_space: ConfigSpace,
    pub activate_evt: EventFd,

    // Implementation specific fields.
    pub id: String,
    pub partuuid: Option<String>,
    pub cache_type: CacheType,
    pub root_device: bool,
    pub read_only: bool,
    pub threaded: bool,
    pub seccomp_filter: Arc<BpfProgram>,

    pub metrics: Arc<BlockDeviceMetrics>,
    pub(crate) state: BlockState,
}

/// State of data-path resources ownership
// TRW: review scope — pub(crate) only so persist.rs can construct on restore.
#[derive(Debug)]
pub(crate) enum BlockState {
    // Transport layer sets configuration inplace before activation
    Configuring(BlockResources, Option<WorkerHandle>),
    // Active state, worker owns data-path resources
    Active(ActiveBlock),
    // Placeholder to hold state when activating
    Placeholder,
}

/// Data-path resources owned by worker
// TRW: review scope — pub(crate) only so persist.rs can construct on restore.
#[derive(Debug)]
pub(crate) struct BlockResources {
    // Transport related fields.
    pub(crate) queues: Vec<Queue>,
    pub(crate) queue_evts: [EventFd; 1],

    // Host file and properties.
    pub(crate) disk: DiskProperties,
    pub(crate) rate_limiter: RateLimiter,
    pub(crate) is_io_engine_throttled: bool,
}

/// What config can access when device is active
#[derive(Debug)]
// one Inline instance/device - no mem lost
#[allow(clippy::large_enum_variant)]
pub(crate) enum ActiveBlock {
    Inline(InlineActive),
    Threaded(ThreadedActive),
}

/// Single threaded mode
#[derive(Debug)]
pub(crate) struct InlineActive {
    worker: BlockWorker,
    queue_cfg: Vec<QueueConfig>,
}

/// Multi-threaded mode
#[derive(Debug)]
pub(crate) struct ThreadedActive {
    pub(crate) worker_handle: WorkerHandle,
    interrupt: Arc<dyn VirtioInterrupt>,
    queue_cfg: Vec<QueueConfig>,
}

#[derive(Debug, Clone, Copy)]
struct QueueConfig {
    max_size: u16,
    ready: bool,
}

impl From<&Queue> for QueueConfig {
    fn from(queue: &Queue) -> Self {
        Self {
            max_size: queue.max_size,
            ready: queue.ready,
        }
    }
}

impl VirtioBlock {
    const PROCESS_ACTIVATE: u32 = 0;
    const PROCESS_QUEUE: u32 = 1;
    const PROCESS_RATE_LIMITER: u32 = 2;
    const PROCESS_ASYNC_COMPLETION: u32 = 3;

    /// Create a new virtio block device that operates on the given file.
    ///
    /// The given file must be seekable and sizable.
    pub fn new(config: VirtioBlockConfig) -> Result<VirtioBlock, VirtioBlockError> {
        let disk_properties = DiskProperties::new(
            config.path_on_host,
            config.is_read_only,
            config.file_engine_type,
        )?;

        let rate_limiter = config
            .rate_limiter
            .map(RateLimiter::from)
            .unwrap_or_default();

        let blk_resources = BlockResources {
            queues: BLOCK_QUEUE_SIZES.iter().map(|&s| Queue::new(s)).collect(),
            queue_evts: [EventFd::new(libc::EFD_NONBLOCK).map_err(VirtioBlockError::EventFd)?],
            disk: disk_properties,
            rate_limiter,
            is_io_engine_throttled: false,
        };

        let mut avail_features = (1u64 << VIRTIO_F_VERSION_1) | (1u64 << VIRTIO_RING_F_EVENT_IDX);

        if config.cache_type == CacheType::Writeback {
            avail_features |= 1u64 << VIRTIO_BLK_F_FLUSH;
        }

        if config.is_read_only {
            avail_features |= 1u64 << VIRTIO_BLK_F_RO;
        };

        let config_space = ConfigSpace {
            capacity: blk_resources.disk.nsectors.to_le(),
        };

        Ok(VirtioBlock {
            avail_features,
            acked_features: 0u64,
            config_space,
            activate_evt: EventFd::new(libc::EFD_NONBLOCK).map_err(VirtioBlockError::EventFd)?,

            id: config.drive_id.clone(),
            partuuid: config.partuuid,
            cache_type: config.cache_type,
            root_device: config.is_root_device,
            read_only: config.is_read_only,

            threaded: config.threaded,
            state: BlockState::Configuring(blk_resources, None),
            metrics: BlockMetricsPerDevice::alloc(config.drive_id),
            seccomp_filter: Arc::new(vec![]),
        })
    }

    fn resources(&self) -> &BlockResources {
        match &self.state {
            BlockState::Configuring(res,_) => res,
            BlockState::Active(ActiveBlock::Inline(ab)) => &ab.worker.resources,
            BlockState::Active(ActiveBlock::Threaded(_)) => unreachable!("to be handled cleanly"), // TRW
            BlockState::Placeholder => unreachable!("not a runtime state"),
        }
    }

    pub(crate) fn resources_mut(&mut self) -> &mut BlockResources {
        match &mut self.state {
            BlockState::Configuring(res, _) => res,
            BlockState::Active(ActiveBlock::Inline(ab)) => &mut ab.worker.resources,
            BlockState::Active(ActiveBlock::Threaded(_)) => unreachable!("to be handled cleanly"), // TRW
            BlockState::Placeholder => unreachable!("not a runtime state"),
        }
    }

    pub(crate) fn set_worker_filter(&mut self, filter: Arc<BpfProgram>) {
        self.seccomp_filter = filter;
    }

    pub(crate) fn disk(&self) -> &DiskProperties { &self.resources().disk }
    pub(crate) fn rate_limiter(&self) -> &RateLimiter { &self.resources().rate_limiter }

    /// Retrieve the file engine type.
    pub(crate) fn file_engine_type(&self) -> FileEngineType {
        match self.disk().file_engine {
            FileEngine::Sync(_) => FileEngineType::Sync,
            FileEngine::Async(_) => FileEngineType::Async,
        }
    }

    /// Returns a copy of a device config
    pub fn config(&self) -> VirtioBlockConfig {
        let rl: RateLimiterConfig = self.rate_limiter().into();
        VirtioBlockConfig {
            drive_id: self.id.clone(),
            path_on_host: self.disk().file_path.clone(),
            is_root_device: self.root_device,
            partuuid: self.partuuid.clone(),
            is_read_only: self.read_only,
            threaded: self.threaded,
            cache_type: self.cache_type,
            rate_limiter: rl.into_option(),
            file_engine_type: self.file_engine_type(),
        }
    }

    /// Update the backing file and the config space of the block device.
    pub fn update_disk_image(&mut self, disk_image_path: String) -> Result<(), VirtioBlockError> {
        let read_only = self.read_only;
        let nsectors = match &mut self.state {
            BlockState::Configuring(res, _) => {
                res.disk.update(disk_image_path, read_only)?;
                res.disk.nsectors
            }
            BlockState::Active(ActiveBlock::Inline(ab)) => {
                ab.worker.update_disk_image(disk_image_path, read_only)?
            }
            BlockState::Active(ActiveBlock::Threaded(ab)) => {
                ab.worker_handle
                    .update_disk_image(disk_image_path, read_only)?
            }
            BlockState::Placeholder => unreachable!("not a runtime state"),
        };

        self.config_space.capacity = nsectors.to_le();

        // Kick the driver to pick up the changes. (But only if the device is already activated).
        if self.is_activated() {
            self.interrupt_trigger()
                .trigger(VirtioInterruptType::Config)
                .unwrap();
        }

        self.metrics.update_count.inc();
        Ok(())
    }

    /// Updates the parameters for the rate limiter
    pub fn update_rate_limiter(&mut self, bytes: BucketUpdate, ops: BucketUpdate) {
        match &mut self.state {
            BlockState::Configuring(res, _) => res.rate_limiter.update_buckets(bytes, ops),
            BlockState::Active(ActiveBlock::Threaded(ab)) => ab.worker_handle.update_rate_limiter(bytes, ops),
            BlockState::Active(ActiveBlock::Inline(ab)) => ab.worker.update_rate_limiter(bytes, ops),
            _ => unreachable!("not a runtime state"),
        }
    }

    pub(crate) fn process_virtio_queues(&mut self) -> Result<(), InvalidAvailIdx> {
        if let BlockState::Active(ActiveBlock::Inline(ab)) = &mut self.state {
            ab.worker.process_virtio_queues()
        } else {
            Ok(())
        }
    }

    /// Test-only forwarder so test helpers can drive the worker's queue handler directly
    /// (production drives it through the `MutEventSubscriber` dispatch instead).
    #[cfg(test)]
    pub(crate) fn process_queue_event(&mut self) {
        if let BlockState::Active(ActiveBlock::Inline(ab)) = &mut self.state {
            ab.worker.process_queue_event()
        }
    }

    /// Test-only forwarder for the worker's async-completion handler.
    #[cfg(test)]
    pub(crate) fn process_async_completion_event(&mut self) {
        if let BlockState::Active(ActiveBlock::Inline(ab)) = &mut self.state {
            ab.worker.process_async_completion_event()
        }
    }

    /// Single thread path prepare save redirecting work to BlockWorker
    pub fn prepare_save(&mut self) {
        match &mut self.state {
            BlockState::Active(ActiveBlock::Inline(ab)) =>ab.worker.prepare_save(),
            BlockState::Active(ActiveBlock::Threaded(ta)) => ta.worker_handle.pause(),
            _ => {},
        }
    }

    fn register_runtime_events(&self, ops: &mut EventOps) {
        if let Err(err) = ops.add(Events::with_data(
            &self.resources().queue_evts[0],
            Self::PROCESS_QUEUE,
            EventSet::IN,
        )) {
            error!("Failed to register queue event: {}", err);
        }
        if let Err(err) = ops.add(Events::with_data(
            &self.resources().rate_limiter,
            Self::PROCESS_RATE_LIMITER,
            EventSet::IN,
        )) {
            error!("Failed to register ratelimiter event: {}", err);
        }
        if let FileEngine::Async(ref engine) = self.resources().disk.file_engine
            && let Err(err) = ops.add(Events::with_data(
            engine.completion_evt(),
            Self::PROCESS_ASYNC_COMPLETION,
            EventSet::IN,
        ))
        {
            error!("Failed to register IO engine completion event: {}", err);
        }
    }

    fn register_activate_event(&self, ops: &mut EventOps) {
        if let Err(err) = ops.add(Events::with_data(
            &self.activate_evt,
            Self::PROCESS_ACTIVATE,
            EventSet::IN,
        )) {
            error!("Failed to register activate event: {}", err);
        }
    }

    fn process_activate_event(&mut self, ops: &mut EventOps) {
        if let Err(err) = self.activate_evt.read() {
            error!("Failed to consume block activate event: {:?}", err);
        }

        // threaded: registers events at init()
        // when the spawned thread subscribes to its event manager
        if !self.threaded {
            self.register_runtime_events(ops);
        }

        // threaded virtio block becomes zombie sub
        if let Err(err) = ops.remove(Events::with_data(
            &self.activate_evt,
            Self::PROCESS_ACTIVATE,
            EventSet::IN,
        )) {
            error!("Failed to un-register activate event: {}", err);
        }
    }

    pub(crate) fn init_events(&mut self, ops: &mut EventOps) {
        if self.is_activated() {
            // hit on restore where the device is already active at sub time
            if !self.threaded {
                self.register_runtime_events(ops);
            }
        } else {
            self.register_activate_event(ops);
        }
    }

    pub(crate) fn process_event(&mut self, source: u32, ops: &mut EventOps) {
        if self.is_activated() {
            match source {
                Self::PROCESS_ACTIVATE => self.process_activate_event(ops),
                Self::PROCESS_QUEUE | Self::PROCESS_RATE_LIMITER | Self::PROCESS_ASYNC_COMPLETION => {
                    if let BlockState::Active(ActiveBlock::Inline(ab)) = &mut self.state {
                        match source {
                            Self::PROCESS_QUEUE => ab.worker.process_queue_event(),
                            Self::PROCESS_RATE_LIMITER => ab.worker.process_rate_limiter_event(),
                            Self::PROCESS_ASYNC_COMPLETION => ab.worker.process_async_completion_event(),
                            _ => unreachable!(),
                        }
                    }
                }
                _ => warn!("Block: Spurious event received: {source:?}"),
            }
        } else {
            warn!(
                "Block: The device is not yet activated. Spurious event received: {:?}",
                source
            );
            match source {
                Self::PROCESS_QUEUE => self.drain_queue_events(),
                Self::PROCESS_RATE_LIMITER => {
                    self.resources_mut().rate_limiter.event_handler();
                }
                Self::PROCESS_ASYNC_COMPLETION => {
                    if let FileEngine::Async(ref engine) = self.resources().disk.file_engine {
                        engine.completion_evt().read();
                    }
                }
                _ => (),
            }
        }
    }
}

impl VirtioDevice for VirtioBlock {
    impl_device_type!(VirtioDeviceType::Block);

    fn avail_features(&self) -> u64 {
        self.avail_features
    }

    fn acked_features(&self) -> u64 {
        self.acked_features
    }

    fn set_acked_features(&mut
                          self, acked_features: u64) {
        self.acked_features = acked_features;
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn queues(&self) -> &[Queue] {
        &self.resources().queues
    }

    fn queues_mut(&mut self) -> &mut [Queue] {
        &mut self.resources_mut().queues
    }

    fn queue_events(&self) -> &[EventFd] { // reached at hot-unplug on active state
        match &self.state {
            BlockState::Configuring(res, _) => &res.queue_evts,
            BlockState::Active(ActiveBlock::Inline(ab)) => &ab.worker.resources.queue_evts,
            BlockState::Active(ActiveBlock::Threaded(ta)) => ta.worker_handle.queue_events(),
            BlockState::Placeholder => unreachable!("not a runtime state"),
        }
    }

    fn interrupt_trigger(&self) -> &dyn VirtioInterrupt {
        match &self.state {
            BlockState::Active(ActiveBlock::Inline(ab)) => ab.worker.active_state.interrupt.deref(),
            BlockState::Active(ActiveBlock::Threaded(ta)) => ta.interrupt.deref(),
            _ => panic!("Device not initialized"),
        }
    }

    fn config_as_bytes(&self) -> &[u8] {
        self.config_space.as_slice()
    }

    fn write_config(&mut self, offset: u64, data: &[u8]) {
        let config_space_bytes = self.config_space.as_mut_slice();
        let start = usize::try_from(offset).ok();
        let end = start.and_then(|s| s.checked_add(data.len()));
        let Some(dst) = start
            .zip(end)
            .and_then(|(start, end)| config_space_bytes.get_mut(start..end))
        else {
            self.metrics.cfg_fails.inc();
            warn!(
                "virtio-block: guest driver attempted to write device config out of bounds \
                 (offset={:#x}, len={:#x})",
                offset,
                data.len()
            );
            return;
        };

        dst.copy_from_slice(data);
    }

    fn activate(
        &mut self,
        mem: GuestMemoryMmap,
        interrupt: Arc<dyn VirtioInterrupt>,
    ) -> Result<(), ActivateError> {
        assert!(!self.is_activated());

        if !matches!(self.state, BlockState::Configuring(..)) {
            return Ok(());
        }

        let BlockState::Configuring(mut res, handle ) =
            std::mem::replace(&mut self.state, BlockState::Placeholder)
        else {
            unreachable!("state checked to be Configuring above");
        };

        for q in res.queues.iter_mut() {
            q.initialize(&mem)
                .map_err(ActivateError::QueueMemoryError)?;
        }

        let event_idx = self.has_feature(u64::from(VIRTIO_RING_F_EVENT_IDX));
        if event_idx {
            for queue in &mut res.queues {
                queue.enable_notif_suppression();
            }
        }

        let queue_cfg = res.queues.iter().map(QueueConfig::from).collect();

        let worker = BlockWorker {
            resources: res,
            active_state: ActiveState { mem, interrupt: interrupt.clone(), },
            metrics: self.metrics.clone(),
        };

        if self.threaded {
            let worker_handle = handle.expect("Worker thread must be spawned before activation");
            worker_handle.start(worker);
            self.state = BlockState::Active(ActiveBlock::Threaded(ThreadedActive { worker_handle, interrupt, queue_cfg }));
        } else {
            self.state = BlockState::Active(ActiveBlock::Inline(InlineActive { worker, queue_cfg }));
        }

        if self.activate_evt.write(1).is_err() {
            self.metrics.activate_fails.inc();
            return Err(ActivateError::EventFd);
        }

        Ok(())
    }

    fn is_activated(&self) -> bool { matches!(&self.state, BlockState::Active(_)) }

    fn deactivate(&mut self) {
        let (res, handle) = match std::mem::replace(&mut self.state, BlockState::Placeholder) {
            BlockState::Active(ActiveBlock::Threaded(ta)) => {
                let Some(res) = ta.worker_handle.reset() else {
                    self.state = BlockState::Active(ActiveBlock::Threaded(ta));
                    return;
                };
                (res, Some(ta.worker_handle))
            }
            BlockState::Active(ActiveBlock::Inline(mut ab)) => {
                ab.worker.drain(true);
                let mut res = ab.worker.resources;
                res.reset_for_reactivation();
                (res, None)
            }
            other => {
                self.state = other;
                return;
            }
        };
        self.state = BlockState::Configuring(res, handle);
    }

    fn reset(&mut self) -> bool {
        self.deactivate();
        if self.is_activated() {
            return false;
        }
        self.set_acked_features(0);

        if let BlockState::Configuring(res, _) = &mut self.state {
            res.reset_for_reactivation();
        } else {
            return false;
        }

        true
    }

    fn _reset(&mut self) -> bool { true }

    fn kick(&mut self) {
        match &self.state {
            BlockState::Active(ActiveBlock::Threaded(ab)) => ab.worker_handle.kick(),
            BlockState::Active(ActiveBlock::Inline(_)) => self.notify_queue_events(),
            _ => {} // not active = nothing to kick
        }
    }

    fn mark_queue_memory_dirty(&mut self, mem: &GuestMemoryMmap) -> Result<(), QueueError> {
        match &mut self.state {
            BlockState::Active(ActiveBlock::Threaded(ta)) => ta.worker_handle.mark_queue_memory_dirty(mem),
            BlockState::Active(ActiveBlock::Inline(ab)) => {
                for queue in ab.worker.resources.queues.clone().iter_mut() {
                    queue.initialize(mem)?
                }
                Ok(())
            },
            _ => Ok(())
        }
    }

    fn spawn_worker(&mut self) -> Result<(), VirtioBlockError>{
        if !self.threaded {
            return Ok(());
        }
        if let BlockState::Configuring(res, handle @ None) = &mut self.state {
            let queue_evts = res
                .queue_evts
                .iter()
                .map(EventFd::try_clone)
                .collect::<Result<Vec<_>, _>>()
                .map_err(VirtioBlockError::EventFd)?;

            let worker = WorkerHandle::spawn(self.seccomp_filter.clone(), queue_evts)
                .map_err(VirtioBlockError::ThreadSpawn)?;

            *handle = Some(worker);
        }
        Ok(())
    }
}

impl ThreadedActive {
    fn teardown(self, flush_mode: FlushMode) {
        self.worker_handle.finish(flush_mode);
    }
}

impl Drop for VirtioBlock {
    fn drop(&mut self) {
        let flush_mode = match self.cache_type {
            CacheType::Unsafe => FlushMode::Drain,
            CacheType::Writeback => FlushMode::DrainAndFlush,
        };
        match std::mem::replace(&mut self.state, BlockState::Placeholder)
        {
            BlockState::Active(ActiveBlock::Threaded(ab)) => ab.teardown(flush_mode),
            BlockState::Active(ActiveBlock::Inline(mut ab)) => {
                match flush_mode {
                    FlushMode::Drain => { ab.worker.drain(true); },
                    FlushMode::DrainAndFlush => { ab.worker.drain_and_flush(true); },
                }
            }
            // drop before activated, still finish thread clean even tho nothing to drain
            BlockState::Configuring(_, Some(handle)) => handle.finish(FlushMode::Drain),
            _ => {}
        };
    }
}


#[cfg(test)]
mod tests {
    use std::fs::metadata;
    use std::io::{Read, Write};
    use std::os::unix::ffi::OsStrExt;
    use std::thread;
    use std::time::Duration;

    use vmm_sys_util::tempfile::TempFile;

    use super::*;
    use crate::check_metric_after_block;
    use crate::devices::virtio::block::virtio::IO_URING_NUM_ENTRIES;
    use crate::devices::virtio::block::virtio::test_utils::{
        default_block, read_blk_req_descriptors, set_queue, set_rate_limiter,
        simulate_async_completion_event, simulate_queue_and_async_completion_events,
        simulate_queue_event,
    };
    use crate::devices::virtio::queue::{VIRTQ_DESC_F_NEXT, VIRTQ_DESC_F_WRITE};
    use crate::devices::virtio::test_utils::{VirtQueue, default_interrupt, default_mem};
    use crate::rate_limiter::TokenType;
    use crate::snapshot::Persist;
    use crate::vstate::memory::{Address, Bytes, GuestAddress};

    #[test]
    fn test_from_config() {
        let block_config = BlockDeviceConfig {
            drive_id: "".to_string(),
            partuuid: None,
            is_root_device: false,
            cache_type: CacheType::Unsafe,

            is_read_only: Some(true),
            threaded: false,
            path_on_host: Some("path".to_string()),
            rate_limiter: None,
            file_engine_type: Default::default(),

            socket: None,
        };
        VirtioBlockConfig::try_from(&block_config).unwrap();

        let block_config = BlockDeviceConfig {
            drive_id: "".to_string(),
            partuuid: None,
            is_root_device: false,
            cache_type: CacheType::Unsafe,

            is_read_only: None,
            threaded: false,
            path_on_host: None,
            rate_limiter: None,
            file_engine_type: Default::default(),

            socket: Some("sock".to_string()),
        };
        VirtioBlockConfig::try_from(&block_config).unwrap_err();

        let block_config = BlockDeviceConfig {
            drive_id: "".to_string(),
            partuuid: None,
            is_root_device: false,
            cache_type: CacheType::Unsafe,

            is_read_only: Some(true),
            threaded: false,
            path_on_host: Some("path".to_string()),
            rate_limiter: None,
            file_engine_type: Default::default(),

            socket: Some("sock".to_string()),
        };
        VirtioBlockConfig::try_from(&block_config).unwrap_err();
    }

    #[test]
    fn test_disk_backing_file_helper() {
        let num_sectors = 2;
        let f = TempFile::new().unwrap();
        let size = u64::from(SECTOR_SIZE) * num_sectors;
        f.as_file().set_len(size).unwrap();

        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let disk_properties =
                DiskProperties::new(String::from(f.as_path().to_str().unwrap()), true, engine)
                    .unwrap();

            assert_eq!(size, u64::from(SECTOR_SIZE) * num_sectors);
            assert_eq!(disk_properties.nsectors, num_sectors);
            // Testing `backing_file.virtio_block_disk_image_id()` implies
            // duplicating that logic in tests, so skipping it.

            let res = DiskProperties::new("invalid-disk-path".to_string(), true, engine);
            assert!(
                matches!(res, Err(VirtioBlockError::BackingFile(_, _))),
                "{:?}",
                res
            );
        }
    }

    #[test]
    fn test_virtio_features() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);

            assert_eq!(block.device_type(), VirtioDeviceType::Block);

            let features: u64 = (1u64 << VIRTIO_F_VERSION_1) | (1u64 << VIRTIO_RING_F_EVENT_IDX);

            assert_eq!(
                block.avail_features_by_page(0),
                (features & 0xffffffff) as u32,
            );
            assert_eq!(block.avail_features_by_page(1), (features >> 32) as u32);

            for i in 2..10 {
                assert_eq!(block.avail_features_by_page(i), 0u32);
            }

            for i in 0..10 {
                block.ack_features_by_page(i, u32::MAX);
            }
            assert_eq!(block.acked_features, features);
        }
    }

    #[test]
    fn test_config_as_bytes() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let block = default_block(engine);

            let config = block.config_as_bytes();
            // The block's backing file size is 0x1000, so there are 8 (4096/512) sectors.
            let expected_config_space = ConfigSpace { capacity: 8 };
            assert_eq!(config, expected_config_space.as_slice());
        }
    }

    #[test]
    fn test_virtio_write_config() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);

            let expected_config_space = ConfigSpace { capacity: 696969 };
            block.write_config(0, expected_config_space.as_slice());

            let mut actual_config_space = ConfigSpace::default();
            block.read_config(0, actual_config_space.as_mut_slice());
            assert_eq!(actual_config_space, expected_config_space);

            // If privileged user writes to `/dev/mem`, in block config space - byte by byte.
            let expected_config_space = ConfigSpace {
                capacity: 0x1122334455667788,
            };
            let expected_config_space_slice = expected_config_space.as_slice();
            for (i, b) in expected_config_space_slice.iter().enumerate() {
                block.write_config(i as u64, &[*b]);
            }
            block.read_config(0, actual_config_space.as_mut_slice());
            assert_eq!(actual_config_space, expected_config_space);

            // Invalid write.
            let new_config_space = ConfigSpace {
                capacity: 0xDEADBEEF,
            };
            block.write_config(5, new_config_space.as_slice());
            // Make sure nothing got written.
            block.read_config(0, actual_config_space.as_mut_slice());
            assert_eq!(actual_config_space, expected_config_space);

            // Large offset that may cause an overflow.
            block.write_config(u64::MAX, new_config_space.as_slice());
            // Make sure nothing got written.
            block.read_config(0, actual_config_space.as_mut_slice());
            assert_eq!(actual_config_space, expected_config_space);
        }
    }

    #[test]
    fn test_invalid_request() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);
            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
            set_queue(&mut block, 0, vq.create_queue());
            block.activate(mem.clone(), interrupt).unwrap();
            read_blk_req_descriptors(&vq);

            let request_type_addr = GuestAddress(vq.dtable[0].addr.get());

            // Request is invalid because the first descriptor is write-only.
            vq.dtable[0]
                .flags
                .set(VIRTQ_DESC_F_NEXT | VIRTQ_DESC_F_WRITE);
            mem.write_obj::<u32>(VIRTIO_BLK_T_IN, request_type_addr)
                .unwrap();

            simulate_queue_event(&mut block, Some(true));

            assert_eq!(vq.used.idx.get(), 1);
            assert_eq!(vq.used.ring[0].get().id, 0);
            assert_eq!(vq.used.ring[0].get().len, 0);
        }
    }

    #[test]
    fn test_addr_out_of_bounds() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);
            // Default mem size is 0x10000
            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
            set_queue(&mut block, 0, vq.create_queue());
            block.activate(mem.clone(), interrupt).unwrap();
            read_blk_req_descriptors(&vq);
            let request_type_addr = GuestAddress(vq.dtable[0].addr.get());

            // Read at out of bounds address.
            {
                vq.used.idx.set(0);
                set_queue(&mut block, 0, vq.create_queue());

                // Mark the next available descriptor.
                vq.avail.idx.set(1);

                vq.dtable[1].set(0x20000, 0x1000, VIRTQ_DESC_F_NEXT | VIRTQ_DESC_F_WRITE, 2);
                mem.write_obj::<u32>(VIRTIO_BLK_T_IN, request_type_addr)
                    .unwrap();

                simulate_queue_and_async_completion_events(&mut block, true);

                assert_eq!(vq.used.idx.get(), 1);

                let used = vq.used.ring[0].get();
                let status_addr = GuestAddress(vq.dtable[2].addr.get());
                assert_eq!(used.len, 1);
                assert_eq!(
                    u32::from(mem.read_obj::<u8>(status_addr).unwrap()),
                    VIRTIO_BLK_S_IOERR
                );
            }

            // Write at out of bounds address.
            {
                vq.used.idx.set(0);
                set_queue(&mut block, 0, vq.create_queue());

                // Mark the next available descriptor.
                vq.avail.idx.set(1);

                vq.dtable[1].set(0x20000, 0x1000, VIRTQ_DESC_F_NEXT, 2);
                mem.write_obj::<u32>(VIRTIO_BLK_T_OUT, request_type_addr)
                    .unwrap();

                simulate_queue_and_async_completion_events(&mut block, true);

                assert_eq!(vq.used.idx.get(), 1);

                let used = vq.used.ring[0].get();
                let status_addr = GuestAddress(vq.dtable[2].addr.get());
                assert_eq!(used.len, 1);
                assert_eq!(
                    u32::from(mem.read_obj::<u8>(status_addr).unwrap()),
                    VIRTIO_BLK_S_IOERR
                );
            }
        }
    }

    #[test]
    fn test_request_parse_failures() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);
            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
            set_queue(&mut block, 0, vq.create_queue());
            block.activate(mem.clone(), interrupt).unwrap();
            read_blk_req_descriptors(&vq);

            let request_type_addr = GuestAddress(vq.dtable[0].addr.get());

            {
                // First descriptor no longer writable.
                vq.dtable[0].flags.set(VIRTQ_DESC_F_NEXT);
                vq.dtable[1].flags.set(VIRTQ_DESC_F_NEXT);

                // Generate a seek execute error caused by a very large sector number.
                let request_header = RequestHeader::new(VIRTIO_BLK_T_OUT, 0x000f_ffff_ffff);
                mem.write_obj::<RequestHeader>(request_header, request_type_addr)
                    .unwrap();

                simulate_queue_event(&mut block, Some(true));

                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);
                assert_eq!(vq.used.ring[0].get().len, 0);
            }

            {
                // Reset the queue to reuse descriptors and memory.
                vq.used.idx.set(0);
                set_queue(&mut block, 0, vq.create_queue());

                vq.dtable[1]
                    .flags
                    .set(VIRTQ_DESC_F_NEXT | VIRTQ_DESC_F_WRITE);
                // Set sector to a valid number large enough that the full 0x1000 read will fail.
                let request_header = RequestHeader::new(VIRTIO_BLK_T_IN, 10);
                mem.write_obj::<RequestHeader>(request_header, request_type_addr)
                    .unwrap();

                simulate_queue_event(&mut block, Some(true));

                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);
                assert_eq!(vq.used.ring[0].get().len, 0);
            }
        }
    }

    #[test]
    fn test_unsupported_request_type() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);
            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
            set_queue(&mut block, 0, vq.create_queue());
            block.activate(mem.clone(), interrupt).unwrap();
            read_blk_req_descriptors(&vq);

            let request_type_addr = GuestAddress(vq.dtable[0].addr.get());
            let status_addr = GuestAddress(vq.dtable[2].addr.get());

            // Currently only VIRTIO_BLK_T_IN, VIRTIO_BLK_T_OUT,
            // VIRTIO_BLK_T_FLUSH and VIRTIO_BLK_T_GET_ID  are supported.
            // Generate an unsupported request.
            let request_header = RequestHeader::new(42, 0);
            mem.write_obj::<RequestHeader>(request_header, request_type_addr)
                .unwrap();

            simulate_queue_event(&mut block, Some(true));

            assert_eq!(vq.used.idx.get(), 1);
            assert_eq!(vq.used.ring[0].get().id, 0);
            assert_eq!(vq.used.ring[0].get().len, 1);
            assert_eq!(
                mem.read_obj::<u32>(status_addr).unwrap(),
                VIRTIO_BLK_S_UNSUPP
            );
        }
    }

    #[test]
    fn test_end_of_region() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);
            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
            set_queue(&mut block, 0, vq.create_queue());
            block.activate(mem.clone(), interrupt).unwrap();
            read_blk_req_descriptors(&vq);
            vq.dtable[1].set(0xf000, 0x1000, VIRTQ_DESC_F_NEXT | VIRTQ_DESC_F_WRITE, 2);

            let request_type_addr = GuestAddress(vq.dtable[0].addr.get());
            let status_addr = GuestAddress(vq.dtable[2].addr.get());

            vq.used.idx.set(0);

            mem.write_obj::<u32>(VIRTIO_BLK_T_IN, request_type_addr)
                .unwrap();
            vq.dtable[1]
                .flags
                .set(VIRTQ_DESC_F_NEXT | VIRTQ_DESC_F_WRITE);

            check_metric_after_block!(
                &block.metrics.read_count,
                1,
                simulate_queue_and_async_completion_events(&mut block, true)
            );

            assert_eq!(vq.used.idx.get(), 1);
            assert_eq!(vq.used.ring[0].get().id, 0);
            // Added status byte length.
            assert_eq!(vq.used.ring[0].get().len, vq.dtable[1].len.get() + 1);
            assert_eq!(mem.read_obj::<u32>(status_addr).unwrap(), VIRTIO_BLK_S_OK);
        }
    }

    #[test]
    fn test_read_write() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);
            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
            set_queue(&mut block, 0, vq.create_queue());
            block.activate(mem.clone(), interrupt).unwrap();
            read_blk_req_descriptors(&vq);

            let request_type_addr = GuestAddress(vq.dtable[0].addr.get());
            let data_addr = GuestAddress(vq.dtable[1].addr.get());
            let status_addr = GuestAddress(vq.dtable[2].addr.get());

            let empty_data = vec![0; 512];
            let rand_data = vmm_sys_util::rand::rand_alphanumerics(1024)
                .as_bytes()
                .to_vec();

            // Write with invalid data len (not a multiple of 512).
            {
                mem.write_obj::<u32>(VIRTIO_BLK_T_OUT, request_type_addr)
                    .unwrap();
                // Make data read only, 512 bytes in len, and set the actual value to be written.
                vq.dtable[1].flags.set(VIRTQ_DESC_F_NEXT);
                vq.dtable[1].len.set(511);
                mem.write_slice(&rand_data[..511], data_addr).unwrap();

                simulate_queue_and_async_completion_events(&mut block, true);

                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);
                assert_eq!(vq.used.ring[0].get().len, 0);

                // Check that the data wasn't written to the file
                let mut buf = [0u8; 512];
                block
                    .disk()
                    .file_engine
                    .file()
                    .seek(SeekFrom::Start(0))
                    .unwrap();
                block.disk().file_engine.file().read_exact(&mut buf).unwrap();
                assert_eq!(buf, empty_data.as_slice());
            }

            // Write from valid address, with an overflowing length.
            {
                let mut block = default_block(engine);

                // Default mem size is 0x10000
                let mem = default_mem();
                let interrupt = default_interrupt();
                let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
                set_queue(&mut block, 0, vq.create_queue());
                block.activate(mem.clone(), interrupt).unwrap();
                read_blk_req_descriptors(&vq);
                let request_type_addr = GuestAddress(vq.dtable[0].addr.get());

                vq.dtable[1].set(0xff00, 0x1000, VIRTQ_DESC_F_NEXT, 2);
                mem.write_obj::<u32>(VIRTIO_BLK_T_OUT, request_type_addr)
                    .unwrap();

                // Mark the next available descriptor.
                vq.avail.idx.set(1);
                vq.used.idx.set(0);

                check_metric_after_block!(
                    &block.metrics.invalid_reqs_count,
                    1,
                    simulate_queue_and_async_completion_events(&mut block, true)
                );

                let used_idx = vq.used.idx.get();
                assert_eq!(used_idx, 1);

                let status_addr = GuestAddress(vq.dtable[2].addr.get());
                assert_eq!(
                    u32::from(mem.read_obj::<u8>(status_addr).unwrap()),
                    VIRTIO_BLK_S_IOERR
                );
            }

            // Write.
            {
                vq.used.idx.set(0);
                set_queue(&mut block, 0, vq.create_queue());

                mem.write_obj::<u32>(VIRTIO_BLK_T_OUT, request_type_addr)
                    .unwrap();
                // Make data read only, 512 bytes in len, and set the actual value to be written.
                vq.dtable[1].flags.set(VIRTQ_DESC_F_NEXT);
                vq.dtable[1].len.set(512);
                mem.write_slice(&rand_data[..512], data_addr).unwrap();

                check_metric_after_block!(
                    &block.metrics.write_count,
                    1,
                    simulate_queue_and_async_completion_events(&mut block, true)
                );

                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);
                assert_eq!(vq.used.ring[0].get().len, 1);
                assert_eq!(mem.read_obj::<u32>(status_addr).unwrap(), VIRTIO_BLK_S_OK);
            }

            // Read with invalid data len (not a multiple of 512).
            {
                vq.used.idx.set(0);
                set_queue(&mut block, 0, vq.create_queue());

                mem.write_obj::<u32>(VIRTIO_BLK_T_IN, request_type_addr)
                    .unwrap();
                vq.dtable[1]
                    .flags
                    .set(VIRTQ_DESC_F_NEXT | VIRTQ_DESC_F_WRITE);
                vq.dtable[1].len.set(511);
                mem.write_slice(empty_data.as_slice(), data_addr).unwrap();

                simulate_queue_and_async_completion_events(&mut block, true);

                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);
                // The descriptor should have been discarded.
                assert_eq!(vq.used.ring[0].get().len, 0);

                // Check that no data was read.
                let mut buf = [0u8; 512];
                mem.read_slice(&mut buf, data_addr).unwrap();
                assert_eq!(buf, empty_data.as_slice());
            }

            // Read.
            {
                vq.used.idx.set(0);
                set_queue(&mut block, 0, vq.create_queue());

                mem.write_obj::<u32>(VIRTIO_BLK_T_IN, request_type_addr)
                    .unwrap();
                vq.dtable[1]
                    .flags
                    .set(VIRTQ_DESC_F_NEXT | VIRTQ_DESC_F_WRITE);
                vq.dtable[1].len.set(512);
                mem.write_slice(empty_data.as_slice(), data_addr).unwrap();

                check_metric_after_block!(
                    &block.metrics.read_count,
                    1,
                    simulate_queue_and_async_completion_events(&mut block, true)
                );

                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);
                // Added status byte length.
                assert_eq!(vq.used.ring[0].get().len, vq.dtable[1].len.get() + 1);
                assert_eq!(mem.read_obj::<u32>(status_addr).unwrap(), VIRTIO_BLK_S_OK);

                // Check that the data is the same that we wrote before
                let mut buf = [0u8; 512];
                mem.read_slice(&mut buf, data_addr).unwrap();
                assert_eq!(buf, &rand_data[..512]);
            }

            // Read with error.
            {
                vq.used.idx.set(0);
                set_queue(&mut block, 0, vq.create_queue());

                mem.write_obj::<u32>(VIRTIO_BLK_T_IN, request_type_addr)
                    .unwrap();
                vq.dtable[1]
                    .flags
                    .set(VIRTQ_DESC_F_NEXT | VIRTQ_DESC_F_WRITE);
                mem.write_slice(empty_data.as_slice(), data_addr).unwrap();

                let size = block
                    .disk()
                    .file_engine
                    .file()
                    .seek(SeekFrom::End(0))
                    .unwrap();
                block.disk().file_engine.file().set_len(size / 2).unwrap();
                mem.write_obj(10, GuestAddress(request_type_addr.0 + 8))
                    .unwrap();

                simulate_queue_and_async_completion_events(&mut block, true);

                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);
                // The descriptor should have been discarded.
                assert_eq!(vq.used.ring[0].get().len, 0);

                // Check that no data was read.
                let mut buf = [0u8; 512];
                mem.read_slice(&mut buf, data_addr).unwrap();
                assert_eq!(buf, empty_data.as_slice());
            }

            // Partial buffer error on read.
            {
                vq.used.idx.set(0);
                set_queue(&mut block, 0, vq.create_queue());

                mem.write_obj::<u32>(VIRTIO_BLK_T_IN, request_type_addr)
                    .unwrap();
                vq.dtable[1]
                    .flags
                    .set(VIRTQ_DESC_F_NEXT | VIRTQ_DESC_F_WRITE);

                let size = block
                    .disk()
                    .file_engine
                    .file()
                    .seek(SeekFrom::End(0))
                    .unwrap();
                block.disk().file_engine.file().set_len(size / 2).unwrap();
                // Update sector number: stored at `request_type_addr.0 + 8`
                mem.write_obj(5, GuestAddress(request_type_addr.0 + 8))
                    .unwrap();

                // This will attempt to read past end of file.
                simulate_queue_and_async_completion_events(&mut block, true);

                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);

                // No data since can't read past end of file, only status byte length.
                assert_eq!(vq.used.ring[0].get().len, 1);
                assert_eq!(
                    mem.read_obj::<u32>(status_addr).unwrap(),
                    VIRTIO_BLK_S_IOERR
                );

                // Check that no data was read since we can't read past the end of the file.
                let mut buf = [0u8; 512];
                mem.read_slice(&mut buf, data_addr).unwrap();
                assert_eq!(buf, empty_data.as_slice());
            }

            {
                // Note: this test case only works because when we truncated the file above (with
                // set_len), we did not update the sector count stored in the block device
                // itself (is still 8, even though the file length is 1024 now, e.g. has 2 sectors).
                // Normally, requests that reach past the final sector are rejected by
                // Request::parse.
                vq.used.idx.set(0);
                set_queue(&mut block, 0, vq.create_queue());

                mem.write_obj::<u32>(VIRTIO_BLK_T_IN, request_type_addr)
                    .unwrap();
                vq.dtable[1]
                    .flags
                    .set(VIRTQ_DESC_F_NEXT | VIRTQ_DESC_F_WRITE);
                vq.dtable[1].len.set(1024);

                mem.write_obj(1, GuestAddress(request_type_addr.0 + 8))
                    .unwrap();

                block
                    .disk()
                    .file_engine
                    .file()
                    .seek(SeekFrom::Start(512))
                    .unwrap();
                block
                    .disk()
                    .file_engine
                    .file()
                    .write_all(&rand_data[512..])
                    .unwrap();

                simulate_queue_and_async_completion_events(&mut block, true);

                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);

                assert_eq!(
                    mem.read_obj::<u32>(status_addr).unwrap(),
                    VIRTIO_BLK_S_IOERR
                );

                // Check that we correctly read the second file sector.
                let mut buf = [0u8; 512];
                mem.read_slice(&mut buf, data_addr).unwrap();
                assert_eq!(buf, rand_data[512..]);
            }

            // Read at valid address, with an overflowing length.
            {
                let mut block = default_block(engine);

                // Default mem size is 0x10000
                let mem = default_mem();
                let interrupt = default_interrupt();
                let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
                set_queue(&mut block, 0, vq.create_queue());
                block.activate(mem.clone(), interrupt).unwrap();
                read_blk_req_descriptors(&vq);
                vq.dtable[1].set(0xff00, 0x1000, VIRTQ_DESC_F_NEXT | VIRTQ_DESC_F_WRITE, 2);

                let request_type_addr = GuestAddress(vq.dtable[0].addr.get());

                // Mark the next available descriptor.
                vq.avail.idx.set(1);
                vq.used.idx.set(0);

                mem.write_obj::<u32>(VIRTIO_BLK_T_IN, request_type_addr)
                    .unwrap();
                vq.dtable[1]
                    .flags
                    .set(VIRTQ_DESC_F_NEXT | VIRTQ_DESC_F_WRITE);

                check_metric_after_block!(
                    &block.metrics.invalid_reqs_count,
                    1,
                    simulate_queue_and_async_completion_events(&mut block, true)
                );

                let used_idx = vq.used.idx.get();
                assert_eq!(used_idx, 1);

                let status_addr = GuestAddress(vq.dtable[2].addr.get());
                assert_eq!(
                    u32::from(mem.read_obj::<u8>(status_addr).unwrap()),
                    VIRTIO_BLK_S_IOERR
                );
            }
        }
    }

    #[test]
    fn test_flush() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);
            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
            set_queue(&mut block, 0, vq.create_queue());
            block.activate(mem.clone(), interrupt).unwrap();
            read_blk_req_descriptors(&vq);

            let request_type_addr = GuestAddress(vq.dtable[0].addr.get());
            let status_addr = GuestAddress(vq.dtable[2].addr.get());

            // Flush completes successfully without a data descriptor.
            {
                vq.dtable[0].next.set(2);

                mem.write_obj::<u32>(VIRTIO_BLK_T_FLUSH, request_type_addr)
                    .unwrap();

                simulate_queue_and_async_completion_events(&mut block, true);
                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);
                assert_eq!(vq.used.ring[0].get().len, 1);
                assert_eq!(mem.read_obj::<u32>(status_addr).unwrap(), VIRTIO_BLK_S_OK);
            }

            // Flush completes successfully even with a data descriptor.
            {
                vq.used.idx.set(0);
                set_queue(&mut block, 0, vq.create_queue());
                vq.dtable[0].next.set(1);

                mem.write_obj::<u32>(VIRTIO_BLK_T_FLUSH, request_type_addr)
                    .unwrap();

                simulate_queue_and_async_completion_events(&mut block, true);
                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);
                // status byte length.
                assert_eq!(vq.used.ring[0].get().len, 1);
                assert_eq!(mem.read_obj::<u32>(status_addr).unwrap(), VIRTIO_BLK_S_OK);
            }
        }
    }

    #[test]
    fn test_get_device_id() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);
            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
            set_queue(&mut block, 0, vq.create_queue());
            block.activate(mem.clone(), interrupt).unwrap();
            read_blk_req_descriptors(&vq);

            let request_type_addr = GuestAddress(vq.dtable[0].addr.get());
            let data_addr = GuestAddress(vq.dtable[1].addr.get());
            let status_addr = GuestAddress(vq.dtable[2].addr.get());
            let blk_metadata = block.disk().file_engine.file().metadata();

            // Test that the driver receives the correct device id.
            {
                vq.dtable[1].len.set(VIRTIO_BLK_ID_BYTES);

                mem.write_obj::<u32>(VIRTIO_BLK_T_GET_ID, request_type_addr)
                    .unwrap();

                simulate_queue_event(&mut block, Some(true));
                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);
                assert_eq!(vq.used.ring[0].get().len, 21);
                assert_eq!(mem.read_obj::<u32>(status_addr).unwrap(), VIRTIO_BLK_S_OK);

                let blk_meta = blk_metadata.unwrap();
                let expected_device_id = format!(
                    "{}{}{}",
                    blk_meta.st_dev(),
                    blk_meta.st_rdev(),
                    blk_meta.st_ino()
                );

                let mut buf = [0; VIRTIO_BLK_ID_BYTES as usize];
                mem.read_slice(&mut buf, data_addr).unwrap();
                let chars_to_trim: &[char] = &['\u{0}'];
                let received_device_id = String::from_utf8(buf.to_ascii_lowercase())
                    .unwrap()
                    .trim_matches(chars_to_trim)
                    .to_string();
                assert_eq!(received_device_id, expected_device_id);
            }

            // Test that a device ID request will be discarded, if it fails to provide enough buffer
            // space.
            {
                vq.used.idx.set(0);
                set_queue(&mut block, 0, vq.create_queue());
                vq.dtable[1].len.set(VIRTIO_BLK_ID_BYTES - 1);

                mem.write_obj::<u32>(VIRTIO_BLK_T_GET_ID, request_type_addr)
                    .unwrap();

                simulate_queue_event(&mut block, Some(true));
                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);
                assert_eq!(vq.used.ring[0].get().len, 0);
            }
        }
    }

    fn add_flush_requests_batch(block: &mut VirtioBlock, vq: &VirtQueue, count: u16) {
        let mem = vq.memory();
        vq.avail.idx.set(0);
        vq.used.idx.set(0);
        set_queue(block, 0, vq.create_queue());

        let hdr_addr = vq
            .end()
            .checked_align_up(std::mem::align_of::<RequestHeader>() as u64)
            .unwrap();
        // Write request header. All requests will use the same header.
        mem.write_obj(RequestHeader::new(VIRTIO_BLK_T_FLUSH, 0), hdr_addr)
            .unwrap();

        let mut status_addr = hdr_addr
            .checked_add(std::mem::size_of::<RequestHeader>() as u64)
            .unwrap()
            .checked_align_up(4)
            .unwrap();

        for i in 0..count {
            let idx = i * 2;

            let hdr_desc = &vq.dtable[idx as usize];
            hdr_desc.addr.set(hdr_addr.0);
            hdr_desc.flags.set(VIRTQ_DESC_F_NEXT);
            hdr_desc.next.set(idx + 1);

            let status_desc = &vq.dtable[idx as usize + 1];
            status_desc.addr.set(status_addr.0);
            status_desc.flags.set(VIRTQ_DESC_F_WRITE);
            status_desc.len.set(4);
            status_addr = status_addr.checked_add(4).unwrap();

            vq.avail.ring[i as usize].set(idx);
            vq.avail.idx.set(i + 1);
        }
    }

    fn check_flush_requests_batch(count: u16, vq: &VirtQueue) {
        let used_idx = vq.used.idx.get();
        assert_eq!(used_idx, count);

        for i in 0..count {
            let used = vq.used.ring[i as usize].get();
            let status_addr = vq.dtable[used.id as usize + 1].addr.get();
            assert_eq!(used.len, 1);
            assert_eq!(
                u32::from(
                    vq.memory()
                        .read_obj::<u8>(GuestAddress(status_addr))
                        .unwrap(),
                ),
                VIRTIO_BLK_S_OK
            );
        }
    }

    #[test]
    fn test_io_engine_throttling() {
        // FullSQueue BlockError
        {
            let mut block = default_block(FileEngineType::Async);

            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, IO_URING_NUM_ENTRIES * 4);
            block.resources_mut().queues[0] = vq.create_queue();
            block.activate(mem.clone(), interrupt).unwrap();

            // Run scenario that doesn't trigger FullSq BlockError: Add sq_size flush requests.
            add_flush_requests_batch(&mut block, &vq, IO_URING_NUM_ENTRIES);
            simulate_queue_event(&mut block, Some(false));
            assert!(!block.resources().is_io_engine_throttled);
            simulate_async_completion_event(&mut block, true);
            check_flush_requests_batch(IO_URING_NUM_ENTRIES, &vq);

            // Run scenario that triggers FullSqError : Add sq_size + 10 flush requests.
            add_flush_requests_batch(&mut block, &vq, IO_URING_NUM_ENTRIES + 10);
            simulate_queue_event(&mut block, Some(false));
            assert!(block.resources().is_io_engine_throttled);
            // When the async_completion_event is triggered:
            // 1. sq_size requests should be processed processed.
            // 2. is_io_engine_throttled should be set back to false.
            // 3. process_queue() should be called again.
            simulate_async_completion_event(&mut block, true);
            assert!(!block.resources().is_io_engine_throttled);
            check_flush_requests_batch(IO_URING_NUM_ENTRIES, &vq);
            // check that process_queue() was called again resulting in the processing of the
            // remaining 10 ops.
            simulate_async_completion_event(&mut block, true);
            assert!(!block.resources().is_io_engine_throttled);
            check_flush_requests_batch(IO_URING_NUM_ENTRIES + 10, &vq);
        }

        // FullCQueue BlockError
        {
            let mut block = default_block(FileEngineType::Async);

            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, IO_URING_NUM_ENTRIES * 4);
            block.resources_mut().queues[0] = vq.create_queue();
            block.activate(mem.clone(), interrupt).unwrap();

            // Run scenario that triggers FullCqError. Push 2 * IO_URING_NUM_ENTRIES and wait for
            // completion. Then try to push another entry.
            add_flush_requests_batch(&mut block, &vq, IO_URING_NUM_ENTRIES);
            simulate_queue_event(&mut block, Some(false));
            assert!(!block.resources().is_io_engine_throttled);
            thread::sleep(Duration::from_millis(150));
            add_flush_requests_batch(&mut block, &vq, IO_URING_NUM_ENTRIES);
            simulate_queue_event(&mut block, Some(false));
            assert!(!block.resources().is_io_engine_throttled);
            thread::sleep(Duration::from_millis(150));

            add_flush_requests_batch(&mut block, &vq, 1);
            simulate_queue_event(&mut block, Some(false));
            assert!(block.resources().is_io_engine_throttled);
            simulate_async_completion_event(&mut block, true);
            assert!(!block.resources().is_io_engine_throttled);
            check_flush_requests_batch(IO_URING_NUM_ENTRIES * 2, &vq);
        }
    }

    #[test]
    fn test_prepare_save() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);

            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
            block.resources_mut().queues[0] = vq.create_queue();
            block.activate(mem.clone(), interrupt).unwrap();

            // Add a batch of flush requests.
            add_flush_requests_batch(&mut block, &vq, 5);
            simulate_queue_event(&mut block, None);
            block.prepare_save();

            // Check that all the pending flush requests were processed during `prepare_save()`.
            check_flush_requests_batch(5, &vq);
        }
    }

    #[test]
    fn test_bandwidth_rate_limiter() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);
            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
            set_queue(&mut block, 0, vq.create_queue());
            block.activate(mem.clone(), interrupt).unwrap();
            read_blk_req_descriptors(&vq);

            let request_type_addr = GuestAddress(vq.dtable[0].addr.get());
            let data_addr = GuestAddress(vq.dtable[1].addr.get());
            let status_addr = GuestAddress(vq.dtable[2].addr.get());

            // Create bandwidth rate limiter that allows only 5120 bytes/s with bucket size of 8
            // bytes.
            let mut rl = RateLimiter::new(512, 0, 100, 0, 0, 0);
            // Use up the budget.
            assert!(rl.consume(512, TokenType::Bytes));

            set_rate_limiter(&mut block, rl);

            mem.write_obj::<u32>(VIRTIO_BLK_T_OUT, request_type_addr)
                .unwrap();
            // Make data read only, 512 bytes in len, and set the actual value to be written
            vq.dtable[1].flags.set(VIRTQ_DESC_F_NEXT);
            vq.dtable[1].len.set(512);
            mem.write_obj::<u64>(123_456_789, data_addr).unwrap();

            // Following write procedure should fail because of bandwidth rate limiting.
            {
                // Trigger the attempt to write.
                check_metric_after_block!(
                    &block.metrics.rate_limiter_throttled_events,
                    1,
                    simulate_queue_event(&mut block, Some(false))
                );

                // Assert that limiter is blocked.
                assert!(block.rate_limiter().is_blocked());
                // Make sure the data is still queued for processing.
                assert_eq!(vq.used.idx.get(), 0);
            }

            // Wait for 100ms to give the rate-limiter timer a chance to replenish.
            // Wait for an extra 50ms to make sure the timerfd event makes its way from the kernel.
            thread::sleep(Duration::from_millis(150));

            // Following write procedure should succeed because bandwidth should now be available.
            {
                check_metric_after_block!(
                    &block.metrics.rate_limiter_throttled_events,
                    0,
                    if let BlockState::Active(ActiveBlock::Inline(ab)) = &mut block.state { ab.worker.process_rate_limiter_event() }
                );
                // Validate the rate_limiter is no longer blocked.
                assert!(!block.rate_limiter().is_blocked());
                // Complete async IO ops if needed
                simulate_async_completion_event(&mut block, true);

                // Make sure the data queue advanced.
                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);
                assert_eq!(vq.used.ring[0].get().len, 1);
                assert_eq!(mem.read_obj::<u32>(status_addr).unwrap(), VIRTIO_BLK_S_OK);
            }
        }
    }

    #[test]
    fn test_ops_rate_limiter() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);
            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
            set_queue(&mut block, 0, vq.create_queue());
            block.activate(mem.clone(), interrupt).unwrap();
            read_blk_req_descriptors(&vq);

            let request_type_addr = GuestAddress(vq.dtable[0].addr.get());
            let data_addr = GuestAddress(vq.dtable[1].addr.get());
            let status_addr = GuestAddress(vq.dtable[2].addr.get());

            // Create ops rate limiter that allows only 10 ops/s with bucket size of 1 ops.
            let mut rl = RateLimiter::new(0, 0, 0, 1, 0, 100);
            // Use up the budget.
            assert!(rl.consume(1, TokenType::Ops));

            set_rate_limiter(&mut block, rl);

            mem.write_obj::<u32>(VIRTIO_BLK_T_OUT, request_type_addr)
                .unwrap();
            // Make data read only, 512 bytes in len, and set the actual value to be written.
            vq.dtable[1].flags.set(VIRTQ_DESC_F_NEXT);
            vq.dtable[1].len.set(512);
            mem.write_obj::<u64>(123_456_789, data_addr).unwrap();

            // Following write procedure should fail because of ops rate limiting.
            {
                // Trigger the attempt to write.
                check_metric_after_block!(
                    &block.metrics.rate_limiter_throttled_events,
                    1,
                    simulate_queue_event(&mut block, Some(false))
                );

                // Assert that limiter is blocked.
                assert!(block.rate_limiter().is_blocked());
                // Make sure the data is still queued for processing.
                assert_eq!(vq.used.idx.get(), 0);
            }

            // Do a second write that still fails but this time on the fast path.
            {
                // Trigger the attempt to write.
                check_metric_after_block!(
                    &block.metrics.rate_limiter_throttled_events,
                    1,
                    simulate_queue_event(&mut block, Some(false))
                );

                // Assert that limiter is blocked.
                assert!(block.rate_limiter().is_blocked());
                // Make sure the data is still queued for processing.
                assert_eq!(vq.used.idx.get(), 0);
            }

            // Wait for 100ms to give the rate-limiter timer a chance to replenish.
            // Wait for an extra 50ms to make sure the timerfd event makes its way from the kernel.
            thread::sleep(Duration::from_millis(150));

            // Following write procedure should succeed because ops budget should now be available.
            {
                check_metric_after_block!(
                    &block.metrics.rate_limiter_throttled_events,
                    0,
                    if let BlockState::Active(ActiveBlock::Inline(ab)) = &mut block.state { ab.worker.process_rate_limiter_event() }
                );
                // Validate the rate_limiter is no longer blocked.
                assert!(!block.rate_limiter().is_blocked());
                // Complete async IO ops if needed
                simulate_async_completion_event(&mut block, true);

                // Make sure the data queue advanced.
                assert_eq!(vq.used.idx.get(), 1);
                assert_eq!(vq.used.ring[0].get().id, 0);
                assert_eq!(vq.used.ring[0].get().len, 1);
                assert_eq!(mem.read_obj::<u32>(status_addr).unwrap(), VIRTIO_BLK_S_OK);
            }
        }
    }

    #[test]
    fn test_update_disk_image() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);
            let mem = default_mem();
            let interrupt = default_interrupt();
            let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
            set_queue(&mut block, 0, vq.create_queue());
            block.activate(mem, interrupt).unwrap();
            let f = TempFile::new().unwrap();
            let path = f.as_path();
            let mdata = metadata(path).unwrap();
            let mut id = vec![0; VIRTIO_BLK_ID_BYTES as usize];
            let str_id = format!("{}{}{}", mdata.st_dev(), mdata.st_rdev(), mdata.st_ino());
            let part_id = str_id.as_bytes();
            id[..cmp::min(part_id.len(), VIRTIO_BLK_ID_BYTES as usize)].clone_from_slice(
                &part_id[..cmp::min(part_id.len(), VIRTIO_BLK_ID_BYTES as usize)],
            );

            block
                .update_disk_image(String::from(path.to_str().unwrap()))
                .unwrap();

            assert_eq!(
                block.disk().file_engine.file().metadata().unwrap().st_ino(),
                mdata.st_ino()
            );
            assert_eq!(block.disk().image_id, id.as_slice());
        }
    }

    #[test]
    fn test_update_disk_image_threaded() {
        let mut block = default_block(FileEngineType::Sync);
        block.threaded = true;
        block.spawn_worker().unwrap();

        let mem = default_mem();
        let interrupt = default_interrupt();
        let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
        set_queue(&mut block, 0, vq.create_queue());
        block.activate(mem, interrupt).unwrap();

        let f = TempFile::new().unwrap();
        f.as_file().set_len(u64::from(SECTOR_SIZE) * 3).unwrap();
        let path = f.as_path();
        let mdata = metadata(path).unwrap();

        block
            .update_disk_image(String::from(path.to_str().unwrap()))
            .unwrap();

        assert_eq!(u64::from_le(block.config_space.capacity), 3);
        assert!(
            block
                .interrupt_trigger()
                .has_pending_interrupt(VirtioInterruptType::Config)
        );

        block.deactivate();
        assert_eq!(
            block.disk().file_engine.file().metadata().unwrap().st_ino(),
            mdata.st_ino()
        );
    }

    #[test]
    fn test_threaded_lifecycle() {
        for engine in [FileEngineType::Sync, FileEngineType::Async] {
            let mut block = default_block(engine);
            block.threaded = true;
            block.spawn_worker().unwrap();

            let mem = default_mem();
            let vq = VirtQueue::new(GuestAddress(0), &mem, 16);
            set_queue(&mut block, 0, vq.create_queue());
            block.activate(mem.clone(), default_interrupt()).unwrap();

            block.prepare_save();
            let state = block.save();
            assert!(state.virtio_state.activated);
            block.mark_queue_memory_dirty(&mem).unwrap();

            block.update_rate_limiter(BucketUpdate::Disabled, BucketUpdate::Disabled);
            block.kick();
            assert!(block.reset());
            assert!(!block.is_activated());
            assert_eq!(block.acked_features(), 0);
            assert!(block.rate_limiter().bandwidth().is_none());
            assert!(block.rate_limiter().ops().is_none());

            set_queue(&mut block, 0, vq.create_queue());
            block.activate(mem.clone(), default_interrupt()).unwrap();
            assert!(block.is_activated());
        }
    }
}
