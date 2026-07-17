// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Defines the structures needed for saving/restoring block devices.

use std::sync::Arc;
use device::ConfigSpace;
use serde::{Deserialize, Serialize};
use vmm_sys_util::eventfd::EventFd;

use super::device::{ActiveBlock, BlockResources, BlockState, DiskProperties};
use super::*;
use crate::devices::virtio::block::persist::BlockConstructorArgs;
use crate::devices::virtio::block::virtio::device::FileEngineType;
use crate::devices::virtio::block::virtio::metrics::BlockMetricsPerDevice;
use crate::devices::virtio::block::virtio::test_utils::rate_limiter;
use crate::devices::virtio::device::VirtioDeviceType;
use crate::devices::virtio::generated::virtio_blk::{VIRTIO_BLK_F_MQ, VIRTIO_BLK_F_RO};
use crate::devices::virtio::persist::{PersistError, VirtioDeviceState};
use crate::rate_limiter::RateLimiter;
use crate::rate_limiter::persist::RateLimiterState;
use crate::snapshot::Persist;
use crate::vmm_config::machine_config::MAX_SUPPORTED_VCPUS;
use crate::vmm_config::RateLimiterConfig;

/// Holds info about block's file engine type. Gets saved in snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileEngineTypeState {
    /// Sync File Engine.
    // If the snap version does not contain the `FileEngineType`, it must have been snapshotted
    // on a VM using the Sync backend.
    #[default]
    Sync,
    /// Async File Engine.
    Async,
}

impl From<FileEngineType> for FileEngineTypeState {
    fn from(file_engine_type: FileEngineType) -> Self {
        match file_engine_type {
            FileEngineType::Sync => FileEngineTypeState::Sync,
            FileEngineType::Async => FileEngineTypeState::Async,
        }
    }
}

impl From<FileEngineTypeState> for FileEngineType {
    fn from(file_engine_type_state: FileEngineTypeState) -> Self {
        match file_engine_type_state {
            FileEngineTypeState::Sync => FileEngineType::Sync,
            FileEngineTypeState::Async => FileEngineType::Async,
        }
    }
}

/// Holds info about the block device. Gets saved in snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtioBlockState {
    id: String,
    partuuid: Option<String>,
    cache_type: CacheType,
    root_device: bool,
    disk_path: String,
    pub virtio_state: VirtioDeviceState,
    rate_limiter_states: Vec<RateLimiterState>,
    file_engine_type: FileEngineTypeState,
    #[serde(default)]
    threaded: bool,
}

impl Persist<'_> for VirtioBlock {
    type State = VirtioBlockState;
    type ConstructorArgs = BlockConstructorArgs;
    type Error = VirtioBlockError;

    fn save(&self) -> Self::State {
        // Save device state.
        let (disk_path, rate_limiter_states, file_engine_type, virtio_state) =
            if let BlockState::Active(ActiveBlock::Threaded(ta)) = &self.state {
                let states: Vec<_> = ta
                    .worker_handles
                    .iter()
                    .map(|handle| handle.get_saved_state())
                    .collect();
                let first = states
                    .first()
                    .expect("threaded block must have at least one worker");
                (
                    first.disk_path.clone(),
                    states
                        .iter()
                        .map(|s| s.rate_limiter_state.clone())
                        .collect(),
                    first.file_engine_type,
                    VirtioDeviceState {
                        device_type: VirtioDeviceType::Block,
                        avail_features: self.avail_features,
                        acked_features: self.acked_features,
                        queues: states
                            .iter()
                            .map(|s| s.queue_state.clone())
                            .collect(),
                        activated: true,
                    }
                )
            } else {
                (
                 self.disk().file_path.clone(),
                 self.resources()
                     .iter()
                     .map(|r| r.rate_limiter.save())
                     .collect(),
                 FileEngineTypeState::from(self.file_engine_type()),
                 VirtioDeviceState::from_device(
                     self,
                     self.resources().iter().map(|r| &r.queue),
                 ),
                )
            };

        VirtioBlockState {
            id: self.id.clone(),
            partuuid: self.partuuid.clone(),
            cache_type: self.cache_type,
            root_device: self.root_device,
            disk_path,
            virtio_state,
            rate_limiter_states,
            file_engine_type,
            threaded: self.threaded,
        }
    }

    fn restore(
        constructor_args: Self::ConstructorArgs,
        state: &Self::State,
    ) -> Result<Self, Self::Error> {
        let is_read_only = state.virtio_state.avail_features & (1u64 << VIRTIO_BLK_F_RO) != 0;
        let num_queues = state.virtio_state.queues.len();
        let max_queues = usize::from(MAX_SUPPORTED_VCPUS);
        if !(1..=max_queues).contains(&num_queues) {
            return Err(VirtioBlockError::InvalidQueueCount(
                u16::try_from(num_queues).unwrap_or(u16::MAX),
                u16::from(MAX_SUPPORTED_VCPUS),
            ));
        }
        if num_queues > 1 && !state.threaded {
            return Err(VirtioBlockError::MultiqueueRequiresThreaded);
        }
        let mq_offered =
            state.virtio_state.avail_features & (1u64 << VIRTIO_BLK_F_MQ) != 0;
        if mq_offered != (num_queues > 1) {
            return Err(VirtioBlockError::Persist(PersistError::InvalidInput));
        }
        if state.rate_limiter_states.len() != num_queues {
            return Err(VirtioBlockError::Persist(PersistError::InvalidInput));
        }

        let queues = state
            .virtio_state
            .build_queues_checked(
                &constructor_args.mem,
                VirtioDeviceType::Block,
                num_queues,
                FIRECRACKER_MAX_QUEUE_SIZE,
            )
            .map_err(VirtioBlockError::Persist)?;

        let mut resources = Vec::with_capacity(num_queues);
        for (queue_index, (queue, rate_limiter_state)) in queues
            .into_iter()
            .zip(&state.rate_limiter_states)
            .enumerate()
        {
            resources.push(BlockResources {
                queue,
                queue_evt: EventFd::new(libc::EFD_NONBLOCK)
                    .map_err(VirtioBlockError::EventFd)?,
                queue_index: u16::try_from(queue_index).unwrap(),
                disk: DiskProperties::new(
                    state.disk_path.clone(),
                    is_read_only,
                    state.file_engine_type.into(),
                )?,
                rate_limiter: RateLimiter::restore((), rate_limiter_state)
                    .map_err(VirtioBlockError::RateLimiter)?,
                is_io_engine_throttled: false,
            });
        }

        let avail_features = state.virtio_state.avail_features;
        let acked_features = state.virtio_state.acked_features;

        let config_space = ConfigSpace::new(
            resources[0].disk.nsectors,
            u16::try_from(num_queues).unwrap(),
        );
        let rate_limiter_config = RateLimiterConfig::from(&resources[0].rate_limiter);

        Ok(VirtioBlock {
            avail_features,
            acked_features,
            config_space,
            activate_evt: EventFd::new(libc::EFD_NONBLOCK).map_err(VirtioBlockError::EventFd)?,

            id: state.id.clone(),
            partuuid: state.partuuid.clone(),
            cache_type: state.cache_type,
            root_device: state.root_device,
            read_only: is_read_only,

            threaded: state.threaded,
            num_queues: u16::try_from(num_queues).unwrap(),
            path_on_host: state.disk_path.clone(),
            rate_limiter_config,
            file_engine_type: state.file_engine_type.into(),
            state: BlockState::Configuring(resources, Vec::new()),
            metrics: BlockMetricsPerDevice::alloc(state.id.clone()),
            seccomp_filter: Arc::new(vec![]),
        })
    }
}

#[cfg(test)]
mod tests {
    use vm_memory::GuestAddress;
    use vmm_sys_util::tempfile::TempFile;

    use super::*;
    use crate::devices::virtio::block::virtio::device::VirtioBlockConfig;
    use crate::devices::virtio::block::virtio::test_utils::set_queue;
    use crate::devices::virtio::device::VirtioDevice;
    use crate::devices::virtio::test_utils::{VirtQueue, default_interrupt, default_mem};
    use crate::rate_limiter::TokenType;
    use crate::vmm_config::TokenBucketConfig;

    #[test]
    fn test_cache_semantic_ser() {
        // We create the backing file here so that it exists for the whole lifetime of the test.
        let f = TempFile::new().unwrap();
        f.as_file().set_len(0x1000).unwrap();

        let config = VirtioBlockConfig {
            drive_id: "test".to_string(),
            path_on_host: f.as_path().to_str().unwrap().to_string(),
            is_root_device: false,
            partuuid: None,
            is_read_only: false,
            threaded: false,
            num_queues: 1,
            cache_type: CacheType::Writeback,
            rate_limiter: None,
            file_engine_type: FileEngineType::default(),
        };

        let block = VirtioBlock::new(config).unwrap();

        // Save the block device.
        let block_state = block.save();
        let _serialized_data = bitcode::serialize(&block_state).unwrap();
    }

    #[test]
    fn test_file_engine_type() {
        // Test conversions between FileEngineType and FileEngineTypeState.
        assert_eq!(
            FileEngineTypeState::Async,
            FileEngineTypeState::from(FileEngineType::Async)
        );
        assert_eq!(
            FileEngineTypeState::Sync,
            FileEngineTypeState::from(FileEngineType::Sync)
        );
        assert_eq!(FileEngineType::Async, FileEngineTypeState::Async.into());
        assert_eq!(FileEngineType::Sync, FileEngineTypeState::Sync.into());
        // Test default impl.
        assert_eq!(FileEngineTypeState::default(), FileEngineTypeState::Sync);
    }

    #[test]
    fn test_persistence() {
        // We create the backing file here so that it exists for the whole lifetime of the test.
        let f = TempFile::new().unwrap();
        f.as_file().set_len(0x1000).unwrap();

        let config = VirtioBlockConfig {
            drive_id: "test".to_string(),
            path_on_host: f.as_path().to_str().unwrap().to_string(),
            is_root_device: false,
            partuuid: None,
            is_read_only: false,
            threaded: true,
            num_queues: 2,
            cache_type: CacheType::Unsafe,
            rate_limiter: None,
            file_engine_type: FileEngineType::default(),
        };

        let block = VirtioBlock::new(config).unwrap();
        let guest_mem = default_mem();

        // Save the block device.
        let block_state = block.save();

        let mut state_with_missing_rate_limiter = block_state.clone();
        state_with_missing_rate_limiter.rate_limiter_states.pop();
        assert!(matches!(
            VirtioBlock::restore(
                BlockConstructorArgs {
                    mem: guest_mem.clone(),
                },
                &state_with_missing_rate_limiter
            ),
            Err(VirtioBlockError::Persist(PersistError::InvalidInput))
        ));

        let mut state_without_mq = block_state.clone();
        state_without_mq.virtio_state.avail_features &= !(1u64 << VIRTIO_BLK_F_MQ);
        assert!(matches!(
            VirtioBlock::restore(
                BlockConstructorArgs {
                    mem: guest_mem.clone(),
                },
                &state_without_mq
            ),
            Err(VirtioBlockError::Persist(PersistError::InvalidInput))
        ));

        let mut single_queue_state_with_mq = block_state.clone();
        single_queue_state_with_mq.virtio_state.queues.truncate(1);
        single_queue_state_with_mq.rate_limiter_states.truncate(1);
        assert!(matches!(
            VirtioBlock::restore(
                BlockConstructorArgs {
                    mem: guest_mem.clone(),
                },
                &single_queue_state_with_mq
            ),
            Err(VirtioBlockError::Persist(PersistError::InvalidInput))
        ));
        single_queue_state_with_mq.virtio_state.avail_features &=
            !(1u64 << VIRTIO_BLK_F_MQ);
        VirtioBlock::restore(
            BlockConstructorArgs {
                mem: guest_mem.clone(),
            },
            &single_queue_state_with_mq,
        )
        .unwrap();

        let serialized_data = bitcode::serialize(&block_state).unwrap();

        // Restore the block device.
        let restored_state = bitcode::deserialize(&serialized_data).unwrap();
        let restored_block = VirtioBlock::restore(
            BlockConstructorArgs { mem: guest_mem },
            &restored_state,
        )
        .unwrap();

        // Test that virtio specific fields are the same.
        assert_eq!(restored_block.device_type(), VirtioDeviceType::Block);
        assert_eq!(restored_block.avail_features(), block.avail_features());
        assert_eq!(restored_block.acked_features(), block.acked_features());
        assert_eq!(
            restored_block
                .resources()
                .iter()
                .map(|resource| &resource.queue)
                .collect::<Vec<_>>(),
            block
                .resources()
                .iter()
                .map(|resource| &resource.queue)
                .collect::<Vec<_>>()
        );
        assert_eq!(restored_block.num_queues, block.num_queues);
        assert_eq!(
            restored_block.config_space.num_queues,
            block.config_space.num_queues
        );
        assert!(!block.is_activated());
        assert!(!restored_block.is_activated());

        // Test that block specific fields are the same.
        assert_eq!(restored_block.disk().file_path, block.disk().file_path);
    }

    #[test]
    fn test_multiqueue_rate_limiter_persistence() {
        let f = TempFile::new().unwrap();
        f.as_file().set_len(0x1000).unwrap();

        let config = VirtioBlockConfig {
            drive_id: "test".to_string(),
            path_on_host: f.as_path().to_str().unwrap().to_string(),
            is_root_device: false,
            partuuid: None,
            is_read_only: false,
            threaded: true,
            num_queues: 2,
            cache_type: CacheType::Unsafe,
            rate_limiter: Some(RateLimiterConfig {
                bandwidth: Some(TokenBucketConfig {
                    size: 100,
                    one_time_burst: Some(0),
                    refill_time: 1_000,
                }),
                ops: None,
            }),
            file_engine_type: FileEngineType::Sync,
        };
        let mut block = VirtioBlock::new(config).unwrap();
        let resources = block.resources_mut();
        assert!(resources[0].rate_limiter.consume(10, TokenType::Bytes));
        assert!(resources[1].rate_limiter.consume(20, TokenType::Bytes));

        block.spawn_worker().unwrap();
        let guest_mem = default_mem();
        let vq0 = VirtQueue::new(GuestAddress(0), &guest_mem, 16);
        let vq1 = VirtQueue::new(GuestAddress(0x1000), &guest_mem, 16);
        set_queue(&mut block, 0, vq0.create_queue());
        set_queue(&mut block, 1, vq1.create_queue());
        block
            .activate(guest_mem.clone(), default_interrupt())
            .unwrap();
        block.prepare_save();

        let state = block.save();
        assert_eq!(state.rate_limiter_states.len(), 2);
        let restored = VirtioBlock::restore(BlockConstructorArgs { mem: guest_mem }, &state)
            .unwrap();

        assert_eq!(
            restored.resources()[0]
                .rate_limiter
                .bandwidth()
                .unwrap()
                .budget(),
            90
        );
        assert_eq!(
            restored.resources()[1]
                .rate_limiter
                .bandwidth()
                .unwrap()
                .budget(),
            80
        );
    }
}
