// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Typed retention lease registry.

use crate::cache::CachePadded;
use crate::error::{RegistryError, Result};
use crate::lifecycle::RegistryLifecycle;
use crate::sync::Mutex;
use crate::types::{CommitTs, LayoutEpoch, ReadTs, SnapshotId, MAX_TRANSACTION_ID};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const LEASE_KIND_COUNT: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RetentionLeaseKind {
    ReadSnapshot = 1,
    LayoutEpoch = 2,
    Backfill = 3,
    DerivedLag = 4,
    Checkpoint = 5,
    WriteConflict = 6,
}

impl RetentionLeaseKind {
    #[inline]
    fn from_raw(raw: u8) -> Option<Self> {
        match raw {
            1 => Some(Self::ReadSnapshot),
            2 => Some(Self::LayoutEpoch),
            3 => Some(Self::Backfill),
            4 => Some(Self::DerivedLag),
            5 => Some(Self::Checkpoint),
            6 => Some(Self::WriteConflict),
            _ => None,
        }
    }

    #[inline]
    pub const fn index(self) -> usize {
        (self as usize) - 1
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RetentionRegistryOptions {
    pub shard_count: usize,
    pub slots_per_shard: usize,
}

impl Default for RetentionRegistryOptions {
    fn default() -> Self {
        Self {
            shard_count: 64,
            slots_per_shard: 256,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionWatermarks {
    pub oldest_read_ts: ReadTs,
    pub oldest_backfill_ts: CommitTs,
    pub oldest_derived_delta_ts: CommitTs,
    pub oldest_conflict_horizon: CommitTs,
    pub oldest_checkpoint_ts: CommitTs,
    pub oldest_layout_epoch: Option<LayoutEpoch>,
    pub lease_counts: [u64; LEASE_KIND_COUNT],
    pub epoch: u64,
}

impl RetentionWatermarks {
    #[inline]
    fn none(epoch: u64) -> Self {
        Self {
            oldest_read_ts: ReadTs::new(MAX_TRANSACTION_ID),
            oldest_backfill_ts: CommitTs::new(MAX_TRANSACTION_ID),
            oldest_derived_delta_ts: CommitTs::new(MAX_TRANSACTION_ID),
            oldest_conflict_horizon: CommitTs::new(MAX_TRANSACTION_ID),
            oldest_checkpoint_ts: CommitTs::new(MAX_TRANSACTION_ID),
            oldest_layout_epoch: None,
            lease_counts: [0; LEASE_KIND_COUNT],
            epoch,
        }
    }

    #[inline]
    pub fn lease_count(&self, kind: RetentionLeaseKind) -> u64 {
        self.lease_counts[kind.index()]
    }
}

impl Default for RetentionWatermarks {
    fn default() -> Self {
        Self::none(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionLeaseInfo {
    pub lease_id: SnapshotId,
    pub kind: RetentionLeaseKind,
    pub commit_ts_floor: Option<CommitTs>,
    pub commit_ts_ceiling: Option<CommitTs>,
    pub layout_epoch_floor: Option<LayoutEpoch>,
    pub last_seen_epoch: u64,
}

pub struct RetentionRegistry {
    inner: Arc<RetentionRegistryInner>,
}

struct RetentionRegistryInner {
    shards: Box<[RetentionShard]>,
    next_shard: AtomicUsize,
    next_lease_id: AtomicU64,
    watermark_epoch: AtomicU64,
    oldest_read_ts: AtomicU64,
    oldest_backfill_ts: AtomicU64,
    oldest_derived_delta_ts: AtomicU64,
    oldest_conflict_horizon: AtomicU64,
    oldest_checkpoint_ts: AtomicU64,
    oldest_layout_epoch: AtomicU64,
    lease_counts: [AtomicU64; LEASE_KIND_COUNT],
    lifecycle: RegistryLifecycle,
}

struct RetentionShard {
    slots: Box<[CachePadded<RetentionSlot>]>,
    free_slots: Mutex<Vec<usize>>,
    summary: CachePadded<RetentionShardSummary>,
}

struct RetentionShardSummary {
    watermarks: RetentionWatermarkAtomics,
    lease_counts: [AtomicU64; LEASE_KIND_COUNT],
    epoch: AtomicU64,
}

struct RetentionWatermarkAtomics {
    oldest_read_ts: AtomicU64,
    oldest_backfill_ts: AtomicU64,
    oldest_derived_delta_ts: AtomicU64,
    oldest_conflict_horizon: AtomicU64,
    oldest_checkpoint_ts: AtomicU64,
    oldest_layout_epoch: AtomicU64,
}

struct RetentionSlot {
    generation: AtomicU64,
    lease_id: AtomicU64,
    kind: AtomicU8,
    commit_ts_floor: AtomicU64,
    commit_ts_ceiling: AtomicU64,
    commit_ts_ceiling_set: AtomicBool,
    layout_epoch_floor: AtomicU64,
    layout_epoch_floor_set: AtomicBool,
    active: AtomicBool,
    last_seen_epoch: AtomicU64,
}

#[derive(Clone)]
struct RetentionSlotRef {
    registry: Arc<RetentionRegistryInner>,
    shard_index: usize,
    slot_index: usize,
    generation: u64,
}

struct RetentionLeaseHandle {
    slot: Option<RetentionSlotRef>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadSnapshotLeaseOwner {
    pub owner_session_id: Option<u64>,
    pub portal_id: Option<Arc<str>>,
}

impl ReadSnapshotLeaseOwner {
    #[inline]
    pub fn for_portal(session_id: u64, portal_id: impl AsRef<str>) -> Self {
        Self {
            owner_session_id: Some(session_id),
            portal_id: Some(Arc::<str>::from(portal_id.as_ref())),
        }
    }

    #[inline]
    pub const fn unowned() -> Self {
        Self {
            owner_session_id: None,
            portal_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSnapshotLeaseTransferError {
    pub expected_owner_session_id: Option<u64>,
    pub actual_owner_session_id: Option<u64>,
}

impl fmt::Display for ReadSnapshotLeaseTransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "read snapshot lease owner mismatch: expected {:?}, actual {:?}",
            self.expected_owner_session_id, self.actual_owner_session_id,
        )
    }
}

impl std::error::Error for ReadSnapshotLeaseTransferError {}

pub struct ReadSnapshotLease {
    handle: RetentionLeaseHandle,
    owner: Mutex<ReadSnapshotLeaseOwner>,
    opened_at: Instant,
}

impl ReadSnapshotLease {
    fn new(handle: RetentionLeaseHandle) -> Self {
        Self {
            handle,
            owner: Mutex::new(ReadSnapshotLeaseOwner::unowned()),
            opened_at: Instant::now(),
        }
    }

    pub fn release(mut self) -> Result<()> {
        self.handle.release()
    }

    pub fn info(&self) -> Result<RetentionLeaseInfo> {
        self.handle.info()
    }

    pub fn lease_id(&self) -> Result<SnapshotId> {
        Ok(self.info()?.lease_id)
    }

    #[inline]
    pub fn opened_at(&self) -> Instant {
        self.opened_at
    }

    #[inline]
    pub fn pinned_duration(&self) -> Duration {
        self.opened_at.elapsed()
    }

    pub fn owner(&self) -> ReadSnapshotLeaseOwner {
        self.owner.lock().clone()
    }

    pub fn bind_owner(&self, owner: ReadSnapshotLeaseOwner) {
        *self.owner.lock() = owner;
    }

    pub fn clear_owner(&self) {
        self.bind_owner(ReadSnapshotLeaseOwner::unowned());
    }

    pub fn transfer_owner(
        &self,
        expected_owner_session_id: Option<u64>,
        new_owner: ReadSnapshotLeaseOwner,
    ) -> std::result::Result<(), ReadSnapshotLeaseTransferError> {
        let mut owner = self.owner.lock();
        let actual_owner_session_id = owner.owner_session_id;
        if actual_owner_session_id != expected_owner_session_id {
            return Err(ReadSnapshotLeaseTransferError {
                expected_owner_session_id,
                actual_owner_session_id,
            });
        }
        *owner = new_owner;
        Ok(())
    }
}

impl fmt::Debug for ReadSnapshotLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadSnapshotLease")
            .field("owner", &self.owner())
            .finish_non_exhaustive()
    }
}

impl Drop for ReadSnapshotLease {
    fn drop(&mut self) {
        let _ = self.handle.release_inner();
    }
}

macro_rules! lease_wrapper {
    ($name:ident) => {
        pub struct $name {
            handle: RetentionLeaseHandle,
        }

        impl $name {
            pub fn release(mut self) -> Result<()> {
                self.handle.release()
            }

            pub fn info(&self) -> Result<RetentionLeaseInfo> {
                self.handle.info()
            }

            pub fn lease_id(&self) -> Result<SnapshotId> {
                Ok(self.info()?.lease_id)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($name)).finish_non_exhaustive()
            }
        }

        impl Drop for $name {
            fn drop(&mut self) {
                let _ = self.handle.release_inner();
            }
        }
    };
}

lease_wrapper!(LayoutEpochLease);
lease_wrapper!(BackfillLease);
lease_wrapper!(DerivedLagLease);
lease_wrapper!(CheckpointLease);
lease_wrapper!(WriteConflictLease);

pub struct RetentionAggregator {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl RetentionRegistry {
    pub fn new(options: RetentionRegistryOptions) -> Self {
        assert!(options.shard_count > 0, "retention registry needs shards");
        assert!(
            options.slots_per_shard > 0,
            "retention registry needs slots per shard"
        );

        let shards = (0..options.shard_count)
            .map(|_| RetentionShard::new(options.slots_per_shard))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            inner: Arc::new(RetentionRegistryInner {
                shards,
                next_shard: AtomicUsize::new(0),
                next_lease_id: AtomicU64::new(1),
                watermark_epoch: AtomicU64::new(0),
                oldest_read_ts: AtomicU64::new(MAX_TRANSACTION_ID),
                oldest_backfill_ts: AtomicU64::new(MAX_TRANSACTION_ID),
                oldest_derived_delta_ts: AtomicU64::new(MAX_TRANSACTION_ID),
                oldest_conflict_horizon: AtomicU64::new(MAX_TRANSACTION_ID),
                oldest_checkpoint_ts: AtomicU64::new(MAX_TRANSACTION_ID),
                oldest_layout_epoch: AtomicU64::new(MAX_TRANSACTION_ID),
                lease_counts: std::array::from_fn(|_| AtomicU64::new(0)),
                lifecycle: RegistryLifecycle::new(),
            }),
        }
    }

    #[inline]
    pub fn with_capacity(shard_count: usize, slots_per_shard: usize) -> Self {
        Self::new(RetentionRegistryOptions {
            shard_count,
            slots_per_shard,
        })
    }

    #[inline]
    pub fn shard_count(&self) -> usize {
        self.inner.shards.len()
    }

    #[inline]
    pub fn slots_per_shard(&self) -> usize {
        self.inner.shards[0].slots.len()
    }

    pub fn lease_read_snapshot(&self, read_ts: ReadTs) -> Result<ReadSnapshotLease> {
        self.register_lease(
            RetentionLeaseKind::ReadSnapshot,
            read_ts.into_raw(),
            None,
            None,
        )
        .map(ReadSnapshotLease::new)
    }

    pub fn lease_layout_epoch(&self, layout_epoch: LayoutEpoch) -> Result<LayoutEpochLease> {
        self.register_lease(
            RetentionLeaseKind::LayoutEpoch,
            MAX_TRANSACTION_ID,
            None,
            Some(layout_epoch.into_raw()),
        )
        .map(|handle| LayoutEpochLease { handle })
    }

    pub fn lease_backfill(&self, backfill_read_ts: CommitTs) -> Result<BackfillLease> {
        self.lease_backfill_range(backfill_read_ts, backfill_read_ts)
    }

    pub fn lease_backfill_range(
        &self,
        backfill_read_ts: CommitTs,
        current_published_ts: CommitTs,
    ) -> Result<BackfillLease> {
        let current_published_ts = CommitTs::new(
            current_published_ts
                .into_raw()
                .max(backfill_read_ts.into_raw()),
        );
        self.register_lease(
            RetentionLeaseKind::Backfill,
            backfill_read_ts.into_raw(),
            Some(current_published_ts.into_raw()),
            None,
        )
        .map(|handle| BackfillLease { handle })
    }

    pub fn lease_derived_lag(&self, indexed_through_ts: CommitTs) -> Result<DerivedLagLease> {
        self.lease_derived_lag_range(indexed_through_ts, indexed_through_ts)
    }

    pub fn lease_derived_lag_range(
        &self,
        indexed_through_ts: CommitTs,
        target_ts: CommitTs,
    ) -> Result<DerivedLagLease> {
        let target_ts = CommitTs::new(target_ts.into_raw().max(indexed_through_ts.into_raw()));
        self.register_lease(
            RetentionLeaseKind::DerivedLag,
            indexed_through_ts.into_raw(),
            Some(target_ts.into_raw()),
            None,
        )
        .map(|handle| DerivedLagLease { handle })
    }

    pub fn lease_checkpoint(&self, required_replay_ts: CommitTs) -> Result<CheckpointLease> {
        self.register_lease(
            RetentionLeaseKind::Checkpoint,
            required_replay_ts.into_raw(),
            None,
            None,
        )
        .map(|handle| CheckpointLease { handle })
    }

    pub fn lease_write_conflict(&self, conflict_horizon: CommitTs) -> Result<WriteConflictLease> {
        self.register_lease(
            RetentionLeaseKind::WriteConflict,
            conflict_horizon.into_raw(),
            None,
            None,
        )
        .map(|handle| WriteConflictLease { handle })
    }

    pub fn watermarks(&self) -> RetentionWatermarks {
        self.inner
            .lifecycle
            .read_consistent(|| self.inner.watermarks())
    }

    pub fn refresh_watermarks(&self) -> RetentionWatermarks {
        let _snapshot = self.inner.lifecycle.snapshot();
        self.inner.scan_and_confirm()
    }

    pub fn confirmed_watermarks(&self) -> RetentionWatermarks {
        self.with_confirmed_watermarks(|watermarks| watermarks)
    }

    /// Run an action against a confirmed watermark while preventing lease
    /// changes from invalidating the reclamation decision before it completes.
    /// The action must not re-enter this registry.
    pub fn with_confirmed_watermarks<R>(&self, action: impl FnOnce(RetentionWatermarks) -> R) -> R {
        let _snapshot = self.inner.lifecycle.snapshot();
        action(self.inner.confirmed_scan_and_publish())
    }

    pub fn spawn_background_aggregator(&self, period: Duration) -> RetentionAggregator {
        let registry = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                registry.refresh_watermarks();
                thread::sleep(period);
            }
        });
        RetentionAggregator {
            stop,
            thread: Some(thread),
        }
    }

    fn register_lease(
        &self,
        kind: RetentionLeaseKind,
        commit_ts_floor: u64,
        commit_ts_ceiling: Option<u64>,
        layout_epoch_floor: Option<u64>,
    ) -> Result<RetentionLeaseHandle> {
        let mut mutation = self.inner.lifecycle.begin_mutation();
        let shard_count = self.inner.shards.len();
        let start = self.inner.next_shard.fetch_add(1, Ordering::Relaxed) % shard_count;
        for offset in 0..shard_count {
            let shard = (start + offset) % shard_count;
            if let Some(slot) = self.allocate_on_shard(
                shard,
                kind,
                commit_ts_floor,
                commit_ts_ceiling,
                layout_epoch_floor,
            )? {
                mutation.mark_changed();
                return Ok(RetentionLeaseHandle { slot: Some(slot) });
            }
        }
        Err(RegistryError::NoSlotAvailable)
    }

    fn allocate_on_shard(
        &self,
        shard_index: usize,
        kind: RetentionLeaseKind,
        commit_ts_floor: u64,
        commit_ts_ceiling: Option<u64>,
        layout_epoch_floor: Option<u64>,
    ) -> Result<Option<RetentionSlotRef>> {
        let Some(shard) = self.inner.shards.get(shard_index) else {
            return Err(RegistryError::InvalidShard);
        };
        let slot_index = {
            let mut free = shard.free_slots.lock();
            free.pop()
        };
        let Some(slot_index) = slot_index else {
            return Ok(None);
        };

        let slot = &shard.slots[slot_index].0;
        let generation = slot
            .generation
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        let lease_id = self.inner.next_lease_id.fetch_add(1, Ordering::AcqRel);
        let epoch = self.inner.watermark_epoch.load(Ordering::Acquire);

        slot.lease_id.store(lease_id, Ordering::Release);
        slot.kind.store(kind as u8, Ordering::Release);
        slot.commit_ts_floor
            .store(commit_ts_floor, Ordering::Release);
        slot.commit_ts_ceiling.store(
            commit_ts_ceiling.unwrap_or(MAX_TRANSACTION_ID),
            Ordering::Release,
        );
        slot.commit_ts_ceiling_set
            .store(commit_ts_ceiling.is_some(), Ordering::Release);
        slot.layout_epoch_floor
            .store(layout_epoch_floor.unwrap_or(0), Ordering::Release);
        slot.layout_epoch_floor_set
            .store(layout_epoch_floor.is_some(), Ordering::Release);
        slot.last_seen_epoch.store(epoch, Ordering::Release);
        slot.active.store(true, Ordering::Release);
        self.inner.lease_counts[kind.index()].fetch_add(1, Ordering::AcqRel);
        self.inner
            .lower_watermark(kind, commit_ts_floor, layout_epoch_floor);

        Ok(Some(RetentionSlotRef {
            registry: self.inner.clone(),
            shard_index,
            slot_index,
            generation,
        }))
    }
}

impl Clone for RetentionRegistry {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Default for RetentionRegistry {
    fn default() -> Self {
        Self::new(RetentionRegistryOptions::default())
    }
}

impl std::fmt::Debug for RetentionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetentionRegistry")
            .field("shard_count", &self.shard_count())
            .field("slots_per_shard", &self.slots_per_shard())
            .field("watermarks", &self.watermarks())
            .finish()
    }
}

impl RetentionAggregator {
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for RetentionAggregator {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl RetentionLeaseHandle {
    fn release(&mut self) -> Result<()> {
        self.release_inner()
    }

    fn release_inner(&mut self) -> Result<()> {
        let slot_ref = self.slot.take().ok_or(RegistryError::ReleasedHandle)?;
        slot_ref.registry.release_slot(&slot_ref)
    }

    fn info(&self) -> Result<RetentionLeaseInfo> {
        self.slot
            .as_ref()
            .ok_or(RegistryError::ReleasedHandle)?
            .info()
    }
}

impl RetentionRegistryInner {
    fn release_slot(&self, slot_ref: &RetentionSlotRef) -> Result<()> {
        let mut mutation = self.lifecycle.begin_mutation();
        let shard = self
            .shards
            .get(slot_ref.shard_index)
            .ok_or(RegistryError::InvalidShard)?;
        let slot = shard
            .slots
            .get(slot_ref.slot_index)
            .ok_or(RegistryError::StaleHandle)
            .map(|slot| &slot.0)?;

        if slot.generation.load(Ordering::Acquire) != slot_ref.generation {
            return Err(RegistryError::StaleHandle);
        }
        if !slot.active.swap(false, Ordering::AcqRel) {
            return Err(RegistryError::StaleHandle);
        }
        let kind = RetentionLeaseKind::from_raw(slot.kind.load(Ordering::Acquire))
            .ok_or(RegistryError::InvalidState)?;

        slot.kind.store(0, Ordering::Release);
        slot.lease_id.store(0, Ordering::Release);
        slot.commit_ts_floor
            .store(MAX_TRANSACTION_ID, Ordering::Release);
        slot.commit_ts_ceiling
            .store(MAX_TRANSACTION_ID, Ordering::Release);
        slot.commit_ts_ceiling_set.store(false, Ordering::Release);
        slot.layout_epoch_floor.store(0, Ordering::Release);
        slot.layout_epoch_floor_set.store(false, Ordering::Release);
        slot.last_seen_epoch.store(
            self.watermark_epoch.load(Ordering::Acquire),
            Ordering::Release,
        );

        let mut free = shard.free_slots.lock();
        free.push(slot_ref.slot_index);
        self.lease_counts[kind.index()].fetch_sub(1, Ordering::AcqRel);
        mutation.mark_changed();
        Ok(())
    }

    fn lower_watermark(
        &self,
        kind: RetentionLeaseKind,
        commit_ts_floor: u64,
        layout_epoch_floor: Option<u64>,
    ) {
        match kind {
            RetentionLeaseKind::ReadSnapshot => {
                lower_atomic_min(&self.oldest_read_ts, commit_ts_floor);
            }
            RetentionLeaseKind::LayoutEpoch => {
                if let Some(layout_epoch_floor) = layout_epoch_floor {
                    lower_atomic_min(&self.oldest_layout_epoch, layout_epoch_floor);
                }
            }
            RetentionLeaseKind::Backfill => {
                lower_atomic_min(&self.oldest_backfill_ts, commit_ts_floor);
            }
            RetentionLeaseKind::DerivedLag => {
                lower_atomic_min(&self.oldest_derived_delta_ts, commit_ts_floor);
            }
            RetentionLeaseKind::Checkpoint => {
                lower_atomic_min(&self.oldest_checkpoint_ts, commit_ts_floor);
            }
            RetentionLeaseKind::WriteConflict => {
                lower_atomic_min(&self.oldest_conflict_horizon, commit_ts_floor);
            }
        }
    }

    fn scan_and_publish(&self) -> RetentionWatermarks {
        let next_epoch = self.watermark_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let mut global = RetentionWatermarks::none(next_epoch);

        for shard in self.shards.iter() {
            let mut local = RetentionWatermarks::none(next_epoch);
            for padded in shard.slots.iter() {
                let slot = &padded.0;
                if !slot.active.load(Ordering::Acquire) {
                    continue;
                }

                let Some(kind) = RetentionLeaseKind::from_raw(slot.kind.load(Ordering::Acquire))
                else {
                    continue;
                };
                let commit_floor = slot.commit_ts_floor.load(Ordering::Acquire);
                let layout_floor = slot
                    .layout_epoch_floor_set
                    .load(Ordering::Acquire)
                    .then(|| slot.layout_epoch_floor.load(Ordering::Acquire));
                local.lease_counts[kind.index()] += 1;

                match kind {
                    RetentionLeaseKind::ReadSnapshot => {
                        local.oldest_read_ts =
                            ReadTs::new(local.oldest_read_ts.into_raw().min(commit_floor));
                    }
                    RetentionLeaseKind::LayoutEpoch => {
                        if let Some(layout_floor) = layout_floor {
                            local.oldest_layout_epoch =
                                min_layout_epoch(local.oldest_layout_epoch, layout_floor);
                        }
                    }
                    RetentionLeaseKind::Backfill => {
                        local.oldest_backfill_ts =
                            CommitTs::new(local.oldest_backfill_ts.into_raw().min(commit_floor));
                    }
                    RetentionLeaseKind::DerivedLag => {
                        local.oldest_derived_delta_ts = CommitTs::new(
                            local.oldest_derived_delta_ts.into_raw().min(commit_floor),
                        );
                    }
                    RetentionLeaseKind::Checkpoint => {
                        local.oldest_checkpoint_ts =
                            CommitTs::new(local.oldest_checkpoint_ts.into_raw().min(commit_floor));
                    }
                    RetentionLeaseKind::WriteConflict => {
                        local.oldest_conflict_horizon = CommitTs::new(
                            local.oldest_conflict_horizon.into_raw().min(commit_floor),
                        );
                    }
                }
            }

            shard.summary.0.store(&local);
            global.merge(local);
        }

        self.oldest_read_ts
            .store(global.oldest_read_ts.into_raw(), Ordering::Release);
        self.oldest_backfill_ts
            .store(global.oldest_backfill_ts.into_raw(), Ordering::Release);
        self.oldest_derived_delta_ts
            .store(global.oldest_derived_delta_ts.into_raw(), Ordering::Release);
        self.oldest_conflict_horizon
            .store(global.oldest_conflict_horizon.into_raw(), Ordering::Release);
        self.oldest_checkpoint_ts
            .store(global.oldest_checkpoint_ts.into_raw(), Ordering::Release);
        self.oldest_layout_epoch.store(
            global
                .oldest_layout_epoch
                .map(LayoutEpoch::into_raw)
                .unwrap_or(MAX_TRANSACTION_ID),
            Ordering::Release,
        );
        for idx in 0..LEASE_KIND_COUNT {
            self.lease_counts[idx].store(global.lease_counts[idx], Ordering::Release);
        }

        global
    }

    fn confirmed_scan_and_publish(&self) -> RetentionWatermarks {
        let dirty_epoch = self.lifecycle.dirty_epoch();
        if self.lifecycle.is_confirmed(dirty_epoch) {
            return self.watermarks();
        }
        self.scan_and_confirm()
    }

    fn scan_and_confirm(&self) -> RetentionWatermarks {
        let watermarks = self.scan_and_publish();
        self.lifecycle.confirm();
        watermarks
    }

    fn watermarks(&self) -> RetentionWatermarks {
        RetentionWatermarks {
            oldest_read_ts: ReadTs::new(self.oldest_read_ts.load(Ordering::Acquire)),
            oldest_backfill_ts: CommitTs::new(self.oldest_backfill_ts.load(Ordering::Acquire)),
            oldest_derived_delta_ts: CommitTs::new(
                self.oldest_derived_delta_ts.load(Ordering::Acquire),
            ),
            oldest_conflict_horizon: CommitTs::new(
                self.oldest_conflict_horizon.load(Ordering::Acquire),
            ),
            oldest_checkpoint_ts: CommitTs::new(self.oldest_checkpoint_ts.load(Ordering::Acquire)),
            oldest_layout_epoch: self.oldest_layout_epoch(),
            lease_counts: std::array::from_fn(|idx| self.lease_counts[idx].load(Ordering::Acquire)),
            epoch: self.watermark_epoch.load(Ordering::Acquire),
        }
    }

    fn oldest_layout_epoch(&self) -> Option<LayoutEpoch> {
        (self.lease_counts[RetentionLeaseKind::LayoutEpoch.index()].load(Ordering::Acquire) > 0)
            .then(|| LayoutEpoch::new(self.oldest_layout_epoch.load(Ordering::Acquire)))
    }
}

impl RetentionSlotRef {
    fn info(&self) -> Result<RetentionLeaseInfo> {
        let shard = self
            .registry
            .shards
            .get(self.shard_index)
            .ok_or(RegistryError::InvalidShard)?;
        let slot = shard
            .slots
            .get(self.slot_index)
            .ok_or(RegistryError::StaleHandle)
            .map(|slot| &slot.0)?;
        if slot.generation.load(Ordering::Acquire) != self.generation
            || !slot.active.load(Ordering::Acquire)
        {
            return Err(RegistryError::StaleHandle);
        }

        let kind = RetentionLeaseKind::from_raw(slot.kind.load(Ordering::Acquire))
            .ok_or(RegistryError::InvalidState)?;
        let commit_floor = slot.commit_ts_floor.load(Ordering::Acquire);
        let commit_ceiling = slot.commit_ts_ceiling.load(Ordering::Acquire);
        let commit_ceiling_set = slot.commit_ts_ceiling_set.load(Ordering::Acquire);
        let layout_floor = slot.layout_epoch_floor.load(Ordering::Acquire);
        let layout_floor_set = slot.layout_epoch_floor_set.load(Ordering::Acquire);
        Ok(RetentionLeaseInfo {
            lease_id: SnapshotId::new(slot.lease_id.load(Ordering::Acquire)),
            kind,
            commit_ts_floor: (kind != RetentionLeaseKind::LayoutEpoch)
                .then(|| CommitTs::new(commit_floor)),
            commit_ts_ceiling: (kind != RetentionLeaseKind::LayoutEpoch && commit_ceiling_set)
                .then(|| CommitTs::new(commit_ceiling)),
            layout_epoch_floor: (kind == RetentionLeaseKind::LayoutEpoch && layout_floor_set)
                .then(|| LayoutEpoch::new(layout_floor)),
            last_seen_epoch: slot.last_seen_epoch.load(Ordering::Acquire),
        })
    }
}

impl RetentionWatermarks {
    fn merge(&mut self, other: Self) {
        self.oldest_read_ts = ReadTs::new(
            self.oldest_read_ts
                .into_raw()
                .min(other.oldest_read_ts.into_raw()),
        );
        self.oldest_backfill_ts = CommitTs::new(
            self.oldest_backfill_ts
                .into_raw()
                .min(other.oldest_backfill_ts.into_raw()),
        );
        self.oldest_derived_delta_ts = CommitTs::new(
            self.oldest_derived_delta_ts
                .into_raw()
                .min(other.oldest_derived_delta_ts.into_raw()),
        );
        self.oldest_conflict_horizon = CommitTs::new(
            self.oldest_conflict_horizon
                .into_raw()
                .min(other.oldest_conflict_horizon.into_raw()),
        );
        self.oldest_checkpoint_ts = CommitTs::new(
            self.oldest_checkpoint_ts
                .into_raw()
                .min(other.oldest_checkpoint_ts.into_raw()),
        );
        self.oldest_layout_epoch =
            merge_layout_epoch_floor(self.oldest_layout_epoch, other.oldest_layout_epoch);
        for idx in 0..LEASE_KIND_COUNT {
            self.lease_counts[idx] += other.lease_counts[idx];
        }
    }
}

impl RetentionShard {
    fn new(slot_count: usize) -> Self {
        let slots = (0..slot_count)
            .map(|_| CachePadded::new(RetentionSlot::empty()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let free_slots = (0..slot_count).rev().collect();
        Self {
            slots,
            free_slots: Mutex::new(free_slots),
            summary: CachePadded::new(RetentionShardSummary::empty()),
        }
    }
}

impl RetentionShardSummary {
    fn empty() -> Self {
        Self {
            watermarks: RetentionWatermarkAtomics::empty(),
            lease_counts: std::array::from_fn(|_| AtomicU64::new(0)),
            epoch: AtomicU64::new(0),
        }
    }

    fn store(&self, watermarks: &RetentionWatermarks) {
        self.watermarks
            .oldest_read_ts
            .store(watermarks.oldest_read_ts.into_raw(), Ordering::Release);
        self.watermarks
            .oldest_backfill_ts
            .store(watermarks.oldest_backfill_ts.into_raw(), Ordering::Release);
        self.watermarks.oldest_derived_delta_ts.store(
            watermarks.oldest_derived_delta_ts.into_raw(),
            Ordering::Release,
        );
        self.watermarks.oldest_conflict_horizon.store(
            watermarks.oldest_conflict_horizon.into_raw(),
            Ordering::Release,
        );
        self.watermarks.oldest_checkpoint_ts.store(
            watermarks.oldest_checkpoint_ts.into_raw(),
            Ordering::Release,
        );
        self.watermarks.oldest_layout_epoch.store(
            watermarks
                .oldest_layout_epoch
                .map(LayoutEpoch::into_raw)
                .unwrap_or(MAX_TRANSACTION_ID),
            Ordering::Release,
        );
        for idx in 0..LEASE_KIND_COUNT {
            self.lease_counts[idx].store(watermarks.lease_counts[idx], Ordering::Release);
        }
        self.epoch.store(watermarks.epoch, Ordering::Release);
    }
}

impl RetentionWatermarkAtomics {
    fn empty() -> Self {
        Self {
            oldest_read_ts: AtomicU64::new(MAX_TRANSACTION_ID),
            oldest_backfill_ts: AtomicU64::new(MAX_TRANSACTION_ID),
            oldest_derived_delta_ts: AtomicU64::new(MAX_TRANSACTION_ID),
            oldest_conflict_horizon: AtomicU64::new(MAX_TRANSACTION_ID),
            oldest_checkpoint_ts: AtomicU64::new(MAX_TRANSACTION_ID),
            oldest_layout_epoch: AtomicU64::new(MAX_TRANSACTION_ID),
        }
    }
}

fn lower_atomic_min(atomic: &AtomicU64, candidate: u64) {
    let mut current = atomic.load(Ordering::Acquire);
    while candidate < current {
        match atomic.compare_exchange(current, candidate, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(next) => current = next,
        }
    }
}

fn min_layout_epoch(current: Option<LayoutEpoch>, candidate: u64) -> Option<LayoutEpoch> {
    Some(LayoutEpoch::new(
        current
            .map(LayoutEpoch::into_raw)
            .unwrap_or(MAX_TRANSACTION_ID)
            .min(candidate),
    ))
}

fn merge_layout_epoch_floor(
    left: Option<LayoutEpoch>,
    right: Option<LayoutEpoch>,
) -> Option<LayoutEpoch> {
    match (left, right) {
        (Some(left), Some(right)) => Some(LayoutEpoch::new(left.into_raw().min(right.into_raw()))),
        (Some(epoch), None) | (None, Some(epoch)) => Some(epoch),
        (None, None) => None,
    }
}

impl RetentionSlot {
    fn empty() -> Self {
        Self {
            generation: AtomicU64::new(0),
            lease_id: AtomicU64::new(0),
            kind: AtomicU8::new(0),
            commit_ts_floor: AtomicU64::new(MAX_TRANSACTION_ID),
            commit_ts_ceiling: AtomicU64::new(MAX_TRANSACTION_ID),
            commit_ts_ceiling_set: AtomicBool::new(false),
            layout_epoch_floor: AtomicU64::new(MAX_TRANSACTION_ID),
            layout_epoch_floor_set: AtomicBool::new(false),
            active: AtomicBool::new(false),
            last_seen_epoch: AtomicU64::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn typed_leases_publish_independent_watermarks() {
        let registry = RetentionRegistry::with_capacity(4, 8);
        let read = registry.lease_read_snapshot(ReadTs::new(50)).unwrap();
        let layout = registry.lease_layout_epoch(LayoutEpoch::new(7)).unwrap();
        let backfill = registry.lease_backfill(CommitTs::new(40)).unwrap();
        let derived = registry.lease_derived_lag(CommitTs::new(30)).unwrap();
        let checkpoint = registry.lease_checkpoint(CommitTs::new(20)).unwrap();
        let conflict = registry.lease_write_conflict(CommitTs::new(10)).unwrap();

        let watermarks = registry.refresh_watermarks();
        assert_eq!(watermarks.oldest_read_ts, ReadTs::new(50));
        assert_eq!(watermarks.oldest_layout_epoch, Some(LayoutEpoch::new(7)));
        assert_eq!(watermarks.oldest_backfill_ts, CommitTs::new(40));
        assert_eq!(watermarks.oldest_derived_delta_ts, CommitTs::new(30));
        assert_eq!(watermarks.oldest_checkpoint_ts, CommitTs::new(20));
        assert_eq!(watermarks.oldest_conflict_horizon, CommitTs::new(10));
        assert_eq!(watermarks.lease_count(RetentionLeaseKind::ReadSnapshot), 1);

        drop((read, layout, backfill, derived, checkpoint, conflict));
        let watermarks = registry.confirmed_watermarks();
        assert_eq!(watermarks.lease_counts, [0; LEASE_KIND_COUNT]);
        assert_eq!(watermarks.oldest_read_ts, ReadTs::new(MAX_TRANSACTION_ID));
        assert_eq!(watermarks.oldest_layout_epoch, None);
    }

    #[test]
    fn lease_info_is_typed() {
        let registry = RetentionRegistry::with_capacity(1, 2);
        let lease = registry.lease_layout_epoch(LayoutEpoch::new(99)).unwrap();
        let info = lease.info().unwrap();

        assert_eq!(info.kind, RetentionLeaseKind::LayoutEpoch);
        assert_eq!(info.commit_ts_floor, None);
        assert_eq!(info.commit_ts_ceiling, None);
        assert_eq!(info.layout_epoch_floor, Some(LayoutEpoch::new(99)));
    }

    #[test]
    fn read_snapshot_lease_tracks_owner_and_pin_duration() {
        let registry = RetentionRegistry::with_capacity(1, 2);
        let lease = registry.lease_read_snapshot(ReadTs::new(42)).unwrap();

        assert_eq!(lease.owner(), ReadSnapshotLeaseOwner::unowned());
        lease.bind_owner(ReadSnapshotLeaseOwner::for_portal(7, "cursor_a"));
        assert_eq!(lease.owner().owner_session_id, Some(7));
        assert_eq!(lease.owner().portal_id.as_deref(), Some("cursor_a"));
        assert!(lease.pinned_duration() >= Duration::ZERO);

        lease
            .transfer_owner(Some(7), ReadSnapshotLeaseOwner::for_portal(7, "cursor_b"))
            .unwrap();
        assert_eq!(lease.owner().portal_id.as_deref(), Some("cursor_b"));

        let err = lease
            .transfer_owner(Some(8), ReadSnapshotLeaseOwner::for_portal(8, "cursor_c"))
            .expect_err("lease transfer must require the current owner");
        assert_eq!(err.actual_owner_session_id, Some(7));

        lease.clear_owner();
        assert_eq!(lease.owner(), ReadSnapshotLeaseOwner::unowned());
    }

    #[test]
    fn derived_lag_lease_records_pinned_delta_range() {
        let registry = RetentionRegistry::with_capacity(1, 2);
        let lease = registry
            .lease_derived_lag_range(CommitTs::new(11), CommitTs::new(19))
            .unwrap();
        let info = lease.info().unwrap();

        assert_eq!(info.kind, RetentionLeaseKind::DerivedLag);
        assert_eq!(info.commit_ts_floor, Some(CommitTs::new(11)));
        assert_eq!(info.commit_ts_ceiling, Some(CommitTs::new(19)));
        assert_eq!(
            registry.refresh_watermarks().oldest_derived_delta_ts,
            CommitTs::new(11)
        );
    }

    #[test]
    fn backfill_lease_records_pinned_delta_range() {
        let registry = RetentionRegistry::with_capacity(1, 2);
        let lease = registry
            .lease_backfill_range(CommitTs::new(21), CommitTs::new(34))
            .unwrap();
        let info = lease.info().unwrap();

        assert_eq!(info.kind, RetentionLeaseKind::Backfill);
        assert_eq!(info.commit_ts_floor, Some(CommitTs::new(21)));
        assert_eq!(info.commit_ts_ceiling, Some(CommitTs::new(34)));
        assert_eq!(
            registry.refresh_watermarks().oldest_backfill_ts,
            CommitTs::new(21)
        );
    }

    #[test]
    fn release_reuses_slot() {
        let registry = RetentionRegistry::with_capacity(1, 1);
        let lease = registry.lease_read_snapshot(ReadTs::new(1)).unwrap();
        lease.release().unwrap();

        let lease = registry.lease_read_snapshot(ReadTs::new(2)).unwrap();
        assert_eq!(
            lease.info().unwrap().commit_ts_floor,
            Some(CommitTs::new(2))
        );
    }

    #[test]
    fn confirmed_watermarks_reuse_cached_epoch_until_lease_changes() {
        let registry = RetentionRegistry::with_capacity(1, 4);
        let lease = registry.lease_read_snapshot(ReadTs::new(10)).unwrap();

        let first = registry.confirmed_watermarks();
        let second = registry.confirmed_watermarks();
        assert_eq!(first.epoch, second.epoch);
        assert_eq!(second.lease_count(RetentionLeaseKind::ReadSnapshot), 1);

        lease.release().unwrap();
        let third = registry.confirmed_watermarks();
        assert!(third.epoch > second.epoch);
        assert_eq!(third.lease_count(RetentionLeaseKind::ReadSnapshot), 0);

        let fourth = registry.confirmed_watermarks();
        assert_eq!(third.epoch, fourth.epoch);
    }

    #[test]
    fn confirmed_action_blocks_new_lease_until_reclamation_finishes() {
        let registry = RetentionRegistry::with_capacity(1, 4);
        let snapshot_registry = registry.clone();
        let (action_started_tx, action_started_rx) = mpsc::channel();
        let (finish_action_tx, finish_action_rx) = mpsc::channel();
        let action = thread::spawn(move || {
            snapshot_registry.with_confirmed_watermarks(|watermarks| {
                action_started_tx.send(()).unwrap();
                finish_action_rx.recv().unwrap();
                watermarks
            })
        });
        action_started_rx.recv().unwrap();

        let lease_registry = registry.clone();
        let (lease_registered_tx, lease_registered_rx) = mpsc::channel();
        let lease = thread::spawn(move || {
            let lease = lease_registry.lease_read_snapshot(ReadTs::new(7)).unwrap();
            lease_registered_tx.send(()).unwrap();
            lease
        });
        assert!(lease_registered_rx
            .recv_timeout(Duration::from_millis(25))
            .is_err());

        finish_action_tx.send(()).unwrap();
        let watermarks = action.join().unwrap();
        assert_eq!(watermarks.lease_count(RetentionLeaseKind::ReadSnapshot), 0);
        lease_registered_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        let lease = lease.join().unwrap();
        assert_eq!(
            registry
                .confirmed_watermarks()
                .lease_count(RetentionLeaseKind::ReadSnapshot),
            1
        );
        drop(lease);
    }
}
