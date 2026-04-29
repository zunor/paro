// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Active transaction lifecycle registry.

use crate::cache::CachePadded;
use crate::error::{RegistryError, Result};
use crate::sync::Mutex;
use crate::types::{ReadTs, TxnId, MAX_TRANSACTION_ID};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ActiveTxnState {
    Empty = 0,
    ReadOnly = 1,
    ReadWrite = 2,
    Frozen = 3,
    Ending = 4,
}

impl ActiveTxnState {
    #[inline]
    fn from_raw(raw: u8) -> Self {
        match raw {
            1 => Self::ReadOnly,
            2 => Self::ReadWrite,
            3 => Self::Frozen,
            4 => Self::Ending,
            _ => Self::Empty,
        }
    }

    #[inline]
    fn is_active(self) -> bool {
        matches!(self, Self::ReadOnly | Self::ReadWrite | Self::Frozen)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ActiveTxnRegistryOptions {
    pub shard_count: usize,
    pub slots_per_shard: usize,
}

impl Default for ActiveTxnRegistryOptions {
    fn default() -> Self {
        Self {
            shard_count: 64,
            slots_per_shard: 256,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveTxnWatermarks {
    pub oldest_active_txn_id: TxnId,
    pub oldest_active_read_ts: ReadTs,
    pub oldest_active_start_ts: ReadTs,
    pub active_count: u64,
    pub oldest_active_rw_read_ts: ReadTs,
    pub oldest_active_rw_start_ts: ReadTs,
    pub active_rw_count: u64,
    pub epoch: u64,
}

impl ActiveTxnWatermarks {
    #[inline]
    fn none(epoch: u64) -> Self {
        Self {
            oldest_active_txn_id: TxnId::new(MAX_TRANSACTION_ID),
            oldest_active_read_ts: ReadTs::new(MAX_TRANSACTION_ID),
            oldest_active_start_ts: ReadTs::new(MAX_TRANSACTION_ID),
            active_count: 0,
            oldest_active_rw_read_ts: ReadTs::new(MAX_TRANSACTION_ID),
            oldest_active_rw_start_ts: ReadTs::new(MAX_TRANSACTION_ID),
            active_rw_count: 0,
            epoch,
        }
    }
}

impl Default for ActiveTxnWatermarks {
    fn default() -> Self {
        Self::none(0)
    }
}

pub struct ActiveTxnRegistry {
    inner: Arc<ActiveTxnRegistryInner>,
}

struct ActiveTxnRegistryInner {
    shards: Box<[ActiveTxnShard]>,
    next_shard: AtomicUsize,
    watermark_epoch: AtomicU64,
    oldest_active_txn_id: AtomicU64,
    oldest_active_read_ts: AtomicU64,
    oldest_active_start_ts: AtomicU64,
    active_count: AtomicU64,
    oldest_active_rw_read_ts: AtomicU64,
    oldest_active_rw_start_ts: AtomicU64,
    active_rw_count: AtomicU64,
    lifecycle_epoch: AtomicU64,
    confirmed_lifecycle_epoch: AtomicU64,
    confirm_scan_lock: Mutex<()>,
}

struct ActiveTxnShard {
    slots: Box<[CachePadded<ActiveTxnSlot>]>,
    free_slots: Mutex<Vec<usize>>,
    summary: CachePadded<ActiveTxnShardSummary>,
}

struct ActiveTxnShardSummary {
    oldest_active_txn_id: AtomicU64,
    oldest_active_read_ts: AtomicU64,
    oldest_active_start_ts: AtomicU64,
    active_count: AtomicU64,
    oldest_active_rw_read_ts: AtomicU64,
    oldest_active_rw_start_ts: AtomicU64,
    active_rw_count: AtomicU64,
    epoch: AtomicU64,
}

struct ActiveTxnSlot {
    generation: AtomicU64,
    txn_id: AtomicU64,
    read_ts: AtomicU64,
    start_ts: AtomicU64,
    state: AtomicU8,
    is_read_write: AtomicBool,
    last_seen_epoch: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveTxnSlotInfo {
    pub txn_id: TxnId,
    pub read_ts: ReadTs,
    pub start_ts: ReadTs,
    pub state: ActiveTxnState,
    pub is_read_write: bool,
    pub last_seen_epoch: u64,
}

#[derive(Clone)]
struct ActiveSlotRef {
    registry: Arc<ActiveTxnRegistryInner>,
    shard_index: usize,
    slot_index: usize,
    generation: u64,
}

pub struct ActiveTxnHandle {
    slot: Option<ActiveSlotRef>,
}

pub struct ActiveRwTxnHandle {
    slot: Option<ActiveSlotRef>,
}

pub struct ActiveTxnAggregator {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ActiveTxnRegistry {
    pub fn new(options: ActiveTxnRegistryOptions) -> Self {
        assert!(options.shard_count > 0, "active registry needs shards");
        assert!(
            options.slots_per_shard > 0,
            "active registry needs slots per shard"
        );

        let shards = (0..options.shard_count)
            .map(|_| ActiveTxnShard::new(options.slots_per_shard))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            inner: Arc::new(ActiveTxnRegistryInner {
                shards,
                next_shard: AtomicUsize::new(0),
                watermark_epoch: AtomicU64::new(0),
                oldest_active_txn_id: AtomicU64::new(MAX_TRANSACTION_ID),
                oldest_active_read_ts: AtomicU64::new(MAX_TRANSACTION_ID),
                oldest_active_start_ts: AtomicU64::new(MAX_TRANSACTION_ID),
                active_count: AtomicU64::new(0),
                oldest_active_rw_read_ts: AtomicU64::new(MAX_TRANSACTION_ID),
                oldest_active_rw_start_ts: AtomicU64::new(MAX_TRANSACTION_ID),
                active_rw_count: AtomicU64::new(0),
                lifecycle_epoch: AtomicU64::new(0),
                confirmed_lifecycle_epoch: AtomicU64::new(u64::MAX),
                confirm_scan_lock: Mutex::new(()),
            }),
        }
    }

    #[inline]
    pub fn with_capacity(shard_count: usize, slots_per_shard: usize) -> Self {
        Self::new(ActiveTxnRegistryOptions {
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

    pub fn register(
        &self,
        txn_id: TxnId,
        read_ts: ReadTs,
        start_ts: ReadTs,
    ) -> Result<ActiveTxnHandle> {
        self.register_inner(txn_id, read_ts, start_ts, false)
            .map(|slot| ActiveTxnHandle { slot: Some(slot) })
    }

    pub fn register_read_write(
        &self,
        txn_id: TxnId,
        read_ts: ReadTs,
        start_ts: ReadTs,
    ) -> Result<ActiveRwTxnHandle> {
        self.register_inner(txn_id, read_ts, start_ts, true)
            .map(|slot| ActiveRwTxnHandle { slot: Some(slot) })
    }

    pub fn try_register_on_shard(
        &self,
        shard_index: usize,
        txn_id: TxnId,
        read_ts: ReadTs,
        start_ts: ReadTs,
    ) -> Result<ActiveTxnHandle> {
        self.allocate_on_shard(shard_index, txn_id, read_ts, start_ts, false)?
            .map(|slot| ActiveTxnHandle { slot: Some(slot) })
            .ok_or(RegistryError::NoSlotAvailable)
    }

    pub fn try_register_read_write_on_shard(
        &self,
        shard_index: usize,
        txn_id: TxnId,
        read_ts: ReadTs,
        start_ts: ReadTs,
    ) -> Result<ActiveRwTxnHandle> {
        self.allocate_on_shard(shard_index, txn_id, read_ts, start_ts, true)?
            .map(|slot| ActiveRwTxnHandle { slot: Some(slot) })
            .ok_or(RegistryError::NoSlotAvailable)
    }

    pub fn watermarks(&self) -> ActiveTxnWatermarks {
        ActiveTxnWatermarks {
            oldest_active_txn_id: TxnId::new(
                self.inner.oldest_active_txn_id.load(Ordering::Acquire),
            ),
            oldest_active_read_ts: ReadTs::new(
                self.inner.oldest_active_read_ts.load(Ordering::Acquire),
            ),
            oldest_active_start_ts: ReadTs::new(
                self.inner.oldest_active_start_ts.load(Ordering::Acquire),
            ),
            active_count: self.inner.active_count.load(Ordering::Acquire),
            oldest_active_rw_read_ts: ReadTs::new(
                self.inner.oldest_active_rw_read_ts.load(Ordering::Acquire),
            ),
            oldest_active_rw_start_ts: ReadTs::new(
                self.inner.oldest_active_rw_start_ts.load(Ordering::Acquire),
            ),
            active_rw_count: self.inner.active_rw_count.load(Ordering::Acquire),
            epoch: self.inner.watermark_epoch.load(Ordering::Acquire),
        }
    }

    pub fn contains_transaction(&self, txn_id: TxnId) -> bool {
        let txn_id = txn_id.into_raw();
        self.inner.shards.iter().any(|shard| {
            shard.slots.iter().any(|padded| {
                let slot = &padded.0;
                let state = ActiveTxnState::from_raw(slot.state.load(Ordering::Acquire));
                state.is_active() && slot.txn_id.load(Ordering::Acquire) == txn_id
            })
        })
    }

    pub fn refresh_watermarks(&self) -> ActiveTxnWatermarks {
        self.inner.scan_and_publish()
    }

    pub fn confirmed_watermarks(&self) -> ActiveTxnWatermarks {
        self.inner.confirmed_scan_and_publish()
    }

    pub fn confirmed_oldest_active_rw_read_ts(&self) -> ReadTs {
        self.confirmed_watermarks().oldest_active_rw_read_ts
    }

    pub fn spawn_background_aggregator(&self, period: Duration) -> ActiveTxnAggregator {
        let registry = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                registry.refresh_watermarks();
                thread::sleep(period);
            }
        });
        ActiveTxnAggregator {
            stop,
            thread: Some(thread),
        }
    }

    fn register_inner(
        &self,
        txn_id: TxnId,
        read_ts: ReadTs,
        start_ts: ReadTs,
        read_write: bool,
    ) -> Result<ActiveSlotRef> {
        let shard_count = self.inner.shards.len();
        let start = self.inner.next_shard.fetch_add(1, Ordering::Relaxed) % shard_count;
        for offset in 0..shard_count {
            let shard = (start + offset) % shard_count;
            if let Some(slot) =
                self.allocate_on_shard(shard, txn_id, read_ts, start_ts, read_write)?
            {
                return Ok(slot);
            }
        }
        Err(RegistryError::NoSlotAvailable)
    }

    fn allocate_on_shard(
        &self,
        shard_index: usize,
        txn_id: TxnId,
        read_ts: ReadTs,
        start_ts: ReadTs,
        read_write: bool,
    ) -> Result<Option<ActiveSlotRef>> {
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
        let epoch = self.inner.watermark_epoch.load(Ordering::Acquire);

        slot.txn_id.store(txn_id.into_raw(), Ordering::Release);
        slot.read_ts.store(read_ts.into_raw(), Ordering::Release);
        slot.start_ts.store(start_ts.into_raw(), Ordering::Release);
        slot.is_read_write.store(read_write, Ordering::Release);
        slot.last_seen_epoch.store(epoch, Ordering::Release);
        slot.state.store(
            if read_write {
                ActiveTxnState::ReadWrite as u8
            } else {
                ActiveTxnState::ReadOnly as u8
            },
            Ordering::Release,
        );
        self.inner.active_count.fetch_add(1, Ordering::AcqRel);
        lower_atomic_min(&self.inner.oldest_active_txn_id, txn_id.into_raw());
        lower_atomic_min(&self.inner.oldest_active_read_ts, read_ts.into_raw());
        lower_atomic_min(&self.inner.oldest_active_start_ts, start_ts.into_raw());
        if read_write {
            self.inner.active_rw_count.fetch_add(1, Ordering::AcqRel);
            lower_atomic_min(&self.inner.oldest_active_rw_read_ts, read_ts.into_raw());
            lower_atomic_min(&self.inner.oldest_active_rw_start_ts, start_ts.into_raw());
        }
        self.inner.mark_lifecycle_dirty();

        Ok(Some(ActiveSlotRef {
            registry: self.inner.clone(),
            shard_index,
            slot_index,
            generation,
        }))
    }
}

impl Clone for ActiveTxnRegistry {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Default for ActiveTxnRegistry {
    fn default() -> Self {
        Self::new(ActiveTxnRegistryOptions::default())
    }
}

impl std::fmt::Debug for ActiveTxnRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveTxnRegistry")
            .field("shard_count", &self.shard_count())
            .field("slots_per_shard", &self.slots_per_shard())
            .field("watermarks", &self.watermarks())
            .finish()
    }
}

impl ActiveTxnHandle {
    pub fn promote(&mut self) -> Result<ActiveRwTxnHandle> {
        let slot = self
            .slot
            .as_ref()
            .ok_or(RegistryError::ReleasedHandle)?
            .clone();
        slot.registry.promote_slot(&slot)?;
        self.slot = None;
        Ok(ActiveRwTxnHandle { slot: Some(slot) })
    }

    pub fn freeze(&self) -> Result<()> {
        self.slot
            .as_ref()
            .ok_or(RegistryError::ReleasedHandle)?
            .freeze()
    }

    pub fn release(mut self) -> Result<()> {
        release_handle_slot(&mut self.slot)
    }

    pub fn info(&self) -> Result<ActiveTxnSlotInfo> {
        self.slot
            .as_ref()
            .ok_or(RegistryError::ReleasedHandle)?
            .info()
    }
}

impl Drop for ActiveTxnHandle {
    fn drop(&mut self) {
        let _ = release_handle_slot(&mut self.slot);
    }
}

impl ActiveRwTxnHandle {
    pub fn freeze(&self) -> Result<()> {
        self.slot
            .as_ref()
            .ok_or(RegistryError::ReleasedHandle)?
            .freeze()
    }

    pub fn release(mut self) -> Result<()> {
        release_handle_slot(&mut self.slot)
    }

    pub fn info(&self) -> Result<ActiveTxnSlotInfo> {
        self.slot
            .as_ref()
            .ok_or(RegistryError::ReleasedHandle)?
            .info()
    }
}

impl Drop for ActiveRwTxnHandle {
    fn drop(&mut self) {
        let _ = release_handle_slot(&mut self.slot);
    }
}

impl ActiveTxnAggregator {
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for ActiveTxnAggregator {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl ActiveTxnRegistryInner {
    fn watermarks(&self) -> ActiveTxnWatermarks {
        ActiveTxnWatermarks {
            oldest_active_txn_id: TxnId::new(self.oldest_active_txn_id.load(Ordering::Acquire)),
            oldest_active_read_ts: ReadTs::new(self.oldest_active_read_ts.load(Ordering::Acquire)),
            oldest_active_start_ts: ReadTs::new(
                self.oldest_active_start_ts.load(Ordering::Acquire),
            ),
            active_count: self.active_count.load(Ordering::Acquire),
            oldest_active_rw_read_ts: ReadTs::new(
                self.oldest_active_rw_read_ts.load(Ordering::Acquire),
            ),
            oldest_active_rw_start_ts: ReadTs::new(
                self.oldest_active_rw_start_ts.load(Ordering::Acquire),
            ),
            active_rw_count: self.active_rw_count.load(Ordering::Acquire),
            epoch: self.watermark_epoch.load(Ordering::Acquire),
        }
    }

    fn promote_slot(&self, slot_ref: &ActiveSlotRef) -> Result<()> {
        let slot = self.slot(slot_ref)?;

        loop {
            let state = ActiveTxnState::from_raw(slot.state.load(Ordering::Acquire));
            match state {
                ActiveTxnState::ReadWrite => {
                    slot.is_read_write.store(true, Ordering::Release);
                    break;
                }
                ActiveTxnState::ReadOnly => {
                    slot.is_read_write.store(true, Ordering::Release);
                    match slot.state.compare_exchange(
                        ActiveTxnState::ReadOnly as u8,
                        ActiveTxnState::ReadWrite as u8,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            self.active_rw_count.fetch_add(1, Ordering::AcqRel);
                            lower_atomic_min(
                                &self.oldest_active_rw_read_ts,
                                slot.read_ts.load(Ordering::Acquire),
                            );
                            lower_atomic_min(
                                &self.oldest_active_rw_start_ts,
                                slot.start_ts.load(Ordering::Acquire),
                            );
                            self.mark_lifecycle_dirty();
                            break;
                        }
                        Err(actual)
                            if ActiveTxnState::from_raw(actual) == ActiveTxnState::ReadWrite =>
                        {
                            continue;
                        }
                        Err(_) => {
                            slot.is_read_write.store(false, Ordering::Release);
                            return Err(RegistryError::InvalidState);
                        }
                    }
                }
                ActiveTxnState::Frozen | ActiveTxnState::Ending | ActiveTxnState::Empty => {
                    return Err(RegistryError::InvalidState);
                }
            }
        }

        slot.last_seen_epoch.store(
            self.watermark_epoch.load(Ordering::Acquire),
            Ordering::Release,
        );
        Ok(())
    }

    fn release_slot(&self, slot_ref: &ActiveSlotRef) -> Result<()> {
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

        let state = ActiveTxnState::from_raw(
            slot.state
                .swap(ActiveTxnState::Ending as u8, Ordering::AcqRel),
        );
        if state == ActiveTxnState::Empty {
            return Err(RegistryError::StaleHandle);
        }

        let was_read_write = slot.is_read_write.load(Ordering::Acquire);
        slot.is_read_write.store(false, Ordering::Release);
        slot.txn_id.store(0, Ordering::Release);
        slot.read_ts.store(MAX_TRANSACTION_ID, Ordering::Release);
        slot.start_ts.store(MAX_TRANSACTION_ID, Ordering::Release);
        slot.last_seen_epoch.store(
            self.watermark_epoch.load(Ordering::Acquire),
            Ordering::Release,
        );
        slot.state
            .store(ActiveTxnState::Empty as u8, Ordering::Release);

        let mut free = shard.free_slots.lock();
        free.push(slot_ref.slot_index);
        self.active_count.fetch_sub(1, Ordering::AcqRel);
        if was_read_write {
            self.active_rw_count.fetch_sub(1, Ordering::AcqRel);
        }
        self.mark_lifecycle_dirty();
        Ok(())
    }

    fn confirmed_scan_and_publish(&self) -> ActiveTxnWatermarks {
        loop {
            let dirty_epoch = self.lifecycle_epoch.load(Ordering::Acquire);
            if self.confirmed_lifecycle_epoch.load(Ordering::Acquire) == dirty_epoch {
                return self.watermarks();
            }

            let _guard = self.confirm_scan_lock.lock();
            let dirty_epoch = self.lifecycle_epoch.load(Ordering::Acquire);
            if self.confirmed_lifecycle_epoch.load(Ordering::Acquire) == dirty_epoch {
                return self.watermarks();
            }

            let watermarks = self.scan_and_publish();
            if self.lifecycle_epoch.load(Ordering::Acquire) == dirty_epoch {
                self.confirmed_lifecycle_epoch
                    .store(dirty_epoch, Ordering::Release);
                return watermarks;
            }
        }
    }

    fn scan_and_publish(&self) -> ActiveTxnWatermarks {
        let next_epoch = self.watermark_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let mut global = ActiveTxnWatermarks::none(next_epoch);

        for shard in self.shards.iter() {
            let mut shard_txn_id = MAX_TRANSACTION_ID;
            let mut shard_active_read = MAX_TRANSACTION_ID;
            let mut shard_active_start = MAX_TRANSACTION_ID;
            let mut shard_active_count = 0_u64;
            let mut shard_read = MAX_TRANSACTION_ID;
            let mut shard_start = MAX_TRANSACTION_ID;
            let mut shard_count = 0_u64;

            for padded in shard.slots.iter() {
                let slot = &padded.0;
                let state = ActiveTxnState::from_raw(slot.state.load(Ordering::Acquire));
                if !state.is_active() {
                    continue;
                }

                let txn_id = slot.txn_id.load(Ordering::Acquire);
                let read_ts = slot.read_ts.load(Ordering::Acquire);
                let start_ts = slot.start_ts.load(Ordering::Acquire);
                shard_txn_id = shard_txn_id.min(txn_id);
                shard_active_read = shard_active_read.min(read_ts);
                shard_active_start = shard_active_start.min(start_ts);
                shard_active_count += 1;

                if !slot.is_read_write.load(Ordering::Acquire) {
                    continue;
                }

                shard_read = shard_read.min(read_ts);
                shard_start = shard_start.min(start_ts);
                shard_count += 1;
            }

            shard
                .summary
                .0
                .oldest_active_txn_id
                .store(shard_txn_id, Ordering::Release);
            shard
                .summary
                .0
                .oldest_active_read_ts
                .store(shard_active_read, Ordering::Release);
            shard
                .summary
                .0
                .oldest_active_start_ts
                .store(shard_active_start, Ordering::Release);
            shard
                .summary
                .0
                .active_count
                .store(shard_active_count, Ordering::Release);
            shard
                .summary
                .0
                .oldest_active_rw_read_ts
                .store(shard_read, Ordering::Release);
            shard
                .summary
                .0
                .oldest_active_rw_start_ts
                .store(shard_start, Ordering::Release);
            shard
                .summary
                .0
                .active_rw_count
                .store(shard_count, Ordering::Release);
            shard.summary.0.epoch.store(next_epoch, Ordering::Release);

            global.oldest_active_txn_id =
                TxnId::new(global.oldest_active_txn_id.into_raw().min(shard_txn_id));
            global.oldest_active_read_ts = ReadTs::new(
                global
                    .oldest_active_read_ts
                    .into_raw()
                    .min(shard_active_read),
            );
            global.oldest_active_start_ts = ReadTs::new(
                global
                    .oldest_active_start_ts
                    .into_raw()
                    .min(shard_active_start),
            );
            global.active_count += shard_active_count;
            global.oldest_active_rw_read_ts =
                ReadTs::new(global.oldest_active_rw_read_ts.into_raw().min(shard_read));
            global.oldest_active_rw_start_ts =
                ReadTs::new(global.oldest_active_rw_start_ts.into_raw().min(shard_start));
            global.active_rw_count += shard_count;
        }

        self.oldest_active_txn_id
            .store(global.oldest_active_txn_id.into_raw(), Ordering::Release);
        self.oldest_active_read_ts
            .store(global.oldest_active_read_ts.into_raw(), Ordering::Release);
        self.oldest_active_start_ts
            .store(global.oldest_active_start_ts.into_raw(), Ordering::Release);
        self.active_count
            .store(global.active_count, Ordering::Release);
        self.oldest_active_rw_read_ts.store(
            global.oldest_active_rw_read_ts.into_raw(),
            Ordering::Release,
        );
        self.oldest_active_rw_start_ts.store(
            global.oldest_active_rw_start_ts.into_raw(),
            Ordering::Release,
        );
        self.active_rw_count
            .store(global.active_rw_count, Ordering::Release);

        global
    }

    fn slot(&self, slot_ref: &ActiveSlotRef) -> Result<&ActiveTxnSlot> {
        let slot = self
            .shards
            .get(slot_ref.shard_index)
            .ok_or(RegistryError::InvalidShard)?
            .slots
            .get(slot_ref.slot_index)
            .ok_or(RegistryError::StaleHandle)
            .map(|slot| &slot.0)?;
        if slot.generation.load(Ordering::Acquire) != slot_ref.generation {
            return Err(RegistryError::StaleHandle);
        }
        Ok(slot)
    }

    #[inline]
    fn mark_lifecycle_dirty(&self) {
        self.lifecycle_epoch.fetch_add(1, Ordering::AcqRel);
    }
}

impl ActiveSlotRef {
    fn freeze(&self) -> Result<()> {
        let slot = self.registry.slot(self)?;
        let state = ActiveTxnState::from_raw(slot.state.load(Ordering::Acquire));
        if !state.is_active() {
            return Err(RegistryError::InvalidState);
        }
        slot.state
            .store(ActiveTxnState::Frozen as u8, Ordering::Release);
        Ok(())
    }

    fn info(&self) -> Result<ActiveTxnSlotInfo> {
        let slot = self.registry.slot(self)?;
        Ok(ActiveTxnSlotInfo {
            txn_id: TxnId::new(slot.txn_id.load(Ordering::Acquire)),
            read_ts: ReadTs::new(slot.read_ts.load(Ordering::Acquire)),
            start_ts: ReadTs::new(slot.start_ts.load(Ordering::Acquire)),
            state: ActiveTxnState::from_raw(slot.state.load(Ordering::Acquire)),
            is_read_write: slot.is_read_write.load(Ordering::Acquire),
            last_seen_epoch: slot.last_seen_epoch.load(Ordering::Acquire),
        })
    }
}

impl ActiveTxnShard {
    fn new(slot_count: usize) -> Self {
        let slots = (0..slot_count)
            .map(|_| CachePadded::new(ActiveTxnSlot::empty()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let free_slots = (0..slot_count).rev().collect();
        Self {
            slots,
            free_slots: Mutex::new(free_slots),
            summary: CachePadded::new(ActiveTxnShardSummary::empty()),
        }
    }
}

impl ActiveTxnShardSummary {
    fn empty() -> Self {
        Self {
            oldest_active_txn_id: AtomicU64::new(MAX_TRANSACTION_ID),
            oldest_active_read_ts: AtomicU64::new(MAX_TRANSACTION_ID),
            oldest_active_start_ts: AtomicU64::new(MAX_TRANSACTION_ID),
            active_count: AtomicU64::new(0),
            oldest_active_rw_read_ts: AtomicU64::new(MAX_TRANSACTION_ID),
            oldest_active_rw_start_ts: AtomicU64::new(MAX_TRANSACTION_ID),
            active_rw_count: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
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

impl ActiveTxnSlot {
    fn empty() -> Self {
        Self {
            generation: AtomicU64::new(0),
            txn_id: AtomicU64::new(0),
            read_ts: AtomicU64::new(MAX_TRANSACTION_ID),
            start_ts: AtomicU64::new(MAX_TRANSACTION_ID),
            state: AtomicU8::new(ActiveTxnState::Empty as u8),
            is_read_write: AtomicBool::new(false),
            last_seen_epoch: AtomicU64::new(0),
        }
    }
}

fn release_handle_slot(slot: &mut Option<ActiveSlotRef>) -> Result<()> {
    let slot_ref = slot.take().ok_or(RegistryError::ReleasedHandle)?;
    slot_ref.registry.release_slot(&slot_ref)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_handle_promotes_and_releases_slot() {
        let registry = ActiveTxnRegistry::with_capacity(2, 2);
        let mut handle = registry
            .try_register_on_shard(0, TxnId::new(100), ReadTs::new(10), ReadTs::new(10))
            .unwrap();

        let rw = handle.promote().unwrap();
        let info = rw.info().unwrap();
        assert_eq!(info.state, ActiveTxnState::ReadWrite);
        assert!(info.is_read_write);

        let watermarks = registry.refresh_watermarks();
        assert_eq!(watermarks.active_rw_count, 1);
        assert_eq!(watermarks.oldest_active_rw_read_ts, ReadTs::new(10));

        rw.release().unwrap();
        let watermarks = registry.confirmed_watermarks();
        assert_eq!(watermarks.active_rw_count, 0);
        assert_eq!(
            watermarks.oldest_active_rw_read_ts,
            ReadTs::new(MAX_TRANSACTION_ID)
        );
    }

    #[test]
    fn frozen_read_write_still_pins_active_rw_watermark() {
        let registry = ActiveTxnRegistry::with_capacity(1, 4);
        let rw = registry
            .register_read_write(TxnId::new(1), ReadTs::new(7), ReadTs::new(8))
            .unwrap();
        rw.freeze().unwrap();

        let watermarks = registry.refresh_watermarks();
        assert_eq!(watermarks.active_rw_count, 1);
        assert_eq!(watermarks.oldest_active_rw_read_ts, ReadTs::new(7));
        assert_eq!(watermarks.oldest_active_rw_start_ts, ReadTs::new(8));
    }

    #[test]
    fn frozen_read_only_cannot_promote_to_read_write() {
        let registry = ActiveTxnRegistry::with_capacity(1, 4);
        let mut handle = registry
            .register(TxnId::new(1), ReadTs::new(7), ReadTs::new(8))
            .unwrap();
        handle.freeze().unwrap();

        let err = match handle.promote() {
            Ok(_) => panic!("frozen read-only transaction must not promote"),
            Err(err) => err,
        };
        assert_eq!(err, RegistryError::InvalidState);

        let watermarks = registry.confirmed_watermarks();
        assert_eq!(watermarks.active_count, 1);
        assert_eq!(watermarks.active_rw_count, 0);
        assert_eq!(
            watermarks.oldest_active_rw_read_ts,
            ReadTs::new(MAX_TRANSACTION_ID)
        );

        handle.release().unwrap();
        let watermarks = registry.confirmed_watermarks();
        assert_eq!(watermarks.active_count, 0);
    }

    #[test]
    fn confirmed_watermarks_reuse_cached_epoch_until_lifecycle_changes() {
        let registry = ActiveTxnRegistry::with_capacity(1, 4);
        let rw = registry
            .register_read_write(TxnId::new(1), ReadTs::new(7), ReadTs::new(8))
            .unwrap();

        let first = registry.confirmed_watermarks();
        let second = registry.confirmed_watermarks();
        assert_eq!(first.epoch, second.epoch);
        assert_eq!(second.active_rw_count, 1);

        rw.release().unwrap();
        let third = registry.confirmed_watermarks();
        assert!(third.epoch > second.epoch);
        assert_eq!(third.active_rw_count, 0);

        let fourth = registry.confirmed_watermarks();
        assert_eq!(third.epoch, fourth.epoch);
    }

    #[test]
    fn shard_local_registration_reports_full_without_global_scan() {
        let registry = ActiveTxnRegistry::with_capacity(1, 1);
        let _handle = registry
            .try_register_on_shard(0, TxnId::new(1), ReadTs::new(1), ReadTs::new(1))
            .unwrap();

        let err = match registry.try_register_on_shard(
            0,
            TxnId::new(2),
            ReadTs::new(2),
            ReadTs::new(2),
        ) {
            Ok(_) => panic!("second registration should not fit into a one-slot shard"),
            Err(err) => err,
        };
        assert_eq!(err, RegistryError::NoSlotAvailable);
    }
}
