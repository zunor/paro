// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PrimaryIndex - in-memory L0 primary key map.
//!
//! Responsibilities:
//! - Sharded in-memory primary-key lookup with length-aware buckets
//! - Inline storage for short/fixed-length keys to reduce heap churn
//! - Memory tracking with capacity-aware accounting and bounded backpressure
//! - Bulk build helpers for loading from existing rowsets/segments

use crate::metrics::storage_metrics;
use crate::primary_key::{ComparableEncoder, RowID, NULL_ROW_ID};
use crate::tablet::{KeysType, TabletSchema, TabletSchemaRef};
use parking_lot::{Condvar, Mutex, RwLock};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const DEFAULT_SHARD_COUNT: usize = 16;
const DEFAULT_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024; // 64MB
const INLINE_KEY_MAX_BYTES: usize = 32;
const HASHMAP_CONTROL_BYTES_PER_BUCKET: usize = 1;
const BACKPRESSURE_WAIT_SLICE: Duration = Duration::from_millis(5);
const BACKPRESSURE_MAX_WAIT: Duration = Duration::from_millis(100);

#[derive(Clone, Copy)]
struct FixedKey {
    len: u8,
    bytes: [u8; INLINE_KEY_MAX_BYTES],
}

impl FixedKey {
    fn from_slice(key: &[u8]) -> Self {
        debug_assert!(key.len() <= INLINE_KEY_MAX_BYTES);
        let mut bytes = [0u8; INLINE_KEY_MAX_BYTES];
        bytes[..key.len()].copy_from_slice(key);
        Self {
            len: key.len() as u8,
            bytes,
        }
    }

    fn to_vec(self) -> Vec<u8> {
        self.bytes[..self.len as usize].to_vec()
    }
}

impl PartialEq for FixedKey {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len
            && self.bytes[..self.len as usize] == other.bytes[..other.len as usize]
    }
}

impl Eq for FixedKey {}

impl Hash for FixedKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.len.hash(state);
        state.write(&self.bytes[..self.len as usize]);
    }
}

type FixedLenBucket = HashMap<FixedKey, RowID>;
type VariableLenBucket = HashMap<Box<[u8]>, RowID>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimaryIndexVersion {
    pub row_id: RowID,
    pub commit_ts: u64,
}

impl PrimaryIndexVersion {
    #[inline]
    pub fn live(row_id: RowID, commit_ts: u64) -> Self {
        Self { row_id, commit_ts }
    }

    #[inline]
    pub fn tombstone(commit_ts: u64) -> Self {
        Self {
            row_id: RowID::from_raw(NULL_ROW_ID),
            commit_ts,
        }
    }

    #[inline]
    pub fn is_tombstone(self) -> bool {
        self.row_id.is_null()
    }

    #[inline]
    pub fn visible_row_id(self) -> Option<RowID> {
        (!self.is_tombstone()).then_some(self.row_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryKeyWriteConflict {
    pub key: Vec<u8>,
    pub version: PrimaryIndexVersion,
}

impl PrimaryKeyWriteConflict {
    #[inline]
    pub fn commit_ts(&self) -> u64 {
        self.version.commit_ts
    }

    #[inline]
    pub fn is_tombstone(&self) -> bool {
        self.version.is_tombstone()
    }
}

const fn fixed_bucket_entry_bytes() -> usize {
    size_of::<FixedKey>() + size_of::<RowID>() + HASHMAP_CONTROL_BYTES_PER_BUCKET
}

const fn overflow_bucket_entry_bytes() -> usize {
    size_of::<Box<[u8]>>() + size_of::<RowID>() + HASHMAP_CONTROL_BYTES_PER_BUCKET
}

const fn overflow_len_bucket_bytes() -> usize {
    size_of::<usize>() + size_of::<VariableLenBucket>() + HASHMAP_CONTROL_BYTES_PER_BUCKET
}

const fn history_bucket_entry_bytes() -> usize {
    size_of::<Box<[u8]>>()
        + size_of::<Vec<PrimaryIndexVersion>>()
        + HASHMAP_CONTROL_BYTES_PER_BUCKET
}

struct PrimaryIndexShard {
    fixed_buckets: Vec<FixedLenBucket>,
    overflow_buckets: HashMap<usize, VariableLenBucket>,
    histories: HashMap<Box<[u8]>, Vec<PrimaryIndexVersion>>,
    len: usize,
    memory_usage: usize,
}

impl Default for PrimaryIndexShard {
    fn default() -> Self {
        Self::new()
    }
}

impl PrimaryIndexShard {
    fn new() -> Self {
        let mut fixed_buckets = Vec::with_capacity(INLINE_KEY_MAX_BYTES + 1);
        for _ in 0..=INLINE_KEY_MAX_BYTES {
            fixed_buckets.push(HashMap::new());
        }
        let mut shard = Self {
            fixed_buckets,
            overflow_buckets: HashMap::new(),
            histories: HashMap::new(),
            len: 0,
            memory_usage: 0,
        };
        shard.memory_usage = shard.recalculate_memory_usage();
        shard
    }

    fn len(&self) -> usize {
        self.len
    }

    fn lookup(&self, key: &[u8]) -> Option<RowID> {
        self.latest_version(key)
            .and_then(PrimaryIndexVersion::visible_row_id)
    }

    fn latest_version(&self, key: &[u8]) -> Option<PrimaryIndexVersion> {
        self.histories
            .get(key)
            .and_then(|chain| chain.last().copied())
    }

    fn lookup_version_at(&self, key: &[u8], read_ts: u64) -> Option<PrimaryIndexVersion> {
        self.histories.get(key).and_then(|chain| {
            let pos = chain.partition_point(|version| version.commit_ts <= read_ts);
            pos.checked_sub(1).and_then(|idx| chain.get(idx)).copied()
        })
    }

    fn first_write_for_key_in_range(
        &self,
        key: &[u8],
        read_ts: u64,
        commit_ts: u64,
    ) -> Option<PrimaryKeyWriteConflict> {
        first_version_in_range(self.histories.get(key)?, read_ts, commit_ts).map(|version| {
            PrimaryKeyWriteConflict {
                key: key.to_vec(),
                version,
            }
        })
    }

    fn first_write_for_key_range_in_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        read_ts: u64,
        commit_ts: u64,
    ) -> Option<PrimaryKeyWriteConflict> {
        if read_ts >= commit_ts {
            return None;
        }

        let mut best = None;
        for (key, chain) in &self.histories {
            if !key_in_bounds(key, lower, upper) {
                continue;
            }
            let Some(version) = first_version_in_range(chain, read_ts, commit_ts) else {
                continue;
            };
            select_earlier_conflict(
                &mut best,
                PrimaryKeyWriteConflict {
                    key: key.to_vec(),
                    version,
                },
            );
        }
        best
    }

    fn insert_or_replace(&mut self, key: Vec<u8>, row_id: RowID) -> (Option<RowID>, isize) {
        self.insert_or_replace_at(key, row_id, 0)
    }

    fn insert_or_replace_at(
        &mut self,
        key: Vec<u8>,
        row_id: RowID,
        commit_ts: u64,
    ) -> (Option<RowID>, isize) {
        let old_usage = self.memory_usage;
        let old = self.insert_or_replace_latest(key.clone(), row_id);
        let history_delta = self.push_version(key, PrimaryIndexVersion::live(row_id, commit_ts));
        self.apply_memory_delta(history_delta);
        let new_usage = self.memory_usage;
        (old, new_usage as isize - old_usage as isize)
    }

    fn batch_insert_or_replace_at(
        &mut self,
        entries: Vec<(Vec<u8>, RowID)>,
        commit_ts: u64,
    ) -> (u64, usize, isize) {
        let old_usage = self.memory_usage;
        let mut conflicts = 0u64;
        let mut processed = 0usize;

        for (key, row_id) in entries {
            if self.insert_or_replace_latest(key.clone(), row_id).is_some() {
                conflicts += 1;
            }
            let history_delta =
                self.push_version(key, PrimaryIndexVersion::live(row_id, commit_ts));
            self.apply_memory_delta(history_delta);
            processed += 1;
        }

        let new_usage = self.memory_usage;
        (
            conflicts,
            processed,
            new_usage as isize - old_usage as isize,
        )
    }

    fn insert_or_replace_latest(&mut self, key: Vec<u8>, row_id: RowID) -> Option<RowID> {
        let (old, delta) = if key.len() <= INLINE_KEY_MAX_BYTES {
            let bucket = &mut self.fixed_buckets[key.len()];
            let old_capacity = bucket.capacity();
            let old = bucket.insert(FixedKey::from_slice(&key), row_id);
            let capacity_delta = bucket.capacity() as isize - old_capacity as isize;
            (old, capacity_delta * fixed_bucket_entry_bytes() as isize)
        } else {
            let key_len = key.len();
            let old_outer_capacity = self.overflow_buckets.capacity();
            let (old, inner_capacity_delta) = {
                let bucket = self.overflow_buckets.entry(key_len).or_default();
                let old_inner_capacity = bucket.capacity();
                let old = bucket.insert(key.into_boxed_slice(), row_id);
                let inner_capacity_delta = bucket.capacity() as isize - old_inner_capacity as isize;
                (old, inner_capacity_delta)
            };
            let outer_capacity_delta =
                self.overflow_buckets.capacity() as isize - old_outer_capacity as isize;
            let key_bytes_delta = if old.is_none() { key_len as isize } else { 0 };
            (
                old,
                outer_capacity_delta * overflow_len_bucket_bytes() as isize
                    + inner_capacity_delta * overflow_bucket_entry_bytes() as isize
                    + key_bytes_delta,
            )
        };
        if old.is_none() {
            self.len += 1;
        }
        self.apply_memory_delta(delta);
        old
    }

    fn remove_at(&mut self, key: &[u8], commit_ts: u64) -> (Option<RowID>, isize) {
        let old_usage = self.memory_usage;
        let removed = self.remove_latest(key);
        let history_delta =
            self.push_version(key.to_vec(), PrimaryIndexVersion::tombstone(commit_ts));
        self.apply_memory_delta(history_delta);
        let new_usage = self.memory_usage;
        (removed, new_usage as isize - old_usage as isize)
    }

    fn batch_apply_versions(
        &mut self,
        entries: Vec<(Vec<u8>, PrimaryIndexVersion)>,
    ) -> (usize, isize) {
        let old_usage = self.memory_usage;
        let mut processed = 0usize;

        for (key, version) in entries {
            if version.is_tombstone() {
                self.remove_latest(&key);
                let history_delta = self.push_version(key, version);
                self.apply_memory_delta(history_delta);
            } else {
                self.insert_or_replace_latest(key.clone(), version.row_id);
                let history_delta = self.push_version(key, version);
                self.apply_memory_delta(history_delta);
            }
            processed += 1;
        }

        let new_usage = self.memory_usage;
        (processed, new_usage as isize - old_usage as isize)
    }

    fn remove_latest(&mut self, key: &[u8]) -> Option<RowID> {
        let removed = if key.len() <= INLINE_KEY_MAX_BYTES {
            let bucket = &mut self.fixed_buckets[key.len()];
            let old_capacity = bucket.capacity();
            let removed = bucket.remove(&FixedKey::from_slice(key));
            let capacity_delta = bucket.capacity() as isize - old_capacity as isize;
            (
                removed,
                capacity_delta * fixed_bucket_entry_bytes() as isize,
            )
        } else {
            let key_len = key.len();
            let mut removed = None;
            let mut delta = 0isize;
            let mut remove_bucket = false;
            let mut bucket_capacity = 0usize;
            let old_outer_capacity = self.overflow_buckets.capacity();

            if let Some(bucket) = self.overflow_buckets.get_mut(&key_len) {
                let old_inner_capacity = bucket.capacity();
                removed = bucket.remove(key);
                let inner_capacity_delta = bucket.capacity() as isize - old_inner_capacity as isize;
                delta += inner_capacity_delta * overflow_bucket_entry_bytes() as isize;
                if removed.is_some() {
                    delta -= key_len as isize;
                    if bucket.is_empty() {
                        bucket_capacity = bucket.capacity();
                        remove_bucket = true;
                    }
                }
            }

            if remove_bucket {
                self.overflow_buckets.remove(&key_len);
                delta -= (bucket_capacity * overflow_bucket_entry_bytes()) as isize;
                let outer_capacity_delta =
                    self.overflow_buckets.capacity() as isize - old_outer_capacity as isize;
                delta += outer_capacity_delta * overflow_len_bucket_bytes() as isize;
            }

            (removed, delta)
        };
        if removed.0.is_some() {
            self.len = self.len.saturating_sub(1);
        }
        self.apply_memory_delta(removed.1);
        removed.0
    }

    fn push_version(&mut self, key: Vec<u8>, version: PrimaryIndexVersion) -> isize {
        let key_len = key.len();
        let old_map_capacity = self.histories.capacity();
        let old_chain_capacity = self.histories.get(key.as_slice()).map(Vec::capacity);
        let new_chain_capacity = {
            let chain = self.histories.entry(key.into_boxed_slice()).or_default();
            match chain
                .last()
                .map(|stored| stored.commit_ts.cmp(&version.commit_ts))
            {
                Some(std::cmp::Ordering::Less) | None => chain.push(version),
                Some(std::cmp::Ordering::Equal) => {
                    if let Some(last) = chain.last_mut() {
                        *last = version;
                    }
                }
                Some(std::cmp::Ordering::Greater) => {
                    match chain.binary_search_by_key(&version.commit_ts, |stored| stored.commit_ts)
                    {
                        Ok(pos) => chain[pos] = version,
                        Err(pos) => chain.insert(pos, version),
                    }
                }
            }
            chain.capacity()
        };

        let map_delta = self.histories.capacity() as isize - old_map_capacity as isize;
        let chain_delta = match old_chain_capacity {
            Some(old_capacity) => new_chain_capacity as isize - old_capacity as isize,
            None => new_chain_capacity as isize,
        };
        let key_delta = if old_chain_capacity.is_none() {
            key_len as isize
        } else {
            0
        };
        map_delta * history_bucket_entry_bytes() as isize
            + chain_delta * size_of::<PrimaryIndexVersion>() as isize
            + key_delta
    }

    fn clear(&mut self) {
        for bucket in &mut self.fixed_buckets {
            bucket.clear();
            bucket.shrink_to_fit();
        }
        self.overflow_buckets.clear();
        self.overflow_buckets.shrink_to_fit();
        self.histories.clear();
        self.histories.shrink_to_fit();
        self.len = 0;
    }

    fn snapshot_into(&self, out: &mut Vec<(Vec<u8>, RowID)>) {
        for bucket in &self.fixed_buckets {
            out.extend(bucket.iter().map(|(key, row_id)| (key.to_vec(), *row_id)));
        }
        for bucket in self.overflow_buckets.values() {
            out.extend(bucket.iter().map(|(key, row_id)| (key.to_vec(), *row_id)));
        }
    }

    fn snapshot_versions_into(&self, out: &mut Vec<(Vec<u8>, PrimaryIndexVersion)>) {
        for (key, chain) in &self.histories {
            out.extend(chain.iter().copied().map(|version| (key.to_vec(), version)));
        }
    }

    fn recalculate_memory_usage(&self) -> usize {
        let mut total = self.fixed_buckets.capacity() * size_of::<FixedLenBucket>();

        for bucket in &self.fixed_buckets {
            total += bucket.capacity() * fixed_bucket_entry_bytes();
        }

        total += self.overflow_buckets.capacity() * overflow_len_bucket_bytes();

        for bucket in self.overflow_buckets.values() {
            let key_bytes = bucket.keys().map(|key| key.len()).sum::<usize>();
            total += bucket.capacity() * overflow_bucket_entry_bytes();
            total += key_bytes;
        }

        total += self.histories.capacity() * history_bucket_entry_bytes();
        for (key, chain) in &self.histories {
            total += key.len();
            total += chain.capacity() * size_of::<PrimaryIndexVersion>();
        }

        total
    }

    fn apply_memory_delta(&mut self, delta: isize) {
        if delta > 0 {
            self.memory_usage += delta as usize;
        } else if delta < 0 {
            self.memory_usage -= (-delta) as usize;
        }
    }
}

/// In-memory primary key index (L0).
pub struct PrimaryIndex {
    shards: Vec<RwLock<PrimaryIndexShard>>,
    memory_usage: AtomicUsize,
    memory_limit: AtomicUsize,
    on_exceed: Mutex<Option<Box<dyn FnMut(usize) + Send>>>,
    backpressure_lock: Mutex<()>,
    backpressure_cv: Condvar,
}

impl std::fmt::Debug for PrimaryIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimaryIndex")
            .field("shards", &self.shards.len())
            .field("memory_usage", &self.memory_usage.load(Ordering::Relaxed))
            .field("memory_limit", &self.memory_limit.load(Ordering::Relaxed))
            .finish()
    }
}

impl Default for PrimaryIndex {
    fn default() -> Self {
        Self::with_options(DEFAULT_SHARD_COUNT, DEFAULT_MEMORY_LIMIT_BYTES)
    }
}

impl PrimaryIndex {
    /// Create with custom shard count and memory limit (bytes).
    pub fn with_options(num_shards: usize, memory_limit: usize) -> Self {
        let shard_count = num_shards.max(1);
        let shards: Vec<_> = (0..shard_count)
            .map(|_| RwLock::new(PrimaryIndexShard::new()))
            .collect();
        let initial_usage = shards
            .iter()
            .map(|shard| shard.read().memory_usage)
            .sum::<usize>();
        storage_metrics().set_primary_index_memory(initial_usage);
        Self {
            shards,
            memory_usage: AtomicUsize::new(initial_usage),
            memory_limit: AtomicUsize::new(memory_limit.max(1)),
            on_exceed: Mutex::new(None),
            backpressure_lock: Mutex::new(()),
            backpressure_cv: Condvar::new(),
        }
    }

    /// Create with defaults (16 shards, 64MB soft limit).
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a callback invoked when memory usage exceeds the limit.
    /// The callback receives current usage in bytes.
    pub fn register_mem_exceed_callback<F>(&self, cb: F)
    where
        F: FnMut(usize) + Send + 'static,
    {
        *self.on_exceed.lock() = Some(Box::new(cb));
    }

    /// Update the soft memory limit (bytes).
    pub fn set_memory_limit(&self, bytes: usize) {
        self.memory_limit.store(bytes.max(1), Ordering::Release);
        self.backpressure_cv.notify_all();
    }

    /// Current approximate memory usage (bytes).
    pub fn memory_usage_bytes(&self) -> usize {
        self.memory_usage.load(Ordering::Acquire)
    }

    /// Number of shards.
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Whether the key exists.
    pub fn contains(&self, key: &[u8]) -> bool {
        let shard = self.shard_for(key);
        self.shards[shard].read().lookup(key).is_some()
    }

    /// Lookup a key.
    pub fn get(&self, key: &[u8]) -> Option<RowID> {
        self.latest_version(key)
            .and_then(PrimaryIndexVersion::visible_row_id)
    }

    /// Lookup the latest version record for a key, including tombstones.
    pub fn latest_version(&self, key: &[u8]) -> Option<PrimaryIndexVersion> {
        let shard = self.shard_for(key);
        let res = self.shards[shard].read().latest_version(key);
        let m = storage_metrics();
        if res.is_some() {
            m.inc_primary_index_hit();
        } else {
            m.inc_primary_index_miss();
        }
        res
    }

    /// Lookup the newest key version visible at `read_ts`.
    pub fn get_version_at(&self, key: &[u8], read_ts: u64) -> Option<PrimaryIndexVersion> {
        let shard = self.shard_for(key);
        let res = self.shards[shard].read().lookup_version_at(key, read_ts);
        let m = storage_metrics();
        if res.is_some() {
            m.inc_primary_index_hit();
        } else {
            m.inc_primary_index_miss();
        }
        res
    }

    /// Lookup a live row id visible at `read_ts`.
    pub fn get_at(&self, key: &[u8], read_ts: u64) -> Option<RowID> {
        self.get_version_at(key, read_ts)
            .and_then(PrimaryIndexVersion::visible_row_id)
    }

    /// Lookup a batch of keys in original order.
    pub fn multi_get<'a, I>(&self, keys: I) -> Vec<Option<RowID>>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let indexed: Vec<(usize, &'a [u8])> = keys.into_iter().enumerate().collect();
        if indexed.is_empty() {
            return Vec::new();
        }

        let mut buckets: Vec<Vec<(usize, &'a [u8])>> = vec![Vec::new(); self.shards.len()];
        for (idx, key) in indexed {
            buckets[self.shard_for(key)].push((idx, key));
        }

        let mut out = vec![None; buckets.iter().map(|b| b.len()).sum()];
        let metrics = storage_metrics();
        for (shard_idx, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let guard = self.shards[shard_idx].read();
            for (idx, key) in bucket {
                let value = guard.lookup(key);
                if value.is_some() {
                    metrics.inc_primary_index_hit();
                } else {
                    metrics.inc_primary_index_miss();
                }
                out[idx] = value;
            }
        }
        out
    }

    /// Lookup version records in original order, preserving tombstones.
    pub fn multi_get_versions_at<'a, I>(
        &self,
        keys: I,
        read_ts: u64,
    ) -> Vec<Option<PrimaryIndexVersion>>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let indexed: Vec<(usize, &'a [u8])> = keys.into_iter().enumerate().collect();
        if indexed.is_empty() {
            return Vec::new();
        }

        let mut buckets: Vec<Vec<(usize, &'a [u8])>> = vec![Vec::new(); self.shards.len()];
        for (idx, key) in indexed {
            buckets[self.shard_for(key)].push((idx, key));
        }

        let mut out = vec![None; buckets.iter().map(|b| b.len()).sum()];
        let metrics = storage_metrics();
        for (shard_idx, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let guard = self.shards[shard_idx].read();
            for (idx, key) in bucket {
                let value = guard.lookup_version_at(key, read_ts);
                if value.is_some() {
                    metrics.inc_primary_index_hit();
                } else {
                    metrics.inc_primary_index_miss();
                }
                out[idx] = value;
            }
        }
        out
    }

    /// Insert or replace a single key.
    pub fn upsert(&self, key: Vec<u8>, location: RowID) -> Option<RowID> {
        self.upsert_at(key, location, 0)
    }

    /// Insert or replace a committed key version.
    pub fn upsert_at(&self, key: Vec<u8>, location: RowID, commit_ts: u64) -> Option<RowID> {
        let shard_idx = self.shard_for(&key);
        let mut guard = self.shards[shard_idx].write();
        let (old, delta) = guard.insert_or_replace_at(key, location, commit_ts);
        self.adjust_memory(delta);
        if old.is_some() {
            storage_metrics().inc_primary_index_conflicts(1);
        }
        old
    }

    /// Remove a key.
    pub fn remove(&self, key: &[u8]) -> Option<RowID> {
        self.remove_at(key, 0)
    }

    /// Append a committed tombstone for a key and remove it from latest lookups.
    pub fn remove_at(&self, key: &[u8], commit_ts: u64) -> Option<RowID> {
        let shard_idx = self.shard_for(key);
        let mut guard = self.shards[shard_idx].write();
        let (removed, delta) = guard.remove_at(key, commit_ts);
        if delta != 0 {
            self.adjust_memory(delta);
        }
        removed
    }

    /// Batch upsert; returns number of processed items.
    pub fn batch_upsert<I>(&self, entries: I) -> usize
    where
        I: IntoIterator<Item = (Vec<u8>, RowID)>,
    {
        self.batch_upsert_at(entries, 0)
    }

    /// Batch upsert at a single commit timestamp.
    pub fn batch_upsert_at<I>(&self, entries: I, commit_ts: u64) -> usize
    where
        I: IntoIterator<Item = (Vec<u8>, RowID)>,
    {
        let mut buckets: Vec<Vec<(Vec<u8>, RowID)>> = vec![Vec::new(); self.shards.len()];
        let mut conflict_count = 0u64;

        for (k, v) in entries {
            let shard = self.shard_for(&k);
            buckets[shard].push((k, v));
        }

        let mut processed = 0usize;
        for (shard_idx, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let mut guard = self.shards[shard_idx].write();
            let (conflicts, count, shard_delta) =
                guard.batch_insert_or_replace_at(bucket, commit_ts);
            conflict_count += conflicts;
            processed += count;
            self.adjust_memory(shard_delta);
        }

        if conflict_count > 0 {
            storage_metrics().inc_primary_index_conflicts(conflict_count);
        }
        processed
    }

    /// Apply already encoded version records.
    pub fn batch_apply_versions<I>(&self, entries: I) -> usize
    where
        I: IntoIterator<Item = (Vec<u8>, PrimaryIndexVersion)>,
    {
        let mut buckets: Vec<Vec<(Vec<u8>, PrimaryIndexVersion)>> =
            vec![Vec::new(); self.shards.len()];

        for (k, v) in entries {
            let shard = self.shard_for(&k);
            buckets[shard].push((k, v));
        }

        let mut processed = 0usize;
        for (shard_idx, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let mut guard = self.shards[shard_idx].write();
            let (count, shard_delta) = guard.batch_apply_versions(bucket);
            processed += count;
            self.adjust_memory(shard_delta);
        }

        processed
    }

    /// Batch replace entries only if their current generation/version is not newer
    /// than the compaction snapshot that produced the replacement rows.
    /// Returns the subset of input entries that were successfully replaced.
    pub fn batch_try_replace<I>(
        &self,
        entries: I,
        mut generation_resolver: impl FnMut(RowID) -> Result<i64>,
        max_generation: i64,
    ) -> Result<Vec<(Vec<u8>, RowID)>>
    where
        I: IntoIterator<Item = (Vec<u8>, RowID)>,
    {
        let mut buckets: Vec<Vec<(Vec<u8>, RowID)>> = vec![Vec::new(); self.shards.len()];

        for (k, v) in entries {
            let shard = self.shard_for(&k);
            buckets[shard].push((k, v));
        }

        let mut successful = Vec::new();

        for (shard_idx, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let mut guard = self.shards[shard_idx].write();
            let mut shard_delta = 0isize;
            for (k, v) in bucket {
                let can_replace = guard
                    .lookup(&k)
                    .map(&mut generation_resolver)
                    .transpose()?
                    .is_some_and(|generation| generation <= max_generation);

                if can_replace {
                    let (_, delta) = guard.insert_or_replace(k.clone(), v);
                    shard_delta += delta;
                    successful.push((k, v));
                }
            }
            self.adjust_memory(shard_delta);
        }

        Ok(successful)
    }

    /// Build the index from a slice of key/RowID pairs (convenience wrapper).
    pub fn bulk_build(&self, pairs: &[(Vec<u8>, RowID)]) -> usize {
        self.batch_upsert(pairs.iter().cloned())
    }

    /// Bulk build from RowIDs with an external key loader.
    pub fn bulk_build_from_row_ids<F>(&self, row_ids: &[RowID], mut key_loader: F) -> Result<usize>
    where
        F: FnMut(&RowID) -> Result<Vec<u8>>,
    {
        let mut pairs = Vec::with_capacity(row_ids.len());
        for row_id in row_ids {
            let key = key_loader(row_id)?;
            pairs.push((key, *row_id));
        }
        Ok(self.batch_upsert(pairs))
    }

    /// Build from a Chunk using the provided serializer and row-id template.
    /// `row_offset_start` is added to the row ordinal within the chunk to form RowID::row_offset.
    pub fn build_from_chunk(
        &self,
        serializer: &PrimaryKeySerializer,
        chunk: &Chunk,
        rssid: u32,
        row_offset_start: u32,
    ) -> Result<usize> {
        let mut pairs = Vec::with_capacity(chunk.size());
        for (row_offset, row_idx) in (0..chunk.size()).enumerate() {
            let key = serializer.encode_row(chunk, row_idx)?;
            let row_id = RowID::new(rssid, row_offset_start + row_offset as u32);
            pairs.push((key, row_id));
        }
        Ok(self.batch_upsert(pairs))
    }

    /// Number of stored entries.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// Clear all entries and reset memory usage.
    pub fn clear(&self) {
        let mut total_usage = 0usize;
        for shard in &self.shards {
            let mut guard = shard.write();
            guard.clear();
            guard.memory_usage = guard.recalculate_memory_usage();
            total_usage += guard.memory_usage;
        }
        self.memory_usage.store(total_usage, Ordering::Release);
        storage_metrics().set_primary_index_memory(total_usage);
        self.backpressure_cv.notify_all();
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot all entries into a vector (unordered).
    pub fn snapshot(&self) -> Vec<(Vec<u8>, RowID)> {
        let mut out = Vec::with_capacity(self.len());
        for shard in &self.shards {
            let guard = shard.read();
            guard.snapshot_into(&mut out);
        }
        out
    }

    /// Snapshot all committed key versions, including tombstones.
    pub fn snapshot_versions(&self) -> Vec<(Vec<u8>, PrimaryIndexVersion)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            let guard = shard.read();
            guard.snapshot_versions_into(&mut out);
        }
        out
    }

    /// Returns true if any committed version for `key` is newer than `read_ts`.
    pub fn has_committed_after(&self, key: &[u8], read_ts: u64) -> bool {
        self.has_write_in_range(key, read_ts, u64::MAX)
    }

    /// Returns true if `key` was written in `(read_ts, commit_ts]`.
    pub fn has_write_in_range(&self, key: &[u8], read_ts: u64, commit_ts: u64) -> bool {
        self.first_write_in_range(key, read_ts, commit_ts).is_some()
    }

    /// Returns the earliest committed write for `key` in `(read_ts, commit_ts]`.
    pub fn first_write_in_range(
        &self,
        key: &[u8],
        read_ts: u64,
        commit_ts: u64,
    ) -> Option<PrimaryKeyWriteConflict> {
        if read_ts >= commit_ts {
            return None;
        }
        let shard = self.shard_for(key);
        self.shards[shard]
            .read()
            .first_write_for_key_in_range(key, read_ts, commit_ts)
    }

    /// Returns the earliest committed write for any key in `(read_ts, commit_ts]`.
    pub fn first_write_for_keys_in_range(
        &self,
        keys: &[Vec<u8>],
        read_ts: u64,
        commit_ts: u64,
    ) -> Option<PrimaryKeyWriteConflict> {
        if keys.is_empty() || read_ts >= commit_ts {
            return None;
        }

        let mut buckets: Vec<Vec<&[u8]>> = vec![Vec::new(); self.shards.len()];
        for key in keys {
            buckets[self.shard_for(key)].push(key.as_slice());
        }

        let mut best = None;
        for (shard_idx, keys) in buckets.into_iter().enumerate() {
            if keys.is_empty() {
                continue;
            }
            let guard = self.shards[shard_idx].read();
            for key in keys {
                if let Some(conflict) = guard.first_write_for_key_in_range(key, read_ts, commit_ts)
                {
                    select_earlier_conflict(&mut best, conflict);
                }
            }
        }
        best
    }

    /// Returns the earliest committed primary-key write in `(read_ts, commit_ts]`
    /// whose encoded key is within the inclusive byte bounds.
    pub fn first_key_range_write_in_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        read_ts: u64,
        commit_ts: u64,
    ) -> Option<PrimaryKeyWriteConflict> {
        if read_ts >= commit_ts {
            return None;
        }

        let mut best = None;
        for shard in &self.shards {
            if let Some(conflict) = shard
                .read()
                .first_write_for_key_range_in_range(lower, upper, read_ts, commit_ts)
            {
                select_earlier_conflict(&mut best, conflict);
            }
        }
        best
    }

    // ------------ internal helpers ------------

    fn shard_for(&self, key: &[u8]) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    fn adjust_memory(&self, delta: isize) {
        if delta == 0 {
            return;
        }
        let usage = if delta > 0 {
            self.memory_usage
                .fetch_add(delta as usize, Ordering::AcqRel)
                + delta as usize
        } else {
            self.memory_usage
                .fetch_sub((-delta) as usize, Ordering::AcqRel)
                - (-delta) as usize
        };

        storage_metrics().set_primary_index_memory(usage);

        if delta < 0 {
            self.backpressure_cv.notify_all();
            return;
        }

        let limit = self.memory_limit.load(Ordering::Acquire);
        if usage > limit {
            if let Some(cb) = self.on_exceed.lock().as_mut() {
                cb(usage);
            }
            self.apply_backpressure();
        }
    }

    fn apply_backpressure(&self) {
        let start = Instant::now();
        let mut guard = self.backpressure_lock.lock();
        while self.memory_usage.load(Ordering::Acquire) > self.memory_limit.load(Ordering::Acquire)
        {
            if start.elapsed() >= BACKPRESSURE_MAX_WAIT {
                break;
            }
            self.backpressure_cv
                .wait_for(&mut guard, BACKPRESSURE_WAIT_SLICE);
        }
    }
}

fn first_version_in_range(
    chain: &[PrimaryIndexVersion],
    read_ts: u64,
    commit_ts: u64,
) -> Option<PrimaryIndexVersion> {
    if read_ts >= commit_ts {
        return None;
    }
    let pos = chain.partition_point(|version| version.commit_ts <= read_ts);
    chain
        .get(pos)
        .copied()
        .filter(|version| version.commit_ts <= commit_ts)
}

fn key_in_bounds(key: &[u8], lower: Option<&[u8]>, upper: Option<&[u8]>) -> bool {
    lower.map_or(true, |lower| key >= lower) && upper.map_or(true, |upper| key <= upper)
}

fn select_earlier_conflict(
    slot: &mut Option<PrimaryKeyWriteConflict>,
    candidate: PrimaryKeyWriteConflict,
) {
    let should_replace = slot
        .as_ref()
        .map(|current| {
            candidate.version.commit_ts < current.version.commit_ts
                || (candidate.version.commit_ts == current.version.commit_ts
                    && candidate.key < current.key)
        })
        .unwrap_or(true);
    if should_replace {
        *slot = Some(candidate);
    }
}

/// Serializer for primary key columns.
#[derive(Debug, Clone)]
pub struct PrimaryKeySerializer {
    key_indices: Vec<usize>,
    key_types: Vec<LogicalType>,
}

impl PrimaryKeySerializer {
    /// Build from a schema reference; validates the schema is PRIMARY_KEYS and has key columns.
    pub fn from_schema(schema: &TabletSchema) -> Result<Self> {
        if schema.keys_type() != KeysType::PrimaryKeys {
            return Err(paro_error::invalid_input(
                "PrimaryKeySerializer requires PRIMARY_KEYS tablet",
            ));
        }
        let num_keys = schema.num_key_columns();
        if num_keys == 0 {
            return Err(paro_error::invalid_input(
                "PRIMARY_KEYS tablet must have at least one key column",
            ));
        }
        let mut key_indices = Vec::with_capacity(num_keys);
        let mut key_types = Vec::with_capacity(num_keys);
        for idx in 0..num_keys {
            let col = schema
                .column(idx)
                .ok_or_else(|| paro_error::invalid_input("Key column index out of range"))?;
            key_indices.push(idx);
            key_types.push(col.logical_type.clone());
        }
        Ok(Self {
            key_indices,
            key_types,
        })
    }

    /// Build from an `Arc` schema.
    pub fn from_schema_ref(schema: &TabletSchemaRef) -> Result<Self> {
        Self::from_schema(schema.as_ref())
    }

    /// Encode a single row from a chunk into deterministic bytes.
    pub fn encode_row(&self, chunk: &Chunk, row: usize) -> Result<Vec<u8>> {
        if chunk.column_count() <= *self.key_indices.iter().max().unwrap_or(&0) {
            return Err(paro_error::invalid_input(
                "Chunk does not contain all key columns",
            ));
        }

        let mut out = Vec::with_capacity(self.key_indices.len() * 16);
        for (idx, ty) in self.key_indices.iter().zip(self.key_types.iter()) {
            let vec = chunk
                .column(*idx)
                .ok_or_else(|| paro_error::invalid_input("Key column missing in chunk"))?;
            let value = vec.get_value(row);
            ComparableEncoder::encode_value(&value, ty, &mut out)?;
        }
        Ok(out)
    }

    /// Encode all rows from a chunk into deterministic key bytes.
    pub fn encode_chunk(&self, chunk: &Chunk) -> Result<Vec<Vec<u8>>> {
        if chunk.column_count() <= *self.key_indices.iter().max().unwrap_or(&0) {
            return Err(paro_error::invalid_input(
                "Chunk does not contain all key columns",
            ));
        }

        let key_vectors: Vec<_> = self
            .key_indices
            .iter()
            .map(|idx| {
                chunk
                    .column(*idx)
                    .cloned()
                    .ok_or_else(|| paro_error::invalid_input("Key column missing in chunk"))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut encoded = Vec::with_capacity(chunk.size());
        for row in 0..chunk.size() {
            let mut out = Vec::with_capacity(self.key_indices.len() * 16);
            for (vec, ty) in key_vectors.iter().zip(self.key_types.iter()) {
                let value = vec.get_value(row);
                ComparableEncoder::encode_value(&value, ty, &mut out)?;
            }
            encoded.push(out);
        }
        Ok(encoded)
    }

    /// Encode a slice of `Value` (mostly for tests) following the same layout.
    pub fn encode_values(&self, values: &[Value]) -> Result<Vec<u8>> {
        if values.len() != self.key_types.len() {
            return Err(paro_error::invalid_input(
                "Value count does not match key columns",
            ));
        }
        let mut out = Vec::with_capacity(values.len() * 16);
        for (val, ty) in values.iter().zip(self.key_types.iter()) {
            ComparableEncoder::encode_value(val, ty, &mut out)?;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::storage_metrics;
    use crate::test_utils::*;
    use paro_common::allocator::default_allocator;
    use paro_common::types::LogicalType;

    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::thread;

    fn sample_schema() -> TabletSchemaRef {
        let columns = vec![
            crate::tablet::tablet_schema::TabletColumn::key(0, "id", LogicalType::Integer),
            crate::tablet::tablet_schema::TabletColumn::new(1, "name", LogicalType::Varchar),
        ];
        Arc::new(TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap())
    }

    fn make_fixed_key(seed: u32) -> Vec<u8> {
        seed.to_be_bytes().repeat(2)
    }

    fn make_long_key(seed: u32) -> Vec<u8> {
        format!("primary-index-long-key-{seed:08x}-payload").into_bytes()
    }

    fn assert_memory_usage_exact(idx: &PrimaryIndex) {
        let mut shard_total = 0usize;
        for shard in &idx.shards {
            let guard = shard.read();
            assert_eq!(guard.memory_usage, guard.recalculate_memory_usage());
            shard_total += guard.memory_usage;
        }
        assert_eq!(shard_total, idx.memory_usage_bytes());
    }

    #[test]
    fn basic_put_get_remove() {
        let idx = PrimaryIndex::new();
        let key = b"k1".to_vec();
        let loc = RowID::new(1, 3);
        idx.upsert(key.clone(), loc);
        assert_eq!(idx.get(&key), Some(loc));
        assert_eq!(idx.len(), 1);
        let removed = idx.remove(&key);
        assert_eq!(removed, Some(loc));
        assert!(idx.get(&key).is_none());
    }

    #[test]
    fn batch_upsert_and_snapshot() {
        let idx = PrimaryIndex::with_options(4, 1024);
        let entries = vec![
            (b"a".to_vec(), RowID::new(1, 1)),
            (b"b".to_vec(), RowID::new(1, 2)),
            (b"c".to_vec(), RowID::new(2, 0)),
        ];
        let added = idx.batch_upsert(entries.clone());
        assert_eq!(added, 3);
        assert_eq!(idx.len(), 3);
        let snap = idx.snapshot();
        assert_eq!(snap.len(), 3);
        assert_eq!(idx.get(b"b"), Some(RowID::new(1, 2)));
    }

    #[test]
    fn contains_and_multi_get_work() {
        let idx = PrimaryIndex::with_options(2, 1024 * 1024);
        let keys = vec![b"a".to_vec(), b"bb".to_vec(), make_long_key(7)];
        for (i, key) in keys.iter().enumerate() {
            idx.upsert(key.clone(), RowID::new(3, i as u32));
        }

        assert!(idx.contains(&keys[0]));
        assert!(!idx.contains(b"missing"));

        let query = vec![
            keys[2].as_slice(),
            b"missing".as_slice(),
            keys[0].as_slice(),
        ];
        let got = idx.multi_get(query);
        assert_eq!(got[0], Some(RowID::new(3, 2)));
        assert_eq!(got[1], None);
        assert_eq!(got[2], Some(RowID::new(3, 0)));
    }

    #[test]
    fn write_conflict_window_finds_point_and_range_versions() {
        let idx = PrimaryIndex::with_options(4, 1024 * 1024);
        idx.upsert_at(b"a".to_vec(), RowID::new(1, 1), 10);
        idx.upsert_at(b"b".to_vec(), RowID::new(1, 2), 20);
        idx.remove_at(b"b", 30);

        assert!(!idx.has_write_in_range(b"a", 10, 30));
        assert!(idx.has_write_in_range(b"a", 9, 10));

        let conflict = idx.first_write_in_range(b"b", 19, 30).unwrap();
        assert_eq!(conflict.commit_ts(), 20);
        assert_eq!(conflict.version.row_id, RowID::new(1, 2));

        let range_conflict = idx
            .first_key_range_write_in_range(Some(b"b"), Some(b"z"), 20, 30)
            .unwrap();
        assert_eq!(range_conflict.key, b"b".to_vec());
        assert!(range_conflict.is_tombstone());
        assert_eq!(range_conflict.commit_ts(), 30);
    }

    #[test]
    fn batch_write_conflict_query_groups_by_shard() {
        let idx = PrimaryIndex::with_options(8, 1024 * 1024);
        idx.batch_upsert_at(
            [
                (b"k1".to_vec(), RowID::new(1, 1)),
                (b"k2".to_vec(), RowID::new(1, 2)),
            ],
            40,
        );

        let conflict = idx
            .first_write_for_keys_in_range(&[b"k0".to_vec(), b"k2".to_vec()], 39, 99)
            .unwrap();
        assert_eq!(conflict.key, b"k2".to_vec());
        assert_eq!(conflict.commit_ts(), 40);
    }

    #[test]
    fn fixed_length_bucket_uses_less_memory_than_long_keys() {
        let fixed = PrimaryIndex::with_options(4, usize::MAX / 2);
        let long = PrimaryIndex::with_options(4, usize::MAX / 2);

        for i in 0..1024u32 {
            fixed.upsert(make_fixed_key(i), RowID::new(1, i));
            long.upsert(make_long_key(i), RowID::new(1, i));
        }

        assert!(fixed.memory_usage_bytes() < long.memory_usage_bytes());
    }

    #[test]
    fn memory_limit_callback_triggers() {
        let idx = Arc::new(PrimaryIndex::with_options(1, 16)); // tiny limit
        let fired = Arc::new(AtomicUsize::new(0));
        let fired_clone = fired.clone();
        idx.register_mem_exceed_callback(move |_| {
            fired_clone.fetch_add(1, Ordering::Relaxed);
        });

        let key = vec![0u8; 32];
        let idx_writer = idx.clone();
        let key_writer = key.clone();
        let handle = thread::spawn(move || {
            idx_writer.upsert(key_writer, RowID::new(1, 1));
        });

        while fired.load(Ordering::Relaxed) == 0 {
            thread::yield_now();
        }
        idx.remove(&key);
        handle.join().unwrap();
        assert!(fired.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn bounded_backpressure_waits_for_memory_relief() {
        let idx = Arc::new(PrimaryIndex::with_options(1, 32));
        let key = vec![9u8; 64];
        let idx_writer = idx.clone();
        let key_writer = key.clone();
        let handle = thread::spawn(move || {
            let start = Instant::now();
            idx_writer.upsert(key_writer, RowID::new(2, 7));
            start.elapsed()
        });

        while idx.memory_usage_bytes() <= 32 {
            thread::yield_now();
        }
        thread::sleep(Duration::from_millis(15));
        idx.remove(&key);

        let elapsed = handle.join().unwrap();
        assert!(elapsed >= Duration::from_millis(10));
    }

    #[test]
    fn single_upsert_memory_tracking_stays_incremental() {
        let idx = PrimaryIndex::with_options(4, usize::MAX / 2);

        for i in 0..4096u32 {
            idx.upsert(make_long_key(i), RowID::new(1, i));
        }

        let snapshot_usage = idx.memory_usage_bytes();
        assert!(snapshot_usage > 0);

        for i in 0..4096u32 {
            assert!(idx.remove(&make_long_key(i)).is_some());
        }

        assert!(idx.memory_usage_bytes() < snapshot_usage);
    }

    #[test]
    fn memory_tracking_matches_full_recalculation_after_mixed_operations() {
        let idx = PrimaryIndex::with_options(4, usize::MAX / 2);

        assert_memory_usage_exact(&idx);

        for i in 0..512u32 {
            idx.upsert(make_fixed_key(i), RowID::new(1, i));
            idx.upsert(make_long_key(i), RowID::new(2, i));
        }
        assert_memory_usage_exact(&idx);

        for i in 0..128u32 {
            idx.upsert(make_fixed_key(i), RowID::new(9, i));
            idx.upsert(make_long_key(i), RowID::new(10, i));
        }
        assert_memory_usage_exact(&idx);

        for i in 128..256u32 {
            idx.remove(&make_fixed_key(i));
            idx.remove(&make_long_key(i));
        }
        assert_memory_usage_exact(&idx);

        idx.clear();
        assert_memory_usage_exact(&idx);
    }

    #[test]
    fn serializer_encodes_row() {
        let schema = sample_schema();
        let serializer = PrimaryKeySerializer::from_schema_ref(&schema).unwrap();

        let alloc = Arc::new(default_allocator());
        let id_vec = paro_common::test_utils::test_i32_vector_with_allocator(&[1], alloc.clone());
        let name_vec =
            paro_common::test_utils::test_string_vector_with_allocator(&["alice"], alloc);
        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(id_vec), Arc::new(name_vec)]);

        let encoded = serializer.encode_row(&chunk, 0).unwrap();
        let expected: Vec<u8> = [0x80u8, 0, 0, 1].to_vec();
        assert_eq!(encoded, expected);
    }

    #[test]
    fn serializer_encode_chunk_matches_row_encoding() {
        let schema = sample_schema();
        let serializer = PrimaryKeySerializer::from_schema_ref(&schema).unwrap();

        let alloc = Arc::new(default_allocator());
        let id_vec =
            paro_common::test_utils::test_i32_vector_with_allocator(&[1, 2, 3], alloc.clone());
        let name_vec =
            paro_common::test_utils::test_string_vector_with_allocator(&["a", "b", "c"], alloc);
        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(id_vec), Arc::new(name_vec)]);

        let batch = serializer.encode_chunk(&chunk).unwrap();
        assert_eq!(batch.len(), 3);
        for (row, encoded) in batch.iter().enumerate() {
            assert_eq!(encoded, &serializer.encode_row(&chunk, row).unwrap());
        }
    }

    #[test]
    fn build_from_chunk_sets_locations() {
        let schema = sample_schema();
        let serializer = PrimaryKeySerializer::from_schema_ref(&schema).unwrap();
        let alloc = Arc::new(default_allocator());
        let id_vec =
            paro_common::test_utils::test_i32_vector_with_allocator(&[10, 20], alloc.clone());
        let name_vec =
            paro_common::test_utils::test_string_vector_with_allocator(&["a", "b"], alloc);
        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(id_vec), Arc::new(name_vec)]);

        let idx = PrimaryIndex::new();
        let added = idx.build_from_chunk(&serializer, &chunk, 7, 0).unwrap();
        assert_eq!(added, 2);
        assert_eq!(
            idx.get(&serializer.encode_row(&chunk, 1).unwrap()),
            Some(RowID::new(7, 1))
        );
    }

    #[test]
    fn metrics_record_hits_conflicts_and_memory() {
        let m = storage_metrics();
        m.reset_for_tests();

        let idx = PrimaryIndex::with_options(2, 1024);
        let key = b"k1".to_vec();
        let loc = RowID::new(1, 0);
        assert!(idx.get(&key).is_none()); // miss
        idx.upsert(key.clone(), loc);
        assert_eq!(idx.get(&key), Some(loc)); // hit
        idx.upsert(key.clone(), RowID::new(2, 0)); // conflict

        let snap = m.snapshot();
        assert!(snap.primary_index_hits >= 1);
        assert!(snap.primary_index_misses >= 1);
        assert!(snap.primary_index_conflicts >= 1);
        assert!(idx.memory_usage_bytes() > 0);
    }
}
