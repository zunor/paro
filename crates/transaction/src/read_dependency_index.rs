// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Serializable read dependency storage.

use crate::cache::CachePadded;
use crate::sync::Mutex;
use crate::{FrozenReadSet, LockResource, ReadDependency, ReadTs, TableId, TxnId};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

pub const DEFAULT_READ_DEPENDENCY_SHARDS: usize = 64;
pub const DEFAULT_PER_TXN_READ_SET_BUDGET_BYTES: usize = 64 * 1024;
pub const DEFAULT_GLOBAL_READ_SET_BUDGET_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadDependencyIndexOptions {
    pub shard_count: usize,
    pub initial_txns_per_shard: usize,
    pub per_txn_budget_bytes: usize,
    pub global_budget_bytes: usize,
}

impl Default for ReadDependencyIndexOptions {
    fn default() -> Self {
        Self {
            shard_count: DEFAULT_READ_DEPENDENCY_SHARDS,
            initial_txns_per_shard: 64,
            per_txn_budget_bytes: DEFAULT_PER_TXN_READ_SET_BUDGET_BYTES,
            global_budget_bytes: DEFAULT_GLOBAL_READ_SET_BUDGET_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadDependencyIndexStats {
    pub shard_count: usize,
    pub txn_count: usize,
    pub dependency_count: usize,
    pub record_count: u64,
    pub coarsened_txn_count: usize,
    pub analytical_scan_marker_count: usize,
    pub memory_usage_bytes: usize,
    pub global_budget_bytes: usize,
    pub coarsen_count: u64,
    pub global_coarsen_count: u64,
    pub state_epoch: u64,
}

/// Lock-free counters consumed by foreground statement telemetry.
///
/// Exact dependency cardinalities remain available through `stats()` for
/// explicit diagnostics.  They require a full shard walk and therefore do
/// not belong on the statement-context construction path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadDependencyTelemetryCounters {
    pub record_count: u64,
    pub coarsen_count: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadDependencyIndexMark {
    pub dependency_count: usize,
    pub coarsening_epoch: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadDependencyRollback {
    pub removed_dependencies: usize,
    pub preserved_due_to_coarsening: bool,
}

#[derive(Debug)]
pub struct ReadDependencyIndex {
    shards: Box<[CachePadded<Mutex<ReadDependencyShard>>]>,
    per_txn_budget_bytes: usize,
    global_budget_bytes: usize,
    memory_usage_bytes: AtomicUsize,
    record_count: AtomicU64,
    coarsen_count: AtomicU64,
    global_coarsen_count: AtomicU64,
    state_epoch: AtomicU64,
}

#[derive(Debug)]
struct ReadDependencyShard {
    txns: HashMap<TxnId, ActiveReadSet>,
}

#[derive(Debug, Clone)]
struct ActiveReadSet {
    read_ts: ReadTs,
    dependencies: Vec<ReadDependency>,
    seen: HashSet<ReadDependency>,
    memory_usage_bytes: usize,
    coarsened: bool,
    coarsening_epoch: u64,
    last_access_epoch: u64,
    ssi: SsiTxnState,
}

#[derive(Debug, Clone)]
pub struct IndexedReadTracker {
    index: Arc<ReadDependencyIndex>,
    txn_id: TxnId,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SsiTxnState {
    pub rw_conflict_in: bool,
    pub rw_conflict_out: bool,
    pub dangerous_structure: bool,
    pub coarse_scan_marker_conflict: bool,
    pub ssi_state_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveReadConflict {
    pub txn_id: TxnId,
    pub read_ts: ReadTs,
    pub dependency: ReadDependency,
    pub write: LockResource,
    pub state_after: SsiTxnState,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActiveWriteConflictEffects {
    pub matched_txn_count: usize,
    pub writer_has_conflict_in: bool,
    pub coarse_scan_marker_conflict: bool,
    pub ssi_effect_epoch: u64,
    pub first_conflict: Option<ActiveReadConflict>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CoarsenCandidate {
    shard_index: usize,
    txn_id: TxnId,
    read_ts: ReadTs,
    last_access_epoch: u64,
}

impl ReadDependencyIndex {
    pub fn new(options: ReadDependencyIndexOptions) -> Self {
        assert!(
            options.shard_count > 0,
            "read dependency index needs shards"
        );
        assert!(
            options.per_txn_budget_bytes > 0,
            "per-txn read set budget must be non-zero"
        );
        assert!(
            options.global_budget_bytes > 0,
            "global read set budget must be non-zero"
        );
        let shards = (0..options.shard_count)
            .map(|_| {
                CachePadded::new(Mutex::new(ReadDependencyShard::with_capacity(
                    options.initial_txns_per_shard,
                )))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            per_txn_budget_bytes: options.per_txn_budget_bytes,
            global_budget_bytes: options.global_budget_bytes,
            memory_usage_bytes: AtomicUsize::new(0),
            record_count: AtomicU64::new(0),
            coarsen_count: AtomicU64::new(0),
            global_coarsen_count: AtomicU64::new(0),
            state_epoch: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn with_shards(shard_count: usize) -> Self {
        Self::new(ReadDependencyIndexOptions {
            shard_count,
            ..ReadDependencyIndexOptions::default()
        })
    }

    pub fn tracker(index: Arc<Self>, txn_id: TxnId, read_ts: ReadTs) -> Arc<IndexedReadTracker> {
        index.register_transaction(txn_id, read_ts);
        Arc::new(IndexedReadTracker { index, txn_id })
    }

    pub fn register_transaction(&self, txn_id: TxnId, read_ts: ReadTs) {
        let shard_index = self.shard_index(txn_id);
        let mut shard = self.shards[shard_index].0.lock();
        if let Some(read_set) = shard.txns.get_mut(&txn_id) {
            read_set.read_ts = read_ts;
            read_set.last_access_epoch = self.state_epoch.load(Ordering::Acquire);
            return;
        }
        let read_set =
            ActiveReadSet::new(txn_id, read_ts, self.state_epoch.load(Ordering::Acquire));
        self.memory_usage_bytes
            .fetch_add(read_set.memory_usage_bytes, Ordering::AcqRel);
        shard.txns.insert(txn_id, read_set);
        self.bump_epoch();
    }

    pub fn release_transaction(&self, txn_id: TxnId) -> Option<FrozenReadSet> {
        let shard_index = self.shard_index(txn_id);
        let mut shard = self.shards[shard_index].0.lock();
        let read_set = shard.txns.remove(&txn_id)?;
        self.memory_usage_bytes
            .fetch_sub(read_set.memory_usage_bytes, Ordering::AcqRel);
        self.bump_epoch();
        Some(read_set.frozen_read_set())
    }

    #[inline]
    pub fn record(&self, txn_id: TxnId, dependency: ReadDependency) -> usize {
        self.record_batch(txn_id, [dependency])
    }

    pub fn record_batch(
        &self,
        txn_id: TxnId,
        dependencies: impl IntoIterator<Item = ReadDependency>,
    ) -> usize {
        let mut dependencies = dependencies
            .into_iter()
            .map(normalize_dependency)
            .collect::<Vec<_>>();
        if dependencies.is_empty() {
            return 0;
        }

        dependencies.sort_by_key(read_dependency_order_key);
        dependencies.dedup();

        let epoch = self.state_epoch.load(Ordering::Acquire).saturating_add(1);
        let inserted = {
            let shard_index = self.shard_index(txn_id);
            let mut shard = self.shards[shard_index].0.lock();
            let mut created = false;
            let read_set = match shard.txns.entry(txn_id) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    created = true;
                    entry.insert(ActiveReadSet::new(txn_id, ReadTs::new(0), epoch))
                }
            };
            let old_usage = if created {
                0
            } else {
                read_set.memory_usage_bytes
            };
            let was_coarsened = read_set.coarsened;
            let inserted = read_set.record_batch(dependencies, self.per_txn_budget_bytes, epoch);
            let new_usage = read_set.memory_usage_bytes;
            apply_memory_delta(&self.memory_usage_bytes, old_usage, new_usage);
            if !was_coarsened && read_set.coarsened {
                self.coarsen_count.fetch_add(1, Ordering::AcqRel);
            }
            if inserted > 0 {
                read_set.last_access_epoch = epoch;
                self.record_count
                    .fetch_add(inserted as u64, Ordering::AcqRel);
                self.bump_epoch();
            }
            inserted
        };
        if inserted > 0 {
            self.enforce_global_budget();
        }
        inserted
    }

    pub fn frozen_read_set(&self, txn_id: TxnId) -> FrozenReadSet {
        let shard_index = self.shard_index(txn_id);
        self.shards[shard_index]
            .0
            .lock()
            .txns
            .get(&txn_id)
            .map(ActiveReadSet::frozen_read_set)
            .unwrap_or_else(FrozenReadSet::empty)
    }

    pub fn dependencies(&self, txn_id: TxnId) -> Vec<ReadDependency> {
        let shard_index = self.shard_index(txn_id);
        self.shards[shard_index]
            .0
            .lock()
            .txns
            .get(&txn_id)
            .map(|read_set| read_set.dependencies.clone())
            .unwrap_or_default()
    }

    pub fn ssi_state(&self, txn_id: TxnId) -> SsiTxnState {
        let shard_index = self.shard_index(txn_id);
        self.shards[shard_index]
            .0
            .lock()
            .txns
            .get(&txn_id)
            .map(|read_set| read_set.ssi)
            .unwrap_or_default()
    }

    pub fn mark_txn_conflict_in(&self, txn_id: TxnId) -> SsiTxnState {
        self.mark_txn_state(txn_id, |state| state.rw_conflict_in = true)
    }

    pub fn mark_txn_conflict_out(&self, txn_id: TxnId) -> SsiTxnState {
        self.mark_txn_state(txn_id, |state| state.rw_conflict_out = true)
    }

    pub fn mark_txn_dangerous(&self, txn_id: TxnId) -> SsiTxnState {
        self.mark_txn_state(txn_id, |state| state.dangerous_structure = true)
    }

    pub fn mark_write_conflicts(
        &self,
        writer_txn_id: TxnId,
        writes: &[LockResource],
    ) -> ActiveWriteConflictEffects {
        if writes.is_empty() {
            return ActiveWriteConflictEffects::default();
        }

        let mut effects = ActiveWriteConflictEffects::default();
        for shard in self.shards.iter() {
            let mut shard = shard.0.lock();
            for (txn_id, read_set) in shard.txns.iter_mut() {
                if *txn_id == writer_txn_id {
                    continue;
                }
                let Some((dependency, write)) = read_set.first_conflicting_read(writes) else {
                    continue;
                };

                let epoch = self.bump_epoch();
                read_set.ssi.rw_conflict_out = true;
                if read_set.ssi.rw_conflict_in {
                    read_set.ssi.dangerous_structure = true;
                }
                let coarse_scan_marker_conflict = dependency.is_coarse_scan_marker();
                read_set.ssi.coarse_scan_marker_conflict |= coarse_scan_marker_conflict;
                read_set.ssi.ssi_state_epoch = epoch;

                effects.matched_txn_count += 1;
                effects.writer_has_conflict_in = true;
                effects.coarse_scan_marker_conflict |= coarse_scan_marker_conflict;
                effects.ssi_effect_epoch = effects.ssi_effect_epoch.max(epoch);
                let conflict = ActiveReadConflict {
                    txn_id: *txn_id,
                    read_ts: read_set.read_ts,
                    dependency,
                    write,
                    state_after: read_set.ssi,
                };
                select_earlier_active_conflict(&mut effects.first_conflict, conflict);
            }
        }
        effects
    }

    #[inline]
    pub fn state_epoch(&self) -> u64 {
        self.state_epoch.load(Ordering::Acquire)
    }

    pub fn stats(&self) -> ReadDependencyIndexStats {
        let mut txn_count = 0;
        let mut dependency_count = 0;
        let mut coarsened_txn_count = 0;
        let mut analytical_scan_marker_count = 0;
        for shard in self.shards.iter() {
            let shard = shard.0.lock();
            txn_count += shard.txns.len();
            for read_set in shard.txns.values() {
                dependency_count += read_set.dependencies.len();
                coarsened_txn_count += usize::from(read_set.coarsened);
                analytical_scan_marker_count += read_set
                    .dependencies
                    .iter()
                    .filter(|dependency| dependency.is_coarse_scan_marker())
                    .count();
            }
        }
        ReadDependencyIndexStats {
            shard_count: self.shards.len(),
            txn_count,
            dependency_count,
            record_count: self.record_count.load(Ordering::Acquire),
            coarsened_txn_count,
            analytical_scan_marker_count,
            memory_usage_bytes: self.memory_usage_bytes.load(Ordering::Acquire),
            global_budget_bytes: self.global_budget_bytes,
            coarsen_count: self.coarsen_count.load(Ordering::Acquire),
            global_coarsen_count: self.global_coarsen_count.load(Ordering::Acquire),
            state_epoch: self.state_epoch(),
        }
    }

    #[inline]
    pub fn telemetry_counters(&self) -> ReadDependencyTelemetryCounters {
        ReadDependencyTelemetryCounters {
            record_count: self.record_count.load(Ordering::Acquire),
            coarsen_count: self.coarsen_count.load(Ordering::Acquire),
        }
    }

    #[inline]
    fn shard_index(&self, txn_id: TxnId) -> usize {
        hash_to_index(&txn_id, self.shards.len())
    }

    #[inline]
    fn bump_epoch(&self) -> u64 {
        self.state_epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn mark_savepoint(&self, txn_id: TxnId) -> ReadDependencyIndexMark {
        let shard_index = self.shard_index(txn_id);
        self.shards[shard_index]
            .0
            .lock()
            .txns
            .get(&txn_id)
            .map(ActiveReadSet::mark_savepoint)
            .unwrap_or_default()
    }

    pub fn rollback_to_savepoint(
        &self,
        txn_id: TxnId,
        mark: ReadDependencyIndexMark,
    ) -> ReadDependencyRollback {
        let shard_index = self.shard_index(txn_id);
        let mut shard = self.shards[shard_index].0.lock();
        let Some(read_set) = shard.txns.get_mut(&txn_id) else {
            return ReadDependencyRollback::default();
        };
        let old_usage = read_set.memory_usage_bytes;
        let rollback = read_set.rollback_to_savepoint(mark);
        if rollback.removed_dependencies > 0 {
            apply_memory_delta(
                &self.memory_usage_bytes,
                old_usage,
                read_set.memory_usage_bytes,
            );
            read_set.last_access_epoch = self.bump_epoch();
        }
        rollback
    }

    fn mark_txn_state(&self, txn_id: TxnId, mutate: impl FnOnce(&mut SsiTxnState)) -> SsiTxnState {
        let shard_index = self.shard_index(txn_id);
        let mut shard = self.shards[shard_index].0.lock();
        let Some(read_set) = shard.txns.get_mut(&txn_id) else {
            return SsiTxnState::default();
        };
        mutate(&mut read_set.ssi);
        read_set.ssi.ssi_state_epoch = self.bump_epoch();
        read_set.ssi
    }

    pub fn mark_txn_coarse_scan_conflict(&self, txn_id: TxnId) -> SsiTxnState {
        self.mark_txn_state(txn_id, |state| state.coarse_scan_marker_conflict = true)
    }

    fn enforce_global_budget(&self) {
        let mut stale_candidates = 0usize;
        while self.memory_usage_bytes.load(Ordering::Acquire) > self.global_budget_bytes {
            let Some(candidate) = self.oldest_coarsenable_read_set() else {
                break;
            };
            if !self.coarsen_transaction(candidate) {
                stale_candidates = stale_candidates.saturating_add(1);
                if stale_candidates > self.shards.len() {
                    break;
                }
                continue;
            }
            stale_candidates = 0;
        }
    }

    fn oldest_coarsenable_read_set(&self) -> Option<CoarsenCandidate> {
        let mut best = None;
        for (shard_index, shard) in self.shards.iter().enumerate() {
            let shard = shard.0.lock();
            for (txn_id, read_set) in &shard.txns {
                if read_set.coarsened {
                    continue;
                }
                let candidate = CoarsenCandidate {
                    shard_index,
                    txn_id: *txn_id,
                    read_ts: read_set.read_ts,
                    last_access_epoch: read_set.last_access_epoch,
                };
                if coarsen_candidate_precedes(candidate, best) {
                    best = Some(candidate);
                }
            }
        }
        best
    }

    fn coarsen_transaction(&self, candidate: CoarsenCandidate) -> bool {
        let mut shard = self.shards[candidate.shard_index].0.lock();
        let Some(read_set) = shard.txns.get_mut(&candidate.txn_id) else {
            return false;
        };
        if read_set.coarsened {
            return false;
        }
        let old_usage = read_set.memory_usage_bytes;
        let epoch = self.bump_epoch();
        read_set.coarsen_to_table_level(epoch);
        apply_memory_delta(
            &self.memory_usage_bytes,
            old_usage,
            read_set.memory_usage_bytes,
        );
        self.global_coarsen_count.fetch_add(1, Ordering::AcqRel);
        self.coarsen_count.fetch_add(1, Ordering::AcqRel);
        true
    }
}

impl Default for ReadDependencyIndex {
    fn default() -> Self {
        Self::new(ReadDependencyIndexOptions::default())
    }
}

impl IndexedReadTracker {
    #[inline]
    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    #[inline]
    pub fn index(&self) -> &Arc<ReadDependencyIndex> {
        &self.index
    }

    #[inline]
    pub fn record(&self, dependency: ReadDependency) {
        self.index.record(self.txn_id, dependency);
    }

    #[inline]
    pub fn record_batch(&self, dependencies: impl IntoIterator<Item = ReadDependency>) -> usize {
        self.index.record_batch(self.txn_id, dependencies)
    }

    #[inline]
    pub fn frozen_read_set(&self) -> FrozenReadSet {
        self.index.frozen_read_set(self.txn_id)
    }

    #[inline]
    pub fn dependencies(&self) -> Vec<ReadDependency> {
        self.index.dependencies(self.txn_id)
    }

    #[inline]
    pub fn mark_savepoint(&self) -> ReadDependencyIndexMark {
        self.index.mark_savepoint(self.txn_id)
    }

    #[inline]
    pub fn rollback_to_savepoint(&self, mark: ReadDependencyIndexMark) -> ReadDependencyRollback {
        self.index.rollback_to_savepoint(self.txn_id, mark)
    }
}

impl ReadDependencyShard {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            txns: HashMap::with_capacity(capacity),
        }
    }
}

impl ActiveReadSet {
    fn new(_txn_id: TxnId, read_ts: ReadTs, last_access_epoch: u64) -> Self {
        Self {
            read_ts,
            dependencies: Vec::new(),
            seen: HashSet::new(),
            memory_usage_bytes: active_read_set_base_bytes(),
            coarsened: false,
            coarsening_epoch: 0,
            last_access_epoch,
            ssi: SsiTxnState::default(),
        }
    }

    fn record_batch(
        &mut self,
        dependencies: Vec<ReadDependency>,
        budget_bytes: usize,
        epoch: u64,
    ) -> usize {
        let mut inserted = 0;
        for dependency in dependencies {
            let dependency = if self.coarsened {
                dependency.table_marker().unwrap_or(dependency)
            } else {
                dependency
            };
            if self.seen.contains(&dependency) {
                continue;
            }
            if !self.coarsened
                && self
                    .memory_usage_bytes
                    .saturating_add(dependency.estimated_bytes())
                    > budget_bytes
            {
                self.coarsen_to_table_level(epoch);
            }
            let dependency = if self.coarsened {
                dependency.table_marker().unwrap_or(dependency)
            } else {
                dependency
            };
            if self.insert_dependency(dependency) {
                inserted += 1;
            }
        }
        inserted
    }

    fn insert_dependency(&mut self, dependency: ReadDependency) -> bool {
        if !self.seen.insert(dependency.clone()) {
            return false;
        }
        self.memory_usage_bytes = self
            .memory_usage_bytes
            .saturating_add(dependency.estimated_bytes());
        self.dependencies.push(dependency);
        true
    }

    fn mark_savepoint(&self) -> ReadDependencyIndexMark {
        ReadDependencyIndexMark {
            dependency_count: self.dependencies.len(),
            coarsening_epoch: self.coarsening_epoch,
        }
    }

    fn rollback_to_savepoint(&mut self, mark: ReadDependencyIndexMark) -> ReadDependencyRollback {
        if self.coarsening_epoch != mark.coarsening_epoch {
            return ReadDependencyRollback {
                removed_dependencies: 0,
                preserved_due_to_coarsening: true,
            };
        }
        if mark.dependency_count >= self.dependencies.len() {
            return ReadDependencyRollback::default();
        }

        let old_len = self.dependencies.len();
        self.dependencies.truncate(mark.dependency_count);
        self.rebuild_seen_and_usage();
        ReadDependencyRollback {
            removed_dependencies: old_len - mark.dependency_count,
            preserved_due_to_coarsening: false,
        }
    }

    fn rebuild_seen_and_usage(&mut self) {
        let mut seen = HashSet::with_capacity(self.dependencies.len());
        let mut usage = active_read_set_base_bytes();
        self.dependencies.retain(|dependency| {
            if !seen.insert(dependency.clone()) {
                return false;
            }
            usage = usage.saturating_add(dependency.estimated_bytes());
            true
        });
        self.seen = seen;
        self.memory_usage_bytes = usage;
    }

    fn coarsen_to_table_level(&mut self, epoch: u64) {
        let mut compacted = Vec::new();
        let mut seen = HashSet::new();
        let mut usage = active_read_set_base_bytes();
        for dependency in self.dependencies.drain(..) {
            let dependency = dependency.table_marker().unwrap_or(dependency);
            if seen.insert(dependency.clone()) {
                usage = usage.saturating_add(dependency.estimated_bytes());
                compacted.push(dependency);
            }
        }
        self.dependencies = compacted;
        self.seen = seen;
        self.memory_usage_bytes = usage;
        self.coarsened = true;
        self.coarsening_epoch = epoch;
    }

    fn frozen_read_set(&self) -> FrozenReadSet {
        FrozenReadSet::from_dependencies_with_coarsening(self.dependencies.clone(), self.coarsened)
    }

    fn first_conflicting_read(
        &self,
        writes: &[LockResource],
    ) -> Option<(ReadDependency, LockResource)> {
        for dependency in &self.dependencies {
            for write in writes {
                if dependency.conflicts_with_write(write) {
                    return Some((dependency.clone(), write.clone()));
                }
            }
        }
        None
    }
}

pub(crate) fn normalize_dependency(dependency: ReadDependency) -> ReadDependency {
    match dependency {
        ReadDependency::Row { table_id, .. } => ReadDependency::Table { table_id },
        other => other,
    }
}

pub(crate) fn compact_read_dependencies(
    dependencies: impl IntoIterator<Item = ReadDependency>,
) -> (Vec<ReadDependency>, bool) {
    let mut compacted = Vec::new();
    let mut seen = HashSet::new();
    let mut coarsened = false;
    for dependency in dependencies {
        coarsened |= matches!(dependency, ReadDependency::Row { .. });
        let normalized = normalize_dependency(dependency);
        if seen.insert(normalized.clone()) {
            compacted.push(normalized);
        }
    }
    compacted.sort_by_key(read_dependency_order_key);
    (compacted, coarsened)
}

fn read_dependency_order_key(dependency: &ReadDependency) -> (u8, u64, u64, u64, u64) {
    match dependency {
        ReadDependency::Table { table_id } => (0, table_id.into_raw(), 0, 0, 0),
        ReadDependency::Tablet {
            table_id,
            tablet_id,
            ..
        } => (1, table_id.into_raw(), *tablet_id, 0, 0),
        ReadDependency::Rowset {
            table_id,
            tablet_id,
            rowset_id,
            ..
        } => (2, table_id.into_raw(), *tablet_id, *rowset_id, 0),
        ReadDependency::KeyRange {
            table_id,
            start_hash,
            end_hash,
        } => (3, table_id.into_raw(), *start_hash, *end_hash, 0),
        ReadDependency::Predicate {
            table_id,
            predicate_hash,
        } => (4, table_id.into_raw(), *predicate_hash, 0, 0),
        ReadDependency::AnalyticalScan { table_id } => (5, table_id.into_raw(), 0, 0, 0),
        ReadDependency::Generation {
            resource_key,
            generation,
        } => {
            let table = resource_key.table_id().map(TableId::into_raw).unwrap_or(0);
            (6, table, *generation, 0, 0)
        }
        ReadDependency::Row { table_id, row_id } => (7, table_id.into_raw(), *row_id, 0, 0),
    }
}

fn coarsen_candidate_precedes(
    candidate: CoarsenCandidate,
    current: Option<CoarsenCandidate>,
) -> bool {
    current
        .map(|current| {
            (
                candidate.last_access_epoch,
                candidate.read_ts,
                candidate.txn_id,
            ) < (current.last_access_epoch, current.read_ts, current.txn_id)
        })
        .unwrap_or(true)
}

fn active_read_set_base_bytes() -> usize {
    std::mem::size_of::<ActiveReadSet>() + 64
}

fn apply_memory_delta(atomic: &AtomicUsize, old_usage: usize, new_usage: usize) {
    if new_usage >= old_usage {
        atomic.fetch_add(new_usage - old_usage, Ordering::AcqRel);
    } else {
        atomic.fetch_sub(old_usage - new_usage, Ordering::AcqRel);
    }
}

fn hash_to_index<T: Hash>(value: &T, shard_count: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

fn select_earlier_active_conflict(
    slot: &mut Option<ActiveReadConflict>,
    candidate: ActiveReadConflict,
) {
    let should_replace = slot
        .as_ref()
        .map(|current| {
            candidate.read_ts < current.read_ts
                || (candidate.read_ts == current.read_ts && candidate.txn_id < current.txn_id)
        })
        .unwrap_or(true);
    if should_replace {
        *slot = Some(candidate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializable_tracker_batches_and_deduplicates_dependencies() {
        let index = Arc::new(ReadDependencyIndex::with_shards(4));
        let tracker =
            ReadDependencyIndex::tracker(Arc::clone(&index), TxnId::new(100), ReadTs::new(7));

        let inserted = tracker.record_batch([
            ReadDependency::Tablet {
                table_id: TableId::new(10),
                tablet_id: 20,
                read_ts: ReadTs::new(7),
                layout_epoch: 3,
                rowset_count: 4,
            },
            ReadDependency::Tablet {
                table_id: TableId::new(10),
                tablet_id: 20,
                read_ts: ReadTs::new(7),
                layout_epoch: 3,
                rowset_count: 4,
            },
            ReadDependency::Row {
                table_id: TableId::new(10),
                row_id: 99,
            },
        ]);

        assert_eq!(inserted, 2);
        assert_eq!(tracker.frozen_read_set().dependency_count(), 2);
        assert!(tracker.dependencies().contains(&ReadDependency::Table {
            table_id: TableId::new(10)
        }));
        assert_eq!(index.stats().txn_count, 1);
    }

    #[test]
    fn per_txn_budget_coarsens_to_table_marker() {
        let index = Arc::new(ReadDependencyIndex::new(ReadDependencyIndexOptions {
            shard_count: 2,
            initial_txns_per_shard: 1,
            per_txn_budget_bytes: active_read_set_base_bytes() + 160,
            ..ReadDependencyIndexOptions::default()
        }));
        let tracker =
            ReadDependencyIndex::tracker(Arc::clone(&index), TxnId::new(101), ReadTs::new(9));

        for rowset_id in 0..32 {
            tracker.record(ReadDependency::Rowset {
                table_id: TableId::new(11),
                tablet_id: 20,
                rowset_id,
                read_ts: ReadTs::new(9),
                layout_epoch: rowset_id,
            });
        }

        let frozen = tracker.frozen_read_set();
        assert!(frozen.is_coarsened());
        assert_eq!(
            frozen.dependencies(),
            &[ReadDependency::Table {
                table_id: TableId::new(11)
            }]
        );
        assert_eq!(index.stats().coarsened_txn_count, 1);
    }

    #[test]
    fn release_removes_memory_and_returns_frozen_read_set() {
        let index = Arc::new(ReadDependencyIndex::with_shards(2));
        let tracker =
            ReadDependencyIndex::tracker(Arc::clone(&index), TxnId::new(102), ReadTs::new(9));
        tracker.record(ReadDependency::Predicate {
            table_id: TableId::new(11),
            predicate_hash: 44,
        });

        let before = index.stats();
        assert_eq!(before.txn_count, 1);
        assert!(before.memory_usage_bytes > 0);

        let frozen = index.release_transaction(TxnId::new(102)).unwrap();
        assert_eq!(frozen.dependency_count(), 1);
        assert_eq!(index.stats().txn_count, 0);
        assert_eq!(index.stats().memory_usage_bytes, 0);
    }

    #[test]
    fn direct_record_registers_txn_memory_once() {
        let index = ReadDependencyIndex::with_shards(2);
        index.record(
            TxnId::new(103),
            ReadDependency::Predicate {
                table_id: TableId::new(12),
                predicate_hash: 7,
            },
        );

        let stats = index.stats();
        assert_eq!(stats.txn_count, 1);
        assert_eq!(stats.dependency_count, 1);
        assert!(stats.memory_usage_bytes >= active_read_set_base_bytes());
        index.release_transaction(TxnId::new(103));
        assert_eq!(index.stats().memory_usage_bytes, 0);
    }

    #[test]
    fn global_budget_coarsens_oldest_active_read_set() {
        let index = Arc::new(ReadDependencyIndex::new(ReadDependencyIndexOptions {
            shard_count: 2,
            initial_txns_per_shard: 1,
            per_txn_budget_bytes: 16 * 1024,
            global_budget_bytes: active_read_set_base_bytes() * 2 + 192,
        }));
        let old = ReadDependencyIndex::tracker(Arc::clone(&index), TxnId::new(201), ReadTs::new(1));
        let new = ReadDependencyIndex::tracker(Arc::clone(&index), TxnId::new(202), ReadTs::new(2));

        for rowset_id in 0..8 {
            old.record(ReadDependency::Rowset {
                table_id: TableId::new(1),
                tablet_id: 10,
                rowset_id,
                read_ts: ReadTs::new(1),
                layout_epoch: rowset_id,
            });
            new.record(ReadDependency::Rowset {
                table_id: TableId::new(2),
                tablet_id: 20,
                rowset_id,
                read_ts: ReadTs::new(2),
                layout_epoch: rowset_id,
            });
        }

        let stats = index.stats();
        assert!(stats.memory_usage_bytes <= stats.global_budget_bytes);
        assert!(stats.global_coarsen_count >= 1);
        assert!(stats.coarsened_txn_count >= 1);
    }

    #[test]
    fn analytical_scan_policy_records_marker() {
        let index = Arc::new(ReadDependencyIndex::with_shards(2));
        let tracker = crate::ReadTrackerHandle::serializable_with_policy(
            Arc::clone(&index),
            TxnId::new(301),
            ReadTs::new(3),
            crate::ReadTrackingPolicy::AnalyticalScan,
        );

        tracker.record_tablet_read(TableId::new(42), 7, ReadTs::new(3), 1, 4);

        assert_eq!(
            tracker.recorded_dependencies(),
            vec![ReadDependency::AnalyticalScan {
                table_id: TableId::new(42)
            }]
        );
        assert_eq!(index.stats().analytical_scan_marker_count, 1);
    }

    #[test]
    fn savepoint_rollback_truncates_uncoarsened_dependencies() {
        let index = Arc::new(ReadDependencyIndex::new(ReadDependencyIndexOptions {
            shard_count: 2,
            initial_txns_per_shard: 1,
            per_txn_budget_bytes: 16 * 1024,
            global_budget_bytes: 16 * 1024,
        }));
        let tracker =
            ReadDependencyIndex::tracker(Arc::clone(&index), TxnId::new(401), ReadTs::new(40));

        tracker.record(ReadDependency::Table {
            table_id: TableId::new(1),
        });
        let mark = tracker.mark_savepoint();
        tracker.record(ReadDependency::Predicate {
            table_id: TableId::new(2),
            predicate_hash: 7,
        });
        tracker.record(ReadDependency::KeyRange {
            table_id: TableId::new(3),
            start_hash: 10,
            end_hash: 20,
        });

        let before = index.stats();
        assert_eq!(before.dependency_count, 3);
        assert_eq!(before.record_count, 3);

        let rollback = tracker.rollback_to_savepoint(mark);

        assert_eq!(rollback.removed_dependencies, 2);
        assert!(!rollback.preserved_due_to_coarsening);
        assert_eq!(
            tracker.dependencies(),
            vec![ReadDependency::Table {
                table_id: TableId::new(1)
            }]
        );
        assert_eq!(index.stats().dependency_count, 1);
    }

    #[test]
    fn savepoint_rollback_preserves_read_set_after_coarsening() {
        let index = Arc::new(ReadDependencyIndex::new(ReadDependencyIndexOptions {
            shard_count: 2,
            initial_txns_per_shard: 1,
            per_txn_budget_bytes: active_read_set_base_bytes() + 96,
            global_budget_bytes: 16 * 1024,
        }));
        let tracker =
            ReadDependencyIndex::tracker(Arc::clone(&index), TxnId::new(402), ReadTs::new(40));

        tracker.record(ReadDependency::Rowset {
            table_id: TableId::new(1),
            tablet_id: 1,
            rowset_id: 1,
            read_ts: ReadTs::new(40),
            layout_epoch: 1,
        });
        let mark = tracker.mark_savepoint();
        for rowset_id in 2..12 {
            tracker.record(ReadDependency::Rowset {
                table_id: TableId::new(1),
                tablet_id: 1,
                rowset_id,
                read_ts: ReadTs::new(40),
                layout_epoch: rowset_id,
            });
        }

        let rollback = tracker.rollback_to_savepoint(mark);

        assert_eq!(rollback.removed_dependencies, 0);
        assert!(rollback.preserved_due_to_coarsening);
        assert!(tracker.frozen_read_set().is_coarsened());
        assert_eq!(
            tracker.dependencies(),
            vec![ReadDependency::Table {
                table_id: TableId::new(1)
            }]
        );
    }
}
