// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Durable write conflict index skeleton.

use crate::active::ActiveTxnRegistry;
use crate::lock_manager::LockResource;
use crate::sync::Mutex;
use crate::types::{CommitTs, ReadTs, MAX_TRANSACTION_ID};
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy)]
pub struct WriteConflictIndexOptions {
    pub shard_count: usize,
    pub initial_entries_per_shard: usize,
}

impl Default for WriteConflictIndexOptions {
    fn default() -> Self {
        Self {
            shard_count: 64,
            initial_entries_per_shard: 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictWrite {
    pub resource: LockResource,
}

impl ConflictWrite {
    pub fn new(resource: LockResource) -> Self {
        Self { resource }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictMatch {
    pub commit_ts: CommitTs,
    pub resource: LockResource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConflictIndexStats {
    pub shard_count: usize,
    pub entry_count: usize,
    pub fine_entry_count: usize,
    pub fine_summary_entry_count: usize,
    pub coarse_entry_count: usize,
    pub durable_ts: CommitTs,
    pub conflict_horizon: CommitTs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteConflictIndexError {
    EmptyWriteSet,
    NonMonotonicCommit {
        previous: CommitTs,
        attempted: CommitTs,
    },
}

#[derive(Debug)]
pub struct WriteConflictIndex {
    shards: Box<[Mutex<ConflictShard>]>,
    durable_ts: AtomicU64,
    conflict_horizon: AtomicU64,
}

#[derive(Debug)]
struct ConflictShard {
    entries: VecDeque<ConflictEntry>,
    last_commit_ts: CommitTs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConflictEntry {
    commit_ts: CommitTs,
    resource: LockResource,
    kind: ConflictEntryKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ConflictEntryKind {
    Fine,
    FineSummary,
    Coarse,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ConflictPlacement {
    shard_index: usize,
    kind: ConflictEntryKind,
    resource: LockResource,
}

#[derive(Debug, Clone, Copy)]
struct ConflictProbe {
    shard_index: usize,
    kind_mask: u8,
}

impl WriteConflictIndex {
    pub fn new(options: WriteConflictIndexOptions) -> Self {
        assert!(options.shard_count > 0, "conflict index needs shards");
        let shards = (0..options.shard_count)
            .map(|_| {
                Mutex::new(ConflictShard::with_capacity(
                    options.initial_entries_per_shard,
                ))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            durable_ts: AtomicU64::new(0),
            conflict_horizon: AtomicU64::new(0),
        }
    }

    pub fn with_shards(shard_count: usize) -> Self {
        Self::new(WriteConflictIndexOptions {
            shard_count,
            ..WriteConflictIndexOptions::default()
        })
    }

    pub fn register_commit(
        &self,
        commit_ts: CommitTs,
        writes: impl IntoIterator<Item = ConflictWrite>,
    ) -> Result<usize, WriteConflictIndexError> {
        let mut writes = writes.into_iter().collect::<Vec<_>>();
        if writes.is_empty() {
            return Err(WriteConflictIndexError::EmptyWriteSet);
        }
        let durable_ts = CommitTs::new(self.durable_ts.load(Ordering::Acquire));
        if commit_ts < durable_ts {
            return Err(WriteConflictIndexError::NonMonotonicCommit {
                previous: durable_ts,
                attempted: commit_ts,
            });
        }
        writes.sort_by(|left, right| left.resource.cmp(&right.resource));
        writes.dedup_by(|left, right| left.resource == right.resource);

        let mut placements = Vec::with_capacity(writes.len() * 3);
        for write in &writes {
            placements.extend(self.placements_for_write(&write.resource));
        }
        placements.sort();
        placements.dedup();

        for placement in placements {
            let mut shard = self.shards[placement.shard_index].lock();
            shard.push(commit_ts, placement.resource, placement.kind)?;
        }
        bump_atomic_max(&self.durable_ts, commit_ts.into_raw());
        Ok(writes.len())
    }

    pub fn has_conflict(
        &self,
        read_ts: ReadTs,
        writes: impl IntoIterator<Item = ConflictWrite>,
    ) -> bool {
        self.first_conflict(read_ts, writes).is_some()
    }

    pub fn first_conflict(
        &self,
        read_ts: ReadTs,
        writes: impl IntoIterator<Item = ConflictWrite>,
    ) -> Option<ConflictMatch> {
        let writes = writes.into_iter().collect::<Vec<_>>();
        if writes.is_empty() {
            return None;
        }

        let mut best = None;
        for write in writes {
            let probes = self.probes_for_query(&write.resource);
            for probe in probes {
                let shard = self.shards[probe.shard_index].lock();
                if let Some(entry) = shard.first_conflict(read_ts, &write.resource, probe.kind_mask)
                {
                    select_earlier_match(
                        &mut best,
                        ConflictMatch {
                            commit_ts: entry.commit_ts,
                            resource: entry.resource.clone(),
                        },
                    );
                }
            }
        }
        best
    }

    pub fn advance_horizon_with_confirmed_active_rw(
        &self,
        published_ts: CommitTs,
        active_registry: &ActiveTxnRegistry,
    ) -> CommitTs {
        let active_rw = active_registry.confirmed_oldest_active_rw_read_ts();
        let active_rw_raw = active_rw.into_raw();
        let candidate = if active_rw_raw == MAX_TRANSACTION_ID {
            published_ts.into_raw()
        } else {
            published_ts.into_raw().min(active_rw_raw)
        };
        self.advance_horizon(CommitTs::new(candidate))
    }

    pub fn advance_horizon(&self, target: CommitTs) -> CommitTs {
        let advanced = bump_atomic_max(&self.conflict_horizon, target.into_raw());
        let horizon = CommitTs::new(advanced);
        for shard in self.shards.iter() {
            shard.lock().gc(horizon);
        }
        horizon
    }

    pub fn stats(&self) -> ConflictIndexStats {
        let mut entry_count = 0;
        let mut fine_entry_count = 0;
        let mut fine_summary_entry_count = 0;
        let mut coarse_entry_count = 0;
        for shard in self.shards.iter() {
            let shard = shard.lock();
            entry_count += shard.entries.len();
            let (fine, summary, coarse) = shard.kind_counts();
            fine_entry_count += fine;
            fine_summary_entry_count += summary;
            coarse_entry_count += coarse;
        }
        ConflictIndexStats {
            shard_count: self.shards.len(),
            entry_count,
            fine_entry_count,
            fine_summary_entry_count,
            coarse_entry_count,
            durable_ts: CommitTs::new(self.durable_ts.load(Ordering::Acquire)),
            conflict_horizon: CommitTs::new(self.conflict_horizon.load(Ordering::Acquire)),
        }
    }

    fn placements_for_write(&self, resource: &LockResource) -> Vec<ConflictPlacement> {
        if is_fine_resource(resource) {
            let mut placements = Vec::new();
            for shard_index in self.fine_shards(resource) {
                placements.push(ConflictPlacement {
                    shard_index,
                    kind: ConflictEntryKind::Fine,
                    resource: resource.clone(),
                });
            }
            placements.push(ConflictPlacement {
                shard_index: self.database_summary_shard(resource.namespace()),
                kind: ConflictEntryKind::FineSummary,
                resource: resource.clone(),
            });
            if let Some(table_id) = resource.table_id() {
                placements.push(ConflictPlacement {
                    shard_index: self.table_summary_shard(resource.namespace(), table_id),
                    kind: ConflictEntryKind::FineSummary,
                    resource: resource.clone(),
                });
            }
            return placements;
        }

        vec![ConflictPlacement {
            shard_index: self.coarse_shard(resource),
            kind: ConflictEntryKind::Coarse,
            resource: resource.clone(),
        }]
    }

    fn probes_for_query(&self, resource: &LockResource) -> Vec<ConflictProbe> {
        let mut probes = Vec::new();
        if is_fine_resource(resource) {
            for shard_index in self.fine_shards(resource) {
                push_probe(&mut probes, shard_index, ConflictEntryKind::Fine.bit());
            }
            push_probe(
                &mut probes,
                self.database_summary_shard(resource.namespace()),
                ConflictEntryKind::Coarse.bit(),
            );
            if let Some(table_id) = resource.table_id() {
                push_probe(
                    &mut probes,
                    self.table_summary_shard(resource.namespace(), table_id),
                    ConflictEntryKind::Coarse.bit(),
                );
            }
            if let Some((table_id, tablet_id)) = resource.tablet_identity() {
                push_probe(
                    &mut probes,
                    self.tablet_summary_shard(resource.namespace(), table_id, tablet_id),
                    ConflictEntryKind::Coarse.bit(),
                );
            }
        } else {
            push_probe(
                &mut probes,
                self.database_summary_shard(resource.namespace()),
                ConflictEntryKind::Coarse.bit(),
            );
            push_probe(
                &mut probes,
                self.coarse_shard(resource),
                ConflictEntryKind::Coarse.bit() | ConflictEntryKind::FineSummary.bit(),
            );
            if let Some(table_id) = resource.table_id() {
                push_probe(
                    &mut probes,
                    self.table_summary_shard(resource.namespace(), table_id),
                    ConflictEntryKind::Coarse.bit() | ConflictEntryKind::FineSummary.bit(),
                );
            }
            if let Some((table_id, tablet_id)) = resource.tablet_identity() {
                push_probe(
                    &mut probes,
                    self.tablet_summary_shard(resource.namespace(), table_id, tablet_id),
                    ConflictEntryKind::Coarse.bit(),
                );
            }
        }
        probes.sort_by_key(|probe| probe.shard_index);
        probes
    }

    fn fine_shards(&self, resource: &LockResource) -> Vec<usize> {
        match resource {
            LockResource::PrimaryKey {
                namespace,
                table_id,
                tablet_id,
                key_hash,
            } => vec![self.primary_key_bucket_shard(*namespace, *table_id, *tablet_id, *key_hash)],
            LockResource::Range {
                namespace,
                table_id,
                tablet_id,
                start_hash,
                end_hash,
            } => self
                .hash_buckets_for_range(*start_hash, *end_hash)
                .into_iter()
                .map(|bucket| self.range_bucket_shard(*namespace, *table_id, *tablet_id, bucket))
                .collect(),
            LockResource::RowId {
                namespace,
                table_id,
                tablet_id,
                rowset_id,
                segment_id,
                row_offset,
            } => vec![hash_to_index(
                &(
                    3u8,
                    *namespace,
                    *table_id,
                    *tablet_id,
                    *rowset_id,
                    *segment_id,
                    ordered_hash_bucket(*row_offset as u64, self.shards.len()),
                ),
                self.shards.len(),
            )],
            _ => vec![self.coarse_shard(resource)],
        }
    }

    fn primary_key_bucket_shard(
        &self,
        namespace: crate::lock_manager::LockNamespace,
        table_id: crate::types::TableId,
        tablet_id: u64,
        key_hash: u64,
    ) -> usize {
        hash_to_index(
            &(
                1u8,
                namespace,
                table_id,
                tablet_id,
                ordered_hash_bucket(key_hash, self.shards.len()),
            ),
            self.shards.len(),
        )
    }

    fn range_bucket_shard(
        &self,
        namespace: crate::lock_manager::LockNamespace,
        table_id: crate::types::TableId,
        tablet_id: u64,
        bucket: usize,
    ) -> usize {
        hash_to_index(
            &(1u8, namespace, table_id, tablet_id, bucket),
            self.shards.len(),
        )
    }

    fn coarse_shard(&self, resource: &LockResource) -> usize {
        match resource {
            LockResource::Database { namespace } => self.database_summary_shard(*namespace),
            LockResource::Table {
                namespace,
                table_id,
            }
            | LockResource::Predicate {
                namespace,
                table_id,
                ..
            } => self.table_summary_shard(*namespace, *table_id),
            LockResource::Tablet {
                namespace,
                table_id,
                tablet_id,
            } => self.tablet_summary_shard(*namespace, *table_id, *tablet_id),
            _ => hash_to_index(&(0u8, resource), self.shards.len()),
        }
    }

    fn database_summary_shard(&self, namespace: crate::lock_manager::LockNamespace) -> usize {
        hash_to_index(&(10u8, namespace), self.shards.len())
    }

    fn table_summary_shard(
        &self,
        namespace: crate::lock_manager::LockNamespace,
        table_id: crate::types::TableId,
    ) -> usize {
        hash_to_index(&(11u8, namespace, table_id), self.shards.len())
    }

    fn tablet_summary_shard(
        &self,
        namespace: crate::lock_manager::LockNamespace,
        table_id: crate::types::TableId,
        tablet_id: u64,
    ) -> usize {
        hash_to_index(&(12u8, namespace, table_id, tablet_id), self.shards.len())
    }

    fn hash_buckets_for_range(&self, start_hash: u64, end_hash: u64) -> Vec<usize> {
        let bucket_count = self.shards.len();
        let start = ordered_hash_bucket(start_hash, bucket_count);
        let end = ordered_hash_bucket(end_hash, bucket_count);
        let mut buckets = Vec::new();
        if start <= end {
            buckets.extend(start..=end);
        } else {
            buckets.extend(start..bucket_count);
            buckets.extend(0..=end);
        }
        buckets
    }
}

impl Default for WriteConflictIndex {
    fn default() -> Self {
        Self::new(WriteConflictIndexOptions::default())
    }
}

impl ConflictShard {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity),
            last_commit_ts: CommitTs::zero(),
        }
    }

    fn push(
        &mut self,
        commit_ts: CommitTs,
        resource: LockResource,
        kind: ConflictEntryKind,
    ) -> Result<(), WriteConflictIndexError> {
        if commit_ts < self.last_commit_ts {
            return Err(WriteConflictIndexError::NonMonotonicCommit {
                previous: self.last_commit_ts,
                attempted: commit_ts,
            });
        }
        self.last_commit_ts = commit_ts;
        self.entries.push_back(ConflictEntry {
            commit_ts,
            resource,
            kind,
        });
        Ok(())
    }

    fn first_conflict(
        &self,
        read_ts: ReadTs,
        resource: &LockResource,
        kind_mask: u8,
    ) -> Option<&ConflictEntry> {
        self.entries.iter().find(|entry| {
            entry.commit_ts.into_raw() > read_ts.into_raw()
                && entry.kind.bit() & kind_mask != 0
                && entry.resource.conflicts_with(resource)
        })
    }

    fn gc(&mut self, horizon: CommitTs) {
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.commit_ts <= horizon)
        {
            self.entries.pop_front();
        }
    }

    fn kind_counts(&self) -> (usize, usize, usize) {
        let mut fine = 0;
        let mut summary = 0;
        let mut coarse = 0;
        for entry in &self.entries {
            match entry.kind {
                ConflictEntryKind::Fine => fine += 1,
                ConflictEntryKind::FineSummary => summary += 1,
                ConflictEntryKind::Coarse => coarse += 1,
            }
        }
        (fine, summary, coarse)
    }
}

fn bump_atomic_max(atomic: &AtomicU64, candidate: u64) -> u64 {
    let mut current = atomic.load(Ordering::Acquire);
    while candidate > current {
        match atomic.compare_exchange(current, candidate, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return candidate,
            Err(next) => current = next,
        }
    }
    current
}

impl ConflictEntryKind {
    const fn bit(self) -> u8 {
        match self {
            Self::Fine => 0b001,
            Self::FineSummary => 0b010,
            Self::Coarse => 0b100,
        }
    }
}

fn is_fine_resource(resource: &LockResource) -> bool {
    matches!(
        resource,
        LockResource::PrimaryKey { .. } | LockResource::RowId { .. } | LockResource::Range { .. }
    )
}

fn push_probe(probes: &mut Vec<ConflictProbe>, shard_index: usize, kind_mask: u8) {
    if let Some(probe) = probes
        .iter_mut()
        .find(|probe| probe.shard_index == shard_index)
    {
        probe.kind_mask |= kind_mask;
    } else {
        probes.push(ConflictProbe {
            shard_index,
            kind_mask,
        });
    }
}

fn hash_to_index<T: Hash>(value: &T, shard_count: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

fn ordered_hash_bucket(hash: u64, bucket_count: usize) -> usize {
    debug_assert!(bucket_count > 0);
    ((hash as u128 * bucket_count as u128) >> 64) as usize
}

fn select_earlier_match(slot: &mut Option<ConflictMatch>, candidate: ConflictMatch) {
    let should_replace = slot
        .as_ref()
        .map(|current| {
            candidate.commit_ts < current.commit_ts
                || (candidate.commit_ts == current.commit_ts
                    && candidate.resource < current.resource)
        })
        .unwrap_or(true);
    if should_replace {
        *slot = Some(candidate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::active::ActiveTxnRegistry;
    use crate::lock_manager::{LockNamespace, LockResource};
    use crate::types::{DatabaseId, TableId, TxnId};

    fn ns() -> LockNamespace {
        LockNamespace::single_tenant(DatabaseId::new(1))
    }

    fn pk(key_hash: u64) -> LockResource {
        LockResource::primary_key(ns(), TableId::new(10), 20, key_hash)
    }

    fn table() -> LockResource {
        LockResource::Table {
            namespace: ns(),
            table_id: TableId::new(10),
        }
    }

    fn tablet() -> LockResource {
        LockResource::Tablet {
            namespace: ns(),
            table_id: TableId::new(10),
            tablet_id: 20,
        }
    }

    fn range(start_hash: u64, end_hash: u64) -> LockResource {
        LockResource::Range {
            namespace: ns(),
            table_id: TableId::new(10),
            tablet_id: 20,
            start_hash,
            end_hash,
        }
    }

    #[test]
    fn conflict_index_finds_writes_after_read_ts() {
        let index = WriteConflictIndex::with_shards(4);
        index
            .register_commit(CommitTs::new(5), [ConflictWrite::new(pk(1))])
            .unwrap();

        assert!(index.has_conflict(ReadTs::new(4), [ConflictWrite::new(pk(1))]));
        assert!(!index.has_conflict(ReadTs::new(5), [ConflictWrite::new(pk(1))]));
        assert!(!index.has_conflict(ReadTs::new(4), [ConflictWrite::new(pk(2))]));
    }

    #[test]
    fn conflict_index_distributes_hot_table_primary_keys() {
        let index = WriteConflictIndex::with_shards(32);
        let shards = (0..512u64)
            .filter_map(|key| {
                index
                    .placements_for_write(&pk(key.wrapping_mul(0x9e37_79b9_7f4a_7c15)))
                    .into_iter()
                    .find(|placement| placement.kind == ConflictEntryKind::Fine)
                    .map(|placement| placement.shard_index)
            })
            .collect::<std::collections::HashSet<_>>();

        assert!(
            shards.len() > 8,
            "hot table primary-key writes should spread across fine shards"
        );
    }

    #[test]
    fn conflict_index_coarse_table_marker_conflicts_with_keys_without_collapsing_key_writes() {
        let index = WriteConflictIndex::with_shards(16);
        index
            .register_commit(CommitTs::new(5), [ConflictWrite::new(pk(1))])
            .unwrap();

        assert!(index.has_conflict(ReadTs::new(4), [ConflictWrite::new(table())]));
        assert!(!index.has_conflict(ReadTs::new(4), [ConflictWrite::new(pk(2))]));

        index
            .register_commit(CommitTs::new(6), [ConflictWrite::new(table())])
            .unwrap();
        assert!(index.has_conflict(ReadTs::new(5), [ConflictWrite::new(pk(2))]));

        let stats = index.stats();
        assert!(stats.fine_entry_count >= 1);
        assert!(stats.fine_summary_entry_count >= 1);
        assert!(stats.coarse_entry_count >= 1);
    }

    #[test]
    fn conflict_index_range_write_uses_key_buckets() {
        let index = WriteConflictIndex::with_shards(16);
        index
            .register_commit(CommitTs::new(5), [ConflictWrite::new(range(10, 20))])
            .unwrap();

        assert!(index.has_conflict(ReadTs::new(4), [ConflictWrite::new(pk(15))]));
        assert!(!index.has_conflict(ReadTs::new(4), [ConflictWrite::new(pk(21))]));
        assert!(index.has_conflict(ReadTs::new(4), [ConflictWrite::new(tablet())]));
    }

    #[test]
    fn conflict_index_gc_uses_confirmed_active_rw_horizon() {
        let index = WriteConflictIndex::with_shards(4);
        index
            .register_commit(
                CommitTs::new(5),
                [ConflictWrite::new(pk(1)), ConflictWrite::new(pk(2))],
            )
            .unwrap();
        index
            .register_commit(CommitTs::new(8), [ConflictWrite::new(pk(3))])
            .unwrap();

        let active = ActiveTxnRegistry::with_capacity(1, 4);
        let _rw = active
            .register_read_write(TxnId::new(100), ReadTs::new(6), ReadTs::new(6))
            .unwrap();

        let horizon = index.advance_horizon_with_confirmed_active_rw(CommitTs::new(10), &active);
        assert_eq!(horizon, CommitTs::new(6));
        assert_eq!(index.stats().fine_entry_count, 1);
        assert!(!index.has_conflict(ReadTs::new(6), [ConflictWrite::new(pk(1))]));
        assert!(index.has_conflict(ReadTs::new(6), [ConflictWrite::new(pk(3))]));
    }

    #[test]
    fn conflict_index_gc_advances_to_published_when_no_active_rw() {
        let index = WriteConflictIndex::with_shards(4);
        index
            .register_commit(CommitTs::new(5), [ConflictWrite::new(pk(1))])
            .unwrap();
        let active = ActiveTxnRegistry::with_capacity(1, 4);

        let horizon = index.advance_horizon_with_confirmed_active_rw(CommitTs::new(10), &active);
        assert_eq!(horizon, CommitTs::new(10));
        assert_eq!(index.stats().entry_count, 0);
    }
}
