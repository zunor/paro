// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Committed transaction summaries retained for SSI validation.

use crate::cache::CachePadded;
use crate::read_dependency_index::compact_read_dependencies;
use crate::sync::Mutex;
use crate::{CommitTs, FrozenReadSet, LockResource, ReadDependency, ReadTs, TxnId};
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

pub const DEFAULT_COMMITTED_TXN_SUMMARY_SHARDS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommittedTxnSummaryIndexOptions {
    pub shard_count: usize,
    pub initial_entries_per_shard: usize,
}

impl Default for CommittedTxnSummaryIndexOptions {
    fn default() -> Self {
        Self {
            shard_count: DEFAULT_COMMITTED_TXN_SUMMARY_SHARDS,
            initial_entries_per_shard: 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressedReadSetSummary {
    dependency_count: usize,
    coarsened: bool,
    dependencies: Vec<ReadDependency>,
}

impl CompressedReadSetSummary {
    pub fn from_frozen(read_set: &FrozenReadSet) -> Self {
        let (dependencies, summary_coarsened) =
            compact_read_dependencies(read_set.dependencies().iter().cloned());
        Self {
            dependency_count: read_set.dependency_count().max(dependencies.len()),
            coarsened: read_set.is_coarsened() || summary_coarsened,
            dependencies,
        }
    }

    #[inline]
    pub const fn dependency_count(&self) -> usize {
        self.dependency_count
    }

    #[inline]
    pub const fn is_coarsened(&self) -> bool {
        self.coarsened
    }

    #[inline]
    pub fn dependencies(&self) -> &[ReadDependency] {
        &self.dependencies
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedTxnSummary {
    pub txn_id: TxnId,
    pub read_ts: ReadTs,
    pub commit_ts: CommitTs,
    pub write_set: Vec<LockResource>,
    pub read_set: CompressedReadSetSummary,
}

impl CommittedTxnSummary {
    pub fn new(
        txn_id: TxnId,
        read_ts: ReadTs,
        commit_ts: CommitTs,
        writes: impl IntoIterator<Item = LockResource>,
        read_set: &FrozenReadSet,
    ) -> Self {
        let mut write_set = writes.into_iter().collect::<Vec<_>>();
        write_set.sort();
        write_set.dedup();
        Self {
            txn_id,
            read_ts,
            commit_ts,
            write_set,
            read_set: CompressedReadSetSummary::from_frozen(read_set),
        }
    }

    #[inline]
    pub fn write_count(&self) -> usize {
        self.write_set.len()
    }

    #[inline]
    pub fn read_dependency_count(&self) -> usize {
        self.read_set.dependency_count()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommittedTxnSummaryStats {
    pub shard_count: usize,
    pub summary_count: usize,
    pub write_dependency_count: usize,
    pub read_dependency_count: usize,
    pub coarsened_read_summary_count: usize,
    pub durable_ts: CommitTs,
    pub conflict_horizon: CommitTs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedTxnConflict {
    pub txn_id: TxnId,
    pub read_ts: ReadTs,
    pub commit_ts: CommitTs,
    pub dependency: ReadDependency,
    pub write: LockResource,
}

impl CommittedTxnConflict {
    #[inline]
    pub const fn is_coarse_scan_marker(&self) -> bool {
        self.dependency.is_coarse_scan_marker()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommittedTxnSummaryError {
    NonMonotonicCommit {
        previous: CommitTs,
        attempted: CommitTs,
    },
}

#[derive(Debug)]
pub struct CommittedTxnSummaryIndex {
    shards: Box<[CachePadded<Mutex<CommittedTxnSummaryShard>>]>,
    durable_ts: AtomicU64,
    conflict_horizon: AtomicU64,
}

#[derive(Debug)]
struct CommittedTxnSummaryShard {
    summaries: VecDeque<CommittedTxnSummary>,
    last_commit_ts: CommitTs,
}

impl CommittedTxnSummaryIndex {
    pub fn new(options: CommittedTxnSummaryIndexOptions) -> Self {
        assert!(options.shard_count > 0, "summary index needs shards");
        let shards = (0..options.shard_count)
            .map(|_| {
                CachePadded::new(Mutex::new(CommittedTxnSummaryShard::with_capacity(
                    options.initial_entries_per_shard,
                )))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            durable_ts: AtomicU64::new(0),
            conflict_horizon: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn with_shards(shard_count: usize) -> Self {
        Self::new(CommittedTxnSummaryIndexOptions {
            shard_count,
            ..CommittedTxnSummaryIndexOptions::default()
        })
    }

    pub fn register_commit(
        &self,
        summary: CommittedTxnSummary,
    ) -> Result<(), CommittedTxnSummaryError> {
        let durable_ts = CommitTs::new(self.durable_ts.load(Ordering::Acquire));
        if summary.commit_ts < durable_ts {
            return Err(CommittedTxnSummaryError::NonMonotonicCommit {
                previous: durable_ts,
                attempted: summary.commit_ts,
            });
        }
        let shard_index = self.shard_index(summary.txn_id);
        let commit_ts = summary.commit_ts;
        self.shards[shard_index].0.lock().push(summary)?;
        bump_atomic_max(&self.durable_ts, commit_ts.into_raw());
        Ok(())
    }

    pub fn advance_horizon(&self, target: CommitTs) -> CommitTs {
        let advanced = bump_atomic_max(&self.conflict_horizon, target.into_raw());
        let horizon = CommitTs::new(advanced);
        for shard in self.shards.iter() {
            shard.0.lock().gc(horizon);
        }
        horizon
    }

    pub fn summaries_after(&self, read_ts: ReadTs) -> Vec<CommittedTxnSummary> {
        let mut summaries = Vec::new();
        for shard in self.shards.iter() {
            let shard = shard.0.lock();
            summaries.extend(
                shard
                    .summaries
                    .iter()
                    .filter(|summary| summary.commit_ts.into_raw() > read_ts.into_raw())
                    .cloned(),
            );
        }
        summaries.sort_by_key(|summary| (summary.commit_ts, summary.txn_id));
        summaries
    }

    pub fn first_write_conflict_for_reads(
        &self,
        read_ts: ReadTs,
        reads: &[ReadDependency],
    ) -> Option<CommittedTxnConflict> {
        if reads.is_empty() {
            return None;
        }
        let mut best = None;
        for shard in self.shards.iter() {
            let shard = shard.0.lock();
            for summary in shard
                .summaries
                .iter()
                .filter(|summary| summary.commit_ts.into_raw() > read_ts.into_raw())
            {
                for dependency in reads {
                    for write in &summary.write_set {
                        if dependency.conflicts_with_write(write) {
                            select_earlier_conflict(
                                &mut best,
                                CommittedTxnConflict {
                                    txn_id: summary.txn_id,
                                    read_ts: summary.read_ts,
                                    commit_ts: summary.commit_ts,
                                    dependency: dependency.clone(),
                                    write: write.clone(),
                                },
                            );
                        }
                    }
                }
            }
        }
        best
    }

    pub fn first_read_conflict_for_writes(
        &self,
        read_ts: ReadTs,
        writes: &[LockResource],
    ) -> Option<CommittedTxnConflict> {
        if writes.is_empty() {
            return None;
        }
        let mut best = None;
        for shard in self.shards.iter() {
            let shard = shard.0.lock();
            for summary in shard
                .summaries
                .iter()
                .filter(|summary| summary.commit_ts.into_raw() > read_ts.into_raw())
            {
                for dependency in summary.read_set.dependencies() {
                    for write in writes {
                        if dependency.conflicts_with_write(write) {
                            select_earlier_conflict(
                                &mut best,
                                CommittedTxnConflict {
                                    txn_id: summary.txn_id,
                                    read_ts: summary.read_ts,
                                    commit_ts: summary.commit_ts,
                                    dependency: dependency.clone(),
                                    write: write.clone(),
                                },
                            );
                        }
                    }
                }
            }
        }
        best
    }

    pub fn stats(&self) -> CommittedTxnSummaryStats {
        let mut summary_count = 0;
        let mut write_dependency_count = 0;
        let mut read_dependency_count = 0;
        let mut coarsened_read_summary_count = 0;
        for shard in self.shards.iter() {
            let shard = shard.0.lock();
            summary_count += shard.summaries.len();
            for summary in &shard.summaries {
                write_dependency_count += summary.write_set.len();
                read_dependency_count += summary.read_set.dependencies().len();
                coarsened_read_summary_count += usize::from(summary.read_set.is_coarsened());
            }
        }
        CommittedTxnSummaryStats {
            shard_count: self.shards.len(),
            summary_count,
            write_dependency_count,
            read_dependency_count,
            coarsened_read_summary_count,
            durable_ts: CommitTs::new(self.durable_ts.load(Ordering::Acquire)),
            conflict_horizon: CommitTs::new(self.conflict_horizon.load(Ordering::Acquire)),
        }
    }

    #[inline]
    fn shard_index(&self, txn_id: TxnId) -> usize {
        hash_to_index(&txn_id, self.shards.len())
    }
}

impl Default for CommittedTxnSummaryIndex {
    fn default() -> Self {
        Self::new(CommittedTxnSummaryIndexOptions::default())
    }
}

impl CommittedTxnSummaryShard {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            summaries: VecDeque::with_capacity(capacity),
            last_commit_ts: CommitTs::zero(),
        }
    }

    fn push(&mut self, summary: CommittedTxnSummary) -> Result<(), CommittedTxnSummaryError> {
        if summary.commit_ts < self.last_commit_ts {
            return Err(CommittedTxnSummaryError::NonMonotonicCommit {
                previous: self.last_commit_ts,
                attempted: summary.commit_ts,
            });
        }
        self.last_commit_ts = summary.commit_ts;
        self.summaries.push_back(summary);
        Ok(())
    }

    fn gc(&mut self, horizon: CommitTs) {
        while self
            .summaries
            .front()
            .is_some_and(|summary| summary.commit_ts <= horizon)
        {
            self.summaries.pop_front();
        }
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

fn hash_to_index<T: Hash>(value: &T, shard_count: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

fn select_earlier_conflict(
    slot: &mut Option<CommittedTxnConflict>,
    candidate: CommittedTxnConflict,
) {
    let should_replace = slot
        .as_ref()
        .map(|current| {
            candidate.commit_ts < current.commit_ts
                || (candidate.commit_ts == current.commit_ts && candidate.txn_id < current.txn_id)
        })
        .unwrap_or(true);
    if should_replace {
        *slot = Some(candidate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LockNamespace, TableId};

    fn table_resource(table_id: u64) -> LockResource {
        LockResource::Table {
            namespace: LockNamespace::single_tenant(crate::DatabaseId::new(1)),
            table_id: TableId::new(table_id),
        }
    }

    #[test]
    fn committed_summary_compresses_read_set_and_dedups_write_set() {
        let read_set = FrozenReadSet::from_dependencies(vec![
            ReadDependency::Row {
                table_id: TableId::new(10),
                row_id: 1,
            },
            ReadDependency::Row {
                table_id: TableId::new(10),
                row_id: 2,
            },
            ReadDependency::Predicate {
                table_id: TableId::new(11),
                predicate_hash: 9,
            },
        ]);
        let summary = CommittedTxnSummary::new(
            TxnId::new(100),
            ReadTs::new(4),
            CommitTs::new(8),
            [table_resource(20), table_resource(20)],
            &read_set,
        );

        assert_eq!(summary.write_count(), 1);
        assert_eq!(summary.read_set.dependencies().len(), 2);
        assert!(summary
            .read_set
            .dependencies()
            .contains(&ReadDependency::Table {
                table_id: TableId::new(10)
            }));
    }

    #[test]
    fn summary_index_retains_until_conflict_horizon() {
        let index = CommittedTxnSummaryIndex::with_shards(4);
        let empty = FrozenReadSet::empty();
        index
            .register_commit(CommittedTxnSummary::new(
                TxnId::new(100),
                ReadTs::new(1),
                CommitTs::new(5),
                [table_resource(10)],
                &empty,
            ))
            .unwrap();
        index
            .register_commit(CommittedTxnSummary::new(
                TxnId::new(101),
                ReadTs::new(2),
                CommitTs::new(8),
                [table_resource(11)],
                &empty,
            ))
            .unwrap();

        assert_eq!(index.summaries_after(ReadTs::new(4)).len(), 2);
        assert_eq!(index.advance_horizon(CommitTs::new(5)), CommitTs::new(5));
        assert_eq!(index.stats().summary_count, 1);
        assert_eq!(
            index.summaries_after(ReadTs::new(4))[0].commit_ts,
            CommitTs::new(8)
        );
    }
}
