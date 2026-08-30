// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::rowset::RowsetId;
use crate::tablet::ColumnId;

use super::capability::SearchIndexKind;
use super::stats::SegmentId;

pub mod exact_merge;
pub(crate) mod reader_warmup;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TailEntryId(pub u64);

impl TailEntryId {
    pub const UNASSIGNED: Self = Self(0);

    pub const fn is_assigned(self) -> bool {
        self.0 != 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TailMutationKind {
    Append,
    Replace,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TailRowImageRef {
    WholeRowset,
    PartialRowset {
        touched_columns: Vec<ColumnId>,
        base_rowids_segments: Vec<SegmentId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TailPendingEntry {
    pub entry_id: TailEntryId,
    pub rowset_id: RowsetId,
    pub segment_ids: Vec<SegmentId>,
    pub mutation: TailMutationKind,
    pub row_count: u64,
    pub byte_count: u64,
    pub row_image_ref: Option<TailRowImageRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TailPendingSet {
    pub entries: Vec<TailPendingEntry>,
}

impl TailPendingSet {
    pub fn is_empty(&self) -> bool {
        self.coverage_rows() == 0
    }

    pub fn coverage_rowsets(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.mutation != TailMutationKind::Delete)
            .map(|entry| entry.rowset_id)
            .collect::<BTreeSet<_>>()
            .len()
    }

    pub fn coverage_segments(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.mutation != TailMutationKind::Delete)
            .flat_map(|entry| {
                entry
                    .segment_ids
                    .iter()
                    .copied()
                    .map(move |segment_id| (entry.rowset_id, segment_id))
            })
            .collect::<BTreeSet<_>>()
            .len()
    }

    pub fn coverage_rows(&self) -> u64 {
        self.entries
            .iter()
            .filter(|entry| entry.mutation != TailMutationKind::Delete)
            .map(|entry| entry.row_count)
            .sum()
    }

    pub fn coverage_bytes(&self) -> u64 {
        self.entries
            .iter()
            .filter(|entry| entry.mutation != TailMutationKind::Delete)
            .map(|entry| entry.byte_count)
            .sum()
    }

    pub fn delete_rows(&self) -> u64 {
        self.entries
            .iter()
            .filter(|entry| entry.mutation == TailMutationKind::Delete)
            .map(|entry| entry.row_count)
            .sum()
    }

    pub fn without_rowsets(&self, removed: &BTreeSet<RowsetId>) -> Self {
        Self {
            entries: self
                .entries
                .iter()
                .filter(|entry| !removed.contains(&entry.rowset_id))
                .cloned()
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailExactMergePolicy {
    pub supported: bool,
    pub soft_row_limit: u64,
    pub hard_row_limit: u64,
}

impl TailExactMergePolicy {
    /// Whether this provider has a correctness-preserving exact tail path.
    ///
    /// Row watermarks drive maintenance urgency and write backpressure only;
    /// they must never make an otherwise legal query unexecutable. The exact
    /// path is streaming and remains the final correctness fallback after a
    /// large atomic transaction crosses the critical maintenance watermark.
    pub const fn exact_tail_merge_enabled(self, _tail_rows: u64) -> bool {
        self.supported
    }
}

pub const fn provider_tail_exact_merge_policy(kind: SearchIndexKind) -> TailExactMergePolicy {
    match kind {
        SearchIndexKind::Hnsw => TailExactMergePolicy {
            supported: true,
            soft_row_limit: 4_096,
            hard_row_limit: 16_384,
        },
        SearchIndexKind::Sparse => TailExactMergePolicy {
            supported: true,
            soft_row_limit: 32_768,
            hard_row_limit: 131_072,
        },
        SearchIndexKind::FullText => TailExactMergePolicy {
            supported: true,
            soft_row_limit: 16_384,
            hard_row_limit: 65_536,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        provider_tail_exact_merge_policy, TailEntryId, TailMutationKind, TailPendingEntry,
        TailPendingSet, TailRowImageRef,
    };
    use crate::search::capability::SearchIndexKind;

    #[test]
    fn coverage_counts_ignore_delete_only_entries() {
        let tail = TailPendingSet {
            entries: vec![
                TailPendingEntry {
                    entry_id: TailEntryId(1),
                    rowset_id: 1,
                    segment_ids: vec![0, 1],
                    mutation: TailMutationKind::Append,
                    row_count: 10,
                    byte_count: 1000,
                    row_image_ref: Some(TailRowImageRef::WholeRowset),
                },
                TailPendingEntry {
                    entry_id: TailEntryId(2),
                    rowset_id: 2,
                    segment_ids: vec![0],
                    mutation: TailMutationKind::Replace,
                    row_count: 3,
                    byte_count: 300,
                    row_image_ref: Some(TailRowImageRef::PartialRowset {
                        touched_columns: vec![1],
                        base_rowids_segments: vec![0],
                    }),
                },
                TailPendingEntry {
                    entry_id: TailEntryId(3),
                    rowset_id: 1,
                    segment_ids: vec![0],
                    mutation: TailMutationKind::Delete,
                    row_count: 4,
                    byte_count: 400,
                    row_image_ref: None,
                },
            ],
        };

        assert_eq!(tail.coverage_rowsets(), 2);
        assert_eq!(tail.coverage_segments(), 3);
        assert_eq!(tail.coverage_rows(), 13);
        assert_eq!(tail.coverage_bytes(), 1300);
        assert_eq!(tail.delete_rows(), 4);
    }

    #[test]
    fn provider_policies_encode_tail_merge_thresholds() {
        let hnsw = provider_tail_exact_merge_policy(SearchIndexKind::Hnsw);
        assert!(hnsw.exact_tail_merge_enabled(2_048));
        assert!(hnsw.exact_tail_merge_enabled(32_768));

        let sparse = provider_tail_exact_merge_policy(SearchIndexKind::Sparse);
        assert!(sparse.exact_tail_merge_enabled(65_536));

        let fulltext = provider_tail_exact_merge_policy(SearchIndexKind::FullText);
        assert!(fulltext.exact_tail_merge_enabled(8_192));
        assert!(fulltext.exact_tail_merge_enabled(100_000));
    }
}
