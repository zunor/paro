// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query-time exact tail merge admission for deferred derived search state.

use crate::search::capability::{CoverageState, SearchIndexKind};
use crate::search::cursor::{PhysicalRowRef, SearchReadSnapshot, VisibleSegment};
use paro_common::error::{self as paro_error, Result};

use crate::metrics::storage_metrics;

use super::provider_tail_exact_merge_policy;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TailWindow {
    pub indexed_through_ts: u64,
    pub target_ts: u64,
    pub committed_rows: u64,
    pub own_write_rows: u64,
    pub committed_segments: usize,
    pub own_write_segments: usize,
    pub committed_bytes: u64,
    pub own_write_bytes: u64,
}

impl TailWindow {
    pub(crate) fn from_segments(
        indexed_through_ts: u64,
        visible_version: i64,
        segments: &[VisibleSegment],
        is_overlay_rowset: impl Fn(crate::rowset::RowsetId) -> bool,
    ) -> Self {
        use std::collections::HashSet;

        let target_ts = visible_version.max(0) as u64;
        let mut window = Self {
            indexed_through_ts,
            target_ts,
            ..Self::default()
        };
        let mut committed_rowsets = HashSet::new();
        let mut own_write_rowsets = HashSet::new();

        for segment in segments {
            let rows = segment.segment.num_rows();
            let rowset_id = segment.rowset_id;
            if is_overlay_rowset(rowset_id) {
                window.own_write_rows = window.own_write_rows.saturating_add(rows);
                window.own_write_segments = window.own_write_segments.saturating_add(1);
                if own_write_rowsets.insert(rowset_id) {
                    window.own_write_bytes = window
                        .own_write_bytes
                        .saturating_add(estimated_rowset_bytes(&segment.rowset));
                }
                continue;
            }

            let rowset_end = segment.rowset.end_version();
            if rowset_end < 0 {
                continue;
            }
            let rowset_end = rowset_end as u64;
            if rowset_end > indexed_through_ts && rowset_end <= target_ts {
                window.committed_rows = window.committed_rows.saturating_add(rows);
                window.committed_segments = window.committed_segments.saturating_add(1);
                if committed_rowsets.insert(rowset_id) {
                    window.committed_bytes = window
                        .committed_bytes
                        .saturating_add(estimated_rowset_bytes(&segment.rowset));
                }
            }
        }

        window
    }

    pub const fn is_empty(&self) -> bool {
        self.committed_rows == 0 && self.own_write_rows == 0
    }

    pub const fn total_rows(&self) -> u64 {
        self.committed_rows.saturating_add(self.own_write_rows)
    }

    pub const fn total_bytes(&self) -> u64 {
        self.committed_bytes.saturating_add(self.own_write_bytes)
    }

    pub const fn lag_span(&self) -> u64 {
        self.target_ts.saturating_sub(self.indexed_through_ts)
    }
}

fn estimated_rowset_bytes(rowset: &crate::rowset::RowsetSharedPtr) -> u64 {
    let on_disk = rowset.total_disk_size();
    if on_disk > 0 {
        on_disk
    } else {
        rowset.num_rows().saturating_mul(64)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TailExactMergeCost {
    pub tail_rows: u64,
    pub tail_bytes: u64,
    pub cpu_ops: u64,
    pub memory_bytes: u64,
    pub io_bytes: u64,
    pub latency_weight: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailExactMergeBudget {
    pub max_tail_exact_merge_rows: u64,
    pub max_tail_exact_merge_bytes: u64,
    pub max_cpu_ops: u64,
    pub max_memory_bytes: u64,
    pub max_io_bytes: u64,
    pub max_latency_weight: u64,
}

impl TailExactMergeBudget {
    pub const fn for_search_kind(kind: SearchIndexKind) -> Self {
        match kind {
            SearchIndexKind::Hnsw => Self {
                max_tail_exact_merge_rows: 16_384,
                max_tail_exact_merge_bytes: 32 * 1024 * 1024,
                max_cpu_ops: 256 * 1024 * 1024,
                max_memory_bytes: 16 * 1024 * 1024,
                max_io_bytes: 32 * 1024 * 1024,
                max_latency_weight: 192 * 1024 * 1024,
            },
            SearchIndexKind::Sparse => Self {
                max_tail_exact_merge_rows: 131_072,
                max_tail_exact_merge_bytes: 96 * 1024 * 1024,
                max_cpu_ops: 96 * 1024 * 1024,
                max_memory_bytes: 32 * 1024 * 1024,
                max_io_bytes: 96 * 1024 * 1024,
                max_latency_weight: 96 * 1024 * 1024,
            },
            SearchIndexKind::FullText => Self {
                max_tail_exact_merge_rows: 65_536,
                max_tail_exact_merge_bytes: 64 * 1024 * 1024,
                max_cpu_ops: 64 * 1024 * 1024,
                max_memory_bytes: 24 * 1024 * 1024,
                max_io_bytes: 64 * 1024 * 1024,
                max_latency_weight: 64 * 1024 * 1024,
            },
        }
    }

    fn first_violation(&self, cost: TailExactMergeCost) -> Option<(&'static str, u64, u64)> {
        [
            ("tail_rows", cost.tail_rows, self.max_tail_exact_merge_rows),
            (
                "tail_bytes",
                cost.tail_bytes,
                self.max_tail_exact_merge_bytes,
            ),
            ("cpu_ops", cost.cpu_ops, self.max_cpu_ops),
            ("memory_bytes", cost.memory_bytes, self.max_memory_bytes),
            ("io_bytes", cost.io_bytes, self.max_io_bytes),
            (
                "latency_weight",
                cost.latency_weight,
                self.max_latency_weight,
            ),
        ]
        .into_iter()
        .find(|(_, actual, limit)| actual > limit)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailExactMergeQueryShape {
    FullText {
        query_terms: usize,
        top_k: usize,
    },
    Sparse {
        query_terms: usize,
        top_k: usize,
    },
    Hnsw {
        dimension: usize,
        ef: Option<usize>,
        top_k: usize,
    },
}

pub(crate) fn ensure_tail_exact_merge_budget(
    snapshot: &SearchReadSnapshot,
    kind: SearchIndexKind,
    column_id: u32,
    shape: TailExactMergeQueryShape,
) -> Result<TailExactMergeCost> {
    let cost = estimate_tail_exact_merge_cost(snapshot, kind, column_id, shape)?;
    storage_metrics().record_tail_exact_merge_cost(cost.latency_weight);
    let budget = TailExactMergeBudget::for_search_kind(kind);
    if let Some((field, actual, limit)) = budget.first_violation(cost) {
        storage_metrics().record_search_tail_exact_merge_rejected(kind, field);
        return Err(paro_error::not_supported(format!(
            "{kind:?} exact tail merge exceeds {field} budget ({actual} > {limit}); keep the derived state required or run catch-up maintenance"
        )));
    }
    storage_metrics().add_search_tail_exact_merge_rows(kind, cost.tail_rows);
    Ok(cost)
}

pub(crate) fn estimate_tail_exact_merge_cost(
    snapshot: &SearchReadSnapshot,
    kind: SearchIndexKind,
    column_id: u32,
    shape: TailExactMergeQueryShape,
) -> Result<TailExactMergeCost> {
    let window = snapshot.tail_window();
    if window.is_empty() {
        return Ok(TailExactMergeCost::default());
    }
    if !coverage_allows_tail_exact_merge(&snapshot.generation.coverage) {
        storage_metrics().record_search_tail_exact_merge_rejected(kind, "coverage");
        return Err(paro_error::not_supported(format!(
            "{kind:?} generation coverage does not expose exact tail merge; keep it required"
        )));
    }
    if !provider_tail_exact_merge_policy(kind).exact_tail_merge_enabled(window.total_rows()) {
        storage_metrics().record_search_tail_exact_merge_rejected(kind, "hard_limit");
        return Err(paro_error::not_supported(format!(
            "{kind:?} tail rows {} exceed exact merge hard limit",
            window.total_rows()
        )));
    }

    let mut exact_rows = 0u64;
    let mut indexed_rows = 0u64;
    for segment in snapshot.table_lease.visible_segments() {
        if !snapshot.is_tail_segment(segment) {
            continue;
        }
        let rows = segment.segment.num_rows();
        if snapshot
            .artifact_for_segment(kind, column_id, segment)
            .is_some()
        {
            indexed_rows = indexed_rows.saturating_add(rows);
        } else {
            exact_rows = exact_rows.saturating_add(rows);
        }
    }

    let cpu_ops = match shape {
        TailExactMergeQueryShape::FullText { query_terms, top_k } => {
            let terms = query_terms.max(1) as u64;
            let heap = top_k.max(1) as u64;
            exact_rows
                .saturating_mul(terms)
                .saturating_mul(32)
                .saturating_add(indexed_rows.saturating_mul(terms))
                .saturating_add(heap.saturating_mul(8))
        }
        TailExactMergeQueryShape::Sparse { query_terms, top_k } => {
            let terms = query_terms.max(1) as u64;
            exact_rows
                .saturating_mul(terms)
                .saturating_mul(8)
                .saturating_add(indexed_rows.saturating_mul(terms))
                .saturating_add((top_k.max(1) as u64).saturating_mul(4))
        }
        TailExactMergeQueryShape::Hnsw {
            dimension,
            ef,
            top_k,
        } => {
            let top_k = top_k.max(1) as u64;
            let probe = ef.unwrap_or_else(|| top_k.max(64) as usize).max(1) as u64;
            let exact_probe_factor = (probe / top_k).max(1);
            exact_rows
                .saturating_mul(dimension.max(1) as u64)
                .saturating_mul(exact_probe_factor)
                .saturating_add(indexed_rows.saturating_mul(probe))
        }
    };

    let row_ref_bytes = std::mem::size_of::<PhysicalRowRef>() as u64;
    let memory_bytes = window
        .total_rows()
        .saturating_mul(row_ref_bytes)
        .saturating_add((snapshot.table_lease.visible_segment_count() as u64).saturating_mul(64));
    let io_bytes = window.total_bytes();
    Ok(TailExactMergeCost {
        tail_rows: window.total_rows(),
        tail_bytes: window.total_bytes(),
        cpu_ops,
        memory_bytes,
        io_bytes,
        latency_weight: cpu_ops
            .saturating_div(4)
            .saturating_add(io_bytes.saturating_div(16)),
    })
}

fn coverage_allows_tail_exact_merge(coverage: &CoverageState) -> bool {
    coverage.supports_exact_tail_merge()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::capability::SearchIndexKind;

    #[test]
    fn tail_window_splits_committed_and_own_write_rows() {
        let mut window = TailWindow {
            indexed_through_ts: 10,
            target_ts: 15,
            committed_rows: 7,
            own_write_rows: 3,
            committed_segments: 1,
            own_write_segments: 1,
            committed_bytes: 700,
            own_write_bytes: 300,
        };

        assert_eq!(window.total_rows(), 10);
        assert_eq!(window.total_bytes(), 1000);
        assert_eq!(window.lag_span(), 5);
        assert!(!window.is_empty());

        window.committed_rows = 0;
        window.own_write_rows = 0;
        assert!(window.is_empty());
    }

    #[test]
    fn hnsw_budget_is_stricter_than_sparse_budget() {
        let hnsw = TailExactMergeBudget::for_search_kind(SearchIndexKind::Hnsw);
        let sparse = TailExactMergeBudget::for_search_kind(SearchIndexKind::Sparse);

        assert!(hnsw.max_tail_exact_merge_rows < sparse.max_tail_exact_merge_rows);
        assert!(hnsw
            .first_violation(TailExactMergeCost {
                tail_rows: hnsw.max_tail_exact_merge_rows + 1,
                ..TailExactMergeCost::default()
            })
            .is_some());
        assert!(sparse
            .first_violation(TailExactMergeCost {
                tail_rows: hnsw.max_tail_exact_merge_rows + 1,
                ..TailExactMergeCost::default()
            })
            .is_none());
    }
}
