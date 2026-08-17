// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared bounded-heap merge helpers for search top-k providers.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use super::cursor::{CandidateBatch, PhysicalRowRef};
use super::posting_stream::PostingPruningHint;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RankedRow {
    pub(crate) row: PhysicalRowRef,
    pub(crate) score: f32,
}

impl RankedRow {
    pub(crate) const fn new(row: PhysicalRowRef, score: f32) -> Self {
        Self { row, score }
    }
}

impl PartialEq for RankedRow {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RankedRow {}

impl PartialOrd for RankedRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedRow {
    fn cmp(&self, other: &Self) -> Ordering {
        // Equal scores fall back to physical row address so ordering is stable,
        // but this remains a semantics-free tie-break rather than a user-visible sort key.
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.row.rowset_id.cmp(&other.row.rowset_id))
            .then_with(|| self.row.segment_id.cmp(&other.row.segment_id))
            .then_with(|| self.row.row_offset.cmp(&other.row.row_offset))
    }
}

#[derive(Debug)]
pub(crate) struct TopKCollector {
    limit: usize,
    heap: BinaryHeap<Reverse<RankedRow>>,
}

impl TopKCollector {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::new(),
        }
    }

    pub(crate) fn push(&mut self, row: RankedRow) {
        if self.limit == 0 {
            return;
        }

        if self.heap.len() < self.limit {
            self.heap.push(Reverse(row));
            return;
        }

        if let Some(peek) = self.heap.peek() {
            if row > peek.0 {
                self.heap.pop();
                self.heap.push(Reverse(row));
            }
        }
    }

    pub(crate) fn extend<I>(&mut self, rows: I)
    where
        I: IntoIterator<Item = RankedRow>,
    {
        for row in rows {
            self.push(row);
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.heap.len()
    }

    #[allow(dead_code)]
    pub(crate) fn min_competitive_score(&self) -> Option<f32> {
        if self.limit == 0 || self.heap.len() < self.limit {
            return None;
        }
        self.heap.peek().map(|entry| entry.0.score)
    }

    #[allow(dead_code)]
    pub(crate) fn pruning_hint(&self, remaining_limit: Option<usize>) -> PostingPruningHint {
        PostingPruningHint::new(self.min_competitive_score(), remaining_limit)
    }

    pub(crate) fn into_sorted_rows(self) -> Vec<RankedRow> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|entry| entry.0)
            .collect()
    }
}

pub(crate) fn ranked_rows_to_batch(rows: Vec<RankedRow>) -> CandidateBatch {
    let mut physical_rows = Vec::with_capacity(rows.len());
    let mut scores = Vec::with_capacity(rows.len());
    for row in rows {
        physical_rows.push(row.row);
        scores.push(row.score);
    }
    CandidateBatch {
        rows: physical_rows,
        scores,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topk_collector_exposes_min_competitive_score_when_full() {
        let mut collector = TopKCollector::new(2);
        assert_eq!(collector.min_competitive_score(), None);

        collector.push(RankedRow::new(
            PhysicalRowRef::new(1, 0, crate::rowset::SegmentRowId::from_raw(0)),
            0.2,
        ));
        assert_eq!(collector.min_competitive_score(), None);

        collector.push(RankedRow::new(
            PhysicalRowRef::new(1, 0, crate::rowset::SegmentRowId::from_raw(1)),
            0.4,
        ));
        assert_eq!(collector.min_competitive_score(), Some(0.2));

        collector.push(RankedRow::new(
            PhysicalRowRef::new(1, 0, crate::rowset::SegmentRowId::from_raw(2)),
            0.1,
        ));
        assert_eq!(collector.min_competitive_score(), Some(0.2));

        collector.push(RankedRow::new(
            PhysicalRowRef::new(1, 0, crate::rowset::SegmentRowId::from_raw(3)),
            0.6,
        ));
        assert_eq!(collector.min_competitive_score(), Some(0.4));

        let hint = collector.pruning_hint(Some(16));
        assert_eq!(hint.min_competitive_score, Some(0.4));
        assert_eq!(hint.remaining_limit, Some(16));
    }
}
