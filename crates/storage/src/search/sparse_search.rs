// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BinaryHeap;

use crate::index::PredicateTree;
use crate::rowset::SparseVector;
use crate::search::row_projection::{materialize_results, snapshot_epoch, ScoredRowRef};
use crate::tablet::{TabletReadGuard, TabletRef};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

pub(crate) fn sparse_vector_search(
    tablet: &TabletRef,
    column_types: &[LogicalType],
    column_id: usize,
    query: &SparseVector,
    k: usize,
    predicate: Option<&PredicateTree>,
    projected_columns: &[usize],
) -> Result<Vec<Chunk>> {
    let version = tablet.max_version();
    let _snapshot = TabletReadGuard::pin(tablet, version);
    let snapshot_epoch = snapshot_epoch(version);
    let rowsets = tablet.capture_consistent_rowsets(version)?;
    let mut min_heap: BinaryHeap<std::cmp::Reverse<ScoredRowRef>> = BinaryHeap::new();

    for rowset in &rowsets {
        rowset.load()?;
        for segment in rowset.segments() {
            let results = segment.sparse_vector_search_with_epoch(
                column_id as u32,
                query,
                k,
                snapshot_epoch,
                predicate,
            )?;

            for point in results {
                let scored = ScoredRowRef {
                    score: point.score,
                    segment: segment.clone(),
                    row_id: point.idx as u32,
                };

                if min_heap.len() < k {
                    min_heap.push(std::cmp::Reverse(scored));
                } else if let Some(peek) = min_heap.peek() {
                    if scored.score > peek.0.score {
                        min_heap.pop();
                        min_heap.push(std::cmp::Reverse(scored));
                    }
                }
            }
        }
    }

    let final_order: Vec<ScoredRowRef> = min_heap
        .into_sorted_vec()
        .into_iter()
        .map(|entry| entry.0)
        .collect();

    materialize_results(tablet, column_types, final_order, projected_columns, false)
}
