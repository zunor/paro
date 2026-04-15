use std::collections::BinaryHeap;

use crate::index::fulltext::query_parser::ParsedQuery;
use crate::index::fulltext::text_index::GlobalFullTextStats;
use crate::index::PredicateTree;
use crate::search::row_projection::{materialize_results, snapshot_epoch, ScoredRowRef};
use crate::tablet::{TabletReadGuard, TabletRef};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

fn finalize_fulltext_heap(
    min_heap: BinaryHeap<std::cmp::Reverse<ScoredRowRef>>,
) -> Vec<ScoredRowRef> {
    min_heap
        .into_sorted_vec()
        .into_iter()
        .map(|entry| entry.0)
        .collect()
}

pub(crate) fn fulltext_search(
    tablet: &TabletRef,
    column_types: &[LogicalType],
    column_id: usize,
    query_text: &str,
    k: usize,
    predicate: Option<&PredicateTree>,
    projected_columns: &[usize],
    global_stats: Option<&GlobalFullTextStats>,
) -> Result<Vec<Chunk>> {
    let version = tablet.max_version();
    let _snapshot = TabletReadGuard::pin(tablet, version);
    let snapshot_epoch = snapshot_epoch(version);
    let rowsets = tablet.capture_consistent_rowsets(version)?;
    let mut min_heap: BinaryHeap<std::cmp::Reverse<ScoredRowRef>> = BinaryHeap::new();

    for rowset in &rowsets {
        rowset.load()?;
        for segment in rowset.segments() {
            let results = segment.fulltext_search_text_with_epoch(
                column_id as u32,
                query_text,
                k,
                snapshot_epoch,
                predicate,
                global_stats,
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

    materialize_results(
        tablet,
        column_types,
        finalize_fulltext_heap(min_heap),
        projected_columns,
        false,
    )
}

pub(crate) fn fulltext_filter(
    tablet: &TabletRef,
    column_types: &[LogicalType],
    column_id: usize,
    query: &ParsedQuery,
    predicate: Option<&PredicateTree>,
    projected_columns: &[usize],
) -> Result<Vec<Chunk>> {
    let version = tablet.max_version();
    let _snapshot = TabletReadGuard::pin(tablet, version);
    let snapshot_epoch = snapshot_epoch(version);
    let rowsets = tablet.capture_consistent_rowsets(version)?;
    let mut matched_rows = Vec::new();

    for rowset in &rowsets {
        rowset.load()?;
        for segment in rowset.segments() {
            let bitmap = segment.fulltext_filter_with_epoch(
                column_id as u32,
                query,
                snapshot_epoch,
                predicate,
            )?;

            for row_id in bitmap.iter() {
                matched_rows.push(ScoredRowRef {
                    score: 0.0,
                    segment: segment.clone(),
                    row_id,
                });
            }
        }
    }

    materialize_results(tablet, column_types, matched_rows, projected_columns, false)
}

pub(crate) fn fulltext_search_parsed(
    tablet: &TabletRef,
    column_types: &[LogicalType],
    column_id: usize,
    query: &ParsedQuery,
    k: usize,
    predicate: Option<&PredicateTree>,
    projected_columns: &[usize],
    global_stats: Option<&GlobalFullTextStats>,
    emit_score: bool,
) -> Result<Vec<Chunk>> {
    let version = tablet.max_version();
    let _snapshot = TabletReadGuard::pin(tablet, version);
    let snapshot_epoch = snapshot_epoch(version);
    let rowsets = tablet.capture_consistent_rowsets(version)?;
    let mut min_heap: BinaryHeap<std::cmp::Reverse<ScoredRowRef>> = BinaryHeap::new();

    for rowset in &rowsets {
        rowset.load()?;
        for segment in rowset.segments() {
            let results = segment.fulltext_search_with_epoch(
                column_id as u32,
                query,
                k,
                snapshot_epoch,
                predicate,
                global_stats,
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

    materialize_results(
        tablet,
        column_types,
        finalize_fulltext_heap(min_heap),
        projected_columns,
        emit_score,
    )
}
