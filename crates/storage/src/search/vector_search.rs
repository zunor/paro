// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BinaryHeap;

use crate::codec::vector_decoder;
use crate::index::hnsw::types::SearchParams;
use crate::index::hnsw::{DistanceMetric, PreparedQuery};
use crate::index::PredicateTree;
use crate::rowset::SegmentSharedPtr;
use crate::search::row_projection::{materialize_results, snapshot_epoch, ScoredRowRef};
use crate::tablet::{ColumnId, TabletReadGuard, TabletRef};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use rayon::prelude::*;

pub(crate) fn vector_search(
    tablet: &TabletRef,
    column_types: &[LogicalType],
    column_id: usize,
    query: &[f32],
    k: usize,
    params: &SearchParams,
    predicate: Option<&PredicateTree>,
    projected_columns: &[usize],
    use_parallel: bool,
) -> Result<Vec<Chunk>> {
    if k == 0 {
        return Ok(Vec::new());
    }

    let vector_dim = resolve_vector_dim(column_types, column_id)?;
    validate_query_dim(query, vector_dim)?;
    let distance = resolve_distance_metric(tablet, column_id);
    let fallback_query = distance.prepare(query);
    let (snapshot_epoch, _snapshot, segments) = load_visible_segments(tablet)?;

    let storage_col_id = column_id as u32;
    let per_segment_results: Vec<Vec<ScoredRowRef>> = if use_parallel {
        segments
            .par_iter()
            .map(|segment| {
                vector_search_on_segment(
                    segment,
                    storage_col_id,
                    query,
                    k,
                    params,
                    predicate,
                    snapshot_epoch,
                    vector_dim,
                    distance,
                    &fallback_query,
                )
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        let mut results = Vec::with_capacity(segments.len());
        for segment in &segments {
            results.push(vector_search_on_segment(
                segment,
                storage_col_id,
                query,
                k,
                params,
                predicate,
                snapshot_epoch,
                vector_dim,
                distance,
                &fallback_query,
            )?);
        }
        results
    };

    let final_order = merge_segment_top_k(per_segment_results, k);

    materialize_results(tablet, column_types, final_order, projected_columns, false)
}

pub(crate) fn vector_search_many(
    tablet: &TabletRef,
    column_types: &[LogicalType],
    column_id: usize,
    queries: &[&[f32]],
    k: usize,
    params: &SearchParams,
    predicate: Option<&PredicateTree>,
    projected_columns: &[usize],
    use_parallel: bool,
) -> Result<Vec<Vec<Chunk>>> {
    if queries.is_empty() {
        return Ok(Vec::new());
    }
    if k == 0 {
        return Ok(vec![Vec::new(); queries.len()]);
    }

    let vector_dim = resolve_vector_dim(column_types, column_id)?;
    validate_query_dims(queries, vector_dim)?;
    let distance = resolve_distance_metric(tablet, column_id);
    let prepared_queries: Vec<PreparedQuery> = queries
        .iter()
        .map(|query| distance.prepare(query))
        .collect();
    let (snapshot_epoch, _snapshot, segments) = load_visible_segments(tablet)?;

    let storage_col_id = column_id as u32;
    let per_segment_results: Vec<Vec<Vec<ScoredRowRef>>> = if use_parallel {
        segments
            .par_iter()
            .map(|segment| {
                vector_search_many_on_segment(
                    segment,
                    storage_col_id,
                    &prepared_queries,
                    k,
                    params,
                    predicate,
                    snapshot_epoch,
                    vector_dim,
                    distance,
                )
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        let mut results = Vec::with_capacity(segments.len());
        for segment in &segments {
            results.push(vector_search_many_on_segment(
                segment,
                storage_col_id,
                &prepared_queries,
                k,
                params,
                predicate,
                snapshot_epoch,
                vector_dim,
                distance,
            )?);
        }
        results
    };

    let final_orders = merge_segment_top_k_many(per_segment_results, k, prepared_queries.len());
    final_orders
        .into_iter()
        .map(|final_order| {
            materialize_results(tablet, column_types, final_order, projected_columns, false)
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn vector_search_on_segment(
    segment: &SegmentSharedPtr,
    storage_col_id: u32,
    query: &[f32],
    k: usize,
    params: &SearchParams,
    predicate: Option<&PredicateTree>,
    snapshot_epoch: u64,
    vector_dim: usize,
    distance: DistanceMetric,
    prepared_query: &PreparedQuery,
) -> Result<Vec<ScoredRowRef>> {
    if segment.hnsw_index(storage_col_id).is_some() {
        return segment
            .vector_search_with_epoch(storage_col_id, query, k, params, snapshot_epoch, predicate)
            .map(|results| {
                results
                    .into_iter()
                    .map(|point| ScoredRowRef {
                        score: point.score,
                        segment: segment.clone(),
                        row_id: point.idx as u32,
                    })
                    .collect()
            });
    }

    let delete_vector = segment.load_delete_vector_with_epoch(snapshot_epoch)?;
    let mut fallback_iter =
        crate::rowset::segment::SegmentIterator::new_with_delete_vector_and_predicate(
            segment.as_ref(),
            vec![storage_col_id],
            delete_vector,
            predicate.cloned(),
        )?;
    let mut fallback_vector = vec![0.0f32; vector_dim];
    let mut min_heap: BinaryHeap<std::cmp::Reverse<ScoredRowRef>> = BinaryHeap::new();

    while fallback_iter.has_next() {
        let (row_ids, columns) = fallback_iter.next_batch(1024)?;
        if row_ids.is_empty() {
            break;
        }
        let Some((_, batch)) = columns.first() else {
            continue;
        };
        let bytes = &batch.data;

        for (i, row_id) in row_ids.iter().enumerate() {
            if vector_decoder::decode_f32_array_row(
                bytes.as_ref(),
                i,
                vector_dim,
                &mut fallback_vector,
            )
            .is_err()
            {
                continue;
            }
            let point = ScoredRowRef {
                score: distance.similarity(prepared_query.as_slice(), &fallback_vector),
                segment: segment.clone(),
                row_id: *row_id,
            };

            push_top_k(&mut min_heap, point, k);
        }
    }

    Ok(heap_into_sorted_vec(min_heap))
}

#[allow(clippy::too_many_arguments)]
fn vector_search_many_on_segment(
    segment: &SegmentSharedPtr,
    storage_col_id: u32,
    prepared_queries: &[PreparedQuery],
    k: usize,
    params: &SearchParams,
    predicate: Option<&PredicateTree>,
    snapshot_epoch: u64,
    vector_dim: usize,
    distance: DistanceMetric,
) -> Result<Vec<Vec<ScoredRowRef>>> {
    if prepared_queries.is_empty() {
        return Ok(Vec::new());
    }

    if segment.hnsw_index(storage_col_id).is_some() {
        return segment
            .vector_search_batch_with_epoch(
                storage_col_id,
                prepared_queries,
                k,
                params,
                snapshot_epoch,
                predicate,
            )
            .map(|results| {
                results
                    .into_iter()
                    .map(|query_results| {
                        query_results
                            .into_iter()
                            .map(|point| ScoredRowRef {
                                score: point.score,
                                segment: segment.clone(),
                                row_id: point.idx as u32,
                            })
                            .collect()
                    })
                    .collect()
            });
    }

    let delete_vector = segment.load_delete_vector_with_epoch(snapshot_epoch)?;
    let mut fallback_iter =
        crate::rowset::segment::SegmentIterator::new_with_delete_vector_and_predicate(
            segment.as_ref(),
            vec![storage_col_id],
            delete_vector,
            predicate.cloned(),
        )?;
    let mut fallback_vector = vec![0.0f32; vector_dim];
    let mut min_heaps: Vec<BinaryHeap<std::cmp::Reverse<ScoredRowRef>>> =
        prepared_queries.iter().map(|_| BinaryHeap::new()).collect();

    while fallback_iter.has_next() {
        let (row_ids, columns) = fallback_iter.next_batch(1024)?;
        if row_ids.is_empty() {
            break;
        }
        let Some((_, batch)) = columns.first() else {
            continue;
        };
        let bytes = &batch.data;

        for (i, row_id) in row_ids.iter().enumerate() {
            if vector_decoder::decode_f32_array_row(
                bytes.as_ref(),
                i,
                vector_dim,
                &mut fallback_vector,
            )
            .is_err()
            {
                continue;
            }
            for (query_idx, prepared_query) in prepared_queries.iter().enumerate() {
                let point = ScoredRowRef {
                    score: distance.similarity(prepared_query.as_slice(), &fallback_vector),
                    segment: segment.clone(),
                    row_id: *row_id,
                };
                push_top_k(&mut min_heaps[query_idx], point, k);
            }
        }
    }

    Ok(min_heaps.into_iter().map(heap_into_sorted_vec).collect())
}

fn resolve_vector_dim(column_types: &[LogicalType], column_id: usize) -> Result<usize> {
    match column_types.get(column_id) {
        Some(LogicalType::Array(inner, dim)) if matches!(**inner, LogicalType::Float) => Ok(*dim),
        other => Err(paro_error::invalid_input(format!(
            "Column {} is not a dense vector column: {:?}",
            column_id, other
        ))),
    }
}

fn validate_query_dim(query: &[f32], vector_dim: usize) -> Result<()> {
    if query.len() != vector_dim {
        return Err(paro_error::invalid_input(format!(
            "Query vector dimension mismatch: expected {}, got {}",
            vector_dim,
            query.len()
        )));
    }
    Ok(())
}

fn validate_query_dims(queries: &[&[f32]], vector_dim: usize) -> Result<()> {
    for (idx, query) in queries.iter().enumerate() {
        if query.len() != vector_dim {
            return Err(paro_error::invalid_input(format!(
                "query[{idx}] dimension mismatch: expected {}, got {}",
                vector_dim,
                query.len()
            )));
        }
    }
    Ok(())
}

fn resolve_distance_metric(tablet: &TabletRef, column_id: usize) -> DistanceMetric {
    tablet
        .schema()
        .and_then(|schema| {
            schema
                .column_by_id(column_id as ColumnId)
                .map(|col| DistanceMetric::from_u8(col.hnsw_distance))
        })
        .unwrap_or(DistanceMetric::Euclidean)
}

fn load_visible_segments(
    tablet: &TabletRef,
) -> Result<(u64, TabletReadGuard, Vec<SegmentSharedPtr>)> {
    let version = tablet.max_version();
    let snapshot = TabletReadGuard::pin(tablet, version);
    let rowsets = tablet.capture_consistent_rowsets(version)?;
    let mut segments = Vec::new();
    for rowset in &rowsets {
        rowset.load()?;
        segments.extend(rowset.segments());
    }
    Ok((snapshot_epoch(version), snapshot, segments))
}

fn push_top_k(
    min_heap: &mut BinaryHeap<std::cmp::Reverse<ScoredRowRef>>,
    point: ScoredRowRef,
    k: usize,
) {
    if min_heap.len() < k {
        min_heap.push(std::cmp::Reverse(point));
    } else if let Some(peek) = min_heap.peek() {
        if point > peek.0 {
            min_heap.pop();
            min_heap.push(std::cmp::Reverse(point));
        }
    }
}

fn heap_into_sorted_vec(
    min_heap: BinaryHeap<std::cmp::Reverse<ScoredRowRef>>,
) -> Vec<ScoredRowRef> {
    min_heap
        .into_sorted_vec()
        .into_iter()
        .map(|entry| entry.0)
        .collect()
}

fn merge_segment_top_k(per_segment_results: Vec<Vec<ScoredRowRef>>, k: usize) -> Vec<ScoredRowRef> {
    let mut min_heap = BinaryHeap::new();
    for segment_results in per_segment_results {
        for point in segment_results {
            push_top_k(&mut min_heap, point, k);
        }
    }
    heap_into_sorted_vec(min_heap)
}

fn merge_segment_top_k_many(
    per_segment_results: Vec<Vec<Vec<ScoredRowRef>>>,
    k: usize,
    num_queries: usize,
) -> Vec<Vec<ScoredRowRef>> {
    let mut heaps: Vec<BinaryHeap<std::cmp::Reverse<ScoredRowRef>>> =
        (0..num_queries).map(|_| BinaryHeap::new()).collect();
    for segment_results in per_segment_results {
        for (query_idx, query_results) in segment_results.into_iter().enumerate() {
            for point in query_results {
                push_top_k(&mut heaps[query_idx], point, k);
            }
        }
    }
    heaps.into_iter().map(heap_into_sorted_vec).collect()
}
