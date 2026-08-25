// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::segment::Segment;
use crate::index::fulltext::query_parser::ParsedQuery;
use crate::index::fulltext::scoring::FullTextScoreMode;
use crate::index::fulltext::text_index::FullTextScoringStats;
use crate::index::hnsw::{
    HnswQueryWideStrategy, HnswSearchFilter, HnswSearchPolicy, HnswSearchResult,
    HnswSearchStrategy, HnswSegmentSearchInput, ScoredPoint, SearchParams,
};
use crate::index::ExactRowSet;
use crate::index::{collect_predicate_columns, IndexEvaluator, PredicateResult, PredicateTree};
use crate::primary_key::DeleteVector;
use crate::rowset::sparse_vector::SparseVector;
use crate::rowset::{SegmentIterator, SegmentRowId};
use crate::tablet::ColumnId;
use paro_common::error::{self as paro_error, Result};
use roaring::RoaringBitmap;
use std::sync::Arc;

impl Segment {
    fn build_exact_predicate_bitmap(
        &self,
        predicate_tree: &PredicateTree,
    ) -> Result<RoaringBitmap> {
        let evaluator = IndexEvaluator::for_segment(self.predicate_indexes()?, self.num_rows());
        let evaluation = evaluator.evaluate_with_proof(predicate_tree);

        if evaluation.is_exact() {
            return predicate_result_to_bitmap(evaluation.candidates, self.num_rows());
        }

        // Candidate-only indexes (zone maps, bloom filters, or an index that
        // cannot cover the complete predicate tree) are pruning hints, never
        // an authorization to weaken SQL semantics. Materialize the exact
        // segment-local selection once, then let every search provider consume
        // the same immutable bitmap contract.
        let mut iter = SegmentIterator::new_with_delete_vector_and_predicate(
            self,
            Vec::new(),
            None,
            Some(predicate_tree.clone()),
        )?;
        let mut bitmap = RoaringBitmap::new();
        loop {
            let (row_ids, _) = iter.next_batch(paro_common::vector::VECTOR_SIZE)?;
            if row_ids.is_empty() {
                break;
            }
            bitmap.extend(row_ids.into_iter().map(SegmentRowId::get));
        }
        Ok(bitmap)
    }

    /// Perform a vector search on this segment.
    pub fn vector_search(
        &self,
        column_id: ColumnId,
        query: &[f32],
        top_k: usize,
        params: &SearchParams,
        policy: &HnswSearchPolicy,
        predicate_tree: Option<&PredicateTree>,
    ) -> Result<Vec<ScoredPoint>> {
        self.vector_search_with_epoch(
            column_id,
            query,
            top_k,
            params,
            policy,
            self.rowset_gen,
            predicate_tree,
        )
        .map(|result| result.points)
    }

    /// Perform a vector search on this segment using a snapshot epoch.
    pub(crate) fn vector_search_with_epoch(
        &self,
        column_id: ColumnId,
        query: &[f32],
        top_k: usize,
        params: &SearchParams,
        policy: &HnswSearchPolicy,
        snapshot_epoch: u64,
        predicate_tree: Option<&PredicateTree>,
    ) -> Result<HnswSearchResult> {
        let filter_row_set = self.build_hnsw_filter_with_epoch(snapshot_epoch, predicate_tree)?;
        let predicate_columns = predicate_tree
            .map(collect_predicate_columns)
            .unwrap_or_default();
        let filter = match (predicate_tree.is_some(), filter_row_set.as_deref()) {
            (_, None) => HnswSearchFilter::None,
            (true, Some(row_set)) => HnswSearchFilter::predicate(row_set, &predicate_columns),
            (false, Some(row_set)) => HnswSearchFilter::Visibility(row_set),
        };
        let matching_rows = filter.row_set().map_or(self.num_rows(), ExactRowSet::len);
        let index = self
            .open_hnsw_index(column_id)?
            .ok_or_else(|| paro_error::object_not_found("HNSW index", column_id.to_string()))?;
        let query_strategy =
            HnswQueryWideStrategy::choose(filter.kind(), matching_rows, self.num_rows(), *policy);
        let strategy = query_strategy.for_segment(HnswSegmentSearchInput {
            filter_kind: filter.kind(),
            matching_rows,
            total_rows: self.num_rows(),
            effective_ef: policy.effective_ef(top_k, params.ef),
            level0_degree: index.build_contract.m0 as usize,
            vector_dimension: index.vector_storage.vector_dim(),
            parallelism: 1,
        });
        let budget = crate::search::ResourceBudget::default();
        index.search_one_with_policy_strategy(
            query, top_k, params, filter, policy, strategy, &budget,
        )
    }

    /// Search with an already prepared exact segment-local filter. Query
    /// providers choose the physical strategy from the same local cardinality,
    /// avoiding both duplicate predicate work and policy drift inside HNSW.
    pub(crate) fn vector_search_with_filter_strategy(
        &self,
        column_id: ColumnId,
        query: &[f32],
        top_k: usize,
        params: &SearchParams,
        filter: HnswSearchFilter<'_>,
        policy: &HnswSearchPolicy,
        strategy: HnswSearchStrategy,
        budget: &crate::search::ResourceBudget,
    ) -> Result<HnswSearchResult> {
        let index = self
            .open_hnsw_index(column_id)?
            .ok_or_else(|| paro_error::object_not_found("HNSW index", column_id.to_string()))?;
        if let Some(row_set) = filter.row_set() {
            if row_set.is_empty() {
                return Ok(HnswSearchResult {
                    points: Vec::new(),
                    scored_points: 0,
                    outcome: crate::index::hnsw::HnswSearchOutcome::new(
                        crate::index::hnsw::HnswSearchPath::ExactScan(
                            crate::index::hnsw::HnswExactScanKind::BaseVectors,
                        ),
                    ),
                });
            }
        }
        index
            .search_one_with_policy_strategy(query, top_k, params, filter, policy, strategy, budget)
    }

    /// Perform a sparse vector search on this segment.
    pub fn sparse_vector_search(
        &self,
        column_id: ColumnId,
        query: &SparseVector,
        top_k: usize,
        predicate_tree: Option<&PredicateTree>,
    ) -> Result<Vec<ScoredPoint>> {
        self.sparse_vector_search_with_epoch(
            column_id,
            query,
            top_k,
            self.rowset_gen,
            predicate_tree,
        )
    }

    /// Perform a sparse vector search on this segment using a snapshot epoch.
    pub(crate) fn sparse_vector_search_with_epoch(
        &self,
        column_id: ColumnId,
        query: &SparseVector,
        top_k: usize,
        snapshot_epoch: u64,
        predicate_tree: Option<&PredicateTree>,
    ) -> Result<Vec<ScoredPoint>> {
        let index = self
            .sparse_index(column_id)
            .ok_or_else(|| paro_error::object_not_found("Sparse index", column_id.to_string()))?;
        let filter_bitmap = self.build_filter_bitmap_with_epoch(snapshot_epoch, predicate_tree)?;
        if let Some(bm) = filter_bitmap.as_ref() {
            if bm.is_empty() {
                return Ok(Vec::new());
            }
        }
        index.search(query, top_k, filter_bitmap.as_ref())
    }

    /// Perform a full-text filter on this segment (returns matching bitmap).
    pub fn fulltext_filter(
        &self,
        column_id: ColumnId,
        query: &ParsedQuery,
        predicate_tree: Option<&PredicateTree>,
    ) -> Result<RoaringBitmap> {
        self.fulltext_filter_with_epoch(column_id, query, self.rowset_gen, predicate_tree)
    }

    /// Perform a full-text filter on this segment (returns matching bitmap).
    pub(crate) fn fulltext_filter_with_epoch(
        &self,
        column_id: ColumnId,
        query: &ParsedQuery,
        snapshot_epoch: u64,
        predicate_tree: Option<&PredicateTree>,
    ) -> Result<RoaringBitmap> {
        let index = self
            .fulltext_index(column_id)
            .ok_or_else(|| paro_error::object_not_found("FullText index", column_id.to_string()))?;
        let filter_bitmap = self.build_filter_bitmap_with_epoch(snapshot_epoch, predicate_tree)?;
        if let Some(bm) = filter_bitmap.as_ref() {
            if bm.is_empty() {
                return Ok(RoaringBitmap::new());
            }
        }
        Ok(index.filter(query, filter_bitmap.as_ref()))
    }

    /// Perform a full-text search on this segment.
    pub fn fulltext_search(
        &self,
        column_id: ColumnId,
        query: &ParsedQuery,
        top_k: usize,
        predicate_tree: Option<&PredicateTree>,
        scoring_stats: Option<&FullTextScoringStats>,
        score_mode: FullTextScoreMode,
    ) -> Result<Vec<ScoredPoint>> {
        self.fulltext_search_with_epoch(
            column_id,
            query,
            top_k,
            self.rowset_gen,
            predicate_tree,
            scoring_stats,
            score_mode,
        )
    }

    /// Perform a full-text search on this segment.
    pub(crate) fn fulltext_search_with_epoch(
        &self,
        column_id: ColumnId,
        query: &ParsedQuery,
        top_k: usize,
        snapshot_epoch: u64,
        predicate_tree: Option<&PredicateTree>,
        scoring_stats: Option<&FullTextScoringStats>,
        score_mode: FullTextScoreMode,
    ) -> Result<Vec<ScoredPoint>> {
        let index = self
            .fulltext_index(column_id)
            .ok_or_else(|| paro_error::object_not_found("FullText index", column_id.to_string()))?;
        let filter_bitmap = self.build_filter_bitmap_with_epoch(snapshot_epoch, predicate_tree)?;
        if let Some(bm) = filter_bitmap.as_ref() {
            if bm.is_empty() {
                return Ok(Vec::new());
            }
        }
        Ok(index.search(
            query,
            top_k,
            filter_bitmap.as_ref(),
            scoring_stats,
            score_mode,
        ))
    }

    /// Parse and perform a full-text search on this segment.
    pub fn fulltext_search_text(
        &self,
        column_id: ColumnId,
        query_text: &str,
        top_k: usize,
        predicate_tree: Option<&PredicateTree>,
        scoring_stats: Option<&FullTextScoringStats>,
    ) -> Result<Vec<ScoredPoint>> {
        self.fulltext_search_text_with_epoch(
            column_id,
            query_text,
            top_k,
            self.rowset_gen,
            predicate_tree,
            scoring_stats,
            FullTextScoreMode::Bm25,
        )
    }

    /// Parse and perform a full-text search on this segment using a snapshot epoch.
    pub(crate) fn fulltext_search_text_with_epoch(
        &self,
        column_id: ColumnId,
        query_text: &str,
        top_k: usize,
        snapshot_epoch: u64,
        predicate_tree: Option<&PredicateTree>,
        scoring_stats: Option<&FullTextScoringStats>,
        score_mode: FullTextScoreMode,
    ) -> Result<Vec<ScoredPoint>> {
        let index = self
            .fulltext_index(column_id)
            .ok_or_else(|| paro_error::object_not_found("FullText index", column_id.to_string()))?;
        let query = index.parse_query(query_text)?;
        self.fulltext_search_with_epoch(
            column_id,
            &query,
            top_k,
            snapshot_epoch,
            predicate_tree,
            scoring_stats,
            score_mode,
        )
    }

    /// Build filter bitmap from scalar predicates and delete vector at snapshot epoch.
    pub(crate) fn build_filter_bitmap_with_epoch(
        &self,
        snapshot_epoch: u64,
        predicate_tree: Option<&PredicateTree>,
    ) -> Result<Option<RoaringBitmap>> {
        let delete_vector = self.load_delete_vector_with_epoch(snapshot_epoch)?;
        self.build_filter_bitmap_with_delete_vector(predicate_tree, delete_vector.as_ref())
    }

    /// Prepare an exact HNSW admission set. A complete scalar access path keeps
    /// its native membership and posting representation for both graph
    /// admission and exact scans; generic predicate trees retain bitmap
    /// algebra until native row sets become composable.
    pub(crate) fn build_hnsw_filter_with_epoch(
        &self,
        snapshot_epoch: u64,
        predicate_tree: Option<&PredicateTree>,
    ) -> Result<Option<Arc<dyn ExactRowSet>>> {
        let delete_vector = self.load_delete_vector_with_epoch(snapshot_epoch)?;
        if delete_vector.is_none() {
            if let Some(tree) = predicate_tree {
                let evaluator =
                    IndexEvaluator::for_segment(self.predicate_indexes()?, self.num_rows());
                if let Some(row_set) = evaluator.compile_exact_row_set(tree) {
                    return Ok(Some(row_set));
                }
            }
        }

        self.build_filter_bitmap_with_delete_vector(predicate_tree, delete_vector.as_ref())
            .map(|bitmap| bitmap.map(|bitmap| Arc::new(bitmap) as Arc<dyn ExactRowSet>))
    }

    pub(crate) fn build_filter_bitmap_with_delete_vector(
        &self,
        predicate_tree: Option<&PredicateTree>,
        delete_vector: Option<&DeleteVector>,
    ) -> Result<Option<RoaringBitmap>> {
        let mut filter_bitmap = None;
        if let Some(tree) = predicate_tree {
            filter_bitmap = Some(self.build_exact_predicate_bitmap(tree)?);
        }

        let combined_filter = if let Some(mut bitmap) = filter_bitmap {
            if let Some(dv) = delete_vector {
                bitmap -= dv.bitmap();
            }
            Some(bitmap)
        } else if let Some(dv) = delete_vector {
            let mut bitmap = RoaringBitmap::new();
            for i in 0..self.num_rows() as u32 {
                if !dv.is_deleted(SegmentRowId::from_raw(i)) {
                    bitmap.insert(i);
                }
            }
            Some(bitmap)
        } else {
            None
        };

        Ok(combined_filter)
    }
}

fn predicate_result_to_bitmap(result: PredicateResult, num_rows: u64) -> Result<RoaringBitmap> {
    Ok(match result {
        PredicateResult::AllMatch => {
            let row_count = u32::try_from(num_rows).map_err(|_| {
                paro_error::data_corrupted("segment row count exceeds exact-filter bitmap domain")
            })?;
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert_range(0..row_count);
            bitmap
        }
        PredicateResult::Unknown => {
            return Err(paro_error::internal(
                "exact predicate proof resolved to an unknown candidate set",
            ))
        }
        PredicateResult::NoneMatch => RoaringBitmap::new(),
        PredicateResult::Bitmap(bitmap) => bitmap,
        PredicateResult::PageRanges(ranges) => {
            let mut bitmap = RoaringBitmap::new();
            for range in ranges {
                if range.start_row < range.end_row {
                    bitmap.insert_range(range.start_row..range.end_row);
                }
            }
            bitmap
        }
    })
}
