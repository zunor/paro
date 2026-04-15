use super::segment::Segment;
use crate::index::fulltext::query_parser::ParsedQuery;
use crate::index::fulltext::text_index::GlobalFullTextStats;
use crate::index::hnsw::{PreparedQuery, ScoredPoint, SearchParams};
use crate::index::{IndexEvaluator, PredicateResult, PredicateTree};
use crate::primary_key::DeleteVector;
use crate::rowset::sparse_vector::SparseVector;
use crate::tablet::ColumnId;
use paro_common::error::{self as paro_error, Result};
use roaring::RoaringBitmap;

impl Segment {
    /// Perform a vector search on this segment.
    pub fn vector_search(
        &self,
        column_id: ColumnId,
        query: &[f32],
        top_k: usize,
        params: &SearchParams,
        predicate_tree: Option<&PredicateTree>,
    ) -> Result<Vec<ScoredPoint>> {
        self.vector_search_with_epoch(
            column_id,
            query,
            top_k,
            params,
            self.rowset_gen,
            predicate_tree,
        )
    }

    /// Perform a vector search on this segment using a snapshot epoch.
    pub(crate) fn vector_search_with_epoch(
        &self,
        column_id: ColumnId,
        query: &[f32],
        top_k: usize,
        params: &SearchParams,
        snapshot_epoch: u64,
        predicate_tree: Option<&PredicateTree>,
    ) -> Result<Vec<ScoredPoint>> {
        let index = self
            .hnsw_index(column_id)
            .ok_or_else(|| paro_error::object_not_found("HNSW index", column_id.to_string()))?;
        let filter_bitmap = self.build_filter_bitmap_with_epoch(snapshot_epoch, predicate_tree)?;
        if let Some(bm) = filter_bitmap.as_ref() {
            if bm.is_empty() {
                return Ok(Vec::new());
            }
        }
        index.search_one(query, top_k, params, filter_bitmap.as_ref())
    }

    /// Perform a batched vector search on this segment.
    pub fn vector_search_batch(
        &self,
        column_id: ColumnId,
        queries: &[PreparedQuery],
        top_k: usize,
        params: &SearchParams,
        predicate_tree: Option<&PredicateTree>,
    ) -> Result<Vec<Vec<ScoredPoint>>> {
        self.vector_search_batch_with_epoch(
            column_id,
            queries,
            top_k,
            params,
            self.rowset_gen,
            predicate_tree,
        )
    }

    /// Perform a batched vector search on this segment using a snapshot epoch.
    pub(crate) fn vector_search_batch_with_epoch(
        &self,
        column_id: ColumnId,
        queries: &[PreparedQuery],
        top_k: usize,
        params: &SearchParams,
        snapshot_epoch: u64,
        predicate_tree: Option<&PredicateTree>,
    ) -> Result<Vec<Vec<ScoredPoint>>> {
        if queries.is_empty() {
            return Ok(Vec::new());
        }

        let index = self
            .hnsw_index(column_id)
            .ok_or_else(|| paro_error::object_not_found("HNSW index", column_id.to_string()))?;
        let filter_bitmap = self.build_filter_bitmap_with_epoch(snapshot_epoch, predicate_tree)?;
        if let Some(bm) = filter_bitmap.as_ref() {
            if bm.is_empty() {
                return Ok(vec![Vec::new(); queries.len()]);
            }
        }
        index.search_many_prepared(queries, top_k, params, filter_bitmap.as_ref())
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
        global_stats: Option<&GlobalFullTextStats>,
    ) -> Result<Vec<ScoredPoint>> {
        self.fulltext_search_with_epoch(
            column_id,
            query,
            top_k,
            self.rowset_gen,
            predicate_tree,
            global_stats,
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
        global_stats: Option<&GlobalFullTextStats>,
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
        Ok(index.search(query, top_k, filter_bitmap.as_ref(), global_stats))
    }

    /// Parse and perform a full-text search on this segment.
    pub fn fulltext_search_text(
        &self,
        column_id: ColumnId,
        query_text: &str,
        top_k: usize,
        predicate_tree: Option<&PredicateTree>,
        global_stats: Option<&GlobalFullTextStats>,
    ) -> Result<Vec<ScoredPoint>> {
        self.fulltext_search_text_with_epoch(
            column_id,
            query_text,
            top_k,
            self.rowset_gen,
            predicate_tree,
            global_stats,
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
        global_stats: Option<&GlobalFullTextStats>,
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
            global_stats,
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

    pub(crate) fn build_filter_bitmap_with_delete_vector(
        &self,
        predicate_tree: Option<&PredicateTree>,
        delete_vector: Option<&DeleteVector>,
    ) -> Result<Option<RoaringBitmap>> {
        let mut filter_bitmap = None;
        if let Some(tree) = predicate_tree {
            let evaluator = IndexEvaluator::new(self.predicate_indexes());
            let result = evaluator.evaluate(tree);

            match result {
                PredicateResult::Bitmap(bitmap) => {
                    filter_bitmap = Some(bitmap);
                }
                PredicateResult::AllMatch => {}
                PredicateResult::NoneMatch => return Ok(Some(RoaringBitmap::new())),
                PredicateResult::PageRanges(ranges) => {
                    let mut bitmap = RoaringBitmap::new();
                    for range in ranges {
                        if range.start_row < range.end_row {
                            bitmap.insert_range(range.start_row..range.end_row);
                        }
                    }
                    filter_bitmap = Some(bitmap);
                }
                PredicateResult::Unknown => {}
            }
        }

        let combined_filter = if let Some(mut bitmap) = filter_bitmap {
            if let Some(dv) = delete_vector {
                bitmap -= dv.bitmap();
            }
            Some(bitmap)
        } else if let Some(dv) = delete_vector {
            let mut bitmap = RoaringBitmap::new();
            for i in 0..self.num_rows() as u32 {
                if !dv.is_deleted(i) {
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
