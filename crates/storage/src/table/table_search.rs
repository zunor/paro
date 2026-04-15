// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::table_handle::TableHandle;
use crate::index::fulltext::query_parser::ParsedQuery;
use crate::index::fulltext::text_index::GlobalFullTextStats;
use crate::index::hnsw::types::SearchParams;
use crate::index::PredicateTree;
use crate::rowset::SparseVector;
use crate::search::fulltext_search;
use crate::search::sparse_search;
use crate::search::vector_search;
use paro_common::chunk::Chunk;
use paro_common::error::Result;

impl TableHandle {
    /// Perform vector search across all rowsets/segments.
    pub fn vector_search(
        &self,
        column_id: usize,
        query: &[f32],
        k: usize,
        params: &SearchParams,
        predicate: Option<&PredicateTree>,
        projected_columns: &[usize],
    ) -> Result<Vec<Chunk>> {
        vector_search::vector_search(
            &self.tablet(),
            self.types(),
            column_id,
            query,
            k,
            params,
            predicate,
            projected_columns,
            true,
        )
    }

    /// Perform batched vector search across all rowsets/segments.
    pub fn vector_search_many(
        &self,
        column_id: usize,
        queries: &[&[f32]],
        k: usize,
        params: &SearchParams,
        predicate: Option<&PredicateTree>,
        projected_columns: &[usize],
    ) -> Result<Vec<Vec<Chunk>>> {
        vector_search::vector_search_many(
            &self.tablet(),
            self.types(),
            column_id,
            queries,
            k,
            params,
            predicate,
            projected_columns,
            true,
        )
    }

    /// Perform sparse vector search across all rowsets/segments.
    pub fn sparse_vector_search(
        &self,
        column_id: usize,
        query: &SparseVector,
        k: usize,
        predicate: Option<&PredicateTree>,
        projected_columns: &[usize],
    ) -> Result<Vec<Chunk>> {
        sparse_search::sparse_vector_search(
            &self.tablet(),
            self.types(),
            column_id,
            query,
            k,
            predicate,
            projected_columns,
        )
    }

    /// Perform full-text search across all rowsets/segments.
    pub fn fulltext_search(
        &self,
        column_id: usize,
        query_text: &str,
        k: usize,
        predicate: Option<&PredicateTree>,
        projected_columns: &[usize],
        global_stats: Option<&GlobalFullTextStats>,
    ) -> Result<Vec<Chunk>> {
        fulltext_search::fulltext_search(
            &self.tablet(),
            self.types(),
            column_id,
            query_text,
            k,
            predicate,
            projected_columns,
            global_stats,
        )
    }

    /// Perform full-text filter across all rowsets/segments.
    pub fn fulltext_filter(
        &self,
        column_id: usize,
        query: &ParsedQuery,
        predicate: Option<&PredicateTree>,
        projected_columns: &[usize],
    ) -> Result<Vec<Chunk>> {
        fulltext_search::fulltext_filter(
            &self.tablet(),
            self.types(),
            column_id,
            query,
            predicate,
            projected_columns,
        )
    }

    /// Perform full-text search using a pre-parsed query.
    pub fn fulltext_search_parsed(
        &self,
        column_id: usize,
        query: &ParsedQuery,
        k: usize,
        predicate: Option<&PredicateTree>,
        projected_columns: &[usize],
        global_stats: Option<&GlobalFullTextStats>,
        emit_score: bool,
    ) -> Result<Vec<Chunk>> {
        fulltext_search::fulltext_search_parsed(
            &self.tablet(),
            self.types(),
            column_id,
            query,
            k,
            predicate,
            projected_columns,
            global_stats,
            emit_score,
        )
    }
}
