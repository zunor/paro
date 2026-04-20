// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::table_handle::TableHandle;
use crate::index::fulltext::query_parser::ParsedQuery;
use crate::index::fulltext::scoring::FullTextScoreMode;
use crate::index::fulltext::text_index::GlobalFullTextStats;
use crate::index::hnsw::types::SearchParams;
use crate::index::PredicateTree;
use crate::rowset::SparseVector;
use crate::search::cursor::{CandidateBatch, GenerationReadLease, OpenedSearchCursor};
use crate::search::fulltext_search::{FullTextFilterProvider, FullTextTopKProvider};
use crate::search::row_fetch::materialize_candidate_batch;
use crate::search::sparse_search::SparseSearchProvider;
use crate::search::vector_search::VectorSearchProvider;
use crate::search::{SearchReadSnapshot, TableReadLease};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

impl TableHandle {
    fn open_search_snapshot(
        &self,
        definition_id: u64,
        table_id: u64,
        visible_version: i64,
    ) -> Result<SearchReadSnapshot> {
        let generation = self
            .open_search_generation_snapshot(definition_id)?
            .ok_or_else(|| {
                paro_error::object_not_found(
                    "Search generation",
                    format!("definition_id={definition_id}"),
                )
            })?;
        let generation_lease = GenerationReadLease::from_snapshot(&generation);
        let (table_snapshot, table_lease) =
            TableReadLease::open(&self.tablet(), table_id, visible_version)?;
        Ok(SearchReadSnapshot::new(
            table_snapshot,
            generation,
            table_lease,
            generation_lease,
        ))
    }

    pub fn open_vector_search_cursor(
        &self,
        column_id: usize,
        query: &[f32],
        k: usize,
        params: SearchParams,
        predicate: Option<PredicateTree>,
        visible_version: i64,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .vector_capability(column_id as u32)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "vector"))?;
        let snapshot = self.open_search_snapshot(
            capability.definition_id,
            capability.table_id,
            visible_version,
        )?;
        VectorSearchProvider::new(
            self.tablet(),
            self.types(),
            column_id,
            query,
            k,
            params,
            predicate,
        )
        .open(snapshot)
    }

    pub fn open_sparse_vector_search_cursor(
        &self,
        column_id: usize,
        query: &SparseVector,
        k: usize,
        predicate: Option<PredicateTree>,
        visible_version: i64,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .sparse_capability(column_id as u32)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "sparse"))?;
        let snapshot = self.open_search_snapshot(
            capability.definition_id,
            capability.table_id,
            visible_version,
        )?;
        Ok(SparseSearchProvider::new(self.tablet(), column_id, query, k, predicate).open(snapshot))
    }

    pub fn open_fulltext_filter_cursor(
        &self,
        column_id: usize,
        query: &ParsedQuery,
        config: &str,
        predicate: Option<PredicateTree>,
        visible_version: i64,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .fulltext_capability(column_id as u32, config)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "fulltext"))?;
        let snapshot = self.open_search_snapshot(
            capability.definition_id,
            capability.table_id,
            visible_version,
        )?;
        Ok(
            FullTextFilterProvider::new(self.tablet(), column_id, query, config, predicate)
                .open(snapshot),
        )
    }

    pub fn open_fulltext_search_cursor(
        &self,
        column_id: usize,
        query: &ParsedQuery,
        k: usize,
        config: &str,
        predicate: Option<PredicateTree>,
        global_stats: Option<GlobalFullTextStats>,
        score_mode: FullTextScoreMode,
        visible_version: i64,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .fulltext_capability(column_id as u32, config)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "fulltext"))?;
        let global_stats =
            global_stats.or_else(|| capability.generation_stats.fulltext_global_stats());
        let snapshot = self.open_search_snapshot(
            capability.definition_id,
            capability.table_id,
            visible_version,
        )?;
        Ok(FullTextTopKProvider::new(
            self.tablet(),
            column_id,
            query,
            k,
            config,
            predicate,
            global_stats,
            score_mode,
        )
        .open(snapshot))
    }

    pub fn materialize_search_batch(
        &self,
        snapshot: &SearchReadSnapshot,
        batch: CandidateBatch,
        projected_columns: &[usize],
        emit_score: bool,
    ) -> Result<Chunk> {
        materialize_candidate_batch(
            &self.tablet(),
            self.types(),
            snapshot,
            batch,
            projected_columns,
            emit_score,
        )
    }
}
