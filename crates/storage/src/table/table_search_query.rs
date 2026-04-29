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
use crate::transaction::overlay_reader::TxnOverlayReader;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_transaction::{DerivedLagLease, TransactionView};
use std::sync::Arc;

impl TableHandle {
    fn lease_search_derived_lag(
        &self,
        indexed_through_ts: u64,
        visible_version: i64,
    ) -> Result<Option<Arc<DerivedLagLease>>> {
        let target_ts = visible_version.max(0) as u64;
        if target_ts <= indexed_through_ts {
            return Ok(None);
        }
        self.tablet()
            .lease_derived_lag_range(indexed_through_ts, target_ts)
            .map(Arc::new)
            .map(Some)
    }

    pub(crate) fn open_search_snapshot(
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
        let derived_lag_lease =
            self.lease_search_derived_lag(generation.indexed_through_ts, visible_version)?;
        let (table_snapshot, table_lease) =
            TableReadLease::open(&self.tablet(), table_id, visible_version)?;
        Ok(
            SearchReadSnapshot::new(table_snapshot, generation, table_lease, generation_lease)
                .with_derived_lag_lease(derived_lag_lease),
        )
    }

    fn open_search_snapshot_with_overlay(
        &self,
        definition_id: u64,
        table_id: u64,
        visible_version: i64,
        overlay: Option<&TxnOverlayReader>,
    ) -> Result<SearchReadSnapshot> {
        if overlay.is_none() {
            return self.open_search_snapshot(definition_id, table_id, visible_version);
        }

        let generation = self
            .open_search_generation_snapshot(definition_id)?
            .ok_or_else(|| {
                paro_error::object_not_found(
                    "Search generation",
                    format!("definition_id={definition_id}"),
                )
            })?;
        let generation_lease = GenerationReadLease::from_snapshot(&generation);
        let derived_lag_lease =
            self.lease_search_derived_lag(generation.indexed_through_ts, visible_version)?;
        let overlay_rowsets = overlay
            .map(TxnOverlayReader::all_rowsets)
            .unwrap_or_default();
        let (table_snapshot, table_lease) = TableReadLease::open_with_overlay_rowsets(
            &self.tablet(),
            table_id,
            visible_version,
            overlay_rowsets,
        )?;
        Ok(
            SearchReadSnapshot::new(table_snapshot, generation, table_lease, generation_lease)
                .with_derived_lag_lease(derived_lag_lease)
                .with_overlay_delete_vectors(overlay.and_then(TxnOverlayReader::delete_vectors)),
        )
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

    pub fn open_vector_search_cursor_for_view(
        &self,
        column_id: usize,
        query: &[f32],
        k: usize,
        params: SearchParams,
        predicate: Option<PredicateTree>,
        view: &TransactionView,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .vector_capability(column_id as u32)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "vector"))?;
        let overlay = TxnOverlayReader::for_tablet(&self.tablet(), view)?;
        let snapshot = self.open_search_snapshot_with_overlay(
            capability.definition_id,
            capability.table_id,
            view.visible_version_i64(),
            overlay.as_ref(),
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
        SparseSearchProvider::new(self.tablet(), column_id, query, k, predicate).open(snapshot)
    }

    pub fn open_sparse_vector_search_cursor_for_view(
        &self,
        column_id: usize,
        query: &SparseVector,
        k: usize,
        predicate: Option<PredicateTree>,
        view: &TransactionView,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .sparse_capability(column_id as u32)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "sparse"))?;
        let overlay = TxnOverlayReader::for_tablet(&self.tablet(), view)?;
        let snapshot = self.open_search_snapshot_with_overlay(
            capability.definition_id,
            capability.table_id,
            view.visible_version_i64(),
            overlay.as_ref(),
        )?;
        SparseSearchProvider::new(self.tablet(), column_id, query, k, predicate).open(snapshot)
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
        FullTextFilterProvider::new(self.tablet(), column_id, query, config, predicate)
            .open(snapshot)
    }

    pub fn open_fulltext_filter_cursor_for_view(
        &self,
        column_id: usize,
        query: &ParsedQuery,
        config: &str,
        predicate: Option<PredicateTree>,
        view: &TransactionView,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .fulltext_capability(column_id as u32, config)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "fulltext"))?;
        let overlay = TxnOverlayReader::for_tablet(&self.tablet(), view)?;
        let snapshot = self.open_search_snapshot_with_overlay(
            capability.definition_id,
            capability.table_id,
            view.visible_version_i64(),
            overlay.as_ref(),
        )?;
        FullTextFilterProvider::new(self.tablet(), column_id, query, config, predicate)
            .open(snapshot)
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
        FullTextTopKProvider::new(
            self.tablet(),
            column_id,
            query,
            k,
            config,
            predicate,
            global_stats,
            score_mode,
        )
        .open(snapshot)
    }

    pub fn open_fulltext_search_cursor_for_view(
        &self,
        column_id: usize,
        query: &ParsedQuery,
        k: usize,
        config: &str,
        predicate: Option<PredicateTree>,
        global_stats: Option<GlobalFullTextStats>,
        score_mode: FullTextScoreMode,
        view: &TransactionView,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .fulltext_capability(column_id as u32, config)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "fulltext"))?;
        let global_stats =
            global_stats.or_else(|| capability.generation_stats.fulltext_global_stats());
        let overlay = TxnOverlayReader::for_tablet(&self.tablet(), view)?;
        let snapshot = self.open_search_snapshot_with_overlay(
            capability.definition_id,
            capability.table_id,
            view.visible_version_i64(),
            overlay.as_ref(),
        )?;
        FullTextTopKProvider::new(
            self.tablet(),
            column_id,
            query,
            k,
            config,
            predicate,
            global_stats,
            score_mode,
        )
        .open(snapshot)
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
