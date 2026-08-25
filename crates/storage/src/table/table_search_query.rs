// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::table_handle::TableHandle;
use crate::index::fulltext::query_parser::ParsedQuery;
use crate::index::fulltext::scoring::FullTextScoreMode;
use crate::index::fulltext::text_index::GlobalFullTextStats;
use crate::index::hnsw::types::SearchParams;
use crate::index::hnsw::DistanceMetric;
use crate::index::PredicateTree;
use crate::rowset::SparseVector;
use crate::search::capability::{CapabilityToken, SearchCapability, SearchIndexKind};
use crate::search::cursor::{
    CandidateBatch, GenerationReadLease, OpenSearchCursorResult, OpenedSearchCursor,
};
use crate::search::providers::fulltext::search::{FullTextFilterProvider, FullTextTopKProvider};
use crate::search::providers::hnsw::search::VectorSearchProvider;
use crate::search::providers::sparse::search::SparseSearchProvider;
use crate::search::row_fetch::materialize_candidate_batch;
use crate::search::{SearchReadOptions, SearchReadSnapshot, TableReadLease};
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
        capability: &SearchCapability,
        visible_version: i64,
        read_options: &SearchReadOptions,
    ) -> Result<SearchReadSnapshot> {
        let token = capability.capability_token();
        match self.open_search_snapshot_with_token_result(
            capability.kind,
            &token,
            visible_version,
            None,
            read_options,
        )? {
            OpenSearchCursorResult::Opened(snapshot) => Ok(snapshot),
            OpenSearchCursorResult::CapabilityTokenStale => Err(paro_error::internal(format!(
                "search capability token stale for definition {} generation {}",
                token.definition_id, token.generation_id
            ))),
            OpenSearchCursorResult::NotQueryable => Err(paro_error::object_not_found(
                "Search generation",
                format!("definition_id={} not queryable", token.definition_id),
            )),
        }
    }

    fn open_search_snapshot_with_overlay(
        &self,
        capability: &SearchCapability,
        visible_version: i64,
        overlay: Option<&TxnOverlayReader>,
        read_options: &SearchReadOptions,
    ) -> Result<SearchReadSnapshot> {
        if overlay.is_none() {
            return self.open_search_snapshot(capability, visible_version, read_options);
        }

        let token = capability.capability_token();
        match self.open_search_snapshot_with_token_result(
            capability.kind,
            &token,
            visible_version,
            overlay,
            read_options,
        )? {
            OpenSearchCursorResult::Opened(snapshot) => Ok(snapshot),
            OpenSearchCursorResult::CapabilityTokenStale => Err(paro_error::internal(format!(
                "search capability token stale for definition {} generation {}",
                token.definition_id, token.generation_id
            ))),
            OpenSearchCursorResult::NotQueryable => Err(paro_error::object_not_found(
                "Search generation",
                format!("definition_id={} not queryable", token.definition_id),
            )),
        }
    }

    fn open_search_snapshot_with_token_result(
        &self,
        kind: SearchIndexKind,
        token: &CapabilityToken,
        visible_version: i64,
        overlay: Option<&TxnOverlayReader>,
        read_options: &SearchReadOptions,
    ) -> Result<OpenSearchCursorResult<SearchReadSnapshot>> {
        let generation = match self.open_search_generation_snapshot_with_token(token)? {
            OpenSearchCursorResult::Opened(generation) => generation,
            OpenSearchCursorResult::CapabilityTokenStale => {
                return Ok(OpenSearchCursorResult::CapabilityTokenStale);
            }
            OpenSearchCursorResult::NotQueryable => {
                return Ok(OpenSearchCursorResult::NotQueryable);
            }
        };
        let generation_lease = GenerationReadLease::from_snapshot(&generation);
        let derived_lag_lease =
            self.lease_search_derived_lag(generation.indexed_through_ts, visible_version)?;
        let overlay_rowsets = overlay
            .map(TxnOverlayReader::all_rowsets)
            .unwrap_or_default();
        let (table_snapshot, table_lease) = if overlay_rowsets.is_empty() {
            TableReadLease::open(
                &self.tablet(),
                self.table_id(),
                visible_version,
                read_options,
            )?
        } else {
            TableReadLease::open_with_overlay_rowsets(
                &self.tablet(),
                self.table_id(),
                visible_version,
                overlay_rowsets,
                read_options,
            )?
        };
        let reader_runtime = self.search_registry.reader_runtime();
        reader_runtime.bind_buffer_pool(read_options.buffer_pool())?;
        let snapshot = SearchReadSnapshot::new(
            table_snapshot,
            kind,
            generation,
            table_lease,
            generation_lease,
            reader_runtime,
        )
        .with_derived_lag_lease(derived_lag_lease)
        .with_overlay_delete_vectors(overlay.and_then(TxnOverlayReader::delete_vectors));
        Ok(OpenSearchCursorResult::Opened(snapshot))
    }

    pub fn open_vector_search_cursor(
        &self,
        column_id: usize,
        query: &[f32],
        distance: DistanceMetric,
        k: usize,
        params: SearchParams,
        predicate: Option<PredicateTree>,
        visible_version: i64,
        read_options: &SearchReadOptions,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .vector_capability(column_id as u32, distance)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "vector"))?;
        let snapshot = self.open_search_snapshot(&capability, visible_version, read_options)?;
        VectorSearchProvider::new(
            self.tablet(),
            self.types(),
            column_id,
            query,
            distance,
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
        distance: DistanceMetric,
        k: usize,
        params: SearchParams,
        predicate: Option<PredicateTree>,
        view: &TransactionView,
        read_options: &SearchReadOptions,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .vector_capability(column_id as u32, distance)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "vector"))?;
        let overlay = TxnOverlayReader::for_tablet(&self.tablet(), view)?;
        let snapshot = self.open_search_snapshot_with_overlay(
            &capability,
            view.visible_version_i64(),
            overlay.as_ref(),
            read_options,
        )?;
        VectorSearchProvider::new(
            self.tablet(),
            self.types(),
            column_id,
            query,
            distance,
            k,
            params,
            predicate,
        )
        .open(snapshot)
    }

    pub fn open_vector_search_cursor_with_token_for_view(
        &self,
        token: &CapabilityToken,
        column_id: usize,
        query: &[f32],
        distance: DistanceMetric,
        k: usize,
        params: SearchParams,
        predicate: Option<PredicateTree>,
        view: &TransactionView,
        read_options: &SearchReadOptions,
    ) -> Result<OpenSearchCursorResult<OpenedSearchCursor>> {
        let overlay = TxnOverlayReader::for_tablet(&self.tablet(), view)?;
        let snapshot = match self.open_search_snapshot_with_token_result(
            SearchIndexKind::Hnsw,
            token,
            view.visible_version_i64(),
            overlay.as_ref(),
            read_options,
        )? {
            OpenSearchCursorResult::Opened(snapshot) => snapshot,
            OpenSearchCursorResult::CapabilityTokenStale => {
                return Ok(OpenSearchCursorResult::CapabilityTokenStale);
            }
            OpenSearchCursorResult::NotQueryable => {
                return Ok(OpenSearchCursorResult::NotQueryable);
            }
        };
        VectorSearchProvider::new(
            self.tablet(),
            self.types(),
            column_id,
            query,
            distance,
            k,
            params,
            predicate,
        )
        .open(snapshot)
        .map(OpenSearchCursorResult::Opened)
    }

    pub fn open_sparse_vector_search_cursor(
        &self,
        column_id: usize,
        query: &SparseVector,
        k: usize,
        predicate: Option<PredicateTree>,
        visible_version: i64,
        read_options: &SearchReadOptions,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .sparse_capability(column_id as u32)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "sparse"))?;
        let snapshot = self.open_search_snapshot(&capability, visible_version, read_options)?;
        SparseSearchProvider::new(self.tablet(), column_id, query, k, predicate).open(snapshot)
    }

    pub fn open_sparse_vector_search_cursor_with_token_for_view(
        &self,
        token: &CapabilityToken,
        column_id: usize,
        query: &SparseVector,
        k: usize,
        predicate: Option<PredicateTree>,
        view: &TransactionView,
        read_options: &SearchReadOptions,
    ) -> Result<OpenSearchCursorResult<OpenedSearchCursor>> {
        let overlay = TxnOverlayReader::for_tablet(&self.tablet(), view)?;
        let snapshot = match self.open_search_snapshot_with_token_result(
            SearchIndexKind::Sparse,
            token,
            view.visible_version_i64(),
            overlay.as_ref(),
            read_options,
        )? {
            OpenSearchCursorResult::Opened(snapshot) => snapshot,
            OpenSearchCursorResult::CapabilityTokenStale => {
                return Ok(OpenSearchCursorResult::CapabilityTokenStale);
            }
            OpenSearchCursorResult::NotQueryable => {
                return Ok(OpenSearchCursorResult::NotQueryable);
            }
        };
        SparseSearchProvider::new(self.tablet(), column_id, query, k, predicate)
            .open(snapshot)
            .map(OpenSearchCursorResult::Opened)
    }

    pub fn open_sparse_vector_search_cursor_for_view(
        &self,
        column_id: usize,
        query: &SparseVector,
        k: usize,
        predicate: Option<PredicateTree>,
        view: &TransactionView,
        read_options: &SearchReadOptions,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .sparse_capability(column_id as u32)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "sparse"))?;
        let overlay = TxnOverlayReader::for_tablet(&self.tablet(), view)?;
        let snapshot = self.open_search_snapshot_with_overlay(
            &capability,
            view.visible_version_i64(),
            overlay.as_ref(),
            read_options,
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
        read_options: &SearchReadOptions,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .fulltext_capability(column_id as u32, config)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "fulltext"))?;
        let snapshot = self.open_search_snapshot(&capability, visible_version, read_options)?;
        FullTextFilterProvider::new(self.tablet(), column_id, query, config, predicate)
            .open(snapshot)
    }

    pub fn open_fulltext_filter_cursor_with_token_for_view(
        &self,
        token: &CapabilityToken,
        column_id: usize,
        query: &ParsedQuery,
        config: &str,
        predicate: Option<PredicateTree>,
        view: &TransactionView,
        read_options: &SearchReadOptions,
    ) -> Result<OpenSearchCursorResult<OpenedSearchCursor>> {
        let overlay = TxnOverlayReader::for_tablet(&self.tablet(), view)?;
        let snapshot = match self.open_search_snapshot_with_token_result(
            SearchIndexKind::FullText,
            token,
            view.visible_version_i64(),
            overlay.as_ref(),
            read_options,
        )? {
            OpenSearchCursorResult::Opened(snapshot) => snapshot,
            OpenSearchCursorResult::CapabilityTokenStale => {
                return Ok(OpenSearchCursorResult::CapabilityTokenStale);
            }
            OpenSearchCursorResult::NotQueryable => {
                return Ok(OpenSearchCursorResult::NotQueryable);
            }
        };
        FullTextFilterProvider::new(self.tablet(), column_id, query, config, predicate)
            .open(snapshot)
            .map(OpenSearchCursorResult::Opened)
    }

    pub fn open_fulltext_filter_cursor_for_view(
        &self,
        column_id: usize,
        query: &ParsedQuery,
        config: &str,
        predicate: Option<PredicateTree>,
        view: &TransactionView,
        read_options: &SearchReadOptions,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .fulltext_capability(column_id as u32, config)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "fulltext"))?;
        let overlay = TxnOverlayReader::for_tablet(&self.tablet(), view)?;
        let snapshot = self.open_search_snapshot_with_overlay(
            &capability,
            view.visible_version_i64(),
            overlay.as_ref(),
            read_options,
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
        read_options: &SearchReadOptions,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .fulltext_capability(column_id as u32, config)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "fulltext"))?;
        let global_stats =
            global_stats.or_else(|| capability.generation_stats.fulltext_global_stats());
        let snapshot = self.open_search_snapshot(&capability, visible_version, read_options)?;
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

    pub fn open_fulltext_search_cursor_with_token_for_view(
        &self,
        token: &CapabilityToken,
        column_id: usize,
        query: &ParsedQuery,
        k: usize,
        config: &str,
        predicate: Option<PredicateTree>,
        global_stats: Option<GlobalFullTextStats>,
        score_mode: FullTextScoreMode,
        view: &TransactionView,
        read_options: &SearchReadOptions,
    ) -> Result<OpenSearchCursorResult<OpenedSearchCursor>> {
        let overlay = TxnOverlayReader::for_tablet(&self.tablet(), view)?;
        let snapshot = match self.open_search_snapshot_with_token_result(
            SearchIndexKind::FullText,
            token,
            view.visible_version_i64(),
            overlay.as_ref(),
            read_options,
        )? {
            OpenSearchCursorResult::Opened(snapshot) => snapshot,
            OpenSearchCursorResult::CapabilityTokenStale => {
                return Ok(OpenSearchCursorResult::CapabilityTokenStale);
            }
            OpenSearchCursorResult::NotQueryable => {
                return Ok(OpenSearchCursorResult::NotQueryable);
            }
        };
        let global_stats =
            global_stats.or_else(|| snapshot.generation.generation_stats.fulltext_global_stats());
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
        .map(OpenSearchCursorResult::Opened)
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
        read_options: &SearchReadOptions,
    ) -> Result<OpenedSearchCursor> {
        let capability = self
            .fulltext_capability(column_id as u32, config)
            .ok_or_else(|| paro_error::object_not_found("Search capability", "fulltext"))?;
        let global_stats =
            global_stats.or_else(|| capability.generation_stats.fulltext_global_stats());
        let overlay = TxnOverlayReader::for_tablet(&self.tablet(), view)?;
        let snapshot = self.open_search_snapshot_with_overlay(
            &capability,
            view.visible_version_i64(),
            overlay.as_ref(),
            read_options,
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
        allocator: Arc<dyn paro_common::allocator::Allocator>,
    ) -> Result<Chunk> {
        materialize_candidate_batch(
            &self.tablet(),
            self.types(),
            snapshot,
            batch,
            projected_columns,
            emit_score,
            allocator,
        )
    }
}
