// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::table_handle::TableHandle;
use crate::index::BoundIndex;
use crate::rowset::RowsetId;
use crate::search::write_path::SearchWriteContext;
use crate::search::{
    CapabilityToken, FullTextIntent, GenerationReadSnapshot, OpenSearchCursorResult,
    SearchBootstrapReport, SearchCapability, SearchGenerationCoverage, SearchIndexDefinition,
    SearchIndexKind, SearchIntent, SearchMaintenanceReport,
};
use crate::tablet::ColumnId;
use paro_common::error::Result;
use std::sync::Arc;

impl TableHandle {
    // ===== Index API facade =====
    pub fn index_count(&self) -> usize {
        self.runtime_indexes.index_count()
    }

    pub fn has_index(&self, name: &str) -> bool {
        self.runtime_indexes.has_index(name)
    }

    pub fn get_index(&self, name: &str) -> Option<Arc<dyn BoundIndex>> {
        self.runtime_indexes.get_index(name)
    }

    pub fn get_indexes(&self) -> Vec<Arc<dyn BoundIndex>> {
        self.runtime_indexes.get_indexes()
    }

    pub fn add_index(&self, index: Arc<dyn BoundIndex>) -> Result<()> {
        self.runtime_indexes.add_index(index)
    }

    pub fn remove_index(&self, name: &str) -> Option<Arc<dyn BoundIndex>> {
        self.runtime_indexes.remove_index(name)
    }

    /// Record that a column declares an ART predicate index in table metadata.
    pub fn declare_art_index(&self, owner: &str, column_id: ColumnId) -> bool {
        self.runtime_indexes
            .declare_art_index(&self.tablet(), owner, column_id)
    }

    /// Activate the durable ART maintenance contract and backfill all visible
    /// segments. The declaration is installed first so concurrent future
    /// writes are covered; readers still require each segment's completeness
    /// credential. A failed backfill rolls the declaration and artifacts back.
    pub fn install_art_index(&self, owner: &str, column_id: ColumnId) -> Result<()> {
        if !self.declare_art_index(owner, column_id) {
            return Ok(());
        }
        if let Err(error) = self.rebuild_art_index(column_id) {
            let cleanup = self.release_art_index(owner, column_id);
            if let Err(cleanup_error) = cleanup {
                return Err(cleanup_error.context(format!(
                    "ART backfill failed before rollback cleanup: {error}"
                )));
            }
            return Err(error);
        }
        Ok(())
    }

    pub fn forget_art_index(&self, owner: &str, column_id: ColumnId) -> bool {
        self.runtime_indexes
            .forget_art_index(&self.tablet(), owner, column_id)
    }

    /// Release one catalog owner and remove physical access paths only after
    /// the last declaration disappears.
    pub fn release_art_index(&self, owner: &str, column_id: ColumnId) -> Result<()> {
        if self.forget_art_index(owner, column_id) {
            self.drop_art_index(column_id)?;
        }
        Ok(())
    }

    pub fn register_search_definition(&self, definition: SearchIndexDefinition) -> Result<()> {
        self.search_registry.install_definition(definition)
    }

    /// Register a definition immediately after this process installed its
    /// complete durable generation. Publication verification finishes before
    /// the capability enters the registry view; recovery registration remains
    /// lazy through [`Self::register_search_definition`].
    pub fn register_published_search_definition(
        &self,
        definition: SearchIndexDefinition,
    ) -> Result<()> {
        self.search_registry
            .install_published_definition(definition)
    }

    pub fn adopt_staged_search_generation_readers(
        &self,
        staged: &crate::search::StagedSearchGeneration,
    ) {
        self.search_registry.adopt_staged_generation_readers(staged);
    }

    pub fn stage_search_definition_generation(
        &self,
        definition: SearchIndexDefinition,
        txn_id: u64,
        stop_check: crate::search::SearchBuildStopCheck,
    ) -> Result<crate::search::StagedSearchGeneration> {
        self.search_registry
            .stage_definition_generation(definition, txn_id, stop_check)
    }

    pub fn unregister_search_definition(&self, definition_id: u64) -> Result<()> {
        self.search_registry.drop_definition(definition_id)
    }

    pub fn unregister_search_definition_by_name(&self, name: &str) -> Result<()> {
        self.search_registry.drop_definition_by_name(name)
    }

    /// Rebuild ART indexes for all visible segments on a target column.
    pub fn rebuild_art_index(&self, column_id: ColumnId) -> Result<()> {
        self.runtime_indexes
            .rebuild_art_index(&self.tablet(), column_id)
    }

    /// Remove ART indexes from all visible segments on a target column.
    pub fn drop_art_index(&self, column_id: ColumnId) -> Result<()> {
        for (_, segment) in self.collect_segments(self.max_version())? {
            segment.drop_art_index(column_id);
        }
        Ok(())
    }

    pub fn vacuum_indexes(&self) {
        // Index metadata currently lives in-memory, so there is nothing to vacuum.
    }

    pub(crate) fn declared_art_columns(&self) -> Vec<ColumnId> {
        self.runtime_indexes.declared_art_columns()
    }

    /// Whether the table has a declared scalar-index maintenance contract for
    /// this column. Segment evaluation independently validates its row-count
    /// completeness credential before treating postings as exact.
    pub fn has_declared_art_index(&self, column_id: ColumnId) -> bool {
        self.runtime_indexes.has_declared_art_index(column_id)
    }

    /// Runtime row coverage for an exact predicate that needs every listed
    /// scalar column. A partially backfilled table is explicitly represented
    /// instead of being costed from a table-level declaration bit.
    pub fn complete_scalar_index_row_coverage(
        &self,
        column_ids: &[ColumnId],
    ) -> Result<(u64, u64)> {
        let segments = self.collect_segments(self.max_version())?;
        let mut covered_rows = 0u64;
        let mut total_rows = 0u64;
        for (_, segment) in segments {
            let rows = segment.num_rows();
            total_rows = total_rows.saturating_add(rows);
            if !column_ids.is_empty()
                && column_ids
                    .iter()
                    .all(|column_id| segment.has_complete_scalar_index(*column_id))
            {
                covered_rows = covered_rows.saturating_add(rows);
            }
        }
        Ok((covered_rows, total_rows))
    }

    /// Count runtime-visible secondary indexes across table-global and segment-local state.
    pub fn recovery_index_count(&self) -> usize {
        self.runtime_indexes.recovery_index_count(&self.tablet())
            + self.search_registry.catalog_definition_count()
    }

    pub(crate) fn search_write_context(&self) -> Result<SearchWriteContext> {
        self.search_registry.write_context()
    }

    pub fn open_search_generation_snapshot(
        &self,
        definition_id: u64,
    ) -> Result<Option<GenerationReadSnapshot>> {
        self.search_registry.open_generation_snapshot(definition_id)
    }

    pub fn open_search_generation_snapshot_with_token(
        &self,
        token: &CapabilityToken,
    ) -> Result<OpenSearchCursorResult<GenerationReadSnapshot>> {
        self.search_registry
            .open_generation_snapshot_with_token(token)
    }

    pub fn search_generation_coverage(
        &self,
        definition_id: u64,
    ) -> Result<Option<SearchGenerationCoverage>> {
        self.search_registry.generation_coverage(definition_id)
    }

    pub fn materialize_search_generation(
        &self,
        definition_id: u64,
    ) -> Result<SearchGenerationCoverage> {
        self.search_registry.materialize_definition(definition_id)
    }

    /// Bring one catalog-owned search generation to the current table version
    /// without compacting immutable table data.
    pub fn materialize_search_generation_by_name(
        &self,
        definition_name: &str,
    ) -> Result<SearchGenerationCoverage> {
        self.search_registry
            .materialize_catalog_definition_by_name(definition_name)
    }

    pub fn bootstrap_search_generations(&self) -> Result<SearchBootstrapReport> {
        self.search_registry.bootstrap_migration()
    }

    /// Run provider-owned derived-state maintenance without rewriting base
    /// rowsets. Instance background maintenance uses this entry point so tail
    /// catch-up, manifest compaction, and sidecar repacking share the search
    /// scheduler's admission policy while table compaction remains owned by
    /// the compaction manager.
    pub fn run_search_maintenance_pass(&self) -> Result<SearchMaintenanceReport> {
        let mut report = self.search_registry.run_maintenance_pass()?;
        if report.manifest_delta_compaction_requested
            && self.search_registry.compact_manifest_deltas()? > 0
        {
            self.search_registry.refresh_all_definitions();
            report = self.search_registry.run_maintenance_pass()?;
        }
        if report.sidecar_repack_requested {
            self.search_registry.refresh_all_definitions();
            report = self.search_registry.run_maintenance_pass()?;
        }
        Ok(report)
    }

    pub fn vector_capability(
        &self,
        column_id: ColumnId,
        distance: crate::index::hnsw::DistanceMetric,
    ) -> Option<SearchCapability> {
        self.search_registry.hnsw_capability(column_id, distance)
    }

    /// Return the mutable query policy associated with the active HNSW
    /// definition. The policy is deliberately separate from artifact
    /// statistics and the immutable build contract.
    pub fn vector_search_policy(
        &self,
        column_id: ColumnId,
        distance: crate::index::hnsw::DistanceMetric,
    ) -> Option<crate::index::hnsw::HnswSearchPolicy> {
        self.search_registry.hnsw_search_policy(column_id, distance)
    }

    pub fn vector_filter_topology(
        &self,
        column_id: ColumnId,
        distance: crate::index::hnsw::DistanceMetric,
    ) -> Option<crate::index::hnsw::HnswFilterTopologyContract> {
        self.search_registry
            .hnsw_filter_topology(column_id, distance)
    }

    pub fn sparse_capability(&self, column_id: ColumnId) -> Option<SearchCapability> {
        self.search_registry
            .capability(SearchIndexKind::Sparse, column_id, None)
    }

    pub fn fulltext_capability(
        &self,
        column_id: ColumnId,
        config: &str,
    ) -> Option<SearchCapability> {
        self.search_registry.fulltext_capability(column_id, config)
    }

    pub fn search_capability(&self, intent: &SearchIntent) -> Option<SearchCapability> {
        match intent {
            SearchIntent::Hnsw(intent) => self.vector_capability(intent.column_id, intent.distance),
            SearchIntent::Sparse(intent) => self.sparse_capability(intent.column_id),
            SearchIntent::FullText(FullTextIntent {
                column_id, config, ..
            }) => self.fulltext_capability(*column_id, config),
        }
    }

    pub fn has_queryable_search_artifact(
        &self,
        kind: SearchIndexKind,
        rowset_id: RowsetId,
        segment_id: u32,
        column_id: ColumnId,
    ) -> bool {
        self.search_registry
            .has_queryable_artifact(kind, rowset_id, segment_id, column_id)
    }
}
