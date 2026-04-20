// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::table_handle::TableHandle;
use crate::index::BoundIndex;
use crate::rowset::RowsetId;
use crate::search::write_path::SearchWritePlan;
use crate::search::{
    FullTextIntent, GenerationReadSnapshot, SearchBootstrapReport, SearchCapability,
    SearchGenerationCoverage, SearchIndexDefinition, SearchIndexKind, SearchIntent,
    SearchMaintenanceReport,
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
    pub fn declare_art_index(&self, column_id: ColumnId) {
        self.runtime_indexes
            .declare_art_index(&self.tablet(), column_id);
    }

    pub fn forget_art_index(&self, column_id: ColumnId) {
        self.runtime_indexes
            .forget_art_index(&self.tablet(), column_id);
    }

    pub fn register_search_definition(&self, definition: SearchIndexDefinition) -> Result<()> {
        self.search_registry.install_definition(definition)
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

    /// Count runtime-visible secondary indexes across table-global and segment-local state.
    pub fn recovery_index_count(&self) -> usize {
        self.runtime_indexes.recovery_index_count(&self.tablet())
            + self.search_registry.catalog_definition_count()
    }

    pub(crate) fn search_write_plan(&self) -> Result<SearchWritePlan> {
        self.search_registry.write_plan()
    }

    pub fn open_search_generation_snapshot(
        &self,
        definition_id: u64,
    ) -> Result<Option<GenerationReadSnapshot>> {
        self.search_registry.open_generation_snapshot(definition_id)
    }

    pub fn search_generation_coverage(
        &self,
        definition_id: u64,
    ) -> Result<Option<SearchGenerationCoverage>> {
        self.search_registry.generation_coverage(definition_id)
    }

    pub fn bootstrap_search_generations(&self) -> Result<SearchBootstrapReport> {
        self.search_registry.bootstrap_migration()
    }

    pub fn search_maintenance_sweep(&self) -> Result<SearchMaintenanceReport> {
        let mut report = self.search_registry.maintenance_sweep()?;
        if report.compaction_requested && self.optimize_compact()? {
            self.search_registry.ensure_fresh();
            report = self.search_registry.maintenance_sweep()?;
        }
        Ok(report)
    }

    pub fn vector_capability(&self, column_id: ColumnId) -> Option<SearchCapability> {
        self.search_registry
            .capability(SearchIndexKind::Hnsw, column_id, None)
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
            SearchIntent::Hnsw(intent) => self.vector_capability(intent.column_id),
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
