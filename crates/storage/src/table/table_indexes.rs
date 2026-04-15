// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::table_handle::TableHandle;
use crate::index::BoundIndex;
use crate::tablet::ColumnId;
use paro_common::error::Result;
use std::sync::Arc;

impl TableHandle {
    // ===== Index API facade =====
    pub fn index_count(&self) -> usize {
        self.index_runtime.index_count()
    }

    pub fn has_index(&self, name: &str) -> bool {
        self.index_runtime.has_index(name)
    }

    pub fn get_index(&self, name: &str) -> Option<Arc<dyn BoundIndex>> {
        self.index_runtime.get_index(name)
    }

    pub fn get_indexes(&self) -> Vec<Arc<dyn BoundIndex>> {
        self.index_runtime.get_indexes()
    }

    pub fn add_index(&self, index: Arc<dyn BoundIndex>) -> Result<()> {
        self.index_runtime.add_index(index)
    }

    pub fn remove_index(&self, name: &str) -> Option<Arc<dyn BoundIndex>> {
        self.index_runtime.remove_index(name)
    }

    /// Record that a column declares an ART predicate index in table metadata.
    pub fn mark_declared_art_index(&self, column_id: ColumnId) {
        self.index_runtime
            .mark_declared_art_index(&self.tablet(), column_id);
    }

    pub fn unmark_declared_art_index(&self, column_id: ColumnId) {
        self.index_runtime
            .unmark_declared_art_index(&self.tablet(), column_id);
    }

    /// Record that a column declares an HNSW vector index in table metadata.
    pub fn mark_declared_vector_index(&self, column_id: ColumnId) {
        self.index_runtime.mark_declared_vector_index(column_id);
    }

    pub fn unmark_declared_vector_index(&self, column_id: ColumnId) {
        self.index_runtime.unmark_declared_vector_index(column_id);
    }

    /// Record that a column declares a sparse vector index in table metadata.
    pub fn mark_declared_sparse_index(&self, column_id: ColumnId) {
        self.index_runtime.mark_declared_sparse_index(column_id);
    }

    pub fn unmark_declared_sparse_index(&self, column_id: ColumnId) {
        self.index_runtime.unmark_declared_sparse_index(column_id);
    }

    /// Record that a column declares a full-text index in table metadata.
    pub fn mark_declared_fulltext_index(&self, column_id: ColumnId) {
        self.index_runtime.mark_declared_fulltext_index(column_id);
    }

    /// Record that a column declares a configured full-text index in table metadata.
    pub fn mark_declared_fulltext_index_with_config(&self, column_id: ColumnId, config: &str) {
        self.index_runtime
            .mark_declared_fulltext_index_with_config(column_id, config);
    }

    pub fn unmark_declared_fulltext_index(&self, column_id: ColumnId) {
        self.index_runtime.unmark_declared_fulltext_index(column_id);
    }

    /// Build runtime full-text indexes for all visible segments on a target column.
    pub fn build_runtime_fulltext_index(&self, column_id: ColumnId) -> Result<()> {
        self.index_runtime
            .build_runtime_fulltext_index(&self.tablet(), column_id)
    }

    pub fn build_runtime_fulltext_index_with_config(
        &self,
        column_id: ColumnId,
        config: &str,
    ) -> Result<()> {
        self.index_runtime.build_runtime_fulltext_index_with_config(
            &self.tablet(),
            column_id,
            config,
        )
    }

    /// Build runtime ART indexes for all visible segments on a target column.
    pub fn build_runtime_art_index(&self, column_id: ColumnId) -> Result<()> {
        self.index_runtime
            .build_runtime_art_index(&self.tablet(), column_id)
    }

    /// Remove runtime ART indexes from all visible segments on a target column.
    pub fn remove_runtime_art_index(&self, column_id: ColumnId) -> Result<()> {
        for (_, segment) in self.collect_segments(self.max_version())? {
            segment.remove_runtime_art_index(column_id);
        }
        Ok(())
    }

    pub fn vacuum_indexes(&self) {
        // Index metadata currently lives in-memory, so there is nothing to vacuum.
    }

    pub(crate) fn declared_art_columns(&self) -> Vec<ColumnId> {
        self.index_runtime.declared_art_columns()
    }

    /// Count runtime-visible secondary indexes across table-global and segment-local state.
    pub fn recovery_runtime_index_count(&self) -> usize {
        self.index_runtime
            .recovery_runtime_index_count(&self.tablet())
    }

    pub(crate) fn declared_fulltext_columns_with_config(&self) -> Vec<(ColumnId, String)> {
        self.index_runtime.declared_fulltext_columns_with_config()
    }

    /// Check if a column has a HNSW vector index.
    pub fn has_vector_index(&self, column_id: ColumnId) -> bool {
        self.index_runtime
            .has_vector_index(&self.tablet(), column_id)
    }

    /// Check if a column has a fulltext index.
    pub fn has_fulltext_index(&self, column_id: ColumnId) -> bool {
        self.index_runtime
            .has_fulltext_index(&self.tablet(), column_id)
    }

    /// Check if a column has a fulltext index matching query config.
    pub fn has_fulltext_index_with_config(&self, column_id: ColumnId, config: &str) -> bool {
        self.index_runtime
            .has_fulltext_index_with_config(&self.tablet(), column_id, config)
    }

    /// Check if a column has a sparse vector index.
    pub fn has_sparse_index(&self, column_id: ColumnId) -> bool {
        self.index_runtime
            .has_sparse_index(&self.tablet(), column_id)
    }
}
