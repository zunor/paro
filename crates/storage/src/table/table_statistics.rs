// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::table_handle::{FullTextIndexCoverage, TableHandle};
use crate::statistics::{
    BaseStatistics, ColumnStatistics, FullTextIndexStatistics, HnswIndexStatistics,
    SparseIndexStatistics,
};
use crate::tablet::ColumnId;
use paro_common::error::Result;

impl TableHandle {
    pub fn column_statistics(&self, column_index: usize) -> Option<BaseStatistics> {
        let stats = self.tablet().statistics().ok()?;
        let col_stats = stats.column(column_index as u32)?;
        Some(Self::base_stats_from_column_stats(
            &col_stats.stats,
            col_stats.null_count,
            col_stats.num_rows,
        ))
    }

    pub fn all_column_statistics(&self) -> Vec<BaseStatistics> {
        let stats = self.tablet().statistics().ok();
        let mut result = Vec::with_capacity(self.types().len());
        for (idx, ty) in self.types().iter().enumerate() {
            let entry = stats
                .as_ref()
                .and_then(|s| s.column(idx as u32))
                .map(|col_stats| {
                    Self::base_stats_from_column_stats(
                        &col_stats.stats,
                        col_stats.null_count,
                        col_stats.num_rows,
                    )
                })
                .unwrap_or_else(|| BaseStatistics::create_empty(ty.clone()));
            result.push(entry);
        }
        result
    }

    /// Aggregate HNSW index statistics across visible rowsets.
    pub fn hnsw_index_statistics(&self, column_id: ColumnId) -> Option<HnswIndexStatistics> {
        self.index_runtime
            .hnsw_index_statistics(&self.tablet(), column_id)
    }

    /// Aggregate sparse vector index statistics across visible rowsets.
    pub fn sparse_index_statistics(&self, column_id: ColumnId) -> Option<SparseIndexStatistics> {
        self.index_runtime
            .sparse_index_statistics(&self.tablet(), column_id)
    }

    /// Aggregate full-text index statistics across visible rowsets.
    pub fn fulltext_index_statistics(
        &self,
        column_id: ColumnId,
    ) -> Option<FullTextIndexStatistics> {
        self.index_runtime
            .fulltext_index_statistics(&self.tablet(), column_id)
    }

    /// Return visible/indexed segment coverage for full-text index payloads.
    pub fn fulltext_index_coverage(&self, column_id: ColumnId) -> Result<FullTextIndexCoverage> {
        self.index_runtime
            .fulltext_index_coverage(&self.tablet(), column_id)
    }

    fn base_stats_from_column_stats(
        column_stats: &ColumnStatistics,
        null_count: u64,
        num_rows: u64,
    ) -> BaseStatistics {
        let mut stats = column_stats.statistics().copy();
        stats.set_distinct_count(column_stats.get_distinct_count());
        if null_count > 0 {
            stats.set_has_null_fast();
        }
        if num_rows > null_count {
            stats.set_has_no_null_fast();
        }
        stats
    }

    /// Number of rows currently marked deleted in tablet statistics.
    pub fn deleted_row_count(&self) -> usize {
        self.tablet()
            .statistics()
            .map(|s| s.delete_stats.num_deleted_rows as usize)
            .unwrap_or(0)
    }
}
