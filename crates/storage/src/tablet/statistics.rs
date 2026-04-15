// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Tablet Statistics
//!
//! Aggregated statistics across all visible rowsets in a tablet.

use crate::rowset::{RowsetSharedPtr, RowsetStatistics};
use crate::statistics::{ColumnStatistics, DeleteStatistics};
use crate::tablet::ColumnId;
use std::collections::HashMap;

/// Aggregated statistics for a column across a tablet.
#[derive(Debug, Clone)]
pub struct TabletColumnStatistics {
    /// Column ID
    pub column_id: ColumnId,
    /// Column-level statistics (min/max, distinct, etc.)
    pub stats: ColumnStatistics,
    /// Total NULL count across rowsets
    pub null_count: u64,
    /// Total rows across rowsets
    pub num_rows: u64,
}

impl TabletColumnStatistics {
    pub fn new(
        column_id: ColumnId,
        stats: ColumnStatistics,
        null_count: u64,
        num_rows: u64,
    ) -> Self {
        Self {
            column_id,
            stats,
            null_count,
            num_rows,
        }
    }

    pub fn has_nulls(&self) -> bool {
        self.null_count > 0
    }
}

/// Tablet-level statistics aggregated from rowsets.
#[derive(Debug, Clone, Default)]
pub struct TabletStatistics {
    /// Total rows in the tablet
    pub num_rows: u64,
    /// Number of visible rowsets
    pub num_rowsets: usize,
    /// Delete statistics across rowsets
    pub delete_stats: DeleteStatistics,
    /// Per-column aggregated statistics
    pub columns: Vec<TabletColumnStatistics>,
}

impl TabletStatistics {
    /// Create empty statistics for a tablet.
    pub fn new(num_rows: u64, num_rowsets: usize) -> Self {
        Self {
            num_rows,
            num_rowsets,
            delete_stats: DeleteStatistics::from_counts(num_rows, 0),
            columns: Vec::new(),
        }
    }

    /// Add statistics from a rowset.
    pub fn add_rowset_stats(&mut self, rowset_stats: &RowsetStatistics) {
        self.num_rowsets += 1;
        self.num_rows += rowset_stats.num_rows;
        let total_deleted =
            self.delete_stats.num_deleted_rows + rowset_stats.delete_stats.num_deleted_rows;
        self.delete_stats = DeleteStatistics::from_counts(self.num_rows, total_deleted);

        let mut index = HashMap::<ColumnId, usize>::new();
        for (idx, col) in self.columns.iter().enumerate() {
            index.insert(col.column_id, idx);
        }

        for col_stats in rowset_stats.columns() {
            match index.get(&col_stats.column_id) {
                Some(&idx) => {
                    let entry = &mut self.columns[idx];
                    entry.stats.merge(&col_stats.stats);
                    entry.null_count += col_stats.null_count;
                    entry.num_rows += col_stats.num_rows;
                }
                None => {
                    self.columns.push(TabletColumnStatistics::new(
                        col_stats.column_id,
                        col_stats.stats.clone(),
                        col_stats.null_count,
                        col_stats.num_rows,
                    ));
                    index.insert(col_stats.column_id, self.columns.len() - 1);
                }
            }
        }
    }

    /// Aggregate statistics from visible rowsets.
    pub fn from_rowsets(rowsets: &[RowsetSharedPtr]) -> paro_common::error::Result<Self> {
        let mut num_rows = 0u64;
        let mut num_rowsets = 0usize;
        let mut num_deleted_rows = 0u64;
        let mut columns_map: HashMap<ColumnId, TabletColumnStatistics> = HashMap::new();

        for rowset in rowsets {
            num_rowsets += 1;
            num_rows += rowset.num_rows();
            num_deleted_rows += rowset.rowset_meta().num_deleted_rows();
            let rowset_stats = rowset.statistics()?;

            for col in rowset_stats.columns() {
                let entry = columns_map.entry(col.column_id).or_insert_with(|| {
                    TabletColumnStatistics::new(col.column_id, col.stats.clone(), 0, 0)
                });
                entry.stats.merge(&col.stats);
                entry.null_count += col.null_count;
                entry.num_rows += col.num_rows;
            }
        }

        let mut columns: Vec<TabletColumnStatistics> = columns_map.into_values().collect();
        columns.sort_by_key(|c| c.column_id);

        Ok(Self {
            num_rows,
            num_rowsets,
            delete_stats: DeleteStatistics::from_counts(num_rows, num_deleted_rows),
            columns,
        })
    }

    /// Get statistics for a specific column ID.
    pub fn column(&self, column_id: ColumnId) -> Option<&TabletColumnStatistics> {
        self.columns.iter().find(|c| c.column_id == column_id)
    }

    /// Iterate over all column statistics.
    pub fn columns(&self) -> &[TabletColumnStatistics] {
        &self.columns
    }
}
