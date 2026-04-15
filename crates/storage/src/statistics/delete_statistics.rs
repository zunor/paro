// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Delete Statistics
//!
//! Tracks delete-vector related statistics at rowset/tablet level.

/// Statistics for delete vectors / deleted rows.
#[derive(Debug, Clone, Copy, Default)]
pub struct DeleteStatistics {
    /// Number of rows marked as deleted.
    pub num_deleted_rows: u64,
    /// Effective row count after deletes.
    pub effective_row_count: u64,
    /// Deleted rows ratio (num_deleted_rows / total_rows).
    pub delete_ratio: f64,
}

impl DeleteStatistics {
    /// Build delete statistics from total rows and deleted rows.
    pub fn from_counts(total_rows: u64, num_deleted_rows: u64) -> Self {
        let effective_row_count = total_rows.saturating_sub(num_deleted_rows);
        let delete_ratio = if total_rows == 0 {
            0.0
        } else {
            num_deleted_rows as f64 / total_rows as f64
        };
        Self {
            num_deleted_rows,
            effective_row_count,
            delete_ratio,
        }
    }
}
