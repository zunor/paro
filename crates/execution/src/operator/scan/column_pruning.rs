// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Column pruning helpers for scan operators.

use paro_storage::tablet::ColumnProjection;

/// Build a column projection for a scan.
///
/// If `column_ids` is empty, this projects all columns [0..total_columns).
pub fn build_column_projection(column_ids: &[usize], total_columns: usize) -> ColumnProjection {
    let output_columns = if column_ids.is_empty() {
        (0..total_columns).collect()
    } else {
        column_ids.to_vec()
    };

    ColumnProjection::new(output_columns)
}
