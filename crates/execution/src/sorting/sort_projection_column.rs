// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Output projection metadata for sorted key and payload columns.

/// Mapping from key/payload layout columns to output columns.
///
/// This structure describes how to project columns from the sorted
/// key/payload data back to the output schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortProjectionColumn {
    /// Whether this column comes from the payload (true) or key (false)
    pub is_payload: bool,
    /// Index in the key or payload layout
    pub layout_col_idx: usize,
    /// Index in the output schema
    pub output_col_idx: usize,
}

impl SortProjectionColumn {
    /// Create a new sort projection column.
    pub fn new(is_payload: bool, layout_col_idx: usize, output_col_idx: usize) -> Self {
        Self {
            is_payload,
            layout_col_idx,
            output_col_idx,
        }
    }
}
