// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical `DELETE`. `RETURNING`, `USING`, and bound constraints are not fully wired.

use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::types::LogicalType;

use crate::plan::LogicalPlan;

/// Delete represents a DELETE operation in the logical plan.
///
/// The child operator provides the rows to delete (typically a scan + filter).
/// The delete operator identifies rows by their row_id column.
///
/// - Has a reference to the table being deleted from
/// - Has a table_index for result projection
/// - Has return_chunk flag for RETURNING clause
/// - Has bound_constraints for constraint checking
#[derive(Debug)]
pub struct Delete {
    /// The table to delete from.
    pub table: Arc<TableCatalogEntry>,
    /// The table index for this delete operation (used for column bindings).
    pub table_index: u32,
    /// Whether to return the deleted rows (RETURNING clause).
    /// Currently not supported, always false.
    pub return_chunk: bool,
    /// True when DELETE has no WHERE predicate and targets the full table.
    pub is_full_table_delete: bool,
    /// The child operator that produces rows to delete.
    /// This is typically a Get + Filter.
    pub child: Box<LogicalPlan>,
}

impl Delete {
    /// Create a new Delete operator.
    pub fn new(
        table: Arc<TableCatalogEntry>,
        table_index: u32,
        child: LogicalPlan,
        is_full_table_delete: bool,
    ) -> Self {
        Self {
            table,
            table_index,
            return_chunk: false,
            is_full_table_delete,
            child: Box::new(child),
        }
    }

    /// Get the name of this operator.
    pub fn name(&self) -> String {
        format!("DELETE FROM {}", self.table.base.base.name)
    }

    /// Get the output types of this operator.
    /// DELETE returns a single BIGINT column with the count of deleted rows.
    pub fn get_types(&self) -> Vec<LogicalType> {
        vec![LogicalType::BigInt]
    }

    /// Get the output column names.
    pub fn get_names(&self) -> Vec<String> {
        vec!["Count".to_string()]
    }
}
