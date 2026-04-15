//! Logical `UPDATE`. `RETURNING`, `FROM`, defaults, and constraints are not fully wired.

use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::types::LogicalType;

use crate::expression::Expression;

use crate::plan::LogicalPlan;

/// Update represents an UPDATE operation in the logical plan.
///
/// The child operator provides the rows to update (typically a scan + filter + projection).
/// The update operator modifies specific columns with new values.
///
/// - Has a reference to the table being updated
/// - Has a table_index for result projection
/// - Has return_chunk flag for RETURNING clause
/// - Has columns (PhysicalIndex) indicating which columns to update
/// - Has bound_defaults for DEFAULT values
/// - Has bound_constraints for constraint checking
/// - Has update_is_del_and_insert flag for certain update strategies
#[derive(Debug)]
pub struct Update {
    /// The table to update.
    pub table: Arc<TableCatalogEntry>,
    /// The table index for this update operation (used for column bindings).
    pub table_index: u32,
    /// Whether to return the updated rows (RETURNING clause).
    /// Currently not supported, always false.
    pub return_chunk: bool,
    /// The indices of the columns being updated.
    pub columns: Vec<usize>,
    /// The expressions for the new values.
    /// These are the bound expressions from the SET clause.
    pub expressions: Vec<Expression>,
    /// The child operator that produces rows to update.
    /// This is typically a Get + Filter + Projection.
    pub child: Box<LogicalPlan>,
}

impl Update {
    /// Create a new Update operator.
    pub fn new(
        table: Arc<TableCatalogEntry>,
        table_index: u32,
        columns: Vec<usize>,
        expressions: Vec<Expression>,
        child: LogicalPlan,
    ) -> Self {
        Self {
            table,
            table_index,
            return_chunk: false,
            columns,
            expressions,
            child: Box::new(child),
        }
    }

    /// Get the name of this operator.
    pub fn name(&self) -> String {
        format!("UPDATE {}", self.table.base.base.name)
    }

    /// Get the output types of this operator.
    /// UPDATE returns a single BIGINT column with the count of updated rows.
    pub fn get_types(&self) -> Vec<LogicalType> {
        vec![LogicalType::BigInt]
    }

    /// Get the output column names.
    pub fn get_names(&self) -> Vec<String> {
        vec!["Count".to_string()]
    }

    /// Get the column names being updated.
    pub fn get_updated_column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter_map(|&idx| self.table.columns.get(idx).map(|c| c.name.clone()))
            .collect()
    }
}
