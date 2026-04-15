//! Logical Insert Operator

use crate::plan::LogicalPlan;
use paro_catalog::entry::TableCatalogEntry;
use paro_common::types::LogicalType;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct InsertOnConflict {
    pub target_columns: Vec<usize>,
    pub action: InsertOnConflictAction,
}

#[derive(Debug, Clone)]
pub enum InsertOnConflictAction {
    DoNothing,
    DoUpdate {
        target_columns: Vec<usize>,
        source_columns: Vec<usize>,
    },
}

#[derive(Debug)]
pub struct Insert {
    /// Target table
    pub table: Arc<TableCatalogEntry>,
    /// Column mapping (which input column goes to which table column)
    pub column_index_map: Vec<usize>,
    /// Expected types for the input columns
    pub expected_types: Vec<LogicalType>,
    /// Optional ON CONFLICT behavior.
    pub on_conflict: Option<InsertOnConflict>,
    /// The source of the data
    pub child: Box<LogicalPlan>,
}

impl Insert {
    pub fn new(
        table: Arc<TableCatalogEntry>,
        column_index_map: Vec<usize>,
        expected_types: Vec<LogicalType>,
        on_conflict: Option<InsertOnConflict>,
        child: LogicalPlan,
    ) -> Self {
        Self {
            table,
            column_index_map,
            expected_types,
            on_conflict,
            child: Box::new(child),
        }
    }
}
