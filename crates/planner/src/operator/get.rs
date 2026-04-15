//! Base table scan. Carries `TableCatalogEntry` so execution can open storage without another catalog lookup.

use std::sync::Arc;

use crate::expression::Expression;
use paro_catalog::entry::TableCatalogEntry;
use paro_common::types::LogicalType;
use paro_storage::table::segment_reorderer::SegmentOrderOptions;

/// Get represents a scan operation on a table.
///
/// This operator is created during planning when a base table is referenced
/// in the FROM clause. It holds all information needed to perform a table scan,
/// including an optional reference to the actual table catalog entry.
#[derive(Debug, Clone)]
pub struct Get {
    /// The index of the table in the bind context.
    pub table_index: usize,
    /// The types of the columns returned by this scan.
    pub returned_types: Vec<LogicalType>,
    /// The names of the columns returned by this scan.
    pub names: Vec<String>,
    /// Stable relation name used for explain output.
    pub relation_name: Option<String>,
    /// Optional user-visible alias.
    pub relation_alias: Option<String>,
    /// The column ids to read from the table.
    pub column_ids: Vec<usize>,
    /// The logical types of the columns in `column_ids`.
    pub column_types: Vec<LogicalType>,
    /// Reference to the table catalog entry.
    /// This provides access to table metadata and storage (segments) during
    /// physical plan generation.
    ///
    /// the table from the bind_data. Here we store it directly for simplicity.
    pub table: Option<Arc<TableCatalogEntry>>,
    /// Optional scan order for segments.
    pub scan_order: Option<SegmentOrderOptions>,
    /// Runtime filters injected by optimizer (e.g. join-derived min/max).
    /// Expressions are bound against this Get's output layout.
    pub runtime_filter_expressions: Vec<Expression>,
}

impl Get {
    /// Create a new Get with a reference to the table catalog entry.
    ///
    /// # Arguments
    /// * `table_index` - The index assigned to this table in the bind context
    /// * `names` - Column names returned by this scan
    /// * `types` - Column types returned by this scan
    /// * `table` - The table catalog entry (provides access to storage)
    pub fn new(
        table_index: usize,
        names: Vec<String>,
        types: Vec<LogicalType>,
        table: Arc<TableCatalogEntry>,
    ) -> Self {
        let column_ids: Vec<usize> = (0..types.len()).collect();
        Self {
            table_index,
            returned_types: types.clone(),
            names,
            relation_name: None,
            relation_alias: None,
            column_ids,
            column_types: types,
            table: Some(table),
            scan_order: None,
            runtime_filter_expressions: Vec::new(),
        }
    }

    /// Create a Get without a table reference.
    ///
    /// This is used for table functions or other scan sources that don't
    /// have a direct table catalog entry.
    pub fn new_without_table(
        table_index: usize,
        names: Vec<String>,
        types: Vec<LogicalType>,
    ) -> Self {
        let column_ids: Vec<usize> = (0..types.len()).collect();
        Self {
            table_index,
            returned_types: types.clone(),
            names,
            relation_name: None,
            relation_alias: None,
            column_ids,
            column_types: types,
            table: None,
            scan_order: None,
            runtime_filter_expressions: Vec::new(),
        }
    }

    /// Get the table catalog entry if available.
    pub fn get_table(&self) -> Option<&Arc<TableCatalogEntry>> {
        self.table.as_ref()
    }

    pub fn with_relation(mut self, relation_name: String, relation_alias: Option<String>) -> Self {
        self.relation_name = Some(relation_name);
        self.relation_alias = relation_alias;
        self
    }
}
