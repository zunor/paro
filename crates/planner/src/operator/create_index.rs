//! Logical operator for `CREATE INDEX`. Ser/de and `ALTER TABLE … ADD CONSTRAINT` integration are incomplete.

use crate::binder::ir::statement::BoundCreateIndexInfo;
use crate::expression::Expression;
use paro_catalog::entry::{CreateIndexInfo, TableCatalogEntry};
use paro_common::types::LogicalType;
use std::sync::Arc;

/// CreateIndex represents a CREATE INDEX operation in the logical plan.
///
/// This operator is responsible for creating an index on a table. It contains:
/// - The index creation metadata (name, type, columns, etc.)
/// - A reference to the target table
/// - The bound expressions for the indexed columns
/// - Unbound expressions for serialization purposes
///
/// The operator is a DDL node and does not return a data rowset.
///
/// - Stores CreateIndexInfo with index metadata
/// - Holds a reference to TableCatalogEntry
/// - Keeps both bound and unbound expressions
/// - Optionally stores AlterTableInfo for ALTER TABLE ADD CONSTRAINT
#[derive(Debug, Clone)]
pub struct CreateIndex {
    /// Index creation information containing all metadata
    pub info: CreateIndexInfo,

    /// Reference to the target table
    pub table: Arc<TableCatalogEntry>,

    /// Bound expressions for the indexed columns
    /// These are used during execution to evaluate index keys
    pub expressions: Vec<Expression>,

    /// Unbound expressions (copies of the original expressions)
    /// These are kept for serialization and plan display purposes
    pub unbound_expressions: Vec<Expression>,
}

impl CreateIndex {
    /// Create a new CreateIndex from bound information.
    ///
    /// # Arguments
    /// * `bound_info` - The bound CREATE INDEX information from the binder
    ///
    /// # Returns
    /// A new CreateIndex operator
    pub fn new(bound_info: BoundCreateIndexInfo) -> Self {
        // Clone expressions for unbound_expressions (for serialization)
        let unbound_expressions = bound_info.expressions.clone();

        Self {
            info: bound_info.info,
            table: bound_info.table,
            expressions: bound_info.expressions,
            unbound_expressions,
        }
    }

    /// Create a CreateIndex with explicit parameters.
    ///
    /// # Arguments
    /// * `info` - The CreateIndexInfo containing index metadata
    /// * `table` - Reference to the target table
    /// * `expressions` - Bound expressions for indexed columns
    pub fn with_info(
        info: CreateIndexInfo,
        table: Arc<TableCatalogEntry>,
        expressions: Vec<Expression>,
    ) -> Self {
        let unbound_expressions = expressions.clone();
        Self {
            info,
            table,
            expressions,
            unbound_expressions,
        }
    }

    /// Get the return types for this operator.
    ///
    /// CREATE INDEX is a DDL statement and returns no columns.
    pub fn get_types(&self) -> Vec<LogicalType> {
        vec![]
    }

    /// Get the index name.
    pub fn index_name(&self) -> &str {
        &self.info.name
    }

    /// Get the table name.
    pub fn table_name(&self) -> &str {
        &self.info.table_name
    }

    /// Get the schema name.
    pub fn schema_name(&self) -> &str {
        &self.info.schema
    }

    /// Check if this is a unique index.
    pub fn is_unique(&self) -> bool {
        self.info.is_unique()
    }

    /// Check if IF NOT EXISTS was specified.
    pub fn if_not_exists(&self) -> bool {
        self.info.if_not_exists
    }

    /// Get the column IDs being indexed.
    pub fn column_ids(&self) -> &[paro_catalog::entry::LogicalIndex] {
        &self.info.column_ids
    }

    /// Get the column types being indexed.
    pub fn column_types(&self) -> &[LogicalType] {
        &self.info.column_types
    }

    /// Get the index type.
    pub fn index_type(&self) -> paro_catalog::entry::IndexType {
        self.info.index_type
    }

    /// Get a display name for this operator.
    pub fn name(&self) -> &'static str {
        "CREATE_INDEX"
    }

    /// Get the full qualified name of the index.
    pub fn full_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.table.database_name(),
            self.info.schema,
            self.info.name
        )
    }
}
