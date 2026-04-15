//! Catalog Entry module
//!
//!
//! ## Entry Hierarchy
//!
//! ```text
//! CatalogEntry (base trait)
//! ├── InCatalogEntry (belongs to a catalog/database)
//! │   ├── SchemaEntry
//! │   └── StandardEntry (belongs to a schema)
//! │       ├── TableCatalogEntry
//! │       ├── ViewCatalogEntry
//! │       ├── IndexCatalogEntry
//! │       ├── SequenceCatalogEntry
//! │       ├── ScalarFunctionCatalogEntry
//! │       ├── AggregateFunctionCatalogEntry
//! │       ├── TableFunctionCatalogEntry
//! │       ├── CopyFunctionCatalogEntry
//! │       └── TypeCatalogEntry
//! ```

// Base types (CatalogEntry trait, CatalogEntryMeta, etc.)
mod catalog_entry;

// Entry type implementations
mod copy_function;
mod function;
mod index;
mod property_graph;
mod schema;
mod sequence;
mod table;
mod type_entry;
mod view;

// Re-export base types
pub use catalog_entry::{
    allocate_object_id, AlterInfo, AlterType, CatalogEntry, CatalogEntryInfo, CatalogEntryMeta,
    CatalogObjectId, CatalogObjectRef, CatalogType, CreateInfo, Dependency, DependencyList,
    DependencyType, InCatalogEntry, OnCreateConflict, OnEntryNotFound, SchemaEntryMeta,
    StandardEntry,
};

// Re-export entry types
pub use copy_function::CopyFunctionCatalogEntry;
pub use function::{
    AggregateFunctionCatalogEntry, ScalarFunctionCatalogEntry, TableFunctionCatalogEntry,
};
pub use index::{
    CreateIndexInfo, FullTextIndexBinding, IndexBuildState, IndexCatalogEntry, IndexCoverage,
    IndexType, LogicalIndex,
};
pub use property_graph::{
    graph_schema_fingerprint, CreatePropertyGraphInfo, EdgeTableInfo, PropertyGraphCatalogEntry,
    VertexTableInfo,
};
pub use schema::{
    AlterEntryAction, AlterEntryInfo, ColumnCommentUpdate, CreateSchemaInfo, DropEntryInfo,
    DropSchemaInfo, SchemaEntry,
};
pub use sequence::{CreateSequenceInfo, SequenceCatalogEntry, SequenceData};
pub use table::{
    ColumnDefinition, Constraint, ConstraintType, CreateTableInfo, TableCatalogEntry, TableType,
};
pub use type_entry::{CreateTypeInfo, TypeCatalogEntry};
pub use view::{CreateViewInfo, ViewCatalogEntry};

use std::sync::Arc;

// ============================================================================
// CatalogEntryEnum - Enum wrapper for all entry types
// ============================================================================

/// Catalog entry enum representing all types of catalog entries.
///
/// This enum provides a unified way to handle different entry types
/// and is used in CatalogCollection for storage.
#[derive(Debug, Clone)]
pub enum CatalogEntryEnum {
    Schema(Arc<SchemaEntry>),
    Table(Arc<TableCatalogEntry>),
    View(Arc<ViewCatalogEntry>),
    Index(Arc<IndexCatalogEntry>),
    PropertyGraph(Arc<PropertyGraphCatalogEntry>),
    Sequence(Arc<SequenceCatalogEntry>),
    ScalarFunction(Arc<ScalarFunctionCatalogEntry>),
    AggregateFunction(Arc<AggregateFunctionCatalogEntry>),
    TableFunction(Arc<TableFunctionCatalogEntry>),
    CopyFunction(Arc<CopyFunctionCatalogEntry>),
    Type(Arc<TypeCatalogEntry>),
}

impl CatalogEntryEnum {
    /// Get the entry type
    pub fn entry_type(&self) -> CatalogType {
        match self {
            CatalogEntryEnum::Schema(_) => CatalogType::Schema,
            CatalogEntryEnum::Table(_) => CatalogType::Table,
            CatalogEntryEnum::View(_) => CatalogType::View,
            CatalogEntryEnum::Index(_) => CatalogType::Index,
            CatalogEntryEnum::PropertyGraph(_) => CatalogType::PropertyGraph,
            CatalogEntryEnum::Sequence(_) => CatalogType::Sequence,
            CatalogEntryEnum::ScalarFunction(_) => CatalogType::ScalarFunction,
            CatalogEntryEnum::AggregateFunction(_) => CatalogType::AggregateFunction,
            CatalogEntryEnum::TableFunction(_) => CatalogType::TableFunction,
            CatalogEntryEnum::CopyFunction(_) => CatalogType::CopyFunction,
            CatalogEntryEnum::Type(_) => CatalogType::Type,
        }
    }

    /// Get the entry name
    pub fn name(&self) -> &str {
        match self {
            CatalogEntryEnum::Schema(e) => &e.base.name,
            CatalogEntryEnum::Table(e) => &e.base.base.name,
            CatalogEntryEnum::View(e) => &e.base.base.name,
            CatalogEntryEnum::Index(e) => &e.base.base.name,
            CatalogEntryEnum::PropertyGraph(e) => &e.base.base.name,
            CatalogEntryEnum::Sequence(e) => &e.base.base.name,
            CatalogEntryEnum::ScalarFunction(e) => &e.base.base.name,
            CatalogEntryEnum::AggregateFunction(e) => &e.base.base.name,
            CatalogEntryEnum::TableFunction(e) => &e.base.base.name,
            CatalogEntryEnum::CopyFunction(e) => &e.base.base.name,
            CatalogEntryEnum::Type(e) => &e.base.base.name,
        }
    }

    /// Stable persisted object identity.
    pub fn object_id(&self) -> CatalogObjectId {
        match self {
            CatalogEntryEnum::Schema(e) => e.base.object_id,
            CatalogEntryEnum::Table(e) => e.base.base.object_id,
            CatalogEntryEnum::View(e) => e.base.base.object_id,
            CatalogEntryEnum::Index(e) => e.base.base.object_id,
            CatalogEntryEnum::PropertyGraph(e) => e.base.base.object_id,
            CatalogEntryEnum::Sequence(e) => e.base.base.object_id,
            CatalogEntryEnum::ScalarFunction(e) => e.base.base.object_id,
            CatalogEntryEnum::AggregateFunction(e) => e.base.base.object_id,
            CatalogEntryEnum::TableFunction(e) => e.base.base.object_id,
            CatalogEntryEnum::CopyFunction(e) => e.base.base.object_id,
            CatalogEntryEnum::Type(e) => e.base.base.object_id,
        }
    }

    /// Get the timestamp
    pub fn timestamp(&self) -> u64 {
        match self {
            CatalogEntryEnum::Schema(e) => e.base.timestamp(),
            CatalogEntryEnum::Table(e) => e.base.base.timestamp(),
            CatalogEntryEnum::View(e) => e.base.base.timestamp(),
            CatalogEntryEnum::Index(e) => e.base.base.timestamp(),
            CatalogEntryEnum::PropertyGraph(e) => e.base.base.timestamp(),
            CatalogEntryEnum::Sequence(e) => e.base.base.timestamp(),
            CatalogEntryEnum::ScalarFunction(e) => e.base.base.timestamp(),
            CatalogEntryEnum::AggregateFunction(e) => e.base.base.timestamp(),
            CatalogEntryEnum::TableFunction(e) => e.base.base.timestamp(),
            CatalogEntryEnum::CopyFunction(e) => e.base.base.timestamp(),
            CatalogEntryEnum::Type(e) => e.base.base.timestamp(),
        }
    }

    /// Check if deleted
    pub fn is_deleted(&self) -> bool {
        match self {
            CatalogEntryEnum::Schema(e) => e.base.is_deleted(),
            CatalogEntryEnum::Table(e) => e.base.base.is_deleted(),
            CatalogEntryEnum::View(e) => e.base.base.is_deleted(),
            CatalogEntryEnum::Index(e) => e.base.base.is_deleted(),
            CatalogEntryEnum::PropertyGraph(e) => e.base.base.is_deleted(),
            CatalogEntryEnum::Sequence(e) => e.base.base.is_deleted(),
            CatalogEntryEnum::ScalarFunction(e) => e.base.base.is_deleted(),
            CatalogEntryEnum::AggregateFunction(e) => e.base.base.is_deleted(),
            CatalogEntryEnum::TableFunction(e) => e.base.base.is_deleted(),
            CatalogEntryEnum::CopyFunction(e) => e.base.base.is_deleted(),
            CatalogEntryEnum::Type(e) => e.base.base.is_deleted(),
        }
    }

    /// Check if internal
    pub fn is_internal(&self) -> bool {
        match self {
            CatalogEntryEnum::Schema(e) => e.internal,
            CatalogEntryEnum::Table(e) => e.base.base.internal,
            CatalogEntryEnum::View(e) => e.base.base.internal,
            CatalogEntryEnum::Index(e) => e.base.base.internal,
            CatalogEntryEnum::PropertyGraph(e) => e.base.base.internal,
            CatalogEntryEnum::Sequence(e) => e.base.base.internal,
            CatalogEntryEnum::ScalarFunction(e) => e.base.base.internal,
            CatalogEntryEnum::AggregateFunction(e) => e.base.base.internal,
            CatalogEntryEnum::TableFunction(e) => e.base.base.internal,
            CatalogEntryEnum::CopyFunction(e) => e.base.base.internal,
            CatalogEntryEnum::Type(e) => e.base.base.internal,
        }
    }

    /// Get catalog (database) name
    pub fn catalog_name(&self) -> &str {
        match self {
            CatalogEntryEnum::Schema(e) => &e.base.catalog,
            CatalogEntryEnum::Table(e) => &e.base.base.catalog,
            CatalogEntryEnum::View(e) => &e.base.base.catalog,
            CatalogEntryEnum::Index(e) => &e.base.base.catalog,
            CatalogEntryEnum::PropertyGraph(e) => &e.base.base.catalog,
            CatalogEntryEnum::Sequence(e) => &e.base.base.catalog,
            CatalogEntryEnum::ScalarFunction(e) => &e.base.base.catalog,
            CatalogEntryEnum::AggregateFunction(e) => &e.base.base.catalog,
            CatalogEntryEnum::TableFunction(e) => &e.base.base.catalog,
            CatalogEntryEnum::CopyFunction(e) => &e.base.base.catalog,
            CatalogEntryEnum::Type(e) => &e.base.base.catalog,
        }
    }

    /// Convert to SQL string
    pub fn to_sql(&self) -> String {
        match self {
            CatalogEntryEnum::Schema(e) => format!("CREATE SCHEMA {};", e.base.name),
            CatalogEntryEnum::Table(e) => e.to_sql(),
            CatalogEntryEnum::View(e) => e.to_sql(),
            CatalogEntryEnum::Index(e) => e.to_sql(),
            CatalogEntryEnum::PropertyGraph(e) => e.to_sql(),
            CatalogEntryEnum::Sequence(e) => e.to_sql(),
            CatalogEntryEnum::ScalarFunction(e) => {
                format!("-- SCALAR FUNCTION {} (built-in)", e.base.base.name)
            }
            CatalogEntryEnum::AggregateFunction(e) => {
                format!("-- AGGREGATE FUNCTION {} (built-in)", e.base.base.name)
            }
            CatalogEntryEnum::TableFunction(e) => {
                format!("-- TABLE FUNCTION {} (built-in)", e.base.base.name)
            }
            CatalogEntryEnum::CopyFunction(e) => {
                format!("-- COPY FUNCTION {} (built-in)", e.base.base.name)
            }
            CatalogEntryEnum::Type(e) => e.to_sql(),
        }
    }

    /// Try to get as schema entry
    pub fn as_schema(&self) -> Option<&SchemaEntry> {
        match self {
            CatalogEntryEnum::Schema(e) => Some(e),
            _ => None,
        }
    }

    /// Try to get as table entry
    pub fn as_table(&self) -> Option<&TableCatalogEntry> {
        match self {
            CatalogEntryEnum::Table(e) => Some(e),
            _ => None,
        }
    }

    /// Try to get as view entry
    pub fn as_view(&self) -> Option<&ViewCatalogEntry> {
        match self {
            CatalogEntryEnum::View(e) => Some(e),
            _ => None,
        }
    }

    /// Try to get as index entry
    pub fn as_index(&self) -> Option<&IndexCatalogEntry> {
        match self {
            CatalogEntryEnum::Index(e) => Some(e),
            _ => None,
        }
    }

    /// Try to get as property graph entry
    pub fn as_property_graph(&self) -> Option<&PropertyGraphCatalogEntry> {
        match self {
            CatalogEntryEnum::PropertyGraph(e) => Some(e),
            _ => None,
        }
    }

    /// Try to get as sequence entry
    pub fn as_sequence(&self) -> Option<&SequenceCatalogEntry> {
        match self {
            CatalogEntryEnum::Sequence(e) => Some(e),
            _ => None,
        }
    }

    /// Try to get as scalar function entry
    pub fn as_scalar_function(&self) -> Option<&ScalarFunctionCatalogEntry> {
        match self {
            CatalogEntryEnum::ScalarFunction(e) => Some(e),
            _ => None,
        }
    }

    /// Try to get as aggregate function entry
    pub fn as_aggregate_function(&self) -> Option<&AggregateFunctionCatalogEntry> {
        match self {
            CatalogEntryEnum::AggregateFunction(e) => Some(e),
            _ => None,
        }
    }

    /// Try to get as table function entry
    pub fn as_table_function(&self) -> Option<&TableFunctionCatalogEntry> {
        match self {
            CatalogEntryEnum::TableFunction(e) => Some(e),
            _ => None,
        }
    }

    /// Try to get as copy function entry
    pub fn as_copy_function(&self) -> Option<&CopyFunctionCatalogEntry> {
        match self {
            CatalogEntryEnum::CopyFunction(e) => Some(e),
            _ => None,
        }
    }

    /// Try to get as type entry
    pub fn as_type(&self) -> Option<&TypeCatalogEntry> {
        match self {
            CatalogEntryEnum::Type(e) => Some(e),
            _ => None,
        }
    }

    pub fn schema_name(&self) -> Option<&str> {
        match self {
            CatalogEntryEnum::Schema(_) => None,
            CatalogEntryEnum::Table(e) => Some(e.base.schema_name.as_str()),
            CatalogEntryEnum::View(e) => Some(e.base.schema_name.as_str()),
            CatalogEntryEnum::Index(e) => Some(e.base.schema_name.as_str()),
            CatalogEntryEnum::PropertyGraph(e) => Some(e.base.schema_name.as_str()),
            CatalogEntryEnum::Sequence(e) => Some(e.base.schema_name.as_str()),
            CatalogEntryEnum::ScalarFunction(e) => Some(e.base.schema_name.as_str()),
            CatalogEntryEnum::AggregateFunction(e) => Some(e.base.schema_name.as_str()),
            CatalogEntryEnum::TableFunction(e) => Some(e.base.schema_name.as_str()),
            CatalogEntryEnum::CopyFunction(e) => Some(e.base.schema_name.as_str()),
            CatalogEntryEnum::Type(e) => Some(e.base.schema_name.as_str()),
        }
    }

    pub fn dependency_list(&self) -> DependencyList {
        match self {
            CatalogEntryEnum::Schema(_) => DependencyList::new(),
            CatalogEntryEnum::Table(e) => e.base.dependencies(),
            CatalogEntryEnum::View(e) => e.base.dependencies(),
            CatalogEntryEnum::Index(e) => e.base.dependencies(),
            CatalogEntryEnum::PropertyGraph(e) => e.base.dependencies(),
            CatalogEntryEnum::Sequence(e) => e.base.dependencies(),
            CatalogEntryEnum::ScalarFunction(e) => e.base.dependencies(),
            CatalogEntryEnum::AggregateFunction(e) => e.base.dependencies(),
            CatalogEntryEnum::TableFunction(e) => e.base.dependencies(),
            CatalogEntryEnum::CopyFunction(e) => e.base.dependencies(),
            CatalogEntryEnum::Type(e) => e.base.dependencies(),
        }
    }

    pub fn object_ref(&self, schema_id: Option<CatalogObjectId>) -> CatalogObjectRef {
        match self {
            CatalogEntryEnum::Schema(e) => CatalogObjectRef::schema(
                e.base.object_id,
                e.base.catalog.clone(),
                e.base.name.clone(),
            ),
            CatalogEntryEnum::Table(e) => CatalogObjectRef::in_schema(
                e.base.base.object_id,
                CatalogType::Table,
                e.base.base.catalog.clone(),
                schema_id,
                e.base.schema_name.clone(),
                e.base.base.name.clone(),
            ),
            CatalogEntryEnum::View(e) => CatalogObjectRef::in_schema(
                e.base.base.object_id,
                CatalogType::View,
                e.base.base.catalog.clone(),
                schema_id,
                e.base.schema_name.clone(),
                e.base.base.name.clone(),
            ),
            CatalogEntryEnum::Index(e) => CatalogObjectRef::in_schema(
                e.base.base.object_id,
                CatalogType::Index,
                e.base.base.catalog.clone(),
                schema_id,
                e.base.schema_name.clone(),
                e.base.base.name.clone(),
            ),
            CatalogEntryEnum::PropertyGraph(e) => CatalogObjectRef::in_schema(
                e.base.base.object_id,
                CatalogType::PropertyGraph,
                e.base.base.catalog.clone(),
                schema_id,
                e.base.schema_name.clone(),
                e.base.base.name.clone(),
            ),
            CatalogEntryEnum::Sequence(e) => CatalogObjectRef::in_schema(
                e.base.base.object_id,
                CatalogType::Sequence,
                e.base.base.catalog.clone(),
                schema_id,
                e.base.schema_name.clone(),
                e.base.base.name.clone(),
            ),
            CatalogEntryEnum::ScalarFunction(e) => CatalogObjectRef::in_schema(
                e.base.base.object_id,
                CatalogType::ScalarFunction,
                e.base.base.catalog.clone(),
                schema_id,
                e.base.schema_name.clone(),
                e.base.base.name.clone(),
            ),
            CatalogEntryEnum::AggregateFunction(e) => CatalogObjectRef::in_schema(
                e.base.base.object_id,
                CatalogType::AggregateFunction,
                e.base.base.catalog.clone(),
                schema_id,
                e.base.schema_name.clone(),
                e.base.base.name.clone(),
            ),
            CatalogEntryEnum::TableFunction(e) => CatalogObjectRef::in_schema(
                e.base.base.object_id,
                CatalogType::TableFunction,
                e.base.base.catalog.clone(),
                schema_id,
                e.base.schema_name.clone(),
                e.base.base.name.clone(),
            ),
            CatalogEntryEnum::CopyFunction(e) => CatalogObjectRef::in_schema(
                e.base.base.object_id,
                CatalogType::CopyFunction,
                e.base.base.catalog.clone(),
                schema_id,
                e.base.schema_name.clone(),
                e.base.base.name.clone(),
            ),
            CatalogEntryEnum::Type(e) => CatalogObjectRef::in_schema(
                e.base.base.object_id,
                CatalogType::Type,
                e.base.base.catalog.clone(),
                schema_id,
                e.base.schema_name.clone(),
                e.base.base.name.clone(),
            ),
        }
    }
}

// ============================================================================
// TableOrView - Union type for table/view lookups
// ============================================================================

/// Represents either a table or a view entry.
///
/// This enum is used when looking up a table reference that could be
/// either a table or a view. Views are expanded to subqueries during binding.
#[derive(Debug, Clone)]
pub enum TableOrView {
    /// A table catalog entry
    Table(Arc<TableCatalogEntry>),
    /// A view catalog entry
    View(Arc<ViewCatalogEntry>),
}

impl TableOrView {
    /// Returns true if this is a table
    pub fn is_table(&self) -> bool {
        matches!(self, TableOrView::Table(_))
    }

    /// Returns true if this is a view
    pub fn is_view(&self) -> bool {
        matches!(self, TableOrView::View(_))
    }

    /// Get the name of the table or view
    pub fn name(&self) -> &str {
        match self {
            TableOrView::Table(t) => &t.base.base.name,
            TableOrView::View(v) => &v.base.base.name,
        }
    }

    /// Get the schema name of the table or view
    pub fn schema_name(&self) -> &str {
        match self {
            TableOrView::Table(t) => &t.base.schema_name,
            TableOrView::View(v) => &v.base.schema_name,
        }
    }

    /// Try to get as a table entry
    pub fn as_table(&self) -> Option<&Arc<TableCatalogEntry>> {
        match self {
            TableOrView::Table(t) => Some(t),
            TableOrView::View(_) => None,
        }
    }

    /// Try to get as a view entry
    pub fn as_view(&self) -> Option<&Arc<ViewCatalogEntry>> {
        match self {
            TableOrView::Table(_) => None,
            TableOrView::View(v) => Some(v),
        }
    }
}
