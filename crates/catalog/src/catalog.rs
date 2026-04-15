// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Abstract [`Catalog`] trait implemented by catalog backends (e.g. [`crate::database_catalog::ParoCatalog`]).

use crate::dependency::DependencyGraph;
use crate::entry::{
    CatalogEntryEnum, CreateSchemaInfo, DropSchemaInfo, OnEntryNotFound, SchemaEntry,
};
use crate::mvcc::CatalogSnapshot;
use paro_common::error::Result;
use std::sync::Arc;

// --- Constants ---

/// Default schema name (PG semantics: "public" is the default schema)
pub const DEFAULT_SCHEMA: &str = "public";

/// System schema name
pub const SYSTEM_SCHEMA: &str = "system";

/// Information schema name
pub const INFORMATION_SCHEMA: &str = "information_schema";

/// PostgreSQL catalog schema name
pub const PG_CATALOG: &str = "pg_catalog";

// --- Info Structs ---

/// Information for looking up an entry.
///
#[derive(Debug, Clone)]
pub struct EntryLookupInfo {
    /// The type of entry to look up
    pub catalog_type: crate::entry::CatalogType,
    /// The name of the entry
    pub name: String,
}

impl EntryLookupInfo {
    pub fn new(catalog_type: crate::entry::CatalogType, name: String) -> Self {
        Self { catalog_type, name }
    }

    pub fn schema(name: String) -> Self {
        Self::new(crate::entry::CatalogType::Schema, name)
    }

    pub fn table(name: String) -> Self {
        Self::new(crate::entry::CatalogType::Table, name)
    }

    pub fn view(name: String) -> Self {
        Self::new(crate::entry::CatalogType::View, name)
    }

    pub fn index(name: String) -> Self {
        Self::new(crate::entry::CatalogType::Index, name)
    }

    pub fn get_entry_name(&self) -> &str {
        &self.name
    }
}

/// Database size information.
///
#[derive(Debug, Clone, Default)]
pub struct DatabaseSize {
    /// Total bytes used
    pub bytes: u64,
    /// Number of blocks
    pub block_count: u64,
    /// Block size
    pub block_size: u64,
    /// Free blocks
    pub free_blocks: u64,
    /// Used blocks
    pub used_blocks: u64,
    /// WAL size
    pub wal_size: u64,
}

/// Metadata block information.
#[derive(Debug, Clone)]
pub struct MetadataBlockInfo {
    /// Block ID
    pub block_id: u64,
    /// Block type
    pub block_type: String,
    /// Number of entries
    pub entry_count: u64,
}

// --- Catalog Trait ---

/// The Catalog trait defines the interface for catalog implementations.
///
///
/// This is the abstract interface that all catalog implementations must implement.
pub trait Catalog: Send + Sync + std::fmt::Debug {
    /// Get the catalog name.
    ///
    fn name(&self) -> &str;

    /// Get the catalog type (e.g., "paro", "postgres").
    ///
    fn get_catalog_type(&self) -> &str;

    /// Check if this is a ParoCatalog.
    ///
    fn is_paro_catalog(&self) -> bool {
        false
    }

    /// Initialize the catalog.
    ///
    ///
    /// # Arguments
    /// * `load_builtin` - Whether to load built-in functions and types
    fn initialize(&self, load_builtin: bool);

    /// Create a schema in the catalog.
    ///
    ///
    /// # Arguments
    /// * `transaction` - The catalog transaction
    /// * `info` - Schema creation information
    ///
    /// # Returns
    /// The created schema entry, or None if IF NOT EXISTS and schema already exists
    fn create_schema(
        &self,
        transaction: &CatalogSnapshot,
        info: &CreateSchemaInfo,
    ) -> Result<Option<Arc<CatalogEntryEnum>>>;

    /// Scan all schemas visible to the transaction.
    ///
    fn scan_schemas<F>(&self, transaction: &CatalogSnapshot, callback: F)
    where
        F: FnMut(&SchemaEntry);

    /// Lookup a schema by name.
    ///
    ///
    /// # Arguments
    /// * `transaction` - The catalog transaction
    /// * `lookup` - Entry lookup information
    /// * `if_not_found` - What to do if the schema is not found
    ///
    /// # Returns
    /// The schema entry if found, or None/Error based on `if_not_found`
    fn lookup_schema(
        &self,
        transaction: &CatalogSnapshot,
        lookup: &EntryLookupInfo,
        if_not_found: OnEntryNotFound,
    ) -> Result<Option<Arc<SchemaEntry>>>;

    /// Drop a schema from the catalog.
    ///
    fn drop_schema(&self, transaction: &CatalogSnapshot, info: &DropSchemaInfo) -> Result<()>;

    /// Get database size information.
    ///
    fn get_database_size(&self) -> DatabaseSize;

    /// Get metadata block information.
    ///
    ///
    /// Returns information about metadata blocks in the storage manager.
    /// For in-memory catalogs, this returns an empty vector.
    fn get_metadata_info(&self) -> Vec<MetadataBlockInfo> {
        Vec::new()
    }

    /// Check if the catalog is in-memory.
    ///
    fn in_memory(&self) -> bool;

    /// Get the database path.
    ///
    fn get_db_path(&self) -> String;

    /// Get the dependency graph.
    ///
    fn get_dependency_graph(&self) -> Option<&DependencyGraph> {
        None
    }

    /// Get the default schema name.
    ///
    fn get_default_schema(&self) -> &str {
        DEFAULT_SCHEMA
    }

    /// Check if a schema name is a default/internal schema.
    fn is_default_schema(name: &str) -> bool {
        let lower = name.to_lowercase();
        lower == DEFAULT_SCHEMA
            || lower == SYSTEM_SCHEMA
            || lower == INFORMATION_SCHEMA
            || lower == PG_CATALOG
    }

    /// Get a schema by name (convenience method).
    ///
    /// This is a convenience method that throws an error if the schema is not found.
    fn get_schema(&self, transaction: &CatalogSnapshot, name: &str) -> Result<Arc<SchemaEntry>> {
        let lookup = EntryLookupInfo::schema(name.to_string());
        self.lookup_schema(transaction, &lookup, OnEntryNotFound::ThrowException)?
            .ok_or_else(|| paro_common::error::schema_not_found(name))
    }

    /// Get all schema names visible to the transaction.
    fn list_schemas(&self, transaction: &CatalogSnapshot) -> Vec<String> {
        let mut names = Vec::new();
        self.scan_schemas(transaction, |schema| {
            names.push(schema.name().to_string());
        });
        names
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_lookup_info() {
        let lookup = EntryLookupInfo::schema("public".to_string());
        assert_eq!(lookup.get_entry_name(), "public");
        assert_eq!(lookup.catalog_type, crate::entry::CatalogType::Schema);

        let lookup = EntryLookupInfo::table("users".to_string());
        assert_eq!(lookup.get_entry_name(), "users");
        assert_eq!(lookup.catalog_type, crate::entry::CatalogType::Table);
    }

    #[test]
    fn test_database_size_default() {
        let size = DatabaseSize::default();
        assert_eq!(size.bytes, 0);
        assert_eq!(size.block_count, 0);
    }
}
