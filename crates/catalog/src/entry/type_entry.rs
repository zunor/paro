// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Type Catalog Entry
//!
//!
//! This module defines TypeCatalogEntry for user-defined types.

use super::catalog_entry::{
    allocate_object_id, AlterInfo, CatalogEntry, CatalogObjectId, CatalogType, CreateInfo,
    DependencyList, InCatalogEntry, OnCreateConflict, SchemaEntryMeta, StandardEntry,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Weak};

// --- CreateTypeInfo ---

/// Information for creating a type entry.
///
#[derive(Debug, Clone)]
pub struct CreateTypeInfo {
    /// Catalog name
    pub catalog: String,
    /// Schema name
    pub schema: String,
    /// Type name
    pub name: String,
    /// The logical type
    pub logical_type: LogicalType,
    /// On conflict behavior
    pub on_conflict: OnCreateConflict,
    /// Whether this is temporary
    pub temporary: bool,
    /// Whether this is internal
    pub internal: bool,
    /// Dependencies
    pub dependencies: DependencyList,
}

impl CreateTypeInfo {
    pub fn new(catalog: String, schema: String, name: String, logical_type: LogicalType) -> Self {
        Self {
            catalog,
            schema,
            name,
            logical_type,
            on_conflict: OnCreateConflict::ErrorOnConflict,
            temporary: false,
            internal: false,
            dependencies: DependencyList::new(),
        }
    }

    pub fn with_on_conflict(mut self, on_conflict: OnCreateConflict) -> Self {
        self.on_conflict = on_conflict;
        self
    }

    pub fn with_internal(mut self) -> Self {
        self.internal = true;
        self
    }
}

// --- TypeCatalogEntry ---

/// Type catalog entry for user-defined types.
///
#[derive(Debug)]
pub struct TypeCatalogEntry {
    /// Standard entry base (includes schema reference)
    pub base: SchemaEntryMeta,
    /// The logical type this entry represents
    pub user_type: LogicalType,
}

impl TypeCatalogEntry {
    /// Create a new type catalog entry.
    pub fn new(info: CreateTypeInfo, timestamp: u64) -> Self {
        let oid = allocate_object_id();
        let mut base = SchemaEntryMeta::new(
            CatalogType::Type,
            info.catalog,
            info.schema,
            info.name,
            oid,
            timestamp,
        );
        base.base.internal = info.internal;
        base.base.temporary = info.temporary;
        base.set_dependencies(info.dependencies);

        Self {
            base,
            user_type: info.logical_type,
        }
    }

    /// Get the logical type
    pub fn get_type(&self) -> &LogicalType {
        &self.user_type
    }

    /// Convert to SQL string
    pub fn to_sql(&self) -> String {
        format!(
            "CREATE TYPE {}.{} AS {};",
            self.base.schema_name, self.base.base.name, self.user_type
        )
    }
}

// --- CatalogEntry trait implementation ---

impl CatalogEntry for TypeCatalogEntry {
    fn object_id(&self) -> CatalogObjectId {
        self.base.base.object_id
    }

    fn name(&self) -> &str {
        &self.base.base.name
    }

    fn entry_type(&self) -> CatalogType {
        CatalogType::Type
    }

    fn catalog_name(&self) -> &str {
        &self.base.base.catalog
    }

    fn timestamp(&self) -> u64 {
        self.base.base.timestamp()
    }

    fn set_timestamp(&self, ts: u64) {
        self.base.base.set_timestamp(ts);
    }

    fn is_deleted(&self) -> bool {
        self.base.base.is_deleted()
    }

    fn set_deleted(&self, deleted: bool) {
        self.base.base.set_deleted(deleted);
    }

    fn child(&self) -> Option<Arc<dyn CatalogEntry>> {
        self.base.base.child()
    }

    fn set_child(&self, child: Option<Arc<dyn CatalogEntry>>) {
        self.base.base.set_child(child);
    }

    fn parent(&self) -> Option<Arc<dyn CatalogEntry>> {
        self.base.base.parent()
    }

    fn set_parent(&self, parent: Option<Weak<dyn CatalogEntry>>) {
        self.base.base.set_parent(parent);
    }

    fn is_temporary(&self) -> bool {
        self.base.base.temporary
    }

    fn is_internal(&self) -> bool {
        self.base.base.internal
    }

    fn comment(&self) -> Option<&str> {
        // Return reference from RwLock - this is a limitation, return None for now
        // In practice, use base.base.comment() which returns Option<String>
        None
    }

    fn set_comment(&self, comment: Option<String>) {
        self.base.base.set_comment(comment);
    }

    fn tags(&self) -> &HashMap<String, String> {
        static EMPTY: LazyLock<HashMap<String, String>> = LazyLock::new(HashMap::new);
        &EMPTY
    }

    fn set_tags(&self, tags: HashMap<String, String>) {
        self.base.base.set_tags(tags);
    }

    fn alter(&self, info: &AlterInfo) -> Result<Arc<dyn CatalogEntry>> {
        // Handle SET COMMENT
        if let Some(new_comment) = &info.new_comment {
            let new_entry = TypeCatalogEntry {
                base: SchemaEntryMeta::new(
                    CatalogType::Type,
                    self.base.base.catalog.clone(),
                    self.base.schema_name.clone(),
                    self.base.base.name.clone(),
                    self.base.base.object_id,
                    info.catalog.parse().unwrap_or(0),
                ),
                user_type: self.user_type.clone(),
            };
            new_entry.base.base.set_comment(Some(new_comment.clone()));
            return Ok(Arc::new(new_entry));
        }

        Err(paro_error::not_implemented("ALTER TYPE"))
    }

    fn undo_alter(&self, _info: &AlterInfo) -> Result<()> {
        Ok(())
    }

    fn rollback(&self, _prev_entry: &dyn CatalogEntry) -> Result<()> {
        Ok(())
    }

    fn copy(&self) -> Result<Arc<dyn CatalogEntry>> {
        let new_entry = TypeCatalogEntry {
            base: SchemaEntryMeta::new(
                CatalogType::Type,
                self.base.base.catalog.clone(),
                self.base.schema_name.clone(),
                self.base.base.name.clone(),
                self.base.base.object_id,
                self.base.base.timestamp(),
            ),
            user_type: self.user_type.clone(),
        };
        Ok(Arc::new(new_entry))
    }

    fn get_info(&self) -> Result<CreateInfo> {
        let mut info = CreateInfo::new(
            CatalogType::Type,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
        );
        info.temporary = self.base.base.temporary;
        info.internal = self.base.base.internal;
        info.sql = Some(self.to_sql());
        Ok(info)
    }

    fn set_as_root(&self) {
        // No-op for now
    }

    fn to_sql(&self) -> String {
        self.to_sql()
    }

    fn serialize(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        // Serialize base
        self.base.base.serialize(writer)?;

        // Serialize schema name
        let schema_bytes = self.base.schema_name.as_bytes();
        writer.write_all(&(schema_bytes.len() as u32).to_le_bytes())?;
        writer.write_all(schema_bytes)?;

        // Serialize user type to a buffer first (LogicalType::serialize requires Sized)
        let mut type_buffer = Vec::new();
        self.user_type.serialize(&mut type_buffer)?;
        writer.write_all(&type_buffer)?;

        Ok(())
    }
}

// --- StandardEntry trait implementation ---

impl StandardEntry for TypeCatalogEntry {
    fn schema_name(&self) -> &str {
        &self.base.schema_name
    }

    fn dependencies(&self) -> &DependencyList {
        static EMPTY: LazyLock<DependencyList> = LazyLock::new(DependencyList::new);
        &EMPTY
    }

    fn set_dependencies(&self, dependencies: DependencyList) {
        self.base.set_dependencies(dependencies);
    }
}

// --- InCatalogEntry trait implementation ---

impl InCatalogEntry for TypeCatalogEntry {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_catalog_entry() {
        let info = CreateTypeInfo::new(
            "main".to_string(),
            "public".to_string(),
            "my_type".to_string(),
            LogicalType::Integer,
        );

        let entry = TypeCatalogEntry::new(info, 100);

        assert_eq!(entry.name(), "my_type");
        assert_eq!(entry.entry_type(), CatalogType::Type);
        assert_eq!(entry.catalog_name(), "main");
        assert_eq!(entry.schema_name(), "public");
        assert_eq!(entry.timestamp(), 100);
        assert!(!entry.is_deleted());
    }

    #[test]
    fn test_to_sql() {
        let info = CreateTypeInfo::new(
            "main".to_string(),
            "public".to_string(),
            "my_int".to_string(),
            LogicalType::Integer,
        );

        let entry = TypeCatalogEntry::new(info, 100);
        let sql = entry.to_sql();

        assert!(sql.contains("CREATE TYPE"));
        assert!(sql.contains("my_int"));
    }
}
