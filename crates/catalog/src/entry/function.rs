// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Function Catalog Entries
//!
//!
//! This module defines all function-related catalog entries:
//! - ScalarFunctionCatalogEntry
//! - AggregateFunctionCatalogEntry
//! - TableFunctionCatalogEntry

use super::catalog_entry::{
    AlterInfo, CatalogEntry, CatalogObjectId, CatalogType, CreateInfo, DependencyList,
    InCatalogEntry, SchemaEntryMeta, StandardEntry,
};
use paro_common::error::{self as paro_error, Result};
use paro_function::aggregate::AggregateFunctionSet;
use paro_function::scalar::ScalarFunctionSet;
use paro_function::table::TableFunctionSet;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Weak};

// ============================================================================
// ScalarFunctionCatalogEntry
// ============================================================================

/// Scalar function catalog entry.
///
#[derive(Debug)]
pub struct ScalarFunctionCatalogEntry {
    /// Standard entry base
    pub base: SchemaEntryMeta,
    /// The function set (supports overloading)
    pub functions: ScalarFunctionSet,
}

impl ScalarFunctionCatalogEntry {
    /// Create a new scalar function catalog entry.
    pub fn new(
        catalog: String,
        schema_name: String,
        functions: ScalarFunctionSet,
        object_id: CatalogObjectId,
        timestamp: u64,
    ) -> Self {
        let base = SchemaEntryMeta::new(
            CatalogType::ScalarFunction,
            catalog,
            schema_name,
            functions.name.clone(),
            object_id,
            timestamp,
        );

        Self { base, functions }
    }

    /// Create an internal (built-in) function entry.
    pub fn new_internal(
        catalog: String,
        schema_name: String,
        functions: ScalarFunctionSet,
        object_id: CatalogObjectId,
    ) -> Self {
        let mut base = SchemaEntryMeta::new(
            CatalogType::ScalarFunction,
            catalog,
            schema_name,
            functions.name.clone(),
            object_id,
            0, // Internal functions have timestamp 0
        );
        base.base.internal = true;

        Self { base, functions }
    }

    /// Get the function set
    pub fn get_functions(&self) -> &ScalarFunctionSet {
        &self.functions
    }
}

impl CatalogEntry for ScalarFunctionCatalogEntry {
    fn object_id(&self) -> CatalogObjectId {
        self.base.base.object_id
    }

    fn name(&self) -> &str {
        &self.base.base.name
    }

    fn entry_type(&self) -> CatalogType {
        CatalogType::ScalarFunction
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

    fn alter(&self, _info: &AlterInfo) -> Result<Arc<dyn CatalogEntry>> {
        Err(paro_error::not_implemented("ALTER FUNCTION"))
    }

    fn undo_alter(&self, _info: &AlterInfo) -> Result<()> {
        Ok(())
    }

    fn rollback(&self, _prev_entry: &dyn CatalogEntry) -> Result<()> {
        Ok(())
    }

    fn copy(&self) -> Result<Arc<dyn CatalogEntry>> {
        let new_entry = ScalarFunctionCatalogEntry {
            base: SchemaEntryMeta::new(
                CatalogType::ScalarFunction,
                self.base.base.catalog.clone(),
                self.base.schema_name.clone(),
                self.base.base.name.clone(),
                self.base.base.object_id,
                self.base.base.timestamp(),
            ),
            functions: self.functions.clone(),
        };
        Ok(Arc::new(new_entry))
    }

    fn get_info(&self) -> Result<CreateInfo> {
        let info = CreateInfo::new(
            CatalogType::ScalarFunction,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
        );
        Ok(info)
    }

    fn set_as_root(&self) {}

    fn to_sql(&self) -> String {
        format!("-- SCALAR FUNCTION {} (built-in)", self.base.base.name)
    }

    fn serialize(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        self.base.base.serialize(writer)?;
        Ok(())
    }
}

impl StandardEntry for ScalarFunctionCatalogEntry {
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

// ============================================================================
// InCatalogEntry trait implementation
// ============================================================================

impl InCatalogEntry for ScalarFunctionCatalogEntry {}

// ============================================================================
// AggregateFunctionCatalogEntry
// ============================================================================

/// Aggregate function catalog entry.
///
#[derive(Debug)]
pub struct AggregateFunctionCatalogEntry {
    /// Standard entry base
    pub base: SchemaEntryMeta,
    /// The function set (supports overloading)
    pub functions: AggregateFunctionSet,
}

impl AggregateFunctionCatalogEntry {
    /// Create a new aggregate function catalog entry.
    pub fn new(
        catalog: String,
        schema_name: String,
        functions: AggregateFunctionSet,
        object_id: CatalogObjectId,
        timestamp: u64,
    ) -> Self {
        let base = SchemaEntryMeta::new(
            CatalogType::AggregateFunction,
            catalog,
            schema_name,
            functions.name.clone(),
            object_id,
            timestamp,
        );

        Self { base, functions }
    }

    /// Create an internal (built-in) function entry.
    pub fn new_internal(
        catalog: String,
        schema_name: String,
        functions: AggregateFunctionSet,
        object_id: CatalogObjectId,
    ) -> Self {
        let mut base = SchemaEntryMeta::new(
            CatalogType::AggregateFunction,
            catalog,
            schema_name,
            functions.name.clone(),
            object_id,
            0,
        );
        base.base.internal = true;

        Self { base, functions }
    }

    /// Get the function set
    pub fn get_functions(&self) -> &AggregateFunctionSet {
        &self.functions
    }
}

impl CatalogEntry for AggregateFunctionCatalogEntry {
    fn object_id(&self) -> CatalogObjectId {
        self.base.base.object_id
    }

    fn name(&self) -> &str {
        &self.base.base.name
    }

    fn entry_type(&self) -> CatalogType {
        CatalogType::AggregateFunction
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

    fn alter(&self, _info: &AlterInfo) -> Result<Arc<dyn CatalogEntry>> {
        Err(paro_error::not_implemented("ALTER FUNCTION"))
    }

    fn undo_alter(&self, _info: &AlterInfo) -> Result<()> {
        Ok(())
    }

    fn rollback(&self, _prev_entry: &dyn CatalogEntry) -> Result<()> {
        Ok(())
    }

    fn copy(&self) -> Result<Arc<dyn CatalogEntry>> {
        let new_entry = AggregateFunctionCatalogEntry {
            base: SchemaEntryMeta::new(
                CatalogType::AggregateFunction,
                self.base.base.catalog.clone(),
                self.base.schema_name.clone(),
                self.base.base.name.clone(),
                self.base.base.object_id,
                self.base.base.timestamp(),
            ),
            functions: self.functions.clone(),
        };
        Ok(Arc::new(new_entry))
    }

    fn get_info(&self) -> Result<CreateInfo> {
        let info = CreateInfo::new(
            CatalogType::AggregateFunction,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
        );
        Ok(info)
    }

    fn set_as_root(&self) {}

    fn to_sql(&self) -> String {
        format!("-- AGGREGATE FUNCTION {} (built-in)", self.base.base.name)
    }

    fn serialize(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        self.base.base.serialize(writer)?;
        Ok(())
    }
}

impl StandardEntry for AggregateFunctionCatalogEntry {
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

// ============================================================================
// InCatalogEntry trait implementation
// ============================================================================

impl InCatalogEntry for AggregateFunctionCatalogEntry {}

// ============================================================================
// TableFunctionCatalogEntry
// ============================================================================

/// Table function catalog entry.
///
#[derive(Debug)]
pub struct TableFunctionCatalogEntry {
    /// Standard entry base
    pub base: SchemaEntryMeta,
    /// The function set (supports overloading)
    pub functions: TableFunctionSet,
}

impl TableFunctionCatalogEntry {
    /// Create a new table function catalog entry.
    pub fn new(
        catalog: String,
        schema_name: String,
        functions: TableFunctionSet,
        object_id: CatalogObjectId,
        timestamp: u64,
    ) -> Self {
        let base = SchemaEntryMeta::new(
            CatalogType::TableFunction,
            catalog,
            schema_name,
            functions.name.clone(),
            object_id,
            timestamp,
        );

        Self { base, functions }
    }

    /// Create an internal (built-in) function entry.
    pub fn new_internal(
        catalog: String,
        schema_name: String,
        functions: TableFunctionSet,
        object_id: CatalogObjectId,
    ) -> Self {
        let mut base = SchemaEntryMeta::new(
            CatalogType::TableFunction,
            catalog,
            schema_name,
            functions.name.clone(),
            object_id,
            0,
        );
        base.base.internal = true;

        Self { base, functions }
    }

    /// Get the function set
    pub fn get_functions(&self) -> &TableFunctionSet {
        &self.functions
    }
}

impl CatalogEntry for TableFunctionCatalogEntry {
    fn object_id(&self) -> CatalogObjectId {
        self.base.base.object_id
    }

    fn name(&self) -> &str {
        &self.base.base.name
    }

    fn entry_type(&self) -> CatalogType {
        CatalogType::TableFunction
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

    fn alter(&self, _info: &AlterInfo) -> Result<Arc<dyn CatalogEntry>> {
        Err(paro_error::not_implemented("ALTER FUNCTION"))
    }

    fn undo_alter(&self, _info: &AlterInfo) -> Result<()> {
        Ok(())
    }

    fn rollback(&self, _prev_entry: &dyn CatalogEntry) -> Result<()> {
        Ok(())
    }

    fn copy(&self) -> Result<Arc<dyn CatalogEntry>> {
        let new_entry = TableFunctionCatalogEntry {
            base: SchemaEntryMeta::new(
                CatalogType::TableFunction,
                self.base.base.catalog.clone(),
                self.base.schema_name.clone(),
                self.base.base.name.clone(),
                self.base.base.object_id,
                self.base.base.timestamp(),
            ),
            functions: self.functions.clone(),
        };
        Ok(Arc::new(new_entry))
    }

    fn get_info(&self) -> Result<CreateInfo> {
        let info = CreateInfo::new(
            CatalogType::TableFunction,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
        );
        Ok(info)
    }

    fn set_as_root(&self) {}

    fn to_sql(&self) -> String {
        format!("-- TABLE FUNCTION {} (built-in)", self.base.base.name)
    }

    fn serialize(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        self.base.base.serialize(writer)?;
        Ok(())
    }
}

impl StandardEntry for TableFunctionCatalogEntry {
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

// ============================================================================
// InCatalogEntry trait implementation
// ============================================================================

impl InCatalogEntry for TableFunctionCatalogEntry {}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_function::table::TableFunction;

    #[test]
    fn test_scalar_function_entry() {
        let set = ScalarFunctionSet::new("my_func".to_string());
        let entry = ScalarFunctionCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            set,
            CatalogObjectId::from_raw(10_001),
            100,
        );

        assert_eq!(entry.name(), "my_func");
        assert_eq!(entry.entry_type(), CatalogType::ScalarFunction);
        assert_eq!(entry.schema_name(), "public");
    }

    #[test]
    fn test_table_function_entry() {
        let mut set = TableFunctionSet::new("generate_series".to_string());
        set.add_function(TableFunction::new(
            "generate_series".to_string(),
            vec![LogicalType::BigInt, LogicalType::BigInt],
        ));

        let entry = TableFunctionCatalogEntry::new(
            "main".to_string(),
            "public".to_string(),
            set,
            CatalogObjectId::from_raw(10_001),
            100,
        );

        assert_eq!(entry.name(), "generate_series");
        assert_eq!(entry.entry_type(), CatalogType::TableFunction);
    }
}
