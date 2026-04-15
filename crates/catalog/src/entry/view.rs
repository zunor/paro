//! View Catalog Entry
//!
//!
//! This module defines ViewCatalogEntry for view metadata.

use super::catalog_entry::{
    allocate_object_id, AlterInfo, CatalogEntry, CatalogObjectId, CatalogType, CreateInfo,
    DependencyList, InCatalogEntry, OnCreateConflict, SchemaEntryMeta, StandardEntry,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::Query;
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::sync::{Arc, LazyLock, Weak};

// --- CreateViewInfo ---

/// Information needed to create a view.
///
#[derive(Debug, Clone)]
pub struct CreateViewInfo {
    /// Catalog name
    pub catalog: String,
    /// Schema name
    pub schema: String,
    /// View name
    pub name: String,
    /// The SELECT query defining the view
    pub query: Box<Query>,
    /// Column aliases (optional, overrides query column names)
    pub aliases: Vec<String>,
    /// Column types (derived from query)
    pub column_types: Vec<LogicalType>,
    /// Column names (derived from query)
    pub column_names: Vec<String>,
    /// On conflict behavior
    pub on_conflict: OnCreateConflict,
    /// Whether this is temporary
    pub temporary: bool,
    /// Original SQL statement
    pub sql: Option<String>,
    /// Dependencies
    pub dependencies: DependencyList,
}

impl CreateViewInfo {
    /// Create a new CreateViewInfo
    pub fn new(schema: String, name: String, query: Box<Query>) -> Self {
        Self {
            catalog: String::new(),
            schema,
            name,
            query,
            aliases: Vec::new(),
            column_types: Vec::new(),
            column_names: Vec::new(),
            on_conflict: OnCreateConflict::ErrorOnConflict,
            temporary: false,
            sql: None,
            dependencies: DependencyList::new(),
        }
    }

    pub fn with_catalog(mut self, catalog: String) -> Self {
        self.catalog = catalog;
        self
    }

    pub fn with_aliases(mut self, aliases: Vec<String>) -> Self {
        self.aliases = aliases;
        self
    }

    pub fn with_column_types(mut self, types: Vec<LogicalType>) -> Self {
        self.column_types = types;
        self
    }

    pub fn with_column_names(mut self, names: Vec<String>) -> Self {
        self.column_names = names;
        self
    }

    pub fn with_or_replace(mut self) -> Self {
        self.on_conflict = OnCreateConflict::ReplaceOnConflict;
        self
    }

    pub fn with_if_not_exists(mut self) -> Self {
        self.on_conflict = OnCreateConflict::IgnoreOnConflict;
        self
    }

    pub fn with_temporary(mut self) -> Self {
        self.temporary = true;
        self
    }

    pub fn with_sql(mut self, sql: String) -> Self {
        self.sql = Some(sql);
        self
    }

    pub fn with_dependencies(mut self, dependencies: DependencyList) -> Self {
        self.dependencies = dependencies;
        self
    }
}

// --- ViewCatalogEntry ---

/// View catalog entry - metadata for a view.
///
#[derive(Debug)]
pub struct ViewCatalogEntry {
    /// Standard entry base (includes schema reference)
    pub base: SchemaEntryMeta,
    /// The SELECT query defining the view
    pub query: Box<Query>,
    /// Column aliases
    pub aliases: Vec<String>,
    /// Column types
    pub column_types: Vec<LogicalType>,
    /// Column names
    pub column_names: Vec<String>,
    /// Original SQL statement
    pub sql: Option<String>,
}

impl ViewCatalogEntry {
    /// Create a new view catalog entry from CreateViewInfo
    pub fn new(info: CreateViewInfo, timestamp: u64, catalog: String) -> Self {
        Self::with_object_id(info, timestamp, catalog, allocate_object_id())
    }

    pub fn with_object_id(
        info: CreateViewInfo,
        timestamp: u64,
        catalog: String,
        object_id: CatalogObjectId,
    ) -> Self {
        let mut base = SchemaEntryMeta::new(
            CatalogType::View,
            catalog,
            info.schema,
            info.name,
            object_id,
            timestamp,
        );
        base.base.temporary = info.temporary;
        base.set_dependencies(info.dependencies);

        Self {
            base,
            query: info.query,
            aliases: info.aliases,
            column_types: info.column_types,
            column_names: info.column_names,
            sql: info.sql,
        }
    }

    /// Get the query
    pub fn get_query(&self) -> &Query {
        &self.query
    }

    /// Get the column aliases
    pub fn get_aliases(&self) -> &[String] {
        &self.aliases
    }

    /// Get the column types
    pub fn get_column_types(&self) -> &[LogicalType] {
        &self.column_types
    }

    /// Get the column names
    pub fn get_column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Check if this view has types defined
    pub fn has_types(&self) -> bool {
        !self.column_types.is_empty()
    }

    /// Convert to SQL CREATE VIEW statement
    pub fn to_sql(&self) -> String {
        if let Some(sql) = &self.sql {
            return sql.clone();
        }

        let mut sql = String::new();
        sql.push_str("CREATE ");

        if self.base.base.temporary {
            sql.push_str("TEMPORARY ");
        }

        sql.push_str("VIEW ");
        sql.push_str(&self.base.schema_name);
        sql.push('.');
        sql.push_str(&self.base.base.name);

        if !self.aliases.is_empty() {
            sql.push_str(" (");
            sql.push_str(&self.aliases.join(", "));
            sql.push(')');
        }

        sql.push_str(" AS ");
        sql.push_str(&self.query.to_string());
        sql.push(';');

        sql
    }

    /// Serialize the view entry to bytes
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // 1. OID
        buffer.write_all(&self.base.base.object_id.raw().to_le_bytes())?;

        // 2. Timestamp
        buffer.write_all(&self.base.base.timestamp().to_le_bytes())?;

        // 3. View name
        let name_bytes = self.base.base.name.as_bytes();
        buffer.write_all(&(name_bytes.len() as u32).to_le_bytes())?;
        buffer.write_all(name_bytes)?;

        // 4. Schema name
        let schema_bytes = self.base.schema_name.as_bytes();
        buffer.write_all(&(schema_bytes.len() as u32).to_le_bytes())?;
        buffer.write_all(schema_bytes)?;

        // 5. Query (as SQL string)
        let query_str = self.query.to_string();
        let query_bytes = query_str.as_bytes();
        buffer.write_all(&(query_bytes.len() as u32).to_le_bytes())?;
        buffer.write_all(query_bytes)?;

        // 6. Aliases
        buffer.write_all(&(self.aliases.len() as u32).to_le_bytes())?;
        for alias in &self.aliases {
            let alias_bytes = alias.as_bytes();
            buffer.write_all(&(alias_bytes.len() as u32).to_le_bytes())?;
            buffer.write_all(alias_bytes)?;
        }

        // 7. Column types
        buffer.write_all(&(self.column_types.len() as u32).to_le_bytes())?;
        for col_type in &self.column_types {
            col_type.serialize(&mut buffer)?;
        }

        // 8. Column names
        buffer.write_all(&(self.column_names.len() as u32).to_le_bytes())?;
        for name in &self.column_names {
            let name_bytes = name.as_bytes();
            buffer.write_all(&(name_bytes.len() as u32).to_le_bytes())?;
            buffer.write_all(name_bytes)?;
        }

        // 9. Temporary flag
        buffer.write_all(&[if self.base.base.temporary { 1u8 } else { 0u8 }])?;

        // 10. SQL (optional)
        if let Some(sql) = &self.sql {
            buffer.write_all(&[1u8])?;
            let sql_bytes = sql.as_bytes();
            buffer.write_all(&(sql_bytes.len() as u32).to_le_bytes())?;
            buffer.write_all(sql_bytes)?;
        } else {
            buffer.write_all(&[0u8])?;
        }

        // 11. Dependencies
        self.base.dependencies().serialize(&mut buffer)?;

        Ok(buffer)
    }

    /// Deserialize a view entry from bytes
    pub fn deserialize(bytes: &[u8], catalog: String) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);

        // 1. OID
        let mut oid_buf = [0u8; 8];
        cursor.read_exact(&mut oid_buf)?;
        let oid = u64::from_le_bytes(oid_buf);

        // 2. Timestamp
        let mut ts_buf = [0u8; 8];
        cursor.read_exact(&mut ts_buf)?;
        let timestamp = u64::from_le_bytes(ts_buf);

        // 3. View name
        let mut len_buf = [0u8; 4];
        cursor.read_exact(&mut len_buf)?;
        let name_len = u32::from_le_bytes(len_buf) as usize;
        let mut name_bytes = vec![0u8; name_len];
        cursor.read_exact(&mut name_bytes)?;
        let view_name = String::from_utf8(name_bytes)
            .map_err(|e| paro_error::internal(format!("Invalid UTF-8 in view name: {}", e)))?;

        // 4. Schema name
        cursor.read_exact(&mut len_buf)?;
        let schema_len = u32::from_le_bytes(len_buf) as usize;
        let mut schema_bytes = vec![0u8; schema_len];
        cursor.read_exact(&mut schema_bytes)?;
        let schema_name = String::from_utf8(schema_bytes)
            .map_err(|e| paro_error::internal(format!("Invalid UTF-8 in schema name: {}", e)))?;

        // 5. Query (as SQL string)
        cursor.read_exact(&mut len_buf)?;
        let query_len = u32::from_le_bytes(len_buf) as usize;
        let mut query_bytes = vec![0u8; query_len];
        cursor.read_exact(&mut query_bytes)?;
        let query_str = String::from_utf8(query_bytes)
            .map_err(|e| paro_error::internal(format!("Invalid UTF-8 in query: {}", e)))?;

        let stmt = paro_parser::parse_one(&query_str)
            .map_err(|e| paro_error::internal(format!("Failed to parse view query: {}", e)))?;

        let query = match stmt.stmt {
            paro_parser::ast::Statement::Query(q) => q,
            _ => {
                return Err(paro_error::internal(
                    "View query must be a SELECT statement",
                ))
            }
        };

        // 6. Aliases
        cursor.read_exact(&mut len_buf)?;
        let alias_count = u32::from_le_bytes(len_buf) as usize;
        let mut aliases = Vec::with_capacity(alias_count);
        for _ in 0..alias_count {
            cursor.read_exact(&mut len_buf)?;
            let alias_len = u32::from_le_bytes(len_buf) as usize;
            let mut alias_bytes = vec![0u8; alias_len];
            cursor.read_exact(&mut alias_bytes)?;
            let alias = String::from_utf8(alias_bytes)
                .map_err(|e| paro_error::internal(format!("Invalid UTF-8 in alias: {}", e)))?;
            aliases.push(alias);
        }

        // 7. Column types
        cursor.read_exact(&mut len_buf)?;
        let type_count = u32::from_le_bytes(len_buf) as usize;
        let mut column_types = Vec::with_capacity(type_count);
        for _ in 0..type_count {
            let col_type = LogicalType::deserialize(&mut cursor)?;
            column_types.push(col_type);
        }

        // 8. Column names
        cursor.read_exact(&mut len_buf)?;
        let name_count = u32::from_le_bytes(len_buf) as usize;
        let mut column_names = Vec::with_capacity(name_count);
        for _ in 0..name_count {
            cursor.read_exact(&mut len_buf)?;
            let col_name_len = u32::from_le_bytes(len_buf) as usize;
            let mut col_name_bytes = vec![0u8; col_name_len];
            cursor.read_exact(&mut col_name_bytes)?;
            let col_name = String::from_utf8(col_name_bytes).map_err(|e| {
                paro_error::internal(format!("Invalid UTF-8 in column name: {}", e))
            })?;
            column_names.push(col_name);
        }

        // 9. Temporary flag
        let mut byte_buf = [0u8; 1];
        cursor.read_exact(&mut byte_buf)?;
        let temporary = byte_buf[0] == 1;

        // 10. SQL (optional)
        cursor.read_exact(&mut byte_buf)?;
        let sql = if byte_buf[0] == 1 {
            cursor.read_exact(&mut len_buf)?;
            let sql_len = u32::from_le_bytes(len_buf) as usize;
            let mut sql_bytes = vec![0u8; sql_len];
            cursor.read_exact(&mut sql_bytes)?;
            Some(
                String::from_utf8(sql_bytes)
                    .map_err(|e| paro_error::internal(format!("Invalid UTF-8 in SQL: {}", e)))?,
            )
        } else {
            None
        };

        let dependencies = DependencyList::deserialize(&mut cursor)?;

        let mut base = SchemaEntryMeta::new(
            CatalogType::View,
            catalog,
            schema_name,
            view_name,
            CatalogObjectId::from_raw(oid),
            timestamp,
        );
        base.base.temporary = temporary;
        base.set_dependencies(dependencies);

        Ok(Self {
            base,
            query,
            aliases,
            column_types,
            column_names,
            sql,
        })
    }
}

// --- CatalogEntry trait implementation ---

impl CatalogEntry for ViewCatalogEntry {
    fn object_id(&self) -> CatalogObjectId {
        self.base.base.object_id
    }

    fn name(&self) -> &str {
        &self.base.base.name
    }

    fn entry_type(&self) -> CatalogType {
        CatalogType::View
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

    fn alter(&self, info: &AlterInfo) -> Result<Arc<dyn CatalogEntry>> {
        if let Some(new_name) = &info.new_name {
            let base = SchemaEntryMeta::new(
                CatalogType::View,
                self.base.base.catalog.clone(),
                self.base.schema_name.clone(),
                new_name.clone(),
                self.base.base.object_id,
                self.base.base.timestamp(),
            );
            let new_entry = ViewCatalogEntry {
                base,
                query: self.query.clone(),
                aliases: self.aliases.clone(),
                column_types: self.column_types.clone(),
                column_names: self.column_names.clone(),
                sql: None,
            };
            return Ok(Arc::new(new_entry));
        }

        Err(paro_error::not_implemented("ALTER VIEW"))
    }

    fn undo_alter(&self, _info: &AlterInfo) -> Result<()> {
        Ok(())
    }

    fn rollback(&self, _prev_entry: &dyn CatalogEntry) -> Result<()> {
        Ok(())
    }

    fn copy(&self) -> Result<Arc<dyn CatalogEntry>> {
        let base = SchemaEntryMeta::new(
            CatalogType::View,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
            self.base.base.object_id,
            self.base.base.timestamp(),
        );
        let new_entry = ViewCatalogEntry {
            base,
            query: self.query.clone(),
            aliases: self.aliases.clone(),
            column_types: self.column_types.clone(),
            column_names: self.column_names.clone(),
            sql: self.sql.clone(),
        };
        Ok(Arc::new(new_entry))
    }

    fn get_info(&self) -> Result<CreateInfo> {
        let mut info = CreateInfo::new(
            CatalogType::View,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
        );
        info.temporary = self.base.base.temporary;
        info.sql = Some(self.to_sql());
        Ok(info)
    }

    fn set_as_root(&self) {}

    fn to_sql(&self) -> String {
        self.to_sql()
    }

    fn serialize(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        self.base.base.serialize(writer)?;
        Ok(())
    }
}

// --- StandardEntry trait implementation ---

impl StandardEntry for ViewCatalogEntry {
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

impl InCatalogEntry for ViewCatalogEntry {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::CatalogObjectRef;

    fn parse_query(sql: &str) -> Box<Query> {
        match paro_parser::parse_one(sql)
            .expect("Failed to parse SQL")
            .stmt
        {
            paro_parser::ast::Statement::Query(q) => q,
            _ => panic!("Expected a SELECT statement"),
        }
    }

    #[test]
    fn test_create_view_info() {
        let query = parse_query("SELECT id, name FROM users WHERE active = true");
        let info = CreateViewInfo::new("public".to_string(), "active_users".to_string(), query)
            .with_aliases(vec!["user_id".to_string(), "user_name".to_string()])
            .with_column_types(vec![LogicalType::BigInt, LogicalType::Varchar]);

        assert_eq!(info.schema, "public");
        assert_eq!(info.name, "active_users");
        assert_eq!(info.aliases.len(), 2);
    }

    #[test]
    fn test_view_catalog_entry() {
        let query = parse_query("SELECT * FROM orders WHERE status = 'pending'");
        let info = CreateViewInfo::new("public".to_string(), "pending_orders".to_string(), query);

        let entry = ViewCatalogEntry::new(info, 100, "main".to_string());

        assert_eq!(entry.name(), "pending_orders");
        assert_eq!(entry.schema_name(), "public");
        assert_eq!(entry.entry_type(), CatalogType::View);
    }

    #[test]
    fn test_to_sql() {
        let query = parse_query("SELECT id, name FROM employees");
        let info = CreateViewInfo::new("hr".to_string(), "emp_view".to_string(), query)
            .with_aliases(vec!["emp_id".to_string(), "emp_name".to_string()]);

        let entry = ViewCatalogEntry::new(info, 100, "main".to_string());
        let sql = entry.to_sql();

        assert!(sql.contains("CREATE VIEW"));
        assert!(sql.contains("hr.emp_view"));
    }

    #[test]
    fn test_view_copy_preserves_object_id() {
        let query = parse_query("SELECT 1 AS id");
        let info = CreateViewInfo::new("public".to_string(), "copy_view".to_string(), query);
        let entry = ViewCatalogEntry::new(info, 100, "main".to_string());

        let copied = entry.copy().unwrap();
        assert_eq!(copied.object_id(), entry.object_id());
    }

    #[test]
    fn test_view_alter_rename_preserves_object_id() {
        let query = parse_query("SELECT 1 AS id");
        let info = CreateViewInfo::new("public".to_string(), "rename_view".to_string(), query);
        let entry = ViewCatalogEntry::new(info, 100, "main".to_string());
        let altered = entry
            .alter(&AlterInfo::rename(
                "main".to_string(),
                "public".to_string(),
                "rename_view".to_string(),
                "renamed_view".to_string(),
            ))
            .unwrap();

        assert_eq!(altered.object_id(), entry.object_id());
        assert_eq!(altered.name(), "renamed_view");
    }

    #[test]
    fn test_view_roundtrip_preserves_object_id() {
        let query = parse_query("SELECT 1 AS id");
        let mut dependencies = DependencyList::new();
        dependencies.add_regular(CatalogObjectRef::in_schema(
            CatalogObjectId::from_raw(7),
            CatalogType::Table,
            "main".to_string(),
            None,
            "public".to_string(),
            "users".to_string(),
        ));
        let entry = ViewCatalogEntry::new(
            CreateViewInfo::new("public".to_string(), "roundtrip_view".to_string(), query)
                .with_dependencies(dependencies),
            100,
            "main".to_string(),
        );

        let bytes = entry.serialize_to_bytes().unwrap();
        let restored = ViewCatalogEntry::deserialize(&bytes, "main".to_string()).unwrap();

        assert_eq!(restored.object_id(), entry.object_id());
        assert_eq!(restored.name(), "roundtrip_view");
        assert_eq!(restored.base.dependencies().len(), 1);
        assert_eq!(
            restored.base.dependencies().dependencies()[0]
                .entry
                .id
                .raw(),
            7
        );
    }
}
