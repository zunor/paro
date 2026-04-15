// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Catalog search path parsing and normalization.
//!
//! Supports simple `schema` and `catalog.schema` entries.

use std::fmt;

use paro_common::error::{self as paro_error, Result};

/// Default schema name (PostgreSQL: `public`). Same as [`crate::catalog::DEFAULT_SCHEMA`].
pub const DEFAULT_SCHEMA: &str = crate::catalog::DEFAULT_SCHEMA;

/// Temporary catalog name
pub const TEMP_CATALOG: &str = "temp";

/// System catalog name
pub const SYSTEM_CATALOG: &str = "system";

/// PostgreSQL catalog schema name
pub const PG_CATALOG_SCHEMA: &str = "pg_catalog";

/// A single entry in the catalog search path.
///
/// Each entry consists of a catalog (database) name and a schema name.
/// In PostgreSQL terms, this corresponds to a schema in the search_path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CatalogSearchEntry {
    /// The catalog (database) name. Empty string means "current database".
    pub catalog: String,
    /// The schema name within the catalog.
    pub schema: String,
}

impl CatalogSearchEntry {
    /// Creates a new catalog search entry.
    pub fn new(catalog: impl Into<String>, schema: impl Into<String>) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
        }
    }

    /// Creates a new entry with only a schema (catalog defaults to current database).
    pub fn schema_only(schema: impl Into<String>) -> Self {
        Self {
            catalog: String::new(),
            schema: schema.into(),
        }
    }

    /// Parses a single search path entry from a string.
    ///
    /// Supports formats:
    /// - `schema` - schema only (catalog is empty)
    /// - `catalog.schema` - fully qualified
    ///
    /// # Errors
    ///
    /// Returns an error if the input is empty or has too many dots.
    pub fn parse(input: &str) -> Result<Self> {
        let input = input.trim();
        if input.is_empty() {
            return Err(paro_error::from_parser("empty search path entry"));
        }

        let parts: Vec<&str> = input.split('.').collect();
        match parts.len() {
            1 => Ok(Self::schema_only(parts[0].trim())),
            2 => Ok(Self::new(parts[0].trim(), parts[1].trim())),
            _ => Err(paro_error::from_parser(format!(
                "too many dots in search path entry: '{}'. Expected [schema] or [catalog.schema]",
                input
            ))),
        }
    }

    /// Parses a comma-separated list of search path entries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let entries = CatalogSearchEntry::parse_list("public, myschema, other.schema")?;
    /// ```
    pub fn parse_list(input: &str) -> Result<Vec<Self>> {
        let input = input.trim();
        if input.is_empty() {
            return Ok(Vec::new());
        }

        input.split(',').map(|s| Self::parse(s.trim())).collect()
    }

    /// Converts a list of entries to a comma-separated string.
    pub fn list_to_string(entries: &[Self]) -> String {
        entries
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl fmt::Display for CatalogSearchEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.catalog.is_empty() {
            write!(f, "{}", self.schema)
        } else {
            write!(f, "{}.{}", self.catalog, self.schema)
        }
    }
}

/// The type of SET operation for the search path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogSetPathType {
    /// SET schema (single schema)
    SetSchema,
    /// SET search_path (multiple schemas)
    SetSearchPath,
}

/// The catalog search path manages the order in which schemas are searched
/// when resolving unqualified object names.
///
/// This is equivalent to PostgreSQL's `search_path` GUC variable.
///
/// # Default Search Path
///
/// The default search path is:
/// 1. `temp.public` - Temporary objects (always first)
/// 2. User-set paths (e.g., `"$user", public`)
/// 3. `<current_db>.public` - Default schema in current database
/// 4. `system.public` - System catalog
/// 5. `system.pg_catalog` - PostgreSQL compatibility catalog
///
/// # Example
///
/// ```ignore
/// let mut path = CatalogSearchPath::new("mydb");
/// path.set_schema("myschema")?;
/// assert_eq!(path.get_default_schema(), "myschema");
/// ```
#[derive(Debug, Clone)]
pub struct CatalogSearchPath {
    /// The current database name (used to resolve empty catalog names)
    current_database: String,
    /// The complete search path including system entries
    paths: Vec<CatalogSearchEntry>,
    /// Only the paths that were explicitly set by the user
    set_paths: Vec<CatalogSearchEntry>,
}

impl CatalogSearchPath {
    /// Creates a new search path with default entries.
    ///
    /// The default path is: `["$user", "public"]` (PostgreSQL compatible)
    pub fn new(current_database: impl Into<String>) -> Self {
        let current_database = current_database.into();
        let mut path = Self {
            current_database,
            paths: Vec::new(),
            set_paths: Vec::new(),
        };
        path.rebuild_paths();
        path
    }

    /// Creates a new search path with the given entries.
    pub fn with_entries(
        current_database: impl Into<String>,
        entries: Vec<CatalogSearchEntry>,
    ) -> Self {
        let current_database = current_database.into();
        let mut path = Self {
            current_database,
            paths: Vec::new(),
            set_paths: entries,
        };
        path.rebuild_paths();
        path
    }

    /// Returns the complete search path.
    pub fn get(&self) -> &[CatalogSearchEntry] {
        &self.paths
    }

    /// Returns only the user-set paths (excluding system entries).
    pub fn get_set_paths(&self) -> &[CatalogSearchEntry] {
        &self.set_paths
    }

    /// Returns the default entry (first non-temp entry).
    ///
    /// This is typically the schema where new objects are created.
    pub fn get_default(&self) -> &CatalogSearchEntry {
        // Skip temp catalog, return the first user entry or system default
        self.paths
            .iter()
            .find(|e| e.catalog != TEMP_CATALOG)
            .unwrap_or(&self.paths[0])
    }

    /// Returns the default schema name.
    ///
    /// This is a convenience method that returns just the schema part
    /// of the default entry.
    pub fn get_default_schema(&self) -> &str {
        &self.get_default().schema
    }

    /// Returns the default schema for a specific catalog.
    pub fn get_default_schema_for_catalog(&self, catalog: &str) -> &str {
        for path in &self.paths {
            if path.catalog == TEMP_CATALOG {
                continue;
            }
            if path.catalog.eq_ignore_ascii_case(catalog)
                || (path.catalog.is_empty() && self.current_database.eq_ignore_ascii_case(catalog))
            {
                return &path.schema;
            }
        }
        DEFAULT_SCHEMA
    }

    /// Returns all schemas for a specific catalog in the search path.
    pub fn get_schemas_for_catalog(&self, catalog: &str) -> Vec<&str> {
        self.paths
            .iter()
            .filter(|p| {
                p.catalog.eq_ignore_ascii_case(catalog)
                    || (p.catalog.is_empty() && self.current_database.eq_ignore_ascii_case(catalog))
            })
            .map(|p| p.schema.as_str())
            .collect()
    }

    /// Checks if a schema is in the search path.
    pub fn schema_in_search_path(&self, catalog: &str, schema: &str) -> bool {
        for path in &self.paths {
            if !path.schema.eq_ignore_ascii_case(schema) {
                continue;
            }
            if path.catalog.eq_ignore_ascii_case(catalog) {
                return true;
            }
            // Empty catalog means current database
            if path.catalog.is_empty() && self.current_database.eq_ignore_ascii_case(catalog) {
                return true;
            }
        }
        false
    }

    /// Sets the search path to a single schema.
    ///
    /// This is equivalent to `SET schema = 'schema_name'` in PostgreSQL.
    pub fn set_schema(&mut self, schema: impl Into<String>) -> Result<()> {
        let schema = schema.into();
        self.set_paths = vec![CatalogSearchEntry::schema_only(schema)];
        self.rebuild_paths();
        Ok(())
    }

    /// Sets the search path to multiple entries.
    ///
    /// This is equivalent to `SET search_path = 'schema1, schema2'` in PostgreSQL.
    pub fn set(
        &mut self,
        entries: Vec<CatalogSearchEntry>,
        set_type: CatalogSetPathType,
    ) -> Result<()> {
        if set_type == CatalogSetPathType::SetSchema && entries.len() != 1 {
            return Err(paro_error::catalog(format!(
                "SET schema can only set 1 schema, got {}",
                entries.len()
            )));
        }
        self.set_paths = entries;
        self.rebuild_paths();
        Ok(())
    }

    /// Resets the search path to the default.
    pub fn reset(&mut self) {
        self.set_paths.clear();
        self.rebuild_paths();
    }

    /// Updates the current database name.
    ///
    /// This is called when the session switches to a different database.
    pub fn set_current_database(&mut self, database: impl Into<String>) {
        self.current_database = database.into();
        self.rebuild_paths();
    }

    /// Returns the current database name.
    pub fn current_database(&self) -> &str {
        &self.current_database
    }

    /// Rebuilds the complete search path from user-set paths.
    ///
    /// The complete path is:
    /// 1. temp.public (temporary objects)
    /// 2. User-set paths
    /// 3. <current_db>.public (if not already in user paths)
    /// 4. system.public
    /// 5. system.pg_catalog
    fn rebuild_paths(&mut self) {
        self.paths.clear();
        self.paths.reserve(self.set_paths.len() + 4);

        // 1. Temp catalog always first
        self.paths
            .push(CatalogSearchEntry::new(TEMP_CATALOG, DEFAULT_SCHEMA));

        // 2. User-set paths (resolve empty catalog to current database)
        for entry in &self.set_paths {
            let catalog = if entry.catalog.is_empty() {
                self.current_database.clone()
            } else {
                entry.catalog.clone()
            };
            self.paths
                .push(CatalogSearchEntry::new(catalog, entry.schema.clone()));
        }

        // 3. Default schema in current database (if not already present)
        let has_default = self.set_paths.iter().any(|e| {
            (e.catalog.is_empty() || e.catalog == self.current_database)
                && e.schema == DEFAULT_SCHEMA
        });
        if !has_default && !self.set_paths.is_empty() {
            self.paths.push(CatalogSearchEntry::new(
                self.current_database.clone(),
                DEFAULT_SCHEMA,
            ));
        }

        // If no user paths, add default public schema
        if self.set_paths.is_empty() {
            self.paths.push(CatalogSearchEntry::new(
                self.current_database.clone(),
                DEFAULT_SCHEMA,
            ));
        }

        // 4. Default system schemas in current database
        self.paths
            .push(CatalogSearchEntry::new("", PG_CATALOG_SCHEMA));
        self.paths
            .push(CatalogSearchEntry::new("", "information_schema"));

        // 5. System catalog (global)
        self.paths
            .push(CatalogSearchEntry::new(SYSTEM_CATALOG, DEFAULT_SCHEMA));
        self.paths
            .push(CatalogSearchEntry::new(SYSTEM_CATALOG, PG_CATALOG_SCHEMA));
    }
}

impl Default for CatalogSearchPath {
    fn default() -> Self {
        Self::new("paro")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================
    // CatalogSearchEntry Tests
    // ============================================================

    #[test]
    fn test_entry_new() {
        let entry = CatalogSearchEntry::new("mydb", "myschema");
        assert_eq!(entry.catalog, "mydb");
        assert_eq!(entry.schema, "myschema");
    }

    #[test]
    fn test_entry_schema_only() {
        let entry = CatalogSearchEntry::schema_only("public");
        assert_eq!(entry.catalog, "");
        assert_eq!(entry.schema, "public");
    }

    #[test]
    fn test_entry_parse_schema_only() {
        let entry = CatalogSearchEntry::parse("public").unwrap();
        assert_eq!(entry.catalog, "");
        assert_eq!(entry.schema, "public");
    }

    #[test]
    fn test_entry_parse_fully_qualified() {
        let entry = CatalogSearchEntry::parse("mydb.myschema").unwrap();
        assert_eq!(entry.catalog, "mydb");
        assert_eq!(entry.schema, "myschema");
    }

    #[test]
    fn test_entry_parse_with_whitespace() {
        let entry = CatalogSearchEntry::parse("  mydb . myschema  ").unwrap();
        assert_eq!(entry.catalog, "mydb");
        assert_eq!(entry.schema, "myschema");
    }

    #[test]
    fn test_entry_parse_empty_error() {
        let result = CatalogSearchEntry::parse("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn test_entry_parse_too_many_dots_error() {
        let result = CatalogSearchEntry::parse("a.b.c");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too many dots"));
    }

    #[test]
    fn test_entry_parse_list() {
        let entries = CatalogSearchEntry::parse_list("public, myschema, other.schema").unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].schema, "public");
        assert_eq!(entries[1].schema, "myschema");
        assert_eq!(entries[2].catalog, "other");
        assert_eq!(entries[2].schema, "schema");
    }

    #[test]
    fn test_entry_parse_list_empty() {
        let entries = CatalogSearchEntry::parse_list("").unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_entry_display() {
        let entry1 = CatalogSearchEntry::schema_only("public");
        assert_eq!(entry1.to_string(), "public");

        let entry2 = CatalogSearchEntry::new("mydb", "myschema");
        assert_eq!(entry2.to_string(), "mydb.myschema");
    }

    #[test]
    fn test_entry_list_to_string() {
        let entries = vec![
            CatalogSearchEntry::schema_only("public"),
            CatalogSearchEntry::new("mydb", "myschema"),
        ];
        assert_eq!(
            CatalogSearchEntry::list_to_string(&entries),
            "public, mydb.myschema"
        );
    }

    // ============================================================
    // CatalogSearchPath Tests
    // ============================================================

    #[test]
    fn test_search_path_default() {
        let path = CatalogSearchPath::new("testdb");

        // Should have: temp.public, testdb.public, system.public, system.pg_catalog
        let paths = path.get();
        assert!(paths.len() >= 4);

        // First should be temp
        assert_eq!(paths[0].catalog, TEMP_CATALOG);
        assert_eq!(paths[0].schema, DEFAULT_SCHEMA);

        // Default schema should be public
        assert_eq!(path.get_default_schema(), DEFAULT_SCHEMA);
    }

    #[test]
    fn test_search_path_set_schema() {
        let mut path = CatalogSearchPath::new("testdb");
        path.set_schema("myschema").unwrap();

        assert_eq!(path.get_default_schema(), "myschema");

        // User-set paths should only contain myschema
        let set_paths = path.get_set_paths();
        assert_eq!(set_paths.len(), 1);
        assert_eq!(set_paths[0].schema, "myschema");
    }

    #[test]
    fn test_search_path_set_multiple() {
        let mut path = CatalogSearchPath::new("testdb");
        let entries = vec![
            CatalogSearchEntry::schema_only("schema1"),
            CatalogSearchEntry::schema_only("schema2"),
        ];
        path.set(entries, CatalogSetPathType::SetSearchPath)
            .unwrap();

        assert_eq!(path.get_default_schema(), "schema1");
        assert_eq!(path.get_set_paths().len(), 2);
    }

    #[test]
    fn test_search_path_set_schema_multiple_error() {
        let mut path = CatalogSearchPath::new("testdb");
        let entries = vec![
            CatalogSearchEntry::schema_only("schema1"),
            CatalogSearchEntry::schema_only("schema2"),
        ];
        let result = path.set(entries, CatalogSetPathType::SetSchema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("only set 1 schema"));
    }

    #[test]
    fn test_search_path_reset() {
        let mut path = CatalogSearchPath::new("testdb");
        path.set_schema("myschema").unwrap();
        assert_eq!(path.get_default_schema(), "myschema");

        path.reset();
        assert_eq!(path.get_default_schema(), DEFAULT_SCHEMA);
        assert!(path.get_set_paths().is_empty());
    }

    #[test]
    fn test_search_path_schema_in_search_path() {
        let mut path = CatalogSearchPath::new("testdb");
        path.set_schema("myschema").unwrap();

        // myschema should be in path (resolved to testdb.myschema)
        assert!(path.schema_in_search_path("testdb", "myschema"));

        // public should also be in path (system entries)
        assert!(path.schema_in_search_path(SYSTEM_CATALOG, DEFAULT_SCHEMA));

        // pg_catalog should be in path
        assert!(path.schema_in_search_path(SYSTEM_CATALOG, PG_CATALOG_SCHEMA));

        // random schema should not be in path
        assert!(!path.schema_in_search_path("testdb", "nonexistent"));
    }

    #[test]
    fn test_search_path_get_schemas_for_catalog() {
        let mut path = CatalogSearchPath::new("testdb");
        path.set_schema("myschema").unwrap();

        let schemas = path.get_schemas_for_catalog("testdb");
        assert!(schemas.contains(&"myschema"));
    }

    #[test]
    fn test_search_path_set_current_database() {
        let mut path = CatalogSearchPath::new("db1");
        assert_eq!(path.current_database(), "db1");

        path.set_current_database("db2");
        assert_eq!(path.current_database(), "db2");

        // Paths should be rebuilt with new database
        let paths = path.get();
        let has_db2 = paths.iter().any(|p| p.catalog == "db2");
        assert!(has_db2);
    }

    #[test]
    fn test_search_path_includes_system_catalogs() {
        let path = CatalogSearchPath::new("testdb");
        let paths = path.get();

        // Should include system.public
        let has_system_public = paths
            .iter()
            .any(|p| p.catalog == SYSTEM_CATALOG && p.schema == DEFAULT_SCHEMA);
        assert!(has_system_public);

        // Should include system.pg_catalog
        let has_pg_catalog = paths
            .iter()
            .any(|p| p.catalog == SYSTEM_CATALOG && p.schema == PG_CATALOG_SCHEMA);
        assert!(has_pg_catalog);
    }

    #[test]
    fn test_search_path_temp_always_first() {
        let mut path = CatalogSearchPath::new("testdb");
        path.set_schema("myschema").unwrap();

        let paths = path.get();
        assert_eq!(paths[0].catalog, TEMP_CATALOG);
    }
}
