// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Default Views - View Definitions and Generator
//!
//!
//!
//! ## Overview
//!
//! This module contains:
//! 1. View definitions (`DefaultView`, `DEFAULT_VIEWS`, etc.)
//! 2. `DefaultViewGenerator` for lazy view initialization
//!
//! ## Default Views (public schema)
//! - `paro_tables` → `SELECT * FROM paro_tables() WHERE NOT internal`
//! - `paro_schemas` → `SELECT * FROM paro_schemas() WHERE NOT internal`
//! - `paro_columns` → `SELECT * FROM paro_columns() WHERE NOT internal`
//! - `paro_views` → `SELECT * FROM paro_views() WHERE NOT internal`
//!
//! ## information_schema Views (SQL Standard)
//! - `schemata`, `tables`, `columns`, `views`
//!
//! ## pg_catalog Views (PostgreSQL Compatibility)
//! - `pg_database`, `pg_namespace`, `pg_tables`, `pg_views`, `pg_class`, `pg_attribute`

use super::DefaultGenerator;
use crate::entry::{CatalogEntryEnum, CatalogObjectIdAllocator, CreateViewInfo, ViewCatalogEntry};
use paro_common::types::LogicalType;
use std::sync::Arc;

// ============================================================================
// View Definitions
// ============================================================================

/// Default view definition
pub struct DefaultView {
    /// View name
    pub name: &'static str,
    /// SQL query defining the view
    pub sql: &'static str,
    /// Column names
    pub column_names: &'static [&'static str],
    /// Column types
    pub column_types: &'static [LogicalType],
}

/// Default views for the public schema
pub static DEFAULT_VIEWS: &[DefaultView] = &[
    DefaultView {
        name: "paro_tables",
        sql:
            "SELECT * FROM paro_tables() WHERE NOT internal AND database_name = current_database()",
        column_names: &[
            "database_name",
            "database_oid",
            "schema_name",
            "schema_oid",
            "table_name",
            "table_oid",
            "internal",
            "temporary",
            "column_count",
            "index_count",
        ],
        column_types: &[
            LogicalType::Varchar, // database_name
            LogicalType::BigInt,  // database_oid
            LogicalType::Varchar, // schema_name
            LogicalType::BigInt,  // schema_oid
            LogicalType::Varchar, // table_name
            LogicalType::BigInt,  // table_oid
            LogicalType::Boolean, // internal
            LogicalType::Boolean, // temporary
            LogicalType::BigInt,  // column_count
            LogicalType::BigInt,  // index_count
        ],
    },
    DefaultView {
        name: "paro_schemas",
        sql:
            "SELECT * FROM paro_schemas() WHERE NOT internal AND database_name = current_database()",
        column_names: &[
            "oid",
            "database_name",
            "database_oid",
            "schema_name",
            "internal",
        ],
        column_types: &[
            LogicalType::BigInt,  // oid
            LogicalType::Varchar, // database_name
            LogicalType::BigInt,  // database_oid
            LogicalType::Varchar, // schema_name
            LogicalType::Boolean, // internal
        ],
    },
    DefaultView {
        name: "paro_columns",
        sql:
            "SELECT * FROM paro_columns() WHERE NOT internal AND database_name = current_database()",
        column_names: &[
            "database_name",
            "database_oid",
            "schema_name",
            "schema_oid",
            "table_name",
            "table_oid",
            "column_name",
            "column_index",
            "internal",
            "data_type",
            "data_type_id",
            "is_nullable",
            "column_default",
            "numeric_precision",
            "numeric_scale",
        ],
        column_types: &[
            LogicalType::Varchar, // database_name
            LogicalType::BigInt,  // database_oid
            LogicalType::Varchar, // schema_name
            LogicalType::BigInt,  // schema_oid
            LogicalType::Varchar, // table_name
            LogicalType::BigInt,  // table_oid
            LogicalType::Varchar, // column_name
            LogicalType::BigInt,  // column_index
            LogicalType::Boolean, // internal
            LogicalType::Varchar, // data_type
            LogicalType::BigInt,  // data_type_id
            LogicalType::Boolean, // is_nullable
            LogicalType::Varchar, // column_default
            LogicalType::BigInt,  // numeric_precision
            LogicalType::BigInt,  // numeric_scale
        ],
    },
    DefaultView {
        name: "paro_views",
        sql: "SELECT * FROM paro_views() WHERE NOT internal AND database_name = current_database()",
        column_names: &[
            "database_name",
            "database_oid",
            "schema_name",
            "schema_oid",
            "view_name",
            "view_oid",
            "internal",
            "temporary",
            "column_count",
            "sql",
        ],
        column_types: &[
            LogicalType::Varchar, // database_name
            LogicalType::BigInt,  // database_oid
            LogicalType::Varchar, // schema_name
            LogicalType::BigInt,  // schema_oid
            LogicalType::Varchar, // view_name
            LogicalType::BigInt,  // view_oid
            LogicalType::Boolean, // internal
            LogicalType::Boolean, // temporary
            LogicalType::BigInt,  // column_count
            LogicalType::Varchar, // sql
        ],
    },
];

// ============================================================================
// information_schema Views (SQL Standard)
// ============================================================================

/// information_schema views for SQL standard compliance.
pub static INFORMATION_SCHEMA_VIEWS: &[DefaultView] = &[
    DefaultView {
        name: "schemata",
        sql: "SELECT \
            database_name AS catalog_name, \
            schema_name, \
            'paro' AS schema_owner, \
            NULL AS default_character_set_catalog, \
            NULL AS default_character_set_schema, \
            NULL AS default_character_set_name \
            FROM paro_schemas() \
            WHERE database_name = current_database()",
        column_names: &[
            "catalog_name",
            "schema_name",
            "schema_owner",
            "default_character_set_catalog",
            "default_character_set_schema",
            "default_character_set_name",
        ],
        column_types: &[
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
        ],
    },
    DefaultView {
        name: "tables",
        sql: "SELECT \
            database_name AS table_catalog, \
            schema_name AS table_schema, \
            table_name, \
            CASE WHEN temporary THEN 'LOCAL TEMPORARY' ELSE 'BASE TABLE' END AS table_type, \
            NULL AS self_referencing_column_name, \
            NULL AS reference_generation, \
            NULL AS user_defined_type_catalog, \
            NULL AS user_defined_type_schema, \
            NULL AS user_defined_type_name, \
            'YES' AS is_insertable_into, \
            'NO' AS is_typed, \
            CASE WHEN temporary THEN 'PRESERVE' ELSE NULL END AS commit_action \
            FROM paro_tables() \
            UNION ALL \
            SELECT \
            database_name AS table_catalog, \
            schema_name AS table_schema, \
            view_name AS table_name, \
            'VIEW' AS table_type, \
            NULL AS self_referencing_column_name, \
            NULL AS reference_generation, \
            NULL AS user_defined_type_catalog, \
            NULL AS user_defined_type_schema, \
            NULL AS user_defined_type_name, \
            'NO' AS is_insertable_into, \
            'NO' AS is_typed, \
            NULL AS commit_action \
            FROM paro_views()",
        column_names: &[
            "table_catalog",
            "table_schema",
            "table_name",
            "table_type",
            "self_referencing_column_name",
            "reference_generation",
            "user_defined_type_catalog",
            "user_defined_type_schema",
            "user_defined_type_name",
            "is_insertable_into",
            "is_typed",
            "commit_action",
        ],
        column_types: &[
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
        ],
    },
    DefaultView {
        name: "columns",
        sql: "SELECT \
            database_name AS table_catalog, \
            schema_name AS table_schema, \
            table_name, \
            column_name, \
            column_index AS ordinal_position, \
            column_default, \
            CASE WHEN is_nullable THEN 'YES' ELSE 'NO' END AS is_nullable, \
            data_type, \
            NULL AS character_maximum_length, \
            NULL AS character_octet_length, \
            numeric_precision, \
            NULL AS numeric_precision_radix, \
            numeric_scale, \
            NULL AS datetime_precision \
            FROM paro_columns() \
            WHERE database_name = current_database()",
        column_names: &[
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position",
            "column_default",
            "is_nullable",
            "data_type",
            "character_maximum_length",
            "character_octet_length",
            "numeric_precision",
            "numeric_precision_radix",
            "numeric_scale",
            "datetime_precision",
        ],
        column_types: &[
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::BigInt,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::BigInt,
        ],
    },
    DefaultView {
        name: "views",
        sql: "SELECT \
            database_name AS table_catalog, \
            schema_name AS table_schema, \
            view_name AS table_name, \
            sql AS view_definition, \
            'NONE' AS check_option, \
            'NO' AS is_updatable, \
            'NO' AS is_insertable_into, \
            'NO' AS is_trigger_updatable, \
            'NO' AS is_trigger_deletable, \
            'NO' AS is_trigger_insertable_into \
            FROM paro_views() \
            WHERE database_name = current_database()",
        column_names: &[
            "table_catalog",
            "table_schema",
            "table_name",
            "view_definition",
            "check_option",
            "is_updatable",
            "is_insertable_into",
            "is_trigger_updatable",
            "is_trigger_deletable",
            "is_trigger_insertable_into",
        ],
        column_types: &[
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
        ],
    },
];

// ============================================================================
// pg_catalog Views (PostgreSQL Compatibility)
// ============================================================================

/// pg_catalog views for PostgreSQL compatibility.
pub static PG_CATALOG_VIEWS: &[DefaultView] = &[
    DefaultView {
        name: "pg_database",
        sql: "SELECT \
            database_oid AS oid, \
            database_name AS datname, \
            0 AS datdba, \
            6 AS encoding, \
            'C' AS datcollate, \
            'C' AS datctype, \
            false AS datistemplate, \
            true AS datallowconn, \
            -1 AS datconnlimit, \
            NULL AS datlastsysoid, \
            NULL AS datfrozenxid, \
            NULL AS datminmxid, \
            NULL AS dattablespace, \
            NULL AS datacl, \
            'c' AS datlocprovider, \
            NULL AS daticulocale, \
            NULL AS daticurules, \
            NULL AS datcollversion \
            FROM paro_databases()",
        column_names: &[
            "oid",
            "datname",
            "datdba",
            "encoding",
            "datcollate",
            "datctype",
            "datistemplate",
            "datallowconn",
            "datconnlimit",
            "datlastsysoid",
            "datfrozenxid",
            "datminmxid",
            "dattablespace",
            "datacl",
            "datlocprovider",
            "daticulocale",
            "daticurules",
            "datcollversion",
        ],
        column_types: &[
            LogicalType::BigInt,
            LogicalType::Varchar,
            LogicalType::BigInt,
            LogicalType::Integer,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Boolean,
            LogicalType::Boolean,
            LogicalType::Integer,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
        ],
    },
    DefaultView {
        name: "pg_namespace",
        sql: "SELECT \
            oid, \
            schema_name AS nspname, \
            0 AS nspowner, \
            NULL AS nspacl \
            FROM paro_schemas() \
            WHERE database_name = current_database()",
        column_names: &["oid", "nspname", "nspowner", "nspacl"],
        column_types: &[
            LogicalType::BigInt,
            LogicalType::Varchar,
            LogicalType::BigInt,
            LogicalType::Varchar,
        ],
    },
    DefaultView {
        name: "pg_tables",
        sql: "SELECT \
            schema_name AS schemaname, \
            table_name AS tablename, \
            'paro' AS tableowner, \
            NULL AS tablespace, \
            index_count > 0 AS hasindexes, \
            false AS hasrules, \
            false AS hastriggers \
            FROM paro_tables() \
            WHERE database_name = current_database()",
        column_names: &[
            "schemaname",
            "tablename",
            "tableowner",
            "tablespace",
            "hasindexes",
            "hasrules",
            "hastriggers",
        ],
        column_types: &[
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Boolean,
            LogicalType::Boolean,
            LogicalType::Boolean,
        ],
    },
    DefaultView {
        name: "pg_views",
        sql: "SELECT \
            schema_name AS schemaname, \
            view_name AS viewname, \
            'paro' AS viewowner, \
            sql AS definition \
            FROM paro_views() \
            WHERE database_name = current_database()",
        column_names: &["schemaname", "viewname", "viewowner", "definition"],
        column_types: &[
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
        ],
    },
    DefaultView {
        name: "pg_class",
        sql: "SELECT \
            table_oid AS oid, \
            table_name AS relname, \
            schema_oid AS relnamespace, \
            0 AS reltype, \
            0 AS relowner, \
            CASE WHEN temporary THEN 't' ELSE 'p' END AS relpersistence, \
            'r' AS relkind, \
            column_count AS relnatts, \
            false AS relhasoids, \
            index_count > 0 AS relhasindex \
            FROM paro_tables() \
            WHERE database_name = current_database() \
            UNION ALL \
            SELECT \
            view_oid AS oid, \
            view_name AS relname, \
            schema_oid AS relnamespace, \
            0 AS reltype, \
            0 AS relowner, \
            CASE WHEN temporary THEN 't' ELSE 'p' END AS relpersistence, \
            'v' AS relkind, \
            column_count AS relnatts, \
            false AS relhasoids, \
            false AS relhasindex \
            FROM paro_views() \
            WHERE database_name = current_database()",
        column_names: &[
            "oid",
            "relname",
            "relnamespace",
            "reltype",
            "relowner",
            "relpersistence",
            "relkind",
            "relnatts",
            "relhasoids",
            "relhasindex",
        ],
        column_types: &[
            LogicalType::BigInt,
            LogicalType::Varchar,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::BigInt,
            LogicalType::Boolean,
            LogicalType::Boolean,
        ],
    },
    DefaultView {
        name: "pg_attribute",
        sql: "SELECT \
            table_oid AS attrelid, \
            column_name AS attname, \
            data_type_id AS atttypid, \
            0 AS attstattarget, \
            NULL AS attlen, \
            column_index AS attnum, \
            0 AS attndims, \
            0 AS attcacheoff, \
            0 AS atttypmod, \
            ' ' AS attbyval, \
            ' ' AS attstorage, \
            ' ' AS attalign, \
            NOT is_nullable AS attnotnull, \
            false AS atthasdef, \
            ' ' AS attidentity, \
            false AS attisdropped, \
            false AS attislocal, \
            0 AS attinhcount, \
            0 AS attcollation \
            FROM paro_columns()",
        column_names: &[
            "attrelid",
            "attname",
            "atttypid",
            "attstattarget",
            "attlen",
            "attnum",
            "attndims",
            "attcacheoff",
            "atttypmod",
            "attbyval",
            "attstorage",
            "attalign",
            "attnotnull",
            "atthasdef",
            "attidentity",
            "attisdropped",
            "attislocal",
            "attinhcount",
            "attcollation",
        ],
        column_types: &[
            LogicalType::BigInt,
            LogicalType::Varchar,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::Varchar,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Boolean,
            LogicalType::Boolean,
            LogicalType::Varchar,
            LogicalType::Boolean,
            LogicalType::Boolean,
            LogicalType::BigInt,
            LogicalType::BigInt,
        ],
    },
    DefaultView {
        name: "pg_settings",
        sql: "SELECT * FROM paro_pg_settings()",
        column_names: &[
            "name",
            "setting",
            "unit",
            "category",
            "short_desc",
            "source",
            "vartype",
            "context",
        ],
        column_types: &[
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
        ],
    },
    DefaultView {
        name: "pg_prepared_statements",
        sql: "SELECT * FROM paro_pg_prepared_statements()",
        column_names: &[
            "name",
            "statement",
            "parameter_types",
            "from_sql",
            "generic_plans",
            "custom_plans",
        ],
        column_types: &[
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Boolean,
            LogicalType::BigInt,
            LogicalType::BigInt,
        ],
    },
    DefaultView {
        name: "pg_cursors",
        sql: "SELECT * FROM paro_pg_cursors()",
        column_names: &[
            "name",
            "statement",
            "is_holdable",
            "is_binary",
            "is_scrollable",
            "snapshot_read_ts",
            "snapshot_pin_duration_us",
            "snapshot_owner_session_id",
            "snapshot_portal_id",
            "snapshot_retention_policy",
        ],
        column_types: &[
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Boolean,
            LogicalType::Boolean,
            LogicalType::Boolean,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::Varchar,
            LogicalType::Varchar,
        ],
    },
];

// ============================================================================
// DefaultViewGenerator
// ============================================================================

/// Default generator for system views within a specific schema.
///
/// Each schema (public, information_schema, pg_catalog) has its own set of
/// default views. This generator creates views on-demand when first accessed.
pub struct DefaultViewGenerator {
    /// The catalog name (database name) for created views
    catalog_name: String,
    /// The schema name this generator is responsible for
    schema_name: String,
    object_id_allocator: Arc<CatalogObjectIdAllocator>,
}

impl DefaultViewGenerator {
    /// Create a new DefaultViewGenerator for the given catalog and schema.
    pub fn new(
        catalog_name: String,
        schema_name: String,
        object_id_allocator: Arc<CatalogObjectIdAllocator>,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            object_id_allocator,
        }
    }

    /// Get the view definitions for this schema.
    fn get_view_definitions(&self) -> &'static [DefaultView] {
        match self.schema_name.to_lowercase().as_str() {
            "public" => DEFAULT_VIEWS,
            "information_schema" => INFORMATION_SCHEMA_VIEWS,
            "pg_catalog" => PG_CATALOG_VIEWS,
            _ => &[],
        }
    }

    /// Find a view definition by name.
    fn find_view_definition(&self, name: &str) -> Option<&'static DefaultView> {
        let lower = name.to_lowercase();
        self.get_view_definitions()
            .iter()
            .find(|v| v.name.to_lowercase() == lower)
    }

    /// Parse the SQL query for a view definition.
    fn parse_view_sql(sql: &str) -> Option<Box<paro_parser::ast::Query>> {
        match paro_parser::parse_one(sql) {
            Ok(stmt) => match stmt.stmt {
                paro_parser::ast::Statement::Query(query) => Some(query),
                _ => None,
            },
            Err(_e) => None,
        }
    }
}

impl DefaultGenerator for DefaultViewGenerator {
    fn is_default_entry(&self, name: &str) -> bool {
        self.find_view_definition(name).is_some()
    }

    fn create_default_entry(&self, name: &str) -> Option<Arc<CatalogEntryEnum>> {
        let view_def = self.find_view_definition(name)?;

        // Parse the SQL query
        let query = Self::parse_view_sql(view_def.sql)?;

        // Create the view info
        let info = CreateViewInfo::new(self.schema_name.clone(), view_def.name.to_string(), query)
            .with_column_names(
                view_def
                    .column_names
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            )
            .with_column_types(view_def.column_types.to_vec())
            .with_sql(view_def.sql.to_string());

        // Create the view entry (timestamp = 0 means committed/permanent)
        let mut view = ViewCatalogEntry::new(
            info,
            0,
            self.catalog_name.clone(),
            self.object_id_allocator.allocate(),
        );

        // Mark as internal (cannot be dropped by users)
        view.base.base.internal = true;

        Some(Arc::new(CatalogEntryEnum::View(Arc::new(view))))
    }

    fn get_default_entries(&self) -> Vec<String> {
        self.get_view_definitions()
            .iter()
            .map(|v| v.name.to_string())
            .collect()
    }
}
