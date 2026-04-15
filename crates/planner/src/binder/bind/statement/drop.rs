// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Drop Statement Binding
//!
//!
//!
//! ## Supported Drop Types
//! - DROP TABLE
//! - DROP DATABASE (Schema)
//! - DROP INDEX
//! - DROP VIEW
//!
//! ## Known Limitations
//! - CASCADE/RESTRICT not fully implemented
//! - No dependency checking

use paro_catalog::entry::CatalogType;
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::{
    DropIndexStmt, DropSchemaStmt, DropSequenceStmt, DropTableStmt, DropViewStmt,
};

use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;

/// The type of object being dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropType {
    Table,
    Schema,
    Index,
    View,
    Sequence,
}

/// Information about a bound DROP statement.
#[derive(Debug, Clone)]
pub struct BoundDropInfo {
    /// The type of object to drop.
    pub drop_type: DropType,
    /// The database name.
    pub database_name: String,
    /// The schema name.
    pub schema_name: String,
    /// The object name.
    pub object_name: String,
    /// Whether IF EXISTS was specified.
    pub if_exists: bool,
    /// Whether CASCADE was specified.
    pub cascade: bool,
}

/// Bind a DROP TABLE statement.
pub fn bind_drop_table(
    binder: &mut Binder,
    drop_table: DropTableStmt,
) -> Result<BoundStatementKind> {
    // 1. Resolve database and schema
    let is_single_part_name = drop_table.database.is_none() && drop_table.schema.is_none();
    let object_name = drop_table.table.name.clone();

    // 2. Resolve target catalog and schema
    let (_table_entry, database_name, schema_name) = if is_single_part_name {
        let search_path = binder.session_context().search_path();
        let mut found = None;

        for search_entry in search_path {
            let catalog_name = if search_entry.catalog.is_empty() {
                binder.catalog().name().to_string()
            } else {
                search_entry.catalog.clone()
            };

            let catalog = if catalog_name == binder.catalog().name() {
                Some(binder.catalog())
            } else {
                binder
                    .session_context()
                    .database(&catalog_name)
                    .map(|db| db.catalog.clone())
            };

            if let Some(catalog) = catalog {
                if let Ok(entry) = catalog.get_table(
                    &binder.catalog_txn_view(),
                    &search_entry.schema,
                    &object_name,
                ) {
                    found = Some((entry, catalog_name, search_entry.schema.clone()));
                    break;
                }
            }
        }

        if let Some(res) = found {
            res
        } else if drop_table.if_exists {
            return Ok(BoundStatementKind::Drop(BoundDropInfo {
                drop_type: DropType::Table,
                database_name: binder.catalog().name().to_string(),
                schema_name: binder.session_context().current_schema().to_string(),
                object_name,
                if_exists: true,
                cascade: false,
            }));
        } else {
            return Err(paro_error::catalog(format!(
                "Table '{}' not found in search path",
                object_name
            )));
        }
    } else {
        let database_name = drop_table
            .database
            .map(|c| c.name)
            .unwrap_or_else(|| binder.catalog().name().to_string());
        let schema_name = drop_table
            .schema
            .map(|d| d.name)
            .unwrap_or_else(|| binder.session_context().current_schema().to_string());

        let entry =
            binder
                .catalog()
                .get_table(&binder.catalog_txn_view(), &schema_name, &object_name);

        match entry {
            Ok(e) => (e, database_name, schema_name),
            Err(e) => {
                if drop_table.if_exists {
                    return Ok(BoundStatementKind::Drop(BoundDropInfo {
                        drop_type: DropType::Table,
                        database_name,
                        schema_name,
                        object_name,
                        if_exists: true,
                        cascade: false,
                    }));
                } else {
                    return Err(e);
                }
            }
        }
    };

    Ok(BoundStatementKind::Drop(BoundDropInfo {
        drop_type: DropType::Table,
        database_name,
        schema_name,
        object_name,
        if_exists: drop_table.if_exists,
        cascade: false,
    }))
}

/// Bind a DROP SCHEMA statement.
pub fn bind_drop_schema(binder: &mut Binder, stmt: DropSchemaStmt) -> Result<BoundStatementKind> {
    let database_name = stmt
        .database
        .map(|c| c.name)
        .unwrap_or_else(|| binder.catalog().name().to_string());
    let schema_name = stmt.schema.name.clone();

    // Validate that the schema exists (unless IF EXISTS is specified)
    let entry = binder
        .catalog()
        .get_schema(&binder.catalog_txn_view(), &schema_name);
    if entry.is_err() && !stmt.if_exists {
        return Err(paro_error::catalog(format!(
            "Schema '{}' does not exist",
            schema_name
        )));
    }

    Ok(BoundStatementKind::Drop(BoundDropInfo {
        drop_type: DropType::Schema,
        database_name,
        schema_name: "public".to_string(), // schema doesn't have a parent schema
        object_name: schema_name,
        if_exists: stmt.if_exists,
        cascade: stmt.cascade,
    }))
}

/// Bind a DROP INDEX statement.
pub fn bind_drop_index(
    binder: &mut Binder,
    drop_index: DropIndexStmt,
) -> Result<BoundStatementKind> {
    let object_name = drop_index.index.name.clone();
    let current_database = binder.catalog().name().to_string();

    let (database_name, schema_name) =
        if drop_index.database.is_none() && drop_index.schema.is_none() {
            let mut found = None;
            for search_entry in binder.session_context().search_path() {
                let catalog_name = if search_entry.catalog.is_empty() {
                    current_database.clone()
                } else {
                    search_entry.catalog.clone()
                };

                let catalog = if catalog_name == current_database {
                    Some(binder.catalog())
                } else {
                    binder
                        .session_context()
                        .database(&catalog_name)
                        .map(|db| db.catalog.clone())
                };

                if let Some(catalog) = catalog {
                    if catalog
                        .get_any_entry(
                            &binder.catalog_txn_view(),
                            &search_entry.schema,
                            CatalogType::Index,
                            &object_name,
                        )
                        .is_ok()
                    {
                        found = Some((catalog_name, search_entry.schema.clone()));
                        break;
                    }
                }
            }

            if let Some(resolved) = found {
                resolved
            } else if drop_index.if_exists {
                (
                    current_database.clone(),
                    binder.session_context().current_schema().to_string(),
                )
            } else {
                return Err(paro_error::catalog(format!(
                    "Index '{}' not found in search path",
                    object_name
                )));
            }
        } else {
            let database_name = drop_index
                .database
                .map(|database| database.name)
                .unwrap_or_else(|| current_database.clone());
            let schema_name = drop_index
                .schema
                .map(|schema| schema.name)
                .unwrap_or_else(|| binder.session_context().current_schema().to_string());

            let catalog = if database_name == current_database {
                binder.catalog()
            } else {
                binder
                    .session_context()
                    .database(&database_name)
                    .ok_or_else(|| {
                        paro_error::catalog(format!("Database '{}' not found", database_name))
                    })?
                    .catalog
                    .clone()
            };

            let exists = catalog
                .get_any_entry(
                    &binder.catalog_txn_view(),
                    &schema_name,
                    CatalogType::Index,
                    &object_name,
                )
                .is_ok();

            if !exists && !drop_index.if_exists {
                return Err(paro_error::object_not_found("index", &object_name));
            }

            (database_name, schema_name)
        };

    Ok(BoundStatementKind::Drop(BoundDropInfo {
        drop_type: DropType::Index,
        database_name,
        schema_name,
        object_name,
        if_exists: drop_index.if_exists,
        cascade: false,
    }))
}

/// Bind a DROP VIEW statement.
///
/// Validates that the view exists (unless IF EXISTS is specified) and
/// constructs a BoundDropInfo with DropType::View.
pub fn bind_drop_view(binder: &mut Binder, drop_view: DropViewStmt) -> Result<BoundStatementKind> {
    let database_name = drop_view
        .database
        .map(|c| c.name)
        .unwrap_or_else(|| binder.catalog().name().to_string());
    let schema_name = drop_view
        .schema
        .map(|s| s.name)
        .unwrap_or_else(|| binder.session_context().current_schema().to_string());
    let object_name = drop_view.view.name.clone();

    // Validate that the view exists (unless IF EXISTS is specified)
    let entry = binder
        .catalog()
        .get_view(&binder.catalog_txn_view(), &schema_name, &object_name);
    if entry.is_err() && !drop_view.if_exists {
        return Err(paro_error::catalog(format!(
            "View '{}' does not exist",
            object_name
        )));
    }

    Ok(BoundStatementKind::Drop(BoundDropInfo {
        drop_type: DropType::View,
        database_name,
        schema_name,
        object_name,
        if_exists: drop_view.if_exists,
        cascade: false,
    }))
}

pub fn bind_drop_sequence(
    binder: &mut Binder,
    drop_sequence: DropSequenceStmt,
) -> Result<BoundStatementKind> {
    let database_name = binder.catalog().name().to_string();
    let schema_name = binder.session_context().current_schema().to_string();
    let object_name = drop_sequence.sequence.name.clone();

    let existing = binder
        .catalog()
        .get_schema(&binder.catalog_txn_view(), &schema_name)
        .ok()
        .and_then(|schema| {
            schema.get_sequence(
                binder.catalog_txn_view().transaction_id,
                binder.catalog_txn_view().start_time,
                &object_name,
            )
        });

    if existing.is_none() && !drop_sequence.if_exists {
        return Err(paro_error::object_not_found("sequence", &object_name));
    }

    Ok(BoundStatementKind::Drop(BoundDropInfo {
        drop_type: DropType::Sequence,
        database_name,
        schema_name,
        object_name,
        if_exists: drop_sequence.if_exists,
        cascade: false,
    }))
}
