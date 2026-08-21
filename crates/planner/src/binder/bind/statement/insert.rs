// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//!

use std::sync::Arc;

use crate::binder::ir::BoundQuery;
use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;
use crate::operator::insert::{InsertOnConflict, InsertOnConflictAction};
use paro_catalog::entry::ConstraintType;
use paro_catalog::entry::TableCatalogEntry;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::{Expr, InsertSource, InsertStmt, OnConflictAction, Query, SetExpr};
use paro_parser::Span;

/// Bound information for an INSERT statement.
#[derive(Debug, Clone)]
pub struct BoundInsertInfo {
    /// The target table to insert into.
    pub table: Arc<TableCatalogEntry>,
    /// The indices of the columns being inserted into.
    pub column_indices: Vec<usize>,
    /// The names of the columns being inserted into.
    pub column_names: Vec<String>,
    /// The expected types for the source data.
    pub expected_types: Vec<LogicalType>,
    /// The bound source of the data (SELECT, VALUES, or SetOperation).
    pub source: Box<BoundQuery>,
    /// Optional ON CONFLICT behavior.
    pub on_conflict: Option<InsertOnConflict>,
}

/// Bind an INSERT statement.
pub fn bind_insert(binder: &mut Binder, stmt: InsertStmt) -> Result<BoundStatementKind> {
    // 1. Resolve target table
    let is_single_part_name = stmt.database.is_none() && stmt.schema.is_none();
    let table_name = stmt.table.name.clone();

    let table_entry = if is_single_part_name {
        let search_path = binder.session_context().search_path();
        let mut found_entry = None;
        for search_entry in search_path {
            let catalog_name = if search_entry.catalog.is_empty() {
                binder.catalog().name().to_string()
            } else {
                search_entry.catalog.clone()
            };

            // Get the catalog for this entry
            let catalog = if catalog_name == binder.catalog().name() {
                Some(binder.catalog())
            } else {
                binder
                    .session_context()
                    .database(&catalog_name)
                    .map(|db| db.catalog.clone())
            };

            if let Some(catalog) = catalog {
                if let Ok(e) = catalog.get_table(
                    &binder.catalog_txn_view(),
                    &search_entry.schema,
                    &table_name,
                ) {
                    found_entry = Some(e);
                    break;
                }
            }
        }
        found_entry
            .ok_or_else(|| paro_error::catalog(format!("Table '{}' not found", table_name)))?
    } else {
        let database_name = stmt
            .database
            .as_ref()
            .map(|i| i.name.clone())
            .unwrap_or_else(|| binder.catalog().name().to_string());
        let schema_name = stmt
            .schema
            .as_ref()
            .map(|i| i.name.clone())
            .unwrap_or_else(|| binder.session_context().current_schema().to_string());

        if database_name != binder.catalog().name() {
            return Err(paro_error::not_implemented(format!(
                "Cross-database INSERT INTO ({})",
                database_name
            )));
        }

        binder
            .catalog()
            .get_table(&binder.catalog_txn_view(), &schema_name, &table_name)?
    };

    // Extract TableCatalogEntry from CatalogEntryEnum
    let table = match table_entry.as_ref() {
        paro_catalog::entry::CatalogEntryEnum::Table(t) => Arc::clone(t),
        _ => return Err(paro_error::wrong_object_type("table", &table_name)),
    };

    let schema_name = table.base.schema_name.clone();

    // 2. Map columns
    let mut column_indices = Vec::new();
    let mut column_names = Vec::new();
    let mut expected_types = Vec::new();

    if stmt.columns.is_empty() {
        // Insert into all columns
        for (i, col) in table.columns.iter().enumerate() {
            column_indices.push(i);
            column_names.push(col.name.clone());
            expected_types.push(col.logical_type.clone());
        }
    } else {
        // Insert into specific columns
        for col_ident in &stmt.columns {
            let col_name = &col_ident.name;
            let found = table
                .columns
                .iter()
                .enumerate()
                .find(|(_, c)| c.name.eq_ignore_ascii_case(col_name));
            if let Some((idx, col)) = found {
                column_indices.push(idx);
                column_names.push(col.name.clone());
                expected_types.push(col.logical_type.clone());
            } else {
                return Err(paro_error::catalog(format!(
                    "Column {} not found in table {}.{}",
                    col_name, schema_name, table_name
                )));
            }
        }
    }

    // 3. Bind source
    let source_query = match stmt.source {
        InsertSource::Values { rows } => Query {
            span: Span::default(),
            with: None,
            body: SetExpr::Values {
                span: Span::default(),
                values: rows,
            },
            order_by: vec![],
            limit: vec![],
            offset: None,
            locking: None,
            ignore_result: false,
        },
        InsertSource::Select { query } => *query,
        _ => {
            return Err(paro_error::not_implemented(
                "Unsupported INSERT source type",
            ));
        }
    };

    let mut bound_source = Box::new(binder.bind_query(source_query)?);

    // 4. Verify column count
    let source_types = bound_source.types();
    if source_types.len() != expected_types.len() {
        return Err(paro_error::syntax(format!(
            "Column count mismatch: table expects {} columns, but source provides {}",
            expected_types.len(),
            source_types.len()
        )));
    }

    // 5. Verify types and add casts if needed
    let cast_functions = binder.session_context().cast_functions();
    bound_source.cast_to_types(&expected_types, &cast_functions)?;

    let on_conflict = if let Some(on_conflict) = stmt.on_conflict {
        let key_columns: Vec<usize> = table
            .constraints()
            .iter()
            .find(|constraint| constraint.constraint_type == ConstraintType::PrimaryKey)
            .map(|constraint| constraint.columns.clone())
            .unwrap_or_default();
        if key_columns.is_empty() {
            return Err(paro_error::not_implemented(
                "ON CONFLICT requires a PRIMARY KEY table",
            ));
        }

        let mut target_columns = Vec::with_capacity(on_conflict.columns.len());
        for ident in &on_conflict.columns {
            let idx = table
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&ident.name))
                .ok_or_else(|| {
                    paro_error::catalog(format!(
                        "Column {} not found in table {}.{}",
                        ident.name, schema_name, table_name
                    ))
                })?;
            target_columns.push(idx);
        }
        if target_columns != key_columns {
            return Err(paro_error::not_implemented(
                "ON CONFLICT target must exactly match PRIMARY KEY columns in key order",
            ));
        }

        Some(match on_conflict.action {
            OnConflictAction::DoNothing => InsertOnConflict {
                target_columns,
                action: InsertOnConflictAction::DoNothing,
            },
            OnConflictAction::DoUpdate { update_list } => {
                let mut update_target_columns = Vec::with_capacity(update_list.len());
                let mut source_columns = Vec::with_capacity(update_list.len());
                for update in update_list {
                    let target_idx = table
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&update.name.name))
                        .ok_or_else(|| {
                            paro_error::catalog(format!(
                                "Column {} not found in table {}.{}",
                                update.name.name, schema_name, table_name
                            ))
                        })?;
                    let Expr::ColumnRef { column, .. } = &update.expr else {
                        return Err(paro_error::not_implemented(
                            "ON CONFLICT DO UPDATE currently only supports EXCLUDED.column assignments",
                        ));
                    };
                    let table_ident = column.table.as_ref().ok_or_else(|| {
                        paro_error::not_implemented(
                            "ON CONFLICT DO UPDATE requires EXCLUDED.column assignments",
                        )
                    })?;
                    if !table_ident.name.eq_ignore_ascii_case("excluded") {
                        return Err(paro_error::not_implemented(
                            "ON CONFLICT DO UPDATE currently only supports EXCLUDED.column assignments",
                        ));
                    }
                    let source_name = column.column.name();
                    let source_idx = table
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(source_name))
                        .ok_or_else(|| {
                            paro_error::catalog(format!(
                                "Column {} not found in table {}.{}",
                                source_name, schema_name, table_name
                            ))
                        })?;
                    update_target_columns.push(target_idx);
                    source_columns.push(source_idx);
                }

                InsertOnConflict {
                    target_columns,
                    action: InsertOnConflictAction::DoUpdate {
                        target_columns: update_target_columns,
                        source_columns,
                    },
                }
            }
        })
    } else {
        None
    };

    Ok(BoundStatementKind::Insert(BoundInsertInfo {
        table: Arc::clone(&table),
        column_indices,
        column_names,
        expected_types,
        source: bound_source,
        on_conflict,
    }))
}
