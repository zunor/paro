// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::bind::type_name::bind_logical_type;
use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;
use paro_catalog::entry::{ColumnDefinition, Constraint};
use paro_common::error::Result;
use paro_parser::ast::{
    ConstraintType as AstConstraintType, CreateOption, CreateTableSource, CreateTableStmt, TypeName,
};
use std::collections::{BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct BoundCreateTableInfo {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<Constraint>,
    pub if_not_exists: bool,
}

fn bind_column_type_with_nullability(
    data_type: &TypeName,
) -> Result<(paro_common::types::LogicalType, bool)> {
    match data_type {
        TypeName::NotNull(inner) => {
            let logical_type = bind_logical_type(inner)?;
            Ok((logical_type, true))
        }
        TypeName::Nullable(inner) => {
            let logical_type = bind_logical_type(inner)?;
            Ok((logical_type, false))
        }
        _ => {
            let logical_type = bind_logical_type(data_type)?;
            Ok((logical_type, false))
        }
    }
}

pub fn bind_create_table(
    binder: &mut Binder,
    statement: CreateTableStmt,
) -> Result<BoundStatementKind> {
    // 1. Resolve table, schema, and database names
    let database_name = statement
        .database
        .as_ref()
        .map(|i| i.name.clone())
        .unwrap_or(binder.catalog().name().to_string());
    let schema_name = statement
        .schema
        .as_ref()
        .map(|i| i.name.clone())
        .unwrap_or_else(|| binder.session_context().current_schema().to_string());
    let table_name = statement.table.name.clone();

    // 2. Verify database exists (multi-tenancy)
    if database_name != binder.catalog().name() {
        return Err(paro_common::error::not_implemented(format!(
            "Cross-database CREATE TABLE ({})",
            database_name
        )));
    }

    // 3. Verify schema exists (MVCC visibility check)
    let _ = binder
        .catalog()
        .get_schema(&binder.catalog_txn_view(), &schema_name)?;

    // 4. Bind columns and constraints
    let mut columns = Vec::new();
    let mut constraints = Vec::new();
    let mut column_name_to_index = HashMap::new();
    let mut not_null_columns = BTreeSet::new();
    let mut has_primary_key = false;
    if let Some(source) = &statement.source {
        match source {
            CreateTableSource::Columns {
                columns: source_columns,
                opt_column_constraints,
                opt_table_constraints,
                ..
            } => {
                for (column_idx, col_def) in source_columns.iter().enumerate() {
                    let col_name = col_def.name.name.clone();
                    if column_name_to_index
                        .insert(col_name.clone(), column_idx)
                        .is_some()
                    {
                        return Err(paro_common::error::catalog(format!(
                            "Duplicate column name '{}' in CREATE TABLE",
                            col_name
                        )));
                    }

                    let (logical_type, mut not_null) =
                        bind_column_type_with_nullability(&col_def.data_type)?;

                    if col_def.is_primary_key {
                        if has_primary_key {
                            return Err(paro_common::error::catalog(
                                "Multiple PRIMARY KEY constraints are not supported".to_string(),
                            ));
                        }
                        constraints.push(Constraint::primary_key(vec![column_idx]));
                        has_primary_key = true;
                        not_null = true;
                    }

                    if not_null {
                        not_null_columns.insert(column_idx);
                    }

                    columns.push(ColumnDefinition {
                        name: col_name,
                        logical_type,
                        not_null,
                        default_value: None,
                        comment: None,
                    });
                }

                if let Some(column_constraints) = opt_column_constraints {
                    for constraint in column_constraints {
                        if let AstConstraintType::Check(expr) = &constraint.constraint_type {
                            constraints.push(Constraint::check(expr.to_string()));
                        }
                    }
                }

                if let Some(table_constraints) = opt_table_constraints {
                    for constraint in table_constraints {
                        match &constraint.constraint_type {
                            AstConstraintType::Check(expr) => {
                                constraints.push(Constraint::check(expr.to_string()));
                            }
                            AstConstraintType::PrimaryKey(primary_columns) => {
                                if has_primary_key {
                                    return Err(paro_common::error::catalog(
                                        "Multiple PRIMARY KEY constraints are not supported"
                                            .to_string(),
                                    ));
                                }
                                if primary_columns.is_empty() {
                                    return Err(paro_common::error::catalog(
                                        "PRIMARY KEY must reference at least one column"
                                            .to_string(),
                                    ));
                                }

                                let mut pk_indices = Vec::with_capacity(primary_columns.len());
                                let mut seen = HashSet::new();
                                for column in primary_columns {
                                    let column_name = &column.name;
                                    let column_idx = *column_name_to_index
                                        .get(column_name)
                                        .ok_or_else(|| {
                                            paro_common::error::catalog(format!(
                                                "PRIMARY KEY column '{}' does not exist",
                                                column_name
                                            ))
                                        })?;

                                    if !seen.insert(column_idx) {
                                        return Err(paro_common::error::catalog(format!(
                                            "Duplicate column '{}' in PRIMARY KEY",
                                            column_name
                                        )));
                                    }

                                    pk_indices.push(column_idx);
                                    not_null_columns.insert(column_idx);
                                    if let Some(column_def) = columns.get_mut(column_idx) {
                                        column_def.not_null = true;
                                    }
                                }

                                constraints.push(Constraint::primary_key(pk_indices));
                                has_primary_key = true;
                            }
                        }
                    }
                }
            }
            CreateTableSource::Like { .. } => {
                return Err(paro_common::error::not_implemented(
                    "CREATE TABLE LIKE is not supported yet",
                ));
            }
        }
    }

    for column_idx in not_null_columns {
        constraints.push(Constraint::not_null(column_idx));
    }

    Ok(BoundStatementKind::CreateTable(BoundCreateTableInfo {
        database_name,
        schema_name,
        table_name,
        columns,
        constraints,
        if_not_exists: matches!(statement.create_option, CreateOption::CreateIfNotExists),
    }))
}
