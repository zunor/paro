// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;
use paro_catalog::entry::{AlterEntryInfo, CatalogType};
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::{
    AlterTableAction, AlterTableStmt, ModifyColumnAction, RenameTableStmt, TableReference,
};

#[derive(Debug, Clone)]
pub struct BoundAlterEntryInfo {
    pub database_name: String,
    pub schema_name: String,
    pub entry_name: String,
    pub info: AlterEntryInfo,
    pub sql: String,
}

pub fn bind_alter_table(binder: &mut Binder, stmt: AlterTableStmt) -> Result<BoundStatementKind> {
    let sql = stmt.to_string();
    let TableReference::Table {
        database,
        schema,
        table,
        ..
    } = stmt.table_reference
    else {
        return Err(paro_error::not_implemented(
            "ALTER TABLE only supports base tables",
        ));
    };

    let database_name = database
        .map(|ident| ident.name)
        .unwrap_or_else(|| binder.catalog().name().to_string());
    if database_name != binder.catalog().name() {
        return Err(paro_error::not_implemented(format!(
            "Cross-database ALTER TABLE ({database_name})",
        )));
    }

    let schema_name = schema
        .map(|ident| ident.name)
        .unwrap_or_else(|| binder.session_context().current_schema().to_string());
    let entry_name = table.name;

    let action_info = match stmt.action {
        AlterTableAction::RenameTable { new_table } => {
            AlterEntryInfo::new(CatalogType::Table, entry_name.clone())
                .with_new_name(new_table.name)
        }
        AlterTableAction::RenameColumn {
            old_column,
            new_column,
        } => AlterEntryInfo::new(CatalogType::Table, entry_name.clone())
            .with_renamed_column(old_column.name, new_column.name),
        AlterTableAction::ModifyTableComment { new_comment } => {
            AlterEntryInfo::new(CatalogType::Table, entry_name.clone())
                .with_new_comment(new_comment)
        }
        AlterTableAction::ModifyColumn {
            action: ModifyColumnAction::Comment(columns),
        } => {
            if columns.is_empty() {
                return Err(paro_error::invalid_input(
                    "ALTER TABLE column comment list must not be empty",
                ));
            }
            let comments = columns
                .into_iter()
                .map(|column| paro_catalog::entry::ColumnCommentUpdate {
                    column_name: column.name.name,
                    comment: column.comment,
                })
                .collect();
            AlterEntryInfo::new(CatalogType::Table, entry_name.clone())
                .with_column_comments(comments)
        }
        other => {
            return Err(paro_error::not_implemented(format!(
                "ALTER TABLE action is not yet supported in txn refactor path: {other}",
            )))
        }
    };

    if binder
        .catalog()
        .get_table(&binder.catalog_txn_view(), &schema_name, &entry_name)
        .is_err()
    {
        if stmt.if_exists {
            return Ok(BoundStatementKind::Dummy);
        }
        return Err(paro_error::object_not_found("table", &entry_name));
    }

    Ok(BoundStatementKind::AlterEntry(BoundAlterEntryInfo {
        database_name,
        schema_name,
        entry_name,
        info: action_info,
        sql,
    }))
}

pub fn bind_rename_table(binder: &mut Binder, stmt: RenameTableStmt) -> Result<BoundStatementKind> {
    let sql = stmt.to_string();
    let RenameTableStmt {
        if_exists,
        database,
        schema,
        table,
        new_database,
        new_schema,
        new_table,
    } = stmt;

    let database_name = database
        .map(|ident| ident.name)
        .unwrap_or_else(|| binder.catalog().name().to_string());
    if database_name != binder.catalog().name() {
        return Err(paro_error::not_implemented(format!(
            "Cross-database RENAME TABLE ({database_name})",
        )));
    }

    let schema_name = schema
        .map(|ident| ident.name)
        .unwrap_or_else(|| binder.session_context().current_schema().to_string());
    let entry_name = table.name;

    if new_database.is_some() {
        return Err(paro_error::not_implemented(
            "Cross-database RENAME TABLE is not yet supported",
        ));
    }

    if binder
        .catalog()
        .get_table(&binder.catalog_txn_view(), &schema_name, &entry_name)
        .is_err()
    {
        if if_exists {
            return Ok(BoundStatementKind::Dummy);
        }
        return Err(paro_error::object_not_found("table", &entry_name));
    }

    let mut info =
        AlterEntryInfo::new(CatalogType::Table, entry_name.clone()).with_new_name(new_table.name);
    if let Some(new_schema) = new_schema {
        info = info.with_new_schema(new_schema.name);
    }

    Ok(BoundStatementKind::AlterEntry(BoundAlterEntryInfo {
        database_name,
        schema_name,
        entry_name,
        info,
        sql,
    }))
}
