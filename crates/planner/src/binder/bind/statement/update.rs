// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::{TableReference, UpdateStmt};

use crate::binder::bind::expr;
use crate::binder::ir::BoundFromItem;
use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;
use crate::expression::Expression;

/// Information about a bound UPDATE statement.
///
///
#[derive(Debug, Clone)]
pub struct BoundUpdateInfo {
    /// The table to update.
    pub table: Arc<TableCatalogEntry>,
    pub table_index: usize,
    /// The indices of the columns being updated.
    pub column_indices: Vec<usize>,
    /// The expressions for the new values.
    pub expressions: Vec<Expression>,
    /// The condition for update (WHERE clause).
    pub condition: Option<Expression>,
}

/// Bind an UPDATE statement.
pub fn bind_update(binder: &mut Binder, update: UpdateStmt) -> Result<BoundStatementKind> {
    // 1. Resolve the table to update
    let table_ref_ast = TableReference::Table {
        span: None,
        database: update.database.clone(),
        schema: update.schema.clone(),
        table: update.table.clone(),
        alias: update.table_alias.clone(),
        temporal: None,
        with_options: None,
        pivot: None,
        unpivot: None,
        sample: None,
    };

    let table_ref = binder.bind_table_ref(table_ref_ast)?;

    // For now, we only support updating a base table.
    let bound_base_table = match table_ref {
        BoundFromItem::BaseTable(bt) => bt,
        _ => {
            return Err(paro_error::not_implemented(
                "UPDATE non-base table".to_string(),
            ))
        }
    };

    let table = bound_base_table.table;
    let mut column_indices = Vec::new();
    let mut expressions = Vec::new();

    // 2. Bind assignments
    for mutation_expr in update.update_list {
        let col_name = &mutation_expr.name.name;
        let col_idx = table
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(col_name))
            .ok_or_else(|| {
                paro_error::catalog(format!(
                    "Column {} not found in table {}",
                    col_name, table.base.base.name
                ))
            })?;

        column_indices.push(col_idx);

        // Bind the expression on the right-hand side
        let bound_expr = expr::bind_expression(binder, mutation_expr.expr)?;
        expressions.push(bound_expr);
    }

    // 3. Bind the WHERE clause if present
    let condition = if let Some(expr) = update.selection {
        Some(expr::bind_expression(binder, expr)?)
    } else {
        None
    };

    // 4. Return the bound update info
    Ok(BoundStatementKind::Update(BoundUpdateInfo {
        table,
        table_index: bound_base_table.table_index,
        column_indices,
        expressions,
        condition,
    }))
}
