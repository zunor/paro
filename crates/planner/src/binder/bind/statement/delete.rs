// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::DeleteStmt;

use crate::binder::bind::expr::bind_expression;
use crate::binder::ir::BoundFromItem;
use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;
use crate::expression::Expression;

/// Information about a bound DELETE statement.
///
///
#[derive(Debug, Clone)]
pub struct BoundDeleteInfo {
    /// The table to delete from.
    pub table: Arc<TableCatalogEntry>,
    pub table_index: usize,
    /// The condition for deletion (WHERE clause).
    pub condition: Option<Expression>,
}

/// Bind a DELETE statement.
pub fn bind_delete(binder: &mut Binder, delete: DeleteStmt) -> Result<BoundStatementKind> {
    // 1. Bind the table reference
    // This adds the table to the BindContext, allowing column resolution in WHERE.
    let table_ref = binder.bind_table_ref(delete.table)?;

    // For now, we only support deleting from a base table.
    let bound_base_table = match table_ref {
        BoundFromItem::BaseTable(bt) => bt,
        _ => {
            return Err(paro_error::not_implemented(
                "DELETE from non-base table".to_string(),
            ))
        }
    };

    // 2. Bind the WHERE clause if present
    let condition = if let Some(expr) = delete.selection {
        Some(bind_expression(binder, expr)?)
    } else {
        None
    };

    // 3. Return the bound delete info
    Ok(BoundStatementKind::Delete(BoundDeleteInfo {
        table: bound_base_table.table,
        table_index: bound_base_table.table_index,
        condition,
    }))
}
