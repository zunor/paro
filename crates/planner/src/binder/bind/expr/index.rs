// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binds expressions in `CREATE INDEX`: column refs only; rejects aggregates, subqueries, and window functions.
//! Expression indexes (e.g. on `lower(name)`) are not supported yet.

use crate::binder::bind::expr;
use crate::binder::Binder;
use crate::expression::{ColumnRefExpression, Expression};
use crate::operator::ColumnBinding;
use paro_catalog::entry::TableCatalogEntry;
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::Expr;
use std::sync::Arc;

/// Binds `CREATE INDEX` index expressions (restricted subset of SQL expressions).
pub struct IndexBinder<'a> {
    binder: &'a mut Binder,
    table: Arc<TableCatalogEntry>,
    table_index: usize,
}

impl<'a> IndexBinder<'a> {
    /// Create a new IndexBinder.
    pub fn new(binder: &'a mut Binder, table: Arc<TableCatalogEntry>, table_index: usize) -> Self {
        Self {
            binder,
            table,
            table_index,
        }
    }

    /// Setup the bind context by registering the target table.
    pub fn setup_bind_context(&mut self) {
        let table_name = self.table.base.base.name.clone();
        let column_names = self.table.columns.iter().map(|c| c.name.clone()).collect();
        let column_types = self
            .table
            .columns
            .iter()
            .map(|c| c.logical_type.clone())
            .collect();

        self.binder.bind_context.add_binding(
            table_name,
            self.table_index,
            column_names,
            column_types,
        );
    }

    /// Bind an expression in a CREATE INDEX statement.
    pub fn bind_expression(&mut self, expr: Expr) -> Result<Expression> {
        let bound_expr = self.bind_internal(expr)?;

        // Validate that no forbidden expressions are used
        self.validate_expression(&bound_expr)?;

        Ok(bound_expr)
    }

    /// Recursively validate the bound expression.
    fn validate_expression(&self, expr: &Expression) -> Result<()> {
        match expr {
            Expression::Window(_) => Err(paro_error::syntax(
                "Window functions are not allowed in index expressions",
            )),
            Expression::Subquery(_) => Err(paro_error::syntax(
                "Subqueries are not allowed in index expressions",
            )),
            Expression::Aggregate(_) => Err(paro_error::syntax(
                "Aggregate functions are not allowed in index expressions",
            )),
            _ => Ok(()),
        }
    }

    /// Internal binding logic.
    fn bind_internal(&mut self, expr: Expr) -> Result<Expression> {
        match expr {
            Expr::ColumnRef { column, .. } => {
                // If there's a table qualifier, it must match our target table
                if let Some(table_name) = &column.table {
                    if !table_name
                        .name
                        .eq_ignore_ascii_case(&self.table.base.base.name)
                    {
                        return Err(paro_error::syntax(format!(
                            "Table qualifier '{}' does not match indexed table '{}'",
                            table_name.name, self.table.base.base.name
                        )));
                    }
                }
                let column_name = column.column.name();
                self.bind_column_ref(column_name)
            }

            // For other expressions, use the standard expression binder
            _ => expr::bind_expression(self.binder, expr),
        }
    }

    /// Bind a column reference to the target table.
    fn bind_column_ref(&mut self, column_name: &str) -> Result<Expression> {
        // Find the column in the target table
        let column_index = self
            .table
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(column_name))
            .ok_or_else(|| {
                paro_error::catalog(format!(
                    "Column '{}' not found in table '{}'",
                    column_name, self.table.base.base.name
                ))
            })?;

        let return_type = self.table.columns[column_index].logical_type.clone();

        Ok(Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(self.table_index, column_index),
            return_type,
        )))
    }

    /// Get the column IDs from the bound expressions.
    pub fn get_column_ids(expressions: &[Expression]) -> Vec<usize> {
        let mut column_ids = Vec::new();
        for expr in expressions {
            if let Expression::ColumnRef(col_ref) = expr {
                column_ids.push(col_ref.binding.column_index);
            }
        }
        column_ids
    }
}
