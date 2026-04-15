// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Select Binder
//!
//!
//!
//! The SelectBinder is responsible for binding expressions within
//! the SELECT list of a SQL statement.

use crate::binder::bind::expr::ExpressionBinder;
use crate::binder::ir::BoundSelect;
use crate::expression::Expression;
use paro_common::error::Result;
use paro_parser::ast::{ColumnRef, Expr};

use super::{BaseSelectBinder, BoundGroupInformation, SelectBindState};

/// The SELECT binder is responsible for binding an expression within the SELECT clause.
///
pub struct SelectBinder<'a> {
    pub base: BaseSelectBinder<'a>,
    pub current_index: Option<usize>,
}

impl<'a> SelectBinder<'a> {
    /// Create a new SelectBinder.
    pub fn new(binder: &'a mut crate::binder::Binder, bind_state: &'a mut SelectBindState) -> Self {
        Self {
            base: BaseSelectBinder::new(binder, bind_state),
            current_index: None,
        }
    }

    /// Create a new SelectBinder with node and group info.
    pub fn with_node_and_info(
        binder: &'a mut crate::binder::Binder,
        bind_state: &'a mut SelectBindState,
        node: &'a mut BoundSelect,
        info: &'a BoundGroupInformation,
    ) -> Self {
        Self {
            base: BaseSelectBinder::with_node_and_info(binder, bind_state, node, info),
            current_index: None,
        }
    }

    /// Bind an expression in the SELECT list.
    pub fn bind(&mut self, expr: Expr) -> Result<Expression> {
        self.bind_expression(expr)
    }

    /// Bind an expression at a specific index in the SELECT list.
    pub fn bind_at_index(&mut self, expr: Expr, index: usize) -> Result<Expression> {
        self.current_index = Some(index);
        let result = self.bind(expr);
        self.current_index = None;
        result
    }

    /// Bind an expression, handling specialized SELECT list types and alias resolution.
    pub fn bind_expression(&mut self, expr: Expr) -> Result<Expression> {
        match expr {
            Expr::ColumnRef { ref column, .. } => {
                // Try to resolve as an alias reference first
                if let Some(bound_alias) = self.try_resolve_alias_reference(column)? {
                    return Ok(bound_alias);
                }

                // Fallback to base binder (handles grouping and regular columns)
                self.base.bind_expression(expr)
            }
            _ => self.base.bind_expression(expr),
        }
    }

    /// Try to resolve an alias reference in the SELECT list.
    ///
    pub fn try_resolve_alias_reference(
        &mut self,
        colref: &ColumnRef,
    ) -> Result<Option<Expression>> {
        // Check if this could be an alias reference (unqualified)
        if colref.schema.is_some() || colref.table.is_some() {
            return Ok(None);
        }

        let column_name = colref.column.name().to_lowercase();
        let original_expr = match self
            .base
            .bind_state
            .resolve_select_alias(&column_name, self.current_index)?
        {
            Some(expr) => expr,
            None => return Ok(None),
        };

        self.base.bind(original_expr).map(Some)
    }

    /// Returns true if the column alias exists.
    pub fn does_column_alias_exist(&self, colref: &ColumnRef) -> bool {
        if colref.schema.is_some() || colref.table.is_some() {
            return false;
        }
        self.base.bind_state.has_alias(colref.column.name())
    }

    /// Get SQL Value Function, overriding to skip if alias exists.
    ///
    pub fn get_sql_value_function(&self, column_name: &str) -> Option<Expr> {
        if self.base.bind_state.has_alias(column_name) {
            // Don't replace SQL value functions if they are in the alias map
            return None;
        }
        ExpressionBinder::get_sql_value_function(&self.base.base, column_name)
    }
}
