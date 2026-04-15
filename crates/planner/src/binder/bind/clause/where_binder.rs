// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use crate::binder::bind::expr::ExpressionBinder;
use crate::expression::{CastExpression, Expression};
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_parser::ast::{ColumnRef, Expr};
use paro_parser::ExprRewriter;

use super::{AliasLookup, SelectBindState};

/// Binder for WHERE clause expressions.
///
pub struct WhereBinder<'a> {
    pub base: ExpressionBinder<'a>,
    pub alias_lookup: Option<AliasLookup>,
    pub bind_state: Option<&'a mut SelectBindState>,
    visited_select_indexes: HashSet<usize>,
}

impl<'a> WhereBinder<'a> {
    /// Create a new WhereBinder.
    pub fn new(binder: &'a mut crate::binder::Binder) -> Self {
        let mut base = ExpressionBinder::new(binder);
        base.target_type = LogicalType::Boolean;
        base.allow_aggregates = false;
        base.allow_window = false;
        base.allow_default = false;

        Self {
            base,
            alias_lookup: None,
            bind_state: None,
            visited_select_indexes: HashSet::new(),
        }
    }

    /// Create a new WhereBinder with column alias support.
    pub fn with_alias_lookup(
        binder: &'a mut crate::binder::Binder,
        alias_lookup: AliasLookup,
        bind_state: &'a mut SelectBindState,
    ) -> Self {
        let mut base = ExpressionBinder::new(binder);
        base.target_type = LogicalType::Boolean;
        base.allow_aggregates = false;
        base.allow_window = false;
        base.allow_default = false;

        Self {
            base,
            alias_lookup: Some(alias_lookup),
            bind_state: Some(bind_state),
            visited_select_indexes: HashSet::new(),
        }
    }

    /// Bind the WHERE clause.
    pub fn bind(&mut self, expr: Expr) -> Result<Expression> {
        let mut bound_expr = self.bind_expression(expr)?;

        // WHERE clause must be BOOLEAN - ExpressionBinder::bind already handles target_type casting
        // but let's be explicit if needed, although ExpressionBinder::bind is usually called.
        if bound_expr.return_type() != LogicalType::Boolean {
            bound_expr = CastExpression::add_cast_if_needed(
                bound_expr,
                LogicalType::Boolean,
                &self.base.binder.cast_functions,
            )?;
        }

        Ok(bound_expr)
    }

    /// Bind an expression, handling specialized WHERE list types.
    pub fn bind_expression(&mut self, expr: Expr) -> Result<Expression> {
        let mut expr = expr;
        self.rewrite_alias_references(&mut expr)?;
        self.base.bind_expression(expr)
    }

    fn rewrite_alias_references(&mut self, expr: &mut Expr) -> Result<()> {
        let mut error = None;
        let mut rewriter = ExprRewriter::new(|expr: &mut Expr| {
            if error.is_some() {
                return;
            }

            let Expr::ColumnRef { column, .. } = expr else {
                return;
            };

            if let Some(result) = self.try_resolve_alias_reference(column) {
                match result {
                    Ok(rewritten) => *expr = rewritten,
                    Err(err) => error = Some(err),
                }
            }
        });
        rewriter.visit(expr);

        if let Some(err) = error {
            return Err(err);
        }

        Ok(())
    }

    /// Try to resolve an alias reference into its original AST expression.
    pub fn try_resolve_alias_reference(&mut self, colref: &ColumnRef) -> Option<Result<Expr>> {
        if !ExpressionBinder::is_potential_alias(colref) {
            return None;
        }

        let alias_lookup = self.alias_lookup.as_ref()?;
        let alias_name = colref.column.name().to_string();
        let (index, mut original_expr) = match alias_lookup.resolve_alias(&alias_name) {
            Ok(Some(value)) => value,
            Ok(None) => return None,
            Err(err) => return Some(Err(err)),
        };
        if self.visited_select_indexes.contains(&index) {
            return Some(Err(paro_common::error::syntax(format!(
                "Circular reference to alias \"{}\"",
                alias_name
            ))));
        }

        if let Some(bind_state) = self.bind_state.as_mut() {
            bind_state.mark_alias_referenced(index);
        }

        self.visited_select_indexes.insert(index);
        let result = self.rewrite_alias_references(&mut original_expr);
        self.visited_select_indexes.remove(&index);
        Some(result.map(|_| original_expr))
    }
}
