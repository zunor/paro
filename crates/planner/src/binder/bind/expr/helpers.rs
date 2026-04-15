// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::dispatcher::ExpressionBinder;
use crate::expression::{Expression, ExpressionIterator};
use paro_common::error::{self as paro_error, ParoError, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::{ColumnRef, Expr, Identifier};
use std::collections::HashSet;

impl ExpressionBinder<'_> {
    /// Check if a column reference is a potential alias.
    pub fn is_potential_alias(colref: &ColumnRef) -> bool {
        if colref.schema.is_none() && colref.table.is_none() {
            return true;
        }
        if colref.schema.is_none() {
            if let Some(ref table) = colref.table {
                return table.name.eq_ignore_ascii_case("alias");
            }
        }
        false
    }

    /// Try to resolve an alias reference.
    pub fn try_resolve_alias_reference(
        &mut self,
        _colref: &ColumnRef,
        _depth: usize,
        _root_expression: bool,
    ) -> Option<Result<Expression>> {
        None
    }

    /// Check if a column alias exists.
    pub fn does_column_alias_exist(&self, _colref: &ColumnRef) -> bool {
        false
    }

    /// Extract correlated expressions from a bound expression.
    pub fn extract_correlated_expressions(binder: &mut crate::binder::Binder, expr: &Expression) {
        if let Expression::ColumnRef(colref) = expr {
            if colref.depth > 0 {
                binder
                    .correlated_columns
                    .push(crate::binder::CorrelatedColumnInfo {
                        table_index: colref.binding.table_index,
                        column_index: colref.binding.column_index,
                        return_type: colref.return_type.clone(),
                        name: String::new(),
                        depth: colref.depth,
                    });
            }
        }

        ExpressionIterator::enumerate_children(expr, |child| {
            Self::extract_correlated_expressions(binder, child);
        });
    }

    /// Entry point for qualifying the column references of the expression.
    pub fn qualify_column_names(binder: &mut crate::binder::Binder, expr: &mut Expr) {
        let mut expression_binder = ExpressionBinder::new(binder);
        let mut lambda_params = Vec::new();
        expression_binder.qualify_column_names_recursive(expr, &mut lambda_params, false);
    }

    /// Recursively qualifies the column references in the expression tree.
    pub fn qualify_column_names_recursive(
        &mut self,
        expr: &mut Expr,
        lambda_params: &mut Vec<HashSet<String>>,
        _within_function_expression: bool,
    ) {
        let mut next_within_function_expression = false;
        match expr {
            Expr::ColumnRef { column, .. } => {
                let column_name = column.column.name();
                for params in lambda_params.iter().rev() {
                    if params.contains(column_name) {
                        return;
                    }
                }

                let mut error = ParoError::default();
                let new_expr = self.qualify_column_name(column, &mut error);

                if let Ok(Some(new_expr)) = new_expr {
                    *expr = new_expr;
                }
                return;
            }
            Expr::FunctionCall { .. } => {
                next_within_function_expression = true;
            }
            _ => {}
        }

        self.visit_expr_children(expr, |binder, child| {
            binder.qualify_column_names_recursive(
                child,
                lambda_params,
                next_within_function_expression,
            );
        });
    }

    fn visit_expr_children<F>(&mut self, expr: &mut Expr, mut f: F)
    where
        F: FnMut(&mut Self, &mut Expr),
    {
        match expr {
            Expr::BinaryOp { left, right, .. } | Expr::JsonOp { left, right, .. } => {
                f(self, left);
                f(self, right);
            }
            Expr::UnaryOp { expr, .. }
            | Expr::IsNull { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::TryCast { expr, .. }
            | Expr::MapAccess { expr, .. }
            | Expr::InSubquery { expr, .. } => f(self, expr),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    f(self, op);
                }
                for cond in conditions {
                    f(self, cond);
                }
                for result in results {
                    f(self, result);
                }
                if let Some(else_expr) = else_result {
                    f(self, else_expr);
                }
            }
            Expr::FunctionCall { func, .. } => {
                for arg in &mut func.args {
                    f(self, arg);
                }
                for order in &mut func.order_by {
                    f(self, &mut order.expr);
                }
                if let Some(filter) = &mut func.filter {
                    f(self, filter);
                }
            }
            Expr::InList { expr, list, .. } => {
                f(self, expr);
                for item in list {
                    f(self, item);
                }
            }
            Expr::IsDistinctFrom { left, right, .. } => {
                f(self, left);
                f(self, right);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                f(self, expr);
                f(self, low);
                f(self, high);
            }
            Expr::Tuple { exprs, .. } | Expr::Array { exprs, .. } => {
                for expr in exprs {
                    f(self, expr);
                }
            }
            _ => {}
        }
    }

    /// Returns a qualified column reference from a column reference.
    pub fn qualify_column_name(
        &mut self,
        colref: &ColumnRef,
        error: &mut ParoError,
    ) -> Result<Option<Expr>> {
        let column_name = colref.column.name();

        if colref.schema.is_none() && colref.table.is_none() {
            if let Some(sql_value_func) = self.get_sql_value_function(column_name) {
                return Ok(Some(sql_value_func));
            }
        }

        if let Some(binding) = self.binder.bind_context.lookup_local_column(
            colref.table.as_ref().map(|table| table.name.as_str()),
            column_name,
        )? {
            return Ok(Some(Expr::ColumnRef {
                span: colref.column.span(),
                column: ColumnRef {
                    schema: None,
                    table: Some(Identifier::from_name(colref.column.span(), binding.alias)),
                    column: colref.column.clone(),
                },
            }));
        }

        *error = paro_error::catalog(format!("Column not found: {}", column_name));
        Ok(None)
    }

    /// Returns the SQL value function for a given column name if it exists.
    pub fn get_sql_value_function(&self, column_name: &str) -> Option<Expr> {
        let func_name = match column_name.to_lowercase().as_str() {
            "current_date" => "current_date",
            "current_time" => "current_time",
            "current_timestamp" | "now" => "current_timestamp",
            "current_user" => "current_user",
            _ => return None,
        };

        let mut func = paro_parser::ast::FunctionCall::default();
        func.name = Identifier::from_name(paro_parser::Span::default(), func_name.to_string());

        Some(Expr::FunctionCall {
            span: paro_parser::Span::default(),
            func,
        })
    }

    /// Check if a type contains a specific type ID.
    pub fn contains_type(logical_type: &LogicalType, target: &LogicalType) -> bool {
        if logical_type == target {
            return true;
        }
        match logical_type {
            LogicalType::List(child) | LogicalType::Array(child, _) => {
                Self::contains_type(child, target)
            }
            LogicalType::Struct(fields) => {
                fields.iter().any(|(_, ty)| Self::contains_type(ty, target))
            }
            _ => false,
        }
    }

    /// Check if a type contains NULL type.
    pub fn contains_null_type(logical_type: &LogicalType) -> bool {
        Self::contains_type(logical_type, &LogicalType::Null)
    }

    /// Exchange NULL type with INTEGER in a type.
    pub fn exchange_null_type(logical_type: &LogicalType) -> LogicalType {
        Self::exchange_type(logical_type, &LogicalType::Null, LogicalType::Integer)
    }

    /// Exchange a specific type with a new type.
    pub fn exchange_type(
        logical_type: &LogicalType,
        target: &LogicalType,
        new_type: LogicalType,
    ) -> LogicalType {
        if logical_type == target {
            return new_type;
        }
        match logical_type {
            LogicalType::List(child) => {
                LogicalType::List(Box::new(Self::exchange_type(child, target, new_type)))
            }
            LogicalType::Array(child, size) => LogicalType::Array(
                Box::new(Self::exchange_type(child, target, new_type)),
                *size,
            ),
            LogicalType::Struct(fields) => LogicalType::Struct(
                fields
                    .iter()
                    .map(|(name, ty)| {
                        (
                            name.clone(),
                            Self::exchange_type(ty, target, new_type.clone()),
                        )
                    })
                    .collect(),
            ),
            _ => logical_type.clone(),
        }
    }
}
