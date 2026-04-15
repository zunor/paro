// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Aggregate Binder
//!
//!
//!
//! The AggregateBinder is responsible for binding aggregate function arguments.
//! It prevents nested aggregates and window functions within aggregate expressions.

use crate::binder::bind::expr::ExpressionBinder;
use crate::expression::Expression;
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::Expr;

/// Binder for aggregate function arguments.
///
///
/// The AggregateBinder handles:
/// - Binding arguments to aggregate functions
/// - Preventing nested aggregate functions
/// - Preventing window functions inside aggregates
///
/// which means it uses a special binding mode where certain expressions
/// are replaced rather than fully bound. In paro, we achieve similar behavior
/// by restricting what can be bound.
pub struct AggregateBinder<'a> {
    /// The base expression binder.
    pub base: ExpressionBinder<'a>,
}

impl<'a> AggregateBinder<'a> {
    /// Create a new AggregateBinder.
    ///
    ///
    /// `AggregateBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context, true)`
    ///
    /// The third parameter `true` sets `replace_binder=true`, which changes
    /// how some expressions are handled. In paro, we achieve this by:
    /// - Disabling aggregate functions (to prevent nesting)
    /// - Disabling window functions
    pub fn new(binder: &'a mut crate::binder::Binder) -> Self {
        let mut base = ExpressionBinder::new(binder);
        // Prevent nested aggregates
        base.allow_aggregates = false;
        // Prevent window functions inside aggregates
        base.allow_window = false;

        Self { base }
    }

    /// Bind an expression within an aggregate function.
    ///
    ///
    /// This method handles special cases for aggregate function arguments:
    /// - Window functions are not allowed (throws error)
    /// - Nested aggregates are caught by the base binder's `allow_aggregates=false`
    pub fn bind(&mut self, expr: Expr) -> Result<Expression> {
        self.bind_expression(expr, 0, false)
    }

    /// Bind an expression at a specific depth.
    ///
    fn bind_expression(
        &mut self,
        expr: Expr,
        _depth: usize,
        _root_expression: bool,
    ) -> Result<Expression> {
        // Check for window functions
        match &expr {
            Expr::FunctionCall { func, .. } if func.window.is_some() => {
                return Err(paro_error::syntax(
                    "aggregate function calls cannot contain window function calls",
                ));
            }
            Expr::CountAll {
                window: Some(_), ..
            } => {
                return Err(paro_error::syntax(
                    "aggregate function calls cannot contain window function calls",
                ));
            }
            _ => {}
        }

        // Use the base binder for all other expressions
        // Note: allow_aggregates=false will catch nested aggregates
        self.base.bind(expr)
    }

    /// Returns the error message for unsupported (nested) aggregates.
    ///
    pub fn unsupported_aggregate_message() -> &'static str {
        "aggregate function calls cannot be nested"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unsupported_aggregate_message() {
        assert_eq!(
            AggregateBinder::unsupported_aggregate_message(),
            "aggregate function calls cannot be nested"
        );
    }
}
