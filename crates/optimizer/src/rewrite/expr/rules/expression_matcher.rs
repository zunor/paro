// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Expression Matcher
//!
//! Pattern matchers for Expression used by optimization rules.
//! Each matcher can match specific expression patterns and collect bindings.
//! Only matchers consumed by the rule program are defined here.

use paro_planner::expression::Expression;

/// Trait for matching expressions.
///
/// Matchers are used by optimization rules to identify expressions
/// that can be transformed.
pub trait ExpressionMatcher {
    /// Check if the expression matches this pattern.
    ///
    /// # Arguments
    /// * `expr` - The expression to match
    /// * `bindings` - Output vector for matched sub-expressions
    ///
    /// # Returns
    /// `true` if the expression matches the pattern
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool;
}

/// Matches any expression.
#[cfg(any(test, feature = "test-support"))]
pub struct AnyExpressionMatcher;

#[cfg(any(test, feature = "test-support"))]
impl ExpressionMatcher for AnyExpressionMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        bindings.push(expr);
        true
    }
}

/// Matches foldable non-constant expressions.
pub struct FoldableConstantMatcher;

impl FoldableConstantMatcher {
    /// Check if an expression is foldable (can be evaluated at compile time).
    pub fn is_foldable(expr: &Expression) -> bool {
        match expr {
            Expression::Constant(_) => true,
            Expression::Function(func) => {
                func.is_foldable_native() && func.children.iter().all(Self::is_foldable)
            }
            Expression::Cast(cast) => Self::is_foldable(&cast.child),
            Expression::Comparison(comp) => {
                Self::is_foldable(&comp.left) && Self::is_foldable(&comp.right)
            }
            Expression::Conjunction(conj) => conj.children.iter().all(Self::is_foldable),
            Expression::Operator(op) => op.children.iter().all(Self::is_foldable),
            Expression::Case(case) => {
                Self::is_foldable(&case.check)
                    && Self::is_foldable(&case.result_if_true)
                    && Self::is_foldable(&case.result_if_false)
            }
            // Column references, runtime parameters, aggregates, subqueries, windows are not foldable.
            Expression::ColumnRef(_)
            | Expression::Parameter(_)
            | Expression::Reference(_)
            | Expression::Aggregate(_)
            | Expression::Subquery(_)
            | Expression::Window(_) => false,
        }
    }
}

impl ExpressionMatcher for FoldableConstantMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        // Don't match pure constants (they're already folded)
        if matches!(expr, Expression::Constant(_)) {
            return false;
        }

        if Self::is_foldable(expr) {
            bindings.push(expr);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::ComparisonExpression;
    use paro_planner::expression::ComparisonType;
    use paro_planner::expression::{ColumnRefExpression, ConstantExpression};

    fn make_constant(value: i32) -> Expression {
        Expression::Constant(
            ConstantExpression {
                value: Value::Integer(value),
                return_type: LogicalType::Integer,
            }
            .into(),
        )
    }

    fn make_column_ref(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression {
                binding: paro_planner::logical::operator::ColumnBinding {
                    table_index,
                    column_index,
                },
                depth: 0,
                return_type: LogicalType::Integer,
            }
            .into(),
        )
    }

    #[test]
    fn test_any_expression_matcher() {
        let matcher = AnyExpressionMatcher;
        let expr = make_constant(42);
        let mut bindings = Vec::new();

        assert!(matcher.matches(&expr, &mut bindings));
        assert_eq!(bindings.len(), 1);
    }

    #[test]
    fn test_foldable_constant_matcher() {
        let matcher = FoldableConstantMatcher;

        // Pure constant - should NOT match (already folded)
        let const_expr = make_constant(42);
        let mut bindings = Vec::new();
        assert!(!matcher.matches(&const_expr, &mut bindings));

        // Column ref - should NOT match (not foldable)
        let col_expr = make_column_ref(0, 0);
        assert!(!matcher.matches(&col_expr, &mut bindings));

        // Comparison of two constants - should match (foldable but not a constant)
        let comp_expr = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::Equal, make_constant(1), make_constant(2))
                .into(),
        );
        assert!(matcher.matches(&comp_expr, &mut bindings));
        assert_eq!(bindings.len(), 1);
    }
}
