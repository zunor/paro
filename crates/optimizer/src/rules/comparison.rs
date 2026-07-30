// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Simplify comparison expressions.
//!
//! Supported rewrites include:
//! - Value-reference comparison with NULL returns NULL (except IS DISTINCT FROM)
//! - `x IS DISTINCT FROM x` → `false` for value references
//! - `x IS NOT DISTINCT FROM x` → `true` for value references
//! - `NOT (x > y)` → `x <= y`
//! - `NOT (x >= y)` → `x < y`
//! - `NOT (x < y)` → `x >= y`
//! - `NOT (x <= y)` → `x > y`
//! - `NOT (x = y)` → `x <> y`
//! - `NOT (x <> y)` → `x = y`

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::OperatorType;
use paro_planner::expression::{ComparisonExpression, ComparisonType};
use paro_planner::expression::{ConstantExpression, Expression};
use paro_planner::operator::LogicalOperator;

use super::expression_matcher::ExpressionMatcher;
use super::rule::{Rule, RuleResult};

/// Matcher for comparison simplification.
/// Matches comparison expressions or NOT expressions containing comparisons.
pub struct ComparisonSimplificationMatcher;

impl ExpressionMatcher for ComparisonSimplificationMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        match expr {
            // Match comparison expressions
            Expression::Comparison(_) => {
                bindings.push(expr);
                true
            }
            // Match NOT expressions containing comparisons
            Expression::Operator(op)
                if op.operator_type == OperatorType::Not && op.children.len() == 1 =>
            {
                if matches!(&op.children[0], Expression::Comparison(_)) {
                    bindings.push(expr);
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

/// Comparison simplification optimization rule.
pub struct ComparisonSimplificationRule {
    matcher: ComparisonSimplificationMatcher,
}

impl ComparisonSimplificationRule {
    pub fn new() -> Self {
        Self {
            matcher: ComparisonSimplificationMatcher,
        }
    }
}

impl Default for ComparisonSimplificationRule {
    fn default() -> Self {
        Self::new()
    }
}

impl Rule for ComparisonSimplificationRule {
    fn matcher(&self) -> &dyn ExpressionMatcher {
        &self.matcher
    }

    fn apply(
        &self,
        _op: &LogicalOperator,
        bindings: Vec<&Expression>,
        _is_root: bool,
    ) -> RuleResult {
        if bindings.is_empty() {
            return RuleResult::NoChange;
        }

        match bindings[0] {
            Expression::Comparison(comp) => simplify_comparison(comp),
            Expression::Operator(op) if op.operator_type == OperatorType::Not => {
                simplify_not_comparison(&op.children[0])
            }
            _ => RuleResult::NoChange,
        }
    }

    fn name(&self) -> &'static str {
        "ComparisonSimplificationRule"
    }
}

/// Simplify a comparison expression.
fn simplify_comparison(comp: &ComparisonExpression) -> RuleResult {
    // Only values without an owned evaluation may be elided. The executor evaluates both sides of
    // a comparison, so folding a function or cast against NULL could suppress side effects or an
    // error.
    let left_is_null = is_null_constant(&comp.left);
    let right_is_null = is_null_constant(&comp.right);

    if (left_is_null || right_is_null)
        && comp.left.is_passive_value()
        && comp.right.is_passive_value()
    {
        // Comparison with NULL returns NULL (except for IS DISTINCT FROM / IS NOT DISTINCT FROM)
        match comp.comparison_type {
            ComparisonType::DistinctFrom | ComparisonType::NotDistinctFrom => {
                // These handle NULL specially, don't simplify here
                RuleResult::NoChange
            }
            _ => {
                // Regular comparison with NULL returns NULL
                RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
                    value: Value::Null(LogicalType::Boolean),
                    return_type: LogicalType::Boolean,
                })))
            }
        }
    } else if is_null_safe_self_comparison(comp) {
        simplify_self_comparison(comp.comparison_type)
    } else {
        RuleResult::NoChange
    }
}

/// Simplify NOT (comparison) by inverting the comparison.
fn simplify_not_comparison(inner: &Expression) -> RuleResult {
    let Expression::Comparison(comp) = inner else {
        return RuleResult::NoChange;
    };

    // Invert the comparison type
    let inverted_type = match comp.comparison_type {
        ComparisonType::Equal => ComparisonType::NotEqual,
        ComparisonType::NotEqual => ComparisonType::Equal,
        ComparisonType::LessThan => ComparisonType::GreaterThanOrEqual,
        ComparisonType::LessThanOrEqual => ComparisonType::GreaterThan,
        ComparisonType::GreaterThan => ComparisonType::LessThanOrEqual,
        ComparisonType::GreaterThanOrEqual => ComparisonType::LessThan,
        // IS DISTINCT FROM and IS NOT DISTINCT FROM are trickier with NOT
        // NOT (x IS DISTINCT FROM y) = x IS NOT DISTINCT FROM y
        ComparisonType::DistinctFrom => ComparisonType::NotDistinctFrom,
        ComparisonType::NotDistinctFrom => ComparisonType::DistinctFrom,
    };

    RuleResult::Changed(Box::new(Expression::Comparison(ComparisonExpression::new(
        inverted_type,
        (*comp.left).clone(),
        (*comp.right).clone(),
    ))))
}

/// Simplify self-comparison (x op x).
fn simplify_self_comparison(comp_type: ComparisonType) -> RuleResult {
    let result = match comp_type {
        ComparisonType::DistinctFrom => Some(false),
        ComparisonType::NotDistinctFrom => Some(true),
        _ => None,
    };

    match result {
        Some(value) => RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
            value: Value::Boolean(value),
            return_type: LogicalType::Boolean,
        }))),
        None => RuleResult::NoChange,
    }
}

/// Check if an expression is a NULL constant.
fn is_null_constant(expr: &Expression) -> bool {
    matches!(expr, Expression::Constant(c) if c.value.is_null())
}

/// Structural equality is sufficient only for null-safe comparisons of values that do not own an
/// evaluation. Function and composite expressions may be volatile, side-effecting, or erroring;
/// folding those would incorrectly remove both evaluations.
fn is_null_safe_self_comparison(comp: &ComparisonExpression) -> bool {
    matches!(
        comp.comparison_type,
        ComparisonType::DistinctFrom | ComparisonType::NotDistinctFrom
    ) && comp.left.is_passive_value()
        && comp.left.equals(&comp.right)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::expression_matcher::ExpressionMatcher;
    use paro_planner::expression::ColumnRefExpression;
    use paro_planner::expression::FunctionExpression;
    use paro_planner::expression::OperatorExpression;

    fn make_constant(value: i32) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Integer(value),
            return_type: LogicalType::Integer,
        })
    }

    fn make_null_constant() -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Null(LogicalType::Integer),
            return_type: LogicalType::Integer,
        })
    }

    fn make_column_ref(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression {
            binding: paro_planner::operator::ColumnBinding {
                table_index,
                column_index,
            },
            depth: 0,
            return_type: LogicalType::Integer,
        })
    }

    fn make_comparison(
        comp_type: ComparisonType,
        left: Expression,
        right: Expression,
    ) -> Expression {
        Expression::Comparison(ComparisonExpression::new(comp_type, left, right))
    }

    fn make_not(child: Expression) -> Expression {
        Expression::Operator(OperatorExpression::new_unary(
            OperatorType::Not,
            child,
            LogicalType::Boolean,
        ))
    }

    fn make_volatile_call() -> Expression {
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .expect("random overload");
        Expression::Function(FunctionExpression::new(
            function,
            vec![],
            LogicalType::Double,
        ))
    }

    #[test]
    fn test_matcher_matches_comparison() {
        let matcher = ComparisonSimplificationMatcher;
        let expr = make_comparison(ComparisonType::Equal, make_constant(1), make_constant(2));
        let mut bindings = Vec::new();
        assert!(matcher.matches(&expr, &mut bindings));
    }

    #[test]
    fn test_matcher_matches_not_comparison() {
        let matcher = ComparisonSimplificationMatcher;
        let comp = make_comparison(ComparisonType::Equal, make_constant(1), make_constant(2));
        let expr = make_not(comp);
        let mut bindings = Vec::new();
        assert!(matcher.matches(&expr, &mut bindings));
    }

    #[test]
    fn test_comparison_with_null() {
        let rule = ComparisonSimplificationRule::new();

        // x = NULL → NULL
        let expr = make_comparison(
            ComparisonType::Equal,
            make_constant(5),
            make_null_constant(),
        );
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert!(c.value.is_null());
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result with NULL"),
        }
    }

    #[test]
    fn test_null_comparison_does_not_elide_volatile_evaluation() {
        let rule = ComparisonSimplificationRule::new();
        let expr = make_comparison(
            ComparisonType::Equal,
            make_volatile_call(),
            Expression::Constant(ConstantExpression {
                value: Value::Null(LogicalType::Double),
                return_type: LogicalType::Double,
            }),
        );
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let result = rule.apply(&LogicalOperator::DummyScan, bindings, false);

        assert!(matches!(result, RuleResult::NoChange));
    }

    #[test]
    fn test_distinct_from_with_null_no_change() {
        let rule = ComparisonSimplificationRule::new();

        // x IS DISTINCT FROM NULL should NOT be simplified to NULL
        let expr = make_comparison(
            ComparisonType::DistinctFrom,
            make_constant(5),
            make_null_constant(),
        );
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        assert!(matches!(result, RuleResult::NoChange));
    }

    #[test]
    fn test_nullable_self_comparison_equal_is_not_simplified() {
        let rule = ComparisonSimplificationRule::new();

        // Column nullability is not encoded in the bound expression, so x = x
        // must retain SQL's UNKNOWN result when x is NULL.
        let col = make_column_ref(0, 0);
        let expr = make_comparison(ComparisonType::Equal, col.clone(), col);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        assert!(matches!(result, RuleResult::NoChange));
    }

    #[test]
    fn test_nullable_self_comparison_not_equal_is_not_simplified() {
        let rule = ComparisonSimplificationRule::new();

        let col = make_column_ref(0, 0);
        let expr = make_comparison(ComparisonType::NotEqual, col.clone(), col);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        assert!(matches!(result, RuleResult::NoChange));
    }

    #[test]
    fn test_nullable_self_comparison_less_than_is_not_simplified() {
        let rule = ComparisonSimplificationRule::new();

        let col = make_column_ref(0, 0);
        let expr = make_comparison(ComparisonType::LessThan, col.clone(), col);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        assert!(matches!(result, RuleResult::NoChange));
    }

    #[test]
    fn test_null_safe_self_comparison_is_simplified() {
        let rule = ComparisonSimplificationRule::new();
        let col = make_column_ref(0, 0);
        let expr = make_comparison(ComparisonType::DistinctFrom, col.clone(), col);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let result = rule.apply(&LogicalOperator::DummyScan, bindings, false);

        let RuleResult::Changed(expr) = result else {
            panic!("expected null-safe self comparison to be simplified");
        };
        let Expression::Constant(constant) = *expr else {
            panic!("expected constant result");
        };
        assert_eq!(constant.value, Value::Boolean(false));
    }

    #[test]
    fn test_volatile_self_comparison_is_not_simplified() {
        let rule = ComparisonSimplificationRule::new();
        let call = make_volatile_call();
        let expr = make_comparison(ComparisonType::DistinctFrom, call.clone(), call);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let result = rule.apply(&LogicalOperator::DummyScan, bindings, false);

        assert!(matches!(result, RuleResult::NoChange));
    }

    #[test]
    fn test_not_greater_than() {
        let rule = ComparisonSimplificationRule::new();

        // NOT (x > y) → x <= y
        let comp = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_column_ref(0, 1),
        );
        let expr = make_not(comp);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Comparison(c) => {
                    assert_eq!(c.comparison_type, ComparisonType::LessThanOrEqual);
                }
                _ => panic!("Expected Comparison"),
            },
            _ => panic!("Expected Changed result with LessThanOrEqual comparison"),
        }
    }

    #[test]
    fn test_not_equal() {
        let rule = ComparisonSimplificationRule::new();

        // NOT (x = y) → x <> y
        let comp = make_comparison(
            ComparisonType::Equal,
            make_column_ref(0, 0),
            make_column_ref(0, 1),
        );
        let expr = make_not(comp);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Comparison(c) => {
                    assert_eq!(c.comparison_type, ComparisonType::NotEqual);
                }
                _ => panic!("Expected Comparison"),
            },
            _ => panic!("Expected Changed result with NotEqual comparison"),
        }
    }

    #[test]
    fn test_not_less_than() {
        let rule = ComparisonSimplificationRule::new();

        // NOT (x < y) → x >= y
        let comp = make_comparison(
            ComparisonType::LessThan,
            make_column_ref(0, 0),
            make_column_ref(0, 1),
        );
        let expr = make_not(comp);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Comparison(c) => {
                    assert_eq!(c.comparison_type, ComparisonType::GreaterThanOrEqual);
                }
                _ => panic!("Expected Comparison"),
            },
            _ => panic!("Expected Changed result with GreaterThanOrEqual comparison"),
        }
    }

    #[test]
    fn test_rule_name() {
        let rule = ComparisonSimplificationRule::new();
        assert_eq!(rule.name(), "ComparisonSimplificationRule");
    }
}
