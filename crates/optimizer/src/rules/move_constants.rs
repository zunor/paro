// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Move constants into a canonical position in expressions.
//!
//! Supported rewrites include:
//! - In commutative operations (+, *): move constant to right side
//!   - `5 + x` → `x + 5`
//!   - `3 * x` → `x * 3`
//! - In comparisons: move constant to right side
//!   - `3 = x` → `x = 3`
//!   - `5 < x` → `x > 5` (flip comparison)
//!
//! This normalization helps other rules match patterns more easily.

use paro_planner::expression::Expression;
use paro_planner::expression::{ComparisonExpression, ComparisonType};
use paro_planner::operator::LogicalOperator;

use super::expression_matcher::ExpressionMatcher;
use super::rule::{Rule, RuleResult};

/// Matches expressions where a constant appears on the left side.
pub struct MoveConstantsMatcher;

impl ExpressionMatcher for MoveConstantsMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        match expr {
            // Match comparisons with constant on left
            Expression::Comparison(comp) => {
                if matches!(&*comp.left, Expression::Constant(_))
                    && !matches!(&*comp.right, Expression::Constant(_))
                {
                    bindings.push(expr);
                    return true;
                }
                false
            }
            // Match commutative functions (+, *) with constant on left
            Expression::Function(func) => {
                let name = func.function.name.as_str();
                if matches!(name, "+" | "*")
                    && func.children.len() == 2
                    && matches!(&func.children[0], Expression::Constant(_))
                    && !matches!(&func.children[1], Expression::Constant(_))
                {
                    bindings.push(expr);
                    return true;
                }
                false
            }
            _ => false,
        }
    }
}

/// Move constants optimization rule.
pub struct MoveConstantsRule {
    matcher: MoveConstantsMatcher,
}

impl MoveConstantsRule {
    pub fn new() -> Self {
        Self {
            matcher: MoveConstantsMatcher,
        }
    }
}

impl Default for MoveConstantsRule {
    fn default() -> Self {
        Self::new()
    }
}

impl Rule for MoveConstantsRule {
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
            Expression::Comparison(comp) => move_comparison_constant(comp),
            Expression::Function(func) => move_function_constant(func),
            _ => RuleResult::NoChange,
        }
    }

    fn name(&self) -> &'static str {
        "MoveConstantsRule"
    }
}

/// Move constant from left to right in comparison, flipping the operator.
fn move_comparison_constant(comp: &ComparisonExpression) -> RuleResult {
    // Flip the comparison type when swapping operands
    let flipped_type = flip_comparison(comp.comparison_type);

    RuleResult::Changed(Box::new(Expression::Comparison(ComparisonExpression::new(
        flipped_type,
        (*comp.right).clone(),
        (*comp.left).clone(),
    ))))
}

/// Flip a comparison type (for swapping operands).
fn flip_comparison(comp_type: ComparisonType) -> ComparisonType {
    match comp_type {
        ComparisonType::Equal => ComparisonType::Equal,
        ComparisonType::NotEqual => ComparisonType::NotEqual,
        ComparisonType::LessThan => ComparisonType::GreaterThan,
        ComparisonType::LessThanOrEqual => ComparisonType::GreaterThanOrEqual,
        ComparisonType::GreaterThan => ComparisonType::LessThan,
        ComparisonType::GreaterThanOrEqual => ComparisonType::LessThanOrEqual,
        ComparisonType::DistinctFrom => ComparisonType::DistinctFrom,
        ComparisonType::NotDistinctFrom => ComparisonType::NotDistinctFrom,
    }
}

/// Move constant from left to right in commutative function (+, *).
fn move_function_constant(func: &paro_planner::expression::FunctionExpression) -> RuleResult {
    use paro_planner::expression::FunctionExpression;

    // Swap children: constant moves to right
    let new_children = vec![func.children[1].clone(), func.children[0].clone()];

    RuleResult::Changed(Box::new(Expression::Function(FunctionExpression {
        function: func.function.clone(),
        children: new_children,
        return_type: func.return_type.clone(),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::expression_matcher::ExpressionMatcher;
    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_function::scalar::{ExpressionState, ScalarFunction};
    use paro_planner::expression::FunctionExpression;
    use paro_planner::expression::{ColumnRefExpression, ConstantExpression};

    fn dummy_fn(
        _input: &Chunk,
        _state: &dyn ExpressionState,
        _result: &mut Vector,
    ) -> paro_common::error::Result<()> {
        Ok(())
    }

    fn make_constant(value: i32) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Integer(value),
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

    fn make_add(left: Expression, right: Expression) -> Expression {
        Expression::Function(FunctionExpression {
            function: ScalarFunction::new(
                "+".to_string(),
                vec![LogicalType::Integer, LogicalType::Integer],
                LogicalType::Integer,
                dummy_fn,
            )
            .into(),
            children: vec![left, right],
            return_type: LogicalType::Integer,
        })
    }

    fn make_multiply(left: Expression, right: Expression) -> Expression {
        Expression::Function(FunctionExpression {
            function: ScalarFunction::new(
                "*".to_string(),
                vec![LogicalType::Integer, LogicalType::Integer],
                LogicalType::Integer,
                dummy_fn,
            )
            .into(),
            children: vec![left, right],
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

    #[test]
    fn test_matcher_matches_add_with_constant_left() {
        let matcher = MoveConstantsMatcher;

        // 5 + x should match
        let expr = make_add(make_constant(5), make_column_ref(0, 0));
        let mut bindings = Vec::new();
        assert!(matcher.matches(&expr, &mut bindings));
    }

    #[test]
    fn test_matcher_rejects_add_with_constant_right() {
        let matcher = MoveConstantsMatcher;

        // x + 5 should NOT match (already in canonical form)
        let expr = make_add(make_column_ref(0, 0), make_constant(5));
        let mut bindings = Vec::new();
        assert!(!matcher.matches(&expr, &mut bindings));
    }

    #[test]
    fn test_matcher_matches_comparison_with_constant_left() {
        let matcher = MoveConstantsMatcher;

        // 3 = x should match
        let expr = make_comparison(
            ComparisonType::Equal,
            make_constant(3),
            make_column_ref(0, 0),
        );
        let mut bindings = Vec::new();
        assert!(matcher.matches(&expr, &mut bindings));
    }

    #[test]
    fn test_matcher_rejects_comparison_with_constant_right() {
        let matcher = MoveConstantsMatcher;

        // x = 3 should NOT match (already in canonical form)
        let expr = make_comparison(
            ComparisonType::Equal,
            make_column_ref(0, 0),
            make_constant(3),
        );
        let mut bindings = Vec::new();
        assert!(!matcher.matches(&expr, &mut bindings));
    }

    #[test]
    fn test_move_add_constant() {
        let rule = MoveConstantsRule::new();

        // 5 + x → x + 5
        let expr = make_add(make_constant(5), make_column_ref(0, 0));
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Function(f) => {
                    assert_eq!(f.function.name, "+");
                    // First child should be column ref
                    assert!(matches!(&f.children[0], Expression::ColumnRef(_)));
                    // Second child should be constant
                    assert!(matches!(&f.children[1], Expression::Constant(_)));
                }
                _ => panic!("Expected Function"),
            },
            _ => panic!("Expected Changed result with function"),
        }
    }

    #[test]
    fn test_move_multiply_constant() {
        let rule = MoveConstantsRule::new();

        // 3 * x → x * 3
        let expr = make_multiply(make_constant(3), make_column_ref(0, 0));
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Function(f) => {
                    assert_eq!(f.function.name, "*");
                    assert!(matches!(&f.children[0], Expression::ColumnRef(_)));
                    assert!(matches!(&f.children[1], Expression::Constant(_)));
                }
                _ => panic!("Expected Function"),
            },
            _ => panic!("Expected Changed result with function"),
        }
    }

    #[test]
    fn test_move_equal_constant() {
        let rule = MoveConstantsRule::new();

        // 3 = x → x = 3
        let expr = make_comparison(
            ComparisonType::Equal,
            make_constant(3),
            make_column_ref(0, 0),
        );
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Comparison(c) => {
                    assert_eq!(c.comparison_type, ComparisonType::Equal);
                    assert!(matches!(&*c.left, Expression::ColumnRef(_)));
                    assert!(matches!(&*c.right, Expression::Constant(_)));
                }
                _ => panic!("Expected Comparison"),
            },
            _ => panic!("Expected Changed result with comparison"),
        }
    }

    #[test]
    fn test_move_less_than_constant_flips() {
        let rule = MoveConstantsRule::new();

        // 5 < x → x > 5
        let expr = make_comparison(
            ComparisonType::LessThan,
            make_constant(5),
            make_column_ref(0, 0),
        );
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Comparison(c) => {
                    // LessThan flips to GreaterThan
                    assert_eq!(c.comparison_type, ComparisonType::GreaterThan);
                    assert!(matches!(&*c.left, Expression::ColumnRef(_)));
                    assert!(matches!(&*c.right, Expression::Constant(_)));
                }
                _ => panic!("Expected Comparison"),
            },
            _ => panic!("Expected Changed result with comparison"),
        }
    }

    #[test]
    fn test_move_greater_than_or_equal_constant_flips() {
        let rule = MoveConstantsRule::new();

        // 5 >= x → x <= 5
        let expr = make_comparison(
            ComparisonType::GreaterThanOrEqual,
            make_constant(5),
            make_column_ref(0, 0),
        );
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Comparison(c) => {
                    // GreaterThanOrEqual flips to LessThanOrEqual
                    assert_eq!(c.comparison_type, ComparisonType::LessThanOrEqual);
                }
                _ => panic!("Expected Comparison"),
            },
            _ => panic!("Expected Changed result with comparison"),
        }
    }

    #[test]
    fn test_rule_name() {
        let rule = MoveConstantsRule::new();
        assert_eq!(rule.name(), "MoveConstantsRule");
    }
}
