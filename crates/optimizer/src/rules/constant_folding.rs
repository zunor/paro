// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Fold bound, context-independent expressions at rewrite time.

use paro_planner::expression::{ConstantExpression, Expression};
use paro_planner::operator::LogicalOperator;

use super::constant_evaluator::evaluate_constant;
use super::expression_matcher::{ExpressionMatcher, FoldableConstantMatcher};
use super::rule::{Rule, RuleResult};

/// Matches foldable expressions but excludes values that are already constants.
pub struct ConstantFoldingMatcher;

impl ExpressionMatcher for ConstantFoldingMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        if matches!(expr, Expression::Constant(_)) {
            return false;
        }
        FoldableConstantMatcher.matches(expr, bindings)
    }
}

/// Evaluates immutable native expression trees through their bound execution
/// kernels and replaces them with typed constants.
pub struct ConstantFoldingRule {
    matcher: ConstantFoldingMatcher,
}

impl ConstantFoldingRule {
    pub fn new() -> Self {
        Self {
            matcher: ConstantFoldingMatcher,
        }
    }
}

impl Default for ConstantFoldingRule {
    fn default() -> Self {
        Self::new()
    }
}

impl Rule for ConstantFoldingRule {
    fn matcher(&self) -> &dyn ExpressionMatcher {
        &self.matcher
    }

    fn apply(
        &self,
        _op: &LogicalOperator,
        bindings: Vec<&Expression>,
        _is_root: bool,
    ) -> RuleResult {
        let Some(expr) = bindings.first() else {
            return RuleResult::NoChange;
        };
        let Some(value) = evaluate_constant(expr) else {
            return RuleResult::NoChange;
        };
        let return_type = value.logical_type();
        RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
            value,
            return_type,
        })))
    }

    fn name(&self) -> &'static str {
        "ConstantFoldingRule"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ComparisonExpression, ComparisonType, ConjunctionExpression, ConjunctionType,
    };

    fn constant(value: Value) -> Expression {
        let return_type = value.logical_type();
        Expression::Constant(ConstantExpression { value, return_type })
    }

    #[test]
    fn matcher_excludes_constants_and_accepts_foldable_trees() {
        let matcher = ConstantFoldingMatcher;
        let literal = constant(Value::Integer(42));
        assert!(!matcher.matches(&literal, &mut Vec::new()));

        let comparison = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            constant(Value::Integer(1)),
            constant(Value::Integer(2)),
        ));
        assert!(matcher.matches(&comparison, &mut Vec::new()));
    }

    #[test]
    fn rule_folds_comparison_with_sql_null_semantics() {
        let rule = ConstantFoldingRule::new();
        let comparison = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            constant(Value::Integer(1)),
            constant(Value::Integer(1)),
        ));
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&comparison, &mut bindings));
        let RuleResult::Changed(result) = rule.apply(&LogicalOperator::DummyScan, bindings, false)
        else {
            panic!("expected folded comparison")
        };
        let Expression::Constant(result) = *result else {
            panic!("expected constant")
        };
        assert_eq!(result.value, Value::Boolean(true));

        let null_comparison = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            constant(Value::Integer(1)),
            constant(Value::Null(LogicalType::Integer)),
        ));
        assert!(matches!(
            evaluate_constant(&null_comparison),
            Some(Value::Null(LogicalType::Boolean))
        ));
    }

    #[test]
    fn rule_folds_boolean_conjunction() {
        let expression = Expression::Conjunction(ConjunctionExpression {
            conjunction_type: ConjunctionType::And,
            children: vec![
                constant(Value::Boolean(true)),
                constant(Value::Boolean(false)),
            ],
        });
        assert_eq!(evaluate_constant(&expression), Some(Value::Boolean(false)));
    }

    #[test]
    fn rule_name_is_stable() {
        assert_eq!(ConstantFoldingRule::new().name(), "ConstantFoldingRule");
    }
}
