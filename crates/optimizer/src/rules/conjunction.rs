//! Simplify conjunctions with constant boolean inputs.
//!
//! Supported rewrites include:
//! - `x AND true` → `x`
//! - `x AND false` → `false`
//! - `x OR true` → `true`
//! - `x OR false` → `x`

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::{ConjunctionExpression, ConjunctionType};
use paro_planner::expression::{ConstantExpression, Expression};
use paro_planner::operator::LogicalOperator;

use super::expression_matcher::ExpressionMatcher;
use super::rule::{Rule, RuleResult};

/// Matches conjunction expressions with at least one constant boolean child.
pub struct ConjunctionSimplificationMatcher;

impl ExpressionMatcher for ConjunctionSimplificationMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        let Expression::Conjunction(conj) = expr else {
            return false;
        };

        // Check if any child is a boolean constant
        let has_bool_constant = conj.children.iter().any(|c| {
            matches!(
                c,
                Expression::Constant(ConstantExpression {
                    value: Value::Boolean(_),
                    ..
                })
            )
        });

        if has_bool_constant {
            bindings.push(expr);
            true
        } else {
            false
        }
    }
}

/// Conjunction simplification optimization rule.
pub struct ConjunctionSimplificationRule {
    matcher: ConjunctionSimplificationMatcher,
}

impl ConjunctionSimplificationRule {
    pub fn new() -> Self {
        Self {
            matcher: ConjunctionSimplificationMatcher,
        }
    }
}

impl Default for ConjunctionSimplificationRule {
    fn default() -> Self {
        Self::new()
    }
}

impl Rule for ConjunctionSimplificationRule {
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

        let Expression::Conjunction(conj) = bindings[0] else {
            return RuleResult::NoChange;
        };

        match conj.conjunction_type {
            ConjunctionType::And => simplify_and(conj),
            ConjunctionType::Or => simplify_or(conj),
        }
    }

    fn name(&self) -> &'static str {
        "ConjunctionSimplificationRule"
    }
}

/// Simplify AND conjunction.
fn simplify_and(conj: &ConjunctionExpression) -> RuleResult {
    let mut remaining_children: Vec<Expression> = Vec::new();

    for child in &conj.children {
        match child {
            Expression::Constant(ConstantExpression {
                value: Value::Boolean(false),
                ..
            }) => {
                // FALSE in AND → entire expression is FALSE
                return RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
                    value: Value::Boolean(false),
                    return_type: LogicalType::Boolean,
                })));
            }
            Expression::Constant(ConstantExpression {
                value: Value::Boolean(true),
                ..
            }) => {
                // TRUE in AND → skip this child (remove it)
            }
            _ => {
                // Keep non-constant children
                remaining_children.push(child.clone());
            }
        }
    }

    build_result(remaining_children, ConjunctionType::And, true)
}

/// Simplify OR conjunction.
fn simplify_or(conj: &ConjunctionExpression) -> RuleResult {
    let mut remaining_children: Vec<Expression> = Vec::new();

    for child in &conj.children {
        match child {
            Expression::Constant(ConstantExpression {
                value: Value::Boolean(true),
                ..
            }) => {
                // TRUE in OR → entire expression is TRUE
                return RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
                    value: Value::Boolean(true),
                    return_type: LogicalType::Boolean,
                })));
            }
            Expression::Constant(ConstantExpression {
                value: Value::Boolean(false),
                ..
            }) => {
                // FALSE in OR → skip this child (remove it)
            }
            _ => {
                // Keep non-constant children
                remaining_children.push(child.clone());
            }
        }
    }

    build_result(remaining_children, ConjunctionType::Or, false)
}

/// Build the result expression from remaining children.
fn build_result(
    remaining: Vec<Expression>,
    conj_type: ConjunctionType,
    default_value: bool,
) -> RuleResult {
    match remaining.len() {
        0 => {
            // All children were constants and removed
            // AND with all TRUE → TRUE, OR with all FALSE → FALSE
            RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
                value: Value::Boolean(default_value),
                return_type: LogicalType::Boolean,
            })))
        }
        1 => {
            // Only one child remaining, return it directly
            RuleResult::Changed(Box::new(remaining.into_iter().next().unwrap()))
        }
        _ => {
            // Multiple children remaining, rebuild conjunction
            RuleResult::Changed(Box::new(Expression::Conjunction(ConjunctionExpression {
                conjunction_type: conj_type,
                children: remaining,
            })))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::expression_matcher::ExpressionMatcher;
    use paro_planner::expression::ColumnRefExpression;

    fn make_bool_constant(value: bool) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Boolean(value),
            return_type: LogicalType::Boolean,
        })
    }

    fn make_column_ref(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression {
            binding: paro_planner::operator::ColumnBinding {
                table_index,
                column_index,
            },
            depth: 0,
            return_type: LogicalType::Boolean,
        })
    }

    fn make_and(children: Vec<Expression>) -> Expression {
        Expression::Conjunction(ConjunctionExpression {
            conjunction_type: ConjunctionType::And,
            children,
        })
    }

    fn make_or(children: Vec<Expression>) -> Expression {
        Expression::Conjunction(ConjunctionExpression {
            conjunction_type: ConjunctionType::Or,
            children,
        })
    }

    #[test]
    fn test_matcher_matches_and_with_constant() {
        let matcher = ConjunctionSimplificationMatcher;
        let expr = make_and(vec![make_column_ref(0, 0), make_bool_constant(true)]);
        let mut bindings = Vec::new();
        assert!(matcher.matches(&expr, &mut bindings));
    }

    #[test]
    fn test_matcher_rejects_and_without_constant() {
        let matcher = ConjunctionSimplificationMatcher;
        let expr = make_and(vec![make_column_ref(0, 0), make_column_ref(0, 1)]);
        let mut bindings = Vec::new();
        assert!(!matcher.matches(&expr, &mut bindings));
    }

    #[test]
    fn test_and_with_true() {
        let rule = ConjunctionSimplificationRule::new();

        // x AND true → x
        let col = make_column_ref(0, 0);
        let expr = make_and(vec![col.clone(), make_bool_constant(true)]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::ColumnRef(c) => {
                    assert_eq!(c.binding.table_index, 0);
                    assert_eq!(c.binding.column_index, 0);
                }
                _ => panic!("Expected ColumnRef"),
            },
            _ => panic!("Expected Changed result with column ref"),
        }
    }

    #[test]
    fn test_and_with_false() {
        let rule = ConjunctionSimplificationRule::new();

        // x AND false → false
        let expr = make_and(vec![make_column_ref(0, 0), make_bool_constant(false)]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert_eq!(c.value, Value::Boolean(false));
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result with false"),
        }
    }

    #[test]
    fn test_or_with_true() {
        let rule = ConjunctionSimplificationRule::new();

        // x OR true → true
        let expr = make_or(vec![make_column_ref(0, 0), make_bool_constant(true)]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert_eq!(c.value, Value::Boolean(true));
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result with true"),
        }
    }

    #[test]
    fn test_or_with_false() {
        let rule = ConjunctionSimplificationRule::new();

        // x OR false → x
        let col = make_column_ref(0, 0);
        let expr = make_or(vec![col.clone(), make_bool_constant(false)]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::ColumnRef(c) => {
                    assert_eq!(c.binding.table_index, 0);
                    assert_eq!(c.binding.column_index, 0);
                }
                _ => panic!("Expected ColumnRef"),
            },
            _ => panic!("Expected Changed result with column ref"),
        }
    }

    #[test]
    fn test_and_all_true() {
        let rule = ConjunctionSimplificationRule::new();

        // true AND true → true
        let expr = make_and(vec![make_bool_constant(true), make_bool_constant(true)]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert_eq!(c.value, Value::Boolean(true));
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result with true"),
        }
    }

    #[test]
    fn test_or_all_false() {
        let rule = ConjunctionSimplificationRule::new();

        // false OR false → false
        let expr = make_or(vec![make_bool_constant(false), make_bool_constant(false)]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert_eq!(c.value, Value::Boolean(false));
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result with false"),
        }
    }

    #[test]
    fn test_and_multiple_with_true() {
        let rule = ConjunctionSimplificationRule::new();

        // x AND y AND true → x AND y
        let expr = make_and(vec![
            make_column_ref(0, 0),
            make_column_ref(0, 1),
            make_bool_constant(true),
        ]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Conjunction(c) => {
                    assert_eq!(c.conjunction_type, ConjunctionType::And);
                    assert_eq!(c.children.len(), 2);
                }
                _ => panic!("Expected Conjunction"),
            },
            _ => panic!("Expected Changed result with conjunction"),
        }
    }

    #[test]
    fn test_or_multiple_with_false() {
        let rule = ConjunctionSimplificationRule::new();

        // x OR y OR false → x OR y
        let expr = make_or(vec![
            make_column_ref(0, 0),
            make_column_ref(0, 1),
            make_bool_constant(false),
        ]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Conjunction(c) => {
                    assert_eq!(c.conjunction_type, ConjunctionType::Or);
                    assert_eq!(c.children.len(), 2);
                }
                _ => panic!("Expected Conjunction"),
            },
            _ => panic!("Expected Changed result with conjunction"),
        }
    }

    #[test]
    fn test_rule_name() {
        let rule = ConjunctionSimplificationRule::new();
        assert_eq!(rule.name(), "ConjunctionSimplificationRule");
    }
}
