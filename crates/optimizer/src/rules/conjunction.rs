// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Simplify conjunctions with constant boolean inputs.
//!
//! Supported rewrites include:
//! - `x AND true` → `x`
//! - `x AND false` → `false` when the other inputs are passive
//! - `x OR true` → `true` when the other inputs are passive
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

/// Matches disjunctions whose branches may share conjuncts.
pub struct CommonConjunctionFactorMatcher;

impl ExpressionMatcher for CommonConjunctionFactorMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        let Expression::Conjunction(conjunction) = expr else {
            return false;
        };
        if conjunction.conjunction_type != ConjunctionType::Or || conjunction.children.len() < 2 {
            return false;
        }
        bindings.push(expr);
        true
    }
}

/// Factor immutable common predicates out of OR branches.
///
/// `(key AND a) OR (key AND b)` becomes `key AND (a OR b)`. Besides reducing duplicate
/// evaluation, this exposes equi-join predicates to filter pushdown and join ordering.
pub struct CommonConjunctionFactorRule {
    matcher: CommonConjunctionFactorMatcher,
}

impl CommonConjunctionFactorRule {
    pub fn new() -> Self {
        Self {
            matcher: CommonConjunctionFactorMatcher,
        }
    }
}

impl Default for CommonConjunctionFactorRule {
    fn default() -> Self {
        Self::new()
    }
}

impl Rule for CommonConjunctionFactorRule {
    fn matcher(&self) -> &dyn ExpressionMatcher {
        &self.matcher
    }

    fn apply(
        &self,
        _op: &LogicalOperator,
        bindings: Vec<&Expression>,
        _is_root: bool,
    ) -> RuleResult {
        let Some(Expression::Conjunction(disjunction)) = bindings.first().copied() else {
            return RuleResult::NoChange;
        };

        // Factoring changes evaluation count and placement. Keep expressions that own an
        // evaluation boundary, side effect, volatile call, or subquery in their original tree.
        if disjunction
            .children
            .iter()
            .any(|branch| branch.evaluation_properties().is_reorder_fence())
        {
            return RuleResult::NoChange;
        }

        factor_common_conjuncts(disjunction)
            .map(|expression| RuleResult::Changed(Box::new(expression)))
            .unwrap_or(RuleResult::NoChange)
    }

    fn name(&self) -> &'static str {
        "CommonConjunctionFactorRule"
    }
}

fn factor_common_conjuncts(disjunction: &ConjunctionExpression) -> Option<Expression> {
    let branches = associative_children(disjunction, ConjunctionType::Or);
    let branch_factors = branches.into_iter().map(conjuncts).collect::<Vec<_>>();
    let first_branch = branch_factors.first()?;

    let mut common = Vec::new();
    for candidate in first_branch {
        if common
            .iter()
            .any(|existing: &&Expression| existing.equals(candidate))
        {
            continue;
        }
        if branch_factors
            .iter()
            .skip(1)
            .all(|branch| branch.iter().any(|factor| factor.equals(candidate)))
        {
            common.push(*candidate);
        }
    }
    if common.is_empty() {
        return None;
    }

    let mut residual_branches = Vec::with_capacity(branch_factors.len());
    for branch in branch_factors {
        let mut residual = branch.into_iter().cloned().collect::<Vec<_>>();
        for common_factor in &common {
            let index = residual
                .iter()
                .position(|factor| factor.equals(common_factor))
                .expect("common factor must exist in every OR branch");
            residual.remove(index);
        }

        // `common OR (common AND x)` is exactly `common`, including under SQL three-valued
        // boolean semantics.
        if residual.is_empty() {
            return Some(build_conjunction(
                common.into_iter().cloned().collect(),
                ConjunctionType::And,
            ));
        }
        residual_branches.push(build_conjunction(residual, ConjunctionType::And));
    }

    let residual = build_conjunction(residual_branches, ConjunctionType::Or);
    let mut result = common.into_iter().cloned().collect::<Vec<_>>();
    result.push(residual);
    Some(build_conjunction(result, ConjunctionType::And))
}

fn conjuncts(expression: &Expression) -> Vec<&Expression> {
    let mut result = Vec::new();
    collect_associative(expression, ConjunctionType::And, &mut result);
    result
}

fn associative_children(
    conjunction: &ConjunctionExpression,
    conjunction_type: ConjunctionType,
) -> Vec<&Expression> {
    let mut result = Vec::new();
    for child in &conjunction.children {
        collect_associative(child, conjunction_type, &mut result);
    }
    result
}

fn collect_associative<'a>(
    expression: &'a Expression,
    conjunction_type: ConjunctionType,
    result: &mut Vec<&'a Expression>,
) {
    if let Expression::Conjunction(conjunction) = expression {
        if conjunction.conjunction_type == conjunction_type {
            for child in &conjunction.children {
                collect_associative(child, conjunction_type, result);
            }
            return;
        }
    }
    result.push(expression);
}

fn build_conjunction(
    mut children: Vec<Expression>,
    conjunction_type: ConjunctionType,
) -> Expression {
    if children.len() == 1 {
        return children.pop().expect("single conjunction child");
    }
    Expression::Conjunction(ConjunctionExpression::new(conjunction_type, children))
}

/// Simplify AND conjunction.
fn simplify_and(conj: &ConjunctionExpression) -> RuleResult {
    if conj.children.iter().any(|child| {
        matches!(
            child,
            Expression::Constant(ConstantExpression {
                value: Value::Boolean(false),
                ..
            })
        )
    }) && conj.children.iter().all(Expression::is_passive_value)
    {
        return RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
            value: Value::Boolean(false),
            return_type: LogicalType::Boolean,
        })));
    }

    let mut remaining_children: Vec<Expression> = Vec::new();
    let mut removed_identity = false;

    for child in &conj.children {
        match child {
            Expression::Constant(ConstantExpression {
                value: Value::Boolean(false),
                ..
            }) => {
                // Keep the absorbing value when another input owns an evaluation.
                remaining_children.push(child.clone());
            }
            Expression::Constant(ConstantExpression {
                value: Value::Boolean(true),
                ..
            }) => {
                // TRUE in AND → skip this child (remove it)
                removed_identity = true;
            }
            _ => {
                // Keep non-constant children
                remaining_children.push(child.clone());
            }
        }
    }

    if !removed_identity {
        return RuleResult::NoChange;
    }
    build_result(remaining_children, ConjunctionType::And, true)
}

/// Simplify OR conjunction.
fn simplify_or(conj: &ConjunctionExpression) -> RuleResult {
    if conj.children.iter().any(|child| {
        matches!(
            child,
            Expression::Constant(ConstantExpression {
                value: Value::Boolean(true),
                ..
            })
        )
    }) && conj.children.iter().all(Expression::is_passive_value)
    {
        return RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
            value: Value::Boolean(true),
            return_type: LogicalType::Boolean,
        })));
    }

    let mut remaining_children: Vec<Expression> = Vec::new();
    let mut removed_identity = false;

    for child in &conj.children {
        match child {
            Expression::Constant(ConstantExpression {
                value: Value::Boolean(true),
                ..
            }) => {
                // Keep the absorbing value when another input owns an evaluation.
                remaining_children.push(child.clone());
            }
            Expression::Constant(ConstantExpression {
                value: Value::Boolean(false),
                ..
            }) => {
                // FALSE in OR → skip this child (remove it)
                removed_identity = true;
            }
            _ => {
                // Keep non-constant children
                remaining_children.push(child.clone());
            }
        }
    }

    if !removed_identity {
        return RuleResult::NoChange;
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
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, FunctionExpression,
    };

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

    fn volatile_bool() -> Expression {
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .expect("random overload");
        let random = || {
            Expression::Function(FunctionExpression::new(
                function.clone(),
                vec![],
                LogicalType::Double,
            ))
        };
        Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            random(),
            random(),
        ))
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
    fn test_and_false_preserves_volatile_evaluation() {
        let rule = ConjunctionSimplificationRule::new();
        let expr = make_and(vec![volatile_bool(), make_bool_constant(false)]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let result = rule.apply(&LogicalOperator::DummyScan, bindings, false);

        assert!(matches!(result, RuleResult::NoChange));
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
    fn test_or_true_preserves_volatile_evaluation() {
        let rule = ConjunctionSimplificationRule::new();
        let expr = make_or(vec![volatile_bool(), make_bool_constant(true)]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let result = rule.apply(&LogicalOperator::DummyScan, bindings, false);

        assert!(matches!(result, RuleResult::NoChange));
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
    fn factors_common_predicate_from_or_branches() {
        let rule = CommonConjunctionFactorRule::new();
        let key = make_column_ref(0, 0);
        let branch = |residual: usize| {
            make_and(vec![
                make_and(vec![make_column_ref(0, 4), key.clone()]),
                make_column_ref(0, residual),
            ])
        };
        let expr = make_or(vec![make_or(vec![branch(1), branch(2)]), branch(3)]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let RuleResult::Changed(rewritten) =
            rule.apply(&LogicalOperator::DummyScan, bindings, true)
        else {
            panic!("expected common predicate factoring");
        };
        let Expression::Conjunction(result) = *rewritten else {
            panic!("expected factored AND");
        };
        assert_eq!(result.conjunction_type, ConjunctionType::And);
        assert_eq!(result.children.len(), 3);
        assert!(result.children.iter().any(|child| child.equals(&key)));
        assert!(matches!(
            result.children.last(),
            Some(Expression::Conjunction(residual))
                if residual.conjunction_type == ConjunctionType::Or
                    && residual.children.len() == 3
        ));
    }

    #[test]
    fn factoring_applies_boolean_absorption() {
        let rule = CommonConjunctionFactorRule::new();
        let key = make_column_ref(0, 0);
        let expr = make_or(vec![
            key.clone(),
            make_and(vec![key.clone(), make_column_ref(0, 1)]),
        ]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let RuleResult::Changed(rewritten) =
            rule.apply(&LogicalOperator::DummyScan, bindings, true)
        else {
            panic!("expected boolean absorption");
        };
        assert!(rewritten.equals(&key));
    }

    #[test]
    fn common_volatile_predicate_is_not_factored() {
        let rule = CommonConjunctionFactorRule::new();
        let volatile = volatile_bool();
        let expr = make_or(vec![
            make_and(vec![volatile.clone(), make_column_ref(0, 1)]),
            make_and(vec![volatile, make_column_ref(0, 2)]),
        ]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        assert!(matches!(
            rule.apply(&LogicalOperator::DummyScan, bindings, true),
            RuleResult::NoChange
        ));
    }

    #[test]
    fn factoring_does_not_move_a_predicate_across_volatile_residuals() {
        let rule = CommonConjunctionFactorRule::new();
        let key = make_column_ref(0, 0);
        let expr = make_or(vec![
            make_and(vec![key.clone(), volatile_bool()]),
            make_and(vec![key, make_column_ref(0, 2)]),
        ]);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        assert!(matches!(
            rule.apply(&LogicalOperator::DummyScan, bindings, true),
            RuleResult::NoChange
        ));
    }

    #[test]
    fn test_rule_name() {
        let rule = ConjunctionSimplificationRule::new();
        assert_eq!(rule.name(), "ConjunctionSimplificationRule");
    }
}
