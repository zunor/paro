// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Apply expression-level rewrite rules to a logical plan.

use std::ops::ControlFlow;

use paro_planner::expression::{Expression, ExpressionIterator};
use paro_planner::operator::LogicalOperator;
use paro_planner::plan::LogicalPlan;
use paro_planner::visitor::enumerate_expressions;

use crate::rules::rule::{Rule, RuleResult};

/// The ExpressionRewriter applies optimization rules to expressions.
///
/// It traverses a logical plan and applies a set of rules to each expression.
/// Rules are applied repeatedly until no more changes are made (fixed point).
pub struct ExpressionRewriter {
    /// The set of rules to apply.
    rules: Vec<Box<dyn Rule>>,
}

impl ExpressionRewriter {
    /// Create a new ExpressionRewriter with no rules.
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Add a rule to the rewriter.
    pub fn add_rule(&mut self, rule: Box<dyn Rule>) {
        self.rules.push(rule);
    }

    /// Rewrite every operator/expression in a logical plan.
    pub fn rewrite_plan(&mut self, plan: &mut LogicalPlan) {
        self.visit_logical_plan(plan);
    }

    /// Visit a logical operator and rewrite all expressions in it.
    fn visit_operator(&mut self, op: &mut LogicalOperator) {
        // First visit children
        self.visit_operator_children(op);

        // Then rewrite expressions in this operator
        self.visit_operator_expressions(op);
    }

    fn visit_logical_plan(&mut self, plan: &mut LogicalPlan) {
        self.visit_operator(&mut plan.operator);
    }

    /// Visit children of a logical operator.
    fn visit_operator_children(&mut self, op: &mut LogicalOperator) {
        let _ = op.visit_children_mut(|child| {
            self.visit_logical_plan(child);
            ControlFlow::Continue(())
        });
    }

    /// Visit and rewrite expressions in a logical operator.
    fn visit_operator_expressions(&mut self, op: &mut LogicalOperator) {
        // Expression rules do not inspect the surrounding operator today; use a stable leaf
        // for the `Rule::apply` context slot so we never clone a full [`LogicalOperator`].
        let rule_ctx = LogicalOperator::DummyScan;

        enumerate_expressions(op, |expr| {
            self.rewrite_expression(expr, &rule_ctx);
        });

        // Aggregate output types are derived from their rewritten roots.
        if let LogicalOperator::Aggregate(aggregate) = op {
            aggregate.recompute_returned_types();
        }
    }

    /// Rewrite an expression by applying rules until fixed point.
    fn rewrite_expression(&self, expr: &mut Expression, op: &LogicalOperator) {
        loop {
            let mut changes_made = false;
            self.apply_rules(expr, op, &mut changes_made, true);
            if !changes_made {
                break;
            }
        }
    }

    /// Apply rules to an expression and its children.
    fn apply_rules(
        &self,
        expr: &mut Expression,
        op: &LogicalOperator,
        changes_made: &mut bool,
        is_root: bool,
    ) {
        // Try to apply each rule to this expression
        for rule in &self.rules {
            let mut bindings = Vec::new();
            if rule.matcher().matches(expr, &mut bindings) {
                // Rule matches, try to apply it
                match rule.apply(op, bindings, is_root) {
                    RuleResult::Changed(new_expr) => {
                        *changes_made = true;
                        // Re-run rules on the new expression
                        *expr = *new_expr;
                        self.apply_rules(expr, op, changes_made, is_root);
                        return;
                    }
                    RuleResult::Rerun => {
                        *changes_made = true;
                        // Re-run rules on the same expression
                        return;
                    }
                    RuleResult::NoChange => {
                        // Continue to next rule
                    }
                }
            }
        }

        // No rule applied, recursively process children
        ExpressionIterator::enumerate_children_mut(expr, |child| {
            self.apply_rules(child, op, changes_made, false);
        });
    }
}

impl Default for ExpressionRewriter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::constant_folding::ConstantFoldingRule;
    use crate::rules::expression_matcher::{
        ComparisonExpressionMatcher, ConstantExpressionMatcher, ExpressionMatcher,
        FoldableConstantMatcher,
    };
    use crate::rules::rule::{Rule, RuleResult};
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::window::WindowFunction;
    use paro_planner::expression::{
        ComparisonExpression, ComparisonType, ConstantExpression, WindowExpression, WindowFrame,
        WindowFrameBound, WindowFrameType,
    };
    use paro_planner::operator::{Distinct, ExpressionGet, Filter, Limit, Projection};

    /// A test rule that adds 1 to integer constants less than 100.
    /// This prevents infinite loops by only applying to small values.
    struct IncrementSmallConstantRule {
        matcher: ConstantExpressionMatcher,
    }

    impl IncrementSmallConstantRule {
        fn new() -> Self {
            Self {
                matcher: ConstantExpressionMatcher,
            }
        }
    }

    impl Rule for IncrementSmallConstantRule {
        fn matcher(&self) -> &dyn ExpressionMatcher {
            &self.matcher
        }

        fn apply(
            &self,
            _op: &LogicalOperator,
            bindings: Vec<&Expression>,
            _is_root: bool,
        ) -> RuleResult {
            if let Expression::Constant(c) = bindings[0] {
                if let Value::Integer(v) = &c.value {
                    // Only increment if less than 100 to prevent infinite loop
                    if *v < 100 {
                        return RuleResult::Changed(Box::new(Expression::Constant(
                            ConstantExpression {
                                value: Value::Integer(v + 1),
                                return_type: LogicalType::Integer,
                            },
                        )));
                    }
                }
            }
            RuleResult::NoChange
        }
    }

    /// A test rule that simplifies `x = x` to `true`.
    struct SelfEqualityRule {
        matcher: ComparisonExpressionMatcher,
    }

    impl SelfEqualityRule {
        fn new() -> Self {
            Self {
                matcher: ComparisonExpressionMatcher::with_type(ComparisonType::Equal),
            }
        }
    }

    impl Rule for SelfEqualityRule {
        fn matcher(&self) -> &dyn ExpressionMatcher {
            &self.matcher
        }

        fn apply(
            &self,
            _op: &LogicalOperator,
            bindings: Vec<&Expression>,
            _is_root: bool,
        ) -> RuleResult {
            if let Expression::Comparison(comp) = bindings[0] {
                // Check if left == right (simplified check for constants)
                if comp.left.equals(&comp.right) {
                    return RuleResult::Changed(Box::new(Expression::Constant(
                        ConstantExpression {
                            value: Value::Boolean(true),
                            return_type: LogicalType::Boolean,
                        },
                    )));
                }
            }
            RuleResult::NoChange
        }
    }

    /// A test rule that folds constant comparisons.
    struct ConstantComparisonFoldingRule {
        matcher: FoldableConstantMatcher,
    }

    impl ConstantComparisonFoldingRule {
        fn new() -> Self {
            Self {
                matcher: FoldableConstantMatcher,
            }
        }
    }

    impl Rule for ConstantComparisonFoldingRule {
        fn matcher(&self) -> &dyn ExpressionMatcher {
            &self.matcher
        }

        fn apply(
            &self,
            _op: &LogicalOperator,
            bindings: Vec<&Expression>,
            _is_root: bool,
        ) -> RuleResult {
            // For testing, just fold comparisons of equal constants to true
            if let Expression::Comparison(comp) = bindings[0] {
                if let (Expression::Constant(left), Expression::Constant(right)) =
                    (&*comp.left, &*comp.right)
                {
                    if comp.comparison_type == ComparisonType::Equal && left.value == right.value {
                        return RuleResult::Changed(Box::new(Expression::Constant(
                            ConstantExpression {
                                value: Value::Boolean(true),
                                return_type: LogicalType::Boolean,
                            },
                        )));
                    }
                }
            }
            RuleResult::NoChange
        }
    }

    fn make_constant(value: i32) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Integer(value),
            return_type: LogicalType::Integer,
        })
    }

    #[test]
    fn test_expression_rewriter_no_rules() {
        let rewriter = ExpressionRewriter::new();
        let mut expr = make_constant(42);
        let op = LogicalOperator::DummyScan;

        rewriter.rewrite_expression(&mut expr, &op);

        // Without rules, expression should be unchanged
        if let Expression::Constant(c) = expr {
            assert_eq!(c.value, Value::Integer(42));
        } else {
            panic!("Expected constant expression");
        }
    }

    #[test]
    fn test_expression_rewriter_with_rule() {
        let mut rewriter = ExpressionRewriter::new();
        rewriter.add_rule(Box::new(IncrementSmallConstantRule::new()));

        // Start with 99, should increment to 100 and stop
        let mut expr = make_constant(99);
        let op = LogicalOperator::DummyScan;

        rewriter.rewrite_expression(&mut expr, &op);

        // Rule should increment 99 to 100
        if let Expression::Constant(c) = expr {
            assert_eq!(c.value, Value::Integer(100));
        } else {
            panic!("Expected constant expression");
        }
    }

    #[test]
    fn test_expression_rewriter_recursive() {
        let mut rewriter = ExpressionRewriter::new();
        rewriter.add_rule(Box::new(IncrementSmallConstantRule::new()));

        // Create comparison: 98 = 99
        let mut expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            make_constant(98),
            make_constant(99),
        ));
        let op = LogicalOperator::DummyScan;

        rewriter.rewrite_expression(&mut expr, &op);

        // Both constants should be incremented to 100
        if let Expression::Comparison(comp) = expr {
            if let Expression::Constant(left) = &*comp.left {
                assert_eq!(left.value, Value::Integer(100));
            } else {
                panic!("Expected constant on left");
            }
            if let Expression::Constant(right) = &*comp.right {
                assert_eq!(right.value, Value::Integer(100));
            } else {
                panic!("Expected constant on right");
            }
        } else {
            panic!("Expected comparison expression");
        }
    }

    #[test]
    fn test_expression_rewriter_visits_window_frame_offsets() {
        let mut rewriter = ExpressionRewriter::new();
        rewriter.add_rule(Box::new(IncrementSmallConstantRule::new()));

        let mut expr = Expression::Window(WindowExpression::native(
            WindowFunction::row_number(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            WindowFrame {
                frame_type: WindowFrameType::Rows,
                start_bound: WindowFrameBound::Offset(Box::new(make_constant(98))),
                start_is_preceding: true,
                end_bound: WindowFrameBound::Offset(Box::new(make_constant(99))),
                end_is_preceding: false,
            },
            false,
        ));

        rewriter.rewrite_expression(&mut expr, &LogicalOperator::DummyScan);
        let Expression::Window(window) = expr else {
            panic!("expected window expression");
        };
        let WindowFrameBound::Offset(start) = &window.frame.start_bound else {
            panic!("expected start offset");
        };
        let WindowFrameBound::Offset(end) = &window.frame.end_bound else {
            panic!("expected end offset");
        };
        assert_constant_integer(Some(start), 100);
        assert_constant_integer(Some(end), 100);
    }

    #[test]
    fn test_expression_rewriter_self_equality() {
        let mut rewriter = ExpressionRewriter::new();
        rewriter.add_rule(Box::new(SelfEqualityRule::new()));

        // Create comparison: 42 = 42
        let mut expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            make_constant(42),
            make_constant(42),
        ));
        let op = LogicalOperator::DummyScan;

        rewriter.rewrite_expression(&mut expr, &op);

        // Should be simplified to true
        if let Expression::Constant(c) = expr {
            assert_eq!(c.value, Value::Boolean(true));
        } else {
            panic!("Expected constant true");
        }
    }

    #[test]
    fn test_visit_operator_filter() {
        let mut rewriter = ExpressionRewriter::new();
        rewriter.add_rule(Box::new(IncrementSmallConstantRule::new()));

        // Create filter with expressions: [99 = 99]
        let condition = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            make_constant(99),
            make_constant(99),
        ));

        let mut op = LogicalOperator::Filter(Filter::new(
            LogicalPlan::synthetic(LogicalOperator::DummyScan),
            vec![condition],
        ));

        rewriter.visit_operator(&mut op);

        // Check that constants were incremented to 100
        if let LogicalOperator::Filter(filter) = op {
            assert_eq!(filter.expressions.len(), 1);
            if let Expression::Comparison(comp) = &filter.expressions[0] {
                if let Expression::Constant(left) = &*comp.left {
                    assert_eq!(left.value, Value::Integer(100));
                }
                if let Expression::Constant(right) = &*comp.right {
                    assert_eq!(right.value, Value::Integer(100));
                }
            }
        }
    }

    #[test]
    fn test_visit_operator_projection() {
        let mut rewriter = ExpressionRewriter::new();
        rewriter.add_rule(Box::new(IncrementSmallConstantRule::new()));

        // Create projection with expressions: [98, 99]
        let mut op = LogicalOperator::Projection(Projection::new(
            0,
            LogicalPlan::synthetic(LogicalOperator::DummyScan),
            vec![make_constant(98), make_constant(99)],
        ));

        rewriter.visit_operator(&mut op);

        // Check that constants were incremented to 100
        if let LogicalOperator::Projection(proj) = op {
            assert_eq!(proj.expressions.len(), 2);
            if let Expression::Constant(c) = &proj.expressions[0] {
                assert_eq!(c.value, Value::Integer(100));
            }
            if let Expression::Constant(c) = &proj.expressions[1] {
                assert_eq!(c.value, Value::Integer(100));
            }
        }
    }

    #[test]
    fn test_visit_operator_expression_get() {
        let mut rewriter = ExpressionRewriter::new();
        rewriter.add_rule(Box::new(ConstantFoldingRule::new()));

        let foldable = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            make_constant(42),
            make_constant(42),
        ));
        let mut op = LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![vec![foldable]],
            vec!["col0".to_string()],
            vec![LogicalType::Boolean],
        ));

        rewriter.visit_operator(&mut op);

        let LogicalOperator::ExpressionGet(values) = op else {
            panic!("expected expression get");
        };
        let Expression::Constant(constant) = &values.expressions[0][0] else {
            panic!("expected foldable VALUES root to be rewritten");
        };
        assert_eq!(constant.value, Value::Boolean(true));
    }

    #[test]
    fn test_visit_canonical_operator_expression_roots() {
        let mut rewriter = ExpressionRewriter::new();
        rewriter.add_rule(Box::new(IncrementSmallConstantRule::new()));

        let values = LogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![vec![make_constant(97)]],
            vec!["col0".to_string()],
            vec![LogicalType::Integer],
        )));
        let distinct = LogicalPlan::synthetic(LogicalOperator::Distinct(Distinct::distinct_on(
            vec![make_constant(98)],
            values,
        )));
        let mut op = LogicalOperator::Limit(Limit::new(
            distinct,
            Some(make_constant(99)),
            Some(make_constant(99)),
        ));

        rewriter.visit_operator(&mut op);

        let LogicalOperator::Limit(limit) = op else {
            panic!("expected limit");
        };
        assert_constant_integer(limit.limit.as_ref(), 100);
        assert_constant_integer(limit.offset.as_ref(), 100);

        let LogicalOperator::Distinct(distinct) = &limit.child.operator else {
            panic!("expected distinct");
        };
        assert_constant_integer(distinct.distinct_targets.first(), 100);

        let LogicalOperator::ExpressionGet(values) = &distinct.child.operator else {
            panic!("expected expression get");
        };
        assert_constant_integer(values.expressions[0].first(), 100);
    }

    fn assert_constant_integer(expr: Option<&Expression>, expected: i32) {
        let Some(Expression::Constant(constant)) = expr else {
            panic!("expected integer constant");
        };
        assert_eq!(constant.value, Value::Integer(expected));
    }

    #[test]
    fn test_constant_folding_rule() {
        let mut rewriter = ExpressionRewriter::new();
        rewriter.add_rule(Box::new(ConstantComparisonFoldingRule::new()));

        // Create comparison: 42 = 42 (should fold to true)
        let mut expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            make_constant(42),
            make_constant(42),
        ));
        let op = LogicalOperator::DummyScan;

        rewriter.rewrite_expression(&mut expr, &op);

        // Should be folded to true
        if let Expression::Constant(c) = expr {
            assert_eq!(c.value, Value::Boolean(true));
        } else {
            panic!("Expected constant true");
        }
    }
}
