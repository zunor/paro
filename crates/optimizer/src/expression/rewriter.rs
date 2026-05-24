// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Apply expression-level rewrite rules to a logical plan.

use std::ops::ControlFlow;

use paro_planner::expression::Expression;
use paro_planner::operator::LogicalOperator;
use paro_planner::plan::LogicalPlan;

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

        match op {
            LogicalOperator::Filter(filter) => {
                let new_exprs: Vec<_> = filter
                    .expressions
                    .iter()
                    .map(|expr| self.rewrite_expression(expr.clone(), &rule_ctx))
                    .collect();
                filter.expressions = new_exprs;
            }
            LogicalOperator::Projection(proj) => {
                let new_exprs: Vec<_> = proj
                    .expressions
                    .iter()
                    .map(|expr| self.rewrite_expression(expr.clone(), &rule_ctx))
                    .collect();
                proj.expressions = new_exprs;
            }
            LogicalOperator::ExternalProject(project) => {
                for expr in &mut project.expressions {
                    expr.expression = self.rewrite_expression(expr.expression.clone(), &rule_ctx);
                }
            }
            LogicalOperator::ExternalTable(table) => {
                table.call_expression =
                    self.rewrite_expression(table.call_expression.clone(), &rule_ctx);
            }
            LogicalOperator::Aggregate(agg) => {
                let new_groups: Vec<_> = agg
                    .groups
                    .iter()
                    .map(|expr| self.rewrite_expression(expr.clone(), &rule_ctx))
                    .collect();
                let new_aggs: Vec<_> = agg
                    .aggregates
                    .iter()
                    .map(|expr| self.rewrite_expression(expr.clone(), &rule_ctx))
                    .collect();
                agg.groups = new_groups;
                agg.aggregates = new_aggs;
                agg.recompute_returned_types();
            }
            LogicalOperator::Join(join) => {
                use paro_planner::operator::Join;
                match join {
                    Join::Comparison(cj) => {
                        for cond in &mut cj.conditions {
                            cond.left = self.rewrite_expression(cond.left.clone(), &rule_ctx);
                            cond.right = self.rewrite_expression(cond.right.clone(), &rule_ctx);
                        }
                    }
                    Join::Any(aj) => {
                        aj.condition = self.rewrite_expression(aj.condition.clone(), &rule_ctx);
                    }
                    Join::Cross(_) => {}
                }
            }
            LogicalOperator::Order(order) => {
                for order_by in &mut order.orders {
                    order_by.expression =
                        self.rewrite_expression(order_by.expression.clone(), &rule_ctx);
                }
            }
            LogicalOperator::TopN(topn) => {
                for order_by in &mut topn.orders {
                    order_by.expression =
                        self.rewrite_expression(order_by.expression.clone(), &rule_ctx);
                }
            }
            LogicalOperator::Window(window) => {
                // Window expressions are WindowExpression, not Expression
                // We need to rewrite the children inside each window expression
                for window_expr in &mut window.expressions {
                    let new_children: Vec<_> = window_expr
                        .children
                        .iter()
                        .map(|c| self.rewrite_expression(c.clone(), &rule_ctx))
                        .collect();
                    window_expr.children = new_children;

                    let new_partitions: Vec<_> = window_expr
                        .partitions
                        .iter()
                        .map(|p| self.rewrite_expression(p.clone(), &rule_ctx))
                        .collect();
                    window_expr.partitions = new_partitions;

                    for order in &mut window_expr.orders {
                        order.expression =
                            self.rewrite_expression(order.expression.clone(), &rule_ctx);
                    }
                }
            }
            LogicalOperator::EmptyResult(_) => {}
            LogicalOperator::Update(update) => {
                let new_exprs: Vec<_> = update
                    .expressions
                    .iter()
                    .map(|expr| self.rewrite_expression(expr.clone(), &rule_ctx))
                    .collect();
                update.expressions = new_exprs;
            }
            LogicalOperator::SearchScan(search) => {
                search.projections = search
                    .projections
                    .iter()
                    .map(|expr| self.rewrite_expression(expr.clone(), &rule_ctx))
                    .collect();
                search.absorbed_predicates = search
                    .absorbed_predicates
                    .iter()
                    .map(|expr| self.rewrite_expression(expr.clone(), &rule_ctx))
                    .collect();
                search.residual_predicates = search
                    .residual_predicates
                    .iter()
                    .map(|expr| self.rewrite_expression(expr.clone(), &rule_ctx))
                    .collect();
                search.score_expression =
                    self.rewrite_expression(search.score_expression.clone(), &rule_ctx);
            }
            LogicalOperator::FullTextFilterScan(scan) => {
                scan.match_expression =
                    self.rewrite_expression(scan.match_expression.clone(), &rule_ctx);
                scan.other_predicates = scan
                    .other_predicates
                    .iter()
                    .map(|expr| self.rewrite_expression(expr.clone(), &rule_ctx))
                    .collect();
                scan.residual_predicates = scan
                    .residual_predicates
                    .iter()
                    .map(|expr| self.rewrite_expression(expr.clone(), &rule_ctx))
                    .collect();
            }
            // Operators without expressions to rewrite
            LogicalOperator::Get(_)
            | LogicalOperator::Limit(_)
            | LogicalOperator::ExpressionGet(_)
            | LogicalOperator::DelimGet(_)
            | LogicalOperator::DependentJoin(_)
            | LogicalOperator::SetOperation(_)
            | LogicalOperator::Distinct(_)
            | LogicalOperator::Explain(_)
            | LogicalOperator::MaterializedCTE(_)
            | LogicalOperator::RecursiveCTE(_)
            | LogicalOperator::CTERef(_)
            | LogicalOperator::TableFunctionGet(_)
            | LogicalOperator::Insert(_)
            | LogicalOperator::Delete(_)
            | LogicalOperator::CopyTo(_)
            | LogicalOperator::Alter(_)
            | LogicalOperator::CreateTable(_)
            | LogicalOperator::CreateRoutine(_)
            | LogicalOperator::CreateSequence(_)
            | LogicalOperator::CreateSchema(_)
            | LogicalOperator::CreateIndex(_)
            | LogicalOperator::CreateView(_)
            | LogicalOperator::CreatePropertyGraph(_)
            | LogicalOperator::DropPropertyGraph(_)
            | LogicalOperator::RefreshPropertyGraph(_)
            | LogicalOperator::Drop(_)
            | LogicalOperator::GraphMatch(_)
            | LogicalOperator::GraphScan(_)
            | LogicalOperator::GraphExpand(_)
            | LogicalOperator::DummyScan => {}
        }
    }

    /// Rewrite an expression by applying rules until fixed point.
    fn rewrite_expression(&self, mut expr: Expression, op: &LogicalOperator) -> Expression {
        loop {
            let mut changes_made = false;
            expr = self.apply_rules(expr, op, &mut changes_made, true);
            if !changes_made {
                break;
            }
        }
        expr
    }

    /// Apply rules to an expression and its children.
    fn apply_rules(
        &self,
        expr: Expression,
        op: &LogicalOperator,
        changes_made: &mut bool,
        is_root: bool,
    ) -> Expression {
        // Try to apply each rule to this expression
        let current_expr = expr;

        for rule in &self.rules {
            let mut bindings = Vec::new();
            if rule.matcher().matches(&current_expr, &mut bindings) {
                // Rule matches, try to apply it
                match rule.apply(op, bindings, is_root) {
                    RuleResult::Changed(new_expr) => {
                        *changes_made = true;
                        // Re-run rules on the new expression
                        return self.apply_rules(*new_expr, op, changes_made, is_root);
                    }
                    RuleResult::Rerun => {
                        *changes_made = true;
                        // Re-run rules on the same expression
                        return current_expr;
                    }
                    RuleResult::NoChange => {
                        // Continue to next rule
                    }
                }
            }
        }

        // No rule applied, recursively process children
        self.apply_rules_to_children(current_expr, op, changes_made)
    }

    /// Apply rules to children of an expression.
    fn apply_rules_to_children(
        &self,
        expr: Expression,
        op: &LogicalOperator,
        changes_made: &mut bool,
    ) -> Expression {
        match expr {
            Expression::Function(mut func) => {
                for child in &mut func.children {
                    *child = self.apply_rules(child.clone(), op, changes_made, false);
                }
                Expression::Function(func)
            }
            Expression::Cast(mut cast) => {
                *cast.child = self.apply_rules(*cast.child, op, changes_made, false);
                Expression::Cast(cast)
            }
            Expression::Conjunction(mut conj) => {
                for child in &mut conj.children {
                    *child = self.apply_rules(child.clone(), op, changes_made, false);
                }
                Expression::Conjunction(conj)
            }
            Expression::Case(mut case) => {
                *case.check = self.apply_rules(*case.check, op, changes_made, false);
                *case.result_if_true =
                    self.apply_rules(*case.result_if_true, op, changes_made, false);
                *case.result_if_false =
                    self.apply_rules(*case.result_if_false, op, changes_made, false);
                Expression::Case(case)
            }
            Expression::Comparison(mut comp) => {
                *comp.left = self.apply_rules(*comp.left, op, changes_made, false);
                *comp.right = self.apply_rules(*comp.right, op, changes_made, false);
                Expression::Comparison(comp)
            }
            Expression::Operator(mut op_expr) => {
                for child in &mut op_expr.children {
                    *child = self.apply_rules(child.clone(), op, changes_made, false);
                }
                Expression::Operator(op_expr)
            }
            Expression::Aggregate(mut agg) => {
                for child in &mut agg.children {
                    *child = self.apply_rules(child.clone(), op, changes_made, false);
                }
                if let Some(filter) = &mut agg.filter {
                    **filter = self.apply_rules((**filter).clone(), op, changes_made, false);
                }
                for order in &mut agg.order_bys {
                    order.expression =
                        self.apply_rules(order.expression.clone(), op, changes_made, false);
                }
                Expression::Aggregate(agg)
            }
            Expression::Window(mut window) => {
                for child in &mut window.children {
                    *child = self.apply_rules(child.clone(), op, changes_made, false);
                }
                for partition in &mut window.partitions {
                    *partition = self.apply_rules(partition.clone(), op, changes_made, false);
                }
                for order in &mut window.orders {
                    order.expression =
                        self.apply_rules(order.expression.clone(), op, changes_made, false);
                }
                Expression::Window(window)
            }
            Expression::Subquery(mut subq) => {
                for child in &mut subq.children {
                    *child = self.apply_rules(child.clone(), op, changes_made, false);
                }
                Expression::Subquery(subq)
            }
            // Leaf expressions have no children
            Expression::Constant(_)
            | Expression::Parameter(_)
            | Expression::ColumnRef(_)
            | Expression::Reference(_) => expr,
        }
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
    use crate::rules::expression_matcher::{
        ComparisonExpressionMatcher, ConstantExpressionMatcher, ExpressionMatcher,
        FoldableConstantMatcher,
    };
    use crate::rules::rule::{Rule, RuleResult};
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{ComparisonExpression, ComparisonType, ConstantExpression};
    use paro_planner::operator::{Filter, Projection};

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
        let expr = make_constant(42);
        let op = LogicalOperator::DummyScan;

        let result = rewriter.rewrite_expression(expr.clone(), &op);

        // Without rules, expression should be unchanged
        if let Expression::Constant(c) = result {
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
        let expr = make_constant(99);
        let op = LogicalOperator::DummyScan;

        let result = rewriter.rewrite_expression(expr, &op);

        // Rule should increment 99 to 100
        if let Expression::Constant(c) = result {
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
        let expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            make_constant(98),
            make_constant(99),
        ));
        let op = LogicalOperator::DummyScan;

        let result = rewriter.rewrite_expression(expr, &op);

        // Both constants should be incremented to 100
        if let Expression::Comparison(comp) = result {
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
    fn test_expression_rewriter_self_equality() {
        let mut rewriter = ExpressionRewriter::new();
        rewriter.add_rule(Box::new(SelfEqualityRule::new()));

        // Create comparison: 42 = 42
        let expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            make_constant(42),
            make_constant(42),
        ));
        let op = LogicalOperator::DummyScan;

        let result = rewriter.rewrite_expression(expr, &op);

        // Should be simplified to true
        if let Expression::Constant(c) = result {
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
    fn test_constant_folding_rule() {
        let mut rewriter = ExpressionRewriter::new();
        rewriter.add_rule(Box::new(ConstantComparisonFoldingRule::new()));

        // Create comparison: 42 = 42 (should fold to true)
        let expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            make_constant(42),
            make_constant(42),
        ));
        let op = LogicalOperator::DummyScan;

        let result = rewriter.rewrite_expression(expr, &op);

        // Should be folded to true
        if let Expression::Constant(c) = result {
            assert_eq!(c.value, Value::Boolean(true));
        } else {
            panic!("Expected constant true");
        }
    }
}
