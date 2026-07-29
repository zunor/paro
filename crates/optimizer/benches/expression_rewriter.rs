// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Measures canonical traversal of nested expression trees.

use divan::Bencher;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_optimizer::expression::rewriter::ExpressionRewriter;
use paro_optimizer::rules::expression_matcher::{AnyExpressionMatcher, ExpressionMatcher};
use paro_optimizer::rules::rule::{Rule, RuleResult};
use paro_planner::expression::{
    ConjunctionExpression, ConjunctionType, ConstantExpression, Expression,
};
use paro_planner::operator::{LogicalOperator, Projection};
use paro_planner::plan::LogicalPlan;

const DEPTH: usize = 10;

fn main() {
    divan::main();
}

#[divan::bench(sample_count = 100)]
fn rewrite_balanced_expression_without_changes(bencher: Bencher) {
    let mut plan = LogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        0,
        LogicalPlan::synthetic(LogicalOperator::DummyScan),
        vec![nested_conjunction(DEPTH)],
    )));
    let mut rewriter = ExpressionRewriter::new();
    rewriter.add_rule(Box::new(NoChangeRule));

    bencher.bench_local(|| {
        rewriter.rewrite_plan(divan::black_box(&mut plan));
    });
}

struct NoChangeRule;

impl Rule for NoChangeRule {
    fn matcher(&self) -> &dyn ExpressionMatcher {
        &AnyExpressionMatcher
    }

    fn apply(
        &self,
        _op: &LogicalOperator,
        _bindings: Vec<&Expression>,
        _is_root: bool,
    ) -> RuleResult {
        RuleResult::NoChange
    }
}

fn nested_conjunction(depth: usize) -> Expression {
    if depth == 0 {
        return Expression::Constant(ConstantExpression::new(
            Value::Boolean(true),
            LogicalType::Boolean,
        ));
    }

    Expression::Conjunction(ConjunctionExpression::new(
        ConjunctionType::And,
        vec![nested_conjunction(depth - 1), nested_conjunction(depth - 1)],
    ))
}
