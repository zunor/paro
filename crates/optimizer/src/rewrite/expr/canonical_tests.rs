// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_common::{runtime_value::Value, types::LogicalType};
use paro_planner::expression::{
    ConjunctionExpression, ConjunctionType, ConstantExpression, Expression,
};
use paro_planner::logical::operator::Filter;

fn boolean(value: bool) -> Expression {
    Expression::Constant(
        ConstantExpression::new(Value::Boolean(value), LogicalType::Boolean).into(),
    )
}

#[test]
fn canonical_roots_reuse_only_unchanged_allocations() {
    let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
        vec![Expression::Conjunction(
            ConjunctionExpression::new(ConjunctionType::And, vec![boolean(true), boolean(true)])
                .into(),
        )],
    )));
    let mut construction = CanonicalScalars::default();
    construction.normalize_plan(&mut plan);
    let first = construction.rewrites;
    assert_eq!(first, 1);
    construction.normalize_plan(&mut plan);
    assert_eq!(
        construction.rewrites, first,
        "canonical root must not run its rules again"
    );
    let LogicalOperator::Filter(filter) = &mut plan.operator else {
        unreachable!()
    };
    // Formerly unique payloads also detach because the cache owns a weak
    // witness. An in-place substitution cannot preserve a completed identity.
    let before = filter.expressions[0].allocation_identity();
    let Expression::Constant(value) = &mut filter.expressions[0] else {
        panic!("folded constant")
    };
    value.value = Value::Boolean(false);
    assert_ne!(filter.expressions[0].allocation_identity(), before);
    let mut oracle = paro_planner::binder::deep_copy::duplicate_plan_preserving_indices(
        &plan,
        paro_planner::binder::context::BindContext::new()
            .shared()
            .as_ref(),
    );
    oracle.visit_post_order_mut(|node| node.id = paro_planner::logical::plan::PlanNodeId(0));
    scalar_normalizer().rewrite_plan(&mut oracle);
    construction.normalize_plan(&mut plan);
    assert_eq!(construction.rewrites, first + 1);
    assert_eq!(format!("{plan:?}"), format!("{oracle:?}"));
    assert!(!construction.completed.get(&before).unwrap().is_alive());
}

#[test]
fn scalar_reuse_does_not_merge_volatile_occurrences() {
    use paro_function::scalar::math::get_random_function;
    use paro_planner::expression::FunctionExpression;
    let function = get_random_function().functions[0].clone();
    let expression =
        Expression::Function(FunctionExpression::new(function, vec![], LogicalType::Double).into());
    let mut operator = LogicalOperator::Filter(Filter {
        child: (),
        expressions: vec![expression.clone(), expression],
        projection_map: paro_planner::logical::operator::ProjectionMap::all(),
    });
    let mut oracle = operator.clone();
    scalar_normalizer().visit_operator_expressions(&mut oracle);
    let mut construction = CanonicalScalars::default();
    construction.normalize_operator(&mut operator);
    construction.normalize_operator(&mut operator);
    assert_eq!(format!("{operator:?}"), format!("{oracle:?}"));
    let LogicalOperator::Filter(filter) = operator else {
        unreachable!()
    };
    assert_eq!(filter.expressions.len(), 2);
    assert!(filter
        .expressions
        .iter()
        .all(|expression| expression.evaluation_properties().is_reorder_fence()));
}

#[test]
fn routing_reopens_for_changes_not_for_unseen_or_detached_roots() {
    let mut construction = CanonicalScalars::default();
    let mut operator = LogicalOperator::Filter(Filter {
        child: (),
        expressions: vec![boolean(true)],
        projection_map: paro_planner::logical::operator::ProjectionMap::all(),
    });
    // An unseen, but already canonical root is not a change.
    assert!(!construction.normalize_operator(&mut operator));
    assert!(!construction.normalize_operator(&mut operator));
    let LogicalOperator::Filter(filter) = &mut operator else {
        unreachable!()
    };
    filter.expressions[0] = Expression::Conjunction(
        ConjunctionExpression::new(ConjunctionType::And, vec![boolean(true), boolean(false)])
            .into(),
    );
    assert!(construction.normalize_operator(&mut operator));
    assert!(!construction.normalize_operator(&mut operator));
}
