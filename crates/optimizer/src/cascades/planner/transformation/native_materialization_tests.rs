// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cascades::budget::{BudgetDimension, SearchBudget};
use crate::cascades::planner::MemoBuilder;
use crate::cascades::rules::TransformationRule;
use paro_common::types::LogicalType;
use paro_function::scalar::{BoundScalarFunction, FunctionErrorMode, ScalarFunctionSet};
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{AggregateExpression, ColumnRefExpression, FunctionExpression};
use paro_planner::operator::{ExpressionGet, JoinCondition};

fn column(table: usize, ordinal: usize) -> Expression {
    Expression::ColumnRef(
        ColumnRefExpression::new(ColumnBinding::new(table, ordinal), LogicalType::Integer).into(),
    )
}

fn source(table: usize) -> OwnedLogicalPlan {
    OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
        table,
        vec![],
        vec!["k".into(), "x".into(), "y".into()],
        vec![LogicalType::Integer; 3],
    )))
}

fn plan(blocked_first: bool) -> OwnedLogicalPlan {
    let mut arithmetic = ScalarFunctionSet::new("-".into());
    paro_function::scalar::operators::arithmetic::register_arithmetic_functions(&mut arithmetic);
    let (function, _) = arithmetic
        .bind(&[LogicalType::Integer, LogicalType::Integer])
        .unwrap();
    // A test-only total scalar contract isolates placement from error semantics.
    let function =
        BoundScalarFunction::from(function).with_error_mode(FunctionErrorMode::Infallible);
    let (sum, _) = paro_function::aggregate::distributive::sum::get_sum_function()
        .bind(&[LogicalType::Integer])
        .unwrap();
    let make = |table, left, right| {
        Expression::Aggregate(
            AggregateExpression::new(
                sum.clone(),
                vec![Expression::Function(
                    FunctionExpression::new(
                        function.clone(),
                        vec![column(table, left), column(table, right)],
                        LogicalType::Integer,
                    )
                    .into(),
                )],
                LogicalType::BigInt,
            )
            .into(),
        )
    };
    // Left input remains live in the join condition; right input can narrow.
    let mut aggregates = vec![make(0, 0, 1), make(1, 1, 2)];
    if !blocked_first {
        aggregates.reverse();
    }
    OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(Aggregate::new(
        10,
        11,
        12,
        OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            source(0),
            source(1),
            vec![JoinCondition::equality(column(0, 0), column(1, 0))],
        ))),
        vec![column(1, 0)],
        vec![],
        aggregates,
        vec![],
    ))))
}

fn materialized_count<Child>(operator: &LogicalOperator<Child>) -> usize {
    let LogicalOperator::Aggregate(aggregate) = operator else {
        panic!("aggregate root")
    };
    aggregate
        .aggregates
        .iter()
        .filter(|expression| {
            matches!(expression, Expression::Aggregate(sum)
            if matches!(sum.children.as_slice(), [Expression::ColumnRef(_)]))
        })
        .count()
}

fn control_plan() -> OwnedLogicalPlan {
    let mut plan = plan(true);
    let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
        unreachable!()
    };
    let LogicalOperator::Join(Join::Comparison(join)) = &mut aggregate.child.operator else {
        unreachable!()
    };
    let names = vec!["k".into(), "x".into(), "y".into()];
    *join.right = OwnedLogicalPlan::synthetic(LogicalOperator::MaterializedCTE(
        paro_planner::operator::MaterializedCTE::new(
            50,
            "materialized_input".into(),
            names.clone(),
            vec![LogicalType::Integer; 3],
            paro_planner::binder::ir::CTEMaterialize::Materialized,
            source(51),
            OwnedLogicalPlan::synthetic(LogicalOperator::CTERef(
                paro_planner::operator::CTERef::new(
                    50,
                    1,
                    "consumer".into(),
                    names,
                    vec![LogicalType::Integer; 3],
                ),
            )),
        )
        .with_ref_count(1),
    ));
    plan
}

fn rejected_plan(control: bool, outer: bool) -> OwnedLogicalPlan {
    let mut plan = if control { control_plan() } else { plan(true) };
    let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
        unreachable!()
    };
    let LogicalOperator::Join(Join::Comparison(join)) = &mut aggregate.child.operator else {
        unreachable!()
    };
    if outer {
        join.join_type = JoinType::Left;
    } else {
        // Both candidate inputs are used by a join condition and must remain.
        join.conditions
            .push(JoinCondition::equality(column(0, 0), column(1, 1)));
    }
    plan
}

#[test]
fn rejected_native_materialization_does_not_rebuild_the_selected_binding() {
    for control in [false, true] {
        for outer in [false, true] {
            let (_, changed) = input_materialization::optimize_plan(
                rejected_plan(control, outer),
                &BindContext::new(),
            )
            .unwrap();
            assert!(!changed);
            let mut input = MemoBuilder::build(
                rejected_plan(control, outer),
                BindContext::new(),
                SearchBudget::default(),
            )
            .unwrap();
            let state = input.planner_state.clone();
            state.write().unwrap().session =
                Some(paro_context::TestStatementContextBuilder::minimal().build());
            let root_expr = input.memo.group(input.root).unwrap().logical_exprs()[0];
            let binding = matching::scoped_pattern_bindings(
                PlannerTransformation::AggregateInputMaterialization,
                input.root,
                root_expr,
                &input.memo,
                &state.read().unwrap(),
                None,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .bindings
            .first()
            .cloned()
            .unwrap();
            let rule = PlannerTransformationRule {
                transformation: PlannerTransformation::AggregateInputMaterialization,
                planner_state: state.clone(),
            };
            let bridges = semantic_plan::owned_binding_instantiation_count();
            let arena = state.read().unwrap().staging_arena.len();
            let mut context = TransformContext::new(&mut input.memo, input.root);
            assert!(rule
                .apply_binding(&binding, &mut context)
                .unwrap()
                .is_empty());
            assert_eq!(
                semantic_plan::owned_binding_instantiation_count(),
                bridges,
                "control={control}, outer={outer}"
            );
            assert_eq!(state.read().unwrap().staging_arena.len(), arena);
        }
    }
}

#[test]
fn production_materialization_wraps_but_does_not_cross_a_cte_owner() {
    let bind = BindContext::new();
    for _ in 0..52 {
        bind.generate_table_index();
    }
    let (reference, changed) = input_materialization::optimize_plan(control_plan(), &bind).unwrap();
    assert!(changed);
    assert_eq!(materialized_count(&reference.operator), 1);
    let mut input =
        MemoBuilder::build(control_plan(), BindContext::new(), SearchBudget::default()).unwrap();
    let state = input.planner_state.clone();
    state.write().unwrap().session =
        Some(paro_context::TestStatementContextBuilder::minimal().build());
    let root_expr = input.memo.group(input.root).unwrap().logical_exprs()[0];
    let join_group = input.memo.logical_expr(root_expr).unwrap().key.children[0];
    let join_expr = input.memo.group(join_group).unwrap().logical_exprs()[0];
    let control_group = input.memo.logical_expr(join_expr).unwrap().key.children[1];
    let control_facts = input.memo.local_statistics_fingerprint(control_group);
    let binding = matching::scoped_pattern_bindings(
        PlannerTransformation::AggregateInputMaterialization,
        input.root,
        root_expr,
        &input.memo,
        &state.read().unwrap(),
        None,
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .bindings
    .first()
    .cloned()
    .unwrap();
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::AggregateInputMaterialization,
        planner_state: state.clone(),
    };
    let bridges = semantic_plan::owned_binding_instantiation_count();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let outputs = rule.apply_binding(&binding, &mut context).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(semantic_plan::owned_binding_instantiation_count(), bridges);
    let join_group = outputs[0].key.children[0];
    let join_expr = context.memo().group(join_group).unwrap().logical_exprs()[0];
    let projection_group = context.memo().logical_expr(join_expr).unwrap().key.children[1];
    let projection_expr = context
        .memo()
        .group(projection_group)
        .unwrap()
        .logical_exprs()[0];
    let projection = context.memo().logical_expr(projection_expr).unwrap();
    assert_eq!(projection.key.children.as_ref(), &[control_group]);
    assert!(matches!(
        &state.read().unwrap().payloads.logical[projection.payload.index()]
            .semantic_template
            .operator,
        LogicalOperator::Projection(_)
    ));
    assert_eq!(
        context.memo().local_statistics_fingerprint(control_group),
        control_facts
    );
}

fn grouping_plan(blocked_first: bool, grouping: usize) -> OwnedLogicalPlan {
    let mut plan = plan(blocked_first);
    let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
        unreachable!()
    };
    match grouping {
        0 => {}
        1 => aggregate.groups.clear(),
        2 => {
            aggregate.grouping_sets = vec![
                paro_planner::operator::aggregate::GroupingSet {
                    expressions: vec![0],
                },
                paro_planner::operator::aggregate::GroupingSet {
                    expressions: vec![],
                },
            ];
            aggregate.grouping_functions = vec![vec![0]];
        }
        3 => {
            use paro_planner::expression::{
                ComparisonExpression, ComparisonType, ReferenceExpression,
            };
            let (max, _) = paro_function::aggregate::distributive::minmax::get_max_function()
                .bind(&[LogicalType::BigInt])
                .unwrap();
            let output = Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(11, 0), LogicalType::BigInt).into(),
            );
            aggregate.post_reduction =
                Some(paro_planner::operator::aggregate::PostAggregateReduction {
                    reduction_index: 15,
                    reducers: vec![Expression::Aggregate(
                        AggregateExpression::new(max, vec![output.clone()], LogicalType::BigInt)
                            .into(),
                    )],
                    scalar_expressions: vec![Expression::Reference(
                        ReferenceExpression::new(0, LogicalType::BigInt).into(),
                    )],
                    predicate: Expression::Comparison(
                        ComparisonExpression::new(
                            ComparisonType::Equal,
                            output,
                            Expression::ColumnRef(
                                ColumnRefExpression::new(
                                    ColumnBinding::new(15, 0),
                                    LogicalType::BigInt,
                                )
                                .into(),
                            ),
                        )
                        .into(),
                    ),
                });
            aggregate.verify_post_reduction().unwrap();
        }
        _ => unreachable!(),
    }
    aggregate.recompute_returned_types();
    plan
}

fn nested_plan(target_left: bool, blocked: bool, outer_left: bool) -> OwnedLogicalPlan {
    let mut plan = plan(true);
    let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
        unreachable!()
    };
    let LogicalOperator::Join(Join::Comparison(join)) = &mut aggregate.child.operator else {
        unreachable!()
    };
    let target = std::mem::replace(
        &mut *join.right,
        OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
    );
    let (left, right) = if target_left {
        (target, source(2))
    } else {
        (source(2), target)
    };
    *join.right = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::comparison(
        JoinType::Inner,
        left,
        right,
        vec![JoinCondition::equality(
            column(
                if target_left { 1 } else { 2 },
                usize::from(blocked && target_left),
            ),
            column(
                if target_left { 2 } else { 1 },
                usize::from(blocked && !target_left),
            ),
        )],
    )));
    if outer_left {
        std::mem::swap(&mut join.left, &mut join.right);
        for condition in &mut join.conditions {
            std::mem::swap(&mut condition.left, &mut condition.right);
        }
    }
    plan
}

#[test]
fn production_materialization_reaches_the_deepest_selected_join_input() {
    for target_left in [true, false] {
        for blocked in [true, false] {
            for outer_left in [true, false] {
                let bind = BindContext::new();
                for _ in 0..13 {
                    bind.generate_table_index();
                }
                let (reference, changed) = input_materialization::optimize_plan(
                    nested_plan(target_left, blocked, outer_left),
                    &bind,
                )
                .unwrap();
                assert!(changed);
                let (_, widths) = reference
                    .try_fold_post_order(|plan, children: Vec<Vec<usize>>| {
                        let mut widths = children.into_iter().flatten().collect::<Vec<_>>();
                        if let LogicalOperator::Projection(projection) = &plan.operator {
                            widths.push(projection.expressions.len());
                        }
                        Ok((plan, widths))
                    })
                    .unwrap();
                assert_eq!(widths, vec![if blocked { 7 } else { 4 }]);
                let mut input = MemoBuilder::build(
                    nested_plan(target_left, blocked, outer_left),
                    BindContext::new(),
                    SearchBudget::default(),
                )
                .unwrap();
                let state = input.planner_state.clone();
                state.write().unwrap().session =
                    Some(paro_context::TestStatementContextBuilder::minimal().build());
                let binding = {
                    let state = state.read().unwrap();
                    matching::scoped_pattern_bindings(
                        PlannerTransformation::AggregateInputMaterialization,
                        input.root,
                        input.memo.group(input.root).unwrap().logical_exprs()[0],
                        &input.memo,
                        &state,
                        None,
                        BudgetDimension::RuleWorkPerGroup,
                    )
                    .unwrap()
                    .bindings
                    .first()
                    .cloned()
                    .unwrap()
                };
                {
                    let state = state.read().unwrap();
                    let mut context = TransformContext::new(&mut input.memo, input.root);
                    let facts = boundary::BoundarySnapshot::read(
                        &mut context,
                        &state,
                        &binding.root,
                        BudgetDimension::RuleWorkPerGroup,
                    )
                    .unwrap()
                    .unwrap();
                    let shell = try_native_input_materialization(
                        &binding.root,
                        context.memo(),
                        &state,
                        &facts,
                    )
                    .unwrap()
                    .unwrap();
                    let native_widths = shell
                        .nodes
                        .iter()
                        .filter_map(|node| match &node.operator {
                            LogicalOperator::Projection(projection) => {
                                Some(projection.expressions.len())
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(
                        native_widths, widths,
                        "target_left={target_left}, blocked={blocked}, outer_left={outer_left}"
                    );
                    shell.layouts().unwrap();
                }
                let rule = PlannerTransformationRule {
                    transformation: PlannerTransformation::AggregateInputMaterialization,
                    planner_state: state.clone(),
                };
                let bridges = semantic_plan::owned_binding_instantiation_count();
                let mut context = TransformContext::new(&mut input.memo, input.root);
                let outputs = rule.apply_binding(&binding, &mut context).unwrap();
                assert_eq!(outputs.len(), 1);
                assert_eq!(semantic_plan::owned_binding_instantiation_count(), bridges);
            }
        }
    }
}

#[test]
fn production_materialization_keeps_success_when_another_input_is_rejected() {
    for blocked_first in [true, false] {
        for grouping in 0..4 {
            let bind = BindContext::new();
            for _ in 0..16 {
                bind.generate_table_index();
            }
            let (reference, changed) =
                input_materialization::optimize_plan(grouping_plan(blocked_first, grouping), &bind)
                    .unwrap();
            assert!(changed);
            assert_eq!(materialized_count(&reference.operator), 1);
            let mut input = MemoBuilder::build(
                grouping_plan(blocked_first, grouping),
                BindContext::new(),
                SearchBudget::default(),
            )
            .unwrap();
            let state = input.planner_state.clone();
            state.write().unwrap().session =
                Some(paro_context::TestStatementContextBuilder::minimal().build());
            let binding = {
                let state = state.read().unwrap();
                matching::scoped_pattern_bindings(
                    PlannerTransformation::AggregateInputMaterialization,
                    input.root,
                    input.memo.group(input.root).unwrap().logical_exprs()[0],
                    &input.memo,
                    &state,
                    None,
                    BudgetDimension::RuleWorkPerGroup,
                )
                .unwrap()
                .bindings
                .first()
                .cloned()
                .expect("production binding")
            };
            let rule = PlannerTransformationRule {
                transformation: PlannerTransformation::AggregateInputMaterialization,
                planner_state: state.clone(),
            };
            let bridges = semantic_plan::owned_binding_instantiation_count();
            let arena = state.read().unwrap().staging_arena.len();
            let mut context = TransformContext::new(&mut input.memo, input.root);
            let outputs = rule.apply_binding(&binding, &mut context).unwrap();
            assert_eq!(outputs.len(), 1);
            assert_eq!(semantic_plan::owned_binding_instantiation_count(), bridges);
            let state = state.read().unwrap();
            assert_eq!(state.staging_arena.len(), arena);
            let operator = &state.payloads.logical[outputs[0].payload.index()]
                .semantic_template
                .operator;
            assert_eq!(materialized_count(operator), 1);
            let LogicalOperator::Aggregate(actual) = operator else {
                unreachable!()
            };
            let LogicalOperator::Aggregate(expected) = &reference.operator else {
                unreachable!()
            };
            assert_eq!(actual.returned_types, expected.returned_types);
            assert_eq!(actual.grouping_functions, expected.grouping_functions);
            actual.verify_post_reduction().unwrap();
            assert_eq!(
                actual.post_reduction.is_some(),
                expected.post_reduction.is_some()
            );
            if let (Some(actual), Some(expected)) =
                (&actual.post_reduction, &expected.post_reduction)
            {
                assert_eq!(actual.reduction_index, expected.reduction_index);
                assert!(actual.predicate.equals(&expected.predicate));
                assert_eq!(actual.reducers.len(), expected.reducers.len());
                assert!(actual
                    .reducers
                    .iter()
                    .zip(&expected.reducers)
                    .all(|(a, b)| a.equals(b)));
                assert_eq!(
                    actual.scalar_expressions.len(),
                    expected.scalar_expressions.len()
                );
                assert!(actual
                    .scalar_expressions
                    .iter()
                    .zip(&expected.scalar_expressions)
                    .all(|(a, b)| a.equals(b)));
            }
            assert_eq!(
                actual
                    .grouping_sets
                    .iter()
                    .map(|set| &set.expressions)
                    .collect::<Vec<_>>(),
                expected
                    .grouping_sets
                    .iter()
                    .map(|set| &set.expressions)
                    .collect::<Vec<_>>()
            );
            let blocked_index = if blocked_first { 0 } else { 1 };
            assert!(actual.aggregates[blocked_index].equals(&expected.aggregates[blocked_index]));
        }
    }
}
