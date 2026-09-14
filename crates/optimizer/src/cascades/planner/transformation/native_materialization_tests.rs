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

#[test]
fn production_materialization_keeps_success_when_another_input_is_rejected() {
    for blocked_first in [true, false] {
        let bind = BindContext::new();
        for _ in 0..13 {
            bind.generate_table_index();
        }
        let (reference, changed) =
            input_materialization::optimize_plan(plan(blocked_first), &bind).unwrap();
        assert!(changed);
        assert_eq!(materialized_count(&reference.operator), 1);
        let mut input = MemoBuilder::build(
            plan(blocked_first),
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
        let blocked_index = if blocked_first { 0 } else { 1 };
        assert!(actual.aggregates[blocked_index].equals(&expected.aggregates[blocked_index]));
    }
}
