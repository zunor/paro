// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::array_agg::get_array_agg_function;
use paro_function::aggregate::distributive::sum::get_sum_function;
use paro_function::aggregate::AggregateFunction;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    AggregateExpression, Expression, ReferenceExpression, WindowExpression, WindowFrame,
};
use paro_planner::operator::{ExpressionGet, LogicalOperator, Window as LogicalWindow};
use paro_planner::plan::LogicalPlan;

use super::{PhysicalNodeKind, PhysicalPlan, PhysicalPlanGenerator, PlanBuildContext};

fn lower_full_partition_window(function: AggregateFunction) -> PhysicalPlan {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["grp".to_string(), "value".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let return_type = function.return_type.clone();
    let aggregate = AggregateExpression::new(
        function,
        vec![Expression::Reference(ReferenceExpression::new(
            1,
            LogicalType::Integer,
        ))],
        return_type,
    );
    let window = LogicalPlan::new(
        &ctx,
        LogicalOperator::Window(LogicalWindow::new(
            2,
            vec![WindowExpression::aggregate(
                aggregate,
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::Integer,
                ))],
                Vec::new(),
                WindowFrame::default(),
            )],
            values,
        )),
    );

    PhysicalPlanGenerator::new(PlanBuildContext::default())
        .generate(&window)
        .expect("lower full-partition aggregate window")
}

#[test]
fn destructor_owned_state_keeps_sorted_window_fallback() {
    let (array_agg, _) = get_array_agg_function()
        .bind(&[LogicalType::Integer])
        .expect("bind integer array_agg");
    assert!(array_agg.destructor.is_some());

    let plan = lower_full_partition_window(array_agg);
    assert!(matches!(
        plan.node(plan.root).kind,
        PhysicalNodeKind::Window(_)
    ));
}

#[test]
fn public_partition_spec_rejects_destructor_owned_state() {
    let (sum, _) = get_sum_function()
        .bind(&[LogicalType::Integer])
        .expect("bind integer sum");
    let plan = lower_full_partition_window(sum);
    let PhysicalNodeKind::PartitionAggregateWindow(spec) = &plan.node(plan.root).kind else {
        panic!("expected partition aggregate window");
    };
    let mut invalid = spec.clone();
    let Expression::Aggregate(aggregate) = &mut invalid.aggregate.aggregates[0] else {
        panic!("expected bound aggregate expression");
    };
    let (array_agg, _) = get_array_agg_function()
        .bind(&[LogicalType::Integer])
        .expect("bind integer array_agg");
    aggregate.function.destructor = array_agg.destructor;

    let error = invalid
        .verify()
        .expect_err("destructor-owned state must be rejected at the public spec boundary");
    assert!(error.to_string().contains("destructor-free states"));
}
