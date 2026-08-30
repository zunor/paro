// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn arena_generator_names_hidden_order_columns() {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string(), "b".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let exprs = vec![
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer)),
    ];
    let project = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(1, values, exprs).with_visible_names(vec!["a".into()]),
        ),
    );
    let order = LogicalPlan::new(
        &ctx,
        LogicalOperator::Order(Order::new(
            project,
            vec![paro_planner::binder::ir::OrderByNode {
                expression: ref_expr(1, LogicalType::Integer),
                ascending: false,
                nulls_first: true,
            }],
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let plan = generator
        .generate(&order)
        .expect("hidden order columns should receive physical names");

    let root = plan.node(plan.root);
    assert_eq!(root.output.names.as_ref(), ["a", "__paro_hidden_1"]);
    let [project_id] = plan.child_ids(&root.children) else {
        panic!("order should have one project child");
    };
    let PhysicalNodeKind::Project(spec) = &plan.node(*project_id).kind else {
        panic!("order child should be a project");
    };
    assert_eq!(spec.output_names.as_ref(), ["a", "__paro_hidden_1"]);
    let explain = plan.format_explain_text_with_spec(&ExplainSpec::default());
    assert!(
        explain.contains("Sort Key: b DESC NULLS FIRST"),
        "{explain}"
    );
    assert!(!explain.contains("Sort Key: #"), "{explain}");
}

#[test]
fn arena_generator_names_hidden_window_child_columns() {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec![
                "visible".to_string(),
                "hidden_a".to_string(),
                "hidden_b".to_string(),
            ],
            vec![
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::Integer,
            ],
        )),
    );
    let exprs = vec![
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(2, LogicalType::Integer)),
    ];
    let project = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(1, values, exprs).with_visible_names(vec!["visible".into()]),
        ),
    );
    let row_number = WindowFunction::row_number();
    let window = LogicalPlan::new(
        &ctx,
        LogicalOperator::Window(LogicalWindow::new(
            2,
            vec![WindowExpression::native(
                row_number.clone(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                WindowFrame::get_default_frame(&row_number),
                false,
            )],
            project,
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let plan = generator
        .generate(&window)
        .expect("window child hidden columns should receive physical names");

    let root = plan.node(plan.root);
    assert_eq!(
        root.output.names.as_ref(),
        ["visible", "__paro_hidden_1", "__paro_hidden_2", "window_1"]
    );
    let PhysicalNodeKind::Window(spec) = &root.kind else {
        panic!("expected root window node");
    };
    assert_eq!(spec.input_width, 3);
    assert_eq!(spec.output_names.len(), spec.output_types.len());
}

#[test]
fn whole_partition_aggregate_window_lowers_to_sort_free_breaker() {
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
    let (sum, target_types) = get_sum_function()
        .bind(&[LogicalType::Integer])
        .expect("bind integer sum");
    assert_eq!(target_types, vec![LogicalType::Integer]);
    let return_type = sum.return_type.clone();
    let aggregate = AggregateExpression::new(
        sum,
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

    let plan = PhysicalPlanGenerator::new(PlanBuildContext::default())
        .generate(&window)
        .expect("lower whole-partition aggregate window");
    let PhysicalNodeKind::PartitionAggregateWindow(spec) = &plan.node(plan.root).kind else {
        panic!("expected sort-free partition aggregate window");
    };
    assert_eq!(spec.detail_columns.as_ref(), [0, 1]);
    assert_eq!(spec.aggregate.grouping_key_count, 1);
    assert_eq!(spec.aggregate.aggregates.len(), 1);
    assert_eq!(spec.output_types.len(), 3);
    spec.verify().expect("partition aggregate spec");
}

#[test]
fn bigint_partition_key_lowers_to_typed_sort_free_breaker() {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["partkey".to_string(), "value".to_string()],
            vec![LogicalType::BigInt, LogicalType::Integer],
        )),
    );
    let aggregate =
        AggregateExpression::new(get_count_star_function(), Vec::new(), LogicalType::BigInt);
    let window = LogicalPlan::new(
        &ctx,
        LogicalOperator::Window(LogicalWindow::new(
            2,
            vec![WindowExpression::aggregate(
                aggregate,
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::BigInt,
                ))],
                Vec::new(),
                WindowFrame::default(),
            )],
            values,
        )),
    );

    let plan = PhysicalPlanGenerator::new(PlanBuildContext::default())
        .generate(&window)
        .expect("lower BIGINT partition aggregate window");
    let PhysicalNodeKind::PartitionAggregateWindow(spec) = &plan.node(plan.root).kind else {
        panic!("expected typed BIGINT partition aggregate window");
    };
    assert_eq!(spec.aggregate.groups[0].return_type(), LogicalType::BigInt);
    spec.verify().expect("BIGINT partition aggregate spec");
}

#[test]
fn ordered_full_partition_aggregate_keeps_the_semantic_window_fallback() {
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
    let (sum, _) = get_sum_function()
        .bind(&[LogicalType::Integer])
        .expect("bind integer sum");
    let aggregate = AggregateExpression::new(
        sum,
        vec![Expression::Reference(ReferenceExpression::new(
            1,
            LogicalType::Integer,
        ))],
        LogicalType::BigInt,
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
                vec![OrderByExpression {
                    expression: Expression::Reference(ReferenceExpression::new(
                        1,
                        LogicalType::Integer,
                    )),
                    ascending: true,
                    nulls_first: false,
                }],
                WindowFrame {
                    frame_type: paro_planner::expression::WindowFrameType::Rows,
                    start_bound: paro_planner::expression::WindowFrameBound::Unbounded,
                    start_is_preceding: true,
                    end_bound: paro_planner::expression::WindowFrameBound::Unbounded,
                    end_is_preceding: false,
                },
            )],
            values,
        )),
    );

    let plan = PhysicalPlanGenerator::new(PlanBuildContext::default())
        .generate(&window)
        .expect("lower ordered aggregate window");
    assert!(matches!(
        plan.node(plan.root).kind,
        PhysicalNodeKind::Window(_)
    ));
}

#[test]
fn arena_generator_lowers_row_literal_union_all_to_values() {
    let ctx = BindContext::new();
    let row = |value| {
        LogicalPlan::new(
            &ctx,
            LogicalOperator::Projection(
                Projection::new(
                    1,
                    LogicalPlan::dummy_scan(&ctx),
                    vec![Expression::Constant(ConstantExpression::new(
                        Value::Integer(value),
                        LogicalType::Integer,
                    ))],
                )
                .with_visible_names(vec!["v".to_string()]),
            ),
        )
    };
    let union = LogicalPlan::new(
        &ctx,
        LogicalOperator::SetOperation(SetOperation::union(
            2,
            row(1),
            row(2),
            true,
            vec![LogicalType::Integer],
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let plan = generator
        .generate(&union)
        .expect("row-literal UNION ALL should lower to values");

    let PhysicalNodeKind::Values(spec) = &plan.node(plan.root).kind else {
        panic!("expected UNION ALL to lower as values");
    };
    assert_eq!(spec.expressions.len(), 2);
    assert_eq!(spec.output_names.as_ref(), ["v"]);
}
