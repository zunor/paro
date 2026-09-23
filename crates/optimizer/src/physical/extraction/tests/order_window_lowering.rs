// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn arena_extractor_names_hidden_order_columns() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string(), "b".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let exprs = vec![
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer).into()),
    ];
    let project = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(1, values, exprs).with_visible_names(vec!["a".into()]),
        ),
    );
    let order = OwnedLogicalPlan::new(
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

    let mut extractor = PhysicalPlanExtractor::new(ExtractionContext::default());
    let plan = extractor
        .extract(order)
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
fn arena_extractor_names_hidden_window_child_columns() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
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
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer).into()),
        Expression::Reference(ReferenceExpression::new(2, LogicalType::Integer).into()),
    ];
    let project = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(1, values, exprs).with_visible_names(vec!["visible".into()]),
        ),
    );
    let row_number = WindowFunction::row_number();
    let window = OwnedLogicalPlan::new(
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

    let mut extractor = PhysicalPlanExtractor::new(ExtractionContext::default());
    let plan = extractor
        .extract(window)
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
    let values = OwnedLogicalPlan::new(
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
        vec![Expression::Reference(
            ReferenceExpression::new(1, LogicalType::Integer).into(),
        )],
        return_type,
    );
    let window = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Window(LogicalWindow::new(
            2,
            vec![WindowExpression::aggregate(
                aggregate,
                vec![Expression::Reference(
                    ReferenceExpression::new(0, LogicalType::Integer).into(),
                )],
                Vec::new(),
                WindowFrame::default(),
            )],
            values,
        )),
    );

    let plan = PhysicalPlanExtractor::new(ExtractionContext::default())
        .extract(window)
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
fn composite_varlen_partition_keys_lower_to_sort_free_breaker() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["category".into(), "class".into(), "value".into()],
            vec![
                LogicalType::Varchar,
                LogicalType::Integer,
                LogicalType::Integer,
            ],
        )),
    );
    let (sum, _) = get_sum_function()
        .bind(&[LogicalType::Integer])
        .expect("bind integer sum");
    let return_type = sum.return_type.clone();
    let aggregate = AggregateExpression::new(
        sum,
        vec![Expression::Reference(
            ReferenceExpression::new(2, LogicalType::Integer).into(),
        )],
        return_type,
    );
    let window = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Window(LogicalWindow::new(
            3,
            vec![WindowExpression::aggregate(
                aggregate,
                vec![
                    Expression::Reference(ReferenceExpression::new(0, LogicalType::Varchar).into()),
                    Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer).into()),
                ],
                Vec::new(),
                WindowFrame::default(),
            )],
            values,
        )),
    );

    let plan = PhysicalPlanExtractor::new(ExtractionContext::default())
        .extract(window)
        .expect("lower composite varlen partition window");
    let PhysicalNodeKind::PartitionAggregateWindow(spec) = &plan.node(plan.root).kind else {
        panic!("expected sort-free partition aggregate window");
    };
    assert_eq!(spec.aggregate.grouping_key_count, 2);
    assert_eq!(
        spec.aggregate
            .groups
            .iter()
            .map(Expression::return_type)
            .collect::<Vec<_>>(),
        vec![LogicalType::Varchar, LogicalType::Integer]
    );
    spec.verify().expect("composite partition aggregate spec");
}

#[test]
fn bigint_partition_key_lowers_to_typed_sort_free_breaker() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
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
    let window = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Window(LogicalWindow::new(
            2,
            vec![WindowExpression::aggregate(
                aggregate,
                vec![Expression::Reference(
                    ReferenceExpression::new(0, LogicalType::BigInt).into(),
                )],
                Vec::new(),
                WindowFrame::default(),
            )],
            values,
        )),
    );

    let plan = PhysicalPlanExtractor::new(ExtractionContext::default())
        .extract(window)
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
    let values = OwnedLogicalPlan::new(
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
        vec![Expression::Reference(
            ReferenceExpression::new(1, LogicalType::Integer).into(),
        )],
        LogicalType::BigInt,
    );
    let window = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Window(LogicalWindow::new(
            2,
            vec![WindowExpression::aggregate(
                aggregate,
                vec![Expression::Reference(
                    ReferenceExpression::new(0, LogicalType::Integer).into(),
                )],
                vec![OrderByExpression {
                    expression: Expression::Reference(
                        ReferenceExpression::new(1, LogicalType::Integer).into(),
                    ),
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

    let plan = PhysicalPlanExtractor::new(ExtractionContext::default())
        .extract(window)
        .expect("lower ordered aggregate window");
    assert!(matches!(
        plan.node(plan.root).kind,
        PhysicalNodeKind::Window(_)
    ));
}

#[test]
fn arena_extractor_lowers_row_literal_union_all_to_values() {
    let ctx = BindContext::new();
    let row = |value| {
        OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Projection(
                Projection::new(
                    1,
                    OwnedLogicalPlan::dummy_scan(&ctx),
                    vec![Expression::Constant(
                        ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
                    )],
                )
                .with_visible_names(vec!["v".to_string()]),
            ),
        )
    };
    let union = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::SetOperation(SetOperation::union(
            2,
            row(1),
            row(2),
            true,
            vec![LogicalType::Integer],
        )),
    );

    let mut extractor = PhysicalPlanExtractor::new(ExtractionContext::default());
    let plan = extractor
        .extract(union)
        .expect("row-literal UNION ALL should lower to values");

    let PhysicalNodeKind::Values(spec) = &plan.node(plan.root).kind else {
        panic!("expected UNION ALL to lower as values");
    };
    assert_eq!(spec.expressions.len(), 2);
    assert_eq!(spec.output_names.as_ref(), ["v"]);
}
