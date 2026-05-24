// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::entry::{EdgeTableInfo, VertexTableInfo};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::window::WindowFunction;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    ConstantExpression, Expression, ReferenceExpression, WindowExpression, WindowFrame,
};
use paro_planner::operator::join::{Join, JoinCondition, JoinType};
use paro_planner::operator::{
    ExpressionGet, Filter, GraphExpand, GraphScan, Limit, LogicalOperator, Order, Projection,
    SetOperation, Window as LogicalWindow,
};
use paro_planner::plan::LogicalPlan;

use super::*;

#[test]
fn arena_generator_builds_streaming_subset_without_runtime_objects() {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let filter = LogicalPlan::new(&ctx, LogicalOperator::Filter(Filter::new(values, vec![])));
    let project_expr = Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer));
    let project = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(1, filter, vec![project_expr]).with_output_names(vec!["a".into()]),
        ),
    );
    let limit = LogicalPlan::new(
        &ctx,
        LogicalOperator::Limit(Limit::new(project, None, None)),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let plan = generator.generate(&limit).expect("subset should lower");

    assert_eq!(plan.nodes.len(), 4);
    assert!(matches!(
        plan.node(plan.root).kind,
        PhysicalNodeKind::Limit(_)
    ));
    assert!(PhysicalPlanGenerator::ensure_fully_typed(&plan).is_ok());
    assert!(plan.format_tree().contains("LIMIT"));
}

#[test]
fn arena_generator_lowers_distinct_to_hash_aggregate() {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let distinct = LogicalPlan::new(
        &ctx,
        LogicalOperator::Distinct(paro_planner::operator::Distinct::new(values)),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let plan = generator
        .generate(&distinct)
        .expect("DISTINCT should lower to typed aggregate");

    let PhysicalNodeKind::Aggregate(spec) = &plan.node(plan.root).kind else {
        panic!("DISTINCT should lower as aggregate");
    };
    assert_eq!(spec.grouping_key_count, 1);
    assert!(spec.aggregates.is_empty());
    assert_eq!(spec.output_names.as_ref(), ["a"]);
    assert_eq!(plan.child_ids(&plan.node(plan.root).children).len(), 1);
    assert!(PhysicalPlanGenerator::ensure_fully_typed(&plan).is_ok());
}

#[test]
fn arena_generator_hands_graph_expand_filters_to_graph_project() {
    let ctx = BindContext::new();
    let scan = LogicalPlan::new(
        &ctx,
        LogicalOperator::GraphScan(GraphScan::new(
            VertexTableInfo {
                table_name: "vertices".to_string(),
                table_oid: 1,
                key_column_ids: vec![0],
                label: "v".to_string(),
                property_column_ids: vec![],
            },
            None,
            0,
            "v".to_string(),
            "g".to_string(),
            "public".to_string(),
        )),
    );
    let mut expand = GraphExpand::new(
        EdgeTableInfo {
            table_name: "edges".to_string(),
            table_oid: 2,
            key_column_ids: vec![0],
            source_key_column_ids: vec![0],
            source_vertex_table: "vertices".to_string(),
            source_ref_column_ids: vec![1],
            destination_key_column_ids: vec![0],
            destination_vertex_table: "vertices".to_string(),
            destination_ref_column_ids: vec![2],
            label: "e".to_string(),
            property_column_ids: vec![],
        },
        paro_planner::operator::graph_expand::ExpandDirection::Forward,
        "v".to_string(),
        0,
        1,
        2,
        "v".to_string(),
        1,
        1,
        "vertices".to_string(),
        scan,
    );
    expand.edge_filter = Some(Expression::Constant(ConstantExpression::new(
        Value::Boolean(true),
        LogicalType::Boolean,
    )));
    expand.target_filter = Some(Expression::Constant(ConstantExpression::new(
        Value::Boolean(true),
        LogicalType::Boolean,
    )));
    let expand = LogicalPlan::new(&ctx, LogicalOperator::GraphExpand(expand));
    let project = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(
                3,
                expand,
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::UBigInt,
                ))],
            )
            .with_output_names(vec!["src".to_string()]),
        ),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let plan = generator
        .generate(&project)
        .expect("graph project should own graph expand filters");

    let PhysicalNodeKind::GraphProject(project_spec) = &plan.node(plan.root).kind else {
        panic!("expected graph project root");
    };
    assert_eq!(project_spec.filters.len(), 2);
    let [expand_id] = plan.child_ids(&plan.node(plan.root).children) else {
        panic!("graph project should have graph expand child");
    };
    let PhysicalNodeKind::GraphExpand(expand_spec) = &plan.node(*expand_id).kind else {
        panic!("graph project child should be graph expand");
    };
    assert!(expand_spec.edge_filter.is_none());
    assert!(expand_spec.target_filter.is_none());
    assert!(!expand_spec.has_path_functions);
    assert_eq!(expand_spec.output_types.len(), 5);
    assert!(PhysicalPlanGenerator::ensure_fully_typed(&plan).is_ok());
}

#[test]
fn arena_generator_lowers_graph_path_functions_with_path_history() {
    let ctx = BindContext::new();
    let scan = LogicalPlan::new(
        &ctx,
        LogicalOperator::GraphScan(GraphScan::new(
            VertexTableInfo {
                table_name: "vertices".to_string(),
                table_oid: 1,
                key_column_ids: vec![0],
                label: "v".to_string(),
                property_column_ids: vec![],
            },
            None,
            0,
            "v".to_string(),
            "g".to_string(),
            "public".to_string(),
        )),
    );
    let mut expand = GraphExpand::new(
        EdgeTableInfo {
            table_name: "edges".to_string(),
            table_oid: 2,
            key_column_ids: vec![0],
            source_key_column_ids: vec![0],
            source_vertex_table: "vertices".to_string(),
            source_ref_column_ids: vec![1],
            destination_key_column_ids: vec![0],
            destination_vertex_table: "vertices".to_string(),
            destination_ref_column_ids: vec![2],
            label: "e".to_string(),
            property_column_ids: vec![],
        },
        paro_planner::operator::graph_expand::ExpandDirection::Forward,
        "v".to_string(),
        0,
        1,
        2,
        "v".to_string(),
        1,
        1,
        "vertices".to_string(),
        scan,
    );
    expand.has_path_functions = true;
    let plan = LogicalPlan::new(&ctx, LogicalOperator::GraphExpand(expand));

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let physical = generator
        .generate(&plan)
        .expect("path functions should lower with path history enabled");

    let PhysicalNodeKind::GraphExpand(spec) = &physical.node(physical.root).kind else {
        panic!(
            "expected GRAPH_EXPAND root, got {:?}",
            physical.node(physical.root).kind
        );
    };
    assert!(spec.has_path_functions);
}

#[test]
fn arena_generator_lowers_single_join_to_typed_hash_path() {
    let ctx = BindContext::new();
    let left = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["l".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let right = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["r".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
    );
    let join = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Single,
            left,
            right,
            vec![condition],
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let plan = generator
        .generate(&join)
        .expect("single join should lower to typed hash join");

    let PhysicalNodeKind::HashJoin(spec) = &plan.node(plan.root).kind else {
        panic!("single join should enter typed hash join after scalar semantics coverage");
    };
    assert_eq!(spec.join_type, JoinType::Single);
    assert_eq!(plan.child_ids(&plan.node(plan.root).children).len(), 2);
    assert!(PhysicalPlanGenerator::ensure_fully_typed(&plan).is_ok());
}

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
            Projection::new(1, values, exprs).with_output_names(vec!["a".into()]),
        ),
    );
    let order = LogicalPlan::new(&ctx, LogicalOperator::Order(Order::new(project, vec![])));

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
            Projection::new(1, values, exprs).with_output_names(vec!["visible".into()]),
        ),
    );
    let row_number = WindowFunction::row_number();
    let window = LogicalPlan::new(
        &ctx,
        LogicalOperator::Window(LogicalWindow::new(
            2,
            vec![WindowExpression {
                function: row_number.clone(),
                children: Vec::new(),
                partitions: Vec::new(),
                orders: Vec::new(),
                frame: WindowFrame::get_default_frame(&row_number),
                ignore_nulls: false,
                return_type: LogicalType::BigInt,
            }],
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
                .with_output_names(vec!["v".to_string()]),
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
