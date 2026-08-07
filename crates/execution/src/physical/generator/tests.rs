// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::{ColumnDefinition, EdgeTableInfo, TableCatalogEntry, VertexTableInfo};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::window::WindowFunction;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    AggregateExpression, ComparisonExpression, ComparisonType, ConjunctionExpression,
    ConjunctionType, ConstantExpression, Expression, OperatorExpression, OperatorType,
    ReferenceExpression, WindowExpression, WindowFrame,
};
use paro_planner::operator::join::{Join, JoinCondition, JoinType};
use paro_planner::operator::{
    Aggregate, ExpressionGet, Filter, Get, GraphExpand, GraphScan, Limit, LogicalOperator, Order,
    Projection, SetOperation, Window as LogicalWindow,
};
use paro_planner::plan::LogicalPlan;
use paro_storage::index::PredicateTree;
use paro_storage::search::{
    CapabilityToken, FullTextIntent, FullTextQueryKind, FullTextQueryStats, FullTextScoreMode,
    NormalizedSearchRequest, ProjectionSpec, SearchCapabilityState, SearchIntent,
    SearchRequestMode,
};
use paro_storage::statistics::StringStats;

use super::*;
use crate::physical::specs::GroupKeyEncoding;

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
fn aggregate_uses_lossless_fixed_width_keys_for_bounded_strings() {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["brand".to_string()],
            vec![LogicalType::Varchar],
        )),
    );
    let count = Expression::Aggregate(AggregateExpression::new(
        get_count_star_function(),
        vec![],
        LogicalType::BigInt,
    ));
    let mut aggregate = Aggregate::new(
        1,
        2,
        3,
        values,
        vec![ref_expr(0, LogicalType::Varchar)],
        vec![],
        vec![count],
        vec![],
    );
    let mut stats = StringStats::create_empty(LogicalType::Varchar);
    StringStats::update(&mut stats, "Brand45");
    aggregate.group_stats[0] = Some(stats);
    let aggregate = LogicalPlan::new(&ctx, LogicalOperator::Aggregate(aggregate));

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let plan = generator
        .generate(&aggregate)
        .expect("aggregate should lower");
    let PhysicalNodeKind::Aggregate(spec) = &plan.node(plan.root).kind else {
        panic!("expected aggregate root");
    };
    assert_eq!(
        spec.group_key_encodings.as_ref(),
        [GroupKeyEncoding::PackedString {
            physical_type: LogicalType::UBigInt,
            max_length: 7,
        }]
    );
}

#[test]
fn aggregate_skips_offset_keys_that_only_replace_row_padding() {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["size".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let count = Expression::Aggregate(AggregateExpression::new(
        get_count_star_function(),
        vec![],
        LogicalType::BigInt,
    ));
    let mut aggregate = Aggregate::new(
        1,
        2,
        3,
        values,
        vec![ref_expr(0, LogicalType::Integer)],
        vec![],
        vec![count],
        vec![],
    );
    let mut stats = paro_storage::statistics::NumericStats::create_empty(LogicalType::Integer);
    paro_storage::statistics::NumericStats::set_min(
        &mut stats,
        &paro_common::runtime_value::Value::Integer(-5),
    );
    paro_storage::statistics::NumericStats::set_max(
        &mut stats,
        &paro_common::runtime_value::Value::Integer(250),
    );
    aggregate.group_stats[0] = Some(stats);
    let aggregate = LogicalPlan::new(&ctx, LogicalOperator::Aggregate(aggregate));

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let plan = generator
        .generate(&aggregate)
        .expect("aggregate should lower");
    let PhysicalNodeKind::Aggregate(spec) = &plan.node(plan.root).kind else {
        panic!("expected aggregate root");
    };
    assert_eq!(
        spec.group_key_encodings.as_ref(),
        [GroupKeyEncoding::Identity]
    );
}

#[test]
fn arena_generator_fuses_aggregate_only_having_into_aggregate_emit() {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["key".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let count = Expression::Aggregate(AggregateExpression::new(
        get_count_star_function(),
        vec![],
        LogicalType::BigInt,
    ));
    let aggregate = LogicalPlan::new(
        &ctx,
        LogicalOperator::Aggregate(Aggregate::new(
            1,
            2,
            3,
            values,
            vec![ref_expr(0, LogicalType::Integer)],
            vec![],
            vec![count],
            vec![],
        )),
    );
    let having = LogicalPlan::new(
        &ctx,
        LogicalOperator::Filter(Filter::new(
            aggregate,
            vec![comparison(
                ComparisonType::GreaterThan,
                ref_expr(1, LogicalType::BigInt),
                Expression::Constant(ConstantExpression::new(
                    Value::BigInt(1),
                    LogicalType::BigInt,
                )),
            )],
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let plan = generator.generate(&having).expect("HAVING should lower");

    let PhysicalNodeKind::Aggregate(spec) = &plan.node(plan.root).kind else {
        panic!("aggregate-only HAVING should be fused into aggregate emit");
    };
    assert_eq!(spec.having_filter.len(), 1);
    let Expression::Comparison(predicate) = &spec.having_filter[0] else {
        panic!("expected rebased HAVING comparison");
    };
    let Expression::Reference(reference) = predicate.left.as_ref() else {
        panic!("expected aggregate reference in HAVING");
    };
    assert_eq!(reference.index, 0);
}

#[test]
fn arena_generator_pushes_filter_predicates_into_rowset_scan() {
    let ctx = BindContext::new();
    let get = LogicalPlan::new(&ctx, LogicalOperator::Get(test_get()));
    let mut filter = Filter::new(
        get,
        vec![
            comparison(
                ComparisonType::GreaterThanOrEqual,
                ref_expr(0, LogicalType::Integer),
                int_const(10),
            ),
            Expression::Operator(OperatorExpression::new(
                OperatorType::In,
                vec![
                    ref_expr(1, LogicalType::Integer),
                    int_const(3),
                    int_const(7),
                ],
                LogicalType::Boolean,
            )),
            Expression::Operator(OperatorExpression::new_unary(
                OperatorType::IsNull,
                ref_expr(2, LogicalType::Varchar),
                LogicalType::Boolean,
            )),
        ],
    );
    filter.projection_map = vec![0];
    let plan = LogicalPlan::new(&ctx, LogicalOperator::Filter(filter));

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let physical = generator.generate(&plan).expect("filter should lower");

    let PhysicalNodeKind::RowsetScan(spec) = &physical.node(physical.root).kind else {
        panic!("fully pushed filter should lower to rowset scan root");
    };
    assert_eq!(spec.column_ids.as_ref(), [0]);
    assert!(spec.residual_predicates.is_empty());
    assert!(!spec.late_materialize);
    let Some(PredicateTree::And(children)) = spec.predicate.as_ref() else {
        panic!("expected conjunctive storage predicate");
    };
    assert_eq!(children.len(), 3);
    assert!(physical
        .format_explain_text_with_spec(&paro_planner::operator::ExplainSpec::default())
        .contains("Pushed Predicate"));
}

#[test]
fn arena_generator_can_disable_rowset_scan_pushdown() {
    let ctx = BindContext::new();
    let get = LogicalPlan::new(&ctx, LogicalOperator::Get(test_get()));
    let filter = Filter::new(
        get,
        vec![comparison(
            ComparisonType::Equal,
            ref_expr(0, LogicalType::Integer),
            int_const(42),
        )],
    );
    let plan = LogicalPlan::new(&ctx, LogicalOperator::Filter(filter));

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext {
        rowset_scan_pushdown: false,
        ..PlanBuildContext::default()
    });
    let physical = generator.generate(&plan).expect("filter should lower");

    let PhysicalNodeKind::Filter(_) = &physical.node(physical.root).kind else {
        panic!("disabled pushdown should keep a filter root");
    };
    let [child] = physical.child_ids(&physical.node(physical.root).children) else {
        panic!("filter should have one rowset child");
    };
    let PhysicalNodeKind::RowsetScan(scan) = &physical.node(*child).kind else {
        panic!("filter child should be rowset scan");
    };
    assert!(scan.predicate.is_none());
    assert!(!scan.late_materialize);
}

#[test]
fn arena_generator_keeps_residual_filter_above_pushed_rowset_scan() {
    let ctx = BindContext::new();
    let get = LogicalPlan::new(&ctx, LogicalOperator::Get(test_get()));
    let filter = Filter::new(
        get,
        vec![Expression::Conjunction(ConjunctionExpression {
            conjunction_type: ConjunctionType::And,
            children: vec![
                comparison(
                    ComparisonType::Equal,
                    ref_expr(0, LogicalType::Integer),
                    int_const(42),
                ),
                Expression::Constant(ConstantExpression::new(
                    Value::Boolean(true),
                    LogicalType::Boolean,
                )),
            ],
        })],
    );
    let plan = LogicalPlan::new(&ctx, LogicalOperator::Filter(filter));

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let physical = generator.generate(&plan).expect("filter should lower");

    let PhysicalNodeKind::Filter(spec) = &physical.node(physical.root).kind else {
        panic!("residual expression should keep a filter root");
    };
    assert_eq!(spec.expressions.len(), 1);
    let [child] = physical.child_ids(&physical.node(physical.root).children) else {
        panic!("filter should have one rowset child");
    };
    let PhysicalNodeKind::RowsetScan(scan) = &physical.node(*child).kind else {
        panic!("filter child should be rowset scan");
    };
    assert!(scan.predicate.is_some());
    assert_eq!(scan.residual_predicates.len(), 1);
}

#[test]
fn arena_generator_pushes_get_runtime_filters_into_rowset_scan() {
    let ctx = BindContext::new();
    let mut get = test_get();
    get.runtime_filter_expressions.push(comparison(
        ComparisonType::LessThanOrEqual,
        ref_expr(0, LogicalType::Integer),
        int_const(99),
    ));
    let plan = LogicalPlan::new(&ctx, LogicalOperator::Get(get));

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let physical = generator.generate(&plan).expect("get should lower");

    let PhysicalNodeKind::RowsetScan(spec) = &physical.node(physical.root).kind else {
        panic!("expected rowset scan");
    };
    assert!(spec.predicate.is_some());
    assert_eq!(spec.runtime_filter_expressions.len(), 1);
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

#[test]
fn arena_generator_lowers_search_scan_with_planned_token() {
    let ctx = BindContext::new();
    let get = test_get();
    let intent = SearchIntent::FullText(FullTextIntent {
        column_id: 2,
        query: "graph".to_string(),
        query_kind: FullTextQueryKind::Legacy,
        query_stats: FullTextQueryStats::new(1),
        config: "simple".to_string(),
        score_mode: FullTextScoreMode::Bm25,
    });
    let token = CapabilityToken {
        definition_id: 42,
        generation_id: 7,
        root_version: 11,
        capability_state: SearchCapabilityState::Queryable,
    };
    let score_expr = Expression::Constant(ConstantExpression::new(
        Value::Float(0.75),
        LogicalType::Float,
    ));
    let search = LogicalSearchScan::new(
        get,
        NormalizedSearchRequest {
            table_id: 1,
            mode: SearchRequestMode::TopK { limit: 5 },
            predicate: None,
            projections: ProjectionSpec {
                columns: vec![2],
                include_score: true,
            },
            intents: vec![intent.clone()],
            fusion: None,
        },
        SearchDecision::IndexScan {
            candidate: SearchCandidate {
                intent,
                token: token.clone(),
                kind: paro_storage::search::SearchIndexKind::FullText,
                estimated_cost: None,
            },
            confidence: paro_planner::operator::Confidence::High,
        },
        vec![ref_expr(2, LogicalType::Varchar), score_expr.clone()],
        9,
        Vec::new(),
        Vec::new(),
        1,
        score_expr,
        false,
        5,
    )
    .with_output_names(vec!["c".to_string(), "score".to_string()]);
    let plan = LogicalPlan::new(&ctx, LogicalOperator::SearchScan(search));

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let physical = generator.generate(&plan).expect("search scan should lower");

    let PhysicalNodeKind::FullTextSearch(spec) = &physical.node(physical.root).kind else {
        panic!("search scan should lower to fulltext source");
    };
    assert_eq!(spec.capability_token, token);
    assert_eq!(spec.column_id, 2);
    assert_eq!(spec.projected_columns.as_ref(), [2]);
    assert!(spec.emit_score);
    assert_eq!(spec.output_names.as_ref(), ["c", "score"]);
}

fn test_get() -> Get {
    let storage = Arc::new(
        paro_storage::table::table_factory::TableFactory::default()
            .create_table(&[
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::Varchar,
            ])
            .expect("table storage"),
    );
    let table = Arc::new(TableCatalogEntry::new(
        "paro".to_string(),
        "public".to_string(),
        "scan_t".to_string(),
        vec![
            ColumnDefinition::new("a".to_string(), LogicalType::Integer),
            ColumnDefinition::new("b".to_string(), LogicalType::Integer),
            ColumnDefinition::new("c".to_string(), LogicalType::Varchar),
        ],
        storage,
        paro_catalog::entry::CatalogObjectId::from_raw(10_001),
        0,
    ));
    Get {
        table_index: 0,
        returned_types: vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Varchar,
        ],
        names: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        relation_name: Some("scan_t".to_string()),
        relation_alias: None,
        column_ids: vec![0, 1, 2],
        column_types: vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Varchar,
        ],
        table: Some(table),
        scan_order: None,
        runtime_filter_expressions: Vec::new(),
    }
}

fn ref_expr(index: usize, ty: LogicalType) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, ty))
}

fn int_const(value: i32) -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::Integer(value),
        LogicalType::Integer,
    ))
}

fn comparison(comparison_type: ComparisonType, left: Expression, right: Expression) -> Expression {
    Expression::Comparison(ComparisonExpression::new(comparison_type, left, right))
}
