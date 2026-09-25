// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn arena_extractor_hands_graph_expand_filters_to_graph_project() {
    let ctx = BindContext::new();
    let scan = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::GraphScan(Box::new(GraphScan::new(
            VertexTableInfo {
                table_name: "vertices".to_string(),
                table_oid: 1,
                key_column_ids: vec![0],
                label: "v".to_string(),
                property_column_ids: vec![],
            },
            None,
            0,
            3,
            "v".to_string(),
            "g".to_string(),
            "public".to_string(),
        ))),
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
        paro_planner::logical::operator::graph_expand::ExpandDirection::Forward,
        "v".to_string(),
        0,
        1,
        2,
        3,
        "v".to_string(),
        1,
        1,
        "vertices".to_string(),
        scan,
    );
    expand.edge_filter = Some(Expression::Constant(
        ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
    ));
    expand.target_filter = Some(Expression::Constant(
        ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
    ));
    let expand = OwnedLogicalPlan::new(&ctx, LogicalOperator::GraphExpand(Box::new(expand)));
    let project = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(
                3,
                expand,
                vec![Expression::Reference(
                    ReferenceExpression::new(0, LogicalType::UBigInt).into(),
                )],
            )
            .with_visible_names(vec!["src".to_string()]),
        ),
    );

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let plan = extractor
        .build(project)
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
    assert!(crate::physical::PhysicalPlanVerifier::verify(&plan).is_ok());
}

#[test]
fn arena_extractor_lowers_graph_path_functions_with_path_history() {
    let ctx = BindContext::new();
    let scan = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::GraphScan(Box::new(GraphScan::new(
            VertexTableInfo {
                table_name: "vertices".to_string(),
                table_oid: 1,
                key_column_ids: vec![0],
                label: "v".to_string(),
                property_column_ids: vec![],
            },
            None,
            0,
            3,
            "v".to_string(),
            "g".to_string(),
            "public".to_string(),
        ))),
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
        paro_planner::logical::operator::graph_expand::ExpandDirection::Forward,
        "v".to_string(),
        0,
        1,
        2,
        3,
        "v".to_string(),
        1,
        1,
        "vertices".to_string(),
        scan,
    );
    expand.has_path_functions = true;
    let plan = OwnedLogicalPlan::new(&ctx, LogicalOperator::GraphExpand(Box::new(expand)));

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let physical = extractor
        .build(plan)
        .expect("path functions should lower with path history enabled");

    let PhysicalNodeKind::GraphExpand(spec) = &physical.node(physical.root).kind else {
        panic!(
            "expected GRAPH_EXPAND root, got {:?}",
            physical.node(physical.root).kind
        );
    };
    assert!(spec.has_path_functions);
}
