// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_external::routine::identity::RoutineCallIdentity;
use paro_external::routine::spec::{
    RoutineId, RoutineNullPolicy, RoutineSemantics, RoutineSideEffects, RoutineStability,
    RowSemantics,
};
use paro_planner::operator::external_project::ExternalCostEstimate;

use crate::physical::specs::{ExternalProjectSpec, ExternalRoutineDescriptor, ExternalTableSpec};

#[test]
fn streaming_topn_guard_rejects_missing_order() {
    let topn = TopNSpec {
        orders: Box::new([]),
        limit: 10,
        offset: 0,
        hnsw_options: Default::default(),
        output_names: vec!["a".to_string()].into_boxed_slice(),
        output_types: vec![LogicalType::Integer].into_boxed_slice(),
    };
    assert!(ensure_streaming_topn_supported(&topn).is_err());
}

#[test]
fn lowerer_routes_external_project_as_typed_transform() {
    let plan = external_project_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert!(graph.pipelines.iter().any(|pipeline| {
        pipeline
            .transforms
            .iter()
            .any(|transform| matches!(transform, TransformSpec::ExternalProject(_)))
    }));
    let external_pipeline = graph
        .pipelines
        .iter()
        .find(|pipeline| {
            pipeline
                .transforms
                .iter()
                .any(|transform| matches!(transform, TransformSpec::ExternalProject(_)))
        })
        .expect("external project pipeline");
    assert_eq!(external_pipeline.properties.capabilities.parallelism.max, 1);
}

#[test]
fn lowerer_routes_external_table_as_typed_breaker() {
    let plan = external_table_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert!(graph
        .pipelines
        .iter()
        .any(|pipeline| matches!(pipeline.sink, SinkSpec::ExternalTable(_))));
    assert!(graph
        .pipelines
        .iter()
        .any(|pipeline| matches!(pipeline.source, SourceSpec::ExternalTable(_))));
}

fn external_project_plan() -> PhysicalPlan {
    let mut nodes = PhysicalPlanNodeArena::default();
    let mut children = PlanChildrenArena::default();
    let child = nodes.push(PhysicalPlanNode {
        id: PhysicalPlanNodeId::INVALID,
        output: RowType::new(vec!["a".to_string()], vec![LogicalType::Integer]),
        cardinality: None,
        kind: PhysicalNodeKind::DummyScan(crate::physical::specs::DummyScanSpec),
        children: PlanChildren::Empty,
        label: OperatorLabel::new(PlanNodeId::SYNTHETIC, "DUMMY_SCAN"),
    });
    let root = nodes.push(PhysicalPlanNode {
        id: PhysicalPlanNodeId::INVALID,
        output: RowType::new(vec!["a".to_string()], vec![LogicalType::Integer]),
        cardinality: None,
        kind: PhysicalNodeKind::ExternalProject(ExternalProjectSpec {
            routines: vec![external_routine_descriptor(RowSemantics::RowPreserving)]
                .into_boxed_slice(),
            expressions: Vec::new().into_boxed_slice(),
            cost: ExternalCostEstimate::default(),
            input_names: vec!["a".to_string()].into_boxed_slice(),
            input_types: vec![LogicalType::Integer].into_boxed_slice(),
            output_names: vec!["a".to_string()].into_boxed_slice(),
            output_types: vec![LogicalType::Integer].into_boxed_slice(),
        }),
        children: children.pack(vec![child]),
        label: OperatorLabel::new(PlanNodeId::SYNTHETIC, "EXTERNAL_PROJECT"),
    });

    PhysicalPlan::new(root, nodes, children, PlanPropertyMap::default())
}

fn external_table_plan() -> PhysicalPlan {
    external_single_node_plan(
        PhysicalNodeKind::ExternalTable(ExternalTableSpec {
            routine: external_routine_descriptor(RowSemantics::RelationExpanding),
            worker_output_types: vec![LogicalType::Integer].into_boxed_slice(),
            emitted_output_types: vec![LogicalType::Integer].into_boxed_slice(),
            argument_count: 0,
            lateral: false,
            parameterized: false,
            estimated_cardinality: 1,
            cost: ExternalCostEstimate::default(),
        }),
        "EXTERNAL_TABLE",
        RowType::new(vec!["a".to_string()], vec![LogicalType::Integer]),
    )
}

fn external_single_node_plan(
    kind: PhysicalNodeKind,
    display_name: &'static str,
    output: RowType,
) -> PhysicalPlan {
    let mut nodes = PhysicalPlanNodeArena::default();
    let root = nodes.push(PhysicalPlanNode {
        id: PhysicalPlanNodeId::INVALID,
        output,
        cardinality: None,
        kind,
        children: PlanChildren::Empty,
        label: OperatorLabel::new(PlanNodeId::SYNTHETIC, display_name),
    });

    PhysicalPlan::new(
        root,
        nodes,
        PlanChildrenArena::default(),
        PlanPropertyMap::default(),
    )
}

fn external_routine_descriptor(row_semantics: RowSemantics) -> ExternalRoutineDescriptor {
    ExternalRoutineDescriptor {
        label: "external_test".to_string(),
        identity: RoutineCallIdentity::Catalog {
            routine_id: RoutineId::from_raw(7),
            generation: 1,
        },
        semantics: RoutineSemantics {
            stability: RoutineStability::Volatile,
            null_policy: RoutineNullPolicy::CalledOnNullInput,
            side_effects: RoutineSideEffects::HasSideEffects,
            row_semantics,
            may_block: true,
        },
        spec: None,
    }
}
