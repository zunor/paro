// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::expression::Expression;
use crate::logical::plan::CardinalityEstimate;
use crate::physical::children::{PlanChildren, PlanChildrenArena};
use crate::physical::node::PhysicalPlanNode;
use crate::physical::plan::{PhysicalPlan, PhysicalPlanNodeArena};
use crate::physical::properties::PlanPropertyMap;
use crate::physical::specs::PhysicalNodeKind;
use crate::physical::{ExecutionResourceContract, PhysicalEdgeKind};

use super::*;
use crate::logical::plan::PlanNodeId;
use crate::physical::cost::MemoryCompletion;
use crate::physical::identity::{MutationBarrierId, SnapshotId};
use crate::physical::specs::{DummyScanSpec, MutationInputSpoolSpec};
use crate::physical::{InlinePlanChildren, ResourceGrantClassId};
use crate::physical::{OperatorLabel, RowType};
use paro_common::types::LogicalType;

fn dummy_plan(prefix_unreachable: bool, label: &str, output_name: &str) -> PhysicalPlan {
    let mut nodes = PhysicalPlanNodeArena::default();
    if prefix_unreachable {
        nodes.push(PhysicalPlanNode {
            id: PhysicalPlanNodeId::INVALID,
            output: RowType::new(Vec::new(), Vec::new()),
            cardinality: None,
            kind: PhysicalNodeKind::DummyScan(DummyScanSpec),
            children: PlanChildren::Empty,
            label: OperatorLabel::new(PlanNodeId::SYNTHETIC, "unreachable"),
        });
    }
    let root = nodes.push(PhysicalPlanNode {
        id: PhysicalPlanNodeId::INVALID,
        output: RowType::new(vec![output_name.to_string()], vec![LogicalType::Unknown]),
        cardinality: None,
        kind: PhysicalNodeKind::DummyScan(DummyScanSpec),
        children: PlanChildren::Empty,
        label: OperatorLabel::new(PlanNodeId::SYNTHETIC, label),
    });
    PhysicalPlan::new(
        root,
        nodes,
        PlanChildrenArena::default(),
        PlanPropertyMap::default(),
    )
}

#[test]
fn structural_identity_ignores_arena_and_display_allocation() {
    let left = dummy_plan(false, "left presentation", "value");
    let right = dummy_plan(true, "right presentation", "value");
    assert_eq!(
        left.structural_identity_fingerprint().unwrap(),
        right.structural_identity_fingerprint().unwrap()
    );
}

#[test]
fn structural_identity_includes_output_layout() {
    let left = dummy_plan(false, "same", "left");
    let right = dummy_plan(false, "same", "right");
    assert_ne!(
        left.structural_identity_fingerprint().unwrap(),
        right.structural_identity_fingerprint().unwrap()
    );
}

#[test]
fn structural_identity_tracks_graph_binding_and_predicate() {
    use crate::physical::specs::GraphScanSpec;
    let mut plan = dummy_plan(false, "graph", "id");
    plan.nodes.get_mut(plan.root).unwrap().kind =
        PhysicalNodeKind::GraphScan(Box::new(GraphScanSpec {
            vertex_info: paro_catalog::entry::VertexTableInfo {
                table_name: "vertices".into(),
                table_oid: 19,
                key_column_ids: vec![0],
                label: "v".into(),
                property_column_ids: vec![1],
            },
            filter: None,
            table_index: 3,
            label: "v".into(),
            graph_name: "g".into(),
            schema_name: "public".into(),
            output_types: Box::new([LogicalType::Integer]),
        }));
    let identity = plan.structural_identity_fingerprint().unwrap();
    let changes: [fn(&mut GraphScanSpec); 4] = [
        |s| s.vertex_info.table_oid += 1,
        |s| s.vertex_info.key_column_ids = vec![1],
        |s| s.table_index += 1,
        |s| {
            s.filter = Some(Expression::Constant(
                crate::expression::ConstantExpression::new(
                    paro_common::runtime_value::Value::Boolean(false),
                    LogicalType::Boolean,
                )
                .into(),
            ))
        },
    ];
    for change in changes {
        let mut changed = plan.clone();
        let PhysicalNodeKind::GraphScan(spec) =
            &mut changed.nodes.get_mut(changed.root).unwrap().kind
        else {
            unreachable!()
        };
        change(spec);
        assert_ne!(identity, changed.structural_identity_fingerprint().unwrap());
    }
}

#[test]
fn structural_identity_excludes_cost_and_resource_operating_point() {
    let mut left = dummy_plan(false, "same", "value");
    let mut right = dummy_plan(false, "same", "value");
    left.nodes
        .get_mut(left.root)
        .expect("dummy root")
        .cardinality = Some(CardinalityEstimate::exact(1));
    right
        .nodes
        .get_mut(right.root)
        .expect("dummy root")
        .cardinality = Some(CardinalityEstimate::exact(99));
    right.execution_resources = Some(ExecutionResourceContract {
        class: ResourceGrantClassId(2),
        minimum_memory_bytes: 1,
        working_set_memory_bytes: 2,
        memory_ceiling_bytes: 3,
        memory_completion: MemoryCompletion::Guaranteed,
        max_parallel_tasks: 4,
        external_worker_slots: 0,
    });
    assert_eq!(
        left.structural_identity_fingerprint().unwrap(),
        right.structural_identity_fingerprint().unwrap()
    );
}

#[test]
fn external_identity_tracks_routine_generation_and_call_shape() {
    use crate::physical::specs::{ExternalRoutineDescriptor, ExternalTableSpec};
    use paro_external::routine::identity::RoutineCallIdentity;
    use paro_external::routine::spec::*;
    let mut plan = dummy_plan(false, "external", "value");
    plan.nodes.get_mut(plan.root).unwrap().kind =
        PhysicalNodeKind::ExternalTable(ExternalTableSpec {
            routine: ExternalRoutineDescriptor {
                label: "not an identity".into(),
                identity: RoutineCallIdentity::Catalog {
                    routine_id: RoutineId(3),
                    generation: 4,
                },
                semantics: RoutineSemantics {
                    stability: RoutineStability::Volatile,
                    null_policy: RoutineNullPolicy::CalledOnNullInput,
                    side_effects: RoutineSideEffects::HasSideEffects,
                    row_semantics: RowSemantics::RelationExpanding,
                    may_block: true,
                },
                spec: None,
            },
            worker_output_types: Box::new([LogicalType::Integer]),
            emitted_output_types: Box::new([LogicalType::Integer]),
            argument_count: 1,
            lateral: true,
            parameterized: true,
            estimated_cardinality: 100,
            cost: Default::default(),
        });
    let identity = plan.structural_identity_fingerprint().unwrap();
    let changes: [fn(&mut ExternalTableSpec); 4] = [
        |s| {
            s.routine.identity = RoutineCallIdentity::Catalog {
                routine_id: RoutineId(3),
                generation: 5,
            }
        },
        |s| s.routine.semantics.null_policy = RoutineNullPolicy::Strict,
        |s| s.parameterized = false,
        |s| s.worker_output_types = Box::new([LogicalType::BigInt]),
    ];
    for change in changes {
        let mut changed = plan.clone();
        let PhysicalNodeKind::ExternalTable(spec) =
            &mut changed.nodes.get_mut(changed.root).unwrap().kind
        else {
            unreachable!()
        };
        change(spec);
        assert_ne!(identity, changed.structural_identity_fingerprint().unwrap());
    }
}

#[test]
fn structural_identity_covers_delimiter_capture_and_scan_contracts() {
    use crate::expression::{Expression, ReferenceExpression};
    use crate::physical::specs::{
        DelimJoinSideSpec, DelimJoinSpec, DelimScanSpec, DelimScanTarget,
    };
    fn fingerprint(kind: PhysicalNodeKind) -> Fingerprint {
        let mut builder = StableFingerprintBuilder::default();
        write_semantic_kind_fields(&mut builder, &kind).unwrap();
        builder.finish()
    }
    let scan = |target| {
        PhysicalNodeKind::DelimScan(DelimScanSpec {
            target,
            output_names: Box::new(["k".into()]),
            output_types: Box::new([LogicalType::Integer]),
        })
    };
    assert_ne!(
        fingerprint(scan(DelimScanTarget::CachedOuter)),
        fingerprint(scan(DelimScanTarget::Values { table_index: 0 }))
    );
    assert_ne!(
        fingerprint(scan(DelimScanTarget::Values { table_index: 1 })),
        fingerprint(scan(DelimScanTarget::Values { table_index: 0 }))
    );
    let mut join = DelimJoinSpec {
        side: DelimJoinSideSpec::Left,
        duplicate_keys: Box::new([Expression::Reference(
            ReferenceExpression::new(0, LogicalType::Integer).into(),
        )]),
        output_names: Box::new(["k".into()]),
        output_types: Box::new([LogicalType::Integer]),
    };
    let original = fingerprint(PhysicalNodeKind::DelimJoin(join.clone()));
    join.side = DelimJoinSideSpec::Right;
    assert_ne!(
        original,
        fingerprint(PhysicalNodeKind::DelimJoin(join.clone()))
    );
    join.side = DelimJoinSideSpec::Left;
    join.duplicate_keys = Box::new([]);
    assert_ne!(original, fingerprint(PhysicalNodeKind::DelimJoin(join)));
}

#[test]
fn structural_identity_covers_cross_product_and_nested_aggregate_payload() {
    use crate::physical::specs::{
        AggregateSpec, CrossProductSpec, PartitionAggregateDomain, PartitionAggregateWindowSpec,
        SpillExecutionPolicy,
    };

    fn fingerprint(kind: PhysicalNodeKind) -> Fingerprint {
        let mut builder = StableFingerprintBuilder::default();
        write_semantic_kind_fields(&mut builder, &kind).unwrap();
        builder.finish()
    }

    let mut cross = CrossProductSpec {
        left_output_types: Box::new([LogicalType::Integer]),
        right_output_types: Box::new([LogicalType::BigInt]),
        output_names: Box::new(["a".into(), "b".into()]),
        output_types: Box::new([LogicalType::Integer, LogicalType::BigInt]),
        spill_policy: SpillExecutionPolicy::Adaptive,
    };
    let original = fingerprint(PhysicalNodeKind::CrossProduct(cross.clone()));
    cross.spill_policy = SpillExecutionPolicy::ForcedExternal;
    assert_ne!(
        original,
        fingerprint(PhysicalNodeKind::CrossProduct(cross.clone()))
    );
    cross.spill_policy = SpillExecutionPolicy::Adaptive;
    cross.left_output_types = Box::new([LogicalType::BigInt]);
    assert_ne!(original, fingerprint(PhysicalNodeKind::CrossProduct(cross)));

    let aggregate = AggregateSpec {
        grouping_key_count: 0,
        initial_lookup_hash_key_count: 0,
        state_output_projection: Box::new([]),
        estimated_input_rows: Some(10),
        projection_exprs: Box::new([]),
        payload_types: Box::new([]),
        groups: Box::new([]),
        group_key_encodings: Box::new([]),
        grouping_sets: Box::new([]),
        aggregates: Box::new([]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([]),
        aggregate_filters: Box::new([]),
        aggregate_orders: Box::new([]),
        post_reduction: None,
        having_filter: Box::new([]),
        spill_policy: SpillExecutionPolicy::Adaptive,
        perfect_hash: None,
        output_names: Box::new([]),
        output_types: Box::new([]),
    };
    let mut window = PartitionAggregateWindowSpec {
        domain: PartitionAggregateDomain::Global,
        input_types: Box::new([LogicalType::Integer]),
        detail_columns: Box::new([0]),
        aggregate,
        output_names: Box::new(["a".into()]),
        output_types: Box::new([LogicalType::Integer]),
    };
    let original = fingerprint(PhysicalNodeKind::PartitionAggregateWindow(Box::new(
        window.clone(),
    )));
    window.aggregate.estimated_input_rows = Some(100);
    assert_eq!(
        original,
        fingerprint(PhysicalNodeKind::PartitionAggregateWindow(Box::new(
            window.clone()
        )))
    );
    window.aggregate.spill_policy = SpillExecutionPolicy::ForcedExternal;
    assert_ne!(
        original,
        fingerprint(PhysicalNodeKind::PartitionAggregateWindow(Box::new(
            window.clone()
        )))
    );
    window.aggregate.spill_policy = SpillExecutionPolicy::Adaptive;
    window.detail_columns = Box::new([]);
    assert_ne!(
        original,
        fingerprint(PhysicalNodeKind::PartitionAggregateWindow(Box::new(window)))
    );
}

#[test]
fn structural_identity_fails_closed_for_cycles_and_invalid_edges() {
    let mut cyclic = dummy_plan(false, "same", "value");
    cyclic.nodes.get_mut(cyclic.root).unwrap().children =
        PlanChildren::Inline(InlinePlanChildren::new(&[cyclic.root]));
    assert_eq!(
        cyclic.structural_identity_fingerprint(),
        Err(PhysicalIdentityError::Cycle)
    );

    let mut invalid_edge = dummy_plan(false, "same", "value");
    invalid_edge.edges.push(
        invalid_edge.root,
        PhysicalPlanNodeId::INVALID,
        PhysicalEdgeKind::Data,
    );
    assert_eq!(
        invalid_edge.structural_identity_fingerprint(),
        Err(PhysicalIdentityError::InvalidEdge)
    );
}

#[test]
fn table_function_identity_tracks_payload_and_rejects_opaque_binding() {
    use crate::physical::specs::TableFunctionScanSpec;
    use paro_function::table::{BoundTableFunctionData, TableFunction, TableFunctionBindData};
    use std::sync::Arc;

    #[derive(Clone)]
    struct Opaque;
    impl TableFunctionBindData for Opaque {
        fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
            Box::new(self.clone())
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    let mut plan = dummy_plan(false, "function", "value");
    let spec = TableFunctionScanSpec {
        function: Arc::new(TableFunction::new("catalog_rows", vec![])),
        bind_data: None,
        table_index: 0,
        arguments: Box::new([]),
        projection_ids: None,
        input_table_types: Box::new([]),
        input_table_names: Box::new([]),
        output_names: Box::new(["value".to_string()]),
        output_types: Box::new([LogicalType::BigInt]),
        with_ordinality: false,
    };
    plan.nodes.get_mut(plan.root).unwrap().kind = PhysicalNodeKind::TableFunctionScan(spec.clone());
    let original = plan.structural_identity_fingerprint().unwrap();
    let mut changed = spec;
    changed.with_ordinality = true;
    plan.nodes.get_mut(plan.root).unwrap().kind =
        PhysicalNodeKind::TableFunctionScan(changed.clone());
    assert_ne!(original, plan.structural_identity_fingerprint().unwrap());
    changed.bind_data = Some(BoundTableFunctionData::new(Box::new(Opaque)));
    plan.nodes.get_mut(plan.root).unwrap().kind = PhysicalNodeKind::TableFunctionScan(changed);
    assert_eq!(
        plan.structural_identity_fingerprint(),
        Err(PhysicalIdentityError::UnsupportedKind {
            kind: "TABLE_FUNCTION_SCAN"
        })
    );
}

#[test]
fn mutation_spool_identity_tracks_the_execution_contract() {
    let mut nodes = PhysicalPlanNodeArena::default();
    let root = nodes.push(PhysicalPlanNode {
        id: PhysicalPlanNodeId::INVALID,
        output: RowType::new(Vec::new(), Vec::new()),
        cardinality: None,
        kind: PhysicalNodeKind::MutationInputSpool(MutationInputSpoolSpec {
            barrier: MutationBarrierId::new(0),
            targets: Default::default(),
            snapshot: SnapshotId::new(0),
        }),
        children: PlanChildren::Empty,
        label: OperatorLabel::new(PlanNodeId::SYNTHETIC, "values"),
    });
    let mut plan = PhysicalPlan::new(
        root,
        nodes,
        PlanChildrenArena::default(),
        PlanPropertyMap::default(),
    );
    let before = plan.structural_identity_fingerprint().unwrap();
    let PhysicalNodeKind::MutationInputSpool(spec) = &mut plan.nodes.get_mut(root).unwrap().kind
    else {
        unreachable!()
    };
    spec.snapshot = SnapshotId::new(1);
    assert_ne!(before, plan.structural_identity_fingerprint().unwrap());
    let PhysicalNodeKind::MutationInputSpool(spec) = &mut plan.nodes.get_mut(root).unwrap().kind
    else {
        unreachable!()
    };
    spec.snapshot = SnapshotId::new(0);
    spec.barrier = MutationBarrierId::new(1);
    assert_ne!(before, plan.structural_identity_fingerprint().unwrap());
}
