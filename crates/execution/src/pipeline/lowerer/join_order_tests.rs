// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_optimizer::physical::{OutputPermutation, ProjectSpec};

fn enable_runtime_filter(mut spec: HashJoinSpec) -> HashJoinSpec {
    spec.runtime_filter = Some(paro_optimizer::physical::HashJoinRuntimeFilterSpec {
        artifact: paro_optimizer::physical::identity::Fingerprint(7),
        wait_policy: paro_optimizer::physical::RuntimeFilterWaitPolicy::WaitComplete,
        condition_indices: Box::new([0]),
        resource: paro_optimizer::physical::RuntimeFilterResourceContract::for_keys(
            &[paro_common::types::LogicalType::Integer],
            1,
        )
        .unwrap(),
    });
    spec
}

#[test]
fn projection_above_hash_join_stays_after_probe() {
    let plan = projection_above_hash_join_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 3);
    assert!(matches!(
        graph.pipelines[1].transforms.as_slice(),
        [TransformSpec::HashJoinProbe(_), TransformSpec::Project(_)]
    ));
    assert!(matches!(
        graph.pipelines[2].source,
        SourceSpec::HashJoinSpillReplay(_)
    ));
    assert!(matches!(
        graph.pipelines[2].transforms.as_slice(),
        [TransformSpec::Project(_)]
    ));
    assert_eq!(graph.pipelines[1].output.names.as_ref(), ["lv"]);
    assert_eq!(
        graph.pipelines[1].output.types.as_ref(),
        [LogicalType::Integer]
    );
}

#[test]
fn union_all_probe_sources_share_one_build_and_runtime_filter_artifact() {
    let mut plan = union_all_probe_hash_join_plan();
    let physical_node_count = plan.nodes.len();
    assert_eq!(
        plan.nodes
            .iter()
            .filter(|node| matches!(node.kind, PhysicalNodeKind::HashJoin(_)))
            .count(),
        1,
        "the selected physical plan owns exactly one join"
    );

    let join_children = plan.child_ids(&plan.node(plan.root).children).to_vec();
    let [probe_root, build_root] = join_children.as_slice() else {
        panic!("hash join should have probe and build children");
    };
    let mut pending = vec![*probe_root];
    let mut probe_scans = Vec::new();
    while let Some(node_id) = pending.pop() {
        let children = plan.child_ids(&plan.node(node_id).children).to_vec();
        if matches!(plan.node(node_id).kind, PhysicalNodeKind::Values(_)) {
            plan.nodes.get_mut(node_id).unwrap().kind =
                PhysicalNodeKind::RowsetScan(rowset_spec_for_test());
            probe_scans.push(node_id);
        } else {
            pending.extend(children);
        }
    }
    assert_eq!(probe_scans.len(), 3);

    let artifact = paro_optimizer::physical::identity::Fingerprint(7);
    let PhysicalNodeKind::HashJoin(spec) = &mut plan.nodes.get_mut(plan.root).unwrap().kind else {
        panic!("expected hash join root");
    };
    *spec = enable_runtime_filter(spec.clone());
    for &scan in &probe_scans {
        plan.edges.push(
            *build_root,
            scan,
            paro_optimizer::physical::PhysicalEdgeKind::RuntimeFilter(artifact),
        );
    }

    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(plan.nodes.len(), physical_node_count);
    assert_eq!(
        plan.nodes
            .iter()
            .filter(|node| matches!(node.kind, PhysicalNodeKind::HashJoin(_)))
            .count(),
        1,
        "pipeline decomposition must not rewrite or copy the Memo winner"
    );
    let hash_builds = graph
        .pipelines
        .iter()
        .filter_map(|pipeline| match &pipeline.sink {
            SinkSpec::HashJoinBuild(build) => Some((pipeline.id, build)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [(build_pipeline, hash_build)] = hash_builds.as_slice() else {
        panic!("all UNION ALL sources must share exactly one hash build");
    };
    assert_eq!(
        hash_build.runtime_filter.as_ref().unwrap().artifact,
        artifact
    );

    let probe_pipelines = graph
        .pipelines
        .iter()
        .filter_map(|pipeline| {
            pipeline
                .transforms
                .iter()
                .find_map(|transform| match transform {
                    TransformSpec::HashJoinProbe(probe) if probe.handle == hash_build.handle => {
                        Some(pipeline)
                    }
                    _ => None,
                })
        })
        .collect::<Vec<_>>();
    assert_eq!(probe_pipelines.len(), 3);
    for pipeline in &probe_pipelines {
        assert!(matches!(
            pipeline.transforms.as_slice(),
            [
                TransformSpec::Filter(_),
                TransformSpec::Filter(_),
                TransformSpec::Project(_),
                TransformSpec::HashJoinProbe(_)
            ]
        ));
        let SourceSpec::Rowset(source) = &pipeline.source else {
            panic!("each physical UNION ALL leaf should remain a rowset source");
        };
        assert_eq!(source.dynamic_runtime_filters.len(), 1);
        assert_eq!(source.dynamic_runtime_filters[0].handle, hash_build.handle);
        assert_eq!(source.dynamic_runtime_filters[0].artifact, artifact);
    }
    assert_eq!(
        graph
            .dependencies
            .iter()
            .filter(|dependency| {
                dependency.producer == *build_pipeline
                    && dependency.kind == DependencyKind::BuildBeforeProbe
            })
            .count(),
        3
    );
}

#[test]
fn union_all_probe_source_collection_uses_an_explicit_stack() {
    let mut plan = union_all_probe_hash_join_plan();
    let join_children = plan.child_ids(&plan.node(plan.root).children).to_vec();
    let [original_probe, build] = join_children.as_slice() else {
        panic!("hash join should have probe and build children");
    };
    let mut probe = *original_probe;
    const DEPTH: usize = 4_096;
    for _ in 0..DEPTH {
        let child = plan.children.pack(vec![probe]);
        probe = plan.nodes.push(PhysicalPlanNode {
            id: PhysicalPlanNodeId::INVALID,
            output: RowType::new(vec!["probe_key".to_string()], vec![LogicalType::Integer]),
            cardinality: None,
            kind: PhysicalNodeKind::Project(ProjectSpec {
                expressions: vec![Expression::Reference(
                    ReferenceExpression::new(0, LogicalType::Integer).into(),
                )]
                .into_boxed_slice(),
                output_names: vec!["probe_key".to_string()].into_boxed_slice(),
                visible_count: 1,
            }),
            children: child,
            label: OperatorLabel::new(PlanNodeId::SYNTHETIC, "PROJECT"),
        });
    }
    let join_children = plan.children.pack(vec![probe, *build]);
    plan.nodes.get_mut(plan.root).unwrap().children = join_children;

    let lowerer = PipelineLowerer::new(&plan);
    let sources = lowerer
        .collect_union_all_probe_sources(probe)
        .unwrap()
        .expect("nested physical UNION ALL should expose its leaves");
    assert_eq!(sources.len(), 3);
    assert!(sources
        .iter()
        .all(|source| source.transforms.len() == DEPTH + 3));
}

#[test]
fn left_deep_hash_join_chain_stays_in_one_probe_pipeline() {
    let plan = left_deep_hash_join_plan_with_context(ExtractionContext::default());
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 4);
    assert!(matches!(
        graph.pipelines[0].sink,
        SinkSpec::HashJoinBuild(_)
    ));
    assert!(matches!(
        graph.pipelines[1].sink,
        SinkSpec::HashJoinBuild(_)
    ));
    assert!(matches!(
        graph.pipelines[2].transforms.as_slice(),
        [
            TransformSpec::HashJoinProbe(_),
            TransformSpec::HashJoinProbe(_)
        ]
    ));
    assert!(matches!(
        graph.pipelines[3].source,
        SourceSpec::HashJoinSpillReplay(_)
    ));
    assert_eq!(graph.dependencies.len(), 3);
    assert_eq!(
        graph
            .dependencies
            .iter()
            .filter(|dependency| dependency.kind == DependencyKind::BuildBeforeProbe)
            .count(),
        2
    );
    assert_eq!(
        graph
            .dependencies
            .iter()
            .filter(|dependency| dependency.kind == DependencyKind::ProbeBeforeSpillReplay)
            .count(),
        1
    );
}

#[test]
fn left_deep_spillable_hash_join_chain_replays_every_fused_join() {
    let plan = left_deep_hash_join_plan_with_context(ExtractionContext {
        grant_spill_policy: paro_optimizer::physical::SpillPolicy::Allowed,
        max_memory: 64 * 1024 * 1024,
        max_threads: 4,
        ..ExtractionContext::default()
    });
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 5);
    assert!(matches!(
        graph.pipelines[2].transforms.as_slice(),
        [
            TransformSpec::HashJoinProbe(_),
            TransformSpec::HashJoinProbe(_)
        ]
    ));
    assert!(matches!(
        graph.pipelines[3].source,
        SourceSpec::HashJoinSpillReplay(_)
    ));
    assert!(matches!(
        graph.pipelines[3].transforms.as_slice(),
        [TransformSpec::HashJoinProbe(_)]
    ));
    assert!(matches!(
        graph.pipelines[4].source,
        SourceSpec::HashJoinSpillReplay(_)
    ));
    assert!(graph.pipelines[4].transforms.is_empty());
    let replay_edges = graph
        .dependencies
        .iter()
        .filter(|dependency| dependency.kind == DependencyKind::ProbeBeforeSpillReplay)
        .collect::<Vec<_>>();
    assert_eq!(replay_edges.len(), 2);
    assert_eq!(replay_edges[0].producer, PipelineId::new(2));
    assert_eq!(replay_edges[0].consumer, PipelineId::new(3));
    assert_eq!(replay_edges[1].producer, PipelineId::new(3));
    assert_eq!(replay_edges[1].consumer, PipelineId::new(4));
}

#[test]
fn direct_rowset_probe_gets_hash_join_runtime_filter_gate() {
    let plan = hash_join_plan(JoinType::Inner);
    let lowerer = PipelineLowerer::new(&plan);
    let spec = match &plan.node(plan.root).kind {
        PhysicalNodeKind::HashJoin(spec) => enable_runtime_filter(spec.clone()),
        _ => panic!("expected hash join plan"),
    };
    let source = SourceSpec::Rowset(RowsetSourceSpec::new(rowset_spec_for_test()));
    let source =
        lowerer.attach_hash_join_runtime_filters(source, &[], BreakerHandleId::new(3), &spec);

    let SourceSpec::Rowset(rowset) = source else {
        panic!("expected rowset source");
    };
    assert_eq!(rowset.dynamic_runtime_filters.len(), 1);
    assert_eq!(
        rowset.dynamic_runtime_filters[0].handle,
        BreakerHandleId::new(3)
    );
    assert_eq!(
        rowset.dynamic_runtime_filters[0].runtime_filter_key_index,
        0
    );
    assert_eq!(rowset.dynamic_runtime_filters[0].probe_column_id, 0);
    assert_eq!(
        rowset.dynamic_runtime_filters[0].artifact,
        paro_optimizer::physical::identity::Fingerprint(7)
    );
}

#[test]
fn exact_unique_payload_free_probe_is_covered_only_by_its_rowset_filter() {
    let plan = hash_join_plan(JoinType::Inner);
    let lowerer = PipelineLowerer::new(&plan);
    let mut spec = match &plan.node(plan.root).kind {
        PhysicalNodeKind::HashJoin(spec) => enable_runtime_filter(spec.clone()),
        _ => panic!("expected hash join plan"),
    };
    spec.build_keys_unique = true;
    spec.build_output_count = 0;
    spec.build_input_projection = Box::new([]);
    spec.build_payload_types = Box::new([]);
    spec.output_names = spec
        .left_output_types
        .iter()
        .map(|_| "probe".into())
        .collect();
    spec.output_types = spec.left_output_types.clone();
    spec.output_permutation = OutputPermutation::identity(spec.output_types.len());

    let handle = BreakerHandleId::new(3);
    let mut transforms = vec![hash_join_probe_transform(handle, &spec)];
    let TransformSpec::HashJoinProbe(probe) = &transforms[0] else {
        panic!("expected hash join probe");
    };
    assert_eq!(probe.covering_runtime_filter_key, None);
    let source = lowerer.attach_hash_join_runtime_filters(
        SourceSpec::Rowset(RowsetSourceSpec::new(rowset_spec_for_test())),
        &[],
        handle,
        &spec,
    );
    super::super::pipelines::confirm_covering_runtime_filters(&source, &mut transforms);
    let TransformSpec::HashJoinProbe(probe) = &transforms[0] else {
        panic!("expected hash join probe");
    };
    assert_eq!(probe.covering_runtime_filter_key, Some(0));

    let mut unmatched = transforms;
    let unrelated_source = SourceSpec::Rowset(RowsetSourceSpec::new(rowset_spec_for_test()));
    super::super::pipelines::confirm_covering_runtime_filters(&unrelated_source, &mut unmatched);
    let TransformSpec::HashJoinProbe(probe) = &unmatched[0] else {
        panic!("expected hash join probe");
    };
    assert_eq!(probe.covering_runtime_filter_key, None);
}

#[test]
fn exact_payload_free_semi_probe_does_not_require_unique_build_keys() {
    let plan = hash_join_plan(JoinType::Semi);
    let lowerer = PipelineLowerer::new(&plan);
    let mut spec = match &plan.node(plan.root).kind {
        PhysicalNodeKind::HashJoin(spec) => enable_runtime_filter(spec.clone()),
        _ => panic!("expected hash join plan"),
    };
    spec.build_keys_unique = false;
    spec.build_output_count = 0;
    spec.build_input_projection = Box::new([]);
    spec.build_payload_types = Box::new([]);

    let handle = BreakerHandleId::new(3);
    let mut transforms = vec![hash_join_probe_transform(handle, &spec)];
    let source = lowerer.attach_hash_join_runtime_filters(
        SourceSpec::Rowset(RowsetSourceSpec::new(rowset_spec_for_test())),
        &[],
        handle,
        &spec,
    );
    super::super::pipelines::confirm_covering_runtime_filters(&source, &mut transforms);
    let TransformSpec::HashJoinProbe(probe) = &transforms[0] else {
        panic!("expected hash join probe");
    };
    assert_eq!(probe.covering_runtime_filter_key, Some(0));
}

#[test]
fn runtime_filter_does_not_cover_non_unique_or_payload_probe() {
    let plan = hash_join_plan(JoinType::Inner);
    let mut spec = match &plan.node(plan.root).kind {
        PhysicalNodeKind::HashJoin(spec) => enable_runtime_filter(spec.clone()),
        _ => panic!("expected hash join plan"),
    };
    assert!(matches!(
        hash_join_probe_transform(BreakerHandleId::new(3), &spec),
        TransformSpec::HashJoinProbe(HashJoinProbeSpec {
            covering_runtime_filter_key: None,
            ..
        })
    ));

    spec.build_keys_unique = true;
    assert!(spec.build_output_count > 0);
    assert!(matches!(
        hash_join_probe_transform(BreakerHandleId::new(3), &spec),
        TransformSpec::HashJoinProbe(HashJoinProbeSpec {
            covering_runtime_filter_key: None,
            ..
        })
    ));
}

#[test]
fn hash_join_without_auxiliary_contract_does_not_install_runtime_filter() {
    let plan = hash_join_plan(JoinType::Inner);
    let lowerer = PipelineLowerer::new(&plan);
    let spec = match &plan.node(plan.root).kind {
        PhysicalNodeKind::HashJoin(spec) => spec.clone(),
        _ => panic!("expected hash join plan"),
    };
    assert!(spec.runtime_filter.is_none());
    let source = SourceSpec::Rowset(RowsetSourceSpec::new(rowset_spec_for_test()));
    let source =
        lowerer.attach_hash_join_runtime_filters(source, &[], BreakerHandleId::new(3), &spec);
    let SourceSpec::Rowset(rowset) = source else {
        panic!("expected rowset source");
    };
    assert!(rowset.dynamic_runtime_filters.is_empty());
}

#[test]
fn left_deep_probe_traces_runtime_filter_to_rowset_column() {
    let plan = hash_join_plan(JoinType::Inner);
    let lowerer = PipelineLowerer::new(&plan);
    let spec = match &plan.node(plan.root).kind {
        PhysicalNodeKind::HashJoin(spec) => enable_runtime_filter(spec.clone()),
        _ => panic!("expected hash join plan"),
    };
    let prior_probe = hash_join_probe_transform(BreakerHandleId::new(2), &spec);
    let source = SourceSpec::Rowset(RowsetSourceSpec::new(rowset_spec_for_test()));
    let source = lowerer.attach_hash_join_runtime_filters(
        source,
        &[prior_probe],
        BreakerHandleId::new(3),
        &spec,
    );

    let SourceSpec::Rowset(rowset) = source else {
        panic!("expected rowset source");
    };
    assert_eq!(rowset.dynamic_runtime_filters.len(), 1);
    assert_eq!(rowset.dynamic_runtime_filters[0].probe_column_id, 0);
    assert_eq!(
        rowset.dynamic_runtime_filters[0].handle,
        BreakerHandleId::new(3)
    );
}

#[test]
fn passthrough_projection_traces_runtime_filter_to_rowset_column() {
    let plan = hash_join_plan(JoinType::Inner);
    let lowerer = PipelineLowerer::new(&plan);
    let spec = match &plan.node(plan.root).kind {
        PhysicalNodeKind::HashJoin(spec) => enable_runtime_filter(spec.clone()),
        _ => panic!("expected hash join plan"),
    };
    let project = TransformSpec::Project(ProjectSpec {
        expressions: vec![Expression::Reference(
            ReferenceExpression::new(0, LogicalType::Integer).into(),
        )]
        .into_boxed_slice(),
        output_names: vec!["key".to_string()].into_boxed_slice(),
        visible_count: 1,
    });
    let source = SourceSpec::Rowset(RowsetSourceSpec::new(rowset_spec_for_test()));
    let source = lowerer.attach_hash_join_runtime_filters(
        source,
        &[project],
        BreakerHandleId::new(3),
        &spec,
    );

    let SourceSpec::Rowset(rowset) = source else {
        panic!("expected rowset source");
    };
    assert_eq!(rowset.dynamic_runtime_filters.len(), 1);
    assert_eq!(rowset.dynamic_runtime_filters[0].probe_column_id, 0);
}

#[test]
fn derived_projection_is_a_runtime_filter_lineage_barrier() {
    let plan = hash_join_plan(JoinType::Inner);
    let lowerer = PipelineLowerer::new(&plan);
    let spec = match &plan.node(plan.root).kind {
        PhysicalNodeKind::HashJoin(spec) => enable_runtime_filter(spec.clone()),
        _ => panic!("expected hash join plan"),
    };
    let project = TransformSpec::Project(ProjectSpec {
        expressions: vec![Expression::Constant(
            ConstantExpression::new(Value::Integer(7), LogicalType::Integer).into(),
        )]
        .into_boxed_slice(),
        output_names: vec!["derived".to_string()].into_boxed_slice(),
        visible_count: 1,
    });
    let source = SourceSpec::Rowset(RowsetSourceSpec::new(rowset_spec_for_test()));
    let source = lowerer.attach_hash_join_runtime_filters(
        source,
        &[project],
        BreakerHandleId::new(3),
        &spec,
    );

    let SourceSpec::Rowset(rowset) = source else {
        panic!("expected rowset source");
    };
    assert!(rowset.dynamic_runtime_filters.is_empty());
}

#[test]
fn left_deep_probe_does_not_trace_build_payload_to_rowset() {
    let plan = hash_join_plan(JoinType::Inner);
    let lowerer = PipelineLowerer::new(&plan);
    let mut spec = match &plan.node(plan.root).kind {
        PhysicalNodeKind::HashJoin(spec) => enable_runtime_filter(spec.clone()),
        _ => panic!("expected hash join plan"),
    };
    let prior_probe = hash_join_probe_transform(BreakerHandleId::new(2), &spec);
    spec.key_conditions[0].left =
        Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer).into());
    let source = SourceSpec::Rowset(RowsetSourceSpec::new(rowset_spec_for_test()));
    let source = lowerer.attach_hash_join_runtime_filters(
        source,
        &[prior_probe],
        BreakerHandleId::new(3),
        &spec,
    );

    let SourceSpec::Rowset(rowset) = source else {
        panic!("expected rowset source");
    };
    assert!(rowset.dynamic_runtime_filters.is_empty());
}

#[test]
fn order_lowers_to_sort_build_emit_breaker_pipelines() {
    let plan = order_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 2);
    assert!(matches!(graph.pipelines[0].sink, SinkSpec::SortBuild(_)));
    assert!(matches!(graph.pipelines[1].source, SourceSpec::SortEmit(_)));
    assert_eq!(graph.dependencies.len(), 1);
    assert_eq!(
        graph.dependencies[0].kind,
        DependencyKind::FinalizeBeforeEmit
    );
    assert_eq!(graph.handles.len(), 1);
}

#[test]
fn projection_above_order_stays_after_sort_emit() {
    let plan = order_with_final_projection_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 2);
    assert!(matches!(graph.pipelines[0].sink, SinkSpec::SortBuild(_)));
    assert!(matches!(graph.pipelines[1].source, SourceSpec::SortEmit(_)));
    assert!(matches!(
        graph.pipelines[1].transforms.as_slice(),
        [TransformSpec::Project(_)]
    ));
    assert_eq!(graph.pipelines[1].output.names.as_ref(), ["a"]);
    assert_eq!(
        graph.dependencies[0].kind,
        DependencyKind::FinalizeBeforeEmit
    );
}

#[test]
fn partitioned_window_lowers_to_build_emit_breaker_pipelines() {
    let plan = partitioned_window_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 2);
    assert!(matches!(graph.pipelines[0].sink, SinkSpec::WindowBuild(_)));
    assert!(matches!(
        graph.pipelines[1].source,
        SourceSpec::WindowEmit(_)
    ));
    assert_eq!(graph.dependencies.len(), 1);
    assert_eq!(
        graph.dependencies[0].kind,
        DependencyKind::FinalizeBeforeEmit
    );
    assert_eq!(graph.handles.len(), 1);
}

#[test]
fn rowset_source_properties_keep_morsel_partitioning() {
    let source = SourceSpec::Rowset(RowsetSourceSpec::new(rowset_spec_for_test()));
    let build = PipelinePropertyAccumulator::start_from_source(&source)
        .close_with_sink(&SinkSpec::ClientResult(ClientResultSpec::default()));

    assert_eq!(build.capabilities.morsel, MorselCapability::Source);
    assert!(build.capabilities.supports_late_materialization);
}

#[test]
fn dummy_and_empty_sources_are_single_task() {
    let ctx = BindContext::new();
    let mut extractor = PhysicalPlanExtractor::new(ExtractionContext::default());
    let dummy = extractor
        .extract(OwnedLogicalPlan::new(&ctx, LogicalOperator::DummyScan))
        .unwrap();
    let mut dummy_lowerer = PipelineLowerer::new(&dummy);
    let dummy_graph = dummy_lowerer.lower_to_pipeline_graph(dummy.root).unwrap();

    assert!(matches!(
        dummy_graph.pipelines[0].source,
        SourceSpec::Dummy(_)
    ));
    assert_eq!(
        dummy_graph.pipelines[0].properties.capabilities.parallelism,
        crate::physical::properties::Parallelism::single()
    );

    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let mut extractor = PhysicalPlanExtractor::new(ExtractionContext::default());
    let empty = extractor
        .extract(OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::EmptyResult(EmptyResult::new(values)),
        ))
        .unwrap();
    let mut empty_lowerer = PipelineLowerer::new(&empty);
    let empty_graph = empty_lowerer.lower_to_pipeline_graph(empty.root).unwrap();

    assert!(matches!(
        empty_graph.pipelines[0].source,
        SourceSpec::Empty(_)
    ));
    assert_eq!(
        empty_graph.pipelines[0].properties.capabilities.parallelism,
        crate::physical::properties::Parallelism::single()
    );
}

#[test]
fn graph_validation_rejects_dependency_cycles() {
    let plan = linear_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let mut pipelines = Vec::new();
    let sink = SinkSpec::ClientResult(ClientResultSpec::default());

    let first = lowerer
        .lower_linear_pipeline(
            plan.root,
            sink.clone(),
            SinkSharing::Exclusive,
            &mut pipelines,
        )
        .unwrap();
    let second = lowerer
        .lower_linear_pipeline(plan.root, sink, SinkSharing::Exclusive, &mut pipelines)
        .unwrap();

    let graph = PipelineGraph {
        pipelines,
        dependencies: vec![
            PipelineDependency {
                producer: first,
                consumer: second,
                kind: DependencyKind::MaterializeBeforeRead,
            },
            PipelineDependency {
                producer: second,
                consumer: first,
                kind: DependencyKind::MaterializeBeforeRead,
            },
        ],
        handles: BreakerHandleCatalogBuilder::default().finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(first),
    };

    assert!(graph.validate().is_err());
}

#[test]
fn physical_extraction_rejects_unimplemented_nodes_before_lowering() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let distinct = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Distinct(Distinct::distinct_on(
            vec![Expression::Reference(
                ReferenceExpression::new(0, LogicalType::Integer).into(),
            )],
            values,
        )),
    );
    let mut extractor = PhysicalPlanExtractor::new(ExtractionContext::default());
    assert!(extractor.extract(distinct).is_err());
}
