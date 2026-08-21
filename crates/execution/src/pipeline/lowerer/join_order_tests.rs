// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

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
fn left_deep_hash_join_chain_stays_in_one_probe_pipeline() {
    let plan = left_deep_hash_join_plan();
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
fn direct_rowset_probe_gets_hash_join_runtime_filter_gate() {
    let plan = hash_join_plan(JoinType::Inner);
    let lowerer = PipelineLowerer::new(&plan);
    let spec = match &plan.node(plan.root).kind {
        PhysicalNodeKind::HashJoin(spec) => spec.clone(),
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
    assert_eq!(rowset.dynamic_runtime_filters[0].build_key_index, 0);
    assert_eq!(rowset.dynamic_runtime_filters[0].probe_column_id, 0);
}

#[test]
fn left_deep_probe_traces_runtime_filter_to_rowset_column() {
    let plan = hash_join_plan(JoinType::Inner);
    let lowerer = PipelineLowerer::new(&plan);
    let spec = match &plan.node(plan.root).kind {
        PhysicalNodeKind::HashJoin(spec) => spec.clone(),
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
fn left_deep_probe_does_not_trace_build_payload_to_rowset() {
    let plan = hash_join_plan(JoinType::Inner);
    let lowerer = PipelineLowerer::new(&plan);
    let mut spec = match &plan.node(plan.root).kind {
        PhysicalNodeKind::HashJoin(spec) => spec.clone(),
        _ => panic!("expected hash join plan"),
    };
    let prior_probe = hash_join_probe_transform(BreakerHandleId::new(2), &spec);
    spec.key_conditions[0].left =
        Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer));
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
    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let dummy = generator
        .generate(&LogicalPlan::new(&ctx, LogicalOperator::DummyScan))
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

    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let empty = generator
        .generate(&LogicalPlan::new(
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
fn lowerer_rejects_unsupported_nodes_before_runtime() {
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
        LogicalOperator::Distinct(Distinct::distinct_on(
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::Integer,
            ))],
            values,
        )),
    );
    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let plan = generator.generate(&distinct).unwrap();
    let mut lowerer = PipelineLowerer::new(&plan);

    assert!(lowerer.lower_to_pipeline_graph(plan.root).is_err());
}
