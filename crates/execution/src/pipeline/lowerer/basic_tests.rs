// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn lowerer_builds_source_transform_sink_pipeline() {
    let plan = linear_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 1);
    let pipeline = &graph.pipelines[0];
    assert!(matches!(pipeline.source, SourceSpec::Values(_)));
    assert_eq!(pipeline.transforms.len(), 3);
    assert!(matches!(pipeline.transforms[0], TransformSpec::Filter(_)));
    assert!(matches!(pipeline.transforms[1], TransformSpec::Project(_)));
    assert!(matches!(pipeline.transforms[2], TransformSpec::Limit(_)));
    assert!(matches!(pipeline.sink, SinkSpec::ClientResult(_)));
    assert!(matches!(graph.root, PipelineRoot::Pipeline(_)));
}

#[test]
fn lowerer_uses_root_output_schema_after_transforms() {
    let plan = projection_changes_schema_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();
    let pipeline = &graph.pipelines[0];

    assert!(matches!(pipeline.source, SourceSpec::Values(_)));
    if let SourceSpec::Values(source) = &pipeline.source {
        assert_eq!(source.output_types.len(), 2);
    }
    assert_eq!(pipeline.output.column_count(), 1);
    assert_eq!(&pipeline.output.names[..], ["b".to_string()]);
    assert_eq!(&pipeline.output.types[..], [LogicalType::Varchar]);
}

#[test]
fn graph_tracks_fan_in_shared_sink_producers() {
    let plan = linear_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let mut pipelines = Vec::new();
    let shared = SharedSinkId::new(0);
    let sink = SinkSpec::ClientResult(ClientResultSpec::default());

    let first = lowerer
        .lower_linear_pipeline(
            plan.root,
            sink.clone(),
            SinkSharing::Shared(shared),
            &mut pipelines,
        )
        .unwrap();
    let second = lowerer
        .lower_linear_pipeline(plan.root, sink, SinkSharing::Shared(shared), &mut pipelines)
        .unwrap();

    let graph = PipelineGraph {
        pipelines,
        dependencies: Vec::new(),
        handles: BreakerHandleCatalogBuilder::default().finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(first),
    };

    graph.validate().unwrap();
    assert_eq!(graph.shared_sink_producers(shared), vec![first, second]);
}

#[test]
fn graph_tracks_breaker_handle_and_dependency() {
    let plan = linear_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let row_type = plan.node(plan.root).output.clone();
    let graph = lowerer
        .lower_materialized_pair(
            plan.root,
            SourceSpec::Materialized(MaterializedSourceSpec {
                handle: BreakerHandleId::new(0),
            }),
            row_type,
        )
        .unwrap();

    assert_eq!(graph.dependencies.len(), 1);
    assert_eq!(
        graph.dependencies[0].kind,
        DependencyKind::MaterializeBeforeRead
    );
    assert_eq!(graph.handles.len(), 1);
    let handle = graph.handles.iter().next().unwrap();
    assert!(handle.producer.is_some());
    assert_eq!(handle.consumers.len(), 1);
}

#[test]
fn grouped_aggregate_lowers_to_build_emit_breaker_pipelines() {
    let plan = grouped_aggregate_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 2);
    assert!(matches!(
        graph.pipelines[0].sink,
        SinkSpec::HashAggregateBuild(_) | SinkSpec::PerfectHashAggregate(_)
    ));
    assert!(matches!(
        graph.pipelines[1].source,
        SourceSpec::HashAggregateEmit(_) | SourceSpec::PerfectHashAggregateEmit(_)
    ));
    assert_eq!(graph.dependencies.len(), 1);
    assert_eq!(
        graph.dependencies[0].kind,
        DependencyKind::FinalizeBeforeEmit
    );
    assert_eq!(graph.handles.len(), 1);
}

#[test]
fn topn_lowers_to_build_emit_breaker_pipelines() {
    let plan = topn_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 2);
    assert!(matches!(graph.pipelines[0].sink, SinkSpec::TopNBuild(_)));
    assert!(matches!(graph.pipelines[1].source, SourceSpec::TopNEmit(_)));
    assert_eq!(graph.dependencies.len(), 1);
    assert_eq!(
        graph.dependencies[0].kind,
        DependencyKind::FinalizeBeforeEmit
    );
    assert_eq!(graph.handles.len(), 1);
}

#[test]
fn hash_join_lowers_replay_fence_for_memory_triggered_external_fallback() {
    let plan = hash_join_plan(JoinType::Inner);
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 3);
    assert!(matches!(
        graph.pipelines[0].sink,
        SinkSpec::HashJoinBuild(_)
    ));
    assert!(matches!(
        graph.pipelines[1].transforms.as_slice(),
        [TransformSpec::HashJoinProbe(_)]
    ));
    assert!(matches!(
        graph.pipelines[2].source,
        SourceSpec::HashJoinSpillReplay(_)
    ));
    assert_eq!(graph.dependencies.len(), 2);
    assert_eq!(graph.dependencies[0].kind, DependencyKind::BuildBeforeProbe);
    assert_eq!(
        graph.dependencies[1].kind,
        DependencyKind::ProbeBeforeSpillReplay
    );
    assert_eq!(graph.handles.len(), 1);
}

#[test]
fn forced_external_hash_join_keeps_spill_replay_pipeline() {
    let plan = hash_join_plan_with_context(
        JoinType::Inner,
        PlanBuildContext {
            force_external: true,
            rowset_scan_pushdown: true,
        },
    );
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 3);
    assert!(matches!(
        graph.pipelines[0].sink,
        SinkSpec::HashJoinBuild(_)
    ));
    if let SinkSpec::HashJoinBuild(spec) = &graph.pipelines[0].sink {
        assert!(spec.force_external);
    }
    assert!(matches!(
        graph.pipelines[2].source,
        SourceSpec::HashJoinSpillReplay(_)
    ));
    assert_eq!(graph.dependencies.len(), 2);
    assert_eq!(graph.dependencies[0].kind, DependencyKind::BuildBeforeProbe);
    assert_eq!(
        graph.dependencies[1].kind,
        DependencyKind::ProbeBeforeSpillReplay
    );
}

#[test]
fn cross_product_lowers_to_materialized_build_probe_pipeline() {
    let plan = cross_product_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 2);
    assert!(matches!(
        graph.pipelines[0].sink,
        SinkSpec::CrossProductBuild(_)
    ));
    assert!(matches!(
        graph.pipelines[1].transforms.as_slice(),
        [TransformSpec::CrossProductProbe(_)]
    ));
    assert_eq!(graph.dependencies.len(), 1);
    assert_eq!(graph.dependencies[0].kind, DependencyKind::BuildBeforeProbe);
    assert_eq!(graph.handles.len(), 1);
    let handle = graph.handles.iter().next().unwrap();
    assert_eq!(handle.kind, BreakerHandleKind::Materialized);
}

#[test]
fn projected_cross_product_folds_into_hash_join_probe_chain() {
    let plan = hash_join_with_projected_cross_product_probe_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 4);
    assert!(graph
        .pipelines
        .iter()
        .any(|pipeline| matches!(pipeline.sink, SinkSpec::CrossProductBuild(_))));
    assert!(graph
        .pipelines
        .iter()
        .any(|pipeline| matches!(pipeline.sink, SinkSpec::HashJoinBuild(_))));
    assert!(matches!(
        graph.pipelines[2].transforms.as_slice(),
        [
            TransformSpec::CrossProductProbe(_),
            TransformSpec::Project(_),
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
    assert!(graph
        .dependencies
        .iter()
        .any(|dependency| dependency.kind == DependencyKind::ProbeBeforeSpillReplay));
}

#[test]
fn right_hash_join_lowers_unmatched_emit_pipeline() {
    let plan = hash_join_plan(JoinType::Right);
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 4);
    assert!(matches!(
        graph.pipelines[0].sink,
        SinkSpec::HashJoinBuild(_)
    ));
    assert!(matches!(
        graph.pipelines[1].transforms.as_slice(),
        [TransformSpec::HashJoinProbe(_)]
    ));
    assert!(matches!(
        graph.pipelines[2].source,
        SourceSpec::HashJoinSpillReplay(_)
    ));
    assert!(matches!(
        graph.pipelines[3].source,
        SourceSpec::HashJoinUnmatched(_)
    ));
    assert_eq!(graph.dependencies[0].kind, DependencyKind::BuildBeforeProbe);
    assert_eq!(
        graph.dependencies[1].kind,
        DependencyKind::ProbeBeforeSpillReplay
    );
    assert_eq!(
        graph.dependencies[2].kind,
        DependencyKind::FinalizeBeforeEmit
    );
}

#[test]
fn right_nested_loop_join_lowers_unmatched_emit_pipeline() {
    let plan = nested_loop_join_plan(JoinType::Right);
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 3);
    assert!(graph
        .pipelines
        .iter()
        .any(|pipeline| matches!(pipeline.sink, SinkSpec::Materialize(_))));
    assert!(graph.pipelines.iter().any(|pipeline| matches!(
        pipeline.transforms.as_slice(),
        [TransformSpec::NestedLoopJoinProbe(_)]
    )));
    assert!(graph
        .pipelines
        .iter()
        .any(|pipeline| matches!(pipeline.source, SourceSpec::NljUnmatched(_))));
    assert!(graph
        .dependencies
        .iter()
        .any(|dependency| dependency.kind == DependencyKind::BuildBeforeProbe));
    assert!(graph
        .dependencies
        .iter()
        .any(|dependency| dependency.kind == DependencyKind::FinalizeBeforeEmit));
}

#[test]
fn sort_range_join_lowers_to_sort_range_probe_pipeline() {
    let plan = sort_range_join_plan(JoinType::Inner);
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 2);
    assert!(graph
        .pipelines
        .iter()
        .any(|pipeline| matches!(pipeline.sink, SinkSpec::Materialize(_))));
    assert!(graph.pipelines.iter().any(|pipeline| matches!(
        pipeline.transforms.as_slice(),
        [TransformSpec::SortRangeJoinProbe(_)]
    )));
    assert!(graph
        .dependencies
        .iter()
        .any(|dependency| dependency.kind == DependencyKind::BuildBeforeProbe));
}

#[test]
fn project_above_nested_loop_join_stays_on_breaker_path() {
    let plan = project_above_nested_loop_join_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert!(graph
        .pipelines
        .iter()
        .any(|pipeline| matches!(pipeline.sink, SinkSpec::Materialize(_))));
    assert!(graph.pipelines.iter().any(|pipeline| matches!(
        pipeline.transforms.as_slice(),
        [
            TransformSpec::NestedLoopJoinProbe(_),
            TransformSpec::Project(_)
        ]
    )));
}

#[test]
fn limit_above_right_nested_loop_join_fails_fast() {
    let plan = limit_above_right_nested_loop_join_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let err = lowerer.lower_to_pipeline_graph(plan.root).unwrap_err();

    assert!(format!("{err}").contains("LIMIT above right/full nested loop join"));
}

#[test]
fn nested_right_nested_loop_join_falls_back_to_materialized_source() {
    let plan = left_deep_right_nested_loop_join_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert!(graph
        .pipelines
        .iter()
        .any(|pipeline| matches!(pipeline.source, SourceSpec::NljUnmatched(_))));
    assert!(graph.pipelines.iter().any(|pipeline| {
        matches!(pipeline.source, SourceSpec::Materialized(_))
            && pipeline
                .transforms
                .iter()
                .any(|transform| matches!(transform, TransformSpec::HashJoinProbe(_)))
    }));
}

#[test]
fn right_hash_join_fans_unmatched_branches_into_parent_aggregate() {
    let plan = aggregate_above_right_anti_hash_join_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 5);
    assert!(matches!(
        graph.pipelines[0].sink,
        SinkSpec::HashJoinBuild(_)
    ));
    assert!(matches!(
        graph.pipelines[1].transforms.as_slice(),
        [TransformSpec::HashJoinProbe(_)]
    ));
    assert!(matches!(
        graph.pipelines[1].sink,
        SinkSpec::UngroupedAggregate(_)
    ));
    assert!(matches!(
        graph.pipelines[2].source,
        SourceSpec::HashJoinSpillReplay(_)
    ));
    assert!(matches!(
        graph.pipelines[2].sink,
        SinkSpec::UngroupedAggregate(_)
    ));
    assert!(matches!(
        graph.pipelines[3].source,
        SourceSpec::HashJoinUnmatched(_)
    ));
    assert!(matches!(
        graph.pipelines[3].sink,
        SinkSpec::UngroupedAggregate(_)
    ));
    assert!(matches!(
        graph.pipelines[4].source,
        SourceSpec::UngroupedAggregateEmit(_)
    ));

    let shared = match graph.pipelines[1].sink_sharing {
        SinkSharing::Shared(shared) => shared,
        SinkSharing::Exclusive => panic!("probe aggregate sink should be shared"),
    };
    assert_eq!(graph.pipelines[2].sink_sharing, SinkSharing::Shared(shared));
    assert_eq!(graph.pipelines[3].sink_sharing, SinkSharing::Shared(shared));
    assert_eq!(
        graph.shared_sink_producers(shared),
        vec![PipelineId::new(1), PipelineId::new(2), PipelineId::new(3)]
    );
    assert_eq!(
        graph
            .dependencies
            .iter()
            .filter(|dependency| dependency.kind == DependencyKind::FinalizeBeforeEmit)
            .count(),
        2
    );
}
