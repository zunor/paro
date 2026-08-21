// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn materialized_cte_lowers_to_typed_materialize_and_scan() {
    let plan = materialized_cte_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 2);
    assert!(matches!(
        graph.pipelines[0].sink,
        SinkSpec::CteMaterialize(_)
    ));
    assert!(matches!(graph.pipelines[1].source, SourceSpec::CteScan(_)));
    assert!(matches!(graph.pipelines[1].sink, SinkSpec::ClientResult(_)));
    assert_eq!(graph.dependencies.len(), 1);
    assert_eq!(
        graph.dependencies[0],
        PipelineDependency {
            producer: PipelineId::new(0),
            consumer: PipelineId::new(1),
            kind: DependencyKind::MaterializeBeforeRead,
        }
    );

    let handle = graph.handles.iter().next().unwrap();
    assert_eq!(handle.kind, BreakerHandleKind::Cte);
    assert_eq!(handle.producer, Some(PipelineId::new(0)));
    assert_eq!(handle.consumers, vec![PipelineId::new(1)]);
}

#[test]
fn recursive_cte_lowers_to_control_region() {
    let plan = recursive_cte_plan(true);
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 3);
    assert!(matches!(
        graph.pipelines[0].sink,
        SinkSpec::RecursiveTableAppend(_)
    ));
    assert!(matches!(
        graph.pipelines[1].source,
        SourceSpec::RecursiveTableScan(_)
    ));
    assert!(matches!(
        graph.pipelines[1].sink,
        SinkSpec::RecursiveTableAppend(_)
    ));
    assert!(matches!(
        graph.pipelines[2].source,
        SourceSpec::RecursiveTableScan(_)
    ));
    assert_eq!(
        graph.root,
        PipelineRoot::ControlRegion(ControlRegionId::new(0))
    );

    let ControlRegion::RecursiveCte(region) = &graph.control_regions[0] else {
        panic!("expected recursive CTE region");
    };
    assert_eq!(region.anchor, PipelineId::new(0));
    assert_eq!(region.recursive, vec![PipelineId::new(1)]);
    assert_eq!(region.emit, PipelineId::new(2));
    assert_eq!(region.dedup, RecursiveCteDedup::None);
    assert_eq!(
        graph
            .dependencies
            .iter()
            .filter(|dependency| matches!(
                dependency.kind,
                DependencyKind::LoopEntry(_) | DependencyKind::LoopBack(_)
            ))
            .count(),
        2
    );
}

#[test]
fn recursive_cte_hoists_loop_invariant_hash_build() {
    let plan = recursive_cte_with_invariant_hash_build_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    let invariant_build = graph
        .pipelines
        .iter()
        .find(|pipeline| matches!(pipeline.sink, SinkSpec::HashJoinBuild(_)))
        .expect("invariant hash build pipeline")
        .id;
    let ControlRegion::RecursiveCte(region) = &graph.control_regions[0] else {
        panic!("expected recursive CTE region");
    };
    assert!(!region.recursive.contains(&invariant_build));
    assert!(region.recursive.iter().any(|pipeline| {
        graph.pipelines[pipeline.index()]
            .transforms
            .iter()
            .any(|transform| matches!(transform, TransformSpec::HashJoinProbe(_)))
    }));
    assert!(graph.dependencies.iter().any(|dependency| {
        dependency.producer == invariant_build
            && region.recursive.contains(&dependency.consumer)
            && matches!(dependency.kind, DependencyKind::BuildBeforeProbe)
    }));
}

#[test]
fn projection_above_recursive_cte_stays_on_emit_pipeline() {
    let plan = projected_recursive_cte_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(
        graph.root,
        PipelineRoot::ControlRegion(ControlRegionId::new(0))
    );
    let ControlRegion::RecursiveCte(region) = &graph.control_regions[0] else {
        panic!("expected recursive CTE region");
    };
    assert!(matches!(
        graph.pipelines[region.emit.index()].transforms.as_slice(),
        [TransformSpec::Project(_)]
    ));
}

#[test]
fn recursive_cte_can_feed_parent_sort_pipeline() {
    let plan = ordered_recursive_cte_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert!(matches!(graph.root, PipelineRoot::Pipeline(_)));
    assert_eq!(graph.control_regions.len(), 1);
    let ControlRegion::RecursiveCte(region) = &graph.control_regions[0] else {
        panic!("expected recursive CTE region");
    };
    assert!(matches!(
        graph.pipelines[region.emit.index()].sink,
        SinkSpec::SortBuild(_)
    ));
    assert!(matches!(
        graph.pipelines.last().map(|pipeline| &pipeline.source),
        Some(SourceSpec::SortEmit(_))
    ));
}

#[test]
fn recursive_union_distinct_lowers_dedup_region() {
    let plan = recursive_cte_plan(false);
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    let ControlRegion::RecursiveCte(region) = &graph.control_regions[0] else {
        panic!("expected recursive CTE region");
    };
    assert!(
        matches!(region.dedup, RecursiveCteDedup::HashSet),
        "UNION distinct must produce a dedup set"
    );
    for pipeline in graph.pipelines.iter().take(2) {
        assert!(
            matches!(pipeline.sink, SinkSpec::RecursiveTableAppend(_)),
            "anchor and recursive must use recursive table append sink"
        );
    }
}

#[test]
fn left_delim_join_lowers_to_correlated_subquery_region() {
    let plan = left_delim_join_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.pipelines.len(), 4);
    assert!(matches!(graph.pipelines[0].sink, SinkSpec::DelimCapture(_)));
    assert!(matches!(
        graph.pipelines[1].source,
        SourceSpec::DelimScan(_)
    ));
    assert!(matches!(
        graph.pipelines[1].sink,
        SinkSpec::HashJoinBuild(_)
    ));
    assert!(matches!(
        graph.pipelines[2].transforms.as_slice(),
        [TransformSpec::HashJoinProbe(_)]
    ));
    assert!(matches!(
        graph.pipelines[3].source,
        SourceSpec::HashJoinSpillReplay(_)
    ));
    assert_eq!(
        graph.root,
        PipelineRoot::ControlRegion(ControlRegionId::new(0))
    );

    let ControlRegion::CorrelatedSubquery(region) = &graph.control_regions[0] else {
        panic!("expected correlated subquery region");
    };
    assert_eq!(region.side, DelimJoinSide::Left);
    assert_eq!(region.capture, PipelineId::new(0));
    assert_eq!(
        region.dependent_roots,
        vec![
            PipelineSubgraphRoot::Pipeline(PipelineId::new(1)),
            PipelineSubgraphRoot::Pipeline(PipelineId::new(2))
        ]
    );
    assert_eq!(region.join, PipelineId::new(3));
    assert!(region.cached_outer.is_some());
}

#[test]
fn delim_join_can_feed_hash_probe_through_materialized_boundary() {
    let plan = hash_join_with_delim_probe_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(graph.control_regions.len(), 1);
    assert!(graph
        .pipelines
        .iter()
        .any(|pipeline| matches!(pipeline.sink, SinkSpec::Materialize(_))));
    assert!(graph.pipelines.iter().any(|pipeline| {
        matches!(pipeline.source, SourceSpec::Materialized(_))
            && pipeline
                .transforms
                .iter()
                .any(|transform| matches!(transform, TransformSpec::HashJoinProbe(_)))
    }));
}

#[test]
fn delim_join_keeps_recursive_dependent_as_control_region_root() {
    let plan = left_delim_join_with_recursive_dependent_plan();
    let mut lowerer = PipelineLowerer::new(&plan);
    let graph = lowerer.lower_to_pipeline_graph(plan.root).unwrap();

    assert_eq!(
        graph.root,
        PipelineRoot::ControlRegion(ControlRegionId::new(1))
    );
    assert_eq!(graph.control_regions.len(), 2);

    let ControlRegion::RecursiveCte(recursive) = &graph.control_regions[0] else {
        panic!("expected nested recursive CTE region");
    };
    assert!(matches!(
        graph.pipelines[recursive.emit.index()].sink,
        SinkSpec::HashJoinBuild(_)
    ));

    let ControlRegion::CorrelatedSubquery(region) = &graph.control_regions[1] else {
        panic!("expected outer correlated subquery region");
    };
    assert_eq!(region.capture, PipelineId::new(0));
    assert_eq!(
        region.dependent_roots,
        vec![
            PipelineSubgraphRoot::ControlRegion(ControlRegionId::new(0)),
            PipelineSubgraphRoot::Pipeline(PipelineId::new(4))
        ]
    );
    assert_ne!(region.join, recursive.emit);
}
