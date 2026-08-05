// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn streaming_shape_guards_reject_misrouted_breakers() {
    let topn = TopNSpec {
        orders: Box::new([]),
        limit: 10,
        offset: 0,
        hnsw_ef_hint: None,
        output_names: vec!["a".to_string()].into_boxed_slice(),
        output_types: vec![LogicalType::Integer].into_boxed_slice(),
    };
    assert!(ensure_streaming_topn_supported(&topn).is_err());

    let aggregate = AggregateSpec {
        grouping_key_count: 1,
        projection_exprs: Box::new([]),
        payload_types: Box::new([]),
        groups: Box::new([]),
        grouping_sets: Box::new([]),
        aggregates: Box::new([]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([]),
        aggregate_filters: Box::new([]),
        aggregate_orders: Box::new([]),
        having_filter: Box::new([]),
        perfect_hash: None,
        output_names: vec!["count".to_string()].into_boxed_slice(),
        output_types: vec![LogicalType::BigInt].into_boxed_slice(),
    };
    assert!(ensure_streaming_aggregate_supported(&aggregate).is_err());
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
