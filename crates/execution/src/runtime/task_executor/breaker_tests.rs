// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn aggregate_breaker_graph(
    build_sink: SinkSpec,
    emit_source: SourceSpec,
    input_rows: Vec<Vec<Expression>>,
    input_types: Vec<LogicalType>,
    output: RowType,
) -> PipelineGraph {
    let input_names = (0..input_types.len())
        .map(|index| format!("c{index}"))
        .collect();
    aggregate_breaker_graph_from_source(
        build_sink,
        emit_source,
        SourceSpec::Values(values_spec(input_rows, input_types.clone())),
        RowType::new(input_names, input_types),
        output,
    )
}

fn aggregate_breaker_graph_from_source(
    build_sink: SinkSpec,
    emit_source: SourceSpec,
    input_source: SourceSpec,
    input: RowType,
    output: RowType,
) -> PipelineGraph {
    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::Aggregate,
        output.clone(),
        PipelineProperties::default(),
    );
    let build_sink = match build_sink {
        SinkSpec::HashAggregateBuild(mut spec) => {
            spec.handle = handle;
            SinkSpec::HashAggregateBuild(spec)
        }
        SinkSpec::UngroupedAggregate(mut spec) => {
            spec.handle = handle;
            SinkSpec::UngroupedAggregate(spec)
        }
        SinkSpec::PerfectHashAggregate(mut spec) => {
            spec.handle = handle;
            SinkSpec::PerfectHashAggregate(spec)
        }
        _ => unreachable!("aggregate breaker test requires aggregate sink"),
    };
    let emit_source = match emit_source {
        SourceSpec::HashAggregateEmit(mut spec) => {
            spec.handle = handle;
            SourceSpec::HashAggregateEmit(spec)
        }
        SourceSpec::UngroupedAggregateEmit(mut spec) => {
            spec.handle = handle;
            SourceSpec::UngroupedAggregateEmit(spec)
        }
        SourceSpec::PerfectHashAggregateEmit(mut spec) => {
            spec.handle = handle;
            SourceSpec::PerfectHashAggregateEmit(spec)
        }
        _ => unreachable!("aggregate breaker test requires aggregate source"),
    };
    let build = PipelineId::new(0);
    let emit = PipelineId::new(1);
    handles.set_producer(handle, build).expect("producer");
    handles.add_consumer(handle, emit).expect("consumer");
    PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: build,
                source: input_source,
                transforms: Vec::new(),
                sink: build_sink,
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: input,
            },
            PipelineSpec {
                id: emit,
                source: emit_source,
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: output.clone(),
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: build,
            consumer: emit,
            kind: DependencyKind::FinalizeBeforeEmit,
        }],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(emit),
    }
}

fn sort_breaker_graph(input_rows: Vec<Vec<Expression>>) -> PipelineGraph {
    let output = RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]);
    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::Sort,
        output.clone(),
        PipelineProperties::default(),
    );
    let build = PipelineId::new(0);
    let emit = PipelineId::new(1);
    handles.set_producer(handle, build).expect("producer");
    handles.add_consumer(handle, emit).expect("consumer");
    PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: build,
                source: SourceSpec::Values(values_spec(input_rows, vec![LogicalType::Integer])),
                transforms: Vec::new(),
                sink: SinkSpec::SortBuild(SortBuildSinkSpec {
                    handle,
                    orders: vec![order_by_ref(0, LogicalType::Integer)].into_boxed_slice(),
                    projection_map: Box::new([0]),
                    input_types: Box::new([LogicalType::Integer]),
                    output_names: Box::new(["v".to_string()]),
                    output_types: Box::new([LogicalType::Integer]),
                    force_external: false,
                    required: Default::default(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: output.clone(),
            },
            PipelineSpec {
                id: emit,
                source: SourceSpec::SortEmit(SortEmitSourceSpec {
                    handle,
                    ordering: ordering_on_first_column(),
                    output_names: Box::new(["v".to_string()]),
                    output_types: Box::new([LogicalType::Integer]),
                }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: build,
            consumer: emit,
            kind: DependencyKind::FinalizeBeforeEmit,
        }],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(emit),
    }
}

fn topn_breaker_graph(input_rows: Vec<Vec<Expression>>, limit: usize) -> PipelineGraph {
    let spec = TopNSpec {
        orders: vec![order_by_ref(0, LogicalType::Integer)].into_boxed_slice(),
        limit,
        offset: 0,
        hnsw_ef_hint: None,
        output_names: Box::new(["v".to_string()]),
        output_types: Box::new([LogicalType::Integer]),
    };
    let output = RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]);
    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::TopN,
        output.clone(),
        PipelineProperties::default(),
    );
    let build = PipelineId::new(0);
    let emit = PipelineId::new(1);
    handles.set_producer(handle, build).expect("producer");
    handles.add_consumer(handle, emit).expect("consumer");
    PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: build,
                source: SourceSpec::Values(values_spec(input_rows, vec![LogicalType::Integer])),
                transforms: Vec::new(),
                sink: SinkSpec::TopNBuild(TopNBuildSinkSpec {
                    handle,
                    spec: spec.clone(),
                    required: Default::default(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: output.clone(),
            },
            PipelineSpec {
                id: emit,
                source: SourceSpec::TopNEmit(TopNEmitSourceSpec { handle, spec }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: build,
            consumer: emit,
            kind: DependencyKind::FinalizeBeforeEmit,
        }],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(emit),
    }
}

fn partitioned_window_spec() -> WindowSpec {
    WindowSpec {
        window_index: 1,
        expressions: vec![WindowExpression::native(
            WindowFunction::rank(),
            Vec::new(),
            vec![reference(0, LogicalType::Integer)],
            vec![OrderByExpression {
                expression: reference(1, LogicalType::Integer),
                ascending: true,
                nulls_first: false,
            }],
            WindowFrame::default(),
            false,
        )]
        .into_boxed_slice(),
        input_width: 2,
        output_names: Box::new(["grp".to_string(), "v".to_string(), "rank".to_string()]),
        output_types: Box::new([
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::BigInt,
        ]),
    }
}

fn window_breaker_graph(input_rows: Vec<Vec<Expression>>) -> PipelineGraph {
    let spec = partitioned_window_spec();
    let output = RowType::new(
        vec!["grp".to_string(), "v".to_string(), "rank".to_string()],
        vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::BigInt,
        ],
    );
    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::Window,
        output.clone(),
        PipelineProperties::default(),
    );
    let build = PipelineId::new(0);
    let emit = PipelineId::new(1);
    handles.set_producer(handle, build).expect("producer");
    handles.add_consumer(handle, emit).expect("consumer");
    PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: build,
                source: SourceSpec::Values(values_spec(
                    input_rows,
                    vec![LogicalType::Integer, LogicalType::Integer],
                )),
                transforms: Vec::new(),
                sink: SinkSpec::WindowBuild(WindowBuildSinkSpec {
                    handle,
                    spec: spec.clone(),
                    required: Default::default(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: RowType::new(
                    vec!["grp".to_string(), "v".to_string()],
                    vec![LogicalType::Integer, LogicalType::Integer],
                ),
            },
            PipelineSpec {
                id: emit,
                source: SourceSpec::WindowEmit(WindowEmitSourceSpec { handle, spec }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: build,
            consumer: emit,
            kind: DependencyKind::FinalizeBeforeEmit,
        }],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(emit),
    }
}

pub(super) fn run_two_stage_breaker(
    graph: PipelineGraph,
    query: &QueryRuntimeContext,
    thread: &ThreadContext,
    wake: &OperatorWakeScope,
) {
    let (build_runtime, emit_runtime) = runtimes_from_graph(query, &graph);
    let mut build = PipelineTaskExecutor::new(
        build_runtime.clone(),
        build_runtime
            .create_task_state(query, paro_common::test_utils::test_allocator())
            .expect("build task"),
    );
    let mut profiler = OperatorProfiler::disabled();
    run_to_done(&mut build, query, thread, wake, &mut profiler);
    // Match scheduler lifecycle: downstream local state may depend on values
    // published by the producer's finalize phase.
    let mut emit = PipelineTaskExecutor::new(
        emit_runtime.clone(),
        emit_runtime
            .create_task_state(query, paro_common::test_utils::test_allocator())
            .expect("emit task"),
    );
    run_to_done(&mut emit, query, thread, wake, &mut profiler);
}

pub(super) fn run_two_stage_breaker_with_profile(
    graph: PipelineGraph,
    query: &QueryRuntimeContext,
    thread: &ThreadContext,
    wake: &OperatorWakeScope,
) -> ExplainProfileSnapshot {
    let profile = ExplainProfiler::new();
    let (build_runtime, emit_runtime) = runtimes_from_graph(query, &graph);
    let mut build = PipelineTaskExecutor::new(
        build_runtime.clone(),
        build_runtime
            .create_task_state(query, paro_common::test_utils::test_allocator())
            .expect("build task"),
    );
    let mut profiler = OperatorProfiler::new(profile.clone());
    run_to_done(&mut build, query, thread, wake, &mut profiler);
    let mut emit = PipelineTaskExecutor::new(
        emit_runtime.clone(),
        emit_runtime
            .create_task_state(query, paro_common::test_utils::test_allocator())
            .expect("emit task"),
    );
    run_to_done(&mut emit, query, thread, wake, &mut profiler);
    profiler.flush();
    profile.snapshot()
}

#[test]
fn ungrouped_aggregate_breaker_merges_and_emits_count() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = ungrouped_count_spec();
    let graph = aggregate_breaker_graph(
        SinkSpec::UngroupedAggregate(UngroupedAggregateSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::UngroupedAggregateEmit(UngroupedAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        vec![
            vec![int_constant(1)],
            vec![int_constant(2)],
            vec![int_constant(3)],
        ],
        vec![LogicalType::Integer],
        RowType::new(vec!["count".to_string()], vec![LogicalType::BigInt]),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(18),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    let chunk = output.pop_front().expect("ungrouped aggregate output");
    assert_eq!(chunk.size(), 1);
    assert_eq!(chunk.column(0).unwrap().get_i64(0), Some(3));
}

#[test]
fn ungrouped_distinct_accepts_batches_larger_than_vector_size() {
    let allocator = paro_common::test_utils::test_allocator();
    let row_count = VECTOR_SIZE * 2;
    let distinct_count = 257usize;
    let mut input =
        Chunk::try_initialize(&[LogicalType::Integer], row_count, allocator).expect("input chunk");
    input.set_cardinality(row_count);
    for row_idx in 0..row_count {
        input
            .set_value(
                0,
                row_idx,
                &Value::Integer((row_idx % distinct_count) as i32),
            )
            .expect("input value");
    }

    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = ungrouped_distinct_count_spec();
    let graph = aggregate_breaker_graph_from_source(
        SinkSpec::UngroupedAggregate(UngroupedAggregateSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::UngroupedAggregateEmit(UngroupedAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        SourceSpec::Chunk(ChunkScanSpec {
            chunks: Arc::from(vec![input].into_boxed_slice()),
            output_names: vec!["v".to_string()].into_boxed_slice(),
            output_types: vec![LogicalType::Integer].into_boxed_slice(),
        }),
        RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]),
        RowType::new(vec!["count".to_string()], vec![LogicalType::BigInt]),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(20),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    let chunk = output.pop_front().expect("ungrouped DISTINCT output");
    assert_eq!(chunk.size(), 1);
    assert_eq!(chunk.column(0).unwrap().get_i64(0), Some(257));
}

#[test]
fn grouped_distinct_lazily_creates_its_finalize_target_table() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = grouped_distinct_count_spec();
    let graph = aggregate_breaker_graph(
        SinkSpec::HashAggregateBuild(HashAggregateBuildSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::HashAggregateEmit(HashAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        vec![
            vec![int_constant(1), int_constant(10)],
            vec![int_constant(1), int_constant(10)],
            vec![int_constant(1), int_constant(11)],
            vec![int_constant(2), int_constant(10)],
            vec![int_constant(2), int_constant(10)],
        ],
        vec![LogicalType::Integer, LogicalType::Integer],
        RowType::new(
            vec!["k".to_string(), "count".to_string()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        ),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(21),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    let mut rows = Vec::new();
    while let Some(chunk) = output.pop_front() {
        rows.extend((0..chunk.size()).map(|row| {
            (
                chunk.column(0).unwrap().get_i32(row).unwrap(),
                chunk.column(1).unwrap().get_i64(row).unwrap(),
            )
        }));
    }
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 2), (2, 1)]);
}

#[test]
fn ungrouped_aggregate_having_can_suppress_its_single_row() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let mut spec = ungrouped_count_spec();
    spec.having_filter = vec![Expression::Comparison(ComparisonExpression::new(
        ComparisonType::GreaterThan,
        reference(0, LogicalType::BigInt),
        Expression::Constant(ConstantExpression::new(
            Value::BigInt(3),
            LogicalType::BigInt,
        )),
    ))]
    .into_boxed_slice();
    let graph = aggregate_breaker_graph(
        SinkSpec::UngroupedAggregate(UngroupedAggregateSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::UngroupedAggregateEmit(UngroupedAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        vec![
            vec![int_constant(1)],
            vec![int_constant(2)],
            vec![int_constant(3)],
        ],
        vec![LogicalType::Integer],
        RowType::new(vec!["count".to_string()], vec![LogicalType::BigInt]),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(19),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    assert!(output.pop_front().is_none());
}

#[test]
fn hash_aggregate_breaker_groups_and_emits_counts() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = grouped_count_spec(None);
    let graph = aggregate_breaker_graph(
        SinkSpec::HashAggregateBuild(HashAggregateBuildSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::HashAggregateEmit(HashAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        vec![
            vec![int_constant(1)],
            vec![int_constant(2)],
            vec![int_constant(1)],
        ],
        vec![LogicalType::Integer],
        RowType::new(
            vec!["k".to_string(), "count".to_string()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        ),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(19),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    let chunk = output.pop_front().expect("hash aggregate output");
    let mut rows = (0..chunk.size())
        .map(|idx| {
            (
                chunk.column(0).unwrap().get_i32(idx).unwrap(),
                chunk.column(1).unwrap().get_i64(idx).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 2), (2, 1)]);
}

#[test]
fn hash_aggregate_breaker_spills_payload_partitions_when_forced_external() {
    let output = QueryOutputPort::unbounded();
    let query = query_context_with_limits(
        output.clone(),
        RuntimeLimits {
            max_threads: 1,
            max_memory: 64 * 1024 * 1024,
            use_temporary_directory: true,
            temporary_directory: unique_temp_dir("paro_aggregate_payload_spill"),
            max_temp_directory_size: None,
            force_external: true,
            rowset_scan_pushdown: true,
            parallel_scheduler: false,
        },
    );
    let spec = grouped_count_spec(None);
    let graph = aggregate_breaker_graph(
        SinkSpec::HashAggregateBuild(HashAggregateBuildSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::HashAggregateEmit(HashAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        vec![
            vec![int_constant(1)],
            vec![int_constant(2)],
            vec![int_constant(1)],
            vec![int_constant(3)],
            vec![int_constant(2)],
        ],
        vec![LogicalType::Integer],
        RowType::new(
            vec!["k".to_string(), "count".to_string()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        ),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(24),
        generation: WakeGeneration(0),
    };
    let profile = run_two_stage_breaker_with_profile(graph, &query, &thread, &wake);

    let mut rows = Vec::new();
    while let Some(chunk) = output.pop_front() {
        rows.extend((0..chunk.size()).map(|idx| {
            (
                chunk.column(0).unwrap().get_i32(idx).unwrap(),
                chunk.column(1).unwrap().get_i64(idx).unwrap(),
            )
        }));
    }
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 2), (2, 2), (3, 1)]);
    assert!(profile.operators.values().any(|actual| {
        actual.runtime.spilled == Some(true)
            && actual.runtime.spilled_bytes.unwrap_or(0) > 0
            && actual.runtime.repartition_depth == Some(1)
    }));
}

#[test]
fn hash_aggregate_breaker_preemptively_spills_payload_under_low_query_cap() {
    let output = QueryOutputPort::unbounded();
    let query = query_context_with_limits(
        output.clone(),
        RuntimeLimits {
            max_threads: 1,
            max_memory: 1024 * 1024,
            use_temporary_directory: true,
            temporary_directory: unique_temp_dir("paro_aggregate_low_cap_payload_spill"),
            max_temp_directory_size: None,
            force_external: false,
            rowset_scan_pushdown: true,
            parallel_scheduler: false,
        },
    );
    let spec = grouped_count_spec(None);
    let graph = aggregate_breaker_graph(
        SinkSpec::HashAggregateBuild(HashAggregateBuildSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::HashAggregateEmit(HashAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        vec![
            vec![int_constant(1)],
            vec![int_constant(2)],
            vec![int_constant(1)],
            vec![int_constant(3)],
            vec![int_constant(2)],
        ],
        vec![LogicalType::Integer],
        RowType::new(
            vec!["k".to_string(), "count".to_string()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        ),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(25),
        generation: WakeGeneration(0),
    };
    let profile = run_two_stage_breaker_with_profile(graph, &query, &thread, &wake);

    let mut rows = Vec::new();
    while let Some(chunk) = output.pop_front() {
        rows.extend((0..chunk.size()).map(|idx| {
            (
                chunk.column(0).unwrap().get_i32(idx).unwrap(),
                chunk.column(1).unwrap().get_i64(idx).unwrap(),
            )
        }));
    }
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 2), (2, 2), (3, 1)]);
    assert!(profile.operators.values().any(|actual| {
        actual.runtime.spilled == Some(true)
            && actual.runtime.spilled_bytes.unwrap_or(0) > 0
            && actual.runtime.repartition_depth == Some(1)
    }));
}

#[test]
fn hash_aggregate_breaker_does_not_spill_for_unrelated_query_memory() {
    let output = QueryOutputPort::unbounded();
    let query = query_context_with_limits(
        output.clone(),
        RuntimeLimits {
            max_threads: 1,
            max_memory: 16 * 1024 * 1024,
            use_temporary_directory: true,
            temporary_directory: unique_temp_dir("paro_aggregate_available_payload_spill"),
            max_temp_directory_size: None,
            force_external: false,
            rowset_scan_pushdown: true,
            parallel_scheduler: false,
        },
    );
    query
        .memory
        .try_grow(15 * 1024 * 1024 + 1)
        .expect("reserve most query memory");
    let spec = grouped_count_spec(None);
    let graph = aggregate_breaker_graph(
        SinkSpec::HashAggregateBuild(HashAggregateBuildSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::HashAggregateEmit(HashAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        vec![
            vec![int_constant(1)],
            vec![int_constant(2)],
            vec![int_constant(1)],
            vec![int_constant(3)],
            vec![int_constant(2)],
        ],
        vec![LogicalType::Integer],
        RowType::new(
            vec!["k".to_string(), "count".to_string()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        ),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(26),
        generation: WakeGeneration(0),
    };
    let profile = run_two_stage_breaker_with_profile(graph, &query, &thread, &wake);

    let mut rows = Vec::new();
    while let Some(chunk) = output.pop_front() {
        rows.extend((0..chunk.size()).map(|idx| {
            (
                chunk.column(0).unwrap().get_i32(idx).unwrap(),
                chunk.column(1).unwrap().get_i64(idx).unwrap(),
            )
        }));
    }
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 2), (2, 2), (3, 1)]);
    assert!(profile
        .operators
        .values()
        .all(|actual| actual.runtime.spilled != Some(true)));
}

#[test]
fn perfect_hash_aggregate_breaker_groups_and_emits_counts() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = grouped_count_spec(Some(PerfectHashAggregatePlan {
        group_minima: vec![1].into_boxed_slice(),
        group_cardinalities: vec![4].into_boxed_slice(),
        max_local_tables: 1,
    }));
    let graph = aggregate_breaker_graph(
        SinkSpec::PerfectHashAggregate(PerfectHashAggregateSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::PerfectHashAggregateEmit(PerfectHashAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        vec![
            vec![int_constant(1)],
            vec![int_constant(2)],
            vec![int_constant(1)],
        ],
        vec![LogicalType::Integer],
        RowType::new(
            vec!["k".to_string(), "count".to_string()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        ),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(20),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    let chunk = output.pop_front().expect("perfect aggregate output");
    let mut rows = (0..chunk.size())
        .map(|idx| {
            (
                chunk.column(0).unwrap().get_i32(idx).unwrap(),
                chunk.column(1).unwrap().get_i64(idx).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 2), (2, 1)]);
}

#[test]
fn perfect_hash_having_rejection_still_validates_every_aggregate_state() {
    let narrow_type = LogicalType::Decimal {
        precision: 10,
        scale: 0,
    };
    let wide_type = LogicalType::Decimal {
        precision: 38,
        scale: 0,
    };
    let (narrow_sum, _) = get_sum_function()
        .bind(std::slice::from_ref(&narrow_type))
        .expect("bind narrow decimal SUM");
    let (wide_sum, _) = get_sum_function()
        .bind(std::slice::from_ref(&wide_type))
        .expect("bind wide decimal SUM");
    assert_eq!(narrow_sum.return_type, wide_type);
    assert_eq!(wide_sum.return_type, wide_type);

    let spec = AggregateSpec {
        grouping_key_count: 1,
        state_output_projection: Box::new([]),
        estimated_input_rows: None,
        projection_exprs: Box::new([]),
        payload_types: Box::new([LogicalType::Integer, narrow_type.clone(), wide_type.clone()]),
        groups: Box::new([reference(0, LogicalType::Integer)]),
        group_key_encodings: Box::new([crate::physical::specs::GroupKeyEncoding::Identity]),
        grouping_sets: Box::new([]),
        aggregates: Box::new([
            Expression::Aggregate(AggregateExpression::new(
                narrow_sum,
                vec![reference(1, narrow_type.clone())],
                wide_type.clone(),
            )),
            Expression::Aggregate(AggregateExpression::new(
                wide_sum,
                vec![reference(2, wide_type.clone())],
                wide_type.clone(),
            )),
        ]),
        grouping_functions: Box::new([]),
        aggregate_inputs: Box::new([Box::new([1]), Box::new([2])]),
        aggregate_filters: Box::new([None, None]),
        aggregate_orders: Box::new([Box::new([]), Box::new([])]),
        post_reduction: None,
        having_filter: Box::new([Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            reference(0, wide_type.clone()),
            Expression::Constant(ConstantExpression::new(
                Value::Decimal(10, 38, 0),
                wide_type.clone(),
            )),
        ))]),
        perfect_hash: Some(PerfectHashAggregatePlan {
            group_minima: Box::new([1]),
            group_cardinalities: Box::new([2]),
            max_local_tables: 1,
        }),
        output_names: Box::new([
            "k".to_string(),
            "small_sum".to_string(),
            "overflowing_sum".to_string(),
        ]),
        output_types: Box::new([LogicalType::Integer, wide_type.clone(), wide_type.clone()]),
    };
    let max_decimal = 10_i128.pow(38) - 1;
    let decimal_constant = |value, ty: &LogicalType| {
        let LogicalType::Decimal { precision, scale } = ty else {
            unreachable!("test decimal constant requires DECIMAL type")
        };
        Expression::Constant(ConstantExpression::new(
            Value::Decimal(value, *precision, *scale),
            ty.clone(),
        ))
    };

    let output = QueryOutputPort::unbounded();
    let query = query_context(output);
    let graph = aggregate_breaker_graph(
        SinkSpec::PerfectHashAggregate(PerfectHashAggregateSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::PerfectHashAggregateEmit(PerfectHashAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        vec![
            vec![
                int_constant(1),
                decimal_constant(1, &narrow_type),
                decimal_constant(max_decimal, &wide_type),
            ],
            vec![
                int_constant(1),
                decimal_constant(1, &narrow_type),
                decimal_constant(max_decimal, &wide_type),
            ],
        ],
        vec![LogicalType::Integer, narrow_type, wide_type.clone()],
        RowType::new(
            vec![
                "k".to_string(),
                "small_sum".to_string(),
                "overflowing_sum".to_string(),
            ],
            vec![LogicalType::Integer, wide_type.clone(), wide_type],
        ),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(31),
        generation: WakeGeneration(0),
    };
    let (build_runtime, emit_runtime) = runtimes_from_graph(&query, &graph);
    let mut build = PipelineTaskExecutor::new(
        build_runtime.clone(),
        build_runtime
            .create_task_state(&query, paro_common::test_utils::test_allocator())
            .expect("build task"),
    );
    let mut profiler = OperatorProfiler::disabled();
    run_to_done(&mut build, &query, &thread, &wake, &mut profiler);

    let mut emit = PipelineTaskExecutor::new(
        emit_runtime.clone(),
        emit_runtime
            .create_task_state(&query, paro_common::test_utils::test_allocator())
            .expect("emit task"),
    );
    let error = (0..32)
        .find_map(|_| {
            emit.step(&mut step_context(&query, &thread, &wake, &mut profiler))
                .err()
        })
        .expect("generic finalize must expose the rejected group's overflow");
    assert!(
        error
            .to_string()
            .contains("Decimal SUM result exceeds precision 38"),
        "unexpected finalize error: {error}"
    );
}

#[test]
fn perfect_hash_post_reduction_retains_every_global_maximum_tie() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = grouped_sum_post_max_spec(
        LogicalType::Integer,
        Some(PerfectHashAggregatePlan {
            group_minima: Box::new([1]),
            group_cardinalities: Box::new([4]),
            max_local_tables: 1,
        }),
        Box::new([]),
    );
    let graph = aggregate_breaker_graph(
        SinkSpec::PerfectHashAggregate(PerfectHashAggregateSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::PerfectHashAggregateEmit(PerfectHashAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        vec![
            vec![int_constant(1), int_constant(4)],
            vec![int_constant(1), int_constant(6)],
            vec![int_constant(2), int_constant(10)],
            vec![int_constant(3), int_constant(9)],
        ],
        vec![LogicalType::Integer, LogicalType::Integer],
        RowType::new(
            vec!["k".to_string(), "sum".to_string()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        ),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(27),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    let mut rows = Vec::new();
    while let Some(chunk) = output.pop_front() {
        rows.extend((0..chunk.size()).map(|row| {
            (
                chunk.column(0).unwrap().get_i32(row).unwrap(),
                chunk.column(1).unwrap().get_i64(row).unwrap(),
            )
        }));
    }
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 10), (2, 10)]);
}

#[test]
fn generic_hash_post_reduction_observes_all_finalized_groups() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = grouped_sum_post_max_spec(LogicalType::Varchar, None, Box::new([]));
    let graph = aggregate_breaker_graph(
        SinkSpec::HashAggregateBuild(HashAggregateBuildSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::HashAggregateEmit(HashAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        vec![
            vec![varchar_constant("alpha"), int_constant(2)],
            vec![varchar_constant("alpha"), int_constant(4)],
            vec![varchar_constant("beta"), int_constant(7)],
            vec![varchar_constant("gamma"), int_constant(3)],
            vec![varchar_constant("gamma"), int_constant(4)],
        ],
        vec![LogicalType::Varchar, LogicalType::Integer],
        RowType::new(
            vec!["k".to_string(), "sum".to_string()],
            vec![LogicalType::Varchar, LogicalType::BigInt],
        ),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(28),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    let mut rows = Vec::new();
    while let Some(chunk) = output.pop_front() {
        for row in 0..chunk.size() {
            let Value::Varchar(key) = chunk.column(0).unwrap().get_value(row) else {
                panic!("expected VARCHAR group key");
            };
            rows.push((key, chunk.column(1).unwrap().get_i64(row).unwrap()));
        }
    }
    rows.sort_unstable();
    assert_eq!(
        rows,
        vec![("beta".to_string(), 7), ("gamma".to_string(), 7)]
    );
}

#[test]
fn external_hash_post_reduction_filters_against_the_global_spilled_domain() {
    let output = QueryOutputPort::unbounded();
    let query = query_context_with_limits(
        output.clone(),
        RuntimeLimits {
            max_threads: 1,
            max_memory: 64 * 1024 * 1024,
            use_temporary_directory: true,
            temporary_directory: unique_temp_dir("paro_post_reduction_payload_spill"),
            max_temp_directory_size: None,
            force_external: true,
            rowset_scan_pushdown: true,
            parallel_scheduler: false,
        },
    );
    let spec = grouped_sum_post_max_spec(LogicalType::Integer, None, Box::new([]));
    let graph = aggregate_breaker_graph(
        SinkSpec::HashAggregateBuild(HashAggregateBuildSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::HashAggregateEmit(HashAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        vec![
            vec![int_constant(1), int_constant(2)],
            vec![int_constant(2), int_constant(5)],
            vec![int_constant(2), int_constant(6)],
            vec![int_constant(3), int_constant(7)],
            vec![int_constant(4), int_constant(3)],
        ],
        vec![LogicalType::Integer, LogicalType::Integer],
        RowType::new(
            vec!["k".to_string(), "sum".to_string()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        ),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(29),
        generation: WakeGeneration(0),
    };
    let profile = run_two_stage_breaker_with_profile(graph, &query, &thread, &wake);

    let mut rows = Vec::new();
    while let Some(chunk) = output.pop_front() {
        rows.extend((0..chunk.size()).map(|row| {
            (
                chunk.column(0).unwrap().get_i32(row).unwrap(),
                chunk.column(1).unwrap().get_i64(row).unwrap(),
            )
        }));
    }
    assert_eq!(rows, vec![(2, 11)]);
    assert!(profile.operators.values().any(|actual| {
        actual.runtime.spilled == Some(true) && actual.runtime.spilled_bytes.unwrap_or(0) > 0
    }));
}

#[test]
fn post_reduction_precedes_having_and_both_reject_null_predicates() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let having = Expression::Comparison(ComparisonExpression::new(
        ComparisonType::LessThan,
        reference(0, LogicalType::BigInt),
        bigint_constant(100),
    ));
    let spec = grouped_sum_post_max_spec(LogicalType::Integer, None, Box::new([having]));
    let graph = aggregate_breaker_graph(
        SinkSpec::HashAggregateBuild(HashAggregateBuildSinkSpec {
            handle: BreakerHandleId::new(0),
            spec: spec.clone(),
            required: Default::default(),
        }),
        SourceSpec::HashAggregateEmit(HashAggregateEmitSourceSpec {
            handle: BreakerHandleId::new(0),
            spec,
        }),
        vec![
            vec![int_constant(1), int_constant(100)],
            vec![int_constant(2), int_constant(60)],
            vec![int_constant(3), null_constant(LogicalType::Integer)],
        ],
        vec![LogicalType::Integer, LogicalType::Integer],
        RowType::new(
            vec!["k".to_string(), "sum".to_string()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        ),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(30),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    while let Some(chunk) = output.pop_front() {
        assert!(chunk.is_empty());
    }
}

#[test]
fn sort_breaker_builds_runs_and_emits_fixed_order() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let graph = sort_breaker_graph(vec![
        vec![int_constant(3)],
        vec![int_constant(1)],
        vec![int_constant(2)],
    ]);
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(21),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    let chunk = output.pop_front().expect("sort output");
    let rows = (0..chunk.size())
        .map(|idx| chunk.column(0).unwrap().get_i32(idx).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![1, 2, 3]);
}

#[test]
fn topn_breaker_merges_heap_and_emits_limit_in_order() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let graph = topn_breaker_graph(
        vec![
            vec![int_constant(4)],
            vec![int_constant(1)],
            vec![int_constant(3)],
            vec![int_constant(2)],
        ],
        2,
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(22),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    let chunk = output.pop_front().expect("topn output");
    let rows = (0..chunk.size())
        .map(|idx| chunk.column(0).unwrap().get_i32(idx).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![1, 2]);
}

#[test]
fn window_breaker_partitions_orders_and_emits_rank() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let graph = window_breaker_graph(vec![
        vec![int_constant(2), int_constant(30)],
        vec![int_constant(1), int_constant(20)],
        vec![int_constant(1), int_constant(10)],
        vec![int_constant(2), int_constant(10)],
    ]);
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(23),
        generation: WakeGeneration(0),
    };
    run_two_stage_breaker(graph, &query, &thread, &wake);

    let chunk = output.pop_front().expect("window output");
    let rows = (0..chunk.size())
        .map(|idx| {
            (
                chunk.column(0).unwrap().get_i32(idx).unwrap(),
                chunk.column(1).unwrap().get_i32(idx).unwrap(),
                chunk.column(2).unwrap().get_i64(idx).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![(1, 10, 1), (1, 20, 2), (2, 10, 1), (2, 30, 2)]);
}
