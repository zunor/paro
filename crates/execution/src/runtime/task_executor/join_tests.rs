// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn value_to_i32(value: &Value) -> Option<i32> {
    match value {
        Value::Integer(value) => Some(*value),
        _ => None,
    }
}

#[test]
fn hash_join_output_more_yields_between_output_chunks() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let row_type = RowType::new(
        vec!["lv".to_string(), "rv".to_string()],
        vec![LogicalType::Integer, LogicalType::Integer],
    );

    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::HashJoinBuild,
        RowType::new(
            vec!["rk".to_string(), "rv".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        ),
        Default::default(),
    );
    let build_id = PipelineId::new(0);
    let probe_id = PipelineId::new(1);
    handles.set_producer(handle, build_id).unwrap();
    handles.add_consumer(handle, probe_id).unwrap();

    let build_rows = (0..(paro_common::vector::VECTOR_SIZE * 2 + 7))
        .map(|idx| vec![int_constant(1), int_constant(idx as i32)])
        .collect::<Vec<_>>();
    let graph = PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: build_id,
                source: SourceSpec::Values(values_spec(
                    build_rows,
                    vec![LogicalType::Integer, LogicalType::Integer],
                )),
                transforms: Vec::new(),
                sink: SinkSpec::HashJoinBuild(HashJoinBuildSinkSpec {
                    handle,
                    join_type: JoinType::Inner,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    build_projection: vec![1].into_boxed_slice(),
                    build_payload_types: vec![LogicalType::Integer].into_boxed_slice(),
                    required: Default::default(),
                    force_external: false,
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: RowType::new(
                    vec!["rk".to_string(), "rv".to_string()],
                    vec![LogicalType::Integer, LogicalType::Integer],
                ),
            },
            PipelineSpec {
                id: probe_id,
                source: SourceSpec::Values(values_spec(
                    vec![vec![int_constant(1), int_constant(42)]],
                    vec![LogicalType::Integer, LogicalType::Integer],
                )),
                transforms: vec![TransformSpec::HashJoinProbe(HashJoinProbeSpec {
                    handle,
                    join_type: JoinType::Inner,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    left_projection: vec![1].into_boxed_slice(),
                    right_projection: vec![0].into_boxed_slice(),
                    output_names: vec!["lv".to_string(), "rv".to_string()].into_boxed_slice(),
                    output_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                })],
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: build_id,
            consumer: probe_id,
            kind: DependencyKind::BuildBeforeProbe,
        }],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(probe_id),
    };
    let programs = PipelineProgramBuilder::default()
        .build_program_set(&graph)
        .expect("program set");
    let registry = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("handles"));
    let build_runtime = Arc::new(
        PipelineRuntime::with_registry(
            programs.get(build_id).unwrap().clone(),
            Arc::clone(&registry),
            query.params.clone(),
            &query,
        )
        .expect("build runtime"),
    );
    let probe_runtime = Arc::new(
        PipelineRuntime::with_registry(
            programs.get(probe_id).unwrap().clone(),
            registry,
            query.params.clone(),
            &query,
        )
        .expect("probe runtime"),
    );

    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(21),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    let build_task = build_runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("build task");
    let mut build_exec = PipelineTaskExecutor::new(build_runtime, build_task);
    run_to_done(&mut build_exec, &query, &thread, &wake, &mut profiler);

    let probe_task = probe_runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("probe task");
    let mut probe_exec = PipelineTaskExecutor::new(probe_runtime, probe_task);
    let first = probe_exec
        .step(&mut step_context(&query, &thread, &wake, &mut profiler))
        .expect("first probe step");
    assert!(matches!(first, TaskStepResult::Continue));
    assert_eq!(
        probe_exec.phase,
        PipelineTaskPhase::RunningTransformOutputMore { transform_idx: 0 }
    );

    run_to_done(&mut probe_exec, &query, &thread, &wake, &mut profiler);

    let mut output_rows = 0;
    let mut chunks = 0;
    while let Some(chunk) = output.pop_front() {
        output_rows += chunk.size();
        chunks += 1;
    }
    assert_eq!(output_rows, paro_common::vector::VECTOR_SIZE * 2 + 7);
    assert!(chunks >= 3, "expected multiple output chunks");
}

#[test]
fn sort_range_join_uses_sorted_range_probe_candidates() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let row_type = RowType::new(
        vec!["lv".to_string(), "rv".to_string()],
        vec![LogicalType::Integer, LogicalType::Integer],
    );

    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::Materialized,
        RowType::new(
            vec!["r1".to_string(), "r2".to_string(), "rv".to_string()],
            vec![
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::Integer,
            ],
        ),
        Default::default(),
    );
    let build_id = PipelineId::new(0);
    let probe_id = PipelineId::new(1);
    handles.set_producer(handle, build_id).unwrap();
    handles.add_consumer(handle, probe_id).unwrap();

    let conditions = vec![
        JoinCondition::new(
            reference(0, LogicalType::Integer),
            reference(0, LogicalType::Integer),
            JoinComparisonType::LessThan,
        ),
        JoinCondition::new(
            reference(1, LogicalType::Integer),
            reference(1, LogicalType::Integer),
            JoinComparisonType::GreaterThan,
        ),
    ];
    let graph = PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: build_id,
                source: SourceSpec::Values(values_spec(
                    vec![
                        vec![int_constant(5), int_constant(0), int_constant(50)],
                        vec![int_constant(7), int_constant(3), int_constant(70)],
                        vec![int_constant(10), int_constant(4), int_constant(100)],
                        vec![int_constant(2), int_constant(9), int_constant(20)],
                    ],
                    vec![
                        LogicalType::Integer,
                        LogicalType::Integer,
                        LogicalType::Integer,
                    ],
                )),
                transforms: Vec::new(),
                sink: SinkSpec::Materialize(MaterializeSinkSpec {
                    handle,
                    required: Default::default(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: RowType::new(
                    vec!["r1".to_string(), "r2".to_string(), "rv".to_string()],
                    vec![
                        LogicalType::Integer,
                        LogicalType::Integer,
                        LogicalType::Integer,
                    ],
                ),
            },
            PipelineSpec {
                id: probe_id,
                source: SourceSpec::Values(values_spec(
                    vec![
                        vec![int_constant(4), int_constant(5), int_constant(100)],
                        vec![int_constant(6), int_constant(4), int_constant(200)],
                    ],
                    vec![
                        LogicalType::Integer,
                        LogicalType::Integer,
                        LogicalType::Integer,
                    ],
                )),
                transforms: vec![TransformSpec::SortRangeJoinProbe(SortRangeJoinProbeSpec {
                    handle,
                    join_type: JoinType::Inner,
                    conditions: conditions.into_boxed_slice(),
                    mark_null_condition_start: None,
                    left_projection: vec![2].into_boxed_slice(),
                    right_projection: vec![2].into_boxed_slice(),
                    right_output_types: vec![
                        LogicalType::Integer,
                        LogicalType::Integer,
                        LogicalType::Integer,
                    ]
                    .into_boxed_slice(),
                    output_names: vec!["lv".to_string(), "rv".to_string()].into_boxed_slice(),
                    output_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                })],
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: build_id,
            consumer: probe_id,
            kind: DependencyKind::BuildBeforeProbe,
        }],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(probe_id),
    };

    let (build_runtime, probe_runtime) = runtimes_from_graph(&query, &graph);
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(23),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    let build_task = build_runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("build task");
    let mut build_exec = PipelineTaskExecutor::new(build_runtime, build_task);
    run_to_done(&mut build_exec, &query, &thread, &wake, &mut profiler);

    let probe_task = probe_runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("probe task");
    let mut probe_exec = PipelineTaskExecutor::new(probe_runtime, probe_task);
    run_to_done(&mut probe_exec, &query, &thread, &wake, &mut profiler);

    let mut rows = Vec::new();
    while let Some(chunk) = output.pop_front() {
        for row in 0..chunk.size() {
            rows.push((chunk.data[0].get_value(row), chunk.data[1].get_value(row)));
        }
    }
    rows.sort_by_key(|(left, right)| {
        (
            value_to_i32(left).expect("left int"),
            value_to_i32(right).expect("right int"),
        )
    });
    assert_eq!(
        rows,
        vec![
            (Value::Integer(100), Value::Integer(50)),
            (Value::Integer(100), Value::Integer(70)),
            (Value::Integer(100), Value::Integer(100)),
            (Value::Integer(200), Value::Integer(70)),
        ]
    );
}

#[test]
fn hash_join_single_probe_errors_on_duplicate_build_matches() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output);
    let join_row_type = RowType::new(
        vec!["lv".to_string(), "rv".to_string()],
        vec![LogicalType::Integer, LogicalType::Integer],
    );

    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::HashJoinBuild,
        join_row_type.clone(),
        Default::default(),
    );
    let build_id = PipelineId::new(0);
    let probe_id = PipelineId::new(1);
    handles.set_producer(handle, build_id).unwrap();
    handles.add_consumer(handle, probe_id).unwrap();

    let graph = PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: build_id,
                source: SourceSpec::Values(values_spec(
                    vec![
                        vec![int_constant(1), int_constant(10)],
                        vec![int_constant(1), int_constant(11)],
                    ],
                    vec![LogicalType::Integer, LogicalType::Integer],
                )),
                transforms: Vec::new(),
                sink: SinkSpec::HashJoinBuild(HashJoinBuildSinkSpec {
                    handle,
                    join_type: JoinType::Single,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    build_projection: vec![1].into_boxed_slice(),
                    build_payload_types: vec![LogicalType::Integer].into_boxed_slice(),
                    required: Default::default(),
                    force_external: false,
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: RowType::new(
                    vec!["rk".to_string(), "rv".to_string()],
                    vec![LogicalType::Integer, LogicalType::Integer],
                ),
            },
            PipelineSpec {
                id: probe_id,
                source: SourceSpec::Values(values_spec(
                    vec![vec![int_constant(1), int_constant(100)]],
                    vec![LogicalType::Integer, LogicalType::Integer],
                )),
                transforms: vec![TransformSpec::HashJoinProbe(HashJoinProbeSpec {
                    handle,
                    join_type: JoinType::Single,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    left_projection: vec![1].into_boxed_slice(),
                    right_projection: vec![0].into_boxed_slice(),
                    output_names: vec!["lv".to_string(), "rv".to_string()].into_boxed_slice(),
                    output_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                })],
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: join_row_type,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: build_id,
            consumer: probe_id,
            kind: DependencyKind::BuildBeforeProbe,
        }],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(probe_id),
    };
    let programs = PipelineProgramBuilder::default()
        .build_program_set(&graph)
        .expect("program set");
    let registry = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("handles"));
    let build_runtime = Arc::new(
        PipelineRuntime::with_registry(
            programs.get(build_id).unwrap().clone(),
            Arc::clone(&registry),
            query.params.clone(),
            &query,
        )
        .expect("build runtime"),
    );
    let probe_runtime = Arc::new(
        PipelineRuntime::with_registry(
            programs.get(probe_id).unwrap().clone(),
            registry,
            query.params.clone(),
            &query,
        )
        .expect("probe runtime"),
    );

    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(22),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    let build_task = build_runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("build task");
    let mut build_exec = PipelineTaskExecutor::new(build_runtime, build_task);
    run_to_done(&mut build_exec, &query, &thread, &wake, &mut profiler);

    let probe_task = probe_runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("probe task");
    let mut probe_exec = PipelineTaskExecutor::new(probe_runtime, probe_task);
    let err = probe_exec
        .step(&mut step_context(&query, &thread, &wake, &mut profiler))
        .expect_err("duplicate single join match should fail");
    assert!(err
        .to_string()
        .contains("More than one row returned by a SINGLE join"));
}

#[test]
fn hash_join_unmatched_source_emits_right_side_rows_after_probe() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let join_row_type = RowType::new(
        vec!["lv".to_string(), "rv".to_string()],
        vec![LogicalType::Integer, LogicalType::Integer],
    );
    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::HashJoinBuild,
        join_row_type.clone(),
        Default::default(),
    );
    let build_id = PipelineId::new(0);
    let probe_id = PipelineId::new(1);
    let unmatched_id = PipelineId::new(2);
    handles.set_producer(handle, build_id).unwrap();
    handles.add_consumer(handle, probe_id).unwrap();
    handles.add_consumer(handle, unmatched_id).unwrap();

    let graph = PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: build_id,
                source: SourceSpec::Values(values_spec(
                    vec![
                        vec![int_constant(1), int_constant(10)],
                        vec![int_constant(2), int_constant(20)],
                    ],
                    vec![LogicalType::Integer, LogicalType::Integer],
                )),
                transforms: Vec::new(),
                sink: SinkSpec::HashJoinBuild(HashJoinBuildSinkSpec {
                    handle,
                    join_type: JoinType::Right,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    build_projection: vec![1].into_boxed_slice(),
                    build_payload_types: vec![LogicalType::Integer].into_boxed_slice(),
                    required: Default::default(),
                    force_external: false,
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: RowType::new(
                    vec!["rk".to_string(), "rv".to_string()],
                    vec![LogicalType::Integer, LogicalType::Integer],
                ),
            },
            PipelineSpec {
                id: probe_id,
                source: SourceSpec::Values(values_spec(
                    vec![vec![int_constant(1), int_constant(100)]],
                    vec![LogicalType::Integer, LogicalType::Integer],
                )),
                transforms: vec![TransformSpec::HashJoinProbe(HashJoinProbeSpec {
                    handle,
                    join_type: JoinType::Right,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    left_projection: vec![1].into_boxed_slice(),
                    right_projection: vec![0].into_boxed_slice(),
                    output_names: vec!["lv".to_string(), "rv".to_string()].into_boxed_slice(),
                    output_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                })],
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: join_row_type.clone(),
            },
            PipelineSpec {
                id: unmatched_id,
                source: SourceSpec::HashJoinUnmatched(HashJoinUnmatchedSourceSpec {
                    handle,
                    join_type: JoinType::Right,
                    left_output_types: vec![LogicalType::Integer].into_boxed_slice(),
                    right_projection: vec![0].into_boxed_slice(),
                    output_names: vec!["lv".to_string(), "rv".to_string()].into_boxed_slice(),
                    output_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: join_row_type,
            },
        ],
        dependencies: vec![
            PipelineDependency {
                producer: build_id,
                consumer: probe_id,
                kind: DependencyKind::BuildBeforeProbe,
            },
            PipelineDependency {
                producer: probe_id,
                consumer: unmatched_id,
                kind: DependencyKind::FinalizeBeforeEmit,
            },
        ],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(unmatched_id),
    };
    let programs = PipelineProgramBuilder::default()
        .build_program_set(&graph)
        .expect("program set");
    let registry = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("handles"));
    let runtimes = [build_id, probe_id, unmatched_id]
        .into_iter()
        .map(|id| {
            Arc::new(
                PipelineRuntime::with_registry(
                    programs.get(id).unwrap().clone(),
                    Arc::clone(&registry),
                    query.params.clone(),
                    &query,
                )
                .expect("runtime"),
            )
        })
        .collect::<Vec<_>>();
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(19),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    for runtime in &runtimes {
        let task = runtime
            .create_task_state(&query, paro_common::test_utils::test_allocator())
            .expect("task");
        let mut executor = PipelineTaskExecutor::new(Arc::clone(runtime), task);
        run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);
    }

    let matched = output.pop_front().expect("right join matched output");
    assert_eq!(matched.size(), 1);
    assert_eq!(matched.column(0).unwrap().get_i32(0), Some(100));
    assert_eq!(matched.column(1).unwrap().get_i32(0), Some(10));

    let unmatched = output.pop_front().expect("right join unmatched output");
    assert_eq!(unmatched.size(), 1);
    assert!(unmatched.column(0).unwrap().is_null(0));
    assert_eq!(unmatched.column(1).unwrap().get_i32(0), Some(20));
}

#[test]
fn project_filter_limit_chain_pushes_without_dyn_dispatch() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Values(values_spec(
            vec![
                vec![bool_constant(true), int_constant(10)],
                vec![bool_constant(false), int_constant(20)],
                vec![bool_constant(true), int_constant(30)],
            ],
            vec![LogicalType::Boolean, LogicalType::Integer],
        )),
        transforms: vec![
            TransformSpec::Filter(FilterSpec {
                expressions: vec![reference(0, LogicalType::Boolean)].into_boxed_slice(),
                projection_map: vec![1].into_boxed_slice(),
            }),
            TransformSpec::Project(ProjectSpec {
                table_index: 0,
                expressions: vec![reference(0, LogicalType::Integer)].into_boxed_slice(),
                output_names: vec!["v".to_string()].into_boxed_slice(),
            }),
            TransformSpec::Limit(LimitSpec {
                limit: Some(int_constant(1)),
                offset: None,
                hnsw_ef_hint: None,
            }),
        ],
        sink: SinkSpec::ClientResult(ClientResultSpec::default()),
        sink_sharing: SinkSharing::Exclusive,
        properties: PipelineProperties::default(),
        output: RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]),
    };
    let runtime = runtime_from_spec(&query, spec);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(11),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);

    let chunk = output.pop_front().expect("pipeline output chunk");
    assert_eq!(chunk.size(), 1);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(10));
    assert!(output.pop_front().is_none());
}

#[test]
fn chunk_and_expression_sources_push_without_physical_operator_bridge() {
    let mut chunk = Chunk::try_initialize(
        &[LogicalType::Integer],
        2,
        paro_common::test_utils::test_allocator(),
    )
    .expect("chunk");
    chunk.try_set_cardinality(2).unwrap();
    chunk.set_value(0, 0, &Value::Integer(41)).unwrap();
    chunk.set_value(0, 1, &Value::Integer(42)).unwrap();

    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Chunk(ChunkScanSpec {
            chunks: Arc::from(vec![chunk].into_boxed_slice()),
            output_names: vec!["v".to_string()].into_boxed_slice(),
            output_types: vec![LogicalType::Integer].into_boxed_slice(),
        }),
        transforms: Vec::new(),
        sink: SinkSpec::ClientResult(ClientResultSpec::default()),
        sink_sharing: SinkSharing::Exclusive,
        properties: PipelineProperties::default(),
        output: RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]),
    };
    let runtime = runtime_from_spec(&query, spec);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(15),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);

    let chunk = output.pop_front().expect("chunk source output");
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(41));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(42));

    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Expression(ExpressionScanSpec {
            table_index: 0,
            expressions: vec![vec![int_constant(7)], vec![int_constant(8)]]
                .into_iter()
                .map(Vec::into_boxed_slice)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            output_names: vec!["v".to_string()].into_boxed_slice(),
            output_types: vec![LogicalType::Integer].into_boxed_slice(),
        }),
        transforms: Vec::new(),
        sink: SinkSpec::ClientResult(ClientResultSpec::default()),
        sink_sharing: SinkSharing::Exclusive,
        properties: PipelineProperties::default(),
        output: RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]),
    };
    let runtime = runtime_from_spec(&query, spec);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    let mut profiler = OperatorProfiler::disabled();
    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);

    let chunk = output.pop_front().expect("expression source output");
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(7));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(8));
}

#[test]
fn table_function_source_runs_bound_function_in_typed_source_path() {
    #[derive(Debug)]
    struct TestLocalState {
        emitted: bool,
    }

    impl LocalTableFunctionState for TestLocalState {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    fn init_local(
        _input: &TableFunctionInitInput,
        _global: Option<&dyn paro_function::table::GlobalTableFunctionState>,
    ) -> paro_common::error::Result<Option<Box<dyn LocalTableFunctionState>>> {
        Ok(Some(Box::new(TestLocalState { emitted: false })))
    }

    fn table_function(
        input: &mut TableFunctionInput,
        output: &mut Chunk,
    ) -> paro_common::error::Result<TableFunctionResult> {
        let state = input
            .local_state
            .as_deref_mut()
            .and_then(|state| state.as_any_mut().downcast_mut::<TestLocalState>())
            .expect("test local state");
        if state.emitted {
            output.try_set_cardinality(0)?;
            return Ok(TableFunctionResult::Finished);
        }
        output.try_set_cardinality(2)?;
        output.set_value(0, 0, &Value::Integer(5)).unwrap();
        output.set_value(0, 1, &Value::Integer(6)).unwrap();
        state.emitted = true;
        Ok(TableFunctionResult::Finished)
    }

    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let function = Arc::new(
        TableFunction::new("typed_test", vec![])
            .with_init_local(init_local)
            .with_function(table_function),
    );
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::TableFunction(TableFunctionScanSpec {
            function,
            bind_data: None,
            table_index: 0,
            arguments: Box::new([]),
            projection_ids: None,
            input_table_types: Box::new([]),
            input_table_names: Box::new([]),
            output_names: vec!["v".to_string()].into_boxed_slice(),
            output_types: vec![LogicalType::Integer].into_boxed_slice(),
            with_ordinality: false,
        }),
        transforms: Vec::new(),
        sink: SinkSpec::ClientResult(ClientResultSpec::default()),
        sink_sharing: SinkSharing::Exclusive,
        properties: PipelineProperties::default(),
        output: RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]),
    };
    let runtime = runtime_from_spec(&query, spec);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(16),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);

    let chunk = output.pop_front().expect("table function output");
    assert_eq!(chunk.size(), 2);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(5));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(6));
}

#[test]
fn topn_aggregate_and_window_stream_through_typed_transforms() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Values(values_spec(
            vec![
                vec![int_constant(3)],
                vec![int_constant(1)],
                vec![int_constant(2)],
            ],
            vec![LogicalType::Integer],
        )),
        transforms: vec![TransformSpec::StreamingTopN(TopNSpec {
            orders: vec![OrderByNode {
                expression: reference(0, LogicalType::Integer),
                ascending: true,
                nulls_first: true,
            }]
            .into_boxed_slice(),
            limit: 2,
            offset: 0,
            hnsw_ef_hint: None,
            output_names: vec!["v".to_string()].into_boxed_slice(),
            output_types: vec![LogicalType::Integer].into_boxed_slice(),
        })],
        sink: SinkSpec::ClientResult(ClientResultSpec::default()),
        sink_sharing: SinkSharing::Exclusive,
        properties: PipelineProperties::default(),
        output: RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]),
    };
    let runtime = runtime_from_spec(&query, spec);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(17),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);
    let chunk = output.pop_front().expect("topn output");
    assert_eq!(chunk.size(), 2);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(1));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(2));

    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let aggregate = Expression::Aggregate(AggregateExpression::new(
        get_count_star_function(),
        vec![],
        LogicalType::BigInt,
    ));
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Values(values_spec(
            vec![
                vec![int_constant(10)],
                vec![int_constant(20)],
                vec![int_constant(30)],
            ],
            vec![LogicalType::Integer],
        )),
        transforms: vec![TransformSpec::StreamingAggregate(AggregateSpec {
            grouping_key_count: 0,
            projection_exprs: Box::new([]),
            payload_types: Box::new([]),
            groups: Box::new([]),
            grouping_sets: Box::new([]),
            aggregates: vec![aggregate].into_boxed_slice(),
            grouping_functions: Box::new([]),
            aggregate_inputs: vec![Vec::<usize>::new().into_boxed_slice()].into_boxed_slice(),
            aggregate_filters: vec![None].into_boxed_slice(),
            aggregate_orders: vec![Vec::<usize>::new().into_boxed_slice()].into_boxed_slice(),
            perfect_hash: None,
            output_names: vec!["count".to_string()].into_boxed_slice(),
            output_types: vec![LogicalType::BigInt].into_boxed_slice(),
        })],
        sink: SinkSpec::ClientResult(ClientResultSpec::default()),
        sink_sharing: SinkSharing::Exclusive,
        properties: PipelineProperties::default(),
        output: RowType::new(vec!["count".to_string()], vec![LogicalType::BigInt]),
    };
    let runtime = runtime_from_spec(&query, spec);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    let mut profiler = OperatorProfiler::disabled();
    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);
    let chunk = output.pop_front().expect("aggregate output");
    assert_eq!(chunk.size(), 1);
    assert_eq!(chunk.column(0).unwrap().get_i64(0), Some(3));

    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let row_number = WindowFunction::row_number();
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Values(values_spec(
            vec![vec![int_constant(10)], vec![int_constant(20)]],
            vec![LogicalType::Integer],
        )),
        transforms: vec![TransformSpec::StreamingWindow(WindowSpec {
            window_index: 0,
            expressions: vec![WindowExpression {
                function: row_number.clone(),
                children: Vec::new(),
                partitions: Vec::new(),
                orders: Vec::new(),
                frame: WindowFrame::get_default_frame(&row_number),
                ignore_nulls: false,
                return_type: LogicalType::BigInt,
            }]
            .into_boxed_slice(),
            input_width: 1,
            output_names: vec!["v".to_string(), "rn".to_string()].into_boxed_slice(),
            output_types: vec![LogicalType::Integer, LogicalType::BigInt].into_boxed_slice(),
        })],
        sink: SinkSpec::ClientResult(ClientResultSpec::default()),
        sink_sharing: SinkSharing::Exclusive,
        properties: PipelineProperties::default(),
        output: RowType::new(
            vec!["v".to_string(), "rn".to_string()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        ),
    };
    let runtime = runtime_from_spec(&query, spec);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    let mut profiler = OperatorProfiler::disabled();
    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);
    let chunk = output.pop_front().expect("window output");
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(10));
    assert_eq!(chunk.column(1).unwrap().get_i64(0), Some(1));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(20));
    assert_eq!(chunk.column(1).unwrap().get_i64(1), Some(2));
}
