// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn null_int_constant() -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::Null(LogicalType::Integer),
        LogicalType::Integer,
    ))
}

fn varchar_constant(value: &str) -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::Varchar(value.to_string()),
        LogicalType::Varchar,
    ))
}

#[test]
fn hash_join_spill_replay_source_is_independent_from_probe_for_in_memory_builds() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::HashJoinBuild,
        RowType::new(
            vec!["lv".to_string(), "rv".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        ),
        Default::default(),
    );
    let replay_id = PipelineId::new(0);
    handles.add_consumer(handle, replay_id).unwrap();
    let graph = PipelineGraph {
        pipelines: vec![PipelineSpec {
            id: replay_id,
            source: SourceSpec::HashJoinSpillReplay(HashJoinSpillReplaySourceSpec {
                handle,
                join_type: JoinType::Inner,
                anti_join_mode: AntiJoinMode::Regular,
                conditions: vec![join_condition()].into_boxed_slice(),
                probe_types: vec![LogicalType::Integer].into_boxed_slice(),
                build_payload_types: vec![LogicalType::Integer].into_boxed_slice(),
                left_projection: vec![0].into_boxed_slice(),
                output_names: vec!["lv".to_string(), "rv".to_string()].into_boxed_slice(),
                output_types: vec![LogicalType::Integer, LogicalType::Integer].into_boxed_slice(),
            }),
            transforms: Vec::new(),
            sink: SinkSpec::ClientResult(ClientResultSpec::default()),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: RowType::new(
                vec!["lv".to_string(), "rv".to_string()],
                vec![LogicalType::Integer, LogicalType::Integer],
            ),
        }],
        dependencies: Vec::new(),
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(replay_id),
    };
    let programs = PipelineProgramBuilder::default()
        .build_program_set(&graph)
        .expect("program set");
    let registry = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("handles"));
    let runtime = Arc::new(
        PipelineRuntime::with_registry(
            programs.get(replay_id).unwrap().clone(),
            registry,
            query.params.clone(),
            &query,
        )
        .expect("replay runtime"),
    );
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(20),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);
    assert!(output.pop_front().is_none());
}

#[test]
fn hash_join_external_spill_replay_source_outputs_probe_matches() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let build_row_type = RowType::new(
        vec!["rk".to_string(), "rv".to_string()],
        vec![LogicalType::Integer, LogicalType::Varchar],
    );
    let join_row_type = RowType::new(
        vec!["lv".to_string(), "rv".to_string()],
        vec![LogicalType::Integer, LogicalType::Varchar],
    );

    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::HashJoinBuild,
        join_row_type.clone(),
        Default::default(),
    );
    let build_id = PipelineId::new(0);
    let probe_id = PipelineId::new(1);
    let replay_id = PipelineId::new(2);
    handles.set_producer(handle, build_id).unwrap();
    handles.add_consumer(handle, probe_id).unwrap();
    handles.add_consumer(handle, replay_id).unwrap();

    let graph = PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: build_id,
                source: SourceSpec::Values(values_spec(
                    vec![
                        vec![int_constant(1), varchar_constant("ALGERIA")],
                        vec![
                            int_constant(2),
                            varchar_constant("a payload longer than the inline string limit"),
                        ],
                    ],
                    vec![LogicalType::Integer, LogicalType::Varchar],
                )),
                transforms: Vec::new(),
                sink: SinkSpec::HashJoinBuild(HashJoinBuildSinkSpec {
                    handle,
                    join_type: JoinType::Inner,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    build_projection: vec![1].into_boxed_slice(),
                    build_payload_types: vec![LogicalType::Varchar].into_boxed_slice(),
                    required: Default::default(),
                    force_external: true,
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: build_row_type,
            },
            PipelineSpec {
                id: probe_id,
                source: SourceSpec::Values(values_spec(
                    vec![
                        vec![int_constant(1), int_constant(100)],
                        vec![int_constant(3), int_constant(300)],
                    ],
                    vec![LogicalType::Integer, LogicalType::Integer],
                )),
                transforms: vec![TransformSpec::HashJoinProbe(HashJoinProbeSpec {
                    handle,
                    join_type: JoinType::Inner,
                    anti_join_mode: AntiJoinMode::Regular,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    left_projection: vec![1].into_boxed_slice(),
                    output_names: vec!["lv".to_string(), "rv".to_string()].into_boxed_slice(),
                    output_types: vec![LogicalType::Integer, LogicalType::Varchar]
                        .into_boxed_slice(),
                })],
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: join_row_type.clone(),
            },
            PipelineSpec {
                id: replay_id,
                source: SourceSpec::HashJoinSpillReplay(HashJoinSpillReplaySourceSpec {
                    handle,
                    join_type: JoinType::Inner,
                    anti_join_mode: AntiJoinMode::Regular,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    probe_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                    build_payload_types: vec![LogicalType::Varchar].into_boxed_slice(),
                    left_projection: vec![1].into_boxed_slice(),
                    output_names: vec!["lv".to_string(), "rv".to_string()].into_boxed_slice(),
                    output_types: vec![LogicalType::Integer, LogicalType::Varchar]
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
                consumer: replay_id,
                kind: DependencyKind::ProbeBeforeSpillReplay,
            },
        ],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(replay_id),
    };
    let programs = PipelineProgramBuilder::default()
        .build_program_set(&graph)
        .expect("program set");
    let registry = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("handles"));
    let runtimes = [build_id, probe_id, replay_id]
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
        task_id: PipelineTaskId(22),
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

    let chunk = output.pop_front().expect("external replay output");
    assert_eq!(chunk.size(), 1);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(100));
    assert_eq!(chunk.column(1).unwrap().get_string(0), Some("ALGERIA"));
    assert!(output.pop_front().is_none());
}

#[test]
fn hash_join_external_right_replay_emits_unmatched_build_rows_once() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let build_row_type = RowType::new(
        vec!["rk".to_string(), "rv".to_string()],
        vec![LogicalType::Integer, LogicalType::Integer],
    );
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
    let replay_id = PipelineId::new(2);
    let unmatched_id = PipelineId::new(3);
    handles.set_producer(handle, build_id).unwrap();
    handles.add_consumer(handle, probe_id).unwrap();
    handles.add_consumer(handle, replay_id).unwrap();
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
                    force_external: true,
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: build_row_type,
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
                    anti_join_mode: AntiJoinMode::Regular,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    left_projection: vec![1].into_boxed_slice(),
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
                id: replay_id,
                source: SourceSpec::HashJoinSpillReplay(HashJoinSpillReplaySourceSpec {
                    handle,
                    join_type: JoinType::Right,
                    anti_join_mode: AntiJoinMode::Regular,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    probe_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                    build_payload_types: vec![LogicalType::Integer].into_boxed_slice(),
                    left_projection: vec![1].into_boxed_slice(),
                    output_names: vec!["lv".to_string(), "rv".to_string()].into_boxed_slice(),
                    output_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                }),
                transforms: Vec::new(),
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
                consumer: replay_id,
                kind: DependencyKind::ProbeBeforeSpillReplay,
            },
            PipelineDependency {
                producer: replay_id,
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
    let runtimes = [build_id, probe_id, replay_id, unmatched_id]
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
        task_id: PipelineTaskId(23),
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

    let mut actual = Vec::new();
    while let Some(chunk) = output.pop_front() {
        for row in 0..chunk.size() {
            let left = if chunk.column(0).unwrap().is_null(row) {
                None
            } else {
                chunk.column(0).unwrap().get_i32(row)
            };
            let right = chunk.column(1).unwrap().get_i32(row);
            actual.push((left, right));
        }
    }
    actual.sort_unstable();
    assert_eq!(actual, vec![(None, Some(20)), (Some(100), Some(10))]);
}

#[test]
fn hash_join_external_right_replay_outputs_build_rows_when_probe_never_spilled() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let build_row_type = RowType::new(
        vec!["rk".to_string(), "rv".to_string()],
        vec![LogicalType::Integer, LogicalType::Integer],
    );
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
    let replay_id = PipelineId::new(1);
    handles.set_producer(handle, build_id).unwrap();
    handles.add_consumer(handle, replay_id).unwrap();

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
                    force_external: true,
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: build_row_type,
            },
            PipelineSpec {
                id: replay_id,
                source: SourceSpec::HashJoinSpillReplay(HashJoinSpillReplaySourceSpec {
                    handle,
                    join_type: JoinType::Right,
                    anti_join_mode: AntiJoinMode::Regular,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    probe_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                    build_payload_types: vec![LogicalType::Integer].into_boxed_slice(),
                    left_projection: vec![1].into_boxed_slice(),
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
        dependencies: vec![PipelineDependency {
            producer: build_id,
            consumer: replay_id,
            kind: DependencyKind::BuildBeforeProbe,
        }],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(replay_id),
    };
    let programs = PipelineProgramBuilder::default()
        .build_program_set(&graph)
        .expect("program set");
    let registry = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("handles"));
    let runtimes = [build_id, replay_id]
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
        task_id: PipelineTaskId(25),
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

    let mut actual = Vec::new();
    while let Some(chunk) = output.pop_front() {
        for row in 0..chunk.size() {
            assert!(chunk.column(0).unwrap().is_null(row));
            actual.push(chunk.column(1).unwrap().get_i32(row));
        }
    }
    actual.sort_unstable();
    assert_eq!(actual, vec![Some(10), Some(20)]);
}

#[test]
fn hash_join_external_mark_replay_preserves_global_build_null_marker() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let build_row_type = RowType::new(
        vec!["rk".to_string(), "rv".to_string()],
        vec![LogicalType::Integer, LogicalType::Integer],
    );
    let mark_row_type = RowType::new(
        vec!["lv".to_string(), "mark".to_string()],
        vec![LogicalType::Integer, LogicalType::Boolean],
    );

    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::HashJoinBuild,
        mark_row_type.clone(),
        Default::default(),
    );
    let build_id = PipelineId::new(0);
    let probe_id = PipelineId::new(1);
    let replay_id = PipelineId::new(2);
    handles.set_producer(handle, build_id).unwrap();
    handles.add_consumer(handle, probe_id).unwrap();
    handles.add_consumer(handle, replay_id).unwrap();

    let graph = PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: build_id,
                source: SourceSpec::Values(values_spec(
                    vec![
                        vec![int_constant(1), int_constant(10)],
                        vec![null_int_constant(), int_constant(20)],
                    ],
                    vec![LogicalType::Integer, LogicalType::Integer],
                )),
                transforms: Vec::new(),
                sink: SinkSpec::HashJoinBuild(HashJoinBuildSinkSpec {
                    handle,
                    join_type: JoinType::Mark,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    build_projection: vec![1].into_boxed_slice(),
                    build_payload_types: vec![LogicalType::Integer].into_boxed_slice(),
                    required: Default::default(),
                    force_external: true,
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: build_row_type,
            },
            PipelineSpec {
                id: probe_id,
                source: SourceSpec::Values(values_spec(
                    vec![
                        vec![int_constant(1), int_constant(100)],
                        vec![int_constant(2), int_constant(200)],
                        vec![null_int_constant(), int_constant(300)],
                    ],
                    vec![LogicalType::Integer, LogicalType::Integer],
                )),
                transforms: vec![TransformSpec::HashJoinProbe(HashJoinProbeSpec {
                    handle,
                    join_type: JoinType::Mark,
                    anti_join_mode: AntiJoinMode::Regular,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    left_projection: vec![1].into_boxed_slice(),
                    output_names: vec!["lv".to_string(), "mark".to_string()].into_boxed_slice(),
                    output_types: vec![LogicalType::Integer, LogicalType::Boolean]
                        .into_boxed_slice(),
                })],
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: mark_row_type.clone(),
            },
            PipelineSpec {
                id: replay_id,
                source: SourceSpec::HashJoinSpillReplay(HashJoinSpillReplaySourceSpec {
                    handle,
                    join_type: JoinType::Mark,
                    anti_join_mode: AntiJoinMode::Regular,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    probe_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                    build_payload_types: vec![LogicalType::Integer].into_boxed_slice(),
                    left_projection: vec![1].into_boxed_slice(),
                    output_names: vec!["lv".to_string(), "mark".to_string()].into_boxed_slice(),
                    output_types: vec![LogicalType::Integer, LogicalType::Boolean]
                        .into_boxed_slice(),
                }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: mark_row_type,
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
                consumer: replay_id,
                kind: DependencyKind::ProbeBeforeSpillReplay,
            },
        ],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(replay_id),
    };
    let (build_runtime, probe_runtime, replay_runtime) = {
        let programs = PipelineProgramBuilder::default()
            .build_program_set(&graph)
            .expect("program set");
        let registry =
            Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("handles"));
        (
            Arc::new(
                PipelineRuntime::with_registry(
                    programs.get(build_id).unwrap().clone(),
                    Arc::clone(&registry),
                    query.params.clone(),
                    &query,
                )
                .expect("build runtime"),
            ),
            Arc::new(
                PipelineRuntime::with_registry(
                    programs.get(probe_id).unwrap().clone(),
                    Arc::clone(&registry),
                    query.params.clone(),
                    &query,
                )
                .expect("probe runtime"),
            ),
            Arc::new(
                PipelineRuntime::with_registry(
                    programs.get(replay_id).unwrap().clone(),
                    registry,
                    query.params.clone(),
                    &query,
                )
                .expect("replay runtime"),
            ),
        )
    };
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(24),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    for runtime in [build_runtime, probe_runtime, replay_runtime] {
        let task = runtime
            .create_task_state(&query, paro_common::test_utils::test_allocator())
            .expect("task");
        let mut executor = PipelineTaskExecutor::new(runtime, task);
        run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);
    }

    let mut rows = Vec::new();
    while let Some(chunk) = output.pop_front() {
        let values = chunk.column(0).expect("value column");
        let marks = chunk.column(1).expect("mark column");
        for row in 0..chunk.size() {
            let mark = if marks.is_null(row) {
                None
            } else {
                marks.get_bool(row)
            };
            rows.push((values.get_i32(row).expect("left value"), mark));
        }
    }
    rows.sort_by_key(|(value, _)| *value);
    assert_eq!(rows, vec![(100, Some(true)), (200, None), (300, None)]);
}
