// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn empty_source_pipeline_reaches_done_through_completion_order() {
    let query = query_context(QueryOutputPort::unbounded());
    let runtime = empty_runtime(&query);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(1),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    for _ in 0..8 {
        let result = executor
            .step(&mut step_context(&query, &thread, &wake, &mut profiler))
            .expect("task step");
        if matches!(result, TaskStepResult::Done) {
            break;
        }
    }

    assert_eq!(executor.phase, PipelineTaskPhase::Done);
}

fn shared_empty_result_runtime(
    query: &QueryRuntimeContext,
    shared: crate::pipeline::graph::SharedSinkId,
    coordinator: Arc<SharedSinkCoordinator>,
    id: PipelineId,
) -> Arc<PipelineRuntime> {
    let program = Arc::new(
        PipelineProgramBuilder::default()
            .build_program(&PipelineSpec {
                id,
                source: SourceSpec::Empty(EmptyResultSpec),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Shared(shared),
                properties: PipelineProperties::default(),
                output: RowType::new(Vec::new(), Vec::<LogicalType>::new()),
            })
            .expect("program"),
    );
    Arc::new(
        PipelineRuntime::with_registry_and_shared_sink(
            program,
            Arc::new(BreakerHandleRegistry::default()),
            query.params.clone(),
            query,
            Some(coordinator),
        )
        .expect("runtime"),
    )
}

#[test]
fn shared_sink_finish_runs_once_after_all_producers_merge() {
    let query = query_context(QueryOutputPort::unbounded());
    let shared = crate::pipeline::graph::SharedSinkId::new(0);
    let coordinator = Arc::new(SharedSinkCoordinator::new(shared));
    coordinator.register_producer().expect("first producer");
    coordinator.register_producer().expect("second producer");
    coordinator.freeze_producer_count().expect("freeze");

    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(1),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    for id in [PipelineId::new(0), PipelineId::new(1)] {
        let runtime = shared_empty_result_runtime(&query, shared, coordinator.clone(), id);
        let task = runtime
            .create_task_state(&query, paro_common::test_utils::test_allocator())
            .expect("task");
        let mut executor = PipelineTaskExecutor::new(runtime, task);
        run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);
    }

    assert_eq!(coordinator.merged_count(), 2);
    assert_eq!(coordinator.state(), SharedSinkState::Finished);
}

#[test]
fn shared_sink_failure_reaches_remaining_producer_runtime() {
    let query = query_context(QueryOutputPort::unbounded());
    let shared = crate::pipeline::graph::SharedSinkId::new(0);
    let coordinator = Arc::new(SharedSinkCoordinator::new(shared));
    coordinator.register_producer().expect("first producer");
    coordinator.register_producer().expect("second producer");
    coordinator.freeze_producer_count().expect("freeze");

    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(1),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    let first =
        shared_empty_result_runtime(&query, shared, coordinator.clone(), PipelineId::new(0));
    let first_task = first
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("first task");
    let mut first_executor = PipelineTaskExecutor::new(first, first_task);
    run_to_done(&mut first_executor, &query, &thread, &wake, &mut profiler);
    assert_eq!(coordinator.merged_count(), 1);
    assert_eq!(coordinator.state(), SharedSinkState::Open);

    assert!(coordinator.fail(QueryErrorId::new(23)));

    let second =
        shared_empty_result_runtime(&query, shared, coordinator.clone(), PipelineId::new(1));
    let second_task = second
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("second task");
    let mut second_executor = PipelineTaskExecutor::new(second, second_task);

    let mut terminal_error = None;
    for _ in 0..32 {
        match second_executor.step(&mut step_context(&query, &thread, &wake, &mut profiler)) {
            Ok(TaskStepResult::Done) => panic!("failed shared sink should reject producer"),
            Ok(TaskStepResult::Continue) | Ok(TaskStepResult::Blocked(_)) => {}
            Err(error) => {
                terminal_error = Some(error);
                break;
            }
        }
    }
    let err = terminal_error.expect("failed shared sink should surface terminal error");

    assert!(err.message().contains("shared sink failed"));
    assert_eq!(coordinator.merged_count(), 1);
    assert_eq!(
        coordinator.state(),
        SharedSinkState::Failed(QueryErrorId::new(23))
    );
}

#[test]
fn completion_result_uses_pending_chunk_when_root_output_is_blocked() {
    let output = QueryOutputPort::bounded(1);
    output
        .try_push(Chunk::try_new(paro_common::test_utils::test_allocator()).unwrap())
        .assert_written();
    let query = query_context(output.clone());
    let runtime = empty_runtime(&query);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    executor.phase = PipelineTaskPhase::Merging;
    executor.completion_stage = PipelineCompletionStage::Finish;

    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(7),
        generation: WakeGeneration(3),
    };
    let mut profiler = OperatorProfiler::disabled();
    let chunk = Chunk::try_new(paro_common::test_utils::test_allocator()).unwrap();

    let result = executor
        .write_completion_result(
            &mut step_context(&query, &thread, &wake, &mut profiler),
            chunk,
        )
        .expect("write completion result");
    let TaskStepResult::Blocked(blocker) = result else {
        panic!("completion output should block");
    };
    assert_eq!(executor.phase, PipelineTaskPhase::Blocked);
    assert!(matches!(
        executor.task.pending,
        PendingChunkState::CompletionResult { .. }
    ));
    assert_eq!(
        blocker.wake.expect("output wake").task_id,
        PipelineTaskId(7)
    );

    output.pop_front();
    executor.resume_after_wake().expect("resume");
    let result = executor
        .step(&mut step_context(&query, &thread, &wake, &mut profiler))
        .expect("resume completion");
    assert!(matches!(result, TaskStepResult::Done));
    assert_eq!(output.len(), 1);
}

#[test]
fn values_source_pushes_owned_chunk_to_client_result() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Values(values_spec(
            vec![vec![int_constant(10)], vec![int_constant(20)]],
            vec![LogicalType::Integer],
        )),
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
        task_id: PipelineTaskId(9),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);

    let chunk = output.pop_front().expect("values output chunk");
    assert_eq!(chunk.size(), 2);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(10));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(20));
}

#[test]
fn materialized_breaker_moves_chunks_through_typed_handle() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let row_type = RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]);

    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::Materialized,
        row_type.clone(),
        Default::default(),
    );
    let producer_id = PipelineId::new(0);
    let consumer_id = PipelineId::new(1);
    handles.set_producer(handle, producer_id).unwrap();
    handles.add_consumer(handle, consumer_id).unwrap();
    let handle_catalog = handles.finish();

    let graph = PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: producer_id,
                source: SourceSpec::Values(values_spec(
                    vec![vec![int_constant(11)], vec![int_constant(22)]],
                    vec![LogicalType::Integer],
                )),
                transforms: Vec::new(),
                sink: SinkSpec::Materialize(MaterializeSinkSpec {
                    handle,
                    required: Default::default(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type.clone(),
            },
            PipelineSpec {
                id: consumer_id,
                source: SourceSpec::Materialized(MaterializedSourceSpec { handle }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: producer_id,
            consumer: consumer_id,
            kind: DependencyKind::MaterializeBeforeRead,
        }],
        handles: handle_catalog,
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(consumer_id),
    };
    let programs = PipelineProgramBuilder::default()
        .build_program_set(&graph)
        .expect("program set");
    let registry = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("handles"));
    let producer = Arc::new(
        PipelineRuntime::with_registry(
            programs.get(producer_id).unwrap().clone(),
            Arc::clone(&registry),
            query.params.clone(),
            &query,
        )
        .expect("producer runtime"),
    );
    let consumer = Arc::new(
        PipelineRuntime::with_registry(
            programs.get(consumer_id).unwrap().clone(),
            registry,
            query.params.clone(),
            &query,
        )
        .expect("consumer runtime"),
    );

    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(10),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    let producer_task = producer
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("producer task");
    let mut producer_exec = PipelineTaskExecutor::new(producer, producer_task);
    run_to_done(&mut producer_exec, &query, &thread, &wake, &mut profiler);

    let consumer_task = consumer
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("consumer task");
    let mut consumer_exec = PipelineTaskExecutor::new(consumer, consumer_task);
    run_to_done(&mut consumer_exec, &query, &thread, &wake, &mut profiler);

    let chunk = output.pop_front().expect("materialized output chunk");
    assert_eq!(chunk.size(), 2);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(11));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(22));
}

#[test]
fn cte_materialize_scan_gives_each_consumer_independent_cursor() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let row_type = RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]);

    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(BreakerHandleKind::Cte, row_type.clone(), Default::default());
    let producer_id = PipelineId::new(0);
    let first_consumer_id = PipelineId::new(1);
    let second_consumer_id = PipelineId::new(2);
    handles.set_producer(handle, producer_id).unwrap();
    handles.add_consumer(handle, first_consumer_id).unwrap();
    handles.add_consumer(handle, second_consumer_id).unwrap();

    let graph = PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: producer_id,
                source: SourceSpec::Values(values_spec(
                    vec![vec![int_constant(7)], vec![int_constant(8)]],
                    vec![LogicalType::Integer],
                )),
                transforms: Vec::new(),
                sink: SinkSpec::CteMaterialize(CteMaterializeSinkSpec {
                    handle,
                    required: Default::default(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type.clone(),
            },
            PipelineSpec {
                id: first_consumer_id,
                source: SourceSpec::CteScan(CteScanSourceSpec { handle }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type.clone(),
            },
            PipelineSpec {
                id: second_consumer_id,
                source: SourceSpec::CteScan(CteScanSourceSpec { handle }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type,
            },
        ],
        dependencies: vec![
            PipelineDependency {
                producer: producer_id,
                consumer: first_consumer_id,
                kind: DependencyKind::MaterializeBeforeRead,
            },
            PipelineDependency {
                producer: producer_id,
                consumer: second_consumer_id,
                kind: DependencyKind::MaterializeBeforeRead,
            },
        ],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(first_consumer_id),
    };
    graph.validate().expect("valid CTE graph");
    let programs = PipelineProgramBuilder::default()
        .build_program_set(&graph)
        .expect("program set");
    let registry = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).expect("handles"));
    let runtime_for = |id| {
        Arc::new(
            PipelineRuntime::with_registry(
                programs.get(id).expect("program").clone(),
                Arc::clone(&registry),
                query.params.clone(),
                &query,
            )
            .expect("runtime"),
        )
    };

    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(11),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    for runtime in [
        runtime_for(producer_id),
        runtime_for(first_consumer_id),
        runtime_for(second_consumer_id),
    ] {
        let task = runtime
            .create_task_state(&query, paro_common::test_utils::test_allocator())
            .expect("task");
        let mut executor = PipelineTaskExecutor::new(runtime, task);
        run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);
    }

    let first = output.pop_front().expect("first CTE consumer chunk");
    let second = output.pop_front().expect("second CTE consumer chunk");
    assert_eq!(first.size(), 2);
    assert_eq!(second.size(), 2);
    assert_eq!(first.column(0).unwrap().get_i32(0), Some(7));
    assert_eq!(first.column(0).unwrap().get_i32(1), Some(8));
    assert_eq!(second.column(0).unwrap().get_i32(0), Some(7));
    assert_eq!(second.column(0).unwrap().get_i32(1), Some(8));
}

#[test]
fn delim_capture_deduplicates_values_and_keeps_cached_outer_explicit() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let mut handles = BreakerHandleCatalogBuilder::default();
    let row_type = RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]);
    let delim = handles.register(
        BreakerHandleKind::Delim,
        row_type.clone(),
        PipelineProperties::default(),
    );
    let cached_outer = handles.register(
        BreakerHandleKind::Delim,
        row_type.clone(),
        PipelineProperties::default(),
    );
    let graph = PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: PipelineId::new(0),
                source: SourceSpec::Values(values_spec(
                    vec![
                        vec![int_constant(1)],
                        vec![int_constant(1)],
                        vec![int_constant(2)],
                    ],
                    vec![LogicalType::Integer],
                )),
                transforms: Vec::new(),
                sink: SinkSpec::DelimCapture(DelimCaptureSinkSpec {
                    handle: delim,
                    duplicate_keys: vec![Expression::Reference(ReferenceExpression::new(
                        0,
                        LogicalType::Integer,
                    ))]
                    .into_boxed_slice(),
                    cached_outer: Some(cached_outer),
                    required: Default::default(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type.clone(),
            },
            PipelineSpec {
                id: PipelineId::new(1),
                source: SourceSpec::DelimScan(DelimScanSourceSpec { handle: delim }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: row_type,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: PipelineId::new(0),
            consumer: PipelineId::new(1),
            kind: DependencyKind::FinalizeBeforeEmit,
        }],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(PipelineId::new(1)),
    };

    let (capture_runtime, scan_runtime) = runtimes_from_graph(&query, &graph);
    let delim_handle = capture_runtime
        .breaker_handles
        .get(HandleRef::<DelimHandle>::new(delim))
        .expect("delim handle");
    let cached_outer_handle = capture_runtime
        .breaker_handles
        .get(HandleRef::<DelimHandle>::new(cached_outer))
        .expect("cached outer handle");
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(1),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    let capture_task = capture_runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("capture task");
    let mut capture_executor = PipelineTaskExecutor::new(capture_runtime, capture_task);
    run_to_done(&mut capture_executor, &query, &thread, &wake, &mut profiler);
    assert!(delim_handle.is_capture_sealed());
    assert_eq!(delim_handle.distinct_key_count(), 2);
    assert_eq!(cached_outer_handle.sealed_value_chunk_count(), 1);

    let scan_task = scan_runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("scan task");
    let mut scan_executor = PipelineTaskExecutor::new(scan_runtime, scan_task);
    run_to_done(&mut scan_executor, &query, &thread, &wake, &mut profiler);

    let chunk = output.pop_front().expect("delim output chunk");
    assert_eq!(chunk.size(), 2);
    assert_eq!(chunk.get_value(0, 0), Some(Value::Integer(1)));
    assert_eq!(chunk.get_value(0, 1), Some(Value::Integer(2)));
}

#[test]
fn delim_capture_with_no_duplicate_keys_stores_zero_column_values() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let mut handles = BreakerHandleCatalogBuilder::default();
    let input_row_type = RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]);
    let delim_row_type = RowType::new(Vec::new(), Vec::new());
    let delim = handles.register(
        BreakerHandleKind::Delim,
        delim_row_type.clone(),
        PipelineProperties::default(),
    );
    let cached_outer = handles.register(
        BreakerHandleKind::Delim,
        input_row_type.clone(),
        PipelineProperties::default(),
    );
    let graph = PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: PipelineId::new(0),
                source: SourceSpec::Values(values_spec(
                    vec![
                        vec![int_constant(1)],
                        vec![int_constant(2)],
                        vec![int_constant(3)],
                    ],
                    vec![LogicalType::Integer],
                )),
                transforms: Vec::new(),
                sink: SinkSpec::DelimCapture(DelimCaptureSinkSpec {
                    handle: delim,
                    duplicate_keys: Vec::new().into_boxed_slice(),
                    cached_outer: Some(cached_outer),
                    required: Default::default(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: input_row_type,
            },
            PipelineSpec {
                id: PipelineId::new(1),
                source: SourceSpec::DelimScan(DelimScanSourceSpec { handle: delim }),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: delim_row_type,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: PipelineId::new(0),
            consumer: PipelineId::new(1),
            kind: DependencyKind::FinalizeBeforeEmit,
        }],
        handles: handles.finish(),
        control_regions: Vec::new(),
        root: PipelineRoot::Pipeline(PipelineId::new(1)),
    };

    let (capture_runtime, scan_runtime) = runtimes_from_graph(&query, &graph);
    let delim_handle = capture_runtime
        .breaker_handles
        .get(HandleRef::<DelimHandle>::new(delim))
        .expect("delim handle");
    let cached_outer_handle = capture_runtime
        .breaker_handles
        .get(HandleRef::<DelimHandle>::new(cached_outer))
        .expect("cached outer handle");
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(2),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    let capture_task = capture_runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("capture task");
    let mut capture_executor = PipelineTaskExecutor::new(capture_runtime, capture_task);
    run_to_done(&mut capture_executor, &query, &thread, &wake, &mut profiler);

    assert_eq!(delim_handle.distinct_key_count(), 1);
    assert_eq!(cached_outer_handle.sealed_value_chunk_count(), 1);
    let sealed = delim_handle.sealed_values().expect("sealed delim values");
    assert_eq!(sealed.len(), 1);
    assert_eq!(sealed[0].column_count(), 0);
    assert_eq!(sealed[0].size(), 1);

    let scan_task = scan_runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("scan task");
    let mut scan_executor = PipelineTaskExecutor::new(scan_runtime, scan_task);
    run_to_done(&mut scan_executor, &query, &thread, &wake, &mut profiler);

    let chunk = output.pop_front().expect("zero-column delim output");
    assert_eq!(chunk.column_count(), 0);
    assert_eq!(chunk.size(), 1);
}

#[test]
fn batch_index_adapter_property_repair_is_zero_copy_pass_through() {
    assert_streaming_property_repair_references_input(PropertyRepairKind::BatchIndexAdapter);
}

#[test]
fn single_task_fallback_property_repair_is_zero_copy_pass_through() {
    assert_streaming_property_repair_references_input(PropertyRepairKind::SingleTaskFallback);
}

fn assert_streaming_property_repair_references_input(kind: PropertyRepairKind) {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let row_type = RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]);
    let source_chunk =
        paro_common::test_utils::test_chunk_from_vectors(vec![Vector::try_from_i32(
            &[31, 41],
            paro_common::test_utils::test_allocator(),
        )
        .expect("source vector")]);
    let source_column = source_chunk.column(0).expect("source column").clone();
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Chunk(ChunkScanSpec {
            chunks: Arc::from(vec![source_chunk].into_boxed_slice()),
            output_names: vec!["v".to_string()].into_boxed_slice(),
            output_types: vec![LogicalType::Integer].into_boxed_slice(),
        }),
        transforms: vec![TransformSpec::PropertyRepair(PropertyRepairSpec { kind })],
        sink: SinkSpec::ClientResult(ClientResultSpec::default()),
        sink_sharing: SinkSharing::Exclusive,
        properties: PipelineProperties::default(),
        output: row_type,
    };
    let runtime = runtime_from_spec(&query, spec);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(12),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);

    let chunk = output.pop_front().expect("streaming repair output");
    assert_eq!(chunk.size(), 2);
    assert!(Arc::ptr_eq(
        chunk.column(0).expect("output column"),
        &source_column
    ));
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(31));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(41));
    assert!(output.pop_front().is_none());
}

#[test]
fn hash_join_build_and_probe_use_typed_handle_without_sink_state() {
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
    handles.set_producer(handle, build_id).unwrap();
    handles.add_consumer(handle, probe_id).unwrap();
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
                    join_type: JoinType::Inner,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    build_projection: vec![1].into_boxed_slice(),
                    build_payload_types: vec![LogicalType::Integer].into_boxed_slice(),
                    build_output_count: 1,
                    required: Default::default(),
                    force_external: false,
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
                    output_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                    reduction_cascade: None,
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
        task_id: PipelineTaskId(18),
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

    let chunk = output.pop_front().expect("join output");
    assert_eq!(chunk.size(), 1);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(100));
    assert_eq!(chunk.column(1).unwrap().get_i32(0), Some(10));
}

#[test]
fn cross_product_probe_reuses_materialized_build_vectors() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let build_row_type = RowType::new(vec!["r".to_string()], vec![LogicalType::Integer]);
    let output_row_type = RowType::new(
        vec!["l".to_string(), "r".to_string()],
        vec![LogicalType::Integer, LogicalType::Integer],
    );

    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::Materialized,
        build_row_type.clone(),
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
                    vec![vec![int_constant(10)], vec![int_constant(20)]],
                    vec![LogicalType::Integer],
                )),
                transforms: Vec::new(),
                sink: SinkSpec::CrossProductBuild(CrossProductBuildSinkSpec {
                    handle,
                    required: Default::default(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: build_row_type,
            },
            PipelineSpec {
                id: probe_id,
                source: SourceSpec::Values(values_spec(
                    vec![vec![int_constant(1)], vec![int_constant(2)]],
                    vec![LogicalType::Integer],
                )),
                transforms: vec![TransformSpec::CrossProductProbe(CrossProductProbeSpec {
                    handle,
                    left_column_count: 1,
                    output_names: vec!["l".to_string(), "r".to_string()].into_boxed_slice(),
                    output_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                })],
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: output_row_type,
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
        task_id: PipelineTaskId(19),
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

    let first = output.pop_front().expect("first cross product output");
    let second = output.pop_front().expect("second cross product output");
    assert_eq!(first.size(), 2);
    assert_eq!(second.size(), 2);
    assert_eq!(first.column(0).unwrap().get_i32(0), Some(1));
    assert_eq!(first.column(1).unwrap().get_i32(0), Some(10));
    assert_eq!(first.column(0).unwrap().get_i32(1), Some(1));
    assert_eq!(first.column(1).unwrap().get_i32(1), Some(20));
    assert_eq!(second.column(0).unwrap().get_i32(0), Some(2));
    assert_eq!(second.column(1).unwrap().get_i32(0), Some(10));
    assert_eq!(second.column(0).unwrap().get_i32(1), Some(2));
    assert_eq!(second.column(1).unwrap().get_i32(1), Some(20));
}

#[test]
fn hash_join_left_probe_null_fills_when_build_is_empty() {
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
    handles.set_producer(handle, build_id).unwrap();
    handles.add_consumer(handle, probe_id).unwrap();

    let graph = PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: build_id,
                source: SourceSpec::Values(values_spec(
                    Vec::new(),
                    vec![LogicalType::Integer, LogicalType::Integer],
                )),
                transforms: Vec::new(),
                sink: SinkSpec::HashJoinBuild(HashJoinBuildSinkSpec {
                    handle,
                    join_type: JoinType::Left,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    build_projection: vec![1].into_boxed_slice(),
                    build_payload_types: vec![LogicalType::Integer].into_boxed_slice(),
                    build_output_count: 1,
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
                    vec![
                        vec![int_constant(1), int_constant(100)],
                        vec![int_constant(2), int_constant(200)],
                    ],
                    vec![LogicalType::Integer, LogicalType::Integer],
                )),
                transforms: vec![TransformSpec::HashJoinProbe(HashJoinProbeSpec {
                    handle,
                    join_type: JoinType::Left,
                    anti_join_mode: AntiJoinMode::Regular,
                    conditions: vec![join_condition()].into_boxed_slice(),
                    left_projection: vec![1].into_boxed_slice(),
                    output_names: vec!["lv".to_string(), "rv".to_string()].into_boxed_slice(),
                    output_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                    reduction_cascade: None,
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
    run_to_done(&mut probe_exec, &query, &thread, &wake, &mut profiler);

    let chunk = output.pop_front().expect("left join output");
    assert_eq!(chunk.size(), 2);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(100));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(200));
    assert!(chunk.column(1).unwrap().is_null(0));
    assert!(chunk.column(1).unwrap().is_null(1));
}
