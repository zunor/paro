// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::physical::specs::{SetOperationInputSide, SetOperationSpec};
use crate::pipeline::graph::{SetOperationEmitSourceSpec, SetOperationInputSinkSpec};
use crate::runtime::sink::{FinishWork, NextFinishTask};
use crate::runtime::state::SinkGlobal;
use paro_common::test_utils::test_allocator;
use paro_planner::expression::{WindowFrameBound, WindowFrameType};
use paro_planner::logical::operator::SetOpType;
use std::sync::atomic::{AtomicUsize, Ordering};

fn grouping_domain_graph() -> PipelineGraph {
    let mut spec = grouped_count_spec(None);
    spec.payload_types = Box::new([LogicalType::Integer]);
    spec.grouping_sets = Box::new([
        Box::new([0]),
        Box::new([0]),
        Box::new([0]),
        Box::new([]),
        Box::new([]),
        Box::new([]),
    ]);
    let output = RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec());
    let input = RowType::new(vec!["k".into()], vec![LogicalType::Integer]);
    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        BreakerHandleKind::Aggregate,
        output.clone(),
        PipelineProperties::default(),
    );
    handles.set_producer(handle, PipelineId::new(0)).unwrap();
    handles.add_consumer(handle, PipelineId::new(1)).unwrap();
    let chunks = (0..3)
        .map(|local| {
            let mut chunk =
                Chunk::try_initialize(&[LogicalType::Integer], VECTOR_SIZE, test_allocator())
                    .unwrap();
            chunk.try_set_cardinality(VECTOR_SIZE).unwrap();
            for row in 0..VECTOR_SIZE {
                chunk
                    .set_value(0, row, &Value::Integer((local * VECTOR_SIZE + row) as i32))
                    .unwrap();
            }
            chunk
        })
        .collect::<Vec<_>>();
    PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: PipelineId::new(0),
                source: SourceSpec::Chunk(ChunkScanSpec {
                    chunks: Arc::from(chunks),
                    output_names: Box::new(["k".into()]),
                    output_types: Box::new([LogicalType::Integer]),
                }),
                transforms: vec![],
                sink: SinkSpec::HashAggregateBuild(HashAggregateBuildSinkSpec {
                    handle,
                    spec: spec.clone(),
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: input,
            },
            PipelineSpec {
                id: PipelineId::new(1),
                source: SourceSpec::HashAggregateEmit(HashAggregateEmitSourceSpec { handle, spec }),
                transforms: vec![],
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: PipelineId::new(0),
            consumer: PipelineId::new(1),
            kind: DependencyKind::FinalizeBeforeEmit,
        }],
        handles: handles.finish(),
        control_regions: vec![],
        root: PipelineRoot::Pipeline(PipelineId::new(1)),
    }
}

/// Decorate the real aggregate driver only to observe scheduler ownership and
/// inject failure after ScheduledFinishTask has acquired its memory/permit.
#[derive(Debug)]
struct ScheduledGroupingProbe {
    inner: Arc<dyn ParallelFinishDriver>,
    failure: Option<bool>,
    cancellation: CancellationToken,
    completed: AtomicUsize,
    helper_tasks: AtomicUsize,
}

impl ParallelFinishDriver for ScheduledGroupingProbe {
    fn next_task(&self, ctx: &mut OperatorFinishContext) -> Result<NextFinishTask> {
        self.inner.next_task(ctx)
    }

    fn run_task(
        &self,
        task: FinishTaskId,
        ctx: &mut OperatorFinishContext,
    ) -> Result<FinishTaskPoll> {
        assert_eq!(ctx.finish_task, Some(task));
        assert!(ctx.thread.total_threads <= 2);
        assert!(ctx.query.memory.task_permits().used_permits() <= 2);
        if ctx.thread.thread_id != 0 {
            self.helper_tasks.fetch_add(1, Ordering::AcqRel);
        }
        if task.0 == 1 {
            match self.failure {
                Some(true) => self.cancellation.cancel(),
                Some(false) => ctx.query.memory.set_capacity_bytes(1),
                None => {}
            }
        }
        let result = self.inner.run_task(task, ctx)?;
        self.completed.fetch_or(1 << task.0, Ordering::AcqRel);
        Ok(result)
    }

    fn finish_group(&self, ctx: &mut OperatorFinishContext) -> Result<()> {
        self.inner.finish_group(ctx)
    }

    fn cancel_group(&self, ctx: &mut OperatorCleanupContext, reason: CancelReason) -> Result<()> {
        self.inner.cancel_group(ctx, reason)
    }
}

fn run_scheduled_grouping_finish(failure: Option<bool>) -> Vec<Vec<Value>> {
    let output = QueryOutputPort::unbounded();
    let mut query = query_context_with_limits(
        output.clone(),
        RuntimeLimits {
            parallel_scheduler: true,
            max_threads: 2,
            max_memory: 256 * 1024 * 1024,
            ..Default::default()
        },
    );
    let permits = query.memory.task_permits();
    permits.set_max_permits(2);
    let token = install_statement_cancellation(&mut query, StatementCancelReason::UserRequest);
    let (build, emit) = runtimes_from_graph(&query, &grouping_domain_graph());
    let baseline_bytes = query.memory.published_used_bytes();
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(210),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    let mut workers = (0..3)
        .map(|_| {
            PipelineTaskExecutor::new_parallel_data_task(
                build.clone(),
                build.create_task_state(&query, test_allocator()).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    // Use the scheduler's chunk-range assignment boundary. Each data task
    // builds one nonempty local before the merge rendezvous.
    for (chunk_idx, worker) in workers.iter_mut().enumerate() {
        worker
            .task
            .data_mut()
            .unwrap()
            .source
            .chunk_mut()
            .unwrap()
            .assign_chunk_range(chunk_idx, chunk_idx + 1);
        worker
            .step(&mut step_context(&query, &thread, &wake, &mut profiler))
            .unwrap();
    }
    for worker in &mut workers {
        let mut done = false;
        for _ in 0..100 {
            if matches!(
                worker
                    .step_until_local_merge(&mut step_context(
                        &query,
                        &thread,
                        &wake,
                        &mut profiler
                    ))
                    .unwrap(),
                TaskStepResult::Done
            ) {
                done = true;
                break;
            }
        }
        assert!(done);
    }
    drop(workers);
    let SinkGlobal::HashAggregateBuild(global) = &build.sink_global else {
        unreachable!()
    };
    global
        .handle
        .with_state_mut(|state| {
            let crate::runtime::breaker::AggregateRuntimeState::Hash(state) = state else {
                unreachable!()
            };
            assert_eq!(state.pending_radix_merges.len(), 3);
            assert!(state
                .pending_radix_merges
                .iter()
                .all(|tables| tables[0].count() == VECTOR_SIZE));
            Ok(())
        })
        .unwrap();
    let coordinator_permit = permits.try_acquire_available().unwrap();
    let mut coordinator = PipelineTaskExecutor::new_finish_task(
        build.clone(),
        build
            .create_finish_task_state(&query, test_allocator())
            .unwrap(),
    );
    for _ in 0..100 {
        if coordinator.completion_stage == PipelineCompletionStage::FinishWork {
            break;
        }
        coordinator
            .step(&mut step_context(&query, &thread, &wake, &mut profiler))
            .unwrap();
    }
    assert_eq!(
        coordinator.completion_stage,
        PipelineCompletionStage::FinishWork
    );
    let mut step = step_context(&query, &thread, &wake, &mut profiler);
    let mut ctx = helpers::finish_context(
        &mut step,
        build.program.id,
        build.program.sink.operator_id,
        None,
        &coordinator.task,
    );
    let FinishWork::Parallel(mut group) = build
        .program
        .sink
        .exec
        .finish_work(&mut ctx, &build.sink_global)
        .unwrap()
    else {
        panic!("expected grouping domain finish group")
    };
    assert_eq!(group.task_count, 6);
    let probe = Arc::new(ScheduledGroupingProbe {
        inner: group.driver,
        failure,
        cancellation: token,
        completed: AtomicUsize::new(0),
        helper_tasks: AtomicUsize::new(0),
    });
    group.driver = probe.clone();
    coordinator.finish_group = Some(group);
    // This step uses drive_parallel_finish_group -> run_parallel_finish_tasks
    // -> ScheduledFinishTask, including helper permits and bounded waves.
    let result = coordinator.step(&mut step_context(&query, &thread, &wake, &mut profiler));
    if let Some(cancel) = failure {
        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains(if cancel { "cancel" } else { "memory" }),
            "{error}"
        );
        assert!(!global.handle.is_finalized());
        global
            .handle
            .with_state_mut(|state| {
                let crate::runtime::breaker::AggregateRuntimeState::Hash(state) = state else {
                    unreachable!()
                };
                assert!(state.tables.is_empty());
                assert!(state.pending_radix_merges.is_empty());
                Ok(())
            })
            .unwrap();
    } else {
        result.unwrap();
        assert_eq!(probe.completed.load(Ordering::Acquire), 0b11_1111);
        assert_eq!(probe.helper_tasks.load(Ordering::Acquire), 3);
        coordinator
            .step(&mut step_context(&query, &thread, &wake, &mut profiler))
            .unwrap();
        assert!(global.handle.is_finalized());
    }
    // The error path must drain all scheduled helpers before returning.
    assert_eq!(permits.used_permits(), 1);
    drop(probe);
    drop(coordinator);
    drop(coordinator_permit);
    assert_eq!(permits.used_permits(), 0);
    if failure.is_some() {
        assert_eq!(query.memory.published_used_bytes(), baseline_bytes);
        return vec![];
    }
    let mut task = PipelineTaskExecutor::new(
        emit.clone(),
        emit.create_task_state(&query, test_allocator()).unwrap(),
    );
    let mut done = false;
    for _ in 0..1000 {
        if matches!(
            task.step(&mut step_context(&query, &thread, &wake, &mut profiler))
                .unwrap(),
            TaskStepResult::Done
        ) {
            done = true;
            break;
        }
    }
    assert!(done);
    let mut result = Vec::new();
    while let Some(chunk) = output.pop_front() {
        result.extend(rows(&[chunk]));
    }
    result
}

#[test]
fn scheduled_grouping_domain_merge_obeys_permits_waves_and_emits_duplicates() {
    let rows = run_scheduled_grouping_finish(None);
    assert_eq!(rows.len(), 9 * VECTOR_SIZE + 3);
    let mut keyed = std::collections::BTreeMap::new();
    let mut totals = 0;
    for row in rows {
        match row[0] {
            Value::Integer(key) => {
                assert!((0..(3 * VECTOR_SIZE) as i32).contains(&key));
                assert_eq!(row[1], Value::BigInt(1));
                *keyed.entry(key).or_insert(0) += 1;
            }
            Value::Null(LogicalType::Integer) => {
                assert_eq!(row[1], Value::BigInt((3 * VECTOR_SIZE) as i64));
                totals += 1;
            }
            _ => panic!("unexpected grouping domain output"),
        }
    }
    assert_eq!(keyed.len(), 3 * VECTOR_SIZE);
    assert!(keyed.values().all(|count| *count == 3));
    assert_eq!(totals, 3);
}

#[test]
fn scheduled_grouping_domain_merge_drains_cancel_and_oom_and_refunds() {
    for cancel in [true, false] {
        assert!(run_scheduled_grouping_finish(Some(cancel)).is_empty());
    }
}

fn inputs(count: usize, groups: usize) -> Vec<Chunk> {
    (0..count)
        .step_by(VECTOR_SIZE)
        .map(|start| {
            let n = (count - start).min(VECTOR_SIZE);
            let mut chunk = Chunk::try_initialize(
                &[LogicalType::Integer, LogicalType::Integer],
                n,
                test_allocator(),
            )
            .unwrap();
            chunk.try_set_cardinality(n).unwrap();
            for row in 0..n {
                let i = count - 1 - start - row;
                let group = i % groups;
                let key = if group == 0 {
                    Value::Null(LogicalType::Integer)
                } else {
                    Value::Integer(group as i32)
                };
                chunk.set_value(0, row, &key).unwrap();
                chunk
                    .set_value(
                        1,
                        row,
                        &if i % 23 == 0 {
                            Value::Null(LogicalType::Integer)
                        } else {
                            Value::Integer((i % 19) as i32)
                        },
                    )
                    .unwrap();
            }
            chunk
        })
        .collect()
}

fn rows(chunks: &[Chunk]) -> Vec<Vec<Value>> {
    chunks
        .iter()
        .flat_map(|chunk| {
            (0..chunk.size()).map(|row| chunk.data.iter().map(|col| col.get_value(row)).collect())
        })
        .collect()
}

fn graph(
    chunks: Vec<Chunk>,
    window: Option<WindowSpec>,
    set: Option<SetOperationSpec>,
) -> PipelineGraph {
    let types = window.as_ref().map_or_else(
        || {
            set.as_ref().map_or_else(
                || vec![LogicalType::Integer, LogicalType::Integer],
                |spec| spec.output_types.to_vec(),
            )
        },
        |s| s.output_types[..s.input_width].to_vec(),
    );
    let output_types = window
        .as_ref()
        .map_or_else(|| types.clone(), |spec| spec.output_types.to_vec());
    let output = RowType::new(
        (0..output_types.len()).map(|i| format!("c{i}")).collect(),
        output_types,
    );
    let input_names = if types.len() == 2 {
        vec!["k".into(), "v".into()]
    } else {
        (0..types.len()).map(|i| format!("c{i}")).collect()
    };
    let mut handles = BreakerHandleCatalogBuilder::default();
    let handle = handles.register(
        if window.is_some() {
            BreakerHandleKind::Window
        } else {
            BreakerHandleKind::SetOperation
        },
        output.clone(),
        PipelineProperties::default(),
    );
    handles.set_producer(handle, PipelineId::new(0)).unwrap();
    handles.add_consumer(handle, PipelineId::new(1)).unwrap();
    let (sink, source) = if let Some(spec) = window {
        (
            SinkSpec::WindowBuild(WindowBuildSinkSpec {
                handle,
                spec: spec.clone(),
            }),
            SourceSpec::WindowEmit(WindowEmitSourceSpec { handle, spec }),
        )
    } else {
        let spec = set.unwrap();
        (
            SinkSpec::SetOperationInput(SetOperationInputSinkSpec {
                handle,
                spec: spec.clone(),
                side: SetOperationInputSide::Left,
            }),
            SourceSpec::SetOperationEmit(SetOperationEmitSourceSpec { handle, spec }),
        )
    };
    PipelineGraph {
        pipelines: vec![
            PipelineSpec {
                id: PipelineId::new(0),
                source: SourceSpec::Chunk(ChunkScanSpec {
                    chunks: Arc::from(chunks),
                    output_names: input_names.clone().into_boxed_slice(),
                    output_types: types.clone().into_boxed_slice(),
                }),
                transforms: vec![],
                sink,
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: RowType::new(input_names, types),
            },
            PipelineSpec {
                id: PipelineId::new(1),
                source,
                transforms: vec![],
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output,
            },
        ],
        dependencies: vec![PipelineDependency {
            producer: PipelineId::new(0),
            consumer: PipelineId::new(1),
            kind: DependencyKind::FinalizeBeforeEmit,
        }],
        handles: handles.finish(),
        control_regions: vec![],
        root: PipelineRoot::Pipeline(PipelineId::new(1)),
    }
}

fn bigint_sequence_fixture(width: usize) -> (Chunk, Vec<Vec<Value>>) {
    use paro_common::vector::{Vector, VectorType};

    let starts = [
        i64::MIN,
        i64::MAX - (VECTOR_SIZE - 1) as i64,
        9_007_199_254_740_993,
    ];
    let columns = (0..width)
        .map(|column| {
            let mut vector =
                Vector::try_sequence(starts[column], 1, VECTOR_SIZE, test_allocator()).unwrap();
            for row in 0..VECTOR_SIZE {
                if ((row % 8) ^ 1) & (1 << column) == 0 {
                    vector.try_set_null(row, true).unwrap();
                }
            }
            assert_eq!(vector.vector_type(), VectorType::Sequence);
            Arc::new(vector)
        })
        .collect();
    let chunk =
        Chunk::try_from_arc_vectors_with_cardinality(columns, VECTOR_SIZE, test_allocator())
            .unwrap();
    // Literal scalar values form the oracle; neither typed-key construction nor
    // a second execution of the set operator supplies the expected rows.
    let expected = (0..VECTOR_SIZE)
        .map(|row| {
            (0..width)
                .map(|column| {
                    if ((row % 8) ^ 1) & (1 << column) == 0 {
                        Value::Null(LogicalType::BigInt)
                    } else {
                        Value::BigInt(starts[column] + row as i64)
                    }
                })
                .collect()
        })
        .collect();
    (chunk, expected)
}

fn run_scheduled_bigint_sequence_set_finish(width: usize, failure: Option<bool>) {
    let (chunk, expected_chunk) = bigint_sequence_fixture(width);
    let spec = SetOperationSpec {
        table_index: 0,
        op: SetOpType::Except,
        all: true,
        output_names: (0..width).map(|i| format!("c{i}")).collect(),
        output_types: vec![LogicalType::BigInt; width].into_boxed_slice(),
    };
    let graph = graph(vec![chunk; 5], None, Some(spec));
    let output = QueryOutputPort::unbounded();
    let mut query = query_context_with_limits(
        output.clone(),
        RuntimeLimits {
            parallel_scheduler: true,
            max_threads: 2,
            max_memory: 256 * 1024 * 1024,
            ..Default::default()
        },
    );
    let permits = query.memory.task_permits();
    permits.set_max_permits(2);
    let token = install_statement_cancellation(&mut query, StatementCancelReason::UserRequest);
    let (build, emit) = runtimes_from_graph(&query, &graph);
    let baseline_bytes = query.memory.published_used_bytes();
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(211),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    let mut worker = PipelineTaskExecutor::new_parallel_data_task(
        build.clone(),
        build.create_task_state(&query, test_allocator()).unwrap(),
    );
    worker
        .task
        .data_mut()
        .unwrap()
        .source
        .chunk_mut()
        .unwrap()
        .assign_chunk_range(0, 5);
    let mut done = false;
    for _ in 0..100 {
        if matches!(
            worker
                .step_until_local_merge(&mut step_context(&query, &thread, &wake, &mut profiler))
                .unwrap(),
            TaskStepResult::Done
        ) {
            done = true;
            break;
        }
    }
    assert!(done);
    drop(worker);
    let SinkGlobal::SetOperationInput(global) = &build.sink_global else {
        unreachable!()
    };
    let coordinator_permit = permits.try_acquire_available().unwrap();
    let mut coordinator = PipelineTaskExecutor::new_finish_task(
        build.clone(),
        build
            .create_finish_task_state(&query, test_allocator())
            .unwrap(),
    );
    for _ in 0..100 {
        if coordinator.completion_stage == PipelineCompletionStage::FinishWork {
            break;
        }
        coordinator
            .step(&mut step_context(&query, &thread, &wake, &mut profiler))
            .unwrap();
    }
    assert_eq!(
        coordinator.completion_stage,
        PipelineCompletionStage::FinishWork
    );
    let mut step = step_context(&query, &thread, &wake, &mut profiler);
    let mut ctx = helpers::finish_context(
        &mut step,
        build.program.id,
        build.program.sink.operator_id,
        None,
        &coordinator.task,
    );
    let FinishWork::Parallel(mut group) = build
        .program
        .sink
        .exec
        .finish_work(&mut ctx, &build.sink_global)
        .unwrap()
    else {
        panic!("expected parallel BIGINT sequence set finish")
    };
    assert_eq!(group.task_count, 2);
    // This decorator changes only failure injection/observation. The driver is
    // the real set finish owner, invoked by ScheduledFinishTask with permits.
    let probe = Arc::new(ScheduledGroupingProbe {
        inner: group.driver,
        failure,
        cancellation: token,
        completed: AtomicUsize::new(0),
        helper_tasks: AtomicUsize::new(0),
    });
    group.driver = probe.clone();
    coordinator.finish_group = Some(group);
    let result = coordinator.step(&mut step_context(&query, &thread, &wake, &mut profiler));
    if let Some(cancel) = failure {
        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains(if cancel { "cancel" } else { "memory" }),
            "{error}"
        );
        assert!(!global.handle.is_sealed());
        assert_eq!(global.handle.sealed_chunk_count(), 0);
        assert_eq!(
            global
                .handle
                .pending_chunk_count(SetOperationInputSide::Left),
            0
        );
        assert!(output.pop_front().is_none());
    } else {
        result.unwrap();
        assert_eq!(probe.completed.load(Ordering::Acquire), 0b11);
        assert!(probe.helper_tasks.load(Ordering::Acquire) >= 1);
        coordinator
            .step(&mut step_context(&query, &thread, &wake, &mut profiler))
            .unwrap();
        assert!(global.handle.is_sealed());
    }
    assert_eq!(permits.used_permits(), 1);
    drop(probe);
    drop(coordinator);
    drop(coordinator_permit);
    assert_eq!(permits.used_permits(), 0);
    if failure.is_some() {
        assert_eq!(query.memory.published_used_bytes(), baseline_bytes);
        return;
    }
    let mut task = PipelineTaskExecutor::new(
        emit.clone(),
        emit.create_task_state(&query, test_allocator()).unwrap(),
    );
    let mut done = false;
    for _ in 0..1000 {
        if matches!(
            task.step(&mut step_context(&query, &thread, &wake, &mut profiler))
                .unwrap(),
            TaskStepResult::Done
        ) {
            done = true;
            break;
        }
    }
    assert!(done);
    let mut actual = Vec::new();
    while let Some(chunk) = output.pop_front() {
        actual.extend(rows(&[chunk]));
    }
    let mut counts = std::collections::HashMap::new();
    let mut first = Vec::new();
    for row in expected_chunk {
        let count = counts.entry(row.clone()).or_insert_with(|| {
            first.push(row);
            0
        });
        *count += 5;
    }
    let expected = first
        .into_iter()
        .flat_map(|row| {
            let count = counts[&row];
            std::iter::repeat_n(row, count)
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "BIGINT sequence width={width}");
}

#[test]
fn scheduled_bigint_sequence_set_finish_preserves_extremes_nulls_and_multiplicity() {
    for width in 1..=3 {
        run_scheduled_bigint_sequence_set_finish(width, None);
    }
}

#[test]
fn scheduled_bigint_sequence_set_finish_drains_cancel_oom_and_refunds() {
    for cancel in [true, false] {
        run_scheduled_bigint_sequence_set_finish(3, Some(cancel));
    }
}

fn execute_finish(
    graph: PipelineGraph,
    right: Vec<Chunk>,
    failure: Option<bool>,
) -> (Vec<Vec<Value>>, usize) {
    let output = QueryOutputPort::unbounded();
    let mut query = query_context_with_limits(
        output.clone(),
        RuntimeLimits {
            parallel_scheduler: true,
            max_memory: 256 * 1024 * 1024,
            ..Default::default()
        },
    );
    query.memory.task_permits().set_max_permits(4);
    let token = install_statement_cancellation(&mut query, StatementCancelReason::UserRequest);
    let (build, emit) = runtimes_from_graph(&query, &graph);
    let mut task = PipelineTaskExecutor::new(
        build.clone(),
        build.create_task_state(&query, test_allocator()).unwrap(),
    );
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(200),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();
    for _ in 0..1000 {
        if matches!(task.phase, PipelineTaskPhase::Merging)
            && matches!(task.completion_stage, PipelineCompletionStage::FinishWork)
        {
            break;
        }
        task.step(&mut step_context(&query, &thread, &wake, &mut profiler))
            .unwrap();
    }
    let mut step = step_context(&query, &thread, &wake, &mut profiler);
    let mut ctx = helpers::finish_context(
        &mut step,
        build.program.id,
        build.program.sink.operator_id,
        None,
        &task.task,
    );
    if let SinkGlobal::SetOperationInput(global) = &build.sink_global {
        let mut router = crate::runtime::breaker::radix::RadixRouter::default();
        let allocator = ctx.memory.accounted_allocator_for(
            paro_common::allocator::MemoryTag::HashTable,
            paro_common::memory::MemoryAccountingClass::NonRevocable,
        );
        let mut routed = Vec::new();
        for mut chunk in right {
            let columns = (0..chunk.data.len()).collect::<Vec<_>>();
            router
                .push(
                    &mut routed,
                    &mut chunk,
                    &columns,
                    4,
                    || allocator.clone(),
                    ctx.cancel,
                )
                .unwrap();
        }
        global
            .handle
            .append_chunks(SetOperationInputSide::Right, &mut routed)
            .unwrap();
    }
    let routing_bytes = match &build.sink_global {
        SinkGlobal::WindowBuild(g) => g.handle.routing_bytes(),
        SinkGlobal::SetOperationInput(g) => g.handle.routing_bytes(),
        _ => unreachable!(),
    };
    // Below the promotion boundary no row hashing or selection buffers are retained.
    if let SourceSpec::Chunk(source) = &graph.pipelines[0].source {
        if source.chunks.iter().map(Chunk::size).sum::<usize>() < 4 * VECTOR_SIZE {
            assert_eq!(routing_bytes, 0);
        }
    }
    let before_finish = query.memory.published_used_bytes();
    let small_failure = failure.filter(|_| match &graph.pipelines[0].source {
        SourceSpec::Chunk(source) => {
            source.chunks.iter().map(Chunk::size).sum::<usize>() < 4 * VECTOR_SIZE
        }
        _ => false,
    });
    if let Some(cancel) = small_failure {
        if cancel {
            token.cancel();
        } else {
            query.memory.set_capacity_bytes(1);
        }
        let error = build
            .program
            .sink
            .exec
            .finish_work(&mut ctx, &build.sink_global)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains(if cancel { "cancel" } else { "memory" }),
            "{error}"
        );
        match &build.sink_global {
            SinkGlobal::WindowBuild(g) => assert!(!g.handle.is_sealed()),
            SinkGlobal::SetOperationInput(g) => assert!(!g.handle.is_sealed()),
            _ => unreachable!(),
        }
        assert_eq!(query.memory.published_used_bytes(), before_finish);
        return (vec![], 0);
    }
    let work = build
        .program
        .sink
        .exec
        .finish_work(&mut ctx, &build.sink_global)
        .unwrap();
    let count = match work {
        FinishWork::None => 0,
        FinishWork::Parallel(group) => {
            let mut ids = Vec::new();
            while let NextFinishTask::Task(id) = group.driver.next_task(&mut ctx).unwrap() {
                ids.push(id);
            }
            if let Some(cancel) = failure {
                assert!(ids.len() > 1);
                group.driver.run_task(ids[0], &mut ctx).unwrap();
                if cancel {
                    token.cancel();
                } else {
                    query.memory.set_capacity_bytes(1);
                }
                let error = group.driver.run_task(ids[1], &mut ctx).unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains(if cancel { "cancel" } else { "memory" }),
                    "{error}"
                );
            } else {
                for id in ids.into_iter().rev() {
                    group.driver.run_task(id, &mut ctx).unwrap();
                }
                group.driver.finish_group(&mut ctx).unwrap();
            }
            group.task_count
        }
    };
    if failure.is_some() {
        match &build.sink_global {
            SinkGlobal::WindowBuild(global) => assert!(!global.handle.is_sealed()),
            SinkGlobal::SetOperationInput(global) => assert!(!global.handle.is_sealed()),
            _ => unreachable!(),
        }
        // The failed group's retained work/results have been dropped; nothing
        // was published to the handle. Finish allocations and sink-owned routing
        // moved into the failed group were refunded.
        assert_eq!(
            query.memory.published_used_bytes(),
            before_finish - routing_bytes
        );
        return (vec![], count);
    }
    build
        .program
        .sink
        .exec
        .finish(&mut ctx, &build.sink_global)
        .unwrap();
    let mut task = PipelineTaskExecutor::new(
        emit.clone(),
        emit.create_task_state(&query, test_allocator()).unwrap(),
    );
    for _ in 0..1000 {
        if matches!(
            task.step(&mut step_context(&query, &thread, &wake, &mut profiler))
                .unwrap(),
            TaskStepResult::Done
        ) {
            break;
        }
    }
    let mut result = Vec::new();
    while let Some(chunk) = output.pop_front() {
        result.extend(rows(&[chunk]));
    }
    (result, count)
}

#[test]
fn window_finish_tasks_preserve_frames_ties_nulls_and_order() {
    for (count, groups) in [
        (0, 1),
        (17, 8),
        (4096, 1),
        (4 * VECTOR_SIZE - 1, 32),
        (4 * VECTOR_SIZE, 32),
        (4 * VECTOR_SIZE + 1, 32),
        (4096, 0),
    ] {
        for frame_type in [WindowFrameType::Rows, WindowFrameType::Range] {
            for mode in 0..if frame_type == WindowFrameType::Rows {
                3
            } else {
                2
            } {
                let sum = get_sum_function().bind(&[LogicalType::Integer]).unwrap().0;
                let ty = sum.return_type.clone();
                let aggregate = AggregateExpression::new(
                    sum,
                    vec![reference(1, LogicalType::Integer)],
                    ty.clone(),
                );
                let expr = WindowExpression::aggregate(
                    aggregate,
                    if groups == 0 {
                        vec![]
                    } else {
                        vec![reference(0, LogicalType::Integer)]
                    },
                    vec![OrderByExpression {
                        expression: reference(1, LogicalType::Integer),
                        ascending: false,
                        nulls_first: true,
                    }],
                    WindowFrame {
                        frame_type,
                        start_bound: if mode == 2 {
                            WindowFrameBound::Offset(Box::new(int_constant(2)))
                        } else {
                            WindowFrameBound::Unbounded
                        },
                        start_is_preceding: true,
                        end_bound: if mode == 1 {
                            WindowFrameBound::Unbounded
                        } else {
                            WindowFrameBound::CurrentRow
                        },
                        end_is_preceding: false,
                    },
                );
                let rank = WindowExpression::native(
                    WindowFunction::rank(),
                    vec![],
                    expr.partitions.clone(),
                    expr.orders.clone(),
                    WindowFrame::default(),
                    false,
                );
                let spec = WindowSpec {
                    window_index: 1,
                    expressions: vec![expr, rank].into_boxed_slice(),
                    input_width: 2,
                    output_names: Box::new(["k".into(), "v".into(), "s".into(), "r".into()]),
                    output_types: vec![
                        LogicalType::Integer,
                        LogicalType::Integer,
                        ty,
                        LogicalType::BigInt,
                    ]
                    .into_boxed_slice(),
                };
                let input = inputs(count, groups.max(1));
                let expected = crate::operators::window::runtime::build_window_output_chunks(
                    &spec,
                    &input,
                    test_allocator(),
                )
                .unwrap();
                let (actual, tasks) = execute_finish(graph(input, Some(spec), None), vec![], None);
                assert_eq!(actual, rows(&expected));
                if groups > 1 && count >= 4 * VECTOR_SIZE {
                    assert!(tasks > 1);
                } else {
                    assert_eq!(tasks, 0);
                }
            }
        }
    }
}

#[test]
fn set_finish_tasks_preserve_all_multiplicities_and_first_seen_order() {
    for count in [
        0,
        17,
        2 * VECTOR_SIZE,
        3 * VECTOR_SIZE,
        4 * VECTOR_SIZE - 1,
        4 * VECTOR_SIZE,
        4 * VECTOR_SIZE + 1,
    ] {
        for op in [SetOpType::Union, SetOpType::Intersect, SetOpType::Except] {
            for all in [false, true] {
                let left = inputs(count, 32);
                let right = inputs(count / 2, 16);
                let mut counts = std::collections::HashMap::new();
                let mut order = Vec::new();
                for (side, input) in [(0, &left), (1, &right)] {
                    for row in rows(input) {
                        let c = counts.entry(row.clone()).or_insert_with(|| {
                            order.push(row);
                            [0usize; 2]
                        });
                        c[side] += 1;
                    }
                }
                let mut expected = Vec::new();
                for row in order {
                    let [l, r] = counts[&row];
                    let n = match (op, all) {
                        (SetOpType::Union, false) => 1,
                        (SetOpType::Union, true) => l + r,
                        (SetOpType::Intersect, false) => usize::from(l > 0 && r > 0),
                        (SetOpType::Intersect, true) => l.min(r),
                        (SetOpType::Except, false) => usize::from(l > 0 && r == 0),
                        (SetOpType::Except, true) => l.saturating_sub(r),
                    };
                    expected.extend(std::iter::repeat_n(row, n));
                }
                if op == SetOpType::Union && all {
                    expected = rows(&left);
                    expected.extend(rows(&right));
                }
                let spec = SetOperationSpec {
                    table_index: 0,
                    op,
                    all,
                    output_names: Box::new(["k".into(), "v".into()]),
                    output_types: Box::new([LogicalType::Integer, LogicalType::Integer]),
                };
                let (actual, tasks) = execute_finish(graph(left, None, Some(spec)), right, None);
                assert_eq!(actual, expected, "{op:?} all={all}");
                if count + count / 2 >= 4 * VECTOR_SIZE && !(op == SetOpType::Union && all) {
                    assert!(tasks > 1);
                } else {
                    assert_eq!(tasks, 0);
                }
            }
        }
    }
}

#[test]
fn failed_or_cancelled_finish_tasks_never_publish() {
    for failure in [false, true] {
        let spec = SetOperationSpec {
            table_index: 0,
            op: SetOpType::Except,
            all: true,
            output_names: Box::new(["k".into(), "v".into()]),
            output_types: Box::new([LogicalType::Integer, LogicalType::Integer]),
        };
        execute_finish(
            graph(inputs(17, 8), None, Some(spec.clone())),
            vec![],
            Some(failure),
        );
        execute_finish(
            graph(inputs(4 * VECTOR_SIZE, 32), None, Some(spec)),
            inputs(1024, 16),
            Some(failure),
        );
        let expr = WindowExpression::native(
            WindowFunction::rank(),
            vec![],
            vec![reference(0, LogicalType::Integer)],
            vec![],
            WindowFrame::default(),
            false,
        );
        let spec = WindowSpec {
            window_index: 1,
            expressions: vec![expr].into_boxed_slice(),
            input_width: 2,
            output_names: Box::new(["k".into(), "v".into(), "r".into()]),
            output_types: Box::new([
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::BigInt,
            ]),
        };
        execute_finish(
            graph(inputs(17, 8), Some(spec.clone()), None),
            vec![],
            Some(failure),
        );
        execute_finish(
            graph(inputs(4 * VECTOR_SIZE, 32), Some(spec), None),
            vec![],
            Some(failure),
        );
    }
}

#[test]
fn window_radix_string_keys_and_float_fallback_match_complete_sort() {
    for ty in [LogicalType::Varchar, LogicalType::Double] {
        let mut input = Vec::new();
        for batch in 0..4 {
            let mut chunk = Chunk::try_initialize(
                &[ty.clone(), LogicalType::Integer],
                VECTOR_SIZE,
                test_allocator(),
            )
            .unwrap();
            chunk.try_set_cardinality(VECTOR_SIZE).unwrap();
            for row in 0..VECTOR_SIZE {
                let i = batch * VECTOR_SIZE + row;
                let key = if i % 17 == 0 {
                    Value::Null(ty.clone())
                } else if ty == LogicalType::Varchar {
                    Value::Varchar(format!("partition-{:03}-long-borrowed-string", i % 37))
                } else {
                    Value::Double(if i % 2 == 0 { 0.0 } else { -0.0 })
                };
                chunk.set_value(0, row, &key).unwrap();
                chunk
                    .set_value(1, row, &Value::Integer((i % 7) as i32))
                    .unwrap();
            }
            // Dictionary vectors exercise logical-to-physical row mapping.
            let mut selection = paro_common::vector::SelectionVector::try_with_capacity(
                VECTOR_SIZE,
                test_allocator(),
            )
            .unwrap();
            selection.set_len(VECTOR_SIZE);
            for row in 0..VECTOR_SIZE {
                selection.try_set(row, VECTOR_SIZE - 1 - row).unwrap();
            }
            chunk.try_slice(&selection, VECTOR_SIZE).unwrap();
            input.push(chunk);
        }
        let expr = WindowExpression::native(
            WindowFunction::rank(),
            vec![],
            vec![reference(0, ty.clone())],
            vec![OrderByExpression {
                expression: reference(1, LogicalType::Integer),
                ascending: false,
                nulls_first: true,
            }],
            WindowFrame::default(),
            false,
        );
        let spec = WindowSpec {
            window_index: 1,
            expressions: vec![expr].into_boxed_slice(),
            input_width: 2,
            output_names: Box::new(["k".into(), "v".into(), "r".into()]),
            output_types: vec![ty.clone(), LogicalType::Integer, LogicalType::BigInt]
                .into_boxed_slice(),
        };
        let expected = crate::operators::window::runtime::build_window_output_chunks(
            &spec,
            &input,
            test_allocator(),
        )
        .unwrap();
        let (actual, tasks) = execute_finish(graph(input, Some(spec), None), vec![], None);
        assert_eq!(actual, rows(&expected));
        if ty == LogicalType::Double {
            assert_eq!(tasks, 0);
        } else {
            assert!(tasks > 1);
        }
    }
}

#[test]
fn radix_sink_memory_failure_leaves_input_unpublished_and_refunds_scratch() {
    let expr = WindowExpression::native(
        WindowFunction::rank(),
        vec![],
        vec![reference(0, LogicalType::Integer)],
        vec![],
        WindowFrame::default(),
        false,
    );
    let window = WindowSpec {
        window_index: 1,
        expressions: vec![expr].into_boxed_slice(),
        input_width: 2,
        output_names: Box::new(["k".into(), "v".into(), "r".into()]),
        output_types: Box::new([
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::BigInt,
        ]),
    };
    let set = SetOperationSpec {
        table_index: 0,
        op: SetOpType::Intersect,
        all: false,
        output_names: Box::new(["k".into(), "v".into()]),
        output_types: Box::new([LogicalType::Integer, LogicalType::Integer]),
    };
    for graph in [
        graph(inputs(4 * VECTOR_SIZE, 32), Some(window), None),
        graph(inputs(4 * VECTOR_SIZE, 32), None, Some(set)),
    ] {
        let query = query_context_with_limits(
            QueryOutputPort::unbounded(),
            RuntimeLimits {
                parallel_scheduler: true,
                max_memory: 256 * 1024 * 1024,
                ..Default::default()
            },
        );
        query.memory.task_permits().set_max_permits(4);
        let (build, _) = runtimes_from_graph(&query, &graph);
        let before = query.memory.published_used_bytes();
        let mut task = PipelineTaskExecutor::new(
            build.clone(),
            build.create_task_state(&query, test_allocator()).unwrap(),
        );
        query.memory.set_capacity_bytes(1);
        let thread = ThreadContext::single_threaded();
        let wake = OperatorWakeScope {
            task_id: PipelineTaskId(201),
            generation: WakeGeneration(0),
        };
        let mut profiler = OperatorProfiler::disabled();
        let error = (0..1000)
            .find_map(|_| {
                task.step(&mut step_context(&query, &thread, &wake, &mut profiler))
                    .err()
            })
            .expect("promotion must fail under the memory cap");
        assert!(error.to_string().contains("memory"), "{error}");
        drop(task);
        assert_eq!(query.memory.published_used_bytes(), before);
        match &build.sink_global {
            SinkGlobal::WindowBuild(g) => {
                assert!(!g.handle.is_sealed());
                assert_eq!(g.handle.pending_chunk_count(), 0);
            }
            SinkGlobal::SetOperationInput(g) => {
                assert!(!g.handle.is_sealed());
                assert_eq!(g.handle.pending_chunk_count(SetOperationInputSide::Left), 0);
            }
            _ => unreachable!(),
        }
    }
}
