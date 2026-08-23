// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::pipeline::program::{PipelineProgram, SinkSlot, SourceSlot};
use crate::runtime::FinishCoordinatorParticipation;
use crate::runtime::{
    ChunkLayout, ClientResultSinkExec, DynGlobalState, DynLocalState, DynSourceExec,
    DynStateTypeId, OperatorCallContext, OperatorRole, PipelineInitContext, PipelineScratchLayout,
    RuntimeOperatorId, RuntimeOperatorOrigin, RuntimeRoleOrdinal, SinkExec, SourceExec,
    SourceGlobal, SourceLocal, SourcePoll,
};

#[derive(Debug)]
struct CountingSourceState;

impl DynGlobalState for CountingSourceState {
    fn state_type(&self) -> DynStateTypeId {
        DynStateTypeId("counting_source")
    }

    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }

    fn as_any_mut(&mut self) -> &mut (dyn std::any::Any + Send + Sync) {
        self
    }
}

impl DynLocalState for CountingSourceState {
    fn state_type(&self) -> DynStateTypeId {
        DynStateTypeId("counting_source")
    }

    fn as_any(&self) -> &(dyn std::any::Any + Send) {
        self
    }

    fn as_any_mut(&mut self) -> &mut (dyn std::any::Any + Send) {
        self
    }
}

#[derive(Debug)]
struct CountingSourceExec {
    local_creations: Arc<std::sync::atomic::AtomicUsize>,
}

impl DynSourceExec for CountingSourceExec {
    fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::Dyn(Box::new(CountingSourceState)))
    }

    fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        self.local_creations
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(SourceLocal::Dyn(Box::new(CountingSourceState)))
    }

    fn poll_next(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &SourceGlobal,
        _local: &mut SourceLocal,
        _output: &mut Chunk,
    ) -> Result<SourcePoll> {
        Ok(SourcePoll::Finished)
    }
}

fn counting_local_runtime(
    query: &QueryRuntimeContext,
    local_creations: Arc<std::sync::atomic::AtomicUsize>,
) -> Arc<PipelineRuntime> {
    let pipeline = PipelineId::new(0);
    let program = Arc::new(PipelineProgram {
        id: pipeline,
        source: SourceSlot {
            operator_id: RuntimeOperatorId::new(0),
            origin: RuntimeOperatorOrigin::new(
                pipeline,
                OperatorRole::Source,
                RuntimeRoleOrdinal::new(0),
            ),
            exec: SourceExec::Dyn(Box::new(CountingSourceExec { local_creations })),
        },
        transforms: Box::new([]),
        sink: SinkSlot {
            operator_id: RuntimeOperatorId::new(1),
            origin: RuntimeOperatorOrigin::new(
                pipeline,
                OperatorRole::Sink,
                RuntimeRoleOrdinal::new(0),
            ),
            exec: SinkExec::ClientResult(ClientResultSinkExec {
                spec: ClientResultSpec::default(),
            }),
        },
        sink_sharing: SinkSharing::Exclusive,
        scratch: PipelineScratchLayout::new(
            ChunkLayout::new(Vec::<LogicalType>::new(), VECTOR_SIZE),
            Vec::new(),
            VECTOR_SIZE,
        ),
        properties: PipelineProperties::default(),
    });
    Arc::new(
        PipelineRuntime::from_catalog(program, &Default::default(), query.params.clone(), query)
            .expect("counting runtime"),
    )
}

#[test]
fn client_result_sink_backpressure_retains_sink_input_without_clone() {
    let output = QueryOutputPort::bounded(1);
    output
        .try_push(Chunk::try_new(paro_common::test_utils::test_allocator()).unwrap())
        .assert_written();
    let query = query_context(output.clone());
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Values(values_spec(
            vec![vec![int_constant(7)]],
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
        task_id: PipelineTaskId(12),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    let result = executor
        .step(&mut step_context(&query, &thread, &wake, &mut profiler))
        .expect("first step");
    let TaskStepResult::Blocked(_) = result else {
        panic!("client result sink should block on full output port");
    };
    assert!(matches!(
        executor.task.pending,
        PendingChunkState::SinkInput { .. }
    ));

    output.pop_front();
    executor.resume_after_wake().expect("resume");
    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);

    let chunk = output.pop_front().expect("resumed sink output");
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(7));
}

#[test]
fn client_result_sink_repeated_backpressure_writes_pending_chunk_once() {
    let output = QueryOutputPort::bounded(1);
    output
        .try_push(Chunk::try_new(paro_common::test_utils::test_allocator()).unwrap())
        .assert_written();
    let query = query_context(output.clone());
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Values(values_spec(
            vec![vec![int_constant(7)], vec![int_constant(8)]],
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
        task_id: PipelineTaskId(15),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    assert!(matches!(
        executor
            .step(&mut step_context(&query, &thread, &wake, &mut profiler))
            .expect("first blocked write"),
        TaskStepResult::Blocked(_)
    ));

    executor
        .resume_after_wake()
        .expect("resume while still full");
    assert!(matches!(
        executor
            .step(&mut step_context(&query, &thread, &wake, &mut profiler))
            .expect("second blocked write"),
        TaskStepResult::Blocked(_)
    ));
    assert_eq!(output.len(), 1);
    assert!(matches!(
        executor.task.pending,
        PendingChunkState::SinkInput { .. }
    ));

    output.pop_front().expect("remove filler");
    executor.resume_after_wake().expect("resume after capacity");
    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);

    let chunk = output.pop_front().expect("resumed sink output");
    assert_eq!(chunk.size(), 2);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(7));
    assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(8));
    assert!(output.pop_front().is_none());
}

#[test]
fn successful_zero_row_completion_records_every_breaker_finish_boundary() {
    let shared_profile = ExplainProfiler::new();
    let query = query_context(QueryOutputPort::unbounded());
    let runtime = empty_runtime(&query);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(20),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::new(shared_profile.clone());

    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);
    profiler.flush();

    let snapshot = shared_profile.snapshot();
    for phase in [
        "breaker_prepare_finish",
        "breaker_finish_work",
        "breaker_finish",
    ] {
        let event = snapshot
            .events
            .iter()
            .find(|event| event.phase == phase)
            .unwrap_or_else(|| panic!("missing successful {phase} phase"));
        assert_eq!(event.rows, 0);
        assert!(event.end_time_ms >= event.start_time_ms);
    }
}

#[test]
fn streaming_limit_stop_pipeline_still_runs_completion() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output.clone());
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Dummy(DummyScanSpec),
        transforms: vec![
            TransformSpec::Project(ProjectSpec {
                expressions: vec![int_constant(1)].into_boxed_slice(),
                output_names: vec!["v".to_string()].into_boxed_slice(),
                visible_count: 1,
            }),
            TransformSpec::Limit(LimitSpec {
                limit: Some(int_constant(0)),
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
        task_id: PipelineTaskId(13),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);

    assert_eq!(executor.phase, PipelineTaskPhase::Done);
    assert!(output.pop_front().is_none());
}

#[test]
fn transform_stop_pipeline_flushes_only_downstream_transforms() {
    let output = QueryOutputPort::unbounded();
    let query = query_context(output);
    let spec = PipelineSpec {
        id: PipelineId::new(0),
        source: SourceSpec::Dummy(DummyScanSpec),
        transforms: vec![
            TransformSpec::Limit(LimitSpec {
                limit: Some(int_constant(0)),
                offset: None,
                hnsw_ef_hint: None,
            }),
            TransformSpec::Project(ProjectSpec {
                expressions: vec![int_constant(1)].into_boxed_slice(),
                output_names: vec!["v".to_string()].into_boxed_slice(),
                visible_count: 1,
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
        task_id: PipelineTaskId(14),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    let result = executor
        .step(&mut step_context(&query, &thread, &wake, &mut profiler))
        .expect("limit stop step");

    assert!(matches!(result, TaskStepResult::Continue));
    assert_eq!(
        executor.phase,
        PipelineTaskPhase::Flushing {
            transform_idx: 1,
            resume_idx: 0
        }
    );
}

#[derive(Debug)]
struct FailingFinishDriver {
    cancel_reason: Arc<Mutex<Option<CancelReason>>>,
}

impl ParallelFinishDriver for FailingFinishDriver {
    fn next_task(&self, _ctx: &mut OperatorFinishContext) -> Result<NextFinishTask> {
        Ok(NextFinishTask::Task(FinishTaskId(1)))
    }

    fn run_task(
        &self,
        _task: FinishTaskId,
        _ctx: &mut OperatorFinishContext,
    ) -> Result<FinishTaskPoll> {
        Err(paro_common::error::internal("finish task failed"))
    }

    fn cancel_group(&self, _ctx: &mut OperatorCleanupContext, reason: CancelReason) -> Result<()> {
        *self.cancel_reason.lock().expect("cancel reason lock") = Some(reason);
        Ok(())
    }
}

#[derive(Debug)]
struct FailingNextTaskDriver {
    cancel_reason: Arc<Mutex<Option<CancelReason>>>,
}

impl ParallelFinishDriver for FailingNextTaskDriver {
    fn next_task(&self, _ctx: &mut OperatorFinishContext) -> Result<NextFinishTask> {
        Err(paro_common::error::internal("next finish task failed"))
    }

    fn run_task(
        &self,
        _task: FinishTaskId,
        _ctx: &mut OperatorFinishContext,
    ) -> Result<FinishTaskPoll> {
        unreachable!("next_task fails before any task can run")
    }

    fn cancel_group(&self, _ctx: &mut OperatorCleanupContext, reason: CancelReason) -> Result<()> {
        *self.cancel_reason.lock().expect("cancel reason lock") = Some(reason);
        Ok(())
    }
}

#[derive(Debug)]
struct PendingFinishDriver {
    reason: BlockReason,
    source: WakeSource,
    cancel_reason: Arc<Mutex<Option<CancelReason>>>,
}

impl ParallelFinishDriver for PendingFinishDriver {
    fn next_task(&self, _ctx: &mut OperatorFinishContext) -> Result<NextFinishTask> {
        Ok(NextFinishTask::Task(FinishTaskId(1)))
    }

    fn run_task(
        &self,
        _task: FinishTaskId,
        ctx: &mut OperatorFinishContext,
    ) -> Result<FinishTaskPoll> {
        Ok(FinishTaskPoll::Pending(
            Blocker::new(self.reason.clone())
                .with_wake(ctx.wake.register(self.source, WakeToken(91))),
        ))
    }

    fn cancel_group(&self, _ctx: &mut OperatorCleanupContext, reason: CancelReason) -> Result<()> {
        *self.cancel_reason.lock().expect("cancel reason lock") = Some(reason);
        Ok(())
    }
}

#[derive(Debug)]
struct ConcurrentFinishDriver {
    task_count: usize,
    issued: std::sync::atomic::AtomicUsize,
    running: std::sync::atomic::AtomicUsize,
    max_running: Arc<std::sync::atomic::AtomicUsize>,
    completed: Arc<std::sync::atomic::AtomicUsize>,
    group_finished: Arc<std::sync::atomic::AtomicUsize>,
    all_tasks_query_accounted: Arc<std::sync::atomic::AtomicBool>,
}

impl ConcurrentFinishDriver {
    fn new(
        task_count: usize,
        max_running: Arc<std::sync::atomic::AtomicUsize>,
        completed: Arc<std::sync::atomic::AtomicUsize>,
        group_finished: Arc<std::sync::atomic::AtomicUsize>,
        all_tasks_query_accounted: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            task_count,
            issued: std::sync::atomic::AtomicUsize::new(0),
            running: std::sync::atomic::AtomicUsize::new(0),
            max_running,
            completed,
            group_finished,
            all_tasks_query_accounted,
        }
    }
}

impl ParallelFinishDriver for ConcurrentFinishDriver {
    fn next_task(&self, _ctx: &mut OperatorFinishContext) -> Result<NextFinishTask> {
        let next = self
            .issued
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if next >= self.task_count {
            return Ok(NextFinishTask::Drained);
        }
        Ok(NextFinishTask::Task(FinishTaskId(next as u32)))
    }

    fn run_task(
        &self,
        _task: FinishTaskId,
        ctx: &mut OperatorFinishContext,
    ) -> Result<FinishTaskPoll> {
        if ctx
            .memory
            .local_grant()
            .and_then(|grant| grant.grant().owner())
            .is_none()
        {
            self.all_tasks_query_accounted
                .store(false, std::sync::atomic::Ordering::Release);
        }
        let now = self
            .running
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        let _ = self.max_running.fetch_update(
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
            |current| Some(current.max(now)),
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
        self.running
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        self.completed
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(FinishTaskPoll::Done)
    }

    fn finish_group(&self, _ctx: &mut OperatorFinishContext) -> Result<()> {
        assert_eq!(
            self.completed.load(std::sync::atomic::Ordering::Acquire),
            self.task_count
        );
        self.group_finished
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }
}

#[test]
fn finish_task_error_cancels_group_as_operator_error() {
    let query = query_context(QueryOutputPort::unbounded());
    let runtime = empty_runtime(&query);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    executor.phase = PipelineTaskPhase::Merging;
    executor.completion_stage = PipelineCompletionStage::FinishWork;

    let cancel_reason = Arc::new(Mutex::new(None));
    executor.finish_group = Some(FinishTaskGroup {
        task_count: 1,
        driver: Arc::new(FailingFinishDriver {
            cancel_reason: cancel_reason.clone(),
        }),
        memory_class: MemoryClass::Blocking,
        coordinator_participation: FinishCoordinatorParticipation::SingleTask,
    });

    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(10),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    let err = executor
        .step(&mut step_context(&query, &thread, &wake, &mut profiler))
        .expect_err("finish task error should propagate");

    assert!(err.to_string().contains("finish task failed"));
    assert_eq!(
        *cancel_reason.lock().expect("cancel reason lock"),
        Some(CancelReason::OperatorError)
    );
    assert!(query.errors.root_error_id().is_some());
}

#[test]
fn parallel_finish_group_dispatches_subtasks_to_scheduler() {
    let shared_profile = ExplainProfiler::new();
    let query =
        query_context(QueryOutputPort::unbounded()).with_explain_profiler(shared_profile.clone());
    query.session.scheduler().set_threads(4).expect("threads");
    let local_creations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let runtime = counting_local_runtime(&query, local_creations.clone());
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    assert_eq!(
        local_creations.load(std::sync::atomic::Ordering::Acquire),
        1,
        "the pipeline worker creates one source local"
    );
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    executor.phase = PipelineTaskPhase::Merging;
    executor.completion_stage = PipelineCompletionStage::FinishWork;

    let max_running = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let group_finished = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let all_tasks_query_accounted = Arc::new(std::sync::atomic::AtomicBool::new(true));
    executor.finish_group = Some(FinishTaskGroup {
        task_count: 4,
        driver: Arc::new(ConcurrentFinishDriver::new(
            4,
            max_running.clone(),
            completed.clone(),
            group_finished.clone(),
            all_tasks_query_accounted.clone(),
        )),
        memory_class: MemoryClass::Blocking,
        coordinator_participation: FinishCoordinatorParticipation::SingleTask,
    });

    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(19),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::new(shared_profile.clone());

    run_to_done(&mut executor, &query, &thread, &wake, &mut profiler);
    profiler.flush();
    assert_eq!(
        local_creations.load(std::sync::atomic::Ordering::Acquire),
        1,
        "parallel finish workers must not construct operator locals"
    );
    assert!(
        all_tasks_query_accounted.load(std::sync::atomic::Ordering::Acquire),
        "parallel finish workers must use owner-backed query memory grants"
    );

    assert_eq!(
        completed.load(std::sync::atomic::Ordering::Acquire),
        4,
        "all finish subtasks should run"
    );
    assert!(
        max_running.load(std::sync::atomic::Ordering::Acquire) > 1,
        "finish subtasks should overlap on scheduler workers"
    );
    assert_eq!(
        group_finished.load(std::sync::atomic::Ordering::Acquire),
        1,
        "the runtime should publish one completed finish group"
    );
    let snapshot = shared_profile.snapshot();
    let finish_tasks = snapshot
        .events
        .iter()
        .filter(|event| event.phase == "breaker_finish_task")
        .collect::<Vec<_>>();
    assert_eq!(finish_tasks.len(), 4);
    let mut task_ranges = finish_tasks
        .iter()
        .map(|event| event.morsel_range.expect("finish task range").start)
        .collect::<Vec<_>>();
    task_ranges.sort_unstable();
    assert_eq!(task_ranges, vec![0, 1, 2, 3]);
    assert!(snapshot
        .events
        .iter()
        .any(|event| event.phase == "breaker_finish_group"));
    assert!(snapshot
        .events
        .iter()
        .any(|event| event.phase == "breaker_finish" && event.rows == 0));
}

#[test]
fn top_level_finish_does_not_construct_data_path_locals() {
    let query = query_context(QueryOutputPort::unbounded());
    let local_creations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let runtime = counting_local_runtime(&query, local_creations.clone());

    crate::runtime::scheduler::run_inline_finish_pipeline(
        runtime,
        &query,
        paro_common::test_utils::test_allocator(),
    )
    .expect("finish-only execution");

    assert_eq!(
        local_creations.load(std::sync::atomic::Ordering::Acquire),
        0,
        "top-level finish must not construct source, transform, or sink locals"
    );
}

#[test]
fn finish_task_discovery_error_cancels_group_as_operator_error() {
    let query = query_context(QueryOutputPort::unbounded());
    let runtime = empty_runtime(&query);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    executor.phase = PipelineTaskPhase::Merging;
    executor.completion_stage = PipelineCompletionStage::FinishWork;

    let cancel_reason = Arc::new(Mutex::new(None));
    executor.finish_group = Some(FinishTaskGroup {
        task_count: 1,
        driver: Arc::new(FailingNextTaskDriver {
            cancel_reason: cancel_reason.clone(),
        }),
        memory_class: MemoryClass::Blocking,
        coordinator_participation: FinishCoordinatorParticipation::SingleTask,
    });

    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(16),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    let err = executor
        .step(&mut step_context(&query, &thread, &wake, &mut profiler))
        .expect_err("finish discovery error should propagate");

    assert!(err.to_string().contains("next finish task failed"));
    assert_eq!(
        *cancel_reason.lock().expect("cancel reason lock"),
        Some(CancelReason::OperatorError)
    );
    assert!(query.errors.root_error_id().is_some());
}

#[test]
fn cancellation_cleans_pending_finish_group_without_recording_operator_error() {
    let mut query = query_context(QueryOutputPort::unbounded());
    let statement_token =
        install_statement_cancellation(&mut query, StatementCancelReason::UserRequest);
    let runtime = empty_runtime(&query);
    let task = runtime
        .create_task_state(&query, paro_common::test_utils::test_allocator())
        .expect("task state");
    let mut executor = PipelineTaskExecutor::new(runtime, task);
    executor.phase = PipelineTaskPhase::Merging;
    executor.completion_stage = PipelineCompletionStage::FinishWork;

    let cancel_reason = Arc::new(Mutex::new(None));
    executor.finish_group = Some(FinishTaskGroup {
        task_count: 1,
        driver: Arc::new(PendingFinishDriver {
            reason: BlockReason::ExternalRuntime,
            source: WakeSource::ExternalRuntime,
            cancel_reason: cancel_reason.clone(),
        }),
        memory_class: MemoryClass::Blocking,
        coordinator_participation: FinishCoordinatorParticipation::SingleTask,
    });

    let thread = ThreadContext::single_threaded();
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(17),
        generation: WakeGeneration(0),
    };
    let mut profiler = OperatorProfiler::disabled();

    let result = executor
        .step(&mut step_context(&query, &thread, &wake, &mut profiler))
        .expect("finish task should block");
    let TaskStepResult::Blocked(blocker) = result else {
        panic!("finish task should be pending");
    };
    assert_eq!(blocker.reason, BlockReason::ExternalRuntime);
    assert_eq!(
        blocker.wake.expect("external wake").source,
        WakeSource::ExternalRuntime
    );

    statement_token.cancel();
    executor
        .resume_after_wake()
        .expect("resume after cancellation");
    let err = executor
        .step(&mut step_context(&query, &thread, &wake, &mut profiler))
        .expect_err("cancelled task should stop before more finish work");

    assert!(err.is_query_canceled());
    assert_eq!(
        *cancel_reason.lock().expect("cancel reason lock"),
        Some(CancelReason::UserRequest)
    );
    assert!(query.errors.root_error_id().is_none());
}

#[test]
fn finish_group_pending_blockers_keep_wake_registration() {
    for (reason, source) in [
        (BlockReason::Memory, WakeSource::Memory),
        (BlockReason::Spill, WakeSource::Spill),
        (BlockReason::ExternalRuntime, WakeSource::ExternalRuntime),
    ] {
        let query = query_context(QueryOutputPort::unbounded());
        let runtime = empty_runtime(&query);
        let task = runtime
            .create_task_state(&query, paro_common::test_utils::test_allocator())
            .expect("task state");
        let mut executor = PipelineTaskExecutor::new(runtime, task);
        executor.phase = PipelineTaskPhase::Merging;
        executor.completion_stage = PipelineCompletionStage::FinishWork;
        executor.finish_group = Some(FinishTaskGroup {
            task_count: 1,
            driver: Arc::new(PendingFinishDriver {
                reason: reason.clone(),
                source,
                cancel_reason: Arc::new(Mutex::new(None)),
            }),
            memory_class: MemoryClass::Blocking,
            coordinator_participation: FinishCoordinatorParticipation::SingleTask,
        });

        let thread = ThreadContext::single_threaded();
        let wake = OperatorWakeScope {
            task_id: PipelineTaskId(18),
            generation: WakeGeneration(0),
        };
        let mut profiler = OperatorProfiler::disabled();

        let result = executor
            .step(&mut step_context(&query, &thread, &wake, &mut profiler))
            .expect("finish task should block");
        let TaskStepResult::Blocked(blocker) = result else {
            panic!("finish task should be pending");
        };
        assert_eq!(blocker.reason, reason);
        let registration = blocker.wake.expect("wake registration");
        assert_eq!(registration.task_id, PipelineTaskId(18));
        assert_eq!(registration.source, source);
    }
}
