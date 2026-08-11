// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PipelineScheduler execution driver.
//!
//! Ready source pipelines share the query worker pool. Each immutable
//! `PipelineRuntime` owns global source/sink state, data tasks create task-local
//! state and consume assigned morsels or shared source work, and one finish
//! worker seals each pipeline after its local merge barrier.

use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use parking_lot::Mutex as ParkingMutex;
use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, ParoError, Result};
use paro_scheduler::task::{
    InterruptState, ProducerToken, Task, TaskExecutionMode, TaskExecutionResult,
};

use crate::explain::profiler::{OperatorProfiler, ProfileMorselRange, ProfileWorkerContext};
use crate::explain::types::ExplainRuntimeStats;
use crate::memory_runtime::{AdmissionWaiterId, PipelineAdmissionGuard};
use crate::physical::properties::Parallelism;
use crate::pipeline::graph::{PipelineGraph, PipelineId, SinkSharing};
use crate::pipeline::PipelineProgramSet;
use crate::runtime::{
    Blocker, BreakerHandleRegistry, OperatorWakeScope, PendingWakeRegistration,
    PipelineDependencyGates, PipelineRuntime, PipelineTaskExecutor, PipelineTaskId,
    PipelineTaskStepContext, QueryRuntimeContext, SharedSinkMergeEvent, SharedSinkRuntimeSet,
    SourceGlobal, SourceLocal, TaskStepResult, WakeKey, WakeSource, WakeToken, WorkGroupCompletion,
};
use crate::thread_context::ThreadContext;

use super::scheduling_policy::{PipelineSchedulingPolicy, ReadyEntry};

pub struct PipelineScheduler<'a> {
    graph: &'a PipelineGraph,
    programs: &'a PipelineProgramSet,
    query: Arc<QueryRuntimeContext>,
    allocator: Arc<dyn Allocator>,
    handles: Arc<BreakerHandleRegistry>,
    shared_sinks: SharedSinkRuntimeSet,
    gates: PipelineDependencyGates,
    finished: Vec<bool>,
    finished_count: usize,
    ready: BinaryHeap<ReadyEntry<PipelineId>>,
    ready_seq: u64,
    policy: PipelineSchedulingPolicy,
}

impl<'a> PipelineScheduler<'a> {
    pub fn run_to_completion_with_registry(
        graph: &'a PipelineGraph,
        programs: &'a PipelineProgramSet,
        handles: Arc<BreakerHandleRegistry>,
        query: &QueryRuntimeContext,
        allocator: Arc<dyn Allocator>,
    ) -> Result<()> {
        let mut scheduler = Self::new(graph, programs, handles, query.clone(), allocator)?;
        scheduler.run()
    }

    pub fn should_use_parallel_scheduler(
        graph: &PipelineGraph,
        query: &QueryRuntimeContext,
    ) -> bool {
        query.session.limits.parallel_scheduler
            && graph.control_regions.is_empty()
            && query.session.number_of_threads() > 1
    }

    fn new(
        graph: &'a PipelineGraph,
        programs: &'a PipelineProgramSet,
        handles: Arc<BreakerHandleRegistry>,
        query: QueryRuntimeContext,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        if !graph.control_regions.is_empty() {
            return Err(paro_error::internal(
                "PipelineScheduler v1 does not execute control-region graphs",
            ));
        }
        let shared_sinks = SharedSinkRuntimeSet::from_graph(graph)?;
        let gates = PipelineDependencyGates::from_graph(graph);
        let mut scheduler = Self {
            graph,
            programs,
            query: Arc::new(query),
            allocator,
            handles,
            shared_sinks,
            gates,
            finished: vec![false; programs.pipeline_count()],
            finished_count: 0,
            ready: BinaryHeap::new(),
            ready_seq: 0,
            policy: PipelineSchedulingPolicy::default(),
        };
        for pipeline in scheduler.gates.ready_pipelines() {
            scheduler.push_ready_pipeline(pipeline, 0);
        }
        Ok(scheduler)
    }

    fn run(&mut self) -> Result<()> {
        while self.finished_count < self.finished.len() {
            self.query.cancellation.check()?;
            let Some(entry) = self.ready.pop() else {
                return Err(paro_error::internal(
                    "pipeline scheduler could not find a ready work unit",
                ));
            };
            let pipeline = entry.payload;
            if self.finished[pipeline.index()] {
                continue;
            }
            if !self.gates.is_ready(pipeline) {
                return Err(paro_error::internal(
                    "pipeline scheduler dequeued a pipeline before its gates opened",
                ));
            }
            let mut candidates = vec![(entry, self.create_runtime(pipeline)?)];
            while candidates.len() < self.query.session.number_of_threads().max(1) {
                let Some(entry) = self.ready.pop() else {
                    break;
                };
                let pipeline = entry.payload;
                if self.finished[pipeline.index()] {
                    continue;
                }
                if !self.gates.is_ready(pipeline) {
                    return Err(paro_error::internal(
                        "pipeline scheduler dequeued a pipeline before its gates opened",
                    ));
                }
                candidates.push((entry, self.create_runtime(pipeline)?));
            }

            let source_capable = candidates
                .iter()
                .filter(|(_, runtime)| has_schedulable_source_work(runtime))
                .count();
            if source_capable < 2 {
                let (_, runtime) = candidates.remove(0);
                for (entry, _) in candidates {
                    self.ready.push(entry);
                }
                self.run_runtime(runtime)?;
                self.mark_pipeline_finished(pipeline);
                continue;
            }

            let mut wave = Vec::with_capacity(source_capable);
            for (entry, runtime) in candidates {
                if has_schedulable_source_work(&runtime) {
                    wave.push((entry.payload, runtime));
                } else {
                    self.ready.push(entry);
                }
            }
            self.run_pipeline_wave(&wave)?;
            for (pipeline, _) in wave {
                self.mark_pipeline_finished(pipeline);
            }
        }
        Ok(())
    }

    fn run_runtime(&self, runtime: Arc<PipelineRuntime>) -> Result<()> {
        let pipeline = runtime.program.id;
        let properties = &self
            .graph
            .pipeline(pipeline)
            .ok_or_else(|| paro_error::internal("pipeline spec missing"))?
            .properties;
        run_bound_pipeline_runtime(
            runtime,
            properties.capabilities.parallelism,
            self.query.clone(),
            self.allocator.clone(),
        )
    }

    fn run_pipeline_wave(&self, wave: &[(PipelineId, Arc<PipelineRuntime>)]) -> Result<()> {
        let mut scheduled = Vec::with_capacity(wave.len());
        for (pipeline, runtime) in wave {
            let properties = &self
                .graph
                .pipeline(*pipeline)
                .ok_or_else(|| paro_error::internal("pipeline spec missing"))?
                .properties;
            match schedule_pipeline_data_tasks(
                runtime.clone(),
                properties.capabilities.parallelism,
                self.query.clone(),
                self.allocator.clone(),
            ) {
                Ok(execution) => scheduled.push(execution),
                Err(error) => {
                    for execution in &scheduled {
                        execution.cancel();
                    }
                    return Err(error);
                }
            }
        }

        for execution_idx in 0..scheduled.len() {
            if let Err(error) = scheduled[execution_idx].wait_and_finish() {
                for execution in &scheduled[execution_idx + 1..] {
                    execution.cancel();
                }
                return Err(error);
            }
        }
        Ok(())
    }

    fn create_runtime(&self, pipeline: PipelineId) -> Result<Arc<PipelineRuntime>> {
        let program = self
            .programs
            .get(pipeline)
            .cloned()
            .ok_or_else(|| paro_error::internal("pipeline program missing"))?;
        let spec = self
            .graph
            .pipeline(pipeline)
            .ok_or_else(|| paro_error::internal("pipeline spec missing"))?;
        let shared_sink = match spec.sink_sharing {
            SinkSharing::Exclusive => None,
            SinkSharing::Shared(id) => self.shared_sinks.get(id),
        };
        Ok(Arc::new(PipelineRuntime::with_registry_and_shared_sink(
            program,
            self.handles.clone(),
            self.query.params.clone(),
            self.query.as_ref(),
            shared_sink,
        )?))
    }

    fn push_ready_pipeline(&mut self, pipeline: PipelineId, dependency_unblocks: u32) {
        let priority = self.policy.ready_priority_for_pipeline(
            self.graph,
            pipeline,
            dependency_unblocks,
            self.ready_seq.min(u32::MAX as u64) as u32,
            self.query.memory.available_bytes(),
        );
        self.ready.push(ReadyEntry {
            priority,
            seq: self.ready_seq,
            payload: pipeline,
        });
        self.ready_seq = self.ready_seq.saturating_add(1);
    }

    fn mark_pipeline_finished(&mut self, pipeline: PipelineId) {
        if self.finished[pipeline.index()] {
            return;
        }
        self.finished[pipeline.index()] = true;
        self.finished_count += 1;
        self.handles.pipeline_finished(pipeline);
        for event in self.gates.mark_finished(pipeline) {
            if !self.finished[event.pipeline.index()] && self.gates.is_ready(event.pipeline) {
                self.push_ready_pipeline(event.pipeline, 1);
            }
        }
    }
}

/// Execute one already-bound pipeline runtime with the same source scheduler used by the ordinary
/// DAG driver. Control regions use this entry point to preserve their phase ordering while still
/// parallelizing each ready phase.
pub(crate) fn run_bound_pipeline_runtime(
    runtime: Arc<PipelineRuntime>,
    parallelism: Parallelism,
    query: Arc<QueryRuntimeContext>,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    let Some(source_work) = source_work(&runtime.source_global) else {
        return run_single_pipeline(runtime, query.as_ref(), allocator, 0, 1);
    };
    let work_unit_count = source_work.work_unit_count();
    let total_threads = pipeline_thread_count(parallelism, work_unit_count, query.as_ref());
    if total_threads <= 1 || work_unit_count <= 1 {
        return run_single_pipeline(runtime, query.as_ref(), allocator, 0, 1);
    }

    let assignments = source_work.into_task_assignments(total_threads);
    run_parallel_data_tasks(
        runtime.clone(),
        assignments,
        total_threads,
        query.clone(),
        allocator.clone(),
    )?;
    if let Some(shared) = runtime.shared_sink.as_ref() {
        match shared.mark_producer_merged()? {
            SharedSinkMergeEvent::WaitingForProducers { .. } => return Ok(()),
            SharedSinkMergeEvent::ReadyToFinish => {
                if !shared.try_begin_finish()? {
                    return Ok(());
                }
            }
        }
    }
    run_finish_task(runtime, total_threads, query, allocator)
}

fn pipeline_thread_count(
    parallelism: Parallelism,
    work_unit_count: usize,
    query: &QueryRuntimeContext,
) -> usize {
    if !query.session.limits.parallel_scheduler || parallelism.max <= 1 || work_unit_count <= 1 {
        return 1;
    }
    let threads = query.session.number_of_threads().max(1);
    parallelism
        .max
        .min(threads)
        .min(work_unit_count)
        .max(parallelism.min)
        .max(1)
}

fn run_single_pipeline(
    runtime: Arc<PipelineRuntime>,
    query: &QueryRuntimeContext,
    allocator: Arc<dyn Allocator>,
    thread_id: usize,
    total_threads: usize,
) -> Result<()> {
    let task = runtime.create_task_state(query, allocator)?;
    let mut executor = PipelineTaskExecutor::new(runtime.clone(), task);
    let thread = ThreadContext::new(thread_id, total_threads);
    let wake = OperatorWakeScope {
        task_id: PipelineTaskId(runtime.program.id.index() as u64),
        generation: query.output.wake_generation(),
    };
    let mut profiler =
        query
            .explain_profiler
            .as_ref()
            .map_or_else(OperatorProfiler::disabled, |profiler| {
                OperatorProfiler::new_with_context(
                    profiler.clone(),
                    ProfileWorkerContext::new(
                        Some(runtime.program.id.index() as u64),
                        Some(runtime.program.id.index() as u64),
                        Some(thread_id as u64),
                        Some(total_threads as u64),
                        None,
                    ),
                )
            });
    let mut ctx = PipelineTaskStepContext {
        query,
        thread: &thread,
        wake: &wake,
        profiler: &mut profiler,
    };
    loop {
        match executor.step(&mut ctx)? {
            TaskStepResult::Continue => {}
            TaskStepResult::Done => {
                profiler.flush();
                return Ok(());
            }
            TaskStepResult::Blocked(blocker) => return Err(single_task_blocked_error(&blocker)),
        }
    }
}

fn run_parallel_data_tasks(
    runtime: Arc<PipelineRuntime>,
    assignments: Vec<SourceTaskAssignment>,
    total_threads: usize,
    query: Arc<QueryRuntimeContext>,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    schedule_data_tasks(runtime, assignments, total_threads, query, allocator)?.wait()
}

struct ScheduledDataTasks {
    scheduler: Arc<paro_scheduler::scheduler::TaskScheduler>,
    producer: ProducerToken,
    group: Arc<PipelineWorkerCoordinator>,
    query: Arc<QueryRuntimeContext>,
}

impl ScheduledDataTasks {
    fn wait(&self) -> Result<()> {
        wait_for_group(
            self.query.as_ref(),
            self.scheduler.as_ref(),
            &self.producer,
            &self.group,
        )
    }

    fn cancel(&self) {
        cancel_and_drain(self.scheduler.as_ref(), &self.producer, &self.group);
    }
}

fn schedule_data_tasks(
    runtime: Arc<PipelineRuntime>,
    assignments: Vec<SourceTaskAssignment>,
    total_threads: usize,
    query: Arc<QueryRuntimeContext>,
    allocator: Arc<dyn Allocator>,
) -> Result<ScheduledDataTasks> {
    if assignments.is_empty() {
        return Err(paro_error::internal(
            "cannot schedule a source pipeline without data assignments",
        ));
    }
    let scheduler = query.session.scheduler().clone();
    let producer = scheduler.create_producer_with_priority(0);
    let group = Arc::new(PipelineWorkerCoordinator::new(assignments.len()));
    let mut tasks = Vec::with_capacity(assignments.len());
    for (task_idx, assignment) in assignments.into_iter().enumerate() {
        let work = WorkUnit::data(runtime.program.id, task_idx as u64, assignment);
        let task = PipelineWorkerTask::new_data(
            runtime.clone(),
            query.clone(),
            allocator.clone(),
            Arc::downgrade(&group),
            work,
            task_idx % total_threads,
            total_threads,
            Some(assignment),
        );
        tasks.push(as_scheduler_task(task));
    }
    producer.schedule_tasks(tasks);
    Ok(ScheduledDataTasks {
        scheduler,
        producer,
        group,
        query,
    })
}

struct ScheduledPipelineExecution {
    runtime: Arc<PipelineRuntime>,
    total_threads: usize,
    data: ScheduledDataTasks,
    query: Arc<QueryRuntimeContext>,
    allocator: Arc<dyn Allocator>,
}

impl ScheduledPipelineExecution {
    fn wait_and_finish(&self) -> Result<()> {
        self.data.wait()?;
        if let Some(shared) = self.runtime.shared_sink.as_ref() {
            match shared.mark_producer_merged()? {
                SharedSinkMergeEvent::WaitingForProducers { .. } => return Ok(()),
                SharedSinkMergeEvent::ReadyToFinish => {
                    if !shared.try_begin_finish()? {
                        return Ok(());
                    }
                }
            }
        }
        run_finish_task(
            self.runtime.clone(),
            self.total_threads,
            self.query.clone(),
            self.allocator.clone(),
        )
    }

    fn cancel(&self) {
        self.data.cancel();
    }
}

fn schedule_pipeline_data_tasks(
    runtime: Arc<PipelineRuntime>,
    parallelism: Parallelism,
    query: Arc<QueryRuntimeContext>,
    allocator: Arc<dyn Allocator>,
) -> Result<ScheduledPipelineExecution> {
    let work = source_work(&runtime.source_global)
        .filter(|work| work.work_unit_count() > 0)
        .ok_or_else(|| paro_error::internal("pipeline wave requires source work"))?;
    let total_threads = pipeline_thread_count(parallelism, work.work_unit_count(), query.as_ref());
    let assignments = work.into_task_assignments(total_threads);
    let data = schedule_data_tasks(
        runtime.clone(),
        assignments,
        total_threads,
        query.clone(),
        allocator.clone(),
    )?;
    Ok(ScheduledPipelineExecution {
        runtime,
        total_threads,
        data,
        query,
        allocator,
    })
}

fn run_finish_task(
    runtime: Arc<PipelineRuntime>,
    total_threads: usize,
    query: Arc<QueryRuntimeContext>,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    let scheduler = query.session.scheduler().clone();
    let producer = scheduler.create_producer_with_priority(0);
    let group = Arc::new(PipelineWorkerCoordinator::new(1));
    let work = WorkUnit::finish(runtime.program.id);
    let task = as_scheduler_task(PipelineWorkerTask::new_finish(
        runtime,
        query.clone(),
        allocator,
        Arc::downgrade(&group),
        work,
        0,
        total_threads,
    ));
    producer.schedule_task(task);
    wait_for_group(query.as_ref(), scheduler.as_ref(), &producer, &group)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct WorkUnitId(u64);

/// A worker that dynamically claims work from source-owned shared state.
///
/// Unlike a morsel, this assignment carries no data range. Its only contract is to bound the
/// number of task-local consumers concurrently draining the source queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedSourceWorker {
    RowsetScan,
    HashAggregateEmit,
    HashJoinUnmatched,
    SortEmit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceTaskAssignment {
    ChunkRange { start: usize, end: usize },
    SharedWorker(SharedSourceWorker),
}

impl SourceTaskAssignment {
    fn morsel_count(self) -> Option<usize> {
        match self {
            Self::ChunkRange { start, end } => Some(end - start),
            Self::SharedWorker(_) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceWork {
    RowsetScan {
        count: usize,
    },
    Chunks {
        count: usize,
    },
    SharedWorkers {
        count: usize,
        worker: SharedSourceWorker,
    },
}

// Keep enough independent work for load balancing without making task-state
// construction scale with the number of source morsels.
const DATA_TASKS_PER_THREAD: usize = 4;

impl SourceWork {
    fn work_unit_count(self) -> usize {
        match self {
            Self::RowsetScan { count }
            | Self::Chunks { count }
            | Self::SharedWorkers { count, .. } => count,
        }
    }

    fn into_task_assignments(self, total_threads: usize) -> Vec<SourceTaskAssignment> {
        match self {
            Self::RowsetScan { count } => (0..count.min(total_threads))
                .map(|_| SourceTaskAssignment::SharedWorker(SharedSourceWorker::RowsetScan))
                .collect(),
            Self::Chunks { count } => partition_morsel_ranges(count, total_threads)
                .map(|(start, end)| SourceTaskAssignment::ChunkRange { start, end })
                .collect(),
            Self::SharedWorkers { count, worker } => (0..count.min(total_threads))
                .map(|_| SourceTaskAssignment::SharedWorker(worker))
                .collect(),
        }
    }
}

fn partition_morsel_ranges(
    morsel_count: usize,
    total_threads: usize,
) -> impl Iterator<Item = (usize, usize)> {
    let task_count = morsel_count.min(total_threads.saturating_mul(DATA_TASKS_PER_THREAD));
    let divisor = task_count.max(1);
    let morsels_per_task = morsel_count / divisor;
    let remainder = morsel_count % divisor;
    (0..task_count).map(move |task_idx| {
        let start = task_idx * morsels_per_task + task_idx.min(remainder);
        let end = start + morsels_per_task + usize::from(task_idx < remainder);
        (start, end)
    })
}

const PROFILE_MORSEL_CHUNK: &str = "chunk";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkUnitKind {
    Data(SourceTaskAssignment),
    Finish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkUnit {
    id: WorkUnitId,
    pipeline: PipelineId,
    kind: WorkUnitKind,
}

impl WorkUnit {
    fn data(pipeline: PipelineId, ordinal: u64, assignment: SourceTaskAssignment) -> Self {
        Self {
            id: WorkUnitId(((pipeline.index() as u64) << 32) | ordinal),
            pipeline,
            kind: WorkUnitKind::Data(assignment),
        }
    }

    fn finish(pipeline: PipelineId) -> Self {
        Self {
            id: WorkUnitId(((pipeline.index() as u64) << 32) | 0xffff_ffff),
            pipeline,
            kind: WorkUnitKind::Finish,
        }
    }
}

struct PipelineWorkerCoordinator {
    completion: WorkGroupCompletion,
    inner: Mutex<PipelineWorkerCoordinatorInner>,
}

struct PipelineWorkerCoordinatorInner {
    waiters: WaiterRegistry,
    blocked: HashMap<WorkUnitId, BlockedWorker>,
    ready: VecDeque<PipelineWorkerTask>,
}

impl PipelineWorkerCoordinator {
    fn new(task_count: usize) -> Self {
        Self {
            completion: WorkGroupCompletion::new(task_count),
            inner: Mutex::new(PipelineWorkerCoordinatorInner {
                waiters: WaiterRegistry::default(),
                blocked: HashMap::new(),
                ready: VecDeque::new(),
            }),
        }
    }

    fn finish(&self, result: Result<()>) {
        self.completion.finish(result);
    }

    fn cancel_queued(&self, count: usize) {
        self.completion.cancel_queued(count);
    }

    fn cancel_blocked(&self) -> usize {
        let mut inner = self
            .inner
            .lock()
            .expect("pipeline worker coordinator lock poisoned");
        // `ready` holds resumed work that has not been handed back to the
        // scheduler yet. Those units have no running worker left to report
        // completion, so cancellation must account for them with blocked work.
        let count = inner.blocked.len().saturating_add(inner.ready.len());
        if count == 0 {
            return 0;
        }
        inner.blocked.clear();
        inner.ready.clear();
        inner.waiters.clear();
        drop(inner);
        self.completion.cancel_queued(count);
        count
    }

    fn block(&self, blocked: BlockedWorker) {
        let mut inner = self
            .inner
            .lock()
            .expect("pipeline worker coordinator lock poisoned");
        if let Some(wake) = blocked.blocker.wake {
            inner.waiters.register(wake, blocked.task.work.id);
            inner.blocked.insert(blocked.task.work.id, blocked);
        } else {
            drop(inner);
            self.completion.finish(Err(paro_error::internal(format!(
                "pipeline work unit {:?} blocked on {:?} without a wake key",
                blocked.task.work.id, blocked.blocker.reason
            ))));
        }
    }

    fn drain_ready(&self, query: &QueryRuntimeContext) -> Vec<PipelineWorkerTask> {
        let mut inner = self
            .inner
            .lock()
            .expect("pipeline worker coordinator lock poisoned");
        let ready = inner.waiters.ready_keys(query);
        let coalesced_wakes = ready.coalesced_wakes as u64;
        for key in ready.keys {
            for unit_id in inner.waiters.wake(key) {
                if let Some(blocked) = inner.blocked.remove(&unit_id) {
                    inner
                        .ready
                        .push_back(blocked.task.into_resumed(coalesced_wakes));
                }
            }
        }
        inner.ready.drain(..).collect()
    }

    fn snapshot(&self) -> Result<Option<usize>> {
        self.completion.snapshot()
    }

    fn remaining(&self) -> usize {
        self.completion.remaining()
    }

    fn wait_for_progress_with_timeout(&self, observed_remaining: usize) {
        self.completion
            .wait_for_progress_with_timeout(observed_remaining);
    }
}

#[derive(Default)]
struct WaiterRegistry {
    by_key: HashMap<WakeKey, Vec<WorkUnitId>>,
    by_unit: HashMap<WorkUnitId, WakeKey>,
}

impl WaiterRegistry {
    fn register(&mut self, wake: PendingWakeRegistration, unit_id: WorkUnitId) {
        let key = wake.key();
        let previous_key = self.by_unit.insert(unit_id, key);
        if previous_key == Some(key) {
            return;
        }
        if let Some(previous_key) = previous_key {
            if let Some(waiters) = self.by_key.get_mut(&previous_key) {
                waiters.retain(|unit| *unit != unit_id);
                if waiters.is_empty() {
                    self.by_key.remove(&previous_key);
                }
            }
        }
        let waiters = self.by_key.entry(key).or_default();
        if !waiters.contains(&unit_id) {
            waiters.push(unit_id);
        }
    }

    fn wake(&mut self, key: WakeKey) -> Vec<WorkUnitId> {
        let units = self.by_key.remove(&key).unwrap_or_default();
        for unit in &units {
            self.by_unit.remove(unit);
        }
        units
    }

    fn ready_keys(&self, query: &QueryRuntimeContext) -> ReadyWakeBatch {
        let mut keys = Vec::new();
        let mut coalesced_wakes = 0;
        for key in self.by_key.keys().copied() {
            if let Some(coalesced) = wake_key_ready(query, key) {
                keys.push(key);
                coalesced_wakes += coalesced as usize;
            }
        }
        ReadyWakeBatch {
            keys,
            coalesced_wakes,
        }
    }

    fn clear(&mut self) {
        self.by_key.clear();
        self.by_unit.clear();
    }
}

struct ReadyWakeBatch {
    keys: Vec<WakeKey>,
    coalesced_wakes: usize,
}

struct BlockedWorker {
    blocker: Blocker,
    task: PipelineWorkerTask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipelineWorkerMode {
    Data,
    Finish,
}

struct PipelineWorkerTask {
    runtime: Arc<PipelineRuntime>,
    query: Arc<QueryRuntimeContext>,
    allocator: Arc<dyn Allocator>,
    group: Weak<PipelineWorkerCoordinator>,
    work: WorkUnit,
    thread_id: usize,
    total_threads: usize,
    mode: PipelineWorkerMode,
    source_assignment: Option<SourceTaskAssignment>,
    executor: Option<PipelineTaskExecutor>,
    profiler: Option<OperatorProfiler>,
    blocked: Option<Blocker>,
    blocked_at: Option<Instant>,
    ready_at: Option<Instant>,
    wake_coalesce_count: u64,
    recorded_start: bool,
    token: Option<ProducerToken>,
}

impl PipelineWorkerTask {
    fn new_data(
        runtime: Arc<PipelineRuntime>,
        query: Arc<QueryRuntimeContext>,
        allocator: Arc<dyn Allocator>,
        group: Weak<PipelineWorkerCoordinator>,
        work: WorkUnit,
        thread_id: usize,
        total_threads: usize,
        source_assignment: Option<SourceTaskAssignment>,
    ) -> Self {
        let ready_at = query.explain_profiler.as_ref().map(|_| Instant::now());
        Self {
            runtime,
            query,
            allocator,
            group,
            work,
            thread_id,
            total_threads,
            mode: PipelineWorkerMode::Data,
            source_assignment,
            executor: None,
            profiler: None,
            blocked: None,
            blocked_at: None,
            ready_at,
            wake_coalesce_count: 0,
            recorded_start: false,
            token: None,
        }
    }

    fn new_finish(
        runtime: Arc<PipelineRuntime>,
        query: Arc<QueryRuntimeContext>,
        allocator: Arc<dyn Allocator>,
        group: Weak<PipelineWorkerCoordinator>,
        work: WorkUnit,
        thread_id: usize,
        total_threads: usize,
    ) -> Self {
        let ready_at = query.explain_profiler.as_ref().map(|_| Instant::now());
        Self {
            runtime,
            query,
            allocator,
            group,
            work,
            thread_id,
            total_threads,
            mode: PipelineWorkerMode::Finish,
            source_assignment: None,
            executor: None,
            profiler: None,
            blocked: None,
            blocked_at: None,
            ready_at,
            wake_coalesce_count: 0,
            recorded_start: false,
            token: None,
        }
    }

    fn into_resumed(mut self, coalesced_wakes: u64) -> Self {
        self.blocked = None;
        self.ready_at = self.query.explain_profiler.as_ref().map(|_| Instant::now());
        self.wake_coalesce_count = self.wake_coalesce_count.saturating_add(coalesced_wakes);
        self
    }

    fn take_blocked_worker(&mut self, blocker: Blocker) -> BlockedWorker {
        BlockedWorker {
            blocker,
            task: PipelineWorkerTask {
                runtime: self.runtime.clone(),
                query: self.query.clone(),
                allocator: self.allocator.clone(),
                group: self.group.clone(),
                work: self.work,
                thread_id: self.thread_id,
                total_threads: self.total_threads,
                mode: self.mode,
                source_assignment: self.source_assignment,
                executor: self.executor.take(),
                profiler: self.profiler.take(),
                blocked: self.blocked.take(),
                blocked_at: self.blocked_at.take(),
                ready_at: self.ready_at.take(),
                wake_coalesce_count: self.wake_coalesce_count,
                recorded_start: self.recorded_start,
                token: None,
            },
        }
    }

    fn ensure_profiler(&mut self) {
        if self.profiler.is_some() {
            return;
        }
        self.profiler = Some(self.query.explain_profiler.as_ref().map_or_else(
            OperatorProfiler::disabled,
            |profiler| {
                OperatorProfiler::new_with_context(
                    profiler.clone(),
                    ProfileWorkerContext::new(
                        Some(self.runtime.program.id.index() as u64),
                        Some(self.work.id.0),
                        Some(self.thread_id as u64),
                        Some(self.total_threads as u64),
                        profile_morsel_range_from_work(self.work),
                    ),
                )
            },
        ));
    }

    fn ensure_executor(&mut self) -> Result<()> {
        if self.executor.is_some() {
            return Ok(());
        }
        let mut task = self
            .runtime
            .create_task_state(self.query.as_ref(), self.allocator.clone())?;
        if let Some(assignment) = self.source_assignment {
            prepare_source_task(&mut task.source, assignment)?;
        }
        self.executor = Some(match self.mode {
            PipelineWorkerMode::Data => {
                PipelineTaskExecutor::new_parallel_data_task(self.runtime.clone(), task)
            }
            PipelineWorkerMode::Finish => {
                PipelineTaskExecutor::new_finish_task(self.runtime.clone(), task)
            }
        });
        Ok(())
    }

    fn run(&mut self) -> Result<Option<Blocker>> {
        self.ensure_profiler();
        let source_node_id = self.runtime.program.source.operator_id.index() as u64;
        if let Some(ready_at) = self.ready_at.take() {
            let ready_time_us = ready_at.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            if ready_time_us > 0 {
                self.profiler
                    .as_mut()
                    .expect("profiler initialized")
                    .record_runtime(
                        source_node_id,
                        ExplainRuntimeStats {
                            scheduler_ready_time_us: Some(ready_time_us),
                            ..ExplainRuntimeStats::default()
                        },
                    );
            }
        }
        let _admission = match self.try_enter_admission()? {
            AdmissionEntry::Acquired(guard) => guard,
            AdmissionEntry::Blocked(blocker) => {
                self.profiler
                    .as_mut()
                    .expect("profiler initialized")
                    .record_blocked(source_node_id, &blocker);
                self.blocked_at = Some(Instant::now());
                self.blocked = Some(blocker.clone());
                return Ok(Some(blocker));
            }
        };
        self.ensure_executor()?;
        let previous_blocker = self.blocked.take();
        let resumed_from_block = previous_blocker.is_some();
        if resumed_from_block {
            let wait_time_us = self
                .blocked_at
                .take()
                .map(|blocked_at| blocked_at.elapsed().as_micros().min(u128::from(u64::MAX)) as u64)
                .unwrap_or(0);
            self.profiler
                .as_mut()
                .expect("profiler initialized")
                .record_wake(source_node_id, previous_blocker.as_ref(), wait_time_us);
            self.executor
                .as_mut()
                .expect("executor initialized")
                .resume_after_wake()?;
        }
        let thread = ThreadContext::new(self.thread_id, self.total_threads);
        let wake = self.wake_scope();
        let mut ctx = PipelineTaskStepContext {
            query: self.query.as_ref(),
            thread: &thread,
            wake: &wake,
            profiler: self.profiler.as_mut().expect("profiler initialized"),
        };
        if self.source_assignment.is_some() && !self.recorded_start {
            ctx.profiler.record_runtime(
                source_node_id,
                ExplainRuntimeStats {
                    scheduler_worker_count: Some(1),
                    scheduler_morsel_count: self
                        .source_assignment
                        .and_then(SourceTaskAssignment::morsel_count)
                        .map(|count| count as u64),
                    ..ExplainRuntimeStats::default()
                },
            );
            self.recorded_start = true;
        }
        if self.wake_coalesce_count > 0 {
            let coalesced_wakes = self.wake_coalesce_count;
            self.wake_coalesce_count = 0;
            ctx.profiler.record_runtime(
                source_node_id,
                ExplainRuntimeStats {
                    scheduler_wake_coalesce_count: Some(coalesced_wakes),
                    ..ExplainRuntimeStats::default()
                },
            );
        }
        loop {
            let step = match self.mode {
                PipelineWorkerMode::Data => self
                    .executor
                    .as_mut()
                    .expect("executor initialized")
                    .step_until_local_merge(&mut ctx)?,
                PipelineWorkerMode::Finish => self
                    .executor
                    .as_mut()
                    .expect("executor initialized")
                    .step(&mut ctx)?,
            };
            match step {
                TaskStepResult::Continue => {}
                TaskStepResult::Done => {
                    self.profiler
                        .as_mut()
                        .expect("profiler initialized")
                        .flush();
                    return Ok(None);
                }
                TaskStepResult::Blocked(blocker) => {
                    ctx.profiler.record_blocked(source_node_id, &blocker);
                    self.blocked = Some(blocker.clone());
                    self.blocked_at = Some(Instant::now());
                    return Ok(Some(blocker));
                }
            }
        }
    }

    fn try_enter_admission(&self) -> Result<AdmissionEntry> {
        let wake = self
            .wake_scope()
            .register(WakeSource::Memory, WakeToken(self.work.id.0));
        let key = wake.key();
        let query = self.query.clone();
        let interrupt = InterruptState::with_callback(Arc::new(move || {
            query.wake_events.wake(key);
            Ok(())
        }));
        let controller = self.query.memory.admission_controller();
        if let Some(guard) =
            controller.try_acquire_for(AdmissionWaiterId(self.work.id.0), interrupt)
        {
            return Ok(AdmissionEntry::Acquired(guard));
        }
        Ok(AdmissionEntry::Blocked(
            Blocker::new(crate::runtime::BlockReason::Memory).with_wake(wake),
        ))
    }

    fn wake_scope(&self) -> OperatorWakeScope {
        OperatorWakeScope {
            task_id: PipelineTaskId(self.work.id.0),
            generation: self.query.output.wake_generation(),
        }
    }
}

enum AdmissionEntry {
    Acquired(PipelineAdmissionGuard),
    Blocked(Blocker),
}

impl Task for PipelineWorkerTask {
    fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        let result = match panic::catch_unwind(AssertUnwindSafe(|| self.run())) {
            Ok(result) => result,
            Err(_) => Err(paro_error::internal("pipeline worker task panicked")),
        };
        let Some(group) = self.group.upgrade() else {
            return Err(paro_error::internal("pipeline worker coordinator dropped"));
        };
        match result {
            Ok(None) => group.finish(Ok(())),
            Ok(Some(blocker)) => {
                group.block(self.take_blocked_worker(blocker));
            }
            Err(error) => group.finish(Err(error)),
        }
        Ok(TaskExecutionResult::Finished)
    }

    fn set_token(&mut self, token: ProducerToken) {
        self.token = Some(token);
    }

    fn get_token(&self) -> Option<ProducerToken> {
        self.token.clone()
    }

    fn task_type(&self) -> &str {
        "PipelineWorkerTask"
    }
}

fn wait_for_group(
    query: &QueryRuntimeContext,
    scheduler: &paro_scheduler::scheduler::TaskScheduler,
    producer: &ProducerToken,
    group: &Arc<PipelineWorkerCoordinator>,
) -> Result<()> {
    let marker = std::sync::atomic::AtomicBool::new(true);
    loop {
        let ready = group.drain_ready(query);
        if !ready.is_empty() {
            producer.schedule_tasks(ready.into_iter().map(as_scheduler_task).collect());
        }
        if let Err(error) = query.cancellation.check() {
            cancel_and_drain(scheduler, producer, group);
            return Err(error);
        }
        match group.snapshot() {
            Ok(None) => return Ok(()),
            Ok(Some(_)) => {}
            Err(error) => {
                cancel_and_drain(scheduler, producer, group);
                return Err(error);
            }
        }
        scheduler.execute_tasks_for_producer(producer, &marker, usize::MAX);
        if let Some(error) = scheduler.get_error_for_producer(producer) {
            cancel_and_drain(scheduler, producer, group);
            return Err(paro_error::internal(error.to_string()));
        }
        let remaining = match group.snapshot() {
            Ok(None) => return Ok(()),
            Ok(Some(remaining)) => remaining,
            Err(error) => {
                cancel_and_drain(scheduler, producer, group);
                return Err(error);
            }
        };
        group.wait_for_progress_with_timeout(remaining);
    }
}

fn cancel_and_drain(
    scheduler: &paro_scheduler::scheduler::TaskScheduler,
    producer: &ProducerToken,
    group: &PipelineWorkerCoordinator,
) {
    let cancelled = scheduler.cancel_tasks_for_producer(producer);
    group.cancel_queued(cancelled);
    group.cancel_blocked();
    wait_for_running_workers(group);
}

fn wait_for_running_workers(group: &PipelineWorkerCoordinator) {
    loop {
        let remaining = group.remaining();
        if remaining == 0 {
            return;
        }
        group.wait_for_progress_with_timeout(remaining);
    }
}

fn as_scheduler_task(task: PipelineWorkerTask) -> Arc<ParkingMutex<dyn Task>> {
    Arc::new(ParkingMutex::new(task))
}

fn source_work(source: &SourceGlobal) -> Option<SourceWork> {
    match source {
        SourceGlobal::Rowset(global) => Some(SourceWork::RowsetScan {
            count: global.morsels.len(),
        }),
        SourceGlobal::Chunk(global) => Some(SourceWork::Chunks {
            count: global.chunks.len(),
        }),
        SourceGlobal::HashAggregateEmit(global) if global.work_count() > 1 => {
            Some(SourceWork::SharedWorkers {
                count: global.work_count(),
                worker: SharedSourceWorker::HashAggregateEmit,
            })
        }
        SourceGlobal::HashJoinUnmatched(global) if global.work_count() > 1 => {
            Some(SourceWork::SharedWorkers {
                count: global.work_count(),
                worker: SharedSourceWorker::HashJoinUnmatched,
            })
        }
        SourceGlobal::SortEmit(_) => Some(SourceWork::SharedWorkers {
            count: 1,
            worker: SharedSourceWorker::SortEmit,
        }),
        _ => None,
    }
}

fn has_schedulable_source_work(runtime: &PipelineRuntime) -> bool {
    source_work(&runtime.source_global).is_some_and(|work| work.work_unit_count() > 0)
}

fn prepare_source_task(source: &mut SourceLocal, assignment: SourceTaskAssignment) -> Result<()> {
    match (source, assignment) {
        (
            SourceLocal::Rowset(_),
            SourceTaskAssignment::SharedWorker(SharedSourceWorker::RowsetScan),
        ) => Ok(()),
        (SourceLocal::Chunk(local), SourceTaskAssignment::ChunkRange { start, end }) => {
            local.assign_chunk_range(start, end);
            Ok(())
        }
        (
            SourceLocal::HashAggregateEmit(_),
            SourceTaskAssignment::SharedWorker(SharedSourceWorker::HashAggregateEmit),
        )
        | (
            SourceLocal::HashJoinUnmatched(_),
            SourceTaskAssignment::SharedWorker(SharedSourceWorker::HashJoinUnmatched),
        )
        | (
            SourceLocal::SortEmit(_),
            SourceTaskAssignment::SharedWorker(SharedSourceWorker::SortEmit),
        ) => Ok(()),
        (source, assignment) => Err(paro_error::internal(format!(
            "source local {} cannot accept scheduler assignment {:?}",
            source.variant_name(),
            assignment
        ))),
    }
}

fn profile_morsel_range_from_work(work: WorkUnit) -> Option<ProfileMorselRange> {
    match work.kind {
        WorkUnitKind::Data(SourceTaskAssignment::ChunkRange { start, end }) => Some(
            ProfileMorselRange::new(PROFILE_MORSEL_CHUNK, start as u64, end as u64),
        ),
        WorkUnitKind::Data(SourceTaskAssignment::SharedWorker(_)) | WorkUnitKind::Finish => None,
    }
}

fn wake_key_ready(query: &QueryRuntimeContext, key: WakeKey) -> Option<u64> {
    match key.source {
        WakeSource::OutputBuffer => (query.output.wake_generation() != key.generation).then_some(0),
        WakeSource::Cancellation => query.cancellation.is_cancelled().then_some(0),
        WakeSource::Memory
        | WakeSource::Spill
        | WakeSource::ExternalRuntime
        | WakeSource::DerivedIndex => query.wake_events.take_ready_with_coalesced(key),
    }
}

fn single_task_blocked_error(blocker: &Blocker) -> ParoError {
    match blocker.wake.map(|wake| wake.source) {
        Some(WakeSource::OutputBuffer) => {
            paro_error::internal("synchronously driven pipeline blocked on output backpressure")
        }
        Some(source) => paro_error::internal(format!(
            "synchronously driven pipeline blocked on unsupported wake source {source:?}"
        )),
        None => paro_error::internal(format!(
            "synchronously driven pipeline blocked on {:?} without a wake key",
            blocker.reason
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::PipelineReadyPriority;

    use super::*;

    #[test]
    fn ready_heap_uses_policy_priority() {
        let mut heap = BinaryHeap::new();
        heap.push(ReadyEntry {
            priority: PipelineReadyPriority::new(1),
            seq: 0,
            payload: PipelineId::new(0),
        });
        heap.push(ReadyEntry {
            priority: PipelineReadyPriority::new(10),
            seq: 1,
            payload: PipelineId::new(1),
        });
        assert_eq!(heap.pop().unwrap().payload, PipelineId::new(1));
    }

    #[test]
    fn source_work_batches_many_morsels_into_bounded_contiguous_ranges() {
        let assignments = SourceWork::Chunks { count: 1_024 }.into_task_assignments(4);

        assert_eq!(assignments.len(), 4 * DATA_TASKS_PER_THREAD);
        assert_eq!(
            assignments.first(),
            Some(&SourceTaskAssignment::ChunkRange { start: 0, end: 64 })
        );
        assert_eq!(
            assignments.last(),
            Some(&SourceTaskAssignment::ChunkRange {
                start: 960,
                end: 1_024
            })
        );
        assert_eq!(
            assignments
                .iter()
                .copied()
                .map(|assignment| assignment.morsel_count().expect("morsel assignment"))
                .sum::<usize>(),
            1_024
        );
        assert!(assignments.windows(2).all(|pair| match pair {
            [
                SourceTaskAssignment::ChunkRange { end, .. },
                SourceTaskAssignment::ChunkRange { start, .. },
            ] => end == start,
            _ => false,
        }));

        assert!(SourceWork::Chunks { count: 0 }
            .into_task_assignments(4)
            .is_empty());
    }

    #[test]
    fn shared_queue_sources_spawn_workers_without_fake_morsels() {
        let assignments = SourceWork::SharedWorkers {
            count: 64,
            worker: SharedSourceWorker::HashAggregateEmit,
        }
        .into_task_assignments(4);

        assert_eq!(assignments.len(), 4);
        assert!(assignments.iter().all(|assignment| {
            *assignment == SourceTaskAssignment::SharedWorker(SharedSourceWorker::HashAggregateEmit)
                && assignment.morsel_count().is_none()
        }));

        let rowset_assignments = SourceWork::RowsetScan { count: 19 }.into_task_assignments(4);
        assert_eq!(rowset_assignments.len(), 4);
        assert!(rowset_assignments.iter().all(|assignment| {
            *assignment == SourceTaskAssignment::SharedWorker(SharedSourceWorker::RowsetScan)
                && assignment.morsel_count().is_none()
        }));
    }

    #[test]
    fn worker_coordinator_cancel_queued_releases_pending_slots() {
        let coordinator = PipelineWorkerCoordinator::new(3);
        coordinator.cancel_queued(2);
        assert_eq!(coordinator.remaining(), 1);
        coordinator.finish(Ok(()));
        assert_eq!(coordinator.snapshot().unwrap(), None);
    }

    #[test]
    fn waiter_registry_wakes_registered_work_units_once() {
        let wake = PendingWakeRegistration {
            task_id: PipelineTaskId(7),
            source: WakeSource::Memory,
            token: crate::runtime::WakeToken(11),
            generation: crate::runtime::WakeGeneration(3),
        };
        let unit = WorkUnitId(99);
        let mut registry = WaiterRegistry::default();

        registry.register(wake, unit);
        registry.register(wake, unit);

        assert_eq!(registry.wake(wake.key()), vec![unit]);
        assert!(registry.wake(wake.key()).is_empty());
    }

    #[test]
    fn waiter_registry_moves_unit_when_wake_key_changes() {
        let old_wake = PendingWakeRegistration {
            task_id: PipelineTaskId(7),
            source: WakeSource::Memory,
            token: crate::runtime::WakeToken(11),
            generation: crate::runtime::WakeGeneration(3),
        };
        let new_wake = PendingWakeRegistration {
            task_id: PipelineTaskId(7),
            source: WakeSource::Spill,
            token: crate::runtime::WakeToken(12),
            generation: crate::runtime::WakeGeneration(3),
        };
        let unit = WorkUnitId(99);
        let mut registry = WaiterRegistry::default();

        registry.register(old_wake, unit);
        registry.register(new_wake, unit);

        assert!(registry.wake(old_wake.key()).is_empty());
        assert_eq!(registry.wake(new_wake.key()), vec![unit]);
    }
}
