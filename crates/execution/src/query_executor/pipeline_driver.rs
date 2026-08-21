// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Fetch-driven scheduler for ordinary typed pipeline DAGs.

use std::collections::{BinaryHeap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};

use crate::explain::profiler::{OperatorProfiler, ProfileWorkerContext};
use crate::pipeline::graph::{PipelineGraph, PipelineId, PipelineRoot, SinkSharing};
use crate::pipeline::PipelineProgramSet;
use crate::runtime::{
    BlockReason, Blocker, BreakerHandleRegistry, CleanupReason, OperatorWakeScope,
    PipelineDependencyGates, PipelineRuntime, PipelineSchedulingPolicy, PipelineTaskExecutor,
    PipelineTaskId, PipelineTaskStepContext, QueryRuntimeContext, ReadyEntry, SharedSinkRuntimeSet,
    TaskStepResult, WakeSource,
};
use crate::thread_context::ThreadContext;

use super::cleanup::{
    cleanup_handles, cleanup_reason_for_error, merge_execution_and_cleanup_result,
};

pub(super) fn supports_fetch_driven_pipeline(graph: &PipelineGraph) -> bool {
    !matches!(
        graph.root,
        PipelineRoot::ControlRegion(_) | PipelineRoot::Utility(_)
    ) && graph.control_regions.is_empty()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineDriveResult {
    ChunkReady,
    Blocked(BlockReason),
    Finished,
}

pub struct PipelineExecutionDriver {
    graph: Arc<PipelineGraph>,
    programs: PipelineProgramSet,
    handles: Arc<BreakerHandleRegistry>,
    shared_sinks: SharedSinkRuntimeSet,
    gates: PipelineDependencyGates,
    finished: Vec<bool>,
    finished_count: usize,
    ready: BinaryHeap<ReadyEntry<PipelineId>>,
    ready_seq: u64,
    running: VecDeque<ActivePipelineTask>,
    policy: PipelineSchedulingPolicy,
    allocator: Arc<dyn Allocator>,
}

impl PipelineExecutionDriver {
    pub(super) fn new(
        graph: Arc<PipelineGraph>,
        programs: PipelineProgramSet,
        query: &QueryRuntimeContext,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        ensure_root_is_pipeline(graph.as_ref())?;
        query.cancellation.check()?;

        let handles = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles)?);
        let shared_sinks = SharedSinkRuntimeSet::from_graph(&graph)?;
        let gates = PipelineDependencyGates::from_graph(&graph);
        let pipeline_count = programs.pipeline_count();
        let mut driver = Self {
            graph,
            programs,
            handles,
            shared_sinks,
            gates,
            finished: vec![false; pipeline_count],
            finished_count: 0,
            ready: BinaryHeap::new(),
            ready_seq: 0,
            running: VecDeque::new(),
            policy: PipelineSchedulingPolicy::default(),
            allocator,
        };
        for pipeline in driver.gates.ready_pipelines() {
            driver.push_ready_pipeline(pipeline, 0, query.memory.available_bytes());
        }
        Ok(driver)
    }

    pub fn drive_until_output_or_finished(
        &mut self,
        query: &QueryRuntimeContext,
    ) -> Result<PipelineDriveResult> {
        self.drive(query, true)
    }

    pub(super) fn run_to_completion(&mut self, query: &QueryRuntimeContext) -> Result<()> {
        let result = self.run_to_completion_inner(query);
        let reason = match result.as_ref() {
            Ok(()) => CleanupReason::Finished,
            Err(error) => cleanup_reason_for_error(query, error),
        };
        let cleanup_result = self.cleanup(query, reason);
        merge_execution_and_cleanup_result(result, cleanup_result)
    }

    fn run_to_completion_inner(&mut self, query: &QueryRuntimeContext) -> Result<()> {
        loop {
            match self.drive(query, false)? {
                PipelineDriveResult::Finished => return Ok(()),
                PipelineDriveResult::ChunkReady => {}
                PipelineDriveResult::Blocked(reason) => {
                    return Err(paro_error::not_implemented(format!(
                        "typed pipeline driver cannot make progress while blocked on {:?}",
                        reason
                    )));
                }
            }
        }
    }

    pub fn cleanup(&mut self, query: &QueryRuntimeContext, reason: CleanupReason) -> Result<()> {
        cleanup_handles(self.handles.as_ref(), query, self.allocator.clone(), reason)
    }

    #[cfg(test)]
    pub(super) fn handles_for_test(&self) -> Arc<BreakerHandleRegistry> {
        self.handles.clone()
    }

    fn drive(
        &mut self,
        query: &QueryRuntimeContext,
        stop_on_output: bool,
    ) -> Result<PipelineDriveResult> {
        loop {
            query.cancellation.check()?;
            if stop_on_output && !query.output.is_empty() {
                return Ok(PipelineDriveResult::ChunkReady);
            }
            if self.finished_count == self.finished.len() {
                return Ok(PipelineDriveResult::Finished);
            }
            if self.running.is_empty() {
                self.start_ready_pipelines(query)?;
            }
            let Some(mut active) = self.running.pop_front() else {
                return Err(paro_error::internal(
                    "typed pipeline scheduler has unfinished work but no running task",
                ));
            };

            match active.step(query)? {
                TaskStepResult::Continue => {
                    self.running.push_back(active);
                }
                TaskStepResult::Done => {
                    let pipeline = active.pipeline;
                    self.mark_pipeline_finished(pipeline, query);
                }
                TaskStepResult::Blocked(blocker) => {
                    self.running.push_front(active);
                    if stop_on_output && !query.output.is_empty() {
                        return Ok(PipelineDriveResult::ChunkReady);
                    }
                    return Ok(PipelineDriveResult::Blocked(blocker.reason));
                }
            }
        }
    }

    fn start_ready_pipelines(&mut self, query: &QueryRuntimeContext) -> Result<()> {
        let target = self.target_active_tasks(query);
        while self.running.len() < target {
            let Some(task) = self.pop_next_ready_pipeline(query)? else {
                break;
            };
            self.running.push_back(task);
        }
        if self.running.is_empty() {
            return Err(paro_error::internal(
                "typed pipeline scheduler could not find a ready pipeline",
            ));
        }
        Ok(())
    }

    fn pop_next_ready_pipeline(
        &mut self,
        query: &QueryRuntimeContext,
    ) -> Result<Option<ActivePipelineTask>> {
        loop {
            let Some(entry) = self.ready.pop() else {
                return Ok(None);
            };
            let pipeline_id = entry.payload;
            if self
                .finished
                .get(pipeline_id.index())
                .copied()
                .unwrap_or(false)
            {
                continue;
            }
            if !self.gates.is_ready(pipeline_id) {
                return Err(paro_error::internal(
                    "typed pipeline scheduler dequeued a pipeline before its gates opened",
                ));
            }
            let thread_id = self.running.len();
            return Ok(Some(self.create_pipeline_task(
                pipeline_id,
                query,
                thread_id,
                self.target_active_tasks(query),
            )?));
        }
    }

    fn target_active_tasks(&self, query: &QueryRuntimeContext) -> usize {
        if !query.session.limits.parallel_scheduler {
            return 1;
        }
        query
            .session
            .number_of_threads()
            .max(1)
            .min(self.ready.len().saturating_add(self.running.len()).max(1))
    }

    fn create_pipeline_task(
        &self,
        pipeline: PipelineId,
        query: &QueryRuntimeContext,
        thread_id: usize,
        total_threads: usize,
    ) -> Result<ActivePipelineTask> {
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
        let runtime = Arc::new(PipelineRuntime::with_registry_and_shared_sink(
            program,
            self.handles.clone(),
            query.params.clone(),
            query,
            shared_sink,
        )?);
        ActivePipelineTask::new(
            runtime,
            query,
            self.allocator.clone(),
            thread_id,
            total_threads,
        )
    }

    fn push_ready_pipeline(
        &mut self,
        pipeline: PipelineId,
        dependency_unblocks: u32,
        available_memory: usize,
    ) {
        let priority = self.policy.ready_priority_for_pipeline(
            self.graph.as_ref(),
            pipeline,
            dependency_unblocks,
            self.ready_seq.min(u32::MAX as u64) as u32,
            available_memory,
        );
        self.ready.push(ReadyEntry {
            priority,
            seq: self.ready_seq,
            payload: pipeline,
        });
        self.ready_seq = self.ready_seq.saturating_add(1);
    }

    fn mark_pipeline_finished(&mut self, pipeline: PipelineId, query: &QueryRuntimeContext) {
        if self.finished[pipeline.index()] {
            return;
        }
        self.finished[pipeline.index()] = true;
        self.finished_count += 1;
        self.handles.pipeline_finished(pipeline);
        for event in self.gates.mark_finished(pipeline) {
            if !self.finished[event.pipeline.index()] && self.gates.is_ready(event.pipeline) {
                self.push_ready_pipeline(event.pipeline, 1, query.memory.available_bytes());
            }
        }
    }
}

fn ensure_root_is_pipeline(graph: &PipelineGraph) -> Result<()> {
    if matches!(graph.root, PipelineRoot::Utility(_)) {
        return Err(paro_error::internal(
            "utility roots should be represented as StatementProgram::Utility",
        ));
    }
    if matches!(graph.root, PipelineRoot::ControlRegion(_)) {
        return Err(paro_error::internal(
            "control-region root cannot use the fetch-driven simple pipeline driver",
        ));
    }
    Ok(())
}

impl Drop for PipelineExecutionDriver {
    fn drop(&mut self) {
        let live_handles = self.handles.live_handle_count();
        if live_handles == 0 {
            return;
        }
        tracing::warn!(
            live_handles,
            "dropping typed pipeline driver with live breaker handles"
        );
        debug_assert_eq!(
            live_handles, 0,
            "PipelineExecutionDriver dropped before breaker cleanup"
        );
    }
}

struct ActivePipelineTask {
    pipeline: PipelineId,
    source_node_id: u64,
    executor: PipelineTaskExecutor,
    thread: ThreadContext,
    wake: OperatorWakeScope,
    profiler: OperatorProfiler,
    blocked: Option<Blocker>,
    blocked_at: Option<Instant>,
}

impl ActivePipelineTask {
    fn new(
        runtime: Arc<PipelineRuntime>,
        query: &QueryRuntimeContext,
        allocator: Arc<dyn Allocator>,
        thread_id: usize,
        total_threads: usize,
    ) -> Result<Self> {
        let task = runtime.create_task_state(query, allocator)?;
        let pipeline = runtime.program.id;
        let source_node_id = runtime.program.source.operator_id.index() as u64;
        let task_id = PipelineTaskId(pipeline.index() as u64);
        let profiler =
            query
                .explain_profiler
                .as_ref()
                .map_or_else(OperatorProfiler::disabled, |profiler| {
                    OperatorProfiler::new_with_context(
                        profiler.clone(),
                        ProfileWorkerContext::new(
                            Some(pipeline.index() as u64),
                            Some(pipeline.index() as u64),
                            Some(thread_id as u64),
                            Some(total_threads.max(1) as u64),
                            None,
                        ),
                    )
                });
        Ok(Self {
            pipeline,
            source_node_id,
            executor: PipelineTaskExecutor::new(runtime, task),
            thread: ThreadContext::new(thread_id, total_threads.max(1)),
            wake: OperatorWakeScope {
                task_id,
                generation: query.output.wake_generation(),
            },
            profiler,
            blocked: None,
            blocked_at: None,
        })
    }

    fn step(&mut self, query: &QueryRuntimeContext) -> Result<TaskStepResult> {
        if self.blocked.is_some() {
            if !self.try_resume_blocked(query)? {
                let blocker = self
                    .blocked
                    .as_ref()
                    .expect("blocked task checked above")
                    .clone();
                return Ok(TaskStepResult::Blocked(blocker));
            }
        }

        self.wake.generation = query.output.wake_generation();
        let result = {
            let mut ctx = PipelineTaskStepContext {
                query,
                thread: &self.thread,
                wake: &self.wake,
                profiler: &mut self.profiler,
            };
            self.executor.step(&mut ctx)?
        };
        match &result {
            TaskStepResult::Done => self.profiler.flush(),
            TaskStepResult::Blocked(blocker) => {
                self.profiler.record_blocked(self.source_node_id, blocker);
                self.blocked = Some(blocker.clone());
                self.blocked_at = Some(Instant::now());
            }
            TaskStepResult::Continue => {}
        }
        Ok(result)
    }

    fn try_resume_blocked(&mut self, query: &QueryRuntimeContext) -> Result<bool> {
        let Some(blocker) = self.blocked.as_ref() else {
            return Ok(true);
        };
        let Some(wake) = blocker.wake else {
            return Ok(false);
        };
        let woke = match wake.source {
            WakeSource::OutputBuffer => query.output.wake_generation() != wake.generation,
            WakeSource::Cancellation => query.cancellation.is_cancelled(),
            // These sources need a scheduler-owned waiter registry. Production
            // fetch-driven operators do not park on them today, so keep the
            // task blocked and let the caller fail fast instead of polling.
            WakeSource::Memory
            | WakeSource::Spill
            | WakeSource::ExternalRuntime
            | WakeSource::DerivedIndex => false,
        };
        if !woke {
            return Ok(false);
        }
        self.executor.resume_after_wake()?;
        let blocker = self.blocked.take();
        let wait_time_us = self
            .blocked_at
            .take()
            .map(|blocked_at| blocked_at.elapsed().as_micros().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0);
        self.profiler
            .record_wake(self.source_node_id, blocker.as_ref(), wait_time_us);
        self.wake.generation = query.output.wake_generation();
        Ok(true)
    }
}
