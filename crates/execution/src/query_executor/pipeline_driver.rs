// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Fetch-driven scheduler for ordinary typed pipeline DAGs.

use std::collections::VecDeque;
use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};

use crate::explain::profiler::OperatorProfiler;
use crate::pipeline::graph::{PipelineGraph, PipelineId, PipelineRoot, SinkSharing};
use crate::pipeline::PipelineProgramSet;
use crate::runtime::{
    BlockReason, Blocker, BreakerHandleRegistry, CleanupReason, OperatorWakeScope,
    PipelineDependencyGates, PipelineRuntime, PipelineTaskExecutor, PipelineTaskId,
    PipelineTaskStepContext, QueryRuntimeContext, SharedSinkRuntimeSet, TaskStepResult, WakeSource,
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
    ready: VecDeque<PipelineId>,
    active: Option<ActivePipelineTask>,
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
        let ready = gates.ready_pipelines().into_iter().collect::<VecDeque<_>>();
        let pipeline_count = programs.pipeline_count();
        Ok(Self {
            graph,
            programs,
            handles,
            shared_sinks,
            gates,
            finished: vec![false; pipeline_count],
            finished_count: 0,
            ready,
            active: None,
            allocator,
        })
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
            if self.active.is_none() {
                self.start_next_ready_pipeline(query)?;
            }
            let Some(active) = self.active.as_mut() else {
                return Err(paro_error::internal(
                    "typed pipeline scheduler has unfinished work but no active task",
                ));
            };

            match active.step(query)? {
                TaskStepResult::Continue => {}
                TaskStepResult::Done => {
                    let pipeline = active.pipeline;
                    self.active = None;
                    self.mark_pipeline_finished(pipeline);
                }
                TaskStepResult::Blocked(blocker) => {
                    if stop_on_output && !query.output.is_empty() {
                        return Ok(PipelineDriveResult::ChunkReady);
                    }
                    return Ok(PipelineDriveResult::Blocked(blocker.reason));
                }
            }
        }
    }

    fn start_next_ready_pipeline(&mut self, query: &QueryRuntimeContext) -> Result<()> {
        loop {
            let Some(pipeline_id) = self.ready.pop_front() else {
                return Err(paro_error::internal(
                    "typed pipeline scheduler could not find a ready pipeline",
                ));
            };
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
            self.active = Some(self.create_pipeline_task(pipeline_id, query)?);
            return Ok(());
        }
    }

    fn create_pipeline_task(
        &self,
        pipeline: PipelineId,
        query: &QueryRuntimeContext,
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
        ActivePipelineTask::new(runtime, query, self.allocator.clone())
    }

    fn mark_pipeline_finished(&mut self, pipeline: PipelineId) {
        if self.finished[pipeline.index()] {
            return;
        }
        self.finished[pipeline.index()] = true;
        self.finished_count += 1;
        for event in self.gates.mark_finished(pipeline) {
            if !self.finished[event.pipeline.index()] && self.gates.is_ready(event.pipeline) {
                self.ready.push_back(event.pipeline);
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
    executor: PipelineTaskExecutor,
    thread: ThreadContext,
    wake: OperatorWakeScope,
    profiler: OperatorProfiler,
    blocked: Option<Blocker>,
}

impl ActivePipelineTask {
    fn new(
        runtime: Arc<PipelineRuntime>,
        query: &QueryRuntimeContext,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let task = runtime.create_task_state(query, allocator)?;
        let pipeline = runtime.program.id;
        let task_id = PipelineTaskId(pipeline.index() as u64);
        let profiler = query
            .explain_profiler
            .as_ref()
            .map_or_else(OperatorProfiler::disabled, |profiler| {
                OperatorProfiler::new(profiler.clone())
            });
        Ok(Self {
            pipeline,
            executor: PipelineTaskExecutor::new(runtime, task),
            thread: ThreadContext::single_threaded(),
            wake: OperatorWakeScope {
                task_id,
                generation: query.output.wake_generation(),
            },
            profiler,
            blocked: None,
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
                self.blocked = Some(blocker.clone());
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
        self.blocked = None;
        self.wake.generation = query.output.wake_generation();
        Ok(true)
    }
}
