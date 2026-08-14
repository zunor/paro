// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Single-task executor state machine for the role-specific runtime.

use std::collections::VecDeque;
use std::sync::Arc;

use paro_common::error::{self as paro_error, ParoError, Result};

use crate::explain::profiler::OperatorProfiler;
use crate::thread_context::ThreadContext;

use super::context::{
    check_cancelled, Blocker, FinishTaskId, OperatorCallContextCell, OperatorCleanupContext,
    OperatorFinishContext, OperatorScratchScope, OperatorWakeScope, QueryOutputWrite,
    QueryRuntimeContext, RetainedMemorySnapshot,
};
use super::ids::RuntimeOperatorId;
use super::pipeline_runtime::PipelineRuntime;
use super::scratch::{
    ChunkLease, PendingChunkState, PipelineScratch, PipelineTaskState, SinkResumeState,
    TransformResumeState,
};
use super::sink::{
    CancelReason, FinishPoll, FinishTaskGroup, FinishTaskPoll, FinishWork, MergePoll,
    NextFinishTask, PrepareFinishPoll, SinkPoll,
};
use super::source::SourcePoll;
use super::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};
use super::SharedSinkMergeEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineTaskPhase {
    Running,
    RunningTransformOutputMore {
        transform_idx: usize,
    },
    Flushing {
        transform_idx: usize,
        resume_idx: usize,
    },
    Merging,
    Blocked,
    Done,
}

#[derive(Debug)]
pub enum TaskStepResult {
    Continue,
    Done,
    Blocked(Blocker),
}

pub struct PipelineTaskStepContext<'a> {
    pub query: &'a QueryRuntimeContext,
    pub thread: &'a ThreadContext,
    pub wake: &'a OperatorWakeScope,
    pub profiler: &'a mut OperatorProfiler,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PipelineCompletionStage {
    MergeLocal,
    PrepareFinish,
    FinishTransforms { next_idx: usize },
    FinishWork,
    Finish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChunkSlot {
    Source,
    Transform(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SinkContinuation {
    Auto,
    Return,
}

#[derive(Debug)]
pub struct PipelineTaskExecutor {
    pub runtime: Arc<PipelineRuntime>,
    pub phase: PipelineTaskPhase,
    pub task: PipelineTaskState,
    completion_stage: PipelineCompletionStage,
    blocked_resume: Option<PipelineTaskPhase>,
    /// OutputMore continuations suspended behind a downstream transform.
    ///
    /// A single pipeline can contain multiple expanding transforms. When a
    /// downstream transform also returns OutputMore, it must be drained before
    /// resuming the upstream transform that produced its current input.
    output_more_continuations: VecDeque<usize>,
    finish_group: Option<FinishTaskGroup>,
    active_finish_task: Option<super::context::FinishTaskId>,
    finish_tasks_completed: usize,
    defer_shared_producer_merge: bool,
    call_context: OperatorCallContextCell,
}

impl PipelineTaskExecutor {
    pub fn new(runtime: Arc<PipelineRuntime>, task: PipelineTaskState) -> Self {
        let call_context =
            OperatorCallContextCell::new(runtime.program.id, runtime.program.source.operator_id);
        Self {
            runtime,
            phase: PipelineTaskPhase::Running,
            task,
            completion_stage: PipelineCompletionStage::MergeLocal,
            blocked_resume: None,
            output_more_continuations: VecDeque::new(),
            finish_group: None,
            active_finish_task: None,
            finish_tasks_completed: 0,
            defer_shared_producer_merge: false,
            call_context,
        }
    }

    /// Create a scheduler data worker whose local merge does not close a shared-sink producer.
    ///
    /// A shared-sink producer is a pipeline, not an individual morsel worker. The scheduler
    /// signals that producer once all of the pipeline's local sink states have merged.
    pub fn new_parallel_data_task(runtime: Arc<PipelineRuntime>, task: PipelineTaskState) -> Self {
        let mut executor = Self::new(runtime, task);
        executor.defer_shared_producer_merge = true;
        executor
    }

    /// Create an executor that starts at the global finish phase.
    ///
    /// Parallel pipeline scheduling runs N data tasks through source,
    /// transform flush, and local sink merge. Once all local merges have
    /// rendezvoused, a single finish task owns the global seal/finalize path.
    pub(crate) fn new_finish_task(runtime: Arc<PipelineRuntime>, task: PipelineTaskState) -> Self {
        debug_assert!(task.is_finish_only());
        let mut executor = Self::new(runtime, task);
        executor.phase = PipelineTaskPhase::Merging;
        executor.completion_stage = PipelineCompletionStage::PrepareFinish;
        executor
    }

    /// Step a data task until its local sink state has been merged.
    ///
    /// This intentionally stops before `prepare_finish`/`finish_work`; those
    /// stages are owned by the scheduler-created finish work unit.
    pub fn step_until_local_merge(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
    ) -> Result<TaskStepResult> {
        if self.transition_done_after_local_merge() {
            return Ok(TaskStepResult::Done);
        }

        let result = self.step(ctx)?;
        if matches!(result, TaskStepResult::Continue) && self.transition_done_after_local_merge() {
            return Ok(TaskStepResult::Done);
        }
        Ok(result)
    }

    fn transition_done_after_local_merge(&mut self) -> bool {
        if self.phase == PipelineTaskPhase::Merging
            && self.completion_stage != PipelineCompletionStage::MergeLocal
        {
            self.phase = PipelineTaskPhase::Done;
            return true;
        }
        false
    }

    pub fn resume_after_wake(&mut self) -> Result<()> {
        if self.phase != PipelineTaskPhase::Blocked {
            return Err(paro_error::internal("only blocked tasks can be resumed"));
        }
        self.phase = self
            .blocked_resume
            .take()
            .unwrap_or(PipelineTaskPhase::Running);
        Ok(())
    }

    pub fn step(&mut self, ctx: &mut PipelineTaskStepContext<'_>) -> Result<TaskStepResult> {
        if let Err(error) = check_cancelled(&ctx.query.cancellation) {
            return self.finish_step_error(ctx, error);
        }
        match self.step_inner(ctx) {
            Ok(result) => Ok(result),
            Err(error) => self.finish_step_error(ctx, error),
        }
    }

    fn step_inner(&mut self, ctx: &mut PipelineTaskStepContext<'_>) -> Result<TaskStepResult> {
        match self.phase {
            PipelineTaskPhase::Running => self.step_running(ctx),
            PipelineTaskPhase::RunningTransformOutputMore { transform_idx } => {
                self.step_running_transform_output_more(ctx, transform_idx)
            }
            PipelineTaskPhase::Flushing { .. } => self.step_flushing(ctx),
            PipelineTaskPhase::Merging => self.step_merging(ctx),
            PipelineTaskPhase::Blocked => Err(paro_error::internal(
                "blocked pipeline task must be resumed by scheduler wake",
            )),
            PipelineTaskPhase::Done => Ok(TaskStepResult::Done),
        }
    }
}

mod finish;
mod helpers;
mod parallel_finish;
mod running;

#[cfg(test)]
mod tests;
