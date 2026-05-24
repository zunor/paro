// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Single-task executor state machine for the role-specific runtime.

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
    finish_group: Option<FinishTaskGroup>,
    active_finish_task: Option<super::context::FinishTaskId>,
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
            finish_group: None,
            active_finish_task: None,
            call_context,
        }
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
mod running;

#[cfg(test)]
mod tests;
