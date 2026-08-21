// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::allocator::MemoryTag;
use paro_function::scalar::FunctionExecContext;

use crate::explain::profiler::{OperatorProfilePhase, ProfileMorselRange};

use super::helpers::finish_context;
use super::parallel_finish::run_parallel_finish_tasks;
use super::*;

impl PipelineTaskExecutor {
    pub(crate) fn step_flushing(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
    ) -> Result<TaskStepResult> {
        let PipelineTaskPhase::Flushing {
            transform_idx,
            resume_idx,
        } = self.phase
        else {
            unreachable!("phase checked by caller");
        };
        if transform_idx >= self.runtime.program.transforms.len() {
            self.phase = PipelineTaskPhase::Merging;
            return Ok(TaskStepResult::Continue);
        }

        let transform_operator = self.runtime.program.transforms[transform_idx].operator_id;
        let transform_node_id = transform_operator.index() as u64;
        ctx.profiler.start_operator(transform_node_id);
        let (poll, output_rows) = {
            let (task, memory) = self.task.data_and_memory_mut()?;
            let memory = memory.call_scope();
            let expression = &mut task.scratch.expression;
            let transform_chunks = &mut task.scratch.transform_chunks;
            let scratch = OperatorScratchScope::from_expression(expression);
            let transform = &self.runtime.program.transforms[transform_idx];
            let global = self
                .runtime
                .transform_globals
                .get(transform_idx)
                .expect("transform global slot must exist during flush");
            let mut call_ctx = self.call_context.context(
                ctx.query,
                transform_operator,
                ctx.thread,
                memory,
                scratch,
                ctx.wake,
                &mut *ctx.profiler,
            );
            let output = transform_chunks
                .get_mut(transform_idx)
                .expect("transform output scratch must exist during flush");
            let poll = transform.exec.flush(
                &mut call_ctx,
                global,
                &mut task.transforms[transform_idx],
                output,
            );
            let output_rows = if matches!(
                poll,
                Ok(TransformFlushPoll::Output | TransformFlushPoll::OutputMore)
            ) {
                output.size() as u64
            } else {
                0
            };
            (poll, output_rows)
        };
        if output_rows == 0 {
            ctx.profiler.cancel_operator(transform_node_id);
        } else {
            ctx.profiler.end_operator(transform_node_id, output_rows);
        }
        let poll = poll?;

        match poll {
            TransformFlushPoll::Done => {
                self.phase = PipelineTaskPhase::Flushing {
                    transform_idx: transform_idx + 1,
                    resume_idx: 0,
                };
                Ok(TaskStepResult::Continue)
            }
            TransformFlushPoll::Output => {
                self.push_flush_output(ctx, transform_idx, resume_idx, false)
            }
            TransformFlushPoll::OutputMore => {
                self.push_flush_output(ctx, transform_idx, resume_idx, true)
            }
            TransformFlushPoll::Pending(blocker) => Ok(self.block(
                PipelineTaskPhase::Flushing {
                    transform_idx,
                    resume_idx,
                },
                blocker,
            )),
        }
    }

    pub(crate) fn push_flush_output(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        transform_idx: usize,
        resume_idx: usize,
        output_more: bool,
    ) -> Result<TaskStepResult> {
        let resume = if output_more {
            TransformResumeState::FlushOutputMore
        } else {
            TransformResumeState::FlushNext
        };
        let result = self.push_transform_output_downstream(ctx, transform_idx, resume)?;
        if !matches!(result, TaskStepResult::Continue) {
            return Ok(result);
        }
        self.phase = PipelineTaskPhase::Flushing {
            transform_idx: if output_more {
                transform_idx
            } else {
                transform_idx + 1
            },
            resume_idx: resume_idx + usize::from(output_more),
        };
        Ok(TaskStepResult::Continue)
    }

    pub(crate) fn step_merging(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
    ) -> Result<TaskStepResult> {
        if matches!(
            self.task.pending,
            PendingChunkState::CompletionResult { .. }
        ) {
            return self.resume_completion_result(ctx);
        }

        match self.completion_stage {
            PipelineCompletionStage::MergeLocal => self.merge_local(ctx),
            PipelineCompletionStage::PrepareFinish => self.prepare_finish(ctx),
            PipelineCompletionStage::FinishTransforms { next_idx } => {
                self.finish_transform(ctx, next_idx)
            }
            PipelineCompletionStage::FinishWork => self.finish_work(ctx),
            PipelineCompletionStage::Finish => self.finish(ctx),
        }
    }

    pub(crate) fn merge_local(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
    ) -> Result<TaskStepResult> {
        let sink_operator = self.runtime.program.sink.operator_id;
        let sink_node_id = sink_operator.index() as u64;
        ctx.profiler.start_operator(sink_node_id);
        let poll = {
            let (task, memory) = self.task.data_and_memory_mut()?;
            let memory = memory.call_scope();
            let scratch = OperatorScratchScope::from_expression(&mut task.scratch.expression);
            let mut call_ctx = self.call_context.context(
                ctx.query,
                sink_operator,
                ctx.thread,
                memory,
                scratch,
                ctx.wake,
                &mut *ctx.profiler,
            );
            self.runtime.program.sink.exec.merge_local(
                &mut call_ctx,
                &self.runtime.sink_global,
                &mut task.sink,
            )
        };
        ctx.profiler.cancel_operator(sink_node_id);
        let poll = poll?;

        match poll {
            MergePoll::Done => self.after_merge_local_done(),
            MergePoll::Pending(blocker) => Ok(self.block(PipelineTaskPhase::Merging, blocker)),
        }
    }

    pub(crate) fn after_merge_local_done(&mut self) -> Result<TaskStepResult> {
        let Some(shared) = self.runtime.shared_sink.as_ref() else {
            self.completion_stage = PipelineCompletionStage::PrepareFinish;
            return Ok(TaskStepResult::Continue);
        };
        if self.defer_shared_producer_merge {
            self.completion_stage = PipelineCompletionStage::PrepareFinish;
            return Ok(TaskStepResult::Continue);
        }

        match shared.mark_producer_merged()? {
            SharedSinkMergeEvent::WaitingForProducers { .. } => {
                self.phase = PipelineTaskPhase::Done;
                Ok(TaskStepResult::Done)
            }
            SharedSinkMergeEvent::ReadyToFinish => {
                if shared.try_begin_finish()? {
                    self.completion_stage = PipelineCompletionStage::PrepareFinish;
                    Ok(TaskStepResult::Continue)
                } else {
                    self.phase = PipelineTaskPhase::Done;
                    Ok(TaskStepResult::Done)
                }
            }
        }
    }

    pub(crate) fn prepare_finish(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
    ) -> Result<TaskStepResult> {
        let sink_operator = self.runtime.program.sink.operator_id;
        let sink_node_id = sink_operator.index() as u64;
        let phase_timer = ctx.profiler.start_phase();
        let poll = {
            let mut finish_ctx = finish_context(
                ctx,
                self.runtime.program.id,
                sink_operator,
                None,
                &self.task,
            );
            self.runtime
                .program
                .sink
                .exec
                .prepare_finish(&mut finish_ctx, &self.runtime.sink_global)
        };
        if poll.is_ok() {
            ctx.profiler.end_phase(
                sink_node_id,
                OperatorProfilePhase::BreakerPrepareFinish,
                phase_timer,
                0,
                None,
            );
        }
        let poll = poll?;

        match poll {
            PrepareFinishPoll::Done => {
                self.completion_stage = PipelineCompletionStage::FinishTransforms { next_idx: 0 };
                Ok(TaskStepResult::Continue)
            }
            PrepareFinishPoll::Pending(blocker) => {
                Ok(self.block(PipelineTaskPhase::Merging, blocker))
            }
        }
    }

    pub(crate) fn finish_transform(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        next_idx: usize,
    ) -> Result<TaskStepResult> {
        if next_idx >= self.runtime.program.transforms.len() {
            self.completion_stage = PipelineCompletionStage::FinishWork;
            return Ok(TaskStepResult::Continue);
        }

        let transform_operator = self.runtime.program.transforms[next_idx].operator_id;
        let transform_node_id = transform_operator.index() as u64;
        ctx.profiler.start_operator(transform_node_id);
        let poll = {
            let transform = &self.runtime.program.transforms[next_idx];
            let global = self
                .runtime
                .transform_globals
                .get(next_idx)
                .expect("transform global slot must exist during finish");
            let mut finish_ctx = finish_context(
                ctx,
                self.runtime.program.id,
                transform_operator,
                None,
                &self.task,
            );
            transform.exec.finish_global(&mut finish_ctx, global)
        };
        ctx.profiler.cancel_operator(transform_node_id);
        let poll = poll?;

        match poll {
            TransformFinishPoll::Done => {
                self.completion_stage = PipelineCompletionStage::FinishTransforms {
                    next_idx: next_idx + 1,
                };
                Ok(TaskStepResult::Continue)
            }
            TransformFinishPoll::Pending(blocker) => {
                Ok(self.block(PipelineTaskPhase::Merging, blocker))
            }
        }
    }

    pub(crate) fn finish_work(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
    ) -> Result<TaskStepResult> {
        if self.finish_group.is_some() {
            return self.drive_parallel_finish(ctx);
        }

        let sink_operator = self.runtime.program.sink.operator_id;
        let sink_node_id = sink_operator.index() as u64;
        let phase_timer = ctx.profiler.start_phase();
        let work = {
            let mut finish_ctx = finish_context(
                ctx,
                self.runtime.program.id,
                sink_operator,
                None,
                &self.task,
            );
            self.runtime
                .program
                .sink
                .exec
                .finish_work(&mut finish_ctx, &self.runtime.sink_global)
        };
        if work.is_ok() {
            ctx.profiler.end_phase(
                sink_node_id,
                OperatorProfilePhase::BreakerFinishWork,
                phase_timer,
                0,
                None,
            );
        }
        let work = work?;

        match work {
            FinishWork::None => {
                self.completion_stage = PipelineCompletionStage::Finish;
                Ok(TaskStepResult::Continue)
            }
            FinishWork::Parallel(group) => {
                if group.task_count == 0 {
                    return Err(paro_common::error::internal(
                        "finish group must declare at least one task",
                    ));
                }
                self.finish_tasks_completed = 0;
                self.finish_group = Some(group);
                self.drive_parallel_finish(ctx)
            }
        }
    }

    pub(crate) fn drive_parallel_finish(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
    ) -> Result<TaskStepResult> {
        let group = self
            .finish_group
            .take()
            .expect("finish group must exist while driving parallel finish");

        if group.task_count > 1 && self.active_finish_task.is_none() {
            let result = self.drive_parallel_finish_group(ctx, &group);
            if matches!(result, Ok(TaskStepResult::Blocked(_))) {
                self.finish_group = Some(group);
            }
            return result;
        }

        if let Some(task_id) = self.active_finish_task {
            let result = self.run_finish_task(ctx, &group, task_id);
            if result.is_ok() && self.completion_stage == PipelineCompletionStage::FinishWork {
                self.finish_group = Some(group);
            }
            return result;
        }

        let next = {
            let mut finish_ctx = finish_context(
                ctx,
                self.runtime.program.id,
                self.runtime.program.sink.operator_id,
                None,
                &self.task,
            );
            match group.driver.next_task(&mut finish_ctx) {
                Ok(next) => next,
                Err(error) => {
                    self.cancel_finish_group(
                        ctx,
                        &group,
                        Self::cancel_reason_for_error(ctx.query, &error),
                    );
                    return Err(error);
                }
            }
        };

        match next {
            NextFinishTask::Task(task_id) => {
                self.active_finish_task = Some(task_id);
                let result = self.run_finish_task(ctx, &group, task_id);
                if result.is_ok() && self.completion_stage == PipelineCompletionStage::FinishWork {
                    self.finish_group = Some(group);
                }
                result
            }
            NextFinishTask::Drained => {
                if self.finish_tasks_completed != group.task_count {
                    return Err(paro_common::error::internal(format!(
                        "finish group task count mismatch: declared={}, completed={}",
                        group.task_count, self.finish_tasks_completed
                    )));
                }
                self.finish_parallel_group(ctx, &group)?;
                self.completion_stage = PipelineCompletionStage::Finish;
                Ok(TaskStepResult::Continue)
            }
            NextFinishTask::Pending(blocker) => {
                self.finish_group = Some(group);
                Ok(self.block(PipelineTaskPhase::Merging, blocker))
            }
        }
    }

    pub(crate) fn run_finish_task(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        group: &FinishTaskGroup,
        task_id: FinishTaskId,
    ) -> Result<TaskStepResult> {
        let sink_node_id = self.runtime.program.sink.operator_id.index() as u64;
        let phase_timer = ctx.profiler.start_phase();
        let task_result = {
            let mut finish_ctx = finish_context(
                ctx,
                self.runtime.program.id,
                self.runtime.program.sink.operator_id,
                Some(task_id),
                &self.task,
            );
            group.driver.run_task(task_id, &mut finish_ctx)
        };
        if task_result.is_ok() {
            ctx.profiler.end_phase(
                sink_node_id,
                OperatorProfilePhase::BreakerFinishTask,
                phase_timer,
                0,
                Some(ProfileMorselRange::new(
                    "finish_task",
                    u64::from(task_id.0),
                    u64::from(task_id.0) + 1,
                )),
            );
        }
        let poll = match task_result {
            Ok(poll) => poll,
            Err(error) => {
                self.cancel_finish_group(
                    ctx,
                    group,
                    Self::cancel_reason_for_error(ctx.query, &error),
                );
                return Err(error);
            }
        };

        match poll {
            FinishTaskPoll::Done => {
                self.active_finish_task = None;
                self.finish_tasks_completed = self
                    .finish_tasks_completed
                    .checked_add(1)
                    .ok_or_else(|| paro_common::error::internal("finish task count overflow"))?;
                Ok(TaskStepResult::Continue)
            }
            FinishTaskPoll::Pending(blocker) => Ok(self.block(PipelineTaskPhase::Merging, blocker)),
        }
    }

    fn drive_parallel_finish_group(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        group: &FinishTaskGroup,
    ) -> Result<TaskStepResult> {
        let mut task_ids = Vec::with_capacity(group.task_count);
        loop {
            let next = {
                let mut finish_ctx = finish_context(
                    ctx,
                    self.runtime.program.id,
                    self.runtime.program.sink.operator_id,
                    None,
                    &self.task,
                );
                match group.driver.next_task(&mut finish_ctx) {
                    Ok(next) => next,
                    Err(error) => {
                        self.cancel_finish_group(
                            ctx,
                            group,
                            Self::cancel_reason_for_error(ctx.query, &error),
                        );
                        return Err(error);
                    }
                }
            };
            match next {
                NextFinishTask::Task(task_id) => task_ids.push(task_id),
                NextFinishTask::Drained => break,
                NextFinishTask::Pending(blocker) => {
                    return Ok(self.block(PipelineTaskPhase::Merging, blocker));
                }
            }
        }

        if task_ids.is_empty() {
            return Err(paro_common::error::internal(
                "parallel finish group issued no tasks",
            ));
        }
        if task_ids.len() != group.task_count {
            return Err(paro_common::error::internal(format!(
                "parallel finish group task count mismatch: declared={}, issued={}",
                group.task_count,
                task_ids.len()
            )));
        }

        let result = run_parallel_finish_tasks(
            self.runtime.clone(),
            ctx.query,
            group.clone(),
            task_ids,
            ctx.query.allocator(MemoryTag::Allocator),
            ctx.profiler,
            self.runtime.program.sink.operator_id.index() as u64,
        );
        if let Err(error) = result {
            self.cancel_finish_group(ctx, group, Self::cancel_reason_for_error(ctx.query, &error));
            return Err(error);
        }
        self.finish_parallel_group(ctx, group)?;
        self.completion_stage = PipelineCompletionStage::Finish;
        Ok(TaskStepResult::Continue)
    }

    fn finish_parallel_group(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        group: &FinishTaskGroup,
    ) -> Result<()> {
        let sink_node_id = self.runtime.program.sink.operator_id.index() as u64;
        let phase_timer = ctx.profiler.start_phase();
        let mut finish_ctx = finish_context(
            ctx,
            self.runtime.program.id,
            self.runtime.program.sink.operator_id,
            None,
            &self.task,
        );
        let result = group.driver.finish_group(&mut finish_ctx);
        if result.is_ok() {
            ctx.profiler.end_phase(
                sink_node_id,
                OperatorProfilePhase::BreakerFinishGroup,
                phase_timer,
                0,
                None,
            );
        }
        if let Err(error) = result {
            self.cancel_finish_group(ctx, group, Self::cancel_reason_for_error(ctx.query, &error));
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn cancel_finish_group(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        group: &FinishTaskGroup,
        reason: CancelReason,
    ) {
        let memory = self.task.memory.call_scope();
        let mut cleanup_ctx = OperatorCleanupContext {
            query: ctx.query,
            pipeline: Some(self.runtime.program.id),
            operator: Some(self.runtime.program.sink.operator_id),
            thread: ctx.thread,
            memory,
            cancel: &ctx.query.cancellation,
            profiler: &mut *ctx.profiler,
        };
        if let Err(error) = group.driver.cancel_group(&mut cleanup_ctx, reason) {
            ctx.query.errors.record_secondary(error);
        }
    }

    pub(crate) fn finish(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
    ) -> Result<TaskStepResult> {
        let sink_operator = self.runtime.program.sink.operator_id;
        let sink_node_id = sink_operator.index() as u64;
        let phase_timer = ctx.profiler.start_phase();
        ctx.profiler.start_operator(sink_node_id);
        let poll = {
            let mut finish_ctx = finish_context(
                ctx,
                self.runtime.program.id,
                sink_operator,
                None,
                &self.task,
            );
            self.runtime
                .program
                .sink
                .exec
                .finish(&mut finish_ctx, &self.runtime.sink_global)
        };
        let output_rows = match &poll {
            Ok(FinishPoll::DoneWithResult(chunk)) => chunk.size() as u64,
            _ => 0,
        };
        if poll.is_ok() {
            ctx.profiler.end_phase(
                sink_node_id,
                OperatorProfilePhase::BreakerFinish,
                phase_timer,
                output_rows,
                None,
            );
        }
        if output_rows == 0 {
            ctx.profiler.cancel_operator(sink_node_id);
        } else {
            ctx.profiler.end_operator(sink_node_id, output_rows);
        }
        let poll = poll?;

        match poll {
            FinishPoll::Done => {
                if let Some(shared) = self.runtime.shared_sink.as_ref() {
                    shared.mark_finished()?;
                }
                self.phase = PipelineTaskPhase::Done;
                Ok(TaskStepResult::Done)
            }
            FinishPoll::DoneWithResult(chunk) => self.write_completion_result(ctx, chunk),
            FinishPoll::Pending(blocker) => Ok(self.block(PipelineTaskPhase::Merging, blocker)),
        }
    }

    pub(crate) fn write_completion_result(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        chunk: paro_common::chunk::Chunk,
    ) -> Result<TaskStepResult> {
        match ctx.query.output.try_push(chunk) {
            QueryOutputWrite::Written => {
                self.phase = PipelineTaskPhase::Done;
                Ok(TaskStepResult::Done)
            }
            QueryOutputWrite::Blocked(chunk) => {
                self.task.pending = PendingChunkState::CompletionResult {
                    chunk: ChunkLease {
                        chunk,
                        memory: RetainedMemorySnapshot::default(),
                    },
                };
                Ok(self.block(
                    PipelineTaskPhase::Merging,
                    Blocker::output_backpressure(ctx.wake, &ctx.query.output),
                ))
            }
        }
    }

    pub(crate) fn resume_completion_result(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
    ) -> Result<TaskStepResult> {
        let pending = std::mem::take(&mut self.task.pending);
        let PendingChunkState::CompletionResult { chunk } = pending else {
            return Err(paro_error::internal(
                "expected completion result pending chunk",
            ));
        };
        let retained = chunk.memory;
        match ctx.query.output.try_push(chunk.chunk) {
            QueryOutputWrite::Written => {
                self.phase = PipelineTaskPhase::Done;
                Ok(TaskStepResult::Done)
            }
            QueryOutputWrite::Blocked(chunk) => {
                self.task.pending = PendingChunkState::CompletionResult {
                    chunk: ChunkLease {
                        chunk,
                        memory: retained,
                    },
                };
                Ok(self.block(
                    PipelineTaskPhase::Merging,
                    Blocker::output_backpressure(ctx.wake, &ctx.query.output),
                ))
            }
        }
    }
}
