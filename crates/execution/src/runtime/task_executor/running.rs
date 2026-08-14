// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::helpers::{chunk_slot_mut, chunk_slot_mut_from_parts, transform_input_output_chunks};
use super::*;

impl PipelineTaskExecutor {
    pub(crate) fn step_running(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
    ) -> Result<TaskStepResult> {
        if !self.task.pending.is_empty() {
            return self.resume_running_pending(ctx);
        }

        let source_operator = self.runtime.program.source.operator_id;
        let source_node_id = source_operator.index() as u64;
        ctx.profiler.start_operator(source_node_id);
        let (poll, output_rows) = {
            let (task, memory) = self.task.data_and_memory_mut();
            let memory = memory.call_scope();
            let scratch = OperatorScratchScope::from_expression(&mut task.scratch.expression);
            let mut call_ctx = self.call_context.context(
                ctx.query,
                source_operator,
                ctx.thread,
                memory,
                scratch,
                ctx.wake,
                &mut *ctx.profiler,
            );
            let poll = self.runtime.program.source.exec.poll_next(
                &mut call_ctx,
                &self.runtime.source_global,
                &mut task.source,
                &mut task.scratch.source_chunk,
            );
            let output_rows = if matches!(poll, Ok(SourcePoll::Output)) {
                task.scratch.source_chunk.size() as u64
            } else {
                0
            };
            (poll, output_rows)
        };
        ctx.profiler.end_operator(source_node_id, output_rows);
        let poll = poll?;

        match poll {
            SourcePoll::Output => self.push_source_output(ctx),
            SourcePoll::Finished => {
                self.phase = PipelineTaskPhase::Flushing {
                    transform_idx: 0,
                    resume_idx: 0,
                };
                Ok(TaskStepResult::Continue)
            }
            SourcePoll::Pending(blocker) => Ok(self.block(PipelineTaskPhase::Running, blocker)),
        }
    }

    pub(crate) fn push_source_output(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
    ) -> Result<TaskStepResult> {
        // Chunk flow:
        //
        //   source -> transform[0] -> ... -> transform[n] -> sink
        //              ^ OutputMore re-enters the same transform after its
        //                current output reaches the sink.
        //
        // Pending owns the currently live scratch slot through ChunkLease, then
        // restores that slot before resuming at the saved transform/sink edge.
        if self.runtime.program.transforms.is_empty() {
            return self.consume_sink_from_slot(
                ctx,
                ChunkSlot::Source,
                SinkResumeState::FromStart,
                SinkContinuation::Auto,
            );
        }
        self.run_transform(ctx, 0, ChunkSlot::Source)
    }

    pub(crate) fn resume_running_pending(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
    ) -> Result<TaskStepResult> {
        match std::mem::take(&mut self.task.pending) {
            PendingChunkState::SourceOutput { chunk } => {
                chunk.restore_into(&mut self.task.data_mut().scratch.source_chunk);
                self.push_source_output(ctx)
            }
            PendingChunkState::TransformOutput {
                transform_idx,
                resume,
                chunk,
            } => {
                let scratch = self
                    .task
                    .data_mut()
                    .scratch
                    .transform_chunk_mut(transform_idx)
                    .ok_or_else(|| {
                        paro_error::internal("pending transform output slot is out of bounds")
                    })?;
                chunk.restore_into(scratch);
                self.resume_transform_output(ctx, transform_idx, resume)
            }
            PendingChunkState::SinkInput { resume, chunk } => {
                let slot = self.final_output_slot();
                self.restore_lease_to_slot(slot, chunk)?;
                self.consume_sink_from_slot(ctx, slot, resume, SinkContinuation::Auto)
            }
            PendingChunkState::CompletionResult { chunk } => {
                self.task.pending = PendingChunkState::CompletionResult { chunk };
                Err(paro_error::internal(
                    "completion result pending chunk cannot resume while task is running",
                ))
            }
            PendingChunkState::Empty => Ok(TaskStepResult::Continue),
        }
    }

    pub(crate) fn run_transform(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        transform_idx: usize,
        input_slot: ChunkSlot,
    ) -> Result<TaskStepResult> {
        let transform_operator = self.runtime.program.transforms[transform_idx].operator_id;
        let transform_node_id = transform_operator.index() as u64;
        ctx.profiler.start_operator(transform_node_id);
        let (poll, output_rows) = {
            let (task, memory) = self.task.data_and_memory_mut();
            let memory = memory.call_scope();
            let scratch_state = &mut task.scratch;
            let scratch = OperatorScratchScope::from_expression(&mut scratch_state.expression);
            let transform = &self.runtime.program.transforms[transform_idx];
            let global = self
                .runtime
                .transform_globals
                .get(transform_idx)
                .expect("transform global slot must exist during running");
            let mut call_ctx = self.call_context.context(
                ctx.query,
                transform_operator,
                ctx.thread,
                memory,
                scratch,
                ctx.wake,
                &mut *ctx.profiler,
            );
            let (input, output) = transform_input_output_chunks(
                &scratch_state.source_chunk,
                &mut scratch_state.transform_chunks,
                input_slot,
                transform_idx,
            )?;
            let poll = transform.exec.transform(
                &mut call_ctx,
                global,
                &mut task.transforms[transform_idx],
                input,
                output,
            );
            let output_rows =
                if matches!(poll, Ok(TransformPoll::Output | TransformPoll::OutputMore)) {
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
            TransformPoll::NeedMoreInput => Ok(TaskStepResult::Continue),
            TransformPoll::Output => self.push_transform_output_from_transform(
                ctx,
                transform_idx,
                TransformResumeState::FromStart,
            ),
            TransformPoll::OutputMore => self.push_transform_output_from_transform(
                ctx,
                transform_idx,
                TransformResumeState::OutputMore,
            ),
            TransformPoll::StopPipeline => {
                self.phase = PipelineTaskPhase::Flushing {
                    transform_idx: transform_idx + 1,
                    resume_idx: 0,
                };
                Ok(TaskStepResult::Continue)
            }
            TransformPoll::Pending(blocker) => {
                self.retain_input_for_transform_pending(input_slot, blocker.retained_memory)?;
                Ok(self.block(PipelineTaskPhase::Running, blocker))
            }
        }
    }

    pub(crate) fn push_transform_output_from_transform(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        transform_idx: usize,
        resume: TransformResumeState,
    ) -> Result<TaskStepResult> {
        let continuations_before = self.output_more_continuations.len();
        let result = self.push_transform_output_downstream(ctx, transform_idx, resume)?;
        // A synchronous non-expanding downstream transform must not resume an
        // older ancestor: its caller still owns that decision. Once a call
        // suspends, `resume_transform_output` becomes the top-level owner and
        // handles both FromStart and OutputMore resumes.
        if matches!(resume, TransformResumeState::OutputMore) {
            self.schedule_transform_resume_after_downstream(
                transform_idx,
                resume,
                continuations_before,
                &result,
            );
        }
        Ok(result)
    }

    pub(crate) fn resume_transform_output(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        transform_idx: usize,
        resume: TransformResumeState,
    ) -> Result<TaskStepResult> {
        let continuations_before = self.output_more_continuations.len();
        let result = self.push_transform_output_downstream(ctx, transform_idx, resume)?;
        if !matches!(result, TaskStepResult::Continue) {
            return Ok(result);
        }
        match resume {
            TransformResumeState::FromStart | TransformResumeState::OutputMore => {
                self.schedule_transform_resume_after_downstream(
                    transform_idx,
                    resume,
                    continuations_before,
                    &result,
                );
            }
            TransformResumeState::FlushNext => {
                self.phase = PipelineTaskPhase::Flushing {
                    transform_idx: transform_idx + 1,
                    resume_idx: 0,
                };
            }
            TransformResumeState::FlushOutputMore => {
                self.phase = PipelineTaskPhase::Flushing {
                    transform_idx,
                    resume_idx: 0,
                };
            }
        }
        Ok(TaskStepResult::Continue)
    }

    fn schedule_transform_resume_after_downstream(
        &mut self,
        transform_idx: usize,
        resume: TransformResumeState,
        continuations_before: usize,
        result: &TaskStepResult,
    ) {
        if !matches!(result, TaskStepResult::Continue) {
            return;
        }
        if matches!(resume, TransformResumeState::OutputMore) {
            if matches!(self.phase, PipelineTaskPhase::Running) {
                self.phase = PipelineTaskPhase::RunningTransformOutputMore { transform_idx };
            } else {
                // Downstream installed its own continuation. Insert this one
                // after continuations created by that downstream call, but
                // before any older upstream ancestors.
                let descendants = self
                    .output_more_continuations
                    .len()
                    .saturating_sub(continuations_before);
                self.output_more_continuations
                    .insert(descendants, transform_idx);
            }
        } else if matches!(self.phase, PipelineTaskPhase::Running) {
            self.schedule_next_output_more_continuation();
        }
    }

    fn schedule_next_output_more_continuation(&mut self) {
        if let Some(transform_idx) = self.output_more_continuations.pop_front() {
            self.phase = PipelineTaskPhase::RunningTransformOutputMore { transform_idx };
        }
    }

    pub(crate) fn push_transform_output_downstream(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        transform_idx: usize,
        resume: TransformResumeState,
    ) -> Result<TaskStepResult> {
        let next_idx = transform_idx + 1;
        if next_idx < self.runtime.program.transforms.len() {
            let result = self.run_transform(ctx, next_idx, ChunkSlot::Transform(transform_idx))?;
            if matches!(result, TaskStepResult::Blocked(_)) {
                if let PendingChunkState::TransformOutput {
                    transform_idx: pending_idx,
                    resume: pending_resume,
                    ..
                } = &mut self.task.pending
                {
                    if *pending_idx == transform_idx {
                        *pending_resume = resume;
                    }
                }
            }
            return Ok(result);
        }

        let sink_resume = match resume {
            TransformResumeState::OutputMore => {
                SinkResumeState::AfterTransformOutputMore { transform_idx }
            }
            TransformResumeState::FlushNext => SinkResumeState::AfterFlushOutput {
                transform_idx,
                output_more: false,
            },
            TransformResumeState::FlushOutputMore => SinkResumeState::AfterFlushOutput {
                transform_idx,
                output_more: true,
            },
            TransformResumeState::FromStart => SinkResumeState::FromStart,
        };
        self.consume_sink_from_slot(
            ctx,
            ChunkSlot::Transform(transform_idx),
            sink_resume,
            SinkContinuation::Return,
        )
    }

    pub(crate) fn continue_transform_output_more(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        transform_idx: usize,
    ) -> Result<TaskStepResult> {
        let input_slot = if transform_idx == 0 {
            ChunkSlot::Source
        } else {
            ChunkSlot::Transform(transform_idx - 1)
        };
        self.run_transform(ctx, transform_idx, input_slot)
    }

    pub(crate) fn step_running_transform_output_more(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        transform_idx: usize,
    ) -> Result<TaskStepResult> {
        self.phase = PipelineTaskPhase::Running;
        let result = self.continue_transform_output_more(ctx, transform_idx)?;
        if matches!(result, TaskStepResult::Continue)
            && matches!(self.phase, PipelineTaskPhase::Running)
        {
            self.schedule_next_output_more_continuation();
        }
        Ok(result)
    }

    pub(crate) fn consume_sink_from_slot(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        slot: ChunkSlot,
        resume: SinkResumeState,
        continuation: SinkContinuation,
    ) -> Result<TaskStepResult> {
        let sink_operator = self.runtime.program.sink.operator_id;
        let sink_node_id = sink_operator.index() as u64;
        ctx.profiler.start_operator(sink_node_id);
        let (poll, input_rows) = {
            let (task, memory) = self.task.data_and_memory_mut();
            let memory = memory.call_scope();
            let scratch_state = &mut task.scratch;
            let scratch = OperatorScratchScope::from_expression(&mut scratch_state.expression);
            let mut call_ctx = self.call_context.context(
                ctx.query,
                sink_operator,
                ctx.thread,
                memory,
                scratch,
                ctx.wake,
                &mut *ctx.profiler,
            );
            let input = chunk_slot_mut_from_parts(
                &mut scratch_state.source_chunk,
                &mut scratch_state.transform_chunks,
                slot,
            )?;
            let input_rows = input.size() as u64;
            let poll = self.runtime.program.sink.exec.consume(
                &mut call_ctx,
                &self.runtime.sink_global,
                &mut task.sink,
                input,
            );
            let consumed_rows =
                if matches!(poll, Ok(SinkPoll::NeedMoreInput | SinkPoll::StopPipeline)) {
                    input_rows
                } else {
                    0
                };
            (poll, consumed_rows)
        };
        if input_rows == 0 {
            ctx.profiler.cancel_operator(sink_node_id);
        } else {
            ctx.profiler.end_operator(sink_node_id, input_rows);
        }
        let poll = poll?;

        match poll {
            SinkPoll::NeedMoreInput if continuation == SinkContinuation::Auto => {
                self.after_sink_input_consumed(ctx, resume)
            }
            SinkPoll::NeedMoreInput => Ok(TaskStepResult::Continue),
            SinkPoll::StopPipeline => {
                self.phase = PipelineTaskPhase::Flushing {
                    transform_idx: self.runtime.program.transforms.len(),
                    resume_idx: 0,
                };
                Ok(TaskStepResult::Continue)
            }
            SinkPoll::Pending(blocker) => {
                let chunk = self.take_lease_from_slot(slot, blocker.retained_memory)?;
                self.task.pending = PendingChunkState::SinkInput { resume, chunk };
                Ok(self.block(PipelineTaskPhase::Running, blocker))
            }
        }
    }

    pub(crate) fn after_sink_input_consumed(
        &mut self,
        _ctx: &mut PipelineTaskStepContext<'_>,
        resume: SinkResumeState,
    ) -> Result<TaskStepResult> {
        match resume {
            SinkResumeState::AfterTransformOutputMore { transform_idx } => {
                self.phase = PipelineTaskPhase::RunningTransformOutputMore { transform_idx };
                Ok(TaskStepResult::Continue)
            }
            SinkResumeState::AfterFlushOutput {
                transform_idx,
                output_more,
            } => {
                self.phase = PipelineTaskPhase::Flushing {
                    transform_idx: if output_more {
                        transform_idx
                    } else {
                        transform_idx + 1
                    },
                    resume_idx: 0,
                };
                Ok(TaskStepResult::Continue)
            }
            SinkResumeState::FromStart
            | SinkResumeState::LocalCursor
            | SinkResumeState::RowOffset(_) => {
                self.schedule_next_output_more_continuation();
                Ok(TaskStepResult::Continue)
            }
        }
    }

    pub(crate) fn retain_input_for_transform_pending(
        &mut self,
        input_slot: ChunkSlot,
        memory: RetainedMemorySnapshot,
    ) -> Result<()> {
        match input_slot {
            ChunkSlot::Source => {
                let chunk = ChunkLease::take_from_scratch(
                    &mut self.task.data_mut().scratch.source_chunk,
                    memory,
                )?;
                self.task.pending = PendingChunkState::SourceOutput { chunk };
            }
            ChunkSlot::Transform(transform_idx) => {
                let chunk = ChunkLease::take_from_scratch(
                    self.task
                        .data_mut()
                        .scratch
                        .transform_chunk_mut(transform_idx)
                        .ok_or_else(|| {
                            paro_error::internal("pending transform input slot is out of bounds")
                        })?,
                    memory,
                )?;
                self.task.pending = PendingChunkState::TransformOutput {
                    transform_idx,
                    resume: TransformResumeState::FromStart,
                    chunk,
                };
            }
        }
        Ok(())
    }

    pub(crate) fn take_lease_from_slot(
        &mut self,
        slot: ChunkSlot,
        memory: RetainedMemorySnapshot,
    ) -> Result<ChunkLease> {
        ChunkLease::take_from_scratch(
            chunk_slot_mut(&mut self.task.data_mut().scratch, slot)?,
            memory,
        )
    }

    pub(crate) fn restore_lease_to_slot(
        &mut self,
        slot: ChunkSlot,
        chunk: ChunkLease,
    ) -> Result<()> {
        chunk.restore_into(chunk_slot_mut(&mut self.task.data_mut().scratch, slot)?);
        Ok(())
    }

    pub(crate) fn final_output_slot(&self) -> ChunkSlot {
        if self.runtime.program.transforms.is_empty() {
            ChunkSlot::Source
        } else {
            ChunkSlot::Transform(self.runtime.program.transforms.len() - 1)
        }
    }
}
