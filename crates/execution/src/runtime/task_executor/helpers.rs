// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl PipelineTaskExecutor {
    pub(crate) fn block(&mut self, resume: PipelineTaskPhase, blocker: Blocker) -> TaskStepResult {
        self.blocked_resume = Some(resume);
        self.phase = PipelineTaskPhase::Blocked;
        TaskStepResult::Blocked(blocker)
    }

    pub(crate) fn record_operator_error(
        &self,
        query: &QueryRuntimeContext,
        error: ParoError,
    ) -> ParoError {
        query.record_operator_error(error.clone());
        error
    }

    pub(crate) fn finish_step_error(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        error: ParoError,
    ) -> Result<TaskStepResult> {
        let reason = Self::cancel_reason_for_error(ctx.query, &error);
        self.cancel_active_finish_group(ctx, reason);
        if reason == CancelReason::OperatorError {
            Err(self.record_operator_error(ctx.query, error))
        } else {
            Err(error)
        }
    }

    pub(crate) fn cancel_reason_for_error(
        query: &QueryRuntimeContext,
        error: &ParoError,
    ) -> CancelReason {
        if error.is_query_canceled() && query.cancellation.is_cancelled() {
            return CancelReason::from_statement(query.cancellation.reason())
                .unwrap_or(CancelReason::UserRequest);
        }
        CancelReason::OperatorError
    }

    pub(crate) fn cancel_active_finish_group(
        &mut self,
        ctx: &mut PipelineTaskStepContext<'_>,
        reason: CancelReason,
    ) {
        let Some(group) = self.finish_group.take() else {
            return;
        };
        self.active_finish_task = None;
        self.finish_tasks_completed = 0;
        self.cancel_finish_group(ctx, &group, reason);
    }
}

pub(crate) fn chunk_slot_mut(
    scratch: &mut PipelineScratch,
    slot: ChunkSlot,
) -> Result<&mut paro_common::chunk::Chunk> {
    chunk_slot_mut_from_parts(
        &mut scratch.source_chunk,
        &mut scratch.transform_chunks,
        slot,
    )
}

pub(crate) fn chunk_slot_mut_from_parts<'a>(
    source: &'a mut paro_common::chunk::Chunk,
    transforms: &'a mut [paro_common::chunk::Chunk],
    slot: ChunkSlot,
) -> Result<&'a mut paro_common::chunk::Chunk> {
    match slot {
        ChunkSlot::Source => Ok(source),
        ChunkSlot::Transform(idx) => transforms
            .get_mut(idx)
            .ok_or_else(|| paro_error::internal("transform chunk slot is out of bounds")),
    }
}

pub(crate) fn transform_input_output_chunks<'a>(
    source: &'a paro_common::chunk::Chunk,
    transforms: &'a mut [paro_common::chunk::Chunk],
    input_slot: ChunkSlot,
    output_idx: usize,
) -> Result<(
    &'a paro_common::chunk::Chunk,
    &'a mut paro_common::chunk::Chunk,
)> {
    match input_slot {
        ChunkSlot::Source => {
            let output = transforms
                .get_mut(output_idx)
                .ok_or_else(|| paro_error::internal("transform output slot is out of bounds"))?;
            Ok((source, output))
        }
        ChunkSlot::Transform(input_idx) => {
            if input_idx == output_idx {
                return Err(paro_error::internal(
                    "transform input and output slots must be distinct",
                ));
            }
            if input_idx < output_idx {
                let (left, right) = transforms.split_at_mut(output_idx);
                let input = left
                    .get(input_idx)
                    .ok_or_else(|| paro_error::internal("transform input slot is out of bounds"))?;
                Ok((input, &mut right[0]))
            } else {
                let (left, right) = transforms.split_at_mut(input_idx);
                let output = left.get_mut(output_idx).ok_or_else(|| {
                    paro_error::internal("transform output slot is out of bounds")
                })?;
                Ok((&right[0], output))
            }
        }
    }
}

pub(crate) fn finish_context<'a>(
    ctx: &'a mut PipelineTaskStepContext<'_>,
    pipeline: crate::pipeline::graph::PipelineId,
    operator: RuntimeOperatorId,
    finish_task: Option<FinishTaskId>,
    task: &'a PipelineTaskState,
) -> OperatorFinishContext<'a> {
    OperatorFinishContext {
        query: ctx.query,
        pipeline,
        operator,
        finish_task,
        thread: ctx.thread,
        memory: task.memory.call_scope(),
        cancel: &ctx.query.cancellation,
        wake: ctx.wake,
        profiler: &mut *ctx.profiler,
    }
}
