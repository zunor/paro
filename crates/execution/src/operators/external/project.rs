// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use crate::execution_context::ExecutionContext;
use crate::operators::external::batching::SubmissionBatchPolicy;
use crate::operators::external::runtime_bridge::{
    ProjectSubmission, RuntimeBridgeOutcome, RuntimeBridgeResponse,
};
use crate::physical::specs::ExternalProjectSpec;
use crate::runtime::context::{
    BlockReason, Blocker, OperatorCallContext, OperatorFinishContext, PipelineInitContext,
    WakeSource, WakeToken,
};
use crate::runtime::state::{
    ExternalProjectTransformGlobal, ExternalProjectTransformLocal, TransformGlobal, TransformLocal,
};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

#[derive(Debug, Clone)]
pub struct ExternalProjectTransformExec {
    pub spec: ExternalProjectSpec,
}

impl ExternalProjectTransformExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        Ok(TransformGlobal::ExternalProject(Arc::new(
            ExternalProjectTransformGlobal {
                batch_policy: SubmissionBatchPolicy::from_dispatch_policy(
                    self.spec.bridge.dispatch_policy(),
                ),
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        external_project_global(global)?;
        Ok(TransformLocal::ExternalProject(
            ExternalProjectTransformLocal {
                ready: VecDeque::new(),
                next_batch_id: 1,
            },
        ))
    }

    pub(crate) fn transform(
        &self,
        ctx: &mut OperatorCallContext,
        global: &TransformGlobal,
        local: &mut TransformLocal,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        let global = external_project_global(global)?;
        let local = external_project_local(local)?;
        if let Some(mut ready) = local.ready.pop_front() {
            output.move_from(&mut ready);
            return Ok(if local.ready.is_empty() {
                TransformPoll::Output
            } else {
                TransformPoll::OutputMore
            });
        }
        if input.is_empty() {
            return Ok(TransformPoll::NeedMoreInput);
        }
        let exec_ctx = ExecutionContext::new(ctx.query.session.clone(), ctx.thread, None);
        let batch_id = local.next_batch_id;
        let submission = ProjectSubmission {
            batch_id,
            input,
            expressions: &self.spec.expressions,
            routines: &self.spec.routines,
            force_tail_flush: false,
            batch_policy: &global.batch_policy,
        };
        local.next_batch_id = local.next_batch_id.saturating_add(1);
        let outcome = self
            .spec
            .bridge
            .execute_project(&exec_ctx, &submission, &ctx.memory)?;
        let (response, blocked) = match outcome {
            RuntimeBridgeOutcome::Ready(response) => (response, false),
            RuntimeBridgeOutcome::Blocked(response) => (response, true),
        };
        enqueue_external_project_output(self, input, response, output, &mut local.ready, ctx)?;
        if blocked {
            return Ok(TransformPoll::Pending(
                Blocker::new(BlockReason::ExternalRuntime).with_wake(ctx.wake.register(
                    WakeSource::ExternalRuntime,
                    WakeToken::external_operator_batch(ctx.operator, batch_id),
                )),
            ));
        }
        Ok(if local.ready.is_empty() {
            TransformPoll::Output
        } else {
            TransformPoll::OutputMore
        })
    }

    pub(crate) fn flush(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        local: &mut TransformLocal,
        output: &mut Chunk,
    ) -> Result<TransformFlushPoll> {
        let local = external_project_local(local)?;
        if let Some(mut ready) = local.ready.pop_front() {
            output.move_from(&mut ready);
            return Ok(if local.ready.is_empty() {
                TransformFlushPoll::Output
            } else {
                TransformFlushPoll::OutputMore
            });
        }
        Ok(TransformFlushPoll::Done)
    }

    pub(crate) fn finish_global(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &TransformGlobal,
    ) -> Result<TransformFinishPoll> {
        Ok(TransformFinishPoll::Done)
    }
}

#[inline(always)]
fn external_project_global(global: &TransformGlobal) -> Result<&ExternalProjectTransformGlobal> {
    match global {
        TransformGlobal::ExternalProject(state) => Ok(state.as_ref()),
        _ => Err(paro_error::internal(
            "external project global state mismatch",
        )),
    }
}

#[inline(always)]
fn external_project_local(
    local: &mut TransformLocal,
) -> Result<&mut ExternalProjectTransformLocal> {
    match local {
        TransformLocal::ExternalProject(state) => Ok(state),
        _ => Err(paro_error::internal(
            "external project local state mismatch",
        )),
    }
}

fn enqueue_external_project_output(
    exec: &ExternalProjectTransformExec,
    input: &Chunk,
    response: RuntimeBridgeResponse,
    output: &mut Chunk,
    ready: &mut VecDeque<Chunk>,
    ctx: &OperatorCallContext,
) -> Result<()> {
    if response.output_batches.len() != 1 {
        return Err(paro_error::internal(
            "external project bridge must return exactly one generated batch",
        ));
    }
    let generated = response
        .output_batches
        .first()
        .expect("generated batch should exist");
    if generated.size() != input.size() {
        return Err(paro_error::internal(format!(
            "external project bridge returned {} rows for {} input rows",
            generated.size(),
            input.size()
        )));
    }
    let allocator = ctx.memory.accounted_allocator_for(
        MemoryTag::ExternalRuntimeHost,
        paro_common::memory::MemoryAccountingClass::NonRevocable,
    );
    let mut passthrough = Chunk::try_init_empty(input.types().as_slice(), allocator.clone())?;
    passthrough.reference(input);
    let mut generated_ref = Chunk::try_init_empty(generated.types().as_slice(), allocator.clone())?;
    generated_ref.reference(generated);
    passthrough.fuse(&mut generated_ref);
    let policy = SubmissionBatchPolicy::from_dispatch_policy(exec.spec.bridge.dispatch_policy());
    let mut batches = policy.rechunk_output(&passthrough, allocator)?;
    let mut first = batches.pop_front().ok_or_else(|| {
        paro_error::internal("external project produced no output for non-empty input")
    })?;
    output.move_from(&mut first);
    ready.append(&mut batches);
    Ok(())
}
