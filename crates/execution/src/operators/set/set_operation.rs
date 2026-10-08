// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::MemoryAccountingClass;

use crate::physical::specs::{SetOperationInputSide, SetOperationSpec};
use crate::runtime::breaker::{HandleRef, SetOperationHandle};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    BreakerHandleGlobal, SetOperationEmitSourceLocal, SetOperationInputSinkLocal, SinkGlobal,
    SinkLocal, SourceGlobal, SourceLocal,
};

#[derive(Debug, Clone)]
pub struct SetOperationInputSinkExec {
    pub handle: HandleRef<SetOperationHandle>,
    pub spec: SetOperationSpec,
    pub side: SetOperationInputSide,
}

impl SetOperationInputSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        Ok(SinkGlobal::SetOperationInput(Arc::new(
            BreakerHandleGlobal {
                handle: ctx.handles.get(self.handle)?,
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        Ok(SinkLocal::SetOperationInput(SetOperationInputSinkLocal {
            columns: (0..self.spec.output_types.len()).collect::<Vec<_>>(),
            partitions: if self.spec.op == paro_planner::logical::operator::SetOpType::Union
                && self.spec.all
            {
                1
            } else {
                ctx.query.max_parallel_tasks().max(1).next_power_of_two()
            },
            ..Default::default()
        }))
    }

    pub(crate) fn consume(
        &self,
        ctx: &mut OperatorCallContext,
        _global: &SinkGlobal,
        local: &mut SinkLocal,
        input: &mut Chunk,
    ) -> Result<SinkPoll> {
        ctx.cancel.check()?;
        if input.is_empty() {
            return Ok(SinkPoll::NeedMoreInput);
        }
        let SinkLocal::SetOperationInput(local) = local else {
            return Err(paro_error::internal(
                "set-operation sink local state mismatch",
            ));
        };
        local.router.push(
            &mut local.chunks,
            input,
            &local.columns,
            local.partitions,
            || {
                ctx.memory.accounted_allocator_for(
                    MemoryTag::HashTable,
                    MemoryAccountingClass::NonRevocable,
                )
            },
            ctx.cancel,
        )?;
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        let SinkGlobal::SetOperationInput(global) = global else {
            return Err(paro_error::internal(
                "set-operation sink global state mismatch",
            ));
        };
        let SinkLocal::SetOperationInput(local) = local else {
            return Err(paro_error::internal(
                "set-operation sink local state mismatch",
            ));
        };
        global.handle.append_chunks(self.side, &mut local.chunks)?;
        local.router = Default::default();
        Ok(MergePoll::Done)
    }

    pub(crate) fn prepare_finish(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &SinkGlobal,
    ) -> Result<PrepareFinishPoll> {
        Ok(PrepareFinishPoll::Done)
    }

    pub(crate) fn finish_work(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishWork> {
        let SinkGlobal::SetOperationInput(global) = global else {
            return Err(paro_error::internal(
                "set-operation sink global state mismatch",
            ));
        };
        global.handle.finish_work(&self.spec, ctx)
    }

    pub(crate) fn finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::SetOperationInput(global) = global else {
            return Err(paro_error::internal(
                "set-operation sink global state mismatch",
            ));
        };
        ctx.cancel.check()?;
        if !global.handle.is_sealed() {
            return Err(paro_error::internal(
                "set-operation finish group did not publish",
            ));
        }
        Ok(FinishPoll::Done)
    }
}

#[derive(Debug, Clone)]
pub struct SetOperationEmitSourceExec {
    pub handle: HandleRef<SetOperationHandle>,
}

impl SetOperationEmitSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::SetOperationEmit(Arc::new(
            BreakerHandleGlobal {
                handle: ctx.handles.get(self.handle)?,
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::SetOperationEmit(
            SetOperationEmitSourceLocal::default(),
        ))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let SourceGlobal::SetOperationEmit(global) = global else {
            return Err(paro_error::internal(
                "set-operation emit source global state mismatch",
            ));
        };
        let SourceLocal::SetOperationEmit(local) = local else {
            return Err(paro_error::internal(
                "set-operation emit source local state mismatch",
            ));
        };
        if !global.handle.is_sealed() {
            return Err(paro_error::internal(
                "set-operation emit source was scheduled before producer sealed the handle",
            ));
        }
        if local.chunks.is_none() {
            local.chunks = Some(global.handle.sealed_chunks()?);
        }
        let chunks = local
            .chunks
            .as_ref()
            .expect("set-operation chunks initialized");
        let Some(chunk) = chunks.get(local.cursor) else {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        };
        local.cursor += 1;
        output.reference(chunk);
        Ok(SourcePoll::Output)
    }
}
