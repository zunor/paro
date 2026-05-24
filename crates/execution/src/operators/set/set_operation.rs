// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_function::scalar::FunctionExecContext;

use crate::physical::properties::RequiredProperties;
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
    pub required: RequiredProperties,
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
        _ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        Ok(SinkLocal::SetOperationInput(
            SetOperationInputSinkLocal::default(),
        ))
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
        local.chunks.push(input.handoff_referencing_vectors());
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
        _ctx: &mut OperatorFinishContext,
        _global: &SinkGlobal,
    ) -> Result<FinishWork> {
        Ok(FinishWork::None)
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
        global
            .handle
            .seal(&self.spec, ctx.query.allocator(MemoryTag::HashTable))?;
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
