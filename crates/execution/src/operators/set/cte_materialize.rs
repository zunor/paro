// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::physical::properties::{MemoryClass, RequiredProperties};
use crate::runtime::breaker::{CteHandle, HandleRef};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{
    FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SingleTaskFinishDriver, SinkPoll,
};
use crate::runtime::state::{BreakerHandleGlobal, CteMaterializeSinkLocal, SinkGlobal, SinkLocal};

#[derive(Debug, Clone)]
pub struct CteMaterializeSinkExec {
    pub handle: HandleRef<CteHandle>,
    pub required: RequiredProperties,
}

impl CteMaterializeSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        Ok(SinkGlobal::CteMaterialize(Arc::new(BreakerHandleGlobal {
            handle: ctx.handles.get(self.handle)?,
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        Ok(SinkLocal::CteMaterialize(CteMaterializeSinkLocal::default()))
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
        let SinkLocal::CteMaterialize(local) = local else {
            return Err(paro_error::internal(
                "CTE materialize sink local state mismatch",
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
        let SinkGlobal::CteMaterialize(global) = global else {
            return Err(paro_error::internal(
                "CTE materialize sink global state mismatch",
            ));
        };
        let SinkLocal::CteMaterialize(local) = local else {
            return Err(paro_error::internal(
                "CTE materialize sink local state mismatch",
            ));
        };
        global.handle.append_chunks(&mut local.chunks)?;
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
        global: &SinkGlobal,
    ) -> Result<FinishWork> {
        let SinkGlobal::CteMaterialize(global) = global else {
            return Err(paro_error::internal(
                "CTE materialize sink global state mismatch",
            ));
        };
        let handle = global.handle.clone();
        Ok(FinishWork::Parallel(SingleTaskFinishDriver::group(
            "cte_materialize_seal",
            MemoryClass::Blocking,
            move |_ctx| handle.seal(),
        )))
    }

    pub(crate) fn finish(
        &self,
        _ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::CteMaterialize(global) = global else {
            return Err(paro_error::internal(
                "CTE materialize sink global state mismatch",
            ));
        };
        if !global.handle.is_sealed() {
            global.handle.seal()?;
        }
        Ok(FinishPoll::Done)
    }
}
