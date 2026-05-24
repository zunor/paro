// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::physical::properties::RequiredProperties;
use crate::runtime::breaker::{HandleRef, RecursiveTableHandle};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::state::{
    RecursiveTableAppendSinkGlobal, RecursiveTableAppendSinkLocal, SinkGlobal, SinkLocal,
};

#[derive(Debug, Clone)]
pub struct RecursiveTableAppendSinkExec {
    pub handle: HandleRef<RecursiveTableHandle>,
    pub required: RequiredProperties,
}

impl RecursiveTableAppendSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        let target = ctx.handles.get(self.handle)?;
        let dedup = target.dedup();
        Ok(SinkGlobal::RecursiveTableAppend(Arc::new(
            RecursiveTableAppendSinkGlobal { target, dedup },
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        Ok(SinkLocal::RecursiveTableAppend(
            RecursiveTableAppendSinkLocal::default(),
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
        let SinkLocal::RecursiveTableAppend(local) = local else {
            return Err(paro_error::internal(
                "recursive table append sink local state mismatch",
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
        let SinkGlobal::RecursiveTableAppend(global) = global else {
            return Err(paro_error::internal(
                "recursive table append sink global state mismatch",
            ));
        };
        let SinkLocal::RecursiveTableAppend(local) = local else {
            return Err(paro_error::internal(
                "recursive table append sink local state mismatch",
            ));
        };
        if let Some(dedup) = global.dedup.as_ref() {
            global.target.append_distinct(dedup, &mut local.chunks)?;
        } else {
            global.target.append_chunks(&mut local.chunks);
        }
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
        _ctx: &mut OperatorFinishContext,
        _global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        Ok(FinishPoll::Done)
    }
}
