// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Client result sink operator — delivers query output to the client channel.

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;

use crate::pipeline::graph::ClientResultSpec;
use crate::runtime::context::{
    Blocker, OperatorCallContext, OperatorFinishContext, PipelineInitContext,
    QueryOutputReferenceWrite,
};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::state::{ClientResultSinkGlobal, ClientResultSinkLocal, SinkGlobal, SinkLocal};

#[derive(Debug, Clone)]
pub struct ClientResultSinkExec {
    pub spec: ClientResultSpec,
}

impl ClientResultSinkExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        Ok(SinkGlobal::ClientResult(Arc::new(ClientResultSinkGlobal)))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        global.client_result()?;
        Ok(SinkLocal::ClientResult(ClientResultSinkLocal))
    }

    pub(crate) fn consume(
        &self,
        ctx: &mut OperatorCallContext,
        _global: &SinkGlobal,
        _local: &mut SinkLocal,
        input: &mut Chunk,
    ) -> Result<SinkPoll> {
        if input.is_empty() {
            return Ok(SinkPoll::NeedMoreInput);
        }

        match ctx.query.output.try_push_reference(input) {
            QueryOutputReferenceWrite::Written => {
                input.clear_rows_preserve_storage();
                Ok(SinkPoll::NeedMoreInput)
            }
            QueryOutputReferenceWrite::Blocked => Ok(SinkPoll::Pending(
                Blocker::output_backpressure(ctx.wake, &ctx.query.output),
            )),
        }
    }

    pub(crate) fn merge_local(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &SinkGlobal,
        _local: &mut SinkLocal,
    ) -> Result<MergePoll> {
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
