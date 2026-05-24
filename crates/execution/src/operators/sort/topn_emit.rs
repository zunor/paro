// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::runtime::breaker::{HandleRef, TopNHandle};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{BreakerHandleGlobal, SourceGlobal, SourceLocal, TopNEmitSourceLocal};

#[derive(Debug, Clone)]
pub struct TopNEmitSourceExec {
    pub handle: HandleRef<TopNHandle>,
}

impl TopNEmitSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::TopNEmit(Arc::new(BreakerHandleGlobal {
            handle: ctx.handles.get(self.handle)?,
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::TopNEmit(TopNEmitSourceLocal::default()))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let SourceGlobal::TopNEmit(global) = global else {
            return Err(paro_error::internal(
                "topn emit source global state mismatch",
            ));
        };
        let SourceLocal::TopNEmit(local) = local else {
            return Err(paro_error::internal(
                "topn emit source local state mismatch",
            ));
        };
        if local.chunks.is_none() {
            local.chunks = Some(global.handle.sealed_chunks()?);
        }
        let chunks = local
            .chunks
            .as_ref()
            .expect("topn emit source chunks initialized");
        let Some(chunk) = chunks.get(local.cursor) else {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        };
        local.cursor += 1;
        output.reference(chunk);
        Ok(SourcePoll::Output)
    }
}
