// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::runtime::breaker::{ExternalTableHandle, HandleRef};
use crate::runtime::context::{
    Blocker, OperatorCallContext, PipelineInitContext, WakeSource, WakeToken,
};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    ExternalTableSourceGlobal, ExternalTableSourceLocal, SourceGlobal, SourceLocal,
};

// ---------------------------------------------------------------------------
// Exec struct
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ExternalTableSourceExec {
    pub handle: HandleRef<ExternalTableHandle>,
}

// ---------------------------------------------------------------------------
// Impl
// ---------------------------------------------------------------------------

impl ExternalTableSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        let handle = ctx.handles.get(self.handle)?;
        Ok(SourceGlobal::ExternalTable(Arc::new(
            ExternalTableSourceGlobal { handle },
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        external_table_source_global(global)?;
        Ok(SourceLocal::ExternalTable(ExternalTableSourceLocal))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        _local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        let global = external_table_source_global(global)?;
        if let Some(batch) = global.handle.shared().pop_visible_batch() {
            *output = batch.chunk;
            return Ok(SourcePoll::Output);
        }
        if global.handle.shared().is_finalized() {
            Ok(SourcePoll::Finished)
        } else {
            let token = WakeToken::external_table(global.handle.metadata().id.index());
            Ok(SourcePoll::Pending(
                Blocker::new(crate::runtime::context::BlockReason::ExternalRuntime)
                    .with_wake(ctx.wake.register(WakeSource::ExternalRuntime, token)),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// State accessor helper
// ---------------------------------------------------------------------------

#[inline(always)]
pub(crate) fn external_table_source_global(
    global: &SourceGlobal,
) -> Result<&ExternalTableSourceGlobal> {
    match global {
        SourceGlobal::ExternalTable(state) => Ok(state.as_ref()),
        _ => Err(paro_error::internal(
            "external table source global state mismatch",
        )),
    }
}
