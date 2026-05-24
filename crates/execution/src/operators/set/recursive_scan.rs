// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::runtime::breaker::{HandleRef, RecursiveTableHandle};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    BreakerHandleGlobal, RecursiveTableScanSourceLocal, SourceGlobal, SourceLocal,
};

#[derive(Debug, Clone)]
pub struct RecursiveTableScanSourceExec {
    pub handle: HandleRef<RecursiveTableHandle>,
}

impl RecursiveTableScanSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::RecursiveTableScan(Arc::new(
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
        Ok(SourceLocal::RecursiveTableScan(
            RecursiveTableScanSourceLocal::default(),
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
        let SourceGlobal::RecursiveTableScan(global) = global else {
            return Err(paro_error::internal(
                "recursive table scan source global state mismatch",
            ));
        };
        let SourceLocal::RecursiveTableScan(local) = local else {
            return Err(paro_error::internal(
                "recursive table scan source local state mismatch",
            ));
        };
        if local.chunks.is_none() {
            local.chunks = Some(global.handle.snapshot_chunks());
        }
        let chunks = local
            .chunks
            .as_ref()
            .expect("recursive table scan chunks initialized");
        let Some(chunk) = chunks.get(local.cursor) else {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        };
        local.cursor += 1;
        output.reference(chunk);
        Ok(SourcePoll::Output)
    }
}
