// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::runtime::breaker::{DelimHandle, HandleRef};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{BreakerHandleGlobal, DelimScanSourceLocal, SourceGlobal, SourceLocal};

#[derive(Debug, Clone)]
pub struct DelimScanSourceExec {
    pub handle: HandleRef<DelimHandle>,
}

impl DelimScanSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::DelimScan(Arc::new(BreakerHandleGlobal {
            handle: ctx.handles.get(self.handle)?,
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::DelimScan(DelimScanSourceLocal::default()))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let SourceGlobal::DelimScan(global) = global else {
            return Err(paro_error::internal(
                "delim scan source global state mismatch",
            ));
        };
        let SourceLocal::DelimScan(local) = local else {
            return Err(paro_error::internal(
                "delim scan source local state mismatch",
            ));
        };
        if local.chunks.is_none() {
            local.chunks = Some(global.handle.sealed_values()?);
        }
        let chunks = local
            .chunks
            .as_ref()
            .expect("delim scan source chunks initialized");
        let Some(chunk) = chunks.get(local.cursor) else {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        };
        if chunk.column_count() != output.column_count() {
            return Err(paro_error::internal(format!(
                "delim scan source column count mismatch: handle has {}, output has {}",
                chunk.column_count(),
                output.column_count()
            )));
        }
        local.cursor += 1;
        output.reference(chunk);
        Ok(SourcePoll::Output)
    }
}
