// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::physical::specs::WindowSpec;
use crate::runtime::breaker::{HandleRef, WindowHandle};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    BreakerHandleGlobal, SourceGlobal, SourceLocal, WindowEmitSourceLocal,
};

#[derive(Debug, Clone)]
pub struct WindowEmitSourceExec {
    pub handle: HandleRef<WindowHandle>,
    pub spec: WindowSpec,
}

impl WindowEmitSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::WindowEmit(Arc::new(BreakerHandleGlobal {
            handle: ctx.handles.get(self.handle)?,
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::WindowEmit(WindowEmitSourceLocal::default()))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let SourceGlobal::WindowEmit(global) = global else {
            return Err(paro_error::internal(
                "window emit source global state mismatch",
            ));
        };
        let SourceLocal::WindowEmit(local) = local else {
            return Err(paro_error::internal(
                "window emit source local state mismatch",
            ));
        };
        debug_assert_eq!(
            self.spec.output_types.len(),
            global.handle.metadata().row_type.column_count(),
            "window emit spec must match handle row type"
        );
        if local.chunks.is_none() {
            local.chunks = Some(global.handle.sealed_chunks()?);
        }
        let chunks = local
            .chunks
            .as_ref()
            .expect("window emit source chunks initialized");
        let Some(chunk) = chunks.get(local.cursor) else {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        };
        local.cursor += 1;
        output.reference(chunk);
        Ok(SourcePoll::Output)
    }
}
