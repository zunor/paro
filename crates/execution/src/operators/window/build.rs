// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_function::scalar::FunctionExecContext;

use crate::physical::properties::MemoryClass;
use crate::physical::specs::WindowSpec;
use crate::runtime::breaker::{HandleRef, WindowHandle};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{
    FinishPoll, FinishTaskGroupRunner, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll,
};
use crate::runtime::state::{BreakerHandleGlobal, SinkGlobal, SinkLocal, WindowBuildSinkLocal};

// ---------------------------------------------------------------------------
// Window build sink
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct WindowBuildSinkExec {
    pub handle: HandleRef<WindowHandle>,
    pub spec: WindowSpec,
}

impl WindowBuildSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        Ok(SinkGlobal::WindowBuild(Arc::new(BreakerHandleGlobal {
            handle: ctx.handles.get(self.handle)?,
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        Ok(SinkLocal::WindowBuild(WindowBuildSinkLocal::default()))
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
        let SinkLocal::WindowBuild(local) = local else {
            return Err(paro_error::internal(
                "window build sink local state mismatch",
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
        let SinkGlobal::WindowBuild(global) = global else {
            return Err(paro_error::internal(
                "window build sink global state mismatch",
            ));
        };
        let SinkLocal::WindowBuild(local) = local else {
            return Err(paro_error::internal(
                "window build sink local state mismatch",
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
        let SinkGlobal::WindowBuild(global) = global else {
            return Err(paro_error::internal(
                "window build sink global state mismatch",
            ));
        };
        let handle = global.handle.clone();
        let spec = self.spec.clone();
        Ok(FinishWork::Parallel(FinishTaskGroupRunner::group(
            "window_seal",
            MemoryClass::Blocking,
            move |ctx| handle.seal(&spec, ctx.query.allocator(MemoryTag::BaseTable)),
        )))
    }

    pub(crate) fn finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::WindowBuild(global) = global else {
            return Err(paro_error::internal(
                "window build sink global state mismatch",
            ));
        };
        if !global.handle.is_sealed() {
            global
                .handle
                .seal(&self.spec, ctx.query.allocator(MemoryTag::BaseTable))?;
        }
        Ok(FinishPoll::Done)
    }
}
