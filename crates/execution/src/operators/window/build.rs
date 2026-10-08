// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::MemoryAccountingClass;

use crate::physical::specs::WindowSpec;
use crate::runtime::breaker::{HandleRef, WindowHandle};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
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
        ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        Ok(SinkLocal::WindowBuild(WindowBuildSinkLocal {
            columns: super::runtime::window_radix_columns(&self.spec)?,
            partitions: ctx.query.max_parallel_tasks().max(1).next_power_of_two(),
            ..Default::default()
        }))
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
        local.router.push(
            &mut local.chunks,
            input,
            &local.columns,
            local.partitions,
            || {
                ctx.memory.accounted_allocator_for(
                    MemoryTag::BaseTable,
                    MemoryAccountingClass::NonRevocable,
                )
            },
            ctx.cancel,
        )?;
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
        local.router = Default::default();
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
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishWork> {
        let SinkGlobal::WindowBuild(global) = global else {
            return Err(paro_error::internal(
                "window build sink global state mismatch",
            ));
        };
        super::finalize::prepare_window_finalize(global.handle.clone(), &self.spec, ctx)
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
        ctx.cancel.check()?;
        if !global.handle.is_sealed() {
            return Err(paro_error::internal("window finish group did not publish"));
        }
        Ok(FinishPoll::Done)
    }
}
