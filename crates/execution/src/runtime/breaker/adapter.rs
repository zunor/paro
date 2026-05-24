// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Typed materialized breaker source/sink adapter.

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::physical::properties::RequiredProperties;

use super::{HandleRef, MaterializedHandle};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{SinkGlobal, SinkLocal, SourceGlobal, SourceLocal};

#[derive(Debug, Clone)]
pub struct MaterializedSourceExec {
    pub handle: HandleRef<MaterializedHandle>,
}

#[derive(Debug)]
pub struct MaterializedSourceGlobal {
    pub handle: Arc<MaterializedHandle>,
}

#[derive(Debug, Default)]
pub struct MaterializedSourceLocal;

impl MaterializedSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::Materialized(Arc::new(
            MaterializedSourceGlobal {
                handle: ctx.handles.get(self.handle)?,
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::Materialized(MaterializedSourceLocal))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        _local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let global = global.materialized()?;
        if !global.handle.is_sealed() {
            return Err(paro_error::internal(
                "materialized source was scheduled before producer sealed the handle",
            ));
        }
        let Some(chunks) = global.handle.sealed_chunks() else {
            return Ok(SourcePoll::Finished);
        };
        let idx = global.handle.next_chunk_index();
        let Some(chunk) = chunks.get(idx) else {
            return Ok(SourcePoll::Finished);
        };
        output.reference(chunk);
        Ok(SourcePoll::Output)
    }
}

#[derive(Debug, Clone)]
pub struct MaterializeSinkExec {
    pub handle: HandleRef<MaterializedHandle>,
    pub required: RequiredProperties,
}

#[derive(Debug)]
pub struct MaterializeSinkGlobal {
    pub handle: Arc<MaterializedHandle>,
}

#[derive(Debug, Default)]
pub struct MaterializeSinkLocal {
    pub chunks: Vec<Chunk>,
}

impl MaterializeSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        Ok(SinkGlobal::Materialize(Arc::new(MaterializeSinkGlobal {
            handle: ctx.handles.get(self.handle)?,
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        Ok(SinkLocal::Materialize(MaterializeSinkLocal::default()))
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
        let SinkLocal::Materialize(local) = local else {
            return Err(paro_error::internal(
                "materialize sink local state mismatch",
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
        let SinkGlobal::Materialize(global) = global else {
            return Err(paro_error::internal(
                "materialize sink global state mismatch",
            ));
        };
        let SinkLocal::Materialize(local) = local else {
            return Err(paro_error::internal(
                "materialize sink local state mismatch",
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
        _global: &SinkGlobal,
    ) -> Result<FinishWork> {
        Ok(FinishWork::None)
    }

    pub(crate) fn finish(
        &self,
        _ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::Materialize(global) = global else {
            return Err(paro_error::internal(
                "materialize sink global state mismatch",
            ));
        };
        global.handle.seal()?;
        Ok(FinishPoll::Done)
    }
}
