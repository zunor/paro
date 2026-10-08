// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryOwner,
};
use paro_common::types::LogicalType;
use paro_storage::buffer::{MemoryTag, DEFAULT_BLOCK_SIZE};
use paro_storage::row::{RowFormat, RowSpillWriter, RowStoreSpillWriter};

use crate::operators::sort::build::query_has_temporary_directory;
use crate::physical::properties::MemoryClass;
use crate::physical::specs::SpillExecutionPolicy;
use crate::runtime::breaker::{CteHandle, HandleRef};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{
    FinishPoll, FinishTaskGroupRunner, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll,
};
use crate::runtime::state::{BreakerHandleGlobal, SinkGlobal, SinkLocal};

#[derive(Debug)]
pub struct CteMaterializeSinkLocal {
    chunks: Vec<Chunk>,
    external: Option<RowStoreSpillWriter<CteRowFormat>>,
}

#[derive(Debug, Clone)]
struct CteRowFormat {
    logical_types: Box<[LogicalType]>,
}

impl RowFormat for CteRowFormat {
    fn name(&self) -> &'static str {
        "cte_rows"
    }

    fn logical_types(&self) -> &[LogicalType] {
        &self.logical_types
    }
}

fn prepare_cte_snapshot(
    ctx: &crate::runtime::context::QueryRuntimeContext,
    handle: &CteHandle,
) -> Result<()> {
    let Some(mut staged) = handle.take_staged_snapshot() else {
        return Ok(());
    };
    if handle.external_selected() || !staged.stores.is_empty() {
        if !query_has_temporary_directory(ctx) {
            return Err(paro_error::out_of_memory(
                "external CTE materialization requires a temporary directory",
            ));
        }
        if !staged.chunks.is_empty() {
            let mut external = cte_spill_writer(ctx, handle);
            for chunk in staged.chunks.drain(..) {
                external.append_chunk(&chunk)?;
            }
            staged.stores.push(external.finish()?);
        }
        for store in staged.stores {
            handle.append_row_store(store)?;
        }
    } else {
        handle.append_chunks(&mut staged.chunks)?;
    }
    Ok(())
}

fn cte_spill_writer(
    ctx: &crate::runtime::context::QueryRuntimeContext,
    handle: &CteHandle,
) -> RowStoreSpillWriter<CteRowFormat> {
    let owner: Arc<dyn MemoryOwner> = ctx.memory.clone();
    RowStoreSpillWriter::new(
        ctx.session.buffer_pool().clone(),
        CteRowFormat {
            logical_types: handle.metadata().row_type.types.clone(),
        },
        MemoryTag::HashTable,
        MemoryAccountingContext::from_owner(
            owner,
            MemoryDomain::Host,
            MemoryTag::HashTable,
            MemoryAccountingClass::Spill,
        ),
    )
}

#[derive(Debug, Clone)]
pub struct CteMaterializeSinkExec {
    pub handle: HandleRef<CteHandle>,
    pub spill_policy: SpillExecutionPolicy,
}

impl CteMaterializeSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        if self.spill_policy == SpillExecutionPolicy::ForcedExternal
            && !query_has_temporary_directory(ctx.query)
        {
            return Err(paro_error::out_of_memory(
                "forced external CTE materialization requires a temporary directory",
            ));
        }
        let handle = ctx.handles.get(self.handle)?;
        if self.spill_policy == SpillExecutionPolicy::ForcedExternal {
            handle.select_external();
        }
        Ok(SinkGlobal::CteMaterialize(Arc::new(BreakerHandleGlobal {
            handle,
        })))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        let SinkGlobal::CteMaterialize(global) = global else {
            return Err(paro_error::internal(
                "CTE materialize sink global state mismatch",
            ));
        };
        let external = global
            .handle
            .external_selected()
            .then(|| cte_spill_writer(ctx.query, global.handle.as_ref()));
        Ok(SinkLocal::CteMaterialize(CteMaterializeSinkLocal {
            chunks: Vec::new(),
            external,
        }))
    }

    pub(crate) fn consume(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
        input: &mut Chunk,
    ) -> Result<SinkPoll> {
        ctx.cancel.check()?;
        if input.is_empty() {
            return Ok(SinkPoll::NeedMoreInput);
        }
        let SinkLocal::CteMaterialize(local) = local else {
            return Err(paro_error::internal(
                "CTE materialize sink local state mismatch",
            ));
        };
        let SinkGlobal::CteMaterialize(global) = global else {
            return Err(paro_error::internal(
                "CTE materialize sink global state mismatch",
            ));
        };
        if self.spill_policy == SpillExecutionPolicy::Adaptive
            && query_has_temporary_directory(ctx.query)
            && ctx.query.memory.available_bytes() <= DEFAULT_BLOCK_SIZE.saturating_mul(2)
        {
            global.handle.select_external();
        }
        if local.external.is_none() && global.handle.external_selected() {
            let mut external = cte_spill_writer(ctx.query, global.handle.as_ref());
            for chunk in local.chunks.drain(..) {
                external.append_chunk(&chunk)?;
            }
            local.external = Some(external);
        }
        if let Some(external) = &mut local.external {
            external.append_chunk(input)?;
        } else {
            local.chunks.push(input.handoff_referencing_vectors());
        }
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        let SinkGlobal::CteMaterialize(global) = global else {
            return Err(paro_error::internal(
                "CTE materialize sink global state mismatch",
            ));
        };
        let SinkLocal::CteMaterialize(local) = local else {
            return Err(paro_error::internal(
                "CTE materialize sink local state mismatch",
            ));
        };
        if let Some(external) = local.external.take() {
            global.handle.stage_row_store(external.finish()?)?;
        }
        global.handle.stage_chunks(&mut local.chunks)?;
        Ok(MergePoll::Done)
    }

    pub(crate) fn prepare_finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<PrepareFinishPoll> {
        let SinkGlobal::CteMaterialize(global) = global else {
            return Err(paro_error::internal(
                "CTE materialize sink global state mismatch",
            ));
        };
        prepare_cte_snapshot(ctx.query, global.handle.as_ref())?;
        Ok(PrepareFinishPoll::Done)
    }

    pub(crate) fn finish_work(
        &self,
        _ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishWork> {
        let SinkGlobal::CteMaterialize(global) = global else {
            return Err(paro_error::internal(
                "CTE materialize sink global state mismatch",
            ));
        };
        let handle = global.handle.clone();
        Ok(FinishWork::Parallel(FinishTaskGroupRunner::group(
            "cte_materialize_seal",
            MemoryClass::Blocking,
            move |_ctx| handle.seal(),
        )))
    }

    pub(crate) fn finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::CteMaterialize(global) = global else {
            return Err(paro_error::internal(
                "CTE materialize sink global state mismatch",
            ));
        };
        prepare_cte_snapshot(ctx.query, global.handle.as_ref())?;
        if !global.handle.is_sealed() {
            global.handle.seal()?;
        }
        Ok(FinishPoll::Done)
    }
}
