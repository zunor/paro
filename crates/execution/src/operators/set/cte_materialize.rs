// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryOwner,
};
use paro_common::types::LogicalType;
use paro_storage::buffer::{MemoryTag, DEFAULT_BLOCK_SIZE};
use paro_storage::row::{RowFormat, RowSpillWriter, RowStore, RowStoreSpillWriter};

use crate::operators::sort::build::query_has_temporary_directory;
use crate::physical::properties::MemoryClass;
use crate::physical::specs::SpillExecutionPolicy;
use crate::runtime::breaker::{CteHandle, HandleRef};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{
    FinishPoll, FinishTaskGroupRunner, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll,
};
use crate::runtime::state::{SinkGlobal, SinkLocal};

#[derive(Debug)]
pub struct CteMaterializeSinkLocal {
    chunks: Vec<Chunk>,
    external: Option<RowStoreSpillWriter<CteRowFormat>>,
}

#[derive(Debug, Clone)]
struct CteRowFormat {
    logical_types: Box<[LogicalType]>,
}

#[derive(Debug, Default)]
struct CteMaterializePending {
    chunks: Vec<Chunk>,
    stores: Vec<RowStore>,
    prepared: bool,
}

#[derive(Debug)]
pub struct CteMaterializeSinkGlobal {
    pub handle: Arc<CteHandle>,
    external_selected: AtomicBool,
    pending: Mutex<CteMaterializePending>,
}

impl CteMaterializeSinkGlobal {
    fn new(handle: Arc<CteHandle>, external_selected: bool) -> Self {
        Self {
            handle,
            external_selected: AtomicBool::new(external_selected),
            pending: Mutex::new(CteMaterializePending::default()),
        }
    }

    #[inline]
    fn external_selected(&self) -> bool {
        self.external_selected.load(Ordering::Acquire)
    }

    #[inline]
    fn select_external(&self) {
        self.external_selected.store(true, Ordering::Release);
    }

    fn append_local(&self, local: &mut CteMaterializeSinkLocal) -> Result<()> {
        let mut pending = self.pending.lock();
        if pending.prepared {
            return Err(paro_error::internal(
                "cannot merge a CTE materialize local after snapshot preparation",
            ));
        }
        if let Some(external) = local.external.take() {
            pending.stores.push(external.finish()?);
        }
        pending.chunks.append(&mut local.chunks);
        Ok(())
    }

    fn prepare_snapshot(&self, ctx: &crate::runtime::context::QueryRuntimeContext) -> Result<()> {
        let (mut chunks, mut stores) = {
            let mut pending = self.pending.lock();
            if pending.prepared {
                return Ok(());
            }
            pending.prepared = true;
            (
                std::mem::take(&mut pending.chunks),
                std::mem::take(&mut pending.stores),
            )
        };

        if self.external_selected() || !stores.is_empty() {
            if !query_has_temporary_directory(ctx) {
                return Err(paro_error::out_of_memory(
                    "external CTE materialization requires a temporary directory",
                ));
            }
            if !chunks.is_empty() {
                let mut external = cte_spill_writer(ctx, self.handle.as_ref());
                for mut chunk in chunks.drain(..) {
                    external.append_chunk(&mut chunk)?;
                }
                stores.push(external.finish()?);
            }
            for store in stores {
                self.handle.append_row_store(store)?;
            }
        } else {
            self.handle.append_chunks(&mut chunks)?;
        }
        Ok(())
    }
}

impl RowFormat for CteRowFormat {
    fn name(&self) -> &'static str {
        "cte_rows"
    }

    fn logical_types(&self) -> &[LogicalType] {
        &self.logical_types
    }
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
        Ok(SinkGlobal::CteMaterialize(Arc::new(
            CteMaterializeSinkGlobal::new(
                ctx.handles.get(self.handle)?,
                self.spill_policy == SpillExecutionPolicy::ForcedExternal,
            ),
        )))
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
            global.select_external();
        }
        if local.external.is_none() && global.external_selected() {
            let mut external = cte_spill_writer(ctx.query, global.handle.as_ref());
            for mut chunk in local.chunks.drain(..) {
                external.append_chunk(&mut chunk)?;
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
        global.append_local(local)?;
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
        global.prepare_snapshot(ctx.query)?;
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
        global.prepare_snapshot(ctx.query)?;
        if !global.handle.is_sealed() {
            global.handle.seal()?;
        }
        Ok(FinishPoll::Done)
    }
}
