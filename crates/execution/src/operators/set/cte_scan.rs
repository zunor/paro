// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_storage::row::{RowScanState, RowStore};

use crate::runtime::breaker::{CteHandle, HandleRef, MaterializedReader};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{SourceGlobal, SourceLocal};

#[derive(Debug)]
pub struct CteScanSourceGlobal {
    reader: MaterializedReader,
    next_chunk: AtomicUsize,
    next_store: AtomicUsize,
}

impl CteScanSourceGlobal {
    fn new(handle: Arc<CteHandle>) -> Self {
        Self {
            reader: MaterializedReader::new(handle.materialized(), "CTE scan"),
            next_chunk: AtomicUsize::new(0),
            next_store: AtomicUsize::new(0),
        }
    }
}

#[derive(Debug, Default)]
pub struct CteScanSourceLocal {
    external_store: Option<Arc<RowStore>>,
    external_scan: RowScanState,
}

#[derive(Debug, Clone)]
pub struct CteScanSourceExec {
    pub handle: HandleRef<CteHandle>,
}

impl CteScanSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::CteScan(Arc::new(CteScanSourceGlobal::new(
            ctx.handles.get(self.handle)?,
        ))))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::CteScan(CteScanSourceLocal::default()))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let SourceGlobal::CteScan(global) = global else {
            return Err(paro_error::internal(
                "CTE scan source global state mismatch",
            ));
        };
        let SourceLocal::CteScan(local) = local else {
            return Err(paro_error::internal("CTE scan source local state mismatch"));
        };
        if let Some(stores) = global.reader.external_row_stores()? {
            loop {
                if local.external_store.is_none() {
                    let index = global.next_store.fetch_add(1, Ordering::Relaxed);
                    let Some(store) = stores.get(index) else {
                        output.try_set_cardinality(0)?;
                        return Ok(SourcePoll::Finished);
                    };
                    local.external_store = Some(Arc::clone(store));
                    local.external_scan.reset();
                }
                let count = local
                    .external_store
                    .as_ref()
                    .expect("external CTE store initialized above")
                    .scan_with_state(&mut local.external_scan, output)?;
                if count > 0 {
                    return Ok(SourcePoll::Output);
                }
                local.external_store = None;
            }
        }

        let chunks = global.reader.sealed_chunks()?;
        let index = global.next_chunk.fetch_add(1, Ordering::Relaxed);
        let Some(chunk) = chunks.get(index) else {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        };
        output.reference(chunk);
        Ok(SourcePoll::Output)
    }
}
