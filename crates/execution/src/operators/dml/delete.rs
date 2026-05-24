// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::Ordering;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_transaction::TableId;

use crate::physical::properties::RequiredProperties;
use crate::physical::specs::DeleteSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::state::{DmlSinkGlobal, EmptyDmlSinkLocal, SinkGlobal, SinkLocal};

use super::helpers::{
    active_transaction, bind_dml_write, collect_row_ids, dml_global, dml_result_chunk,
    primary_key_columns, storage_table,
};

#[derive(Debug, Clone)]
pub struct DeleteSinkExec {
    pub spec: DeleteSpec,
    pub required: RequiredProperties,
}

impl DeleteSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        bind_dml_write(ctx, &self.spec.table, true)?;
        Ok(SinkGlobal::Dml(Arc::new(DmlSinkGlobal::default())))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        dml_global(global)?;
        Ok(SinkLocal::EmptyDml(EmptyDmlSinkLocal))
    }

    pub(crate) fn consume(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        _local: &mut SinkLocal,
        input: &mut Chunk,
    ) -> Result<SinkPoll> {
        let global = dml_global(global)?;
        let storage = storage_table(&self.spec.table)?;
        let txn = active_transaction(ctx, "DELETE")?;
        if self.spec.is_full_table_delete {
            if global
                .full_table_delete_executed
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                ctx.query
                    .transaction
                    .read_tracker()
                    .record_predicate(TableId::new(storage.table_id()), 0);
                let deleted = storage.delete_all(&ctx.query.transaction, txn.clone())?;
                global
                    .affected_count
                    .fetch_add(deleted as u64, Ordering::SeqCst);
                if deleted > 0 {
                    txn.record_graph_delete(self.spec.table.base.base.object_id.raw(), deleted);
                }
            }
            return Ok(SinkPoll::StopPipeline);
        }
        if input.is_empty() {
            return Ok(SinkPoll::NeedMoreInput);
        }
        let deleted = if let Some(pk_cols) = primary_key_columns(&self.spec.table) {
            let mut key_vectors = Vec::with_capacity(pk_cols.len());
            for idx in pk_cols {
                let col = input.column(idx).ok_or_else(|| {
                    paro_error::internal(format!("primary key column {} not found", idx))
                })?;
                key_vectors.push(col.clone());
            }
            let key_chunk = Chunk::from_arc_vectors(key_vectors, input.allocator().clone());
            storage.delete_by_primary_keys(&ctx.query.transaction, &key_chunk, txn.clone())?
        } else {
            let row_ids = collect_row_ids(input, self.spec.row_id_index, true)?;
            storage.delete(&ctx.query.transaction, &row_ids, txn.clone())?
        };
        global
            .affected_count
            .fetch_add(deleted as u64, Ordering::SeqCst);
        if deleted > 0 {
            txn.record_graph_delete(self.spec.table.base.base.object_id.raw(), deleted);
        }
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &SinkGlobal,
        _local: &mut SinkLocal,
    ) -> Result<MergePoll> {
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
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let count = dml_global(global)?.affected_count.load(Ordering::SeqCst);
        Ok(FinishPoll::DoneWithResult(dml_result_chunk(ctx, count)?))
    }
}
