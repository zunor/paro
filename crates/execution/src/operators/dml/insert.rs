// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::Ordering;
use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_function::scalar::FunctionExecContext;
use paro_planner::operator::InsertOnConflictAction;
use paro_storage::table::table_handle::InsertOnConflictAction as StorageInsertOnConflictAction;

use crate::physical::properties::RequiredProperties;
use crate::physical::specs::InsertSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::state::{DmlSinkGlobal, InsertSinkLocal, SinkGlobal, SinkLocal};

use super::helpers::{
    active_transaction, bind_dml_write, dml_global, dml_result_chunk, flush_insert_buffered_chunks,
    initialize_insert_buffering, insert_local, storage_table, DEFAULT_COPY_BUFFER_SIZE,
    DEFAULT_COPY_FLUSH_THREADS,
};

#[derive(Debug, Clone)]
pub struct InsertSinkExec {
    pub spec: InsertSpec,
    pub required: RequiredProperties,
}

impl InsertSinkExec {
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
        Ok(SinkLocal::Insert(InsertSinkLocal {
            copy_buffer_size: DEFAULT_COPY_BUFFER_SIZE,
            copy_flush_threads: DEFAULT_COPY_FLUSH_THREADS,
            ..Default::default()
        }))
    }

    pub(crate) fn consume(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
        input: &mut Chunk,
    ) -> Result<SinkPoll> {
        if input.is_empty() {
            return Ok(SinkPoll::NeedMoreInput);
        }
        let global = dml_global(global)?;
        let local = insert_local(local)?;
        initialize_insert_buffering(ctx, &self.spec, local)?;
        let storage = storage_table(&self.spec.table)?;
        let table_types = storage.types();
        let mut append_chunk = Chunk::try_initialize(
            table_types,
            input.size(),
            ctx.query.allocator(MemoryTag::Allocator),
        )?;
        for (input_idx, table_idx) in self.spec.column_index_map.iter().enumerate() {
            append_chunk.data[*table_idx] = input.data[input_idx].clone();
        }
        append_chunk.try_set_cardinality(input.size())?;
        let append_chunk = append_chunk.try_deep_copy(ctx.query.allocator(MemoryTag::MemTable))?;
        let txn = active_transaction(ctx, "INSERT")?;
        let mut affected_rows = input.size();
        if let Some(on_conflict) = &self.spec.on_conflict {
            if local.copy_buffering_enabled {
                return Err(paro_error::not_implemented(
                    "COPY FROM does not support ON CONFLICT yet",
                ));
            }
            let storage_action = match &on_conflict.action {
                InsertOnConflictAction::DoNothing => StorageInsertOnConflictAction::DoNothing,
                InsertOnConflictAction::DoUpdate {
                    target_columns,
                    source_columns,
                } => StorageInsertOnConflictAction::DoUpdate {
                    target_columns: target_columns.clone(),
                    source_columns: source_columns.clone(),
                },
            };
            let _guard = global
                .append_lock
                .lock()
                .map_err(|error| paro_error::internal(error.to_string()))?;
            affected_rows = storage.insert_on_conflict(
                &ctx.query.transaction,
                &append_chunk,
                &storage_action,
                txn.clone(),
            )?;
            global
                .affected_count
                .fetch_add(affected_rows as u64, Ordering::SeqCst);
        } else if local.copy_buffering_enabled {
            local.buffered_rows += append_chunk.size();
            local.buffered_chunks.push(append_chunk);
            if local.buffered_rows >= local.copy_buffer_size {
                let _guard = global
                    .append_lock
                    .lock()
                    .map_err(|error| paro_error::internal(error.to_string()))?;
                let flushed = flush_insert_buffered_chunks(ctx, &storage, txn.clone(), local)?;
                global
                    .affected_count
                    .fetch_add(flushed as u64, Ordering::SeqCst);
            }
        } else {
            storage.append_with_transaction(&ctx.query.transaction, &append_chunk, txn.clone())?;
            global
                .affected_count
                .fetch_add(input.size() as u64, Ordering::SeqCst);
        }
        txn.record_graph_insert(self.spec.table.base.base.object_id.raw(), affected_rows);
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        let global = dml_global(global)?;
        let local = insert_local(local)?;
        if !local.copy_buffering_enabled || local.buffered_rows == 0 {
            return Ok(MergePoll::Done);
        }
        let storage = storage_table(&self.spec.table)?;
        let txn = active_transaction(ctx, "INSERT merge")?;
        let _guard = global
            .append_lock
            .lock()
            .map_err(|error| paro_error::internal(error.to_string()))?;
        let flushed = flush_insert_buffered_chunks(ctx, &storage, txn, local)?;
        global
            .affected_count
            .fetch_add(flushed as u64, Ordering::SeqCst);
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
