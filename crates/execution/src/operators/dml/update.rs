// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::Ordering;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;

use crate::physical::properties::RequiredProperties;
use crate::physical::specs::UpdateSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::state::{DmlSinkGlobal, EmptyDmlSinkLocal, SinkGlobal, SinkLocal};

use super::helpers::{
    active_transaction, bind_dml_write, collect_row_ids, collect_updated_column_values, dml_global,
    dml_result_chunk, storage_table,
};

#[derive(Debug, Clone)]
pub struct UpdateSinkExec {
    pub spec: UpdateSpec,
    pub required: RequiredProperties,
}

impl UpdateSinkExec {
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
        if input.is_empty() {
            return Ok(SinkPoll::NeedMoreInput);
        }
        let storage = storage_table(&self.spec.table)?;
        let column_values = collect_updated_column_values(input, &self.spec.columns)?;
        let row_ids = collect_row_ids(input, self.spec.row_id_index, false)?;
        let txn = active_transaction(ctx, "UPDATE")?;
        let updated = storage.update(
            &ctx.query.transaction,
            &row_ids,
            &self.spec.columns,
            &column_values,
            txn.clone(),
        )?;
        dml_global(global)?
            .affected_count
            .fetch_add(updated as u64, Ordering::SeqCst);
        if updated > 0 {
            let updated_columns = self
                .spec
                .columns
                .iter()
                .filter_map(|idx| u32::try_from(*idx).ok())
                .collect::<Vec<_>>();
            txn.record_graph_update(
                self.spec.table.base.base.object_id.raw(),
                updated,
                &updated_columns,
            );
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
