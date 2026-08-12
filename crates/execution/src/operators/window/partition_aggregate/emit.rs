// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_function::scalar::FunctionExecContext;

use crate::operators::aggregate::build_helpers::{build_groups_chunk, group_payload_refs};
use crate::operators::output::ensure_source_output;
use crate::physical::specs::PartitionAggregateWindowSpec;
use crate::runtime::breaker::{HandleRef, PartitionAggregateWindowHandle};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{SourceGlobal, SourceLocal};

use super::state::{
    emit_global, emit_local_mut, PartitionAggregateEmitGlobal, PartitionAggregateEmitLocal,
};

#[derive(Debug, Clone)]
pub struct PartitionAggregateWindowEmitSourceExec {
    pub handle: HandleRef<PartitionAggregateWindowHandle>,
    pub spec: PartitionAggregateWindowSpec,
}

impl PartitionAggregateWindowEmitSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        self.spec.verify()?;
        Ok(SourceGlobal::PartitionAggregateWindowEmit(Arc::new(
            PartitionAggregateEmitGlobal::new(ctx.handles.get(self.handle)?),
        )))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::PartitionAggregateWindowEmit(
            PartitionAggregateEmitLocal::try_new(
                ctx.query
                    .allocator(paro_common::allocator::MemoryTag::Window),
            )?,
        ))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let global = emit_global(global)?;
        let snapshot = Arc::clone(global.snapshot()?);
        let local = emit_local_mut(local)?;
        if snapshot.is_external() {
            loop {
                if let Some(cursor) = local.external_cursor.as_mut() {
                    let count = cursor.next_chunk(output)?;
                    if count > 0 {
                        return Ok(SourcePoll::Output);
                    }
                    local.external_cursor = None;
                }
                let partition_index = global.claim_batch();
                let Some(store) = snapshot.take_output(partition_index) else {
                    output.try_set_cardinality(0)?;
                    return Ok(SourcePoll::Finished);
                };
                local.external_cursor = Some(store.into_reclaimable().into_reclaiming_scanner());
            }
        }
        let batch_index = global.claim_batch();
        let Some((payload, index)) = snapshot.in_memory_batch(batch_index) else {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        };
        ensure_source_output(output, &self.spec.output_types, payload.size().max(1))?;
        output.try_reset_writable_suffix(
            self.spec.detail_column_count(),
            output.allocator().clone(),
        )?;
        for (target, &source_index) in output.data.iter_mut().zip(self.spec.detail_columns.iter()) {
            *target = Arc::clone(payload.column(source_index).ok_or_else(|| {
                paro_error::internal("partition aggregate payload detail column is missing")
            })?);
        }
        output.try_set_cardinality(payload.size())?;
        let group_refs = group_payload_refs(&self.spec.aggregate)?;
        let keys = build_groups_chunk(payload, &group_refs)?;
        index.attach_aggregates(
            &keys,
            &mut local.aggregate_selection,
            output,
            self.spec.detail_column_count(),
        )?;
        Ok(SourcePoll::Output)
    }
}
