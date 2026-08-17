// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::Vector;
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
        if let Some(aggregates) = snapshot.global_aggregates() {
            loop {
                if local.external_cursor.is_some() {
                    if local.global_external_chunk.is_none() {
                        local.global_external_chunk = Some(Chunk::try_initialize(
                            &self.spec.output_types[..self.spec.detail_column_count()],
                            paro_common::vector::VECTOR_SIZE,
                            output.allocator().clone(),
                        )?);
                    }
                    let count = local
                        .external_cursor
                        .as_mut()
                        .expect("global external cursor checked above")
                        .next_chunk(
                            local
                                .global_external_chunk
                                .as_mut()
                                .expect("global external detail chunk initialized above"),
                        )?;
                    if count > 0 {
                        prepare_projected_detail_output(
                            &self.spec,
                            local
                                .global_external_chunk
                                .as_ref()
                                .expect("global external detail chunk initialized above"),
                            output,
                        )?;
                        append_global_aggregates(&self.spec, aggregates, output)?;
                        return Ok(SourcePoll::Output);
                    }
                    local.external_cursor = None;
                }
                let batch_index = global.claim_batch();
                if let Some((payload, aggregates)) = snapshot.global_batch(batch_index) {
                    prepare_detail_output(&self.spec, payload, output)?;
                    append_global_aggregates(&self.spec, aggregates, output)?;
                    return Ok(SourcePoll::Output);
                }
                let Some(store) = snapshot.take_global_external_payload(batch_index) else {
                    output.try_set_cardinality(0)?;
                    return Ok(SourcePoll::Finished);
                };
                local.external_cursor = Some(store.into_reclaimable().into_reclaiming_scanner());
            }
        }
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
        prepare_detail_output(&self.spec, payload, output)?;
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

fn append_global_aggregates(
    spec: &PartitionAggregateWindowSpec,
    aggregates: &[paro_common::runtime_value::Value],
    output: &mut Chunk,
) -> Result<()> {
    if aggregates.len() != spec.aggregate_column_count() {
        return Err(paro_error::internal(format!(
            "global aggregate window result width mismatch: expected={}, actual={}",
            spec.aggregate_column_count(),
            aggregates.len()
        )));
    }
    if output.column_count() != spec.output_types.len()
        || output.types().as_slice() != spec.output_types.as_ref()
    {
        return Err(paro_error::internal(
            "global aggregate detail output was not prepared",
        ));
    }
    for (offset, value) in aggregates.iter().enumerate() {
        let target = spec.detail_column_count() + offset;
        output.data[target] = Arc::new(Vector::try_constant_from_value(
            spec.output_types[target].clone(),
            value.clone(),
            output.size(),
            output.allocator().clone(),
        )?);
    }
    Ok(())
}

fn prepare_projected_detail_output(
    spec: &PartitionAggregateWindowSpec,
    payload: &Chunk,
    output: &mut Chunk,
) -> Result<()> {
    ensure_source_output(output, &spec.output_types, payload.size().max(1))?;
    output.try_reset_writable_suffix(spec.detail_column_count(), output.allocator().clone())?;
    if payload.column_count() != spec.detail_column_count() {
        return Err(paro_error::internal(
            "global external detail payload width mismatch",
        ));
    }
    for (target, source) in output.data.iter_mut().zip(&payload.data) {
        *target = Arc::clone(source);
    }
    output.try_set_cardinality(payload.size())
}

fn prepare_detail_output(
    spec: &PartitionAggregateWindowSpec,
    payload: &Chunk,
    output: &mut Chunk,
) -> Result<()> {
    ensure_source_output(output, &spec.output_types, payload.size().max(1))?;
    output.try_reset_writable_suffix(spec.detail_column_count(), output.allocator().clone())?;
    for (target, &source_index) in output.data.iter_mut().zip(spec.detail_columns.iter()) {
        *target = Arc::clone(payload.column(source_index).ok_or_else(|| {
            paro_error::internal("partition aggregate payload detail column is missing")
        })?);
    }
    output.try_set_cardinality(payload.size())
}
