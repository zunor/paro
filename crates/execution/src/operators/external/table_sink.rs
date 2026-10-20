// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::{Vector, VectorSelection};

use crate::operators::external::batching::SubmissionBatchPolicy;
use crate::operators::external::runtime_bridge::{ExternalRuntimeBridge, TableSubmission};
use crate::operators::external::table_state::TableOutputBatch;
use crate::physical::specs::ExternalTableSpec;
use crate::runtime::breaker::{ExternalTableHandle, HandleRef};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::state::{
    ExternalTableSinkGlobal, ExternalTableSinkLocal, SinkGlobal, SinkLocal,
};

#[derive(Debug, Clone)]
pub struct ExternalTableSinkExec {
    pub handle: HandleRef<ExternalTableHandle>,
    pub spec: ExternalTableSpec,
    pub bridge: Arc<ExternalRuntimeBridge>,
}

impl ExternalTableSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        let handle = ctx.handles.get(self.handle)?;
        Ok(SinkGlobal::ExternalTable(Arc::new(
            ExternalTableSinkGlobal { handle },
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        external_table_global(global)?;
        Ok(SinkLocal::ExternalTable(ExternalTableSinkLocal {
            next_batch_id: 1,
            next_partition_id: 1,
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
        let global = external_table_global(global)?;
        let local = external_table_local(local)?;
        // A correlated table call owns one outer row. Its result may contain
        // zero or many rows, so each call retains that row's provenance.
        if self.spec.emitted_output_types.len() > self.spec.worker_output_types.len() {
            for row in 0..input.size() {
                ctx.query.cancellation.check()?;
                let mut single = input.clone_referencing_vectors();
                single.try_slice_range(row, 1)?;
                self.execute_batch(ctx, global, local, &single)?;
            }
        } else {
            self.execute_batch(ctx, global, local, input)?;
        }
        Ok(SinkPoll::NeedMoreInput)
    }

    fn execute_batch(
        &self,
        ctx: &mut OperatorCallContext,
        global: &ExternalTableSinkGlobal,
        local: &mut ExternalTableSinkLocal,
        input: &Chunk,
    ) -> Result<()> {
        let batch_id = local.next_batch_id;
        let submission = TableSubmission {
            batch_id,
            input,
            routine: &self.spec.routine,
            output_types: &self.spec.worker_output_types,
            lateral: self.spec.lateral,
            parameterized: self.spec.parameterized,
        };
        local.next_batch_id = local.next_batch_id.saturating_add(1);
        let response = self
            .bridge
            .execute_table(ctx.query, &submission, &ctx.memory)?;
        let mut batches = Vec::with_capacity(response.output_batches.len());
        for mut chunk in response.output_batches {
            append_passthrough_columns(&self.spec, input, &mut chunk)?;
            let bytes = SubmissionBatchPolicy::estimate_chunk_bytes(&chunk);
            batches.push(TableOutputBatch {
                chunk,
                bytes,
                partition_id: local.next_partition_id,
                partition_end: false,
            });
            local.next_partition_id = local.next_partition_id.saturating_add(1);
        }
        global.handle.shared().enqueue_output_batches(batches);
        Ok(())
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
        _ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        external_table_global(global)?
            .handle
            .shared()
            .mark_finalized();
        Ok(FinishPoll::Done)
    }
}

fn append_passthrough_columns(
    spec: &ExternalTableSpec,
    input: &Chunk,
    output: &mut Chunk,
) -> Result<()> {
    let worker_width = spec.worker_output_types.len();
    let emitted_width = spec.emitted_output_types.len();
    if emitted_width < worker_width {
        return Err(paro_error::internal(format!(
            "external table emitted width {emitted_width} is smaller than worker width {worker_width}"
        )));
    }
    let passthrough_count = emitted_width - worker_width;
    if passthrough_count == 0 {
        return Ok(());
    }
    if input.size() != 1 {
        return Err(paro_error::contract_violation(format!(
            "correlated table invocation requires one parameter row, got {}",
            input.size()
        )));
    }
    let first_passthrough = spec.argument_count;
    let required_input_width = first_passthrough + passthrough_count;
    if input.column_count() < required_input_width {
        return Err(paro_error::internal(format!(
            "external table input has {} columns, but {} argument columns plus {passthrough_count} pass-through columns are required",
            input.column_count(),
            spec.argument_count
        )));
    }

    for offset in 0..passthrough_count {
        let source_idx = first_passthrough + offset;
        let expected_type = &spec.emitted_output_types[worker_width + offset];
        let column = input.column(source_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "external table pass-through column {source_idx} is out of bounds"
            ))
        })?;
        if column.logical_type() != expected_type {
            return Err(paro_error::internal(format!(
                "external table pass-through column {source_idx} has type {:?}, expected {:?}",
                column.logical_type(),
                expected_type
            )));
        }
        let repeated = Vector::try_gather_ref(
            Arc::clone(column),
            VectorSelection::Repeated {
                index: 0,
                count: output.size(),
            },
        )?;
        output.try_push_column(Arc::new(repeated), output.size())?;
    }
    Ok(())
}

#[inline(always)]
fn external_table_global(global: &SinkGlobal) -> Result<&ExternalTableSinkGlobal> {
    match global {
        SinkGlobal::ExternalTable(state) => Ok(state.as_ref()),
        _ => Err(paro_error::internal(
            "external table sink global state mismatch",
        )),
    }
}

#[inline(always)]
fn external_table_local(local: &mut SinkLocal) -> Result<&mut ExternalTableSinkLocal> {
    match local {
        SinkLocal::ExternalTable(state) => Ok(state),
        _ => Err(paro_error::internal(
            "external table sink local state mismatch",
        )),
    }
}
