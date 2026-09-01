// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VectorSelection, VECTOR_SIZE};
use paro_storage::row::RowStore;

use crate::runtime::breaker::{HandleRef, MaterializedHandle, MaterializedReader};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{CrossProductProbeTransformLocal, TransformGlobal, TransformLocal};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};

#[derive(Debug, Clone)]
pub struct CrossProductProbeTransformExec {
    pub handle: HandleRef<MaterializedHandle>,
    pub left_column_count: usize,
    pub output_types: Box<[LogicalType]>,
}

impl CrossProductProbeTransformExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        Ok(TransformGlobal::CrossProductProbe(Arc::new(
            MaterializedReader::new(ctx.handles.get(self.handle)?, "cross product probe"),
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        Ok(TransformLocal::CrossProductProbe(
            CrossProductProbeTransformLocal::default(),
        ))
    }

    pub(crate) fn transform(
        &self,
        ctx: &mut OperatorCallContext,
        global: &TransformGlobal,
        local: &mut TransformLocal,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        ctx.cancel.check()?;
        let TransformGlobal::CrossProductProbe(global) = global else {
            return Err(paro_error::internal(
                "cross product probe transform global state mismatch",
            ));
        };
        let TransformLocal::CrossProductProbe(local) = local else {
            return Err(paro_error::internal(
                "cross product probe transform local state mismatch",
            ));
        };
        if input.is_empty() {
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        }
        if self.left_column_count != input.column_count() {
            return Err(paro_error::internal(
                "cross product probe left column count does not match input",
            ));
        }
        let Some(right_types) = self.output_types.get(self.left_column_count..) else {
            return Err(paro_error::internal(
                "cross product output has fewer columns than its left input",
            ));
        };

        if let Some(stores) = global.external_row_stores()? {
            return transform_external_cross_product(
                input,
                output,
                local,
                stores,
                self.left_column_count,
                right_types,
            );
        }

        let build_chunks = global.sealed_chunks()?;
        if build_chunks.iter().all(Chunk::is_empty) {
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        }
        if right_types.len() != right_build_column_count(build_chunks.as_ref()) {
            return Err(paro_error::internal(
                "cross product probe output type count does not match input and build columns",
            ));
        }

        if !local.probe_in_progress {
            local.probe_row = 0;
            local.build_chunk = 0;
            local.build_row = 0;
            local.probe_in_progress = true;
        }

        loop {
            if local.probe_row >= input.size() {
                local.probe_in_progress = false;
                output.try_set_cardinality(0)?;
                return Ok(TransformPoll::NeedMoreInput);
            }

            while let Some(chunk) = build_chunks.get(local.build_chunk) {
                if local.build_row < chunk.size() {
                    break;
                }
                local.build_chunk += 1;
                local.build_row = 0;
            }

            let Some(build_chunk) = build_chunks.get(local.build_chunk) else {
                local.probe_row += 1;
                local.build_chunk = 0;
                local.build_row = 0;
                continue;
            };

            let remaining = build_chunk.size() - local.build_row;
            let count = remaining.min(VECTOR_SIZE);
            emit_cross_product_batch(
                input,
                build_chunk,
                self.left_column_count,
                local.probe_row,
                local.build_row,
                count,
                output,
            )?;

            local.build_row += count;
            if local.build_row >= build_chunk.size() {
                local.build_chunk += 1;
                local.build_row = 0;
            }
            if local.build_chunk >= build_chunks.len() {
                local.probe_row += 1;
                local.build_chunk = 0;
                local.build_row = 0;
            }

            if local.probe_row >= input.size() {
                local.probe_in_progress = false;
                return Ok(TransformPoll::Output);
            }
            return Ok(TransformPoll::OutputMore);
        }
    }

    pub(crate) fn flush(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        _local: &mut TransformLocal,
        _output: &mut Chunk,
    ) -> Result<TransformFlushPoll> {
        Ok(TransformFlushPoll::Done)
    }

    pub(crate) fn finish_global(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &TransformGlobal,
    ) -> Result<TransformFinishPoll> {
        Ok(TransformFinishPoll::Done)
    }
}

fn transform_external_cross_product(
    input: &Chunk,
    output: &mut Chunk,
    local: &mut CrossProductProbeTransformLocal,
    stores: &[Arc<RowStore>],
    left_column_count: usize,
    right_types: &[LogicalType],
) -> Result<TransformPoll> {
    if stores.iter().all(|store| store.count() == 0) {
        output.try_set_cardinality(0)?;
        return Ok(TransformPoll::NeedMoreInput);
    }
    if stores
        .iter()
        .any(|store| store.layout().types() != right_types)
    {
        return Err(paro_error::internal(
            "external cross product build schema does not match its physical contract",
        ));
    }

    if !local.probe_in_progress {
        local.probe_row = 0;
        local.external_store = 0;
        local.external_scan.reset();
        local.probe_in_progress = true;
    }
    if local.external_chunk.is_none() {
        local.external_chunk = Some(Chunk::try_initialize(
            right_types,
            VECTOR_SIZE,
            input.allocator().clone(),
        )?);
    }

    loop {
        if local.probe_row >= input.size() {
            local.probe_in_progress = false;
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        }

        let Some(store) = stores.get(local.external_store) else {
            local.probe_row += 1;
            local.external_store = 0;
            local.external_scan.reset();
            continue;
        };
        let build = local
            .external_chunk
            .as_mut()
            .expect("external cross product scratch initialized above");
        let count = store.scan_with_state(&mut local.external_scan, build)?;
        if count == 0 {
            local.external_store += 1;
            local.external_scan.reset();
            continue;
        }

        emit_cross_product_batch(
            input,
            build,
            left_column_count,
            local.probe_row,
            0,
            count,
            output,
        )?;
        // A row-store scan is intentionally vector-at-a-time. Returning
        // OutputMore keeps both the retained working set and the output bounded
        // without requiring a cardinality estimate.
        return Ok(TransformPoll::OutputMore);
    }
}

fn right_build_column_count(build_chunks: &[Chunk]) -> usize {
    build_chunks
        .iter()
        .find(|chunk| !chunk.is_empty())
        .map(Chunk::column_count)
        .unwrap_or(0)
}

fn emit_cross_product_batch(
    input: &Chunk,
    build: &Chunk,
    left_column_count: usize,
    probe_row: usize,
    build_row: usize,
    count: usize,
    output: &mut Chunk,
) -> Result<()> {
    let allocator = input.allocator().clone();
    let left_selection = SelectionVector::try_repeated(probe_row, count, allocator.clone())?;
    let right_selection = VectorSelection::range(build_row, count);
    let mut vectors = Vec::with_capacity(left_column_count + build.column_count());

    for column in 0..left_column_count {
        vectors.push(Arc::new(Vector::try_dictionary(
            Arc::clone(&input.data[column]),
            left_selection.clone(),
        )?));
    }
    for column in 0..build.column_count() {
        vectors.push(Arc::new(Vector::try_gather_ref(
            Arc::clone(&build.data[column]),
            right_selection.clone(),
        )?));
    }

    *output = Chunk::from_arc_vectors(vectors, allocator);
    output.try_set_cardinality(count)?;
    Ok(())
}
