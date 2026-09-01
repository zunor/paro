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
        if let Some(build) = singleton_build_chunk(build_chunks.as_ref()) {
            emit_scalar_build_batch(input, build, self.left_column_count, output)?;
            return Ok(TransformPoll::Output);
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
        local.external_chunk_ready = false;
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
        let build = local
            .external_chunk
            .as_mut()
            .expect("external cross product scratch initialized above");
        if !local.external_chunk_ready {
            loop {
                let Some(store) = stores.get(local.external_store) else {
                    local.probe_in_progress = false;
                    output.try_set_cardinality(0)?;
                    return Ok(TransformPoll::NeedMoreInput);
                };
                let count = store.scan_with_state(&mut local.external_scan, build)?;
                if count > 0 {
                    local.external_chunk_ready = true;
                    local.probe_row = 0;
                    break;
                }
                local.external_store += 1;
                local.external_scan.reset();
            }
        }

        let count = build.size();
        if count == 1 {
            emit_scalar_build_batch(input, build, left_column_count, output)?;
            local.external_chunk_ready = false;
            local.probe_row = 0;
            return Ok(TransformPoll::OutputMore);
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
        local.probe_row += 1;
        if local.probe_row >= input.size() {
            // Reuse each external build block for the whole probe vector
            // before advancing the disk cursor. This changes external cross
            // product I/O from one full build scan per probe row to one scan
            // per probe chunk while retaining vector-bounded output.
            local.external_chunk_ready = false;
            local.probe_row = 0;
        }
        return Ok(TransformPoll::OutputMore);
    }
}

fn singleton_build_chunk(build_chunks: &[Chunk]) -> Option<&Chunk> {
    let mut singleton = None;
    for chunk in build_chunks.iter().filter(|chunk| !chunk.is_empty()) {
        if chunk.size() != 1 || singleton.is_some() {
            return None;
        }
        singleton = Some(chunk);
    }
    singleton
}

fn emit_scalar_build_batch(
    input: &Chunk,
    build: &Chunk,
    left_column_count: usize,
    output: &mut Chunk,
) -> Result<()> {
    if build.size() != 1 {
        return Err(paro_error::internal(
            "scalar cross product build must contain exactly one row",
        ));
    }
    let allocator = input.allocator().clone();
    let scalar_selection = SelectionVector::try_repeated(0, input.size(), allocator.clone())?;
    let mut vectors = Vec::with_capacity(left_column_count + build.column_count());
    vectors.extend(input.data.iter().take(left_column_count).cloned());
    for column in 0..build.column_count() {
        vectors.push(Arc::new(Vector::try_dictionary(
            Arc::clone(&build.data[column]),
            scalar_selection.clone(),
        )?));
    }
    *output = Chunk::from_arc_vectors(vectors, allocator);
    output.try_set_cardinality(input.size())?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::test_utils::{test_allocator, test_chunk_from_vectors};

    fn integers(values: &[i32]) -> Chunk {
        test_chunk_from_vectors(vec![
            Vector::try_from_i32(values, test_allocator()).expect("integer vector")
        ])
    }

    #[test]
    fn singleton_build_is_broadcast_over_the_probe_vector() {
        let input = integers(&[10, 20, 30]);
        let build = integers(&[7]);
        let mut output = Chunk::try_new(test_allocator()).expect("output chunk");

        emit_scalar_build_batch(&input, &build, 1, &mut output).expect("scalar broadcast");

        assert_eq!(output.size(), 3);
        assert_eq!(output.column(0).unwrap().get_i32(0), Some(10));
        assert_eq!(output.column(0).unwrap().get_i32(2), Some(30));
        assert_eq!(output.column(1).unwrap().get_i32(0), Some(7));
        assert_eq!(output.column(1).unwrap().get_i32(2), Some(7));
    }

    #[test]
    fn singleton_detection_rejects_multiple_nonempty_chunks() {
        let chunks = vec![integers(&[1]), integers(&[2])];
        assert!(singleton_build_chunk(&chunks).is_none());
    }
}
