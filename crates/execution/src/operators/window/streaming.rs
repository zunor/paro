// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_function::window::WindowFunctionType;

use crate::physical::specs::WindowSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{
    StreamingWindowTransformGlobal, StreamingWindowTransformLocal, TransformGlobal, TransformLocal,
};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};

#[derive(Debug, Clone)]
pub struct StreamingWindowTransformExec {
    pub spec: WindowSpec,
}

impl StreamingWindowTransformExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        ensure_streaming_window_supported(&self.spec)?;
        Ok(TransformGlobal::StreamingWindow(Arc::new(
            StreamingWindowTransformGlobal,
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        Ok(TransformLocal::StreamingWindow(
            StreamingWindowTransformLocal { next_row_number: 1 },
        ))
    }

    pub(crate) fn transform(
        &self,
        ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        local: &mut TransformLocal,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        ctx.cancel.check()?;
        let TransformLocal::StreamingWindow(local) = local else {
            return Err(paro_error::internal(
                "streaming window transform local state mismatch",
            ));
        };
        if input.is_empty() {
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        }

        let mut vectors = Vec::with_capacity(self.spec.output_types.len());
        vectors.extend(input.data.iter().cloned());
        // The row-number vector is output-owned and may be retained by a sink,
        // so it cannot be reused from local state safely. Fill it directly and
        // skip the old temporary Vec<i64> allocation.
        let mut row_number = Vector::try_new(
            LogicalType::BigInt,
            input.size().max(1),
            input.allocator().clone(),
        )?;
        row_number.try_set_count(input.size())?;
        unsafe {
            let data = row_number.flat_data_mut::<i64>();
            for offset in 0..input.size() {
                *data.add(offset) = local.next_row_number + offset as i64;
            }
        }
        let row_number = Arc::new(row_number);
        for _ in &self.spec.expressions {
            vectors.push(Arc::clone(&row_number));
        }
        local.next_row_number = local.next_row_number.saturating_add(input.size() as i64);
        *output = Chunk::from_arc_vectors(vectors, input.allocator().clone());
        output.try_set_cardinality(input.size())?;
        Ok(TransformPoll::Output)
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

fn ensure_streaming_window_supported(spec: &WindowSpec) -> Result<()> {
    let supported = spec.expressions.iter().all(|expr| {
        expr.function.function_type == WindowFunctionType::RowNumber
            && expr.children.is_empty()
            && expr.partitions.is_empty()
            && expr.orders.is_empty()
    });
    if supported {
        Ok(())
    } else {
        Err(paro_error::not_implemented(
            "streaming window currently supports ROW_NUMBER() without PARTITION BY or ORDER BY",
        ))
    }
}
