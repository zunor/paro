// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::vector::Vector;
use paro_function::scalar::FunctionExecContext;
use paro_planner::expression::Expression;

use crate::physical::specs::LimitSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{
    StreamingLimitTransformGlobal, StreamingLimitTransformLocal, TransformGlobal, TransformLocal,
};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};

#[derive(Debug, Clone)]
pub struct StreamingLimitTransformExec {
    pub spec: LimitSpec,
}

impl StreamingLimitTransformExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        Ok(TransformGlobal::StreamingLimit(Arc::new(
            StreamingLimitTransformGlobal {
                limit: optional_usize_constant(self.spec.limit.as_ref())?,
                offset: optional_usize_constant(self.spec.offset.as_ref())?.unwrap_or(0),
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        Ok(TransformLocal::StreamingLimit(
            StreamingLimitTransformLocal::default(),
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
        let TransformGlobal::StreamingLimit(global) = global else {
            return Err(paro_error::internal(
                "streaming limit transform global state mismatch",
            ));
        };
        let TransformLocal::StreamingLimit(local) = local else {
            return Err(paro_error::internal(
                "streaming limit transform local state mismatch",
            ));
        };

        let limit = global.limit.unwrap_or(usize::MAX);
        let offset = global.offset;
        let max_element = offset.saturating_add(limit);
        // Lowering forces streaming LIMIT into a single-task pipeline, so the
        // global row cursor is task-local and does not need an atomic.
        let current_offset = local.current_offset;
        local.current_offset = local.current_offset.saturating_add(input.size());
        if input.is_empty() || limit == 0 || current_offset >= max_element {
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::StopPipeline);
        }

        let Some((start, count)) =
            streaming_limit_slice(input.size(), current_offset, offset, limit)
        else {
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        };
        if count == 0 {
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        }

        if start == 0 && count == input.size() {
            output.reference(input);
        } else {
            let mut scratch = ctx.scratch.expr();
            let selection = scratch.selection(count, ctx.query.allocator(MemoryTag::BaseTable))?;
            for idx in 0..count {
                selection.set(idx, start + idx);
            }
            let mut vectors = Vec::with_capacity(input.column_count());
            for column in &input.data {
                vectors.push(Arc::new(Vector::try_dictionary(
                    Arc::clone(column),
                    &*selection,
                )?));
            }
            *output = Chunk::from_arc_vectors(vectors, input.allocator().clone());
            output.try_set_cardinality(count)?;
        }
        local.emitted = local.emitted.saturating_add(count);
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

fn optional_usize_constant(expr: Option<&Expression>) -> Result<Option<usize>> {
    let Some(expr) = expr else {
        return Ok(None);
    };
    let Expression::Constant(constant) = expr else {
        return Err(paro_error::not_implemented(
            "streaming limit parameter expressions are lowered in a later runtime phase",
        ));
    };
    match &constant.value {
        Value::Null(_) => Ok(None),
        value => {
            let Some(value) = value.as_i64() else {
                return Err(paro_error::internal(
                    "limit/offset expression is not integral",
                ));
            };
            if value < 0 {
                return Err(paro_error::invalid_input("limit/offset cannot be negative"));
            }
            Ok(Some(value as usize))
        }
    }
}

fn streaming_limit_slice(
    input_size: usize,
    current_offset: usize,
    offset: usize,
    limit: usize,
) -> Option<(usize, usize)> {
    let max_element = offset.saturating_add(limit);
    if current_offset < offset {
        if current_offset + input_size > offset {
            let start = offset - current_offset;
            let count = limit.min(input_size - start);
            Some((start, count))
        } else {
            None
        }
    } else {
        let count = if current_offset + input_size >= max_element {
            max_element - current_offset
        } else {
            input_size
        };
        Some((0, count))
    }
}
