// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_function::scalar::FunctionExecContext;
use paro_planner::expression::Expression;

use crate::expression_executor::rows::ExpressionRowsExecutor;
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;

#[derive(Debug)]
pub struct ExpressionRowsSourceLocal {
    pub cursor: usize,
    evaluator: ExpressionRowsExecutor,
}

impl ExpressionRowsSourceLocal {
    pub fn try_new(
        ctx: &PipelineInitContext<'_>,
        rows: &[Box<[Expression]>],
        output_types: &[LogicalType],
    ) -> Result<Self> {
        Ok(Self {
            cursor: 0,
            evaluator: ExpressionRowsExecutor::try_new(
                rows,
                output_types,
                ctx.query.session.as_ref(),
                ctx.query.allocator(MemoryTag::BaseTable),
            )?,
        })
    }
}

pub(crate) fn poll_expression_rows(
    ctx: &mut OperatorCallContext,
    rows: &[Box<[Expression]>],
    local: &mut ExpressionRowsSourceLocal,
    output: &mut Chunk,
) -> Result<SourcePoll> {
    if local.cursor >= local.evaluator.row_count() {
        return Ok(SourcePoll::Finished);
    }

    let remaining = local.evaluator.row_count() - local.cursor;
    let batch_size = remaining.min(output.capacity().max(1));
    local.evaluator.execute_batch(
        local.cursor,
        batch_size,
        rows,
        ctx.query.params.as_ref(),
        ctx.query,
        output,
    )?;
    local.cursor += batch_size;
    Ok(SourcePoll::Output)
}
