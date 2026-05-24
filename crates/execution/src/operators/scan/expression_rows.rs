// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_function::scalar::FunctionExecContext;

use crate::expression_executor::executor::ExpressionExecutor;
use crate::runtime::context::OperatorCallContext;
use crate::runtime::source::SourcePoll;
use crate::runtime::ExpressionEvalInput;

pub(crate) fn poll_expression_rows(
    ctx: &mut OperatorCallContext,
    rows: &[Box<[paro_planner::expression::Expression]>],
    output_types: &[LogicalType],
    cursor: &mut usize,
    scalar_scratch: &mut Vec<Vector>,
    output: &mut Chunk,
    label: &'static str,
) -> Result<SourcePoll> {
    if *cursor >= rows.len() {
        return Ok(SourcePoll::Finished);
    }

    let remaining = rows.len() - *cursor;
    let batch_size = remaining.min(output.capacity().max(1));
    if output.column_count() != output_types.len() || output.capacity() < batch_size {
        *output = Chunk::try_initialize(
            output_types,
            batch_size.max(1),
            ctx.query.allocator(MemoryTag::BaseTable),
        )?;
    } else {
        output.try_reset(output.allocator().clone())?;
    }
    output.try_set_cardinality(batch_size)?;

    let mut dummy = Chunk::try_initialize(&[], 1, ctx.query.allocator(MemoryTag::BaseTable))?;
    dummy.try_set_cardinality(1)?;
    prepare_scalar_scratch(
        scalar_scratch,
        output_types,
        ctx.query.allocator(MemoryTag::BaseTable),
    )?;

    for output_row in 0..batch_size {
        let row = &rows[*cursor + output_row];
        if row.len() != output_types.len() {
            return Err(paro_error::internal(format!(
                "{label} row has {} expressions but output has {} columns",
                row.len(),
                output_types.len()
            )));
        }
        let mut executor = ExpressionExecutor::with_expressions(row);
        for expr_idx in 0..row.len() {
            let vector = &mut scalar_scratch[expr_idx];
            if vector.logical_type() != &row[expr_idx].return_type() {
                *vector = Vector::try_new(
                    row[expr_idx].return_type(),
                    1,
                    ctx.query.allocator(MemoryTag::BaseTable),
                )?;
            } else {
                vector.try_reset_for_execution(1, ctx.query.allocator(MemoryTag::BaseTable))?;
            }
            vector.try_set_count(1)?;
            executor.execute_into_with_input(
                expr_idx,
                ExpressionEvalInput {
                    params: ctx.query.params.as_ref(),
                    columns: &dummy,
                },
                None,
                1,
                ctx.query,
                vector,
            )?;
            let value = vector.get_value(0);
            output
                .set_value(expr_idx, output_row, &value)
                .ok_or_else(|| paro_error::internal(format!("failed to write {label} result")))?;
        }
    }

    *cursor += batch_size;
    Ok(SourcePoll::Output)
}

fn prepare_scalar_scratch(
    scratch: &mut Vec<Vector>,
    output_types: &[LogicalType],
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    if scratch.len() != output_types.len() {
        scratch.clear();
        scratch.reserve(output_types.len());
        for ty in output_types {
            scratch.push(Vector::try_new(ty.clone(), 1, allocator.clone())?);
        }
        return Ok(());
    }

    for (vector, ty) in scratch.iter_mut().zip(output_types) {
        if vector.logical_type() != ty {
            *vector = Vector::try_new(ty.clone(), 1, allocator.clone())?;
        }
    }
    Ok(())
}
