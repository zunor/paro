// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Hash-join key type derivation and vectorized key evaluation.

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::scalar::FunctionExecContext;
use paro_planner::operator::join::JoinCondition;

use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::runtime::context::OperatorCallContext;
use crate::runtime::ExpressionEvalInput;

#[derive(Debug, Clone, Copy)]
pub(crate) enum JoinKeySide {
    Probe,
    Build,
}

pub(crate) fn join_key_types(
    conditions: &[JoinCondition],
    side: JoinKeySide,
) -> Box<[LogicalType]> {
    conditions
        .iter()
        .map(|condition| match side {
            JoinKeySide::Probe => condition.left.return_type(),
            JoinKeySide::Build => condition.right.return_type(),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

pub(crate) fn evaluate_join_keys_into(
    ctx: &mut OperatorCallContext,
    input: &Chunk,
    conditions: &[JoinCondition],
    executors: &mut [ExpressionExecutor],
    key_types: &[LogicalType],
    side: JoinKeySide,
    slot: &mut Option<Chunk>,
) -> Result<()> {
    if conditions.len() != executors.len() {
        return Err(paro_error::internal(
            "hash join key executor count does not match condition count",
        ));
    }
    if conditions.len() != key_types.len() {
        return Err(paro_error::internal(
            "hash join key type count does not match condition count",
        ));
    }
    let required_capacity = input.size().max(1);
    let needs_new = slot.as_ref().map_or(true, |keys| {
        keys.column_count() != key_types.len()
            || keys.capacity() < required_capacity
            || keys
                .data
                .iter()
                .zip(key_types.iter())
                .any(|(vector, ty)| vector.logical_type() != ty)
    });
    if needs_new {
        *slot = Some(Chunk::try_initialize(
            key_types,
            required_capacity,
            ctx.query.allocator(MemoryTag::BaseTable),
        )?);
    }
    let keys = slot
        .as_mut()
        .expect("hash join key chunk was initialized above");
    keys.try_reset(ctx.query.allocator(MemoryTag::BaseTable))?;
    for (key_idx, (condition, executor)) in conditions.iter().zip(executors.iter_mut()).enumerate()
    {
        let logical_type = match side {
            JoinKeySide::Probe => condition.left.return_type(),
            JoinKeySide::Build => condition.right.return_type(),
        };
        let vector = keys.column_mut(key_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing hash join key vector while evaluating key {key_idx}"
            ))
        })?;
        if vector.logical_type() != &logical_type {
            return Err(paro_error::internal(format!(
                "hash join key type mismatch at key {key_idx}: expected={logical_type}, actual={}",
                vector.logical_type()
            )));
        }
        executor.execute_kernel_into(
            0,
            VectorKernelInput::from_eval_input(ExpressionEvalInput {
                params: ctx.query.params.as_ref(),
                columns: input,
            }),
            ctx.query,
            vector,
        )?;
    }
    keys.try_set_cardinality(input.size())?;
    Ok(())
}
