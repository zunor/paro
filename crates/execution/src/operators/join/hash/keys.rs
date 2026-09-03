// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Hash-join key type derivation and vectorized key evaluation.

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::scalar::FunctionExecContext;
use paro_planner::expression::Expression;
use paro_planner::operator::join::JoinCondition;
use std::sync::Arc;

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
    if let Some(vectors) = direct_join_key_vectors(input, conditions, key_types, side) {
        *slot = Some(Chunk::try_from_arc_vectors_with_cardinality(
            vectors,
            input.size(),
            input.allocator().clone(),
        )?);
        return Ok(());
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

/// Borrow physical input vectors when every join key is already a direct
/// reference. The returned chunk owns only `Arc` handles; computed/cast keys
/// continue through the expression executors and their writable scratch.
fn direct_join_key_vectors(
    input: &Chunk,
    conditions: &[JoinCondition],
    key_types: &[LogicalType],
    side: JoinKeySide,
) -> Option<Vec<Arc<paro_common::vector::Vector>>> {
    conditions
        .iter()
        .zip(key_types)
        .map(|(condition, key_type)| {
            let expression = match side {
                JoinKeySide::Probe => &condition.left,
                JoinKeySide::Build => &condition.right,
            };
            let Expression::Reference(reference) = expression else {
                return None;
            };
            let vector = input.column(reference.index)?;
            (vector.logical_type() == key_type && &reference.return_type == key_type)
                .then(|| Arc::clone(vector))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_planner::expression::{ConstantExpression, ReferenceExpression};
    use paro_planner::operator::join::JoinComparisonType;

    fn equality(left: Expression, right: Expression) -> JoinCondition {
        JoinCondition::new(left, right, JoinComparisonType::Equal)
    }

    #[test]
    fn direct_reference_keys_borrow_input_vectors() {
        let allocator = paro_common::test_utils::test_allocator();
        let mut input =
            Chunk::try_initialize(&[LogicalType::Integer, LogicalType::BigInt], 2, allocator)
                .unwrap();
        input.try_set_cardinality(2).unwrap();
        let conditions = [equality(
            Expression::Reference(ReferenceExpression::new(1, LogicalType::BigInt)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt)),
        )];

        let vectors = direct_join_key_vectors(
            &input,
            &conditions,
            &[LogicalType::BigInt],
            JoinKeySide::Probe,
        )
        .expect("direct key projection");
        assert!(Arc::ptr_eq(&vectors[0], input.column(1).unwrap()));
    }

    #[test]
    fn computed_key_keeps_expression_execution_path() {
        let allocator = paro_common::test_utils::test_allocator();
        let input = Chunk::try_initialize(&[LogicalType::Integer], 1, allocator).unwrap();
        let conditions = [equality(
            Expression::Constant(ConstantExpression::new(
                Value::Integer(7),
                LogicalType::Integer,
            )),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        )];

        assert!(direct_join_key_vectors(
            &input,
            &conditions,
            &[LogicalType::Integer],
            JoinKeySide::Probe,
        )
        .is_none());
    }
}
