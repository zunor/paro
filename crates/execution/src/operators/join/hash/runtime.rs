// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for the role-split hash join runtime.

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext, MemoryDomain};
use paro_common::types::LogicalType;
use std::sync::Arc;

use paro_common::vector::{SelectionVector, Vector};
use paro_function::scalar::FunctionExecContext;
use paro_planner::operator::join::{JoinCondition, JoinType};

use crate::expression_executor::executor::ExpressionExecutor;
use crate::join_hashtable::join_hashtable::JoinHashTable;
use crate::join_hashtable::scan_structure::ScanStructure;
use crate::operators::join::join_result_helpers::{
    construct_anti_join_result, construct_left_outer_result, construct_mark_join_result,
};
use crate::runtime::context::OperatorCallContext;
use crate::runtime::context::QueryRuntimeContext;
use crate::runtime::ExpressionEvalInput;

#[derive(Debug, Clone, Copy)]
pub(crate) enum JoinKeySide {
    Probe,
    Build,
}

pub(crate) fn hash_join_memory_context(query: &QueryRuntimeContext) -> MemoryAccountingContext {
    let owner: Arc<dyn paro_common::memory::MemoryOwner> = query.memory.clone();
    MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        MemoryTag::HashTable,
        MemoryAccountingClass::Revocable,
    )
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
        executor.execute_into_with_input(
            0,
            ExpressionEvalInput {
                params: ctx.query.params.as_ref(),
                columns: input,
            },
            None,
            input.size(),
            ctx.query,
            vector,
        )?;
    }
    keys.try_set_cardinality(input.size())?;
    Ok(())
}

pub(crate) fn build_payload_chunk_ref<'a>(
    input: &'a Chunk,
    projection: &[usize],
    output_types: &[LogicalType],
    slot: &'a mut Option<Chunk>,
) -> Result<&'a Chunk> {
    if output_types.is_empty() {
        if slot.as_ref().map_or(true, |payload| {
            payload.column_count() != 0 || payload.capacity() < input.size().max(1)
        }) {
            *slot = Some(Chunk::try_initialize(
                &[],
                input.size().max(1),
                input.allocator().clone(),
            )?);
        }
        let payload = slot
            .as_mut()
            .expect("hash join empty payload chunk was initialized above");
        payload.try_set_cardinality(input.size())?;
        return Ok(payload);
    }

    let projected_len = if projection.is_empty() {
        input.column_count()
    } else {
        projection.len()
    };
    if projected_len != output_types.len() {
        return Err(paro_error::internal(format!(
            "hash join build payload projection has {projected_len} columns but {} types",
            output_types.len()
        )));
    }

    let identity_projection = projection.is_empty()
        || (projection.len() == input.column_count()
            && projection
                .iter()
                .copied()
                .enumerate()
                .all(|(output_idx, input_idx)| output_idx == input_idx));
    if identity_projection {
        return Ok(input);
    }

    if slot.is_none() {
        *slot = Some(Chunk::try_new(input.allocator().clone())?);
    }
    let payload = slot
        .as_mut()
        .expect("hash join projected payload chunk was initialized above");
    payload.data.clear();
    payload.data.reserve(projection.len());
    for &column_idx in projection {
        payload
            .data
            .push(Arc::clone(input.data.get(column_idx).ok_or_else(|| {
                paro_error::internal("hash join build projection out of bounds")
            })?));
    }
    payload.set_capacity(input.size().max(1));
    payload.try_set_cardinality(input.size())?;
    Ok(payload)
}

pub(crate) fn compute_hashes_for_keys_into(
    hash_table: &JoinHashTable,
    keys: &Chunk,
    slot: &mut Option<Vector>,
) -> Result<()> {
    let required_capacity = keys.size().max(1);
    let needs_new = slot.as_ref().map_or(true, |hashes| {
        hashes.logical_type() != &LogicalType::UBigInt || hashes.capacity() < required_capacity
    });
    if needs_new {
        *slot = Some(Vector::try_new(
            LogicalType::UBigInt,
            required_capacity,
            keys.allocator().clone(),
        )?);
    }
    let hashes = slot
        .as_mut()
        .expect("hash join hash vector was initialized above");
    hash_table.compute_key_hashes(keys, hashes)?;
    Ok(())
}

pub(crate) fn build_probe_spill_chunk_into(
    input: &Chunk,
    hashes: &Vector,
    slot: &mut Option<Chunk>,
) -> Result<()> {
    if hashes.len() != input.size() {
        return Err(paro_error::internal(format!(
            "hash join probe hash vector cardinality mismatch: hashes={} input={}",
            hashes.len(),
            input.size()
        )));
    }
    if slot.is_none() {
        *slot = Some(Chunk::try_new(input.allocator().clone())?);
    }
    let chunk = slot
        .as_mut()
        .expect("hash join probe spill chunk was initialized above");
    chunk.data.clear();
    chunk.data.reserve(input.column_count().saturating_add(1));
    for col_idx in 0..input.column_count() {
        chunk
            .data
            .push(Arc::clone(input.column(col_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "missing input column while building hash join probe spill chunk: {col_idx}"
                ))
            })?));
    }
    chunk.data.push(Arc::new(hashes.clone()));
    chunk.set_capacity(input.size().max(1));
    chunk.try_set_cardinality(input.size())?;
    Ok(())
}

pub(crate) fn probe_input_from_spill_chunk_into(
    spill_chunk: &Chunk,
    probe_column_count: usize,
    slot: &mut Option<Chunk>,
) -> Result<()> {
    if spill_chunk.column_count() < probe_column_count {
        return Err(paro_error::internal(format!(
            "hash join probe spill chunk has {} columns but replay expects {probe_column_count}",
            spill_chunk.column_count()
        )));
    }
    if slot.is_none() {
        *slot = Some(Chunk::try_new(spill_chunk.allocator().clone())?);
    }
    let input = slot
        .as_mut()
        .expect("hash join replay input chunk was initialized above");
    input.data.clear();
    input.data.reserve(probe_column_count);
    input
        .data
        .extend(spill_chunk.data[..probe_column_count].iter().cloned());
    input.set_capacity(spill_chunk.size().max(1));
    input.try_set_cardinality(spill_chunk.size())?;
    Ok(())
}

pub(crate) fn scan_hash_join_results(
    join_type: JoinType,
    probe_keys: &Chunk,
    input: &Chunk,
    output: &mut Chunk,
    hash_table: &JoinHashTable,
    scan_structure: &mut ScanStructure,
    left_projection: &[usize],
    right_projection: &[usize],
) -> Result<usize> {
    match join_type {
        JoinType::Inner | JoinType::Right => scan_structure.next_inner_join(
            probe_keys,
            input,
            output,
            hash_table,
            left_projection,
            right_projection,
        ),
        JoinType::Left | JoinType::Outer => scan_structure.next_left_join(
            probe_keys,
            input,
            output,
            hash_table,
            left_projection,
            right_projection,
        ),
        JoinType::Semi => {
            scan_structure.next_semi_join(probe_keys, input, output, hash_table, left_projection)
        }
        JoinType::Anti => {
            scan_structure.next_anti_join(probe_keys, input, output, hash_table, left_projection)
        }
        JoinType::Mark => {
            scan_structure.next_mark_join(probe_keys, input, output, hash_table, left_projection)
        }
        JoinType::Single => scan_structure.next_single_join(
            probe_keys,
            input,
            output,
            hash_table,
            left_projection,
            right_projection,
        ),
        JoinType::RightSemi | JoinType::RightAnti => {
            scan_structure.next_right_semi_or_anti_join(probe_keys, hash_table)
        }
        JoinType::Invalid => Err(paro_error::internal("invalid hash join type")),
    }
}

pub(crate) fn emit_empty_build_probe_result(
    join_type: JoinType,
    input: &Chunk,
    left_projection: &[usize],
    right_projection: &[usize],
    output_types: &[LogicalType],
    output: &mut Chunk,
) -> Result<usize> {
    output.try_set_cardinality(0)?;
    if input.is_empty() {
        return Ok(0);
    }
    match join_type {
        JoinType::Left | JoinType::Outer | JoinType::Single => {
            let left_len = if left_projection.is_empty() {
                input.column_count()
            } else {
                left_projection.len()
            };
            let right_types = output_types.get(left_len..).ok_or_else(|| {
                paro_error::internal("hash join output type layout is shorter than left projection")
            })?;
            let sel = SelectionVector::try_incremental(input.size(), output.allocator().clone())?;
            construct_left_outer_result(
                input,
                &sel,
                input.size(),
                left_projection,
                right_types,
                output,
            )?;
            Ok(output.size())
        }
        JoinType::Anti => {
            let sel = SelectionVector::try_incremental(input.size(), output.allocator().clone())?;
            construct_anti_join_result(input, &sel, input.size(), left_projection, output)?;
            Ok(output.size())
        }
        JoinType::Mark => {
            let markers = vec![Some(false); input.size()];
            construct_mark_join_result(input, left_projection, &markers, output)?;
            Ok(output.size())
        }
        JoinType::Inner
        | JoinType::Right
        | JoinType::Semi
        | JoinType::RightSemi
        | JoinType::RightAnti
        | JoinType::Invalid => {
            let _ = right_projection;
            Ok(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    use super::*;

    fn input_chunk() -> Chunk {
        let allocator = paro_common::test_utils::test_allocator();
        let mut chunk =
            Chunk::try_initialize(&[LogicalType::Integer, LogicalType::Integer], 2, allocator)
                .expect("chunk");
        chunk.try_set_cardinality(2).unwrap();
        chunk.set_value(0, 0, &Value::Integer(1)).unwrap();
        chunk.set_value(0, 1, &Value::Integer(2)).unwrap();
        chunk.set_value(1, 0, &Value::Integer(10)).unwrap();
        chunk.set_value(1, 1, &Value::Integer(20)).unwrap();
        chunk
    }

    #[test]
    fn build_payload_empty_projection_reuses_input_columns_without_projection_vec() {
        let input = input_chunk();
        let mut slot = None;
        let payload = build_payload_chunk_ref(
            &input,
            &[],
            &[LogicalType::Integer, LogicalType::Integer],
            &mut slot,
        )
        .expect("payload");

        assert_eq!(payload.size(), 2);
        assert!(Arc::ptr_eq(&payload.data[0], &input.data[0]));
        assert!(Arc::ptr_eq(&payload.data[1], &input.data[1]));
        assert!(slot.is_none());
    }

    #[test]
    fn build_payload_can_be_empty_for_payload_free_joins() {
        let input = input_chunk();
        let mut slot = None;
        let payload = build_payload_chunk_ref(&input, &[], &[], &mut slot).expect("payload");

        assert_eq!(payload.size(), 2);
        assert_eq!(payload.column_count(), 0);
        assert!(slot.is_some());
    }

    #[test]
    fn build_payload_projects_columns_only_when_projection_is_non_identity() {
        let input = input_chunk();
        let mut slot = None;
        let payload = build_payload_chunk_ref(&input, &[1], &[LogicalType::Integer], &mut slot)
            .expect("payload");

        assert_eq!(payload.size(), 2);
        assert_eq!(payload.column(0).unwrap().get_i32(0), Some(10));
        assert_eq!(payload.column(0).unwrap().get_i32(1), Some(20));
        assert!(Arc::ptr_eq(&payload.data[0], &input.data[1]));
        assert!(slot.is_some());
    }

    #[test]
    fn build_payload_reuses_projected_metadata_slot_across_chunks() {
        let input = input_chunk();
        let mut slot = None;
        build_payload_chunk_ref(&input, &[1], &[LogicalType::Integer], &mut slot)
            .expect("warm payload");
        let data_ptr = slot.as_ref().expect("projected payload slot").data.as_ptr();
        let data_capacity = slot
            .as_ref()
            .expect("projected payload slot")
            .data
            .capacity();

        build_payload_chunk_ref(&input, &[1], &[LogicalType::Integer], &mut slot)
            .expect("reused payload");

        let payload = slot.as_ref().expect("projected payload slot");
        assert_eq!(payload.data.as_ptr(), data_ptr);
        assert_eq!(payload.data.capacity(), data_capacity);
    }
}
