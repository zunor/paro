// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering as CmpOrdering;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};

use crate::execution_context::ExecutionContext;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::join_hashtable::join_hashtable::JoinHashTable;
use crate::join_hashtable::scan_structure::ScanStructure;
use crate::result_type::OperatorResultType;

#[derive(Debug)]
pub(super) struct ResidualConditionExecutors {
    pub(super) left: ExpressionExecutor,
    pub(super) right: ExpressionExecutor,
}

#[derive(Debug)]
pub(super) struct ProbeKeyExecutors {
    pub(super) executors: Vec<ExpressionExecutor>,
}

fn materialize_key_vector(
    ctx: &ExecutionContext,
    vector: Arc<Vector>,
    logical_type: LogicalType,
    count: usize,
) -> Result<Arc<Vector>> {
    let allocator = ctx.allocator(paro_common::allocator::MemoryTag::BaseTable);
    let mut flat = Vector::try_new(logical_type, count.max(1), allocator)?;
    flat.set_len(count);
    for row_idx in 0..count {
        flat.copy_at(row_idx, vector.as_ref(), row_idx);
    }
    Ok(Arc::new(flat))
}

pub(super) fn evaluate_probe_keys(
    ctx: &ExecutionContext,
    input: &Chunk,
    equality_conditions: &[JoinCondition],
    probe_key_executors: &mut ProbeKeyExecutors,
) -> Result<Chunk> {
    let mut key_vectors = Vec::with_capacity(equality_conditions.len());
    for (cond, executor) in equality_conditions
        .iter()
        .zip(probe_key_executors.executors.iter_mut())
    {
        let vec = executor.execute_expression(0, input, None, input.size(), ctx)?;
        key_vectors.push(materialize_key_vector(
            ctx,
            vec,
            cond.left.return_type(),
            input.size(),
        )?);
    }
    let mut probe_keys = Chunk::from_arc_vectors(key_vectors, input.allocator().clone());
    probe_keys.set_cardinality(input.size());
    Ok(probe_keys)
}

pub(super) fn prepare_output_chunk(
    chunk: &mut Chunk,
    types: &[LogicalType],
    capacity: usize,
) -> Result<()> {
    let needs_reinit = chunk.column_count() != types.len()
        || chunk.capacity() < capacity
        || chunk.types() != types;
    if needs_reinit {
        let allocator = chunk.allocator().clone();
        *chunk = Chunk::try_initialize(types, capacity, allocator)?;
    } else {
        chunk.try_reset(chunk.allocator().clone())?;
    }
    Ok(())
}

pub(super) fn residual_condition_matches(
    comparison: JoinComparisonType,
    left: &Value,
    right: &Value,
) -> bool {
    match comparison {
        JoinComparisonType::Equal => !left.is_null() && !right.is_null() && left == right,
        JoinComparisonType::NotEqual => !left.is_null() && !right.is_null() && left != right,
        JoinComparisonType::LessThan => {
            !left.is_null()
                && !right.is_null()
                && left.partial_cmp(right) == Some(CmpOrdering::Less)
        }
        JoinComparisonType::GreaterThan => {
            !left.is_null()
                && !right.is_null()
                && left.partial_cmp(right) == Some(CmpOrdering::Greater)
        }
        JoinComparisonType::LessThanOrEqual => {
            !left.is_null()
                && !right.is_null()
                && matches!(
                    left.partial_cmp(right),
                    Some(CmpOrdering::Less | CmpOrdering::Equal)
                )
        }
        JoinComparisonType::GreaterThanOrEqual => {
            !left.is_null()
                && !right.is_null()
                && matches!(
                    left.partial_cmp(right),
                    Some(CmpOrdering::Greater | CmpOrdering::Equal)
                )
        }
        JoinComparisonType::NotDistinctFrom => {
            (left.is_null() && right.is_null())
                || (!left.is_null() && !right.is_null() && left == right)
        }
        JoinComparisonType::DistinctFrom => {
            (left.is_null() && !right.is_null())
                || (!left.is_null() && right.is_null())
                || (!left.is_null() && !right.is_null() && left != right)
        }
    }
}

pub(super) fn filter_residual_matches(
    ctx: &ExecutionContext,
    left_input: &Chunk,
    hash_table: &JoinHashTable,
    residual_conditions_on_build_payload: &[JoinCondition],
    residual_condition_executors: &mut [ResidualConditionExecutors],
    build_payload_types: &[LogicalType],
    lhs_sel: &SelectionVector,
    rhs_ptrs: &[usize],
    match_count: usize,
    output_sel: &mut SelectionVector,
) -> Result<usize> {
    if residual_conditions_on_build_payload.is_empty() || match_count == 0 {
        for i in 0..match_count {
            output_sel.set(i, i);
        }
        return Ok(match_count);
    }

    let mut build_chunk = Chunk::try_initialize(
        build_payload_types,
        match_count,
        left_input.allocator().clone(),
    )?;
    build_chunk.set_cardinality(match_count);

    for (build_col_idx, _build_type) in build_payload_types.iter().enumerate() {
        let column = build_chunk.column_mut(build_col_idx).ok_or_else(|| {
            paro_error::internal(format!("Build chunk column {} not found", build_col_idx))
        })?;

        for (row_idx, row_ptr) in rhs_ptrs.iter().enumerate().take(match_count) {
            let value = hash_table.read_build_value(*row_ptr, build_col_idx);
            column.set_value(row_idx, &value);
        }
    }

    let mut surviving =
        SelectionVector::try_incremental(match_count, output_sel.allocator().clone())?;
    let mut surviving_count = match_count;

    for (condition, executors) in residual_conditions_on_build_payload
        .iter()
        .zip(residual_condition_executors.iter_mut())
    {
        if surviving_count == 0 {
            break;
        }

        let left_vec =
            executors
                .left
                .execute_expression(0, left_input, Some(lhs_sel), match_count, ctx)?;
        let right_vec =
            executors
                .right
                .execute_expression(0, &build_chunk, None, match_count, ctx)?;

        let mut next_count = 0;
        for i in 0..surviving_count {
            let match_idx = surviving.get(i);
            let left_val = left_vec.get_value(match_idx);
            let right_val = right_vec.get_value(match_idx);
            if residual_condition_matches(condition.comparison, &left_val, &right_val) {
                output_sel.set(next_count, match_idx);
                next_count += 1;
            }
        }

        for i in 0..next_count {
            surviving.set(i, output_sel.get(i));
        }
        surviving_count = next_count;
    }

    for i in 0..surviving_count {
        output_sel.set(i, surviving.get(i));
    }

    Ok(surviving_count)
}

pub(super) fn scan_join_results(
    ctx: &ExecutionContext,
    join_type: JoinType,
    probe_keys: &Chunk,
    input: &Chunk,
    chunk: &mut Chunk,
    scan_structure: &mut ScanStructure,
    hash_table: &JoinHashTable,
    left_projection_map: &[usize],
    right_projection_map: &[usize],
    residual_conditions_on_build_payload: &[JoinCondition],
    residual_condition_executors: &mut [ResidualConditionExecutors],
    build_payload_types: &[LogicalType],
) -> Result<usize> {
    if residual_conditions_on_build_payload.is_empty() {
        match join_type {
            JoinType::Inner | JoinType::Right => scan_structure.next_inner_join(
                probe_keys,
                input,
                chunk,
                hash_table,
                left_projection_map,
                right_projection_map,
            ),
            JoinType::Left | JoinType::Outer => scan_structure.next_left_join(
                probe_keys,
                input,
                chunk,
                hash_table,
                left_projection_map,
                right_projection_map,
            ),
            JoinType::Semi => scan_structure.next_semi_join(
                probe_keys,
                input,
                chunk,
                hash_table,
                left_projection_map,
            ),
            JoinType::Anti => scan_structure.next_anti_join(
                probe_keys,
                input,
                chunk,
                hash_table,
                left_projection_map,
            ),
            JoinType::Mark => scan_structure.next_mark_join(
                probe_keys,
                input,
                chunk,
                hash_table,
                left_projection_map,
            ),
            JoinType::Single => scan_structure.next_single_join(
                probe_keys,
                input,
                chunk,
                hash_table,
                left_projection_map,
                right_projection_map,
            ),
            JoinType::RightSemi | JoinType::RightAnti => {
                scan_structure.next_right_semi_or_anti_join(probe_keys, hash_table)
            }
            _ => Err(paro_error::not_implemented(format!(
                "{} hash join result construction",
                join_type
            ))),
        }
    } else {
        let residual_filter = |lhs_sel: &SelectionVector,
                               rhs_ptrs: &[usize],
                               match_count: usize,
                               out_sel: &mut SelectionVector|
         -> Result<usize> {
            filter_residual_matches(
                ctx,
                input,
                hash_table,
                residual_conditions_on_build_payload,
                residual_condition_executors,
                build_payload_types,
                lhs_sel,
                rhs_ptrs,
                match_count,
                out_sel,
            )
        };

        match join_type {
            JoinType::Inner | JoinType::Right => scan_structure.next_inner_join_with_filter(
                probe_keys,
                input,
                chunk,
                hash_table,
                left_projection_map,
                right_projection_map,
                residual_filter,
            ),
            JoinType::Left | JoinType::Outer => scan_structure.next_left_join_with_filter(
                probe_keys,
                input,
                chunk,
                hash_table,
                left_projection_map,
                right_projection_map,
                residual_filter,
            ),
            JoinType::Semi => scan_structure.next_semi_join_with_filter(
                probe_keys,
                input,
                chunk,
                hash_table,
                left_projection_map,
                residual_filter,
            ),
            JoinType::Anti => scan_structure.next_anti_join_with_filter(
                probe_keys,
                input,
                chunk,
                hash_table,
                left_projection_map,
                residual_filter,
            ),
            JoinType::Mark => scan_structure.next_mark_join_with_filter(
                probe_keys,
                input,
                chunk,
                hash_table,
                left_projection_map,
                residual_filter,
            ),
            JoinType::Single => scan_structure.next_single_join_with_filter(
                probe_keys,
                input,
                chunk,
                hash_table,
                left_projection_map,
                right_projection_map,
                residual_filter,
            ),
            JoinType::RightSemi | JoinType::RightAnti => scan_structure
                .next_right_semi_or_anti_join_with_filter(probe_keys, hash_table, residual_filter),
            _ => Err(paro_error::not_implemented(format!(
                "{} hash join result construction",
                join_type
            ))),
        }
    }
}

pub(super) fn result_for_probe_batch(count: usize, scan_finished: bool) -> OperatorResultType {
    if count > 0 && !scan_finished {
        OperatorResultType::HaveMoreOutput
    } else {
        OperatorResultType::NeedMoreInput
    }
}

#[cfg(test)]
mod tests {

    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::operator::join::JoinComparisonType;

    use crate::result_type::OperatorResultType;

    use super::{prepare_output_chunk, residual_condition_matches, result_for_probe_batch};

    #[test]
    fn result_for_probe_batch_reports_more_output_only_for_unfinished_scans() {
        assert_eq!(
            result_for_probe_batch(1, false),
            OperatorResultType::HaveMoreOutput
        );
        assert_eq!(
            result_for_probe_batch(1, true),
            OperatorResultType::NeedMoreInput
        );
        assert_eq!(
            result_for_probe_batch(0, false),
            OperatorResultType::NeedMoreInput
        );
    }

    #[test]
    fn prepare_output_chunk_reuses_compatible_chunks() {
        let mut chunk =
            paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 4);
        chunk.set_cardinality(2);

        prepare_output_chunk(&mut chunk, &[LogicalType::Integer], 4).unwrap();

        assert_eq!(chunk.column_count(), 1);
        assert_eq!(chunk.capacity(), 4);
        assert_eq!(chunk.size(), 0);
    }

    #[test]
    fn residual_condition_matches_handles_distinct_null_semantics() {
        let null = Value::Null(LogicalType::Integer);
        let one = Value::Integer(1);

        assert!(residual_condition_matches(
            JoinComparisonType::NotDistinctFrom,
            &null,
            &null,
        ));
        assert!(residual_condition_matches(
            JoinComparisonType::DistinctFrom,
            &null,
            &one,
        ));
        assert!(!residual_condition_matches(
            JoinComparisonType::Equal,
            &null,
            &one,
        ));
    }
}
