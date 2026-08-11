// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Hash-join probe result emission helpers.

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::SelectionVector;
use paro_planner::operator::join::{AntiJoinMode, JoinType};

use crate::join_hashtable::scan_structure::ScanStructure;
use crate::join_hashtable::JoinHashTable;
use crate::operators::join::hash::residual::HashJoinResidualProbeState;
use crate::operators::join::join_result_helpers::{
    construct_anti_join_result, construct_left_outer_result, construct_mark_join_result,
};
use crate::runtime::context::QueryRuntimeContext;

pub(crate) fn scan_hash_join_results(
    join_type: JoinType,
    anti_join_mode: AntiJoinMode,
    probe_keys: &Chunk,
    input: &Chunk,
    output: &mut Chunk,
    hash_table: &JoinHashTable,
    scan_structure: &mut ScanStructure,
    left_projection: &[usize],
    residual: Option<&mut HashJoinResidualProbeState>,
    runtime: &QueryRuntimeContext,
) -> Result<usize> {
    if let Some(residual) = residual {
        let mut select = |lhs_sel: &SelectionVector,
                          rhs_pointers: &[usize],
                          match_count: usize,
                          output: &mut SelectionVector| {
            residual.select_matches(
                runtime,
                hash_table,
                lhs_sel,
                rhs_pointers,
                match_count,
                output,
            )
        };
        return match join_type {
            JoinType::Inner | JoinType::Right => scan_structure.next_inner_join_with_filter(
                probe_keys,
                input,
                output,
                hash_table,
                left_projection,
                &mut select,
            ),
            JoinType::Left | JoinType::Outer => scan_structure.next_left_join_with_filter(
                probe_keys,
                input,
                output,
                hash_table,
                left_projection,
                &mut select,
            ),
            JoinType::Semi => scan_structure.next_semi_join_with_filter(
                probe_keys,
                input,
                output,
                hash_table,
                left_projection,
                &mut select,
            ),
            JoinType::Anti if anti_join_mode == AntiJoinMode::Regular => scan_structure
                .next_anti_join_with_filter(
                    probe_keys,
                    input,
                    output,
                    hash_table,
                    left_projection,
                    &mut select,
                ),
            JoinType::Single => scan_structure.next_single_join_with_filter(
                probe_keys,
                input,
                output,
                hash_table,
                left_projection,
                &mut select,
            ),
            JoinType::RightSemi | JoinType::RightAnti => scan_structure
                .next_right_semi_or_anti_join_with_filter(probe_keys, hash_table, &mut select),
            JoinType::Mark | JoinType::Anti | JoinType::Invalid => Err(paro_error::internal(
                "hash join residual predicate is not valid for this join mode",
            )),
        };
    }
    match join_type {
        JoinType::Inner | JoinType::Right => {
            scan_structure.next_inner_join(probe_keys, input, output, hash_table, left_projection)
        }
        JoinType::Left | JoinType::Outer => {
            scan_structure.next_left_join(probe_keys, input, output, hash_table, left_projection)
        }
        JoinType::Semi => {
            scan_structure.next_semi_join(probe_keys, input, output, hash_table, left_projection)
        }
        JoinType::Anti => match anti_join_mode {
            AntiJoinMode::Regular => scan_structure.next_anti_join(
                probe_keys,
                input,
                output,
                hash_table,
                left_projection,
            ),
            AntiJoinMode::NullAware => scan_structure.next_null_aware_anti_join(
                probe_keys,
                input,
                output,
                hash_table,
                left_projection,
            ),
        },
        JoinType::Mark => {
            scan_structure.next_mark_join(probe_keys, input, output, hash_table, left_projection)
        }
        JoinType::Single => {
            scan_structure.next_single_join(probe_keys, input, output, hash_table, left_projection)
        }
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
    output_types: &[LogicalType],
    output: &mut Chunk,
) -> Result<usize> {
    output.try_set_cardinality(0)?;
    if input.is_empty() {
        return Ok(0);
    }
    match join_type {
        JoinType::Left | JoinType::Outer | JoinType::Single => {
            let left_len = left_projection.len();
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
        | JoinType::Invalid => Ok(0),
    }
}
