// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(crate) fn hash_join_left_projection(join: &ComparisonJoin<PreparedChild>) -> Vec<usize> {
    match join.join_type {
        JoinType::RightSemi | JoinType::RightAnti => Vec::new(),
        _ => join.left_projection_map.to_indices(join.left.types().len()),
    }
}

pub(crate) fn hash_join_right_projection(join: &ComparisonJoin<PreparedChild>) -> Vec<usize> {
    match join.join_type {
        JoinType::Semi | JoinType::Anti | JoinType::Mark => Vec::new(),
        _ => join
            .right_projection_map
            .to_indices(join.right.types().len()),
    }
}

pub(crate) fn comparison_join_output_names(
    join: &ComparisonJoin<PreparedChild>,
) -> Result<Vec<String>> {
    let left_projection = hash_join_left_projection(join);
    let right_projection = hash_join_right_projection(join);
    let left_names = project_output_names(
        join.left.as_ref(),
        &left_projection,
        "comparison join left output",
    )?;
    let right_names = project_output_names(
        join.right.as_ref(),
        &right_projection,
        "comparison join right output",
    )?;
    Ok(join_output_names(join.join_type, left_names, right_names))
}

pub(crate) fn supports_typed_hash_join_type(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left
            | JoinType::Right
            | JoinType::Inner
            | JoinType::Outer
            | JoinType::Semi
            | JoinType::Anti
            | JoinType::Mark
            | JoinType::Single
            | JoinType::RightSemi
            | JoinType::RightAnti
    )
}

pub(crate) fn supports_external_hash_join_type(join_type: JoinType) -> bool {
    // Spill replay partitions both sides by the complete equality-key tuple,
    // carries the build-wide NULL-key bit for MARK semantics, and scans build
    // match bits per partition for right/full preservation. SINGLE duplicate
    // detection is also local to one complete-key partition. Consequently
    // every typed hash-join contract has an exact external counterpart.
    supports_typed_hash_join_type(join_type)
}

pub(crate) fn is_hash_join_comparison(comparison: JoinComparisonType) -> bool {
    matches!(
        comparison,
        JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
    )
}

pub(crate) fn nlj_left_projection(join: &ComparisonJoin<PreparedChild>) -> Vec<usize> {
    match join.join_type {
        JoinType::RightSemi | JoinType::RightAnti => Vec::new(),
        _ => join.left_projection_map.to_indices(join.left.types().len()),
    }
}

pub(crate) fn nlj_right_projection(join: &ComparisonJoin<PreparedChild>) -> Vec<usize> {
    match join.join_type {
        JoinType::Semi | JoinType::Anti | JoinType::Mark => Vec::new(),
        _ => join
            .right_projection_map
            .to_indices(join.right.types().len()),
    }
}

pub(crate) fn join_output_names(
    join_type: JoinType,
    left_names: Vec<String>,
    right_names: Vec<String>,
) -> Vec<String> {
    match join_type {
        JoinType::Semi | JoinType::Anti => left_names,
        JoinType::RightSemi | JoinType::RightAnti => right_names,
        JoinType::Mark => {
            let mut names = left_names;
            names.push("mark".to_string());
            names
        }
        _ => {
            let mut names = left_names;
            names.extend(right_names);
            names
        }
    }
}
