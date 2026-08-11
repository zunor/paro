// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_planner::operator::join::{JoinComparisonType, JoinCondition};

pub(super) fn hash_key_conditions(conditions: &[JoinCondition]) -> Box<[JoinCondition]> {
    select_hash_conditions(conditions, true)
}

pub(super) fn hash_residual_conditions(conditions: &[JoinCondition]) -> Box<[JoinCondition]> {
    select_hash_conditions(conditions, false)
}

fn select_hash_conditions(conditions: &[JoinCondition], select_keys: bool) -> Box<[JoinCondition]> {
    conditions
        .iter()
        .filter(|condition| {
            let is_key = matches!(
                condition.comparison,
                JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
            );
            is_key == select_keys
        })
        .cloned()
        .collect::<Vec<_>>()
        .into_boxed_slice()
}
