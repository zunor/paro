// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Expression rewriting engine and column binding replacement.

pub mod binding_replacer;
pub mod in_clause;
pub mod rewriter;

use paro_planner::operator::{Join, LogicalOperator};

pub(crate) fn join_has_evaluation_fence(join: &Join) -> bool {
    match join {
        Join::Comparison(join) => join.conditions.iter().any(|condition| {
            condition.left.evaluation_properties().is_reorder_fence()
                || condition.right.evaluation_properties().is_reorder_fence()
        }),
        Join::Any(join) => join.condition.evaluation_properties().is_reorder_fence(),
        Join::Cross(_) => false,
    }
}

/// Whether the join/filter region that join-order optimization would extract owns an evaluation
/// fence. Operators that become atomic relations deliberately stop the traversal.
pub(crate) fn join_tree_has_evaluation_fence(join: &Join) -> bool {
    join_has_evaluation_fence(join)
        || [join.left(), join.right()]
            .into_iter()
            .any(|child| join_region_has_evaluation_fence(&child.operator))
}

fn join_region_has_evaluation_fence(operator: &LogicalOperator) -> bool {
    match operator {
        LogicalOperator::Join(join) => join_tree_has_evaluation_fence(join),
        LogicalOperator::Filter(filter) => {
            filter
                .expressions
                .iter()
                .any(|expr| expr.evaluation_properties().is_reorder_fence())
                || join_region_has_evaluation_fence(&filter.child.operator)
        }
        _ => false,
    }
}
