// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Defer wide dimension payload until after a fact-side partial aggregate.
//!
//! A grouped analytical query often joins a small dimension only to group by a
//! descriptive string. Materializing that string for every fact row is much
//! more expensive than grouping the fact stream by the equality key, joining
//! the compact partials to the dimension, and merging partial aggregate
//! results by the original SQL group. The final merge is required even when a
//! key is declared unique: SQL groups by the payload value, and two different
//! dimension keys may legally carry the same payload.

use paro_planner::expression::{AggregateType, Expression};
use paro_planner::logical::operator::{Aggregate, ComparisonJoin, JoinComparisonType, JoinType};
use std::cell::Cell;

#[cfg(test)]
mod oracle;
#[cfg(test)]
pub(crate) use oracle::optimize_plan;
#[cfg(test)]
mod join_region;
#[cfg(test)]
use oracle::{expression_domain, ExpressionDomain};

/// Necessary root-only eligibility shared by dispatch and both rewrite paths.
/// These fields belong to the immutable aggregate shell. In particular this
/// must not inspect the child: a later child alternative can expose a dimension
/// join even when the current input cannot be deferred.
pub(crate) fn root_eligible<Child>(aggregate: &Aggregate<Child>) -> bool {
    aggregate.post_reduction.is_none()
        && !aggregate.aggregates.is_empty()
        && aggregate.has_plain_grouping_domain()
}

/// A partial-state law, not merely an aggregate with the same name. Reuse
/// this check for every planner that moves aggregation across an inner join.
pub(crate) fn partial_merge(
    expression: &Expression,
) -> Option<paro_function::aggregate::AggregateFunction> {
    let Expression::Aggregate(partial) = expression else {
        return None;
    };
    if partial.aggr_type != AggregateType::NonDistinct
        || !partial.order_bys.is_empty()
        || partial
            .children
            .iter()
            .any(|child| !expression_is_movable(child))
        || partial
            .filter
            .as_deref()
            .is_some_and(|filter| !expression_is_movable(filter))
    {
        return None;
    }
    let merge = partial.function.partial_merge_function()?;
    (merge.arguments == [partial.return_type.clone()] && merge.return_type == partial.return_type)
        .then_some(merge)
}

fn inline_projection<Child>(
    expression: &Expression,
    projection: &paro_planner::logical::operator::Projection<Child>,
) -> Option<Expression> {
    let invalid = Cell::new(false);
    let result = expression.clone().replace_column_ref(&|column| {
        if column.depth != 0 {
            invalid.set(true);
            return None;
        }
        if column.binding.table_index != projection.table_index {
            return None;
        }
        let Some(replacement) = projection.expressions.get(column.binding.column_index) else {
            invalid.set(true);
            return None;
        };
        Some(replacement.clone())
    });
    (!invalid.get()).then_some(result)
}

fn expression_is_movable(expression: &Expression) -> bool {
    let properties = expression.evaluation_properties();
    properties.can_share_evaluation() && !properties.is_reorder_fence()
}

pub(crate) fn inline_projections<Child>(
    expression: &Expression,
    projections: &[&paro_planner::logical::operator::Projection<Child>],
) -> Option<Expression> {
    projections
        .iter()
        .try_fold(expression.clone(), |expression, projection| {
            inline_projection(&expression, projection)
        })
}

pub(crate) fn is_plain_inner_equi_join<Child>(join: &ComparisonJoin<Child>) -> bool {
    join.join_type == JoinType::Inner
        && join.build_side_constraint
            == paro_planner::logical::operator::JoinBuildSideConstraint::Either
        && !join.conditions.is_empty()
        && join.mark_index.is_none()
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
        && join.conditions.iter().all(|condition| {
            condition.comparison == JoinComparisonType::Equal
                && expression_is_movable(&condition.left)
                && expression_is_movable(&condition.right)
        })
}
