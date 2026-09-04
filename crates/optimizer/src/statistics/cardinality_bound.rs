// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Semantic row-count bounds shared by statistics and Cascades properties.
//!
//! These bounds must survive data changes and substitution by an equivalent
//! expression. Snapshot row counts, selectivity estimates, and HLL domains do
//! not belong here.

use paro_planner::operator::{Join, JoinType, LogicalOperator, SetOpType};

pub(crate) fn derive_maximum_cardinality(
    operator: &LogicalOperator,
    child_maximum_cardinalities: &[Option<u64>],
) -> Option<u64> {
    let unary_bound = || child_maximum_cardinalities.first().copied().flatten();
    let binary_product = || {
        child_maximum_cardinalities
            .first()
            .copied()
            .flatten()?
            .checked_mul(child_maximum_cardinalities.get(1).copied().flatten()?)
    };
    match operator {
        LogicalOperator::DummyScan => Some(1),
        LogicalOperator::EmptyResult(_) => Some(0),
        // Resident rows are snapshot evidence. Prepared plans survive DML.
        LogicalOperator::Get(_) => None,
        LogicalOperator::ExpressionGet(values) => u64::try_from(values.expressions.len()).ok(),
        LogicalOperator::Projection(_)
        | LogicalOperator::Filter(_)
        | LogicalOperator::RowFetch(_)
        | LogicalOperator::ExternalProject(_)
        | LogicalOperator::Limit(_)
        | LogicalOperator::Order(_)
        | LogicalOperator::Distinct(_)
        | LogicalOperator::Window(_) => unary_bound(),
        LogicalOperator::Aggregate(aggregate)
            if aggregate.groups.is_empty() && aggregate.grouping_sets.is_empty() =>
        {
            Some(1)
        }
        LogicalOperator::Aggregate(aggregate) if !aggregate.grouping_sets.is_empty() => {
            unary_bound().and_then(|input| {
                aggregate
                    .grouping_sets
                    .iter()
                    .try_fold(0_u64, |bound, set| {
                        bound.checked_add(if set.expressions.is_empty() { 1 } else { input })
                    })
            })
        }
        LogicalOperator::Aggregate(_) => unary_bound(),
        LogicalOperator::TopN(topn) => {
            unary_bound().map(|bound| bound.min(u64::try_from(topn.limit).unwrap_or(u64::MAX)))
        }
        LogicalOperator::Join(Join::Cross(_)) => binary_product(),
        LogicalOperator::Join(Join::Comparison(join)) => match join.join_type {
            JoinType::Semi | JoinType::Anti | JoinType::Mark | JoinType::Single => {
                child_maximum_cardinalities.first().copied().flatten()
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                child_maximum_cardinalities.get(1).copied().flatten()
            }
            JoinType::Inner => binary_product(),
            JoinType::Left => child_maximum_cardinalities
                .first()
                .copied()
                .flatten()
                .and_then(|left| {
                    child_maximum_cardinalities
                        .get(1)
                        .copied()
                        .flatten()
                        .and_then(|right| left.checked_mul(right.max(1)))
                }),
            JoinType::Right => child_maximum_cardinalities
                .get(1)
                .copied()
                .flatten()
                .and_then(|right| {
                    child_maximum_cardinalities
                        .first()
                        .copied()
                        .flatten()
                        .and_then(|left| right.checked_mul(left.max(1)))
                }),
            JoinType::Outer => (|| {
                let left = child_maximum_cardinalities.first().copied().flatten()?;
                let right = child_maximum_cardinalities.get(1).copied().flatten()?;
                left.checked_mul(right)?
                    .checked_add(left)?
                    .checked_add(right)
            })(),
            JoinType::Invalid => None,
        },
        LogicalOperator::SetOperation(set) => match set.setop_type {
            SetOpType::Union => {
                let left = child_maximum_cardinalities.first().copied().flatten();
                let right = child_maximum_cardinalities.get(1).copied().flatten();
                left.zip(right)
                    .and_then(|(left, right)| left.checked_add(right))
            }
            SetOpType::Intersect => {
                let left = child_maximum_cardinalities.first().copied().flatten();
                let right = child_maximum_cardinalities.get(1).copied().flatten();
                left.zip(right).map(|(left, right)| left.min(right))
            }
            SetOpType::Except => child_maximum_cardinalities.first().copied().flatten(),
        },
        LogicalOperator::MaterializedCTE(_) => {
            child_maximum_cardinalities.get(1).copied().flatten()
        }
        _ => None,
    }
}
