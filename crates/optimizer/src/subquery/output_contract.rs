// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binding-domain output contracts for proof-driven subtree rewrites.

use std::collections::HashSet;

use paro_planner::expression::{Expression, ExpressionIterator, ExpressionVisitDecision};
use paro_planner::operator::{ColumnBinding, Join, JoinType, LogicalOperator};

/// The output-layout guarantee made by the rewrite consuming this contract.
///
/// A stable-prefix rewrite may remove only a trailing suffix, so stored
/// projection ordinals can be converted into explicit binding obligations.
/// An arbitrary-layout rewrite must stop at every stored positional map.
#[derive(Clone, Copy)]
pub(crate) enum RewriteOutputShape {
    ArbitraryLayout,
    StablePrefix,
}

/// Bindings consumed between a subtree and an explicit ancestor boundary.
/// Unknown positional or implicit consumers terminate propagation.
#[derive(Clone, Default)]
pub(crate) struct OutputContract {
    pub(crate) referenced: HashSet<ColumnBinding>,
}

impl OutputContract {
    pub(crate) fn from_expressions<'a>(
        expressions: impl IntoIterator<Item = &'a Expression>,
    ) -> Self {
        let mut contract = Self::default();
        contract.extend(expressions);
        contract
    }

    pub(crate) fn extend<'a>(&mut self, expressions: impl IntoIterator<Item = &'a Expression>) {
        for expression in expressions {
            ExpressionIterator::visit(expression, &mut |candidate| {
                if let Expression::ColumnRef(column) = candidate {
                    self.referenced.insert(column.binding);
                }
                ExpressionVisitDecision::Descend
            });
        }
    }

    pub(crate) fn references(&self, binding: ColumnBinding) -> bool {
        self.referenced.contains(&binding)
    }
}

pub(crate) fn child_output_contracts(
    operator: &LogicalOperator,
    inherited: Option<&OutputContract>,
    rewrite_shape: RewriteOutputShape,
) -> Vec<Option<OutputContract>> {
    match operator {
        LogicalOperator::Projection(projection) => vec![Some(OutputContract::from_expressions(
            projection.expressions.iter(),
        ))],
        LogicalOperator::Aggregate(aggregate) => vec![Some(OutputContract::from_expressions(
            aggregate.groups.iter().chain(aggregate.aggregates.iter()),
        ))],
        LogicalOperator::Filter(filter) if inherited.is_some() => {
            let mut contract = inherited.cloned().unwrap_or_default();
            contract.extend(filter.expressions.iter());
            if !extend_stored_projection(
                &mut contract,
                filter.child.as_ref(),
                &filter.projection_map,
                rewrite_shape,
            ) {
                return vec![None];
            }
            vec![Some(contract)]
        }
        LogicalOperator::Order(order) if inherited.is_some() => {
            let mut contract = inherited.cloned().unwrap_or_default();
            contract.extend(order.orders.iter().map(|order| &order.expression));
            if !extend_stored_projection(
                &mut contract,
                order.child.as_ref(),
                &order.projection_map,
                rewrite_shape,
            ) {
                return vec![None];
            }
            vec![Some(contract)]
        }
        LogicalOperator::Limit(limit) if inherited.is_some() => {
            let mut contract = inherited.cloned().unwrap_or_default();
            contract.extend(limit.limit.iter().chain(limit.offset.iter()));
            vec![Some(contract)]
        }
        LogicalOperator::TopN(topn) if inherited.is_some() => {
            let mut contract = inherited.cloned().unwrap_or_default();
            contract.extend(topn.orders.iter().map(|order| &order.expression));
            vec![Some(contract)]
        }
        LogicalOperator::Join(Join::Comparison(join)) if inherited.is_some() => {
            let mut consumed = inherited.cloned().unwrap_or_default();
            consumed.extend(
                join.conditions
                    .iter()
                    .flat_map(|condition| [&condition.left, &condition.right])
                    .chain(join.duplicate_eliminated_columns.iter()),
            );
            match join.join_type {
                JoinType::Semi | JoinType::Anti => {
                    if extend_stored_projection(
                        &mut consumed,
                        join.left.as_ref(),
                        &join.left_projection_map,
                        rewrite_shape,
                    ) {
                        vec![Some(consumed), None]
                    } else {
                        vec![None, None]
                    }
                }
                JoinType::RightSemi | JoinType::RightAnti => {
                    if extend_stored_projection(
                        &mut consumed,
                        join.right.as_ref(),
                        &join.right_projection_map,
                        rewrite_shape,
                    ) {
                        vec![None, Some(consumed)]
                    } else {
                        vec![None, None]
                    }
                }
                _ => vec![None, None],
            }
        }
        _ => operator.children().into_iter().map(|_| None).collect(),
    }
}

fn extend_stored_projection(
    contract: &mut OutputContract,
    child: &paro_planner::plan::LogicalPlan,
    projection: &paro_planner::operator::ProjectionMap,
    rewrite_shape: RewriteOutputShape,
) -> bool {
    if projection.is_all() {
        return true;
    }
    if !matches!(rewrite_shape, RewriteOutputShape::StablePrefix) {
        return false;
    }
    let bindings = child.get_column_bindings();
    let Some(indices) = projection.as_columns() else {
        return false;
    };
    let Some(selected) = indices
        .iter()
        .map(|&index| bindings.get(index).copied())
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    contract.referenced.extend(selected);
    true
}
