// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! One-boundary transfer of a relational key domain. Memo owns recursion and
//! enumerates the alternatives below the unconsumed group inputs.

use std::collections::BTreeSet;

use paro_common::error::Result;
use paro_planner::expression::Expression;
use paro_planner::operator::{ComparisonJoin, Join, JoinComparisonType, JoinType, LogicalOperator};
use paro_planner::plan::{arena::LogicalPlanNode, NodeStats, OwnedLogicalPlan};

pub(crate) fn transfer(plan: OwnedLogicalPlan) -> Result<Option<OwnedLogicalPlan>> {
    let LogicalOperator::Join(Join::Comparison(mut domain)) = plan.into_operator() else {
        return Ok(None);
    };
    if domain.join_type != JoinType::Semi
        || domain.conditions.is_empty()
        || !domain.duplicate_eliminated_columns.is_empty()
        || domain.delim_flipped
        || domain.conditions.iter().any(|condition| {
            condition.comparison != JoinComparisonType::Equal
                || condition.left.evaluation_properties().is_reorder_fence()
                || condition.right.evaluation_properties().is_reorder_fence()
        })
    {
        return Ok(None);
    }
    let mut probe = *domain.left;
    let mut fenced = false;
    paro_planner::visitor::enumerate_expressions(&mut probe.operator, |expression| {
        fenced |= expression.evaluation_properties().is_reorder_fence();
    });
    if fenced {
        return Ok(None);
    }
    let target = match &probe.operator {
        LogicalOperator::Projection(project) => {
            for condition in &mut domain.conditions {
                let Expression::ColumnRef(column) = &condition.left else {
                    return Ok(None);
                };
                if column.depth != 0 || column.binding.table_index != project.table_index {
                    return Ok(None);
                }
                let Some(expression) = project.expressions.get(column.binding.column_index) else {
                    return Ok(None);
                };
                if expression.return_type() != column.return_type {
                    return Ok(None);
                }
                condition.left = expression.clone();
            }
            0
        }
        LogicalOperator::Aggregate(aggregate)
            if aggregate.has_plain_grouping_domain()
                && !aggregate.groups.is_empty()
                && aggregate.post_reduction.is_none() =>
        {
            for condition in &mut domain.conditions {
                let Expression::ColumnRef(column) = &condition.left else {
                    return Ok(None);
                };
                if column.depth != 0 || column.binding.table_index != aggregate.group_index {
                    return Ok(None);
                }
                let Some(expression) = aggregate.groups.get(column.binding.column_index) else {
                    return Ok(None);
                };
                if expression.return_type() != column.return_type {
                    return Ok(None);
                }
                condition.left = expression.clone();
            }
            0
        }
        LogicalOperator::Filter(_) | LogicalOperator::Order(_) => 0,
        LogicalOperator::Join(Join::Comparison(join))
            if join.join_type == JoinType::Inner
                && join.duplicate_eliminated_columns.is_empty()
                && !join.delim_flipped =>
        {
            let mut keys = Vec::new();
            for condition in &domain.conditions {
                crate::column::lifetime::ColumnLifetimeAnalyzer::extract_column_bindings(
                    &condition.left,
                    &mut keys,
                );
            }
            if keys.is_empty() {
                return Ok(None);
            }
            let owned = [&join.left, &join.right].map(|child| {
                let bindings = child
                    .get_column_bindings()
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                keys.iter().all(|key| bindings.contains(key))
            });
            match owned {
                [true, false] => 0,
                [false, true] => 1,
                _ => return Ok(None),
            }
        }
        _ => return Ok(None),
    };
    let (mut shell, mut inputs) = LogicalPlanNode::detach(probe);
    let child = inputs.remove(target);
    let restricted = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(JoinType::Semi, *child, *domain.right, domain.conditions),
    )));
    inputs.insert(target, Box::new(restricted));
    shell.stats = NodeStats::default();
    Ok(Some(shell.assemble(inputs)?))
}
