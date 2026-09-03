// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Isolate the dimension carrying the widest grouping payload in an inner-join region.

use std::collections::HashSet;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{AggregateType, Expression};
use paro_planner::operator::{
    ColumnBinding, ComparisonJoin, Join, JoinComparisonType, JoinCondition, JoinType,
    LogicalOperator,
};
use paro_planner::plan::LogicalPlan;

use crate::expression::traversal::visit_expression;

use super::{expression_domain, inline_projections, ExpressionDomain};

/// Rotate one pure inner-equi region so the dimension carrying the widest SQL
/// grouping payload is the direct right child. This is semantic region
/// decomposition, not a join-order decision: the fact-side joins retain all
/// predicates, and Memo still costs both the original and deferred forms.
pub(super) fn isolate_widest_dimension(
    mut plan: LogicalPlan,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    let Some((projection_depth, table_index)) = widest_dimension_candidate(&plan) else {
        return Ok(plan);
    };
    let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
        return Ok(plan);
    };
    let child = std::mem::replace(
        &mut aggregate.child,
        Box::new(LogicalPlan::synthetic(LogicalOperator::DummyScan)),
    );
    aggregate.child = Box::new(isolate_below_projections(
        *child,
        projection_depth,
        table_index,
        bind_context,
    )?);
    Ok(plan)
}

fn widest_dimension_candidate(plan: &LogicalPlan) -> Option<(usize, usize)> {
    let LogicalOperator::Aggregate(aggregate) = &plan.operator else {
        return None;
    };
    if aggregate.post_reduction.is_some()
        || aggregate.aggregates.is_empty()
        || !aggregate.has_plain_grouping_domain()
    {
        return None;
    }
    let mut projections = Vec::new();
    let mut region = aggregate.child.as_ref();
    while let LogicalOperator::Projection(projection) = &region.operator {
        projections.push(projection);
        region = projection.child.as_ref();
    }
    let mut relations = Vec::new();
    let mut conditions = Vec::new();
    collect_inner_equi_region(region, &mut relations, &mut conditions)?;
    if relations.len() < 2 {
        return None;
    }
    let expanded_groups = aggregate
        .groups
        .iter()
        .map(|expression| inline_projections(expression, &projections))
        .collect::<Option<Vec<_>>>()?;
    let expanded_aggregates = aggregate
        .aggregates
        .iter()
        .map(|expression| inline_projections(expression, &projections))
        .collect::<Option<Vec<_>>>()?;
    if expanded_aggregates.iter().any(|expression| {
        let Expression::Aggregate(aggregate) = expression else {
            return true;
        };
        aggregate.aggr_type != AggregateType::NonDistinct
            || !aggregate.order_bys.is_empty()
            || aggregate.children.iter().any(|child| !is_movable(child))
            || aggregate
                .filter
                .as_deref()
                .is_some_and(|filter| !is_movable(filter))
            || aggregate
                .function
                .partial_merge_function()
                .is_none_or(|merge| {
                    merge.arguments != [aggregate.return_type.clone()]
                        || merge.return_type != aggregate.return_type
                })
    }) {
        return None;
    }
    let all_bindings = region
        .get_column_bindings()
        .into_iter()
        .collect::<HashSet<_>>();

    relations
        .into_iter()
        .filter_map(|relation| {
            let table_index = dimension_relation_table_index(relation)?;
            let dimension_bindings = relation
                .get_column_bindings()
                .into_iter()
                .collect::<HashSet<_>>();
            let fact_bindings = all_bindings
                .difference(&dimension_bindings)
                .copied()
                .collect::<HashSet<_>>();
            let mut payload_width = 0usize;
            for group in &expanded_groups {
                match expression_domain(group, &fact_bindings, &dimension_bindings) {
                    ExpressionDomain::Dimension if is_movable(group) => {
                        payload_width = payload_width.saturating_add(group_width(group));
                    }
                    ExpressionDomain::Fact | ExpressionDomain::Constant if is_movable(group) => {}
                    _ => return None,
                }
            }
            if payload_width == 0
                || expanded_aggregates.iter().any(|expression| {
                    !matches!(
                        expression_domain(expression, &fact_bindings, &dimension_bindings),
                        ExpressionDomain::Fact | ExpressionDomain::Constant
                    )
                })
                || !conditions.iter().any(|condition| {
                    condition_crosses_boundary(condition, &fact_bindings, &dimension_bindings)
                })
            {
                return None;
            }
            Some((payload_width, table_index))
        })
        .max_by_key(|(width, table_index)| (*width, std::cmp::Reverse(*table_index)))
        .map(|(_, table_index)| (projections.len(), table_index))
}

/// A CTE reference is a stable relational leaf just like a base scan. Keeping
/// the recognizer at this semantic boundary lets a separate sharing rule
/// materialize a repeated dimension without disabling fact-side
/// preaggregation.
fn dimension_relation_table_index(relation: &LogicalPlan) -> Option<usize> {
    match &relation.operator {
        LogicalOperator::Get(get) => Some(get.table_index),
        LogicalOperator::CTERef(reference) => Some(reference.table_index),
        _ => None,
    }
}

fn is_movable(expression: &Expression) -> bool {
    let properties = expression.evaluation_properties();
    properties.can_share_evaluation() && !properties.is_reorder_fence()
}

fn group_width(expression: &Expression) -> usize {
    match expression.return_type() {
        LogicalType::Varchar => 32,
        LogicalType::Blob => 64,
        logical_type => logical_type.type_size().max(1),
    }
}

fn collect_inner_equi_region<'a>(
    plan: &'a LogicalPlan,
    relations: &mut Vec<&'a LogicalPlan>,
    conditions: &mut Vec<&'a JoinCondition>,
) -> Option<()> {
    match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) if is_plain_inner_equi_join(join) => {
            collect_inner_equi_region(join.left.as_ref(), relations, conditions)?;
            collect_inner_equi_region(join.right.as_ref(), relations, conditions)?;
            conditions.extend(join.conditions.iter());
        }
        _ => relations.push(plan),
    }
    Some(())
}

fn is_plain_inner_equi_join(join: &ComparisonJoin) -> bool {
    join.join_type == JoinType::Inner
        && !join.conditions.is_empty()
        && join.mark_index.is_none()
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
        && join.conditions.iter().all(|condition| {
            condition.comparison == JoinComparisonType::Equal
                && is_movable(&condition.left)
                && is_movable(&condition.right)
        })
}

fn condition_crosses_boundary(
    condition: &JoinCondition,
    fact: &HashSet<ColumnBinding>,
    dimension: &HashSet<ColumnBinding>,
) -> bool {
    matches!(
        (
            expression_domain(&condition.left, fact, dimension),
            expression_domain(&condition.right, fact, dimension),
        ),
        (ExpressionDomain::Fact, ExpressionDomain::Dimension)
            | (ExpressionDomain::Dimension, ExpressionDomain::Fact)
    )
}

fn isolate_below_projections(
    mut plan: LogicalPlan,
    projection_depth: usize,
    table_index: usize,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    if projection_depth == 0 {
        return isolate_join_region(plan, table_index, bind_context);
    }
    let LogicalOperator::Projection(projection) = &mut plan.operator else {
        return Err(paro_error::internal(
            "dimension boundary lost its projection spine",
        ));
    };
    let child = std::mem::replace(
        &mut projection.child,
        Box::new(LogicalPlan::synthetic(LogicalOperator::DummyScan)),
    );
    projection.child = Box::new(isolate_below_projections(
        *child,
        projection_depth - 1,
        table_index,
        bind_context,
    )?);
    Ok(plan)
}

fn isolate_join_region(
    plan: LogicalPlan,
    table_index: usize,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    let root_id = plan.id;
    let root_stats = plan.stats.clone();
    let mut relations = Vec::new();
    let mut conditions = Vec::new();
    flatten_inner_equi_region(plan, &mut relations, &mut conditions)?;
    let dimension_position = relations
        .iter()
        .position(|relation| dimension_relation_table_index(relation) == Some(table_index))
        .ok_or_else(|| paro_error::internal("dimension relation disappeared from join region"))?;
    let dimension = relations.remove(dimension_position);
    let dimension_bindings = dimension
        .get_column_bindings()
        .into_iter()
        .collect::<HashSet<_>>();
    let mut boundary = Vec::new();
    let mut fact_conditions = Vec::new();
    for condition in conditions {
        if expression_references_any(&condition.left, &dimension_bindings)
            || expression_references_any(&condition.right, &dimension_bindings)
        {
            boundary.push(condition);
        } else {
            fact_conditions.push(condition);
        }
    }
    if boundary.is_empty() {
        return Err(paro_error::internal(
            "isolated dimension has no join boundary",
        ));
    }
    let fact = rebuild_inner_equi_region(relations, fact_conditions, bind_context)?;
    let fact_bindings = fact
        .get_column_bindings()
        .into_iter()
        .collect::<HashSet<_>>();
    let boundary = boundary
        .into_iter()
        .map(|condition| orient_condition(condition, &fact_bindings, &dimension_bindings))
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan {
        id: root_id,
        stats: root_stats,
        operator: LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            fact,
            dimension,
            boundary,
        ))),
    })
}

fn flatten_inner_equi_region(
    plan: LogicalPlan,
    relations: &mut Vec<LogicalPlan>,
    conditions: &mut Vec<JoinCondition>,
) -> Result<()> {
    let (id, stats, operator) = plan.into_parts();
    match operator {
        LogicalOperator::Join(Join::Comparison(join)) if is_plain_inner_equi_join(&join) => {
            flatten_inner_equi_region(*join.left, relations, conditions)?;
            flatten_inner_equi_region(*join.right, relations, conditions)?;
            conditions.extend(join.conditions);
        }
        operator => relations.push(LogicalPlan {
            id,
            stats,
            operator,
        }),
    }
    Ok(())
}

fn rebuild_inner_equi_region(
    mut relations: Vec<LogicalPlan>,
    mut conditions: Vec<JoinCondition>,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    if relations.is_empty() {
        return Err(paro_error::internal(
            "dimension isolation removed the complete join region",
        ));
    }
    let mut current = relations.remove(0);
    while !relations.is_empty() {
        let current_bindings = current
            .get_column_bindings()
            .into_iter()
            .collect::<HashSet<_>>();
        let position = relations
            .iter()
            .position(|relation| {
                let relation_bindings = relation
                    .get_column_bindings()
                    .into_iter()
                    .collect::<HashSet<_>>();
                conditions.iter().any(|condition| {
                    condition_crosses_boundary(condition, &current_bindings, &relation_bindings)
                })
            })
            .ok_or_else(|| paro_error::internal("fact join region became disconnected"))?;
        let relation = relations.remove(position);
        let relation_bindings = relation
            .get_column_bindings()
            .into_iter()
            .collect::<HashSet<_>>();
        let mut join_conditions = Vec::new();
        conditions.retain(|condition| {
            if condition_crosses_boundary(condition, &current_bindings, &relation_bindings) {
                join_conditions.push(condition.clone());
                false
            } else {
                true
            }
        });
        let join_conditions = join_conditions
            .into_iter()
            .map(|condition| orient_condition(condition, &current_bindings, &relation_bindings))
            .collect::<Result<Vec<_>>>()?;
        current = LogicalPlan::new(
            bind_context,
            LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Inner,
                current,
                relation,
                join_conditions,
            ))),
        );
    }
    if !conditions.is_empty() {
        return Err(paro_error::internal(
            "fact join region retained unassigned predicates",
        ));
    }
    Ok(current)
}

/// Put each comparison operand in the child scope where physical lowering
/// evaluates it. A flattened inner-join predicate retains the orientation of
/// its former tree, which is not necessarily the orientation of the rebuilt
/// tree.
fn orient_condition(
    mut condition: JoinCondition,
    left_bindings: &HashSet<ColumnBinding>,
    right_bindings: &HashSet<ColumnBinding>,
) -> Result<JoinCondition> {
    match (
        expression_domain(&condition.left, left_bindings, right_bindings),
        expression_domain(&condition.right, left_bindings, right_bindings),
    ) {
        (ExpressionDomain::Fact, ExpressionDomain::Dimension) => Ok(condition),
        (ExpressionDomain::Dimension, ExpressionDomain::Fact) => {
            std::mem::swap(&mut condition.left, &mut condition.right);
            condition.comparison = condition.comparison.flip();
            Ok(condition)
        }
        _ => Err(paro_error::internal(
            "join-region predicate does not connect the rebuilt children",
        )),
    }
}

fn expression_references_any(expression: &Expression, bindings: &HashSet<ColumnBinding>) -> bool {
    let mut found = false;
    visit_expression(expression, &mut |expression| {
        if let Expression::ColumnRef(column) = expression {
            found |= column.depth == 0 && bindings.contains(&column.binding);
        }
    });
    found
}
