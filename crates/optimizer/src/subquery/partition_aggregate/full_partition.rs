// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Complete per-key fallback for correlated scalar aggregates.
//!
//! The rewrite removes the delimiter relation, groups the complete inner
//! relation by its correlation keys, and attaches that unique result with an
//! ordinary left join. Both blocking stages then use the regular spillable
//! aggregate and hash-join contracts.

use super::*;

pub(super) struct FullPartitionJoinRewrite {
    inner_keys: Vec<Expression>,
    delim_table_index: usize,
    correlation_key_count: usize,
}

pub(super) fn recognize_full_partition_join(
    plan: &LogicalPlan,
    _output_contract: Option<&OutputContract>,
) -> Option<FullPartitionJoinRewrite> {
    // This fallback preserves the complete join and filter output contract, so
    // it does not need the narrower projection-map preconditions used by the
    // window and grouped-HAVING rewrites.
    let LogicalOperator::Filter(filter) = &plan.operator else {
        return None;
    };
    let LogicalOperator::Join(Join::Comparison(join)) = &filter.child.operator else {
        return None;
    };
    if !canonical_scalar_delim_join(join) {
        return None;
    }
    let scalar = peel_scalar_branch(&join.right)?;
    let delim = find_only_delim_get(&scalar.aggregate.child)?;
    if delim.chunk_types.len() != join.duplicate_eliminated_columns.len() {
        return None;
    }
    let correlation = match_correlation_keys(
        &scalar.aggregate.child,
        delim.table_index,
        &join.duplicate_eliminated_columns,
    )?;
    if !validate_full_partition_binding_contract(join, &scalar, delim, &correlation.inner_keys)
        || !full_partition_input_is_movable(&scalar.aggregate.child)
        || !delim_source_path_is_removable(
            &scalar.aggregate.child,
            delim.table_index,
            correlation.inner_keys.len(),
        )
    {
        return None;
    }
    (correlation.inner_keys.len() == scalar.aggregate.groups.len()).then_some(
        FullPartitionJoinRewrite {
            inner_keys: correlation.inner_keys,
            delim_table_index: delim.table_index,
            correlation_key_count: join.duplicate_eliminated_columns.len(),
        },
    )
}

fn validate_full_partition_binding_contract(
    join: &ComparisonJoin,
    scalar: &ScalarBranch<'_>,
    delim: &paro_planner::operator::DelimGet,
    inner_keys: &[Expression],
) -> bool {
    let key_count = join.duplicate_eliminated_columns.len();
    if scalar.aggregate.groups.len() != key_count
        || scalar.projection_group_count() != key_count
        || join.conditions.len() != key_count
        || inner_keys.len() != key_count
    {
        return false;
    }
    for ordinal in 0..key_count {
        let expected_delim = ColumnBinding::new(delim.table_index, ordinal);
        let group = &scalar.aggregate.groups[ordinal];
        let groups_delim = matches!(group, Expression::ColumnRef(column)
            if column.depth == 0
                && column.binding == expected_delim
                && delim.chunk_types.get(ordinal) == Some(&column.return_type));
        if !groups_delim && !same_column_expression(group, &inner_keys[ordinal]) {
            return false;
        }
        let expected_group_output = ColumnBinding::new(scalar.aggregate.group_index, ordinal);
        let Some(projection_expression) = scalar.group_projection_expression(ordinal) else {
            return false;
        };
        if !matches!(projection_expression, Expression::ColumnRef(column)
            if column.depth == 0
                && column.binding == expected_group_output
                && group.return_type() == column.return_type)
        {
            return false;
        }
        let Some(condition) = join.conditions.get(ordinal) else {
            return false;
        };
        let expected_rhs = scalar.group_projection_binding(ordinal);
        let matches = |outer: &Expression, right: &Expression| {
            same_column_expression(outer, &join.duplicate_eliminated_columns[ordinal])
                && matches!(right, Expression::ColumnRef(column)
                    if column.depth == 0
                        && column.binding == expected_rhs
                        && projection_expression.return_type() == column.return_type)
        };
        if !(matches(&condition.left, &condition.right)
            || matches(&condition.right, &condition.left))
        {
            return false;
        }
    }
    true
}

/// Moving from a delimiter-restricted scan to a complete per-key aggregate
/// can evaluate rows for keys absent from the current outer stream. Admit that
/// change only when every newly exposed expression is shareable and has no
/// evaluation fence.
fn full_partition_input_is_movable(plan: &LogicalPlan) -> bool {
    let local = match &plan.operator {
        LogicalOperator::Get(_) | LogicalOperator::CTERef(_) | LogicalOperator::DelimGet(_) => true,
        LogicalOperator::Filter(filter) => filter.expressions.iter().all(is_movable),
        LogicalOperator::Projection(projection) => projection.expressions.iter().all(is_movable),
        LogicalOperator::Aggregate(aggregate) => {
            aggregate.grouping_sets.is_empty()
                && aggregate.grouping_functions.is_empty()
                && aggregate.post_reduction.is_none()
                && aggregate.groups.iter().all(is_movable)
                && aggregate.aggregates.iter().all(is_movable)
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            clean_inner_join(join)
                && join
                    .conditions
                    .iter()
                    .all(|condition| is_movable(&condition.left) && is_movable(&condition.right))
        }
        _ => false,
    };
    local
        && plan
            .children()
            .into_iter()
            .all(full_partition_input_is_movable)
}

/// Prove that the delimiter leaf is owned by one clean inner-join edge and
/// that every join above it is independent of the delimiter binding. This
/// admits a normal join tree around the correlated source without allowing a
/// hidden delimiter predicate to disappear with the leaf.
fn delim_source_path_is_removable(
    plan: &LogicalPlan,
    delim_table_index: usize,
    correlation_key_count: usize,
) -> bool {
    if direct_delim_join_source(plan, delim_table_index, correlation_key_count).is_some() {
        return true;
    }
    let LogicalOperator::Join(Join::Comparison(join)) = &plan.operator else {
        return false;
    };
    if !clean_inner_join(join)
        || join.conditions.iter().any(|condition| {
            expression_references_table(&condition.left, delim_table_index)
                || expression_references_table(&condition.right, delim_table_index)
        })
    {
        return false;
    }
    match (
        plan_references_delim(&join.left, delim_table_index),
        plan_references_delim(&join.right, delim_table_index),
    ) {
        (true, false) => {
            delim_source_path_is_removable(&join.left, delim_table_index, correlation_key_count)
        }
        (false, true) => {
            delim_source_path_is_removable(&join.right, delim_table_index, correlation_key_count)
        }
        _ => false,
    }
}

fn remove_delim_source_path(
    mut plan: LogicalPlan,
    delim_table_index: usize,
    correlation_key_count: usize,
) -> Result<LogicalPlan> {
    let direct = direct_delim_join_source(&plan, delim_table_index, correlation_key_count);
    let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator else {
        return Err(paro_error::internal(
            "full-partition delimiter path lost its comparison join",
        ));
    };
    if let Some(side) = direct {
        return Ok(match side {
            DirectSourceSide::LeftDelim => std::mem::replace(
                &mut *join.right,
                LogicalPlan::synthetic(LogicalOperator::DummyScan),
            ),
            DirectSourceSide::RightDelim => std::mem::replace(
                &mut *join.left,
                LogicalPlan::synthetic(LogicalOperator::DummyScan),
            ),
        });
    }
    let left_has_delim = plan_references_delim(&join.left, delim_table_index);
    let right_has_delim = plan_references_delim(&join.right, delim_table_index);
    let child = match (left_has_delim, right_has_delim) {
        (true, false) => &mut join.left,
        (false, true) => &mut join.right,
        _ => {
            return Err(paro_error::internal(
                "full-partition delimiter path became ambiguous",
            ));
        }
    };
    let owned = std::mem::replace(
        &mut **child,
        LogicalPlan::synthetic(LogicalOperator::DummyScan),
    );
    **child = remove_delim_source_path(owned, delim_table_index, correlation_key_count)?;
    Ok(plan)
}

pub(super) fn apply_full_partition_join(
    plan: LogicalPlan,
    rewrite: FullPartitionJoinRewrite,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    let LogicalOperator::Filter(mut filter) = plan.into_operator() else {
        return Err(paro_error::internal(
            "full-partition witness no longer points to a Filter",
        ));
    };
    let LogicalOperator::Join(Join::Comparison(mut outer_join)) = (*filter.child).into_operator()
    else {
        return Err(paro_error::internal(
            "full-partition witness no longer points to a comparison join",
        ));
    };
    let LogicalOperator::Projection(mut scalar_projection) = (*outer_join.right).into_operator()
    else {
        return Err(paro_error::internal(
            "full-partition witness lost the scalar projection",
        ));
    };
    let LogicalOperator::Aggregate(mut aggregate) = (*scalar_projection.child).into_operator()
    else {
        return Err(paro_error::internal(
            "full-partition witness lost the grouped scalar aggregate",
        ));
    };
    let inner = remove_delim_source_path(
        *aggregate.child,
        rewrite.delim_table_index,
        rewrite.correlation_key_count,
    )?;
    if aggregate.groups.len() != rewrite.inner_keys.len() {
        return Err(paro_error::internal(
            "full-partition witness changed correlation-key arity",
        ));
    }
    aggregate.groups = rewrite.inner_keys;
    aggregate.child = Box::new(inner);
    scalar_projection.child = Box::new(LogicalPlan::new(
        bind_context,
        LogicalOperator::Aggregate(aggregate),
    ));

    // GROUP BY proves at most one right row per complete key. LEFT therefore
    // has the same scalar cardinality as SINGLE. The scalar projection keeps
    // its hidden presence carrier, so an unmatched key remains distinguishable
    // from a matched aggregate whose SQL value is NULL.
    outer_join.join_type = JoinType::Left;
    outer_join.duplicate_eliminated_columns.clear();
    outer_join.delim_flipped = false;
    for condition in &mut outer_join.conditions {
        condition.comparison = JoinComparisonType::Equal;
    }
    outer_join.right = Box::new(LogicalPlan::new(
        bind_context,
        LogicalOperator::Projection(scalar_projection),
    ));
    filter.child = Box::new(LogicalPlan::new(
        bind_context,
        LogicalOperator::Join(Join::Comparison(outer_join)),
    ));
    Ok(LogicalPlan::new(
        bind_context,
        LogicalOperator::Filter(filter),
    ))
}
