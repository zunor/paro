// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Stable semantic planner view materialized from Memo expressions.
//!
//! Planner payloads retain an executable extraction recipe, including its
//! positional ABI. Rules never inspect that recipe directly: this module
//! restores semantic bindings and canonical output maps after choosing the
//! group's initial expression as its deterministic representative.

use super::*;

pub(super) fn materialize(
    memo: &Memo,
    state: &PlannerTransformState,
    expr: LogicalExprId,
) -> Result<LogicalPlan> {
    // Rebuild the tree first, then normalize it exactly once. Normalizing at
    // every recursive return revisits each descendant once per ancestor and
    // turns a linear materialization into quadratic work on UNION chains.
    let plan = materialize_raw(memo, state, expr)?;
    Ok(normalize_semantic_view(plan))
}

fn materialize_raw(
    memo: &Memo,
    state: &PlannerTransformState,
    expr: LogicalExprId,
) -> Result<LogicalPlan> {
    let logical = memo
        .logical_expr(expr)
        .ok_or_else(|| paro_error::internal("planner rule references unknown expression"))?;
    let payload = state
        .payloads
        .logical
        .get(logical.payload.index())
        .ok_or_else(|| paro_error::internal("planner rule references unknown payload"))?;
    let mut children = Vec::with_capacity(logical.key.children.len());
    for child in logical.key.children.iter().copied() {
        let child_expr = memo
            .group(child)
            .and_then(|group| group.logical_exprs().first())
            .copied()
            .ok_or_else(|| paro_error::internal("planner rule found an empty child group"))?;
        children.push(materialize_raw(memo, state, child_expr)?);
    }
    let mut children = children.into_iter();
    let mut plan = duplicate_plan_preserving_indices(
        &payload.extraction_template,
        state.bind_context.shared().as_ref(),
    )
    .try_map_children(|_| {
        children
            .next()
            .ok_or_else(|| paro_error::internal("planner payload lost a child expression"))
    })?;
    if children.next().is_some() {
        return Err(paro_error::internal(
            "planner payload child arity disagrees with Memo expression",
        ));
    }
    plan.stats.estimated_cardinality = payload.output_estimate;
    Ok(plan)
}

fn normalize_semantic_view(plan: LogicalPlan) -> LogicalPlan {
    let plan = restore_direct_filter_bindings(plan);
    let plan = restore_canonical_projection_maps(plan);
    lower_positive_consumed_mark_filter(plan)
}

fn restore_direct_filter_bindings(plan: LogicalPlan) -> LogicalPlan {
    plan.map_children(restore_direct_filter_bindings)
        .map_operator(|operator| match operator {
            LogicalOperator::Filter(mut filter) => {
                let bindings = filter.child.get_column_bindings();
                for expression in &mut filter.expressions {
                    let Expression::Reference(reference) = expression else {
                        continue;
                    };
                    let Some(binding) = bindings.get(reference.index).copied() else {
                        continue;
                    };
                    *expression = Expression::ColumnRef(ColumnRefExpression::new(
                        binding,
                        reference.return_type.clone(),
                    ));
                }
                LogicalOperator::Filter(filter)
            }
            operator => operator,
        })
}

fn restore_canonical_projection_maps(plan: LogicalPlan) -> LogicalPlan {
    plan.map_children(restore_canonical_projection_maps)
        .map_operator(|operator| match operator {
            LogicalOperator::Filter(mut filter) => {
                filter.projection_map = paro_planner::operator::ProjectionMap::all();
                LogicalOperator::Filter(filter)
            }
            LogicalOperator::Join(Join::Comparison(mut join))
                if join.join_type == JoinType::Inner =>
            {
                join.left_projection_map = paro_planner::operator::ProjectionMap::all();
                join.right_projection_map = paro_planner::operator::ProjectionMap::all();
                LogicalOperator::Join(Join::Comparison(join))
            }
            operator => operator,
        })
}

fn lower_positive_consumed_mark_filter(plan: LogicalPlan) -> LogicalPlan {
    let plan = plan.map_children(lower_positive_consumed_mark_filter);
    let LogicalPlan {
        id,
        stats,
        operator,
    } = plan;
    let LogicalOperator::Filter(filter) = operator else {
        return LogicalPlan {
            id,
            stats,
            operator,
        };
    };
    let [Expression::ColumnRef(marker)] = filter.expressions.as_slice() else {
        return LogicalPlan {
            id,
            stats,
            operator: LogicalOperator::Filter(filter),
        };
    };
    let child = *filter.child;
    let (child_id, child_stats, mut join) = match child {
        LogicalPlan {
            id,
            stats,
            operator: LogicalOperator::Join(Join::Comparison(join)),
        } => (id, stats, join),
        child => {
            return LogicalPlan {
                id,
                stats,
                operator: LogicalOperator::Filter(paro_planner::operator::Filter {
                    child: Box::new(child),
                    ..filter
                }),
            };
        }
    };
    let Some(mark_index) = join.mark_index else {
        return LogicalPlan {
            id,
            stats,
            operator: LogicalOperator::Filter(paro_planner::operator::Filter {
                child: Box::new(LogicalPlan {
                    id: child_id,
                    stats: child_stats,
                    operator: LogicalOperator::Join(Join::Comparison(join)),
                }),
                ..filter
            }),
        };
    };
    if join.join_type != JoinType::Mark
        || marker.depth != 0
        || marker.binding != ColumnBinding::new(mark_index, 0)
    {
        return LogicalPlan {
            id,
            stats,
            operator: LogicalOperator::Filter(paro_planner::operator::Filter {
                child: Box::new(LogicalPlan {
                    id: child_id,
                    stats: child_stats,
                    operator: LogicalOperator::Join(Join::Comparison(join)),
                }),
                ..filter
            }),
        };
    }
    join.join_type = JoinType::Semi;
    join.mark_index = None;
    join.mark_semantics = paro_planner::operator::MarkJoinSemantics::NotMark;
    join.left_projection_map = paro_planner::operator::ProjectionMap::all();
    join.right_projection_map = paro_planner::operator::ProjectionMap::none();
    LogicalPlan {
        id,
        stats,
        operator: LogicalOperator::Join(Join::Comparison(join)),
    }
}
