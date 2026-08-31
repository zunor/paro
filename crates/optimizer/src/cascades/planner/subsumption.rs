// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Input normalization local to aggregate join-subsumption proof attempts.

use super::*;

pub(super) fn normalize_input(plan: LogicalPlan) -> LogicalPlan {
    let plan = restore_direct_filter_bindings(plan);
    let plan = restore_semantic_projection_maps(plan);
    let plan = lower_positive_consumed_mark_filter(plan);
    FilterPushdown::new().rewrite_plan(plan)
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

fn restore_semantic_projection_maps(plan: LogicalPlan) -> LogicalPlan {
    plan.map_children(restore_semantic_projection_maps)
        .map_operator(|operator| match operator {
            LogicalOperator::Filter(mut filter) => {
                filter.projection_map = paro_planner::operator::ProjectionMap::all();
                LogicalOperator::Filter(filter)
            }
            LogicalOperator::Join(Join::Comparison(mut join)) => {
                if join.join_type == JoinType::Inner {
                    join.left_projection_map = paro_planner::operator::ProjectionMap::all();
                    join.right_projection_map = paro_planner::operator::ProjectionMap::all();
                }
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
