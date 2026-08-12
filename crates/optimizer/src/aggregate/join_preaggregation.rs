// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Reduce multiplicative outer joins before grouped aggregation.
//!
//! For an equality left join grouped by the preserved key, distributive
//! aggregates over only the nullable side can be computed once per nullable
//! key before the join. The original aggregate then merges finalized partials.
//! This preserves duplicate keys on the left and the zero result of COUNT for
//! unmatched rows, while bounding the join build and output to one row per
//! nullable-side key.

use std::collections::HashMap;
use std::sync::Arc;

use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    AggregateExpression, AggregateType, ColumnRefExpression, Expression,
};
use paro_planner::operator::{
    Aggregate, ColumnBinding, ComparisonJoin, Join, JoinComparisonType, JoinType, LogicalOperator,
};
use paro_planner::plan::LogicalPlan;
use paro_storage::statistics::ColumnStatistics;

/// Pre-aggregate eligible nullable sides when statistics predict a material
/// reduction in join rows.
pub fn optimize_plan(
    plan: LogicalPlan,
    bind_context: &BindContext,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
) -> LogicalPlan {
    fn rewrite(
        plan: LogicalPlan,
        bind_context: &BindContext,
        column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    ) -> LogicalPlan {
        let mut plan = plan.map_children(|child| rewrite(child, bind_context, column_stats));
        let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
            return plan;
        };
        let Some(right_key) = JoinPreaggregation::candidate_right_key(aggregate) else {
            return plan;
        };
        if !JoinPreaggregation::estimated_to_reduce(aggregate, right_key, column_stats) {
            return plan;
        }
        JoinPreaggregation::rewrite(aggregate, bind_context);
        plan
    }

    rewrite(plan, bind_context, column_stats)
}

struct JoinPreaggregation;

impl JoinPreaggregation {
    const MIN_INPUT_ROWS: u64 = 1_024;
    const MIN_REDUCTION_NUMERATOR: u64 = 3;
    const MIN_REDUCTION_DENOMINATOR: u64 = 2;

    fn candidate_right_key(aggregate: &Aggregate) -> Option<ColumnBinding> {
        if aggregate.groups.len() != 1
            || aggregate.aggregates.is_empty()
            || !aggregate.grouping_sets.is_empty()
            || !aggregate.grouping_functions.is_empty()
        {
            return None;
        }
        let LogicalOperator::Join(Join::Comparison(join)) = &aggregate.child.operator else {
            return None;
        };
        if !Self::clean_left_join(join) || join.conditions.len() != 1 {
            return None;
        }
        let condition = &join.conditions[0];
        if condition.comparison != JoinComparisonType::Equal {
            return None;
        }

        let left_bindings = join.left.get_column_bindings();
        let right_bindings = join.right.get_column_bindings();
        let condition_left = Self::column_binding(&condition.left)?;
        let condition_right = Self::column_binding(&condition.right)?;
        let (left_key, right_key) = if left_bindings.contains(&condition_left)
            && right_bindings.contains(&condition_right)
        {
            (condition_left, condition_right)
        } else if left_bindings.contains(&condition_right)
            && right_bindings.contains(&condition_left)
        {
            (condition_right, condition_left)
        } else {
            return None;
        };
        if Self::column_binding(&aggregate.groups[0]) != Some(left_key) {
            return None;
        }

        for expression in &aggregate.aggregates {
            let Expression::Aggregate(partial) = expression else {
                return None;
            };
            if partial.aggr_type != AggregateType::NonDistinct
                || partial.filter.is_some()
                || !partial.order_bys.is_empty()
                || partial.children.len() != 1
                || partial.function.partial_merge_function().is_none()
            {
                return None;
            }
            let input = Self::column_binding(&partial.children[0])?;
            if !right_bindings.contains(&input) {
                return None;
            }
            let merge = partial.function.partial_merge_function()?;
            if merge.arguments != [partial.return_type.clone()]
                || merge.return_type != partial.return_type
            {
                return None;
            }
        }
        Some(right_key)
    }

    fn estimated_to_reduce(
        aggregate: &Aggregate,
        right_key: ColumnBinding,
        column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    ) -> bool {
        let LogicalOperator::Join(Join::Comparison(join)) = &aggregate.child.operator else {
            return false;
        };
        let Some(input_rows) = join
            .right
            .stats
            .estimated_cardinality
            .map(|estimate| estimate.expected)
        else {
            return false;
        };
        let distinct = column_stats
            .get(&right_key)
            .map(|stats| stats.get_distinct_count() as u64)
            .unwrap_or(0)
            .min(input_rows);
        input_rows >= Self::MIN_INPUT_ROWS
            && distinct > 0
            && input_rows.saturating_mul(Self::MIN_REDUCTION_DENOMINATOR)
                >= distinct.saturating_mul(Self::MIN_REDUCTION_NUMERATOR)
    }

    fn rewrite(aggregate: &mut Aggregate, bind_context: &BindContext) -> bool {
        let Some(right_key) = Self::candidate_right_key(aggregate) else {
            return false;
        };
        let LogicalOperator::Join(Join::Comparison(join)) = &mut aggregate.child.operator else {
            return false;
        };
        let right_bindings = join.right.get_column_bindings();
        let condition = &mut join.conditions[0];
        let right_is_condition_right = Self::column_binding(&condition.right) == Some(right_key);
        let right_is_condition_left = Self::column_binding(&condition.left) == Some(right_key);
        if right_is_condition_left == right_is_condition_right {
            return false;
        }

        let group_index = bind_context.generate_table_index();
        let aggregate_index = bind_context.generate_table_index();
        let groupings_index = bind_context.generate_table_index();
        let partials = aggregate.aggregates.clone();
        let merge_expressions = partials
            .iter()
            .enumerate()
            .map(|(index, expression)| {
                let Expression::Aggregate(partial) = expression else {
                    unreachable!("candidate aggregates were validated")
                };
                let merge = partial
                    .function
                    .partial_merge_function()
                    .expect("candidate partial merge was validated");
                let partial_ref = Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(aggregate_index, index),
                    partial.return_type.clone(),
                ));
                Expression::Aggregate(AggregateExpression::new(
                    merge,
                    vec![partial_ref],
                    partial.return_type.clone(),
                ))
            })
            .collect::<Vec<_>>();

        debug_assert!(partials.iter().all(|expression| {
            let Expression::Aggregate(partial) = expression else {
                return false;
            };
            Self::column_binding(&partial.children[0])
                .is_some_and(|binding| right_bindings.contains(&binding))
        }));
        let right = std::mem::replace(
            join.right.as_mut(),
            LogicalPlan::synthetic(LogicalOperator::DummyScan),
        );
        let partial_plan = LogicalPlan::new(
            bind_context,
            LogicalOperator::Aggregate(Aggregate::new(
                group_index,
                aggregate_index,
                groupings_index,
                right,
                vec![if right_is_condition_right {
                    condition.right.clone()
                } else {
                    condition.left.clone()
                }],
                vec![],
                partials,
                vec![],
            )),
        );
        *join.right = partial_plan;

        let group_ref = Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(group_index, 0),
            if right_is_condition_right {
                condition.right.return_type()
            } else {
                condition.left.return_type()
            },
        ));
        if right_is_condition_right {
            condition.right = group_ref;
        } else {
            condition.left = group_ref;
        }
        aggregate.aggregates = merge_expressions;
        aggregate.recompute_returned_types();
        true
    }

    fn clean_left_join(join: &ComparisonJoin) -> bool {
        join.join_type == JoinType::Left
            && join.mark_index.is_none()
            && join.duplicate_eliminated_columns.is_empty()
            && !join.delim_flipped
            && join.left_projection_map.is_all()
            && join.right_projection_map.is_all()
    }

    fn column_binding(expression: &Expression) -> Option<ColumnBinding> {
        let Expression::ColumnRef(column) = expression else {
            return None;
        };
        (column.depth == 0).then_some(column.binding)
    }
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::count::get_count_function;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{AggregateExpression, ColumnRefExpression, Expression};
    use paro_planner::operator::{
        Aggregate, ColumnBinding, ComparisonJoin, ExpressionGet, Join, JoinCondition, JoinType,
        LogicalOperator,
    };
    use paro_planner::plan::LogicalPlan;

    use super::JoinPreaggregation;

    fn column(table: usize, index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table, index),
            LogicalType::BigInt,
        ))
    }

    fn input(table: usize, columns: usize) -> LogicalPlan {
        LogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            table,
            vec![],
            (0..columns).map(|idx| format!("c{idx}")).collect(),
            vec![LogicalType::BigInt; columns],
        )))
    }

    fn count(input: Expression) -> Expression {
        let (function, targets) = get_count_function().bind(&[LogicalType::BigInt]).unwrap();
        assert_eq!(targets, [LogicalType::BigInt]);
        Expression::Aggregate(AggregateExpression::new(
            function,
            vec![input],
            LogicalType::BigInt,
        ))
    }

    fn candidate(bind_context: &BindContext) -> LogicalPlan {
        let join = LogicalPlan::new(
            bind_context,
            LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Left,
                input(1, 1),
                input(2, 2),
                vec![JoinCondition::equality(column(1, 0), column(2, 0))],
            ))),
        );
        LogicalPlan::new(
            bind_context,
            LogicalOperator::Aggregate(Aggregate::new(
                3,
                4,
                5,
                join,
                vec![column(1, 0)],
                vec![],
                vec![count(column(2, 1))],
                vec![],
            )),
        )
    }

    #[test]
    fn count_over_left_join_is_decomposed_into_partial_and_merge() {
        let bind_context = BindContext::new();
        let mut plan = candidate(&bind_context);
        let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
            panic!("aggregate root")
        };

        assert!(JoinPreaggregation::rewrite(aggregate, &bind_context));

        let Expression::Aggregate(merge) = &aggregate.aggregates[0] else {
            panic!("merge aggregate")
        };
        assert_eq!(merge.function.name, "count_partial_merge");
        let LogicalOperator::Join(Join::Comparison(join)) = &aggregate.child.operator else {
            panic!("left join")
        };
        let LogicalOperator::Aggregate(partial) = &join.right.operator else {
            panic!("right partial aggregate")
        };
        assert_eq!((partial.groups.len(), partial.aggregates.len()), (1, 1));
        assert_eq!(partial.aggregates[0].return_type(), LogicalType::BigInt);
    }

    #[test]
    fn rewrite_declines_count_from_preserved_side() {
        let bind_context = BindContext::new();
        let mut plan = candidate(&bind_context);
        let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
            unreachable!()
        };
        aggregate.aggregates = vec![count(column(1, 0))];

        assert!(!JoinPreaggregation::rewrite(aggregate, &bind_context));
    }
}
