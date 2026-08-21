// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Replace aggregate inputs whose NULL behavior has become irrelevant.
//!
//! Bound aggregate functions publish an explicit equivalent implementation;
//! this pass supplies the independent, exact no-NULL proof. Display names are
//! never used as semantic capabilities.

use std::collections::HashMap;
use std::sync::Arc;

use paro_planner::expression::{AggregateType, Expression};
use paro_planner::operator::{binding_preserving_get, ColumnBinding, LogicalOperator};
use paro_planner::plan::LogicalPlan;
use paro_storage::statistics::ColumnStatistics;

pub fn optimize_plan(
    plan: LogicalPlan,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
) -> LogicalPlan {
    plan.map_children(|child| optimize_plan(child, column_stats))
        .map_operator(|operator| match operator {
            LogicalOperator::Aggregate(mut aggregate) => {
                for expression in &mut aggregate.aggregates {
                    rewrite_aggregate(expression, aggregate.child.as_ref(), column_stats);
                }
                LogicalOperator::Aggregate(aggregate)
            }
            operator => operator,
        })
}

fn rewrite_aggregate(
    expression: &mut Expression,
    child: &LogicalPlan,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
) {
    let Expression::Aggregate(aggregate) = expression else {
        return;
    };
    if aggregate.aggr_type != AggregateType::NonDistinct || aggregate.children.len() != 1 {
        return;
    }
    let Expression::ColumnRef(input) = &aggregate.children[0] else {
        return;
    };
    if input.depth != 0 || !binding_is_non_null_at(child, input.binding, column_stats) {
        return;
    }
    let Some(replacement) = aggregate.function.non_null_input_function() else {
        return;
    };
    if replacement.return_type != aggregate.function.return_type
        || replacement.return_type != aggregate.return_type
        || replacement.empty_input != aggregate.function.empty_input
        || !replacement.arguments.is_empty()
        || replacement.varargs.is_some()
    {
        return;
    }
    aggregate.function = replacement;
    aggregate.children.clear();
}

/// Prove non-NULL at the aggregate input, not merely at the base binding.
///
/// Column statistics are keyed by binding and therefore cannot distinguish a
/// base column from the NULL-extended copy produced by an outer join. Only
/// row-preserving unary operators may carry the base proof to this use site.
fn binding_is_non_null_at(
    plan: &LogicalPlan,
    binding: ColumnBinding,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
) -> bool {
    if !plan.get_column_bindings().contains(&binding) {
        return false;
    }
    let Some(get) = binding_preserving_get(plan) else {
        return false;
    };
    binding.table_index == get.table_index
        && get.stored_column(binding.column_index).is_some()
        && column_stats
            .get(&binding)
            .is_some_and(|statistics| !statistics.statistics().can_have_null())
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::count::get_count_function;
    use paro_planner::expression::{AggregateExpression, ColumnRefExpression};
    use paro_planner::operator::{
        ComparisonJoin, Filter, Get, Join, JoinComparisonType, JoinCondition, JoinType, Limit,
        Order, TopN,
    };
    use paro_storage::statistics::BaseStatistics;

    fn count(binding: ColumnBinding) -> Expression {
        let (function, _) = get_count_function()
            .bind(&[LogicalType::BigInt])
            .expect("bind count");
        Expression::Aggregate(AggregateExpression::new(
            function,
            vec![Expression::ColumnRef(ColumnRefExpression::new(
                binding,
                LogicalType::BigInt,
            ))],
            LogicalType::BigInt,
        ))
    }

    fn scan(binding: ColumnBinding) -> LogicalPlan {
        LogicalPlan::synthetic(LogicalOperator::Get(Get::new_without_table(
            binding.table_index,
            vec!["value".to_string()],
            vec![LogicalType::BigInt],
        )))
    }

    #[test]
    fn exact_non_null_count_uses_zero_argument_equivalent() {
        let binding = ColumnBinding::new(1, 0);
        let mut expression = count(binding);
        let mut statistics = HashMap::new();
        statistics.insert(
            binding,
            Arc::new(ColumnStatistics::new(BaseStatistics::new(
                LogicalType::BigInt,
            ))),
        );

        rewrite_aggregate(&mut expression, &scan(binding), &statistics);

        let Expression::Aggregate(aggregate) = expression else {
            panic!("expected aggregate");
        };
        assert_eq!(aggregate.function.name, "count_star");
        assert!(aggregate.children.is_empty());
    }

    #[test]
    fn nullable_count_keeps_its_input() {
        let binding = ColumnBinding::new(1, 0);
        let mut expression = count(binding);
        let mut statistics = HashMap::new();
        statistics.insert(
            binding,
            Arc::new(ColumnStatistics::new(BaseStatistics::create_unknown(
                LogicalType::BigInt,
            ))),
        );

        rewrite_aggregate(&mut expression, &scan(binding), &statistics);

        let Expression::Aggregate(aggregate) = expression else {
            panic!("expected aggregate");
        };
        assert_eq!(aggregate.function.name, "count");
        assert_eq!(aggregate.children.len(), 1);
    }

    #[test]
    fn outer_join_null_extension_rejects_base_non_null_statistics() {
        let left = ColumnBinding::new(1, 0);
        let right = ColumnBinding::new(2, 0);
        let mut expression = count(right);
        let child = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Left,
                scan(left),
                scan(right),
                vec![JoinCondition::new(
                    Expression::ColumnRef(ColumnRefExpression::new(left, LogicalType::BigInt)),
                    Expression::ColumnRef(ColumnRefExpression::new(right, LogicalType::BigInt)),
                    JoinComparisonType::Equal,
                )],
            ),
        )));
        let mut statistics = HashMap::new();
        statistics.insert(
            right,
            Arc::new(ColumnStatistics::new(BaseStatistics::new(
                LogicalType::BigInt,
            ))),
        );

        rewrite_aggregate(&mut expression, &child, &statistics);

        let Expression::Aggregate(aggregate) = expression else {
            panic!("expected aggregate");
        };
        assert_eq!(aggregate.function.name, "count");
        assert_eq!(aggregate.children.len(), 1);
    }

    #[test]
    fn binding_preserving_unaries_carry_the_non_null_proof() {
        let binding = ColumnBinding::new(1, 0);
        let mut statistics = HashMap::new();
        statistics.insert(
            binding,
            Arc::new(ColumnStatistics::new(BaseStatistics::new(
                LogicalType::BigInt,
            ))),
        );
        let wrappers = [
            LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                scan(binding),
                Vec::new(),
            ))),
            LogicalPlan::synthetic(LogicalOperator::Order(Order::new(
                scan(binding),
                Vec::new(),
            ))),
            LogicalPlan::synthetic(LogicalOperator::TopN(TopN::new(
                scan(binding),
                Vec::new(),
                10,
                0,
            ))),
            LogicalPlan::synthetic(LogicalOperator::Limit(Limit::new(
                scan(binding),
                None,
                None,
            ))),
        ];

        for child in wrappers {
            assert!(binding_is_non_null_at(&child, binding, &statistics));
        }
    }
}
