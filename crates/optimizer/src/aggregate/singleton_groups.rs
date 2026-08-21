// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Prove grouped aggregates whose settled input has at most one row per key.
//!
//! A nullable-side aggregate grouped by its join key is unique by
//! construction. Joining it to a preserved relation whose GROUP BY domain
//! contains a declared, NULL-safe unique key therefore leaves at most one row
//! in every outer group. Partial-merge functions may publish an exact scalar
//! singleton law; physical lowering can then avoid constructing a second hash
//! table without guessing aggregate semantics from a function name.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use paro_function::aggregate::AggregateSingletonMerge;
use paro_planner::expression::{AggregateType, Expression};
use paro_planner::operator::{
    Aggregate, ColumnBinding, GroupInputMultiplicity, Join, JoinComparisonType, JoinType,
    LogicalOperator,
};
use paro_planner::plan::LogicalPlan;
use paro_storage::statistics::ColumnStatistics;

use crate::statistics::unique_keys::declared_unique_keys;

pub fn optimize_plan(
    plan: LogicalPlan,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
) -> LogicalPlan {
    plan.map_children(|child| optimize_plan(child, column_stats))
        .map_operator(|operator| match operator {
            LogicalOperator::Aggregate(mut aggregate) => {
                aggregate.group_input_multiplicity = if proves_at_most_one(&aggregate, column_stats)
                {
                    GroupInputMultiplicity::AtMostOne
                } else {
                    GroupInputMultiplicity::Arbitrary
                };
                LogicalOperator::Aggregate(aggregate)
            }
            operator => operator,
        })
}

fn proves_at_most_one(
    aggregate: &Aggregate,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
) -> bool {
    if aggregate.post_reduction.is_some()
        || aggregate.aggregates.is_empty()
        || !aggregate.has_plain_grouping_domain()
    {
        return false;
    }
    let LogicalOperator::Join(Join::Comparison(join)) = &aggregate.child.operator else {
        return false;
    };
    if join.join_type != JoinType::Left
        || join.conditions.is_empty()
        || join.mark_index.is_some()
        || !join.duplicate_eliminated_columns.is_empty()
        || join.delim_flipped
        || join
            .conditions
            .iter()
            .any(|condition| condition.comparison != JoinComparisonType::Equal)
    {
        return false;
    }
    let LogicalOperator::Get(preserved) = &join.left.operator else {
        return false;
    };
    let LogicalOperator::Aggregate(partial) = &join.right.operator else {
        return false;
    };
    if !partial.has_plain_grouping_domain() || partial.post_reduction.is_some() {
        return false;
    }

    let group_bindings = aggregate
        .groups
        .iter()
        .map(column_binding)
        .collect::<Option<HashSet<_>>>();
    let Some(group_bindings) = group_bindings else {
        return false;
    };
    let unique_group = declared_unique_keys(preserved).into_iter().any(|key| {
        key.bindings
            .iter()
            .all(|binding| group_bindings.contains(binding))
            && key.is_unique_with_nulls_equal(|binding| {
                column_stats
                    .get(&binding)
                    .is_some_and(|statistics| !statistics.statistics().can_have_null())
            })
    });
    if !unique_group {
        return false;
    }

    // Every grouping output of the nullable-side aggregate must participate
    // in ordinary equality. Otherwise multiple partial rows could match one
    // preserved key even though the partial itself is grouped.
    let expected_partial_keys = (0..partial.groups.len())
        .map(|ordinal| ColumnBinding::new(partial.group_index, ordinal))
        .collect::<HashSet<_>>();
    let preserved_bindings = join
        .left
        .get_column_bindings()
        .into_iter()
        .collect::<HashSet<_>>();
    let mut matched_partial_keys = HashSet::with_capacity(expected_partial_keys.len());
    for condition in &join.conditions {
        let (left, right) = (
            column_binding(&condition.left),
            column_binding(&condition.right),
        );
        let partial_key = match (left, right) {
            (Some(left), Some(right))
                if preserved_bindings.contains(&left) && expected_partial_keys.contains(&right) =>
            {
                Some(right)
            }
            (Some(left), Some(right))
                if expected_partial_keys.contains(&left) && preserved_bindings.contains(&right) =>
            {
                Some(left)
            }
            _ => None,
        };
        let Some(partial_key) = partial_key else {
            return false;
        };
        if !matched_partial_keys.insert(partial_key) {
            return false;
        }
    }
    if matched_partial_keys != expected_partial_keys {
        return false;
    }

    let aggregates_valid = aggregate.aggregates.iter().all(|expression| {
        let Expression::Aggregate(merge) = expression else {
            return false;
        };
        if merge.aggr_type != AggregateType::NonDistinct
            || merge.filter.is_some()
            || !merge.order_bys.is_empty()
            || merge.children.len() != 1
            || merge.function.singleton_merge().is_none()
        {
            return false;
        }
        let Some(binding) = column_binding(&merge.children[0]) else {
            return false;
        };
        if binding.table_index != partial.aggregate_index {
            return false;
        }
        let Some(Expression::Aggregate(source)) = partial.aggregates.get(binding.column_index)
        else {
            return false;
        };
        let semantics = source
            .function
            .partial_merge_function()
            .is_some_and(|expected| expected.execution_semantics_equal(&merge.function));
        semantics
            && matches!(
                merge.function.singleton_merge(),
                Some(AggregateSingletonMerge::Input | AggregateSingletonMerge::InputOr(_))
            )
    });
    aggregates_valid
}

fn column_binding(expression: &Expression) -> Option<ColumnBinding> {
    let Expression::ColumnRef(column) = expression else {
        return None;
    };
    (column.depth == 0).then_some(column.binding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_catalog::entry::{
        CatalogObjectId, ColumnDefinition, Constraint, CreateTableInfo, TableCatalogEntry,
    };
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::count::get_count_function;
    use paro_planner::expression::{AggregateExpression, ColumnRefExpression};
    use paro_planner::operator::{ComparisonJoin, ExpressionGet, Get, JoinCondition};
    use paro_storage::statistics::{BaseStatistics, ColumnStatistics};
    use paro_storage::table::table_factory::TableFactory;

    fn column(table: usize, ordinal: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table, ordinal),
            LogicalType::BigInt,
        ))
    }

    fn count(input: Expression) -> Expression {
        let (function, _) = get_count_function()
            .bind(&[LogicalType::BigInt])
            .expect("bind count");
        Expression::Aggregate(AggregateExpression::new(
            function,
            vec![input],
            LogicalType::BigInt,
        ))
    }

    fn candidate() -> (LogicalPlan, HashMap<ColumnBinding, Arc<ColumnStatistics>>) {
        let types = vec![LogicalType::BigInt];
        let storage = Arc::new(TableFactory::default().create_table(&types).unwrap());
        let info = CreateTableInfo::new(
            "paro".to_string(),
            "public".to_string(),
            "preserved".to_string(),
            vec![ColumnDefinition::new(
                "key".to_string(),
                LogicalType::BigInt,
            )],
        )
        .with_constraints(vec![Constraint::unique(vec![0])]);
        let table = Arc::new(
            TableCatalogEntry::from_info(info, storage, CatalogObjectId::from_raw(91_001), 0)
                .unwrap(),
        );
        let left = LogicalPlan::synthetic(LogicalOperator::Get(Get::new(
            1,
            vec!["key".to_string()],
            types,
            table,
        )));
        let source_count = count(column(2, 1));
        let Expression::Aggregate(source) = &source_count else {
            unreachable!()
        };
        let merge = source.function.partial_merge_function().unwrap();
        let right = LogicalPlan::synthetic(LogicalOperator::Aggregate(Aggregate::new(
            3,
            4,
            5,
            LogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
                2,
                vec![],
                vec!["key".to_string(), "value".to_string()],
                vec![LogicalType::BigInt, LogicalType::BigInt],
            ))),
            vec![column(2, 0)],
            vec![],
            vec![source_count],
            vec![],
        )));
        let join = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Left,
                left,
                right,
                vec![JoinCondition::equality(column(1, 0), column(3, 0))],
            ),
        )));
        let outer = Aggregate::new(
            6,
            7,
            8,
            join,
            vec![column(1, 0)],
            vec![],
            vec![Expression::Aggregate(AggregateExpression::new(
                merge,
                vec![column(4, 0)],
                LogicalType::BigInt,
            ))],
            vec![],
        );
        let mut statistics = HashMap::new();
        statistics.insert(
            ColumnBinding::new(1, 0),
            Arc::new(ColumnStatistics::new(BaseStatistics::create_empty(
                LogicalType::BigInt,
            ))),
        );
        (
            LogicalPlan::synthetic(LogicalOperator::Aggregate(outer)),
            statistics,
        )
    }

    #[test]
    fn unique_preserved_key_and_partial_merge_prove_singleton_groups() {
        let (plan, statistics) = candidate();
        let optimized = optimize_plan(plan, &statistics);
        let LogicalOperator::Aggregate(aggregate) = optimized.operator else {
            panic!("aggregate root")
        };
        assert_eq!(
            aggregate.group_input_multiplicity,
            GroupInputMultiplicity::AtMostOne
        );
    }

    #[test]
    fn nullable_unique_key_does_not_prove_group_by_singletons() {
        let (plan, mut statistics) = candidate();
        statistics.insert(
            ColumnBinding::new(1, 0),
            ColumnStatistics::create_unknown(LogicalType::BigInt),
        );
        let optimized = optimize_plan(plan, &statistics);
        let LogicalOperator::Aggregate(aggregate) = optimized.operator else {
            panic!("aggregate root")
        };
        assert_eq!(
            aggregate.group_input_multiplicity,
            GroupInputMultiplicity::Arbitrary
        );
    }
}
