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
//!
//! Catalog UNIQUE constraints are declared optimizer guarantees, including
//! `NOT ENFORCED` declarations. Unlike join elimination, singleton lowering
//! makes duplicate declared keys observable as extra output groups; data that
//! violates its declared constraint is outside this optimization's contract.

use std::collections::HashMap;
use std::sync::Arc;

use paro_planner::expression::Expression;
use paro_planner::operator::{
    binding_preserving_get, Aggregate, ColumnBinding, GroupInputMultiplicity, Join,
    LogicalOperator, SingletonGroupProof,
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
                aggregate.group_input_multiplicity = prove_at_most_one(&aggregate, column_stats)
                    .map(GroupInputMultiplicity::AtMostOne)
                    .unwrap_or(GroupInputMultiplicity::Arbitrary);
                LogicalOperator::Aggregate(aggregate)
            }
            operator => operator,
        })
}

fn prove_at_most_one(
    aggregate: &Aggregate,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
) -> Option<SingletonGroupProof> {
    let LogicalOperator::Join(Join::Comparison(join)) = &aggregate.child.operator else {
        return None;
    };
    let preserved = binding_preserving_get(join.left.as_ref())?;
    let group_bindings = aggregate
        .groups
        .iter()
        .map(column_binding)
        .collect::<Option<Vec<_>>>()?;
    let key = declared_unique_keys(preserved).into_iter().find(|key| {
        key.bindings
            .iter()
            .all(|binding| group_bindings.contains(binding))
            && key.is_unique_with_nulls_equal(|binding| {
                column_stats
                    .get(&binding)
                    .is_some_and(|statistics| !statistics.statistics().can_have_null())
            })
    })?;
    let proof = SingletonGroupProof::new(key.bindings);
    if !proof.is_valid_for(aggregate) {
        return None;
    };
    Some(proof)
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
    use paro_function::aggregate::distributive::count::{
        get_count_function, get_count_star_function,
    };
    use paro_planner::binder::ir::GroupingSet;
    use paro_planner::expression::{AggregateExpression, ColumnRefExpression, ReferenceExpression};
    use paro_planner::operator::{
        ComparisonJoin, ExpressionGet, Filter, Get, JoinComparisonType, JoinCondition, JoinType,
    };
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

    fn candidate_join_mut(plan: &mut LogicalPlan) -> &mut ComparisonJoin {
        let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
            panic!("aggregate root")
        };
        let LogicalOperator::Join(Join::Comparison(join)) = &mut aggregate.child.operator else {
            panic!("comparison join child")
        };
        join
    }

    fn candidate_aggregate_mut(plan: &mut LogicalPlan) -> &mut Aggregate {
        let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
            panic!("aggregate root")
        };
        aggregate
    }

    #[test]
    fn unique_preserved_key_and_partial_merge_prove_singleton_groups() {
        let (plan, statistics) = candidate();
        let optimized = optimize_plan(plan, &statistics);
        let LogicalOperator::Aggregate(aggregate) = optimized.operator else {
            panic!("aggregate root")
        };
        assert!(matches!(
            aggregate.group_input_multiplicity,
            GroupInputMultiplicity::AtMostOne(_)
        ));
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

    #[test]
    fn preserved_filter_keeps_the_declared_key_proof() {
        let (mut plan, statistics) = candidate();
        let join = candidate_join_mut(&mut plan);
        let left = std::mem::replace(
            &mut join.left,
            Box::new(LogicalPlan::synthetic(LogicalOperator::DummyScan)),
        );
        join.left = Box::new(LogicalPlan::synthetic(LogicalOperator::Filter(
            Filter::new(*left, Vec::new()),
        )));

        let optimized = optimize_plan(plan, &statistics);
        let LogicalOperator::Aggregate(aggregate) = optimized.operator else {
            panic!("aggregate root")
        };
        assert!(matches!(
            aggregate.group_input_multiplicity,
            GroupInputMultiplicity::AtMostOne(_)
        ));
    }

    #[test]
    fn uncovered_partial_group_key_rejects_singleton_lowering() {
        let (mut plan, statistics) = candidate();
        let join = candidate_join_mut(&mut plan);
        let LogicalOperator::Aggregate(partial) = &mut join.right.operator else {
            panic!("partial aggregate")
        };
        partial.groups.push(column(2, 1));
        partial.recompute_returned_types();

        let optimized = optimize_plan(plan, &statistics);
        let LogicalOperator::Aggregate(aggregate) = optimized.operator else {
            panic!("aggregate root")
        };
        assert_eq!(
            aggregate.group_input_multiplicity,
            GroupInputMultiplicity::Arbitrary
        );
    }

    #[test]
    fn non_equality_join_rejects_singleton_lowering() {
        let (mut plan, statistics) = candidate();
        candidate_join_mut(&mut plan).conditions[0].comparison = JoinComparisonType::GreaterThan;

        let optimized = optimize_plan(plan, &statistics);
        let LogicalOperator::Aggregate(aggregate) = optimized.operator else {
            panic!("aggregate root")
        };
        assert_eq!(
            aggregate.group_input_multiplicity,
            GroupInputMultiplicity::Arbitrary
        );
    }

    #[test]
    fn resolved_input_references_preserve_the_structural_witness() {
        let (plan, statistics) = candidate();
        let mut optimized = optimize_plan(plan, &statistics);
        let aggregate = candidate_aggregate_mut(&mut optimized);
        let GroupInputMultiplicity::AtMostOne(proof) = aggregate.group_input_multiplicity.clone()
        else {
            panic!("singleton proof")
        };
        let LogicalOperator::Join(Join::Comparison(join)) = &mut aggregate.child.operator else {
            panic!("comparison join")
        };
        let left_bindings = join.left.get_column_bindings();
        let right_bindings = join.right.get_column_bindings();
        let left_key = left_bindings
            .iter()
            .position(|binding| *binding == ColumnBinding::new(1, 0))
            .expect("left key");
        let right_key = right_bindings
            .iter()
            .position(|binding| *binding == ColumnBinding::new(3, 0))
            .expect("right key");
        join.conditions[0].left =
            Expression::Reference(ReferenceExpression::new(left_key, LogicalType::BigInt));
        join.conditions[0].right =
            Expression::Reference(ReferenceExpression::new(right_key, LogicalType::BigInt));

        let child_bindings = aggregate.child.get_column_bindings();
        let group_key = child_bindings
            .iter()
            .position(|binding| *binding == ColumnBinding::new(1, 0))
            .expect("group key");
        let partial_value = child_bindings
            .iter()
            .position(|binding| *binding == ColumnBinding::new(4, 0))
            .expect("partial value");
        aggregate.groups[0] =
            Expression::Reference(ReferenceExpression::new(group_key, LogicalType::BigInt));
        let Expression::Aggregate(merge) = &mut aggregate.aggregates[0] else {
            panic!("merge aggregate")
        };
        merge.children[0] =
            Expression::Reference(ReferenceExpression::new(partial_value, LogicalType::BigInt));

        assert!(proof.is_valid_for(aggregate));
    }

    #[test]
    fn merge_without_singleton_law_rejects_projection_lowering() {
        let (mut plan, statistics) = candidate();
        let aggregate = candidate_aggregate_mut(&mut plan);
        let Expression::Aggregate(merge) = &mut aggregate.aggregates[0] else {
            panic!("merge aggregate")
        };
        merge.function = get_count_star_function();
        assert!(merge.function.singleton_merge().is_none());

        let optimized = optimize_plan(plan, &statistics);
        let LogicalOperator::Aggregate(aggregate) = optimized.operator else {
            panic!("aggregate root")
        };
        assert_eq!(
            aggregate.group_input_multiplicity,
            GroupInputMultiplicity::Arbitrary
        );
    }

    #[test]
    fn multiple_grouping_domains_reject_singleton_lowering() {
        let (mut plan, statistics) = candidate();
        candidate_aggregate_mut(&mut plan).grouping_sets = vec![
            GroupingSet {
                expressions: vec![0],
            },
            GroupingSet {
                expressions: Vec::new(),
            },
        ];

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
