// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Proof-driven predicates derived from a join region's equality classes.
//!
//! This module never consumes statistics. Every emitted predicate follows
//! solely from an INNER equality and an original non-null constant equality,
//! so estimates cannot change query semantics.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use paro_planner::expression::{
    ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
};
use paro_planner::logical::operator::{ColumnBinding, JoinType};

use super::query_graph::FilterInfo;
use super::relation::JoinRelationSetManager;
use super::relation_manager::RelationManager;

pub(super) fn infer_equality_constants(
    filters: &[Arc<FilterInfo>],
    relations: &RelationManager,
    sets: &mut JoinRelationSetManager,
) -> Vec<Arc<FilterInfo>> {
    let first_filter_index = filters
        .iter()
        .map(|filter| filter.filter_index)
        .max()
        .map_or(0, |index| index.saturating_add(1));

    inferred_predicates(filters)
        .into_iter()
        .enumerate()
        .filter_map(|(offset, (column, constant))| {
            let relation = relations.get_relation_id(column.binding.table_index)?;
            let expression = Expression::Comparison(
                ComparisonExpression::new(
                    ComparisonType::Equal,
                    Expression::ColumnRef(column.clone().into()),
                    Expression::Constant(
                        ConstantExpression {
                            value: constant.value,
                            return_type: column.return_type.clone(),
                        }
                        .into(),
                    ),
                )
                .into(),
            );
            let set = sets.get_relation(relation);
            let mut filter =
                FilterInfo::new_inner(expression, set, first_filter_index.saturating_add(offset));
            filter.set_left_binding(column.binding, relation);
            Some(Arc::new(filter))
        })
        .collect()
}

fn inferred_predicates(
    filters: &[Arc<FilterInfo>],
) -> Vec<(ColumnRefExpression, ConstantExpression)> {
    let mut adjacency = HashMap::<ColumnBinding, Vec<ColumnBinding>>::new();
    let mut columns = HashMap::<ColumnBinding, ColumnRefExpression>::new();
    let mut constants = Vec::<(ColumnBinding, ConstantExpression)>::new();
    let mut existing = HashSet::<(ColumnBinding, String)>::new();

    for filter in filters {
        if filter.join_type() != JoinType::Inner {
            continue;
        }
        let Expression::Comparison(comparison) = &filter.filter else {
            continue;
        };
        if comparison.comparison_type != ComparisonType::Equal {
            continue;
        }
        match (comparison.left.as_ref(), comparison.right.as_ref()) {
            (Expression::ColumnRef(left), Expression::ColumnRef(right)) => {
                columns
                    .entry(left.binding)
                    .or_insert_with(|| left.as_ref().clone());
                columns
                    .entry(right.binding)
                    .or_insert_with(|| right.as_ref().clone());
                adjacency
                    .entry(left.binding)
                    .or_default()
                    .push(right.binding);
                adjacency
                    .entry(right.binding)
                    .or_default()
                    .push(left.binding);
            }
            (Expression::ColumnRef(column), Expression::Constant(constant))
            | (Expression::Constant(constant), Expression::ColumnRef(column))
                if !constant.value.is_null() =>
            {
                columns
                    .entry(column.binding)
                    .or_insert_with(|| column.as_ref().clone());
                constants.push((column.binding, constant.as_ref().clone()));
                existing.insert((column.binding, format!("{:?}", constant.value)));
            }
            _ => {}
        }
    }

    let mut inferred = Vec::new();
    for (source, constant) in constants {
        let value_key = format!("{:?}", constant.value);
        let mut visited = HashSet::new();
        let mut frontier = vec![source];
        while let Some(binding) = frontier.pop() {
            if !visited.insert(binding) {
                continue;
            }
            if existing.insert((binding, value_key.clone())) {
                if let Some(column) = columns.get(&binding) {
                    inferred.push((column.clone(), constant.clone()));
                }
            }
            frontier.extend(adjacency.get(&binding).into_iter().flatten().copied());
        }
    }
    inferred
}

#[cfg(test)]
mod tests {
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    use super::*;

    fn column(table: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(table, 0), LogicalType::Integer).into(),
        )
    }

    fn filter(
        expression: Expression,
        set: Arc<super::super::relation::JoinRelationSet>,
        index: usize,
    ) -> Arc<FilterInfo> {
        Arc::new(FilterInfo::new_inner(expression, set, index))
    }

    #[test]
    fn inner_equality_class_propagates_non_null_constants() {
        let mut sets = JoinRelationSetManager::new();
        let equality = filter(
            Expression::Comparison(
                ComparisonExpression::new(ComparisonType::Equal, column(10), column(20)).into(),
            ),
            sets.get_relation_from_vec(vec![0, 1]),
            0,
        );
        let constant = filter(
            Expression::Comparison(
                ComparisonExpression::new(
                    ComparisonType::Equal,
                    column(10),
                    Expression::Constant(
                        ConstantExpression::new(Value::Integer(25), LogicalType::Integer).into(),
                    ),
                )
                .into(),
            ),
            sets.get_relation(0),
            1,
        );

        let inferred = inferred_predicates(&[equality, constant]);
        assert_eq!(inferred.len(), 1);
        assert_eq!(inferred[0].0.binding, ColumnBinding::new(20, 0));
        assert_eq!(inferred[0].1.value, Value::Integer(25));
    }
}
