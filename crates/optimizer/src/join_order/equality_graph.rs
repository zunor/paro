// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Precompiled equality-class graph topology for join cardinality estimation.

use std::collections::HashMap;

use paro_planner::expression::{ComparisonType, Expression};
use paro_planner::operator::{ColumnBinding, JoinType};

use super::cardinality::RelationsSetToStats;

#[derive(Debug, Clone)]
pub(super) struct EqualityGraphVertex {
    pub(super) relation: usize,
    pub(super) binding: ColumnBinding,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct EqualityGraphEdge {
    pub(super) left: usize,
    pub(super) right: usize,
    pub(super) filter_index: usize,
}

#[derive(Debug, Clone)]
pub(super) struct EqualityClassGraph {
    pub(super) stats_index: usize,
    pub(super) vertices: Vec<EqualityGraphVertex>,
    pub(super) edges: Vec<EqualityGraphEdge>,
    /// Edge indexes adjacent to each binding vertex. Cardinality estimation
    /// enumerates many relation subsets, so compile topology once instead of
    /// rescanning every edge for every step of every spanning tree.
    pub(super) adjacency: Vec<Vec<usize>>,
}

impl EqualityClassGraph {
    pub(super) fn build_all(stats_sets: &[RelationsSetToStats]) -> Vec<Self> {
        stats_sets
            .iter()
            .enumerate()
            .filter_map(|(stats_index, stats)| Self::build(stats_index, stats))
            .collect()
    }

    fn build(stats_index: usize, stats: &RelationsSetToStats) -> Option<Self> {
        let mut vertices = Vec::<EqualityGraphVertex>::new();
        let mut vertex_index = HashMap::<ColumnBinding, usize>::new();
        let mut edges = Vec::new();

        for filter in &stats.filters {
            if filter.join_type() != JoinType::Inner || !is_equality(&filter.filter) {
                continue;
            }
            let (Some(left_set), Some(right_set)) = (filter.left_set(), filter.right_set()) else {
                continue;
            };
            if left_set.count() != 1 || right_set.count() != 1 {
                continue;
            }
            let left_relation = left_set.relations()[0];
            let right_relation = right_set.relations()[0];
            if left_relation == right_relation {
                continue;
            }

            let (Some(left_binding), Some(right_binding)) =
                (filter.left_binding, filter.right_binding)
            else {
                continue;
            };
            debug_assert_eq!(left_binding.relation, left_relation);
            debug_assert_eq!(right_binding.relation, right_relation);

            // Vertices represent equality domains, not relations. A self-join
            // can equate two columns from one relation alias with a column on
            // the other alias; merging those columns into one vertex silently
            // drops a rank factor from the equality graph.
            let left = *vertex_index.entry(left_binding.column).or_insert_with(|| {
                let index = vertices.len();
                vertices.push(EqualityGraphVertex {
                    relation: left_relation,
                    binding: left_binding.column,
                });
                index
            });
            let right = *vertex_index.entry(right_binding.column).or_insert_with(|| {
                let index = vertices.len();
                vertices.push(EqualityGraphVertex {
                    relation: right_relation,
                    binding: right_binding.column,
                });
                index
            });
            edges.push(EqualityGraphEdge {
                left,
                right,
                filter_index: filter.filter_index,
            });
        }

        if edges.is_empty() {
            return None;
        }
        let mut adjacency = vec![Vec::new(); vertices.len()];
        for (edge_index, edge) in edges.iter().enumerate() {
            adjacency[edge.left].push(edge_index);
            adjacency[edge.right].push(edge_index);
        }
        Some(Self {
            stats_index,
            vertices,
            edges,
            adjacency,
        })
    }
}

fn is_equality(expression: &Expression) -> bool {
    match expression {
        Expression::Comparison(comparison) => matches!(
            comparison.comparison_type,
            ComparisonType::Equal | ComparisonType::NotDistinctFrom
        ),
        _ => false,
    }
}

pub(super) fn find_component(parents: &mut [usize], index: usize) -> usize {
    let mut root = index;
    while parents[root] != root {
        root = parents[root];
    }

    let mut current = index;
    while parents[current] != current {
        let parent = parents[current];
        parents[current] = root;
        current = parent;
    }
    root
}

pub(super) fn union_components(
    parents: &mut [usize],
    sizes: &mut [usize],
    left: usize,
    right: usize,
) {
    let mut left = find_component(parents, left);
    let mut right = find_component(parents, right);
    if left == right {
        return;
    }
    if sizes[left] < sizes[right] {
        std::mem::swap(&mut left, &mut right);
    }
    parents[right] = left;
    sizes[left] += sizes[right];
}

#[cfg(test)]
mod tests {
    use super::is_equality;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ComparisonExpression, ComparisonType, ConjunctionExpression, ConjunctionType,
        ConstantExpression, Expression,
    };

    fn equality() -> Expression {
        Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::Constant(ConstantExpression::new(
                Value::Integer(1),
                LogicalType::Integer,
            )),
            Expression::Constant(ConstantExpression::new(
                Value::Integer(1),
                LogicalType::Integer,
            )),
        ))
    }

    #[test]
    fn only_atomic_equalities_enter_the_equality_graph() {
        assert!(is_equality(&equality()));
        let disjunction = Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::Or,
            vec![equality(), equality()],
        ));
        assert!(!is_equality(&disjunction));
        let conjunction = Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::And,
            vec![equality(), equality()],
        ));
        assert!(!is_equality(&conjunction));
    }
}
