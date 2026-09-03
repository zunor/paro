// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared declared-key proofs with explicit SQL NULL semantics.
//!
//! Catalog `UNIQUE` keys are declared optimizer guarantees, not storage-
//! enforced indexes. Callers may rely on the declaration, including SQL's
//! allowance for multiple NULL tuples, but must make their NULL equality
//! semantics explicit in every proof.

use std::collections::{HashMap, HashSet};

use paro_catalog::entry::ConstraintType;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::{
    ColumnBinding, Get, Join, JoinComparisonType, JoinCondition, JoinType, LogicalOperator,
};
use paro_planner::plan::LogicalPlan;

/// Evidence that every candidate key binding is evaluated by an ordinary
/// equality predicate and therefore rejects NULL before uniqueness is used.
#[derive(Debug, Clone)]
pub(crate) struct NullRejectedKeyProof {
    keys: Box<[NullRejectedRightKey]>,
}

#[derive(Debug, Clone)]
struct NullRejectedRightKey {
    left: Expression,
    right: ColumnRefExpression,
}

impl NullRejectedKeyProof {
    pub(crate) fn from_equal_right_keys(conditions: &[JoinCondition]) -> Option<Self> {
        if conditions.is_empty() {
            return None;
        }
        let keys = conditions
            .iter()
            .cloned()
            .map(|condition| {
                if condition.comparison != JoinComparisonType::Equal {
                    return None;
                }
                let Expression::ColumnRef(right) = condition.right else {
                    return None;
                };
                if right.depth != 0 {
                    return None;
                }
                Some(NullRejectedRightKey {
                    left: condition.left,
                    right,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self { keys: keys.into() })
    }

    pub(crate) fn bindings(&self) -> impl Iterator<Item = ColumnBinding> + '_ {
        self.keys.iter().map(|key| key.right.binding)
    }

    /// Reconstruct the exact ordinary-equality conditions encoded by the
    /// witness. Equality is a type invariant rather than mutable payload.
    #[cfg(test)]
    pub(crate) fn conditions(&self) -> impl Iterator<Item = JoinCondition> + '_ {
        self.keys.iter().map(|key| {
            JoinCondition::new(
                key.left.clone(),
                Expression::ColumnRef(key.right.clone()),
                JoinComparisonType::Equal,
            )
        })
    }

    pub(crate) fn right_keys(&self) -> impl Iterator<Item = (&Expression, &ColumnRefExpression)> {
        self.keys.iter().map(|key| (&key.left, &key.right))
    }
}

pub(crate) struct DeclaredUniqueKey {
    pub(crate) bindings: Vec<ColumnBinding>,
    primary_key: bool,
}

impl DeclaredUniqueKey {
    pub(crate) fn is_unique_with_nulls_rejected(&self, proof: &NullRejectedKeyProof) -> bool {
        self.bindings
            .iter()
            .all(|binding| proof.bindings().any(|candidate| candidate == *binding))
    }

    /// Whether the declared key remains unique when NULL tuples compare equal,
    /// as they do in GROUP BY. Key coverage is deliberately a separate proof
    /// obligation so all callers use the same division of responsibility.
    pub(crate) fn is_unique_with_nulls_equal(
        &self,
        mut has_no_null: impl FnMut(ColumnBinding) -> bool,
    ) -> bool {
        self.primary_key || self.bindings.iter().copied().all(&mut has_no_null)
    }
}

pub(crate) fn declared_unique_keys(get: &Get) -> Vec<DeclaredUniqueKey> {
    let Some(table) = &get.table else {
        return Vec::new();
    };
    let mut column_indices = HashMap::with_capacity(get.column_sources.len());
    for column_index in 0..get.column_sources.len() {
        if let Some(column_id) = get.stored_column(column_index) {
            column_indices.entry(column_id).or_insert(column_index);
        }
    }
    table
        .constraints()
        .iter()
        .filter(|constraint| {
            matches!(
                constraint.constraint_type,
                ConstraintType::Unique | ConstraintType::PrimaryKey
            ) && !constraint.columns.is_empty()
        })
        .filter_map(|constraint| {
            let bindings = constraint
                .columns
                .iter()
                .map(|column_id| {
                    column_indices
                        .get(column_id)
                        .copied()
                        .map(|column_index| ColumnBinding::new(get.table_index, column_index))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(DeclaredUniqueKey {
                bindings,
                primary_key: constraint.constraint_type == ConstraintType::PrimaryKey,
            })
        })
        .collect()
}

/// A key over the positional output of one logical node.
///
/// The key is unique whenever all of its columns are non-NULL. This is the
/// common contract shared by catalog `UNIQUE` constraints and SQL grouping:
/// ordinary equality predicates reject NULL and may therefore consume either
/// proof, while null-safe equality must not.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ProvenUniqueOutputKey {
    output_indices: Vec<usize>,
}

#[derive(Debug)]
struct UniqueKeyState {
    layout: paro_planner::operator::LogicalOutputLayout,
    keys: Vec<ProvenUniqueOutputKey>,
}

enum UniqueKeyTask<'a> {
    Visit(&'a LogicalPlan),
    Finish(&'a LogicalPlan, usize),
}

/// Return the proven keys as stable output bindings for statistics consumers.
pub(crate) fn proven_unique_keys(plan: &LogicalPlan) -> Vec<Vec<ColumnBinding>> {
    let state = derive_unique_key_state(plan);
    state
        .keys
        .into_iter()
        .filter_map(|key| {
            key.output_indices
                .into_iter()
                .map(|index| state.layout.bindings().get(index).copied())
                .collect()
        })
        .collect()
}

/// Prove that the supplied expressions cover one key of the current relation.
/// Callers remain responsible for proving ordinary-equality NULL rejection.
pub(crate) fn expressions_cover_unique_key(
    plan: &LogicalPlan,
    expressions: &[&Expression],
) -> bool {
    if expressions.is_empty() {
        return false;
    }
    let state = derive_unique_key_state(plan);
    let covered = expressions
        .iter()
        .filter_map(|expression| expression_output_index(expression, &state.layout))
        .collect::<HashSet<_>>();
    covered.len() == expressions.len()
        && state.keys.iter().any(|key| {
            !key.output_indices.is_empty()
                && key
                    .output_indices
                    .iter()
                    .all(|index| covered.contains(index))
        })
}

fn derive_unique_key_state(plan: &LogicalPlan) -> UniqueKeyState {
    let mut tasks = vec![UniqueKeyTask::Visit(plan)];
    let mut states = Vec::<UniqueKeyState>::new();
    while let Some(task) = tasks.pop() {
        match task {
            UniqueKeyTask::Visit(plan) => {
                let children = plan.children();
                tasks.push(UniqueKeyTask::Finish(plan, children.len()));
                tasks.extend(children.into_iter().rev().map(UniqueKeyTask::Visit));
            }
            UniqueKeyTask::Finish(plan, child_count) => {
                let child_offset = states
                    .len()
                    .checked_sub(child_count)
                    .expect("unique-key traversal lost a child state");
                let children = states.split_off(child_offset);
                let child_layouts = children
                    .iter()
                    .map(|state| state.layout.clone())
                    .collect::<Vec<_>>();
                let layout = plan.operator.output_layout_from_children(&child_layouts);
                let keys = derive_local_unique_keys(&plan.operator, &layout, &children);
                states.push(UniqueKeyState { layout, keys });
            }
        }
    }
    assert_eq!(
        states.len(),
        1,
        "unique-key traversal must produce exactly one root state"
    );
    states.pop().expect("root unique-key state was checked")
}

fn derive_local_unique_keys(
    operator: &LogicalOperator,
    layout: &paro_planner::operator::LogicalOutputLayout,
    children: &[UniqueKeyState],
) -> Vec<ProvenUniqueOutputKey> {
    let mut keys = match operator {
        LogicalOperator::Get(get) => declared_keys_in_layout(get, layout),
        LogicalOperator::SearchScan(search) => declared_keys_in_layout(&search.get, layout),
        LogicalOperator::FullTextFilterScan(search) => declared_keys_in_layout(&search.get, layout),
        LogicalOperator::Filter(filter) => project_unique_keys(
            child_keys(children, 0),
            &filter
                .projection_map
                .to_indices(child_layout(children, 0).len()),
            0,
        ),
        LogicalOperator::Order(order) => project_unique_keys(
            child_keys(children, 0),
            &order
                .projection_map
                .to_indices(child_layout(children, 0).len()),
            0,
        ),
        LogicalOperator::Projection(projection) => {
            let child = child_state(children, 0);
            let sources = projection
                .expressions
                .iter()
                .map(|expression| expression_output_index(expression, &child.layout))
                .collect::<Vec<_>>();
            remap_unique_keys(&child.keys, &sources)
        }
        LogicalOperator::Limit(_)
        | LogicalOperator::TopN(_)
        | LogicalOperator::Window(_)
        | LogicalOperator::EmptyResult(_)
        | LogicalOperator::RowFetch(_)
        | LogicalOperator::ExternalProject(_)
        | LogicalOperator::GraphExpand(_) => child_keys(children, 0).to_vec(),
        LogicalOperator::Distinct(_) => {
            let mut keys = child_keys(children, 0).to_vec();
            if !layout.is_empty() {
                keys.push(ProvenUniqueOutputKey {
                    output_indices: (0..layout.len()).collect(),
                });
            }
            keys
        }
        LogicalOperator::Aggregate(aggregate) if aggregate.has_plain_grouping_domain() => {
            vec![ProvenUniqueOutputKey {
                output_indices: (0..aggregate.groups.len()).collect(),
            }]
        }
        LogicalOperator::Join(Join::Comparison(join))
            if join.duplicate_eliminated_columns.is_empty() && !join.delim_flipped =>
        {
            comparison_join_unique_keys(join, children)
        }
        LogicalOperator::MaterializedCTE(_) => child_keys(children, 1).to_vec(),
        _ => Vec::new(),
    };
    normalize_unique_keys(&mut keys);
    keys
}

fn comparison_join_unique_keys(
    join: &paro_planner::operator::ComparisonJoin,
    children: &[UniqueKeyState],
) -> Vec<ProvenUniqueOutputKey> {
    let left = child_state(children, 0);
    let right = child_state(children, 1);
    let mut left_equalities = HashSet::new();
    let mut right_equalities = HashSet::new();
    for condition in &join.conditions {
        if condition.comparison != JoinComparisonType::Equal {
            continue;
        }
        let Some(left_index) = expression_output_index(&condition.left, &left.layout) else {
            continue;
        };
        let Some(right_index) = expression_output_index(&condition.right, &right.layout) else {
            continue;
        };
        left_equalities.insert(left_index);
        right_equalities.insert(right_index);
    }
    let left_join_key_unique = relation_key_is_covered(&left.keys, &left_equalities);
    let right_join_key_unique = relation_key_is_covered(&right.keys, &right_equalities);

    let left_projection = join.left_projection_map.to_indices(left.layout.len());
    let right_projection = join.right_projection_map.to_indices(right.layout.len());
    let mut keys = Vec::new();
    let preserve_left = match join.join_type {
        JoinType::Semi | JoinType::Anti | JoinType::Mark | JoinType::Single => true,
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Outer => {
            right_join_key_unique
        }
        JoinType::RightSemi | JoinType::RightAnti | JoinType::Invalid => false,
    };
    let preserve_right = match join.join_type {
        JoinType::RightSemi | JoinType::RightAnti => true,
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Outer | JoinType::Single => {
            left_join_key_unique
        }
        JoinType::Semi | JoinType::Anti | JoinType::Mark | JoinType::Invalid => false,
    };
    if preserve_left {
        keys.extend(project_unique_keys(&left.keys, &left_projection, 0));
    }
    if preserve_right {
        keys.extend(project_unique_keys(
            &right.keys,
            &right_projection,
            left_projection.len(),
        ));
    }
    keys
}

fn declared_keys_in_layout(
    get: &Get,
    layout: &paro_planner::operator::LogicalOutputLayout,
) -> Vec<ProvenUniqueOutputKey> {
    declared_unique_keys(get)
        .into_iter()
        .filter_map(|key| {
            let output_indices = key
                .bindings
                .iter()
                .map(|binding| {
                    layout
                        .bindings()
                        .iter()
                        .position(|candidate| candidate == binding)
                })
                .collect::<Option<Vec<_>>>()?;
            Some(ProvenUniqueOutputKey { output_indices })
        })
        .collect()
}

fn project_unique_keys(
    keys: &[ProvenUniqueOutputKey],
    projected_child_indices: &[usize],
    output_offset: usize,
) -> Vec<ProvenUniqueOutputKey> {
    keys.iter()
        .filter_map(|key| {
            let output_indices = key
                .output_indices
                .iter()
                .map(|child_index| {
                    projected_child_indices
                        .iter()
                        .position(|candidate| candidate == child_index)
                        .map(|index| output_offset + index)
                })
                .collect::<Option<Vec<_>>>()?;
            Some(ProvenUniqueOutputKey { output_indices })
        })
        .collect()
}

fn remap_unique_keys(
    keys: &[ProvenUniqueOutputKey],
    source_by_output: &[Option<usize>],
) -> Vec<ProvenUniqueOutputKey> {
    keys.iter()
        .filter_map(|key| {
            let output_indices = key
                .output_indices
                .iter()
                .map(|child_index| {
                    source_by_output
                        .iter()
                        .position(|source| source == &Some(*child_index))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(ProvenUniqueOutputKey { output_indices })
        })
        .collect()
}

fn expression_output_index(
    expression: &Expression,
    layout: &paro_planner::operator::LogicalOutputLayout,
) -> Option<usize> {
    match expression {
        Expression::Reference(reference) => {
            (reference.index < layout.len()).then_some(reference.index)
        }
        Expression::ColumnRef(column) if column.depth == 0 => layout
            .bindings()
            .iter()
            .position(|binding| *binding == column.binding),
        _ => None,
    }
}

fn relation_key_is_covered(keys: &[ProvenUniqueOutputKey], covered: &HashSet<usize>) -> bool {
    keys.iter().any(|key| {
        !key.output_indices.is_empty()
            && key
                .output_indices
                .iter()
                .all(|index| covered.contains(index))
    })
}

fn normalize_unique_keys(keys: &mut Vec<ProvenUniqueOutputKey>) {
    for key in keys.iter_mut() {
        key.output_indices.sort_unstable();
        key.output_indices.dedup();
    }
    keys.retain(|key| !key.output_indices.is_empty());
    keys.sort_by(|left, right| {
        left.output_indices
            .len()
            .cmp(&right.output_indices.len())
            .then_with(|| left.output_indices.cmp(&right.output_indices))
    });
    let mut retained = Vec::<ProvenUniqueOutputKey>::new();
    for key in keys.drain(..) {
        if retained.iter().any(|candidate| {
            candidate
                .output_indices
                .iter()
                .all(|index| key.output_indices.contains(index))
        }) {
            continue;
        }
        retained.push(key);
    }
    *keys = retained;
}

fn child_state(children: &[UniqueKeyState], index: usize) -> &UniqueKeyState {
    children
        .get(index)
        .expect("unique-key derivation requires its logical child state")
}

fn child_layout(
    children: &[UniqueKeyState],
    index: usize,
) -> &paro_planner::operator::LogicalOutputLayout {
    &child_state(children, index).layout
}

fn child_keys(children: &[UniqueKeyState], index: usize) -> &[ProvenUniqueOutputKey] {
    &child_state(children, index).keys
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;
    use paro_planner::expression::ColumnRefExpression;

    use super::*;

    fn column(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table_index, column_index),
            LogicalType::BigInt,
        ))
    }

    #[test]
    fn null_rejection_proof_requires_ordinary_equality() {
        let equal = JoinCondition::new(column(1, 0), column(2, 0), JoinComparisonType::Equal);
        let proof = NullRejectedKeyProof::from_equal_right_keys(&[equal.clone()])
            .expect("ordinary equality proves NULL rejection");
        let condition = proof.conditions().next().expect("sole equality condition");
        assert_eq!(condition.comparison, JoinComparisonType::Equal);
        assert!(matches!(&condition.right,
            Expression::ColumnRef(column) if column.binding == ColumnBinding::new(2, 0)));

        let null_safe = JoinCondition::new(
            column(1, 0),
            column(2, 0),
            JoinComparisonType::NotDistinctFrom,
        );
        assert!(NullRejectedKeyProof::from_equal_right_keys(&[null_safe]).is_none());
    }

    #[test]
    fn nullable_unique_key_requires_its_typed_null_rejection_proof() {
        let key = DeclaredUniqueKey {
            bindings: vec![ColumnBinding::new(2, 0), ColumnBinding::new(2, 1)],
            primary_key: false,
        };
        let conditions = [
            JoinCondition::new(column(1, 0), column(2, 0), JoinComparisonType::Equal),
            JoinCondition::new(column(1, 1), column(2, 1), JoinComparisonType::Equal),
        ];
        let complete = NullRejectedKeyProof::from_equal_right_keys(&conditions).unwrap();
        assert!(key.is_unique_with_nulls_rejected(&complete));

        let incomplete = NullRejectedKeyProof::from_equal_right_keys(&conditions[..1]).unwrap();
        assert!(!key.is_unique_with_nulls_rejected(&incomplete));
    }
}
