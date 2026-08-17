// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared declared-key proofs with explicit SQL NULL semantics.
//!
//! Catalog `UNIQUE` keys are declared optimizer guarantees, not storage-
//! enforced indexes. Callers may rely on the declaration, including SQL's
//! allowance for multiple NULL tuples, but must make their NULL equality
//! semantics explicit in every proof.

use std::collections::HashMap;

use paro_catalog::entry::ConstraintType;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::{ColumnBinding, Get, JoinComparisonType, JoinCondition};

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
    let mut column_indices = HashMap::with_capacity(get.column_ids.len());
    for (column_index, &column_id) in get.column_ids.iter().enumerate() {
        column_indices.entry(column_id).or_insert(column_index);
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
