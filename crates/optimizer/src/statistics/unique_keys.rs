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
use paro_planner::expression::Expression;
use paro_planner::operator::{ColumnBinding, Get, JoinComparisonType, JoinCondition};

/// Evidence that every candidate key binding is evaluated by an ordinary
/// equality predicate and therefore rejects NULL before uniqueness is used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NullRejectedKeyProof {
    bindings: Box<[ColumnBinding]>,
}

impl NullRejectedKeyProof {
    pub(crate) fn from_equal_right_keys(conditions: &[JoinCondition]) -> Option<Self> {
        if conditions.is_empty()
            || conditions
                .iter()
                .any(|condition| condition.comparison != JoinComparisonType::Equal)
        {
            return None;
        }
        let bindings = conditions
            .iter()
            .map(|condition| match &condition.right {
                Expression::ColumnRef(column) if column.depth == 0 => Some(column.binding),
                _ => None,
            })
            .collect::<Option<Box<[_]>>>()?;
        Some(Self { bindings })
    }

    pub(crate) fn bindings(&self) -> &[ColumnBinding] {
        &self.bindings
    }

    pub(crate) fn proves(&self, conditions: &[JoinCondition]) -> bool {
        conditions.len() == self.bindings.len()
            && conditions
                .iter()
                .zip(self.bindings.iter())
                .all(|(condition, expected)| {
                    condition.comparison == JoinComparisonType::Equal
                        && matches!(&condition.right,
                            Expression::ColumnRef(column)
                                if column.depth == 0 && column.binding == *expected)
                })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyNullSemantics<'a> {
    /// An equality predicate rejects every row with a NULL key component.
    NullRejected(&'a NullRejectedKeyProof),
    /// NULL key components participate in equality, as they do in GROUP BY.
    NullsEqual,
}

pub(crate) struct DeclaredUniqueKey {
    pub(crate) bindings: Vec<ColumnBinding>,
    primary_key: bool,
}

impl DeclaredUniqueKey {
    pub(crate) fn is_covered_by(&self, candidates: &[ColumnBinding]) -> bool {
        self.bindings
            .iter()
            .all(|binding| candidates.contains(binding))
    }

    pub(crate) fn proves_uniqueness(
        &self,
        null_semantics: KeyNullSemantics<'_>,
        mut has_no_null: impl FnMut(ColumnBinding) -> bool,
    ) -> bool {
        match null_semantics {
            KeyNullSemantics::NullRejected(proof) => self.is_covered_by(proof.bindings()),
            KeyNullSemantics::NullsEqual => {
                self.primary_key || self.bindings.iter().copied().all(&mut has_no_null)
            }
        }
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
        assert!(proof.proves(&[equal]));
        assert!(!proof.proves(&[JoinCondition::new(
            column(1, 0),
            column(2, 1),
            JoinComparisonType::Equal,
        )]));

        let null_safe = JoinCondition::new(
            column(1, 0),
            column(2, 0),
            JoinComparisonType::NotDistinctFrom,
        );
        assert!(NullRejectedKeyProof::from_equal_right_keys(&[null_safe]).is_none());
    }
}
