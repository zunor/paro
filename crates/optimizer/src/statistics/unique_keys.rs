// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared declared-key proofs with explicit SQL NULL semantics.

use paro_catalog::entry::ConstraintType;
use paro_planner::operator::{ColumnBinding, Get};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyNullSemantics {
    /// An equality predicate rejects every row with a NULL key component.
    NullRejected,
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
        null_semantics: KeyNullSemantics,
        mut has_no_null: impl FnMut(ColumnBinding) -> bool,
    ) -> bool {
        match null_semantics {
            KeyNullSemantics::NullRejected => true,
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
                    get.column_ids
                        .iter()
                        .position(|candidate| candidate == column_id)
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
