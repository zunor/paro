// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Typed planner operand domains at the native scalar boundary.

use std::collections::{BTreeMap, HashMap};

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::expression::{ColumnRefExpression, Expression, ReferenceExpression};
use paro_planner::operator::ColumnBinding;

use super::super::column::{ColumnCatalog, ColumnOrigin, ColumnVisibility};
use super::super::ids::{ColumnId, StableFingerprintBuilder};

/// A relational binding and a reducer-local slot can have the same owner and
/// ordinal. They still name different values, even if their types also match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum BindingDomain {
    Relation,
    PostAggregateReducer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BindingKey {
    binding: ColumnBinding,
    type_id: u32,
    domain: BindingDomain,
}

/// Stable planner binding to optimizer column identities with an append-only
/// journal. Rollback removes only the transaction's delta, including hidden
/// scalar scopes; no complete query map is cloned for a rule attempt.
#[derive(Debug, Clone, Default)]
pub(crate) struct BindingCatalog {
    entries: BTreeMap<BindingKey, ColumnId>,
    by_column: BTreeMap<ColumnId, BindingKey>,
    insertions: Vec<BindingKey>,
    /// Exact query-local type interning keeps hot binding lookup independent
    /// of cryptographic type fingerprints.
    types: Vec<LogicalType>,
    type_ids: HashMap<LogicalType, u32>,
}

impl BindingCatalog {
    /// Read relational ownership without rebuilding an executable scalar.
    /// Private reducer slots cannot satisfy a relational placement proof.
    pub(crate) fn relation_binding(&self, column: ColumnId) -> Option<ColumnBinding> {
        let key = self.by_column.get(&column)?;
        (key.domain == BindingDomain::Relation).then_some(key.binding)
    }

    pub(crate) fn get(
        &self,
        table_index: usize,
        column_index: usize,
        logical_type: &LogicalType,
    ) -> Option<&ColumnId> {
        self.get_in(
            BindingDomain::Relation,
            ColumnBinding::new(table_index, column_index),
            logical_type,
        )
    }

    fn get_in(
        &self,
        domain: BindingDomain,
        binding: ColumnBinding,
        logical_type: &LogicalType,
    ) -> Option<&ColumnId> {
        let type_id = *self.type_ids.get(logical_type)?;
        self.entries.get(&BindingKey {
            binding,
            type_id,
            domain,
        })
    }

    pub(crate) fn insert(
        &mut self,
        table_index: usize,
        column_index: usize,
        logical_type: &LogicalType,
        column: ColumnId,
    ) -> Result<()> {
        self.insert_in(
            BindingDomain::Relation,
            ColumnBinding::new(table_index, column_index),
            logical_type,
            column,
        )
    }

    fn insert_in(
        &mut self,
        domain: BindingDomain,
        binding: ColumnBinding,
        logical_type: &LogicalType,
        column: ColumnId,
    ) -> Result<()> {
        let type_id = if let Some(type_id) = self.type_ids.get(logical_type).copied() {
            type_id
        } else {
            let type_id = u32::try_from(self.types.len())
                .map_err(|_| paro_error::internal("query type arena exceeds u32 identity space"))?;
            self.types.push(logical_type.clone());
            self.type_ids.insert(logical_type.clone(), type_id);
            type_id
        };
        let key = BindingKey {
            binding,
            type_id,
            domain,
        };
        if let Some(existing) = self.entries.get(&key) {
            if *existing != column {
                return Err(paro_error::internal(
                    "planner binding changed its optimizer column identity",
                ));
            }
            return Ok(());
        }
        if self
            .by_column
            .get(&column)
            .is_some_and(|existing| *existing != key)
        {
            return Err(paro_error::internal(
                "optimizer column acquired incompatible planner operand bindings",
            ));
        }
        self.entries.insert(key, column);
        self.by_column.insert(column, key);
        self.insertions.push(key);
        Ok(())
    }

    pub(super) fn intern_reducer_output(
        &mut self,
        owner: usize,
        ordinal: usize,
        logical_type: &LogicalType,
        columns: &mut ColumnCatalog,
    ) -> Result<ColumnId> {
        let domain = BindingDomain::PostAggregateReducer;
        let binding = ColumnBinding::new(owner, ordinal);
        if let Some(column) = self.get_in(domain, binding, logical_type) {
            return Ok(*column);
        }
        let mut key = StableFingerprintBuilder::default();
        key.write_bytes(b"paro.post-aggregate-reducer-output.v1");
        key.write_u64(owner as u64);
        key.write_u64(ordinal as u64);
        super::super::scalar::encode_logical_type(&mut key, logical_type);
        let column = columns.intern(
            logical_type.clone(),
            true,
            ColumnOrigin::Internal { key: key.finish() },
            ColumnVisibility::Hidden,
            None,
        )?;
        self.insert_in(domain, binding, logical_type, column)?;
        Ok(column)
    }

    /// Restore the operand in the selected local evaluation scope. A private
    /// reducer slot cannot escape as an ordinary relational ColumnRef, nor
    /// can an input column be mistaken for a reducer-local Reference.
    pub(super) fn export_column(
        &self,
        column: ColumnId,
        logical_type: &LogicalType,
        depth: usize,
        reducer_owner: Option<usize>,
    ) -> Result<Expression> {
        let key = self.by_column.get(&column).ok_or_else(|| {
            paro_error::internal("native scalar column has no planner operand binding")
        })?;
        if self.types.get(key.type_id as usize) != Some(logical_type) {
            return Err(paro_error::internal(
                "native scalar column changed its binding type",
            ));
        }
        match (key.domain, reducer_owner) {
            (BindingDomain::Relation, None) => Ok(Expression::ColumnRef(
                ColumnRefExpression::with_depth(key.binding, logical_type.clone(), depth).into(),
            )),
            (BindingDomain::PostAggregateReducer, Some(owner))
                if key.binding.table_index == owner && depth == 0 =>
            {
                Ok(Expression::Reference(
                    ReferenceExpression::new(key.binding.column_index, logical_type.clone()).into(),
                ))
            }
            _ => Err(paro_error::internal(
                "native scalar column escapes its operand domain",
            )),
        }
    }

    pub(crate) fn checkpoint(&self) -> usize {
        self.insertions.len()
    }

    pub(crate) fn rollback_to(&mut self, checkpoint: usize) -> Result<()> {
        if checkpoint > self.insertions.len() {
            return Err(paro_error::internal(
                "binding catalog rollback exceeds its insertion journal",
            ));
        }
        while self.insertions.len() > checkpoint {
            let key = self.insertions.pop().expect("journal length was checked");
            let Some(column) = self.entries.remove(&key) else {
                return Err(paro_error::internal(
                    "binding catalog insertion journal disagrees with its index",
                ));
            };
            if self.by_column.remove(&column) != Some(key) {
                return Err(paro_error::internal(
                    "binding catalog insertion journal disagrees with its reverse index",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reducer_domains_are_private_typed_and_transactional() {
        let mut bindings = BindingCatalog::default();
        let mut columns = ColumnCatalog::default();
        let ty = LogicalType::BigInt;
        let relation = super::super::intern_column_binding(
            ColumnBinding::new(17, 0),
            ty.clone(),
            &mut bindings,
            &mut columns,
        )
        .unwrap();
        let checkpoint = bindings.checkpoint();
        let column_checkpoint = columns.len();
        let reducer = bindings
            .intern_reducer_output(17, 0, &ty, &mut columns)
            .unwrap();
        assert_ne!(relation, reducer);
        assert_eq!(bindings.get(17, 0, &ty), Some(&relation));
        assert_eq!(
            bindings
                .intern_reducer_output(17, 0, &ty, &mut columns)
                .unwrap(),
            reducer
        );
        assert_ne!(
            bindings
                .intern_reducer_output(18, 0, &ty, &mut columns)
                .unwrap(),
            reducer
        );
        assert_ne!(
            bindings
                .intern_reducer_output(17, 1, &ty, &mut columns)
                .unwrap(),
            reducer
        );
        assert_ne!(
            bindings
                .intern_reducer_output(17, 0, &LogicalType::Integer, &mut columns)
                .unwrap(),
            reducer
        );
        assert_eq!(
            columns.get(reducer).unwrap().visibility,
            ColumnVisibility::Hidden
        );
        bindings.rollback_to(checkpoint).unwrap();
        columns.truncate(column_checkpoint).unwrap();
        assert_eq!(bindings.get(17, 0, &ty), Some(&relation));
        assert!(bindings
            .get_in(
                BindingDomain::PostAggregateReducer,
                ColumnBinding::new(17, 0),
                &ty
            )
            .is_none());
        assert_eq!(
            bindings
                .intern_reducer_output(17, 0, &ty, &mut columns)
                .unwrap(),
            reducer
        );
    }

    #[test]
    fn export_rejects_private_scope_escape_type_changes_and_stale_columns() {
        let mut bindings = BindingCatalog::default();
        let mut columns = ColumnCatalog::default();
        let ty = LogicalType::BigInt;
        let relation = super::super::intern_column_binding(
            ColumnBinding::new(17, 0),
            ty.clone(),
            &mut bindings,
            &mut columns,
        )
        .unwrap();
        let checkpoint = bindings.checkpoint();
        let reducer = bindings
            .intern_reducer_output(17, 0, &ty, &mut columns)
            .unwrap();
        let Expression::Reference(reference) =
            bindings.export_column(reducer, &ty, 0, Some(17)).unwrap()
        else {
            panic!("expected local reference")
        };
        assert_eq!(reference.index, 0);
        let Expression::ColumnRef(reference) =
            bindings.export_column(relation, &ty, 2, None).unwrap()
        else {
            panic!("expected correlated column")
        };
        assert_eq!(reference.binding, ColumnBinding::new(17, 0));
        assert_eq!(reference.depth, 2);
        for (column, ty, depth, owner) in [
            (reducer, LogicalType::BigInt, 0, None),
            (reducer, LogicalType::BigInt, 0, Some(18)),
            (reducer, LogicalType::BigInt, 1, Some(17)),
            (reducer, LogicalType::Integer, 0, Some(17)),
            (relation, LogicalType::BigInt, 0, Some(17)),
            (relation, LogicalType::Integer, 0, None),
        ] {
            assert!(bindings.export_column(column, &ty, depth, owner).is_err());
        }
        // Conflicting reverse identity is rejected before either index changes.
        assert!(bindings.insert(18, 0, &ty, relation).is_err());
        assert!(bindings.get(18, 0, &ty).is_none());
        bindings.rollback_to(checkpoint).unwrap();
        assert!(bindings.export_column(reducer, &ty, 0, Some(17)).is_err());
        assert!(bindings.export_column(relation, &ty, 0, None).is_ok());
    }
}
