// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Memoized bound-root imports with explicit namespace and allocation validity.
//!
//! This is an import cache, not an alternative scalar representation. Values
//! are canonical ScalarExprIds; native rewrites continue to use the scalar DAG.

use super::super::catalog_identity::CatalogVersion;
use super::super::ids::{ColumnId, ScalarExprId};
use paro_planner::expression::{Expression, ExpressionIdentity, ExpressionWitness};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundImportDomain(pub CatalogVersion, pub CatalogVersion);

#[derive(Debug, Clone)]
struct ImportedRoot {
    source: ExpressionWitness,
    references: Box<[ColumnId]>,
    scalar: ScalarExprId,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BoundImportCache {
    domain: Option<BoundImportDomain>,
    roots: HashMap<ExpressionIdentity, Vec<ImportedRoot>>,
    next_sweep: usize,
    pub(crate) hits: u64,
    pub(crate) misses: u64,
}

impl BoundImportCache {
    pub(crate) fn clear(&mut self) {
        self.roots.clear();
        self.domain = None;
        self.next_sweep = 256;
    }

    pub(crate) fn lookup(
        &mut self,
        domain: BoundImportDomain,
        expression: &Expression,
        references: &[ColumnId],
    ) -> Option<ScalarExprId> {
        if self.domain != Some(domain) {
            self.clear();
            self.domain = Some(domain);
        }
        let found = self
            .roots
            .get(&expression.allocation_identity())
            .and_then(|entries| {
                entries
                    .iter()
                    .find(|entry| {
                        entry.references.as_ref() == references && entry.source.matches(expression)
                    })
                    .map(|entry| entry.scalar)
            });
        if found.is_some() {
            self.hits += 1;
        } else {
            self.misses += 1;
        }
        found
    }

    pub(crate) fn insert(
        &mut self,
        expression: &Expression,
        references: &[ColumnId],
        scalar: ScalarExprId,
    ) {
        // Geometric collection bounds stale weak control blocks between
        // sweeps. Live source roots keep their canonical imports; dead source
        // payloads/children are never retained by the cache in the first place.
        if self.roots.len() >= self.next_sweep {
            self.roots
                .retain(|_, entries| entries.first().is_some_and(|entry| entry.source.is_alive()));
            self.next_sweep = self.roots.len().saturating_mul(2).max(256);
        }
        self.roots
            .entry(expression.allocation_identity())
            .or_default()
            .push(ImportedRoot {
                source: expression.witness(),
                references: references.into(),
                scalar,
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cascades::scalar::ScalarKind;
    use crate::cascades::scalar_lowering::{
        intern_column_binding, intern_expression, BindingCatalog,
    };
    use crate::cascades::{
        ColumnCatalog, ColumnOrigin, ColumnVisibility, Fingerprint, ScalarArena,
    };
    use paro_common::{runtime_value::Value, types::LogicalType};
    use paro_planner::{
        expression::{ColumnRefExpression, ConstantExpression, ReferenceExpression},
        operator::ColumnBinding,
    };

    fn constant(value: bool) -> Expression {
        Expression::Constant(
            ConstantExpression::new(Value::Boolean(value), LogicalType::Boolean).into(),
        )
    }

    #[test]
    fn repeated_imports_skip_lowering_but_keep_reference_domains_distinct() {
        let mut arena = ScalarArena::default();
        let mut bindings = BindingCatalog::default();
        let mut columns = ColumnCatalog::default();
        let ids = (0..2)
            .map(|table| {
                intern_column_binding(
                    ColumnBinding::new(table, 0),
                    LogicalType::Integer,
                    &mut bindings,
                    &mut columns,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let expression =
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into());
        let a = intern_expression(
            &expression,
            &ids[..1],
            &mut bindings,
            &mut columns,
            &mut arena,
        )
        .unwrap();
        let b = intern_expression(
            &expression,
            &ids[1..],
            &mut bindings,
            &mut columns,
            &mut arena,
        )
        .unwrap();
        assert_ne!(a, b);
        for _ in 0..100 {
            for (references, expected) in [(&ids[..1], a), (&ids[1..], b)] {
                assert_eq!(
                    intern_expression(
                        &expression,
                        references,
                        &mut bindings,
                        &mut columns,
                        &mut arena
                    )
                    .unwrap(),
                    expected
                );
            }
        }
        assert_eq!(arena.bound_import_counts(), (200, 2));
        assert_eq!(arena.len(), 2);
    }

    #[test]
    fn rollback_of_each_namespace_invalidates_imports_before_ordinal_reuse() {
        let mut arena = ScalarArena::default();
        let mut bindings = BindingCatalog::default();
        let mut columns = ColumnCatalog::default();
        let expression = constant(true);
        let original =
            intern_expression(&expression, &[], &mut bindings, &mut columns, &mut arena).unwrap();
        arena.truncate(0).unwrap();
        let other = intern_expression(
            &constant(false),
            &[],
            &mut bindings,
            &mut columns,
            &mut arena,
        )
        .unwrap();
        assert_eq!(other, original);
        let restored =
            intern_expression(&expression, &[], &mut bindings, &mut columns, &mut arena).unwrap();
        assert_ne!(
            restored, other,
            "target rollback cannot validate a reused ScalarExprId"
        );

        let checkpoint = bindings.checkpoint();
        let old_column = intern_column_binding(
            ColumnBinding::new(0, 0),
            LogicalType::Integer,
            &mut bindings,
            &mut columns,
        )
        .unwrap();
        let expression = Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer).into(),
        );
        intern_expression(&expression, &[], &mut bindings, &mut columns, &mut arena).unwrap();
        bindings.rollback_to(checkpoint).unwrap();
        let new_column = columns
            .intern(
                LogicalType::Integer,
                true,
                ColumnOrigin::Internal {
                    key: Fingerprint(123),
                },
                ColumnVisibility::Visible,
                None,
            )
            .unwrap();
        assert_ne!(old_column, new_column);
        bindings
            .insert(0, 0, &LogicalType::Integer, new_column)
            .unwrap();
        let rebound =
            intern_expression(&expression, &[], &mut bindings, &mut columns, &mut arena).unwrap();
        assert_eq!(
            arena.get(rebound).unwrap().kind,
            ScalarKind::Column(new_column)
        );

        let reference =
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into());
        intern_expression(
            &reference,
            &[old_column],
            &mut bindings,
            &mut columns,
            &mut arena,
        )
        .unwrap();
        columns.truncate(0).unwrap();
        let reused = columns
            .intern(
                LogicalType::BigInt,
                true,
                ColumnOrigin::Internal {
                    key: Fingerprint(124),
                },
                ColumnVisibility::Visible,
                None,
            )
            .unwrap();
        assert_eq!(reused, old_column);
        assert!(
            intern_expression(
                &reference,
                &[reused],
                &mut bindings,
                &mut columns,
                &mut arena
            )
            .is_err(),
            "a cached import must not hide a changed reference-column type"
        );
    }

    #[test]
    fn source_mutation_catalog_forks_and_failed_imports_are_not_cache_hits() {
        let mut arena = ScalarArena::default();
        let mut bindings = BindingCatalog::default();
        let mut columns = ColumnCatalog::default();
        let mut expression = constant(true);
        let a =
            intern_expression(&expression, &[], &mut bindings, &mut columns, &mut arena).unwrap();
        if let Expression::Constant(payload) = &mut expression {
            payload.value = Value::Boolean(false);
        }
        let b =
            intern_expression(&expression, &[], &mut bindings, &mut columns, &mut arena).unwrap();
        assert_ne!(a, b);
        assert_eq!(arena.bound_import_counts(), (0, 2));
        let mut forked_columns = columns.clone();
        assert_eq!(
            intern_expression(
                &expression,
                &[],
                &mut bindings,
                &mut forked_columns,
                &mut arena
            )
            .unwrap(),
            b
        );
        let mut forked_bindings = bindings.clone();
        assert_eq!(
            intern_expression(
                &expression,
                &[],
                &mut forked_bindings,
                &mut forked_columns,
                &mut arena
            )
            .unwrap(),
            b
        );
        assert_eq!(arena.bound_import_counts(), (0, 4));
        let bad = Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into());
        for _ in 0..2 {
            assert!(intern_expression(&bad, &[], &mut bindings, &mut columns, &mut arena).is_err());
        }
        assert_eq!(arena.bound_import_counts(), (0, 6));
        assert!(!arena
            .bound_imports
            .roots
            .contains_key(&bad.allocation_identity()));
    }

    #[test]
    fn dead_source_control_blocks_are_collected_without_pinning_scalar_trees() {
        let mut arena = ScalarArena::default();
        let mut bindings = BindingCatalog::default();
        let mut columns = ColumnCatalog::default();
        let live = constant(true);
        let id = intern_expression(&live, &[], &mut bindings, &mut columns, &mut arena).unwrap();
        for _ in 0..5_000 {
            let temporary = constant(false);
            intern_expression(&temporary, &[], &mut bindings, &mut columns, &mut arena).unwrap();
        }
        assert!(arena.bound_imports.roots.len() <= 257);
        assert_eq!(arena.len(), 2);
        assert_eq!(
            intern_expression(&live, &[], &mut bindings, &mut columns, &mut arena).unwrap(),
            id
        );
        assert_eq!(arena.bound_import_counts().0, 1);
    }
}
