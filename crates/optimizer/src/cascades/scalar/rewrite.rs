// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Persistent, typed substitution over native scalar operands.

use std::collections::HashMap;

use paro_common::error::{self as paro_error, Result};

use super::super::ids::{ColumnId, ScalarExprId};
use super::{ScalarArena, ScalarKind, ScalarSpec};

impl ScalarArena {
    /// Substitute local input columns, retaining every untouched scalar id.
    /// Correlated columns and parameters belong to another binding context.
    ///
    /// This constructs an expression, not a proof that moving/duplicating its
    /// evaluation is legal. The relational rule must establish that contract.
    /// Intrinsic call properties are replayed with the new child's properties.
    /// Failure/cancellation rolls back only this operation's appended nodes.
    pub fn substitute_columns(
        &mut self,
        root: ScalarExprId,
        mut replacement: impl FnMut(ColumnId) -> Option<ScalarExprId>,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<ScalarExprId> {
        let generation = self.len();
        let result = (|| {
            let mut pending = vec![(root, 0)];
            let mut completed = HashMap::new();
            while let Some((id, cursor)) = pending.pop() {
                if completed.contains_key(&id) {
                    continue;
                }
                let node = self.get(id).ok_or_else(|| {
                    paro_error::internal("scalar substitution references an unknown node")
                })?;
                if cursor == 0 {
                    checkpoint()?;
                    if let ScalarKind::Column(column) = node.kind {
                        if let Some(other) = replacement(column) {
                            if self
                                .get(other)
                                .is_none_or(|other| other.logical_type != node.logical_type)
                            {
                                return Err(paro_error::internal(
                                    "scalar substitution changed its column type",
                                ));
                            }
                            completed.insert(id, other);
                            continue;
                        }
                    }
                }
                if let Some(child) = node.children.get(cursor) {
                    pending.push((id, cursor + 1));
                    pending.push((*child, 0));
                    continue;
                }
                let children =
                    node.children
                        .iter()
                        .map(|id| {
                            completed.get(id).copied().ok_or_else(|| {
                                paro_error::internal("native substitution lost a child")
                            })
                        })
                        .collect::<Result<Box<[_]>>>()?;
                let rewritten = if children == node.children {
                    id
                } else {
                    self.intern(ScalarSpec {
                        kind: node.kind.clone(),
                        logical_type: node.logical_type.clone(),
                        children,
                        local_properties: node.local_properties,
                    })?
                };
                completed.insert(id, rewritten);
            }
            completed
                .remove(&root)
                .ok_or_else(|| paro_error::internal("native substitution lost its root"))
        })();
        if result.is_err() {
            self.truncate(generation)?;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ScalarLiteral, ScalarLocalProperties, Volatility};
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    fn node(
        arena: &mut ScalarArena,
        kind: ScalarKind,
        children: &[ScalarExprId],
        props: ScalarLocalProperties,
    ) -> ScalarExprId {
        arena
            .intern(ScalarSpec {
                kind,
                logical_type: LogicalType::Boolean,
                children: children.into(),
                local_properties: props,
            })
            .unwrap()
    }

    #[test]
    fn substitution_reuses_ids_respects_lexical_scope_and_rederives_effects() {
        let mut arena = ScalarArena::default();
        let old = node(
            &mut arena,
            ScalarKind::Column(ColumnId(0)),
            &[],
            Default::default(),
        );
        let outer = node(
            &mut arena,
            ScalarKind::CorrelatedColumn {
                column: ColumnId(0),
                depth: 2,
            },
            &[],
            Default::default(),
        );
        let parameter = node(
            &mut arena,
            ScalarKind::Parameter(0),
            &[],
            Default::default(),
        );
        let parent = node(
            &mut arena,
            ScalarKind::Case,
            &[old, outer, parameter],
            ScalarLocalProperties {
                may_error: true,
                ..Default::default()
            },
        );
        let volatile = node(
            &mut arena,
            ScalarKind::Case,
            &[old, old, old],
            ScalarLocalProperties {
                volatility: Volatility::Volatile,
                ..Default::default()
            },
        );
        let length = arena.len();
        assert_eq!(
            arena
                .substitute_columns(parent, |_| None, || Ok(()))
                .unwrap(),
            parent
        );
        assert_eq!(arena.len(), length);
        let changed = arena
            .substitute_columns(parent, |_| Some(volatile), || Ok(()))
            .unwrap();
        let result = arena.get(changed).unwrap();
        assert_eq!(&*result.children, &[volatile, outer, parameter]);
        assert!(result.properties.may_error);
        assert_eq!(result.properties.volatility, Volatility::Volatile);
        assert_eq!(arena.len(), length + 1);
    }

    #[test]
    fn substitution_failure_rolls_back_its_append_delta() {
        let mut arena = ScalarArena::default();
        let column = node(
            &mut arena,
            ScalarKind::Column(ColumnId(0)),
            &[],
            Default::default(),
        );
        let constant = node(
            &mut arena,
            ScalarKind::Constant {
                value: ScalarLiteral::new(Value::Boolean(true), LogicalType::Boolean),
            },
            &[],
            Default::default(),
        );
        let inner = node(&mut arena, ScalarKind::And, &[column], Default::default());
        let sibling = node(
            &mut arena,
            ScalarKind::Parameter(0),
            &[],
            Default::default(),
        );
        let root = node(
            &mut arena,
            ScalarKind::Case,
            &[inner, sibling, column],
            Default::default(),
        );
        let length = arena.len();
        let mut visits = 0;
        let error = arena
            .substitute_columns(
                root,
                |_| Some(constant),
                || {
                    visits += 1;
                    if visits == 4 {
                        Err(paro_error::internal("cancel after first child changed"))
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("cancel after"));
        assert_eq!(arena.len(), length);
        let changed = arena
            .substitute_columns(root, |_| Some(constant), || Ok(()))
            .unwrap();
        assert_ne!(root, changed);
        assert_eq!(arena.len(), length + 2);
    }
}
