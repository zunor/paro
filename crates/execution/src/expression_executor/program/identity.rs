// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Collision-safe identity for compiled scalar expressions.

use std::collections::HashMap;
use std::sync::Arc;

use paro_function::scalar::cast::{BoundCastInfo, CastDispatch};
use paro_function::scalar::{
    function_data_equals, BoundScalarFunction, InitLocalStateFn, ScalarDispatch,
};
use paro_planner::expression::Expression;

use super::expression_fingerprint;

/// Fingerprints accelerate bucket lookup; full execution identity decides
/// equality. A hash collision can therefore only cost a comparison and can
/// never merge two expressions.
#[derive(Debug, Clone)]
pub(super) struct ExpressionIdentity {
    pub(super) fingerprint: u64,
    expression: Expression,
}

impl ExpressionIdentity {
    pub(super) fn new(expression: &Expression) -> Self {
        Self {
            fingerprint: expression_fingerprint(expression),
            expression: expression.clone(),
        }
    }
}

impl PartialEq for ExpressionIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.fingerprint == other.fingerprint
            && expression_execution_identity_equals(&self.expression, &other.expression)
    }
}

impl Eq for ExpressionIdentity {}

/// Collision-safe map whose hash table only owns immutable fingerprints.
///
/// Expression snapshots live in bucket values rather than hash keys. This is
/// important because rejected planner variants may contain catalog objects
/// with interior mutability; only the scalar identity comparator is allowed to
/// inspect snapshots.
#[derive(Debug)]
pub(super) struct ExpressionIdentityMap<V> {
    buckets: HashMap<u64, Vec<(ExpressionIdentity, V)>>,
    len: usize,
}

impl<V> Default for ExpressionIdentityMap<V> {
    fn default() -> Self {
        Self {
            buckets: HashMap::new(),
            len: 0,
        }
    }
}

impl<V> ExpressionIdentityMap<V> {
    pub(super) fn get(&self, identity: &ExpressionIdentity) -> Option<&V> {
        self.buckets
            .get(&identity.fingerprint)?
            .iter()
            .find(|(candidate, _)| candidate == identity)
            .map(|(_, value)| value)
    }

    pub(super) fn contains(&self, identity: &ExpressionIdentity) -> bool {
        self.get(identity).is_some()
    }

    pub(super) fn get_or_insert_with(
        &mut self,
        identity: ExpressionIdentity,
        create: impl FnOnce() -> V,
    ) -> &mut V {
        let bucket = self.buckets.entry(identity.fingerprint).or_default();
        if let Some(index) = bucket
            .iter()
            .position(|(candidate, _)| candidate == &identity)
        {
            return &mut bucket[index].1;
        }
        bucket.push((identity, create()));
        self.len += 1;
        &mut bucket.last_mut().expect("identity bucket was appended").1
    }

    pub(super) fn insert(&mut self, identity: ExpressionIdentity, value: V) -> bool {
        let bucket = self.buckets.entry(identity.fingerprint).or_default();
        if bucket.iter().any(|(candidate, _)| candidate == &identity) {
            return false;
        }
        bucket.push((identity, value));
        self.len += 1;
        true
    }

    pub(super) fn into_entries(self) -> impl Iterator<Item = (ExpressionIdentity, V)> {
        self.buckets.into_values().flatten()
    }
}

#[derive(Debug, Default)]
pub(super) struct ExpressionIdentitySet(ExpressionIdentityMap<()>);

impl ExpressionIdentitySet {
    pub(super) fn contains(&self, identity: &ExpressionIdentity) -> bool {
        self.0.contains(identity)
    }

    pub(super) fn insert(&mut self, identity: ExpressionIdentity) -> bool {
        self.0.insert(identity, ())
    }
}

fn expression_execution_identity_equals(left: &Expression, right: &Expression) -> bool {
    match (left, right) {
        (Expression::Constant(left), Expression::Constant(right)) => {
            left.return_type == right.return_type && left.value == right.value
        }
        (Expression::ColumnRef(left), Expression::ColumnRef(right)) => {
            left.binding == right.binding
                && left.return_type == right.return_type
                && left.depth == right.depth
        }
        (Expression::Function(left), Expression::Function(right)) => {
            bound_function_identity_equals(&left.function, &right.function)
                && left.return_type == right.return_type
                && expression_slices_identity_equal(&left.children, &right.children)
        }
        (Expression::Cast(left), Expression::Cast(right)) => {
            left.target_type == right.target_type
                && left.try_cast == right.try_cast
                && cast_identity_equals(&left.cast_info, &right.cast_info)
                && expression_execution_identity_equals(&left.child, &right.child)
        }
        (Expression::Conjunction(left), Expression::Conjunction(right)) => {
            left.conjunction_type == right.conjunction_type
                && expression_slices_identity_equal(&left.children, &right.children)
        }
        (Expression::Case(left), Expression::Case(right)) => {
            left.return_type == right.return_type
                && expression_execution_identity_equals(&left.check, &right.check)
                && expression_execution_identity_equals(&left.result_if_true, &right.result_if_true)
                && expression_execution_identity_equals(
                    &left.result_if_false,
                    &right.result_if_false,
                )
        }
        (Expression::Comparison(left), Expression::Comparison(right)) => {
            left.comparison_type == right.comparison_type
                && expression_execution_identity_equals(&left.left, &right.left)
                && expression_execution_identity_equals(&left.right, &right.right)
        }
        (Expression::Operator(left), Expression::Operator(right)) => {
            left.operator_type == right.operator_type
                && left.return_type == right.return_type
                && expression_slices_identity_equal(&left.children, &right.children)
        }
        (Expression::Parameter(left), Expression::Parameter(right)) => left.slot == right.slot,
        (Expression::Reference(left), Expression::Reference(right)) => {
            left.index == right.index && left.return_type == right.return_type
        }
        // Scalar program compilation rejects these variants. Keep cache
        // identity reflexive and conservative if an invariant violation reaches
        // this layer: planner equality may miss reuse, never invent it.
        (Expression::Aggregate(_), Expression::Aggregate(_))
        | (Expression::Subquery(_), Expression::Subquery(_))
        | (Expression::Window(_), Expression::Window(_)) => {
            left.return_type() == right.return_type() && left.equals(right)
        }
        _ => false,
    }
}

fn expression_slices_identity_equal(left: &[Expression], right: &[Expression]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| expression_execution_identity_equals(left, right))
}

fn bound_function_identity_equals(left: &BoundScalarFunction, right: &BoundScalarFunction) -> bool {
    left.name == right.name
        && left.arguments == right.arguments
        && left.return_type == right.return_type
        && scalar_dispatch_identity_equals(left.dispatch, right.dispatch)
        && option_function_pointer_equals(left.init_local_state, right.init_local_state)
        && left.stability == right.stability
        && left.null_handling == right.null_handling
        && left.side_effects == right.side_effects
        && left.varargs == right.varargs
        && left.error_mode == right.error_mode
        && left.dictionary_strategy == right.dictionary_strategy
        && function_data_equals(left.bind_data.as_ref(), right.bind_data.as_ref())
}

fn scalar_dispatch_identity_equals(left: ScalarDispatch, right: ScalarDispatch) -> bool {
    match (left, right) {
        (ScalarDispatch::Direct(left), ScalarDispatch::Direct(right))
        | (ScalarDispatch::Variadic(left), ScalarDispatch::Variadic(right)) => {
            std::ptr::fn_addr_eq(left, right)
        }
        _ => false,
    }
}

fn option_function_pointer_equals(
    left: Option<InitLocalStateFn>,
    right: Option<InitLocalStateFn>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => std::ptr::fn_addr_eq(left, right),
        _ => false,
    }
}

fn cast_identity_equals(left: &BoundCastInfo, right: &BoundCastInfo) -> bool {
    cast_dispatch_identity_equals(left.dispatch, right.dispatch)
        && left.context_dependency() == right.context_dependency()
        && match (&left.cast_data, &right.cast_data) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
}

fn cast_dispatch_identity_equals(left: CastDispatch, right: CastDispatch) -> bool {
    match (left, right) {
        (CastDispatch::Fixed(left), CastDispatch::Fixed(right)) => {
            std::ptr::fn_addr_eq(left, right)
        }
        (CastDispatch::Varlen(left), CastDispatch::Varlen(right)) => {
            std::ptr::fn_addr_eq(left, right)
        }
        (CastDispatch::Array(left), CastDispatch::Array(right)) => {
            std::ptr::fn_addr_eq(left, right)
        }
        (CastDispatch::Struct(left), CastDispatch::Struct(right)) => {
            std::ptr::fn_addr_eq(left, right)
        }
        _ => false,
    }
}
