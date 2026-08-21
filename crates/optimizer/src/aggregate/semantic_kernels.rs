// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact bound-kernel identity proofs used by semantic aggregate rewrites.

use paro_function::scalar::cast::{BoundCastInfo, CastDispatch};
use paro_function::scalar::{BoundScalarFunction, ScalarDispatch};
use paro_planner::expression::AggregateExpression;

/// Compare the complete bound aggregate execution contract, never its display
/// name. Extensions may reuse a built-in signature with different state
/// transitions or bind payload.
pub(crate) fn aggregate_kernels_equal(
    left: &AggregateExpression,
    right: &AggregateExpression,
) -> bool {
    left.function.execution_semantics_equal(&right.function)
        && match (&left.bind_info, &right.bind_info) {
            (Some(left), Some(right)) => left.equals(&**right),
            (None, None) => true,
            _ => false,
        }
}

pub(crate) fn scalar_kernels_equal(
    left: &BoundScalarFunction,
    right: &BoundScalarFunction,
) -> bool {
    left.name == right.name
        && left.arguments == right.arguments
        && left.return_type == right.return_type
        && scalar_dispatch_equal(left.dispatch, right.dispatch)
        && left.predicate_projection == right.predicate_projection
        && match (left.init_local_state, right.init_local_state) {
            (Some(left), Some(right)) => std::ptr::fn_addr_eq(left, right),
            (None, None) => true,
            _ => false,
        }
        && left.stability == right.stability
        && left.null_handling == right.null_handling
        && left.side_effects == right.side_effects
        && left.varargs == right.varargs
        && left.error_mode == right.error_mode
        && left.dictionary_strategy == right.dictionary_strategy
        && match (&left.bind_data, &right.bind_data) {
            (Some(left), Some(right)) => left.equals(&**right),
            (None, None) => true,
            _ => false,
        }
}

pub(crate) fn scalar_dispatch_equal(left: ScalarDispatch, right: ScalarDispatch) -> bool {
    match (left, right) {
        (ScalarDispatch::Direct(left), ScalarDispatch::Direct(right))
        | (ScalarDispatch::Variadic(left), ScalarDispatch::Variadic(right)) => {
            std::ptr::fn_addr_eq(left, right)
        }
        _ => false,
    }
}

pub(crate) fn cast_kernels_equal(left: &BoundCastInfo, right: &BoundCastInfo) -> bool {
    left.type_contract() == right.type_contract()
        && left.context_dependency() == right.context_dependency()
        && cast_dispatch_equal(left.dispatch, right.dispatch)
        && match (&left.cast_data, &right.cast_data) {
            // `BoundCastData` deliberately has no semantic-equality method.
            // Shared identity is the only proof available without guessing
            // from a concrete payload type or its Debug representation.
            (Some(left), Some(right)) => std::sync::Arc::ptr_eq(left, right),
            (None, None) => true,
            _ => false,
        }
}

pub(crate) fn cast_dispatch_equal(left: CastDispatch, right: CastDispatch) -> bool {
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
