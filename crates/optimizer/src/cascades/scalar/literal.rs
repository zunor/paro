// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable, inspectable literal operands for native scalar rules.
//!
//! A digest selects an interning bucket; it is not the value of a SQL literal
//! and cannot prove equality. Keep the leaf allocation (never an expression
//! subtree) so rules can consume typed values without rebuilding an owned IR.

use std::hash::{Hash, Hasher};

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::{ConstantExpression, Expression, SharedExpressionPayload};

use super::super::ids::{Fingerprint, StableFingerprintBuilder};
use super::super::scalar_lowering::{encode_value, value_fingerprint};

#[derive(Debug, Clone)]
pub struct ScalarLiteral {
    leaf: SharedExpressionPayload<ConstantExpression>,
    fingerprint: Fingerprint,
}

impl ScalarLiteral {
    pub fn new(value: Value, logical_type: LogicalType) -> Self {
        Self::from_bound(&ConstantExpression::new(value, logical_type).into())
    }

    pub(crate) fn from_bound(leaf: &SharedExpressionPayload<ConstantExpression>) -> Self {
        Self {
            leaf: leaf.clone(),
            fingerprint: value_fingerprint(&leaf.value),
        }
    }

    pub fn value(&self) -> &Value {
        &self.leaf.value
    }

    pub fn logical_type(&self) -> &LogicalType {
        &self.leaf.return_type
    }

    pub(super) fn expression(&self) -> Expression {
        Expression::Constant(self.leaf.clone())
    }

    pub(super) fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }
}

impl PartialEq for ScalarLiteral {
    fn eq(&self, other: &Self) -> bool {
        if self.leaf.ptr_eq(&other.leaf) {
            return true;
        }
        if self.fingerprint != other.fingerprint {
            return false;
        }
        if self.logical_type() != other.logical_type() {
            return false;
        }
        // Nested runtime values have a recursive PartialEq. Their exact,
        // iterative encoding keeps interning stack-safe, including typed NULL
        // and nested type metadata. The common atomic path needs no allocation.
        if matches!(
            self.value(),
            Value::Null(_) | Value::List(..) | Value::Array(..) | Value::Struct(..)
        ) || matches!(
            other.value(),
            Value::Null(_) | Value::List(..) | Value::Array(..) | Value::Struct(..)
        ) {
            let encoding = |value: &Value| {
                let mut builder = StableFingerprintBuilder::recording();
                encode_value(&mut builder, value);
                builder.finish_recording().1
            };
            encoding(self.value()) == encoding(other.value())
        } else {
            self.value() == other.value()
        }
    }
}

impl Eq for ScalarLiteral {}

impl Hash for ScalarLiteral {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fingerprint.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cascades::scalar::{ScalarArena, ScalarKind, ScalarLocalProperties, ScalarSpec};

    #[test]
    fn native_literal_owns_a_stable_value_after_the_bound_occurrence_changes() {
        let mut bound: SharedExpressionPayload<ConstantExpression> =
            ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into();
        let literal = ScalarLiteral::from_bound(&bound);
        bound.value = Value::Integer(7);
        drop(bound);
        assert_eq!(literal.value(), &Value::Integer(1));
        assert_eq!(literal.logical_type(), &LogicalType::Integer);
        assert!(literal.leaf.ptr_eq(&literal.clone().leaf));
    }

    #[test]
    fn literal_digest_collision_cannot_merge_different_scalar_nodes() {
        let left = ScalarLiteral::new(Value::Integer(1), LogicalType::Integer);
        let mut right = ScalarLiteral::new(Value::Integer(2), LogicalType::Integer);
        right.fingerprint = left.fingerprint;
        let mut arena = ScalarArena::default();
        let mut intern = |value| {
            arena
                .intern(ScalarSpec {
                    kind: ScalarKind::Constant { value },
                    logical_type: LogicalType::Integer,
                    children: Box::new([]),
                    local_properties: ScalarLocalProperties::default(),
                })
                .unwrap()
        };
        let first = intern(left.clone());
        let second = intern(right);
        assert_ne!(first, second);
        assert_eq!(first, intern(left));
        assert_eq!(
            arena.get(first).unwrap().fingerprint,
            arena.get(second).unwrap().fingerprint
        );
    }

    #[test]
    fn literal_node_cannot_override_the_operand_type() {
        let mut arena = ScalarArena::default();
        let result = arena.intern(ScalarSpec {
            kind: ScalarKind::Constant {
                value: ScalarLiteral::new(Value::Integer(1), LogicalType::Integer),
            },
            logical_type: LogicalType::BigInt,
            children: Box::new([]),
            local_properties: ScalarLocalProperties::default(),
        });
        assert!(result.unwrap_err().to_string().contains("literal type"));
        assert!(arena.is_empty());
    }

    #[test]
    fn exact_value_identity_preserves_float_bits_and_nested_values() {
        let literal = |value| ScalarLiteral::new(value, LogicalType::Double);
        assert_ne!(literal(Value::Double(0.0)), literal(Value::Double(-0.0)));
        let nan = f64::from_bits(0x7ff8_0000_0000_0001);
        assert_eq!(literal(Value::Double(nan)), literal(Value::Double(nan)));
        assert_ne!(
            literal(Value::Double(nan)),
            literal(Value::Double(f64::NAN))
        );
        let list = || {
            Value::List(
                vec![Value::Integer(1), Value::Null(LogicalType::Integer)],
                LogicalType::Integer,
            )
        };
        assert_eq!(
            ScalarLiteral::new(list(), LogicalType::List(Box::new(LogicalType::Integer))),
            ScalarLiteral::new(list(), LogicalType::List(Box::new(LogicalType::Integer))),
        );
        assert_ne!(
            ScalarLiteral::new(Value::Integer(1), LogicalType::Integer),
            ScalarLiteral::new(Value::Integer(1), LogicalType::BigInt),
        );
    }
}
