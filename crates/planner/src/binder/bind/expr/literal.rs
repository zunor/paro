// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Constant Expression Binder
//!
//! - String literals are bound to VARCHAR type
//! - No temporary "literal" types exist - all constants have concrete types

use crate::expression::{ConstantExpression, Expression};
use ethnum::i256;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_parser::ast::Literal;

/// Bind a constant literal from the AST.
///
/// - Values fitting in i32 range → INTEGER
/// - Values fitting in i64 range → BIGINT
/// - Larger values → HUGEINT
///
pub fn bind_literal(value: Literal) -> Result<Expression> {
    let (value, return_type) = match value {
        Literal::UInt64(n) => {
            if n <= i32::MAX as u64 {
                // Fits in INTEGER (i32)
                (Value::Integer(n as i32), LogicalType::Integer)
            } else if n <= i64::MAX as u64 {
                // Fits in BIGINT (i64)
                (Value::BigInt(n as i64), LogicalType::BigInt)
            } else {
                // Needs HUGEINT (i128)
                (Value::HugeInt(n as i128), LogicalType::HugeInt)
            }
        }
        Literal::Float64(f) => (Value::Double(f), LogicalType::Double),
        Literal::Decimal256 {
            value,
            precision: _,
            scale,
        } => {
            if scale == 0 {
                let i128_value = value.as_i128();
                if value == i256::from(i128_value) {
                    if i128_value >= i32::MIN as i128 && i128_value <= i32::MAX as i128 {
                        (Value::Integer(i128_value as i32), LogicalType::Integer)
                    } else if i128_value >= i64::MIN as i128 && i128_value <= i64::MAX as i128 {
                        (Value::BigInt(i128_value as i64), LogicalType::BigInt)
                    } else {
                        (Value::HugeInt(i128_value), LogicalType::HugeInt)
                    }
                } else {
                    let u128_value = value.as_u128();
                    if value == i256::from(u128_value) {
                        (Value::UHugeInt(u128_value), LogicalType::UHugeInt)
                    } else {
                        // Fallback for literals beyond 128-bit ranges
                        let f = value.as_f64();
                        (Value::Double(f), LogicalType::Double)
                    }
                }
            } else {
                // For now, convert to double
                let divisor = 10f64.powi(scale as i32);
                let f = (value.as_i128() as f64) / divisor;
                (Value::Double(f), LogicalType::Double)
            }
        }
        Literal::String(s) => (Value::Varchar(s.clone()), LogicalType::Varchar),
        Literal::Boolean(b) => (Value::Boolean(b), LogicalType::Boolean),
        Literal::Null => (Value::Null(LogicalType::Null), LogicalType::Null),
    };

    Ok(Expression::Constant(ConstantExpression {
        value,
        return_type,
    }))
}
