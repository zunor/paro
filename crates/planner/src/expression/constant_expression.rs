// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bound Constant Expression
//!
//!

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

/// A bound constant value.
#[derive(Debug, Clone)]
pub struct ConstantExpression {
    pub value: Value,
    pub return_type: LogicalType,
}

impl ConstantExpression {
    pub fn new(value: Value, return_type: LogicalType) -> Self {
        Self { value, return_type }
    }
}
