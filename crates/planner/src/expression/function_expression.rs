// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::Expression;
use paro_common::types::LogicalType;
use paro_function::scalar::BoundScalarFunction;

#[derive(Debug, Clone)]
pub struct FunctionExpression {
    pub function: BoundScalarFunction,
    pub children: Vec<Expression>,
    pub return_type: LogicalType,
}

impl FunctionExpression {
    pub fn new<F>(function: F, children: Vec<Expression>, return_type: LogicalType) -> Self
    where
        F: Into<BoundScalarFunction>,
    {
        Self {
            function: function.into(),
            children,
            return_type,
        }
    }
}
