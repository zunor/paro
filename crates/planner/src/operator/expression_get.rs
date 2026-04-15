// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Expression Get Operator (for VALUES clause)

use crate::expression::Expression;
use paro_common::types::LogicalType;

#[derive(Debug, Clone)]
pub struct ExpressionGet {
    /// The table index for the columns produced by this operator
    pub table_index: usize,
    /// The expressions (rows x columns)
    pub expressions: Vec<Vec<Expression>>,
    /// Column names
    pub names: Vec<String>,
    /// Column types
    pub types: Vec<LogicalType>,
}

impl ExpressionGet {
    pub fn new(
        table_index: usize,
        expressions: Vec<Vec<Expression>>,
        names: Vec<String>,
        types: Vec<LogicalType>,
    ) -> Self {
        Self {
            table_index,
            expressions,
            names,
            types,
        }
    }
}
