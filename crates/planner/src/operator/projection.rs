// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Projection Operator
//!
//!

use crate::expression::Expression;
use crate::plan::LogicalPlan;
use paro_common::types::LogicalType;

/// Projection represents a projection operation (SELECT list).
#[derive(Debug)]
pub struct Projection {
    pub table_index: usize,
    pub expressions: Vec<Expression>,
    pub output_names: Vec<String>,
    pub child: Box<LogicalPlan>,
    pub returned_types: Vec<LogicalType>, // Cached types of expressions
}

impl Projection {
    pub fn new(table_index: usize, child: LogicalPlan, expressions: Vec<Expression>) -> Self {
        let returned_types = expressions.iter().map(|e| e.return_type()).collect();
        let output_names = (0..expressions.len())
            .map(|idx| format!("expr_{}", idx + 1))
            .collect();
        Self {
            table_index,
            expressions,
            output_names,
            child: Box::new(child),
            returned_types,
        }
    }

    pub fn with_output_names(mut self, output_names: Vec<String>) -> Self {
        self.output_names = output_names;
        self
    }
}
