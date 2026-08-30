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
    /// User-visible names form a prefix of `expressions`. Query binding may
    /// append unnamed execution columns for ORDER BY/window evaluation; those
    /// columns are pruned before the client result boundary.
    pub visible_names: Vec<String>,
    /// Number of leading expressions that own a SQL-visible identity. The
    /// remaining names describe optimizer carrier slots only.
    pub visible_count: usize,
    /// Optional relation namespace owned by the visible output prefix. This
    /// survives CTE inlining so physical plans can qualify self-join inputs.
    pub visible_qualifier: Option<String>,
    pub child: Box<LogicalPlan>,
    pub returned_types: Vec<LogicalType>, // Cached types of expressions
}

impl Projection {
    pub fn new(table_index: usize, child: LogicalPlan, expressions: Vec<Expression>) -> Self {
        let visible_count = expressions.len();
        let returned_types = expressions.iter().map(|e| e.return_type()).collect();
        let visible_names = (0..expressions.len())
            .map(|idx| format!("expr_{}", idx + 1))
            .collect();
        Self {
            table_index,
            expressions,
            visible_names,
            visible_count,
            visible_qualifier: None,
            child: Box::new(child),
            returned_types,
        }
    }

    /// Replace the visible-name prefix without pretending hidden execution
    /// columns have user-facing names.
    pub fn with_visible_names(mut self, visible_names: Vec<String>) -> Self {
        self.visible_count = visible_names.len();
        self.visible_names = visible_names;
        self
    }

    pub fn with_internal_outputs(mut self) -> Self {
        self.visible_count = 0;
        self
    }

    pub fn with_visible_qualifier(mut self, qualifier: impl Into<String>) -> Self {
        self.visible_qualifier = Some(qualifier.into());
        self
    }

    /// Return a stable name for any output ordinal. Hidden columns receive an
    /// internal name instead of indexing past the visible prefix.
    pub fn name_at(&self, output_index: usize) -> Option<String> {
        (output_index < self.expressions.len()).then(|| {
            self.visible_names
                .get(output_index)
                .cloned()
                .unwrap_or_else(|| format!("__paro_hidden_{output_index}"))
        })
    }
}
