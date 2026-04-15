// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! `DISTINCT` and `DISTINCT ON` (ordering picks the surviving row per group).

use paro_common::types::LogicalType;

use crate::binder::ir::OrderByNode;
use crate::expression::Expression;
use crate::plan::LogicalPlan;

/// The type of DISTINCT operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistinctType {
    /// Regular DISTINCT - removes all duplicate rows.
    #[default]
    Distinct,
    DistinctOn,
}

/// Distinct represents a DISTINCT operation.
///
/// groups by all columns and returns one row per group.
///
/// For DISTINCT ON, the `order_by` field determines which row to keep
/// for each distinct group (the first row according to the ORDER BY).
#[derive(Debug)]
pub struct Distinct {
    /// The type of distinct operation.
    pub distinct_type: DistinctType,
    /// The expressions to compute distinctness on.
    /// For regular DISTINCT, this is empty (all columns).
    /// For DISTINCT ON, this contains the ON expressions.
    pub distinct_targets: Vec<Expression>,
    /// The ORDER BY modifier (optional, only for DISTINCT ON).
    /// Used to determine which row to keep for each distinct group.
    pub order_by: Option<Vec<OrderByNode>>,
    /// The child operator.
    pub child: Box<LogicalPlan>,
}

impl Distinct {
    /// Create a new Distinct for regular DISTINCT.
    pub fn new(child: LogicalPlan) -> Self {
        Self {
            distinct_type: DistinctType::Distinct,
            distinct_targets: Vec::new(),
            order_by: None,
            child: Box::new(child),
        }
    }

    /// Create a new Distinct for DISTINCT ON.
    pub fn distinct_on(targets: Vec<Expression>, child: LogicalPlan) -> Self {
        Self {
            distinct_type: DistinctType::DistinctOn,
            distinct_targets: targets,
            order_by: None,
            child: Box::new(child),
        }
    }

    /// Create a new Distinct for DISTINCT ON with ORDER BY.
    pub fn distinct_on_with_order(
        targets: Vec<Expression>,
        order_by: Vec<OrderByNode>,
        child: LogicalPlan,
    ) -> Self {
        Self {
            distinct_type: DistinctType::DistinctOn,
            distinct_targets: targets,
            order_by: Some(order_by),
            child: Box::new(child),
        }
    }

    /// Get the output types (same as child).
    pub fn get_types(&self) -> Vec<LogicalType> {
        self.child.types()
    }

    /// Get the operator name.
    pub fn name(&self) -> &'static str {
        match self.distinct_type {
            DistinctType::Distinct => "DISTINCT",
            DistinctType::DistinctOn => "DISTINCT_ON",
        }
    }

    /// Check if this is a DISTINCT ON operation.
    pub fn is_distinct_on(&self) -> bool {
        self.distinct_type == DistinctType::DistinctOn
    }
}
