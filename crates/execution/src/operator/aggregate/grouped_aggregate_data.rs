// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::expression::{AggregateExpression, Expression};

/// Planner/execution bridge for aggregate extraction results.
///
/// After expression extraction, both grouped and ungrouped aggregates consume
/// pre-evaluated payload columns through `ReferenceExpression`s.
#[derive(Debug, Clone, Default)]
pub struct GroupedAggregateData {
    /// Projection expressions materialized above the aggregate child.
    pub projection_exprs: Vec<Expression>,
    /// Output types of the projection payload.
    pub payload_types: Vec<LogicalType>,
    /// Extracted group expressions (references into the payload chunk).
    pub groups: Vec<Expression>,
    /// GROUPING SET definitions; each entry lists indices into `groups`.
    pub grouping_sets: Vec<Vec<usize>>,
    /// Extracted aggregate expressions.
    pub aggregates: Vec<Expression>,
    /// GROUPING() metadata.
    pub grouping_functions: Vec<Vec<usize>>,
    /// Payload input columns per aggregate.
    pub aggregate_inputs: Vec<Vec<usize>>,
    /// Optional FILTER payload column per aggregate.
    pub aggregate_filters: Vec<Option<usize>>,
    /// ORDER BY payload columns per aggregate.
    pub aggregate_orders: Vec<Vec<usize>>,
}

/// Compatibility alias for the design doc terminology.
pub type AggregatePlanData = GroupedAggregateData;

impl GroupedAggregateData {
    pub fn has_projection(&self) -> bool {
        !self.projection_exprs.is_empty()
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    pub fn aggregate_count(&self) -> usize {
        self.aggregates.len()
    }

    pub fn aggregate_expr(&self, idx: usize) -> Result<&AggregateExpression> {
        match self.aggregates.get(idx) {
            Some(Expression::Aggregate(agg)) => Ok(agg),
            Some(_) => Err(paro_error::internal(format!(
                "Expected AggregateExpression at aggregate index {idx}"
            ))),
            None => Err(paro_error::internal(format!(
                "Aggregate index out of bounds: {idx}"
            ))),
        }
    }

    pub fn aggregate_return_types(&self) -> Vec<LogicalType> {
        self.aggregates
            .iter()
            .map(|expr| expr.return_type())
            .collect()
    }
}

pub fn reference_index(expr: &Expression) -> Result<usize> {
    match expr {
        Expression::Reference(reference) => Ok(reference.index),
        _ => Err(paro_error::internal(format!(
            "Expected ReferenceExpression, found {:?}",
            expr
        ))),
    }
}
