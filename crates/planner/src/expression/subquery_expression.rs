// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bound Subquery Expression
//!
//!

use super::ComparisonType;
use super::Expression;
use crate::binder::context::BindSnapshot;
use crate::binder::CorrelatedColumnInfo;
use crate::plan::PlannedStatement;
use paro_common::types::LogicalType;
use std::sync::Arc;

/// The type of subquery expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryType {
    /// Regular scalar subquery: `(SELECT...)`
    Scalar,
    /// EXISTS subquery: `EXISTS (SELECT...)`
    Exists,
    /// NOT EXISTS subquery: `NOT EXISTS (SELECT...)`
    NotExists,
    /// ANY/IN subquery: `x IN (SELECT...)` or `x = ANY(SELECT...)`
    Any,
    /// ALL subquery: `x > ALL(SELECT...)`
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SubqueryPlanningState {
    #[default]
    Unplanned,
    Planning,
    Planned,
}

/// A bound subquery expression.
#[derive(Debug, Clone)]
pub struct SubqueryExpression {
    /// The type of subquery.
    pub subquery_type: SubqueryType,
    /// The bound subquery statement.
    pub subquery: Arc<PlannedStatement>,
    /// The child expressions to compare with (for IN/ANY/ALL).
    pub children: Vec<Expression>,
    /// The original subquery output types for the child comparison slots.
    pub child_types: Vec<LogicalType>,
    /// The coerced comparison target types for the child comparison slots.
    pub child_targets: Vec<LogicalType>,
    /// The comparison operator (for ANY/ALL).
    pub comparison_type: ComparisonType,
    /// The return type of the expression.
    pub return_type: LogicalType,
    /// Correlated columns used by this subquery.
    pub correlated_columns: Vec<CorrelatedColumnInfo>,
    /// Scope snapshot from when the subquery was bound (used for plan copy / binding remap).
    pub bind_snapshot: Arc<BindSnapshot>,
    /// Delayed-planning state for this subquery node in planner-owned trees.
    pub planning_state: SubqueryPlanningState,
}

impl SubqueryExpression {
    pub fn return_type(&self) -> LogicalType {
        self.return_type.clone()
    }
}
