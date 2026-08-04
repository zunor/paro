// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Planner Implementation
//!
//!
//!
//!
//! The Planner creates a logical query plan from the parsed SQL statements.
//! It orchestrates the binding (semantic analysis) and logical plan generation.
//!

use crate::binder::ir::statement::BoundStatementKind;
use crate::binder::Binder;
use crate::plan::LogicalPlan;
use crate::stack::maybe_grow_planner_stack;
use crate::verify::verify_physical_planner_invariants;
use paro_common::error::Result;
use paro_common::logging::targets;
use paro_common::types::LogicalType;
use paro_context::StatementContext;
use paro_parser::ast::Statement;
use paro_parser::Range;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

/// Statement properties extracted during planning.
///
#[derive(Debug, Clone, Default)]
pub struct StatementProperties {
    /// Whether the statement is read-only (SELECT, SHOW, etc.)
    pub read_only: bool,
    /// Whether the statement requires a valid transaction
    pub requires_valid_transaction: bool,
    /// Whether the statement might modify metadata
    pub allows_stream_result: bool,
    /// Whether the statement is an explain statement
    pub is_explain: bool,
    /// Whether the statement returns results
    pub returns_result_set: bool,
}

/// The planner creates a logical query plan from the parsed SQL statements
/// using the Binder and LogicalPlanGenerator.
///
///
///
/// # Example
///
/// ```ignore
/// let mut planner = Planner::new(session);
/// planner.create_plan(stmt)?;
/// let plan = planner.plan.take().unwrap();
/// let names = planner.names;
/// let types = planner.types;
/// ```
pub struct Planner {
    /// The binder used for semantic analysis.
    pub binder: Binder,

    /// The session context for accessing catalog, transaction, and settings.
    context: Arc<StatementContext>,

    /// The resulting logical plan.
    pub plan: Option<LogicalPlan>,

    /// Result column names.
    pub names: Vec<String>,

    /// Result column types.
    pub types: Vec<LogicalType>,

    /// Statement properties extracted during planning.
    pub properties: StatementProperties,
}

impl Planner {
    /// Create a new Planner with the given session context.
    ///
    ///
    /// # Arguments
    ///
    /// * `context` - The session context providing access to catalog and transaction
    pub fn new(context: Arc<StatementContext>) -> Self {
        Self {
            binder: Binder::new(context.clone()),
            context,
            plan: None,
            names: Vec::new(),
            types: Vec::new(),
            properties: StatementProperties::default(),
        }
    }

    pub fn new_with_parameters(
        context: Arc<StatementContext>,
        parameter_types: Vec<LogicalType>,
        placeholder_indexes: BTreeMap<Range, usize>,
    ) -> Self {
        Self {
            binder: Binder::with_parameters(context.clone(), parameter_types, placeholder_indexes),
            context,
            plan: None,
            names: Vec::new(),
            types: Vec::new(),
            properties: StatementProperties::default(),
        }
    }

    /// Create a logical plan from a SQL statement.
    ///
    /// This method coordinates the full planning pipeline:
    /// 1. Bind the statement (semantic analysis)
    /// 2. Extract result names and types
    /// 3. Create the logical plan
    /// 4. Flatten dependent joins (decorrelate subqueries)
    /// 5. Set statement properties
    ///
    ///
    /// # Arguments
    ///
    /// * `stmt` - The parsed SQL statement
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, with the plan stored in `self.plan`.
    ///
    /// # Errors
    ///
    /// Returns an error if binding or planning fails.
    pub fn create_plan(&mut self, stmt: Statement) -> Result<()> {
        maybe_grow_planner_stack(|| self.create_plan_inner(stmt))
    }

    fn create_plan_inner(&mut self, stmt: Statement) -> Result<()> {
        let started_at = Instant::now();
        debug!(target: targets::PLANNER, "Planning started");

        // 1. Bind the statement (semantic analysis)
        let bound_kind = self.binder.bind_statement_kind(stmt)?;

        // 2. Extract result names and types
        self.names = bound_kind.names();
        self.types = bound_kind.types();

        // 3. Set statement properties
        self.properties = Self::extract_properties(&bound_kind);

        // 4. Create the logical plan
        let mut logical_plan = self.binder.create_plan(bound_kind)?;

        // 5. Flatten dependent joins (decorrelate subqueries)
        logical_plan = self.binder.flatten_dependent_joins(logical_plan)?;
        verify_physical_planner_invariants(&logical_plan.operator)?;

        self.plan = Some(logical_plan);
        debug!(
            target: targets::PLANNER,
            read_only = self.properties.read_only,
            requires_valid_transaction = self.properties.requires_valid_transaction,
            allows_stream_result = self.properties.allows_stream_result,
            is_explain = self.properties.is_explain,
            returns_result_set = self.properties.returns_result_set,
            result_columns = self.names.len(),
            result_types = self.types.len(),
            elapsed_ms = started_at.elapsed().as_millis(),
            "Planning completed"
        );
        Ok(())
    }

    /// Get a reference to the session context.
    #[inline]
    pub fn context(&self) -> &StatementContext {
        self.context.as_ref()
    }

    /// Get a reference to the binder.
    #[inline]
    pub fn binder(&self) -> &Binder {
        &self.binder
    }

    /// Get a mutable reference to the binder.
    #[inline]
    pub fn binder_mut(&mut self) -> &mut Binder {
        &mut self.binder
    }

    /// Take the logical plan, consuming it from the planner.
    #[inline]
    pub fn take_plan(&mut self) -> Option<LogicalPlan> {
        self.plan.take()
    }

    /// Extract statement properties from a bound statement.
    fn extract_properties(bound_kind: &BoundStatementKind) -> StatementProperties {
        match bound_kind {
            BoundStatementKind::Query(_) => StatementProperties {
                read_only: true,
                requires_valid_transaction: true,
                allows_stream_result: true,
                is_explain: false,
                returns_result_set: true,
            },
            BoundStatementKind::Insert(_)
            | BoundStatementKind::Delete(_)
            | BoundStatementKind::Update(_)
            | BoundStatementKind::Copy(_) => StatementProperties {
                read_only: false,
                requires_valid_transaction: true,
                allows_stream_result: false,
                is_explain: false,
                returns_result_set: false,
            },
            BoundStatementKind::CreateTable(_)
            | BoundStatementKind::CreateRoutine(_)
            | BoundStatementKind::CreateSequence(_)
            | BoundStatementKind::CreateSchema(_)
            | BoundStatementKind::CreateIndex(_)
            | BoundStatementKind::CreatePropertyGraph(_)
            | BoundStatementKind::CreateView(_)
            | BoundStatementKind::AlterEntry(_)
            | BoundStatementKind::Drop(_)
            | BoundStatementKind::DropPropertyGraph(_)
            | BoundStatementKind::RefreshPropertyGraph(_) => StatementProperties {
                read_only: false,
                requires_valid_transaction: true,
                allows_stream_result: false,
                is_explain: false,
                returns_result_set: false,
            },
            BoundStatementKind::Explain(_) => StatementProperties {
                read_only: true,
                requires_valid_transaction: true,
                allows_stream_result: true,
                is_explain: true,
                returns_result_set: true,
            },
            BoundStatementKind::Dummy => StatementProperties::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Basic tests to verify compilation
    #[test]
    fn test_statement_properties_default() {
        let props = StatementProperties::default();
        assert!(!props.read_only);
        assert!(!props.requires_valid_transaction);
        assert!(!props.allows_stream_result);
        assert!(!props.is_explain);
        assert!(!props.returns_result_set);
    }
}
