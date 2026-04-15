// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Explain Operator
//!
//!

use crate::plan::LogicalPlan;

/// EXPLAIN operator mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExplainMode {
    /// Plain `EXPLAIN`.
    #[default]
    Plan,
    /// `EXPLAIN ANALYZE`.
    Analyze,
}

/// EXPLAIN output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExplainFormat {
    /// PostgreSQL-style text output.
    #[default]
    Text,
    /// Structured JSON output.
    Json,
}

/// EXPLAIN detail switches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExplainDetail {
    pub verbose: bool,
    pub summary: bool,
    pub timing: bool,
    pub memory: bool,
}

impl Default for ExplainDetail {
    fn default() -> Self {
        Self {
            verbose: false,
            summary: true,
            timing: true,
            memory: true,
        }
    }
}

/// Structured EXPLAIN spec shared across binder/planner/execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExplainSpec {
    pub mode: ExplainMode,
    pub format: ExplainFormat,
    pub detail: ExplainDetail,
}

impl ExplainSpec {
    pub fn text_plan() -> Self {
        Self::default()
    }

    pub fn text_analyze() -> Self {
        Self {
            mode: ExplainMode::Analyze,
            ..Self::default()
        }
    }
}

/// Explain wraps a query plan that needs to be rendered.
#[derive(Debug)]
pub struct Explain {
    /// Child logical plan that EXPLAIN targets.
    pub child: Box<LogicalPlan>,
    /// Structured explain spec.
    pub spec: ExplainSpec,
    /// Optional unoptimized logical plan string.
    pub logical_plan_unopt: Option<String>,
    /// Optional optimized logical plan string.
    pub logical_plan_opt: Option<String>,
}

impl Explain {
    pub fn new(child: LogicalPlan, spec: ExplainSpec) -> Self {
        Self {
            child: Box::new(child),
            spec,
            logical_plan_unopt: None,
            logical_plan_opt: None,
        }
    }
}
