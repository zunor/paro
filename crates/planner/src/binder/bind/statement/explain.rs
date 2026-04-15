//! Bind EXPLAIN Statement
//!
//!

use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;
use crate::operator::{
    Explain, ExplainDetail, ExplainFormat, ExplainMode, ExplainSpec, LogicalOperator,
};
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::{ExplainKind, ExplainOption, Statement};
use paro_parser::Span;

/// Bound information for EXPLAIN / EXPLAIN ANALYZE statements.
#[derive(Debug)]
pub struct BoundExplainInfo {
    /// Explain operator already wrapping the target child plan.
    pub plan: LogicalOperator,
    /// Structured EXPLAIN spec.
    pub spec: ExplainSpec,
    /// EXPLAIN options from parser (reserved for future use).
    pub options: Vec<ExplainOption>,
}

/// Bind an EXPLAIN statement.
pub fn bind_explain(
    binder: &mut Binder,
    query: Statement,
    kind: ExplainKind,
    options: (Span, Vec<ExplainOption>),
) -> Result<BoundStatementKind> {
    let mode = match kind {
        ExplainKind::Plan => ExplainMode::Plan,
        ExplainKind::AnalyzePlan => ExplainMode::Analyze,
        _ => {
            return Err(paro_error::not_implemented(format!(
                "EXPLAIN kind {:?} is not supported yet",
                kind
            )));
        }
    };

    bind_explain_impl(binder, query, mode, options.1)
}

/// Bind an EXPLAIN ANALYZE statement.
pub fn bind_explain_analyze(binder: &mut Binder, query: Statement) -> Result<BoundStatementKind> {
    bind_explain_impl(binder, query, ExplainMode::Analyze, vec![])
}

fn bind_explain_impl(
    binder: &mut Binder,
    query: Statement,
    mode: ExplainMode,
    options: Vec<ExplainOption>,
) -> Result<BoundStatementKind> {
    let mut detail = ExplainDetail::default();
    for option in &options {
        match option {
            ExplainOption::Verbose => detail.verbose = true,
            ExplainOption::Logical | ExplainOption::Optimized | ExplainOption::Decorrelated => {
                return Err(paro_error::not_implemented(format!(
                    "EXPLAIN option {:?} is not supported yet",
                    option
                )));
            }
        }
    }

    let format = match binder.session_context().statement_format() {
        None => ExplainFormat::Text,
        Some(format) if format.eq_ignore_ascii_case("text") => ExplainFormat::Text,
        Some(format) if format.eq_ignore_ascii_case("json") => ExplainFormat::Json,
        Some(other) => {
            return Err(paro_error::not_implemented(format!(
                "EXPLAIN FORMAT {} is not supported yet",
                other
            )));
        }
    };
    let spec = ExplainSpec {
        mode,
        format,
        detail,
    };

    // 1) Bind the inner statement.
    let bound_inner = binder.bind_statement_kind(query)?;

    // 2) Create the inner logical plan (root [`LogicalPlan`]).
    let child_plan = binder.create_plan(bound_inner)?;

    // 3) Wrap it in Explain and return BoundStatementKind::Explain.
    let explain = LogicalOperator::Explain(Explain::new(child_plan, spec));
    Ok(BoundStatementKind::Explain(BoundExplainInfo {
        plan: explain,
        spec,
        options,
    }))
}
