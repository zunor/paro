// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::Result;
use paro_common::logging::targets;
use paro_common::types::LogicalType;
use paro_context::StatementContext;
use paro_execution::query_executor::compiled::{CompiledStatement, ResultColumnDesc};
use paro_parser::ast::Statement;
use paro_planner::planner::Planner;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error};

pub fn compile_statement(ctx: Arc<StatementContext>, stmt: Statement) -> Result<CompiledStatement> {
    compile_statement_with_parameter_types(ctx, stmt, &[])
}

pub fn compile_statement_with_parameter_types(
    ctx: Arc<StatementContext>,
    stmt: Statement,
    parameter_types: &[LogicalType],
) -> Result<CompiledStatement> {
    let statement_tag = stmt.to_string();
    let started_at = Instant::now();
    let statement_trace = ctx.statement_trace();
    if let Some(trace) = &statement_trace {
        trace.record_event("compile", "compiler_entry");
    }
    debug!(
        target: targets::QUERY,
        statement_tag = %statement_tag,
        "Statement compilation pipeline started"
    );

    let bind_and_plan_started = Instant::now();
    let mut planner = if parameter_types.is_empty() {
        Planner::new(ctx.clone())
    } else {
        Planner::new_with_parameters(ctx.clone(), parameter_types.to_vec())
    };
    if let Err(error) = planner.create_plan(stmt) {
        if let Some(trace) = &statement_trace {
            trace.record_span("compile", "bind_and_plan", bind_and_plan_started);
            trace.record_event("compile", "bind_and_plan_error");
        }
        error!(
            target: targets::PLANNER,
            statement_tag = %statement_tag,
            error = %error,
            stage = "planner",
            "Statement planning failed"
        );
        return Err(error);
    }
    if let Some(trace) = &statement_trace {
        trace.record_span("compile", "bind_and_plan", bind_and_plan_started);
    }
    let result_names = planner.names.clone();
    let result_types = planner.types.clone();

    let logical_plan = planner
        .take_plan()
        .ok_or_else(|| paro_common::error::internal("Planner plan is None".to_string()))?;
    debug!(
        target: targets::PLANNER,
        statement_tag = %statement_tag,
        result_columns = result_names.len(),
        result_types = result_types.len(),
        "Logical plan created"
    );

    let optimizer_started = Instant::now();
    let mut optimizer = paro_optimizer::Optimizer::new(planner.binder, ctx.clone());
    let optimized = match optimizer.optimize(logical_plan) {
        Ok(plan) => plan,
        Err(error) => {
            if let Some(trace) = &statement_trace {
                trace.record_span("compile", "optimizer", optimizer_started);
                trace.record_event("compile", "optimizer_error");
            }
            error!(
                target: targets::OPTIMIZER,
                statement_tag = %statement_tag,
                error = %error,
                stage = "optimizer",
                "Statement optimization failed"
            );
            return Err(error);
        }
    };
    let compile_work = paro_context::compile_work_evidence_enabled().then(|| {
        let mut work = optimizer.compile_work();
        work.optimizer_elapsed_us = u64::try_from(optimizer_started.elapsed().as_micros()).unwrap_or(u64::MAX);
        work
    });
    if let Some(trace) = &statement_trace {
        trace.record_span("compile", "optimizer", optimizer_started);
    }
    debug!(
        target: targets::OPTIMIZER,
        statement_tag = %statement_tag,
        "Logical plan optimized"
    );

    let verify_started = Instant::now();
    let verification = match &optimized {
        paro_optimizer::OptimizedStatement::Physical(portfolio) => portfolio
            .verify_result_types(&result_types)
            .and_then(|()| portfolio.verify()),
        paro_optimizer::OptimizedStatement::ExplainAnalyze { target, .. } => target.verify(),
    };
    if let Err(error) = verification {
        if let Some(trace) = &statement_trace {
            trace.record_span("compile", "verify", verify_started);
            trace.record_event("compile", "verification_error");
        }
        return Err(error);
    }
    if let Some(trace) = &statement_trace {
        trace.record_span("compile", "verify", verify_started);
    }

    let runtime_image_started = Instant::now();
    let executable = match optimized {
        paro_optimizer::OptimizedStatement::Physical(plan) => {
            paro_execution::pipeline::StatementProgram::deferred_physical_portfolio(plan)?
        }
        paro_optimizer::OptimizedStatement::ExplainAnalyze { target, spec } => {
            let target =
                paro_execution::pipeline::StatementProgram::deferred_physical_portfolio(target)?;
            paro_execution::pipeline::StatementProgram::ExplainAnalyze {
                target: Box::new(target),
                spec,
            }
        }
    };
    if let Some(trace) = &statement_trace {
        trace.record_span("compile", "runtime_image", runtime_image_started);
        trace.record_event("compile", "executable_image_frozen");
    }
    debug!(
        target: targets::EXECUTOR,
        statement_tag = %statement_tag,
        "Runtime program generated"
    );

    let compiled = CompiledStatement::new(
        executable,
        result_names
            .into_iter()
            .zip(result_types)
            .map(|(name, logical_type)| ResultColumnDesc { name, logical_type })
            .collect(),
        parameter_types.to_vec(),
        ctx.compile_environment_key(),
    );

    // The optimizer and planner state are no longer needed once the deferred
    // executable image has been materialized.  Keep this release boundary in
    // the same trace as compiler return so a cold sample can distinguish
    // image construction from memory retained until admission.
    drop(optimizer);
    if let Some(trace) = &statement_trace {
        trace.record_event("compile", "planning_state_released");
    }

    debug!(
        target: targets::QUERY,
        statement_tag = %statement_tag,
        result_columns = compiled.result_schema().len(),
        elapsed_ms = started_at.elapsed().as_millis(),
        "Statement compilation pipeline completed"
    );

    if let Some(trace) = &statement_trace {
        trace.record_event("compile", "compiler_return");
    }

    Ok(match compile_work {
        Some(mut work) => {
            work.compiler_elapsed_us = u64::try_from(started_at.elapsed().as_micros()).unwrap_or(u64::MAX);
            compiled.with_compile_work(work)
        }
        None => compiled,
    })
}
