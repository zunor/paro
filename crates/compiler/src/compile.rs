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
    debug!(
        target: targets::QUERY,
        statement_tag = %statement_tag,
        "Statement compilation pipeline started"
    );

    let mut planner = if parameter_types.is_empty() {
        Planner::new(ctx.clone())
    } else {
        Planner::new_with_parameters(ctx.clone(), parameter_types.to_vec())
    };
    if let Err(error) = planner.create_plan(stmt) {
        error!(
            target: targets::PLANNER,
            statement_tag = %statement_tag,
            error = %error,
            stage = "planner",
            "Statement planning failed"
        );
        return Err(error);
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

    let mut optimizer = paro_optimizer::Optimizer::new(planner.binder, ctx.clone());
    let optimized = match optimizer.optimize(logical_plan) {
        Ok(plan) => plan,
        Err(error) => {
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
    debug!(
        target: targets::OPTIMIZER,
        statement_tag = %statement_tag,
        "Logical plan optimized"
    );

    let plan_dependencies = match &optimized {
        paro_optimizer::OptimizedStatement::Physical(portfolio) => {
            portfolio.combined_dependencies()?
        }
        paro_optimizer::OptimizedStatement::ExplainAnalyze { target, .. } => {
            target.combined_dependencies()?
        }
    };

    let executable = match optimized {
        paro_optimizer::OptimizedStatement::Physical(plan) => {
            lower_runtime_program(plan, ctx.limits.max_memory, &statement_tag)?
        }
        paro_optimizer::OptimizedStatement::ExplainAnalyze { target, spec } => {
            let target = lower_runtime_program(target, ctx.limits.max_memory, &statement_tag)?;
            paro_execution::pipeline::StatementProgram::ExplainAnalyze {
                target: Box::new(target),
                spec,
            }
        }
    };
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
        plan_dependencies,
    );

    debug!(
        target: targets::QUERY,
        statement_tag = %statement_tag,
        result_columns = compiled.result_schema().len(),
        elapsed_ms = started_at.elapsed().as_millis(),
        "Statement compilation pipeline completed"
    );

    Ok(compiled)
}

fn lower_runtime_program(
    portfolio: paro_optimizer::physical::PhysicalPlanPortfolio,
    available_memory_bytes: usize,
    statement_tag: &str,
) -> Result<paro_execution::pipeline::StatementProgram> {
    let available_memory_bytes = if available_memory_bytes == 0 {
        u64::MAX
    } else {
        u64::try_from(available_memory_bytes).unwrap_or(u64::MAX)
    };
    match paro_execution::pipeline::StatementProgram::from_physical_portfolio(
        portfolio,
        available_memory_bytes,
        u16::MAX,
    ) {
        Ok(program) => Ok(program),
        Err(error) => {
            error!(
                target: targets::EXECUTOR,
                statement_tag = %statement_tag,
                error = %error,
                stage = "runtime_program",
                "Runtime program generation failed"
            );
            Err(error)
        }
    }
}
