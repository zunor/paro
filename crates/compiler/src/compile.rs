// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::Result;
use paro_common::logging::targets;
use paro_common::typed_parameters::TypedParameterEnv;
use paro_context::StatementContext;
use paro_execution::query_executor::compiled::{CompiledStatement, ResultColumnDesc};
use paro_execution::runtime::{ParameterBindingEpoch, ParameterBindings};
use paro_parser::ast::{Expr, Statement};
use paro_parser::{Range, StatementVisitor};
use paro_planner::operator::{ExplainMode, LogicalOperator};
use paro_planner::planner::Planner;
use paro_planner::verify::verify_physical_planner_invariants;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error};

pub fn compile_statement(ctx: Arc<StatementContext>, stmt: Statement) -> Result<CompiledStatement> {
    compile_statement_with_parameters(ctx, stmt, &TypedParameterEnv::default())
}

pub fn compile_statement_with_parameters(
    ctx: Arc<StatementContext>,
    stmt: Statement,
    parameter_env: &TypedParameterEnv,
) -> Result<CompiledStatement> {
    let statement_tag = stmt.to_string();
    let started_at = Instant::now();
    debug!(
        target: targets::QUERY,
        statement_tag = %statement_tag,
        "Statement compilation pipeline started"
    );

    let mut planner = if parameter_env.is_empty() {
        Planner::new(ctx.clone())
    } else {
        Planner::new_with_parameters(
            ctx.clone(),
            parameter_env.clone(),
            build_placeholder_indexes(&stmt)?,
        )
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

    let mut optimizer = paro_optimizer::optimizer::Optimizer::new(planner.binder, ctx.clone());
    let mut optimized_plan = match optimizer.optimize(logical_plan) {
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

    let executable = if let LogicalOperator::Explain(explain) = &mut optimized_plan.operator {
        if explain.spec.mode == ExplainMode::Analyze {
            let target_plan = match generate_typed_physical_plan(ctx.as_ref(), &mut explain.child) {
                Ok(plan) => plan,
                Err(error) => {
                    error!(
                        target: targets::EXECUTOR,
                        statement_tag = %statement_tag,
                        error = %error,
                        stage = "physical_plan",
                        "EXPLAIN ANALYZE target physical plan generation failed"
                    );
                    return Err(error);
                }
            };
            let target =
                match paro_execution::pipeline::StatementProgram::from_physical_plan(target_plan) {
                    Ok(program) => program,
                    Err(error) => {
                        error!(
                            target: targets::EXECUTOR,
                            statement_tag = %statement_tag,
                            error = %error,
                            stage = "runtime_program",
                            "EXPLAIN ANALYZE target runtime program generation failed"
                        );
                        return Err(error);
                    }
                };
            paro_execution::pipeline::StatementProgram::ExplainAnalyze {
                target: Box::new(target),
                spec: explain.spec,
            }
        } else {
            compile_regular_statement(ctx.as_ref(), &mut optimized_plan, &statement_tag)?
        }
    } else {
        compile_regular_statement(ctx.as_ref(), &mut optimized_plan, &statement_tag)?
    };
    debug!(
        target: targets::EXECUTOR,
        statement_tag = %statement_tag,
        "Runtime program generated"
    );

    let parameter_types = parameter_env
        .logical_types()
        .into_iter()
        .map(|ty| ty.unwrap_or(paro_common::types::LogicalType::Unknown))
        .collect::<Vec<_>>();
    let parameter_bindings =
        ParameterBindings::from_typed_env(parameter_env, ParameterBindingEpoch::new(1));
    let compiled = CompiledStatement::new_with_bindings(
        executable,
        result_names
            .into_iter()
            .zip(result_types)
            .map(|(name, logical_type)| ResultColumnDesc { name, logical_type })
            .collect(),
        parameter_types,
        parameter_bindings,
    );

    debug!(
        target: targets::QUERY,
        statement_tag = %statement_tag,
        result_columns = compiled.result_schema.len(),
        elapsed_ms = started_at.elapsed().as_millis(),
        "Statement compilation pipeline completed"
    );

    Ok(compiled)
}

fn compile_regular_statement(
    ctx: &StatementContext,
    optimized_plan: &mut paro_planner::plan::LogicalPlan,
    statement_tag: &str,
) -> Result<paro_execution::pipeline::StatementProgram> {
    let arena_plan = match generate_typed_physical_plan(ctx, optimized_plan) {
        Ok(plan) => plan,
        Err(error) => {
            error!(
                target: targets::EXECUTOR,
                statement_tag = %statement_tag,
                error = %error,
                stage = "physical_plan",
                "Physical plan generation failed"
            );
            return Err(error);
        }
    };
    match paro_execution::pipeline::StatementProgram::from_physical_plan(arena_plan) {
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

fn generate_typed_physical_plan(
    ctx: &StatementContext,
    logical_plan: &mut paro_planner::plan::LogicalPlan,
) -> Result<paro_execution::physical::PhysicalPlan> {
    verify_physical_planner_invariants(&logical_plan.operator)?;
    paro_execution::column_binding_resolver::ColumnBindingResolver::resolve(
        &mut logical_plan.operator,
    )?;
    let mut generator = paro_execution::physical::PhysicalPlanGenerator::new(
        paro_execution::physical::PlanBuildContext {
            force_external: ctx.limits.force_external,
            rowset_scan_pushdown: ctx.limits.rowset_scan_pushdown,
        },
    );
    generator.generate(logical_plan)
}

fn build_placeholder_indexes(stmt: &Statement) -> Result<BTreeMap<Range, usize>> {
    let mut next_index = 0usize;
    let mut placeholders = BTreeMap::new();
    let mut visitor = StatementVisitor::new(
        |expr| {
            if let Expr::Placeholder { span } = expr {
                let Some(span) = span else {
                    return;
                };
                placeholders.entry(*span).or_insert_with(|| {
                    let current = next_index;
                    next_index += 1;
                    current
                });
            }
        },
        |_| {},
    );
    visitor.visit(stmt);

    if placeholders.len() != next_index {
        return Err(paro_common::error::protocol_violation(
            "duplicate placeholder spans detected during parameterized compilation".to_string(),
        ));
    }

    Ok(placeholders)
}
