// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::Result;
use paro_common::logging::targets;
use paro_common::typed_parameters::TypedParameterEnv;
use paro_context::StatementContext;
use paro_execution::query_executor::compiled::{CompiledStatement, ResultColumnDesc};
use paro_parser::ast::{Expr, Statement};
use paro_parser::{Range, StatementVisitor};
use paro_planner::planner::Planner;
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

    let generator = paro_execution::physical_plan::generator::PhysicalPlanGenerator::new(ctx);
    let physical_plan = match generator.plan(&mut optimized_plan) {
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
    debug!(
        target: targets::EXECUTOR,
        statement_tag = %statement_tag,
        "Physical plan generated"
    );

    let compiled = CompiledStatement {
        physical_plan,
        result_schema: result_names
            .into_iter()
            .zip(result_types)
            .map(|(name, logical_type)| ResultColumnDesc { name, logical_type })
            .collect(),
        parameter_types: Vec::new(),
    };

    debug!(
        target: targets::QUERY,
        statement_tag = %statement_tag,
        result_columns = compiled.result_schema.len(),
        elapsed_ms = started_at.elapsed().as_millis(),
        "Statement compilation pipeline completed"
    );

    Ok(compiled)
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
