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
    // Diagnostic entry includes AST identity preparation; the existing normal
    // compiler clock and its evidence boundary remain unchanged.
    let capture_started = ctx.options.compile_capture.as_ref().map(|_| Instant::now());
    let statement_tag = stmt.to_string();
    let started_at = Instant::now();
    if let Some(capture) = &ctx.options.compile_capture {
        use paro_context::compile_diagnostics::Observation::Observed;
        capture.update(|r| {
            r.input_fingerprint = Observed(paro_context::statement_fingerprint(&statement_tag));
            r.planning_settings = Observed(ctx.settings.planning_fingerprint());
            r.available_memory_bytes = Observed(ctx.compile_resources.available_memory_bytes);
            r.available_parallel_tasks = Observed(ctx.compile_resources.available_parallel_tasks);
        });
    }
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
    if let Some(capture) = &ctx.options.compile_capture {
        capture.update(|r| {
            r.bind_ns = paro_context::compile_diagnostics::Observation::Observed(
                bind_and_plan_started.elapsed().as_nanos() as u64,
            )
        });
    }
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
    let partition = paro_optimizer::work_partition::begin(
        optimizer_started,
        ctx.options.compile_capture.as_ref().is_some_and(|capture| {
            capture.level() == paro_context::compile_diagnostics::CaptureLevel::Detail
        }),
    );
    paro_optimizer::cascades::memo::diagnostic_snapshot::clear();
    let mut optimizer = paro_optimizer::Optimizer::new(planner.binder, ctx.clone());
    let optimized = match optimizer.optimize(logical_plan) {
        Ok(plan) => plan,
        Err(error) => {
            if let Some(report) = partition.finish(Instant::now()) {
                if let Some(capture) = &ctx.options.compile_capture {
                    capture.update(|r| {
                        r.optimizer_work = paro_context::compile_diagnostics::Observation::Observed(
                            report.snapshot(),
                        )
                    });
                }
                let _ = report.write(&statement_tag, false);
            }
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
    let optimizer_finished = Instant::now();
    if let Some(capture) = &ctx.options.compile_capture {
        capture.update(|r| {
            r.optimizer_ns = paro_context::compile_diagnostics::Observation::Observed(
                optimizer_finished
                    .duration_since(optimizer_started)
                    .as_nanos() as u64,
            )
        });
    }
    let partition_report = partition.finish(optimizer_finished);
    let mut compile_work = paro_context::compile_work_evidence_enabled().then(|| {
        let mut work = optimizer.compile_work();
        work.optimizer_elapsed_us = u64::try_from(
            optimizer_finished
                .duration_since(optimizer_started)
                .as_micros(),
        )
        .unwrap_or(u64::MAX);
        work
    });
    if let Some(trace) = &statement_trace {
        trace.record_span("compile", "optimizer", optimizer_started);
    }
    if let Err(error) = paro_optimizer::cascades::memo::diagnostic_snapshot::flush(&statement_tag) {
        tracing::warn!(%error, "frontier diagnostic snapshot write failed");
    }
    if let Some(report) = partition_report {
        if let Some(capture) = &ctx.options.compile_capture {
            capture.update(|r| {
                r.optimizer_work =
                    paro_context::compile_diagnostics::Observation::Observed(report.snapshot())
            });
        }
        if let Err(error) = report.write(&statement_tag, true) {
            tracing::warn!(%error, "optimizer work partition write failed");
        }
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
    if let Some(capture) = &ctx.options.compile_capture {
        use paro_context::compile_diagnostics::Observation::Observed;
        capture.update(|r| {
            r.verify_ns = Observed(
                runtime_image_started
                    .duration_since(verify_started)
                    .as_nanos() as u64,
            );
            r.safety_verified = Observed(true);
            r.output_columns = Observed(result_names.len());
            if let paro_optimizer::OptimizedStatement::Physical(portfolio) = &optimized {
                if let Some(class) = portfolio
                    .grant_search
                    .as_ref()
                    .and_then(|s| s.expected_class)
                {
                    r.expected_class = Observed(class.0);
                    let mut matches = portfolio
                        .variants
                        .iter()
                        .filter(|v| v.admissible_classes.contains(&class));
                    if let Some(variant) = matches.next() {
                        if matches.next().is_none() {
                            r.selected_fingerprint = Observed([
                                (variant.physical_fingerprint.0 >> 64) as u64,
                                variant.physical_fingerprint.0 as u64,
                            ]);
                        }
                    }
                }
            }
        });
        if let paro_optimizer::OptimizedStatement::Physical(portfolio) = &optimized {
            use paro_context::compile_diagnostics::{VariantSummary, MAX_VARIANTS};
            capture.variants(
                portfolio.variants.len(),
                portfolio
                    .variants
                    .iter()
                    .take(MAX_VARIANTS)
                    .enumerate()
                    .filter_map(|(ordinal, variant)| {
                        let admissible_classes = variant
                            .admissible_classes
                            .iter()
                            .try_fold(0u64, |mask, class| {
                                1u64.checked_shl(class.0).map(|bit| mask | bit)
                            })?;
                        Some(VariantSummary {
                            ordinal: ordinal as u16,
                            physical_fingerprint: [
                                (variant.physical_fingerprint.0 >> 64) as u64,
                                variant.physical_fingerprint.0 as u64,
                            ],
                            admissible_classes,
                        })
                    }),
            );
        }
    }
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
        trace.record_event("compile", "compiled_artifact_ready");
    }
    debug!(
        target: targets::EXECUTOR,
        statement_tag = %statement_tag,
        "Runtime program generated"
    );

    let mut compiled = CompiledStatement::new(
        executable,
        result_names
            .into_iter()
            .zip(result_types)
            .map(|(name, logical_type)| ResultColumnDesc { name, logical_type })
            .collect(),
        parameter_types.to_vec(),
        ctx.compile_environment_key(),
    )?;
    if let Some(capture) = &ctx.options.compile_capture {
        use std::hash::{Hash, Hasher};
        let mut identity = std::collections::hash_map::DefaultHasher::new();
        for column in compiled.result_schema() {
            column.name.hash(&mut identity);
            column.logical_type.hash(&mut identity);
        }
        capture.update(|r| {
            r.output_identity =
                paro_context::compile_diagnostics::Observation::Observed(identity.finish())
        });
    }

    // Copy the small immutable summaries before releasing the planner.  The
    // compiler clock is finalized below, after that release, so the normal
    // receipt and compile-work channels retain the historical compiler
    // boundary instead of silently excluding planner-state cleanup.
    let mut compile_receipt = optimizer.compile_receipt();

    // The optimizer and planner state are no longer needed once the deferred
    // executable image has been materialized.  Keep this release boundary in
    // the same trace as compiler return so a cold sample can distinguish
    // image construction from memory retained until admission.
    drop(optimizer);
    if let Some(trace) = &statement_trace {
        trace.record_event("compile", "planning_state_released");
    }

    let compiler_elapsed_us = u64::try_from(started_at.elapsed().as_micros()).unwrap_or(u64::MAX);
    if let Some(work) = compile_work.as_mut() {
        work.compiler_elapsed_us = compiler_elapsed_us;
    }
    if let Some(receipt) = compile_receipt.as_mut() {
        receipt.artifact_identity = Some(compiled.artifact_identity());
        receipt.compile_work = compile_work;
    }
    if let Some(receipt) = compile_receipt {
        compiled = compiled.with_compile_receipt(receipt);
    }
    if let Some(work) = compile_work {
        compiled = compiled.with_compile_work(work);
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

    if let Some(capture) = &ctx.options.compile_capture {
        use paro_context::compile_diagnostics::Observation::Observed;
        capture.update(|r| {
            r.finish_ns = Observed(runtime_image_started.elapsed().as_nanos() as u64);
            r.compiler_ns =
                Observed(capture_started.unwrap_or(started_at).elapsed().as_nanos() as u64);
            if let (
                Observed(total),
                Observed(bind),
                Observed(opt),
                Observed(verify),
                Observed(finish),
            ) = (
                r.compiler_ns,
                r.bind_ns,
                r.optimizer_ns,
                r.verify_ns,
                r.finish_ns,
            ) {
                r.compiler_other_ns = Observed(total.saturating_sub(bind + opt + verify + finish));
            }
            r.artifact = paro_context::compile_diagnostics::ArtifactStatus::CompiledArtifactReady;
            r.artifact_identity = Observed(compiled.artifact_identity());
            r.outcome = paro_context::compile_diagnostics::CompileOutcome::Success;
        });
    }
    Ok(compiled)
}
