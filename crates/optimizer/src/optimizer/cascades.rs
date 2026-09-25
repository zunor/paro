// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Explicit Memo strategy adapter. The default staged driver does not enter here.

use super::*;

impl Optimizer {
    pub(super) fn optimize_cascades(
        &mut self,
        query: OwnedLogicalPlan,
        statement_layer: QueryStatementLayer,
        explain: Option<ExplainEnvelope>,
        grant_classes: Box<[ResourceGrantClass]>,
        started_at: Instant,
        pre_partition: crate::diagnostics::work::Scope,
    ) -> Result<OptimizedStatement> {
        let observes_optimizer_diagnostics = observes_optimizer_diagnostics(&query);
        if self.ctx.session.settings.optimizer_aggregate_strategy()?
            != paro_context::OptimizerAggregateStrategy::Joint
        {
            return Err(paro_common::error::invalid_input(
                "optimizer_aggregate_strategy=single_stage requires optimizer_search_policy=pipeline",
            ));
        }
        let phase_started = Instant::now();
        let phase_allocated = paro_common::allocator::thread_allocated_bytes();
        let graph_plans = enumerate_graph_region_plans(
            query,
            &self.binder.bind_context,
            self.budget.max_graph_frontiers,
        )?;
        let mut alternatives = Vec::with_capacity(graph_plans.len().saturating_mul(3));
        for (index, graph_plan) in graph_plans.into_iter().enumerate() {
            let (graph_plan, correlated_seed) =
                if contains_redundant_computation_region(&graph_plan)
                    || scalar_aggregate_fusion::contains_candidate_root(&graph_plan)
                {
                    let (graph_plan, correlated_seed) = fork_plan_preserving_indices(
                        graph_plan,
                        self.binder.bind_context.shared().as_ref(),
                    )?;
                    (
                        graph_plan,
                        Some(self.prepare_correlated_seed(correlated_seed)),
                    )
                } else {
                    (graph_plan, None)
                };
            let canonical = self.canonicalize_query(graph_plan)?;
            let (baseline_input, distinct_input) =
                distinct_decomposition::fork_candidate(canonical, &self.binder.bind_context)?;
            let baseline = self.settle_relational_baseline(baseline_input)?;
            let distinct_feasibility_candidate = distinct_input
                .map(|input| self.distinct_aggregate_feasibility_candidate(input))
                .transpose()?
                .flatten();
            alternatives.push(baseline.into_alternative(if index == 0 {
                AlternativeOrigin::Baseline
            } else {
                AlternativeOrigin::Specialized {
                    rule: GRAPH_REGION_ENUMERATOR_RULE,
                }
            }));

            if let Some(plan) = distinct_feasibility_candidate {
                alternatives.push(plan.into_alternative(
                    // DISTINCT modifier state is not spillable.  This
                    // equivalent is a mandatory resource-feasibility path,
                    // not optional transformation work: exhausting or
                    // disabling optional rules must not make a valid query
                    // uncompilable under a finite grant.
                    AlternativeOrigin::Specialized {
                        rule: DISTINCT_AGGREGATE_FEASIBILITY_RULE,
                    },
                ));
            }
            let Some(correlated_seed) = correlated_seed else {
                continue;
            };
            let correlated_canonical = match self.canonicalize_query(correlated_seed) {
                Ok(plan) => plan,
                Err(error) => {
                    debug!(
                        target: targets::OPTIMIZER,
                        %error,
                        "correlated-region preparation rejected an invalid optional candidate"
                    );
                    continue;
                }
            };
            if alternatives.len() >= self.budget.max_optional_logical_exprs_per_group as usize + 1 {
                break;
            }
            let (aggregate_input, scalar_reuse_input) = fork_plan_preserving_indices(
                correlated_canonical,
                self.binder.bind_context.shared().as_ref(),
            )?;
            match self.correlated_aggregate_candidate(aggregate_input) {
                Ok(candidate) => {
                    let (base_plan, payload_plan) = fork_plan_preserving_indices(
                        candidate.plan,
                        self.binder.bind_context.shared().as_ref(),
                    )?;
                    let payload_input = CandidatePlan {
                        plan: payload_plan,
                        column_stats: candidate.column_stats.clone(),
                    };
                    alternatives.push(
                        CandidatePlan {
                            plan: base_plan,
                            column_stats: candidate.column_stats,
                        }
                        .into_alternative(AlternativeOrigin::Specialized {
                            rule: CORRELATED_AGGREGATE_REGION_RULE,
                        }),
                    );
                    if alternatives.len()
                        < self.budget.max_optional_logical_exprs_per_group as usize + 1
                    {
                        match self.correlated_topn_payload_candidate(payload_input) {
                            Ok(Some(plan)) => alternatives.push(plan.into_alternative(
                                AlternativeOrigin::Specialized {
                                    rule: CORRELATED_TOPN_PAYLOAD_REGION_RULE,
                                },
                            )),
                            Ok(None) => {}
                            Err(error) => debug!(
                                target: targets::OPTIMIZER,
                                %error,
                                "correlated TopN payload recipe rejected an invalid optional candidate"
                            ),
                        }
                    }
                }
                Err(error) => debug!(
                    target: targets::OPTIMIZER,
                    %error,
                    "correlated-aggregate enumerator rejected an invalid optional candidate"
                ),
            }
            if alternatives.len() >= self.budget.max_optional_logical_exprs_per_group as usize + 1 {
                break;
            }
            match self
                .settle_query_candidate(scalar_reuse_input)
                .and_then(|plan| self.scalar_reuse_candidate(plan))
            {
                Ok(plan) => {
                    alternatives.push(plan.into_alternative(AlternativeOrigin::Specialized {
                        rule: SCALAR_REUSE_REGION_RULE,
                    }))
                }
                Err(error) => debug!(
                    target: targets::OPTIMIZER,
                    %error,
                    "scalar-reuse enumerator rejected an invalid optional candidate"
                ),
            }
        }
        self.ctx.profiler.record(
            OptimizerComponent::SemanticNormalization,
            phase_started.elapsed(),
        );
        self.ctx.profiler.record_component_allocation(
            OptimizerComponent::SemanticNormalization,
            paro_common::allocator::allocated_bytes_since(phase_allocated),
        );
        let query_ir_phase_started = Instant::now();
        let query_ir_phase_allocated = paro_common::allocator::thread_allocated_bytes();
        for alternative in &alternatives {
            verify_physical_planner_invariants(&alternative.plan.operator)?;
        }
        let strong_incumbent_experiment = std::env::var_os("PARO_STRONG_INCUMBENT_EXPERIMENT")
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
        let strong_incumbent_provide_bound = strong_incumbent_experiment
            && std::env::var_os("PARO_STRONG_INCUMBENT_PROVIDE_BOUND")
                .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
        let strong_incumbent_inject_logical = strong_incumbent_experiment
            && std::env::var_os("PARO_STRONG_INCUMBENT_INJECT_LOGICAL")
                .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
        let (mode, extraction, search_phase_started, search_phase_allocated) =
            if strong_incumbent_experiment {
                // Build two independent Memo instances from the same semantic
                // alternatives. The first one is used only to produce a verified
                // immutable upper bound; the second one owns the measured proof
                // search. No search frontier, task registry, or planner payload
                // arena crosses this boundary.
                let mut source_binder = self.binder.clone();
                source_binder.bind_context = self.binder.bind_context.with_independent_plan_ids();
                let source_bind_shared = source_binder.bind_context.shared().clone();
                let mut source_alternatives = Vec::with_capacity(alternatives.len());
                let mut target_alternatives = Vec::with_capacity(alternatives.len());
                for alternative in alternatives {
                    let source = alternative.source;
                    let column_stats = alternative.column_stats;
                    // Keep the exact original plan in the measured target
                    // Memo so the no-seed controls have the same initial
                    // domain as the ordinary production path. Only the
                    // source Memo receives a deep duplicate; its fresh
                    // plan-node IDs must never become a target-search
                    // variable. Rebuilding both branches here can change
                    // insertion order and therefore confound the isolation
                    // experiment even when the trees are semantically equal.
                    let source_plan = duplicate_plan_preserving_indices(
                        &alternative.plan,
                        source_bind_shared.as_ref(),
                    );
                    let target_plan = alternative.plan;
                    source_alternatives.push(LogicalAlternative {
                        plan: source_plan,
                        source,
                        column_stats: column_stats.clone(),
                    });
                    target_alternatives.push(LogicalAlternative {
                        plan: target_plan,
                        source,
                        column_stats,
                    });
                }

                let source_input = self
                    .build_search_input_with_binder(
                        source_alternatives,
                        &statement_layer,
                        &source_binder,
                    )?
                    .with_strong_incumbent_export(true)
                    // C/D must use the same known seed.  The pruning switch
                    // belongs to the independent target proof Memo, not to
                    // source seed generation.
                    .with_certified_group_pruning(false);
                self.ctx.profiler.record(
                    OptimizerComponent::QueryIrConstruction,
                    query_ir_phase_started.elapsed(),
                );
                self.ctx.profiler.record_component_allocation(
                    OptimizerComponent::QueryIrConstruction,
                    paro_common::allocator::allocated_bytes_since(query_ir_phase_allocated),
                );
                let search_phase_started = Instant::now();
                let search_phase_allocated = paro_common::allocator::thread_allocated_bytes();
                let source_started = Instant::now();
                let source_output = source_input.optimize(&grant_classes)?;
                let source_total_us =
                    u64::try_from(source_started.elapsed().as_micros()).unwrap_or(u64::MAX);
                let source_complete = source_output.search_summary.is_complete();
                let source_obligations = source_output.search_summary.obligations.len() as u64;
                let source_profile_us = source_output.search_milestones.search_return_profile_us;
                let source_export_us = source_output.strong_incumbent_export_us;
                let source_logical_plans = source_output.strong_incumbent_logical_plans.into_vec();
                let plans = source_output.strong_incumbent_plans.into_vec();
                let source_seed_count = plans.len();
                if plans.is_empty() {
                    return Err(paro_error::internal(
                        "strong incumbent experiment produced no verified seed plan",
                    ));
                }
                if strong_incumbent_inject_logical && source_logical_plans.len() != plans.len() {
                    return Err(paro_error::internal(format!(
                        "strong incumbent logical/physical seed count mismatch: logical={} physical={}",
                        source_logical_plans.len(),
                        plans.len()
                    )));
                }
                if let Some(trace) = self.ctx.session.statement_trace() {
                    for (seed_ordinal, seed) in plans.iter().enumerate() {
                        record_seed_plan_evidence(
                            trace.as_ref(),
                            &format!("strong_incumbent_source_seed_{seed_ordinal}"),
                            seed,
                            source_logical_plans.get(seed_ordinal),
                        );
                    }
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_provide_bound",
                        u64::from(strong_incumbent_provide_bound),
                    );
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_inject_logical",
                        u64::from(strong_incumbent_inject_logical),
                    );
                    trace.record_event("optimizer", "strong_incumbent_seed_source_complete");
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_seed_source_count",
                        source_seed_count as u64,
                    );
                    if let Some(plan) = plans.first() {
                        trace.record_value(
                            "optimizer",
                            "strong_incumbent_seed_plan_identity_lo",
                            plan.plan_identity().0 as u64,
                        );
                        trace.record_value(
                            "optimizer",
                            "strong_incumbent_seed_plan_identity_hi",
                            (plan.plan_identity().0 >> 64) as u64,
                        );
                    }
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_seed_source_total_us",
                        source_total_us,
                    );
                    if let Some(export_us) = source_export_us {
                        trace.record_value(
                            "optimizer",
                            "strong_incumbent_seed_source_export_us",
                            export_us,
                        );
                    }
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_seed_source_search_complete",
                        u64::from(source_complete),
                    );
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_seed_source_obligations",
                        source_obligations,
                    );
                    if let Some(profile_us) = source_profile_us {
                        trace.record_value(
                            "optimizer",
                            "strong_incumbent_seed_source_search_us",
                            profile_us,
                        );
                    }
                }

                let seed_column_stats = target_alternatives
                    .first()
                    .map(|alternative| alternative.column_stats.clone())
                    .unwrap_or_else(|| Arc::new(HashMap::new()));
                let seed_output_table_index = target_alternatives
                    .first()
                    .and_then(|alternative| alternative.plan.get_column_bindings().first().copied())
                    .map(|binding| binding.table_index);
                let expected_seed_output_bindings = target_alternatives
                    .first()
                    .map(|alternative| alternative.plan.get_column_bindings())
                    .unwrap_or_default();
                let mut pricing_alternatives =
                    if strong_incumbent_provide_bound && !strong_incumbent_inject_logical {
                        target_alternatives
                            .iter()
                            .map(|alternative| LogicalAlternative {
                                plan: duplicate_plan_preserving_indices(
                                    &alternative.plan,
                                    source_bind_shared.as_ref(),
                                ),
                                source: alternative.source,
                                column_stats: alternative.column_stats.clone(),
                            })
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                let mut target_alternatives = target_alternatives;
                for (seed_ordinal, plan) in source_logical_plans.into_iter().enumerate() {
                    if !strong_incumbent_inject_logical && !strong_incumbent_provide_bound {
                        continue;
                    }
                    {
                        let source_bindings = plan.get_column_bindings();
                        let plan = if strong_incumbent_inject_logical {
                            duplicate_plan_preserving_indices(
                                &plan,
                                self.binder.bind_context.shared().as_ref(),
                            )
                        } else {
                            plan
                        };
                        let plan = if let Some(table_index) = seed_output_table_index {
                            rebase_seed_output_table_index(plan, table_index)?
                        } else {
                            plan
                        };
                        let rebased_output_bindings = plan.get_column_bindings();
                        if rebased_output_bindings != expected_seed_output_bindings {
                            return Err(paro_error::internal(format!(
                                "strong seed output layout cannot be rebased: expected={expected_seed_output_bindings:?}, actual={rebased_output_bindings:?}"
                            )));
                        }
                        if let Some(trace) = self.ctx.session.statement_trace() {
                            trace.record_value(
                                "optimizer",
                                &format!("strong_incumbent_seed_{seed_ordinal}_target_table_index"),
                                seed_output_table_index.unwrap_or(usize::MAX) as u64,
                            );
                            trace.record_value(
                                "optimizer",
                                &format!(
                                    "strong_incumbent_seed_{seed_ordinal}_source_output_width"
                                ),
                                source_bindings.len() as u64,
                            );
                            trace.record_value(
                                "optimizer",
                                &format!(
                                    "strong_incumbent_seed_{seed_ordinal}_source_output_table"
                                ),
                                source_bindings
                                    .first()
                                    .map(|binding| binding.table_index as u64)
                                    .unwrap_or(u64::MAX),
                            );
                            let target_bindings = plan.get_column_bindings();
                            trace.record_value(
                                "optimizer",
                                &format!(
                                    "strong_incumbent_seed_{seed_ordinal}_rebased_output_table"
                                ),
                                target_bindings
                                    .first()
                                    .map(|binding| binding.table_index as u64)
                                    .unwrap_or(u64::MAX),
                            );
                        }
                        let alternative = LogicalAlternative {
                            plan,
                            source: AlternativeOrigin::Specialized {
                                rule: STRONG_INCUMBENT_SEED_RULE,
                            },
                            column_stats: seed_column_stats.clone(),
                        };
                        if strong_incumbent_inject_logical {
                            target_alternatives.push(alternative);
                        } else {
                            pricing_alternatives.push(alternative);
                        }
                    }
                }
                // Keep the Memo independent while making a source winner's
                // transformed logical shell available for re-pricing. The
                // target registry, facts, grant and costs are rebuilt below;
                // no source frontier or search cache crosses this boundary.
                let mut target_input =
                    self.build_search_input(target_alternatives, &statement_layer)?;
                let mode = target_input.mode;
                if strong_incumbent_provide_bound {
                    if strong_incumbent_inject_logical {
                        for plan in plans {
                            target_input = target_input.with_strong_incumbent_plan(plan);
                        }
                    } else {
                        let pricing_input = self.build_search_input_with_binder(
                            pricing_alternatives,
                            &statement_layer,
                            &source_binder,
                        )?;
                        let pricing_started = Instant::now();
                        let repriced = pricing_input
                            .reprice_seed_plans_in_isolated_memo(&grant_classes, &plans)?;
                        let pricing_us = u64::try_from(pricing_started.elapsed().as_micros())
                            .unwrap_or(u64::MAX);
                        if let Some(trace) = self.ctx.session.statement_trace() {
                            trace.record_value(
                                "optimizer",
                                "strong_incumbent_seed_isolated_reprice_us",
                                pricing_us,
                            );
                            trace.record_value(
                                "optimizer",
                                "strong_incumbent_seed_isolated_reprice_count",
                                repriced.len() as u64,
                            );
                            for (incumbent_ordinal, incumbent) in repriced.iter().enumerate() {
                                record_priced_incumbent_evidence(
                                    trace.as_ref(),
                                    &format!("strong_incumbent_target_priced_{incumbent_ordinal}"),
                                    incumbent,
                                );
                            }
                        }
                        for incumbent in repriced {
                            target_input = target_input.with_prepriced_strong_incumbent(incumbent);
                        }
                    }
                }
                if let Some(trace) = self.ctx.session.statement_trace() {
                    trace.record_event("optimizer", "strong_incumbent_seed_target_begin");
                }
                let target_started = Instant::now();
                let extraction = target_input.optimize(&grant_classes)?;
                let target_total_us =
                    u64::try_from(target_started.elapsed().as_micros()).unwrap_or(u64::MAX);
                if let Some(trace) = self.ctx.session.statement_trace() {
                    trace.record_event("optimizer", "strong_incumbent_seed_target_complete");
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_seed_target_total_us",
                        target_total_us,
                    );
                    if let Some(reprice_us) = extraction.strong_incumbent_reprice_us {
                        trace.record_value(
                            "optimizer",
                            "strong_incumbent_seed_target_reprice_us",
                            reprice_us,
                        );
                        trace.record_value(
                            "optimizer",
                            "strong_incumbent_seed_target_reprice_count",
                            extraction.strong_incumbent_reprice_count,
                        );
                        if let Some(identity) = extraction.strong_incumbent_plan_identity {
                            trace.record_value(
                                "optimizer",
                                "strong_incumbent_seed_target_plan_identity_lo",
                                identity.0 as u64,
                            );
                            trace.record_value(
                                "optimizer",
                                "strong_incumbent_seed_target_plan_identity_hi",
                                (identity.0 >> 64) as u64,
                            );
                        }
                    }
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_seed_target_search_complete",
                        u64::from(extraction.search_summary.is_complete()),
                    );
                }
                (
                    mode,
                    extraction,
                    search_phase_started,
                    search_phase_allocated,
                )
            } else {
                let input = self.build_search_input(alternatives, &statement_layer)?;
                let mode = input.mode;
                self.ctx.profiler.record(
                    OptimizerComponent::QueryIrConstruction,
                    query_ir_phase_started.elapsed(),
                );
                self.ctx.profiler.record_component_allocation(
                    OptimizerComponent::QueryIrConstruction,
                    paro_common::allocator::allocated_bytes_since(query_ir_phase_allocated),
                );
                let search_phase_started = Instant::now();
                let search_phase_allocated = paro_common::allocator::thread_allocated_bytes();
                drop(pre_partition);
                let extraction = input.optimize(&grant_classes)?;
                (
                    mode,
                    extraction,
                    search_phase_started,
                    search_phase_allocated,
                )
            };
        self.compile_receipt = Some(CompileReceiptSummary {
            schema_version: paro_context::COMPILE_RECEIPT_SCHEMA_VERSION,
            artifact_identity: None,
            search_stop: Observation::Observed(match extraction.search_stop.reason {
                crate::cascades::engine::SearchStopReason::Complete => SearchStop::Complete,
                crate::cascades::engine::SearchStopReason::SearchIncomplete => {
                    SearchStop::Incomplete
                }
                crate::cascades::engine::SearchStopReason::Deadline => SearchStop::Deadline,
                crate::cascades::engine::SearchStopReason::BudgetLimited => {
                    SearchStop::BudgetLimited
                }
                crate::cascades::engine::SearchStopReason::RuleFailure => SearchStop::RuleFailure,
                crate::cascades::engine::SearchStopReason::QualityPolicySatisfied => {
                    SearchStop::QualityPolicySatisfied
                }
            }),
            search_complete: Observation::Observed(extraction.search_summary.is_complete()),
            quality_policy_satisfied: Observation::Observed(matches!(
                extraction.quality_policy_status,
                crate::cascades::quality::QualityPolicyStatus::Satisfied(_)
            )),
            budget_limited: Observation::Observed(extraction.search_stop.budget_limited),
            obligations: Observation::Observed(extraction.search_summary.obligations.len() as u64),
            groups: Observation::Observed(extraction.search_summary.groups),
            logical_expressions: Observation::Observed(
                extraction.search_summary.logical_expressions,
            ),
            physical_expressions: Observation::Observed(
                extraction.search_summary.physical_expressions,
            ),
            expected_class: extraction
                .grant_search
                .as_ref()
                .and_then(|coverage| coverage.expected_class)
                .map_or(
                    Observation::Uncovered(
                        paro_context::compile_diagnostics::UncoveredReason::NotInstrumented,
                    ),
                    |class| Observation::Observed(class.0),
                ),
            variant_count: Observation::Observed(extraction.variants.len()),
            omitted_variants: 0,
            compile_work: None,
        });
        let _finish_partition =
            crate::diagnostics::work::enter(crate::diagnostics::work::Bucket::Finish);
        if let Some(capture) = &self.ctx.session.options.compile_capture {
            use paro_context::compile_diagnostics::{
                Observation::Observed, RuleSummary, SearchStop,
            };
            let mut search_counters = extraction.search_summary.work_counters.clone();
            search_counters.insert(
                "search_complete",
                u64::from(extraction.search_summary.is_complete()),
            );
            search_counters.insert("memo_group_count", extraction.search_summary.groups);
            search_counters.insert(
                "memo_logical_expression_count",
                extraction.search_summary.logical_expressions,
            );
            search_counters.insert(
                "memo_physical_expression_count",
                extraction.search_summary.physical_expressions,
            );
            search_counters.insert(
                "search_rule_failure_count",
                extraction
                    .search_summary
                    .obligations
                    .iter()
                    .filter(|obligation| {
                        matches!(
                            obligation.reason,
                            crate::cascades::budget::SearchIncompleteReason::RuleFailure { .. }
                        )
                    })
                    .count() as u64,
            );
            search_counters.insert(
                "search_deadline_reached",
                u64::from(
                    extraction
                        .search_summary
                        .obligations
                        .iter()
                        .any(|obligation| {
                            obligation.reason
                                == crate::cascades::budget::SearchIncompleteReason::Deadline
                        }),
                ),
            );
            capture.update(|record| {
                record.groups = Observed(extraction.search_summary.groups);
                record.logical_expressions =
                    Observed(extraction.search_summary.logical_expressions);
                record.physical_expressions =
                    Observed(extraction.search_summary.physical_expressions);
                record.obligations = Observed(extraction.search_summary.obligations.len() as u64);
                record.search_complete = Observed(extraction.search_summary.is_complete());
                record.quality_policy_satisfied = Observed(matches!(
                    extraction.quality_policy_status,
                    crate::cascades::quality::QualityPolicyStatus::Satisfied(_)
                ));
                record.budget_limited = Observed(extraction.search_stop.budget_limited);
                record.search_stop = Observed(match extraction.search_stop.reason {
                    crate::cascades::engine::SearchStopReason::Complete => SearchStop::Complete,
                    crate::cascades::engine::SearchStopReason::SearchIncomplete => {
                        SearchStop::Incomplete
                    }
                    crate::cascades::engine::SearchStopReason::Deadline => SearchStop::Deadline,
                    crate::cascades::engine::SearchStopReason::BudgetLimited => {
                        SearchStop::BudgetLimited
                    }
                    crate::cascades::engine::SearchStopReason::RuleFailure => {
                        SearchStop::RuleFailure
                    }
                    crate::cascades::engine::SearchStopReason::QualityPolicySatisfied => {
                        SearchStop::QualityPolicySatisfied
                    }
                });
            });
            capture.search_counters(search_counters);
            let active_rules: std::collections::BTreeSet<_> = extraction
                .rule_attempts
                .keys()
                .chain(extraction.rule_elapsed.keys())
                .chain(extraction.rule_insertions.keys())
                .chain(extraction.rule_binding_work.keys())
                .copied()
                .collect();
            for id in active_rules {
                let binding = extraction
                    .rule_binding_work
                    .get(&id)
                    .copied()
                    .unwrap_or_default();
                capture.rule(RuleSummary {
                    id: id.0,
                    binding_calls: binding.calls,
                    binding_ns: u64::try_from(binding.elapsed.as_nanos()).unwrap_or(u64::MAX),
                    attempts: extraction.rule_attempts.get(&id).copied().unwrap_or(0),
                    inserted: extraction.rule_insertions.get(&id).copied().unwrap_or(0),
                    elapsed_ns: extraction
                        .rule_elapsed
                        .get(&id)
                        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)),
                });
            }
            if capture.level() == paro_context::compile_diagnostics::CaptureLevel::Detail {
                capture.detail_omitted(
                    extraction
                        .search_milestones
                        .candidate_lifecycle_dropped
                        .saturating_add(
                            extraction
                                .search_milestones
                                .transformation_task_lifecycle_dropped,
                        ),
                );
                use paro_context::compile_diagnostics::{
                    BindingRef, CandidateRef, DetailEvent, FingerprintRef, GoalRef, LogicalExprRef,
                    MemoGroupRef, PhysicalExprRef, RuleRef,
                };
                // Child and fact records are independent producer streams.
                // They retain the candidate event as a causal parent, but
                // never reuse the parent's sequence as their own stream
                // sequence.
                let mut candidate_child_sequence = 0_u64;
                let mut fact_sequence = 0_u64;
                for id in extraction
                    .rule_attempts
                    .keys()
                    .chain(extraction.rule_elapsed.keys())
                    .chain(extraction.rule_insertions.keys())
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>()
                {
                    let binding = extraction
                        .rule_binding_work
                        .get(&id)
                        .copied()
                        .unwrap_or_default();
                    capture.detail(DetailEvent::RuleSummary {
                        source_sequence: id.0 as u64,
                        rule: RuleRef(id.0),
                        binding_calls: binding.calls,
                        binding_ns: u64::try_from(binding.elapsed.as_nanos()).unwrap_or(u64::MAX),
                        attempts: extraction.rule_attempts.get(&id).copied().unwrap_or(0),
                        inserted: extraction.rule_insertions.get(&id).copied().unwrap_or(0),
                        elapsed_ns: extraction
                            .rule_elapsed
                            .get(&id)
                            .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)),
                    });
                }
                for event in &extraction.search_milestones.candidate_lifecycle {
                    let source_sequence = event.source_sequence;
                    if matches!(
                        event.stage,
                        crate::cascades::engine::CandidateLifecycleStage::LogicalPublished
                            | crate::cascades::engine::CandidateLifecycleStage::PhysicalRecipePublished
                    ) {
                        capture.detail(DetailEvent::Proposal {
                            source_sequence,
                            event_time_us: event.elapsed_us,
                            stage: event.stage as u8,
                            group: MemoGroupRef(event.group.0 as u64),
                            source: event.source.map(|source| LogicalExprRef(source.index() as u64)),
                            logical: event.logical.map(|logical| LogicalExprRef(logical.index() as u64)),
                            physical: event.physical.map(|physical| PhysicalExprRef(physical.index() as u64)),
                            binding: event.binding.map(|binding| BindingRef([(binding.0 >> 64) as u64, binding.0 as u64])),
                            rule: event.rule.map(|rule| RuleRef(rule.0)),
                        });
                    }
                    let goal = event.goal.map(|goal| GoalRef {
                        required: goal.required.0 as u64,
                        grant: goal.grant.stable_tag(),
                        context: goal.context.0 as u64,
                    });
                    // A goal-bearing candidate is not itself a quality
                    // decision.  The candidate event carries the goal, while
                    // quality remains represented only by the sealed Summary
                    // facts and actual handoff receipt.
                    capture.detail(DetailEvent::Candidate {
                        source_sequence,
                        event_time_us: event.elapsed_us,
                        stage: event.stage as u8,
                        group: MemoGroupRef(event.group.0 as u64),
                        goal,
                        candidate: event
                            .candidate
                            .map(|candidate| CandidateRef(candidate.index() as u64)),
                        source: event
                            .source
                            .map(|source| LogicalExprRef(source.index() as u64)),
                        source_child: event
                            .source_child
                            .map(|child| LogicalExprRef(child.index() as u64)),
                        logical: event
                            .logical
                            .map(|logical| LogicalExprRef(logical.index() as u64)),
                        physical: event
                            .physical
                            .map(|physical| PhysicalExprRef(physical.index() as u64)),
                        recipe: event.recipe.map(|recipe| {
                            FingerprintRef([(recipe.0 >> 64) as u64, recipe.0 as u64])
                        }),
                        rule: event.rule.map(|rule| RuleRef(rule.0)),
                        expected_cost_bits: event.expected_cost_bits,
                        upper_cost_bits: event.upper_cost_bits,
                    });
                    for (ordinal, child) in event.children.iter().enumerate() {
                        let source_sequence = candidate_child_sequence;
                        candidate_child_sequence = candidate_child_sequence.saturating_add(1);
                        capture.detail(DetailEvent::CandidateChild {
                            source_sequence,
                            parent_event_id: event.source_sequence,
                            ordinal: u32::try_from(ordinal).unwrap_or(u32::MAX),
                            event_time_us: event.elapsed_us,
                            stage: event.stage as u8,
                            candidate: event
                                .candidate
                                .map(|candidate| CandidateRef(candidate.index() as u64)),
                            child_group: MemoGroupRef(child.group.0 as u64),
                            child_candidate: CandidateRef(child.candidate.index() as u64),
                            goal: GoalRef {
                                required: child.goal.required.0 as u64,
                                grant: child.goal.grant.stable_tag(),
                                context: child.goal.context.0 as u64,
                            },
                        });
                    }
                    for (ordinal, fact) in event.facts.iter().enumerate() {
                        let source_sequence = fact_sequence;
                        fact_sequence = fact_sequence.saturating_add(1);
                        capture.detail(DetailEvent::Fact {
                            source_sequence,
                            parent_event_id: event.source_sequence,
                            ordinal: u32::try_from(ordinal).unwrap_or(u32::MAX),
                            event_time_us: event.elapsed_us,
                            candidate: event
                                .candidate
                                .map(|candidate| CandidateRef(candidate.index() as u64)),
                            group: MemoGroupRef(fact.group.0 as u64),
                            logical_fact: FingerprintRef([
                                (fact.logical_fact_fingerprint.0 >> 64) as u64,
                                fact.logical_fact_fingerprint.0 as u64,
                            ]),
                            statistics_snapshot: FingerprintRef([
                                (fact.statistics_snapshot_fingerprint.0 >> 64) as u64,
                                fact.statistics_snapshot_fingerprint.0 as u64,
                            ]),
                        });
                    }
                }
                for event in &extraction.search_milestones.transformation_task_lifecycle {
                    let source_sequence = event.source_sequence;
                    capture.detail(DetailEvent::Task {
                        source_sequence,
                        event_time_us: event.last_run_us.unwrap_or_default(),
                        group: MemoGroupRef(event.group.0 as u64),
                        expression: LogicalExprRef(event.expression.index() as u64),
                        rule: RuleRef(event.rule.0),
                        first_binding: event.first_binding.map(|binding| {
                            BindingRef([(binding.0 >> 64) as u64, binding.0 as u64])
                        }),
                        first_run_us: event.first_run_us,
                        first_published_us: event.first_published_us,
                        match_count: event.match_count,
                        applicable_count: event.applicable_count,
                        published_count: event.published_count,
                        no_match_count: event.no_match_count,
                        no_output_count: event.no_output_count,
                        budget_rejected_count: event.budget_rejected_count,
                    });
                }
                for (source_sequence, variant) in extraction.variants.iter().enumerate() {
                    capture.detail(DetailEvent::Grant {
                        source_sequence: source_sequence as u64,
                        class: variant.class.0,
                        physical_fingerprint: FingerprintRef([
                            (variant.physical_fingerprint.0 >> 64) as u64,
                            variant.physical_fingerprint.0 as u64,
                        ]),
                        expected_cost_bits: variant.cost.score.range.expected.to_bits(),
                        max_parallel_tasks: variant.cost.max_parallel_tasks,
                    });
                }
                let quality = &extraction.quality_last_evaluation;
                let (quality_candidate, quality_goal) = extraction.quality_last_evaluation_identity;
                let quality_event_time_us = extraction
                    .search_milestones
                    .quality_policy_satisfied_us
                    .or(extraction.search_stop.actual_stop_us)
                    .unwrap_or_default();
                capture.detail(DetailEvent::Quality {
                    // The quality producer emits one final snapshot for this
                    // compile.  Its sequence is local to this snapshot
                    // stream, not the renderer's category traversal order.
                    source_sequence: 0,
                    event_time_us: quality_event_time_us,
                    candidate: quality_candidate
                        .map(|candidate| CandidateRef(candidate.index() as u64)),
                    goal: quality_goal.map(|goal| GoalRef {
                        required: goal.required.0 as u64,
                        grant: goal.grant.stable_tag(),
                        context: goal.context.0 as u64,
                    }),
                    completed: quality.completed,
                    not_applicable: quality.not_applicable,
                    missing_evidence: quality.missing_evidence,
                    suspended: quality.suspended,
                    missing_facts: quality.missing_facts,
                    missing_bundles: quality
                        .missing_bundles
                        .iter()
                        .map(|bundle| bundle.0)
                        .collect(),
                    missing_fact_kinds: quality
                        .missing_fact_kinds
                        .iter()
                        .map(|fact| fact.stable_tag())
                        .collect(),
                    policy_satisfied: matches!(
                        extraction.quality_policy_status,
                        crate::cascades::quality::QualityPolicyStatus::Satisfied(_)
                    ),
                });
                capture.detail(DetailEvent::Search {
                    source_sequence: 0,
                    groups: extraction.search_summary.groups,
                    logical_expressions: extraction.search_summary.logical_expressions,
                    physical_expressions: extraction.search_summary.physical_expressions,
                    obligations: extraction.search_summary.obligations.len() as u64,
                    stop: match extraction.search_stop.reason {
                        crate::cascades::engine::SearchStopReason::Complete => {
                            paro_context::compile_diagnostics::SearchStop::Complete
                        }
                        crate::cascades::engine::SearchStopReason::SearchIncomplete => {
                            paro_context::compile_diagnostics::SearchStop::Incomplete
                        }
                        crate::cascades::engine::SearchStopReason::Deadline => {
                            paro_context::compile_diagnostics::SearchStop::Deadline
                        }
                        crate::cascades::engine::SearchStopReason::BudgetLimited => {
                            paro_context::compile_diagnostics::SearchStop::BudgetLimited
                        }
                        crate::cascades::engine::SearchStopReason::RuleFailure => {
                            paro_context::compile_diagnostics::SearchStop::RuleFailure
                        }
                        crate::cascades::engine::SearchStopReason::QualityPolicySatisfied => {
                            paro_context::compile_diagnostics::SearchStop::QualityPolicySatisfied
                        }
                    },
                });
            }
        }
        if paro_context::compile_work_evidence_enabled() {
            self.compile_work.rule_elapsed_us = extraction
                .rule_elapsed
                .values()
                .map(|duration| u64::try_from(duration.as_micros()).unwrap_or(u64::MAX))
                .fold(0u64, u64::saturating_add);
            self.compile_work.child_combination_cost_synthesis_count = extraction
                .search_summary
                .work_counters
                .get("child_combination_cost_synthesis_count")
                .copied()
                .unwrap_or(0);
        }
        if let Some(trace) = self.ctx.session.statement_trace() {
            let summary = &extraction.search_summary;
            for (variant_ordinal, variant) in extraction.variants.iter().enumerate() {
                record_variant_evidence(
                    trace.as_ref(),
                    &format!("final_winner_{variant_ordinal}"),
                    variant,
                );
            }
            trace.record_value("optimizer", "memo_group_count", summary.groups);
            trace.record_value(
                "optimizer",
                "memo_logical_expression_count",
                summary.logical_expressions,
            );
            trace.record_value(
                "optimizer",
                "memo_physical_expression_count",
                summary.physical_expressions,
            );
            trace.record_value(
                "optimizer",
                "transformation_apply_attempt_count",
                extraction.rule_attempts.values().copied().sum(),
            );
            trace.record_value(
                "optimizer",
                "transformation_inserted_count",
                extraction.rule_insertions.values().copied().sum(),
            );
            trace.record_value(
                "optimizer",
                "transformation_allocated_bytes",
                extraction.rule_allocated_bytes.values().copied().sum(),
            );
            trace.record_value(
                "optimizer",
                "transformation_budget_exhaustion_count",
                extraction.rule_budget_exhaustions.values().copied().sum(),
            );
            for (name, count) in &summary.work_counters {
                trace.record_value("optimizer", name, *count);
            }
            for (rule, profile) in &extraction.rule_work_profile {
                let rule_name = crate::cascades::rules::transformation_rule_name(*rule)
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("unknown_rule_{}", rule.0));
                for (guard, count) in profile.rejection_guards.iter() {
                    if count != 0 {
                        let event = format!("rule.{rule_name}.rejection_guard.{}", guard.name());
                        trace.record_value("optimizer", &event, count);
                    }
                }
                for (phase, count) in [
                    ("discovered", profile.discovered),
                    ("matched", profile.matched),
                    ("applicable", profile.applicable),
                    ("constructed", profile.constructed),
                    ("published", profile.published),
                    ("rejected", profile.rejected),
                    ("ineffective", profile.ineffective),
                    ("root_consumed", profile.root_consumed),
                ] {
                    let event = format!("rule.{rule_name}.{phase}");
                    trace.record_value("optimizer", &event, count);
                }
                for (phase, elapsed) in [
                    ("first_enqueued_us", profile.first_enqueued_us),
                    (
                        "first_dependencies_ready_us",
                        profile.first_dependencies_ready_us,
                    ),
                    ("first_run_us", profile.first_run_us),
                    ("first_discovered_us", profile.first_discovered_us),
                    ("first_matched_us", profile.first_matched_us),
                    ("first_applicable_us", profile.first_applicable_us),
                    ("first_published_us", profile.first_published_us),
                ] {
                    if let Some(elapsed) = elapsed {
                        let event = format!("rule.{rule_name}.{phase}");
                        trace.record_value("optimizer", &event, elapsed);
                    }
                }
                if let Some(elapsed) = extraction.rule_elapsed.get(rule) {
                    let event = format!("rule.{rule_name}.elapsed_us");
                    trace.record_value(
                        "optimizer",
                        &event,
                        u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
                    );
                }
            }
            for (name, elapsed) in [
                ("first_safe_us", extraction.search_milestones.first_safe_us),
                (
                    "first_optional_ready_us",
                    extraction.search_milestones.first_optional_ready_us,
                ),
                (
                    "first_optional_selected_us",
                    extraction.search_milestones.first_optional_selected_us,
                ),
                (
                    "first_logical_publication_us",
                    extraction.search_milestones.first_logical_publication_us,
                ),
                (
                    "quality_policy_satisfied_us",
                    extraction.search_milestones.quality_policy_satisfied_us,
                ),
            ] {
                if let Some(elapsed) = elapsed {
                    trace.record_value("optimizer", name, elapsed);
                }
            }
            for (name, candidate) in [
                (
                    "first_safe_candidate",
                    extraction.search_milestones.safe_candidate,
                ),
                (
                    "first_optional_ready_candidate",
                    extraction.search_milestones.optional_ready_candidate,
                ),
                (
                    "first_optional_selected_candidate",
                    extraction.search_milestones.optional_selected_candidate,
                ),
                (
                    "quality_policy_candidate",
                    extraction.search_milestones.quality_policy_candidate,
                ),
            ] {
                if let Some(candidate) = candidate {
                    trace.record_value("optimizer", name, candidate.index() as u64);
                }
            }
            trace.record_value(
                "optimizer",
                "candidate_lifecycle_event_count",
                extraction.search_milestones.candidate_lifecycle.len() as u64,
            );
            trace.record_value(
                "optimizer",
                "candidate_lifecycle_event_dropped",
                extraction.search_milestones.candidate_lifecycle_dropped,
            );
            for (stage, (stored, dropped)) in extraction
                .search_milestones
                .candidate_lifecycle_stage_stored
                .iter()
                .zip(
                    extraction
                        .search_milestones
                        .candidate_lifecycle_stage_dropped
                        .iter(),
                )
                .enumerate()
            {
                trace.record_value(
                    "optimizer",
                    &format!("candidate_lifecycle_stage_{stage}.stored"),
                    *stored,
                );
                trace.record_value(
                    "optimizer",
                    &format!("candidate_lifecycle_stage_{stage}.dropped"),
                    *dropped,
                );
            }
            for (event_index, event) in extraction
                .search_milestones
                .candidate_lifecycle
                .iter()
                .enumerate()
            {
                let prefix = format!("candidate_lifecycle_{event_index}");
                trace.record_value("optimizer", &format!("{prefix}.stage"), event.stage as u64);
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.elapsed_us"),
                    event.elapsed_us,
                );
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.group"),
                    event.group.0 as u64,
                );
                if let Some(goal) = event.goal {
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.goal_required"),
                        goal.required.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.goal_grant"),
                        goal.grant.stable_tag(),
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.goal_context"),
                        goal.context.0 as u64,
                    );
                }
                for (name, value) in [
                    (
                        "candidate",
                        event.candidate.map(|value| value.index() as u64),
                    ),
                    ("source", event.source.map(|value| value.index() as u64)),
                    (
                        "source_child",
                        event.source_child.map(|value| value.index() as u64),
                    ),
                    ("logical", event.logical.map(|value| value.index() as u64)),
                    ("physical", event.physical.map(|value| value.index() as u64)),
                    ("rule", event.rule.map(|value| value.0 as u64)),
                ] {
                    if let Some(value) = value {
                        trace.record_value("optimizer", &format!("{prefix}.{name}"), value);
                    }
                }
                if let Some(recipe) = event.recipe {
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.recipe_lo"),
                        recipe.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.recipe_hi"),
                        (recipe.0 >> 64) as u64,
                    );
                }
                if let Some(binding) = event.binding {
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.binding_lo"),
                        binding.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.binding_hi"),
                        (binding.0 >> 64) as u64,
                    );
                }
                if let Some(cost) = event.expected_cost_bits {
                    trace.record_value("optimizer", &format!("{prefix}.expected_cost_bits"), cost);
                }
                if let Some(cost) = event.upper_cost_bits {
                    trace.record_value("optimizer", &format!("{prefix}.upper_cost_bits"), cost);
                }
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.child_count"),
                    event.children.len() as u64,
                );
                for (child_index, child) in event.children.iter().enumerate() {
                    let child_prefix = format!("{prefix}.child_{child_index}");
                    trace.record_value(
                        "optimizer",
                        &format!("{child_prefix}.group"),
                        child.group.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{child_prefix}.candidate"),
                        child.candidate.index() as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{child_prefix}.goal_required"),
                        child.goal.required.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{child_prefix}.goal_grant"),
                        child.goal.grant.stable_tag(),
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{child_prefix}.goal_context"),
                        child.goal.context.0 as u64,
                    );
                }
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.fact_count"),
                    event.facts.len() as u64,
                );
                for (fact_index, fact) in event.facts.iter().enumerate() {
                    let fact_prefix = format!("{prefix}.fact_{fact_index}");
                    trace.record_value(
                        "optimizer",
                        &format!("{fact_prefix}.group"),
                        fact.group.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{fact_prefix}.logical_fact_lo"),
                        fact.logical_fact_fingerprint.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{fact_prefix}.logical_fact_hi"),
                        (fact.logical_fact_fingerprint.0 >> 64) as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{fact_prefix}.statistics_lo"),
                        fact.statistics_snapshot_fingerprint.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{fact_prefix}.statistics_hi"),
                        (fact.statistics_snapshot_fingerprint.0 >> 64) as u64,
                    );
                }
            }
            trace.record_value(
                "optimizer",
                "transformation_task_lifecycle_count",
                extraction
                    .search_milestones
                    .transformation_task_lifecycle
                    .len() as u64,
            );
            trace.record_value(
                "optimizer",
                "transformation_task_lifecycle_dropped",
                extraction
                    .search_milestones
                    .transformation_task_lifecycle_dropped,
            );
            for (task_index, task) in extraction
                .search_milestones
                .transformation_task_lifecycle
                .iter()
                .enumerate()
            {
                let prefix = format!("transformation_task_{task_index}");
                for (name, value) in [
                    ("group", Some(task.group.index() as u64)),
                    ("expression", Some(task.expression.index() as u64)),
                    ("rule", Some(task.rule.0 as u64)),
                    ("first_enqueued_us", task.first_enqueued_us),
                    ("first_run_us", task.first_run_us),
                    (
                        "first_dependencies_ready_us",
                        task.first_dependencies_ready_us,
                    ),
                    ("first_matched_us", task.first_matched_us),
                    ("first_no_match_us", task.first_no_match_us),
                    ("first_applicable_us", task.first_applicable_us),
                    ("first_published_us", task.first_published_us),
                    ("first_no_output_us", task.first_no_output_us),
                    ("first_budget_rejected_us", task.first_budget_rejected_us),
                    ("last_enqueued_us", task.last_enqueued_us),
                    (
                        "last_dependencies_ready_us",
                        task.last_dependencies_ready_us,
                    ),
                    ("last_run_us", task.last_run_us),
                    ("last_published_us", task.last_published_us),
                    (
                        "first_binding_lo",
                        task.first_binding.map(|value| value.0 as u64),
                    ),
                    (
                        "first_binding_hi",
                        task.first_binding.map(|value| (value.0 >> 64) as u64),
                    ),
                    ("match_count", Some(task.match_count)),
                    ("no_match_count", Some(task.no_match_count)),
                    ("applicable_count", Some(task.applicable_count)),
                    ("no_output_count", Some(task.no_output_count)),
                    ("published_count", Some(task.published_count)),
                    ("budget_rejected_count", Some(task.budget_rejected_count)),
                ] {
                    if let Some(value) = value {
                        trace.record_value("optimizer", &format!("{prefix}.{name}"), value);
                    }
                }
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.read_count"),
                    task.last_reads.len() as u64,
                );
                for (read_index, read) in task.last_reads.iter().enumerate() {
                    let read_prefix = format!("{prefix}.read_{read_index}");
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.group"),
                        read.group.index() as u64,
                    );
                    if let Some(revision) = read.logical_frontier_revision {
                        trace.record_value(
                            "optimizer",
                            &format!("{read_prefix}.logical_frontier_revision"),
                            revision,
                        );
                    }
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.logical_fact_lo"),
                        read.logical_fact_fingerprint.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.logical_fact_hi"),
                        (read.logical_fact_fingerprint.0 >> 64) as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.statistics_lo"),
                        read.statistics_snapshot_fingerprint.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.statistics_hi"),
                        (read.statistics_snapshot_fingerprint.0 >> 64) as u64,
                    );
                }
            }
            for checkpoint in &extraction.search_milestones.search_checkpoints {
                let goal = checkpoint.goal;
                let prefix = format!(
                    "search_checkpoint_{}ms_required{}_grant{}_row{}_objective{}_context{}",
                    checkpoint.target_ms,
                    goal.required.0,
                    goal.grant.stable_tag(),
                    goal.row_goal.stable_tag(),
                    goal.objective.stable_tag(),
                    goal.context.0,
                );
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.observed_us"),
                    checkpoint.observed_us,
                );
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.has_candidate"),
                    u64::from(checkpoint.candidate.is_some()),
                );
                if let Some(candidate) = checkpoint.candidate {
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.candidate"),
                        candidate.index() as u64,
                    );
                }
                for (name, cost) in [
                    ("expected_cost_bits", checkpoint.expected_cost),
                    ("risk_adjusted_cost_bits", checkpoint.risk_adjusted_cost),
                    ("upper_cost_bits", checkpoint.upper_cost),
                ] {
                    if let Some(cost) = cost {
                        trace.record_value(
                            "optimizer",
                            &format!("{prefix}.{name}"),
                            cost.to_bits(),
                        );
                    }
                }
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.search_complete"),
                    u64::from(checkpoint.search_complete),
                );
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.frozen"),
                    u64::from(checkpoint.frozen),
                );
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.choice_count"),
                    checkpoint.choices.len() as u64,
                );
                for (choice_index, choice) in checkpoint.choices.iter().enumerate() {
                    let choice_prefix = format!("{prefix}.choice_{choice_index}");
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.group"),
                        choice.reference.group.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.candidate"),
                        choice.reference.candidate.index() as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.goal_required"),
                        choice.reference.goal.required.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.goal_grant"),
                        choice.reference.goal.grant.stable_tag(),
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.goal_context"),
                        choice.reference.goal.context.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.logical"),
                        choice.logical.index() as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.physical"),
                        choice.physical.index() as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.logical_payload"),
                        choice.logical_payload as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.physical_payload"),
                        choice.physical_payload as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.physical_fingerprint_lo"),
                        choice.physical_fingerprint.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.physical_fingerprint_hi"),
                        (choice.physical_fingerprint.0 >> 64) as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.child_count"),
                        choice.children.len() as u64,
                    );
                    for (child_index, child) in choice.children.iter().enumerate() {
                        let child_prefix = format!("{choice_prefix}.child_{child_index}");
                        trace.record_value(
                            "optimizer",
                            &format!("{child_prefix}.group"),
                            child.group.0 as u64,
                        );
                        trace.record_value(
                            "optimizer",
                            &format!("{child_prefix}.candidate"),
                            child.candidate.index() as u64,
                        );
                        trace.record_value(
                            "optimizer",
                            &format!("{child_prefix}.goal_required"),
                            child.goal.required.0 as u64,
                        );
                        trace.record_value(
                            "optimizer",
                            &format!("{child_prefix}.goal_grant"),
                            child.goal.grant.stable_tag(),
                        );
                        trace.record_value(
                            "optimizer",
                            &format!("{child_prefix}.goal_context"),
                            child.goal.context.0 as u64,
                        );
                    }
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.rule_count"),
                        choice.rules.len() as u64,
                    );
                    for (rule_index, rule) in choice.rules.iter().enumerate() {
                        trace.record_value(
                            "optimizer",
                            &format!("{choice_prefix}.rule_{rule_index}"),
                            rule.0 as u64,
                        );
                    }
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.selected_rule_count"),
                        choice.selected_rules.len() as u64,
                    );
                    for (rule_index, rule) in choice.selected_rules.iter().enumerate() {
                        trace.record_value(
                            "optimizer",
                            &format!("{choice_prefix}.selected_rule_{rule_index}"),
                            rule.0 as u64,
                        );
                    }
                }
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.fact_read_count"),
                    checkpoint.fact_reads.len() as u64,
                );
                for (read_index, read) in checkpoint.fact_reads.iter().enumerate() {
                    let read_prefix = format!("{prefix}.fact_read_{read_index}");
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.group"),
                        read.group.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.logical_frontier_present"),
                        u64::from(read.logical_frontier_revision.is_some()),
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.physical_frontier_present"),
                        u64::from(read.physical_frontier_revision.is_some()),
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.logical_fact_lo"),
                        read.logical_fact_fingerprint.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.logical_fact_hi"),
                        (read.logical_fact_fingerprint.0 >> 64) as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.statistics_lo"),
                        read.statistics_snapshot_fingerprint.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.statistics_hi"),
                        (read.statistics_snapshot_fingerprint.0 >> 64) as u64,
                    );
                }
            }
            trace.record_event(
                "optimizer",
                if summary.is_complete() {
                    "search_complete"
                } else {
                    "search_incomplete"
                },
            );
        }
        // All capture/trace readers have finished. Transfer diagnostics once;
        // publishing the session summary must not clone the retained matrix.
        self.ctx
            .profiler
            .record_rule_attempts(extraction.rule_attempts);
        self.ctx
            .profiler
            .record_rule_insertions(extraction.rule_insertions);
        self.ctx
            .profiler
            .record_rule_elapsed(extraction.rule_elapsed);
        self.ctx
            .profiler
            .record_rule_allocated_bytes(extraction.rule_allocated_bytes);
        self.ctx
            .profiler
            .record_rule_budget_exhaustions(extraction.rule_budget_exhaustions);
        self.ctx
            .profiler
            .record_search_summary(extraction.search_summary);
        self.ctx.profiler.record(
            match mode {
                crate::cascades::SearchMode::Direct => OptimizerComponent::DirectPhysicalSearch,
                crate::cascades::SearchMode::Memo | crate::cascades::SearchMode::Regional => {
                    OptimizerComponent::MemoExploration
                }
            },
            search_phase_started.elapsed(),
        );
        self.ctx.profiler.record_component_allocation(
            match mode {
                crate::cascades::SearchMode::Direct => OptimizerComponent::DirectPhysicalSearch,
                crate::cascades::SearchMode::Memo | crate::cascades::SearchMode::Regional => {
                    OptimizerComponent::MemoExploration
                }
            },
            paro_common::allocator::allocated_bytes_since(search_phase_allocated),
        );
        // Selected occurrences were locally verified during their post-order
        // construction; their child schemas and scalar slots are immutable.
        // That work is included in PhysicalExtraction, not an empty timer
        // labelled as a second independent verification pass.
        let phase_started = Instant::now();
        let phase_allocated = paro_common::allocator::thread_allocated_bytes();
        let mut variants = extraction.variants.into_vec();
        for variant in &mut variants {
            self.attach_statement_layer(variant, &statement_layer)?;
        }
        let analyze_spec = explain.as_ref().and_then(|explain| {
            (explain.spec.mode == paro_planner::operator::ExplainMode::Analyze)
                .then_some(explain.spec)
        });
        let result = if let Some(spec) = analyze_spec {
            OptimizedStatement::ExplainAnalyze {
                target: self.extract_physical(variants, &grant_classes, extraction.grant_search)?,
                spec,
            }
        } else {
            if let Some(explain) = &explain {
                for variant in &mut variants {
                    self.attach_explain_layer(variant, explain)?;
                }
            }
            OptimizedStatement::Physical(self.extract_physical(
                variants,
                &grant_classes,
                extraction.grant_search,
            )?)
        };
        self.ctx.profiler.record(
            OptimizerComponent::PhysicalExtraction,
            phase_started.elapsed(),
        );
        self.ctx.profiler.record_component_allocation(
            OptimizerComponent::PhysicalExtraction,
            paro_common::allocator::allocated_bytes_since(phase_allocated),
        );
        if !observes_optimizer_diagnostics {
            publish_optimizer_profile_snapshot(
                self.ctx.session.diagnostics.as_ref(),
                std::mem::take(&mut self.ctx.profiler).into_snapshot(),
            );
        }
        debug!(
            target: targets::OPTIMIZER,
            ?mode,
            elapsed_ms = started_at.elapsed().as_millis(),
            "bounded optimizer completed"
        );
        Ok(result)
    }
}
