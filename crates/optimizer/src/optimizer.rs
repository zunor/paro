// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Long-term optimizer entry point: semantic normalization, bounded search,
//! verified extraction. It has no pass-disable compatibility surface.

use std::sync::Arc;
use std::time::Instant;

use crate::physical::requirements::{
    ProvidedMaterialization, ProvidedMutationSafety, ProvidedOrdering, ProvidedPartitioning,
    ProvidedReplayability, ProvidedRepresentation, ResultGuarantee,
};
use crate::physical::{
    CompiledPhysicalPlan, Fingerprint, PhysicalGrantContract, PlanOrigin, ProvidedProperties,
    RequiredProperties, ResourceGrantClass, ResourceGrantClassId, SpillPolicy,
    StableFingerprintBuilder,
};
use paro_catalog::entry::CatalogEntry;
use paro_common::error::Result;
use paro_common::identity::GraphId;
use paro_common::logging::targets;
use paro_context::{CompileReceiptSummary, StatementContext};
use paro_planner::binder::deep_copy::duplicate_plan_preserving_indices;
use paro_planner::binder::Binder;
use paro_planner::logical::operator::LogicalOperator;
use paro_planner::logical::plan::OwnedLogicalPlan;
use tracing::debug;

use crate::context::OptimizationContext;
use crate::diagnostics::profile::publish_optimizer_profile_snapshot;
use crate::physical::{
    ImplementationContract, PhysicalBuildContext, PhysicalImplementationFlavor, PhysicalPlanBuilder,
};
use crate::statement::{QueryStatementLayer, StatementBody, StatementPlan};

use crate::cost::calibration::{LocalOperatorWork, MachineCalibrationBundle};
use crate::physical::cost::{CompactRange, PhysicalCost};
use crate::physical::identity::OpClassId;
use crate::region::limits::RegionLimits;

use crate::diagnostics::profile::OptimizerComponent;
use crate::physical::choose;
use crate::rewrite::aggregate::distinct_decomposition;
use crate::statement::ExplainEnvelope;
use paro_common::error as paro_error;
use paro_context::compile_diagnostics::{
    work::WorkKind, DetailEvent, Observation::Observed, PlanningStatus,
};
use paro_planner::logical::operator::ExplainMode;

#[cfg(test)]
mod tests;
mod write;

pub struct Optimizer {
    binder: Binder,
    ctx: OptimizationContext,
    limits: RegionLimits,
    calibration: Arc<MachineCalibrationBundle>,
    compile_work: paro_context::CompileWork,
    compile_receipt: Option<CompileReceiptSummary>,
}

/// Complete optimizer output.  Execution receives no logical tree and makes
/// no algorithm choice; EXPLAIN ANALYZE is the sole statement-level wrapper.
#[derive(Debug)]
pub enum OptimizedStatement {
    Physical(CompiledPhysicalPlan),
    ExplainAnalyze {
        target: CompiledPhysicalPlan,
        spec: paro_planner::logical::operator::ExplainSpec,
    },
}

impl Optimizer {
    #[cfg(test)]
    pub(crate) fn prepare_region_for_test(
        mut self,
        plan: OwnedLogicalPlan,
    ) -> Result<(
        OwnedLogicalPlan,
        OptimizationContext,
        Arc<MachineCalibrationBundle>,
    )> {
        let plan = self.normalization().normalize(plan)?;
        let candidate = self.normalization().settle_relational_baseline(plan)?;
        self.ctx.column_stats = candidate.column_stats;
        Ok((candidate.plan, self.ctx, self.calibration))
    }

    pub fn new(binder: Binder, session: Arc<StatementContext>) -> Self {
        Self {
            ctx: OptimizationContext::new(session, binder.bind_context.clone()),
            binder,
            limits: RegionLimits::default(),
            calibration: Arc::new(MachineCalibrationBundle::builtin_production()),
            compile_work: Default::default(),
            compile_receipt: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_limits(mut self, limits: RegionLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn compile_work(&self) -> paro_context::CompileWork {
        self.compile_work
    }

    pub fn compile_receipt(&self) -> Option<CompileReceiptSummary> {
        self.compile_receipt
    }

    pub fn optimize(&mut self, plan: OwnedLogicalPlan) -> Result<OptimizedStatement> {
        let pre_partition =
            crate::diagnostics::work::enter(crate::diagnostics::work::Bucket::Normalization);
        // Allocation attribution is a planning concern. The global allocator
        // remains installed for observation, while counter updates are scoped
        // to this synchronous compiler operation so execution pays no tax.
        let _allocation_metrics = paro_common::allocator::begin_allocation_metrics();
        let started_at = Instant::now();
        let statement = StatementPlan::split(plan, self.ctx.session.transaction_visible_version())?;

        let (query, statement_layer) = match statement.body {
            StatementBody::Query { query, layer } => (*query, *layer),
            StatementBody::Utility(utility) => {
                let result = OptimizedStatement::Physical(
                    self.extract_utility(*utility, self.resource_grant()?)?,
                );
                publish_optimizer_profile_snapshot(
                    self.ctx.session.diagnostics.as_ref(),
                    std::mem::take(&mut self.ctx.profiler).into_snapshot(),
                );
                debug!(
                    target: targets::OPTIMIZER,
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "statement-only optimizer lowering completed"
                );
                return Ok(result);
            }
        };
        let explain = statement.explain;
        drop(pre_partition);
        self.optimize_query(query, statement_layer, explain)
    }

    fn normalization(&self) -> crate::rewrite::program::Normalization<'_> {
        crate::rewrite::program::Normalization {
            ctx: &self.ctx,
            binder: &self.binder,
        }
    }

    fn resource_grant(&self) -> Result<ResourceGrantClass> {
        let resources = self
            .ctx
            .session
            .compile_resources
            .expected_grant(
                self.ctx.session.limits.max_memory,
                self.ctx.session.limits.max_threads,
            )
            .ok_or_else(|| {
                paro_error::configuration_limit_exceeded(
                    "no resource envelope is available for compilation",
                )
            })?;
        Ok(ResourceGrantClass {
            id: ResourceGrantClassId(0),
            hard_memory_bytes: resources.hard_memory_bytes,
            spill_policy: if self.ctx.session.limits.use_temporary_directory {
                SpillPolicy::Allowed
            } else {
                SpillPolicy::Forbidden
            },
            max_parallel_tasks: resources.max_parallel_tasks,
        })
    }

    fn statement_local_cost(
        &self,
        stable_tag: u64,
        cardinality: Option<paro_planner::logical::plan::CardinalityEstimate>,
    ) -> Result<PhysicalCost> {
        let rows = match cardinality {
            Some(cardinality) => CompactRange::new(
                cardinality.min as f64,
                cardinality.expected as f64,
                cardinality.max as f64,
            )?,
            None => CompactRange::point(1.0)?,
        };
        let mut work = LocalOperatorWork::default();
        work.add(OpClassId(1_000 + stable_tag as u32), rows)?;
        self.calibration.fold(&work)
    }

    fn extract_utility(
        &self,
        utility: OwnedLogicalPlan,
        grant: ResourceGrantClass,
    ) -> Result<CompiledPhysicalPlan> {
        let mut logical =
            duplicate_plan_preserving_indices(&utility, self.binder.bind_context.shared().as_ref());
        crate::physical::slot_assignment::assign_expression_slots(&mut logical.operator)?;
        let plan = PhysicalPlanBuilder::new(PhysicalBuildContext {
            force_external: self.ctx.session.limits.force_external,
            grant_spill_policy: grant.spill_policy,
            rowset_scan_pushdown: self.ctx.session.limits.rowset_scan_pushdown,
            max_threads: usize::from(grant.max_parallel_tasks),
            scan_access_cost: Default::default(),
            dependency_template: self.plan_dependency_template_for(&logical)?,
        })
        .build(logical)?;
        let fingerprint =
            plan.artifact_fingerprint(utility_plan_fingerprint(&plan.node(plan.root).kind)?)?;
        CompiledPhysicalPlan::new(grant, plan, fingerprint)
    }

    fn plan_dependency_template(&self) -> crate::physical::PlanDependencies {
        fn revision(domain: &[u8], values: impl IntoIterator<Item = u64>) -> Fingerprint {
            let mut fingerprint = StableFingerprintBuilder::default();
            fingerprint.write_bytes(domain);
            for value in values {
                fingerprint.write_u64(value);
            }
            fingerprint.finish()
        }

        let mut calibration = StableFingerprintBuilder::default();
        calibration.write_bytes(b"paro.machine-calibration.v1");
        calibration.write_u64(self.calibration.revision.0 as u64);
        calibration.write_bytes(self.calibration.hardware_class.as_bytes());
        calibration.write_bytes(self.calibration.corpus_id.as_bytes());
        calibration.write_bytes(self.calibration.provenance.as_bytes());

        let config_values = [
            u64::from(self.limits.connected_pairs),
            u64::from(self.limits.exact_relations),
            u64::from(self.limits.candidates_per_subset),
            self.ctx.session.limits.max_threads as u64,
            self.ctx.session.limits.rowset_scan_pushdown as u64,
            self.ctx.session.limits.force_external as u64,
            self.ctx.session.limits.use_temporary_directory as u64,
        ];
        crate::physical::PlanDependencies {
            machine_calibration_revision: calibration.finish(),
            estimator_revision: revision(b"paro.estimator-algebra", [1]),
            rule_set_revision: revision(b"paro.rule-set", [4]),
            plan_stability_policy_revision: revision(b"paro.plan-stability-policy", [1]),
            optimizer_config_fingerprint: revision(b"paro.staged-planner-config.v1", config_values),
            physical_abi_revision: revision(b"paro.physical-abi", [4]),
            ..Default::default()
        }
    }

    fn plan_dependency_template_for<P: paro_planner::logical::plan::LogicalPlanRead>(
        &self,
        plan: &P,
    ) -> Result<crate::physical::PlanDependencies> {
        fn graph_key(id: &GraphId) -> Fingerprint {
            let mut fingerprint = StableFingerprintBuilder::default();
            fingerprint.write_bytes(b"paro.graph-generation.v1");
            fingerprint.write_bytes(id.runtime_key().as_bytes());
            fingerprint.finish()
        }

        fn collect<P: paro_planner::logical::plan::LogicalPlanRead>(
            optimizer: &Optimizer,
            plan: &P,
            dependencies: &mut crate::physical::PlanDependencies,
        ) {
            if let LogicalOperator::GraphScan(scan) = plan.operator() {
                let id = GraphId::new(
                    optimizer.ctx.session.current_database(),
                    &scan.schema_name,
                    &scan.graph_name,
                );
                if let Some(snapshot) = optimizer.ctx.session.graph_snapshot(&id) {
                    dependencies
                        .graph_generations
                        .insert(graph_key(&id), snapshot.generation_id());
                }
            }
            plan.operator()
                .visit_child_links(&mut |child| collect(optimizer, &**child, dependencies));
        }

        let mut dependencies = self.plan_dependency_template();
        collect(self, plan, &mut dependencies);
        for table in
            crate::physical::access::index::AccessPlanner::planning_observation_tables(plan)?
        {
            let object = {
                let mut fingerprint = StableFingerprintBuilder::default();
                fingerprint.write_u64(1);
                fingerprint.write_u64(table.object_id().raw());
                fingerprint.finish()
            };
            dependencies.search_planning_signatures.insert(
                object,
                table
                    .storage
                    .as_ref()
                    .map_or(0, |storage| storage.search_planning_signature()),
            );
        }
        Ok(dependencies)
    }
}

fn statement_contract(
    grant: PhysicalGrantContract,
    physical_fingerprint: Fingerprint,
    cost: PhysicalCost,
) -> ImplementationContract {
    ImplementationContract {
        required: RequiredProperties::default(),
        provided: ProvidedProperties {
            ordering: ProvidedOrdering::Unordered,
            partitioning: ProvidedPartitioning::Singleton,
            materialization: ProvidedMaterialization::default(),
            mutation_safety: ProvidedMutationSafety::NotApplicable,
            representation: ProvidedRepresentation::Flat,
            replayability: ProvidedReplayability::OnePass,
            result_guarantee: ResultGuarantee::Exact,
        },
        cost,
        grant,
        origin: PlanOrigin::StatementLowering,
        goal_fingerprint: physical_fingerprint,
        physical_fingerprint,
        implementation: PhysicalImplementationFlavor::Structural,
        region_owner: None,
        owned_artifacts: Box::new([]),
    }
}

fn statement_fingerprint(
    layer: &QueryStatementLayer,
    child: Fingerprint,
    write: Option<&crate::physical::WriteContract>,
) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.statement.query-wrapper.v1");
    fingerprint.write_u64(layer.stable_tag());
    fingerprint.write_fingerprint(child);
    if let Some(write) = write {
        fingerprint.write_u64(write.target_object_id);
        fingerprint.write_u64(write.snapshot_version);
        fingerprint.write_u64(write.modified_columns.len() as u64);
        for column in &write.modified_columns {
            fingerprint.write_u64(*column as u64);
        }
        fingerprint.write_u64(write.modified_key_columns.len() as u64);
        for column in &write.modified_key_columns {
            fingerprint.write_u64(*column as u64);
        }
        fingerprint.write_u64(match write.returning {
            crate::physical::ReturningImageContract::CountOnly => 0,
            crate::physical::ReturningImageContract::BeforeImage => 1,
            crate::physical::ReturningImageContract::AfterImage => 2,
        });
    }
    if let QueryStatementLayer::CopyTo { file_path, .. } = layer {
        fingerprint.write_bytes(file_path.as_bytes());
    }
    fingerprint.finish()
}

fn utility_plan_fingerprint(kind: &crate::physical::PhysicalNodeKind) -> Result<Fingerprint> {
    use crate::physical::{PhysicalNodeKind, UtilitySpec};

    let tag = match kind {
        PhysicalNodeKind::DummyScan(_) => 0,
        PhysicalNodeKind::Utility(utility) => match utility {
            UtilitySpec::CreateTable(_) => 1,
            UtilitySpec::CreateView(_) => 2,
            UtilitySpec::CreateSchema(_) => 3,
            UtilitySpec::CreateSequence(_) => 4,
            UtilitySpec::CreateIndex(_) => 5,
            UtilitySpec::CreateRoutine(_) => 6,
            UtilitySpec::CreatePropertyGraph(_) => 7,
            UtilitySpec::Alter(_) => 8,
            UtilitySpec::Drop(_) => 9,
            UtilitySpec::DropPropertyGraph(_) => 10,
            UtilitySpec::RefreshPropertyGraph(_) => 11,
        },
        _ => {
            return Err(paro_common::error::internal(
                "utility extraction produced a non-utility physical root",
            ));
        }
    };
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.statement.utility-plan.v2");
    fingerprint.write_u64(tag);
    Ok(fingerprint.finish())
}

impl Optimizer {
    pub(super) fn optimize_query(
        &mut self,
        query: OwnedLogicalPlan,
        layer: QueryStatementLayer,
        explain: Option<ExplainEnvelope>,
    ) -> Result<OptimizedStatement> {
        let started = Instant::now();
        let normalization_scope =
            crate::diagnostics::work::enter(crate::diagnostics::work::Bucket::Normalization);
        self.ctx.session.cancellation.check()?;
        let mut grant = self.resource_grant()?;
        let query = self.normalization().normalize(query)?;
        let (query, _) = distinct_decomposition::optimize_plan(query, &self.ctx.bind_context)?;
        let candidate = self.normalization().settle_relational_baseline(query)?;
        let plan = self.assign_node_ids(candidate.plan)?;
        self.ctx.column_stats = candidate.column_stats;
        self.ctx
            .profiler
            .record(OptimizerComponent::SemanticNormalization, started.elapsed());
        let normalization_us = started.elapsed().as_micros() as u64;
        drop(normalization_scope);
        self.record_stage(0, WorkKind::Normalization, normalization_us, 1, 0);

        let region_started = Instant::now();
        let region_scope =
            crate::diagnostics::work::enter(crate::diagnostics::work::Bucket::RegionPlanning);
        let regions = crate::region::plan(
            plan,
            &mut self.ctx,
            &self.binder,
            grant,
            &self.calibration,
            &self.limits,
        )?;
        let region_work = regions.aggregates;
        let join_work = regions.joins;
        let mut candidate = regions.relation;
        candidate.plan = self.assign_node_ids(candidate.plan)?;
        self.ctx.column_stats = candidate.column_stats;
        self.ctx.profiler.record(
            OptimizerComponent::RegionOptimization,
            region_started.elapsed(),
        );
        let regions_us = region_started.elapsed().as_micros() as u64;
        drop(region_scope);
        self.record_stage(
            1,
            WorkKind::RegionPlanning,
            regions_us,
            region_work.transitions + join_work.transitions,
            region_work.budget_fallbacks + join_work.budget_fallbacks,
        );

        let physical_started = Instant::now();
        let physical_scope =
            crate::diagnostics::work::enter(crate::diagnostics::work::Bucket::PhysicalSelection);
        let mut physical_operating_points = 0_u64;
        let mut selection = loop {
            self.ctx.session.cancellation.check()?;
            physical_operating_points += 1;
            if let Some(selected) = choose::select(
                &candidate.plan,
                &self.ctx.column_stats,
                grant,
                &self.calibration,
                &self.ctx.session,
            )? {
                break selected;
            }
            if grant.max_parallel_tasks <= 1 {
                return Err(paro_error::invalid_input(
                    "pipeline plan exceeds its executable memory envelope",
                ));
            }
            // A smaller DOP is a local executable operating point, not a
            // second logical planning pass.
            grant.max_parallel_tasks = (grant.max_parallel_tasks / 2).max(1);
        };
        self.ctx.profiler.record(
            OptimizerComponent::PhysicalSelection,
            physical_started.elapsed(),
        );
        let selection_us = physical_started.elapsed().as_micros() as u64;
        drop(physical_scope);
        self.record_stage(
            2,
            WorkKind::PhysicalSelection,
            selection_us,
            selection.alternatives,
            0,
        );
        let physical_started = Instant::now();
        let extraction_scope =
            crate::diagnostics::work::enter(crate::diagnostics::work::Bucket::PhysicalLowering);
        let (mut plan, enforcers, writes) =
            self.attach_statement(candidate.plan, &layer, grant, &mut selection)?;
        let analyze_spec = explain
            .as_ref()
            .and_then(|e| (e.spec.mode == ExplainMode::Analyze).then_some(e.spec));
        let mut contracts = selection.contracts;
        if let Some(e) = explain.filter(|_| analyze_spec.is_none()) {
            let mut contract = contracts
                .get(&plan.id)
                .ok_or_else(|| paro_error::internal("pipeline root contract is absent"))?
                .clone();
            contract.implementation = PhysicalImplementationFlavor::Structural;
            contract.region_owner = None;
            contract.origin = crate::physical::PlanOrigin::StatementLowering;
            contract.owned_artifacts = Box::new([]);
            plan = e.attach(plan);
            Arc::make_mut(&mut contracts).insert(plan.id, contract);
        }
        let dependency_template = self.plan_dependency_template_for(&plan)?;
        let physical = PhysicalPlanBuilder::new(PhysicalBuildContext {
            force_external: self.ctx.session.limits.force_external,
            grant_spill_policy: grant.spill_policy,
            rowset_scan_pushdown: self.ctx.session.limits.rowset_scan_pushdown,

            max_threads: usize::from(grant.max_parallel_tasks),
            scan_access_cost: Default::default(),
            dependency_template,
        })
        .with_implementation_contracts(contracts)
        .with_mutation_barriers(enforcers)
        .with_statement_write_contracts(writes)
        .requiring_implementation_contracts()
        .build(plan)?;
        let identity = physical
            .structural_identity_fingerprint()
            .map_err(|error| {
                paro_error::internal(format!("pipeline physical identity: {error}"))
            })?;
        let fingerprint = physical.artifact_fingerprint(identity)?;
        let artifact = CompiledPhysicalPlan::new(grant, physical, fingerprint)?;
        self.ctx.profiler.record(
            OptimizerComponent::PhysicalExtraction,
            physical_started.elapsed(),
        );
        let extraction_us = physical_started.elapsed().as_micros() as u64;
        drop(extraction_scope);
        self.record_stage(
            3,
            WorkKind::PhysicalLowering,
            extraction_us,
            selection.nodes,
            0,
        );
        self.compile_work = paro_context::CompileWork {
            optimizer_elapsed_us: started.elapsed().as_micros() as u64,
            normalization_elapsed_us: normalization_us,
            physical_alternatives: selection.alternatives,
            ..Default::default()
        };
        let budget_limited = region_work.budget_fallbacks != 0 || join_work.budget_fallbacks != 0;
        let planning_status = if budget_limited {
            PlanningStatus::PlannedWithFallback
        } else {
            PlanningStatus::Planned
        };
        self.compile_receipt = Some(CompileReceiptSummary {
            schema_version: paro_context::COMPILE_RECEIPT_SCHEMA_VERSION,
            artifact_identity: None,
            // A completed finite program is not a proof over all SQL plans.
            planning_status: Observed(planning_status),

            budget_limited: Observed(budget_limited),

            expected_class: Observed(grant.id.0),
            variant_count: Observed(1),
            omitted_variants: 0,
            compile_work: None,
        });
        if let Some(capture) = &self.ctx.session.options.compile_capture {
            capture.update(|record| {
                record.budget_limited = Observed(budget_limited);
                record.planning_status = Observed(planning_status);
                record.safety_verified = Observed(true);
            });
            capture.search_counters(std::collections::BTreeMap::from([
                ("normalization_us", normalization_us),
                ("regions_us", regions_us),
                ("selection_us", selection_us),
                ("extraction_us", extraction_us),
                ("selected_nodes", selection.nodes),
                ("physical_operating_points", physical_operating_points),
                ("local_alternatives", selection.alternatives),
                ("aggregate_decisions", region_work.regions),
                ("joint_transitions", region_work.transitions),
                ("partial_states", region_work.partial_states),
                ("selected_partial", region_work.selected_partial),
                ("response_join_regions", join_work.regions),
                ("response_join_transitions", join_work.transitions),
                (
                    "borrowed_cuts",
                    region_work.borrowed_cuts + join_work.borrowed_cuts,
                ),
                (
                    "completed_region_outputs",
                    region_work.completed_outputs + join_work.completed_outputs,
                ),
                ("response_join_fallbacks", join_work.budget_fallbacks),
                ("joint_budget_fallbacks", region_work.budget_fallbacks),
            ]));
        }
        publish_optimizer_profile_snapshot(
            self.ctx.session.diagnostics.as_ref(),
            std::mem::take(&mut self.ctx.profiler).into_snapshot(),
        );
        Ok(match analyze_spec {
            Some(spec) => OptimizedStatement::ExplainAnalyze {
                target: artifact,
                spec,
            },
            None => OptimizedStatement::Physical(artifact),
        })
    }

    fn record_stage(
        &self,
        source_sequence: u64,
        stage: WorkKind,
        elapsed_us: u64,
        items: u64,
        fallbacks: u64,
    ) {
        if let Some(capture) = &self.ctx.session.options.compile_capture {
            capture.detail(DetailEvent::Stage {
                source_sequence,
                stage,
                elapsed_ns: elapsed_us.saturating_mul(1_000),
                items,
                fallbacks,
            });
        }
    }

    /// Committed tree occurrences, not rewrite source ids, own physical
    /// contracts. Rewrites can preserve a source id on multiple replacement
    /// shells; assign unique occurrences once at each selection boundary.
    fn assign_node_ids(&self, plan: OwnedLogicalPlan) -> Result<OwnedLogicalPlan> {
        plan.try_map_post_order(|mut node| {
            node.id = self.ctx.bind_context.next_plan_id();
            Ok(node)
        })
    }
}
