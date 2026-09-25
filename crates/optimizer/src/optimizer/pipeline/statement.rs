// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Statement effects attach after the relation is selected. A self-reading
//! mutation consumes a real materialized input, never a claimed snapshot proof.

use super::*;
use crate::cascades::enforcer::EnforcerStep;
use crate::cascades::engine::{enforcer_cost, EnforcerCostInput};
use crate::physical::requirements::MutationSafetyRequirement;
use crate::physical::{
    ExtractedEnforcerContract, ExtractedEnforcerContracts, ExtractedPhysicalEnforcer,
    MutationBarrierId, StatementWriteContracts,
};

impl Optimizer {
    pub(super) fn attach_pipeline_statement(
        &self,
        plan: OwnedLogicalPlan,
        layer: &QueryStatementLayer,
        grant: ResourceGrantClass,
        selection: &mut direct::DirectSelection,
    ) -> Result<(
        OwnedLogicalPlan,
        ExtractedEnforcerContracts,
        StatementWriteContracts,
    )> {
        let mut enforcers = ExtractedEnforcerContracts::default();
        let mut writes = StatementWriteContracts::default();
        if matches!(layer, QueryStatementLayer::Query) {
            return Ok((plan, enforcers, writes));
        }
        let mut child = selection
            .contracts
            .get(&plan.id)
            .ok_or_else(|| paro_error::internal("pipeline statement lost its selected input"))?
            .clone();
        if let Some(write) = layer.write_contract() {
            if let MutationSafetyRequirement::StableReadBeforeWrite { targets, snapshot } =
                &write.mutation_safety
            {
                let estimate = plan.stats.estimated_cardinality.ok_or_else(|| {
                    paro_error::invalid_input("mutation input has no materialization estimate")
                })?;
                let step = EnforcerStep::MutationInputSpool {
                    barrier: MutationBarrierId(0),
                };
                // Reuse the common executable-enforcer cost contract. This is
                // a pure price calculation, not a call into Memo search.
                let phase = enforcer_cost(
                    std::slice::from_ref(&step),
                    EnforcerCostInput {
                        rows: CompactRange::new(
                            estimate.min as f64,
                            estimate.expected as f64,
                            estimate.max as f64,
                        )?,
                        row_width_bytes: crate::physical::implementation::planner_row_width(
                            &plan,
                            Default::default(),
                        ),
                        hard_memory_bytes: grant
                            .hard_memory_bytes
                            .saturating_sub(child.cost.minimum_memory_bytes),
                        spill_policy: grant.spill_policy,
                        max_parallel_tasks: grant.max_parallel_tasks,
                    },
                    &self.calibration,
                )?
                .ok_or_else(|| {
                    paro_error::invalid_input("mutation input spool exceeds the memory envelope")
                })?;
                child.required.mutation_safety = write.mutation_safety.clone();
                child.provided = step.apply(child.provided, &child.required)?;
                child.cost = phase.compose_after(child.cost)?;
                child.origin = PlanOrigin::Enforcer(step.stable_fingerprint());
                child.region_owner = None;
                child.owned_artifacts = Box::new([]);
                child.implementation = PhysicalImplementationFlavor::Structural;
                Arc::make_mut(&mut enforcers).insert(
                    plan.id,
                    Box::new([ExtractedEnforcerContract {
                        enforcer: ExtractedPhysicalEnforcer::MutationInputSpool {
                            barrier: MutationBarrierId(0),
                            targets: targets.clone(),
                            snapshot: *snapshot,
                        },
                        contract: child.clone(),
                    }]),
                );
            }
        }
        let estimate = plan.stats.estimated_cardinality;
        let mut statement = layer.attach(plan);
        if let Some(rows) = layer.result_cardinality() {
            statement.stats.estimated_cardinality = Some(rows);
        }
        let cost = child
            .cost
            .sequential(self.statement_local_cost(layer.stable_tag(), estimate)?)?;
        let fingerprint =
            statement_fingerprint(layer, child.physical_fingerprint, layer.write_contract());
        Arc::make_mut(&mut selection.contracts).insert(
            statement.id,
            statement_contract(child.grant, fingerprint, cost),
        );
        if let Some(write) = layer.write_contract() {
            Arc::make_mut(&mut writes).insert(statement.id, write.clone());
        }
        selection.cost = cost;
        Ok((statement, enforcers, writes))
    }
}
