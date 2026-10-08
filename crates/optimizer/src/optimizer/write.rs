// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Statement effects attach after the relation is selected. A self-reading
//! mutation consumes a real materialized input, never a claimed snapshot proof.

use crate::optimizer::{statement_contract, statement_fingerprint, Optimizer};
use crate::physical::requirements::MutationSafetyRequirement;
use crate::physical::{choose, PhysicalImplementationFlavor};
use crate::physical::{
    MutationBarrierContract, MutationBarrierId, MutationBarriers, StatementWriteContracts,
};
use crate::statement::QueryStatementLayer;
use paro_common::error::{self as paro_error, Result};
use paro_planner::logical::plan::OwnedLogicalPlan;
use paro_planner::physical::cost::CompactRange;
use paro_planner::physical::{PlanOrigin, ResourceGrantClass};
use std::sync::Arc;

impl Optimizer {
    pub(super) fn attach_statement(
        &self,
        plan: OwnedLogicalPlan,
        layer: &QueryStatementLayer,
        grant: ResourceGrantClass,
        selection: &mut choose::PhysicalSelection,
    ) -> Result<(OwnedLogicalPlan, MutationBarriers, StatementWriteContracts)> {
        let mut barriers = MutationBarriers::default();
        let mut writes = StatementWriteContracts::default();
        if matches!(layer, QueryStatementLayer::Query) {
            return Ok((plan, barriers, writes));
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
                let barrier = MutationBarrierId(0);
                let phase = crate::cost::materialization::mutation_input(
                    CompactRange::new(
                        estimate.min as f64,
                        estimate.expected as f64,
                        estimate.max as f64,
                    )?,
                    crate::physical::implementation::planner_row_width(&plan, Default::default()),
                    grant
                        .hard_memory_bytes
                        .saturating_sub(child.cost.minimum_memory_bytes),
                    grant.max_parallel_tasks,
                    &self.calibration,
                )?
                .ok_or_else(|| {
                    paro_error::invalid_input("mutation input spool exceeds the memory envelope")
                })?;
                child.required.mutation_safety = write.mutation_safety.clone();
                child.provided = crate::physical::mutation::materialize_input(
                    child.provided,
                    &child.required,
                    barrier,
                )?;
                child.cost = child.cost.sequential(phase)?;
                child.origin = PlanOrigin::Enforcer(crate::physical::mutation::identity(barrier));
                child.region_owner = None;
                child.owned_artifacts = Box::new([]);
                child.implementation = PhysicalImplementationFlavor::Structural;
                Arc::make_mut(&mut barriers).insert(
                    plan.id,
                    MutationBarrierContract {
                        barrier,
                        targets: targets.clone(),
                        snapshot: *snapshot,
                        implementation: child.clone(),
                    },
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
        Ok((statement, barriers, writes))
    }
}
