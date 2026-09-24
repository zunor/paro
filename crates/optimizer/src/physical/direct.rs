// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bottom-up physical selection over one committed relation tree.
//! Alternatives live only for the current operator; there is no Memo, goal
//! registry, immutable candidate archive, or grant-portfolio cross product.

use std::collections::HashMap;
use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_planner::operator::ColumnBinding;
use paro_planner::plan::{OwnedLogicalPlan, PlanNodeId};
use paro_storage::statistics::ColumnStatistics;

use super::implementation::*;
use super::local_cost::*;
use super::*;
use crate::cascades::calibration::MachineCalibrationBundle;
use crate::cascades::rules::GrantDependencyDescriptor;
use crate::cascades::scalar_lowering::BindingCatalog;

pub(crate) struct DirectSelection {
    pub contracts: WinnerPhysicalContracts,
    pub cost: SearchCost,
    pub nodes: u64,
    pub alternatives: u64,
}

struct Completed {
    cost: SearchCost,
    hard_rows: Option<u64>,
    result: requirements::ResultGuarantee,
}

/// Memory floors are conservatively simultaneous. Elastic state shares one
/// admitted query pool; only independently spillable/bounded local contracts
/// may reach this composition. This is not a search lower-bound calculation.
fn compose(
    local: SearchCost,
    children: &[&Completed],
    grant: ResourceGrantClass,
) -> Result<Option<SearchCost>> {
    let mut cost = local;
    for child in children {
        let floor = cost
            .minimum_memory_bytes
            .checked_add(child.cost.minimum_memory_bytes)
            .ok_or_else(|| paro_error::internal("pipeline memory floor overflow"))?;
        let peak = cost
            .peak_memory_upper
            .saturating_add(child.cost.peak_memory_upper);
        let retained = cost
            .non_revocable_memory_upper
            .saturating_add(child.cost.non_revocable_memory_upper);
        let preferred = cost
            .preferred_memory_bytes()
            .saturating_add(child.cost.preferred_memory_bytes());
        let completion = cost.memory_completion.overlapping(
            cost.peak_memory_upper,
            child.cost.memory_completion,
            child.cost.peak_memory_upper,
        );
        cost = cost.sequential(child.cost)?;
        if floor > grant.hard_memory_bytes
            || (!completion.is_runtime_capped() && retained > grant.hard_memory_bytes)
            || (grant.spill_policy == SpillPolicy::Forbidden && peak > grant.hard_memory_bytes)
        {
            return Ok(None);
        }
        cost.minimum_memory_bytes = floor;
        cost.peak_memory_upper = peak.min(grant.hard_memory_bytes).max(floor);
        cost.non_revocable_memory_upper = retained;
        cost.revocable_memory_target = preferred.min(cost.peak_memory_upper).saturating_sub(floor);
        cost.memory_completion = completion;
        if completion.is_runtime_capped() {
            cost.apply_runtime_cap(grant.hard_memory_bytes, floor)?;
        }
        cost.validate()?;
    }
    Ok(Some(cost))
}

pub(crate) fn select(
    root: &OwnedLogicalPlan,
    statistics: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    grant: ResourceGrantClass,
    calibration: &MachineCalibrationBundle,
    session: &paro_context::StatementContext,
) -> Result<Option<DirectSelection>> {
    use PhysicalImplementationFlavor as F;
    let mut pending = vec![(root, false)];
    let mut completed = HashMap::<PlanNodeId, Completed>::new();
    let mut contracts = HashMap::new();
    let mut alternatives = 0;
    let bindings = BindingCatalog::default();
    while let Some((plan, visited)) = pending.pop() {
        session.cancellation.check()?;
        if !visited {
            pending.push((plan, true));
            pending.extend(
                plan.children()
                    .into_iter()
                    .rev()
                    .map(|child| (child, false)),
            );
            continue;
        }
        let children = plan
            .children()
            .into_iter()
            .map(|child| {
                completed
                    .get(&child.id)
                    .ok_or_else(|| paro_error::internal("pipeline child was not selected"))
            })
            .collect::<Result<Vec<_>>>()?;
        let bounds = children
            .iter()
            .map(|c| c.hard_rows)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let template = planner_cost_facts(plan, statistics, &bindings, Default::default())?;
        let facts = selected_cost_facts(plan, &template, bounds)?;
        let implementations = planner_implementation_set(plan, session.limits.rowset_scan_pushdown);
        let model = LocalCostModel {
            operator_type: plan.operator.op_type(),
            local_cost: planner_operator_cost(
                plan,
                children.len(),
                facts.output_rows_hard_upper,
                &facts.child_rows_hard_upper,
                Default::default(),
            )?,
            baseline: implementations.baseline,
            resource_sensitive: planner_grant_dependency(&plan.operator)
                == GrantDependencyDescriptor::Sensitive,
            spillable: planner_operator_spillable(&plan.operator),
            perfect_hash: facts.perfect_hash,
            runtime_filter_key_types: &facts.runtime_filter_key_types,
        };
        let mut best: Option<(F, SearchCost)> = None;
        for flavor in std::iter::once(implementations.baseline).chain(
            [
                F::PerfectHashAggregate,
                F::SingletonAggregateProjection,
                F::HashJoinRuntimeFilter,
                F::HashJoinBuildLeft,
                F::HashJoinBuildLeftRuntimeFilter,
                F::SortRangeJoin,
                F::ClassicIeJoin,
                F::PartitionAggregateWindow,
                F::CrossProductExternal,
            ]
            .into_iter()
            .filter(|f| implementations.supports(*f)),
        ) {
            alternatives += 1;
            let local = implementation_cost(
                &model,
                &facts,
                flavor,
                calibration,
                grant.max_parallel_tasks,
            )?;
            let Some(mut local) = fit_local_cost(
                local,
                flavor_spillable(&model, flavor),
                grant,
                session.limits.force_external,
            )?
            else {
                continue;
            };
            local.max_parallel_tasks = grant.max_parallel_tasks;
            local.output_pipeline_tasks = useful_output_tasks(&facts, grant.max_parallel_tasks);
            let Some(cost) = compose(local, &children, grant)? else {
                continue;
            };
            if best.as_ref().is_none_or(|(_, previous)| {
                ObjectiveProfile::Latency.compare(&cost, previous).is_lt()
            }) {
                best = Some((flavor, cost));
            }
        }
        let Some((implementation, cost)) = best else {
            return Ok(None);
        };
        // This identity is local to the selected tree, not a cross-run plan
        // digest. The physical canonical encoder owns the latter boundary.
        let mut identity = StableFingerprintBuilder::default();
        identity.write_bytes(b"paro.pipeline.selected-node.v1");
        identity.write_u64(contracts.len() as u64);
        let identity = identity.finish();
        let rf = matches!(
            implementation,
            F::HashJoinRuntimeFilter | F::HashJoinBuildLeftRuntimeFilter
        );
        let result = children
            .iter()
            .map(|child| child.result)
            .chain(std::iter::once(operator_result_guarantee(&plan.operator)))
            .find(|result| matches!(result, requirements::ResultGuarantee::ApproximateAllowed(_)))
            .unwrap_or(requirements::ResultGuarantee::Exact);
        let provided = ProvidedProperties {
            ordering: requirements::ProvidedOrdering::Unordered,
            partitioning: requirements::ProvidedPartitioning::Singleton,
            materialization: Default::default(),
            replayability: requirements::ProvidedReplayability::OnePass,
            representation: requirements::ProvidedRepresentation::Flat,
            mutation_safety: requirements::ProvidedMutationSafety::NotApplicable,
            result_guarantee: result,
        };
        let contract = WinnerPhysicalContract {
            required: RequiredProperties {
                result_guarantee: result,
                ..Default::default()
            },
            provided,
            cost,
            grant: PhysicalGrantContract::Class(grant.id),
            origin: if rf {
                PlanOrigin::SpecializedRegion(identity)
            } else {
                PlanOrigin::Direct
            },
            goal_fingerprint: identity,
            physical_fingerprint: identity,
            implementation,
            region_owner: rf.then_some(identity),
            owned_artifacts: if rf {
                Box::new([OwnedAuxiliaryArtifact {
                    fingerprint: identity,
                    kind: AuxiliaryArtifactKind::RuntimeFilter,
                }])
            } else {
                Box::new([])
            },
        };
        if contracts.insert(plan.id, contract).is_some() {
            return Err(paro_error::internal(
                "pipeline relation tree contains duplicate node ids",
            ));
        }
        completed.insert(
            plan.id,
            Completed {
                cost,
                hard_rows: facts.output_rows_hard_upper,
                result,
            },
        );
    }
    let cost = completed
        .get(&root.id)
        .ok_or_else(|| paro_error::internal("pipeline root is absent"))?
        .cost;
    Ok(Some(DirectSelection {
        nodes: contracts.len() as u64,
        contracts: Arc::new(contracts),
        cost,
        alternatives,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(spill_policy: SpillPolicy) -> ResourceGrantClass {
        ResourceGrantClass {
            id: ResourceGrantClassId(0),
            hard_memory_bytes: 100,
            spill_policy,
            max_parallel_tasks: 1,
        }
    }

    fn memory(floor: u64, peak: u64) -> SearchCost {
        SearchCost {
            minimum_memory_bytes: floor,
            non_revocable_memory_upper: floor,
            peak_memory_upper: peak,
            revocable_memory_target: peak - floor,
            ..SearchCost::ZERO
        }
    }

    fn child(cost: SearchCost) -> Completed {
        Completed {
            cost,
            hard_rows: None,
            result: requirements::ResultGuarantee::Exact,
        }
    }

    #[test]
    fn overlapping_floors_cannot_be_clamped_into_feasibility() {
        assert!(compose(
            memory(60, 80),
            &[&child(memory(50, 80))],
            grant(SpillPolicy::Allowed)
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn elastic_states_share_admitted_pool_but_no_spill_cannot() {
        let cost = compose(
            memory(20, 80),
            &[&child(memory(20, 80))],
            grant(SpillPolicy::Allowed),
        )
        .unwrap()
        .unwrap();
        assert_eq!(cost.minimum_memory_bytes, 40);
        assert_eq!(cost.peak_memory_upper, 100);
        cost.validate().unwrap();
        assert!(compose(
            memory(20, 80),
            &[&child(memory(20, 80))],
            grant(SpillPolicy::Forbidden)
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn runtime_cap_does_not_become_completion_proof() {
        let mut cost = memory(10, 100);
        cost.memory_completion = MemoryCompletion::runtime_capped_unbounded();
        cost.non_revocable_memory_upper = 100;
        let cost = compose(cost, &[&child(memory(10, 30))], grant(SpillPolicy::Allowed))
            .unwrap()
            .unwrap();
        assert!(cost.memory_completion.is_runtime_capped());
        assert_eq!(cost.minimum_memory_bytes, 20);
        assert_eq!(cost.peak_memory_upper, 100);
    }
}
