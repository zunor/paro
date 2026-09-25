// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bottom-up physical selection over one committed relation tree.
//! Alternatives live only for the current operator; there is no global goal
//! registry, immutable candidate archive, or grant-artifact cross product.

use std::collections::HashMap;
use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_planner::logical::operator::ColumnBinding;
use paro_planner::logical::plan::{OwnedLogicalPlan, PlanNodeId};
use paro_storage::statistics::ColumnStatistics;

use super::cost::CompactRange;
use super::implementation::*;
use super::*;
use crate::cost::calibration::MachineCalibrationBundle;
use crate::cost::operator::*;
use crate::cost::source::GrantDependencyDescriptor;

pub(crate) struct PhysicalSelection {
    pub contracts: ImplementationContracts,
    pub cost: PhysicalCost,
    pub nodes: u64,
    pub alternatives: u64,
    pub response: PhysicalResponse,
}

/// The physical response of a completed relation, shared by regional search
/// and committed-tree selection. Estimates are not semantic row bounds.
#[derive(Clone)]
pub(crate) struct PhysicalResponse {
    pub cost: PhysicalCost,
    pub hard_rows: Option<u64>,
    pub result: requirements::ResultGuarantee,
    sources: Box<[SourceResponse]>,
}

/// Only output materialization is movable across an RF boundary. Compressed
/// page access, decoding, independent operators and memory floors are not.
/// Multiple demands conservatively retain the strongest response; multiplying
/// correlated membership estimates would invent independence.
#[derive(Clone)]
struct SourceResponse {
    source: crate::cost::source::WorkSourceId,
    base: PhysicalCost,
    charged: PhysicalCost,
    retained_ppm: u32,
}

fn source_materialization(
    facts: &ResolvedPlannerCostFacts,
    calibration: &MachineCalibrationBundle,
) -> Result<Option<SourceResponse>> {
    let Some(source) = facts
        .scan_work_source
        .filter(|_| facts.scan_access_width.is_some())
    else {
        return Ok(None);
    };
    let mut work = crate::cost::calibration::LocalOperatorWork::default();
    work.add(
        crate::cost::calibration::OP_TUPLE_BYTE_BLOCK,
        scaled_work(facts.output_rows, facts.output_row_width as f64 / 32.0)?,
    )?;
    let base = calibration.fold(&work)?.work_only();
    Ok(Some(SourceResponse {
        source,
        base,
        charged: base,
        retained_ppm: 1_000_000,
    }))
}

fn source_retention(
    source: &SourceResponse,
    facts: &ResolvedPlannerCostFacts,
    flavor: PhysicalImplementationFlavor,
    tasks: u16,
) -> Result<u32> {
    use PhysicalImplementationFlavor as F;
    let (sources, build_index, distinct) = match flavor {
        F::HashJoinRuntimeFilter => (
            &facts.runtime_filter_probe_sources,
            1,
            facts.runtime_filter_build_distinct_expected,
        ),
        F::HashJoinBuildLeftRuntimeFilter => (
            &facts.runtime_filter_build_left_probe_sources,
            0,
            facts.runtime_filter_build_left_distinct_expected,
        ),
        _ => return Ok(source.retained_ppm),
    };
    let Some(input) = sources.iter().find(|s| s.source == source.source) else {
        return Ok(source.retained_ppm);
    };
    let build = facts.child_rows[build_index];
    let domain = runtime_filter_build_domain(distinct, build)?;
    let resource = RuntimeFilterResourceContract::for_keys(&facts.runtime_filter_key_types, tasks)?;
    let exactness = runtime_filter_exactness(
        &resource,
        facts.child_rows_hard_upper[build_index],
        domain.expected,
    );
    let retained = runtime_filtered_probe_work(input.rows, domain, input.multiplicity, exactness)?;
    let ppm = if input.rows.expected > 0.0 {
        (retained.expected / input.rows.expected * 1_000_000.0)
            .ceil()
            .clamp(0.0, 1_000_000.0) as u32
    } else {
        1_000_000
    };
    Ok(source.retained_ppm.min(ppm))
}

pub(crate) struct LocalSelection {
    pub implementation: PhysicalImplementationFlavor,
    pub response: PhysicalResponse,
    pub alternatives: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct SelectionEnvironment<'a> {
    pub grant: ResourceGrantClass,
    pub calibration: &'a MachineCalibrationBundle,
    pub session: &'a paro_context::StatementContext,
}

/// Memory floors are conservatively simultaneous. Elastic state shares one
/// admitted query pool; only independently spillable/bounded local contracts
/// may reach this composition. This is not a search lower-bound calculation.
fn compose(
    local: PhysicalCost,
    children: &[&PhysicalResponse],
    grant: ResourceGrantClass,
) -> Result<Option<PhysicalCost>> {
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
) -> Result<Option<PhysicalSelection>> {
    select_impl(root, statistics, grant, calibration, session, true)
}

/// Cost a short-lived region without publishing physical contracts, RF
/// ownership, fingerprints or executable alternatives. Only the committed
/// tree is materialized by `select`.
pub(crate) fn estimate_response(
    root: &OwnedLogicalPlan,
    statistics: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    grant: ResourceGrantClass,
    calibration: &MachineCalibrationBundle,
    session: &paro_context::StatementContext,
) -> Result<Option<PhysicalResponse>> {
    let _scope =
        crate::diagnostics::work::enter(crate::diagnostics::work::Bucket::PhysicalSelection);
    Ok(
        select_impl(root, statistics, grant, calibration, session, false)?
            .map(|selection| selection.response),
    )
}

fn select_impl(
    root: &OwnedLogicalPlan,
    statistics: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    grant: ResourceGrantClass,
    calibration: &MachineCalibrationBundle,
    session: &paro_context::StatementContext,
    publish: bool,
) -> Result<Option<PhysicalSelection>> {
    use PhysicalImplementationFlavor as F;
    let mut pending = vec![(root, false)];
    let mut completed = HashMap::<PlanNodeId, PhysicalResponse>::new();
    let mut contracts = HashMap::new();
    let mut alternatives = 0;
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
        let Some(selected) =
            select_local(plan, statistics, &children, grant, calibration, session)?
        else {
            return Ok(None);
        };
        alternatives += selected.alternatives;
        let implementation = selected.implementation;
        let cost = selected.response.cost;
        let result = selected.response.result;
        completed.insert(plan.id, selected.response);
        if !publish {
            continue;
        }
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
        let provided = ProvidedProperties {
            ordering: requirements::ProvidedOrdering::Unordered,
            partitioning: requirements::ProvidedPartitioning::Singleton,
            materialization: Default::default(),
            replayability: requirements::ProvidedReplayability::OnePass,
            representation: requirements::ProvidedRepresentation::Flat,
            mutation_safety: requirements::ProvidedMutationSafety::NotApplicable,
            result_guarantee: result,
        };
        let contract = ImplementationContract {
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
    }
    let cost = completed
        .get(&root.id)
        .ok_or_else(|| paro_error::internal("pipeline root is absent"))?
        .cost;
    Ok(Some(PhysicalSelection {
        nodes: completed.len() as u64,
        contracts: Arc::new(contracts),
        cost,
        alternatives,
        response: completed.get(&root.id).expect("selected root").clone(),
    }))
}

/// Price precisely one operator against already completed child responses.
/// Region candidates may use fact-backed child boundaries; this function
/// never selects or prices their descendants again.
pub(crate) fn select_local(
    plan: &OwnedLogicalPlan,
    statistics: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    children: &[&PhysicalResponse],
    grant: ResourceGrantClass,
    calibration: &MachineCalibrationBundle,
    session: &paro_context::StatementContext,
) -> Result<Option<LocalSelection>> {
    let bounds = children
        .iter()
        .map(|c| c.hard_rows)
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let template = planner_cost_facts(plan, statistics, Default::default())?;
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
    select_costed(
        &model,
        &facts,
        implementations,
        operator_result_guarantee(&plan.operator),
        children,
        SelectionEnvironment {
            grant,
            calibration,
            session,
        },
    )
}

/// Pure physical response evaluation. Input preparation is owned by the
/// relation representation; this kernel neither builds plans nor traverses
/// descendants. Both committed selection and regional transitions use it.
fn select_costed(
    model: &LocalCostModel<'_>,
    facts: &ResolvedPlannerCostFacts,
    implementations: PlannerImplementationSet,
    result: requirements::ResultGuarantee,
    children: &[&PhysicalResponse],
    environment: SelectionEnvironment<'_>,
) -> Result<Option<LocalSelection>> {
    use PhysicalImplementationFlavor as F;
    let SelectionEnvironment {
        grant,
        calibration,
        session,
    } = environment;
    let mut alternatives = 0;
    let mut best: Option<(F, PhysicalCost)> = None;
    let own_source = source_materialization(facts, calibration)?;
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
        let mut local =
            implementation_cost(model, facts, flavor, calibration, grant.max_parallel_tasks)?;
        if let Some(source) = &own_source {
            // Source access stays fully charged; only this output-vector
            // component may be replaced by a legal parent RF response.
            local = local.sequential(source.base)?;
        }
        let Some(mut local) = fit_local_cost(
            local,
            flavor_spillable(model, flavor),
            grant,
            session.limits.force_external,
        )?
        else {
            continue;
        };
        local.max_parallel_tasks = grant.max_parallel_tasks;
        local.output_pipeline_tasks = useful_output_tasks(facts, grant.max_parallel_tasks);
        let Some(mut cost) = compose(local, children, grant)? else {
            continue;
        };
        for source in children.iter().flat_map(|child| child.sources.iter()) {
            let retained = source_retention(source, facts, flavor, grant.max_parallel_tasks)?;
            if retained != source.retained_ppm {
                cost = cost.replace_work(
                    source.charged,
                    source.base.retain_work(retained, 1_000_000)?,
                )?;
            }
        }
        if best.as_ref().is_none_or(|(_, previous)| {
            crate::cost::ranking::compare_latency(&cost, previous).is_lt()
        }) {
            best = Some((flavor, cost));
        }
    }
    let Some((implementation, cost)) = best else {
        return Ok(None);
    };
    let mut sources = Vec::new();
    // A breaker closes source response ownership. A future RF cannot travel
    // through it merely because its column names happen to be preserved.
    if matches!(
        model.operator_type,
        paro_planner::logical::operator::LogicalOperatorType::Filter
            | paro_planner::logical::operator::LogicalOperatorType::Projection
            | paro_planner::logical::operator::LogicalOperatorType::ComparisonJoin
            | paro_planner::logical::operator::LogicalOperatorType::CrossProduct
    ) {
        for source in children.iter().flat_map(|child| child.sources.iter()) {
            let retained_ppm =
                source_retention(source, facts, implementation, grant.max_parallel_tasks)?;
            sources.push(SourceResponse {
                source: source.source,
                base: source.base,
                charged: source.base.retain_work(retained_ppm, 1_000_000)?,
                retained_ppm,
            });
        }
    }
    sources.extend(own_source);
    let result = children
        .iter()
        .map(|child| child.result)
        .chain(std::iter::once(result))
        .find(|result| matches!(result, requirements::ResultGuarantee::ApproximateAllowed(_)))
        .unwrap_or(requirements::ResultGuarantee::Exact);
    Ok(Some(LocalSelection {
        implementation,
        alternatives,
        response: PhysicalResponse {
            cost,
            hard_rows: facts.output_rows_hard_upper,
            result,
            sources: sources.into_boxed_slice(),
        },
    }))
}

/// Price a regional operator over completed relation summaries. Executable
/// nodes, binding arenas and physical payloads are constructed only after
/// regional selection; no owned child tree is needed to resolve these facts.
pub(crate) fn select_native(
    operator: &paro_planner::logical::operator::LogicalOperator<
        paro_planner::logical::operator::SubplanRef,
    >,
    stats: &paro_planner::logical::plan::NodeStats,
    layout: &paro_planner::logical::operator::LogicalOutputLayout,
    child_layouts: &[paro_planner::logical::operator::LogicalOutputLayout],
    statistics: &dyn crate::estimate::ColumnStatisticsLookup,
    children: &[&PhysicalResponse],
    environment: SelectionEnvironment<'_>,
) -> Result<Option<LocalSelection>> {
    let session = environment.session;
    let mut references = smallvec::SmallVec::<[_; 2]>::new();
    operator.visit_child_links(&mut |child| references.push(child));
    if references.len() != children.len() || references.len() != child_layouts.len() {
        return Err(paro_error::internal(
            "regional response child arity mismatch",
        ));
    }
    let inputs = references
        .iter()
        .zip(child_layouts)
        .map(|(reference, layout)| RuntimeFilterInput::Boundary {
            layout,
            facts: &reference.facts,
        })
        .collect::<smallvec::SmallVec<[_; 2]>>();
    let range = |estimate: Option<paro_planner::logical::plan::CardinalityEstimate>| {
        let r = estimate.unwrap_or(paro_planner::logical::plan::CardinalityEstimate {
            min: 0,
            expected: 1,
            max: 4,
        });
        CompactRange::new(r.min as f64, r.expected as f64, r.max as f64)
    };
    let rows = references
        .iter()
        .map(|c| range(c.facts.cardinality))
        .collect::<Result<Box<[_]>>>()?;
    let bounds = children.iter().map(|c| c.hard_rows).collect::<Box<[_]>>();
    let widths = child_layouts
        .iter()
        .map(|l| planner_row_width_from_layout(l, Default::default()))
        .collect::<smallvec::SmallVec<[_; 2]>>();
    let risks = references
        .iter()
        .map(|c| c.facts.cardinality.map_or(1, |r| r.max))
        .collect::<smallvec::SmallVec<[_; 2]>>();
    let width = planner_row_width_from_layout(layout, Default::default());
    let maximum = crate::estimate::cardinality_bound::derive_maximum_cardinality(operator, &bounds);
    let template = planner_native_cost_facts(
        operator,
        (&risks, &widths),
        width,
        Default::default(),
        &inputs,
        statistics,
    )?;
    let facts = resolve_cost_facts(
        &template,
        range(stats.estimated_cardinality)?,
        rows,
        maximum,
        bounds,
    )?;
    let implementations =
        planner_native_implementation_set(operator, session.limits.rowset_scan_pushdown, &inputs);
    let expected = facts
        .child_rows
        .iter()
        .map(|r| r.expected)
        .collect::<smallvec::SmallVec<[_; 2]>>();
    let model = LocalCostModel {
        operator_type: operator.op_type(),
        local_cost: planner_native_operator_cost(
            operator,
            stats,
            children.len(),
            maximum,
            (&facts.child_rows_hard_upper, &expected, &widths),
            width,
        )?,
        baseline: implementations.baseline,
        resource_sensitive: planner_grant_dependency(operator)
            == GrantDependencyDescriptor::Sensitive,
        spillable: planner_operator_spillable(operator),
        perfect_hash: facts.perfect_hash,
        runtime_filter_key_types: &facts.runtime_filter_key_types,
    };
    select_costed(
        &model,
        &facts,
        implementations,
        operator_result_guarantee(operator),
        children,
        environment,
    )
}

pub(crate) fn access(
    plan: OwnedLogicalPlan,
    ctx: &crate::context::OptimizationContext,
) -> Result<OwnedLogicalPlan> {
    ctx.session.cancellation.check()?;
    if let Some(provider) = crate::physical::access::index::AccessPlanner::new()
        .physical_candidate_for_root(&plan, ctx)?
    {
        return Ok(provider);
    }
    plan.try_map_children(|child| access(child, ctx))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rf_facts() -> ResolvedPlannerCostFacts {
        let plan = OwnedLogicalPlan::synthetic(
            paro_planner::logical::operator::LogicalOperator::DummyScan,
        );
        let template = planner_cost_facts(&plan, &HashMap::new(), Default::default()).unwrap();
        let mut facts = resolve_cost_facts(
            &template,
            CompactRange::point(10.0).unwrap(),
            Box::new([
                CompactRange::point(100.0).unwrap(),
                CompactRange::point(10.0).unwrap(),
            ]),
            Some(1000),
            Box::new([Some(100), Some(10)]),
        )
        .unwrap();
        facts.runtime_filter_key_types = Box::new([paro_common::types::LogicalType::Integer]);
        facts.runtime_filter_build_distinct_expected = Some(10);
        facts.runtime_filter_probe_sources = Box::new([ResolvedRuntimeFilterSource {
            source: crate::cost::source::WorkSourceId(7),
            rows: CompactRange::point(100.0).unwrap(),
            multiplicity: RuntimeFilterProbeMultiplicity::DeclaredUnique,
        }]);
        facts
    }

    #[test]
    fn rf_response_is_source_scoped_and_repeated_demands_are_idempotent() {
        let mut facts = rf_facts();
        let base = base_table_scan_cost(CompactRange::point(100.0).unwrap(), 32)
            .unwrap()
            .work_only();
        let mut source = SourceResponse {
            source: crate::cost::source::WorkSourceId(7),
            base,
            charged: base,
            retained_ppm: 1_000_000,
        };
        let flavor = PhysicalImplementationFlavor::HashJoinRuntimeFilter;
        let first = source_retention(&source, &facts, flavor, 4).unwrap();
        assert_eq!(first, 100_000);
        source.retained_ppm = first;
        assert_eq!(source_retention(&source, &facts, flavor, 4).unwrap(), first);
        facts.runtime_filter_probe_sources[0].source = crate::cost::source::WorkSourceId(8);
        source.retained_ppm = 1_000_000;
        assert_eq!(
            source_retention(&source, &facts, flavor, 4).unwrap(),
            1_000_000
        );
        facts.runtime_filter_probe_sources[0].source = source.source;
        facts.runtime_filter_probe_sources[0].multiplicity =
            RuntimeFilterProbeMultiplicity::Unknown;
        assert_eq!(
            source_retention(&source, &facts, flavor, 4).unwrap(),
            1_000_000
        );
    }

    #[test]
    fn replacing_materialization_does_not_discount_decode_or_memory() {
        let facts = rf_facts();
        let decode = base_table_scan_cost(CompactRange::point(100.0).unwrap(), 128).unwrap();
        let materialize = base_table_scan_cost(CompactRange::point(100.0).unwrap(), 32).unwrap();
        let source = SourceResponse {
            source: facts.runtime_filter_probe_sources[0].source,
            base: materialize,
            charged: materialize,
            retained_ppm: 1_000_000,
        };
        let retained = source_retention(
            &source,
            &facts,
            PhysicalImplementationFlavor::HashJoinRuntimeFilter,
            4,
        )
        .unwrap();
        let mut total = decode.sequential(materialize).unwrap();
        total.minimum_memory_bytes = 20;
        total.peak_memory_upper = 100;
        let changed = total
            .replace_work(
                materialize,
                materialize.retain_work(retained, 1_000_000).unwrap(),
            )
            .unwrap();
        assert_eq!(
            changed.score.range.expected,
            decode.score.range.expected + materialize.score.range.expected * 0.1
        );
        assert_eq!(changed.score.range.upper, total.score.range.upper);
        assert_eq!(changed.minimum_memory_bytes, 20);
        assert_eq!(changed.peak_memory_upper, 100);
    }

    fn grant(spill_policy: SpillPolicy) -> ResourceGrantClass {
        ResourceGrantClass {
            id: ResourceGrantClassId(0),
            hard_memory_bytes: 100,
            spill_policy,
            max_parallel_tasks: 1,
        }
    }

    fn memory(floor: u64, peak: u64) -> PhysicalCost {
        PhysicalCost {
            minimum_memory_bytes: floor,
            non_revocable_memory_upper: floor,
            peak_memory_upper: peak,
            revocable_memory_target: peak - floor,
            ..PhysicalCost::ZERO
        }
    }

    fn child(cost: PhysicalCost) -> PhysicalResponse {
        PhysicalResponse {
            cost,
            hard_rows: None,
            result: requirements::ResultGuarantee::Exact,
            sources: Box::new([]),
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
