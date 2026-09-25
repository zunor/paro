// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared, read-only implementation eligibility and local cost inputs.
//! This module does not allocate Memo groups or schedule search tasks.

use super::cost::{CompactRange, ResourceDimension, ScoreSummary, SearchCost};
use super::{ColumnId, Fingerprint, PhysicalImplementationFlavor, StableFingerprintBuilder};
use crate::binding::BindingCatalog;
use crate::cost::operator::base_table_scan_cost;
use crate::cost::operator::RuntimeFilterProbeMultiplicity;
use crate::cost::response::GrantDependencyDescriptor;
use crate::cost::response::WorkSourceId;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::expression::Expression;
use paro_planner::operator::join::AntiJoinMode;
use paro_planner::operator::{ColumnBinding, Join, JoinComparisonType, JoinType, LogicalOperator};
use paro_planner::physical::scalar_identity::expression_fingerprint;
use paro_planner::plan::CardinalityEstimate;
use paro_planner::plan::{NodeStats, OwnedLogicalPlan};
use std::collections::BTreeMap;

pub(crate) fn operator_result_guarantee<Child>(
    operator: &LogicalOperator<Child>,
) -> super::requirements::ResultGuarantee {
    use super::requirements::ResultGuarantee;
    let approximate = matches!(operator, LogicalOperator::SearchScan(scan)
        if scan.request.intents.iter().any(|intent| matches!(intent,
            paro_storage::search::SearchIntent::Hnsw(hnsw) if hnsw.options.objective
                == paro_storage::index::hnsw::HnswSearchObjective::CostOptimized)));
    if approximate {
        ResultGuarantee::ApproximateAllowed(super::identity::QualityPolicyId(1))
    } else {
        ResultGuarantee::Exact
    }
}

pub(crate) fn planner_grant_dependency<Child>(
    operator: &LogicalOperator<Child>,
) -> GrantDependencyDescriptor {
    if matches!(
        operator,
        LogicalOperator::Aggregate(_)
            | LogicalOperator::Distinct(_)
            | LogicalOperator::Order(_)
            | LogicalOperator::TopN(_)
            | LogicalOperator::Window(_)
            | LogicalOperator::MaterializedCTE(_)
            | LogicalOperator::RecursiveCTE(_)
            | LogicalOperator::Join(_)
    ) {
        GrantDependencyDescriptor::Sensitive
    } else if matches!(
        operator,
        LogicalOperator::Get(_)
            | LogicalOperator::SearchScan(_)
            | LogicalOperator::FullTextFilterScan(_)
            | LogicalOperator::GraphScan(_)
            | LogicalOperator::CTERef(_)
            | LogicalOperator::ExternalTable(_)
    ) {
        GrantDependencyDescriptor::Parallelism
    } else {
        GrantDependencyDescriptor::Invariant
    }
}

pub(crate) fn planner_operator_cost(
    plan: &OwnedLogicalPlan,
    child_count: usize,
    output_rows_hard_upper: Option<u64>,
    child_rows_hard_upper: &[Option<u64>],
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
) -> Result<SearchCost> {
    match &plan.operator {
        LogicalOperator::SearchScan(scan) => return search_decision_cost(&scan.decision),
        LogicalOperator::FullTextFilterScan(scan) => return search_decision_cost(&scan.decision),
        LogicalOperator::Get(get) => {
            let rows = plan.stats.estimated_cardinality.unwrap_or(
                paro_planner::plan::CardinalityEstimate {
                    min: 0,
                    expected: 1,
                    max: 4,
                },
            );
            let rows = CompactRange::new(rows.min as f64, rows.expected as f64, rows.max as f64)?;
            return base_table_scan_cost(rows, planner_scan_access_width(get, scan_access_cost));
        }
        LogicalOperator::ExternalProject(project) => {
            return external_operator_cost(project.cost, plan.stats.estimated_cardinality);
        }
        LogicalOperator::ExternalTable(table) => {
            return external_operator_cost(table.cost, plan.stats.estimated_cardinality);
        }
        _ => {}
    }
    let expected_rows = plan
        .stats
        .estimated_cardinality
        .map(|cardinality| cardinality.expected as f64)
        .unwrap_or(1.0)
        .max(1.0);
    // Structural operators still move their output tuple. Pricing only row
    // count makes a wide pre-TopN payload indistinguishable from a locator and
    // systematically rejects a proven late-fetch alternative because RowFetch
    // adds one node. Use a stable byte-work proxy until machine calibration
    // publishes operator-specific structural coefficients.
    let output_row_width = planner_row_width(plan, scan_access_cost);
    let width_factor = (output_row_width as f64 / 32.0).max(1.0);
    let materialization_write = match &plan.operator {
        LogicalOperator::MaterializedCTE(cte) => {
            let producer_rows =
                cte.cte_query
                    .stats
                    .estimated_cardinality
                    .unwrap_or(CardinalityEstimate {
                        min: 0,
                        expected: 1,
                        max: 4,
                    });
            let producer_width = planner_row_width(&cte.cte_query, scan_access_cost);
            let factor = (producer_width as f64 / 32.0).max(1.0);
            (
                producer_rows.expected as f64 * factor,
                producer_rows.max as f64 * factor,
            )
        }
        _ => (0.0, 0.0),
    };
    let expected = expected_rows * width_factor + child_count as f64 + materialization_write.0;
    let upper_rows = plan
        .stats
        .estimated_cardinality
        .map(|cardinality| cardinality.max as f64)
        .unwrap_or(expected_rows * 4.0)
        .max(expected_rows);
    let upper = upper_rows * width_factor + child_count as f64 + materialization_write.1;
    let mut cost = SearchCost {
        score: ScoreSummary {
            range: CompactRange::new(1.0, expected, upper)?,
            risk_adjusted: expected + (upper - expected) * 0.5,
        },
        work_latency: CompactRange::new(1.0, expected, upper)?,
        critical_path: CompactRange::new(1.0, expected, upper)?,
        ..SearchCost::ZERO
    };
    if planner_grant_dependency(&plan.operator) == GrantDependencyDescriptor::Sensitive {
        // Retained memory belongs to operator state, not to the number of rows
        // the operator happens to emit.  A cross product materializes only its
        // right build input; charging the Cartesian output here can reject a
        // tiny build side by many orders of magnitude under a hard grant.
        let resident_plan = match &plan.operator {
            LogicalOperator::Join(Join::Cross(cross)) => cross.right.as_ref(),
            LogicalOperator::MaterializedCTE(cte) => cte.cte_query.as_ref(),
            _ => plan,
        };
        let row_width = planner_row_width(resident_plan, scan_access_cost);
        let resident_rows = match &plan.operator {
            LogicalOperator::Join(Join::Cross(_)) => child_rows_hard_upper
                .get(1)
                .copied()
                .flatten()
                .unwrap_or(u64::MAX),
            LogicalOperator::MaterializedCTE(_) => child_rows_hard_upper
                .first()
                .copied()
                .flatten()
                .unwrap_or(u64::MAX),
            _ => output_rows_hard_upper.unwrap_or(u64::MAX),
        };
        cost.peak_memory_upper = resident_rows.saturating_mul(row_width);
        let resident_expected_rows = resident_plan
            .stats
            .estimated_cardinality
            .map(|cardinality| cardinality.expected as f64)
            .unwrap_or(1.0);
        cost.resources_expected[ResourceDimension::MemoryWrite as usize] =
            resident_expected_rows * row_width as f64;
        cost.resources_risk_upper[ResourceDimension::MemoryWrite as usize] =
            cost.peak_memory_upper as f64;
    }
    cost.validate()?;
    Ok(cost)
}

/// Cost a closed native operator from the facts already reduced by staging.
/// This is intentionally the same structural proxy as the owned planner path;
/// only the input representation changes, so native migration cannot silently
/// change ranking or grant behavior.
pub(crate) fn planner_native_operator_cost<Child>(
    operator: &LogicalOperator<Child>,
    stats: &NodeStats,
    child_count: usize,
    output_rows_hard_upper: Option<u64>,
    child_sizes: (&[Option<u64>], &[f64], &[u64]),
    output_row_width: u64,
) -> Result<SearchCost> {
    let (child_rows_hard_upper, child_expected_rows, child_row_widths) = child_sizes;
    if child_count != child_rows_hard_upper.len()
        || child_count != child_expected_rows.len()
        || child_count != child_row_widths.len()
    {
        return Err(paro_error::internal(
            "native operator cost child fact arity mismatch",
        ));
    }
    let expected_rows = stats
        .estimated_cardinality
        .map(|cardinality| cardinality.expected as f64)
        .unwrap_or(1.0)
        .max(1.0);
    let width_factor = (output_row_width as f64 / 32.0).max(1.0);
    let materialization_write = match operator {
        LogicalOperator::MaterializedCTE(_) => {
            let producer_expected = child_expected_rows.first().copied().unwrap_or(1.0);
            let producer_upper = child_rows_hard_upper
                .first()
                .copied()
                .flatten()
                .unwrap_or_else(|| (producer_expected.max(1.0) as u64).saturating_mul(4));
            let producer_width = child_row_widths.first().copied().unwrap_or(1);
            let factor = (producer_width as f64 / 32.0).max(1.0);
            (producer_expected * factor, producer_upper as f64 * factor)
        }
        _ => (0.0, 0.0),
    };
    let expected = expected_rows * width_factor + child_count as f64 + materialization_write.0;
    let upper_rows = stats
        .estimated_cardinality
        .map(|cardinality| cardinality.max as f64)
        .unwrap_or(expected_rows * 4.0)
        .max(expected_rows);
    let upper = upper_rows * width_factor + child_count as f64 + materialization_write.1;
    let mut cost = SearchCost {
        score: ScoreSummary {
            range: CompactRange::new(1.0, expected, upper)?,
            risk_adjusted: expected + (upper - expected) * 0.5,
        },
        work_latency: CompactRange::new(1.0, expected, upper)?,
        critical_path: CompactRange::new(1.0, expected, upper)?,
        ..SearchCost::ZERO
    };
    if planner_grant_dependency(operator) == GrantDependencyDescriptor::Sensitive {
        let (resident_expected_rows, resident_row_width, resident_rows_upper) = match operator {
            LogicalOperator::Join(Join::Cross(_)) => (
                child_expected_rows.get(1).copied().unwrap_or(1.0),
                child_row_widths.get(1).copied().unwrap_or(1),
                child_rows_hard_upper
                    .get(1)
                    .copied()
                    .flatten()
                    .unwrap_or(u64::MAX),
            ),
            LogicalOperator::MaterializedCTE(_) => (
                child_expected_rows.first().copied().unwrap_or(1.0),
                child_row_widths.first().copied().unwrap_or(1),
                child_rows_hard_upper
                    .first()
                    .copied()
                    .flatten()
                    .unwrap_or(u64::MAX),
            ),
            _ => (
                expected_rows,
                output_row_width,
                output_rows_hard_upper.unwrap_or(u64::MAX),
            ),
        };
        cost.peak_memory_upper = resident_rows_upper.saturating_mul(resident_row_width);
        cost.resources_expected[ResourceDimension::MemoryWrite as usize] =
            resident_expected_rows * resident_row_width as f64;
        cost.resources_risk_upper[ResourceDimension::MemoryWrite as usize] =
            cost.peak_memory_upper as f64;
    }
    cost.validate()?;
    Ok(cost)
}

pub(crate) fn search_decision_cost(
    decision: &paro_planner::operator::SearchDecision,
) -> Result<SearchCost> {
    fn candidate_score(candidate: &paro_planner::operator::SearchCandidate) -> Option<f64> {
        candidate
            .estimated_cost()
            .map(|estimate| estimate.score)
            .filter(|score| score.is_finite() && *score >= 0.0)
    }

    let (expected, upper) = match decision {
        paro_planner::operator::SearchDecision::IndexScan { candidate, .. } => {
            let expected = candidate_score(candidate).unwrap_or(1.0).max(1.0);
            (expected, expected * 2.0)
        }
        paro_planner::operator::SearchDecision::Adaptive {
            candidates,
            sequential,
        } => {
            let index = candidates
                .iter()
                .filter_map(candidate_score)
                .min_by(f64::total_cmp)
                .unwrap_or(1.0)
                .max(1.0);
            let sequential = sequential
                .estimated_cost
                .map(|estimate| estimate.score)
                .filter(|score| score.is_finite() && *score >= 0.0)
                .unwrap_or(index)
                .max(1.0);
            // Observation has a bounded cost; the upper envelope must retain
            // the slower arm because admission cannot assume which one wins.
            (
                index.min(sequential) + 1.0,
                index.max(sequential) * 2.0 + 1.0,
            )
        }
    };
    let lower = (expected * 0.5).min(expected);
    let range = CompactRange::new(lower, expected, upper.max(expected))?;
    let mut cost = SearchCost {
        score: ScoreSummary {
            range,
            risk_adjusted: expected + (range.upper - expected) * 0.5,
        },
        work_latency: range,
        critical_path: range,
        ..SearchCost::ZERO
    };
    cost.resources_expected[ResourceDimension::RandomIo as usize] = expected;
    cost.resources_risk_upper[ResourceDimension::RandomIo as usize] = range.upper;
    cost.validate()?;
    Ok(cost)
}

pub(crate) fn external_operator_cost(
    estimate: paro_planner::operator::external_project::ExternalCostEstimate,
    cardinality: Option<paro_planner::plan::CardinalityEstimate>,
) -> Result<SearchCost> {
    let rows = cardinality.unwrap_or(paro_planner::plan::CardinalityEstimate {
        min: 0,
        expected: 100,
        max: 1_000_000,
    });
    let lower = estimate.startup_cost
        + estimate.per_row_cost * rows.min as f64
        + estimate.bytes_cost * rows.min as f64;
    let expected = estimate.startup_cost
        + estimate.per_row_cost * rows.expected as f64
        + estimate.bytes_cost * rows.expected as f64
        + estimate.queue_risk;
    let upper = estimate.startup_cost
        + estimate.per_row_cost * rows.max as f64
        + estimate.bytes_cost * rows.max as f64
        + estimate.queue_risk * 4.0;
    let range = CompactRange::new(lower.min(expected), expected, upper.max(expected))?;
    let mut cost = SearchCost {
        score: ScoreSummary {
            range,
            risk_adjusted: expected + (range.upper - expected) * 0.5,
        },
        work_latency: range,
        critical_path: range,
        external_workers: paro_planner::physical::ExternalWorkerRequirementSetId(1),
        external_worker_slots_upper: 1,
        ..SearchCost::ZERO
    };
    cost.resources_expected[ResourceDimension::Cpu as usize] =
        estimate.per_row_cost * rows.expected as f64;
    cost.resources_risk_upper[ResourceDimension::Cpu as usize] =
        estimate.per_row_cost * rows.max as f64;
    cost.resources_expected[ResourceDimension::Network as usize] =
        estimate.bytes_cost * rows.expected as f64;
    cost.resources_risk_upper[ResourceDimension::Network as usize] =
        estimate.bytes_cost * rows.max as f64;
    cost.validate()?;
    Ok(cost)
}

/// Resolve a selected tree's local facts without a Memo or a fact revision.
pub(crate) fn selected_cost_facts(
    plan: &OwnedLogicalPlan,
    template: &PlannerCostFacts,
    child_rows_hard_upper: Box<[Option<u64>]>,
) -> Result<crate::cost::operator::ResolvedPlannerCostFacts> {
    use super::cost::CompactRange;
    let rows = |plan: &OwnedLogicalPlan| {
        let r =
            plan.stats
                .estimated_cardinality
                .unwrap_or(paro_planner::plan::CardinalityEstimate {
                    min: 0,
                    expected: 1,
                    max: 4,
                });
        CompactRange::new(r.min as f64, r.expected as f64, r.max as f64)
    };
    let output_rows = rows(plan)?;
    let child_rows = plan
        .children()
        .into_iter()
        .map(|p| rows(p))
        .collect::<Result<Vec<_>>>()?
        .into_boxed_slice();
    let output_rows_hard_upper = crate::estimate::cardinality_bound::derive_maximum_cardinality(
        &plan.operator,
        &child_rows_hard_upper,
    );
    resolve_cost_facts(
        template,
        output_rows,
        child_rows,
        output_rows_hard_upper,
        child_rows_hard_upper,
    )
}

/// Resolve one operator's scalar cardinality inputs without a planner tree.
/// Keep this conversion shared with committed physical selection.
pub(crate) fn resolve_cost_facts(
    template: &PlannerCostFacts,
    output_rows: CompactRange,
    child_rows: Box<[CompactRange]>,
    output_rows_hard_upper: Option<u64>,
    child_rows_hard_upper: Box<[Option<u64>]>,
) -> Result<crate::cost::operator::ResolvedPlannerCostFacts> {
    use crate::cost::operator::{ResolvedPlannerCostFacts, ResolvedRuntimeFilterSource};
    Ok(ResolvedPlannerCostFacts {
        output_rows,
        child_rows,
        output_rows_hard_upper,
        child_rows_hard_upper,
        child_row_widths: template.child_row_widths.clone(),
        child_materialization_risk_rows: template.child_materialization_risk_rows.clone(),
        output_row_width: template.output_row_width,
        hash_key_width: template.hash_key_width,
        scan_access_width: template.scan_access_width,
        scan_physical_rows: template.scan_physical_rows,
        scan_work_source: template.scan_work_source,
        perfect_hash: template.perfect_hash,
        topn_capacity: template.topn_capacity,
        runtime_filter_probe_multiplicity: template.runtime_filter_probe_multiplicity,
        runtime_filter_build_left_probe_multiplicity: template
            .runtime_filter_build_left_probe_multiplicity,
        runtime_filter_probe_source_rows: template
            .runtime_filter_probe_source_rows
            .map(|rows| CompactRange::new(rows.min as f64, rows.expected as f64, rows.max as f64))
            .transpose()?,
        runtime_filter_build_left_probe_source_rows: template
            .runtime_filter_build_left_probe_source_rows
            .map(|rows| CompactRange::new(rows.min as f64, rows.expected as f64, rows.max as f64))
            .transpose()?,
        runtime_filter_probe_sources: template
            .runtime_filter_probe_sources
            .iter()
            .map(|source| {
                Ok(ResolvedRuntimeFilterSource {
                    source: source.source,
                    rows: CompactRange::new(
                        source.rows.min as f64,
                        source.rows.expected as f64,
                        source.rows.max as f64,
                    )?,
                    multiplicity: source.multiplicity,
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice(),
        runtime_filter_build_left_probe_sources: template
            .runtime_filter_build_left_probe_sources
            .iter()
            .map(|source| {
                Ok(ResolvedRuntimeFilterSource {
                    source: source.source,
                    rows: CompactRange::new(
                        source.rows.min as f64,
                        source.rows.expected as f64,
                        source.rows.max as f64,
                    )?,
                    multiplicity: source.multiplicity,
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice(),
        runtime_filter_build_distinct_expected: template.runtime_filter_build_distinct_expected,
        runtime_filter_build_left_distinct_expected: template
            .runtime_filter_build_left_distinct_expected,
        runtime_filter_build_domain_identity: None,
        runtime_filter_build_left_domain_identity: None,
        runtime_filter_key_types: template.runtime_filter_key_types.clone(),
    })
}

pub(crate) fn planner_operator_spillable<Child>(operator: &LogicalOperator<Child>) -> bool {
    match operator {
        LogicalOperator::Aggregate(aggregate) => {
            !aggregate.groups.is_empty()
                && aggregate.aggregates.iter().all(|expression| {
                    matches!(
                        expression,
                        Expression::Aggregate(aggregate)
                            if !aggregate.is_distinct() && aggregate.order_bys.is_empty()
                    )
                })
        }
        LogicalOperator::Distinct(_) | LogicalOperator::Order(_) | LogicalOperator::Window(_) => {
            true
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            crate::physical::lower::helpers::supports_external_hash_join_type(join.join_type)
        }
        // Cross product has two explicit physical implementations. This flag
        // advertises the external one; the in-memory implementation remains a
        // separate non-spillable candidate.
        LogicalOperator::Join(Join::Cross(_)) => true,
        LogicalOperator::MaterializedCTE(_) => true,
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct PlannerImplementationCapabilities {
    perfect_hash_aggregate: bool,
    singleton_aggregate_projection: bool,
    sort_range_join: bool,
    classic_ie_join: bool,
    hash_join_build_left_runtime_filter: bool,
    hash_join_runtime_filter: bool,
    partition_aggregate_window: bool,
}

pub(crate) fn planner_implementation_set(
    plan: &OwnedLogicalPlan,
    rowset_scan_pushdown: bool,
) -> PlannerImplementationSet {
    let mut capabilities = PlannerImplementationCapabilities::default();
    match &plan.operator {
        LogicalOperator::Aggregate(aggregate) => {
            capabilities.perfect_hash_aggregate =
                crate::physical::lower::helpers::can_use_perfect_hash_aggregate(
                    aggregate,
                    &aggregate.groups,
                    &aggregate.aggregates,
                )
                .is_some();
            capabilities.singleton_aggregate_projection =
                crate::physical::lower::aggregate::supports_singleton_aggregate_projection(
                    aggregate,
                );
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            capabilities.hash_join_build_left_runtime_filter =
                supports_build_left_runtime_filter_auxiliary(join, rowset_scan_pushdown);
            capabilities.hash_join_runtime_filter =
                supports_runtime_filter_auxiliary(join, rowset_scan_pushdown);
            capabilities.sort_range_join =
                crate::physical::lower::inequality_join_gate::is_sort_range_join_candidate(
                    join,
                    plan.stats.estimated_cardinality,
                );
            capabilities.classic_ie_join =
                crate::physical::lower::inequality_join_gate::is_classic_ie_join_candidate(
                    join,
                    plan.stats.estimated_cardinality,
                );
        }
        LogicalOperator::Window(window) => {
            capabilities.partition_aggregate_window =
                crate::physical::lower::misc::supports_partition_aggregate_window(window);
        }
        _ => {}
    }
    planner_implementation_set_for_operator(&plan.operator, capabilities)
}

/// Native shells consume the same RF admission predicates as owned nodes.
/// Boundary source lineage is complete across the group's legal alternatives;
/// absent lineage stays unknown. Inequality gates still need their own facts.
pub(crate) fn planner_native_implementation_set<Child>(
    operator: &LogicalOperator<Child>,
    rowset_scan_pushdown: bool,
    inputs: &[RuntimeFilterInput<'_>],
) -> PlannerImplementationSet {
    let mut capabilities = PlannerImplementationCapabilities::default();
    match operator {
        LogicalOperator::Aggregate(aggregate) => {
            capabilities.perfect_hash_aggregate =
                crate::physical::lower::helpers::can_use_perfect_hash_aggregate(
                    aggregate,
                    &aggregate.groups,
                    &aggregate.aggregates,
                )
                .is_some();
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            if let [left, right] = inputs {
                capabilities.hash_join_runtime_filter =
                    supports_runtime_filter_input(join, *left, rowset_scan_pushdown);
                capabilities.hash_join_build_left_runtime_filter =
                    supports_build_left_runtime_filter_input(join, *right, rowset_scan_pushdown);
            }
        }
        _ => {}
    };
    planner_implementation_set_for_operator(operator, capabilities)
}

/// Derive implementation capabilities from an operator shell and immutable
/// facts. Native transformation staging uses this entry point so it does not
/// rebuild a shallow `OwnedLogicalPlan` merely to inspect operator-local
/// capabilities. Both inputs use the same RF admission and source-work
/// routines; only their read-only lineage views differ.
fn planner_implementation_set_for_operator<Child>(
    operator: &LogicalOperator<Child>,
    capabilities: PlannerImplementationCapabilities,
) -> PlannerImplementationSet {
    match operator {
        LogicalOperator::Aggregate(_aggregate) => PlannerImplementationSet {
            baseline: PhysicalImplementationFlavor::HashAggregate,
            perfect_hash_aggregate: capabilities.perfect_hash_aggregate,
            singleton_aggregate_projection: capabilities.singleton_aggregate_projection,
            ..PlannerImplementationSet::STRUCTURAL
        },
        LogicalOperator::Join(Join::Comparison(join)) => {
            if !join.duplicate_eliminated_columns.is_empty() || join.delim_flipped {
                return PlannerImplementationSet::STRUCTURAL;
            }
            let has_hash_key = join.conditions.iter().any(|condition| {
                matches!(
                    condition.comparison,
                    JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
                )
            });
            let has_non_hash_condition = join.conditions.iter().any(|condition| {
                !matches!(
                    condition.comparison,
                    JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
                )
            });
            let mark_contract_supported = crate::physical::hash_join_mark_contract_is_supported(
                join.join_type,
                join.mark_semantics,
                has_non_hash_condition,
            );
            let supports_hash_type = matches!(
                join.join_type,
                JoinType::Left
                    | JoinType::Right
                    | JoinType::Inner
                    | JoinType::Outer
                    | JoinType::Semi
                    | JoinType::Anti
                    | JoinType::Mark
                    | JoinType::Single
                    | JoinType::RightSemi
                    | JoinType::RightAnti
            );
            let supports_build_left_type = matches!(
                join.join_type,
                JoinType::Left
                    | JoinType::Right
                    | JoinType::Inner
                    | JoinType::Outer
                    | JoinType::Semi
                    | JoinType::Anti
                    | JoinType::RightSemi
                    | JoinType::RightAnti
            ) && join.anti_join_mode == AntiJoinMode::Regular;
            let baseline = if has_hash_key
                && mark_contract_supported
                && supports_hash_type
                && join.build_side_constraint.allows_right()
            {
                PhysicalImplementationFlavor::HashJoin
            } else if has_hash_key
                && mark_contract_supported
                && supports_build_left_type
                && join.build_side_constraint.allows_left()
            {
                PhysicalImplementationFlavor::HashJoinBuildLeft
            } else if join.anti_join_mode == AntiJoinMode::NullAware {
                // The extractor reports the precise semantic capability error;
                // keep structural lowering for this malformed/non-hashable
                // shape rather than advertising NLJ as null-aware.
                PhysicalImplementationFlavor::Structural
            } else {
                PhysicalImplementationFlavor::NestedLoopJoin
            };
            PlannerImplementationSet {
                baseline,
                hash_join_build_left: baseline == PhysicalImplementationFlavor::HashJoin
                    && supports_build_left_type
                    && join.build_side_constraint.allows_left(),
                hash_join_build_left_runtime_filter: supports_build_left_type
                    && join.build_side_constraint.allows_left()
                    && capabilities.hash_join_build_left_runtime_filter,
                hash_join_runtime_filter: has_hash_key
                    && supports_hash_type
                    && join.build_side_constraint.allows_right()
                    && capabilities.hash_join_runtime_filter,
                sort_range_join: capabilities.sort_range_join,
                classic_ie_join: capabilities.classic_ie_join,
                ..PlannerImplementationSet::STRUCTURAL
            }
        }
        LogicalOperator::Window(_window) => PlannerImplementationSet {
            baseline: PhysicalImplementationFlavor::Window,
            partition_aggregate_window: capabilities.partition_aggregate_window,
            ..PlannerImplementationSet::STRUCTURAL
        },
        LogicalOperator::Join(Join::Any(join)) => PlannerImplementationSet {
            baseline: if join.build_side_constraint.allows_right() {
                PhysicalImplementationFlavor::NestedLoopJoin
            } else {
                PhysicalImplementationFlavor::Structural
            },
            ..PlannerImplementationSet::STRUCTURAL
        },
        LogicalOperator::Join(Join::Cross(join)) => {
            if join.build_side_constraint.allows_right() {
                PlannerImplementationSet {
                    baseline: PhysicalImplementationFlavor::CrossProductInMemory,
                    external_cross_product: true,
                    ..PlannerImplementationSet::STRUCTURAL
                }
            } else {
                PlannerImplementationSet::STRUCTURAL
            }
        }
        LogicalOperator::Order(_) => PlannerImplementationSet {
            baseline: PhysicalImplementationFlavor::AdaptiveSort,
            ..PlannerImplementationSet::STRUCTURAL
        },
        LogicalOperator::TopN(_) => PlannerImplementationSet {
            baseline: PhysicalImplementationFlavor::HeapTopN,
            ..PlannerImplementationSet::STRUCTURAL
        },
        _ => PlannerImplementationSet::STRUCTURAL,
    }
}

/// The same source-work contract can be read from an existing planner tree or
/// an immutable native boundary. The native view never creates a tree or
/// chooses a representative expression from a Memo group.
#[derive(Clone, Copy)]
pub(crate) enum RuntimeFilterInput<'a> {
    Owned(&'a OwnedLogicalPlan),
    Boundary {
        layout: &'a paro_planner::operator::LogicalOutputLayout,
        facts: &'a paro_planner::operator::bound_reference::BoundRelationFacts,
    },
}

impl<'a> RuntimeFilterInput<'a> {
    fn bindings(self) -> std::borrow::Cow<'a, [ColumnBinding]> {
        match self {
            Self::Owned(plan) => plan.get_column_bindings().into(),
            Self::Boundary { layout, .. } => layout.bindings().into(),
        }
    }

    fn lineages(self, output_index: usize) -> Option<RuntimeFilterProbeLineage<'a>> {
        match self {
            Self::Owned(plan) => runtime_filter_probe_lineages(plan, output_index),
            Self::Boundary { layout, facts } => {
                if layout.types() != facts.types() {
                    return None;
                }
                let columns = facts.source_lineage.get(output_index)?.as_ref()?;
                Some(RuntimeFilterProbeLineage {
                    sources: columns
                        .iter()
                        .map(|column| RuntimeFilterProbeSource {
                            plan: None,
                            output_index: column.column,
                            boundary: Some(column),
                        })
                        .collect(),
                })
            }
        }
    }

    fn probe_multiplicity(self, expressions: &[&Expression]) -> RuntimeFilterProbeMultiplicity {
        match self {
            Self::Owned(plan) => {
                infer_runtime_filter_probe_multiplicity(plan, expressions.iter().copied())
            }
            Self::Boundary { layout, facts } => {
                if layout.types() == facts.types()
                    && crate::estimate::unique_keys::expressions_cover_unique_key_from_facts(
                        layout,
                        &facts.unique_keys,
                        expressions,
                    )
                {
                    RuntimeFilterProbeMultiplicity::DeclaredUnique
                } else {
                    RuntimeFilterProbeMultiplicity::Unknown
                }
            }
        }
    }
}

pub(crate) fn supports_runtime_filter_auxiliary(
    join: &paro_planner::operator::ComparisonJoin,
    rowset_scan_pushdown: bool,
) -> bool {
    supports_runtime_filter_input(
        join,
        RuntimeFilterInput::Owned(&join.left),
        rowset_scan_pushdown,
    )
}

fn supports_runtime_filter_input<Child>(
    join: &paro_planner::operator::join::ComparisonJoin<Child>,
    probe: RuntimeFilterInput<'_>,
    rowset_scan_pushdown: bool,
) -> bool {
    if !rowset_scan_pushdown
        || !matches!(
            join.join_type,
            JoinType::Inner | JoinType::Semi | JoinType::RightSemi | JoinType::RightAnti
        )
    {
        return false;
    }

    let probe_bindings = probe.bindings();
    join.conditions.iter().any(|condition| {
        if condition.comparison != JoinComparisonType::Equal {
            return false;
        }
        if !crate::physical::RuntimeFilterResourceContract::for_keys(
            &[condition.right.return_type()],
            1,
        )
        .is_ok_and(|contract| {
            contract.capability != crate::physical::RuntimeFilterCapability::Disabled
        }) {
            return false;
        }
        let output_index = match &condition.left {
            Expression::ColumnRef(column) if column.depth == 0 => probe_bindings
                .iter()
                .position(|binding| *binding == column.binding),
            Expression::Reference(reference) => Some(reference.index),
            _ => None,
        };
        output_index.is_some_and(|index| {
            probe
                .lineages(index)
                .is_some_and(|lineage| !lineage.sources.is_empty())
        })
    })
}

pub(crate) fn supports_build_left_runtime_filter_auxiliary(
    join: &paro_planner::operator::ComparisonJoin,
    rowset_scan_pushdown: bool,
) -> bool {
    supports_build_left_runtime_filter_input(
        join,
        RuntimeFilterInput::Owned(&join.right),
        rowset_scan_pushdown,
    )
}

fn supports_build_left_runtime_filter_input<Child>(
    join: &paro_planner::operator::join::ComparisonJoin<Child>,
    probe: RuntimeFilterInput<'_>,
    rowset_scan_pushdown: bool,
) -> bool {
    if !rowset_scan_pushdown
        || join.anti_join_mode != AntiJoinMode::Regular
        || !matches!(
            join.join_type,
            JoinType::Inner
                | JoinType::Left
                | JoinType::Semi
                | JoinType::Anti
                | JoinType::RightSemi
        )
    {
        return false;
    }

    let probe_bindings = probe.bindings();
    join.conditions.iter().any(|condition| {
        if condition.comparison != JoinComparisonType::Equal {
            return false;
        }
        if !crate::physical::RuntimeFilterResourceContract::for_keys(
            &[condition.left.return_type()],
            1,
        )
        .is_ok_and(|contract| {
            contract.capability != crate::physical::RuntimeFilterCapability::Disabled
        }) {
            return false;
        }
        let output_index = match &condition.right {
            Expression::ColumnRef(column) if column.depth == 0 => probe_bindings
                .iter()
                .position(|binding| *binding == column.binding),
            Expression::Reference(reference) => Some(reference.index),
            _ => None,
        };
        output_index
            .and_then(|index| probe.lineages(index))
            .is_some_and(|lineage| !lineage.sources.is_empty())
    })
}

struct RuntimeFilterProbeLineage<'a> {
    sources: Vec<RuntimeFilterProbeSource<'a>>,
}

#[derive(Clone, Copy)]
struct RuntimeFilterProbeSource<'a> {
    plan: Option<&'a OwnedLogicalPlan>,
    output_index: usize,
    boundary: Option<&'a paro_planner::operator::bound_reference::BoundSourceColumn>,
}

/// Freeze the existing source-lineage contract at a local relation boundary.
/// Regional costing must expose precisely the RF capability that committed
/// tree selection sees; an opaque child must not silently disable that choice.
pub(crate) fn planner_source_lineage(
    plan: &OwnedLogicalPlan,
) -> Vec<Option<Vec<paro_planner::operator::bound_reference::BoundSourceColumn>>> {
    use paro_planner::operator::bound_reference::BoundSourceColumn;
    (0..plan.types().len())
        .map(|ordinal| {
            runtime_filter_probe_lineages(plan, ordinal)?
                .sources
                .into_iter()
                .map(|source| {
                    if let Some(boundary) = source.boundary {
                        return Some(boundary.clone());
                    }
                    let plan = source.plan?;
                    let binding = *plan.get_column_bindings().get(source.output_index)?;
                    let ty = plan.types().get(source.output_index)?.clone();
                    let expression = Expression::ColumnRef(
                        paro_planner::expression::ColumnRefExpression::new(binding, ty).into(),
                    );
                    let multiplicity = infer_runtime_filter_probe_multiplicity(plan, [&expression]);
                    Some(BoundSourceColumn {
                        source: runtime_filter_source_id(source)?.0,
                        occurrence: plan.id.0 as usize,
                        column: source.output_index,
                        rows: plan.stats.estimated_cardinality,
                        distinct: match multiplicity {
                            RuntimeFilterProbeMultiplicity::EstimatedDistinct { keys } => {
                                Some(keys)
                            }
                            _ => None,
                        },
                        unique: matches!(
                            multiplicity,
                            RuntimeFilterProbeMultiplicity::DeclaredUnique
                        ),
                    })
                })
                .collect()
        })
        .collect()
}

fn runtime_filter_source_id(source: RuntimeFilterProbeSource<'_>) -> Option<WorkSourceId> {
    if let Some(boundary) = source.boundary {
        return Some(WorkSourceId(boundary.source));
    }
    let get = match &source.plan?.operator {
        LogicalOperator::Get(get) => get,
        LogicalOperator::SearchScan(search) => &search.get,
        LogicalOperator::FullTextFilterScan(search) => &search.get,
        _ => return None,
    };
    Some(WorkSourceId(get.table_index))
}

fn runtime_filter_input_source_facts<'a>(
    input: RuntimeFilterInput<'_>,
    expressions: impl IntoIterator<Item = &'a Expression>,
) -> Option<Box<[PlannerRuntimeFilterSource]>> {
    let bindings = input.bindings();
    let mut expected_sources = None;
    let mut source_keys = BTreeMap::<
        WorkSourceId,
        (
            RuntimeFilterProbeSource<'_>,
            BTreeMap<usize, RuntimeFilterProbeSource<'_>>,
        ),
    >::new();
    let mut saw_expression = false;
    for expression in expressions {
        saw_expression = true;
        let output_index = match expression {
            Expression::ColumnRef(column) if column.depth == 0 => bindings
                .iter()
                .position(|binding| *binding == column.binding),
            Expression::Reference(reference) => Some(reference.index),
            _ => None,
        }?;
        let lineage = input.lineages(output_index)?;
        let mut current_sources = lineage
            .sources
            .iter()
            .copied()
            .map(runtime_filter_source_id)
            .collect::<Option<Vec<_>>>()?;
        current_sources.sort_unstable();
        current_sources.dedup();
        if current_sources.is_empty()
            || expected_sources
                .as_ref()
                .is_some_and(|expected| expected != &current_sources)
        {
            return None;
        }
        expected_sources.get_or_insert(current_sources);
        for source in lineage.sources {
            let source_id = runtime_filter_source_id(source)?;
            let entry = source_keys
                .entry(source_id)
                .or_insert_with(|| (source, BTreeMap::new()));
            // One binding identity names one physical rowset occurrence. If a
            // future lineage maps it to two plan nodes, decline instead of
            // merging unrelated statistics under one source-work identity.
            let same_occurrence = match (entry.0.boundary, source.boundary) {
                (Some(left), Some(right)) => left.occurrence == right.occurrence,
                (None, None) => std::ptr::eq(entry.0.plan?, source.plan?),
                _ => false,
            };
            if !same_occurrence {
                return None;
            }
            entry.1.insert(source.output_index, source);
        }
    }
    if !saw_expression {
        return None;
    }
    source_keys
        .into_iter()
        .map(|(source, (first, keys))| {
            if let Some(boundary) = first.boundary {
                let multiplicity = if keys
                    .values()
                    .any(|key| key.boundary.is_some_and(|key| key.unique))
                {
                    RuntimeFilterProbeMultiplicity::DeclaredUnique
                } else if keys.len() == 1 {
                    boundary
                        .distinct
                        .map_or(RuntimeFilterProbeMultiplicity::Unknown, |keys| {
                            RuntimeFilterProbeMultiplicity::EstimatedDistinct { keys }
                        })
                } else {
                    RuntimeFilterProbeMultiplicity::Unknown
                };
                return Some(PlannerRuntimeFilterSource {
                    source,
                    rows: boundary.rows?,
                    multiplicity,
                });
            }
            let plan = first.plan?;
            let types = plan.types();
            let bindings = plan.get_column_bindings();
            let key_expressions = keys
                .into_keys()
                .map(|index| {
                    Some(Expression::ColumnRef(
                        paro_planner::expression::ColumnRefExpression::new(
                            *bindings.get(index)?,
                            types.get(index)?.clone(),
                        )
                        .into(),
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            let rows = plan.stats.estimated_cardinality?;
            let multiplicity = infer_runtime_filter_probe_multiplicity(
                plan,
                key_expressions.iter().collect::<Vec<_>>(),
            );
            Some(PlannerRuntimeFilterSource {
                source,
                rows,
                multiplicity,
            })
        })
        .collect::<Option<Vec<_>>>()
        .map(Vec::into_boxed_slice)
}

#[cfg(test)]
pub(crate) fn runtime_filter_probe_sources(
    join: &paro_planner::operator::ComparisonJoin,
) -> Option<Box<[PlannerRuntimeFilterSource]>> {
    runtime_filter_input_source_facts(
        RuntimeFilterInput::Owned(&join.left),
        join.conditions
            .iter()
            .filter(|condition| condition.comparison == JoinComparisonType::Equal)
            .map(|condition| &condition.left),
    )
}

fn runtime_filter_probe_lineages(
    plan: &OwnedLogicalPlan,
    output_index: usize,
) -> Option<RuntimeFilterProbeLineage<'_>> {
    match &plan.operator {
        LogicalOperator::BoundReference(reference) => {
            let columns = reference.facts.source_lineage.get(output_index)?.as_ref()?;
            Some(RuntimeFilterProbeLineage {
                sources: columns
                    .iter()
                    .map(|column| RuntimeFilterProbeSource {
                        plan: Some(plan),
                        output_index: column.column,
                        boundary: Some(column),
                    })
                    .collect(),
            })
        }
        LogicalOperator::Get(get)
            if get.table.is_some() && get.stored_column(output_index).is_some() =>
        {
            Some(RuntimeFilterProbeLineage {
                sources: vec![RuntimeFilterProbeSource {
                    plan: Some(plan),
                    output_index,
                    boundary: None,
                }],
            })
        }
        LogicalOperator::SearchScan(search) if search.get.table.is_some() => {
            let source_index = match search.projections.get(output_index)? {
                Expression::Reference(reference) => reference.index,
                Expression::ColumnRef(column) if column.depth == 0 => {
                    (0..search.get.returned_types.len()).find(|index| {
                        ColumnBinding::new(search.get.table_index, *index) == column.binding
                    })?
                }
                _ => return None,
            };
            search.get.stored_column(source_index)?;
            Some(RuntimeFilterProbeLineage {
                sources: vec![RuntimeFilterProbeSource {
                    plan: Some(plan),
                    output_index,
                    boundary: None,
                }],
            })
        }
        LogicalOperator::FullTextFilterScan(search) if search.get.table.is_some() => {
            let source_index = *search
                .projection_map
                .to_indices(search.get.returned_types.len())
                .get(output_index)?;
            search.get.stored_column(source_index)?;
            Some(RuntimeFilterProbeLineage {
                sources: vec![RuntimeFilterProbeSource {
                    plan: Some(plan),
                    output_index,
                    boundary: None,
                }],
            })
        }
        LogicalOperator::Filter(filter) => {
            let child_index = filter
                .projection_map
                .to_indices(filter.child.types().len())
                .get(output_index)
                .copied()?;
            runtime_filter_probe_lineages(&filter.child, child_index)
        }
        LogicalOperator::Projection(projection)
            if !matches!(projection.child.operator, LogicalOperator::RowFetch(_)) =>
        {
            let child_bindings = projection.child.get_column_bindings();
            let child_index = match projection.expressions.get(output_index)? {
                Expression::ColumnRef(column) if column.depth == 0 => child_bindings
                    .iter()
                    .position(|binding| *binding == column.binding),
                Expression::Reference(reference) => Some(reference.index),
                _ => None,
            }?;
            runtime_filter_probe_lineages(&projection.child, child_index)
        }
        LogicalOperator::SetOperation(setop)
            if setop.setop_type == paro_planner::operator::SetOpType::Union && setop.setop_all =>
        {
            if output_index >= setop.column_count {
                return None;
            }
            let mut left = runtime_filter_probe_lineages(&setop.left, output_index)?;
            let right = runtime_filter_probe_lineages(&setop.right, output_index)?;
            left.sources.extend(right.sources);
            Some(left)
        }
        LogicalOperator::Join(Join::Comparison(inner))
            if inner.duplicate_eliminated_columns.is_empty() && !inner.delim_flipped =>
        {
            let left_projection = inner
                .left_projection_map
                .to_indices(inner.left.types().len());
            if let Some(&child_index) = left_projection.get(output_index) {
                if !inner.join_type.preserves_left_values() {
                    return None;
                }
                return runtime_filter_probe_lineages(&inner.left, child_index);
            }
            if !inner.join_type.preserves_right_values() {
                // A NULL-extended value no longer has exact source lineage.
                // Pushing a predicate into its stored origin could change
                // which preserved rows are considered matched.
                return None;
            }
            let right_output = output_index.checked_sub(left_projection.len())?;
            let right_projection = inner
                .right_projection_map
                .to_indices(inner.right.types().len());
            runtime_filter_probe_lineages(&inner.right, *right_projection.get(right_output)?)
        }
        // A CTE reference is not a rowset consumer. Crossing it requires one
        // AuxiliaryPlanRegion jointly owned by the CTE producer, every
        // reference, and the runtime-filter build.
        _ => None,
    }
}

fn runtime_filter_input_source_rows<'a>(
    input: RuntimeFilterInput<'_>,
    expressions: impl IntoIterator<Item = &'a Expression>,
) -> Option<paro_planner::plan::CardinalityEstimate> {
    let probe_bindings = input.bindings();
    expressions.into_iter().find_map(|expression| {
        let output_index = match expression {
            Expression::ColumnRef(column) if column.depth == 0 => probe_bindings
                .iter()
                .position(|binding| *binding == column.binding),
            Expression::Reference(reference) => Some(reference.index),
            _ => None,
        }?;
        let lineage = input.lineages(output_index)?;
        lineage.sources.into_iter().try_fold(
            paro_planner::plan::CardinalityEstimate::exact(0),
            |sum, source| {
                let rows = source.boundary.map_or(
                    source
                        .plan
                        .and_then(|plan| plan.stats.estimated_cardinality),
                    |column| column.rows,
                )?;
                Some(paro_planner::plan::CardinalityEstimate {
                    min: sum.min.saturating_add(rows.min),
                    expected: sum.expected.saturating_add(rows.expected),
                    max: sum.max.saturating_add(rows.max),
                })
            },
        )
    })
}

#[derive(Debug, Clone)]
pub(crate) struct PlannerCostFacts {
    pub(crate) child_row_widths: Box<[u64]>,
    /// Expression-local cardinality risk for materializing each child. Unlike
    /// `child_rows_hard_upper`, this is statistical evidence used for ranking
    /// only; it never proves capacity or query correctness.
    pub(crate) child_materialization_risk_rows: Box<[u64]>,
    pub(crate) output_row_width: u64,
    /// Bytes participating in one hash key. Row-oriented hash work is
    /// calibrated for one integral key; wider/composite keys pay separately.
    pub(crate) hash_key_width: Option<u64>,
    /// Bytes physically read from base-table column sources for each scan
    /// row. `None` identifies a non-scan structural operator.
    pub(crate) scan_access_width: Option<u64>,
    /// Snapshot physical rows presented by a base-table source before
    /// predicates. This is task-supply evidence only: it affects duration
    /// ranking, never cardinality or a semantic upper bound.
    pub(crate) scan_physical_rows: Option<u64>,
    pub(crate) scan_work_source: Option<WorkSourceId>,
    pub(crate) perfect_hash: Option<crate::physical::PerfectHashResourceContract>,
    pub(crate) topn_capacity: Option<u64>,
    pub(crate) runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity,
    pub(crate) runtime_filter_build_left_probe_multiplicity: RuntimeFilterProbeMultiplicity,
    pub(crate) runtime_filter_probe_source_rows: Option<paro_planner::plan::CardinalityEstimate>,
    pub(crate) runtime_filter_build_left_probe_source_rows:
        Option<paro_planner::plan::CardinalityEstimate>,
    pub(crate) runtime_filter_probe_sources: Box<[PlannerRuntimeFilterSource]>,
    pub(crate) runtime_filter_build_left_probe_sources: Box<[PlannerRuntimeFilterSource]>,
    /// Snapshot estimate of the distinct build-key domain. This ranks
    /// runtime-filter benefit; it never proves capacity or correctness.
    pub(crate) runtime_filter_build_distinct_expected: Option<u64>,
    /// Stable output identity used to resolve the current build domain from
    /// the right child group at cost-composition time.
    pub(crate) runtime_filter_build_domain_column: Option<ColumnId>,
    /// Identity of all equality-key expressions and their input layout.
    /// Unlike a single-column NDV lookup this exists for composite keys.
    pub(crate) runtime_filter_build_key: Option<Fingerprint>,
    /// Snapshot estimate for the logical-left key domain when a physical
    /// implementation inverts build and probe.
    pub(crate) runtime_filter_build_left_distinct_expected: Option<u64>,
    pub(crate) runtime_filter_build_left_domain_column: Option<ColumnId>,
    pub(crate) runtime_filter_build_left_key: Option<Fingerprint>,
    pub(crate) runtime_filter_key_types: Box<[LogicalType]>,
}

/// One physical rowset lane reached by a runtime-filter key lineage. Source
/// cardinality and key multiplicity stay attached to the lane: summing them
/// first loses a declared-unique proof when another lineage is non-unique.
#[derive(Debug, Clone)]
pub(crate) struct PlannerRuntimeFilterSource {
    pub(crate) source: WorkSourceId,
    pub(crate) rows: paro_planner::plan::CardinalityEstimate,
    pub(crate) multiplicity: RuntimeFilterProbeMultiplicity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlannerImplementationSet {
    pub(crate) baseline: PhysicalImplementationFlavor,
    pub(crate) perfect_hash_aggregate: bool,
    pub(crate) sort_range_join: bool,
    pub(crate) classic_ie_join: bool,
    pub(crate) hash_join_build_left: bool,
    pub(crate) hash_join_build_left_runtime_filter: bool,
    pub(crate) hash_join_runtime_filter: bool,
    pub(crate) partition_aggregate_window: bool,
    pub(crate) singleton_aggregate_projection: bool,
    pub(crate) external_cross_product: bool,
}

impl PlannerImplementationSet {
    pub(crate) const STRUCTURAL: Self = Self {
        baseline: PhysicalImplementationFlavor::Structural,
        perfect_hash_aggregate: false,
        sort_range_join: false,
        classic_ie_join: false,
        hash_join_build_left: false,
        hash_join_build_left_runtime_filter: false,
        hash_join_runtime_filter: false,
        partition_aggregate_window: false,
        singleton_aggregate_projection: false,
        external_cross_product: false,
    };

    pub(crate) fn supports(self, flavor: PhysicalImplementationFlavor) -> bool {
        match flavor {
            PhysicalImplementationFlavor::PerfectHashAggregate => self.perfect_hash_aggregate,
            PhysicalImplementationFlavor::SortRangeJoin => self.sort_range_join,
            PhysicalImplementationFlavor::ClassicIeJoin => self.classic_ie_join,
            PhysicalImplementationFlavor::HashJoinBuildLeft => self.hash_join_build_left,
            PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter => {
                self.hash_join_build_left_runtime_filter
            }
            PhysicalImplementationFlavor::HashJoinRuntimeFilter => self.hash_join_runtime_filter,
            PhysicalImplementationFlavor::PartitionAggregateWindow => {
                self.partition_aggregate_window
            }
            PhysicalImplementationFlavor::SingletonAggregateProjection => {
                self.singleton_aggregate_projection
            }
            PhysicalImplementationFlavor::CrossProductExternal => self.external_cross_product,
            _ => false,
        }
    }
}
pub(crate) fn planner_cost_facts(
    plan: &OwnedLogicalPlan,
    column_stats: &dyn crate::estimate::ColumnStatisticsLookup,
    binding_ids: &BindingCatalog,
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
) -> Result<PlannerCostFacts> {
    let children = plan.children();
    let child_row_widths = children
        .iter()
        .map(|child| planner_row_width(child, scan_access_cost))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let child_materialization_risk_rows = children
        .iter()
        .map(|child| {
            child
                .stats
                .materialization_risk_cardinality
                .or_else(|| child.stats.estimated_cardinality.map(|rows| rows.max))
                .unwrap_or(1)
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let output_row_width = planner_row_width(plan, scan_access_cost);
    let hash_key_width = match &plan.operator {
        LogicalOperator::Aggregate(aggregate) => Some(
            aggregate
                .groups
                .iter()
                .map(|group| scan_access_cost.estimated_width(&group.return_type()) as u64)
                .sum(),
        ),
        LogicalOperator::Join(Join::Comparison(join))
            if join.conditions.iter().any(|condition| {
                matches!(
                    condition.comparison,
                    JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
                )
            }) =>
        {
            Some(
                join.conditions
                    .iter()
                    .filter(|condition| {
                        matches!(
                            condition.comparison,
                            JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
                        )
                    })
                    .map(|condition| {
                        scan_access_cost.estimated_width(&condition.right.return_type()) as u64
                    })
                    .sum(),
            )
        }
        _ => None,
    };
    let scan_access_width = match &plan.operator {
        LogicalOperator::Get(get) => Some(planner_scan_access_width(get, scan_access_cost)),
        _ => None,
    };
    let scan_physical_rows = match &plan.operator {
        LogicalOperator::Get(get) => get
            .table
            .as_ref()
            .and_then(|table| table.get_storage())
            // Task supply describes physical source work, not an ANALYZE
            // catalog estimate. The latter can be absent on a fully populated
            // table, or stale after an append. Capture storage's row evidence
            // on this cost-fact boundary; it is advisory, never a row bound.
            .and_then(|storage| storage.total_rows().ok())
            .map(|rows| rows as u64),
        _ => None,
    };
    let scan_work_source = match &plan.operator {
        LogicalOperator::Get(get) if get.table.is_some() => Some(WorkSourceId(get.table_index)),
        LogicalOperator::SearchScan(search) if search.get.table.is_some() => {
            Some(WorkSourceId(search.get.table_index))
        }
        LogicalOperator::FullTextFilterScan(search) if search.get.table.is_some() => {
            Some(WorkSourceId(search.get.table_index))
        }
        _ => None,
    };
    let perfect_hash = match &plan.operator {
        LogicalOperator::Aggregate(aggregate) => {
            crate::physical::aggregate_planning::plan_perfect_hash_aggregate(
                aggregate,
                &aggregate.groups,
                &aggregate.aggregates,
            )
            .map(|plan| plan.resource)
        }
        _ => None,
    };
    let topn_capacity = match &plan.operator {
        LogicalOperator::TopN(topn) => Some(
            u64::try_from(topn.limit)
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(topn.offset).unwrap_or(u64::MAX)),
        ),
        _ => None,
    };
    let runtime_filter_key_types = match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) => join
            .conditions
            .iter()
            .filter(|condition| condition.comparison == JoinComparisonType::Equal)
            .map(|condition| condition.right.return_type())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        _ => Box::new([]),
    };
    let mut result = PlannerCostFacts {
        child_row_widths,
        child_materialization_risk_rows,
        output_row_width,
        hash_key_width,
        scan_access_width,
        scan_physical_rows,
        scan_work_source,
        perfect_hash,
        topn_capacity,
        runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_build_left_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_probe_source_rows: None,
        runtime_filter_build_left_probe_source_rows: None,
        runtime_filter_probe_sources: Box::new([]),
        runtime_filter_build_left_probe_sources: Box::new([]),
        runtime_filter_build_distinct_expected: None,
        runtime_filter_build_domain_column: None,
        runtime_filter_build_key: None,
        runtime_filter_build_left_distinct_expected: None,
        runtime_filter_build_left_domain_column: None,
        runtime_filter_build_left_key: None,
        runtime_filter_key_types,
    };
    if let LogicalOperator::Join(Join::Comparison(join)) = &plan.operator {
        fill_runtime_filter_cost_facts(
            &mut result,
            join,
            RuntimeFilterInput::Owned(&join.left),
            RuntimeFilterInput::Owned(&join.right),
            column_stats,
            binding_ids,
        );
    }
    Ok(result)
}

/// Build cost facts directly from a closed native operator shell.  The child
/// tree has already been reduced to immutable `NodeState` facts by staging, so
/// recreating `BoundReference` plans here would only pay owned-IR allocation
/// without adding evidence. RF source work is derived by the same routines as
/// owned staging, using the boundary's complete source/occurrence coverage.
pub(crate) fn planner_native_cost_facts<Child>(
    operator: &LogicalOperator<Child>,
    child_sizes: (&[u64], &[u64]),
    output_row_width: u64,
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
    inputs: &[RuntimeFilterInput<'_>],
    column_stats: &dyn crate::estimate::ColumnStatisticsLookup,
    binding_ids: &BindingCatalog,
) -> Result<PlannerCostFacts> {
    let (child_materialization_risk_rows, child_row_widths) = child_sizes;
    let mut operator_child_count = 0;
    operator.visit_child_links(&mut |_| operator_child_count += 1);
    if operator_child_count != child_materialization_risk_rows.len()
        || child_materialization_risk_rows.len() != child_row_widths.len()
    {
        return Err(paro_error::internal(
            "native cost facts child stats/width arity mismatch",
        ));
    }
    let child_materialization_risk_rows = child_materialization_risk_rows
        .iter()
        .copied()
        .collect::<Box<[_]>>();
    let hash_key_width = match operator {
        LogicalOperator::Aggregate(aggregate) => Some(
            aggregate
                .groups
                .iter()
                .map(|group| scan_access_cost.estimated_width(&group.return_type()) as u64)
                .sum(),
        ),
        LogicalOperator::Join(Join::Comparison(join))
            if join.conditions.iter().any(|condition| {
                matches!(
                    condition.comparison,
                    JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
                )
            }) =>
        {
            Some(
                join.conditions
                    .iter()
                    .filter(|condition| {
                        matches!(
                            condition.comparison,
                            JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
                        )
                    })
                    .map(|condition| {
                        scan_access_cost.estimated_width(&condition.right.return_type()) as u64
                    })
                    .sum(),
            )
        }
        _ => None,
    };
    let perfect_hash = match operator {
        LogicalOperator::Aggregate(aggregate) => {
            crate::physical::aggregate_planning::plan_perfect_hash_aggregate(
                aggregate,
                &aggregate.groups,
                &aggregate.aggregates,
            )
            .map(|plan| plan.resource)
        }
        _ => None,
    };
    let topn_capacity = match operator {
        LogicalOperator::TopN(topn) => Some(
            u64::try_from(topn.limit)
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(topn.offset).unwrap_or(u64::MAX)),
        ),
        _ => None,
    };
    let runtime_filter_key_types = match operator {
        LogicalOperator::Join(Join::Comparison(join)) => join
            .conditions
            .iter()
            .filter(|condition| condition.comparison == JoinComparisonType::Equal)
            .map(|condition| condition.right.return_type())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        _ => Box::new([]),
    };
    let mut result = PlannerCostFacts {
        child_row_widths: child_row_widths.iter().copied().collect(),
        child_materialization_risk_rows,
        output_row_width,
        hash_key_width,
        scan_access_width: None,
        scan_physical_rows: None,
        scan_work_source: None,
        perfect_hash,
        topn_capacity,
        runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_build_left_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_probe_source_rows: None,
        runtime_filter_build_left_probe_source_rows: None,
        runtime_filter_probe_sources: Box::new([]),
        runtime_filter_build_left_probe_sources: Box::new([]),
        runtime_filter_build_distinct_expected: None,
        runtime_filter_build_domain_column: None,
        runtime_filter_build_key: None,
        runtime_filter_build_left_distinct_expected: None,
        runtime_filter_build_left_domain_column: None,
        runtime_filter_build_left_key: None,
        runtime_filter_key_types,
    };
    if let (LogicalOperator::Join(Join::Comparison(join)), [left, right]) = (operator, inputs) {
        fill_runtime_filter_cost_facts(&mut result, join, *left, *right, column_stats, binding_ids);
    }
    Ok(result)
}

fn fill_runtime_filter_cost_facts<Child>(
    result: &mut PlannerCostFacts,
    join: &paro_planner::operator::join::ComparisonJoin<Child>,
    left: RuntimeFilterInput<'_>,
    right: RuntimeFilterInput<'_>,
    column_stats: &dyn crate::estimate::ColumnStatisticsLookup,
    binding_ids: &BindingCatalog,
) {
    let left_keys = join
        .conditions
        .iter()
        .filter(|condition| condition.comparison == JoinComparisonType::Equal)
        .map(|condition| &condition.left)
        .collect::<Vec<_>>();
    let right_keys = join
        .conditions
        .iter()
        .filter(|condition| condition.comparison == JoinComparisonType::Equal)
        .map(|condition| &condition.right)
        .collect::<Vec<_>>();
    result.runtime_filter_probe_multiplicity = left.probe_multiplicity(&left_keys);
    result.runtime_filter_build_left_probe_multiplicity = right.probe_multiplicity(&right_keys);
    result.runtime_filter_probe_source_rows =
        runtime_filter_input_source_rows(left, left_keys.iter().copied());
    result.runtime_filter_build_left_probe_source_rows =
        runtime_filter_input_source_rows(right, right_keys.iter().copied());
    result.runtime_filter_probe_sources =
        runtime_filter_input_source_facts(left, left_keys.iter().copied()).unwrap_or_default();
    result.runtime_filter_build_left_probe_sources =
        runtime_filter_input_source_facts(right, right_keys.iter().copied()).unwrap_or_default();
    let left_bindings = left.bindings();
    let right_bindings = right.bindings();
    result.runtime_filter_build_distinct_expected =
        join_key_distinct_expected(join, column_stats, JoinKeySide::Right, &right_bindings);
    result.runtime_filter_build_left_distinct_expected =
        join_key_distinct_expected(join, column_stats, JoinKeySide::Left, &left_bindings);
    result.runtime_filter_build_domain_column =
        join_key_domain_column(join, binding_ids, JoinKeySide::Right, &right_bindings);
    result.runtime_filter_build_left_domain_column =
        join_key_domain_column(join, binding_ids, JoinKeySide::Left, &left_bindings);
    result.runtime_filter_build_key = join_key_identity(&right_keys, &right_bindings);
    result.runtime_filter_build_left_key = join_key_identity(&left_keys, &left_bindings);
}

pub(crate) fn planner_row_width_from_layout(
    layout: &paro_planner::operator::LogicalOutputLayout,
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
) -> u64 {
    layout
        .types()
        .iter()
        .map(|logical_type| scan_access_cost.estimated_width(logical_type) as u64)
        .sum::<u64>()
        .saturating_add(std::mem::size_of::<u64>() as u64)
}

pub(crate) fn planner_row_width(
    plan: &OwnedLogicalPlan,
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
) -> u64 {
    plan.types()
        .iter()
        .map(|logical_type| scan_access_cost.estimated_width(logical_type) as u64)
        .sum::<u64>()
        .saturating_add(std::mem::size_of::<u64>() as u64)
}

/// Bytes physically sourced by one base-table scan row. Virtual rowids are
/// already available from the scan cursor and therefore carry through parent
/// tuples without reading a stored column. Derived prefixes pay only their
/// bounded produced width; duplicate stored projections share one source.
pub(crate) fn planner_scan_access_width(
    get: &paro_planner::operator::Get,
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
) -> u64 {
    use paro_planner::operator::GetColumnSource;

    let mut stored_widths = std::collections::BTreeMap::<usize, u64>::new();
    for (index, source) in get.column_sources.iter().enumerate() {
        match source {
            GetColumnSource::Stored { column_id } => {
                let source_width = get
                    .column_types
                    .get(index)
                    .map(|ty| scan_access_cost.estimated_width(ty) as u64)
                    .unwrap_or(0);
                stored_widths
                    .entry(*column_id)
                    .and_modify(|width| *width = (*width).max(source_width))
                    .or_insert(source_width);
            }
            GetColumnSource::MatchedUtf8Prefix {
                source_column,
                byte_width,
            } => {
                let source_width = u64::try_from(*byte_width).unwrap_or(u64::MAX);
                stored_widths
                    .entry(*source_column)
                    .and_modify(|width| *width = (*width).max(source_width))
                    .or_insert(source_width);
            }
            GetColumnSource::VirtualRowId => {}
        }
    }
    stored_widths
        .values()
        .copied()
        .fold(0u64, u64::saturating_add)
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum JoinKeySide {
    Left,
    Right,
}

fn join_key_distinct_expected<Child>(
    join: &paro_planner::operator::join::ComparisonJoin<Child>,
    column_stats: &dyn crate::estimate::ColumnStatisticsLookup,
    side: JoinKeySide,
    bindings: &[ColumnBinding],
) -> Option<u64> {
    let mut equalities = join
        .conditions
        .iter()
        .filter(|condition| condition.comparison == JoinComparisonType::Equal);
    let condition = equalities.next()?;
    if equalities.next().is_some() {
        // Per-column membership for a composite key represents a superset of
        // build tuples, so a single-column NDV is not its retained domain.
        return None;
    }
    let expression = match side {
        JoinKeySide::Left => &condition.left,
        JoinKeySide::Right => &condition.right,
    };
    let binding = match expression {
        Expression::ColumnRef(column) if column.depth == 0 => column.binding,
        Expression::Reference(reference) => *bindings.get(reference.index)?,
        _ => return None,
    };
    column_stats
        .get(&binding)
        .map(|statistics| statistics.distinct_evidence().point)
        .filter(|distinct| *distinct > 0)
}

fn join_key_domain_column<Child>(
    join: &paro_planner::operator::join::ComparisonJoin<Child>,
    binding_ids: &BindingCatalog,
    side: JoinKeySide,
    bindings: &[ColumnBinding],
) -> Option<ColumnId> {
    let mut equalities = join
        .conditions
        .iter()
        .filter(|condition| condition.comparison == JoinComparisonType::Equal);
    let condition = equalities.next()?;
    if equalities.next().is_some() {
        return None;
    }
    let expression = match side {
        JoinKeySide::Left => &condition.left,
        JoinKeySide::Right => &condition.right,
    };
    let (binding, logical_type) = match expression {
        Expression::ColumnRef(column) if column.depth == 0 => {
            (column.binding, column.return_type.clone())
        }
        Expression::Reference(reference) => (
            *bindings.get(reference.index)?,
            reference.return_type.clone(),
        ),
        _ => return None,
    };
    binding_ids
        .get(binding.table_index, binding.column_index, &logical_type)
        .copied()
}

/// The key has a semantic identity even when no single-column statistic can
/// estimate its joint NDV. Reuse the expression encoder; include the input
/// layout so positional references cannot alias a different column binding.
pub(crate) fn join_key_identity(
    keys: &[&Expression],
    bindings: &[ColumnBinding],
) -> Option<Fingerprint> {
    if keys.is_empty() {
        return None;
    }
    let mut identity = StableFingerprintBuilder::default();
    identity.write_bytes(b"paro.runtime-filter-key.v1");
    identity.write_u64(bindings.len() as u64);
    for binding in bindings {
        let mut encoded = StableFingerprintBuilder::default();
        encoded.write_u64(binding.table_index as u64);
        encoded.write_u64(binding.column_index as u64);
        identity.write_fingerprint(encoded.finish());
    }
    identity.write_u64(keys.len() as u64);
    // Keep the executable tuple order and multiplicity. Reordering an equality
    // conjunction is logically harmless, but does not prove the same encoded
    // runtime-filter key. A conservative distinct identity is safe here.
    for key in keys {
        identity.write_fingerprint(expression_fingerprint(key));
    }
    Some(identity.finish())
}

pub(crate) fn infer_runtime_filter_probe_multiplicity<'a>(
    plan: &OwnedLogicalPlan,
    equality_expressions: impl IntoIterator<Item = &'a Expression>,
) -> RuntimeFilterProbeMultiplicity {
    let equality_expressions = equality_expressions.into_iter().collect::<Vec<_>>();
    if !equality_expressions.is_empty()
        && crate::estimate::unique_keys::expressions_cover_unique_key(plan, &equality_expressions)
    {
        return RuntimeFilterProbeMultiplicity::DeclaredUnique;
    }
    let get = match &plan.operator {
        LogicalOperator::Get(get) => get,
        LogicalOperator::Filter(filter) if filter.projection_map.is_all() => {
            let LogicalOperator::Get(get) = &filter.child.operator else {
                return RuntimeFilterProbeMultiplicity::Unknown;
            };
            get
        }
        _ => return RuntimeFilterProbeMultiplicity::Unknown,
    };
    let equality_bindings = equality_expressions
        .into_iter()
        .filter_map(|expression| match expression {
            Expression::ColumnRef(column) if column.depth == 0 => Some(column.binding),
            _ => None,
        })
        .collect::<std::collections::HashSet<_>>();
    let bindings = equality_bindings.iter().copied().collect::<Vec<_>>();
    let [binding] = bindings.as_slice() else {
        return RuntimeFilterProbeMultiplicity::Unknown;
    };
    if binding.table_index != get.table_index {
        return RuntimeFilterProbeMultiplicity::Unknown;
    }
    let column_index = binding.column_index;
    let Some(column_id) = get.stored_column(column_index) else {
        return RuntimeFilterProbeMultiplicity::Unknown;
    };
    let Some(storage) = get.table.as_ref().and_then(|table| table.get_storage()) else {
        return RuntimeFilterProbeMultiplicity::Unknown;
    };
    let Some(rows) = storage.total_rows().ok().filter(|rows| *rows > 0) else {
        return RuntimeFilterProbeMultiplicity::Unknown;
    };
    let Some(distinct) = storage
        .column_statistics(column_id)
        .map(|statistics| statistics.distinct_evidence().point)
        .filter(|distinct| *distinct > 0)
    else {
        return RuntimeFilterProbeMultiplicity::Unknown;
    };
    RuntimeFilterProbeMultiplicity::EstimatedDistinct {
        keys: distinct.min(rows as u64),
    }
}
