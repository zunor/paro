// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical implementation admission and calibrated local costing.

use super::*;

mod facts;

pub(super) use facts::{expression_cost_facts, planner_cost_facts, planner_row_width};

pub(super) fn planner_implementation_set(
    plan: &LogicalPlan,
    rowset_scan_pushdown: bool,
) -> PlannerImplementationSet {
    match &plan.operator {
        LogicalOperator::Aggregate(aggregate) => PlannerImplementationSet {
            baseline: PhysicalImplementationFlavor::HashAggregate,
            perfect_hash_aggregate:
                crate::physical::extraction::helpers::can_use_perfect_hash_aggregate(
                    aggregate,
                    &aggregate.groups,
                    &aggregate.aggregates,
                )
                .is_some(),
            singleton_aggregate_projection:
                crate::physical::extraction::aggregate::supports_singleton_aggregate_projection(
                    aggregate,
                ),
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
            let mark_has_residual = join.join_type == JoinType::Mark
                && join.conditions.iter().any(|condition| {
                    !matches!(
                        condition.comparison,
                        JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
                    )
                });
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
            let baseline = if has_hash_key && !mark_has_residual && supports_hash_type {
                PhysicalImplementationFlavor::HashJoin
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
                hash_join_runtime_filter: baseline == PhysicalImplementationFlavor::HashJoin
                    && supports_runtime_filter_auxiliary(join, rowset_scan_pushdown),
                sort_range_join:
                    crate::physical::extraction::inequality_join_gate::is_sort_range_join_candidate(
                        join,
                        plan.stats.estimated_cardinality,
                    ),
                classic_ie_join:
                    crate::physical::extraction::inequality_join_gate::is_classic_ie_join_candidate(
                        join,
                        plan.stats.estimated_cardinality,
                    ),
                ..PlannerImplementationSet::STRUCTURAL
            }
        }
        LogicalOperator::Window(window) => PlannerImplementationSet {
            baseline: PhysicalImplementationFlavor::Window,
            partition_aggregate_window:
                crate::physical::extraction::misc::supports_partition_aggregate_window(window),
            ..PlannerImplementationSet::STRUCTURAL
        },
        _ => PlannerImplementationSet::STRUCTURAL,
    }
}

pub(super) fn supports_runtime_filter_auxiliary(
    join: &paro_planner::operator::ComparisonJoin,
    rowset_scan_pushdown: bool,
) -> bool {
    if !matches!(
        join.join_type,
        JoinType::Inner | JoinType::Semi | JoinType::RightSemi | JoinType::RightAnti
    ) {
        return false;
    }
    fn probe_lineage<'a>(
        plan: &'a LogicalPlan,
        rowset_scan_pushdown: bool,
    ) -> Option<(&'a paro_planner::operator::Get, Vec<Option<usize>>)> {
        match &plan.operator {
            LogicalOperator::Get(get) => {
                Some((get, (0..get.returned_types.len()).map(Some).collect()))
            }
            LogicalOperator::Filter(filter) if rowset_scan_pushdown => {
                let (get, child_lineage) = probe_lineage(&filter.child, rowset_scan_pushdown)?;
                let filter_is_fully_pushable = [
                    filter.expressions.as_slice(),
                    get.runtime_filter_expressions.as_slice(),
                ]
                .into_iter()
                .all(|expressions| {
                    crate::physical::extraction::predicate_builder::build_predicate_tree(
                        expressions,
                        get,
                    )
                    .is_ok_and(|(_, residual)| residual.is_empty())
                });
                if !filter_is_fully_pushable {
                    return None;
                }
                let projection = filter.projection_map.to_indices(filter.child.types().len());
                Some((
                    get,
                    projection
                        .into_iter()
                        .map(|index| child_lineage.get(index).copied().flatten())
                        .collect(),
                ))
            }
            LogicalOperator::Projection(projection) => {
                let (get, child_lineage) = probe_lineage(&projection.child, rowset_scan_pushdown)?;
                let child_bindings = projection.child.get_column_bindings();
                let lineage = projection
                    .expressions
                    .iter()
                    .map(|expression| {
                        let child_index = match expression {
                            Expression::ColumnRef(column) if column.depth == 0 => child_bindings
                                .iter()
                                .position(|binding| *binding == column.binding),
                            Expression::Reference(reference) => Some(reference.index),
                            _ => None,
                        }?;
                        child_lineage.get(child_index).copied().flatten()
                    })
                    .collect::<Vec<_>>();
                Some((get, lineage))
            }
            // A CTE reference is not a rowset consumer. Crossing it requires
            // one AuxiliaryPlanRegion jointly owned by the CTE producer,
            // every reference, and the runtime-filter build. Keep the local
            // implementation closed until that ownership is represented in
            // Memo; installing a filter on only one reference is unsound.
            _ => None,
        }
    }

    let Some((get, output_lineage)) = probe_lineage(&join.left, rowset_scan_pushdown) else {
        return false;
    };
    if get.table.is_none() {
        return false;
    }
    let probe_bindings = join.left.get_column_bindings();
    join.conditions.iter().any(|condition| {
        if condition.comparison != JoinComparisonType::Equal {
            return false;
        }
        let output_index = match &condition.left {
            Expression::ColumnRef(column) if column.depth == 0 => probe_bindings
                .iter()
                .position(|binding| *binding == column.binding),
            Expression::Reference(reference) => Some(reference.index),
            _ => None,
        };
        output_index
            .and_then(|index| output_lineage.get(index).copied().flatten())
            .is_some_and(|get_index| get.stored_column(get_index).is_some())
    })
}

pub(super) fn selected_implementation_flavor(
    id: ImplementationId,
    implementations: PlannerImplementationSet,
) -> Result<PhysicalImplementationFlavor> {
    match id {
        PLANNER_BASELINE_IMPLEMENTATION => Ok(implementations.baseline),
        PLANNER_PERFECT_HASH_AGGREGATE => Ok(PhysicalImplementationFlavor::PerfectHashAggregate),
        PLANNER_SORT_RANGE_JOIN => Ok(PhysicalImplementationFlavor::SortRangeJoin),
        PLANNER_CLASSIC_IE_JOIN => Ok(PhysicalImplementationFlavor::ClassicIeJoin),
        PLANNER_SEARCH_PROVIDER => Ok(PhysicalImplementationFlavor::SearchProvider),
        PLANNER_HASH_JOIN_RUNTIME_FILTER => Ok(PhysicalImplementationFlavor::HashJoinRuntimeFilter),
        PLANNER_PARTITION_AGGREGATE_WINDOW => {
            Ok(PhysicalImplementationFlavor::PartitionAggregateWindow)
        }
        PLANNER_SINGLETON_AGGREGATE_PROJECTION => {
            Ok(PhysicalImplementationFlavor::SingletonAggregateProjection)
        }
        _ => Err(paro_error::internal(
            "winner references an implementation unavailable for its logical expression",
        )),
    }
}

pub(super) fn planner_region_contract(
    memo: &Memo,
    facet: Option<Fingerprint>,
    artifact_kind: Option<RegionArtifactKind>,
) -> Result<Option<RegionCandidateContract>> {
    let Some(facet) = facet else {
        if artifact_kind.is_some() {
            return Err(paro_error::internal(
                "region artifact has no owning planning facet",
            ));
        }
        return Ok(None);
    };
    let region = memo.regions().region_for_facet(facet).ok_or_else(|| {
        paro_error::internal("physical implementation references an unowned planning facet")
    })?;
    let artifacts = artifact_kind
        .map(|kind| {
            vec![RegionOwnedArtifact {
                fingerprint: facet,
                kind,
            }]
            .into_boxed_slice()
        })
        .unwrap_or_default();
    Ok(Some(RegionCandidateContract {
        region,
        facets: vec![facet].into_boxed_slice(),
        artifacts,
    }))
}

const OP_HASH_BUILD_ROW: OpClassId = OpClassId(1);
const OP_HASH_PROBE_ROW: OpClassId = OpClassId(2);
const OP_NESTED_LOOP_PAIR: OpClassId = OpClassId(3);
const OP_SORT_COMPARE: OpClassId = OpClassId(4);
pub(super) const OP_RANGE_JOIN_ROW: OpClassId = OpClassId(5);
pub(super) const OP_IE_JOIN_ROW: OpClassId = OpClassId(6);
const OP_HASH_AGGREGATE_ROW: OpClassId = OpClassId(7);
const OP_HASH_AGGREGATE_GROUP: OpClassId = OpClassId(8);
const OP_PERFECT_AGGREGATE_ROW: OpClassId = OpClassId(9);
const OP_PERFECT_AGGREGATE_SLOT: OpClassId = OpClassId(10);
// 11 and 12 are the stable runtime-filter classes in `calibration`; keep the
// planner-local namespace disjoint so calibration cannot silently price an
// unrelated operator with runtime-filter coefficients.
const OP_WINDOW_ROW: OpClassId = OpClassId(13);
const OP_PARTITION_AGGREGATE_WINDOW_ROW: OpClassId = OpClassId(14);
const OP_SINGLETON_AGGREGATE_PROJECT_ROW: OpClassId = OpClassId(15);

pub(super) fn multiply_work(left: CompactRange, right: CompactRange) -> Result<CompactRange> {
    CompactRange::new(
        left.lower * right.lower,
        left.expected * right.expected,
        left.upper * right.upper,
    )
}

pub(super) fn sort_work(rows: CompactRange) -> Result<CompactRange> {
    let comparisons = |value: f64| {
        if value <= 1.0 {
            value
        } else {
            value * value.log2()
        }
    };
    CompactRange::new(
        comparisons(rows.lower),
        comparisons(rows.expected),
        comparisons(rows.upper),
    )
}

pub(super) fn implementation_cost(
    metadata: &PlannerOperatorMetadata,
    facts: &ResolvedPlannerCostFacts,
    flavor: PhysicalImplementationFlavor,
    calibration: &MachineCalibrationBundle,
) -> Result<SearchCost> {
    let mut work = LocalOperatorWork::default();
    let mut peak_memory_upper;
    match flavor {
        PhysicalImplementationFlavor::Structural => {
            return refreshed_structural_cost(metadata, facts)
        }
        PhysicalImplementationFlavor::SearchProvider => {
            return Err(paro_error::internal(
                "search provider cost must come from its physical payload",
            ));
        }
        _ => add_tuple_byte_work(&mut work, facts)?,
    }
    match flavor {
        PhysicalImplementationFlavor::Structural | PhysicalImplementationFlavor::SearchProvider => {
            unreachable!()
        }
        PhysicalImplementationFlavor::HashAggregate => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_HASH_AGGREGATE_ROW, input)?;
            work.add(OP_HASH_AGGREGATE_GROUP, facts.output_rows)?;
            peak_memory_upper = facts
                .output_rows_hard_upper
                .unwrap_or(u64::MAX)
                .saturating_mul(facts.output_row_width.saturating_add(16));
        }
        PhysicalImplementationFlavor::PerfectHashAggregate => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            let slots = facts.perfect_hash_slots.ok_or_else(|| {
                paro_error::internal("perfect-hash candidate lost its proven key domain")
            })?;
            work.add(OP_PERFECT_AGGREGATE_ROW, input)?;
            work.add(
                OP_PERFECT_AGGREGATE_SLOT,
                CompactRange::point(slots as f64)?,
            )?;
            peak_memory_upper = slots.saturating_mul(facts.output_row_width.saturating_add(16));
        }
        PhysicalImplementationFlavor::SingletonAggregateProjection => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_SINGLETON_AGGREGATE_PROJECT_ROW, input)?;
            peak_memory_upper = 0;
        }
        PhysicalImplementationFlavor::Window => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_SORT_COMPARE, sort_work(input)?)?;
            work.add(OP_WINDOW_ROW, input)?;
            peak_memory_upper = facts
                .child_rows_hard_upper
                .first()
                .copied()
                .flatten()
                .unwrap_or(u64::MAX)
                .saturating_mul(facts.output_row_width.max(32));
        }
        PhysicalImplementationFlavor::PartitionAggregateWindow => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_PARTITION_AGGREGATE_WINDOW_ROW, input)?;
            peak_memory_upper = facts
                .child_rows_hard_upper
                .first()
                .copied()
                .flatten()
                .unwrap_or(u64::MAX)
                .saturating_mul(facts.output_row_width.max(32));
        }
        PhysicalImplementationFlavor::HashJoin
        | PhysicalImplementationFlavor::HashJoinRuntimeFilter => {
            let left = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(CompactRange::ZERO);
            let right = facts
                .child_rows
                .get(1)
                .copied()
                .unwrap_or(CompactRange::ZERO);
            work.add(OP_HASH_BUILD_ROW, right)?;
            let probe = if flavor == PhysicalImplementationFlavor::HashJoinRuntimeFilter {
                work.add(OP_RUNTIME_FILTER_BUILD_ROW, right)?;
                work.add(OP_RUNTIME_FILTER_APPLY_ROW, left)?;
                runtime_filtered_probe_work(left, right)?
            } else {
                left
            };
            work.add(OP_HASH_PROBE_ROW, probe.checked_add(facts.output_rows)?)?;
            let right_hard_upper = facts
                .child_rows_hard_upper
                .get(1)
                .copied()
                .flatten()
                .unwrap_or(u64::MAX);
            peak_memory_upper =
                right_hard_upper.saturating_mul(facts.output_row_width.saturating_div(2).max(32));
            if flavor == PhysicalImplementationFlavor::HashJoinRuntimeFilter {
                // The execution policy freezes at most a bounded exact domain
                // before degrading to min/max. Charge the upper envelope here
                // so the auxiliary artifact participates in grant admission.
                peak_memory_upper = peak_memory_upper
                    .saturating_add(right_hard_upper.min(65_536).saturating_mul(24));
            }
        }
        PhysicalImplementationFlavor::NestedLoopJoin => {
            let left = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(CompactRange::ZERO);
            let right = facts
                .child_rows
                .get(1)
                .copied()
                .unwrap_or(CompactRange::ZERO);
            work.add(OP_NESTED_LOOP_PAIR, multiply_work(left, right)?)?;
            work.add(OP_RANGE_JOIN_ROW, facts.output_rows)?;
            peak_memory_upper = 0;
        }
        PhysicalImplementationFlavor::SortRangeJoin
        | PhysicalImplementationFlavor::ClassicIeJoin => {
            let left = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(CompactRange::ZERO);
            let right = facts
                .child_rows
                .get(1)
                .copied()
                .unwrap_or(CompactRange::ZERO);
            work.add(
                OP_SORT_COMPARE,
                sort_work(left)?.checked_add(sort_work(right)?)?,
            )?;
            work.add(
                if flavor == PhysicalImplementationFlavor::SortRangeJoin {
                    OP_RANGE_JOIN_ROW
                } else {
                    OP_IE_JOIN_ROW
                },
                left.checked_add(right)?.checked_add(facts.output_rows)?,
            )?;
            let left_hard = facts
                .child_rows_hard_upper
                .first()
                .copied()
                .flatten()
                .unwrap_or(u64::MAX);
            let right_hard = facts
                .child_rows_hard_upper
                .get(1)
                .copied()
                .flatten()
                .unwrap_or(u64::MAX);
            peak_memory_upper = left_hard
                .saturating_add(right_hard)
                .saturating_mul(facts.output_row_width.max(32));
        }
    }
    let mut cost = calibration.fold(&work)?;
    cost.peak_memory_upper = peak_memory_upper;
    cost.validate()?;
    Ok(cost)
}

fn refreshed_structural_cost(
    metadata: &PlannerOperatorMetadata,
    facts: &ResolvedPlannerCostFacts,
) -> Result<SearchCost> {
    if matches!(
        metadata.operator_type,
        LogicalOperatorType::SearchScan
            | LogicalOperatorType::FullTextFilterScan
            | LogicalOperatorType::ExternalProject
            | LogicalOperatorType::ExternalTable
    ) {
        return Ok(metadata.local_cost);
    }
    let width_factor = (facts.output_row_width as f64 / 32.0).max(1.0);
    let child_count = facts.child_rows.len() as f64;
    let expected = facts.output_rows.expected.max(1.0) * width_factor + child_count;
    let upper =
        facts.output_rows.upper.max(facts.output_rows.expected) * width_factor + child_count;
    let range = CompactRange::new(1.0_f64.min(expected), expected, upper.max(expected))?;
    let mut cost = SearchCost {
        score: ScoreSummary {
            range,
            risk_adjusted: expected + (upper - expected) * 0.5,
        },
        critical_path: range,
        ..SearchCost::ZERO
    };
    if metadata.grant_dependency == GrantDependencyDescriptor::Sensitive {
        let (rows, width) = if metadata.operator_type == LogicalOperatorType::CrossProduct {
            (
                facts
                    .child_rows_hard_upper
                    .get(1)
                    .copied()
                    .flatten()
                    .unwrap_or(u64::MAX),
                facts
                    .child_row_widths
                    .get(1)
                    .copied()
                    .unwrap_or(facts.output_row_width),
            )
        } else {
            (
                facts.output_rows_hard_upper.unwrap_or(u64::MAX),
                facts.output_row_width,
            )
        };
        cost.peak_memory_upper = rows.saturating_mul(width);
        let resident_expected = if metadata.operator_type == LogicalOperatorType::CrossProduct {
            facts
                .child_rows
                .get(1)
                .copied()
                .unwrap_or(CompactRange::ZERO)
                .expected
        } else {
            facts.output_rows.expected
        };
        cost.resources_expected[ResourceDimension::MemoryWrite as usize] =
            resident_expected * width as f64;
        cost.resources_risk_upper[ResourceDimension::MemoryWrite as usize] =
            cost.peak_memory_upper as f64;
    }
    cost.validate()?;
    Ok(cost)
}

pub(super) fn add_tuple_byte_work(
    work: &mut LocalOperatorWork,
    facts: &ResolvedPlannerCostFacts,
) -> Result<()> {
    const BYTE_BLOCK: f64 = 32.0;

    let mut blocks = scaled_work(
        facts.output_rows,
        facts.output_row_width as f64 / BYTE_BLOCK,
    )?;
    for (rows, width) in facts.child_rows.iter().zip(facts.child_row_widths.iter()) {
        blocks = blocks.checked_add(scaled_work(*rows, *width as f64 / BYTE_BLOCK)?)?;
    }
    work.add(OP_TUPLE_BYTE_BLOCK, blocks)
}

pub(super) fn scaled_work(range: CompactRange, factor: f64) -> Result<CompactRange> {
    CompactRange::new(
        range.lower * factor,
        range.expected * factor,
        range.upper * factor,
    )
}

pub(super) fn runtime_filtered_probe_work(
    probe: CompactRange,
    build: CompactRange,
) -> Result<CompactRange> {
    let retained = |probe_rows: f64, build_rows: f64| {
        if probe_rows <= 0.0 {
            return 0.0;
        }
        let ratio = (build_rows / probe_rows).clamp(0.0, 1.0);
        // Without a joint histogram the expected benefit is deliberately
        // damped. The upper bound retains the no-benefit fallback.
        probe_rows * ratio.sqrt().clamp(0.1, 1.0)
    };
    let expected = retained(probe.expected, build.expected);
    CompactRange::new(0.0, expected, probe.upper.max(expected))
}

pub(super) fn is_contextual_operator(operator: &LogicalOperator) -> bool {
    matches!(
        operator,
        LogicalOperator::Join(_)
            | LogicalOperator::DependentJoin(_)
            | LogicalOperator::Aggregate(_)
            | LogicalOperator::MaterializedCTE(_)
            | LogicalOperator::RecursiveCTE(_)
            | LogicalOperator::GraphMatch(_)
            | LogicalOperator::GraphExpand(_)
            | LogicalOperator::SearchScan(_)
            | LogicalOperator::FullTextFilterScan(_)
    )
}

pub(super) fn required_region_kind(operator: &LogicalOperator) -> Option<RegionFacetKind> {
    match operator {
        LogicalOperator::DependentJoin(_) => Some(RegionFacetKind::Parameterization),
        LogicalOperator::MaterializedCTE(_) => Some(RegionFacetKind::Sharing),
        LogicalOperator::RecursiveCTE(_) => Some(RegionFacetKind::Recursion),
        _ => None,
    }
}

pub(super) fn planner_region_facet(
    kind: RegionFacetKind,
    criticality: FacetCriticality,
    logical: LogicalExprId,
    operator: Fingerprint,
    scope: BTreeSet<GroupId>,
) -> RegionFacet {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.planning-region.facet.v1");
    fingerprint.write_u64(kind as u64);
    fingerprint.write_u64(criticality as u64);
    fingerprint.write_u64(logical.0 as u64);
    fingerprint.write_fingerprint(operator);
    RegionFacet {
        fingerprint: fingerprint.finish(),
        kind,
        criticality,
        priority: match criticality {
            FacetCriticality::Required => 100 + kind as u16,
            FacetCriticality::Optional => 1_000 + kind as u16,
        },
        scope,
    }
}

pub(super) fn planner_operator_cost(
    plan: &LogicalPlan,
    child_count: usize,
    output_rows_hard_upper: Option<u64>,
    child_rows_hard_upper: &[Option<u64>],
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
) -> Result<SearchCost> {
    match &plan.operator {
        LogicalOperator::SearchScan(scan) => return search_decision_cost(&scan.decision),
        LogicalOperator::FullTextFilterScan(scan) => return search_decision_cost(&scan.decision),
        LogicalOperator::ExternalProject(project) => {
            return external_operator_cost(project.cost, plan.stats.estimated_cardinality)
        }
        LogicalOperator::ExternalTable(table) => {
            return external_operator_cost(table.cost, plan.stats.estimated_cardinality)
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
    let expected = expected_rows * width_factor + child_count as f64;
    let upper_rows = plan
        .stats
        .estimated_cardinality
        .map(|cardinality| cardinality.max as f64)
        .unwrap_or(expected_rows * 4.0)
        .max(expected_rows);
    let upper = upper_rows * width_factor + child_count as f64;
    let mut cost = SearchCost {
        score: ScoreSummary {
            range: CompactRange::new(1.0, expected, upper)?,
            risk_adjusted: expected + (upper - expected) * 0.5,
        },
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
            _ => plan,
        };
        let row_width = planner_row_width(resident_plan, scan_access_cost);
        let resident_rows = match &plan.operator {
            LogicalOperator::Join(Join::Cross(_)) => child_rows_hard_upper
                .get(1)
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

pub(super) fn search_decision_cost(
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
        critical_path: range,
        ..SearchCost::ZERO
    };
    cost.resources_expected[ResourceDimension::RandomIo as usize] = expected;
    cost.resources_risk_upper[ResourceDimension::RandomIo as usize] = range.upper;
    cost.validate()?;
    Ok(cost)
}

pub(super) fn external_operator_cost(
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
        critical_path: range,
        external_workers: crate::cascades::ids::ExternalWorkerRequirementSetId(1),
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
