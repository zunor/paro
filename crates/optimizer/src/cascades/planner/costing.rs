// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical implementation admission and calibrated local costing.

use super::*;

mod facts;

pub(super) use facts::{
    expression_cost_facts, planner_cost_facts, planner_row_width, planner_scan_access_width,
};

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
                    && supports_build_left_runtime_filter_auxiliary(join, rowset_scan_pushdown),
                hash_join_runtime_filter: has_hash_key
                    && supports_hash_type
                    && join.build_side_constraint.allows_right()
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

pub(super) fn supports_runtime_filter_auxiliary(
    join: &paro_planner::operator::ComparisonJoin,
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

    let probe_bindings = join.left.get_column_bindings();
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
            runtime_filter_probe_lineages(&join.left, index)
                .is_some_and(|lineage| !lineage.sources.is_empty())
        })
    })
}

pub(super) fn supports_build_left_runtime_filter_auxiliary(
    join: &paro_planner::operator::ComparisonJoin,
    rowset_scan_pushdown: bool,
) -> bool {
    if !rowset_scan_pushdown
        || join.anti_join_mode != AntiJoinMode::Regular
        || !matches!(
            join.join_type,
            JoinType::Inner | JoinType::Left | JoinType::Semi | JoinType::Anti
        )
    {
        return false;
    }

    let probe_bindings = join.right.get_column_bindings();
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
            .and_then(|index| runtime_filter_probe_lineages(&join.right, index))
            .is_some_and(|lineage| !lineage.sources.is_empty())
    })
}

struct RuntimeFilterProbeLineage<'a> {
    sources: Vec<&'a LogicalPlan>,
}

fn runtime_filter_work_sources(
    plan: &LogicalPlan,
    expression: &Expression,
) -> Option<Vec<WorkSourceId>> {
    let bindings = plan.get_column_bindings();
    let output_index = match expression {
        Expression::ColumnRef(column) if column.depth == 0 => bindings
            .iter()
            .position(|binding| *binding == column.binding),
        Expression::Reference(reference) => Some(reference.index),
        _ => None,
    }?;
    let lineage = runtime_filter_probe_lineages(plan, output_index)?;
    let mut sources = lineage
        .sources
        .into_iter()
        .map(|source| {
            let get = match &source.operator {
                LogicalOperator::Get(get) => get,
                LogicalOperator::SearchScan(search) => &search.get,
                LogicalOperator::FullTextFilterScan(search) => &search.get,
                _ => return None,
            };
            Some(WorkSourceId(get.table_index))
        })
        .collect::<Option<Vec<_>>>()?;
    sources.sort_unstable();
    sources.dedup();
    (!sources.is_empty()).then_some(sources)
}

fn common_runtime_filter_work_sources<'a>(
    plan: &LogicalPlan,
    expressions: impl IntoIterator<Item = &'a Expression>,
) -> Option<Box<[WorkSourceId]>> {
    let mut sources = expressions
        .into_iter()
        .map(|expression| runtime_filter_work_sources(plan, expression));
    let source = sources.next()??;
    sources
        .all(|candidate| candidate.as_ref() == Some(&source))
        .then(|| source.into_boxed_slice())
}

pub(super) fn runtime_filter_probe_work_sources(
    join: &paro_planner::operator::ComparisonJoin,
) -> Option<Box<[WorkSourceId]>> {
    common_runtime_filter_work_sources(
        &join.left,
        join.conditions
            .iter()
            .filter(|condition| condition.comparison == JoinComparisonType::Equal)
            .map(|condition| &condition.left),
    )
}

pub(super) fn runtime_filter_build_left_probe_work_sources(
    join: &paro_planner::operator::ComparisonJoin,
) -> Option<Box<[WorkSourceId]>> {
    common_runtime_filter_work_sources(
        &join.right,
        join.conditions
            .iter()
            .filter(|condition| condition.comparison == JoinComparisonType::Equal)
            .map(|condition| &condition.right),
    )
}

fn runtime_filter_probe_lineages(
    plan: &LogicalPlan,
    output_index: usize,
) -> Option<RuntimeFilterProbeLineage<'_>> {
    match &plan.operator {
        LogicalOperator::Get(get)
            if get.table.is_some() && get.stored_column(output_index).is_some() =>
        {
            Some(RuntimeFilterProbeLineage {
                sources: vec![plan],
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
                sources: vec![plan],
            })
        }
        LogicalOperator::FullTextFilterScan(search) if search.get.table.is_some() => {
            let source_index = *search
                .projection_map
                .to_indices(search.get.returned_types.len())
                .get(output_index)?;
            search.get.stored_column(source_index)?;
            Some(RuntimeFilterProbeLineage {
                sources: vec![plan],
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
            if matches!(inner.join_type, JoinType::Inner | JoinType::Left)
                && inner.duplicate_eliminated_columns.is_empty()
                && !inner.delim_flipped =>
        {
            let left_projection = inner
                .left_projection_map
                .to_indices(inner.left.types().len());
            if let Some(&child_index) = left_projection.get(output_index) {
                return runtime_filter_probe_lineages(&inner.left, child_index);
            }
            if inner.join_type == JoinType::Left {
                // A left outer join preserves every row from its left child.
                // Sideways filtering a key whose output lineage stays on that
                // side can only remove rows that the later consuming join
                // would reject; tracing into the nullable build side would
                // instead change whether a preserved row is matched.
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

fn runtime_filter_source_rows(
    plan: &LogicalPlan,
    expressions: impl IntoIterator<Item = Expression>,
) -> Option<paro_planner::plan::CardinalityEstimate> {
    let probe_bindings = plan.get_column_bindings();
    expressions.into_iter().find_map(|expression| {
        let output_index = match expression {
            Expression::ColumnRef(column) if column.depth == 0 => probe_bindings
                .iter()
                .position(|binding| *binding == column.binding),
            Expression::Reference(reference) => Some(reference.index),
            _ => None,
        }?;
        let lineage = runtime_filter_probe_lineages(plan, output_index)?;
        lineage.sources.into_iter().try_fold(
            paro_planner::plan::CardinalityEstimate::exact(0),
            |sum, source| {
                let rows = source.stats.estimated_cardinality?;
                Some(paro_planner::plan::CardinalityEstimate {
                    min: sum.min.saturating_add(rows.min),
                    expected: sum.expected.saturating_add(rows.expected),
                    max: sum.max.saturating_add(rows.max),
                })
            },
        )
    })
}

pub(super) fn runtime_filter_probe_source_rows(
    join: &paro_planner::operator::ComparisonJoin,
) -> Option<paro_planner::plan::CardinalityEstimate> {
    runtime_filter_source_rows(
        &join.left,
        join.conditions
            .iter()
            .filter(|condition| condition.comparison == JoinComparisonType::Equal)
            .map(|condition| condition.left.clone()),
    )
}

pub(super) fn runtime_filter_build_left_probe_source_rows(
    join: &paro_planner::operator::ComparisonJoin,
) -> Option<paro_planner::plan::CardinalityEstimate> {
    runtime_filter_source_rows(
        &join.right,
        join.conditions
            .iter()
            .filter(|condition| condition.comparison == JoinComparisonType::Equal)
            .map(|condition| condition.right.clone()),
    )
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
        PLANNER_HASH_JOIN_BUILD_LEFT => Ok(PhysicalImplementationFlavor::HashJoinBuildLeft),
        PLANNER_HASH_JOIN_BUILD_LEFT_RUNTIME_FILTER => {
            Ok(PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter)
        }
        PLANNER_PARTITION_AGGREGATE_WINDOW => {
            Ok(PhysicalImplementationFlavor::PartitionAggregateWindow)
        }
        PLANNER_SINGLETON_AGGREGATE_PROJECTION => {
            Ok(PhysicalImplementationFlavor::SingletonAggregateProjection)
        }
        PLANNER_EXTERNAL_CROSS_PRODUCT => Ok(PhysicalImplementationFlavor::CrossProductExternal),
        _ => Err(paro_error::internal(
            "winner references an implementation unavailable for its logical expression",
        )),
    }
}

pub(super) fn planner_region_contract(
    memo: &Memo,
    facet: Option<Fingerprint>,
) -> Result<Option<RegionCandidateContract>> {
    let Some(facet) = facet else {
        return Ok(None);
    };
    let region = memo.regions().region_for_facet(facet).ok_or_else(|| {
        paro_error::internal("physical implementation references an unowned planning facet")
    })?;
    Ok(Some(RegionCandidateContract {
        region,
        facets: vec![facet].into_boxed_slice(),
        artifacts: Box::new([]),
        artifact_dependencies: Box::new([]),
    }))
}

pub(super) fn planner_runtime_filter_region_contract(
    memo: &Memo,
    facet: Option<Fingerprint>,
    build: RegionBoundaryEndpoint,
    probe: RegionBoundaryEndpoint,
) -> Result<RegionCandidateContract> {
    let facet = facet.ok_or_else(|| {
        paro_error::internal("runtime-filter artifact has no owning planning facet")
    })?;
    let region = memo.regions().region_for_facet(facet).ok_or_else(|| {
        paro_error::internal("physical implementation references an unowned planning facet")
    })?;
    Ok(RegionCandidateContract {
        region,
        facets: vec![facet].into_boxed_slice(),
        artifacts: Box::new([RegionOwnedArtifact {
            fingerprint: facet,
            kind: RegionArtifactKind::RuntimeFilter,
        }]),
        artifact_dependencies: Box::new([RegionArtifactDependencyContract {
            artifact: facet,
            producer: build,
            consumer: probe,
            kind: RegionDependencyKind::ControlWaitComplete,
        }]),
    })
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

fn topn_work(rows: CompactRange, capacity: u64) -> Result<CompactRange> {
    let comparisons_per_row = (capacity.max(2) as f64).log2();
    scaled_work(rows, comparisons_per_row)
}

pub(super) fn implementation_cost(
    metadata: &PlannerOperatorMetadata,
    facts: &ResolvedPlannerCostFacts,
    flavor: PhysicalImplementationFlavor,
    calibration: &MachineCalibrationBundle,
    max_concurrent_tasks: u16,
) -> Result<SearchCost> {
    let mut work = LocalOperatorWork::default();
    let peak_memory_upper;
    match flavor {
        PhysicalImplementationFlavor::Structural => {
            return refreshed_structural_cost(metadata, facts, calibration, max_concurrent_tasks)
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
        PhysicalImplementationFlavor::AdaptiveSort => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_SORT_COMPARE, sort_work(input)?)?;
            peak_memory_upper = facts
                .child_rows_hard_upper
                .first()
                .copied()
                .flatten()
                .unwrap_or(u64::MAX)
                .saturating_mul(
                    facts
                        .child_row_widths
                        .first()
                        .copied()
                        .unwrap_or(facts.output_row_width)
                        .max(1),
                );
        }
        PhysicalImplementationFlavor::HeapTopN => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            let capacity = facts.topn_capacity.ok_or_else(|| {
                paro_error::internal("heap TopN candidate lost its bounded capacity")
            })?;
            work.add(OP_SORT_COMPARE, topn_work(input, capacity)?)?;
            peak_memory_upper = capacity.saturating_mul(
                facts
                    .child_row_widths
                    .first()
                    .copied()
                    .unwrap_or(facts.output_row_width)
                    .max(1),
            );
        }
        PhysicalImplementationFlavor::HashAggregate => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_HASH_AGGREGATE_ROW, input)?;
            add_hash_key_byte_work(&mut work, input, facts.hash_key_width)?;
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
            let resource = facts.perfect_hash.ok_or_else(|| {
                paro_error::internal("perfect-hash candidate lost its proven key domain")
            })?;
            work.add(OP_PERFECT_AGGREGATE_ROW, input)?;
            work.add(
                OP_PERFECT_AGGREGATE_SLOT,
                CompactRange::point(resource.slots as f64)?,
            )?;
            peak_memory_upper = resource.memory.preferred_memory_bytes().unwrap_or(u64::MAX);
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
        | PhysicalImplementationFlavor::HashJoinBuildLeft
        | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
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
            let build_left = matches!(
                flavor,
                PhysicalImplementationFlavor::HashJoinBuildLeft
                    | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
            );
            let build = if build_left { left } else { right };
            let ordinary_probe = if build_left { right } else { left };
            let build_index = usize::from(!build_left);
            let build_materialization_risk = facts
                .child_materialization_risk_rows
                .get(build_index)
                .copied()
                .unwrap_or(0) as f64;
            let build_work = CompactRange::new(
                build.lower,
                build.expected,
                build.upper.max(build_materialization_risk),
            )?;
            let build_hard_upper = facts
                .child_rows_hard_upper
                .get(build_index)
                .copied()
                .flatten();
            work.add(OP_HASH_BUILD_ROW, build_work)?;
            let probe = if flavor == PhysicalImplementationFlavor::HashJoinRuntimeFilter {
                let build_domain = runtime_filter_build_domain(facts, right)?;
                work.add(OP_RUNTIME_FILTER_BUILD_ROW, build_work)?;
                // A non-local runtime filter runs at the traced rowset source,
                // before any intervening joins. Price every source-row lookup;
                // charging only the already-reduced logical child makes a
                // second sideways filter appear almost free and can select a
                // physically slower plan.
                work.add(
                    OP_RUNTIME_FILTER_APPLY_ROW,
                    facts.runtime_filter_probe_source_rows.unwrap_or(left),
                )?;
                let resource = crate::physical::RuntimeFilterResourceContract::for_keys(
                    &facts.runtime_filter_key_types,
                    max_concurrent_tasks,
                )?;
                let exact_expected = resource.guarantees_exact_single_key(build_hard_upper)
                    || resource.expects_exact_single_key(build_domain.expected);
                runtime_filtered_probe_work(
                    left,
                    build_domain,
                    facts.runtime_filter_probe_multiplicity,
                    exact_expected,
                )?
            } else if flavor == PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter {
                let build_domain = CompactRange::new(0.0, left.expected, left.upper)?;
                let source = facts
                    .runtime_filter_build_left_probe_source_rows
                    .unwrap_or(right);
                work.add(OP_RUNTIME_FILTER_BUILD_ROW, build_work)?;
                work.add(OP_RUNTIME_FILTER_APPLY_ROW, source)?;
                let resource = crate::physical::RuntimeFilterResourceContract::for_keys(
                    &facts.runtime_filter_key_types,
                    max_concurrent_tasks,
                )?;
                let exact_expected = resource.guarantees_exact_single_key(build_hard_upper)
                    || resource.expects_exact_single_key(build_domain.expected);
                runtime_filtered_probe_work(
                    source,
                    build_domain,
                    facts.runtime_filter_build_left_probe_multiplicity,
                    exact_expected,
                )?
            } else {
                ordinary_probe
            };
            add_hash_key_byte_work(
                &mut work,
                build_work.checked_add(probe)?,
                facts.hash_key_width,
            )?;
            work.add(OP_HASH_PROBE_ROW, probe.checked_add(facts.output_rows)?)?;
            peak_memory_upper = build_hard_upper
                .unwrap_or(u64::MAX)
                .saturating_mul(facts.output_row_width.saturating_div(2).max(32));
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
            peak_memory_upper = if flavor == PhysicalImplementationFlavor::CrossProductExternal {
                0
            } else {
                facts
                    .child_rows_hard_upper
                    .get(1)
                    .copied()
                    .flatten()
                    .unwrap_or(u64::MAX)
                    .saturating_mul(
                        facts
                            .child_row_widths
                            .get(1)
                            .copied()
                            .unwrap_or(facts.output_row_width)
                            .max(1),
                    )
            };
        }
        PhysicalImplementationFlavor::CrossProductInMemory
        | PhysicalImplementationFlavor::CrossProductExternal => {
            let right = facts
                .child_rows
                .get(1)
                .copied()
                .unwrap_or(CompactRange::ZERO);
            // Materialization writes the build once and the probe reads it for
            // every left row. The tuple-byte base charge already includes the
            // logical output; external execution adds a second build pass for
            // serialization and replay.
            if flavor == PhysicalImplementationFlavor::CrossProductExternal {
                let width = facts
                    .child_row_widths
                    .get(1)
                    .copied()
                    .unwrap_or(facts.output_row_width);
                work.add(
                    OP_TUPLE_BYTE_BLOCK,
                    scaled_work(right, width as f64 / 16.0)?,
                )?;
            }
            peak_memory_upper = facts
                .child_rows_hard_upper
                .get(1)
                .copied()
                .flatten()
                .unwrap_or(u64::MAX)
                .saturating_mul(
                    facts
                        .child_row_widths
                        .get(1)
                        .copied()
                        .unwrap_or(facts.output_row_width)
                        .max(1),
                );
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
    let mut cost = calibration.fold_for_tasks(
        &work,
        implementation_parallelism(flavor),
        max_concurrent_tasks,
    )?;
    let retained_memory_target = expected_retained_memory_target(facts, flavor, peak_memory_upper);
    apply_execution_memory_contract(
        metadata,
        flavor,
        peak_memory_upper,
        retained_memory_target,
        max_concurrent_tasks,
        &mut cost,
    )?;
    if flavor == PhysicalImplementationFlavor::CrossProductExternal {
        let right = facts
            .child_rows
            .get(1)
            .copied()
            .unwrap_or(CompactRange::ZERO);
        let width = facts
            .child_row_widths
            .get(1)
            .copied()
            .unwrap_or(facts.output_row_width)
            .max(1);
        add_spill_cost(
            &mut cost,
            (right.expected * width as f64).min(u64::MAX as f64) as u64,
        )?;
    }
    cost.validate()?;
    Ok(cost)
}

/// Isolate the full-source predicate-evaluation work already charged by a
/// runtime-filter implementation. Candidate composition removes this term and
/// rebuilds all predicates on the same source in selectivity order, matching
/// the staged rowset evaluator without discounting independent join work.
pub(super) fn runtime_filter_apply_cost(
    facts: &ResolvedPlannerCostFacts,
    flavor: PhysicalImplementationFlavor,
    calibration: &MachineCalibrationBundle,
    max_concurrent_tasks: u16,
) -> Result<Option<SearchCost>> {
    let rows = match flavor {
        PhysicalImplementationFlavor::HashJoinRuntimeFilter => {
            facts.runtime_filter_probe_source_rows.unwrap_or_else(|| {
                facts
                    .child_rows
                    .first()
                    .copied()
                    .unwrap_or(CompactRange::ZERO)
            })
        }
        PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter => facts
            .runtime_filter_build_left_probe_source_rows
            .or_else(|| facts.child_rows.get(1).copied())
            .unwrap_or(CompactRange::ZERO),
        _ => return Ok(None),
    };
    let mut work = LocalOperatorWork::default();
    work.add(OP_RUNTIME_FILTER_APPLY_ROW, rows)?;
    Ok(Some(calibration.fold_for_tasks(
        &work,
        ParallelWorkProfile::Pipeline,
        max_concurrent_tasks,
    )?))
}

fn implementation_parallelism(flavor: PhysicalImplementationFlavor) -> ParallelWorkProfile {
    match flavor {
        PhysicalImplementationFlavor::AdaptiveSort
        | PhysicalImplementationFlavor::HeapTopN
        | PhysicalImplementationFlavor::HashAggregate
        | PhysicalImplementationFlavor::PerfectHashAggregate
        | PhysicalImplementationFlavor::Window
        | PhysicalImplementationFlavor::PartitionAggregateWindow
        | PhysicalImplementationFlavor::SortRangeJoin
        | PhysicalImplementationFlavor::ClassicIeJoin => ParallelWorkProfile::BlockingMerge,
        PhysicalImplementationFlavor::HashJoin
        | PhysicalImplementationFlavor::HashJoinBuildLeft
        | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        | PhysicalImplementationFlavor::HashJoinRuntimeFilter
        | PhysicalImplementationFlavor::NestedLoopJoin
        | PhysicalImplementationFlavor::CrossProductInMemory
        | PhysicalImplementationFlavor::CrossProductExternal => ParallelWorkProfile::Pipeline,
        PhysicalImplementationFlavor::SingletonAggregateProjection
        | PhysicalImplementationFlavor::Structural
        | PhysicalImplementationFlavor::SearchProvider => ParallelWorkProfile::Serial,
    }
}

fn apply_execution_memory_contract(
    metadata: &PlannerOperatorMetadata,
    flavor: PhysicalImplementationFlavor,
    retained_memory_upper: u64,
    retained_memory_target: u64,
    max_concurrent_tasks: u16,
    cost: &mut SearchCost,
) -> Result<()> {
    use crate::physical::resources::{
        ExecutionMemoryContract, BLOCKING_FIXED_SCRATCH_BYTES, BLOCKING_PER_TASK_SCRATCH_BYTES,
        SPILL_BUFFER_MINIMUM_BYTES,
    };
    use crate::physical::MemoryCompletion;

    let stateful = retained_memory_upper > 0
        || matches!(
            flavor,
            PhysicalImplementationFlavor::HashAggregate
                | PhysicalImplementationFlavor::PerfectHashAggregate
                | PhysicalImplementationFlavor::HashJoin
                | PhysicalImplementationFlavor::HashJoinBuildLeft
                | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
                | PhysicalImplementationFlavor::HashJoinRuntimeFilter
                | PhysicalImplementationFlavor::NestedLoopJoin
                | PhysicalImplementationFlavor::SortRangeJoin
                | PhysicalImplementationFlavor::ClassicIeJoin
                | PhysicalImplementationFlavor::CrossProductInMemory
                | PhysicalImplementationFlavor::CrossProductExternal
                | PhysicalImplementationFlavor::AdaptiveSort
                | PhysicalImplementationFlavor::HeapTopN
                | PhysicalImplementationFlavor::Window
                | PhysicalImplementationFlavor::PartitionAggregateWindow
        );
    if !stateful {
        return Ok(());
    }

    let spillable = implementation_spillable(metadata, flavor);
    let contract = if flavor == PhysicalImplementationFlavor::PerfectHashAggregate {
        metadata
            .cost_facts
            .perfect_hash
            .ok_or_else(|| {
                paro_error::internal("perfect-hash memory contract disappeared during costing")
            })?
            .memory
    } else if spillable {
        let fixed_non_revocable_bytes = if matches!(
            flavor,
            PhysicalImplementationFlavor::HashJoinRuntimeFilter
                | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        ) {
            crate::physical::RuntimeFilterResourceContract::for_keys(
                &metadata.cost_facts.runtime_filter_key_types,
                max_concurrent_tasks,
            )?
            .peak_memory_bytes
        } else {
            0
        };
        let base = ExecutionMemoryContract {
            fixed_non_revocable_bytes,
            fixed_scratch_bytes: BLOCKING_FIXED_SCRATCH_BYTES,
            per_task_scratch_bytes: BLOCKING_PER_TASK_SCRATCH_BYTES,
            max_concurrent_tasks,
            revocable_minimum_bytes: 0,
            revocable_target_bytes: 0,
            spill_buffer_minimum_bytes: SPILL_BUFFER_MINIMUM_BYTES,
        };
        let minimum = base.minimum_memory_bytes()?;
        let contract = ExecutionMemoryContract {
            // The hard upper may deliberately remain unknown (`u64::MAX`).
            // A preferred target is not a correctness proof and must stay a
            // representable addition to the executable floor; admission will
            // cap it to the granted working set while spill preserves
            // progress at the floor.
            revocable_target_bytes: retained_memory_target
                .min(retained_memory_upper)
                .min(u64::MAX - minimum),
            ..base
        };
        let peak_memory_upper = if retained_memory_upper == u64::MAX {
            u64::MAX
        } else {
            minimum.saturating_add(retained_memory_upper)
        };
        publish_execution_memory_contract(
            cost,
            contract,
            contract.fixed_non_revocable_bytes,
            peak_memory_upper,
            MemoryCompletion::Guaranteed,
        )?;
        return Ok(());
    } else if retained_memory_upper == u64::MAX {
        // A missing semantic row bound cannot prove how much state this
        // non-spillable operator will eventually retain.  It also must not
        // erase the only semantically valid implementation.  Publish the
        // executable scratch floor and preserve the unknown retained-state
        // upper explicitly; grant admission will cap resident memory while
        // keeping the weaker completion contract visible to plan selection.
        let contract = ExecutionMemoryContract {
            fixed_non_revocable_bytes: 0,
            fixed_scratch_bytes: BLOCKING_FIXED_SCRATCH_BYTES,
            per_task_scratch_bytes: 0,
            max_concurrent_tasks: 0,
            revocable_minimum_bytes: 0,
            revocable_target_bytes: 0,
            spill_buffer_minimum_bytes: 0,
        };
        publish_execution_memory_contract(
            cost,
            contract,
            u64::MAX,
            u64::MAX,
            MemoryCompletion::runtime_capped_unbounded(),
        )?;
        return Ok(());
    } else {
        ExecutionMemoryContract {
            fixed_non_revocable_bytes: retained_memory_upper,
            fixed_scratch_bytes: BLOCKING_FIXED_SCRATCH_BYTES,
            per_task_scratch_bytes: 0,
            max_concurrent_tasks: 0,
            revocable_minimum_bytes: 0,
            revocable_target_bytes: 0,
            spill_buffer_minimum_bytes: 0,
        }
    };
    publish_execution_memory_contract(
        cost,
        contract,
        contract.fixed_non_revocable_bytes,
        contract.preferred_memory_bytes()?,
        MemoryCompletion::Guaranteed,
    )
}

fn publish_execution_memory_contract(
    cost: &mut SearchCost,
    contract: crate::physical::resources::ExecutionMemoryContract,
    non_revocable_memory_upper: u64,
    peak_memory_upper: u64,
    memory_completion: crate::physical::MemoryCompletion,
) -> Result<()> {
    contract.validate()?;
    cost.non_revocable_memory_upper = non_revocable_memory_upper;
    cost.minimum_memory_bytes = contract.minimum_memory_bytes()?;
    cost.revocable_memory_target = contract.revocable_target_bytes;
    cost.peak_memory_upper = peak_memory_upper;
    cost.memory_completion = memory_completion;
    cost.validate()
}

fn expected_retained_memory_target(
    facts: &ResolvedPlannerCostFacts,
    flavor: PhysicalImplementationFlavor,
    retained_memory_upper: u64,
) -> u64 {
    let child = |index: usize| {
        facts
            .child_rows
            .get(index)
            .copied()
            .unwrap_or(CompactRange::ZERO)
            .expected
    };
    let child_width = |index: usize| {
        facts
            .child_row_widths
            .get(index)
            .copied()
            .unwrap_or(facts.output_row_width)
            .max(1)
    };
    let expected = match flavor {
        PhysicalImplementationFlavor::AdaptiveSort => estimated_bytes(child(0), child_width(0)),
        PhysicalImplementationFlavor::HeapTopN => facts
            .topn_capacity
            .unwrap_or(0)
            .saturating_mul(child_width(0)),
        PhysicalImplementationFlavor::HashAggregate => estimated_bytes(
            facts.output_rows.expected,
            facts.output_row_width.saturating_add(16),
        ),
        PhysicalImplementationFlavor::Window
        | PhysicalImplementationFlavor::PartitionAggregateWindow => {
            estimated_bytes(child(0), facts.output_row_width.max(32))
        }
        PhysicalImplementationFlavor::HashJoin
        | PhysicalImplementationFlavor::HashJoinBuildLeft
        | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        | PhysicalImplementationFlavor::HashJoinRuntimeFilter => {
            let build_index = usize::from(!matches!(
                flavor,
                PhysicalImplementationFlavor::HashJoinBuildLeft
                    | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
            ));
            estimated_bytes(
                child(build_index),
                facts.output_row_width.saturating_div(2).max(32),
            )
        }
        PhysicalImplementationFlavor::NestedLoopJoin
        | PhysicalImplementationFlavor::CrossProductInMemory => {
            estimated_bytes(child(1), child_width(1))
        }
        PhysicalImplementationFlavor::SortRangeJoin
        | PhysicalImplementationFlavor::ClassicIeJoin => estimated_bytes(
            child(0).max(0.0) + child(1).max(0.0),
            facts.output_row_width.max(32),
        ),
        PhysicalImplementationFlavor::PerfectHashAggregate
        | PhysicalImplementationFlavor::SingletonAggregateProjection
        | PhysicalImplementationFlavor::CrossProductExternal
        | PhysicalImplementationFlavor::Structural
        | PhysicalImplementationFlavor::SearchProvider => retained_memory_upper,
    };
    expected.min(retained_memory_upper)
}

fn estimated_bytes(rows: f64, width: u64) -> u64 {
    if rows <= 0.0 {
        0
    } else if rows >= u64::MAX as f64 / width as f64 {
        u64::MAX
    } else {
        (rows.ceil() as u64).saturating_mul(width)
    }
}

fn refreshed_structural_cost(
    metadata: &PlannerOperatorMetadata,
    facts: &ResolvedPlannerCostFacts,
    calibration: &MachineCalibrationBundle,
    max_concurrent_tasks: u16,
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
    if metadata.operator_type == LogicalOperatorType::Get {
        // Search-provider replacements reuse the logical Get implementation
        // metadata with provider-specific facts. Only an actual base-table
        // scan owns an access-width frontier; provider costs remain the
        // immutable contract recorded by that implementation.
        let cost = facts.scan_access_width.map_or_else(
            || Ok(metadata.local_cost),
            |access_width| base_table_scan_cost(facts.output_rows, access_width),
        )?;
        return calibration.apply_parallelism(
            cost,
            ParallelWorkProfile::Pipeline,
            max_concurrent_tasks,
        );
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
        let retained_child = match metadata.operator_type {
            LogicalOperatorType::CrossProduct => Some(1),
            LogicalOperatorType::MaterializedCTE => Some(0),
            _ => None,
        };
        let (rows, width) = retained_child.map_or_else(
            || {
                (
                    facts.output_rows_hard_upper.unwrap_or(u64::MAX),
                    facts.output_row_width,
                )
            },
            |child| {
                (
                    facts
                        .child_rows_hard_upper
                        .get(child)
                        .copied()
                        .flatten()
                        .unwrap_or(u64::MAX),
                    facts
                        .child_row_widths
                        .get(child)
                        .copied()
                        .unwrap_or(facts.output_row_width),
                )
            },
        );
        cost.peak_memory_upper = rows.saturating_mul(width);
        let resident_expected = retained_child.map_or(facts.output_rows.expected, |child| {
            facts
                .child_rows
                .get(child)
                .copied()
                .unwrap_or(CompactRange::ZERO)
                .expected
        });
        cost.resources_expected[ResourceDimension::MemoryWrite as usize] =
            resident_expected * width as f64;
        cost.resources_risk_upper[ResourceDimension::MemoryWrite as usize] =
            cost.peak_memory_upper as f64;
        apply_execution_memory_contract(
            metadata,
            metadata.implementations.baseline,
            cost.peak_memory_upper,
            estimated_bytes(resident_expected, width).min(cost.peak_memory_upper),
            max_concurrent_tasks,
            &mut cost,
        )?;
    }
    cost.validate()?;
    calibration.apply_parallelism(cost, ParallelWorkProfile::Pipeline, max_concurrent_tasks)
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

pub(super) fn add_hash_key_byte_work(
    work: &mut LocalOperatorWork,
    rows: CompactRange,
    key_width: Option<u64>,
) -> Result<()> {
    const INTEGRAL_KEY_BASELINE_BYTES: u64 = 8;
    const BYTE_BLOCK: f64 = 32.0;

    let excess_bytes = key_width
        .unwrap_or(INTEGRAL_KEY_BASELINE_BYTES)
        .saturating_sub(INTEGRAL_KEY_BASELINE_BYTES);
    if excess_bytes != 0 {
        work.add(
            OP_HASH_KEY_BYTE_BLOCK,
            scaled_work(rows, excess_bytes as f64 / BYTE_BLOCK)?,
        )?;
    }
    Ok(())
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
    probe_multiplicity: RuntimeFilterProbeMultiplicity,
    exact_single_key: bool,
) -> Result<CompactRange> {
    let retained = |probe_rows: f64, build_rows: f64| {
        if probe_rows <= 0.0 {
            return 0.0;
        }
        let ratio = (build_rows / probe_rows).clamp(0.0, 1.0);
        // Domain size alone does not describe probe-key skew. Preserve a
        // conservative expected-work floor until propagated distribution
        // evidence can price page pruning and surviving rows independently.
        probe_rows * ratio.sqrt().clamp(0.1, 1.0)
    };
    let expected = match (exact_single_key, probe_multiplicity) {
        (true, RuntimeFilterProbeMultiplicity::DeclaredUnique) => {
            retained(probe.expected, build.expected).min(build.expected)
        }
        (true, RuntimeFilterProbeMultiplicity::EstimatedUnique) => {
            retained(probe.expected, build.expected).min(build.expected * 1.25)
        }
        (true, RuntimeFilterProbeMultiplicity::Unknown) | (false, _) => {
            retained(probe.expected, build.expected)
                .max(probe.expected * 0.25)
                .min(probe.expected)
        }
    };
    // Every current representation can fall back to a range, and multi-key
    // filters are installed independently per column. Neither contract can
    // prove a survivor cardinality below the complete probe.
    let upper = probe.upper;
    CompactRange::new(0.0, expected, upper.max(expected))
}

pub(super) fn runtime_filter_build_domain(
    facts: &ResolvedPlannerCostFacts,
    build_rows: CompactRange,
) -> Result<CompactRange> {
    let expected = facts
        .runtime_filter_build_distinct_expected
        .map(|distinct| distinct as f64)
        .unwrap_or(build_rows.expected)
        .min(build_rows.expected);
    // Snapshot NDV is an expected-cost input only. The full build cardinality
    // remains the upper domain so stale statistics cannot fabricate a proof.
    CompactRange::new(0.0, expected, build_rows.upper)
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
    logical_identity: Fingerprint,
    operator: Fingerprint,
    scope: BTreeSet<GroupId>,
) -> RegionFacet {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.planning-region.facet.v1");
    fingerprint.write_u64(kind as u64);
    fingerprint.write_u64(criticality as u64);
    // A facet belongs to a logical expression identity, not to the arena slot
    // that happened to receive it.  Transformations construct their root key
    // before the engine publishes a LogicalExprId; using the canonical key
    // fingerprint lets initial and transformed expressions participate in the
    // same physical-property search without an insertion-order dependency.
    fingerprint.write_fingerprint(logical_identity);
    fingerprint.write_fingerprint(operator);
    RegionFacet {
        fingerprint: fingerprint.finish(),
        kind,
        criticality,
        priority: match criticality {
            FacetCriticality::Required => 100 + kind as u16,
            FacetCriticality::Optional => 1_000 + kind as u16,
        },
        scope_contract: if kind == RegionFacetKind::RuntimeFilter {
            crate::cascades::region::RegionScopeContract::OwnerWithImmediateInputs
        } else {
            crate::cascades::region::RegionScopeContract::Exact
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

fn base_table_scan_cost(rows: CompactRange, access_width: u64) -> Result<SearchCost> {
    // A scan pays one fixed cursor/vector unit per row plus actual storage
    // source bytes. Do not floor the byte component: doing so makes a virtual
    // rowid indistinguishable from another stored fixed-width column.
    let range = scaled_work(rows, 1.0 + access_width as f64 / 32.0)?;
    let mut cost = SearchCost {
        score: ScoreSummary {
            range,
            risk_adjusted: range.expected + (range.upper - range.expected) * 0.5,
        },
        critical_path: range,
        ..SearchCost::ZERO
    };
    cost.resources_expected[ResourceDimension::Cpu as usize] = range.expected;
    cost.resources_risk_upper[ResourceDimension::Cpu as usize] = range.upper;
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

#[cfg(test)]
mod tests {
    use super::{
        runtime_filtered_probe_work, sort_work, topn_work, CompactRange,
        RuntimeFilterProbeMultiplicity,
    };

    #[test]
    fn bounded_topn_prices_heap_work_instead_of_a_full_sort() {
        let rows = CompactRange::new(100_000.0, 100_000.0, 100_000.0).unwrap();
        let sort = sort_work(rows).unwrap();
        let topn = topn_work(rows, 100).unwrap();

        assert!(topn.expected < sort.expected);
        assert!(topn.upper < sort.upper);
    }

    #[test]
    fn runtime_filter_fallback_keeps_the_complete_probe_as_risk_upper() {
        let probe = CompactRange::new(100_000.0, 100_000.0, 100_000.0).unwrap();
        let build = CompactRange::new(10_000.0, 12_000.0, 15_000.0).unwrap();

        let unique = runtime_filtered_probe_work(
            probe,
            build,
            RuntimeFilterProbeMultiplicity::DeclaredUnique,
            true,
        )
        .unwrap();
        let unconstrained = runtime_filtered_probe_work(
            probe,
            build,
            RuntimeFilterProbeMultiplicity::Unknown,
            false,
        )
        .unwrap();
        let small_exact_domain = CompactRange::new(50.0, 100.0, 150.0).unwrap();
        let exact_unconstrained = runtime_filtered_probe_work(
            probe,
            small_exact_domain,
            RuntimeFilterProbeMultiplicity::Unknown,
            true,
        )
        .unwrap();
        let coarse_unconstrained = runtime_filtered_probe_work(
            probe,
            small_exact_domain,
            RuntimeFilterProbeMultiplicity::Unknown,
            false,
        )
        .unwrap();

        assert_eq!(unique.upper, 100_000.0);
        assert_eq!(unconstrained.upper, 100_000.0);
        assert_eq!(exact_unconstrained.upper, 100_000.0);
        assert!(unique.expected < unconstrained.expected);
        assert_eq!(exact_unconstrained.expected, coarse_unconstrained.expected);
    }
}
