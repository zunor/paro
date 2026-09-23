// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical/physical property contracts, grant sensitivity, and guarantees.

use super::*;
use crate::cascades::rules::{DomainProofId, EvaluationOccurrenceId};
#[cfg(test)]
use crate::physical::MemoryCompletion;

pub(super) fn optimization_goal_fingerprint(goal: OptimizationGoal) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_u64(goal.required.0 as u64);
    fingerprint.write_u64(goal.row_goal.stable_tag());
    fingerprint.write_u64(goal.objective.stable_tag());
    fingerprint.write_u64(goal.grant.stable_tag());
    fingerprint.write_u64(goal.context.0 as u64);
    fingerprint.finish()
}

pub(super) fn derive_provided_ordering<Child>(
    operator: &LogicalOperator<Child>,
    output_columns: &[ColumnId],
    child_columns: Option<&[ColumnId]>,
    binding_ids: &BindingCatalog,
) -> ProvidedOrdering {
    if let LogicalOperator::SearchScan(search) = operator {
        return search
            .score_output_index
            .and_then(|index| output_columns.get(index))
            .copied()
            .map(|column| ProvidedOrdering::Ordered {
                keys: vec![OrderingKey {
                    column,
                    direction: if search.order_ascending {
                        SortDirection::Asc
                    } else {
                        SortDirection::Desc
                    },
                    nulls: if search.order_ascending {
                        NullOrder::Last
                    } else {
                        NullOrder::First
                    },
                    collation: None,
                }]
                .into_boxed_slice(),
                scope: OrderingScope::Global,
            })
            .unwrap_or(ProvidedOrdering::Unordered);
    }
    let orders: &[OrderByNode] = match operator {
        LogicalOperator::Order(order) => &order.orders,
        LogicalOperator::TopN(topn) => &topn.orders,
        _ => return ProvidedOrdering::Unordered,
    };
    let keys = orders
        .iter()
        .map(|order| {
            let column = match &order.expression {
                Expression::ColumnRef(column) if column.depth == 0 => binding_ids
                    .get(
                        column.binding.table_index,
                        column.binding.column_index,
                        &column.return_type,
                    )
                    .copied(),
                Expression::Reference(reference) => child_columns
                    .and_then(|columns| columns.get(reference.index))
                    .copied(),
                _ => None,
            }?;
            Some(OrderingKey {
                column,
                direction: if order.ascending {
                    SortDirection::Asc
                } else {
                    SortDirection::Desc
                },
                nulls: if order.nulls_first {
                    NullOrder::First
                } else {
                    NullOrder::Last
                },
                collation: None,
            })
        })
        .collect::<Option<Vec<_>>>();
    match keys {
        Some(keys) if !keys.is_empty() => ProvidedOrdering::Ordered {
            keys: keys.into_boxed_slice(),
            scope: OrderingScope::Global,
        },
        _ => ProvidedOrdering::Unordered,
    }
}

pub(super) fn derive_logical_properties<Child>(
    operator: &LogicalOperator<Child>,
    child_maximum_cardinalities: &[Option<u64>],
) -> LogicalProperties {
    // Group properties describe the output relation, never a particular
    // expression's relationship to its children.  Only publish bounds that
    // survive substitution by an equivalent expression.
    let maximum_cardinality = crate::statistics::cardinality_bound::derive_maximum_cardinality(
        operator,
        child_maximum_cardinalities,
    );
    LogicalProperties {
        maximum_cardinality,
        ..Default::default()
    }
}

pub(super) fn derive_group_cardinality<Child>(
    operator: &LogicalOperator<Child>,
    children: &[GroupId],
    stats: &NodeStats,
    recipe: Fingerprint,
) -> GroupCardinality {
    let inherited_child = match operator {
        LogicalOperator::Projection(_)
        | LogicalOperator::RowFetch(_)
        | LogicalOperator::ExternalProject(_)
        | LogicalOperator::Order(_)
        | LogicalOperator::Window(_) => children.first().copied(),
        LogicalOperator::MaterializedCTE(_) => children.get(1).copied(),
        _ => None,
    };
    if let Some(input) = inherited_child {
        return GroupCardinality::inherit(recipe, input);
    }
    let kind = match stats.cardinality_provenance {
        paro_planner::plan::CardinalityProvenance::Unknown => {
            return GroupCardinality::unknown(recipe, CardinalityRecipeKind::Statistics);
        }
        paro_planner::plan::CardinalityProvenance::Statistics => CardinalityRecipeKind::Statistics,
        paro_planner::plan::CardinalityProvenance::JoinGraph => CardinalityRecipeKind::JoinRegion,
    };
    stats
        .estimated_cardinality
        .map_or(GroupCardinality::unknown(recipe, kind), |estimate| {
            GroupCardinality::new(recipe, kind, estimate.min, estimate.expected, estimate.max)
        })
}

/// Build a cardinality-recipe witness from semantic operator bytes rather
/// than Memo-local scalar/expression ordinals. A frozen incumbent may be
/// checked against a separately constructed Memo; numeric arena ids are
/// intentionally allowed to differ across that boundary.
pub(super) fn stable_cardinality_recipe(
    operator_fingerprint: Fingerprint,
    operator_encoding: &[u8],
) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.cardinality-recipe.v2");
    fingerprint.write_fingerprint(operator_fingerprint);
    fingerprint.write_bytes(operator_encoding);
    fingerprint.finish()
}

pub(super) fn planner_grant_dependency<Child>(
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

pub(super) fn planner_operator_spillable<Child>(operator: &LogicalOperator<Child>) -> bool {
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
            crate::physical::extraction::helpers::supports_external_hash_join_type(join.join_type)
        }
        // Cross product has two explicit physical implementations. This flag
        // advertises the external one; the in-memory implementation remains a
        // separate non-spillable candidate.
        LogicalOperator::Join(Join::Cross(_)) => true,
        LogicalOperator::MaterializedCTE(_) => true,
        _ => false,
    }
}

pub(super) fn implementation_spillable(
    metadata: &PlannerOperatorMetadata,
    flavor: PhysicalImplementationFlavor,
) -> bool {
    match flavor {
        PhysicalImplementationFlavor::HashJoin
        | PhysicalImplementationFlavor::HashJoinBuildLeft
        | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        | PhysicalImplementationFlavor::HashJoinRuntimeFilter
        | PhysicalImplementationFlavor::HashAggregate
        | PhysicalImplementationFlavor::PartitionAggregateWindow
        | PhysicalImplementationFlavor::Window => metadata.spillable,
        PhysicalImplementationFlavor::AdaptiveSort => metadata.spillable,
        PhysicalImplementationFlavor::CrossProductExternal => metadata.spillable,
        PhysicalImplementationFlavor::Structural => metadata.spillable,
        PhysicalImplementationFlavor::NestedLoopJoin
        | PhysicalImplementationFlavor::CrossProductInMemory
        | PhysicalImplementationFlavor::HeapTopN
        | PhysicalImplementationFlavor::PerfectHashAggregate
        | PhysicalImplementationFlavor::SingletonAggregateProjection
        | PhysicalImplementationFlavor::SortRangeJoin
        | PhysicalImplementationFlavor::ClassicIeJoin
        | PhysicalImplementationFlavor::SearchProvider => false,
    }
}

pub(super) fn planner_structural_retained_children<Child>(
    operator: &LogicalOperator<Child>,
) -> u64 {
    match operator {
        LogicalOperator::Order(_)
        | LogicalOperator::TopN(_)
        | LogicalOperator::Distinct(_)
        | LogicalOperator::Window(_) => 0b1,
        LogicalOperator::MaterializedCTE(_)
        | LogicalOperator::RecursiveCTE(_)
        | LogicalOperator::DependentJoin(_)
        | LogicalOperator::Join(_) => 0b11,
        _ => 0,
    }
}

fn retained_ratio_ppm(retained: f64, source: f64) -> u32 {
    if source <= 0.0 {
        1_000_000
    } else {
        ((retained / source).clamp(0.0, 1.0) * 1_000_000.0).round() as u32
    }
}

fn retained_upper_ratio_ppm(retained: f64, source: f64, expected_ppm: u32) -> u32 {
    if source <= 0.0 {
        1_000_000
    } else {
        // This ratio participates in a hard work upper bound. Rounding it down
        // would turn a conservative cardinality bound into an optimistic one.
        // It must also dominate the separately estimated expected ratio: the
        // two ratios use different points of their respective ranges, so that
        // ordering does not follow from `retained.expected <= retained.upper`.
        (((retained / source).clamp(0.0, 1.0) * 1_000_000.0).ceil() as u32).max(expected_ppm)
    }
}

fn runtime_filter_source_retentions(
    sources: &[ResolvedRuntimeFilterSource],
    build_domain: CompactRange,
    exactness: RuntimeFilterExactness,
    semantic_proof: Fingerprint,
    build_domain_identity: Fingerprint,
    evaluation: Fingerprint,
) -> Result<Box<[SidewaysFilterSource]>> {
    sources
        .iter()
        .map(|source| {
            let retained = if build_domain.expected < source.rows.expected {
                runtime_filtered_probe_work(
                    source.rows,
                    build_domain,
                    source.multiplicity,
                    exactness,
                )?
            } else {
                source.rows
            };
            let expected_retained_ppm = retained_ratio_ppm(retained.expected, source.rows.expected);
            let mut domain = StableFingerprintBuilder::default();
            domain.write_bytes(b"paro.runtime-filter-domain.v2");
            domain.write_fingerprint(semantic_proof);
            // The semantic build relation/key domain is not the physical
            // implementation and not the evaluation occurrence.  Keeping it
            // in the proof identity prevents nested same-shaped joins from
            // sharing a domain while allowing physical alternatives in one
            // logical group to share the proof.
            domain.write_fingerprint(build_domain_identity);
            domain.write_u64(source.source.0 as u64);
            Ok(SidewaysFilterSource {
                source: source.source,
                domain: DomainProofId(domain.finish()),
                evaluation: EvaluationOccurrenceId(evaluation),
                expected_retained_ppm,
                upper_retained_ppm: retained_upper_ratio_ppm(
                    retained.upper,
                    source.rows.upper,
                    expected_retained_ppm,
                ),
            })
        })
        .collect::<Result<Vec<_>>>()
        .map(Vec::into_boxed_slice)
}

/// Identify one physical evaluation occurrence without using it as the
/// semantic build-domain proof. The logical expression occurrence and goal
/// make nested same-shaped joins distinct; the physical fingerprint
/// distinguishes genuinely different implementations of that occurrence.
pub(super) fn runtime_filter_evaluation_identity(
    expression: LogicalExprId,
    goal: OptimizationGoal,
    physical_fingerprint: Fingerprint,
) -> Fingerprint {
    let mut identity = StableFingerprintBuilder::default();
    identity.write_bytes(b"paro.runtime-filter-evaluation.v2");
    identity.write_u64(expression.0 as u64);
    identity.write_fingerprint(optimization_goal_fingerprint(goal));
    identity.write_fingerprint(physical_fingerprint);
    identity.finish()
}

pub(super) fn planner_cost_composition(
    metadata: &PlannerOperatorMetadata,
    flavor: PhysicalImplementationFlavor,
    facts: &ResolvedPlannerCostFacts,
    max_concurrent_tasks: u16,
    evaluation_identity: Fingerprint,
) -> Result<CostComposition> {
    if metadata.operator_type == LogicalOperatorType::EmptyResult {
        return Ok(CostComposition::LocalOnly);
    }
    if let Some(source) = facts.scan_work_source {
        let source_rows = facts.scan_physical_rows.unwrap_or_else(|| {
            facts
                .output_rows
                .expected
                .ceil()
                .clamp(0.0, u64::MAX as f64) as u64
        });
        return Ok(CostComposition::Source {
            source,
            source_rows,
        });
    }
    let overlapping_children = match flavor {
        PhysicalImplementationFlavor::HashJoin
        | PhysicalImplementationFlavor::HashJoinBuildLeft
        | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        | PhysicalImplementationFlavor::HashJoinRuntimeFilter => 0b11,
        PhysicalImplementationFlavor::CrossProductInMemory
        | PhysicalImplementationFlavor::CrossProductExternal => 0b11,
        PhysicalImplementationFlavor::HashAggregate
        | PhysicalImplementationFlavor::PerfectHashAggregate
        | PhysicalImplementationFlavor::Window
        | PhysicalImplementationFlavor::PartitionAggregateWindow => 0b1,
        PhysicalImplementationFlavor::AdaptiveSort | PhysicalImplementationFlavor::HeapTopN => 0b1,
        PhysicalImplementationFlavor::SingletonAggregateProjection => 0,
        PhysicalImplementationFlavor::Structural => metadata.structural_retained_children,
        PhysicalImplementationFlavor::NestedLoopJoin
        | PhysicalImplementationFlavor::SortRangeJoin
        | PhysicalImplementationFlavor::ClassicIeJoin
        | PhysicalImplementationFlavor::SearchProvider => 0,
    };
    if flavor == PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter {
        let build = facts
            .child_rows
            .first()
            .copied()
            .unwrap_or(CompactRange::ZERO);
        let build_domain =
            runtime_filter_build_domain(facts.runtime_filter_build_left_distinct_expected, build)?;
        let resource = crate::physical::RuntimeFilterResourceContract::for_keys(
            &facts.runtime_filter_key_types,
            max_concurrent_tasks,
        )?;
        let build_hard_upper = facts.child_rows_hard_upper.first().copied().flatten();
        let exactness =
            runtime_filter_exactness(&resource, build_hard_upper, build_domain.expected);
        if facts.runtime_filter_build_left_probe_sources.is_empty() {
            return Ok(CostComposition::RetainedState {
                overlapping_children,
            });
        }
        return Ok(CostComposition::SidewaysFilter {
            overlapping_children,
            filtered_child: 1,
            sources: runtime_filter_source_retentions(
                &facts.runtime_filter_build_left_probe_sources,
                build_domain,
                exactness,
                metadata.operator_fingerprint,
                facts
                    .runtime_filter_build_left_domain_identity
                    .ok_or_else(|| {
                        paro_error::internal(
                            "runtime-filter build-left candidate lost its domain identity",
                        )
                    })?,
                evaluation_identity,
            )?,
        });
    }
    if flavor == PhysicalImplementationFlavor::HashJoinRuntimeFilter {
        if facts.runtime_filter_probe_sources.is_empty() {
            return Ok(CostComposition::RetainedState {
                overlapping_children,
            });
        }
        let build = facts
            .child_rows
            .get(1)
            .copied()
            .unwrap_or(CompactRange::ZERO);
        let build_domain =
            runtime_filter_build_domain(facts.runtime_filter_build_distinct_expected, build)?;
        // A non-local runtime filter is evaluated at the traced rowset source,
        // before intervening joins. Compare the build domain with that source;
        // using the already-reduced join child cardinality incorrectly rejects
        // filters that can remove source I/O before another selective join.
        let resource = crate::physical::RuntimeFilterResourceContract::for_keys(
            &facts.runtime_filter_key_types,
            max_concurrent_tasks,
        )?;
        let build_hard_upper = facts.child_rows_hard_upper.get(1).copied().flatten();
        let exactness =
            runtime_filter_exactness(&resource, build_hard_upper, build_domain.expected);
        return Ok(CostComposition::SidewaysFilter {
            overlapping_children,
            filtered_child: 0,
            sources: runtime_filter_source_retentions(
                &facts.runtime_filter_probe_sources,
                build_domain,
                exactness,
                metadata.operator_fingerprint,
                facts.runtime_filter_build_domain_identity.ok_or_else(|| {
                    paro_error::internal("runtime-filter candidate lost its build domain identity")
                })?,
                evaluation_identity,
            )?,
        });
    }
    if overlapping_children == 0 {
        Ok(CostComposition::Sequential)
    } else {
        Ok(CostComposition::RetainedState {
            overlapping_children,
        })
    }
}

pub(super) fn planner_task_supply_contract(
    metadata: &PlannerOperatorMetadata,
    flavor: PhysicalImplementationFlavor,
    facts: &ResolvedPlannerCostFacts,
    calibration: &MachineCalibrationBundle,
    max_concurrent_tasks: u16,
) -> Result<TaskSupplyContract> {
    if facts.scan_work_source.is_some() {
        return Ok(TaskSupplyContract::Source {
            tasks: useful_parallel_tasks_for_facts(facts, max_concurrent_tasks),
        });
    }
    let output_tasks = useful_output_tasks(facts, max_concurrent_tasks);
    let contract = match flavor {
        PhysicalImplementationFlavor::Structural => match metadata.operator_type {
            LogicalOperatorType::Limit | LogicalOperatorType::EmptyResult => {
                TaskSupplyContract::Serial
            }
            LogicalOperatorType::LogicalUnion
            | LogicalOperatorType::LogicalIntersect
            | LogicalOperatorType::LogicalExcept
            | LogicalOperatorType::Order
            | LogicalOperatorType::TopN
            | LogicalOperatorType::Window => TaskSupplyContract::Breaker {
                input: 0,
                output_tasks: 1,
                profile: ParallelWorkProfile::BlockingMerge,
            },
            LogicalOperatorType::MaterializedCTE | LogicalOperatorType::Distinct => {
                TaskSupplyContract::Breaker {
                    input: 0,
                    output_tasks,
                    profile: ParallelWorkProfile::BlockingMerge,
                }
            }
            _ if !facts.child_rows.is_empty() => TaskSupplyContract::Streaming { input: 0 },
            LogicalOperatorType::GraphScan
            | LogicalOperatorType::CTERef
            | LogicalOperatorType::ExternalTable => TaskSupplyContract::Source {
                tasks: output_tasks,
            },
            _ => TaskSupplyContract::Serial,
        },
        PhysicalImplementationFlavor::HashJoin
        | PhysicalImplementationFlavor::HashJoinBuildLeft
        | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        | PhysicalImplementationFlavor::HashJoinRuntimeFilter => {
            let build_left = matches!(
                flavor,
                PhysicalImplementationFlavor::HashJoinBuildLeft
                    | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
            );
            TaskSupplyContract::BuildProbe {
                build: u8::from(!build_left),
                probe: u8::from(build_left),
                build_work_ppm: hash_join_build_work_ppm(facts, flavor, calibration)?,
            }
        }
        PhysicalImplementationFlavor::HashAggregate
        | PhysicalImplementationFlavor::PerfectHashAggregate
        | PhysicalImplementationFlavor::PartitionAggregateWindow => TaskSupplyContract::Breaker {
            input: 0,
            output_tasks,
            profile: ParallelWorkProfile::BlockingMerge,
        },
        PhysicalImplementationFlavor::AdaptiveSort
        | PhysicalImplementationFlavor::HeapTopN
        | PhysicalImplementationFlavor::Window
        | PhysicalImplementationFlavor::SortRangeJoin
        | PhysicalImplementationFlavor::ClassicIeJoin => TaskSupplyContract::Breaker {
            input: 0,
            output_tasks: 1,
            profile: ParallelWorkProfile::BlockingMerge,
        },
        PhysicalImplementationFlavor::NestedLoopJoin
        | PhysicalImplementationFlavor::CrossProductInMemory
        | PhysicalImplementationFlavor::CrossProductExternal => TaskSupplyContract::BuildProbe {
            build: 1,
            probe: 0,
            build_work_ppm: build_probe_byte_work_ppm(facts),
        },
        PhysicalImplementationFlavor::SingletonAggregateProjection
        | PhysicalImplementationFlavor::SearchProvider => TaskSupplyContract::Serial,
    };
    Ok(contract)
}

fn build_probe_byte_work_ppm(facts: &ResolvedPlannerCostFacts) -> u32 {
    let build = facts
        .child_rows
        .get(1)
        .map(|rows| {
            estimated_bytes(
                rows.expected,
                facts.child_row_widths.get(1).copied().unwrap_or(1),
            )
        })
        .unwrap_or(0);
    let probe = facts
        .child_rows
        .first()
        .map(|rows| {
            estimated_bytes(
                rows.expected,
                facts.child_row_widths.first().copied().unwrap_or(1),
            )
        })
        .unwrap_or(0)
        .saturating_add(estimated_bytes(
            facts.output_rows.expected,
            facts.output_row_width,
        ));
    let total = build.saturating_add(probe);
    if total == 0 {
        0
    } else {
        ((build as f64 / total as f64 * 1_000_000.0).round() as u32).min(1_000_000)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        retained_ratio_ppm, retained_upper_ratio_ppm, runtime_filter_source_retentions,
        selected_node_grant_contract,
    };
    use crate::cascades::cost::CompactRange;
    use crate::cascades::ids::Fingerprint;
    use crate::cascades::ids::{AdmissibleGrantSetId, ResourceGrantClassId};
    use crate::cascades::memo::GrantGoalKey;
    use crate::cascades::planner::costing::RuntimeFilterExactness;
    use crate::cascades::planner::state::ResolvedRuntimeFilterSource;
    use crate::cascades::planner::state::RuntimeFilterProbeMultiplicity;
    use crate::cascades::rules::GrantDependencyDescriptor;
    use crate::cascades::rules::WorkSourceId;
    use crate::physical::PhysicalGrantContract;

    #[test]
    fn selected_implementation_contract_is_independent_of_sibling_search_requirements() {
        let goal = GrantGoalKey::Class(ResourceGrantClassId(7));
        assert_eq!(
            selected_node_grant_contract(GrantDependencyDescriptor::Invariant, goal, 4).unwrap(),
            PhysicalGrantContract::Invariant,
        );
        assert_eq!(
            selected_node_grant_contract(GrantDependencyDescriptor::Parallelism, goal, 4).unwrap(),
            PhysicalGrantContract::Parallelism { tasks: 4 },
        );
        assert_eq!(
            selected_node_grant_contract(GrantDependencyDescriptor::Sensitive, goal, 4).unwrap(),
            PhysicalGrantContract::Class(ResourceGrantClassId(7)),
        );
        assert!(
            selected_node_grant_contract(
                GrantDependencyDescriptor::Sensitive,
                GrantGoalKey::Invariant(AdmissibleGrantSetId(1)),
                4,
            )
            .is_err()
        );
    }

    #[test]
    fn retained_upper_ratio_never_rounds_below_the_proof() {
        assert_eq!(retained_ratio_ppm(1.0, 3.0), 333_333);
        assert_eq!(retained_upper_ratio_ppm(1.0, 3.0, 0), 333_334);
    }

    #[test]
    fn retained_upper_ratio_dominates_the_expected_ratio() {
        assert_eq!(retained_upper_ratio_ppm(1.0, 10.0, 250_000), 250_000);
    }

    #[test]
    fn runtime_filter_domain_is_shared_only_for_the_same_semantic_build() {
        let sources = [ResolvedRuntimeFilterSource {
            source: WorkSourceId(7),
            rows: CompactRange::point(100.0).unwrap(),
            multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        }];
        let first = runtime_filter_source_retentions(
            &sources,
            CompactRange::point(10.0).unwrap(),
            RuntimeFilterExactness::Expected,
            Fingerprint(1),
            Fingerprint(11),
            Fingerprint(101),
        )
        .unwrap();
        let same_build_different_evaluation = runtime_filter_source_retentions(
            &sources,
            CompactRange::point(10.0).unwrap(),
            RuntimeFilterExactness::Expected,
            Fingerprint(1),
            Fingerprint(11),
            Fingerprint(202),
        )
        .unwrap();
        let different_build = runtime_filter_source_retentions(
            &sources,
            CompactRange::point(10.0).unwrap(),
            RuntimeFilterExactness::Expected,
            Fingerprint(1),
            Fingerprint(22),
            Fingerprint(303),
        )
        .unwrap();

        assert_eq!(first[0].domain, same_build_different_evaluation[0].domain);
        assert_ne!(
            first[0].evaluation,
            same_build_different_evaluation[0].evaluation
        );
        assert_ne!(first[0].domain, different_build[0].domain);
    }
}

pub(super) fn append_grant_fingerprint(
    fingerprint: &mut StableFingerprintBuilder,
    dependency: GrantDependencyDescriptor,
    grant: GrantGoalKey,
) {
    if dependency != GrantDependencyDescriptor::Invariant {
        fingerprint.write_u64(grant.stable_tag());
    }
}

/// Dependency of this selected implementation, not the union of alternatives
/// searched in its Memo group. A grant-independent provider can legitimately
/// win a class-specific goal created by a memory-sensitive sibling algorithm.
pub(super) fn implementation_grant_dependency(
    metadata: &PlannerOperatorMetadata,
    flavor: PhysicalImplementationFlavor,
) -> GrantDependencyDescriptor {
    if flavor == PhysicalImplementationFlavor::SearchProvider {
        GrantDependencyDescriptor::Invariant
    } else if flavor == metadata.implementations.baseline {
        metadata.grant_dependency
    } else {
        GrantDependencyDescriptor::Sensitive
    }
}

pub(super) fn selected_node_grant_contract(
    dependency: GrantDependencyDescriptor,
    goal: GrantGoalKey,
    max_tasks: u16,
) -> Result<crate::physical::PhysicalGrantContract> {
    use crate::physical::PhysicalGrantContract;
    match dependency {
        GrantDependencyDescriptor::Invariant => Ok(PhysicalGrantContract::Invariant),
        GrantDependencyDescriptor::Parallelism if max_tasks > 0 => {
            Ok(PhysicalGrantContract::Parallelism { tasks: max_tasks })
        }
        GrantDependencyDescriptor::Sensitive => match goal {
            GrantGoalKey::Class(class) => Ok(PhysicalGrantContract::Class(class)),
            _ => Err(paro_error::internal(
                "class-sensitive node was selected under a shared goal",
            )),
        },
        GrantDependencyDescriptor::Parallelism => {
            Err(paro_error::internal("selected node has zero task capacity"))
        }
    }
}

pub(super) fn cost_for_grant(
    mut cost: SearchCost,
    dependency: GrantDependencyDescriptor,
    spillable: bool,
    grant: GrantGoalKey,
    classes: &BTreeMap<crate::cascades::ids::ResourceGrantClassId, ResourceGrantClass>,
    force_spill: bool,
) -> Result<Option<SearchCost>> {
    if dependency != GrantDependencyDescriptor::Sensitive {
        if cost.peak_memory_upper == u64::MAX || cost.memory_completion.is_runtime_capped() {
            return Err(paro_error::internal(
                "unbounded or runtime-capped memory makes an implementation grant-sensitive",
            ));
        }
        cost.validate()?;
        return Ok(Some(cost));
    }
    let GrantGoalKey::Class(class_id) = grant else {
        return Err(paro_error::internal(
            "grant-sensitive implementation received an invariant goal",
        ));
    };
    let class = classes.get(&class_id).ok_or_else(|| {
        paro_error::internal("physical implementation references an unknown grant class")
    })?;
    if cost.minimum_memory_bytes > class.hard_memory_bytes {
        return Ok(None);
    }
    if spillable && class.spill_policy == SpillPolicy::Forbidden {
        // In a no-spill class the entire retained state is required for
        // forward progress. It is neither revocable nor a soft target, even
        // when the same implementation can be adaptive in another class.
        if force_spill
            || cost.peak_memory_upper == u64::MAX
            || cost.peak_memory_upper > class.hard_memory_bytes
        {
            return Ok(None);
        }
        cost.minimum_memory_bytes = cost.minimum_memory_bytes.max(cost.peak_memory_upper);
        cost.non_revocable_memory_upper =
            cost.non_revocable_memory_upper.max(cost.peak_memory_upper);
        cost.revocable_memory_target = 0;
        cost.validate()?;
        return Ok(Some(cost));
    }
    if cost.peak_memory_upper == u64::MAX {
        if spillable && class.spill_policy == SpillPolicy::Allowed {
            // These implementations allocate all retained state through the
            // query pool. The allocator bounds resident memory and the spill
            // protocol proves forward progress. Spill volume stays UNKNOWN.
            cost.peak_memory_upper = class.hard_memory_bytes;
            cost.revocable_memory_target = cost
                .revocable_memory_target
                .min(class.hard_memory_bytes - cost.minimum_memory_bytes);
            cost.validate()?;
            return Ok(Some(cost));
        }
        if cost.memory_completion.is_runtime_capped() {
            // The query allocator is the resident-memory proof for this
            // explicitly best-effort implementation.  This does not promote
            // it to a forward-progress guarantee: portfolio selection keeps
            // preferring any fully bounded or spillable alternative.
            cost.apply_runtime_cap(class.hard_memory_bytes, cost.minimum_memory_bytes)?;
            return Ok(Some(cost));
        }
        return Ok(None);
    }
    if force_spill && spillable {
        if class.spill_policy == SpillPolicy::Forbidden {
            return Ok(None);
        } else {
            let spilled = cost.revocable_memory_target.max(1);
            cost.peak_memory_upper = cost.peak_memory_upper.min(class.hard_memory_bytes);
            cost.revocable_memory_target = cost
                .revocable_memory_target
                .min(cost.peak_memory_upper - cost.minimum_memory_bytes);
            add_spill_cost(&mut cost, spilled)?;
            cost.validate()?;
            return Ok(Some(cost));
        }
    }
    if cost.peak_memory_upper <= class.hard_memory_bytes {
        return Ok(Some(cost));
    }
    if spillable && class.spill_policy == SpillPolicy::Allowed {
        let spilled = cost
            .preferred_memory_bytes()
            .saturating_sub(class.hard_memory_bytes);
        cost.peak_memory_upper = class.hard_memory_bytes;
        cost.revocable_memory_target = cost
            .revocable_memory_target
            .min(class.hard_memory_bytes - cost.minimum_memory_bytes);
        if spilled > 0 {
            add_spill_cost(&mut cost, spilled)?;
        }
        cost.validate()?;
        return Ok(Some(cost));
    }
    if cost.memory_completion.is_runtime_capped() {
        cost.apply_runtime_cap(class.hard_memory_bytes, cost.minimum_memory_bytes)?;
        return Ok(Some(cost));
    }
    Ok(None)
}

pub(super) fn planner_enforcer_cost_input(
    facts: &ResolvedPlannerCostFacts,
    grant: GrantGoalKey,
    classes: &BTreeMap<crate::cascades::ids::ResourceGrantClassId, ResourceGrantClass>,
) -> Result<crate::cascades::engine::EnforcerCostInput> {
    let mut input = crate::cascades::engine::EnforcerCostInput::unbounded(
        facts.output_rows,
        facts.output_row_width,
    );
    if let GrantGoalKey::Class(class) = grant {
        let class = classes.get(&class).ok_or_else(|| {
            paro_error::internal("enforcer costing references an unknown grant class")
        })?;
        input.hard_memory_bytes = class.hard_memory_bytes;
        input.spill_policy = class.spill_policy;
        input.max_parallel_tasks = class.max_parallel_tasks.max(1);
    } else if let GrantGoalKey::Parallelism { tasks, .. } = grant {
        input.max_parallel_tasks = tasks;
    }
    Ok(input)
}

pub(super) fn add_spill_cost(cost: &mut SearchCost, spilled: u64) -> Result<()> {
    cost.spill_bytes_expected = cost.spill_bytes_expected.saturating_add(spilled);
    let io_work = (spilled as f64 / 4096.0).max(1.0);
    let spill_range = CompactRange::new(io_work, io_work * 2.0, io_work * 6.0)?;
    cost.score.range = cost.score.range.checked_add(spill_range)?;
    cost.score.risk_adjusted += io_work * 3.0;
    cost.critical_path = cost.critical_path.checked_add(spill_range)?;
    cost.resources_expected[ResourceDimension::SequentialIo as usize] += io_work * 2.0;
    cost.resources_risk_upper[ResourceDimension::SequentialIo as usize] += io_work * 6.0;
    cost.validate()?;
    Ok(())
}

pub(super) fn provided_result_guarantee<Child>(
    operator: &LogicalOperator<Child>,
) -> ResultGuarantee {
    match operator {
        LogicalOperator::SearchScan(scan)
            if scan.request.intents.iter().any(|intent| {
                matches!(
                    intent,
                    paro_storage::search::SearchIntent::Hnsw(hnsw)
                        if hnsw.options.objective
                            == paro_storage::index::hnsw::HnswSearchObjective::CostOptimized
                )
            }) =>
        {
            ResultGuarantee::ApproximateAllowed(COST_OPTIMIZED_SEARCH_POLICY)
        }
        _ => ResultGuarantee::Exact,
    }
}

pub(super) fn search_payload_fingerprint(
    logical_fingerprint: Fingerprint,
    operator: &LogicalOperator,
) -> Fingerprint {
    fn write_candidate(
        fingerprint: &mut StableFingerprintBuilder,
        candidate: &paro_planner::operator::SearchCandidate,
    ) {
        fingerprint.write_u64(candidate.token.definition_id);
        fingerprint.write_u64(candidate.token.generation_id);
        fingerprint.write_u64(candidate.token.root_version);
    }

    fn write_decision(
        fingerprint: &mut StableFingerprintBuilder,
        decision: &paro_planner::operator::SearchDecision,
    ) {
        match decision {
            paro_planner::operator::SearchDecision::IndexScan { candidate, .. } => {
                fingerprint.write_u64(0);
                write_candidate(fingerprint, candidate);
            }
            paro_planner::operator::SearchDecision::Adaptive {
                candidates,
                sequential,
            } => {
                fingerprint.write_u64(1);
                fingerprint.write_u64(sequential.table_id);
                fingerprint.write_u64(candidates.len() as u64);
                for candidate in candidates {
                    write_candidate(fingerprint, candidate);
                }
            }
        }
    }

    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.physical.search-provider.v1");
    fingerprint.write_fingerprint(logical_fingerprint);
    match operator {
        LogicalOperator::SearchScan(search) => {
            fingerprint.write_u64(0);
            encode_search_request(&mut fingerprint, &search.request);
            fingerprint.write_u64(
                search
                    .score_output_index
                    .and_then(|index| u64::try_from(index).ok())
                    .unwrap_or(u64::MAX),
            );
            fingerprint.write_u64(search.order_ascending as u64);
            fingerprint.write_u64(search.limit as u64);
            write_decision(&mut fingerprint, &search.decision);
        }
        LogicalOperator::FullTextFilterScan(search) => {
            fingerprint.write_u64(1);
            encode_search_request(&mut fingerprint, &search.request);
            encode_projection_map(&mut fingerprint, &search.projection_map);
            write_decision(&mut fingerprint, &search.decision);
        }
        _ => fingerprint.write_u64(u64::MAX),
    }
    fingerprint.finish()
}

/// Bind-time query options form the root semantic contract. With Exact as the
/// API default, seeing CostOptimized here necessarily represents an explicit
/// opt-in and may therefore admit the matching approximate provider policy.
pub(super) fn required_result_guarantee(plan: &OwnedLogicalPlan) -> ResultGuarantee {
    match &plan.operator {
        LogicalOperator::TopN(topn)
            if topn.hnsw_options.objective
                == paro_storage::index::hnsw::HnswSearchObjective::CostOptimized =>
        {
            ResultGuarantee::ApproximateAllowed(COST_OPTIMIZED_SEARCH_POLICY)
        }
        LogicalOperator::SearchScan(_) => provided_result_guarantee(&plan.operator),
        _ => plan
            .children()
            .into_iter()
            .map(required_result_guarantee)
            .find(|guarantee| matches!(guarantee, ResultGuarantee::ApproximateAllowed(_)))
            .unwrap_or(ResultGuarantee::Exact),
    }
}

#[cfg(test)]
mod resource_contract_tests {
    use super::*;

    fn class(spill_policy: SpillPolicy) -> ResourceGrantClass {
        ResourceGrantClass {
            id: crate::cascades::ids::ResourceGrantClassId(7),
            hard_memory_bytes: 1024 * 1024,
            spill_policy,
            max_parallel_tasks: 1,
        }
    }

    fn cost(peak: u64, minimum: u64) -> SearchCost {
        SearchCost {
            peak_memory_upper: peak,
            minimum_memory_bytes: minimum,
            revocable_memory_target: peak.saturating_sub(minimum),
            ..SearchCost::ZERO
        }
    }

    #[test]
    fn unknown_state_requires_a_forward_progress_spill_contract() {
        let no_spill = class(SpillPolicy::Forbidden);
        let spill = class(SpillPolicy::Allowed);
        let grant = GrantGoalKey::Class(no_spill.id);

        assert!(
            cost_for_grant(
                cost(u64::MAX, 800 * 1024),
                GrantDependencyDescriptor::Sensitive,
                true,
                grant,
                &BTreeMap::from([(no_spill.id, no_spill)]),
                false,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            cost_for_grant(
                cost(u64::MAX, 800 * 1024),
                GrantDependencyDescriptor::Sensitive,
                true,
                grant,
                &BTreeMap::from([(spill.id, spill)]),
                false,
            )
            .unwrap()
            .unwrap()
            .peak_memory_upper,
            spill.hard_memory_bytes
        );
    }

    #[test]
    fn explicitly_runtime_capped_state_remains_an_admissible_fallback() {
        let no_spill = class(SpillPolicy::Forbidden);
        let mut estimate = cost(u64::MAX, 64 * 1024);
        estimate.non_revocable_memory_upper = u64::MAX;
        estimate.revocable_memory_target = 0;
        estimate.memory_completion = MemoryCompletion::runtime_capped_unbounded();

        let admitted = cost_for_grant(
            estimate,
            GrantDependencyDescriptor::Sensitive,
            false,
            GrantGoalKey::Class(no_spill.id),
            &BTreeMap::from([(no_spill.id, no_spill)]),
            false,
        )
        .unwrap()
        .expect("a runtime-capped semantic baseline must survive planning");

        assert_eq!(admitted.minimum_memory_bytes, 64 * 1024);
        assert_eq!(
            admitted.non_revocable_memory_upper,
            no_spill.hard_memory_bytes
        );
        assert_eq!(admitted.peak_memory_upper, no_spill.hard_memory_bytes);
        assert_eq!(
            admitted.memory_completion,
            MemoryCompletion::runtime_capped_unbounded()
        );
    }

    #[test]
    fn runtime_capped_state_cannot_claim_grant_invariance() {
        let no_spill = class(SpillPolicy::Forbidden);
        let mut estimate = cost(u64::MAX, 64 * 1024);
        estimate.non_revocable_memory_upper = u64::MAX;
        estimate.revocable_memory_target = 0;
        estimate.memory_completion = MemoryCompletion::runtime_capped_unbounded();

        let error = cost_for_grant(
            estimate,
            GrantDependencyDescriptor::Invariant,
            false,
            GrantGoalKey::Invariant(crate::cascades::ids::AdmissibleGrantSetId(0)),
            &BTreeMap::from([(no_spill.id, no_spill)]),
            false,
        )
        .expect_err("runtime-capped cost must participate in grant optimization");

        assert!(
            error
                .to_string()
                .contains("runtime-capped memory makes an implementation grant-sensitive")
        );
    }

    #[test]
    fn forced_external_representation_is_not_faked_in_a_no_spill_class() {
        let no_spill = class(SpillPolicy::Forbidden);
        assert!(
            cost_for_grant(
                cost(900 * 1024, 800 * 1024),
                GrantDependencyDescriptor::Sensitive,
                true,
                GrantGoalKey::Class(no_spill.id),
                &BTreeMap::from([(no_spill.id, no_spill)]),
                true,
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn finite_adaptive_state_becomes_a_floor_without_spill_permission() {
        let no_spill = class(SpillPolicy::Forbidden);
        let admitted = cost_for_grant(
            cost(900 * 1024, 800 * 1024),
            GrantDependencyDescriptor::Sensitive,
            true,
            GrantGoalKey::Class(no_spill.id),
            &BTreeMap::from([(no_spill.id, no_spill)]),
            false,
        )
        .unwrap()
        .expect("finite retained state fits the no-spill grant");

        assert_eq!(admitted.minimum_memory_bytes, 900 * 1024);
        assert_eq!(admitted.non_revocable_memory_upper, 900 * 1024);
        assert_eq!(admitted.revocable_memory_target, 0);
    }

    #[test]
    fn hard_state_upper_does_not_create_expected_spill_when_the_target_fits() {
        let spill = class(SpillPolicy::Allowed);
        let mut estimate = cost(2 * 1024 * 1024, 256 * 1024);
        estimate.revocable_memory_target = 256 * 1024;

        let admitted = cost_for_grant(
            estimate,
            GrantDependencyDescriptor::Sensitive,
            true,
            GrantGoalKey::Class(spill.id),
            &BTreeMap::from([(spill.id, spill)]),
            false,
        )
        .unwrap()
        .expect("spill-bounded state is executable");

        assert_eq!(admitted.peak_memory_upper, spill.hard_memory_bytes);
        assert_eq!(admitted.revocable_memory_target, 256 * 1024);
        assert_eq!(admitted.spill_bytes_expected, 0);
    }
}
