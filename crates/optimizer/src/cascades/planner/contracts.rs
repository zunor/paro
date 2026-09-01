// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical/physical property contracts, grant sensitivity, and guarantees.

use super::*;

pub(super) fn optimization_goal_fingerprint(goal: OptimizationGoal) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_u64(goal.required.0 as u64);
    fingerprint.write_u64(goal.row_goal.stable_tag());
    fingerprint.write_u64(goal.objective.0 as u64);
    fingerprint.write_u64(goal.grant.stable_tag());
    fingerprint.write_u64(goal.context.0 as u64);
    fingerprint.finish()
}

pub(super) fn derive_provided_ordering(
    operator: &LogicalOperator,
    output_columns: &[ColumnId],
    child_columns: Option<&[ColumnId]>,
    binding_ids: &BindingCatalog,
) -> ProvidedOrdering {
    if let LogicalOperator::SearchScan(search) = operator {
        return output_columns
            .get(search.score_projection_index)
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
                    .get(&(
                        column.binding.table_index,
                        column.binding.column_index,
                        logical_type_fingerprint(&column.return_type),
                    ))
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

pub(super) fn derive_logical_properties(
    operator: &LogicalOperator,
    child_maximum_cardinalities: &[Option<u64>],
) -> LogicalProperties {
    // Group properties describe the output relation, never a particular
    // expression's relationship to its children.  Only publish bounds that
    // survive substitution by an equivalent expression.
    let unary_bound = || child_maximum_cardinalities.first().copied().flatten();
    let binary_product = || {
        child_maximum_cardinalities
            .first()
            .copied()
            .flatten()?
            .checked_mul(child_maximum_cardinalities.get(1).copied().flatten()?)
    };
    let maximum_cardinality = match operator {
        LogicalOperator::DummyScan => Some(1),
        LogicalOperator::EmptyResult(_) => Some(0),
        // A table's resident row count is local to the compilation snapshot.
        // Prepared plans survive DML, so it is an estimate rather than a
        // schema invariant and cannot participate in a correctness proof.
        LogicalOperator::Get(_) => None,
        LogicalOperator::ExpressionGet(values) => u64::try_from(values.expressions.len()).ok(),
        LogicalOperator::Projection(_)
        | LogicalOperator::Filter(_)
        | LogicalOperator::RowFetch(_)
        | LogicalOperator::ExternalProject(_)
        | LogicalOperator::Limit(_)
        | LogicalOperator::Order(_)
        | LogicalOperator::Distinct(_)
        | LogicalOperator::Window(_) => unary_bound(),
        LogicalOperator::Aggregate(aggregate)
            if aggregate.groups.is_empty() && aggregate.grouping_sets.is_empty() =>
        {
            Some(1)
        }
        LogicalOperator::Aggregate(aggregate) if !aggregate.grouping_sets.is_empty() => {
            unary_bound().and_then(|input| {
                aggregate
                    .grouping_sets
                    .iter()
                    .try_fold(0_u64, |bound, set| {
                        bound.checked_add(if set.expressions.is_empty() { 1 } else { input })
                    })
            })
        }
        LogicalOperator::Aggregate(_) => unary_bound(),
        LogicalOperator::TopN(topn) => {
            unary_bound().map(|bound| bound.min(u64::try_from(topn.limit).unwrap_or(u64::MAX)))
        }
        LogicalOperator::Join(Join::Cross(_)) => binary_product(),
        LogicalOperator::Join(Join::Comparison(join)) => match join.join_type {
            JoinType::Semi | JoinType::Anti | JoinType::Mark | JoinType::Single => {
                child_maximum_cardinalities.first().copied().flatten()
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                child_maximum_cardinalities.get(1).copied().flatten()
            }
            JoinType::Inner => binary_product(),
            JoinType::Left => child_maximum_cardinalities
                .first()
                .copied()
                .flatten()
                .and_then(|left| {
                    child_maximum_cardinalities
                        .get(1)
                        .copied()
                        .flatten()
                        .and_then(|right| left.checked_mul(right.max(1)))
                }),
            JoinType::Right => child_maximum_cardinalities
                .get(1)
                .copied()
                .flatten()
                .and_then(|right| {
                    child_maximum_cardinalities
                        .first()
                        .copied()
                        .flatten()
                        .and_then(|left| right.checked_mul(left.max(1)))
                }),
            JoinType::Outer => (|| {
                let left = child_maximum_cardinalities.first().copied().flatten()?;
                let right = child_maximum_cardinalities.get(1).copied().flatten()?;
                left.checked_mul(right)?
                    .checked_add(left)?
                    .checked_add(right)
            })(),
            JoinType::Invalid => None,
        },
        LogicalOperator::SetOperation(set) => match set.setop_type {
            paro_planner::operator::SetOpType::Union => {
                let left = child_maximum_cardinalities.first().copied().flatten();
                let right = child_maximum_cardinalities.get(1).copied().flatten();
                left.zip(right)
                    .and_then(|(left, right)| left.checked_add(right))
            }
            paro_planner::operator::SetOpType::Intersect => {
                let left = child_maximum_cardinalities.first().copied().flatten();
                let right = child_maximum_cardinalities.get(1).copied().flatten();
                left.zip(right).map(|(left, right)| left.min(right))
            }
            paro_planner::operator::SetOpType::Except => {
                child_maximum_cardinalities.first().copied().flatten()
            }
        },
        LogicalOperator::MaterializedCTE(_) => {
            child_maximum_cardinalities.get(1).copied().flatten()
        }
        _ => None,
    };
    LogicalProperties {
        maximum_cardinality,
        ..Default::default()
    }
}

pub(super) fn derive_group_cardinality(
    operator: &LogicalOperator,
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
        paro_planner::plan::CardinalityProvenance::Statistics => CardinalityRecipeKind::Statistics,
        paro_planner::plan::CardinalityProvenance::JoinGraph => CardinalityRecipeKind::JoinRegion,
    };
    stats
        .estimated_cardinality
        .map_or(GroupCardinality::unknown(recipe, kind), |estimate| {
            GroupCardinality::new(recipe, kind, estimate.min, estimate.expected, estimate.max)
        })
}

pub(super) fn planner_grant_dependency(operator: &LogicalOperator) -> GrantDependencyDescriptor {
    if matches!(
        operator,
        LogicalOperator::Aggregate(_)
            | LogicalOperator::Distinct(_)
            | LogicalOperator::Order(_)
            | LogicalOperator::TopN(_)
            | LogicalOperator::Window(_)
            | LogicalOperator::MaterializedCTE(_)
            | LogicalOperator::RecursiveCTE(_)
            | LogicalOperator::Join(Join::Comparison(_))
            | LogicalOperator::Join(Join::Cross(_))
    ) {
        GrantDependencyDescriptor::Sensitive
    } else {
        GrantDependencyDescriptor::Invariant
    }
}

pub(super) fn planner_operator_spillable(operator: &LogicalOperator) -> bool {
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

pub(super) fn planner_structural_retained_children(operator: &LogicalOperator) -> u64 {
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

pub(super) fn planner_cost_composition(
    metadata: &PlannerOperatorMetadata,
    flavor: PhysicalImplementationFlavor,
) -> CostComposition {
    let overlapping_children = match flavor {
        PhysicalImplementationFlavor::HashJoin
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
    if overlapping_children == 0 {
        CostComposition::Sequential
    } else {
        CostComposition::RetainedState {
            overlapping_children,
        }
    }
}

pub(super) fn append_grant_fingerprint(
    fingerprint: &mut StableFingerprintBuilder,
    dependency: GrantDependencyDescriptor,
    grant: GrantGoalKey,
) {
    if dependency == GrantDependencyDescriptor::Sensitive {
        fingerprint.write_u64(grant.stable_tag());
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
    if dependency == GrantDependencyDescriptor::Invariant {
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
        return Ok(None);
    }
    if force_spill && spillable {
        if class.spill_policy == SpillPolicy::Forbidden {
            return Ok(None);
        } else {
            let spilled = cost.peak_memory_upper.max(1);
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
            .peak_memory_upper
            .saturating_sub(class.hard_memory_bytes);
        cost.peak_memory_upper = class.hard_memory_bytes;
        cost.revocable_memory_target = cost
            .revocable_memory_target
            .min(class.hard_memory_bytes - cost.minimum_memory_bytes);
        add_spill_cost(&mut cost, spilled)?;
        cost.validate()?;
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

pub(super) fn provided_result_guarantee(operator: &LogicalOperator) -> ResultGuarantee {
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
            fingerprint.write_u64(search.score_projection_index as u64);
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
pub(super) fn required_result_guarantee(plan: &LogicalPlan) -> ResultGuarantee {
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
            concurrency_class: 1,
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

        assert!(cost_for_grant(
            cost(u64::MAX, 800 * 1024),
            GrantDependencyDescriptor::Sensitive,
            true,
            grant,
            &BTreeMap::from([(no_spill.id, no_spill)]),
            false,
        )
        .unwrap()
        .is_none());
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
    fn forced_external_representation_is_not_faked_in_a_no_spill_class() {
        let no_spill = class(SpillPolicy::Forbidden);
        assert!(cost_for_grant(
            cost(900 * 1024, 800 * 1024),
            GrantDependencyDescriptor::Sensitive,
            true,
            GrantGoalKey::Class(no_spill.id),
            &BTreeMap::from([(no_spill.id, no_spill)]),
            true,
        )
        .unwrap()
        .is_none());
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
}
