// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Independent whole-frozen-DAG derivation used only by contract tests.
//! Production uses dependency-driven local selected properties.

use super::*;

pub(super) fn frozen_choice_fingerprint(frozen: &FrozenCandidate) -> Fingerprint {
    candidate_choice_fingerprint(
        frozen.reference,
        &frozen.winner,
        &frozen.logical,
        &frozen.physical,
    )
}

pub(super) fn collect_frozen_choices(
    frozen: &FrozenCandidate,
    choices: &mut Vec<Fingerprint>,
    visited: &mut BTreeSet<CandidateId>,
) {
    if !visited.insert(frozen.reference.candidate) {
        return;
    }
    choices.push(frozen_choice_fingerprint(frozen));
    for child in frozen.children.iter() {
        collect_frozen_choices(child, choices, visited);
    }
}

pub(super) fn selected_aggregate_region_shape(
    frozen: &FrozenCandidate,
    state: &PlannerTransformState,
    visited: &mut BTreeSet<CandidateId>,
) -> SelectedAggregateRegionShape {
    if !visited.insert(frozen.reference.candidate) {
        return SelectedAggregateRegionShape::default();
    }
    let operator = state
        .payloads
        .logical
        .get(frozen.logical.payload.index())
        .map(|payload| &payload.semantic_template.operator);
    let mut shape = SelectedAggregateRegionShape::default();
    if matches!(operator, Some(LogicalOperator::Aggregate(_))) {
        shape.aggregates = 1;
        if frozen.children.len() == 1 {
            let join = &frozen.children[0];
            if let Some(LogicalOperator::Join(Join::Comparison(join_operator))) = state
                .payloads
                .logical
                .get(join.logical.payload.index())
                .map(|payload| &payload.semantic_template.operator)
            {
                if !join_operator.conditions.is_empty() && join.children.len() == 2 {
                    let outer = match operator {
                        Some(LogicalOperator::Aggregate(outer)) => outer,
                        _ => unreachable!("aggregate operator disappeared during inspection"),
                    };
                    shape.decomposed = join.children.iter().any(|partial| {
                        matches!(
                            state
                                .payloads
                                .logical
                                .get(partial.logical.payload.index())
                                .map(|payload| &payload.semantic_template.operator),
                            Some(LogicalOperator::Aggregate(partial))
                                if aggregate_merge_contract_matches(outer, partial)
                        )
                    });
                }
            }
        }
    }
    if matches!(operator, Some(LogicalOperator::Join(_))) {
        shape.joins = 1;
    }
    for child in frozen.children.iter() {
        let child_shape = selected_aggregate_region_shape(child, state, visited);
        shape.aggregates = shape.aggregates.saturating_add(child_shape.aggregates);
        shape.joins = shape.joins.saturating_add(child_shape.joins);
        shape.decomposed |= child_shape.decomposed;
    }
    shape
}

pub(super) fn collect_region_fact_fingerprint(
    memo: &Memo,
    root: &FrozenCandidate,
    arm: &FrozenCandidate,
    goal: OptimizationGoal,
) -> Option<Fingerprint> {
    let mut facts = BTreeMap::new();
    let mut visited = BTreeSet::new();
    fn visit(
        memo: &Memo,
        frozen: &FrozenCandidate,
        facts: &mut BTreeMap<GroupId, (Fingerprint, Fingerprint)>,
        visited: &mut BTreeSet<CandidateId>,
    ) -> bool {
        if !visited.insert(frozen.reference.candidate) {
            return true;
        }
        let group = memo.canonical_group(frozen.reference.group);
        let Some(group_ref) = memo.group(group) else {
            return false;
        };
        facts.insert(
            group,
            (
                group_ref.logical_fact_fingerprint(),
                group_ref.statistics_snapshot_fingerprint(),
            ),
        );
        frozen
            .children
            .iter()
            .all(|child| visit(memo, child, facts, visited))
    }
    if !visit(memo, root, &mut facts, &mut visited) || !visit(memo, arm, &mut facts, &mut visited) {
        return None;
    }
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.quality.aggregate-region-facts.v1");
    fingerprint.write_u64(goal.required.0 as u64);
    fingerprint.write_u64(goal.grant.stable_tag());
    fingerprint.write_u64(goal.row_goal.stable_tag());
    fingerprint.write_u64(goal.objective.stable_tag());
    fingerprint.write_u64(goal.context.0 as u64);
    fingerprint.write_u64(facts.len() as u64);
    for (group, (logical, statistics)) in facts {
        fingerprint.write_u64(group.0 as u64);
        fingerprint.write_fingerprint(logical);
        fingerprint.write_fingerprint(statistics);
    }
    Some(fingerprint.finish())
}

pub(super) fn selected_subtree_contains_union(
    frozen: &FrozenCandidate,
    state: &PlannerTransformState,
    visited: &mut BTreeSet<CandidateId>,
) -> bool {
    if !visited.insert(frozen.reference.candidate) {
        return false;
    }
    let is_union = state
        .payloads
        .logical
        .get(frozen.logical.payload.index())
        .is_some_and(|payload| {
            matches!(
                &payload.semantic_template.operator,
                LogicalOperator::SetOperation(setop)
                    if setop.setop_type == paro_planner::operator::SetOpType::Union
                        && setop.setop_all
            )
        });
    is_union
        || frozen
            .children
            .iter()
            .any(|child| selected_subtree_contains_union(child, state, visited))
}

pub(super) fn selected_aggregate_region_witnesses(
    memo: &Memo,
    state: &PlannerTransformState,
    root: &FrozenCandidate,
    goal: OptimizationGoal,
) -> Option<Vec<AggregateRegionWitness>> {
    let mut witnesses = Vec::new();
    let mut path = Vec::new();
    fn visit_union(
        memo: &Memo,
        state: &PlannerTransformState,
        union: &FrozenCandidate,
        root_candidate: CandidateId,
        goal: OptimizationGoal,
        path: &mut Vec<u32>,
        witnesses: &mut Vec<AggregateRegionWitness>,
    ) -> Option<()> {
        let operator = state
            .payloads
            .logical
            .get(union.logical.payload.index())
            .map(|payload| &payload.semantic_template.operator);
        if let Some(LogicalOperator::SetOperation(setop)) = operator {
            if setop.setop_type == paro_planner::operator::SetOpType::Union && setop.setop_all {
                for (index, arm) in union.children.iter().enumerate() {
                    path.push(index as u32);
                    let mut shape_visited = BTreeSet::new();
                    let shape = selected_aggregate_region_shape(arm, state, &mut shape_visited);
                    let mut union_visited = BTreeSet::new();
                    if shape.aggregates > 0
                        && shape.joins > 0
                        && !selected_subtree_contains_union(arm, state, &mut union_visited)
                    {
                        let mut choices = Vec::new();
                        let mut choice_visited = BTreeSet::new();
                        collect_frozen_choices(arm, &mut choices, &mut choice_visited);
                        let fact_fingerprint =
                            collect_region_fact_fingerprint(memo, union, arm, goal)?;
                        let mut region = StableFingerprintBuilder::default();
                        region.write_bytes(b"paro.quality.aggregate-region.v3");
                        region.write_u64(root_candidate.index() as u64);
                        region.write_u64(arm.reference.candidate.index() as u64);
                        region.write_fingerprint(frozen_choice_fingerprint(union));
                        region.write_u64(path.len() as u64);
                        for component in path.iter().copied() {
                            region.write_u64(component as u64);
                        }
                        region.write_fingerprint(fact_fingerprint);
                        // The oracle still expands the complete subtree. Its
                        // first element must be the exact anchor consumed by
                        // the production compositional witness.
                        let anchor_choice = *choices.first()?;
                        assert_eq!(anchor_choice, frozen_choice_fingerprint(arm));
                        region.write_fingerprint(anchor_choice);
                        witnesses.push(AggregateRegionWitness {
                            region: region.finish(),
                            candidate: root_candidate,
                            anchor: arm.reference.candidate,
                            fact_fingerprint,
                            anchor_choice,
                            covered: shape.decomposed,
                        });
                    }
                    visit_union(memo, state, arm, root_candidate, goal, path, witnesses)?;
                    path.pop();
                }
                return Some(());
            }
        }
        for child in union.children.iter() {
            visit_union(memo, state, child, root_candidate, goal, path, witnesses)?;
        }
        Some(())
    }
    visit_union(
        memo,
        state,
        root,
        root.reference.candidate,
        goal,
        &mut path,
        &mut witnesses,
    )?;
    if witnesses.is_empty() {
        let mut shape_visited = BTreeSet::new();
        let shape = selected_aggregate_region_shape(root, state, &mut shape_visited);
        if shape.aggregates > 0 {
            let mut choices = Vec::new();
            let mut choice_visited = BTreeSet::new();
            collect_frozen_choices(root, &mut choices, &mut choice_visited);
            let fact_fingerprint = collect_region_fact_fingerprint(memo, root, root, goal)?;
            let mut region = StableFingerprintBuilder::default();
            region.write_bytes(b"paro.quality.aggregate-region.root.v2");
            region.write_u64(root.reference.candidate.index() as u64);
            region.write_fingerprint(fact_fingerprint);
            let anchor_choice = *choices.first()?;
            assert_eq!(anchor_choice, frozen_choice_fingerprint(root));
            region.write_fingerprint(anchor_choice);
            witnesses.push(AggregateRegionWitness {
                region: region.finish(),
                candidate: root.reference.candidate,
                anchor: root.reference.candidate,
                fact_fingerprint,
                anchor_choice,
                covered: shape.decomposed,
            });
        }
    }
    Some(witnesses)
}

pub(super) fn frozen_quality_evidence(
    memo: &Memo,
    reference: ChildWinnerRef,
    frozen: &FrozenCandidate,
    goal: OptimizationGoal,
    state: &PlannerTransformState,
) -> Result<Option<NativeQualityEvidence>> {
    let _partition =
        crate::diagnostics::work::enter(crate::diagnostics::work::Bucket::QualityEvidence);
    let Some(required) = memo.required(goal.required) else {
        return Ok(None);
    };
    let mut capabilities = BTreeSet::new();
    let mut facts = BTreeSet::new();
    #[derive(Default)]
    struct QualityWalk {
        nodes: Vec<QualityCandidateNode>,
        choices: Vec<Fingerprint>,
        rules: BTreeSet<RuleId>,
        shape: NativeQualityShape,
        cte_producers: BTreeSet<usize>,
        cte_consumers: BTreeSet<usize>,
        cte_producer_witnesses: BTreeSet<usize>,
        has_filter: bool,
        has_get: bool,
        has_join: bool,
        has_join_region: bool,
        has_aggregate: bool,
        has_cte_consumer: bool,
        has_cte_producer: bool,
        has_ordering: bool,
        has_graph: bool,
        has_dependent: bool,
        exact_contract: bool,
        visited: BTreeSet<CandidateId>,
    }

    fn visit(
        frozen: &FrozenCandidate,
        state: &PlannerTransformState,
        walk: &mut QualityWalk,
    ) -> Result<()> {
        let QualityWalk {
            nodes,
            choices,
            rules,
            shape,
            cte_producers,
            cte_consumers,
            cte_producer_witnesses: _,
            has_filter,
            has_get,
            has_join,
            has_join_region,
            has_aggregate,
            has_cte_consumer,
            has_cte_producer,
            has_ordering,
            has_graph,
            has_dependent,
            exact_contract,
            visited,
        } = walk;
        if !visited.insert(frozen.reference.candidate) {
            return Ok(());
        }
        shape.nodes = shape.nodes.saturating_add(1);
        if frozen.physical.id != frozen.winner.expression
            || frozen.logical.id != frozen.physical.key.logical
            || frozen.physical.key.children.len() != frozen.logical.key.children.len()
            || frozen
                .physical
                .key
                .children
                .iter()
                .zip(frozen.logical.key.children.iter())
                .any(|(physical_child, logical_child)| *physical_child != *logical_child)
            || frozen.children.len() != frozen.winner.children.len()
            || frozen
                .children
                .iter()
                .zip(frozen.winner.children.iter())
                .any(|(child, reference)| child.reference != *reference)
        {
            *exact_contract = false;
            return Ok(());
        }
        let Some(metadata) = state.metadata.get(&frozen.logical.payload) else {
            *exact_contract = false;
            return Ok(());
        };
        let Some(physical_payload) = state.payloads.get_physical(frozen.physical.payload) else {
            *exact_contract = false;
            return Ok(());
        };
        if !selected_physical_contract_is_exact(
            &frozen.logical,
            &frozen.physical,
            metadata,
            &physical_payload,
        ) {
            *exact_contract = false;
            return Ok(());
        }
        choices.push(frozen_choice_fingerprint(frozen));
        nodes.push(QualityCandidateNode {
            reference: frozen.reference,
            logical: frozen.logical.id,
            physical: frozen.physical.id,
            children: Arc::from(frozen.winner.children.as_ref()),
        });
        let selected_rules: BTreeSet<_> =
            selected_payload_rule_proofs(&frozen.logical, metadata).collect();
        if metadata.origin_rule.is_some() && selected_rules.is_empty() {
            // The sidecar says this payload came from a rule, but the
            // selected Memo expression has no corresponding equivalence
            // proof.  Fail closed instead of trusting origin metadata.
            *exact_contract = false;
            return Ok(());
        }
        rules.extend(selected_rules.iter().copied());
        *exact_contract &= frozen.winner.provided.result_guarantee == ResultGuarantee::Exact
            && metadata.provided.result_guarantee == ResultGuarantee::Exact;
        match metadata.operator_type {
            LogicalOperatorType::Filter | LogicalOperatorType::FullTextFilterScan => {
                *has_filter = true
            }
            LogicalOperatorType::Get
            | LogicalOperatorType::SearchScan
            | LogicalOperatorType::TableFunctionGet => *has_get = true,
            LogicalOperatorType::ComparisonJoin
            | LogicalOperatorType::AnyJoin
            | LogicalOperatorType::CrossProduct => {
                *has_join = true;
                *has_join_region |= frozen.winner.joint_cost_proof.is_some();
                shape.joins = shape.joins.saturating_add(1);
                if frozen.winner.joint_cost_proof.is_some() {
                    shape.join_region_witness_nodes =
                        shape.join_region_witness_nodes.saturating_add(1);
                }
            }
            LogicalOperatorType::Aggregate => {
                *has_aggregate = true;
                shape.aggregates = shape.aggregates.saturating_add(1);
            }
            LogicalOperatorType::CTERef => {
                *has_cte_consumer = true;
                if let LogicalOperator::CTERef(reference) = &state.payloads.logical
                    [frozen.logical.payload.index()]
                .semantic_template
                .operator
                {
                    cte_consumers.insert(reference.cte_index);
                }
            }
            LogicalOperatorType::MaterializedCTE | LogicalOperatorType::RecursiveCTE => {
                *has_cte_producer = true;
                let cte_index = match &state.payloads.logical[frozen.logical.payload.index()]
                    .semantic_template
                    .operator
                {
                    LogicalOperator::MaterializedCTE(cte) => Some(cte.cte_index),
                    LogicalOperator::RecursiveCTE(cte) => Some(cte.cte_index),
                    _ => None,
                };
                if let Some(cte_index) = cte_index {
                    cte_producers.insert(cte_index);
                }
            }
            LogicalOperatorType::Order | LogicalOperatorType::TopN => *has_ordering = true,
            LogicalOperatorType::DependentJoin => *has_dependent = true,
            LogicalOperatorType::GraphMatch
            | LogicalOperatorType::GraphScan
            | LogicalOperatorType::GraphExpand
            | LogicalOperatorType::CreatePropertyGraph => *has_graph = true,
            _ => {}
        }
        if matches!(
            frozen.physical.key.implementation,
            PLANNER_HASH_JOIN_RUNTIME_FILTER | PLANNER_HASH_JOIN_BUILD_LEFT_RUNTIME_FILTER
        ) {
            shape.runtime_filter_joins = shape.runtime_filter_joins.saturating_add(1);
        }
        for child in frozen.children.iter() {
            visit(child, state, walk)?;
        }
        Ok(())
    }

    let mut walk = QualityWalk {
        exact_contract: true,
        ..QualityWalk::default()
    };
    visit(frozen, state, &mut walk)?;
    walk.cte_producer_witnesses = quality_domain::cte_domain_witnesses(memo, &walk.nodes, state);
    let QualityWalk {
        choices,
        rules,
        mut shape,
        cte_producers,
        cte_consumers,
        cte_producer_witnesses,
        has_filter,
        has_get,
        has_join,
        has_join_region,
        has_aggregate,
        has_cte_consumer,
        has_cte_producer,
        has_ordering,
        has_graph,
        has_dependent,
        exact_contract,
        ..
    } = walk;
    if !exact_contract || choices.is_empty() {
        return Ok(None);
    }

    if frozen.winner.provided.satisfies(required) {
        facts.insert(BundleFact::OutputDemand);
    }
    let Some(pending_domain_transfers) = quality_domain::pending_transfers(frozen, state) else {
        return Ok(None);
    };
    if pending_domain_transfers.is_empty() && has_filter && has_get {
        facts.insert(BundleFact::PredicateDomain);
    }
    if has_join && has_join_region {
        facts.insert(BundleFact::JoinRegion);
    }
    let Some(aggregate_regions) = selected_aggregate_region_witnesses(memo, state, frozen, goal)
    else {
        return Ok(None);
    };
    shape.aggregate_witness_nodes = aggregate_regions
        .iter()
        .filter(|witness| witness.covered)
        .count() as u32;
    if has_aggregate
        && !aggregate_regions.is_empty()
        && aggregate_regions.iter().all(|witness| witness.covered)
    {
        facts.insert(BundleFact::AggregateDecomposition);
    }
    // A CTE bundle is complete only when every selected producer domain
    // that has a selected consumer has its own proof-bearing restriction.
    // This prevents one branch/consumer's transform from certifying a
    // different branch that merely shares the same CTE index.
    if has_cte_consumer
        && has_cte_producer
        && !cte_consumers.is_empty()
        && cte_consumers.is_subset(&cte_producers)
        && cte_producers.is_subset(&cte_producer_witnesses)
    {
        facts.insert(BundleFact::CteConsumerDemand);
    }
    if exact_contract && (has_aggregate || has_join) {
        facts.insert(BundleFact::NullSemantics);
    }
    if exact_contract {
        facts.insert(BundleFact::ProviderCapability);
    }
    if matches!(
        frozen.winner.provided.ordering,
        ProvidedOrdering::Ordered { .. }
    ) {
        facts.insert(BundleFact::OrderingDemand);
    }

    if has_filter && has_get {
        capabilities.insert(BundleCapability::ScanPredicate);
    }
    if has_join {
        capabilities.insert(BundleCapability::SmallJoin);
    }
    if has_aggregate && has_cte_consumer && has_cte_producer {
        capabilities.insert(BundleCapability::SharedAggregate);
    }
    if has_dependent {
        capabilities.insert(BundleCapability::CorrelatedSubquery);
    }
    if has_ordering {
        capabilities.insert(BundleCapability::Ordering);
    }
    if has_graph {
        capabilities.insert(BundleCapability::GraphProvider);
    }

    let mut region = StableFingerprintBuilder::default();
    region.write_bytes(b"paro.quality.native-region.v1");
    region.write_u64(reference.group.0 as u64);
    region.write_u64(reference.candidate.index() as u64);
    region.write_fingerprint(frozen.winner.physical_fingerprint);
    for choice in &choices {
        region.write_fingerprint(*choice);
    }
    let region = region.finish();
    let mut proof = StableFingerprintBuilder::default();
    proof.write_bytes(b"paro.quality.native-evidence.v1");
    proof.write_fingerprint(region);
    proof.write_u64(rules.len() as u64);
    for rule in &rules {
        proof.write_u64(rule.0 as u64);
    }
    proof.write_u64(capabilities.len() as u64);
    proof.write_u64(facts.len() as u64);
    proof.write_u64(aggregate_regions.len() as u64);
    for witness in &aggregate_regions {
        proof.write_fingerprint(witness.region);
        proof.write_fingerprint(witness.fact_fingerprint);
        proof.write_u64(u64::from(witness.covered));
    }
    Ok(Some(NativeQualityEvidence {
        pending_domain_transfers,
        capabilities,
        facts,
        region,
        applicability_proof: proof.finish(),
        choices: choices.into_boxed_slice(),
        aggregate_regions: aggregate_regions.into_boxed_slice(),
        selected_rules: rules.into_iter().collect(),
        shape,
    }))
}
