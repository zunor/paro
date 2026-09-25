// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Requests owned by the missing obligations of one executable candidate.
//!
//! This orders existing tasks; it does not discharge an obligation, remove a
//! legal alternative, or replace the model-cost frontier or quality policy.

use super::*;
use crate::cascades::quality::{
    BundleFact, NativeQualityEvidence, QualityCandidateNode, QualityEvidenceProvider,
};
use crate::cascades::rules::{DomainContinuation, QualityDependency};
use crate::cascades::tasks::ReadSetId;

type ProductionObligation = (BundleFact, GroupId);
type SelectedLogicalChoice = (GroupId, LogicalExprId);

#[derive(Debug)]
pub(super) struct QualityProductionRequest {
    candidate: CandidateId,
    reads: ReadSetId,
    obligations: BTreeMap<ProductionObligation, BTreeSet<SelectedLogicalChoice>>,
    /// Exact selected-path bindings which can run in this Memo before the
    /// broad PredicateTransfer matcher. The ordinary task remains queued and
    /// still covers every other legal alternative.
    domain_bindings: Box<[PatternBinding]>,
    deficit: usize,
    cost: f64,
}

impl QualityProductionRequest {
    fn from_preflight(
        memo: &Memo,
        reference: ChildWinnerRef,
        nodes: &[QualityCandidateNode],
        reads: ReadSetId,
        evidence: &NativeQualityEvidence,
        missing: &[BundleFact],
        domain_bindings: Box<[PatternBinding]>,
    ) -> Option<Self> {
        let candidate = reference.candidate;
        let mut by_candidate = BTreeMap::new();
        for node in nodes {
            if node.reference.candidate == candidate
                && (node.reference.group != reference.group
                    || node.reference.goal != reference.goal)
            {
                return None;
            }
            if by_candidate
                .insert(node.reference.candidate, node)
                .is_some()
            {
                return None;
            }
        }
        if !by_candidate.contains_key(&candidate) {
            return None;
        }
        if evidence
            .aggregate_regions
            .iter()
            .any(|region| region.candidate != candidate)
            || evidence
                .aggregate_regions
                .iter()
                .any(|region| !by_candidate.contains_key(&region.anchor))
            || evidence
                .pending_domain_transfers
                .iter()
                .any(|node| !by_candidate.contains_key(node))
        {
            return None;
        }

        let mut obligations = BTreeMap::new();
        let mut uncovered = 0_usize;
        for &fact in missing {
            if fact == BundleFact::AggregateDecomposition {
                for region in evidence
                    .aggregate_regions
                    .iter()
                    .filter(|region| !region.covered)
                {
                    let arm = by_candidate.get(&region.anchor)?;
                    let mut choices = BTreeSet::new();
                    let mut pending = vec![arm.reference];
                    let mut visited = BTreeSet::new();
                    while let Some(node_reference) = pending.pop() {
                        if !visited.insert(node_reference.candidate) {
                            continue;
                        }
                        let node = *by_candidate.get(&node_reference.candidate)?;
                        choices.insert((memo.canonical_group(node.reference.group), node.logical));
                        pending.extend(node.children.iter().copied());
                    }
                    uncovered += 1;
                    obligations.insert((fact, memo.canonical_group(arm.reference.group)), choices);
                }
            } else if fact == BundleFact::PredicateDomain
                && !evidence.pending_domain_transfers.is_empty()
            {
                for candidate in &evidence.pending_domain_transfers {
                    let node = *by_candidate.get(candidate)?;
                    let group = memo.canonical_group(node.reference.group);
                    obligations
                        .entry((fact, group))
                        .or_insert_with(BTreeSet::new)
                        .insert((group, node.logical));
                }
            } else if matches!(
                fact,
                BundleFact::PredicateDomain
                    | BundleFact::CteConsumerDemand
                    | BundleFact::JoinRegion
            ) {
                obligations.insert(
                    (fact, memo.canonical_group(reference.group)),
                    by_candidate
                        .values()
                        .map(|node| (memo.canonical_group(node.reference.group), node.logical))
                        .collect(),
                );
            }
        }

        let cost = memo
            .resolve_child_winner(reference)?
            .cost
            .score
            .range
            .expected;
        Some(Self {
            candidate,
            reads,
            obligations,
            domain_bindings,
            deficit: missing.len().saturating_add(uncovered.saturating_sub(1)),
            cost,
        })
    }

    #[cfg(test)]
    fn from_candidate(
        memo: &Memo,
        frozen: &Arc<FrozenCandidate>,
        reads: ReadSetId,
        evidence: &NativeQualityEvidence,
        missing: &[BundleFact],
    ) -> Option<Self> {
        let candidate = frozen.reference.candidate;
        if evidence
            .aggregate_regions
            .iter()
            .any(|region| region.candidate != candidate)
        {
            return None;
        }
        let mut nodes = BTreeMap::new();
        let mut pending = vec![frozen];
        while let Some(node) = pending.pop() {
            if nodes.insert(node.reference.candidate, node).is_none() {
                pending.extend(node.children.iter());
            }
        }
        // Even a covered witness must belong to this exact frozen DAG.
        if evidence
            .aggregate_regions
            .iter()
            .any(|region| !nodes.contains_key(&region.anchor))
            || evidence
                .pending_domain_transfers
                .iter()
                .any(|choice| !nodes.contains_key(choice))
        {
            return None;
        }
        let mut obligations = BTreeMap::new();
        let mut uncovered = 0_usize;
        for &fact in missing {
            if fact == BundleFact::AggregateDecomposition {
                for region in evidence
                    .aggregate_regions
                    .iter()
                    .filter(|region| !region.covered)
                {
                    let arm = *nodes.get(&region.anchor)?;
                    let mut choices = BTreeSet::new();
                    let mut visited = BTreeSet::new();
                    let mut pending = vec![arm];
                    while let Some(node) = pending.pop() {
                        if visited.insert(node.reference.candidate) {
                            choices.insert((
                                memo.canonical_group(node.reference.group),
                                node.logical.id,
                            ));
                            pending.extend(node.children.iter());
                        }
                    }
                    uncovered += 1;
                    obligations.insert((fact, memo.canonical_group(arm.reference.group)), choices);
                }
            } else if fact == BundleFact::PredicateDomain
                && !evidence.pending_domain_transfers.is_empty()
            {
                for choice in &evidence.pending_domain_transfers {
                    let node = *nodes.get(choice)?;
                    let group = memo.canonical_group(node.reference.group);
                    obligations
                        .entry((fact, group))
                        .or_insert_with(BTreeSet::new)
                        .insert((group, node.logical.id));
                }
            } else if matches!(
                fact,
                BundleFact::PredicateDomain
                    | BundleFact::CteConsumerDemand
                    | BundleFact::JoinRegion
            ) {
                // Producers are exact selected logical choices, not every
                // alternative in those groups. Rule dispatch supplies the
                // operator applicability and exact binding/fact subscriptions.
                obligations.insert(
                    (fact, memo.canonical_group(frozen.reference.group)),
                    nodes
                        .values()
                        .map(|node| (memo.canonical_group(node.reference.group), node.logical.id))
                        .collect(),
                );
            }
        }
        Some(Self {
            candidate,
            reads,
            obligations,
            domain_bindings: Box::new([]),
            // Count the policy's real missing facts, including unsupported
            // ones. Two missing aggregate regions cannot count as one done
            // region, and aggregate coverage alone cannot hide missing domains.
            deficit: missing.len().saturating_add(uncovered.saturating_sub(1)),
            cost: frozen.winner.cost.score.range.expected,
        })
    }

    fn prefers(&self, other: &Self) -> bool {
        self.deficit
            .cmp(&other.deficit)
            .then_with(|| self.cost.total_cmp(&other.cost))
            .then_with(|| self.candidate.cmp(&other.candidate))
            .is_lt()
    }
}

fn produces(fact: BundleFact, dependency: Option<QualityDependency>) -> bool {
    // Join choices consume restricted input domains as well as join-order
    // alternatives. A producer-level filter can satisfy the coarse domain
    // bundle while its safe propagation into this selected join's inputs is
    // still pending. Keep those exact domain producers eligible for an
    // outstanding join request; this is not additional completion evidence.
    matches!(
        (fact, dependency),
        (
            BundleFact::AggregateDecomposition,
            Some(QualityDependency::NarrowAggregate)
        ) | (
            BundleFact::PredicateDomain,
            Some(QualityDependency::DomainRestriction)
        ) | (
            BundleFact::CteConsumerDemand,
            Some(QualityDependency::ConsumerDemand | QualityDependency::DomainRestriction)
        ) | (
            BundleFact::JoinRegion,
            Some(QualityDependency::DomainRestriction | QualityDependency::JoinSelection)
        )
    )
}

impl StableAgenda {
    fn matching_choice_key(
        &self,
        (group, expression): SelectedLogicalChoice,
        mut relevant: impl FnMut(RuleId) -> bool,
    ) -> Option<TaskKey> {
        let start = SearchTask::Transform {
            group,
            expression,
            rule: RuleId(0),
        };
        self.keys.range(start..)
            .take_while(|(task, _)| matches!(task, SearchTask::Transform { group: owner, expression: selected, .. } if *owner == group && *selected == expression))
            .filter(|(task, _)| matches!(task, SearchTask::Transform { rule, .. } if relevant(*rule)))
            .map(|(_, key)| *key)
            .min()
    }
}

impl CascadesEngine {
    fn enqueue_quality_forced_binding(&mut self, goal: OptimizationGoal, binding: PatternBinding) {
        let task = TransformationTaskId {
            group: self.memo.canonical_group(binding.root_group()),
            expression: binding.root_expression(),
            rule: crate::cascades::rules::PREDICATE_TRANSFER_RULE,
            binding: Some(binding.fingerprint),
        };
        let queue = self
            .quality_forced_transform_bindings
            .entry((goal, task))
            .or_default();
        if !queue.iter().any(|existing| existing == &binding) {
            queue.push_back(binding);
        }
    }

    fn quality_request_is_preferred(
        &self,
        goal: OptimizationGoal,
        request: &QualityProductionRequest,
    ) -> bool {
        let Some(previous) = self.quality_production_requests.get(&goal) else {
            return true;
        };
        let current = self
            .task_registry
            .read_set(previous.reads)
            .is_some_and(|reads| reads.is_current(&self.memo).is_ok_and(|current| current));
        !current || request.prefers(previous)
    }

    fn install_quality_production_request(
        &mut self,
        goal: OptimizationGoal,
        request: QualityProductionRequest,
    ) -> Result<()> {
        // Request preference depends on obligations/cost/identity, not on
        // the selected-path binding payload. Keep the existing validated
        // request before replacing it with work that ranks worse. A stale
        // read set still requires a replacement; this is not a certificate
        // cache.
        if !self.quality_request_is_preferred(goal, &request) {
            return Ok(());
        }
        // Continuations belong to the exact request whose selected path
        // produced them.  Replacing that request must not let a stale path
        // leak into a new candidate's quality lane.
        self.quality_pending_domain_continuations.remove(&goal);
        self.quality_forced_transform_bindings
            .retain(|(entry_goal, _), _| *entry_goal != goal);
        self.quality_production_requests.insert(goal, request);
        let bindings = self
            .quality_production_requests
            .get(&goal)
            .map(|request| request.domain_bindings.to_vec())
            .unwrap_or_default();
        for binding in bindings {
            self.enqueue_quality_forced_binding(goal, binding);
        }
        Ok(())
    }

    /// Publish a continuation only after its producer transaction committed.
    /// The pending sidecar keeps exact reads/context until the existing forced
    /// transformation queue dispatches the binding; no second scheduler is
    /// introduced and an ordinary matcher remains responsible for all other
    /// legal alternatives.
    pub(super) fn enqueue_quality_domain_continuations(
        &mut self,
        goal: OptimizationGoal,
        continuations: Vec<DomainContinuation>,
    ) -> Result<()> {
        if continuations.is_empty() || !self.quality_production_requests.contains_key(&goal) {
            return Ok(());
        }
        let (enqueued, bindings) = {
            let pending = self
                .quality_pending_domain_continuations
                .entry(goal)
                .or_default();
            let mut enqueued = 0_u64;
            for continuation in continuations {
                if self
                    .memo
                    .group(self.memo.canonical_group(continuation.hole))
                    .is_none()
                {
                    continue;
                }
                if !pending.iter().any(|existing| {
                    existing.binding == continuation.binding
                        && existing.context == continuation.context
                        && existing.occurrence == continuation.occurrence
                        && existing.predicates.len() == continuation.predicates.len()
                        && existing
                            .predicates
                            .iter()
                            .zip(continuation.predicates.iter())
                            .all(|(left, right)| {
                                paro_planner::physical::scalar_identity::expression_fingerprint(left)
                                    == paro_planner::physical::scalar_identity::expression_fingerprint(
                                        right,
                                    )
                            })
                }) {
                    pending.push(continuation);
                    enqueued = enqueued.saturating_add(1);
                }
            }
            let bindings = pending
                .iter()
                .map(|continuation| continuation.binding.clone())
                .collect::<Vec<_>>();
            (enqueued, bindings)
        };
        self.quality_domain_continuation_enqueued_count = self
            .quality_domain_continuation_enqueued_count
            .saturating_add(enqueued);
        for binding in bindings {
            self.enqueue_quality_forced_binding(goal, binding);
        }
        Ok(())
    }

    pub(super) fn record_quality_production_request_preflight(
        &mut self,
        provider: &dyn QualityEvidenceProvider,
        selected: (OptimizationGoal, ChildWinnerRef, &Winner),
        nodes: &[QualityCandidateNode],
        reads: ReadSetId,
        evidence: &NativeQualityEvidence,
        missing: &[BundleFact],
    ) -> Result<()> {
        let (goal, reference, winner) = selected;
        let _partition =
            crate::diagnostics::work::enter(crate::diagnostics::work::Bucket::QualityProduction);
        let Some(mut request) = QualityProductionRequest::from_preflight(
            &self.memo,
            reference,
            nodes,
            reads,
            evidence,
            missing,
            Box::new([]),
        ) else {
            return Ok(());
        };
        // The preflight reference graph is cheap enough to decide whether a
        // request is preferred, but selected-path bindings are a production
        // payload.  Do not construct them for an obligation that will be
        // rejected by the current request/goal ordering.
        let needs_domain_bindings = missing.contains(&BundleFact::PredicateDomain);
        if !self.quality_request_is_preferred(goal, &request) {
            if needs_domain_bindings {
                self.quality_preflight_domain_binding_preference_skip_count = self
                    .quality_preflight_domain_binding_preference_skip_count
                    .saturating_add(1);
            }
            return Ok(());
        }
        if needs_domain_bindings {
            self.quality_preflight_domain_binding_provider_call_count = self
                .quality_preflight_domain_binding_provider_call_count
                .saturating_add(1);
            request.domain_bindings =
                provider.selected_domain_bindings(&self.memo, reference, winner, nodes, goal)?;
        }
        self.install_quality_production_request(goal, request)
    }

    #[cfg(test)]
    pub(super) fn record_quality_production_request(
        &mut self,
        goal: OptimizationGoal,
        frozen: &Arc<FrozenCandidate>,
        reads: ReadSetId,
        evidence: &NativeQualityEvidence,
        missing: &[BundleFact],
    ) -> Result<()> {
        let _partition =
            crate::diagnostics::work::enter(crate::diagnostics::work::Bucket::QualityProduction);
        let Some(mut request) =
            QualityProductionRequest::from_candidate(&self.memo, frozen, reads, evidence, missing)
        else {
            return Ok(());
        };
        // Keep this check before selected binding construction. The binding
        // is an ordering hint and must not be built for a request which is
        // already superseded by an equally current, better obligation.
        if !self.quality_request_is_preferred(goal, &request) {
            return Ok(());
        }
        if missing.contains(&BundleFact::PredicateDomain)
            && self
                .memo
                .budget()
                .transformation_enabled(crate::cascades::rules::PREDICATE_TRANSFER_RULE)
        {
            request.domain_bindings = self
                .registry
                .transformations()
                .find(|rule| rule.id() == crate::cascades::rules::PREDICATE_TRANSFER_RULE)
                .map(|rule| rule.selected_quality_bindings(&self.memo, frozen))
                .transpose()?
                .unwrap_or_default();
        }
        self.install_quality_production_request(goal, request)
    }

    pub(super) fn pop_transformation_task(
        &mut self,
        agenda: &mut StableAgenda,
    ) -> Result<Option<SearchTask>> {
        if !self.quality_handoff_enabled {
            return Ok(agenda.pop());
        }
        let mut stale = Vec::new();
        let mut obligations =
            BTreeMap::<ProductionObligation, BTreeSet<SelectedLogicalChoice>>::new();
        for (&goal, request) in &self.quality_production_requests {
            let current = match self.task_registry.read_set(request.reads) {
                Some(reads) => reads.is_current(&self.memo)?,
                None => false,
            };
            if !current || self.quality_ready_winners.contains_key(&goal) {
                stale.push(goal);
                continue;
            }
            for (&obligation, choices) in &request.obligations {
                obligations
                    .entry(obligation)
                    .or_default()
                    .extend(choices.iter().copied());
            }
        }
        for goal in stale {
            self.quality_production_requests.remove(&goal);
            self.quality_pending_domain_continuations.remove(&goal);
            self.quality_forced_transform_bindings
                .retain(|(entry_goal, _), _| *entry_goal != goal);
        }
        // Execute one exact selected path before taking a general quality
        // task. The ordinary task is left queued (or inserted if it was
        // already consumed), so this accelerator cannot narrow the legal
        // search domain or make the direct binding stand in for closure.
        if let Some((goal, task, _binding)) = self
            .quality_forced_transform_bindings
            .iter()
            .filter_map(|((goal, task), bindings)| {
                bindings
                    .front()
                    .cloned()
                    .map(|binding| (*goal, *task, binding))
            })
            .min_by_key(|(goal, task, binding)| (*goal, *task, binding.fingerprint))
        {
            let ordinary_task = SearchTask::Transform {
                group: task.group,
                expression: task.expression,
                rule: task.rule,
            };
            if !agenda.keys.contains_key(&ordinary_task) {
                let expression = self
                    .memo
                    .logical_expr(task.expression)
                    .ok_or_else(|| paro_error::internal("quality binding lost its expression"))?;
                let rule = self
                    .registry
                    .transformation(task.rule)
                    .ok_or_else(|| paro_error::internal("quality binding lost its rule"))?;
                let context = RuleContext {
                    memo: &self.memo,
                    group: task.group,
                };
                let quality_stage = self.quality_stage_for_rule(rule, false, false);
                agenda.push(
                    TaskKey {
                        demand_stage: 1,
                        quality_stage,
                        priority: rule.promise(expression, &context).priority,
                        kind: TaskKind::Transform,
                        stable_id: task.rule.0,
                        group: task.group,
                        expression: task.expression,
                        goal: None,
                    },
                    ordinary_task,
                );
            }
            let forced_key = (goal, task);
            let binding = self
                .quality_forced_transform_bindings
                .get_mut(&forced_key)
                .and_then(|bindings| bindings.pop_front())
                .ok_or_else(|| paro_error::internal("quality binding queue disappeared"))?;
            if self
                .quality_forced_transform_bindings
                .get(&forced_key)
                .is_some_and(|bindings| bindings.is_empty())
            {
                self.quality_forced_transform_bindings.remove(&forced_key);
            }
            let continuation = self
                .quality_pending_domain_continuations
                .get_mut(&goal)
                .and_then(|pending| {
                    pending
                        .iter()
                        .position(|candidate| candidate.binding == binding)
                        .map(|index| pending.remove(index))
                });
            if self
                .quality_pending_domain_continuations
                .get(&goal)
                .is_some_and(Vec::is_empty)
            {
                self.quality_pending_domain_continuations.remove(&goal);
            }
            self.quality_active_forced_transform_binding = Some((task, binding));
            self.quality_active_forced_transform_goal = Some(goal);
            if continuation.is_some() {
                self.quality_domain_continuation_dispatch_count = self
                    .quality_domain_continuation_dispatch_count
                    .saturating_add(1);
                let elapsed = self
                    .profile_elapsed_us()
                    .unwrap_or_else(|| self.memo.control().elapsed_us());
                self.quality_domain_continuation_first_us
                    .get_or_insert(elapsed);
                self.quality_domain_continuation_last_us = Some(elapsed);
            }
            self.quality_active_domain_continuation = continuation;
            self.quality_producer_dispatch_count += 1;
            self.quality_direct_binding_dispatch_count += 1;
            let elapsed = self
                .profile_elapsed_us()
                .unwrap_or_else(|| self.memo.control().elapsed_us());
            self.quality_direct_binding_first_us.get_or_insert(elapsed);
            self.quality_direct_binding_last_us = Some(elapsed);
            return Ok(Some(ordinary_task));
        }
        let mut obligations = obligations.into_iter().collect::<Vec<_>>();
        if let Some(previous) = self.quality_last_production_obligation {
            let start = obligations.partition_point(|(obligation, _)| *obligation <= previous);
            obligations.rotate_left(start);
        }
        for (obligation, choices) in obligations {
            let key = choices
                .into_iter()
                .filter_map(|choice| {
                    agenda.matching_choice_key(choice, |id| {
                        self.registry
                            .transformation(id)
                            .is_some_and(|rule| produces(obligation.0, rule.quality_dependency()))
                    })
                })
                .min();
            if let Some(key) = key {
                let task = agenda
                    .tasks
                    .remove(&key)
                    .expect("indexed producer disappeared");
                agenda.keys.remove(&task);
                self.quality_last_production_obligation = Some(obligation);
                self.quality_producer_dispatch_count += 1;
                return Ok(Some(task));
            }
        }
        if self.diagnostic_obligation_only {
            // Do not treat the broad quality bootstrap lane as an obligation:
            // only an exact forced binding or a producer indexed by a current
            // missing choice may run here. Once those directed tasks are
            // exhausted, leave the ordinary agenda untouched and return an
            // explicitly incomplete diagnostic stop. This is the experiment
            // that distinguishes a real obligation dependency from the old
            // global quality-priority queue.
            if !self.diagnostic_obligation_lane_exhausted {
                self.diagnostic_obligation_lane_exhausted = true;
                self.diagnostic_obligation_deferred_task_count = agenda.len() as u64;
                for rule in agenda.pending_transform_rules() {
                    let profile = self.rule_work_profile.entry(rule).or_default();
                    profile.deferred = profile.deferred.saturating_add(1);
                }
            }
            return Ok(None);
        }
        Ok(agenda.pop())
    }
}
