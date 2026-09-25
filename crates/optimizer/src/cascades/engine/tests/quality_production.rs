// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Candidate-owned ordering only: no policy, rule, or budget changes.
//! Include with #[path = "tests/quality_production.rs"] mod quality_production;

use super::*;
use crate::cascades::quality::{AggregateRegionWitness, NativeQualityEvidence, NativeQualityShape};
use crate::cascades::tasks::ReadSetId;

struct DagImplementation;

impl PhysicalImplementation for DagImplementation {
    fn id(&self) -> ImplementationId {
        ImplementationId(901)
    }
    fn matches(
        &self,
        _: &crate::cascades::memo::LogicalExpr,
        _: OptimizationGoal,
        _: &ImplementationContext<'_>,
    ) -> bool {
        true
    }
    fn candidates(
        &self,
        expr: LogicalExprId,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>> {
        let mut candidates = FixedLeafImplementation {
            id: self.id(),
            score: 1.0,
            mandatory: true,
        }
        .candidates(expr, goal, ctx)?;
        let children = ctx.memo.logical_expr(expr).unwrap().key.children.clone();
        candidates[0].child_goals = children.iter().map(|child| (*child, goal)).collect();
        candidates[0].key.children = children;
        Ok(candidates)
    }
}

struct NarrowRule(RuleId);

struct DomainRule;

struct CountSelectedBindings(Arc<std::sync::atomic::AtomicUsize>);
impl TransformationRule for CountSelectedBindings {
    fn id(&self) -> RuleId {
        crate::cascades::rules::PREDICATE_TRANSFER_RULE
    }
    fn matches_root(&self, _: &crate::cascades::memo::LogicalExpr) -> bool {
        false
    }
    fn matches(&self, _: &crate::cascades::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        false
    }
    fn apply(
        &self,
        _: LogicalExprId,
        _: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        Ok(Box::new([]))
    }
    fn selected_quality_bindings(
        &self,
        _: &Memo,
        _: &FrozenCandidate,
    ) -> Result<Box<[PatternBinding]>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Box::new([]))
    }
}

#[test]
fn quality_request_rejects_before_binding_construction_but_reopens_stale_reads() {
    use crate::cascades::quality::BundleFact::{CteConsumerDemand, JoinRegion, PredicateDomain};
    use std::sync::atomic::{AtomicUsize, Ordering};
    for reverse in [false, true] {
        let mut f = Fixture::new(reverse, [101, 102, 103]);
        let calls = Arc::new(AtomicUsize::new(0));
        f.engine
            .registry
            .register_transformation(CountSelectedBindings(calls.clone()))
            .unwrap();
        let evidence = f.evidence([true, true]);
        // Independent ranking oracle: smaller number of missing facts wins;
        // equal cost/identity leaves the existing request and its payload intact.
        for (missing, expected) in [
            (vec![PredicateDomain, JoinRegion], 1),
            (vec![PredicateDomain, JoinRegion], 1),
            (vec![PredicateDomain, JoinRegion, CteConsumerDemand], 1),
            (vec![PredicateDomain], 2),
        ] {
            f.engine
                .record_quality_production_request(f.goal, &f.frozen, f.reads, &evidence, &missing)
                .unwrap();
            assert_eq!(calls.load(Ordering::Relaxed), expected);
        }
        // Only a child fact changes. Ranking cannot authorize retention of
        // stale work; a worse request must still rebuild in the new context.
        f.engine
            .memo
            .group_mut(f.regions[0])
            .unwrap()
            .logical_properties
            .maximum_cardinality = Some(1);
        let reads = f
            .engine
            .winner_fact_reads(f.frozen.reference.group, &f.frozen.winner)
            .unwrap();
        let reads = f.engine.task_registry.intern_read_set(reads);
        assert_ne!(reads, f.reads);
        f.engine
            .record_quality_production_request(
                f.goal,
                &f.frozen,
                reads,
                &evidence,
                &[PredicateDomain, JoinRegion],
            )
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }
}

impl TransformationRule for DomainRule {
    fn id(&self) -> RuleId {
        RuleId(201)
    }
    fn quality_dependency(&self) -> Option<QualityDependency> {
        Some(QualityDependency::DomainRestriction)
    }
    fn matches_root(&self, expr: &crate::cascades::memo::LogicalExpr) -> bool {
        QualityLaneRule.matches_root(expr)
    }
    fn matches(&self, expr: &crate::cascades::memo::LogicalExpr, ctx: &RuleContext<'_>) -> bool {
        QualityLaneRule.matches(expr, ctx)
    }
    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        QualityLaneRule.apply(expr, ctx)
    }
}

impl TransformationRule for NarrowRule {
    fn id(&self) -> RuleId {
        self.0
    }
    fn quality_dependency(&self) -> Option<QualityDependency> {
        Some(QualityDependency::NarrowAggregate)
    }
    fn matches_root(&self, expr: &crate::cascades::memo::LogicalExpr) -> bool {
        QualityLaneRule.matches_root(expr)
    }
    fn matches(&self, expr: &crate::cascades::memo::LogicalExpr, ctx: &RuleContext<'_>) -> bool {
        QualityLaneRule.matches(expr, ctx)
    }
    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        QualityLaneRule.apply(expr, ctx)
    }
}

struct Fixture {
    engine: CascadesEngine,
    goal: OptimizationGoal,
    regions: [GroupId; 2],
    producers: [Vec<GroupId>; 2],
    unrelated: GroupId,
    frozen: Arc<FrozenCandidate>,
    reads: ReadSetId,
}

impl Fixture {
    fn new(reverse_groups: bool, rule_ids: [u32; 3]) -> Self {
        Self::build(reverse_groups, rule_ids, false)
    }

    fn build(reverse_groups: bool, rule_ids: [u32; 3], wrapped: bool) -> Self {
        let mut memo = Memo::new(crate::cascades::budget::SearchBudget::default());
        let mut add = |operator: u128, children: Box<[GroupId]>| {
            let group = memo.create_group(
                schema(),
                LogicalProperties::default(),
                GroupCardinality::default(),
            );
            memo.insert_logical(
                group,
                LogicalExprKey {
                    operator: Fingerprint(operator),
                    scalars: Box::new([]),
                    children,
                },
                LogicalPayloadId(group.0),
                EquivalenceProof::Initial,
            )
            .unwrap();
            group
        };
        let a = add(10, Box::new([]));
        // Two producer groups in arm A deliberately precede B's producer IDs.
        // Flattening all descendants into a global round-robin serves A twice.
        let a_parent = if wrapped { add(10, Box::new([a])) } else { a };
        let b = add(10, Box::new([]));
        let mut producers = [if wrapped { vec![a, a_parent] } else { vec![a] }, vec![b]];
        // Toy tags: 10 is NarrowAggregate-compatible; 20 is a Projection
        // wrapper; 30 is the UNION root. Only the producer tasks are queued.
        let mut regions = if wrapped {
            [add(20, Box::new([a_parent])), add(20, Box::new([b]))]
        } else {
            [a, b]
        };
        if reverse_groups {
            regions.reverse();
            producers.reverse();
        }
        let unrelated = add(10, Box::new([]));
        let root = add(if wrapped { 30 } else { 10 }, Box::new(regions));
        let goal = OptimizationGoal {
            required: memo.intern_required(required()).unwrap(),
            row_goal: RowGoal::All,
            objective: ObjectiveProfile::Latency,
            grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
            context: OptimizationContextId(0),
        };
        let mut registry = ImplementationRegistry::default();
        registry.register_implementation(DagImplementation).unwrap();
        registry.register_transformation(AddEquivalent).unwrap();
        for id in rule_ids {
            registry
                .register_transformation(NarrowRule(RuleId(id)))
                .unwrap();
        }
        let mut engine = CascadesEngine::new(memo, registry);
        engine.optimize_group(root, goal).unwrap();
        engine.optimize_group(unrelated, goal).unwrap();
        let winner = engine
            .memo()
            .group(root)
            .unwrap()
            .winner(goal)
            .unwrap()
            .clone();
        let frozen = engine
            .memo()
            .freeze_candidate_tree(ChildWinnerRef {
                group: root,
                goal,
                candidate: winner.candidate,
            })
            .unwrap();
        assert_eq!(frozen.children.len(), 2);
        let reads = engine.winner_fact_reads(root, &winner).unwrap();
        let reads = engine.task_registry.intern_read_set(reads);
        engine.set_quality_policy_handoff_enabled(true);
        Self {
            engine,
            goal,
            regions,
            producers,
            unrelated,
            frozen,
            reads,
        }
    }

    fn evidence(&self, covered: [bool; 2]) -> NativeQualityEvidence {
        NativeQualityEvidence {
            pending_domain_transfers: Box::new([]),
            capabilities: BTreeSet::new(),
            facts: BTreeSet::new(),
            region: Fingerprint(900),
            applicability_proof: Fingerprint(901),
            choices: Box::new([]),
            aggregate_regions: self
                .regions
                .iter()
                .zip(covered)
                .enumerate()
                .map(|(i, (group, covered))| {
                    let node = self
                        .frozen
                        .children
                        .iter()
                        .find(|node| node.reference.group == *group)
                        .unwrap();
                    AggregateRegionWitness {
                        region: Fingerprint(910 + i as u128),
                        candidate: self.frozen.reference.candidate,
                        anchor: node.reference.candidate,
                        fact_fingerprint: Fingerprint(920 + i as u128),
                        anchor_choice: Fingerprint(0),
                        covered,
                    }
                })
                .collect(),
            selected_rules: Box::new([]),
            shape: NativeQualityShape::default(),
        }
    }

    fn record(&mut self, evidence: &NativeQualityEvidence) {
        self.engine
            .record_quality_production_request(
                self.goal,
                &self.frozen,
                self.reads,
                evidence,
                &if evidence
                    .aggregate_regions
                    .iter()
                    .any(|region| !region.covered)
                {
                    vec![crate::cascades::quality::BundleFact::AggregateDecomposition]
                } else {
                    vec![]
                },
            )
            .unwrap();
    }

    fn task(&self, group: GroupId, rule: u32) -> SearchTask {
        SearchTask::Transform {
            group,
            expression: self.engine.memo().group(group).unwrap().logical_exprs()[0],
            rule: RuleId(rule),
        }
    }

    fn agenda(&self, items: &[(GroupId, u32, u16)]) -> StableAgenda {
        let mut agenda = StableAgenda::default();
        for &(group, rule, priority) in items {
            let task = self.task(group, rule);
            let SearchTask::Transform { expression, .. } = task else {
                unreachable!()
            };
            agenda.push(
                TaskKey {
                    demand_stage: 1,
                    quality_stage: u8::MAX,
                    priority,
                    kind: TaskKind::Transform,
                    stable_id: rule,
                    group,
                    expression,
                    goal: None,
                },
                task,
            );
        }
        agenda
    }
}

#[test]
fn quality_production_domain_request_targets_only_the_selected_pending_filter() {
    use crate::cascades::quality::BundleFact;
    for reverse in [false, true] {
        let mut f = Fixture::new(reverse, [101, 102, 103]);
        f.engine
            .registry
            .register_transformation(DomainRule)
            .unwrap();
        let [a, b] = f.regions;
        let node = f
            .frozen
            .children
            .iter()
            .find(|node| node.reference.group == b)
            .unwrap();
        let mut evidence = f.evidence([true, true]);
        evidence.pending_domain_transfers = Box::new([node.reference.candidate]);
        f.engine
            .record_quality_production_request(
                f.goal,
                &f.frozen,
                f.reads,
                &evidence,
                &[BundleFact::PredicateDomain],
            )
            .unwrap();
        let mut agenda = f.agenda(&[(f.unrelated, 201, 0), (a, 201, 1), (b, 201, 2)]);
        assert_eq!(
            f.engine.pop_transformation_task(&mut agenda).unwrap(),
            Some(f.task(b, 201))
        );
        assert_eq!(
            agenda.tasks.len(),
            2,
            "other domain tasks remain in the ordinary closure"
        );
        f.engine.quality_production_requests.clear();
        evidence.pending_domain_transfers = Box::new([CandidateId(u32::MAX)]);
        f.engine
            .record_quality_production_request(
                f.goal,
                &f.frozen,
                f.reads,
                &evidence,
                &[BundleFact::PredicateDomain],
            )
            .unwrap();
        assert!(
            f.engine.quality_production_requests.is_empty(),
            "foreign branch is not this candidate's debt"
        );
    }
}

#[test]
fn quality_production_dispatches_a_group_hole_continuation_with_its_reads() {
    let mut f = Fixture::new(false, [101, 102, 103]);
    f.engine
        .registry
        .register_transformation(CountSelectedBindings(Arc::new(
            std::sync::atomic::AtomicUsize::new(0),
        )))
        .unwrap();
    let group = f.regions[0];
    let expression = f.engine.memo().group(group).unwrap().logical_exprs()[0];
    let binding = PatternBinding {
        root: PatternOperand::Expression {
            group,
            expression,
            children: Box::new([]),
        },
        fingerprint: Fingerprint(991),
    };
    let read = PatternRead::from_group(f.engine.memo(), group).unwrap();
    let evidence = f.evidence([true, false]);
    f.record(&evidence);
    f.engine
        .enqueue_quality_domain_continuations(
            f.goal,
            vec![DomainContinuation {
                binding: binding.clone(),
                hole: group,
                predicates: Box::new([]),
                reads: Box::new([read]),
                occurrence: expression,
                context: OptimizationContextId(0),
            }],
        )
        .unwrap();
    f.engine
        .enqueue_quality_domain_continuations(
            f.goal,
            vec![DomainContinuation {
                binding: binding.clone(),
                hole: group,
                predicates: Box::new([]),
                reads: Box::new([read]),
                occurrence: expression,
                context: OptimizationContextId(0),
            }],
        )
        .unwrap();
    assert_eq!(
        f.engine.quality_domain_continuation_enqueued_count, 1,
        "same continuation is published once"
    );
    let mut agenda = f.agenda(&[(group, crate::cascades::rules::PREDICATE_TRANSFER_RULE.0, 0)]);
    assert!(f
        .engine
        .pop_transformation_task(&mut agenda)
        .unwrap()
        .is_some());
    assert_eq!(f.engine.quality_active_forced_transform_goal, Some(f.goal));
    let continuation = f
        .engine
        .quality_active_domain_continuation
        .as_ref()
        .expect("forced dispatch lost continuation metadata");
    assert_eq!(continuation.binding, binding);
    assert_eq!(continuation.hole, group);
    assert_eq!(continuation.reads.as_ref(), &[read]);
    assert_eq!(f.engine.quality_domain_continuation_dispatch_count, 1);
}

#[test]
fn quality_production_aggregate_coverage_does_not_hide_missing_domain_work() {
    use crate::cascades::quality::BundleFact;
    let mut f = Fixture::new(false, [101, 102, 103]);
    f.engine
        .registry
        .register_transformation(DomainRule)
        .unwrap();
    let [a, b] = f.regions;
    // An aggregate-complete root is still missing three independent facts.
    // Its exact selected domain producers must remain requested.
    let evidence = f.evidence([true, true]);
    f.engine
        .record_quality_production_request(
            f.goal,
            &f.frozen,
            f.reads,
            &evidence,
            &[
                BundleFact::PredicateDomain,
                BundleFact::CteConsumerDemand,
                BundleFact::JoinRegion,
            ],
        )
        .unwrap();
    let mut agenda = f.agenda(&[(f.unrelated, 201, 0), (a, 101, 1), (b, 201, 2)]);
    assert_eq!(
        f.engine.pop_transformation_task(&mut agenda).unwrap(),
        Some(f.task(b, 201))
    );
    // A successor with the domains resolved and one missing aggregate region
    // has fewer real obligations. Do not pin production to aggregate-only
    // coverage of the predecessor when that predecessor lacks the domains.
    let evidence = f.evidence([true, false]);
    f.engine
        .record_quality_production_request(
            f.goal,
            &f.frozen,
            f.reads,
            &evidence,
            &[BundleFact::AggregateDecomposition, BundleFact::JoinRegion],
        )
        .unwrap();
    let mut agenda = f.agenda(&[(f.unrelated, 201, 0), (a, 101, 1), (b, 101, 2), (b, 201, 3)]);
    let first = f
        .engine
        .pop_transformation_task(&mut agenda)
        .unwrap()
        .unwrap();
    let second = f
        .engine
        .pop_transformation_task(&mut agenda)
        .unwrap()
        .unwrap();
    assert_eq!(
        BTreeSet::from([first, second]),
        BTreeSet::from([f.task(b, 101), f.task(b, 201)]),
        "both the missing aggregate and join's input-domain producer get progress"
    );
    assert_eq!(
        f.engine.pop_transformation_task(&mut agenda).unwrap(),
        Some(f.task(f.unrelated, 201))
    );
    assert_eq!(
        f.engine.pop_transformation_task(&mut agenda).unwrap(),
        Some(f.task(a, 101))
    );
    assert!(f
        .engine
        .pop_transformation_task(&mut agenda)
        .unwrap()
        .is_none());
}

#[test]
fn quality_production_does_not_promote_other_alternatives_of_the_selected_group() {
    let mut f = Fixture::new(false, [101, 102, 103]);
    let [a, _] = f.regions;
    let selected = f.engine.memo().group(a).unwrap().logical_exprs()[0];
    let alternative = f
        .engine
        .memo_mut()
        .insert_logical(
            a,
            LogicalExprKey {
                operator: Fingerprint(10),
                scalars: Box::new([]),
                children: Box::new([f.unrelated]),
            },
            LogicalPayloadId(999),
            EquivalenceProof::Transformation {
                rule: RuleId(101),
                source: selected,
                premise: Fingerprint(999),
            },
        )
        .unwrap();
    let reads = f
        .engine
        .winner_fact_reads(f.frozen.reference.group, &f.frozen.winner)
        .unwrap();
    f.reads = f.engine.task_registry.intern_read_set(reads);
    f.record(&f.evidence([false, true]));
    let mut agenda = f.agenda(&[(a, 101, 20)]);
    let other = SearchTask::Transform {
        group: a,
        expression: alternative,
        rule: RuleId(101),
    };
    agenda.push(
        TaskKey {
            demand_stage: 0,
            quality_stage: 0,
            priority: 0,
            kind: TaskKind::Transform,
            stable_id: 101,
            group: a,
            expression: alternative,
            goal: None,
        },
        other,
    );
    assert_eq!(
        f.engine.pop_transformation_task(&mut agenda).unwrap(),
        Some(f.task(a, 101)),
        "the witness requests an exact selected producer, not its entire equivalence group"
    );
    assert_eq!(
        f.engine.pop_transformation_task(&mut agenda).unwrap(),
        Some(other),
        "the nonselected alternative remains in the legal agenda"
    );
    assert!(f
        .engine
        .pop_transformation_task(&mut agenda)
        .unwrap()
        .is_none());
}

#[test]
fn quality_production_exact_frozen_root_requests_only_its_uncovered_anchor() {
    let mut f = Fixture::new(false, [101, 102, 103]);
    let [a, b] = f.regions;
    let valid = f.evidence([true, false]);
    let foreign = f
        .engine
        .memo()
        .group(f.unrelated)
        .unwrap()
        .winner(f.goal)
        .unwrap()
        .candidate;
    let items = [(f.unrelated, 101, 0), (a, 101, 1), (b, 101, 2)];

    // Existing candidate in another DAG is not an anchor of this frozen root.
    // Even a covered witness must belong to the exact candidate and DAG.
    for invalid in 0..3 {
        let mut evidence = valid.clone();
        match invalid {
            0 => evidence.aggregate_regions[0].candidate = foreign,
            1 => evidence.aggregate_regions[1].anchor = foreign,
            _ => evidence.aggregate_regions[0].anchor = CandidateId::new(1_000_000),
        }
        f.record(&evidence);
        assert!(f.engine.quality_production_requests.is_empty());
        let mut agenda = f.agenda(&items);
        assert_eq!(
            f.engine.pop_transformation_task(&mut agenda).unwrap(),
            Some(f.task(f.unrelated, 101))
        );
    }

    f.record(&valid);
    let mut agenda = f.agenda(&items);
    assert_eq!(
        f.engine.pop_transformation_task(&mut agenda).unwrap(),
        Some(f.task(b, 101))
    );
    assert_eq!(
        f.engine.pop_transformation_task(&mut agenda).unwrap(),
        Some(f.task(f.unrelated, 101))
    );
    assert_eq!(
        f.engine.pop_transformation_task(&mut agenda).unwrap(),
        Some(f.task(a, 101)),
        "covered region stays runnable but is not preferred"
    );
    assert!(f
        .engine
        .pop_transformation_task(&mut agenda)
        .unwrap()
        .is_none());
    assert_eq!(f.engine.quality_producer_dispatch_count, 1);

    f.record(&f.evidence([true, true]));
    let mut agenda = f.agenda(&items);
    assert_eq!(
        f.engine.pop_transformation_task(&mut agenda).unwrap(),
        Some(f.task(f.unrelated, 101))
    );
}

#[test]
fn quality_production_round_robin_preserves_the_entire_finite_agenda_under_id_permutation() {
    // Independent symbolic domain, not reconstructed from the drained agenda:
    // three distinct A tasks, one B task, one unrelated task, one ordinary A task.
    let expected = BTreeSet::from([(0, 0), (0, 1), (0, 2), (1, 0), (2, 0), (0, 3)]);
    let mut orders = BTreeSet::new();
    for reverse_groups in [false, true] {
        for ids in [[101, 102, 103], [103, 101, 102]] {
            let mut f = Fixture::new(reverse_groups, ids);
            let [a, b] = f.regions;
            f.record(&f.evidence([false, false]));
            let items = [
                (a, ids[0], 10),
                (a, ids[1], 10),
                (a, ids[2], 10),
                (b, ids[0], 20),
                (f.unrelated, ids[0], 0),
                (a, 5, 1),
            ];
            let exact: BTreeSet<_> = items
                .iter()
                .map(|&(group, rule, _)| f.task(group, rule))
                .collect();
            let mut agenda = f.agenda(&items);
            let mut seen = BTreeSet::new();
            let mut order = Vec::new();
            for _ in 0..items.len() {
                let task = f
                    .engine
                    .pop_transformation_task(&mut agenda)
                    .unwrap()
                    .expect("a legal task was lost");
                assert!(seen.insert(task), "one exact task was dispatched twice");
                let SearchTask::Transform { group, rule, .. } = task else {
                    unreachable!()
                };
                let owner = if group == a {
                    0
                } else if group == b {
                    1
                } else {
                    assert_eq!(group, f.unrelated);
                    2
                };
                let semantic_rule = if rule == RuleId(5) {
                    3
                } else {
                    ids.iter().position(|id| *id == rule.0).unwrap()
                };
                order.push((owner, semantic_rule));
            }
            assert_eq!(
                order[..2]
                    .iter()
                    .map(|(owner, _)| *owner)
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([0, 1]),
                "B must run within two requests despite A's larger backlog"
            );
            assert!(
                order[..2].iter().all(|(_, rule)| *rule != 3),
                "ordinary rules are not NarrowAggregate work"
            );
            assert_eq!(seen, exact);
            assert_eq!(order.iter().copied().collect::<BTreeSet<_>>(), expected);
            assert!(f
                .engine
                .pop_transformation_task(&mut agenda)
                .unwrap()
                .is_none());
            assert!(agenda.keys.is_empty() && agenda.tasks.is_empty());
            assert_eq!(f.engine.quality_producer_dispatch_count, 4);
            orders.insert(order);
        }
    }
    assert!(
        orders.len() > 1,
        "the permutations must really exercise different orders"
    );
}

#[test]
fn quality_production_child_fact_change_revokes_old_readset_priority() {
    let mut f = Fixture::new(false, [101, 102, 103]);
    let [a, b] = f.regions;
    f.record(&f.evidence([true, false]));
    assert!(f
        .engine
        .task_registry
        .read_set(f.reads)
        .unwrap()
        .is_current(f.engine.memo())
        .unwrap());
    let root_read =
        PatternRead::facts_from_group(f.engine.memo(), f.frozen.reference.group).unwrap();
    // Change only a child's facts, not the root, expression IDs, or frozen DAG.
    f.engine
        .memo_mut()
        .group_mut(b)
        .unwrap()
        .logical_properties
        .maximum_cardinality = Some(1);
    assert_eq!(
        PatternRead::facts_from_group(f.engine.memo(), f.frozen.reference.group).unwrap(),
        root_read
    );
    assert!(!f
        .engine
        .task_registry
        .read_set(f.reads)
        .unwrap()
        .is_current(f.engine.memo())
        .unwrap());
    let mut agenda = f.agenda(&[(f.unrelated, 101, 0), (a, 5, 1), (b, 101, 2)]);
    assert_eq!(
        f.engine.pop_transformation_task(&mut agenda).unwrap(),
        Some(f.task(f.unrelated, 101))
    );
    assert!(f.engine.quality_production_requests.is_empty());
    assert_eq!(
        f.engine.pop_transformation_task(&mut agenda).unwrap(),
        Some(f.task(a, 5))
    );
    assert_eq!(
        f.engine.pop_transformation_task(&mut agenda).unwrap(),
        Some(f.task(b, 101))
    );
    assert!(f
        .engine
        .pop_transformation_task(&mut agenda)
        .unwrap()
        .is_none());
    assert_eq!(f.engine.quality_producer_dispatch_count, 0);
}

#[test]
fn quality_production_wrapped_arms_rotate_by_region_and_refresh_same_candidate_coverage() {
    let mut f = Fixture::build(false, [101, 102, 103], true);
    let a = f.producers[0].clone();
    let b = f.producers[1][0];
    assert_eq!(a.len(), 2);
    for arm in f.frozen.children.iter() {
        assert_eq!(arm.logical.key.operator, Fingerprint(20));
        assert_ne!(arm.reference.group, arm.children[0].reference.group);
    }
    let missing = f.evidence([false, false]);
    for witness in missing.aggregate_regions.iter() {
        assert!(f
            .frozen
            .children
            .iter()
            .any(|arm| arm.reference.candidate == witness.anchor));
    }
    f.record(&missing);
    let candidate = f.frozen.reference.candidate;
    let read_id = f.reads;
    let reads = f.engine.task_registry.read_set(read_id).unwrap().clone();
    let items = [
        (f.unrelated, 101, 0),
        (a[0], 5, 1),
        (a[0], 101, 10),
        (a[1], 102, 11),
        (b, 101, 20),
    ];
    // There is no task owned by either Projection anchor. The v1 anchor.group
    // lookup falls back to the unrelated task and performs zero region work.
    assert!(items.iter().all(|(group, _, _)| !f.regions.contains(group)));
    let exact: BTreeSet<_> = items
        .iter()
        .map(|&(group, rule, _)| f.task(group, rule))
        .collect();
    let mut agenda = f.agenda(&items);
    let mut seen = BTreeSet::new();
    let mut served_regions = BTreeSet::new();
    for _ in 0..2 {
        let task = f
            .engine
            .pop_transformation_task(&mut agenda)
            .unwrap()
            .unwrap();
        assert!(seen.insert(task));
        let SearchTask::Transform { group, rule, .. } = task else {
            unreachable!()
        };
        assert_ne!(rule, RuleId(5), "ordinary work is not a missing producer");
        let region = if a.contains(&group) {
            0
        } else {
            assert_eq!(group, b, "dispatch must descend within one frozen arm");
            1
        };
        assert!(
            served_regions.insert(region),
            "A's two descendants must not delay B"
        );
    }
    assert_eq!(served_regions, BTreeSet::from([0, 1]));
    assert_eq!(f.engine.quality_producer_dispatch_count, 2);

    // Same frozen candidate and exact ReadSet, but newer evidence covers A.
    // Its second producer is still queued; it must immediately lose preference.
    let covered_a = f.evidence([true, false]);
    assert_eq!(covered_a.aggregate_regions[0].candidate, candidate);
    assert_eq!(f.reads, read_id);
    f.record(&covered_a);
    assert_eq!(f.engine.task_registry.read_set(read_id).unwrap(), &reads);
    assert!(reads.is_current(f.engine.memo()).unwrap());
    let next = f
        .engine
        .pop_transformation_task(&mut agenda)
        .unwrap()
        .unwrap();
    assert_eq!(
        next,
        f.task(f.unrelated, 101),
        "updated coverage must replace the request even for the same candidate/read identity"
    );
    assert!(seen.insert(next));
    assert_eq!(f.engine.quality_producer_dispatch_count, 2);

    // Covered work is demoted, never deleted. Finish only the finite agenda;
    // neither a new optimize pass nor refreshed facts may repair a lost task.
    for _ in seen.len()..items.len() {
        let task = f
            .engine
            .pop_transformation_task(&mut agenda)
            .unwrap()
            .unwrap();
        assert!(seen.insert(task));
    }
    assert_eq!(seen, exact);
    assert!(f
        .engine
        .pop_transformation_task(&mut agenda)
        .unwrap()
        .is_none());
    assert!(agenda.keys.is_empty() && agenda.tasks.is_empty());
    assert_eq!(f.engine.quality_producer_dispatch_count, 2);
}
