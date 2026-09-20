// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exercise lazy scheduling and admission using actual frozen engine winners.
use super::*;
use crate::physical::PhysicalPlanPortfolio;
use std::sync::Mutex;

fn classes() -> [ResourceGrantClass; 3] {
    [0, 1, 2].map(|id| ResourceGrantClass {
        id: ResourceGrantClassId(id),
        hard_memory_bytes: (1 << 20) << id,
        max_parallel_tasks: 1,
        spill_policy: SpillPolicy::Forbidden,
    })
}

struct ObservedLeaf {
    calls: Arc<Mutex<Vec<(Fingerprint, GrantGoalKey)>>>,
    invariant: bool,
    rewindable: bool,
}

impl PhysicalImplementation for ObservedLeaf {
    fn id(&self) -> ImplementationId {
        LeafImplementation.id()
    }

    fn grant_dependency_for(
        &self,
        _: &crate::cascades::memo::LogicalExpr,
        _: &ImplementationContext<'_>,
    ) -> GrantDependencyDescriptor {
        if self.invariant {
            GrantDependencyDescriptor::Invariant
        } else {
            GrantDependencyDescriptor::Sensitive
        }
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
        let op = ctx.memo.logical_expr(expr).unwrap().key.operator;
        self.calls.lock().unwrap().push((op, goal.grant));
        let mut candidates = LeafImplementation.candidates(expr, goal, ctx)?;
        for candidate in &mut candidates {
            if !self.invariant {
                let GrantGoalKey::Class(id) = goal.grant else {
                    panic!("lost class")
                };
                let memory = classes()
                    .into_iter()
                    .find(|class| class.id == id)
                    .unwrap()
                    .hard_memory_bytes;
                candidate.local_cost.peak_memory_upper = memory;
                candidate.local_cost.minimum_memory_bytes = memory;
                candidate.enforcer_cost_input.hard_memory_bytes = memory;
                candidate.physical_fingerprint = Fingerprint(op.0 * 10 + u128::from(id.0));
                candidate.key.payload_fingerprint = candidate.physical_fingerprint;
            }
            if self.rewindable {
                candidate.provided.replayability = ProvidedReplayability::Rewindable;
            }
        }
        Ok(candidates)
    }
}

type ObservedGrantFixture = (
    CascadesEngine,
    GroupId,
    OptimizationGoal,
    Arc<Mutex<Vec<(Fingerprint, GrantGoalKey)>>>,
);

fn fixture(invariant: bool, rewindable: bool) -> ObservedGrantFixture {
    let (mut engine, root, mut goal) = engine_with_budget(Default::default());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ImplementationRegistry::default();
    registry.register_transformation(AddEquivalent).unwrap();
    registry
        .register_implementation(ObservedLeaf {
            calls: calls.clone(),
            invariant,
            rewindable,
        })
        .unwrap();
    engine.registry = registry;
    if rewindable {
        let mut required = required();
        required.replayability = ReplayabilityRequirement::Rewindable;
        goal.required = engine.memo_mut().intern_required(required).unwrap();
    }
    (engine, root, goal, calls)
}

#[test]
fn lazy_grants_keep_all_exact_safe_dags_and_search_only_expected() {
    let (mut engine, root, goal, calls) = fixture(false, false);
    let output = engine
        .optimize_for_expected_grant(
            root,
            goal,
            AdmissibleGrantSetId(9),
            classes(),
            SearchMode::Memo,
            Some(ResourceGrantClassId(2)),
        )
        .unwrap();
    assert_eq!(output.winners.len(), 3);
    assert_eq!(output.stop.reason, SearchStopReason::SearchIncomplete);
    assert!(!output.stop.budget_limited);
    let deferred: BTreeSet<_> = engine
        .memo
        .search_obligations()
        .iter()
        .filter_map(|obligation| match obligation.reason {
            crate::cascades::budget::SearchIncompleteReason::OptionalGrantDeferred(class) => {
                Some(class)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        deferred,
        BTreeSet::from([ResourceGrantClassId(0), ResourceGrantClassId(1)])
    );
    assert!(!engine.memo.search_obligations_empty());
    assert_eq!(output.safe_winners.len(), 3);
    assert_eq!(
        output.grant_search.as_ref().unwrap().mandatory_only_classes,
        BTreeSet::from([ResourceGrantClassId(0), ResourceGrantClassId(1)])
    );
    assert_eq!(
        output.grant_search.as_ref().unwrap().optional_classes,
        BTreeSet::from([ResourceGrantClassId(2)])
    );
    for (winner, safe) in output.winners.iter().zip(output.safe_winners.iter()) {
        assert_eq!(safe.goal.grant, GrantGoalKey::Class(safe.class));
        assert_eq!(
            safe.winner.physical_fingerprint,
            Fingerprint(100 + u128::from(safe.class.0))
        );
        assert_eq!(
            safe.frozen.winner.cost.memory_completion,
            MemoryCompletion::Guaranteed
        );
        if winner.class != ResourceGrantClassId(2) {
            assert!(
                Arc::ptr_eq(&winner.frozen, &safe.frozen),
                "fallback must be the original FrozenCandidate"
            );
        } else {
            assert_eq!(winner.winner.physical_fingerprint, Fingerprint(112));
        }
    }
    let optional_goals: BTreeSet<_> = calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(op, _)| *op == Fingerprint(11))
        .map(|(_, goal)| *goal)
        .collect();
    assert_eq!(
        optional_goals,
        BTreeSet::from([GrantGoalKey::Class(ResourceGrantClassId(2))])
    );
    assert!(engine.active_optional_grant.is_none());
    assert_eq!(engine.grant_classes.len(), 3);

    let portfolio = PhysicalPlanPortfolio::build(
        ObjectiveProfile::Latency,
        classes(),
        output
            .winners
            .iter()
            .chain(output.safe_winners.iter())
            .map(|winner| {
                (
                    winner.class,
                    winner.frozen.clone(),
                    winner.winner.physical_fingerprint,
                    winner.winner.cost,
                )
            }),
    )
    .unwrap()
    .with_grant_search(output.grant_search)
    .unwrap();
    let admitted = portfolio.admit(1 << 20, 1, 0, |_| true).unwrap();
    assert_eq!(admitted.resources.class, ResourceGrantClassId(0));
    assert!(Arc::ptr_eq(&admitted.plan, &output.safe_winners[0].frozen));
    assert_eq!(
        admitted.resources.memory_completion,
        MemoryCompletion::Guaranteed
    );
    assert!(portfolio.admit((1 << 20) - 1, 1, 0, |_| true).is_err());
    // Dynamic rejection cannot substitute an unverified/retagged active plan.
    assert!(portfolio.admit(1 << 20, 1, 0, |_| false).is_err());
}

#[test]
fn lazy_grants_preserve_invariant_goal_sharing() {
    let (mut engine, root, goal, calls) = fixture(true, false);
    let output = engine
        .optimize_for_expected_grant(
            root,
            goal,
            AdmissibleGrantSetId(9),
            classes(),
            SearchMode::Memo,
            Some(ResourceGrantClassId(2)),
        )
        .unwrap();
    assert!(matches!(
        output.sensitivity,
        GrantSensitivitySummary::Shared(_)
    ));
    assert_eq!(output.safe_winners.len(), 3);
    let goals: BTreeSet<_> = calls
        .lock()
        .unwrap()
        .iter()
        .map(|(_, goal)| *goal)
        .collect();
    assert_eq!(
        goals,
        BTreeSet::from([GrantGoalKey::Invariant(AdmissibleGrantSetId(9))])
    );
    assert!(output
        .safe_winners
        .windows(2)
        .all(|pair| pair[0].winner.candidate == pair[1].winner.candidate));
}

#[test]
fn lazy_grants_preserve_required_enforcement_class_derivation() {
    let (mut engine, root, goal, calls) = fixture(true, true);
    let output = engine
        .optimize_for_expected_grant(
            root,
            goal,
            AdmissibleGrantSetId(9),
            classes(),
            SearchMode::Memo,
            Some(ResourceGrantClassId(2)),
        )
        .unwrap();
    assert!(matches!(
        output.sensitivity,
        GrantSensitivitySummary::RequiredEnforcement { .. }
    ));
    assert_eq!(output.safe_winners.len(), 3);
    for safe in &output.safe_winners {
        assert_eq!(safe.goal.grant, GrantGoalKey::Class(safe.class));
        assert_eq!(safe.goal.required, goal.required);
    }
    let optional: BTreeSet<_> = calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(op, _)| *op == Fingerprint(11))
        .map(|(_, goal)| *goal)
        .collect();
    assert_eq!(
        optional,
        BTreeSet::from([GrantGoalKey::Class(ResourceGrantClassId(2))])
    );
}

#[test]
fn lazy_grants_no_available_class_is_explicit_mandatory_only() {
    for mode in [SearchMode::Direct, SearchMode::Memo] {
        let (mut engine, root, goal, calls) = fixture(false, false);
        let output = engine
            .optimize_for_expected_grant(root, goal, AdmissibleGrantSetId(9), classes(), mode, None)
            .unwrap();
        assert_eq!(output.safe_winners.len(), 3);
        assert_eq!(output.stop.reason, SearchStopReason::SearchIncomplete);
        assert!(!output.stop.budget_limited);
        assert!(!engine.memo.search_obligations_empty());
        assert_eq!(engine.memo.search_obligations().len(), 3);
        assert!(engine
            .memo
            .search_obligations()
            .iter()
            .all(|obligation| matches!(
                obligation.reason,
                crate::cascades::budget::SearchIncompleteReason::OptionalGrantDeferred(_)
            )));
        let coverage = output.grant_search.unwrap();
        assert_eq!(coverage.expected_class, None);
        assert!(coverage.optional_classes.is_empty());
        assert_eq!(
            coverage.mandatory_only_classes,
            classes().map(|class| class.id).into_iter().collect()
        );
        assert!(!calls
            .lock()
            .unwrap()
            .iter()
            .any(|(op, _)| *op == Fingerprint(11)));
        assert!(output
            .winners
            .iter()
            .zip(output.safe_winners.iter())
            .all(|(winner, safe)| Arc::ptr_eq(&winner.frozen, &safe.frozen)));
    }
}

#[test]
fn lazy_grants_pre_optional_stop_does_not_claim_an_optional_search() {
    let (mut engine, root, goal, _) = fixture(false, false);
    engine.memo.control().expire();
    let output = engine
        .optimize_for_expected_grant(
            root,
            goal,
            AdmissibleGrantSetId(9),
            classes(),
            SearchMode::Memo,
            Some(ResourceGrantClassId(2)),
        )
        .unwrap();
    assert_eq!(output.safe_winners.len(), 3);
    let coverage = output.grant_search.unwrap();
    assert_eq!(coverage.expected_class, Some(ResourceGrantClassId(2)));
    assert!(coverage.optional_classes.is_empty());
    assert_eq!(coverage.mandatory_only_classes.len(), 3);
    assert!(engine.active_optional_grant.is_none());
}

#[test]
fn partial_grants_publish_only_verified_images_and_do_not_invent_fallbacks() {
    // Missing the expected class is not the same as missing every class.
    // Cover both paths and both search modes through the real engine.
    for missing in [0, 2] {
        for mode in [SearchMode::Direct, SearchMode::Memo] {
            let (mut engine, root, goal, _) = fixture(false, false);
            let mut grants = classes();
            grants[missing].hard_memory_bytes = 1;
            let output = engine
                .optimize_for_expected_grant(
                    root,
                    goal,
                    AdmissibleGrantSetId(9),
                    grants,
                    mode,
                    Some(ResourceGrantClassId(2)),
                )
                .unwrap();
            let missing = ResourceGrantClassId(missing as u32);
            assert_eq!(output.winners.len(), 2);
            assert_eq!(output.safe_winners.len(), 2);
            assert!(output.winners.iter().all(|winner| winner.class != missing));
            assert_eq!(
                output.grant_search.as_ref().unwrap().unresolved_classes,
                BTreeSet::from([missing])
            );
            let portfolio = PhysicalPlanPortfolio::build(
                ObjectiveProfile::Latency,
                grants,
                output.winners.iter().map(|winner| {
                    (
                        winner.class,
                        winner.frozen.clone(),
                        winner.winner.physical_fingerprint,
                        winner.winner.cost,
                    )
                }),
            )
            .unwrap()
            .with_grant_search(output.grant_search)
            .unwrap();
            let admitted = portfolio.admit(4 << 20, 1, 0, |_| true).unwrap();
            assert_ne!(admitted.resources.class, missing);
            assert!(output
                .winners
                .iter()
                .any(|winner| Arc::ptr_eq(&winner.frozen, &admitted.plan)));
            // Shrinking actual resources cannot admit the absent one-byte class.
            assert!(portfolio
                .admit(1, 1, 0, |_| true)
                .unwrap_err()
                .sqlstate()
                .is_resource_error());
            assert!(engine.active_optional_grant.is_none());
        }
    }
}

#[test]
fn no_verified_grant_is_a_resource_error_not_a_false_infeasibility_proof() {
    for mode in [SearchMode::Direct, SearchMode::Memo] {
        let (mut engine, root, goal, _) = fixture(false, false);
        let grants = classes().map(|mut class| {
            class.hard_memory_bytes = 1;
            class
        });
        let error = engine
            .optimize_for_expected_grant(
                root,
                goal,
                AdmissibleGrantSetId(9),
                grants,
                mode,
                Some(ResourceGrantClassId(2)),
            )
            .unwrap_err();
        assert!(error.sqlstate().is_resource_error(), "{error}");
        assert!(error.to_string().contains("infeasibility is not proven"));
        assert!(engine.active_optional_grant.is_none());
    }
}

struct FailingGrantLeaf {
    optional: bool,
    error: paro_error::ParoError,
}

impl PhysicalImplementation for FailingGrantLeaf {
    fn id(&self) -> ImplementationId {
        LeafImplementation.id()
    }

    fn grant_dependency_for(
        &self,
        _: &crate::cascades::memo::LogicalExpr,
        _: &ImplementationContext<'_>,
    ) -> GrantDependencyDescriptor {
        GrantDependencyDescriptor::Sensitive
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
        let op = ctx.memo.logical_expr(expr).unwrap().key.operator;
        // Class zero already has an executable winner when mandatory fails.
        if (self.optional && op == Fingerprint(11))
            || (!self.optional && goal.grant == GrantGoalKey::Class(ResourceGrantClassId(1)))
        {
            return Err(self.error.clone());
        }
        LeafImplementation.candidates(expr, goal, ctx)
    }
}

#[test]
fn grant_failures_are_not_swallowed_as_missing_candidates_or_safe_fallbacks() {
    for optional in [false, true] {
        for error in [
            paro_error::internal("injected broken grant contract"),
            paro_error::configuration_limit_exceeded("injected producer resource failure"),
        ] {
            let (mut engine, root, goal, _) = fixture(false, false);
            let mut registry = ImplementationRegistry::default();
            registry.register_transformation(AddEquivalent).unwrap();
            registry
                .register_implementation(FailingGrantLeaf {
                    optional,
                    error: error.clone(),
                })
                .unwrap();
            engine.registry = registry;
            let observed = engine
                .optimize_for_expected_grant(
                    root,
                    goal,
                    AdmissibleGrantSetId(9),
                    classes(),
                    SearchMode::Memo,
                    Some(ResourceGrantClassId(2)),
                )
                .unwrap_err();
            assert_eq!(observed.sqlstate(), error.sqlstate());
            assert_eq!(observed.to_string(), error.to_string());
            assert!(engine.active_optional_grant.is_none());
            assert!(!engine.mandatory_only);
        }
    }
}

#[test]
fn lazy_grants_cancel_and_deadline_roll_back_and_clear_temporary_goals() {
    for cancel in [false, true] {
        let (mut engine, root, goal, calls) = fixture(false, false);
        engine.registry = ImplementationRegistry::default();
        engine
            .registry
            .register_implementation(ObservedLeaf {
                calls,
                invariant: false,
                rewindable: false,
            })
            .unwrap();
        engine
            .registry
            .register_transformation(StopAfterMemoWrite { cancel })
            .unwrap();
        let result = engine.optimize_for_expected_grant(
            root,
            goal,
            AdmissibleGrantSetId(9),
            classes(),
            SearchMode::Memo,
            Some(ResourceGrantClassId(2)),
        );
        assert!(engine.active_optional_grant.is_none());
        assert!(!engine.mandatory_only);
        assert_eq!(engine.grant_classes.len(), 3);
        assert_eq!(engine.memo.group_count(), 1, "staged group must roll back");
        if cancel {
            assert!(result.unwrap_err().is_query_canceled());
            assert!(engine.quality_production_requests.is_empty());
            assert!(engine.quality_active_forced_transform_binding.is_none());
        } else {
            let output = result.unwrap();
            assert_eq!(output.winners.len(), 3);
            assert!(output
                .winners
                .iter()
                .zip(output.safe_winners.iter())
                .all(|(winner, safe)| Arc::ptr_eq(&winner.frozen, &safe.frozen)));
        }
    }
}
