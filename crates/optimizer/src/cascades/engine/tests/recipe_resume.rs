// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Include from engine/tests.rs with #[path = "tests/recipe_resume.rs"] mod recipe_resume;
//! These tests use only targeted optimize_group calls, never a final optimize()
//! or cost-epoch reset that could rediscover a lost continuation.

use super::*;

#[test]
fn unchanged_parent_recipe_consumes_a_new_optional_leaf() {
    let (mut engine, root, goal) = strong_tree_engine();
    let child = engine
        .memo
        .logical_expr(engine.memo.group(root).unwrap().logical_exprs()[0])
        .unwrap()
        .key
        .children[0];
    engine.registry = ImplementationRegistry::default();
    for implementation in [
        TreeImplementation {
            id: ImplementationId(40),
            operator: Fingerprint(100),
            child: None,
            child_row_goal: None,
            local_score: 100.0,
            mandatory: true,
        },
        TreeImplementation {
            id: ImplementationId(41),
            operator: Fingerprint(200),
            child: Some(child),
            child_row_goal: None,
            local_score: 0.0,
            mandatory: true,
        },
        TreeImplementation {
            id: ImplementationId(42),
            operator: Fingerprint(100),
            child: None,
            child_row_goal: None,
            local_score: 1.0,
            mandatory: false,
        },
    ] {
        engine
            .registry
            .register_implementation(implementation)
            .unwrap();
    }
    engine.mandatory_only = true;
    engine.optimize_group(root, goal).unwrap();
    let baseline = engine
        .memo
        .group(root)
        .unwrap()
        .winner(goal)
        .unwrap()
        .clone();
    let before = engine.child_combination_cost_synthesis_count;
    assert_eq!(baseline.cost.score.range.expected, 100.0);
    engine.mandatory_only = false;
    engine.open_optional_implementation_domain().unwrap();
    engine.optimize_group(root, goal).unwrap();
    let selected = engine.memo.group(root).unwrap().winner(goal).unwrap();
    assert_eq!(selected.cost.score.range.expected, 1.0);
    assert_eq!(engine.memo.group(root).unwrap().physical_exprs().len(), 1);
    assert_eq!(
        engine.child_combination_cost_synthesis_count - before,
        2,
        "price only the new leaf and its parent combination, not the baseline again"
    );
    assert_eq!(
        engine
            .memo
            .resolve_child_winner(ChildWinnerRef {
                group: root,
                goal,
                candidate: baseline.candidate
            })
            .unwrap()
            .cost,
        baseline.cost
    );
}

#[test]
fn opening_optional_domain_keeps_exact_prices_but_reopens_coverage() {
    let (mut engine, root, child, goal) = resume_engine(false);
    engine.mandatory_only = true;
    engine.optimize_group(root, goal).unwrap();
    let epoch = engine.memo.cost_epoch_value();
    let candidate = engine
        .memo
        .group(root)
        .unwrap()
        .winner(goal)
        .unwrap()
        .candidate;
    let child_candidates = engine
        .memo
        .group(child)
        .unwrap()
        .winner_frontier(goal)
        .unwrap()
        .candidates()
        .iter()
        .map(|c| c.candidate)
        .collect::<Vec<_>>();
    let synthesized = engine.child_combination_cost_synthesis_count;
    let published = engine.memo.published_winner_count();
    let mandatory_domain = engine.physical_search_domain(root, goal).unwrap();
    engine.mandatory_only = false;
    engine.open_optional_implementation_domain().unwrap();
    assert_eq!(engine.memo.cost_epoch_value(), epoch);
    assert_ne!(
        engine.physical_search_domain(root, goal).unwrap(),
        mandatory_domain
    );
    assert!(engine.physical_completion_proofs.is_empty());
    assert_eq!(
        engine
            .memo
            .group(root)
            .unwrap()
            .winner(goal)
            .unwrap()
            .candidate,
        candidate
    );
    engine.optimize_group(root, goal).unwrap();
    assert_eq!(engine.child_combination_cost_synthesis_count, synthesized);
    assert_eq!(engine.memo.published_winner_count(), published);
    assert_eq!(
        engine
            .memo
            .group(child)
            .unwrap()
            .winner_frontier(goal)
            .unwrap()
            .candidates()
            .iter()
            .map(|c| c.candidate)
            .collect::<Vec<_>>(),
        child_candidates
    );
    assert!(
        !engine.physical_task_cache[&(child, goal)].mandatory_only,
        "unchanged parent recipes must still open their children's optional domain"
    );
}

#[test]
fn child_combination_refs_rebuilds_exact_choices_from_stable_ids() {
    let (_, _, child, goal) = resume_engine(false);
    let candidates = [CandidateId::new(41), CandidateId::new(7)];
    let goals = [(child, goal), (child, goal)];
    let refs = child_combination_refs(&candidates, &goals).unwrap();
    assert_eq!(refs.len(), candidates.len());
    assert_eq!(refs[0].group, child);
    assert_eq!(refs[0].goal, goal);
    assert_eq!(refs[0].candidate, CandidateId::new(41));
    assert_eq!(refs[1].candidate, CandidateId::new(7));
}

// Reuse the existing source-response toy. Only the initial parent recipe can
// be withheld: its sequential prefix is legal in the same open source context.
struct ResumeImplementation {
    filter_parent: bool,
}

impl PhysicalImplementation for ResumeImplementation {
    fn id(&self) -> ImplementationId {
        SourceSensitiveAlternativeImplementation.id()
    }

    fn matches(
        &self,
        logical: &crate::cascades::memo::LogicalExpr,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> bool {
        SourceSensitiveAlternativeImplementation.matches(logical, goal, ctx)
    }

    fn candidates(
        &self,
        expr: LogicalExprId,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>> {
        let mut candidates =
            SourceSensitiveAlternativeImplementation.candidates(expr, goal, ctx)?;
        if !self.filter_parent
            && ctx.memo.logical_expr(expr).unwrap().key.operator == Fingerprint(203)
        {
            candidates[0].cost_composition = CostComposition::Sequential;
            candidates[0].source_filter_apply_cost = None;
        }
        Ok(candidates)
    }
}

fn resume_engine(filter_parent: bool) -> (CascadesEngine, GroupId, GroupId, OptimizationGoal) {
    let mut budget = crate::cascades::budget::SearchBudget::default();
    // Only a runaway guard for the old pending_retry spin, not a quality oracle.
    // begin_optional() below activates it; successful retries never expire it.
    budget.optional_time_limit = Some(std::time::Duration::from_secs(5));
    let mut memo = Memo::new(budget);
    let mut add = |operator: u128, children: Box<[GroupId]>, group: Option<GroupId>| {
        let group = group.unwrap_or_else(|| {
            memo.create_group(
                schema(),
                LogicalProperties::default(),
                GroupCardinality::default(),
            )
        });
        memo.insert_logical(
            group,
            LogicalExprKey {
                operator: Fingerprint(operator),
                scalars: Box::new([]),
                children,
            },
            LogicalPayloadId(operator as u32),
            if operator == 202 {
                EquivalenceProof::Normalization { rule: RuleId(701) }
            } else {
                EquivalenceProof::Initial
            },
        )
        .unwrap();
        group
    };
    let scan_a = add(211, Box::new([]), None);
    let scan_b = add(212, Box::new([]), None);
    let child = add(201, Box::new([scan_a]), None);
    add(202, Box::new([scan_b]), Some(child));
    let root = add(203, Box::new([child]), None);
    // Both parent recipes observe exactly this same child goal. No new demand
    // context or child ReadSet is introduced when the filter recipe is appended.
    let context = memo
        .intern_source_demand_context(
            OptimizationContextId(0),
            BTreeSet::from([WorkSourceId(777)]),
        )
        .unwrap();
    let goal = OptimizationGoal {
        required: memo.intern_required(required()).unwrap(),
        row_goal: RowGoal::All,
        objective: ObjectiveProfile::Latency,
        grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
        context,
    };
    let mut registry = ImplementationRegistry::default();
    registry
        .register_implementation(ResumeImplementation { filter_parent })
        .unwrap();
    (CascadesEngine::new(memo, registry), root, child, goal)
}

fn response_choices(
    engine: &CascadesEngine,
    child: GroupId,
    goal: OptimizationGoal,
) -> (CandidateId, CandidateId) {
    let frontier = engine
        .memo()
        .group(child)
        .unwrap()
        .winner_frontier(goal)
        .unwrap();
    assert_eq!(frontier.candidates().len(), 2);
    let selected = frontier.selected().unwrap();
    assert_eq!(selected.cost.score.range.expected, 110.0);
    let other = frontier
        .candidates()
        .iter()
        .find(|candidate| candidate.candidate != selected.candidate)
        .unwrap();
    assert_eq!(other.cost.score.range.expected, 120.0);
    (selected.candidate, other.candidate)
}

#[test]
fn redundant_dirty_notification_does_not_reopen_a_complete_read_context() {
    let (mut engine, root, _, goal) = resume_engine(false);
    engine.optimize_group(root, goal).unwrap();
    let baseline = engine
        .memo()
        .group(root)
        .unwrap()
        .winner(goal)
        .unwrap()
        .candidate;
    let reopened = engine.task_registry.profile().reopened_evaluations;
    let synthesized = engine.child_combination_cost_synthesis_count;
    let reads = engine.physical_read_set(root, goal).unwrap();
    let recipes = engine
        .recipes
        .keys()
        .filter(|(physical, recipe_goal, _)| {
            *recipe_goal == goal
                && engine
                    .memo()
                    .group(root)
                    .unwrap()
                    .physical_exprs()
                    .contains(physical)
        })
        .map(|(physical, _, fingerprint)| (*physical, *fingerprint))
        .collect();
    engine.physical_dirty_recipes.insert((root, goal), recipes);
    engine.optimize_group(root, goal).unwrap();
    assert_eq!(engine.physical_read_set(root, goal).unwrap(), reads);
    assert_eq!(
        engine
            .memo()
            .group(root)
            .unwrap()
            .winner(goal)
            .unwrap()
            .candidate,
        baseline
    );
    assert_eq!(engine.child_combination_cost_synthesis_count, synthesized);
    assert_eq!(
        engine.task_registry.profile().reopened_evaluations,
        reopened
    );
}

#[test]
fn completion_only_notification_does_not_reprocess_priced_recipes() {
    let (mut engine, root, child, goal) = resume_engine(false);
    // First complete the real parent/child task chain.  The later notification
    // is deliberately isolated from the already-consumed frontier response.
    engine.optimize_group(root, goal).unwrap();
    assert!(engine.physical_task_cache[&(root, goal)].complete);

    let synthesized = engine.child_combination_cost_synthesis_count;
    let reprocessed = engine.physical_recipe_reprocess_count;
    // The child frontier is unchanged. Remove any already-observed work so
    // this assertion isolates the later completion-only notification.
    engine.physical_dirty_recipes.remove(&(root, goal));
    let child_cache = engine
        .physical_task_cache
        .get_mut(&(child, goal))
        .expect("the parent task must have observed its child");
    child_cache.complete = false;

    // The child closes without changing its frontier.  This is a completion
    // response, not a cost/frontier response: the parent must become pending
    // without receiving a dirty recipe set.
    engine.note_physical_completion_change(child, goal, true);
    assert!(engine.physical_completion_pending.contains(&(root, goal)));
    assert!(!engine.physical_dirty_recipes.contains_key(&(root, goal)));
    engine
        .physical_task_cache
        .get_mut(&(child, goal))
        .unwrap()
        .complete = true;

    engine.optimize_group(root, goal).unwrap();
    assert_eq!(
        engine.child_combination_cost_synthesis_count, synthesized,
        "completion-only progress must not re-synthesize priced combinations"
    );
    assert_eq!(
        engine.physical_recipe_reprocess_count, reprocessed,
        "completion-only progress must not reprocess old recipes"
    );
    assert!(engine.physical_task_cache[&(root, goal)].complete);
}

#[test]
fn resident_read_snapshot_shares_storage_and_refreshes_exact_child_goal() {
    let (mut engine, root, child, goal) = resume_engine(false);
    engine.optimize_group(root, goal).unwrap();
    let resident = engine.physical_task_cache[&(root, goal)].clone();
    let (same, changed) = engine
        .physical_read_set_incremental(root, goal, Some(&resident))
        .unwrap();
    assert!(!changed);
    assert_eq!(same.reads().as_ptr(), resident.reads.reads().as_ptr());
    let cloned = resident.clone();
    assert_eq!(
        cloned.reads.reads().as_ptr(),
        resident.reads.reads().as_ptr()
    );
    assert!(Arc::ptr_eq(
        cloned.dependencies.as_ref().unwrap(),
        resident.dependencies.as_ref().unwrap()
    ));

    // A fact update in an actual consumed child must detach, not mutate the
    // task's completed witness or silently reuse its cost context.
    engine.memo.group_mut(child).unwrap().cardinality = GroupCardinality::new(
        Fingerprint(456),
        crate::cascades::memo::CardinalityRecipeKind::Statistics,
        456,
        456,
        456,
    );
    let (fresh, changed) = engine
        .physical_read_set_incremental(root, goal, Some(&resident))
        .unwrap();
    assert!(changed);
    assert_ne!(fresh.reads().as_ptr(), resident.reads.reads().as_ptr());
    assert!(!resident.reads.is_current(&engine.memo).unwrap());
    assert!(fresh.is_current(&engine.memo).unwrap());
    assert_eq!(fresh, engine.physical_read_set(root, goal).unwrap());
}

#[test]
fn recipe_resume_completed_same_readset_accepts_appended_physical_recipe() {
    let (mut engine, root, child, goal) = resume_engine(false);
    engine.optimize_group(root, goal).unwrap();
    let (selected, other) = response_choices(&engine, child, goal);
    let baseline = engine
        .memo()
        .group(root)
        .unwrap()
        .winner(goal)
        .unwrap()
        .clone();
    assert_eq!(baseline.cost.score.range.expected, 110.0);
    assert_eq!(baseline.children[0].candidate, selected);
    let cached = engine.physical_task_cache.get(&(root, goal)).unwrap();
    assert!(
        cached.complete,
        "the prefix must actually be complete, not merely paused"
    );
    let cursor = cached.recipe_cursor;
    let reads = engine.physical_read_set(root, goal).unwrap();
    let task_count = engine.task_registry.task_count();
    let reopened = engine.task_registry.profile().reopened_evaluations;
    let epoch = engine.memo().cost_epoch_value();

    // Append a physical response to an already-enumerated logical expression.
    // Its child dependencies are unchanged; only the owned recipe stream grows.
    let logical = engine.memo().group(root).unwrap().logical_exprs()[0];
    let mut candidate = SourceSensitiveAlternativeImplementation
        .candidates(
            logical,
            goal,
            &ImplementationContext {
                memo: engine.memo(),
                group: root,
            },
        )
        .unwrap()
        .into_vec()
        .pop()
        .unwrap();
    candidate.key.payload_fingerprint = Fingerprint(9_001);
    candidate.physical_fingerprint = Fingerprint(9_001);
    engine
        .admit_candidate(
            root,
            logical,
            SourceSensitiveAlternativeImplementation.id(),
            goal,
            candidate,
        )
        .unwrap();
    assert_eq!(engine.physical_read_set(root, goal).unwrap(), reads);
    assert_eq!(engine.next_recipe_sequence[&(root, goal)], cursor + 1);
    assert!(engine.physical_task_cache[&(root, goal)].complete);

    engine.optimize_group(root, goal).unwrap();

    // Independent finite oracle, not the production composition function:
    // plain parent: min(100 + 10, 0 + 120) = 110;
    // filter parent: min(100 + 10 * .01, 0 + 120 * .01) = 1.2.
    let winner = engine.memo().group(root).unwrap().winner(goal).unwrap();
    assert!((winner.cost.score.range.expected - 1.2).abs() < 1e-9);
    assert_eq!(winner.children[0].goal, goal);
    assert_eq!(winner.children[0].candidate, other);
    assert_eq!(response_choices(&engine, child, goal), (selected, other));
    assert_eq!(
        engine
            .memo()
            .resolve_child_winner(baseline.children[0])
            .unwrap()
            .candidate,
        selected
    );
    assert_eq!(engine.memo().cost_epoch_value(), epoch);
    assert_eq!(
        engine.task_registry.task_count(),
        task_count,
        "reuse the exact evaluation identity"
    );
    assert_eq!(
        engine.task_registry.profile().reopened_evaluations,
        reopened + 1
    );
    assert_eq!(
        engine.physical_task_cache[&(root, goal)].recipe_cursor,
        cursor + 1
    );
    assert!(engine.physical_task_cache[&(root, goal)].complete);
}

#[test]
fn recipe_resume_budget_retry_pauses_then_prices_nonselected_child_once() {
    let (mut engine, root, child, goal) = resume_engine(true);
    let dimension = BudgetDimension::ChildFrontierCombination;
    engine
        .memo
        .group_ledger_mut(root)
        .unwrap()
        .set_limit(dimension, 0);
    engine.memo.control().begin_optional();
    engine.optimize_group(root, goal).unwrap();
    assert!(!engine.memo.control().deadline_reached());
    let (selected, other) = response_choices(&engine, child, goal);
    let baseline = engine
        .memo()
        .group(root)
        .unwrap()
        .winner(goal)
        .unwrap()
        .clone();
    assert_eq!(baseline.children[0].candidate, selected);
    assert!((baseline.cost.score.range.expected - 100.1).abs() < 1e-9);
    let key = (baseline.expression, goal, Fingerprint(203));
    let tuple: Box<[CandidateId]> = Box::new([other]);
    let state = &engine.child_combination_states[&key];
    assert_eq!(state.budget_rejected, BTreeSet::from([tuple.clone()]));
    assert_eq!(state.priced.len(), 1);
    assert!(
        !state.priced.contains_key(&tuple),
        "a denied tuple has no cost yet"
    );
    assert!(!engine.physical_task_cache[&(root, goal)].complete);
    let reads = engine.physical_read_set(root, goal).unwrap();
    let epoch = engine.memo().cost_epoch_value();
    let synthesized = engine.child_combination_cost_synthesis_count;
    let rejected = engine.child_combination_budget_rejection_count;

    // Changing credit is not a Memo publication. Explicitly wake just the
    // retained recipe, without invalidating its task or resetting tuple state.
    // First retry with zero credit: pending_retry must attempt admission again,
    // then pause, not repeatedly skip the same item until the deadline.
    engine
        .physical_dirty_recipes
        .entry((root, goal))
        .or_default()
        .insert((key.0, key.2));
    engine.optimize_group(root, goal).unwrap();
    assert!(
        !engine.memo.control().deadline_reached(),
        "pending_retry spun instead of pausing"
    );
    assert_eq!(
        engine.child_combination_budget_rejection_count,
        rejected + 1
    );
    assert_eq!(engine.child_combination_cost_synthesis_count, synthesized);
    assert_eq!(
        engine.child_combination_states[&key].budget_rejected,
        BTreeSet::from([tuple.clone()])
    );
    assert_eq!(
        engine
            .memo()
            .group(root)
            .unwrap()
            .ledger
            .consumed(dimension),
        0
    );

    engine
        .memo
        .group_ledger_mut(root)
        .unwrap()
        .set_limit(dimension, 1);
    engine
        .physical_dirty_recipes
        .entry((root, goal))
        .or_default()
        .insert((key.0, key.2));
    engine.optimize_group(root, goal).unwrap();
    assert!(!engine.memo.control().deadline_reached());
    assert_eq!(engine.physical_read_set(root, goal).unwrap(), reads);
    assert_eq!(engine.memo().cost_epoch_value(), epoch);
    assert_eq!(
        engine.child_combination_cost_synthesis_count,
        synthesized + 1
    );
    let state = &engine.child_combination_states[&key];
    assert!(state.budget_rejected.is_empty());
    assert_eq!(state.priced.len(), 2);
    assert!(state.priced.contains_key(&tuple));
    let winner = engine.memo().group(root).unwrap().winner(goal).unwrap();
    // The selected child stays 110; only the parent's filter makes 120 better:
    // min(100 + 10 * .01, 0 + 120 * .01) = 1.2, independently of Memo costing.
    assert!((winner.cost.score.range.expected - 1.2).abs() < 1e-9);
    assert_eq!(winner.children[0].candidate, other);
    assert_eq!(winner.children[0].goal, goal);
    assert_eq!(response_choices(&engine, child, goal), (selected, other));

    // Re-delivery is idempotent: no second synthesis, publication, or credit.
    let publications = engine.memo().published_winner_count();
    engine
        .physical_dirty_recipes
        .entry((root, goal))
        .or_default()
        .insert((key.0, key.2));
    engine.optimize_group(root, goal).unwrap();
    assert_eq!(
        engine.child_combination_cost_synthesis_count,
        synthesized + 1
    );
    assert_eq!(engine.memo().published_winner_count(), publications);
    let ledger = &engine.memo().group(root).unwrap().ledger;
    assert_eq!(ledger.consumed(dimension), 1);
    assert_eq!(
        ledger
            .exhaustion_events()
            .filter(|(dim, _)| *dim == dimension)
            .count(),
        1
    );
    // Historical budget evidence is intentionally not erased by the retry;
    // recovery of the candidate is distinct from certifying global closure.
}
