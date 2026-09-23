// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn local_bound_preparation_requires_a_live_pruning_request() {
    let (mut engine, root, goal) = engine(0);
    engine.terminal_bound_goals.insert((root, goal));
    let mut recipe = recipe(Box::new([]));
    recipe.task_supply = TaskSupplyContract::Serial;
    recipe.cost_composition = CostComposition::Sequential;
    assert!(!engine
        .recipe_is_provably_worse(root, goal, &recipe)
        .unwrap());
    assert!(recipe.certified_local_work.get().is_none());
    engine.set_certified_group_pruning_enabled(true);
    // Enabling proofs alone is insufficient: this group has no upper bound.
    assert!(!engine
        .recipe_is_provably_worse(root, goal, &recipe)
        .unwrap());
    assert!(recipe.certified_local_work.get().is_none());
    engine.optimize_group(root, goal).unwrap();
    let _ = engine
        .recipe_is_provably_worse(root, goal, &recipe)
        .unwrap();
    assert!(recipe.certified_local_work.get().is_some());
}

#[test]
fn scalar_bound_does_not_prune_parent_responses_or_quality_candidates() {
    let (mut engine, root, goal) = strong_tree_engine();
    engine.set_certified_group_pruning_enabled(true);
    engine.optimize(root, goal, SearchMode::Memo).unwrap();
    let child = engine
        .memo
        .logical_expr(engine.memo.group(root).unwrap().logical_exprs()[0])
        .unwrap()
        .key
        .children[0];
    let mut recipe = recipe(Box::new([]));
    recipe.local_cost = cost(1_000_000.0);
    recipe.task_supply = TaskSupplyContract::Serial;
    recipe.cost_composition = CostComposition::Sequential;
    // Even an enormous child-local latency is not a proof for every parent
    // continuation. This guard is independent of its current scalar winner.
    assert!(!engine
        .recipe_is_provably_worse(child, goal, &recipe)
        .unwrap());
    assert!(recipe.certified_local_work.get().is_none());
    engine.set_quality_policy_handoff_enabled(true);
    assert!(!engine
        .recipe_is_provably_worse(root, goal, &recipe)
        .unwrap());
    assert!(recipe.certified_local_work.get().is_none());
    engine.set_quality_policy_handoff_enabled(false);
    assert!(engine
        .recipe_is_provably_worse(root, goal, &recipe)
        .unwrap());
}

#[test]
fn unsupported_parent_requirement_does_not_prepare_children() {
    let (mut engine, root, mut goal) = strong_tree_engine();
    let class = crate::physical::ResourceGrantClass {
        id: ResourceGrantClassId(7),
        hard_memory_bytes: 1024,
        spill_policy: SpillPolicy::Forbidden,
        max_parallel_tasks: 1,
    };
    engine.prime_grant_context([class]).unwrap();
    goal.grant = GrantGoalKey::Class(class.id);
    let child = engine
        .memo
        .logical_expr(engine.memo.group(root).unwrap().logical_exprs()[0])
        .unwrap()
        .key
        .children[0];
    let mut unsupported = required();
    unsupported.replayability = ReplayabilityRequirement::Rewindable;
    let unsupported_goal = OptimizationGoal {
        required: engine.memo.intern_required(unsupported).unwrap(),
        ..goal
    };
    engine.optimize_group(root, unsupported_goal).unwrap();
    assert!(engine
        .memo
        .group(root)
        .unwrap()
        .winner(unsupported_goal)
        .is_none());
    assert!(engine
        .memo
        .group(child)
        .unwrap()
        .physical_exprs()
        .is_empty());
    assert_eq!(engine.child_combination_cost_synthesis_count, 0);

    // Failure of this requirement is not a group-wide infeasibility fact.
    // The ordinary goal must still build and consume the child normally.
    engine.optimize_group(root, goal).unwrap();
    assert!(engine.memo.group(root).unwrap().winner(goal).is_some());
    assert!(!engine
        .memo
        .group(child)
        .unwrap()
        .physical_exprs()
        .is_empty());
}

#[test]
fn former_terminal_goal_reopens_its_skipped_recipes_when_used_as_a_child() {
    let (mut engine, root, goal) = strong_tree_engine();
    let child = engine
        .memo
        .logical_expr(engine.memo.group(root).unwrap().logical_exprs()[0])
        .unwrap()
        .key
        .children[0];
    engine
        .registry
        .register_implementation(TreeImplementation {
            id: ImplementationId(44),
            operator: Fingerprint(100),
            child: None,
            child_row_goal: None,
            local_score: 1_000_000.0,
            mandatory: false,
        })
        .unwrap();
    engine.set_certified_group_pruning_enabled(true);
    engine.optimize(child, goal, SearchMode::Direct).unwrap();
    assert!(engine.certified_recipe_prune_count > 0);
    let skipped = *engine
        .recipes
        .keys()
        .find(|(physical, _, _)| {
            engine
                .memo
                .physical_expr(*physical)
                .unwrap()
                .key
                .implementation
                == ImplementationId(44)
        })
        .unwrap();
    assert!(!engine.child_combination_states.contains_key(&skipped));

    engine.optimize(root, goal, SearchMode::Direct).unwrap();
    assert!(!engine.child_combination_states[&skipped].priced.is_empty());
}

#[test]
fn enforcement_preparation_is_goal_local_and_does_not_cache_feasibility() {
    use super::super::super::properties::{
        NullOrder, OrderingKey, OrderingScope, RequiredOrdering, SortDirection,
    };
    let (mut engine, root, goal) = engine(0);
    engine.optimize_group(root, goal).unwrap();
    let mut required = required();
    required.ordering = OrderingRequirement::Ordered(RequiredOrdering {
        keys: Box::new([OrderingKey {
            column: ColumnId(0),
            direction: SortDirection::Asc,
            nulls: NullOrder::Last,
            collation: None,
        }]),
        scope: OrderingScope::Global,
    });
    let sorted_goal = OptimizationGoal {
        required: engine.memo.intern_required(required).unwrap(),
        ..goal
    };
    engine.optimize_group(root, sorted_goal).unwrap();
    let unsorted = engine
        .recipes
        .iter()
        .find(|((_, g, _), _)| *g == goal)
        .unwrap()
        .1;
    assert!(unsorted
        .enforcement
        .get()
        .unwrap()
        .as_ref()
        .unwrap()
        .steps
        .is_empty());
    let ((physical, _, _), sorted) = engine
        .recipes
        .iter()
        .find(|((_, g, _), _)| *g == sorted_goal)
        .unwrap();
    let (physical, sorted) = (*physical, Arc::clone(sorted));
    let builds = engine.physical_enforcement_builds;
    let enforced = engine
        .prepare_enforcement(physical, sorted_goal, &sorted)
        .unwrap()
        .unwrap();
    assert!(matches!(enforced.steps.as_ref(), [EnforcerStep::Sort(_)]));
    let input = EnforcerCostInput {
        hard_memory_bytes: 0,
        spill_policy: SpillPolicy::Forbidden,
        ..EnforcerCostInput::unbounded(CompactRange::point(100.0).unwrap(), 8)
    };
    assert!(
        enforcer_cost(&enforced.steps, input, engine.memo.calibration())
            .unwrap()
            .is_none()
    );
    assert!(enforcer_cost(
        &enforced.steps,
        EnforcerCostInput {
            spill_policy: SpillPolicy::Allowed,
            ..input
        },
        engine.memo.calibration()
    )
    .unwrap()
    .is_some());
    assert_eq!(engine.physical_enforcement_builds, builds);
}

fn recipe(children: Box<[(GroupId, OptimizationGoal)]>) -> CostRecipe {
    CostRecipe {
        sequence: 0,
        child_goals: children,
        local_cost: cost(7.0),
        source_filter_apply_cost: None,
        task_supply: TaskSupplyContract::Streaming { input: 0 },
        cost_composition: CostComposition::SidewaysFilter {
            overlapping_children: 0,
            filtered_child: 0,
            sources: Box::new([SidewaysFilterSource {
                source: WorkSourceId(1),
                domain: DomainProofId(Fingerprint(2)),
                evaluation: EvaluationOccurrenceId(Fingerprint(3)),
                expected_retained_ppm: 400_000,
                upper_retained_ppm: 800_000,
            }]),
        },
        spillable: false,
        enforcer_cost_input: EnforcerCostInput::unbounded(CompactRange::point(20.0).unwrap(), 8),
        physical_fingerprint: Fingerprint(10),
        region: None,
        certified_local_work: OnceLock::new(),
        immutable_cost_identity: OnceLock::new(),
        enforcement: OnceLock::new(),
    }
}

#[test]
fn immutable_recipe_identity_covers_local_resources_phase_and_source_response() {
    let (engine, owner, goal) = engine(0);
    let base = recipe(Box::new([]));
    let base_context =
        child_combination_cost_context_fingerprint(&engine.memo, owner, goal, &base).unwrap();
    let original = *base.immutable_cost_identity.get().unwrap();
    for change in 0..13 {
        // A replacement recipe has a new immutable payload; published recipes
        // are never patched in place when model/grant evidence changes.
        let mut changed = recipe(Box::new([]));
        match change {
            0 => changed.local_cost = cost(8.0),
            1 => changed.source_filter_apply_cost = Some(cost(1.0)),
            2 => changed.task_supply = TaskSupplyContract::Source { tasks: 4 },
            3 => changed.spillable = true,
            4 => changed.enforcer_cost_input.rows = CompactRange::point(30.0).unwrap(),
            5 => changed.enforcer_cost_input.row_width_bytes = 16,
            6 => changed.enforcer_cost_input.hard_memory_bytes = 1234,
            7 => changed.enforcer_cost_input.max_parallel_tasks = 3,
            8 => changed.physical_fingerprint = Fingerprint(11),
            _ => {
                let CostComposition::SidewaysFilter {
                    sources,
                    overlapping_children,
                    ..
                } = &mut changed.cost_composition
                else {
                    unreachable!()
                };
                match change {
                    9 => *overlapping_children = 1,
                    10 => sources[0].domain = DomainProofId(Fingerprint(4)),
                    11 => sources[0].evaluation = EvaluationOccurrenceId(Fingerprint(5)),
                    12 => sources[0].expected_retained_ppm = 300_000,
                    _ => unreachable!(),
                }
            }
        }
        let context =
            child_combination_cost_context_fingerprint(&engine.memo, owner, goal, &changed)
                .unwrap();
        assert_ne!(context, base_context, "omitted immutable input {change}");
        assert_ne!(*changed.immutable_cost_identity.get().unwrap(), original);
        assert_eq!(
            context,
            child_combination_cost_context_fingerprint(&engine.memo, owner, goal, &changed)
                .unwrap()
        );
    }
}

#[test]
fn cached_recipe_identity_does_not_cache_child_facts_grant_or_calibration() {
    let (mut engine, owner, goal) = engine(0);
    let child = engine.memo.create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let unrelated = engine.memo.create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let recipe = recipe(Box::new([(child, goal)]));
    let context = |memo: &Memo, goal| {
        child_combination_cost_context_fingerprint(memo, owner, goal, &recipe).unwrap()
    };
    let initial = context(&engine.memo, goal);
    let immutable = *recipe.immutable_cost_identity.get().unwrap();
    engine
        .memo
        .group_mut(unrelated)
        .unwrap()
        .logical_properties
        .maximum_cardinality = Some(5);
    assert_eq!(context(&engine.memo, goal), initial);
    engine
        .memo
        .group_mut(child)
        .unwrap()
        .logical_properties
        .maximum_cardinality = Some(5);
    let refined = context(&engine.memo, goal);
    assert_ne!(refined, initial);
    engine.memo.group_mut(child).unwrap().cardinality = GroupCardinality::new(
        Fingerprint(77),
        super::super::super::memo::CardinalityRecipeKind::Statistics,
        0,
        3,
        5,
    );
    let statistics = context(&engine.memo, goal);
    assert_ne!(statistics, refined);
    for changed_goal in [
        OptimizationGoal {
            grant: GrantGoalKey::Class(ResourceGrantClassId(9)),
            ..goal
        },
        OptimizationGoal {
            row_goal: RowGoal::AtMost(1),
            ..goal
        },
        OptimizationGoal {
            context: OptimizationContextId(9),
            ..goal
        },
    ] {
        assert_ne!(context(&engine.memo, changed_goal), statistics);
    }
    let mut calibration = MachineCalibrationBundle::default();
    calibration.risk_weight += 0.5;
    engine.memo.set_calibration(Arc::new(calibration));
    assert_ne!(context(&engine.memo, goal), statistics);
    assert_eq!(*recipe.immutable_cost_identity.get().unwrap(), immutable);
}

#[test]
fn recipe_context_still_resolves_canonical_child_groups_after_merge() {
    let (mut engine, owner, goal) = engine(0);
    let left = engine.memo.create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let right = engine.memo.create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let old = recipe(Box::new([(right, goal)]));
    let before =
        child_combination_cost_context_fingerprint(&engine.memo, owner, goal, &old).unwrap();
    let canonical = engine.merge_groups(left, right).unwrap();
    let new = recipe(Box::new([(canonical, goal)]));
    let resolved =
        child_combination_cost_context_fingerprint(&engine.memo, owner, goal, &old).unwrap();
    assert_ne!(resolved, before);
    assert_eq!(
        resolved,
        child_combination_cost_context_fingerprint(&engine.memo, owner, goal, &new).unwrap()
    );
}
