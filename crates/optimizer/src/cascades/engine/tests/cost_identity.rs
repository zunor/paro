// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

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
        certified_local_work: None,
        immutable_cost_identity: OnceLock::new(),
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
