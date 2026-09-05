// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Memo identity, rollback, frontier, and group-merge tests.

use paro_common::types::LogicalType;

use super::*;
use crate::cascades::column::{ColumnDesc, ColumnOrigin, ColumnVisibility};
use crate::cascades::cost::{CompactRange, ScoreSummary};
use crate::cascades::properties::{
    MaterializationRequirement, MutationSafetyRequirement, OrderingRequirement,
    PartitioningRequirement, ProvidedMaterialization, ProvidedMutationSafety, ProvidedOrdering,
    ProvidedPartitioning, ProvidedReplayability, ProvidedRepresentation, ReplayabilityRequirement,
    RepresentationRequirement, ResultGuarantee,
};

fn schema(column: u32) -> GroupSchema {
    GroupSchema::new([ColumnDesc {
        id: super::super::ids::ColumnId(column),
        logical_type: LogicalType::Integer,
        nullable: false,
        origin: ColumnOrigin::Derived {
            key: Fingerprint(column as u128),
        },
        visibility: ColumnVisibility::Visible,
        name_hint: None,
    }])
    .unwrap()
}

fn provided() -> ProvidedProperties {
    ProvidedProperties {
        ordering: ProvidedOrdering::Unordered,
        partitioning: ProvidedPartitioning::Singleton,
        materialization: ProvidedMaterialization::default(),
        mutation_safety: ProvidedMutationSafety::NotApplicable,
        representation: ProvidedRepresentation::Flat,
        replayability: ProvidedReplayability::OnePass,
        result_guarantee: ResultGuarantee::Exact,
    }
}

fn enforcer_cost_input() -> super::super::engine::EnforcerCostInput {
    super::super::engine::EnforcerCostInput::unbounded(CompactRange::point(1.0).unwrap(), 8)
}

fn required() -> RequiredProperties {
    RequiredProperties {
        ordering: OrderingRequirement::Any,
        partitioning: PartitioningRequirement::Any,
        materialization: MaterializationRequirement::default(),
        mutation_safety: MutationSafetyRequirement::None,
        representation: RepresentationRequirement::Any,
        replayability: ReplayabilityRequirement::Any,
        result_guarantee: ResultGuarantee::Exact,
    }
}

#[test]
fn rule_history_is_expression_local_and_duplicate_expr_is_deduped() {
    let mut memo = Memo::new(SearchBudget::default());
    let group = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let key = LogicalExprKey {
        operator: Fingerprint(10),
        scalars: Box::new([]),
        children: Box::new([]),
    };
    let expression = memo
        .insert_logical(
            group,
            key.clone(),
            LogicalPayloadId(0),
            EquivalenceProof::Initial,
        )
        .unwrap();
    let duplicate = memo
        .insert_logical(
            group,
            key,
            LogicalPayloadId(0),
            EquivalenceProof::Normalization { rule: RuleId(1) },
        )
        .unwrap();
    assert_eq!(expression, duplicate);
    assert!(memo.mark_rule_applied(expression, RuleId(7)).unwrap());
    assert!(!memo.mark_rule_applied(expression, RuleId(7)).unwrap());
}

#[test]
fn logical_frontier_revision_never_aliases_a_rolled_back_candidate() {
    let mut memo = Memo::new(SearchBudget::default());
    let group = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let initial = memo
        .insert_logical(
            group,
            LogicalExprKey {
                operator: Fingerprint(10),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(0),
            EquivalenceProof::Initial,
        )
        .unwrap();
    let initial_revision = memo.group(group).unwrap().logical_expression_version();
    let savepoint = memo.transformation_savepoint();
    memo.insert_logical(
        group,
        LogicalExprKey {
            operator: Fingerprint(20),
            scalars: Box::new([]),
            children: Box::new([]),
        },
        LogicalPayloadId(1),
        EquivalenceProof::Normalization { rule: RuleId(1) },
    )
    .unwrap();
    let transient_revision = memo.group(group).unwrap().logical_expression_version();

    memo.rollback_transformation(savepoint).unwrap();
    let rollback_revision = memo.group(group).unwrap().logical_expression_version();
    assert!(initial_revision < transient_revision);
    assert!(transient_revision < rollback_revision);

    memo.insert_logical(
        group,
        LogicalExprKey {
            operator: Fingerprint(30),
            scalars: Box::new([]),
            children: Box::new([]),
        },
        LogicalPayloadId(2),
        EquivalenceProof::Transformation {
            rule: RuleId(2),
            source: initial,
            premise: Fingerprint(10),
        },
    )
    .unwrap();
    assert!(memo.group(group).unwrap().logical_expression_version() > rollback_revision);
}

#[test]
fn winner_is_keyed_by_goal_and_uses_stable_tie_break() {
    let mut memo = Memo::new(SearchBudget::default());
    let group = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let logical = memo
        .insert_logical(
            group,
            LogicalExprKey {
                operator: Fingerprint(1),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(0),
            EquivalenceProof::Initial,
        )
        .unwrap();
    let properties = provided();
    let physical = memo
        .insert_physical(
            group,
            PhysicalExprKey {
                implementation: ImplementationId(1),
                logical,
                children: Box::new([]),
                payload_fingerprint: Fingerprint(1),
            },
            PhysicalPayloadId(0),
            properties.clone(),
        )
        .unwrap();
    let required = memo.intern_required(required()).unwrap();
    let goal = OptimizationGoal {
        required,
        row_goal: RowGoal::All,
        objective: ObjectiveProfileId(0),
        grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
        context: OptimizationContextId(0),
    };
    let cost = SearchCost {
        score: ScoreSummary {
            range: CompactRange::point(1.0).unwrap(),
            risk_adjusted: 1.0,
        },
        ..SearchCost::ZERO
    };
    memo.record_winner(
        group,
        goal,
        Winner {
            expression: physical,
            child_goals: Box::new([]),
            enforcers: Box::new([]),
            enforcer_cost_input: enforcer_cost_input(),
            provided: properties.clone(),
            local_cost: cost,
            source_filter_apply_cost: None,
            cost_composition: CostComposition::Sequential,
            cost,
            source_work: Box::new([]),
            physical_fingerprint: Fingerprint(20),
            joint_cost_proof: None,
        },
    )
    .unwrap();
    memo.record_winner(
        group,
        goal,
        Winner {
            expression: physical,
            child_goals: Box::new([]),
            enforcers: Box::new([]),
            enforcer_cost_input: enforcer_cost_input(),
            provided: properties,
            local_cost: cost,
            source_filter_apply_cost: None,
            cost_composition: CostComposition::Sequential,
            cost,
            source_work: Box::new([]),
            physical_fingerprint: Fingerprint(10),
            joint_cost_proof: None,
        },
    )
    .unwrap();
    assert_eq!(
        memo.group(group)
            .unwrap()
            .winner(goal)
            .unwrap()
            .physical_fingerprint,
        Fingerprint(10)
    );
}

#[test]
fn exact_tie_keeps_the_mandatory_expression_ahead_of_ephemeral_fingerprints() {
    let mut memo = Memo::new(SearchBudget::default());
    let group = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let baseline_logical = memo
        .insert_logical(
            group,
            LogicalExprKey {
                operator: Fingerprint(100),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(0),
            EquivalenceProof::Initial,
        )
        .unwrap();
    let optional_logical = memo
        .insert_logical(
            group,
            LogicalExprKey {
                operator: Fingerprint(200),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(1),
            EquivalenceProof::Transformation {
                rule: RuleId(7),
                source: baseline_logical,
                premise: Fingerprint(100),
            },
        )
        .unwrap();
    let properties = provided();
    let baseline = memo
        .insert_physical(
            group,
            PhysicalExprKey {
                implementation: ImplementationId(1),
                logical: baseline_logical,
                children: Box::new([]),
                payload_fingerprint: Fingerprint(u128::MAX),
            },
            PhysicalPayloadId(0),
            properties.clone(),
        )
        .unwrap();
    let optional = memo
        .insert_physical(
            group,
            PhysicalExprKey {
                implementation: ImplementationId(1),
                logical: optional_logical,
                children: Box::new([]),
                payload_fingerprint: Fingerprint(0),
            },
            PhysicalPayloadId(1),
            properties.clone(),
        )
        .unwrap();
    let goal = OptimizationGoal {
        required: memo.intern_required(required()).unwrap(),
        row_goal: RowGoal::All,
        objective: ObjectiveProfileId(0),
        grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
        context: OptimizationContextId(0),
    };
    let cost = SearchCost {
        score: ScoreSummary {
            range: CompactRange::point(1.0).unwrap(),
            risk_adjusted: 1.0,
        },
        ..SearchCost::ZERO
    };
    for (expression, fingerprint) in [
        (optional, Fingerprint(0)),
        (baseline, Fingerprint(u128::MAX)),
    ] {
        memo.record_winner(
            group,
            goal,
            Winner {
                expression,
                child_goals: Box::new([]),
                enforcers: Box::new([]),
                enforcer_cost_input: enforcer_cost_input(),
                provided: properties.clone(),
                local_cost: cost,
                source_filter_apply_cost: None,
                cost_composition: CostComposition::Sequential,
                cost,
                source_work: Box::new([]),
                physical_fingerprint: fingerprint,
                joint_cost_proof: None,
            },
        )
        .unwrap();
    }
    assert_eq!(
        memo.group(group).unwrap().winner(goal).unwrap().expression,
        baseline
    );
}

#[test]
fn winner_frontier_retains_non_dominated_resource_tradeoffs() {
    let mut budget = SearchBudget::default();
    budget.max_pareto_winners_per_goal = 4;
    let mut memo = Memo::new(budget);
    let group = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let logical = memo
        .insert_logical(
            group,
            LogicalExprKey {
                operator: Fingerprint(1),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(0),
            EquivalenceProof::Initial,
        )
        .unwrap();
    let properties = provided();
    let physical = memo
        .insert_physical(
            group,
            PhysicalExprKey {
                implementation: ImplementationId(1),
                logical,
                children: Box::new([]),
                payload_fingerprint: Fingerprint(1),
            },
            PhysicalPayloadId(0),
            properties.clone(),
        )
        .unwrap();
    let goal = OptimizationGoal {
        required: memo.intern_required(required()).unwrap(),
        row_goal: RowGoal::All,
        objective: ObjectiveProfileId(0),
        grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
        context: OptimizationContextId(0),
    };
    for (score, memory, fingerprint) in [(1.0, 100, 1), (2.0, 10, 2), (3.0, 200, 3)] {
        let cost = SearchCost {
            score: ScoreSummary {
                range: CompactRange::point(score).unwrap(),
                risk_adjusted: score,
            },
            critical_path: CompactRange::point(score).unwrap(),
            peak_memory_upper: memory,
            revocable_memory_target: memory,
            ..SearchCost::ZERO
        };
        memo.record_winner(
            group,
            goal,
            Winner {
                expression: physical,
                child_goals: Box::new([]),
                enforcers: Box::new([]),
                enforcer_cost_input: enforcer_cost_input(),
                provided: properties.clone(),
                local_cost: cost,
                source_filter_apply_cost: None,
                cost_composition: CostComposition::Sequential,
                cost,
                source_work: Box::new([]),
                physical_fingerprint: Fingerprint(fingerprint),
                joint_cost_proof: None,
            },
        )
        .unwrap();
    }
    let frontier = memo.group(group).unwrap().winner_frontier(goal).unwrap();
    assert_eq!(frontier.candidates().len(), 2);
    assert_eq!(
        frontier.selected().unwrap().physical_fingerprint,
        Fingerprint(1)
    );
}

#[test]
fn latency_and_robustness_profiles_rank_uncertainty_explicitly() {
    let winner = |expression, expected_path, expected_work, risk_upper, risk_adjusted| {
        let cost = SearchCost {
            score: ScoreSummary {
                range: CompactRange::new(expected_work, expected_work, risk_upper).unwrap(),
                risk_adjusted,
            },
            critical_path: CompactRange::new(expected_path, expected_path, risk_upper).unwrap(),
            ..SearchCost::ZERO
        };
        Winner {
            expression: PhysicalExprId::new(expression),
            child_goals: Box::new([]),
            enforcers: Box::new([]),
            enforcer_cost_input: enforcer_cost_input(),
            provided: provided(),
            local_cost: cost,
            source_filter_apply_cost: None,
            cost_composition: CostComposition::Sequential,
            cost,
            source_work: Box::new([]),
            physical_fingerprint: Fingerprint(expression as u128),
            joint_cost_proof: None,
        }
    };
    let fast_expected = winner(1, 10.0, 20.0, 100.0, 80.0);
    let narrow_uncertainty = winner(2, 20.0, 22.0, 30.0, 25.0);

    assert_eq!(
        compare_objective(&fast_expected, &narrow_uncertainty, ObjectiveProfileId(0)),
        std::cmp::Ordering::Less,
        "the latency profile optimizes expected response time"
    );
    assert_eq!(
        compare_objective(&fast_expected, &narrow_uncertainty, ObjectiveProfileId(3)),
        std::cmp::Ordering::Greater,
        "the robustness profile optimizes the hard uncertainty bound"
    );
}

#[test]
fn group_merge_rejects_output_contract_change() {
    let mut memo = Memo::new(SearchBudget::default());
    let left = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let right = memo.create_group(
        schema(2),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    assert!(memo.merge_groups(left, right).is_err());
}

#[test]
fn group_merge_intersects_independently_proven_cardinality_bounds() {
    let mut memo = Memo::new(SearchBudget::default());
    let mut loose = LogicalProperties::default();
    loose.maximum_cardinality = Some(16);
    let mut tight = LogicalProperties::default();
    tight.maximum_cardinality = Some(4);
    let left = memo.create_group(schema(1), loose, GroupCardinality::default());
    let right = memo.create_group(schema(1), tight, GroupCardinality::default());

    let group = memo.merge_groups(left, right).unwrap();

    assert_eq!(
        memo.group(group)
            .unwrap()
            .logical_properties
            .maximum_cardinality,
        Some(4)
    );
}

#[test]
fn canonical_cardinality_recipe_preserves_peer_uncertainty_and_kind_priority() {
    let statistics_a =
        GroupCardinality::new(Fingerprint(20), CardinalityRecipeKind::Statistics, 4, 9, 14);
    let statistics_b =
        GroupCardinality::new(Fingerprint(10), CardinalityRecipeKind::Statistics, 5, 5, 8);
    let statistics_c = GroupCardinality::new(
        Fingerprint(30),
        CardinalityRecipeKind::Statistics,
        1,
        20,
        40,
    );
    let forward = statistics_a.clone().canonical_with(statistics_b.clone());
    let reverse = statistics_b.clone().canonical_with(statistics_a.clone());
    assert_eq!(forward, reverse);
    assert_eq!(forward.representative(), Some((4, 7, 14)));
    assert_eq!(forward.recipe, Fingerprint(10));
    assert_eq!(
        statistics_a
            .clone()
            .canonical_with(statistics_b.clone())
            .canonical_with(statistics_c.clone()),
        statistics_a.canonical_with(statistics_b.canonical_with(statistics_c))
    );

    let region = GroupCardinality::new(Fingerprint(30), CardinalityRecipeKind::JoinRegion, 5, 6, 7);
    assert_eq!(forward.clone().canonical_with(region.clone()), region);
    assert_eq!(region.clone().canonical_with(forward), region);

    let inherited = GroupCardinality::inherit(Fingerprint(40), GroupId::new(1));
    let refined = GroupCardinality::inherit(Fingerprint(50), GroupId::new(2))
        .with_kind(CardinalityRecipeKind::ConstraintRefined);
    assert_eq!(inherited.clone().canonical_with(refined.clone()), refined);
    assert_eq!(refined.clone().canonical_with(inherited), refined);
}

#[test]
fn inherited_cardinality_tracks_child_and_respects_group_hard_bound() {
    let mut memo = Memo::new(SearchBudget::default());
    let child = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::new(
            Fingerprint(1),
            CardinalityRecipeKind::Statistics,
            80,
            100,
            120,
        ),
    );
    let parent = memo.create_group(
        schema(1),
        LogicalProperties {
            maximum_cardinality: Some(90),
            ..LogicalProperties::default()
        },
        GroupCardinality::inherit(Fingerprint(2), child),
    );

    assert_eq!(memo.cardinality_estimate(parent), Some((80, 90, 90)));

    memo.group_mut(child).unwrap().cardinality =
        GroupCardinality::new(Fingerprint(3), CardinalityRecipeKind::JoinRegion, 4, 5, 6);
    assert_eq!(memo.cardinality_estimate(parent), Some((4, 5, 6)));
}

#[test]
fn peer_row_preserving_recipes_track_every_equivalent_input() {
    let mut memo = Memo::new(SearchBudget::default());
    let first = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::new(
            Fingerprint(1),
            CardinalityRecipeKind::Statistics,
            10,
            20,
            30,
        ),
    );
    let second = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::new(Fingerprint(2), CardinalityRecipeKind::Statistics, 5, 8, 12),
    );
    let inherited = GroupCardinality::inherit(Fingerprint(3), first)
        .canonical_with(GroupCardinality::inherit(Fingerprint(4), second));
    let parent = memo.create_group(schema(1), LogicalProperties::default(), inherited);

    assert_eq!(memo.cardinality_estimate(parent), Some((5, 14, 30)));
}

#[test]
fn winner_recording_recomputes_local_cost_instead_of_trusting_total() {
    let mut memo = Memo::new(SearchBudget::default());
    let group = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let logical = memo
        .insert_logical(
            group,
            LogicalExprKey {
                operator: Fingerprint(1),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(0),
            EquivalenceProof::Initial,
        )
        .unwrap();
    let properties = provided();
    let physical = memo
        .insert_physical(
            group,
            PhysicalExprKey {
                implementation: ImplementationId(1),
                logical,
                children: Box::new([]),
                payload_fingerprint: Fingerprint(1),
            },
            PhysicalPayloadId(0),
            properties.clone(),
        )
        .unwrap();
    let goal = OptimizationGoal {
        required: memo.intern_required(required()).unwrap(),
        row_goal: RowGoal::All,
        objective: ObjectiveProfileId(0),
        grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
        context: OptimizationContextId(0),
    };
    let local = SearchCost {
        score: ScoreSummary {
            range: CompactRange::point(1.0).unwrap(),
            risk_adjusted: 1.0,
        },
        critical_path: CompactRange::point(1.0).unwrap(),
        ..SearchCost::ZERO
    };
    let falsified = SearchCost {
        score: ScoreSummary {
            range: CompactRange::point(0.5).unwrap(),
            risk_adjusted: 0.5,
        },
        critical_path: CompactRange::point(0.5).unwrap(),
        ..SearchCost::ZERO
    };
    assert!(memo
        .record_winner(
            group,
            goal,
            Winner {
                expression: physical,
                child_goals: Box::new([]),
                enforcers: Box::new([]),
                enforcer_cost_input: enforcer_cost_input(),
                provided: properties,
                local_cost: local,
                source_filter_apply_cost: None,
                cost_composition: CostComposition::Sequential,
                cost: falsified,
                source_work: Box::new([]),
                physical_fingerprint: Fingerprint(1),
                joint_cost_proof: None,
            },
        )
        .is_err());
}

#[test]
fn equivalent_region_facets_merge_their_best_scheduling_priority() {
    use super::super::region::{FacetCriticality, RegionFacet, RegionFacetKind};

    let mut memo = Memo::new(SearchBudget::default());
    let group = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let fingerprint = Fingerprint(91);
    memo.upsert_region_facet(RegionFacet {
        fingerprint,
        kind: RegionFacetKind::RuntimeFilter,
        criticality: FacetCriticality::Optional,
        priority: 2_003,
        scope_contract: super::super::region::RegionScopeContract::OwnerWithImmediateInputs,
        scope: std::iter::once(group).collect(),
    })
    .unwrap();
    memo.upsert_region_facet(RegionFacet {
        fingerprint,
        kind: RegionFacetKind::RuntimeFilter,
        criticality: FacetCriticality::Optional,
        priority: 1_003,
        scope_contract: super::super::region::RegionScopeContract::OwnerWithImmediateInputs,
        scope: std::iter::once(group).collect(),
    })
    .unwrap();

    let facet = memo
        .regions()
        .nodes
        .iter()
        .flat_map(|region| region.facets.iter())
        .find(|facet| facet.fingerprint == fingerprint)
        .unwrap();
    assert_eq!(facet.priority, 1_003);
}

#[test]
fn optimization_context_catalog_is_linear_and_sealed_before_search() {
    let mut memo = Memo::new(SearchBudget::default());
    let group = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    memo.insert_logical(
        group,
        LogicalExprKey {
            operator: Fingerprint(1),
            scalars: Box::new([]),
            children: Box::new([]),
        },
        LogicalPayloadId(0),
        EquivalenceProof::Initial,
    )
    .unwrap();
    let input = memo
        .intern_optimization_context(OptimizationContext::new([Fingerprint(11)]))
        .unwrap();
    let child = memo
        .intern_optimization_context(OptimizationContext::new([Fingerprint(22)]))
        .unwrap();

    assert_eq!(
        memo.optimization_context(input),
        Some(&OptimizationContext::new([Fingerprint(11)]))
    );
    assert_eq!(
        memo.optimization_context(child),
        Some(&OptimizationContext::new([Fingerprint(22)]))
    );
    memo.freeze_optimization_contexts().unwrap();

    let error = memo
        .intern_optimization_context(OptimizationContext::new([Fingerprint(33)]))
        .expect_err("optional search cannot create context combinations");
    assert!(error
        .to_string()
        .contains("immutable after initial Memo binding"));
}

#[test]
fn optimization_context_catalog_rejects_a_superlinear_initial_state() {
    let mut memo = Memo::new(SearchBudget::default());
    let group = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    memo.insert_logical(
        group,
        LogicalExprKey {
            operator: Fingerprint(1),
            scalars: Box::new([]),
            children: Box::new([]),
        },
        LogicalPayloadId(0),
        EquivalenceProof::Initial,
    )
    .unwrap();
    for facet in [11, 22, 33] {
        memo.intern_optimization_context(OptimizationContext::new([Fingerprint(facet)]))
            .unwrap();
    }

    let error = memo
        .freeze_optimization_contexts()
        .expect_err("one expression admits at most two non-root contexts");
    assert!(error.to_string().contains("exceeds its linear bound"));
}
