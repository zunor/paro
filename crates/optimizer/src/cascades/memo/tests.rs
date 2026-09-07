// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Memo identity, rollback, frontier, and group-merge tests.

use paro_common::types::LogicalType;

use super::*;
use crate::cascades::budget::BudgetDecision;
use crate::cascades::column::{ColumnDesc, ColumnOrigin, ColumnVisibility};
use crate::cascades::cost::{CompactRange, ScoreSummary};
use crate::cascades::ids::ColumnId;
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
fn relational_hash_collision_requires_exact_operator_encoding() {
    let mut memo = Memo::new(SearchBudget::default());
    let group = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let key = LogicalExprKey {
        operator: Fingerprint(77),
        scalars: Box::new([]),
        children: Box::new([]),
    };
    let first = memo
        .insert_logical_with_operator_encoding(
            group,
            key.clone(),
            LogicalPayloadId(1),
            EquivalenceProof::Initial,
            b"operator-a".to_vec().into_boxed_slice(),
        )
        .unwrap();
    let collision = memo
        .insert_logical_with_operator_encoding(
            group,
            key.clone(),
            LogicalPayloadId(2),
            EquivalenceProof::Normalization { rule: RuleId(1) },
            b"operator-b".to_vec().into_boxed_slice(),
        )
        .unwrap();
    let duplicate = memo
        .insert_logical_with_operator_encoding(
            group,
            key,
            LogicalPayloadId(3),
            EquivalenceProof::Normalization { rule: RuleId(2) },
            b"operator-a".to_vec().into_boxed_slice(),
        )
        .unwrap();

    assert_ne!(first, collision);
    assert_eq!(first, duplicate);
    assert_eq!(memo.group(group).unwrap().logical_exprs().len(), 2);
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
        objective: ObjectiveProfile::Latency,
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
            candidate: CandidateId::INVALID,
            expression: physical,
            children: Box::new([]),
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
    let retired_candidate = memo.group(group).unwrap().winner(goal).unwrap().candidate;
    memo.record_winner(
        group,
        goal,
        Winner {
            candidate: CandidateId::INVALID,
            expression: physical,
            children: Box::new([]),
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
    assert_eq!(
        memo.resolve_child_winner(ChildWinnerRef {
            group,
            goal,
            candidate: retired_candidate,
        })
        .unwrap()
        .physical_fingerprint,
        Fingerprint(20),
        "an exact child reference must survive frontier pruning"
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
        objective: ObjectiveProfile::Latency,
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
                candidate: CandidateId::INVALID,
                expression,
                children: Box::new([]),
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
        objective: ObjectiveProfile::Latency,
        grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
        context: OptimizationContextId(0),
    };
    // More than the old fixed frontier capacity. None of these memory/work
    // tradeoffs can be discarded without knowing the parent context.
    let tradeoffs = (1_u64..=16).map(|rank| (rank as f64, 1000 - rank * 50, u128::from(rank)));
    for (score, memory, fingerprint) in tradeoffs.chain([(100.0, 2000, 100)]) {
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
                candidate: CandidateId::INVALID,
                expression: physical,
                children: Box::new([]),
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
    assert_eq!(frontier.candidates().len(), 16);
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
            work_latency: CompactRange::new(expected_work, expected_work, risk_upper).unwrap(),
            max_parallel_tasks: 4,
            critical_path: CompactRange::new(expected_path, expected_path, risk_upper).unwrap(),
            ..SearchCost::ZERO
        };
        Winner {
            candidate: CandidateId::INVALID,
            expression: PhysicalExprId::new(expression),
            children: Box::new([]),
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
    let parallel_uncertain = winner(1, 26.0, 104.0, 200.0, 150.0);
    let serial_robust = winner(2, 100.0, 100.0, 110.0, 105.0);

    assert_eq!(
        compare_objective(
            &parallel_uncertain,
            &serial_robust,
            ObjectiveProfile::Latency,
        ),
        std::cmp::Ordering::Less,
        "latency combines capacity-adjusted work with dependency span"
    );
    assert_eq!(
        compare_objective(
            &parallel_uncertain,
            &serial_robust,
            ObjectiveProfile::Robustness,
        ),
        std::cmp::Ordering::Greater,
        "the robustness profile optimizes the hard uncertainty bound"
    );
}

#[test]
fn frontier_and_admission_share_the_same_objective_contract() {
    use crate::physical::{PhysicalPlanPortfolio, ResourceGrantClass, SpillPolicy};

    let winner = |expression: usize, expected: f64, upper: f64, risk_adjusted: f64| {
        let range = CompactRange::new(expected, expected, upper).unwrap();
        let cost = SearchCost {
            score: ScoreSummary {
                range,
                risk_adjusted,
            },
            work_latency: range,
            critical_path: range,
            ..SearchCost::ZERO
        };
        Winner {
            candidate: CandidateId::INVALID,
            expression: PhysicalExprId::new(expression),
            children: Box::new([]),
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
    let latency = winner(1, 20.0, 100.0, 80.0);
    let robust = winner(2, 22.0, 30.0, 25.0);
    let goal = OptimizationGoal {
        required: PropertySetId(0),
        row_goal: RowGoal::All,
        objective: ObjectiveProfile::Latency,
        grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
        context: OptimizationContextId(0),
    };
    let mut frontier = WinnerFrontier::default();
    frontier.insert(goal, latency.clone());
    frontier.insert(goal, robust.clone());
    assert_eq!(
        frontier.selected().unwrap().physical_fingerprint,
        latency.physical_fingerprint
    );

    let class = ResourceGrantClass {
        id: ResourceGrantClassId(1),
        hard_memory_bytes: 100,
        spill_policy: SpillPolicy::Allowed,
        max_parallel_tasks: 1,
    };
    let portfolio = PhysicalPlanPortfolio::build(
        goal.objective,
        [class],
        [
            (
                class.id,
                "latency",
                latency.physical_fingerprint,
                latency.cost,
            ),
            (class.id, "robust", robust.physical_fingerprint, robust.cost),
        ],
    )
    .unwrap();
    let admitted = portfolio.admit(100, 1, 0, |_| true).unwrap();
    assert_eq!(admitted.physical_fingerprint, latency.physical_fingerprint);
}

#[test]
fn dominance_preserves_equal_work_latency_span_tradeoffs() {
    let mut fast_uncertain = SearchCost {
        score: ScoreSummary {
            range: CompactRange::new(20.0, 20.0, 100.0).unwrap(),
            risk_adjusted: 60.0,
        },
        work_latency: CompactRange::new(20.0, 20.0, 100.0).unwrap(),
        critical_path: CompactRange::new(10.0, 10.0, 100.0).unwrap(),
        ..SearchCost::ZERO
    };
    let mut slow_robust = SearchCost {
        score: ScoreSummary {
            range: CompactRange::new(20.0, 20.0, 30.0).unwrap(),
            risk_adjusted: 25.0,
        },
        work_latency: CompactRange::new(20.0, 20.0, 30.0).unwrap(),
        critical_path: CompactRange::new(20.0, 20.0, 30.0).unwrap(),
        ..SearchCost::ZERO
    };
    // Resource arrays participate in dominance independently of the scalar
    // score and must carry the same expected/risk tradeoff.
    fast_uncertain.resources_expected[0] = 20.0;
    fast_uncertain.resources_risk_upper[0] = 100.0;
    slow_robust.resources_expected[0] = 20.0;
    slow_robust.resources_risk_upper[0] = 30.0;

    assert!(!slow_robust.dominates(&fast_uncertain));
    assert!(!fast_uncertain.dominates(&slow_robust));
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
fn cte_reference_domain_reads_the_current_producer_group_fact() {
    let mut memo = Memo::new(SearchBudget::default());
    let mut producer_properties = LogicalProperties::default();
    producer_properties.column_domains.insert(
        ColumnId::new(0),
        GroupColumnDomain::new(Some(10), Some(20)).unwrap(),
    );
    let producer = memo.create_group(
        schema(1),
        producer_properties,
        GroupCardinality::new(Fingerprint(1), CardinalityRecipeKind::Statistics, 5, 10, 20),
    );

    let mut reference_properties = LogicalProperties::default();
    reference_properties.column_domains.insert(
        ColumnId::new(1),
        GroupColumnDomain::new(Some(100), None).unwrap(),
    );
    reference_properties
        .cte_references
        .insert(CteReferenceDomain {
            cte_index: 7,
            columns: vec![ColumnId::new(1)].into_boxed_slice(),
        });
    let reference = memo.create_group(schema(2), reference_properties, GroupCardinality::default());
    memo.register_cte_producer(7, producer, vec![ColumnId::new(0)].into_boxed_slice());

    assert_eq!(
        memo.column_domain(reference, ColumnId::new(1))
            .and_then(GroupColumnDomain::expected),
        Some(10)
    );
    assert_eq!(memo.cardinality_estimate(reference), Some((5, 10, 20)));
    memo.group_mut(producer)
        .unwrap()
        .logical_properties
        .column_domains
        .insert(
            ColumnId::new(0),
            GroupColumnDomain::new(Some(4), Some(8)).unwrap(),
        );
    memo.group_mut(producer).unwrap().cardinality = GroupCardinality::new(
        Fingerprint(2),
        CardinalityRecipeKind::ConstraintRefined,
        2,
        4,
        8,
    );
    assert_eq!(
        memo.column_domain(reference, ColumnId::new(1))
            .and_then(GroupColumnDomain::expected),
        Some(4)
    );
    assert_eq!(memo.cardinality_estimate(reference), Some((2, 4, 8)));
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
fn final_winner_verifier_recomputes_cost_outside_frontier_admission() {
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
        objective: ObjectiveProfile::Latency,
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
                candidate: CandidateId::INVALID,
                expression: physical,
                children: Box::new([]),
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
        .unwrap());
    assert!(crate::cascades::verifier::WinnerVerifier::verify(&memo).is_err());
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
fn repeated_region_facet_upsert_is_allocation_free() {
    use super::super::region::{FacetCriticality, RegionFacet, RegionFacetKind};

    let mut memo = Memo::new(SearchBudget::default());
    let group = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let facet = RegionFacet {
        fingerprint: Fingerprint(92),
        kind: RegionFacetKind::RuntimeFilter,
        criticality: FacetCriticality::Optional,
        priority: 1_003,
        scope_contract: super::super::region::RegionScopeContract::OwnerWithImmediateInputs,
        scope: std::iter::once(group).collect(),
    };
    memo.upsert_region_facet(facet.clone()).unwrap();
    let nodes = memo.regions().nodes.as_ptr();
    memo.upsert_region_facet(facet).unwrap();
    assert_eq!(memo.regions().nodes.as_ptr(), nodes);
}

#[test]
fn region_facet_batch_matches_individual_normalization() {
    use super::super::region::{FacetCriticality, RegionFacet, RegionFacetKind};

    fn memo_with_groups() -> (Memo, [GroupId; 3]) {
        let mut memo = Memo::new(SearchBudget::default());
        let groups = std::array::from_fn(|_| {
            memo.create_group(
                schema(1),
                LogicalProperties::default(),
                GroupCardinality::default(),
            )
        });
        (memo, groups)
    }
    fn facets(groups: [GroupId; 3]) -> Vec<RegionFacet> {
        [[groups[0], groups[1]], [groups[1], groups[2]]]
            .into_iter()
            .enumerate()
            .map(|(index, scope)| RegionFacet {
                fingerprint: Fingerprint(100 + index as u128),
                kind: RegionFacetKind::RuntimeFilter,
                criticality: FacetCriticality::Optional,
                priority: 1_003,
                scope_contract: super::super::region::RegionScopeContract::OwnerWithImmediateInputs,
                scope: scope.into_iter().collect(),
            })
            .collect()
    }

    let (mut individual, individual_groups) = memo_with_groups();
    for facet in facets(individual_groups) {
        individual.upsert_region_facet(facet).unwrap();
    }
    let (mut batched, batched_groups) = memo_with_groups();
    batched
        .upsert_region_facets(facets(batched_groups))
        .unwrap();
    assert_eq!(individual.regions(), batched.regions());
}

#[test]
fn optional_group_budgets_are_query_global_isolated_and_observable() {
    let mut budget = SearchBudget::default();
    budget.max_optional_groups_per_initial_group = 1;
    budget.max_optional_composition_groups_per_initial_group = 1;
    let mut memo = Memo::new(budget);
    memo.create_optional_group(
        BudgetDimension::Group,
        Fingerprint(1),
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    )
    .unwrap();
    assert!(memo
        .create_optional_group(
            BudgetDimension::Group,
            Fingerprint(2),
            schema(1),
            LogicalProperties::default(),
            GroupCardinality::default(),
        )
        .unwrap()
        .is_none());
    assert_eq!(
        memo.exhaustion_counts().get(&BudgetDimension::Group),
        Some(&1)
    );
    memo.create_optional_group(
        BudgetDimension::CompositionGroup,
        Fingerprint(3),
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    )
    .expect("local expansion must not consume the composition reserve");
    assert!(memo
        .create_optional_group(
            BudgetDimension::CompositionGroup,
            Fingerprint(4),
            schema(1),
            LogicalProperties::default(),
            GroupCardinality::default(),
        )
        .unwrap()
        .is_none());
    assert_eq!(
        memo.exhaustion_counts()
            .get(&BudgetDimension::CompositionGroup),
        Some(&1)
    );
}

#[test]
fn duplicate_optional_group_identity_is_an_invariant_error() {
    let mut memo = Memo::new(SearchBudget::default());
    memo.create_optional_group(
        BudgetDimension::Group,
        Fingerprint(41),
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    )
    .unwrap()
    .expect("first allocation");
    let error = memo
        .create_optional_group(
            BudgetDimension::Group,
            Fingerprint(41),
            schema(1),
            LogicalProperties::default(),
            GroupCardinality::default(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("allocation identity was reused"));
}

#[test]
fn optional_group_budget_scales_with_initial_memo_and_rollback_refunds_credit() {
    let mut budget = SearchBudget::default();
    budget.max_optional_groups_per_initial_group = 2;
    let mut memo = Memo::new(budget);
    for column in 0..3 {
        memo.create_group(
            schema(column),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
    }
    memo.seal_optional_group_budget();

    let savepoint = memo.transformation_savepoint();
    memo.create_optional_group(
        BudgetDimension::Group,
        Fingerprint(10),
        schema(10),
        LogicalProperties::default(),
        GroupCardinality::default(),
    )
    .unwrap();
    memo.rollback_transformation(savepoint).unwrap();

    for identity in 10..16 {
        memo.create_optional_group(
            BudgetDimension::Group,
            Fingerprint(identity),
            schema(identity as u32),
            LogicalProperties::default(),
            GroupCardinality::default(),
        )
        .unwrap();
    }
    assert!(memo
        .create_optional_group(
            BudgetDimension::Group,
            Fingerprint(16),
            schema(16),
            LogicalProperties::default(),
            GroupCardinality::default(),
        )
        .unwrap()
        .is_none());
}

#[test]
fn optional_group_rollback_preserves_exhaustion_evidence() {
    let mut budget = SearchBudget::default();
    budget.max_optional_groups_per_initial_group = 0;
    let mut memo = Memo::new(budget);
    memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    memo.seal_optional_group_budget();
    let savepoint = memo.transformation_savepoint();
    assert!(memo
        .create_optional_group(
            BudgetDimension::Group,
            Fingerprint(20),
            schema(2),
            LogicalProperties::default(),
            GroupCardinality::default(),
        )
        .unwrap()
        .is_none());
    memo.rollback_transformation(savepoint).unwrap();
    assert_eq!(
        memo.exhaustion_counts().get(&BudgetDimension::Group),
        Some(&1)
    );
}

#[test]
fn composition_rule_work_is_isolated_from_descendant_expansion() {
    let mut budget = SearchBudget::default();
    budget.max_rule_work_units_per_group = 1;
    budget.max_composition_rule_work_units_per_group = 1;
    let mut memo = Memo::new(budget);
    let group = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let ledger = &mut memo.group_mut(group).unwrap().ledger;
    assert_eq!(
        ledger.admit_optional(BudgetDimension::RuleWorkPerGroup, Fingerprint(1)),
        BudgetDecision::Allowed
    );
    assert_eq!(
        ledger.admit_optional(BudgetDimension::RuleWorkPerGroup, Fingerprint(2)),
        BudgetDecision::Exhausted
    );
    assert_eq!(
        ledger.admit_optional(BudgetDimension::CompositionRuleWorkPerGroup, Fingerprint(3)),
        BudgetDecision::Allowed
    );
    assert_eq!(
        ledger.admit_optional(BudgetDimension::CompositionRuleWorkPerGroup, Fingerprint(4)),
        BudgetDecision::Exhausted
    );
}

#[test]
fn composition_fire_and_output_budgets_are_isolated_from_local_rewrites() {
    let mut budget = SearchBudget::default();
    budget.max_rule_firings_per_group = 1;
    budget.max_composition_rule_firings_per_group = 1;
    budget.max_optional_logical_exprs_per_group = 1;
    budget.max_optional_composition_logical_exprs_per_group = 1;
    let mut ledger = SearchLedger::new(budget);

    assert_eq!(
        ledger.admit_optional(BudgetDimension::RuleFirePerGroup, Fingerprint(1)),
        BudgetDecision::Allowed
    );
    assert_eq!(
        ledger.admit_optional(BudgetDimension::RuleFirePerGroup, Fingerprint(2)),
        BudgetDecision::Exhausted
    );
    assert_eq!(
        ledger.admit_optional(BudgetDimension::CompositionRuleFirePerGroup, Fingerprint(3)),
        BudgetDecision::Allowed
    );
    assert_eq!(
        ledger.admit_optional(BudgetDimension::LogicalExprPerGroup, Fingerprint(4)),
        BudgetDecision::Allowed
    );
    assert_eq!(
        ledger.admit_optional(BudgetDimension::LogicalExprPerGroup, Fingerprint(5)),
        BudgetDecision::Exhausted
    );
    assert_eq!(
        ledger.admit_optional(
            BudgetDimension::CompositionLogicalExprPerGroup,
            Fingerprint(6)
        ),
        BudgetDecision::Allowed
    );
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
