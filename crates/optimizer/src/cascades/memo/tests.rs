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
            cost_composition: CostComposition::Sequential,
            cost,
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
            cost_composition: CostComposition::Sequential,
            cost,
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
                cost_composition: CostComposition::Sequential,
                cost,
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
                cost_composition: CostComposition::Sequential,
                cost,
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
fn canonical_cardinality_recipe_is_order_independent_and_authority_aware() {
    let statistics_a =
        GroupCardinality::new(Fingerprint(20), CardinalityAuthority::Statistics, 4, 9, 14);
    let statistics_b =
        GroupCardinality::new(Fingerprint(10), CardinalityAuthority::Statistics, 5, 5, 8);
    let forward = statistics_a.canonical_with(statistics_b);
    let reverse = statistics_b.canonical_with(statistics_a);
    assert_eq!(forward, reverse);
    assert_eq!(forward.representative(), Some((5, 5, 8)));

    let region = GroupCardinality::new(Fingerprint(30), CardinalityAuthority::JoinRegion, 5, 6, 7);
    assert_eq!(forward.canonical_with(region), region);
    assert_eq!(region.canonical_with(forward), region);

    let inherited = GroupCardinality::inherit(Fingerprint(40), GroupId::new(1));
    let mut refined = GroupCardinality::inherit(Fingerprint(50), GroupId::new(2));
    refined.authority = CardinalityAuthority::ConstraintRefined;
    assert_eq!(inherited.canonical_with(refined), refined);
    assert_eq!(refined.canonical_with(inherited), refined);
}

#[test]
fn inherited_cardinality_tracks_child_and_respects_group_hard_bound() {
    let mut memo = Memo::new(SearchBudget::default());
    let child = memo.create_group(
        schema(1),
        LogicalProperties::default(),
        GroupCardinality::new(
            Fingerprint(1),
            CardinalityAuthority::Statistics,
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
        GroupCardinality::new(Fingerprint(3), CardinalityAuthority::JoinRegion, 4, 5, 6);
    assert_eq!(memo.cardinality_estimate(parent), Some((4, 5, 6)));
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
                cost_composition: CostComposition::Sequential,
                cost: falsified,
                physical_fingerprint: Fingerprint(1),
                joint_cost_proof: None,
            },
        )
        .is_err());
}
