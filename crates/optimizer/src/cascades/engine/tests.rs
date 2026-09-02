// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cascades engine scheduling, transaction, and costing tests.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use paro_common::types::LogicalType;

use super::*;
use crate::cascades::column::{ColumnDesc, ColumnOrigin, ColumnVisibility, GroupSchema};
use crate::cascades::cost::{CompactRange, ScoreSummary};
use crate::cascades::ids::{
    AdmissibleGrantSetId, ColumnId, LogicalPayloadId, ObjectiveProfileId, OptimizationContextId,
    PhysicalPayloadId,
};
use crate::cascades::memo::{
    GrantGoalKey, GroupCardinality, LogicalExprKey, LogicalProperties, PhysicalExprKey, RowGoal,
};
use crate::cascades::properties::{
    MutationSafetyRequirement, OrderingRequirement, PartitioningRequirement,
    ProvidedMaterialization, ProvidedMutationSafety, ProvidedOrdering, ProvidedPartitioning,
    ProvidedReplayability, ProvidedRepresentation, ReplayabilityRequirement,
    RepresentationRequirement, RequiredProperties, ResultGuarantee,
};
use crate::cascades::rules::{
    EquivalentExpression, GrantDependencyDescriptor, PhysicalImplementation, RulePromise,
    TransformationRule,
};

fn schema() -> GroupSchema {
    GroupSchema::new([ColumnDesc {
        id: ColumnId(0),
        logical_type: LogicalType::BigInt,
        nullable: false,
        origin: ColumnOrigin::Derived {
            key: Fingerprint(1),
        },
        visibility: ColumnVisibility::Visible,
        name_hint: None,
    }])
    .unwrap()
}

fn required() -> RequiredProperties {
    RequiredProperties {
        ordering: OrderingRequirement::Any,
        partitioning: PartitioningRequirement::Any,
        materialization: Default::default(),
        mutation_safety: MutationSafetyRequirement::None,
        representation: RepresentationRequirement::Flat,
        replayability: ReplayabilityRequirement::Any,
        result_guarantee: ResultGuarantee::Exact,
    }
}

fn provided() -> super::super::properties::ProvidedProperties {
    super::super::properties::ProvidedProperties {
        ordering: ProvidedOrdering::Unordered,
        partitioning: ProvidedPartitioning::Singleton,
        materialization: ProvidedMaterialization::default(),
        mutation_safety: ProvidedMutationSafety::NotApplicable,
        representation: ProvidedRepresentation::Flat,
        replayability: ProvidedReplayability::OnePass,
        result_guarantee: ResultGuarantee::Exact,
    }
}

fn cost(score: f64) -> SearchCost {
    SearchCost {
        score: ScoreSummary {
            range: CompactRange::point(score).unwrap(),
            risk_adjusted: score,
        },
        critical_path: CompactRange::point(score).unwrap(),
        ..SearchCost::ZERO
    }
}

struct AddEquivalent;

impl TransformationRule for AddEquivalent {
    fn id(&self) -> RuleId {
        RuleId(5)
    }

    fn promise(&self, _: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> RulePromise {
        RulePromise::HIGH
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        Ok(vec![EquivalentExpression {
            target_group: ctx.group(),
            key: LogicalExprKey {
                operator: Fingerprint(11),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            payload: LogicalPayloadId(1),
            logical_properties: LogicalProperties::default(),
            cardinality: GroupCardinality::default(),
            proof: EquivalenceProof::Transformation {
                rule: self.id(),
                source: expr,
                premise: Fingerprint(77),
            },
        }]
        .into_boxed_slice())
    }
}

struct AddBoundedFrontier;

impl TransformationRule for AddBoundedFrontier {
    fn id(&self) -> RuleId {
        RuleId(9)
    }

    fn output_bound(&self, _: &RuleContext<'_>) -> usize {
        2
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        Ok([11, 12]
            .into_iter()
            .map(|operator| EquivalentExpression {
                target_group: ctx.group(),
                key: LogicalExprKey {
                    operator: Fingerprint(operator),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                payload: LogicalPayloadId(operator as u32),
                logical_properties: LogicalProperties::default(),
                cardinality: GroupCardinality::default(),
                proof: EquivalenceProof::Transformation {
                    rule: self.id(),
                    source: expr,
                    premise: Fingerprint(76),
                },
            })
            .collect())
    }
}

struct FailAfterMemoWrite;

impl TransformationRule for FailAfterMemoWrite {
    fn id(&self) -> RuleId {
        RuleId(6)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn apply(
        &self,
        _: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        ctx.memo_mut().create_group(
            schema(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        Err(paro_error::internal("injected optional-rule failure"))
    }
}

struct DuplicateEquivalent;

impl TransformationRule for DuplicateEquivalent {
    fn id(&self) -> RuleId {
        RuleId(8)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        Ok(vec![EquivalentExpression {
            target_group: ctx.group(),
            key: ctx.memo().logical_expr(expr).unwrap().key.clone(),
            payload: LogicalPayloadId(0),
            logical_properties: LogicalProperties::default(),
            cardinality: GroupCardinality::default(),
            proof: EquivalenceProof::Transformation {
                rule: self.id(),
                source: expr,
                premise: Fingerprint(79),
            },
        }]
        .into_boxed_slice())
    }
}

struct AddEquivalentWithNewChild;

impl TransformationRule for AddEquivalentWithNewChild {
    fn id(&self) -> RuleId {
        RuleId(20)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        let child = ctx.memo_mut().create_group(
            schema(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        ctx.memo_mut().insert_logical(
            child,
            LogicalExprKey {
                operator: Fingerprint(30),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(3),
            EquivalenceProof::Initial,
        )?;
        Ok(vec![EquivalentExpression {
            target_group: ctx.group(),
            key: LogicalExprKey {
                operator: Fingerprint(11),
                scalars: Box::new([]),
                children: Box::new([child]),
            },
            payload: LogicalPayloadId(4),
            logical_properties: LogicalProperties::default(),
            cardinality: GroupCardinality::default(),
            proof: EquivalenceProof::Transformation {
                rule: self.id(),
                source: expr,
                premise: Fingerprint(80),
            },
        }]
        .into_boxed_slice())
    }
}

struct RewriteNewChild;

impl TransformationRule for RewriteNewChild {
    fn id(&self) -> RuleId {
        RuleId(21)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        expr.key.operator == Fingerprint(30)
    }

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        Ok(vec![EquivalentExpression {
            target_group: ctx.group(),
            key: LogicalExprKey {
                operator: Fingerprint(31),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            payload: LogicalPayloadId(5),
            logical_properties: LogicalProperties::default(),
            cardinality: GroupCardinality::default(),
            proof: EquivalenceProof::Transformation {
                rule: self.id(),
                source: expr,
                premise: Fingerprint(81),
            },
        }]
        .into_boxed_slice())
    }
}

struct RejectAfterSidecarWrite {
    sidecar: Arc<AtomicUsize>,
}

impl TransformationRule for RejectAfterSidecarWrite {
    fn id(&self) -> RuleId {
        RuleId(7)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        let target = ctx.memo_mut().create_group(
            schema(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        self.sidecar.fetch_add(1, Ordering::SeqCst);
        let sidecar = self.sidecar.clone();
        ctx.enlist_rollback(move || {
            sidecar.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        });
        Ok(vec![EquivalentExpression {
            target_group: target,
            key: LogicalExprKey {
                operator: Fingerprint(12),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            payload: LogicalPayloadId(2),
            logical_properties: LogicalProperties::default(),
            cardinality: GroupCardinality::default(),
            proof: EquivalenceProof::Transformation {
                rule: self.id(),
                source: expr,
                premise: Fingerprint(78),
            },
        }]
        .into_boxed_slice())
    }
}

struct LeafImplementation;

impl PhysicalImplementation for LeafImplementation {
    fn id(&self) -> ImplementationId {
        ImplementationId(3)
    }

    fn matches(
        &self,
        _: &super::super::memo::LogicalExpr,
        _: OptimizationGoal,
        _: &ImplementationContext<'_>,
    ) -> bool {
        true
    }

    fn candidates(
        &self,
        expr: LogicalExprId,
        _: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>> {
        let logical = ctx.memo.logical_expr(expr).unwrap();
        let score = if logical.key.operator == Fingerprint(11) {
            1.0
        } else {
            5.0
        };
        Ok(vec![PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id(),
                logical: expr,
                children: Box::new([]),
                payload_fingerprint: logical.key.operator,
            },
            payload: PhysicalPayloadId(expr.0),
            provided: provided(),
            child_goals: Box::new([]),
            local_cost: cost(score),
            cost_composition: CostComposition::Sequential,
            spillable: false,
            enforcer_cost_input: EnforcerCostInput::unbounded(CompactRange::point(1.0)?, 8),
            physical_fingerprint: logical.key.operator,
            region: None,
            mandatory: logical.key.operator == Fingerprint(10),
        }]
        .into_boxed_slice())
    }
}

fn engine(optional_rules: u32) -> (CascadesEngine, GroupId, OptimizationGoal) {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.max_rule_firings_per_group = optional_rules;
    engine_with_budget(budget)
}

fn engine_with_budget(
    budget: super::super::budget::SearchBudget,
) -> (CascadesEngine, GroupId, OptimizationGoal) {
    let mut memo = Memo::new(budget);
    let group = memo.create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    memo.insert_logical(
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
    let required = memo.intern_required(required()).unwrap();
    let goal = OptimizationGoal {
        required,
        row_goal: RowGoal::All,
        objective: ObjectiveProfileId(0),
        grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
        context: OptimizationContextId(0),
    };
    let mut registry = ImplementationRegistry::default();
    registry.register_transformation(AddEquivalent).unwrap();
    registry
        .register_implementation(LeafImplementation)
        .unwrap();
    (CascadesEngine::new(memo, registry), group, goal)
}

#[test]
fn optional_transformation_can_improve_mandatory_baseline() {
    let (mut engine, group, goal) = engine(8);
    let winner = engine.optimize(group, goal, SearchMode::Memo).unwrap();
    assert!(winner.cost.score.risk_adjusted < 5.0);
    assert_eq!(winner.physical_fingerprint, Fingerprint(11));
}

#[test]
fn bounded_region_rule_reserves_and_publishes_its_complete_frontier() {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.disable_transformation(RuleId(5));
    budget.max_optional_logical_exprs_per_group = 2;
    let (mut engine, group, goal) = engine_with_budget(budget);
    engine
        .registry
        .register_transformation(AddBoundedFrontier)
        .unwrap();

    let winner = engine.optimize(group, goal, SearchMode::Memo).unwrap();

    assert_eq!(winner.physical_fingerprint, Fingerprint(11));
    assert_eq!(engine.memo.group(group).unwrap().logical_exprs().len(), 3);
    assert_eq!(engine.effective_rule_insertions().get(&RuleId(9)), Some(&2));
    assert_eq!(
        engine
            .memo
            .group(group)
            .unwrap()
            .ledger
            .consumed(BudgetDimension::LogicalExprPerGroup),
        2
    );
}

#[test]
fn disabled_transformation_keeps_the_mandatory_baseline() {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.disable_transformation(RuleId(5));
    let (mut engine, group, goal) = engine_with_budget(budget);
    let winner = engine.optimize(group, goal, SearchMode::Memo).unwrap();
    assert_eq!(winner.physical_fingerprint, Fingerprint(10));
}

#[test]
fn exhausted_optional_budget_still_extracts_baseline() {
    let (mut engine, group, goal) = engine(0);
    let winner = engine.optimize(group, goal, SearchMode::Memo).unwrap();
    assert_eq!(winner.cost.score.risk_adjusted, 5.0);
    assert_eq!(winner.physical_fingerprint, Fingerprint(10));
}

#[test]
fn failed_optional_transformation_rolls_back_and_keeps_baseline() {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.disable_transformation(RuleId(5));
    let (mut engine, group, goal) = engine_with_budget(budget);
    engine
        .registry
        .register_transformation(FailAfterMemoWrite)
        .unwrap();
    let groups_before = engine.memo.group_count();

    let winner = engine.optimize(group, goal, SearchMode::Memo).unwrap();

    assert_eq!(winner.physical_fingerprint, Fingerprint(10));
    assert_eq!(engine.memo.group_count(), groups_before);
    assert_eq!(
        engine
            .memo
            .group(group)
            .unwrap()
            .ledger
            .consumed(BudgetDimension::LogicalExprPerGroup),
        0
    );
}

#[test]
fn engine_rejection_rolls_back_memo_and_enlisted_side_state() {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.disable_transformation(RuleId(5));
    let (mut engine, group, goal) = engine_with_budget(budget);
    let sidecar = Arc::new(AtomicUsize::new(0));
    engine
        .registry
        .register_transformation(RejectAfterSidecarWrite {
            sidecar: sidecar.clone(),
        })
        .unwrap();
    let groups_before = engine.memo.group_count();

    let winner = engine.optimize(group, goal, SearchMode::Memo).unwrap();

    assert_eq!(winner.physical_fingerprint, Fingerprint(10));
    assert_eq!(engine.memo.group_count(), groups_before);
    assert_eq!(sidecar.load(Ordering::SeqCst), 0);
    assert_eq!(
        engine
            .memo
            .group(group)
            .unwrap()
            .ledger
            .consumed(BudgetDimension::LogicalExprPerGroup),
        0
    );
}

#[test]
fn duplicate_transformation_does_not_mutate_existing_proofs() {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.disable_transformation(RuleId(5));
    let (mut engine, group, goal) = engine_with_budget(budget);
    engine
        .registry
        .register_transformation(DuplicateEquivalent)
        .unwrap();
    let expression = engine.memo.group(group).unwrap().logical_exprs()[0];
    let proofs_before = engine.memo.logical_expr(expression).unwrap().proofs.clone();

    let winner = engine.optimize(group, goal, SearchMode::Memo).unwrap();

    assert_eq!(winner.physical_fingerprint, Fingerprint(10));
    assert_eq!(
        engine.memo.logical_expr(expression).unwrap().proofs,
        proofs_before
    );
    assert_eq!(engine.rule_attempts().get(&RuleId(8)), Some(&1));
    assert!(!engine.effective_rule_insertions().contains_key(&RuleId(8)));
}

#[test]
fn committed_child_groups_are_scheduled_for_exploration() {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.disable_transformation(RuleId(5));
    let (mut engine, _, _) = engine_with_budget(budget);
    engine
        .registry
        .register_transformation(AddEquivalentWithNewChild)
        .unwrap();
    engine
        .registry
        .register_transformation(RewriteNewChild)
        .unwrap();

    engine.explore_transformations().unwrap();

    assert_eq!(
        engine.effective_rule_insertions().get(&RuleId(20)),
        Some(&1),
        "effective insertions: {:?}",
        engine.effective_rule_insertions()
    );
    assert_eq!(
        engine.effective_rule_insertions().get(&RuleId(21)),
        Some(&1)
    );
    assert_eq!(engine.rule_attempts().get(&RuleId(20)), Some(&1));
    assert_eq!(engine.rule_attempts().get(&RuleId(21)), Some(&1));
}

#[test]
fn direct_and_memo_share_implementation_registry() {
    let (mut direct, group, goal) = engine(8);
    let direct_winner = direct.optimize(group, goal, SearchMode::Direct).unwrap();
    assert_eq!(direct_winner.physical_fingerprint, Fingerprint(10));

    let (mut memo, group, goal) = engine(8);
    let memo_winner = memo.optimize(group, goal, SearchMode::Memo).unwrap();
    assert_eq!(memo_winner.physical_fingerprint, Fingerprint(11));
}

struct ReplaceInfeasibleBranch;

impl TransformationRule for ReplaceInfeasibleBranch {
    fn id(&self) -> RuleId {
        RuleId(13)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        expr.key.operator == Fingerprint(30)
    }

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        Ok(vec![EquivalentExpression {
            target_group: ctx.group(),
            key: LogicalExprKey {
                operator: Fingerprint(31),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            payload: LogicalPayloadId(2),
            logical_properties: LogicalProperties::default(),
            cardinality: GroupCardinality::default(),
            proof: EquivalenceProof::Transformation {
                rule: self.id(),
                source: expr,
                premise: Fingerprint(31),
            },
        }]
        .into_boxed_slice())
    }
}

struct FeasibleAlternativeImplementation;

impl PhysicalImplementation for FeasibleAlternativeImplementation {
    fn id(&self) -> ImplementationId {
        ImplementationId(14)
    }

    fn matches(
        &self,
        expr: &super::super::memo::LogicalExpr,
        _: OptimizationGoal,
        _: &ImplementationContext<'_>,
    ) -> bool {
        matches!(expr.key.operator, Fingerprint(30) | Fingerprint(31))
    }

    fn candidates(
        &self,
        expr: LogicalExprId,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>> {
        let logical = ctx.memo.logical_expr(expr).unwrap();
        let children = logical.key.children.clone();
        let child_goals = children
            .iter()
            .copied()
            .map(|child| (child, goal))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(vec![PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id(),
                logical: expr,
                children,
                payload_fingerprint: logical.key.operator,
            },
            payload: PhysicalPayloadId(logical.payload.0),
            provided: provided(),
            child_goals,
            local_cost: cost(1.0),
            cost_composition: CostComposition::Sequential,
            spillable: false,
            enforcer_cost_input: EnforcerCostInput::unbounded(CompactRange::point(1.0)?, 8),
            physical_fingerprint: logical.key.operator,
            region: None,
            mandatory: logical.key.operator == Fingerprint(30),
        }]
        .into_boxed_slice())
    }
}

#[test]
fn infeasible_child_rejects_only_its_parent_recipe() {
    let mut memo = Memo::new(super::super::budget::SearchBudget::default());
    let child = memo.create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    memo.insert_logical(
        child,
        LogicalExprKey {
            operator: Fingerprint(32),
            scalars: Box::new([]),
            children: Box::new([]),
        },
        LogicalPayloadId(0),
        EquivalenceProof::Initial,
    )
    .unwrap();
    let root = memo.create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    memo.insert_logical(
        root,
        LogicalExprKey {
            operator: Fingerprint(30),
            scalars: Box::new([]),
            children: Box::new([child]),
        },
        LogicalPayloadId(1),
        EquivalenceProof::Initial,
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
    let mut registry = ImplementationRegistry::default();
    registry
        .register_transformation(ReplaceInfeasibleBranch)
        .unwrap();
    registry
        .register_implementation(FeasibleAlternativeImplementation)
        .unwrap();
    let mut engine = CascadesEngine::new(memo, registry);

    let winner = engine.optimize(root, goal, SearchMode::Memo).unwrap();

    assert_eq!(winner.physical_fingerprint, Fingerprint(31));
}

#[test]
fn blocking_enforcers_participate_in_grant_feasibility() {
    let rows = CompactRange::point(1_000.0).unwrap();
    let too_small = EnforcerCostInput {
        rows,
        row_width_bytes: 16,
        hard_memory_bytes: 1_024,
        spill_policy: SpillPolicy::Forbidden,
        max_parallel_tasks: 1,
    };
    assert!(enforcer_cost(
        &[EnforcerStep::MutationInputSpool {
            barrier: super::super::ids::MutationBarrierId(0),
        }],
        too_small,
        &MachineCalibrationBundle::default(),
    )
    .unwrap()
    .is_none());

    let spillable = EnforcerCostInput {
        spill_policy: SpillPolicy::Allowed,
        ..too_small
    };
    let ordering = super::super::properties::RequiredOrdering {
        keys: vec![super::super::properties::OrderingKey {
            column: ColumnId(0),
            direction: super::super::properties::SortDirection::Asc,
            nulls: super::super::properties::NullOrder::Last,
            collation: None,
        }]
        .into_boxed_slice(),
        scope: super::super::properties::OrderingScope::Global,
    };
    let cost = enforcer_cost(
        &[EnforcerStep::Sort(ordering)],
        spillable,
        &MachineCalibrationBundle::default(),
    )
    .unwrap()
    .expect("sort may spill under an allowed grant");
    assert_eq!(cost.peak_memory_upper, 1_024);
    assert!(cost.spill_bytes_expected > 0);
    assert!(
        cost.resources_expected[super::super::cost::ResourceDimension::SequentialIo as usize] > 0.0
    );
}

#[test]
fn retained_operator_state_overlaps_child_pipeline_memory() {
    let local = SearchCost {
        non_revocable_memory_upper: 100,
        minimum_memory_bytes: 100,
        peak_memory_upper: 100,
        ..cost(1.0)
    };
    let child = SearchCost {
        non_revocable_memory_upper: 40,
        minimum_memory_bytes: 40,
        peak_memory_upper: 40,
        ..cost(1.0)
    };
    let sequential = compose_candidate_cost(local, &[child], CostComposition::Sequential)
        .expect("sequential composition");
    let retained = compose_candidate_cost(
        local,
        &[child],
        CostComposition::RetainedState {
            overlapping_children: 1,
        },
    )
    .expect("retained-state composition");
    assert_eq!(sequential.peak_memory_upper, 100);
    assert_eq!(retained.peak_memory_upper, 140);
}

#[test]
fn schema_only_child_does_not_contribute_execution_cost() {
    let local = cost(1.0);
    let child = SearchCost {
        minimum_memory_bytes: 40,
        peak_memory_upper: 80,
        ..cost(100.0)
    };
    let composed = compose_candidate_cost(local, &[child], CostComposition::LocalOnly)
        .expect("schema-only composition");

    assert_eq!(composed, local);
}

#[test]
fn sideways_filter_scales_work_without_weakening_resource_proofs() {
    let child = SearchCost {
        non_revocable_memory_upper: 40,
        minimum_memory_bytes: 40,
        peak_memory_upper: 80,
        ..cost(100.0)
    };
    let filtered = compose_candidate_cost(
        SearchCost::ZERO,
        &[child],
        CostComposition::SidewaysFilter {
            overlapping_children: 1,
            filtered_child: 0,
            expected_retained_ppm: 100_000,
            upper_retained_ppm: 1_000_000,
        },
    )
    .expect("sideways-filter composition");

    assert_eq!(filtered.score.range.expected, 10.0);
    assert_eq!(filtered.score.range.upper, 100.0);
    assert_eq!(filtered.score.risk_adjusted, 55.0);
    assert_eq!(filtered.non_revocable_memory_upper, 40);
    assert_eq!(filtered.minimum_memory_bytes, 40);
    assert_eq!(filtered.peak_memory_upper, 80);
}

#[test]
fn revocable_retained_state_shares_one_query_pool() {
    let local = SearchCost {
        peak_memory_upper: 100,
        ..cost(1.0)
    };
    let child = SearchCost {
        peak_memory_upper: 40,
        ..cost(1.0)
    };
    let retained = compose_candidate_cost(
        local,
        &[child],
        CostComposition::RetainedState {
            overlapping_children: 1,
        },
    )
    .expect("revocable retained-state composition");
    assert_eq!(retained.non_revocable_memory_upper, 0);
    assert_eq!(retained.peak_memory_upper, 100);
}

#[test]
fn mandatory_unknown_nonspill_state_is_not_a_hard_memory_proof() {
    let local = SearchCost {
        peak_memory_upper: u64::MAX,
        ..cost(1.0)
    };
    let child = SearchCost {
        peak_memory_upper: 40,
        ..cost(1.0)
    };
    let grant = EnforcerCostInput {
        rows: CompactRange::point(1.0).unwrap(),
        row_width_bytes: 8,
        hard_memory_bytes: 100,
        spill_policy: SpillPolicy::Forbidden,
        max_parallel_tasks: 1,
    };

    let fitted = fit_local_retained_state_to_grant(
        local,
        &[child],
        CostComposition::RetainedState {
            overlapping_children: 1,
        },
        false,
        grant,
    )
    .unwrap();
    assert!(fitted.is_none());
}

#[test]
fn spill_capability_does_not_imply_spill_permission() {
    let local = SearchCost {
        minimum_memory_bytes: 64,
        peak_memory_upper: u64::MAX,
        ..cost(1.0)
    };
    let grant = EnforcerCostInput {
        rows: CompactRange::point(1.0).unwrap(),
        row_width_bytes: 8,
        hard_memory_bytes: 100,
        spill_policy: SpillPolicy::Forbidden,
        max_parallel_tasks: 1,
    };

    let fitted =
        fit_local_retained_state_to_grant(local, &[], CostComposition::Sequential, true, grant)
            .unwrap();
    assert!(fitted.is_none());
}

#[test]
fn mandatory_nonspill_parent_can_coexist_with_a_revocable_child() {
    let local = SearchCost {
        non_revocable_memory_upper: 120,
        minimum_memory_bytes: 120,
        peak_memory_upper: 120,
        ..cost(1.0)
    };
    let child = SearchCost {
        peak_memory_upper: 1_024,
        ..cost(1.0)
    };
    let grant = EnforcerCostInput {
        rows: CompactRange::point(1.0).unwrap(),
        row_width_bytes: 8,
        hard_memory_bytes: 1_024,
        spill_policy: SpillPolicy::Forbidden,
        max_parallel_tasks: 1,
    };

    let fitted = fit_local_retained_state_to_grant(
        local,
        &[child],
        CostComposition::RetainedState {
            overlapping_children: 1,
        },
        false,
        grant,
    )
    .unwrap();
    assert!(fitted.is_some());
}

#[test]
fn overlapping_non_revocable_state_must_fit_the_grant() {
    let local = SearchCost {
        non_revocable_memory_upper: 120,
        minimum_memory_bytes: 120,
        peak_memory_upper: 120,
        ..cost(1.0)
    };
    let child = SearchCost {
        non_revocable_memory_upper: 1_024,
        minimum_memory_bytes: 1_024,
        peak_memory_upper: 1_024,
        ..cost(1.0)
    };
    let grant = EnforcerCostInput {
        rows: CompactRange::point(1.0).unwrap(),
        row_width_bytes: 8,
        hard_memory_bytes: 1_024,
        spill_policy: SpillPolicy::Allowed,
        max_parallel_tasks: 1,
    };

    let fitted = fit_local_retained_state_to_grant(
        local,
        &[child],
        CostComposition::RetainedState {
            overlapping_children: 1,
        },
        false,
        grant,
    )
    .unwrap();
    assert!(fitted.is_none());
}

struct GrantTreeImplementation;

impl PhysicalImplementation for GrantTreeImplementation {
    fn id(&self) -> ImplementationId {
        ImplementationId(12)
    }

    fn grant_dependency_for(
        &self,
        expr: &super::super::memo::LogicalExpr,
        _: &ImplementationContext<'_>,
    ) -> GrantDependencyDescriptor {
        if expr.key.operator == Fingerprint(20) {
            GrantDependencyDescriptor::Sensitive
        } else {
            GrantDependencyDescriptor::Invariant
        }
    }

    fn matches(
        &self,
        _: &super::super::memo::LogicalExpr,
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
        let logical = ctx.memo.logical_expr(expr).unwrap();
        let children = logical.key.children.clone();
        let child_goals = children
            .iter()
            .copied()
            .map(|child| (child, goal))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(vec![PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id(),
                logical: expr,
                children,
                payload_fingerprint: logical.key.operator,
            },
            payload: PhysicalPayloadId(expr.0),
            provided: provided(),
            child_goals,
            local_cost: cost(1.0),
            cost_composition: CostComposition::Sequential,
            spillable: false,
            enforcer_cost_input: EnforcerCostInput::unbounded(CompactRange::point(1.0)?, 8),
            physical_fingerprint: logical.key.operator,
            region: None,
            mandatory: true,
        }]
        .into_boxed_slice())
    }
}

#[test]
fn grant_sensitive_parent_reuses_invariant_child_goal_across_classes() {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.max_grant_classes = 2;
    let mut memo = Memo::new(budget);
    let child = memo.create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    memo.insert_logical(
        child,
        LogicalExprKey {
            operator: Fingerprint(21),
            scalars: Box::new([]),
            children: Box::new([]),
        },
        LogicalPayloadId(0),
        EquivalenceProof::Initial,
    )
    .unwrap();
    let root = memo.create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    memo.insert_logical(
        root,
        LogicalExprKey {
            operator: Fingerprint(20),
            scalars: Box::new([]),
            children: vec![child].into_boxed_slice(),
        },
        LogicalPayloadId(1),
        EquivalenceProof::Initial,
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
    let mut registry = ImplementationRegistry::default();
    registry
        .register_implementation(GrantTreeImplementation)
        .unwrap();
    let mut engine = CascadesEngine::new(memo, registry);
    let optimized = engine
        .optimize_for_grants(
            root,
            goal,
            AdmissibleGrantSetId(9),
            [ResourceGrantClassId(1), ResourceGrantClassId(2)],
            SearchMode::Direct,
        )
        .unwrap();
    assert!(optimized.sensitivity.is_sensitive());
    let child_goals = engine
        .memo()
        .group(child)
        .unwrap()
        .winners()
        .map(|(goal, _)| goal.grant)
        .collect::<Vec<_>>();
    assert_eq!(
        child_goals,
        vec![GrantGoalKey::Invariant(AdmissibleGrantSetId(9))]
    );
    let root_goals = engine
        .memo()
        .group(root)
        .unwrap()
        .winners()
        .map(|(goal, _)| goal.grant)
        .collect::<Vec<_>>();
    assert_eq!(
        root_goals,
        vec![
            GrantGoalKey::Class(ResourceGrantClassId(1)),
            GrantGoalKey::Class(ResourceGrantClassId(2)),
        ]
    );
}
