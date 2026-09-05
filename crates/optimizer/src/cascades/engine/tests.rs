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
    AdmissibleGrantSetId, ColumnId, LogicalPayloadId, OptimizationContextId, PhysicalPayloadId,
};
use crate::cascades::memo::{
    GrantGoalKey, GroupCardinality, LogicalExprKey, LogicalProperties, OptimizationContext,
    PhysicalExprKey, RowGoal,
};
use crate::cascades::properties::{
    MutationSafetyRequirement, OrderingRequirement, PartitioningRequirement,
    ProvidedMaterialization, ProvidedMutationSafety, ProvidedOrdering, ProvidedPartitioning,
    ProvidedReplayability, ProvidedRepresentation, ReplayabilityRequirement,
    RepresentationRequirement, RequiredProperties, ResultGuarantee,
};
use crate::cascades::region::RegionArtifactDependencyContract;
use crate::cascades::rules::{
    DomainProofId, EquivalentExpression, EvaluationOccurrenceId, GrantDependencyDescriptor,
    PhysicalImplementation, RulePromise, SidewaysFilterSource, TransformationRule,
};
use crate::physical::ObjectiveProfile;

#[test]
fn streaming_task_supply_is_inherited_from_the_child_pipeline() {
    let calibration = MachineCalibrationBundle::default();
    let mut child = cost(10.0);
    child.max_parallel_tasks = 1;
    child.output_pipeline_tasks = 1;
    let local = cost(100.0);
    let resolved = resolve_task_supply(
        local,
        &[child],
        &TaskSupplyContract::Streaming { input: 0 },
        &calibration,
    )
    .unwrap();
    assert_eq!(resolved.max_parallel_tasks, 1);
    assert_eq!(resolved.output_pipeline_tasks, 1);
    assert_eq!(resolved.work_latency.expected, 100.0);
}

#[test]
fn build_supply_does_not_manufacture_probe_parallelism() {
    let calibration = MachineCalibrationBundle::default();
    let mut build = cost(10.0);
    build.max_parallel_tasks = 8;
    build.output_pipeline_tasks = 8;
    let mut probe = cost(10.0);
    probe.max_parallel_tasks = 1;
    probe.output_pipeline_tasks = 1;
    let resolved = resolve_task_supply(
        cost(100.0),
        &[probe, build],
        &TaskSupplyContract::BuildProbe {
            build: 1,
            probe: 0,
            build_work_ppm: 750_000,
        },
        &calibration,
    )
    .unwrap();
    assert_eq!(resolved.max_parallel_tasks, 8);
    assert_eq!(resolved.output_pipeline_tasks, 1);
    assert_eq!(resolved.work_latency.expected, 100.0);
}

#[test]
fn breaker_creates_only_its_declared_emit_supply() {
    let calibration = MachineCalibrationBundle::default();
    let mut input = cost(10.0);
    input.max_parallel_tasks = 8;
    input.output_pipeline_tasks = 8;
    let resolved = resolve_task_supply(
        cost(100.0),
        &[input],
        &TaskSupplyContract::Breaker {
            input: 0,
            output_tasks: 2,
            profile: ParallelWorkProfile::BlockingMerge,
        },
        &calibration,
    )
    .unwrap();
    assert_eq!(resolved.max_parallel_tasks, 8);
    assert_eq!(resolved.output_pipeline_tasks, 2);
    assert_eq!(resolved.work_latency.expected, 100.0);
}

#[test]
fn task_supply_operating_points_conserve_work() {
    let calibration = MachineCalibrationBundle::default();
    for tasks in [1, 2, 4, 8] {
        let resolved = resolve_task_supply(
            cost(100.0),
            &[],
            &TaskSupplyContract::Source { tasks },
            &calibration,
        )
        .unwrap();
        assert_eq!(resolved.max_parallel_tasks, tasks);
        assert_eq!(resolved.output_pipeline_tasks, tasks);
        assert_eq!(resolved.work_latency.expected, 100.0);
    }
}

#[test]
fn source_filter_replacement_folds_each_source_phase_once() {
    let calibration = MachineCalibrationBundle::default();
    let source = WorkSourceId(91);
    for tasks in [1, 2, 4, 8] {
        let scan = resolve_task_supply(
            cost(100.0),
            &[],
            &TaskSupplyContract::Source { tasks },
            &calibration,
        )
        .unwrap();
        let scan = compose_candidate_cost_with_sources_at(
            scan,
            None,
            &[],
            &[],
            CostComposition::Source {
                source,
                source_rows: 100,
            },
            &calibration,
        )
        .unwrap();
        let filtered = compose_candidate_cost_with_sources_at(
            cost(20.0),
            Some(cost(20.0)),
            &[scan.cost],
            &[scan.source_work.as_ref()],
            CostComposition::SidewaysFilter {
                overlapping_children: 0,
                filtered_child: 0,
                sources: Box::new([retained_source(source, 500_000, 500_000)]),
            },
            &calibration,
        )
        .unwrap();
        let lane = &filtered.source_work[0];
        assert_eq!(lane.phase_tasks, tasks);
        assert_eq!(lane.cost.work_latency.expected, 50.0);
        assert_eq!(lane.filter_apply_cost.work_latency.expected, 20.0);
        assert_eq!(lane.phased_cost.work_latency.expected, 70.0);
        let expected = calibration
            .rephase(
                lane.cost.sequential(lane.filter_apply_cost).unwrap(),
                ParallelWorkProfile::Pipeline,
                tasks,
                tasks,
            )
            .unwrap();
        assert_eq!(lane.phased_cost.critical_path, expected.critical_path);
    }
}

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
        work_latency: CompactRange::point(score).unwrap(),
        critical_path: CompactRange::point(score).unwrap(),
        ..SearchCost::ZERO
    }
}

fn retained_source(
    source: WorkSourceId,
    expected_retained_ppm: u32,
    upper_retained_ppm: u32,
) -> SidewaysFilterSource {
    SidewaysFilterSource {
        source,
        domain: DomainProofId(Fingerprint(
            ((source.0 as u128) << 64)
                | ((u128::from(expected_retained_ppm)) << 32)
                | u128::from(upper_retained_ppm),
        )),
        evaluation: EvaluationOccurrenceId(Fingerprint(
            ((source.0 as u128) << 64)
                | ((u128::from(expected_retained_ppm)) << 32)
                | u128::from(upper_retained_ppm),
        )),
        expected_retained_ppm,
        upper_retained_ppm,
    }
}

#[test]
fn joint_cost_proof_resolves_both_runtime_filter_build_orientations() {
    let goal = OptimizationGoal {
        required: super::super::ids::PropertySetId(0),
        row_goal: RowGoal::All,
        objective: ObjectiveProfile::Latency,
        grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
        context: OptimizationContextId(0),
    };
    let mut memo = Memo::new(super::super::budget::SearchBudget::default());
    let groups = (0..=12)
        .map(|_| {
            memo.create_group(
                schema(),
                LogicalProperties::default(),
                GroupCardinality::default(),
            )
        })
        .collect::<Vec<_>>();
    let owner = groups[12];
    let first = groups[10];
    let second = groups[11];
    let canonical_owner = memo.merge_groups(owner, groups[0]).unwrap();
    let canonical_first = memo.merge_groups(first, groups[1]).unwrap();
    let canonical_second = memo.merge_groups(second, groups[2]).unwrap();
    let facet = Fingerprint(90);

    for (producer, consumer, expected_producer, expected_consumer) in [
        (
            RegionBoundaryEndpoint::Input(1),
            RegionBoundaryEndpoint::Input(0),
            canonical_second,
            canonical_first,
        ),
        (
            RegionBoundaryEndpoint::Input(0),
            RegionBoundaryEndpoint::Input(1),
            canonical_first,
            canonical_second,
        ),
    ] {
        let recipe = CostRecipe {
            child_goals: Box::new([(first, goal), (second, goal)]),
            local_cost: SearchCost::ZERO,
            source_filter_apply_cost: None,
            task_supply: TaskSupplyContract::Serial,
            cost_composition: CostComposition::Sequential,
            spillable: false,
            enforcer_cost_input: EnforcerCostInput::unbounded(CompactRange::point(1.0).unwrap(), 8),
            physical_fingerprint: Fingerprint(91),
            region: Some(RegionCandidateContract {
                region: super::super::ids::RegionId::new(0),
                facets: Box::new([facet]),
                artifacts: Box::new([super::super::region::RegionOwnedArtifact {
                    fingerprint: facet,
                    kind: RegionArtifactKind::RuntimeFilter,
                }]),
                artifact_dependencies: Box::new([RegionArtifactDependencyContract {
                    artifact: facet,
                    producer,
                    consumer,
                    kind: RegionDependencyKind::ControlWaitComplete,
                }]),
            }),
        };

        let proof = build_joint_cost_proof(&memo, owner, &recipe, SearchCost::ZERO)
            .unwrap()
            .expect("region recipe must produce a proof");
        assert_eq!(proof.owner_group, canonical_owner);
        assert_eq!(proof.boundary_goals[0].0, canonical_first);
        assert_eq!(proof.boundary_goals[1].0, canonical_second);
        let control = proof
            .dependencies
            .iter()
            .find(|dependency| dependency.kind == RegionDependencyKind::ControlWaitComplete)
            .expect("runtime-filter proof must carry a control edge");
        assert_eq!(control.producer, expected_producer);
        assert_eq!(control.consumer, expected_consumer);
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

    fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
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

struct AttemptSearchTimeContextExpansion;

impl TransformationRule for AttemptSearchTimeContextExpansion {
    fn id(&self) -> RuleId {
        RuleId(21)
    }

    fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
    }

    fn apply(
        &self,
        _: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        ctx.memo_mut()
            .intern_optimization_context(OptimizationContext::new([Fingerprint(999)]))?;
        unreachable!("search-time context interning must be rejected")
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

    fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
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

    fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
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

    fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
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

    fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
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

    fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
        expr.key.operator == Fingerprint(30)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
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

struct AddChildAlternative {
    id: RuleId,
}

impl TransformationRule for AddChildAlternative {
    fn id(&self) -> RuleId {
        self.id
    }

    fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
        expr.key.operator == Fingerprint(40)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
    }

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        Ok(vec![EquivalentExpression {
            target_group: ctx.group(),
            key: LogicalExprKey {
                operator: Fingerprint(41),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            payload: LogicalPayloadId(41),
            logical_properties: LogicalProperties::default(),
            cardinality: GroupCardinality::default(),
            proof: EquivalenceProof::Transformation {
                rule: self.id(),
                source: expr,
                premise: Fingerprint(401),
            },
        }]
        .into_boxed_slice())
    }
}

struct RewriteParentAfterChildAlternative {
    id: RuleId,
}

impl TransformationRule for RewriteParentAfterChildAlternative {
    fn id(&self) -> RuleId {
        self.id
    }

    fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
        expr.key.operator == Fingerprint(50)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, ctx: &RuleContext<'_>) -> bool {
        if !self.matches_root(expr) {
            return false;
        }
        let [child] = expr.key.children.as_ref() else {
            return false;
        };
        ctx.memo.group(*child).is_some_and(|group| {
            group.logical_exprs().iter().any(|expression| {
                ctx.memo
                    .logical_expr(*expression)
                    .is_some_and(|expression| expression.key.operator == Fingerprint(41))
            })
        })
    }

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        let source = ctx.memo().logical_expr(expr).unwrap();
        Ok(vec![EquivalentExpression {
            target_group: ctx.group(),
            key: LogicalExprKey {
                operator: Fingerprint(51),
                scalars: Box::new([]),
                children: source.key.children.clone(),
            },
            payload: LogicalPayloadId(51),
            logical_properties: LogicalProperties::default(),
            cardinality: GroupCardinality::default(),
            proof: EquivalenceProof::Transformation {
                rule: self.id(),
                source: expr,
                premise: Fingerprint(501),
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

    fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
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
            source_filter_apply_cost: None,
            task_supply: TaskSupplyContract::Serial,
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
        objective: ObjectiveProfile::Latency,
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

fn transformation_chain_engine(
    depth: usize,
    root_operator: Fingerprint,
    budget: super::super::budget::SearchBudget,
) -> CascadesEngine {
    assert!(depth > 0);
    let mut memo = Memo::new(budget);
    let mut child = None;
    for ordinal in 0..depth {
        let group = memo.create_group(
            schema(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        memo.insert_logical(
            group,
            LogicalExprKey {
                operator: if ordinal + 1 == depth {
                    root_operator
                } else {
                    Fingerprint(40)
                },
                scalars: Box::new([]),
                children: child.into_iter().collect(),
            },
            LogicalPayloadId(ordinal as u32),
            EquivalenceProof::Initial,
        )
        .unwrap();
        child = Some(group);
    }
    let mut registry = ImplementationRegistry::default();
    registry.register_transformation(AddEquivalent).unwrap();
    CascadesEngine::new(memo, registry)
}

#[test]
fn optional_transformation_can_improve_mandatory_baseline() {
    let (mut engine, group, goal) = engine(8);
    let winner = engine.optimize(group, goal, SearchMode::Memo).unwrap();
    assert!(winner.cost.score.risk_adjusted < 5.0);
    assert_eq!(winner.physical_fingerprint, Fingerprint(11));
}

#[test]
fn engine_seals_context_catalog_before_optional_search() {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.disable_transformation(RuleId(5));
    let (mut engine, group, goal) = engine_with_budget(budget);
    engine
        .registry
        .register_transformation(AttemptSearchTimeContextExpansion)
        .unwrap();

    let winner = engine.optimize(group, goal, SearchMode::Memo).unwrap();

    assert_eq!(winner.physical_fingerprint, Fingerprint(10));
    assert!(engine
        .memo()
        .optimization_context(OptimizationContextId(1))
        .is_none());
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
fn root_dispatch_does_not_subscribe_structurally_impossible_rules() {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.max_rule_firings_per_group = 0;
    budget.max_rule_work_units_per_group = 0;
    let mut engine = transformation_chain_engine(256, Fingerprint(40), budget);

    engine.explore_transformations().unwrap();

    assert!(engine.transformation_observations.is_empty());
    assert!(engine.transformation_subscribers.is_empty());
}

#[test]
fn zero_rule_budget_precedes_dependency_observation() {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.max_rule_firings_per_group = 0;
    budget.max_rule_work_units_per_group = 0;
    let mut engine = transformation_chain_engine(256, Fingerprint(10), budget);

    engine.explore_transformations().unwrap();

    assert!(engine.transformation_observations.is_empty());
    assert!(engine.transformation_subscribers.is_empty());
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
fn parent_transformations_close_over_late_child_alternatives_independent_of_rule_order() {
    fn explore(parent_rule: RuleId, child_rule: RuleId) -> bool {
        let mut budget = super::super::budget::SearchBudget::default();
        budget.disable_transformation(RuleId(5));
        let (mut engine, root, _) = engine_with_budget(budget);
        let child = engine.memo_mut().create_group(
            schema(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        engine
            .memo_mut()
            .insert_logical(
                child,
                LogicalExprKey {
                    operator: Fingerprint(40),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                LogicalPayloadId(40),
                EquivalenceProof::Initial,
            )
            .unwrap();
        engine
            .memo_mut()
            .insert_logical(
                root,
                LogicalExprKey {
                    operator: Fingerprint(50),
                    scalars: Box::new([]),
                    children: Box::new([child]),
                },
                LogicalPayloadId(50),
                EquivalenceProof::Normalization { rule: RuleId(50) },
            )
            .unwrap();
        engine
            .registry
            .register_transformation(RewriteParentAfterChildAlternative { id: parent_rule })
            .unwrap();
        engine
            .registry
            .register_transformation(AddChildAlternative { id: child_rule })
            .unwrap();

        engine.explore_transformations().unwrap();

        engine
            .memo()
            .group(root)
            .unwrap()
            .logical_exprs()
            .iter()
            .any(|expression| {
                engine
                    .memo()
                    .logical_expr(*expression)
                    .is_some_and(|expression| expression.key.operator == Fingerprint(51))
            })
    }

    assert!(explore(RuleId(30), RuleId(31)), "parent ran before child");
    assert!(explore(RuleId(31), RuleId(30)), "child ran before parent");
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

    fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
        expr.key.operator == Fingerprint(30)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
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
            source_filter_apply_cost: None,
            task_supply: TaskSupplyContract::Serial,
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
        objective: ObjectiveProfile::Latency,
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
    let sequential = compose_candidate_cost_with_sources(
        local,
        None,
        &[child],
        &[&[]],
        CostComposition::Sequential,
    )
    .expect("sequential composition")
    .cost;
    let retained = compose_candidate_cost_with_sources(
        local,
        None,
        &[child],
        &[&[]],
        CostComposition::RetainedState {
            overlapping_children: 1,
        },
    )
    .expect("retained-state composition")
    .cost;
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
    let composed = compose_candidate_cost_with_sources(
        local,
        None,
        &[child],
        &[&[]],
        CostComposition::LocalOnly,
    )
    .expect("schema-only composition")
    .cost;

    assert_eq!(composed, local);
}

#[test]
fn sideways_filter_scales_work_without_weakening_resource_proofs() {
    let source = WorkSourceId(7);
    let child = SearchCost {
        non_revocable_memory_upper: 40,
        minimum_memory_bytes: 40,
        peak_memory_upper: 80,
        ..cost(100.0)
    };
    let source_work = [SourceWork {
        source,
        source_rows: 100,
        base_cost: child.work_only(),
        cost: child.work_only(),
        retentions: Box::new([]),
        filters: Box::new([]),
        filter_apply_cost: SearchCost::ZERO,
        phased_cost: child.work_only(),
        phase_tasks: 1,
    }];
    let filtered = compose_candidate_cost_with_sources(
        SearchCost::ZERO,
        Some(SearchCost::ZERO),
        &[child],
        &[&source_work],
        CostComposition::SidewaysFilter {
            overlapping_children: 1,
            filtered_child: 0,
            sources: Box::new([retained_source(source, 100_000, 1_000_000)]),
        },
    )
    .expect("sideways-filter composition")
    .cost;

    assert_eq!(filtered.score.range.expected, 10.0);
    assert_eq!(filtered.score.range.upper, 100.0);
    assert_eq!(filtered.score.risk_adjusted, 55.0);
    assert_eq!(filtered.non_revocable_memory_upper, 40);
    assert_eq!(filtered.minimum_memory_bytes, 40);
    assert_eq!(filtered.peak_memory_upper, 80);
}

#[test]
fn sideways_filter_attributes_one_predicate_cost_across_union_sources() {
    let left_source = WorkSourceId(8);
    let right_source = WorkSourceId(9);
    let left = compose_candidate_cost_with_sources(
        cost(100.0),
        None,
        &[],
        &[],
        CostComposition::Source {
            source: left_source,
            source_rows: 100,
        },
    )
    .unwrap();
    let right = compose_candidate_cost_with_sources(
        cost(300.0),
        None,
        &[],
        &[],
        CostComposition::Source {
            source: right_source,
            source_rows: 300,
        },
    )
    .unwrap();
    let union = compose_candidate_cost_with_sources(
        SearchCost::ZERO,
        None,
        &[left.cost, right.cost],
        &[left.source_work.as_ref(), right.source_work.as_ref()],
        CostComposition::Sequential,
    )
    .unwrap();
    let filtered = compose_candidate_cost_with_sources(
        cost(40.0),
        Some(cost(20.0)),
        &[union.cost],
        &[union.source_work.as_ref()],
        CostComposition::SidewaysFilter {
            overlapping_children: 0,
            filtered_child: 0,
            sources: Box::new([
                retained_source(left_source, 500_000, 1_000_000),
                retained_source(right_source, 500_000, 1_000_000),
            ]),
        },
    )
    .unwrap();

    // The two scans retain 50% of their work. The one 20-unit predicate
    // application is split 1:3 by source work, while the remaining 20 units
    // of operator-local work stay outside both source lanes.
    assert_eq!(filtered.cost.score.range.expected, 240.0);
    assert_eq!(filtered.source_work[0].cost.score.range.expected, 50.0);
    assert_eq!(filtered.source_work[1].cost.score.range.expected, 150.0);
    assert_eq!(
        filtered.source_work[0]
            .filter_apply_cost
            .score
            .range
            .expected,
        5.0
    );
    assert_eq!(
        filtered.source_work[1]
            .filter_apply_cost
            .score
            .range
            .expected,
        15.0
    );
}

#[test]
fn sideways_filter_preserves_source_local_risk_bounds() {
    let unique_source = WorkSourceId(80);
    let repeated_source = WorkSourceId(81);
    let lanes = [
        SourceWork {
            source: unique_source,
            source_rows: 100,
            base_cost: cost(100.0),
            cost: cost(100.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(100.0),
            phase_tasks: 1,
        },
        SourceWork {
            source: repeated_source,
            source_rows: 300,
            base_cost: cost(300.0),
            cost: cost(300.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(300.0),
            phase_tasks: 1,
        },
    ];
    let filtered = compose_candidate_cost_with_sources(
        cost(40.0),
        Some(cost(20.0)),
        &[cost(400.0)],
        &[&lanes],
        CostComposition::SidewaysFilter {
            overlapping_children: 0,
            filtered_child: 0,
            sources: Box::new([
                retained_source(unique_source, 100_000, 200_000),
                retained_source(repeated_source, 500_000, 1_000_000),
            ]),
        },
    )
    .unwrap();

    assert_eq!(filtered.source_work[0].cost.score.range.expected, 10.0);
    assert_eq!(filtered.source_work[0].cost.score.range.upper, 20.0);
    assert_eq!(filtered.source_work[1].cost.score.range.expected, 150.0);
    assert_eq!(filtered.source_work[1].cost.score.range.upper, 300.0);
    assert_eq!(filtered.cost.score.range.expected, 200.0);
    // Evaluation work is partitioned by immutable source-row ownership; its
    // upper work cannot grow merely because one source is split into lanes.
    assert_eq!(filtered.cost.score.range.upper, 360.0);
}

#[test]
fn sideways_filter_degrades_to_matching_source_lanes() {
    let matched = WorkSourceId(18);
    let missing = WorkSourceId(19);
    let unrelated = WorkSourceId(20);
    let lanes = [
        SourceWork {
            source: matched,
            source_rows: 100,
            base_cost: cost(100.0),
            cost: cost(100.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(100.0),
            phase_tasks: 1,
        },
        SourceWork {
            source: unrelated,
            source_rows: 300,
            base_cost: cost(300.0),
            cost: cost(300.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(300.0),
            phase_tasks: 1,
        },
    ];
    let filtered = compose_candidate_cost_with_sources(
        cost(40.0),
        Some(cost(20.0)),
        &[cost(400.0)],
        &[&lanes],
        CostComposition::SidewaysFilter {
            overlapping_children: 0,
            filtered_child: 0,
            sources: Box::new([
                retained_source(matched, 500_000, 1_000_000),
                retained_source(missing, 500_000, 1_000_000),
            ]),
        },
    )
    .expect("missing source-work lineage must be a safe cost degradation");

    assert_eq!(filtered.cost.score.range.expected, 390.0);
    assert_eq!(filtered.source_work[0].cost.score.range.expected, 50.0);
    assert_eq!(
        filtered.source_work[0]
            .filter_apply_cost
            .score
            .range
            .expected,
        20.0
    );
    assert_eq!(filtered.source_work[1].cost.score.range.expected, 300.0);
    assert_eq!(
        filtered.source_work[1]
            .filter_apply_cost
            .score
            .range
            .expected,
        0.0
    );
}

#[test]
fn sideways_filter_accepts_multiple_lanes_for_one_source() {
    let source = WorkSourceId(21);
    let lanes = [
        SourceWork {
            source,
            source_rows: 100,
            base_cost: cost(100.0),
            cost: cost(100.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(100.0),
            phase_tasks: 1,
        },
        SourceWork {
            source,
            source_rows: 300,
            base_cost: cost(300.0),
            cost: cost(300.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(300.0),
            phase_tasks: 1,
        },
    ];
    let filtered = compose_candidate_cost_with_sources(
        cost(40.0),
        Some(cost(20.0)),
        &[cost(400.0)],
        &[&lanes],
        CostComposition::SidewaysFilter {
            overlapping_children: 0,
            filtered_child: 0,
            sources: Box::new([retained_source(source, 500_000, 1_000_000)]),
        },
    )
    .expect("one source may legitimately own multiple physical work lanes");

    assert_eq!(filtered.cost.score.range.expected, 240.0);
    assert_eq!(filtered.source_work[0].cost.score.range.expected, 50.0);
    assert_eq!(filtered.source_work[1].cost.score.range.expected, 150.0);
    assert_eq!(
        filtered.source_work[0]
            .filter_apply_cost
            .score
            .range
            .expected,
        5.0
    );
    assert_eq!(
        filtered.source_work[1]
            .filter_apply_cost
            .score
            .range
            .expected,
        15.0
    );
}

#[test]
fn sideways_filter_with_no_physical_lane_is_retained() {
    let declared = WorkSourceId(22);
    let unrelated = WorkSourceId(23);
    let lanes = [SourceWork {
        source: unrelated,
        source_rows: 400,
        base_cost: cost(400.0),
        cost: cost(400.0),
        retentions: Box::new([]),
        filters: Box::new([]),
        filter_apply_cost: SearchCost::ZERO,
        phased_cost: cost(400.0),
        phase_tasks: 1,
    }];
    let filtered = compose_candidate_cost_with_sources(
        cost(40.0),
        None,
        &[cost(400.0)],
        &[&lanes],
        CostComposition::SidewaysFilter {
            overlapping_children: 0,
            filtered_child: 0,
            sources: Box::new([retained_source(declared, 1, 1_000_000)]),
        },
    )
    .expect("unmatched logical lineage must retain physical work without a filter cost");

    assert_eq!(filtered.cost.score.range.expected, 440.0);
    assert_eq!(filtered.source_work.as_ref(), lanes);
}

#[test]
fn repeated_sideways_filters_scale_only_the_matching_source_lane() {
    let source = WorkSourceId(11);
    let scan = compose_candidate_cost_with_sources(
        cost(100.0),
        None,
        &[],
        &[],
        CostComposition::Source {
            source,
            source_rows: 100,
        },
    )
    .unwrap();
    let independent_parent = compose_candidate_cost_with_sources(
        cost(50.0),
        None,
        &[scan.cost],
        &[scan.source_work.as_ref()],
        CostComposition::Sequential,
    )
    .unwrap();
    let first = compose_candidate_cost_with_sources(
        cost(10.0),
        Some(SearchCost::ZERO),
        &[independent_parent.cost],
        &[independent_parent.source_work.as_ref()],
        CostComposition::SidewaysFilter {
            overlapping_children: 0,
            filtered_child: 0,
            sources: Box::new([retained_source(source, 500_000, 1_000_000)]),
        },
    )
    .unwrap();
    let second = compose_candidate_cost_with_sources(
        cost(20.0),
        Some(SearchCost::ZERO),
        &[first.cost],
        &[first.source_work.as_ref()],
        CostComposition::SidewaysFilter {
            overlapping_children: 0,
            filtered_child: 0,
            sources: Box::new([retained_source(source, 100_000, 1_000_000)]),
        },
    )
    .unwrap();

    assert_eq!(second.cost.score.range.expected, 85.0);
    assert_eq!(second.source_work.len(), 1);
    assert_eq!(second.source_work[0].cost.score.range.expected, 5.0);
}

#[test]
fn exact_survivor_bounds_are_absolute_and_proof_idempotent() {
    let source = WorkSourceId(777);
    let scan = compose_candidate_cost_with_sources(
        cost(1_000.0),
        None,
        &[],
        &[],
        CostComposition::Source {
            source,
            source_rows: 1_000,
        },
    )
    .unwrap();
    let first_proof = retained_source(source, 100_000, 100_000);
    let apply = |input: &ComposedCost, proof: SidewaysFilterSource| {
        compose_candidate_cost_with_sources(
            SearchCost::ZERO,
            Some(SearchCost::ZERO),
            &[input.cost],
            &[input.source_work.as_ref()],
            CostComposition::SidewaysFilter {
                overlapping_children: 0,
                filtered_child: 0,
                sources: Box::new([proof]),
            },
        )
        .unwrap()
    };

    let first = apply(&scan, first_proof);
    let duplicate = apply(&first, first_proof);
    assert_eq!(duplicate.source_work[0].cost.score.range.expected, 100.0);
    assert_eq!(duplicate.source_work[0].cost.score.range.upper, 100.0);

    let correlated = apply(
        &first,
        SidewaysFilterSource {
            domain: DomainProofId(Fingerprint(first_proof.domain.0 .0 + 1)),
            evaluation: EvaluationOccurrenceId(Fingerprint(first_proof.evaluation.0 .0 + 1)),
            ..first_proof
        },
    );
    assert_eq!(correlated.source_work[0].cost.score.range.expected, 10.0);
    assert_eq!(correlated.source_work[0].cost.score.range.upper, 100.0);
}

#[test]
fn source_predicate_cost_is_reordered_by_runtime_selectivity() {
    let source = WorkSourceId(12);
    let scan = compose_candidate_cost_with_sources(
        cost(100.0),
        None,
        &[],
        &[],
        CostComposition::Source {
            source,
            source_rows: 100,
        },
    )
    .unwrap();
    let first = compose_candidate_cost_with_sources(
        cost(30.0),
        Some(cost(20.0)),
        &[scan.cost],
        &[scan.source_work.as_ref()],
        CostComposition::SidewaysFilter {
            overlapping_children: 0,
            filtered_child: 0,
            sources: Box::new([retained_source(source, 500_000, 1_000_000)]),
        },
    )
    .unwrap();
    let second = compose_candidate_cost_with_sources(
        cost(30.0),
        Some(cost(20.0)),
        &[first.cost],
        &[first.source_work.as_ref()],
        CostComposition::SidewaysFilter {
            overlapping_children: 0,
            filtered_child: 0,
            sources: Box::new([retained_source(source, 100_000, 1_000_000)]),
        },
    )
    .unwrap();

    // Base access is retained by 5%; each join keeps 10 units of independent
    // work, and predicate evaluation is 20 + (10% * 20), not tree-order
    // evaluation 20 + (50% * 20).
    assert_eq!(second.cost.score.range.expected, 47.0);
    assert_eq!(
        second.source_work[0].filter_apply_cost.score.range.expected,
        22.0
    );
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
    let retained = compose_candidate_cost_with_sources(
        local,
        None,
        &[child],
        &[&[]],
        CostComposition::RetainedState {
            overlapping_children: 1,
        },
    )
    .expect("revocable retained-state composition")
    .cost;
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
            source_filter_apply_cost: None,
            task_supply: TaskSupplyContract::Serial,
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
        objective: ObjectiveProfile::Latency,
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

struct SourceSensitiveAlternativeImplementation;

impl PhysicalImplementation for SourceSensitiveAlternativeImplementation {
    fn id(&self) -> ImplementationId {
        ImplementationId(700)
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
        let source = WorkSourceId(777);
        let (work, composition, apply_cost) = match logical.key.operator.0 {
            201 => (100.0, CostComposition::Sequential, None),
            202 => (0.0, CostComposition::Sequential, None),
            211 => (
                10.0,
                CostComposition::Source {
                    source,
                    source_rows: 10,
                },
                None,
            ),
            212 => (
                120.0,
                CostComposition::Source {
                    source,
                    source_rows: 120,
                },
                None,
            ),
            203 => (
                0.0,
                CostComposition::SidewaysFilter {
                    overlapping_children: 0,
                    filtered_child: 0,
                    sources: Box::new([retained_source(source, 10_000, 1_000_000)]),
                },
                Some(cost(0.0)),
            ),
            _ => unreachable!(),
        };
        Ok(Box::new([PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id(),
                logical: expr,
                children: logical.key.children.clone(),
                payload_fingerprint: logical.key.operator,
            },
            payload: PhysicalPayloadId(logical.payload.0),
            provided: provided(),
            child_goals: logical
                .key
                .children
                .iter()
                .map(|child| (*child, goal))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            local_cost: cost(work),
            source_filter_apply_cost: apply_cost,
            task_supply: TaskSupplyContract::Serial,
            cost_composition: composition,
            spillable: false,
            enforcer_cost_input: EnforcerCostInput::unbounded(CompactRange::point(1.0)?, 8),
            physical_fingerprint: logical.key.operator,
            region: None,
            mandatory: true,
        }]))
    }
}

#[test]
fn parent_costs_every_source_sensitive_child_frontier_candidate() {
    let mut memo = Memo::new(super::super::budget::SearchBudget::default());
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
    let goal = OptimizationGoal {
        required: memo.intern_required(required()).unwrap(),
        row_goal: RowGoal::All,
        objective: ObjectiveProfile::Latency,
        grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
        context: OptimizationContextId(0),
    };
    let mut registry = ImplementationRegistry::default();
    registry
        .register_implementation(SourceSensitiveAlternativeImplementation)
        .unwrap();
    let mut engine = CascadesEngine::new(memo, registry);

    let winner = engine.optimize(root, goal, SearchMode::Memo).unwrap();

    // Independent exhaustive oracle: remove each candidate's source phase
    // from its complete cost, apply the proven absolute survivor ratio once,
    // and enumerate the whole child frontier. This intentionally does not
    // call the production composition routine or its helper algebra.
    let (oracle_cost, oracle_candidate) = engine
        .memo()
        .group(child)
        .unwrap()
        .winner_frontier(goal)
        .unwrap()
        .candidates()
        .iter()
        .map(|candidate| {
            let source_cost = candidate
                .source_work
                .iter()
                .map(|lane| lane.phased_cost.score.range.expected)
                .sum::<f64>();
            let retained_source_cost = candidate
                .source_work
                .iter()
                .map(|lane| lane.base_cost.score.range.expected * 0.01)
                .sum::<f64>();
            (
                candidate.cost.score.range.expected - source_cost + retained_source_cost,
                candidate.candidate,
            )
        })
        .min_by(|left, right| left.0.total_cmp(&right.0))
        .unwrap();
    assert!((winner.cost.score.range.expected - 1.2).abs() < 1e-9);
    assert!((winner.cost.score.range.expected - oracle_cost).abs() < 1e-9);
    assert_eq!(winner.children.len(), 1);
    assert_eq!(winner.children[0].candidate, oracle_candidate);
    let selected_child = engine
        .memo()
        .resolve_child_winner(winner.children[0])
        .unwrap();
    assert_eq!(selected_child.local_cost.score.range.expected, 0.0);
    assert_eq!(
        engine
            .memo()
            .group(child)
            .unwrap()
            .winner_frontier(goal)
            .unwrap()
            .candidates()
            .len(),
        2
    );
}

#[test]
fn source_predicate_costs_commute_across_join_composition() {
    let left_source = WorkSourceId(800);
    let right_source = WorkSourceId(801);
    let scan = |source| {
        compose_candidate_cost_with_sources(
            cost(100.0),
            None,
            &[],
            &[],
            CostComposition::Source {
                source,
                source_rows: 1_000,
            },
        )
        .unwrap()
    };
    let left = scan(left_source);
    let right = scan(right_source);
    let union = compose_candidate_cost_with_sources(
        SearchCost::ZERO,
        None,
        &[left.cost, right.cost],
        &[left.source_work.as_ref(), right.source_work.as_ref()],
        CostComposition::Sequential,
    )
    .unwrap();
    let apply = |input: &ComposedCost, id: u128, left: u32, right: u32| {
        let source = |work_source, retained| SidewaysFilterSource {
            source: work_source,
            domain: DomainProofId(Fingerprint(id + work_source.0 as u128)),
            evaluation: EvaluationOccurrenceId(Fingerprint(id)),
            expected_retained_ppm: retained,
            upper_retained_ppm: 1_000_000,
        };
        compose_candidate_cost_with_sources(
            cost(100.0),
            Some(cost(100.0)),
            &[input.cost],
            &[input.source_work.as_ref()],
            CostComposition::SidewaysFilter {
                overlapping_children: 0,
                filtered_child: 0,
                sources: Box::new([source(left_source, left), source(right_source, right)]),
            },
        )
        .unwrap()
    };
    let ab = apply(&apply(&union, 11, 100_000, 500_000), 12, 200_000, 800_000);
    let ba = apply(&apply(&union, 12, 200_000, 800_000), 11, 100_000, 500_000);
    assert_eq!(ab.source_work, ba.source_work);
    assert!((ab.cost.score.range.expected - ba.cost.score.range.expected).abs() < 1e-9);
}

#[test]
fn duplicate_domain_proof_does_not_shrink_later_evaluation_domain() {
    let source = WorkSourceId(900);
    let scan = compose_candidate_cost_with_sources(
        cost(1_000.0),
        None,
        &[],
        &[],
        CostComposition::Source {
            source,
            source_rows: 1_000,
        },
    )
    .unwrap();
    let apply = |input: &ComposedCost, proof: u128, occurrence: u128, retained: u32| {
        compose_candidate_cost_with_sources(
            cost(1_000.0),
            Some(cost(1_000.0)),
            &[input.cost],
            &[input.source_work.as_ref()],
            CostComposition::SidewaysFilter {
                overlapping_children: 0,
                filtered_child: 0,
                sources: Box::new([SidewaysFilterSource {
                    source,
                    domain: DomainProofId(Fingerprint(proof)),
                    evaluation: EvaluationOccurrenceId(Fingerprint(occurrence)),
                    expected_retained_ppm: retained,
                    upper_retained_ppm: 1_000_000,
                }]),
            },
        )
        .unwrap()
    };
    let first = apply(&scan, 21, 101, 100_000);
    let repeated = apply(&first, 21, 102, 100_000);
    let final_filter = apply(&repeated, 22, 103, 1_000_000);
    let lane = &final_filter.source_work[0];
    assert_eq!(lane.retentions.len(), 2);
    assert_eq!(lane.filters.len(), 3);
    assert_eq!(lane.filter_apply_cost.score.range.expected, 1_200.0);
}

#[test]
fn source_predicate_attribution_is_invariant_to_lane_partitioning() {
    let source = WorkSourceId(901);
    let lane = |rows, work| SourceWork {
        source,
        source_rows: rows,
        base_cost: cost(work),
        cost: cost(work),
        retentions: Box::new([]),
        filters: Box::new([]),
        filter_apply_cost: SearchCost::ZERO,
        phased_cost: cost(work),
        phase_tasks: 1,
    };
    let composition = CostComposition::SidewaysFilter {
        overlapping_children: 0,
        filtered_child: 0,
        sources: Box::new([SidewaysFilterSource {
            source,
            domain: DomainProofId(Fingerprint(31)),
            evaluation: EvaluationOccurrenceId(Fingerprint(131)),
            expected_retained_ppm: 250_000,
            upper_retained_ppm: 1_000_000,
        }]),
    };
    let merged_lanes = [lane(1_000, 100.0)];
    let split_lanes = [lane(250, 25.0), lane(750, 75.0)];
    let merged = compose_candidate_cost_with_sources(
        cost(20.0),
        Some(cost(20.0)),
        &[cost(100.0)],
        &[&merged_lanes],
        composition.clone(),
    )
    .unwrap();
    let split = compose_candidate_cost_with_sources(
        cost(20.0),
        Some(cost(20.0)),
        &[cost(100.0)],
        &[&split_lanes],
        composition,
    )
    .unwrap();
    assert!((merged.cost.score.range.expected - split.cost.score.range.expected).abs() < 1e-9);
    assert_eq!(
        merged
            .source_work
            .iter()
            .map(|lane| lane.filter_apply_cost.score.range.expected)
            .sum::<f64>(),
        split
            .source_work
            .iter()
            .map(|lane| lane.filter_apply_cost.score.range.expected)
            .sum::<f64>()
    );
}

struct BudgetLimitedCombinationImplementation;

impl PhysicalImplementation for BudgetLimitedCombinationImplementation {
    fn id(&self) -> ImplementationId {
        ImplementationId(701)
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
        let source = WorkSourceId(777);
        let (work, composition, apply_cost) = match logical.key.operator.0 {
            201 => (100.0, CostComposition::Sequential, None),
            202 => (95.0, CostComposition::Sequential, None),
            211 => (
                10.0,
                CostComposition::Source {
                    source,
                    source_rows: 10,
                },
                None,
            ),
            212 => (
                25.0,
                CostComposition::Source {
                    source,
                    source_rows: 25,
                },
                None,
            ),
            204 => (0.0, CostComposition::Sequential, None),
            213 => (
                130.0,
                CostComposition::Source {
                    source,
                    source_rows: 130,
                },
                None,
            ),
            203 => (
                0.0,
                CostComposition::SidewaysFilter {
                    overlapping_children: 0,
                    filtered_child: 0,
                    sources: Box::new([retained_source(source, 10_000, 1_000_000)]),
                },
                Some(cost(0.0)),
            ),
            _ => unreachable!(),
        };
        Ok(Box::new([PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id(),
                logical: expr,
                children: logical.key.children.clone(),
                payload_fingerprint: logical.key.operator,
            },
            payload: PhysicalPayloadId(logical.payload.0),
            provided: provided(),
            child_goals: logical
                .key
                .children
                .iter()
                .map(|child| (*child, goal))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            local_cost: cost(work),
            source_filter_apply_cost: apply_cost,
            task_supply: TaskSupplyContract::Serial,
            cost_composition: composition,
            spillable: false,
            enforcer_cost_input: EnforcerCostInput::unbounded(CompactRange::point(1.0).unwrap(), 8),
            physical_fingerprint: logical.key.operator,
            region: None,
            mandatory: true,
        }]))
    }
}

#[test]
fn child_product_cutoff_records_budget_limited_completion() {
    fn search(limit: u32) -> (f64, usize, usize, usize) {
        let mut budget = super::super::budget::SearchBudget::default();
        budget.max_child_frontier_combinations_per_group = limit;
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
                if matches!(operator, 202 | 204) {
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
        let scan_c = add(213, Box::new([]), None);
        let child = add(201, Box::new([scan_a]), None);
        add(202, Box::new([scan_b]), Some(child));
        add(204, Box::new([scan_c]), Some(child));
        let root = add(203, Box::new([child]), None);
        let goal = OptimizationGoal {
            required: memo.intern_required(required()).unwrap(),
            row_goal: RowGoal::All,
            objective: ObjectiveProfile::Latency,
            grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
            context: OptimizationContextId(0),
        };
        let mut registry = ImplementationRegistry::default();
        registry
            .register_implementation(BudgetLimitedCombinationImplementation)
            .unwrap();
        let mut engine = CascadesEngine::new(memo, registry);
        let winner = engine.optimize(root, goal, SearchMode::Memo).unwrap();
        let root_group = engine.memo().group(root).unwrap();
        let consumed = root_group
            .ledger
            .consumed(BudgetDimension::ChildFrontierCombination);
        let exhausted = root_group
            .ledger
            .exhaustion_events()
            .filter(|(dimension, _)| *dimension == BudgetDimension::ChildFrontierCombination)
            .count();
        let width = engine
            .memo()
            .group(child)
            .unwrap()
            .winner_frontier(goal)
            .unwrap()
            .candidates()
            .len();
        (winner.cost.score.range.expected, consumed, exhausted, width)
    }
    let (baseline_only, zero_consumed, zero_exhausted, zero_width) = search(0);
    let (chosen, consumed, exhausted, width) = search(1);
    let (oracle, _, _, _) = search(8);
    assert_eq!(zero_width, 3);
    assert_eq!(zero_consumed, 0);
    assert!(zero_exhausted > 0);
    assert_eq!(width, 3);
    assert_eq!(consumed, 1);
    assert!(exhausted > 0);
    assert!(baseline_only >= chosen);
    assert!(chosen > oracle);
    assert!((oracle - 1.3).abs() < 1e-9);
}
