// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cascades engine scheduling, transaction, and costing tests.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use paro_common::types::LogicalType;

use super::*;
use crate::cascades::column::{ColumnDesc, ColumnOrigin, ColumnVisibility, GroupSchema};
use crate::cascades::cost::{CompactRange, ScoreSummary};
use crate::cascades::ids::{
    AdmissibleGrantSetId, CalibrationRevisionId, CandidateId, ColumnId, LogicalPayloadId,
    OptimizationContextId, PhysicalExprId, PhysicalPayloadId, ResourceGrantClassId,
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
use crate::cascades::region::{
    FacetCriticality, RegionArtifactDependencyContract, RegionFacet, RegionFacetKind, RegionForest,
    RegionScopeContract,
};
use crate::cascades::rules::{
    DomainProofId, EquivalentExpression, EvaluationOccurrenceId, GrantDependencyDescriptor,
    PatternBinding, PatternBindingSet, PatternEnumerationCompletion, PhysicalImplementation,
    QualityDependency, RulePromise, SidewaysFilterSource, TransformationBudgetClass,
    TransformationRule,
};
use crate::cascades::tasks::TaskState;
use crate::physical::ObjectiveProfile;

#[path = "tests/closure.rs"]
mod closure;

#[path = "tests/grant_capacity.rs"]
mod grant_capacity;

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
    memo.set_regions(
        RegionForest::normalize(
            [RegionFacet {
                fingerprint: facet,
                kind: RegionFacetKind::RuntimeFilter,
                criticality: FacetCriticality::Optional,
                priority: 1,
                scope_contract: RegionScopeContract::OwnerWithImmediateInputs,
                scope: [canonical_owner].into_iter().collect(),
            }],
            8,
            8,
        )
        .unwrap(),
    );

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
            sequence: 0,
            child_goals: Box::new([(first, goal), (second, goal)]),
            local_cost: SearchCost::ZERO,
            source_filter_apply_cost: None,
            task_supply: TaskSupplyContract::Serial,
            cost_composition: CostComposition::Sequential,
            spillable: false,
            enforcer_cost_input: EnforcerCostInput::unbounded(CompactRange::point(1.0).unwrap(), 8),
            physical_fingerprint: Fingerprint(91),
            certified_local_work: None,
            region: Some(RegionCandidateContract {
                // RegionId is an ephemeral forest position. The facet
                // fingerprint is the stable recipe identity after runtime
                // facet normalization reassigns positions.
                region: super::super::ids::RegionId::new(99),
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
        assert_eq!(
            proof.region,
            memo.regions().region_for_facet(facet).unwrap()
        );
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
            operator_encoding: None,
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

struct QualityLaneRule;

impl TransformationRule for QualityLaneRule {
    fn id(&self) -> RuleId {
        RuleId(6)
    }

    fn quality_dependency(&self) -> Option<QualityDependency> {
        Some(QualityDependency::NarrowAggregate)
    }

    fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
    }

    fn apply(
        &self,
        _expr: LogicalExprId,
        _ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        Ok(Box::new([]))
    }
}

#[test]
fn quality_publication_promotes_only_its_local_followup_lane() {
    let (mut ordinary_engine, ordinary_group, _) =
        engine_with_budget(super::super::budget::SearchBudget::default());
    ordinary_engine
        .registry
        .register_transformation(QualityLaneRule)
        .unwrap();
    ordinary_engine.set_quality_policy_handoff_enabled(true);
    let mut ordinary = StableAgenda::default();
    ordinary_engine
        .schedule_transformations(ordinary_group, &mut ordinary)
        .unwrap();
    let ordinary_quality_stage = ordinary
        .tasks
        .iter()
        .find_map(|(key, task)| match task {
            SearchTask::Transform {
                rule: RuleId(6), ..
            } => Some(key.quality_stage),
            _ => None,
        })
        .expect("quality rule must be scheduled");
    assert_eq!(ordinary_quality_stage, u8::MAX);

    let (mut promoted_engine, promoted_group, _) =
        engine_with_budget(super::super::budget::SearchBudget::default());
    promoted_engine
        .registry
        .register_transformation(QualityLaneRule)
        .unwrap();
    promoted_engine.set_quality_policy_handoff_enabled(true);
    let mut promoted = StableAgenda::default();
    promoted_engine
        .schedule_transformations_with_lane(promoted_group, &mut promoted, true)
        .unwrap();
    let promoted_quality_stage = promoted
        .tasks
        .iter()
        .find_map(|(key, task)| match task {
            SearchTask::Transform {
                rule: RuleId(6), ..
            } => Some(key.quality_stage),
            _ => None,
        })
        .expect("quality rule must be scheduled");
    assert_eq!(
        promoted_quality_stage,
        QualityDependency::NarrowAggregate.stage()
    );
    let ordinary_rule_stage = promoted
        .tasks
        .iter()
        .find_map(|(key, task)| match task {
            SearchTask::Transform {
                rule: RuleId(5), ..
            } => Some(key.quality_stage),
            _ => None,
        })
        .expect("ordinary rule must be scheduled");
    assert_eq!(ordinary_rule_stage, u8::MAX);
}

struct EnumerateTwoCompositionBindings;

impl TransformationRule for EnumerateTwoCompositionBindings {
    fn id(&self) -> RuleId {
        RuleId(22)
    }

    fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
        expr.key.operator == Fingerprint(10)
    }

    fn budget_class(&self) -> TransformationBudgetClass {
        TransformationBudgetClass::Composition
    }

    fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
    }

    fn bindings(&self, expr: LogicalExprId, ctx: &RuleContext<'_>) -> Result<PatternBindingSet> {
        let logical = ctx.memo.logical_expr(expr).unwrap();
        let root = PatternBinding::root_only(ctx.group, expr, logical).root;
        Ok(PatternBindingSet {
            bindings: vec![
                PatternBinding {
                    root: root.clone(),
                    fingerprint: Fingerprint(220),
                },
                PatternBinding {
                    root,
                    fingerprint: Fingerprint(221),
                },
            ]
            .into_boxed_slice(),
            reads: Box::new([]),
            work_units: 3,
            work_dimension: BudgetDimension::CompositionRuleWorkPerGroup,
            completion: PatternEnumerationCompletion::Complete,
        })
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
            payload: LogicalPayloadId(22),
            operator_encoding: None,
            logical_properties: LogicalProperties::default(),
            cardinality: GroupCardinality::default(),
            proof: EquivalenceProof::Transformation {
                rule: self.id(),
                source: expr,
                premise: Fingerprint(220),
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

    fn output_bound(&self, _: &PatternBinding, _: &RuleContext<'_>) -> usize {
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
                operator_encoding: None,
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
            operator_encoding: None,
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
            operator_encoding: None,
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
            operator_encoding: None,
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
            operator_encoding: None,
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
            operator_encoding: None,
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
            operator_encoding: None,
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

struct FixedLeafImplementation {
    id: ImplementationId,
    score: f64,
    mandatory: bool,
}

impl PhysicalImplementation for FixedLeafImplementation {
    fn id(&self) -> ImplementationId {
        self.id
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
        Ok(vec![PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id,
                logical: expr,
                children: Box::new([]),
                payload_fingerprint: Fingerprint(self.id.0 as u128),
            },
            payload: PhysicalPayloadId(expr.0),
            provided: provided(),
            child_goals: Box::new([]),
            local_cost: cost(self.score),
            source_filter_apply_cost: None,
            task_supply: TaskSupplyContract::Serial,
            cost_composition: CostComposition::Sequential,
            spillable: false,
            enforcer_cost_input: EnforcerCostInput::unbounded(CompactRange::point(1.0)?, 8),
            physical_fingerprint: logical.key.operator,
            region: None,
            mandatory: self.mandatory,
        }]
        .into_boxed_slice())
    }
}

fn strong_seed_engine() -> (CascadesEngine, GroupId, OptimizationGoal) {
    strong_seed_engine_with_prefix(false)
}

fn strong_seed_engine_with_prefix(
    prefix_group: bool,
) -> (CascadesEngine, GroupId, OptimizationGoal) {
    let mut memo = Memo::new(super::super::budget::SearchBudget::default());
    if prefix_group {
        let dummy = memo.create_group(
            schema(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        memo.insert_logical(
            dummy,
            LogicalExprKey {
                operator: Fingerprint(999),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(999),
            EquivalenceProof::Initial,
        )
        .unwrap();
    }
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
    registry
        .register_implementation(FixedLeafImplementation {
            id: ImplementationId(40),
            score: 1.0,
            mandatory: true,
        })
        .unwrap();
    registry
        .register_implementation(FixedLeafImplementation {
            id: ImplementationId(41),
            score: 100.0,
            mandatory: false,
        })
        .unwrap();
    (CascadesEngine::new(memo, registry), group, goal)
}

struct TreeImplementation {
    id: ImplementationId,
    operator: Fingerprint,
    child: Option<GroupId>,
    child_row_goal: Option<RowGoal>,
    local_score: f64,
    mandatory: bool,
}

impl PhysicalImplementation for TreeImplementation {
    fn id(&self) -> ImplementationId {
        self.id
    }

    fn matches(
        &self,
        logical: &super::super::memo::LogicalExpr,
        _: OptimizationGoal,
        _: &ImplementationContext<'_>,
    ) -> bool {
        logical.key.operator == self.operator
    }

    fn candidates(
        &self,
        expr: LogicalExprId,
        goal: OptimizationGoal,
        _: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>> {
        let local_score =
            if self.operator == Fingerprint(100) && goal.row_goal == RowGoal::AtMost(1) {
                10.0
            } else {
                self.local_score
            };
        let (children, child_goals) = match self.child {
            Some(child) => {
                let mut child_goal = goal;
                child_goal.row_goal = self.child_row_goal.unwrap_or(RowGoal::All);
                (vec![child], vec![(child, child_goal)])
            }
            None => (Vec::new(), Vec::new()),
        };
        Ok(vec![PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id,
                logical: expr,
                children: children.into_boxed_slice(),
                payload_fingerprint: Fingerprint(self.id.0 as u128),
            },
            payload: PhysicalPayloadId(expr.0),
            provided: provided(),
            child_goals: child_goals.into_boxed_slice(),
            local_cost: cost(local_score),
            source_filter_apply_cost: None,
            task_supply: TaskSupplyContract::Serial,
            cost_composition: CostComposition::Sequential,
            spillable: false,
            enforcer_cost_input: EnforcerCostInput::unbounded(CompactRange::point(1.0)?, 8),
            physical_fingerprint: Fingerprint(self.id.0 as u128),
            region: None,
            mandatory: self.mandatory,
        }]
        .into_boxed_slice())
    }
}

fn strong_tree_engine() -> (CascadesEngine, GroupId, OptimizationGoal) {
    let mut memo = Memo::new(super::super::budget::SearchBudget::default());
    let child = memo.create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    memo.insert_logical(
        child,
        LogicalExprKey {
            operator: Fingerprint(100),
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
            operator: Fingerprint(200),
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
        .register_implementation(TreeImplementation {
            id: ImplementationId(40),
            operator: Fingerprint(100),
            child: None,
            child_row_goal: None,
            local_score: 1.0,
            mandatory: true,
        })
        .unwrap();
    registry
        .register_implementation(TreeImplementation {
            id: ImplementationId(41),
            operator: Fingerprint(200),
            child: Some(child),
            child_row_goal: Some(RowGoal::All),
            local_score: 0.0,
            mandatory: true,
        })
        .unwrap();
    registry
        .register_implementation(TreeImplementation {
            id: ImplementationId(42),
            operator: Fingerprint(200),
            child: Some(child),
            child_row_goal: Some(RowGoal::AtMost(1)),
            local_score: 0.0,
            mandatory: false,
        })
        .unwrap();
    (CascadesEngine::new(memo, registry), root, goal)
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

#[test]
fn engine_group_merge_redirects_tasks_and_discards_stale_transform_state() {
    let (mut engine, canonical_source, goal) = engine(0);
    let secondary = engine.memo_mut().create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let task = match engine
        .task_registry
        .request(
            TaskIntent::Optimize {
                group: secondary,
                goal,
            },
            ReadSet::empty(),
        )
        .unwrap()
    {
        TaskRequest::Leader(task) => task,
        request => panic!("unexpected merge task request: {request:?}"),
    };
    engine.task_registry.start(task).unwrap();

    let transform_task = TransformationTaskId {
        group: secondary,
        expression: LogicalExprId::new(0),
        rule: RuleId::new(77),
    };
    engine
        .transformation_subscribers
        .entry(secondary)
        .or_default()
        .insert(transform_task);
    engine.transformation_observations.insert(
        transform_task,
        Box::new([PatternRead::from_group(&engine.memo, secondary).unwrap()]),
    );

    let canonical = engine.merge_groups(canonical_source, secondary).unwrap();

    assert_eq!(canonical, canonical_source);
    assert_eq!(engine.memo.canonical_group(secondary), canonical);
    assert_eq!(engine.task_registry.canonical_group(secondary), canonical);
    assert_eq!(
        engine.task_registry.state(task),
        Some(TaskState::Invalidated)
    );
    assert!(engine.transformation_observations.is_empty());
    assert!(!engine
        .transformation_subscribers
        .values()
        .any(|tasks| tasks.contains(&transform_task)));
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
fn certified_group_pruning_is_independent_of_rule_tracing() {
    let (mut engine, group, goal) = engine(8);
    engine.set_certified_group_pruning_enabled(true);
    engine.set_rule_work_profile_enabled(false);

    let winner = engine.optimize(group, goal, SearchMode::Memo).unwrap();
    assert_eq!(winner.physical_fingerprint, Fingerprint(11));

    let counters = engine.search_work_counters();
    assert_eq!(counters["certified_group_pruning_enabled"], 1);
    assert!(counters["certified_bound_compute_us"] > 0);
    assert!(engine.task_registry().profile().bound_proofs > 0);
}

#[test]
fn strong_incumbent_crosses_a_fresh_memo_and_prunes_before_children() {
    let (mut source, source_group, source_goal) = strong_seed_engine();
    let source_winner = source
        .optimize(source_group, source_goal, SearchMode::Memo)
        .unwrap();
    assert_eq!(source_winner.cost.score.range.expected, 1.0);
    let plan = source
        .export_seed_plan(source_group, source_goal)
        .unwrap()
        .expect("the verified source winner is exportable");
    assert_eq!(plan.candidate(), source_winner.candidate);
    assert_eq!(plan.frozen().winner.children.len(), 0);

    let (mut target, target_group, target_goal) = strong_seed_engine();
    let priced = target.reprice_seed_plan(target_group, plan).unwrap();
    target.install_priced_incumbent(priced).unwrap();
    target.set_certified_group_pruning_enabled(true);
    target.set_rule_work_profile_enabled(false);
    let target_winner = target
        .optimize(target_group, target_goal, SearchMode::Memo)
        .unwrap();

    assert_eq!(target_winner.cost.score.range.expected, 1.0);
    let counters = target.search_work_counters();
    assert_eq!(counters["strong_incumbent_seed_count"], 1);
    assert_eq!(counters["strong_incumbent_active_count"], 1);
    assert!(counters["certified_bound_pruned_before_children_count"] > 0);
}

#[test]
fn unrelated_group_merge_does_not_drop_a_current_strong_incumbent() {
    let (mut source, source_group, source_goal) = strong_seed_engine();
    source
        .optimize(source_group, source_goal, SearchMode::Memo)
        .unwrap();
    let plan = source
        .export_seed_plan(source_group, source_goal)
        .unwrap()
        .expect("the verified source winner is exportable");

    let (mut target, target_group, target_goal) = strong_seed_engine();
    let priced = target
        .reprice_seed_plan(target_group, plan)
        .expect("the destination must re-price the selected DAG");
    target.install_priced_incumbent(priced).unwrap();
    let unrelated_left = target.memo_mut().create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let unrelated_right = target.memo_mut().create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    target
        .merge_groups(unrelated_left, unrelated_right)
        .expect("an unrelated equivalent merge should succeed");

    let counters = target.search_work_counters();
    assert_eq!(counters["strong_incumbent_seed_count"], 1);
    assert_eq!(counters["strong_incumbent_active_count"], 1);
    assert_eq!(counters["strong_incumbent_invalid_lookup_count"], 0);
    assert_eq!(target_goal, source_goal);
}

#[test]
fn completed_child_no_plan_below_prunes_a_parent_before_cost_synthesis() {
    let (mut source, source_root, source_goal) = strong_tree_engine();
    let source_winner = source
        .optimize(source_root, source_goal, SearchMode::Memo)
        .unwrap();
    assert_eq!(source_winner.cost.score.range.expected, 1.0);
    let plan = source
        .export_seed_plan(source_root, source_goal)
        .unwrap()
        .expect("the source tree winner is exportable");

    let (mut target, target_root, target_goal) = strong_tree_engine();
    let priced = target.reprice_seed_plan(target_root, plan).unwrap();
    target.install_priced_incumbent(priced).unwrap();
    target.set_certified_group_pruning_enabled(true);
    target.set_rule_work_profile_enabled(false);
    let target_winner = target
        .optimize(target_root, target_goal, SearchMode::Memo)
        .unwrap();

    assert_eq!(target_winner.cost.score.range.expected, 1.0);
    let counters = target.search_work_counters();
    assert!(counters["certified_bound_pruned_after_children_count"] > 0);
    assert!(target.task_registry().profile().bound_proofs > 0);
}

#[test]
fn priced_seed_is_rejected_after_a_child_fact_change() {
    let (mut source, source_group, source_goal) = strong_seed_engine();
    source
        .optimize(source_group, source_goal, SearchMode::Memo)
        .unwrap();
    let plan = source
        .export_seed_plan(source_group, source_goal)
        .unwrap()
        .expect("source winner should produce a seed plan");
    assert!(source.price_seed_plan(&plan).is_ok());
    source
        .memo_mut()
        .group_mut(source_group)
        .unwrap()
        .cardinality = GroupCardinality::new(
        Fingerprint(992),
        super::super::memo::CardinalityRecipeKind::Statistics,
        1,
        2,
        4,
    );
    assert!(source.price_seed_plan(&plan).is_err());

    let (mut target, target_group, _) = strong_seed_engine();
    let priced = target.reprice_seed_plan(target_group, plan).unwrap();
    target
        .memo_mut()
        .group_mut(target_group)
        .unwrap()
        .cardinality = GroupCardinality::new(
        Fingerprint(991),
        super::super::memo::CardinalityRecipeKind::Statistics,
        1,
        4,
        9,
    );
    let error = target
        .install_priced_incumbent(priced)
        .expect_err("a fact change must invalidate the old price");
    assert!(error.to_string().contains("read witness is stale"));
}

#[test]
fn priced_seed_is_rejected_after_calibration_change() {
    let (mut source, source_group, source_goal) = strong_seed_engine();
    source
        .optimize(source_group, source_goal, SearchMode::Memo)
        .unwrap();
    let plan = source
        .export_seed_plan(source_group, source_goal)
        .unwrap()
        .expect("source winner should produce a seed plan");

    let (mut target, target_group, _) = strong_seed_engine();
    let priced = target.reprice_seed_plan(target_group, plan).unwrap();
    let mut calibration = MachineCalibrationBundle::default();
    calibration.revision = CalibrationRevisionId(99);
    target.memo_mut().set_calibration(Arc::new(calibration));
    let error = target
        .install_priced_incumbent(priced)
        .expect_err("a calibration change must invalidate the old price");
    assert!(error.to_string().contains("cost context changed"));
}

#[test]
fn priced_seed_is_rejected_after_calibration_payload_change_without_revision_bump() {
    let (mut source, source_group, source_goal) = strong_seed_engine();
    source
        .optimize(source_group, source_goal, SearchMode::Memo)
        .unwrap();
    let plan = source
        .export_seed_plan(source_group, source_goal)
        .unwrap()
        .expect("source winner should produce a seed plan");

    let (mut target, target_group, _) = strong_seed_engine();
    let priced = target.reprice_seed_plan(target_group, plan).unwrap();
    let mut calibration = MachineCalibrationBundle::default();
    calibration.risk_weight += 0.25;
    target.memo_mut().set_calibration(Arc::new(calibration));
    let error = target
        .install_priced_incumbent(priced)
        .expect_err("a payload change must invalidate the old price even with the same revision");
    assert!(error.to_string().contains("cost context changed"));
}

#[test]
fn priced_seed_is_rejected_after_grant_operating_point_change() {
    let class = crate::physical::ResourceGrantClass {
        id: ResourceGrantClassId(7),
        hard_memory_bytes: 1024,
        spill_policy: crate::physical::SpillPolicy::Forbidden,
        max_parallel_tasks: 1,
    };
    let mut source_goal;
    let (mut source, source_group, base_source_goal) = strong_seed_engine();
    source_goal = base_source_goal;
    source_goal.grant = GrantGoalKey::Class(class.id);
    source.prime_grant_context([class]).unwrap();
    source
        .optimize(source_group, source_goal, SearchMode::Memo)
        .unwrap();
    let plan = source
        .export_seed_plan(source_group, source_goal)
        .unwrap()
        .expect("source winner should produce a seed plan");

    let (mut target, target_group, base_target_goal) = strong_seed_engine();
    let mut target_goal = base_target_goal;
    target_goal.grant = GrantGoalKey::Class(class.id);
    target.prime_grant_context([class]).unwrap();
    let priced = target
        .reprice_seed_plan_for_goal(target_group, target_goal, plan)
        .unwrap();
    let changed_class = crate::physical::ResourceGrantClass {
        hard_memory_bytes: 2048,
        ..class
    };
    target.prime_grant_context([changed_class]).unwrap();
    let error = target
        .install_priced_incumbent(priced)
        .expect_err("a grant operating point change must invalidate the old price");
    assert!(error.to_string().contains("cost context changed"));
}

#[test]
fn seed_plan_repricing_handles_memo_group_id_renaming() {
    let (mut source, source_group, source_goal) = strong_seed_engine();
    source
        .optimize(source_group, source_goal, SearchMode::Memo)
        .unwrap();
    let plan = source
        .export_seed_plan(source_group, source_goal)
        .unwrap()
        .expect("source winner should produce a seed plan");
    let identity = plan.plan_identity();

    let (mut target, target_group, target_goal) = strong_seed_engine_with_prefix(true);
    let priced = target
        .reprice_seed_plan_for_goal(target_group, target_goal, plan)
        .unwrap();
    assert_eq!(priced.plan().plan_identity(), identity);
    target.install_priced_incumbent(priced).unwrap();
    let winner = target
        .optimize(target_group, target_goal, SearchMode::Memo)
        .unwrap();
    assert_eq!(winner.cost.score.range.expected, 1.0);
}

#[test]
fn strong_seed_keeps_optimal_result_consistent_through_grant_entry_and_pruning_toggle() {
    let (mut source, source_group, source_goal) = strong_seed_engine();
    source
        .optimize(source_group, source_goal, SearchMode::Memo)
        .unwrap();
    let plan = source
        .export_seed_plan(source_group, source_goal)
        .unwrap()
        .expect("source winner should produce a seed plan");

    let class = crate::physical::ResourceGrantClass {
        id: ResourceGrantClassId(1),
        hard_memory_bytes: u64::MAX,
        spill_policy: crate::physical::SpillPolicy::Allowed,
        max_parallel_tasks: 1,
    };
    let mut results = Vec::new();
    for pruning in [false, true] {
        let (mut target, target_group, target_goal) = strong_seed_engine();
        let installed = target
            .install_strong_incumbent_for_grants(
                target_group,
                target_goal,
                AdmissibleGrantSetId(0),
                [class],
                std::slice::from_ref(&plan),
            )
            .unwrap();
        assert_eq!(installed, 1);
        target.set_certified_group_pruning_enabled(pruning);
        let optimization = target
            .optimize_for_grants(
                target_group,
                target_goal,
                AdmissibleGrantSetId(0),
                [class],
                SearchMode::Memo,
            )
            .unwrap();
        assert_eq!(optimization.winners.len(), 1);
        assert_eq!(
            target.search_work_counters()["strong_incumbent_seed_count"],
            1
        );
        results.push(optimization.winners[0].winner.cost);
    }
    assert_eq!(results[0], results[1]);
}

#[test]
fn frozen_winner_keeps_exact_payload_after_frontier_reset() {
    let (mut engine, group, goal) = engine(8);
    let winner = engine.optimize(group, goal, SearchMode::Memo).unwrap();
    let reference = ChildWinnerRef {
        group,
        goal,
        candidate: winner.candidate,
    };
    let frozen = engine.memo().freeze_candidate_tree(reference).unwrap();

    assert_eq!(frozen.reference, reference);
    assert_eq!(frozen.winner.candidate, winner.candidate);
    assert_eq!(frozen.physical.id, winner.expression);
    assert_eq!(frozen.logical.id, frozen.physical.key.logical);
    assert_eq!(frozen.children.len(), winner.children.len());

    // A later cost epoch may clear the live frontier, but it must not erase
    // the immutable handoff artifact or require a search pass to reconstruct
    // its physical payload.
    engine.memo_mut().clear_cost_frontiers().unwrap();
    assert_eq!(frozen.winner.candidate, winner.candidate);
    assert_eq!(frozen.physical.id, winner.expression);
}

#[test]
fn grant_deadline_returns_a_frozen_resource_safe_incumbent() {
    let budget = super::super::budget::SearchBudget {
        optional_time_limit: Some(Duration::ZERO),
        ..Default::default()
    };
    let (mut engine, group, goal) = engine_with_budget(budget);
    let optimized = engine
        .optimize_for_grants(
            group,
            goal,
            AdmissibleGrantSetId(0),
            [ResourceGrantClass {
                id: ResourceGrantClassId(1),
                hard_memory_bytes: 1 << 30,
                max_parallel_tasks: 1,
                spill_policy: SpillPolicy::Allowed,
            }],
            SearchMode::Memo,
        )
        .unwrap();

    assert_eq!(optimized.stop.reason, SearchStopReason::Deadline);
    assert!(!optimized.stop.budget_limited);
    assert!(optimized.stop.actual_stop_us.is_some());
    let winner = &optimized.winners[0];
    assert_eq!(winner.winner.candidate, winner.frozen.winner.candidate);
    assert_eq!(winner.frozen.reference.group, group);
    crate::cascades::verifier::WinnerVerifier::verify_candidate_tree(
        engine.memo(),
        ChildWinnerRef {
            group,
            goal: winner.goal,
            candidate: winner.winner.candidate,
        },
    )
    .unwrap();
}

#[test]
fn rule_work_profile_is_opt_in_for_diagnostic_cohorts() {
    let (mut normal, group, goal) = engine(8);
    normal.optimize(group, goal, SearchMode::Memo).unwrap();
    assert!(normal.rule_work_profile().is_empty());
    assert_eq!(normal.search_milestones(), &SearchMilestones::default());

    let (mut diagnostic, group, goal) = engine(8);
    diagnostic.set_rule_work_profile_enabled(true);
    diagnostic.optimize(group, goal, SearchMode::Memo).unwrap();
    assert!(!diagnostic.rule_work_profile().is_empty());
    assert!(diagnostic
        .rule_work_profile()
        .values()
        .any(|profile| profile.first_discovered_us.is_some()));
    assert!(diagnostic.search_milestones().first_safe_us.is_some());
    assert!(diagnostic
        .search_milestones()
        .first_optional_ready_us
        .is_some());
}

#[test]
fn diagnostic_search_checkpoints_keep_exact_goal_and_candidate_quality() {
    let (mut engine, group, goal) = engine(8);
    engine.set_rule_work_profile_enabled(true);
    engine.optimize(group, goal, SearchMode::Memo).unwrap();
    let winner = engine
        .memo()
        .group(group)
        .and_then(|group| group.winner(goal))
        .cloned()
        .expect("synthetic engine must have a root winner");

    engine.begin_diagnostic_profile(group, std::iter::once(goal));
    engine.diagnostic_search_complete = true;
    engine.profile_started_at = Some(Instant::now() - Duration::from_millis(125));
    engine.record_search_checkpoints(group);

    assert_eq!(engine.search_milestones().search_checkpoints.len(), 5);
    for checkpoint in &engine.search_milestones().search_checkpoints {
        assert_eq!(checkpoint.goal, goal);
        assert!(checkpoint.observed_us >= checkpoint.target_ms * 1_000);
        assert_eq!(checkpoint.candidate, Some(winner.candidate));
        assert_eq!(
            checkpoint.expected_cost,
            Some(winner.cost.score.range.expected)
        );
        assert_eq!(
            checkpoint.risk_adjusted_cost,
            Some(winner.cost.score.risk_adjusted)
        );
        assert_eq!(checkpoint.upper_cost, Some(winner.cost.score.range.upper));
        assert!(checkpoint.search_complete);
    }
}

#[test]
fn zero_wall_budget_returns_a_verified_incumbent_without_charging_work() {
    let budget = super::super::budget::SearchBudget {
        optional_time_limit: Some(Duration::ZERO),
        ..Default::default()
    };
    let (mut engine, group, goal) = engine_with_budget(budget);
    let winner = engine.optimize(group, goal, SearchMode::Memo).unwrap();
    assert_eq!(winner.physical_fingerprint, Fingerprint(10));
    assert!(engine.rule_attempts().is_empty());
    assert_eq!(engine.memo.search_obligations().len(), 1);
    assert_eq!(
        engine.memo.search_obligations()[0].reason,
        crate::cascades::budget::SearchIncompleteReason::Deadline
    );
    assert_eq!(
        engine
            .memo
            .group(group)
            .unwrap()
            .ledger
            .consumed(BudgetDimension::RuleWorkPerGroup),
        0
    );
    crate::cascades::verifier::WinnerVerifier::verify(&engine.memo).unwrap();
}

#[test]
fn expired_search_can_produce_an_incumbent_for_a_new_requirement_without_new_credit() {
    let budget = super::super::budget::SearchBudget {
        optional_time_limit: Some(Duration::ZERO),
        ..Default::default()
    };
    let (mut engine, group, goal) = engine_with_budget(budget);
    engine.optimize(group, goal, SearchMode::Memo).unwrap();
    let row_goal = OptimizationGoal {
        row_goal: RowGoal::AtMost(1),
        ..goal
    };
    let winner = engine.optimize(group, row_goal, SearchMode::Memo).unwrap();
    assert_eq!(winner.physical_fingerprint, Fingerprint(10));
    assert!(engine.memo.control().deadline_reached());
    assert!(!engine.memo.control().checkpoint().unwrap());
    assert!(engine.rule_attempts().is_empty());
    crate::cascades::verifier::WinnerVerifier::verify(&engine.memo).unwrap();
}

struct StopAfterMemoWrite {
    cancel: bool,
}

impl TransformationRule for StopAfterMemoWrite {
    fn id(&self) -> RuleId {
        RuleId(906)
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
        ctx.memo_mut().create_group(
            schema(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        if self.cancel {
            // Both can become visible at the same boundary. The statement
            // cancellation must win over the optional-search fallback.
            ctx.memo().control().expire();
            return Err(paro_error::query_canceled());
        }
        ctx.memo().control().expire();
        AddEquivalent.apply(expr, ctx)
    }
}

#[test]
fn deadline_mid_attempt_rolls_back_and_extracts_the_archived_incumbent() {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.disable_transformation(RuleId(5));
    let (mut engine, group, goal) = engine_with_budget(budget);
    engine
        .registry
        .register_transformation(StopAfterMemoWrite { cancel: false })
        .unwrap();
    let winner = engine.optimize(group, goal, SearchMode::Memo).unwrap();
    assert_eq!(engine.memo.group_count(), 1);
    assert_eq!(winner.physical_fingerprint, Fingerprint(10));
    assert!(
        engine.memo.group(group).unwrap().winner(goal).is_none(),
        "new cost epoch has no complete root"
    );
    let archived = engine
        .memo
        .resolve_child_winner(ChildWinnerRef {
            group,
            goal,
            candidate: winner.candidate,
        })
        .unwrap();
    assert_eq!(archived.physical_fingerprint, winner.physical_fingerprint);
    crate::cascades::verifier::WinnerVerifier::verify_candidate_tree(
        &engine.memo,
        ChildWinnerRef {
            group,
            goal,
            candidate: winner.candidate,
        },
    )
    .unwrap();
    assert_eq!(engine.memo.search_obligations().len(), 1);
    assert_eq!(
        engine.memo.search_obligations()[0].reason,
        crate::cascades::budget::SearchIncompleteReason::Deadline
    );
}

#[test]
fn statement_cancellation_is_not_an_advisory_rule_failure() {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.disable_transformation(RuleId(5));
    let (mut engine, group, goal) = engine_with_budget(budget);
    engine
        .registry
        .register_transformation(StopAfterMemoWrite { cancel: true })
        .unwrap();
    let error = engine.optimize(group, goal, SearchMode::Memo).unwrap_err();
    assert!(error.is_query_canceled());
    assert_eq!(engine.memo.group_count(), 1);
    assert!(!engine
        .memo
        .search_obligations()
        .iter()
        .any(|obligation| matches!(
            obligation.reason,
            crate::cascades::budget::SearchIncompleteReason::RuleFailure { .. }
        )));
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
    assert!(!engine.memo().search_obligations().is_empty());
}

#[test]
fn shared_child_product_is_lazy_and_uses_immutable_candidate_references() {
    let (engine, group, goal) = engine(8);
    let candidate = ChildWinnerRef {
        group,
        goal,
        candidate: crate::cascades::ids::CandidateId::new(0),
    };
    let frontiers = vec![vec![candidate; 2]; 64];
    let mut batch = child_winner_combinations(&frontiers, 8);
    assert!(matches!(
        batch.completion,
        EnumerationCompletion::BudgetLimited {
            first_omitted_ordinal: 8,
            ..
        }
    ));
    assert_eq!(
        batch.combinations.len(),
        9,
        "eight candidates plus one denial witness"
    );
    assert_eq!(
        batch
            .combinations
            .frontiers
            .iter()
            .map(Vec::len)
            .sum::<usize>(),
        128
    );
    assert!(batch
        .combinations
        .all(|combination| combination.len() == 64));
    assert_eq!(
        engine.memo().group_count(),
        1,
        "enumeration cannot allocate Memo nodes"
    );
}

#[test]
fn incremental_child_combination_oracle_covers_only_the_frontier_delta() {
    fn frontiers(values: &[&[u32]]) -> Box<[Box<[CandidateId]>]> {
        values
            .iter()
            .map(|values| {
                let mut values = values
                    .iter()
                    .copied()
                    .map(|value| CandidateId::new(value as usize))
                    .collect::<Vec<_>>();
                values.sort_unstable();
                values.into_boxed_slice()
            })
            .collect::<Vec<_>>()
            .into_boxed_slice()
    }

    fn drain(state: &mut ChildCombinationState) -> Vec<Box<[CandidateId]>> {
        let mut result = Vec::new();
        while let Some((children, _mandatory)) = state.next_unpriced_domain_tuple() {
            result.push(children);
        }
        result
    }

    let initial = frontiers(&[&[1, 2], &[10, 11]]);
    let mut state = ChildCombinationState::default();
    state.reset_for_context(
        initial.clone(),
        Fingerprint(1),
        0,
        Some(Box::new([CandidateId::new(2), CandidateId::new(10)])),
    );
    let mut seen = BTreeSet::new();
    for _ in 0..2 {
        seen.insert(
            state
                .next_unpriced_domain_tuple()
                .expect("the initial cursor has a resumable prefix")
                .0,
        );
    }
    assert_eq!(
        seen.len(),
        2,
        "the pause point has a stable two-tuple prefix"
    );

    // A one-sided addition resumes the two old tuples and adds only the two
    // products containing the new left candidate. The exact CandidateId
    // tuple, not its old frontier ordinal, is the coverage oracle.
    let grown_left = frontiers(&[&[1, 2, 3], &[10, 11]]);
    state.observe_frontiers(grown_left.clone());
    let left_delta = drain(&mut state).into_iter().collect::<BTreeSet<_>>();
    assert_eq!(left_delta.len(), 4, "two old and two new tuples resume");
    assert_eq!(
        left_delta
            .iter()
            .filter(|children| children[0] == CandidateId::new(3))
            .count(),
        2
    );
    seen.extend(left_delta);
    assert_eq!(seen.len(), 6);

    // Adding a candidate on the other side creates the three products in its
    // new slice, including the cross-product with the earlier addition, but
    // does not reopen any of the six tuples already covered.
    let grown = frontiers(&[&[1, 2, 3], &[10, 11, 12]]);
    state.observe_frontiers(grown.clone());
    let right_delta = drain(&mut state).into_iter().collect::<BTreeSet<_>>();
    assert_eq!(right_delta.len(), 3);
    assert!(right_delta
        .iter()
        .all(|children| children[1] == CandidateId::new(12)));
    assert!(seen.is_disjoint(&right_delta));
    seen.extend(right_delta);
    assert_eq!(seen.len(), 9, "the full 3-by-3 product is covered once");

    // A deterministic objective over the exact tuples is an independent
    // quality oracle: with enough budget the best tuple must remain visible,
    // regardless of the pause or frontier publication batches.
    let best = seen
        .iter()
        .min_by_key(|children| {
            (
                children[0].index() + children[1].index(),
                children[0],
                children[1],
            )
        })
        .expect("the product oracle is non-empty");
    assert_eq!(best.as_ref(), &[CandidateId::new(1), CandidateId::new(10)]);

    // A reorder is represented by the same stable ID set and a crop removes
    // choices from the active domain; neither operation reopens a priced
    // product or manufactures a new tuple.
    state.observe_frontiers(frontiers(&[&[3, 2, 1], &[12, 10, 11]]));
    assert!(drain(&mut state).is_empty());
    state.observe_frontiers(frontiers(&[&[2, 3], &[10, 11]]));
    assert!(drain(&mut state).is_empty());

    // A changed cost/fact context explicitly restarts the cursor. This is the
    // invalidation boundary; frontier growth alone did not restart it.
    state.reset_for_context(
        grown,
        Fingerprint(2),
        1,
        Some(Box::new([CandidateId::new(2), CandidateId::new(10)])),
    );
    assert!(state.priced.is_empty());
    assert!(state.resource_rejected.is_empty());
    assert!(state.budget_rejected.is_empty());
    assert!(!drain(&mut state).is_empty());
}

#[test]
fn child_combination_event_interning_is_exact() {
    let (mut engine, group, goal) = engine(0);
    let left = ChildWinnerRef {
        group,
        goal,
        candidate: CandidateId::new(1),
    };
    let right = ChildWinnerRef {
        group,
        goal,
        candidate: CandidateId::new(2),
    };
    let first = [left, right];
    let reordered = [right, left];
    let event = engine
        .intern_child_combination_event(PhysicalExprId(7), goal, Fingerprint(11), &first)
        .unwrap();
    assert_eq!(
        event,
        engine
            .intern_child_combination_event(PhysicalExprId(7), goal, Fingerprint(11), &first)
            .unwrap()
    );
    assert_ne!(
        event,
        engine
            .intern_child_combination_event(PhysicalExprId(7), goal, Fingerprint(11), &reordered)
            .unwrap()
    );
    assert_eq!(engine.child_combination_events.len(), 2);
    assert!(event.0 & (1_u128 << 127) != 0);
}

#[test]
fn binding_enumeration_work_is_charged_once_in_its_declared_pool() {
    let mut budget = super::super::budget::SearchBudget::default();
    budget.disable_transformation(RuleId(5));
    budget.max_rule_work_units_per_group = 0;
    budget.max_rule_firings_per_group = 0;
    budget.max_optional_logical_exprs_per_group = 0;
    budget.max_composition_rule_work_units_per_group = 3;
    budget.max_composition_rule_firings_per_group = 2;
    budget.max_optional_composition_logical_exprs_per_group = 2;
    let (mut engine, group, _) = engine_with_budget(budget);
    engine
        .registry
        .register_transformation(EnumerateTwoCompositionBindings)
        .unwrap();

    engine.explore_transformations().unwrap();

    let group_ref = engine.memo.group(group).unwrap();
    assert!(group_ref.logical_exprs().iter().any(|expression| {
        engine
            .memo
            .logical_expr(*expression)
            .is_some_and(|logical| logical.key.operator == Fingerprint(11))
    }));
    assert_eq!(
        group_ref.ledger.consumed(BudgetDimension::RuleWorkPerGroup),
        0
    );
    assert_eq!(
        group_ref.ledger.consumed(BudgetDimension::RuleFirePerGroup),
        0
    );
    assert_eq!(
        group_ref
            .ledger
            .consumed(BudgetDimension::LogicalExprPerGroup),
        0
    );
    assert_eq!(
        group_ref
            .ledger
            .consumed(BudgetDimension::CompositionRuleWorkPerGroup),
        3,
        "one matcher frontier costs three units regardless of its two bindings"
    );
    assert_eq!(
        group_ref
            .ledger
            .exhaustion_events()
            .filter(|(dimension, _)| { *dimension == BudgetDimension::CompositionRuleWorkPerGroup })
            .count(),
        0,
        "a second binding must not repay the matcher frontier"
    );
    assert_eq!(
        group_ref
            .ledger
            .consumed(BudgetDimension::CompositionRuleFirePerGroup),
        2
    );
    assert_eq!(
        group_ref
            .ledger
            .consumed(BudgetDimension::CompositionLogicalExprPerGroup),
        1,
        "the duplicate second output releases its composition reservation"
    );
}

#[test]
fn saturated_transformation_cursor_invalidates_when_an_observed_frontier_advances() {
    let (mut engine, owner, _) = engine(8);
    let expression = engine.memo.group(owner).unwrap().logical_exprs()[0];
    let observed = engine.memo_mut().create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    engine
        .memo_mut()
        .insert_logical(
            observed,
            LogicalExprKey {
                operator: Fingerprint(40),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(40),
            EquivalenceProof::Initial,
        )
        .unwrap();
    let task = TransformationTaskId {
        group: owner,
        expression,
        rule: RuleId(5),
    };
    let read = PatternRead::from_group(engine.memo(), observed).unwrap();

    engine
        .seed_transformation_observation(task, &[read])
        .unwrap();
    assert!(engine.transformation_observation_is_current(task).unwrap());

    engine
        .memo_mut()
        .insert_logical(
            observed,
            LogicalExprKey {
                operator: Fingerprint(41),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(41),
            EquivalenceProof::Normalization { rule: RuleId(41) },
        )
        .unwrap();

    assert!(!engine.transformation_observation_is_current(task).unwrap());
}

#[test]
fn incremental_binding_applications_reuse_old_matches_but_refresh_changed_facts() {
    use super::super::rules::PatternOperand;

    struct CountBindings(Arc<AtomicUsize>);
    impl TransformationRule for CountBindings {
        fn id(&self) -> RuleId {
            RuleId(903)
        }
        fn matches_root(&self, expr: &super::super::memo::LogicalExpr) -> bool {
            expr.key.operator == Fingerprint(50)
        }
        fn matches(&self, expr: &super::super::memo::LogicalExpr, _: &RuleContext<'_>) -> bool {
            self.matches_root(expr)
        }
        fn bindings(
            &self,
            expression: LogicalExprId,
            ctx: &RuleContext<'_>,
        ) -> Result<PatternBindingSet> {
            let child = ctx.memo.logical_expr(expression).unwrap().key.children[0];
            Ok(PatternBindingSet {
                bindings: ctx
                    .memo
                    .group(child)
                    .unwrap()
                    .logical_exprs()
                    .iter()
                    .map(|alternative| PatternBinding {
                        root: PatternOperand::Expression {
                            group: ctx.group,
                            expression,
                            children: Box::new([PatternOperand::Expression {
                                group: child,
                                expression: *alternative,
                                children: Box::new([]),
                            }]),
                        },
                        // Deliberately collide: the fingerprint is only a bucket.
                        fingerprint: Fingerprint(0),
                    })
                    .collect(),
                reads: Box::new([
                    PatternRead::from_group(ctx.memo, child)?,
                    PatternRead::facts_from_group(ctx.memo, ctx.group)?,
                ]),
                work_units: 1,
                work_dimension: BudgetDimension::RuleWorkPerGroup,
                completion: PatternEnumerationCompletion::Complete,
            })
        }
        fn binding_reads(
            &self,
            _: &PatternBinding,
            reads: &[PatternRead],
            ctx: &RuleContext<'_>,
        ) -> Result<Box<[PatternRead]>> {
            reads
                .iter()
                .map(|read| PatternRead::facts_from_group(ctx.memo, read.group))
                .collect()
        }
        fn apply(
            &self,
            _: LogicalExprId,
            _: &mut TransformContext<'_>,
        ) -> Result<Box<[EquivalentExpression]>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new([]))
        }
    }

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
    let calls = Arc::new(AtomicUsize::new(0));
    engine
        .registry
        .register_transformation(CountBindings(calls.clone()))
        .unwrap();
    engine.explore_transformations().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    engine
        .memo_mut()
        .insert_logical(
            child,
            LogicalExprKey {
                operator: Fingerprint(41),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(41),
            EquivalenceProof::Normalization { rule: RuleId(41) },
        )
        .unwrap();
    engine.explore_transformations().unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "only the new binding runs, even with colliding fingerprints"
    );

    engine.memo_mut().group_mut(child).unwrap().cardinality = GroupCardinality::new(
        Fingerprint(991),
        super::super::memo::CardinalityRecipeKind::Statistics,
        1,
        4,
        9,
    );
    engine.explore_transformations().unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        4,
        "new child facts invalidate both applications"
    );
    engine
        .memo_mut()
        .group_mut(root)
        .unwrap()
        .logical_properties
        .maximum_cardinality = Some(100);
    engine.explore_transformations().unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        6,
        "root facts are application dependencies too"
    );
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
    assert!(
        engine
            .memo
            .search_obligations()
            .iter()
            .any(|obligation| matches!(
                obligation.reason,
                super::super::budget::SearchIncompleteReason::RuleFailure { .. }
            )),
        "an advisory failure is not a complete search"
    );
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
fn application_only_fact_reads_wake_a_completed_negative_match() {
    struct ReadFacts {
        group: GroupId,
        calls: Arc<AtomicUsize>,
    }
    impl TransformationRule for ReadFacts {
        fn id(&self) -> RuleId {
            RuleId(904)
        }
        fn matches_root(&self, expression: &super::super::memo::LogicalExpr) -> bool {
            expression.key.operator == Fingerprint(10)
        }
        fn matches(
            &self,
            expression: &super::super::memo::LogicalExpr,
            _: &RuleContext<'_>,
        ) -> bool {
            self.matches_root(expression)
        }
        fn apply(
            &self,
            _: LogicalExprId,
            context: &mut TransformContext<'_>,
        ) -> Result<Box<[EquivalentExpression]>> {
            assert!(context.admit_fact_work(BudgetDimension::RuleWorkPerGroup, 1)?);
            context.record_fact_read(PatternRead::from_group(context.memo(), self.group)?);
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new([]))
        }
    }
    let mut budget = super::super::budget::SearchBudget::default();
    budget.disable_transformation(RuleId(5));
    let (mut engine, root, _) = engine_with_budget(budget);
    let evidence = engine.memo_mut().create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    engine
        .registry
        .register_transformation(ReadFacts {
            group: evidence,
            calls: calls.clone(),
        })
        .unwrap();
    engine.explore_transformations().unwrap();
    engine.explore_transformations().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    engine
        .memo_mut()
        .group_mut(evidence)
        .unwrap()
        .logical_properties
        .maximum_cardinality = Some(1);
    engine.explore_transformations().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    engine
        .memo_mut()
        .insert_logical(
            evidence,
            LogicalExprKey {
                operator: Fingerprint(61),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(61),
            EquivalenceProof::Initial,
        )
        .unwrap();
    engine.explore_transformations().unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "a non-selected alternative is evidence too"
    );
    assert_eq!(engine.memo().group(root).unwrap().logical_exprs().len(), 1);
}

#[test]
fn equal_resolved_fact_values_refresh_cursors_without_reapplying() {
    struct ReadValueFacts {
        group: GroupId,
        calls: Arc<AtomicUsize>,
        value: Arc<AtomicUsize>,
    }
    impl TransformationRule for ReadValueFacts {
        fn id(&self) -> RuleId {
            RuleId(905)
        }
        fn matches_root(&self, expression: &super::super::memo::LogicalExpr) -> bool {
            expression.key.operator == Fingerprint(10)
        }
        fn matches(
            &self,
            expression: &super::super::memo::LogicalExpr,
            _: &RuleContext<'_>,
        ) -> bool {
            self.matches_root(expression)
        }
        fn binding_fact_value(
            &self,
            _: &PatternBinding,
            context: &mut TransformContext<'_>,
        ) -> Result<Option<Fingerprint>> {
            context.record_fact_read(PatternRead::from_group(context.memo(), self.group)?);
            Ok(Some(Fingerprint(self.value.load(Ordering::SeqCst) as u128)))
        }
        fn apply(
            &self,
            _: LogicalExprId,
            context: &mut TransformContext<'_>,
        ) -> Result<Box<[EquivalentExpression]>> {
            context.record_fact_read(PatternRead::from_group(context.memo(), self.group)?);
            context.record_fact_value(Fingerprint(self.value.load(Ordering::SeqCst) as u128));
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new([]))
        }
    }

    let mut budget = super::super::budget::SearchBudget::default();
    budget.disable_transformation(RuleId(5));
    let (mut engine, root, _) = engine_with_budget(budget);
    let evidence = engine.memo_mut().create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let value = Arc::new(AtomicUsize::new(777));
    engine
        .registry
        .register_transformation(ReadValueFacts {
            group: evidence,
            calls: calls.clone(),
            value: value.clone(),
        })
        .unwrap();
    engine.explore_transformations().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    engine
        .memo_mut()
        .insert_logical(
            evidence,
            LogicalExprKey {
                operator: Fingerprint(61),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(61),
            EquivalenceProof::Initial,
        )
        .unwrap();
    engine.explore_transformations().unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a recipe-only revision with the same resolved fact value is a cache hit"
    );
    value.store(778, Ordering::SeqCst);
    engine
        .memo_mut()
        .group_mut(evidence)
        .unwrap()
        .logical_properties
        .maximum_cardinality = Some(1);
    engine.explore_transformations().unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "a changed resolved fact value invalidates the cached application"
    );
    assert_eq!(engine.memo().group(root).unwrap().logical_exprs().len(), 1);
}

#[test]
fn later_binding_observations_keep_all_application_fact_subscriptions() {
    let (mut engine, root, _) = engine_with_budget(super::super::budget::SearchBudget::default());
    let evidence = engine.memo_mut().create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let task = TransformationTaskId {
        group: root,
        expression: engine.memo().group(root).unwrap().logical_exprs()[0],
        rule: RuleId(904),
    };
    let root_read = PatternRead::from_group(engine.memo(), root).unwrap();
    let fact_read = PatternRead::from_group(engine.memo(), evidence).unwrap();
    engine
        .transformation_fact_observations
        .insert(task, vec![fact_read]);
    engine
        .seed_transformation_observation(task, &[root_read, fact_read])
        .unwrap();
    engine
        .seed_transformation_observation(task, &[root_read])
        .unwrap();
    assert!(engine.transformation_subscribers[&evidence].contains(&task));
    engine
        .memo_mut()
        .group_mut(evidence)
        .unwrap()
        .logical_properties
        .maximum_cardinality = Some(1);
    assert!(!engine.transformation_observation_is_current(task).unwrap());
}

#[test]
fn subscription_delta_matches_an_independent_set_difference() {
    fn reads(mask: u32, revision: u64) -> Vec<PatternRead> {
        let mut result = Vec::new();
        for group in 0..7 {
            if mask & (1 << group) == 0 {
                continue;
            }
            // A single subscription can carry both a facts-only and a
            // frontier read; revision changes must not rewrite membership.
            for frontier in [None, Some(revision)] {
                result.push(PatternRead {
                    group: GroupId(group),
                    logical_frontier_revision: frontier,
                    physical_frontier_revision: None,
                    logical_fact_fingerprint: Fingerprint(revision.into()),
                    statistics_snapshot_fingerprint: Fingerprint(u128::from(revision) + 1),
                });
            }
        }
        result
    }
    for before in 0..128 {
        for after in 0..128 {
            let left = reads(before, 10);
            let right = reads(after, 20);
            let a = left.iter().map(|read| read.group).collect::<BTreeSet<_>>();
            let b = right.iter().map(|read| read.group).collect::<BTreeSet<_>>();
            let expected = a
                .difference(&b)
                .map(|group| (*group, false))
                .chain(b.difference(&a).map(|group| (*group, true)))
                .collect::<BTreeSet<_>>();
            let mut actual = Vec::new();
            visit_read_group_delta(&left, &right, |group, subscribe| {
                actual.push((group, subscribe))
            });
            assert_eq!(
                actual.len(),
                expected.len(),
                "no duplicate membership mutations"
            );
            assert_eq!(actual.into_iter().collect::<BTreeSet<_>>(), expected);
        }
    }
}

#[test]
fn physical_child_frontier_change_is_narrowed_to_dependent_recipes() {
    let owner = GroupId::new(10);
    let child = GroupId::new(11);
    let owner_read = PatternRead {
        group: owner,
        logical_frontier_revision: Some(1),
        physical_frontier_revision: None,
        logical_fact_fingerprint: Fingerprint(2),
        statistics_snapshot_fingerprint: Fingerprint(3),
    };
    let previous = ReadSet::new([
        owner_read,
        PatternRead {
            group: child,
            logical_frontier_revision: Some(4),
            physical_frontier_revision: Some(5),
            logical_fact_fingerprint: Fingerprint(6),
            statistics_snapshot_fingerprint: Fingerprint(7),
        },
    ]);
    let current = ReadSet::new([
        owner_read,
        PatternRead {
            group: child,
            physical_frontier_revision: Some(8),
            ..previous.reads()[1]
        },
    ]);

    assert_eq!(
        physical_changed_child_groups(owner, Some(&previous), &current),
        BTreeSet::from([child])
    );
    assert!(!physical_read_requires_full_recost(
        owner,
        Some(&previous),
        &current
    ));

    let missing_child = ReadSet::new([owner_read]);
    assert!(physical_read_requires_full_recost(
        owner,
        Some(&previous),
        &missing_child
    ));
}

#[test]
fn repeated_fact_cursors_update_in_place_without_downgrading_frontier_reads() {
    let (mut engine, root, _) = engine_with_budget(super::super::budget::SearchBudget::default());
    let task = TransformationTaskId {
        group: root,
        expression: engine.memo().group(root).unwrap().logical_exprs()[0],
        rule: RuleId(904),
    };
    let frontier = PatternRead::from_group(engine.memo(), root).unwrap();
    let mut observations = BTreeMap::new();
    CascadesEngine::merge_transformation_fact_reads(
        engine.memo(),
        &mut observations,
        task,
        &[frontier],
    )
    .unwrap();
    let allocation = observations[&task].as_ptr();
    for maximum in 0..100 {
        engine
            .memo_mut()
            .group_mut(root)
            .unwrap()
            .logical_properties
            .maximum_cardinality = Some(maximum);
        let facts = PatternRead::facts_from_group(engine.memo(), root).unwrap();
        CascadesEngine::merge_transformation_fact_reads(
            engine.memo(),
            &mut observations,
            task,
            &[facts],
        )
        .unwrap();
        assert_eq!(
            observations[&task].as_slice(),
            &[PatternRead::from_group(engine.memo(), root).unwrap()]
        );
        assert_eq!(observations[&task].as_ptr(), allocation);
    }
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
            operator_encoding: None,
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
    .expect("sort may spill under an allowed grant")
    .phase()
    .expect("a sort creates an execution phase");
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
    let source_work = [SourceWorkData {
        source,
        source_rows: 100,
        base_cost: child.work_only(),
        cost: child.work_only(),
        retentions: Box::new([]),
        filters: Box::new([]),
        filter_apply_cost: SearchCost::ZERO,
        phased_cost: child.work_only(),
        phase_tasks: 1,
    }
    .into()];
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
fn retained_memory_matches_an_independent_phase_footprint_oracle() {
    let mut points = Vec::new();
    for floor in [0, 10, 20] {
        for elastic in [0, 30] {
            for extra_peak in [0, 10] {
                points.push(SearchCost {
                    non_revocable_memory_upper: floor,
                    minimum_memory_bytes: floor,
                    revocable_memory_target: elastic,
                    peak_memory_upper: floor + elastic + extra_peak,
                    ..SearchCost::ZERO
                });
            }
        }
    }
    // Enumerate real phases independently of the summary-composition helper:
    // each input runs alone or overlaps retained parent state, never another
    // sibling. One elastic query pool services the largest active working set.
    for &local in &points {
        for &left in &points {
            for &right in &points {
                for mask in 0..4 {
                    let children = [left, right];
                    let phases = [
                        vec![local],
                        vec![left],
                        vec![right],
                        if mask & 1 != 0 {
                            vec![local, left]
                        } else {
                            vec![]
                        },
                        if mask & 2 != 0 {
                            vec![local, right]
                        } else {
                            vec![]
                        },
                    ];
                    let mut floor = 0;
                    let mut preferred = 0;
                    let mut peak = 0;
                    for phase in phases {
                        let phase_floor = phase.iter().map(|p| p.minimum_memory_bytes).sum::<u64>();
                        let elastic = phase
                            .iter()
                            .map(|p| p.revocable_memory_target)
                            .max()
                            .unwrap_or(0);
                        floor = floor.max(phase_floor);
                        preferred = preferred.max(phase_floor + elastic);
                        peak = peak
                            .max(phase.iter().map(|p| p.peak_memory_upper).max().unwrap_or(0))
                            .max(phase_floor + elastic);
                    }
                    let actual = compose_candidate_cost_with_sources(
                        local,
                        None,
                        &children,
                        &[&[], &[]],
                        CostComposition::RetainedState {
                            overlapping_children: mask,
                        },
                    )
                    .unwrap()
                    .cost;
                    assert_eq!(
                        (
                            actual.minimum_memory_bytes,
                            actual.preferred_memory_bytes(),
                            actual.peak_memory_upper
                        ),
                        (floor, preferred, peak),
                        "local={local:?} children={children:?} mask={mask}"
                    );
                }
            }
        }
    }
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
        SourceWorkData {
            source: unique_source,
            source_rows: 100,
            base_cost: cost(100.0),
            cost: cost(100.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(100.0),
            phase_tasks: 1,
        }
        .into(),
        SourceWorkData {
            source: repeated_source,
            source_rows: 300,
            base_cost: cost(300.0),
            cost: cost(300.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(300.0),
            phase_tasks: 1,
        }
        .into(),
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
        SourceWorkData {
            source: matched,
            source_rows: 100,
            base_cost: cost(100.0),
            cost: cost(100.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(100.0),
            phase_tasks: 1,
        }
        .into(),
        SourceWorkData {
            source: unrelated,
            source_rows: 300,
            base_cost: cost(300.0),
            cost: cost(300.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(300.0),
            phase_tasks: 1,
        }
        .into(),
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
        5.0,
        "predicate work is attributed by the matched immutable row domain"
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
fn source_filter_charges_full_evaluation_domain_after_prior_retention() {
    let source = WorkSourceId(2_001);
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
    // A previous runtime filter has already reduced the lane's current work
    // to ten units.  The next predicate still evaluates its immutable input
    // domain and therefore costs the full operator-local term, not ten units.
    let reduced = SourceWorkData {
        cost: cost(10.0),
        phased_cost: cost(10.0),
        ..scan.source_work[0].snapshot().clone()
    }
    .into();
    let filtered = compose_candidate_cost_with_sources(
        cost(100.0),
        Some(cost(100.0)),
        &[cost(10.0)],
        &[std::slice::from_ref(&reduced)],
        CostComposition::SidewaysFilter {
            overlapping_children: 0,
            filtered_child: 0,
            sources: Box::new([retained_source(source, 500_000, 500_000)]),
        },
    )
    .unwrap();
    assert_eq!(
        filtered.source_work[0]
            .filter_apply_cost
            .work_latency
            .expected,
        100.0,
        "evaluation cost must use full_apply_cost, never the reduced lane cost"
    );
    assert_eq!(filtered.source_work[0].filters[0].evaluation_rows, 1_000);
}

#[test]
fn source_filter_keeps_unmatched_apply_work_on_the_parent() {
    let matched = WorkSourceId(2_010);
    let unrelated = WorkSourceId(2_011);
    let lanes = [
        SourceWorkData {
            source: matched,
            source_rows: 100,
            base_cost: cost(100.0),
            cost: cost(100.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(100.0),
            phase_tasks: 1,
        }
        .into(),
        SourceWorkData {
            source: unrelated,
            source_rows: 900,
            base_cost: cost(900.0),
            cost: cost(900.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(900.0),
            phase_tasks: 1,
        }
        .into(),
    ];
    let filtered = compose_candidate_cost_with_sources(
        cost(100.0),
        Some(cost(100.0)),
        &[cost(1_000.0)],
        &[&lanes],
        CostComposition::SidewaysFilter {
            overlapping_children: 0,
            filtered_child: 0,
            sources: Box::new([retained_source(matched, 500_000, 500_000)]),
        },
    )
    .unwrap();
    assert_eq!(filtered.source_work[0].filters.len(), 1);
    assert!(filtered.source_work[1].filters.is_empty());
    assert!(!filtered.source_work[0].shares_payload(&lanes[0]));
    assert!(filtered.source_work[1].shares_payload(&lanes[1]));
    assert!(
        lanes[0].filters.is_empty(),
        "publishing a filtered response must not change its child"
    );
    assert_eq!(lanes[0].cost.score.range.expected, 100.0);
    assert_eq!(
        filtered.cost.score.range.expected, 1_050.0,
        "the 90% lineage gap must remain charged at the parent"
    );
}

#[test]
fn sideways_filter_accepts_multiple_lanes_for_one_source() {
    let source = WorkSourceId(21);
    let lanes = [
        SourceWorkData {
            source,
            source_rows: 100,
            base_cost: cost(100.0),
            cost: cost(100.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(100.0),
            phase_tasks: 1,
        }
        .into(),
        SourceWorkData {
            source,
            source_rows: 300,
            base_cost: cost(300.0),
            cost: cost(300.0),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(300.0),
            phase_tasks: 1,
        }
        .into(),
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
    let lanes = [SourceWorkData {
        source: unrelated,
        source_rows: 400,
        base_cost: cost(400.0),
        cost: cost(400.0),
        retentions: Box::new([]),
        filters: Box::new([]),
        filter_apply_cost: SearchCost::ZERO,
        phased_cost: cost(400.0),
        phase_tasks: 1,
    }
    .into()];
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
    assert!(independent_parent.source_work[0].shares_payload(&scan.source_work[0]));
    assert!(!first.source_work[0].shares_payload(&scan.source_work[0]));
    assert_eq!(scan.source_work[0].cost.score.range.expected, 100.0);
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
    assert!(duplicate.source_work[0].shares_payload(&first.source_work[0]));

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
            [1, 2].map(|id| ResourceGrantClass {
                id: ResourceGrantClassId(id),
                hard_memory_bytes: 1 << 30,
                max_parallel_tasks: 4,
                spill_policy: SpillPolicy::Allowed,
            }),
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
        .winner_frontier(winner.children[0].goal)
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
            .winner_frontier(winner.children[0].goal)
            .unwrap()
            .candidates()
            .len(),
        2
    );
    assert_ne!(winner.children[0].goal.context, goal.context);
    // A closed consumer cannot filter the source, so the locally dominated
    // response candidate is unnecessary there. The independent parent oracle
    // above still consumes both candidates in its explicit filter context.
    engine.optimize(child, goal, SearchMode::Direct).unwrap();
    assert_eq!(
        engine
            .memo()
            .group(child)
            .unwrap()
            .winner_frontier(goal)
            .unwrap()
            .candidates()
            .len(),
        1
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
    let lane = |rows, work| {
        SourceWorkData {
            source,
            source_rows: rows,
            base_cost: cost(work),
            cost: cost(work),
            retentions: Box::new([]),
            filters: Box::new([]),
            filter_apply_cost: SearchCost::ZERO,
            phased_cost: cost(work),
            phase_tasks: 1,
        }
        .into()
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
            .winner_frontier(winner.children[0].goal)
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

struct PipelineSupplyImplementation;

impl PhysicalImplementation for PipelineSupplyImplementation {
    fn id(&self) -> ImplementationId {
        ImplementationId(9_871)
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
        let source = logical.key.children.is_empty();
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
            local_cost: cost(10_000.0),
            source_filter_apply_cost: None,
            task_supply: if source {
                TaskSupplyContract::Source { tasks: 4 }
            } else {
                TaskSupplyContract::Streaming { input: 0 }
            },
            cost_composition: if source {
                CostComposition::Source {
                    source: WorkSourceId(9_871),
                    source_rows: 10_000,
                }
            } else {
                CostComposition::Sequential
            },
            spillable: false,
            enforcer_cost_input: EnforcerCostInput::unbounded(CompactRange::point(10_000.0)?, 8),
            physical_fingerprint: logical.key.operator,
            region: None,
            mandatory: true,
        }]))
    }
}

#[test]
fn empty_enforcement_preserves_source_supply_through_engine() {
    let mut memo = Memo::new(super::super::budget::SearchBudget::default());
    let source = memo.create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    let parent = memo.create_group(
        schema(),
        LogicalProperties::default(),
        GroupCardinality::default(),
    );
    for (group, children) in [(source, vec![]), (parent, vec![source])] {
        memo.insert_logical(
            group,
            LogicalExprKey {
                operator: Fingerprint(9_871 + group.0 as u128),
                scalars: Box::new([]),
                children: children.into_boxed_slice(),
            },
            LogicalPayloadId(group.0),
            EquivalenceProof::Initial,
        )
        .unwrap();
    }
    let goal = OptimizationGoal {
        required: memo.intern_required(required()).unwrap(),
        row_goal: RowGoal::All,
        objective: ObjectiveProfile::Latency,
        grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
        context: OptimizationContextId(0),
    };
    let mut registry = ImplementationRegistry::default();
    registry
        .register_implementation(PipelineSupplyImplementation)
        .unwrap();
    let mut engine = CascadesEngine::new(memo, registry);
    let leaf = engine.optimize(source, goal, SearchMode::Memo).unwrap();
    let parent = engine.optimize(parent, goal, SearchMode::Memo).unwrap();
    assert_eq!(leaf.cost.output_pipeline_tasks, 4);
    assert_eq!(parent.local_cost.output_pipeline_tasks, 4);
}

#[test]
fn one_pipeline_coordination_is_invariant_to_streaming_boundaries() {
    let calibration = MachineCalibrationBundle::default();
    let source = resolve_task_supply(
        cost(10_000.0),
        &[],
        &TaskSupplyContract::Source { tasks: 4 },
        &calibration,
    )
    .unwrap();
    let projection = resolve_task_supply(
        cost(10_000.0),
        &[source],
        &TaskSupplyContract::Streaming { input: 0 },
        &calibration,
    )
    .unwrap();
    let split = source.sequential(projection).unwrap();
    let fused = resolve_task_supply(
        cost(20_000.0),
        &[],
        &TaskSupplyContract::Source { tasks: 4 },
        &calibration,
    )
    .unwrap();
    assert_eq!(split.work_latency, fused.work_latency);
    assert_eq!(split.critical_path, fused.critical_path);
}
