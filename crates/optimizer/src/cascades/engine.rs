// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Deterministic mandatory-baseline plus bounded optional Cascades search.

use std::collections::{BTreeMap, BTreeSet};

use paro_common::error::{self as paro_error, Result};

use super::budget::{BudgetDecision, BudgetDimension};
use super::calibration::{
    LocalOperatorWork, MachineCalibrationBundle, ParallelWorkProfile, OP_ENFORCER_RANDOM_FETCH,
    OP_ENFORCER_SORT_COMPARE, OP_ENFORCER_SPILL_PAGE, OP_ENFORCER_STREAM_ROW,
};
use super::cost::{CompactRange, MemoryCompletion, ResourceDimension, SearchCost};
use super::enforcer::{EnforcementPlanner, EnforcerStep};
use super::grant::{derive_grant_sensitivity, verify_grant_invariance, GrantSensitivitySummary};
use super::ids::{
    AdmissibleGrantSetId, Fingerprint, GroupId, ImplementationId, LogicalExprId, PhysicalExprId,
    ResourceGrantClassId, RuleId, StableFingerprintBuilder,
};
use super::memo::{
    EquivalenceProof, GrantGoalKey, GroupCardinality, LogicalProperties, Memo, OptimizationGoal,
    Winner,
};
use super::region::{
    JointCostProof, RegionArtifactKind, RegionBoundaryEndpoint, RegionCandidateContract,
    RegionDependencyEdge, RegionDependencyKind,
};
#[cfg(test)]
use super::rules::WorkSourceId;
use super::rules::{
    CostComposition, ImplementationContext, ImplementationRegistry, PhysicalCandidate, RuleContext,
    SourceFilterWork, SourceWork, TransformContext,
};
use crate::physical::SpillPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    /// Narrow shapes: shared implementations/properties/costing, no equivalent
    /// relational exploration.
    Direct,
    /// Contextual Memo exploration with bounded transformations.
    Memo,
}

#[derive(Debug, Clone)]
pub struct GrantWinner {
    pub class: ResourceGrantClassId,
    pub goal: OptimizationGoal,
    pub winner: Winner,
}

#[derive(Debug, Clone)]
pub struct GrantOptimization {
    pub sensitivity: GrantSensitivitySummary,
    pub winners: Box<[GrantWinner]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum TaskKind {
    Transform,
    Implement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct TaskKey {
    priority: u16,
    kind: TaskKind,
    stable_id: u32,
    group: GroupId,
    expression: LogicalExprId,
    goal: Option<OptimizationGoal>,
}

struct TransformationInsertion {
    groups: BTreeSet<GroupId>,
    properties: Vec<(GroupId, LogicalProperties, GroupCardinality)>,
    expressions: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct TransformationTaskId {
    group: GroupId,
    expression: LogicalExprId,
    rule: RuleId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchTask {
    Transform {
        group: GroupId,
        expression: LogicalExprId,
        rule: RuleId,
    },
    Implement {
        group: GroupId,
        expression: LogicalExprId,
        implementation: ImplementationId,
        goal: OptimizationGoal,
    },
}

#[derive(Debug, Default)]
struct StableAgenda {
    tasks: BTreeMap<TaskKey, SearchTask>,
}

impl StableAgenda {
    fn push(&mut self, key: TaskKey, task: SearchTask) {
        self.tasks.entry(key).or_insert(task);
    }

    fn pop(&mut self) -> Option<SearchTask> {
        let key = self.tasks.keys().next().copied()?;
        self.tasks.remove(&key)
    }
}

#[derive(Debug, Clone)]
struct CostRecipe {
    child_goals: Box<[(GroupId, OptimizationGoal)]>,
    local_cost: SearchCost,
    source_filter_apply_cost: Option<SearchCost>,
    cost_composition: CostComposition,
    spillable: bool,
    enforcer_cost_input: EnforcerCostInput,
    physical_fingerprint: Fingerprint,
    region: Option<RegionCandidateContract>,
}

/// Fixed-size evidence used to replay property-enforcement cost. The row
/// interval is the candidate output estimate; the grant fields make blocking
/// enforcers part of feasibility rather than an extraction-time surprise.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EnforcerCostInput {
    pub rows: super::cost::CompactRange,
    pub row_width_bytes: u64,
    pub hard_memory_bytes: u64,
    pub spill_policy: SpillPolicy,
    pub max_parallel_tasks: u16,
}

impl EnforcerCostInput {
    pub fn unbounded(rows: super::cost::CompactRange, row_width_bytes: u64) -> Self {
        Self {
            rows,
            row_width_bytes: row_width_bytes.max(1),
            hard_memory_bytes: u64::MAX,
            spill_policy: SpillPolicy::Allowed,
            max_parallel_tasks: 1,
        }
    }
}

/// The engine is deliberately operator-agnostic. Domain implementations live
/// in the registry; this type owns stable scheduling, budgets, enforcement,
/// recursive goal optimization, and winner verification.
#[derive(Debug)]
pub struct CascadesEngine {
    memo: Memo,
    registry: ImplementationRegistry,
    enforcement: EnforcementPlanner,
    recipes: BTreeMap<(PhysicalExprId, OptimizationGoal, Fingerprint), CostRecipe>,
    implemented_goals: BTreeSet<(GroupId, OptimizationGoal)>,
    infeasible_goals: BTreeSet<(GroupId, OptimizationGoal)>,
    active_goals: BTreeSet<(GroupId, OptimizationGoal)>,
    grant_class_sets: BTreeMap<ResourceGrantClassId, AdmissibleGrantSetId>,
    grant_sensitivity: BTreeMap<GroupId, GrantSensitivitySummary>,
    rule_attempts: BTreeMap<RuleId, u64>,
    effective_rule_insertions: BTreeMap<RuleId, u64>,
    /// Last child-expression frontier consumed by each transformation task.
    /// A task that declined a match is recorded as well: a later child
    /// alternative may make that same pattern applicable.
    transformation_observations: BTreeMap<TransformationTaskId, Box<[(GroupId, u64)]>>,
    /// Reverse index for incrementally closing transformation dependencies.
    /// Subscribers are woken only after a Memo transaction commits.
    transformation_subscribers: BTreeMap<GroupId, BTreeSet<TransformationTaskId>>,
    region_candidates: BTreeMap<super::ids::RegionId, BTreeSet<Fingerprint>>,
}

impl CascadesEngine {
    pub fn new(memo: Memo, registry: ImplementationRegistry) -> Self {
        let budget = memo.budget().clone();
        Self {
            memo,
            registry,
            enforcement: EnforcementPlanner::new(
                budget.max_optional_enforcer_depth,
                budget.max_optional_enforcer_chains_per_goal,
            ),
            recipes: BTreeMap::new(),
            implemented_goals: BTreeSet::new(),
            infeasible_goals: BTreeSet::new(),
            active_goals: BTreeSet::new(),
            grant_class_sets: BTreeMap::new(),
            grant_sensitivity: BTreeMap::new(),
            rule_attempts: BTreeMap::new(),
            effective_rule_insertions: BTreeMap::new(),
            transformation_observations: BTreeMap::new(),
            transformation_subscribers: BTreeMap::new(),
            region_candidates: BTreeMap::new(),
        }
    }

    pub fn memo(&self) -> &Memo {
        &self.memo
    }

    pub fn memo_mut(&mut self) -> &mut Memo {
        &mut self.memo
    }

    pub fn optimize(
        &mut self,
        root: GroupId,
        goal: OptimizationGoal,
        mode: SearchMode,
    ) -> Result<Winner> {
        // CascadesEngine is also usable with a hand-built Memo. Seal at the
        // actual phase boundary rather than relying on one particular builder
        // to have done so: optional rules may only propagate expression-path
        // contexts admitted with the initial logical forest.
        self.memo.freeze_optimization_contexts()?;
        let root = self.memo.canonical_group(root);
        if mode == SearchMode::Memo {
            self.explore_transformations()?;
        }
        self.optimize_group(root, goal)?;
        super::verifier::MemoVerifier::verify(&self.memo, None)?;
        self.memo
            .group(root)
            .and_then(|group| group.winner(goal))
            .cloned()
            .ok_or_else(|| self.infeasible_goal_error(root, goal))
    }

    /// Optimize a bounded set of grant classes while sharing the complete
    /// mandatory search for grant-invariant closures. Sensitivity is derived
    /// after logical exploration so a transformation cannot invalidate the
    /// proof by introducing a hidden memory-class implementation.
    pub fn optimize_for_grants(
        &mut self,
        root: GroupId,
        base_goal: OptimizationGoal,
        admissible_set: AdmissibleGrantSetId,
        classes: impl IntoIterator<Item = ResourceGrantClassId>,
        mode: SearchMode,
    ) -> Result<GrantOptimization> {
        let classes: BTreeSet<_> = classes.into_iter().collect();
        if classes.is_empty() {
            return Err(paro_error::internal(
                "grant portfolio optimization requires at least one class",
            ));
        }
        if classes.len() > usize::from(self.memo.budget().max_grant_classes) {
            return Err(paro_error::internal(
                "grant portfolio exceeds the bounded class count",
            ));
        }
        self.memo.freeze_optimization_contexts()?;
        if mode == SearchMode::Memo {
            self.explore_transformations()?;
        }
        self.grant_class_sets.clear();
        self.grant_class_sets
            .extend(classes.iter().copied().map(|class| (class, admissible_set)));
        self.grant_sensitivity.clear();
        let root = self.memo.canonical_group(root);
        let mut sensitivity = self.grant_sensitivity(root)?;
        if matches!(
            self.memo
                .required(base_goal.required)
                .map(|required| &required.mutation_safety),
            Some(super::properties::MutationSafetyRequirement::StableReadBeforeWrite { .. })
        ) {
            // MutationInputSpool is a blocking, currently non-spillable
            // correctness enforcer. It must never hide under an invariant
            // root goal even when every relational implementation is itself
            // grant invariant.
            sensitivity = GrantSensitivitySummary::Sensitive {
                witness_group: root,
                witness_implementation: ImplementationId::INVALID,
            };
        }
        let mut winners = Vec::with_capacity(classes.len());
        match &sensitivity {
            GrantSensitivitySummary::Invariant(proof) => {
                let goal = OptimizationGoal {
                    grant: GrantGoalKey::Invariant(admissible_set),
                    ..base_goal
                };
                let winner = self.optimize(root, goal, SearchMode::Direct)?;
                verify_grant_invariance(&self.memo, &self.registry, proof)?;
                winners.extend(classes.into_iter().map(|class| GrantWinner {
                    class,
                    goal,
                    winner: winner.clone(),
                }));
            }
            GrantSensitivitySummary::Sensitive { .. } => {
                let mut last_infeasible = None;
                for class in classes {
                    let goal = OptimizationGoal {
                        grant: GrantGoalKey::Class(class),
                        ..base_goal
                    };
                    self.optimize_group(root, goal)?;
                    if let Some(winner) = self
                        .memo
                        .group(root)
                        .and_then(|group| group.winner(goal))
                        .cloned()
                    {
                        winners.push(GrantWinner {
                            class,
                            goal,
                            winner,
                        });
                    } else {
                        last_infeasible = Some(goal);
                    }
                }
                super::verifier::MemoVerifier::verify(&self.memo, None)?;
                if winners.is_empty() {
                    return Err(
                        self.infeasible_goal_error(root, last_infeasible.unwrap_or(base_goal))
                    );
                }
            }
        }
        Ok(GrantOptimization {
            sensitivity,
            winners: winners.into_boxed_slice(),
        })
    }

    fn grant_sensitivity(&mut self, group: GroupId) -> Result<GrantSensitivitySummary> {
        let group = self.memo.canonical_group(group);
        if let Some(summary) = self.grant_sensitivity.get(&group) {
            return Ok(summary.clone());
        }
        let summary = derive_grant_sensitivity(&self.memo, &self.registry, group)?;
        self.grant_sensitivity.insert(group, summary.clone());
        Ok(summary)
    }

    fn normalized_child_grant(
        &mut self,
        child: GroupId,
        parent: GrantGoalKey,
    ) -> Result<GrantGoalKey> {
        match parent {
            GrantGoalKey::Invariant(set) => Ok(GrantGoalKey::Invariant(set)),
            GrantGoalKey::Class(class) => {
                let set = self.grant_class_sets.get(&class).copied().ok_or_else(|| {
                    paro_error::internal(
                        "class goal was optimized outside a declared admissible grant set",
                    )
                })?;
                Ok(match self.grant_sensitivity(child)? {
                    GrantSensitivitySummary::Invariant(_) => GrantGoalKey::Invariant(set),
                    GrantSensitivitySummary::Sensitive { .. } => GrantGoalKey::Class(class),
                })
            }
        }
    }

    fn explore_transformations(&mut self) -> Result<()> {
        let mut agenda = StableAgenda::default();
        for group_index in 0..self.memo.group_count() {
            let group = GroupId::new(group_index);
            if self.memo.canonical_group(group) == group {
                self.schedule_transformations(group, &mut agenda)?;
            }
        }
        while let Some(task) = agenda.pop() {
            let SearchTask::Transform {
                group,
                expression,
                rule,
            } = task
            else {
                unreachable!("transformation agenda contains implementation task")
            };
            let task_id = TransformationTaskId {
                group,
                expression,
                rule,
            };
            let Some(dependency_version) = self.observe_transformation_inputs(task_id)? else {
                continue;
            };
            let matches = {
                let rule_impl = self
                    .registry
                    .transformation(rule)
                    .ok_or_else(|| paro_error::internal("rule disappeared from registry"))?;
                let expression_ref = self.memo.logical_expr(expression).ok_or_else(|| {
                    paro_error::internal("rule task references unknown expression")
                })?;
                let context = RuleContext {
                    memo: &self.memo,
                    group,
                };
                rule_impl.matches(expression_ref, &context)
            };
            if !matches {
                continue;
            }
            // `applied_rules` remains an audit of whether this rule has ever
            // reached apply for the expression. Incremental idempotence is
            // governed by the dependency-version observation above.
            self.memo.mark_rule_applied(expression, rule)?;
            let event = transformation_event(group, expression, rule, dependency_version);
            let admitted = self
                .memo
                .group_mut(group)
                .ok_or_else(|| paro_error::internal("rule task references unknown group"))?
                .ledger
                .admit_optional(BudgetDimension::RuleFirePerGroup, event);
            if admitted == BudgetDecision::Exhausted {
                continue;
            }
            if !admit_transformation_work(
                &mut self.memo,
                group,
                expression,
                rule,
                dependency_version,
            )? {
                continue;
            }
            // Reserve the complete bounded frontier before the rule may append
            // payloads or child groups. TransformContext mutations are
            // append-only and become reachable through those roots, so
            // post-apply budget rejection would manufacture orphan Memo
            // state. Ordinary local rules reserve one slot; region owners can
            // declare a larger deterministic bound.
            let output_bound = {
                let rule_impl = self
                    .registry
                    .transformation(rule)
                    .ok_or_else(|| paro_error::internal("rule disappeared from registry"))?;
                let context = RuleContext {
                    memo: &self.memo,
                    group,
                };
                rule_impl.output_bound(&context)
            };
            let mut output_events = Vec::with_capacity(output_bound);
            for ordinal in 0..output_bound {
                let event = transformation_output_event(
                    group,
                    expression,
                    rule,
                    dependency_version,
                    ordinal,
                );
                let admitted = self
                    .memo
                    .group_mut(group)
                    .ok_or_else(|| paro_error::internal("rule task references unknown group"))?
                    .ledger
                    .admit_optional(BudgetDimension::LogicalExprPerGroup, event);
                if admitted == BudgetDecision::Exhausted {
                    break;
                }
                output_events.push(event);
            }
            if output_events.is_empty() {
                continue;
            }
            *self.rule_attempts.entry(rule).or_default() += 1;
            // The context owns the complete attempt. Its Memo snapshot is
            // lazy, and rule-specific side state enlists in the same rollback
            // domain before its first write.
            let mut context = TransformContext::new(&mut self.memo, group);
            let outputs_result = {
                let rule_impl = self
                    .registry
                    .transformation(rule)
                    .ok_or_else(|| paro_error::internal("rule disappeared from registry"))?;
                rule_impl.apply(expression, &mut context)
            };
            let outputs = match outputs_result {
                Ok(outputs) => outputs,
                Err(error) => {
                    context.rollback()?;
                    release_transformation_output_reservations(
                        &mut self.memo,
                        group,
                        &output_events,
                    )?;
                    tracing::debug!(
                        target: "paro::optimizer",
                        %error,
                        rule = rule.0,
                        group = group.index(),
                        "discarded failed optional transformation"
                    );
                    continue;
                }
            };
            if outputs.is_empty() {
                context.rollback()?;
                release_transformation_output_reservations(&mut self.memo, group, &output_events)?;
                continue;
            }
            if outputs.len() > output_events.len() {
                context.rollback()?;
                release_transformation_output_reservations(&mut self.memo, group, &output_events)?;
                tracing::debug!(
                    target: "paro::optimizer",
                    rule = rule.0,
                    group = group.index(),
                    output_count = outputs.len(),
                    reserved_outputs = output_events.len(),
                    "discarded optional transformation whose frontier exceeded its declared bound"
                );
                continue;
            }
            let insertion = (|| -> Result<TransformationInsertion> {
                let mut inserted_groups = BTreeSet::new();
                let mut inserted_properties = Vec::new();
                let mut inserted_expressions = 0usize;
                for output in outputs {
                    validate_transformation_proof(rule, expression, &output.proof)?;
                    let target = context.memo().canonical_group(output.target_group);
                    if target != context.memo().canonical_group(group) {
                        return Err(paro_error::internal(
                            "a local transformation must target its source equivalence group",
                        ));
                    }
                    // A duplicate output is an ineffective transformation.
                    // Do not call `insert_logical`: that method is allowed to
                    // enrich the proof set of an existing expression, while
                    // this attempt must remain completely side-effect free.
                    if context
                        .memo()
                        .logical_expr_for_key(target, &output.key)
                        .is_some()
                    {
                        continue;
                    }
                    let before = context
                        .memo()
                        .group(target)
                        .map(|group| group.logical_exprs().len())
                        .unwrap_or(0);
                    context.memo_mut().insert_logical(
                        target,
                        output.key,
                        output.payload,
                        output.proof,
                    )?;
                    let after = context
                        .memo()
                        .group(target)
                        .expect("target group was validated")
                        .logical_exprs()
                        .len();
                    if after > before {
                        inserted_groups.insert(target);
                        inserted_expressions += after - before;
                        inserted_properties.push((
                            target,
                            output.logical_properties,
                            output.cardinality,
                        ));
                    }
                }
                Ok(TransformationInsertion {
                    groups: inserted_groups,
                    properties: inserted_properties,
                    expressions: inserted_expressions,
                })
            })();
            let TransformationInsertion {
                groups: mut inserted_groups,
                properties: inserted_properties,
                expressions: inserted_expressions,
            } = match insertion {
                Ok(result) => result,
                Err(error) => {
                    context.rollback()?;
                    release_transformation_output_reservations(
                        &mut self.memo,
                        group,
                        &output_events,
                    )?;
                    tracing::debug!(
                        target: "paro::optimizer",
                        %error,
                        rule = rule.0,
                        group = group.index(),
                        "discarded invalid optional transformation output"
                    );
                    continue;
                }
            };
            if inserted_groups.is_empty() {
                // A duplicate root is not an effective transformation. Drop
                // any staged child groups and planner payloads with it.
                context.rollback()?;
                release_transformation_output_reservations(&mut self.memo, group, &output_events)?;
            } else {
                let appended_groups = context.commit()?;
                release_transformation_output_reservations(
                    &mut self.memo,
                    group,
                    &output_events[inserted_expressions.min(output_events.len())..],
                )?;
                for (target, properties, cardinality) in inserted_properties {
                    let group = self.memo.group_mut(target).ok_or_else(|| {
                        paro_error::internal("committed transformation lost its target group")
                    })?;
                    group.logical_properties.merge_equivalent_facts(&properties);
                    group.cardinality =
                        std::mem::take(&mut group.cardinality).canonical_with(cardinality);
                }
                *self.effective_rule_insertions.entry(rule).or_default() +=
                    u64::try_from(inserted_expressions).unwrap_or(u64::MAX);
                inserted_groups.extend(appended_groups);
            }
            for target in inserted_groups {
                self.schedule_transformations(target, &mut agenda)?;
                self.schedule_transformation_dependents(target, &mut agenda)?;
            }
        }
        Ok(())
    }

    pub fn effective_rule_insertions(&self) -> &BTreeMap<RuleId, u64> {
        &self.effective_rule_insertions
    }

    pub fn rule_attempts(&self) -> &BTreeMap<RuleId, u64> {
        &self.rule_attempts
    }

    fn schedule_transformations(&self, group: GroupId, agenda: &mut StableAgenda) -> Result<()> {
        let group_ref = self
            .memo
            .group(group)
            .ok_or_else(|| paro_error::internal("cannot schedule an unknown Memo group"))?;
        for &expression in group_ref.logical_exprs() {
            let expression_ref = self.memo.logical_expr(expression).unwrap();
            let context = RuleContext {
                memo: &self.memo,
                group,
            };
            for rule in self.registry.transformations() {
                if !self.memo.budget().transformation_enabled(rule.id()) {
                    continue;
                }
                let promise = rule.promise(expression_ref, &context);
                let key = TaskKey {
                    priority: promise.priority,
                    kind: TaskKind::Transform,
                    stable_id: rule.id().0,
                    group,
                    expression,
                    goal: None,
                };
                agenda.push(
                    key,
                    SearchTask::Transform {
                        group,
                        expression,
                        rule: rule.id(),
                    },
                );
            }
        }
        Ok(())
    }

    fn schedule_transformation_dependents(
        &self,
        group: GroupId,
        agenda: &mut StableAgenda,
    ) -> Result<()> {
        let group = self.memo.canonical_group(group);
        let subscribers = self
            .transformation_subscribers
            .get(&group)
            .cloned()
            .unwrap_or_default();
        for subscriber in subscribers {
            let expression_ref =
                self.memo
                    .logical_expr(subscriber.expression)
                    .ok_or_else(|| {
                        paro_error::internal(
                            "transformation subscriber references unknown expression",
                        )
                    })?;
            let rule = self
                .registry
                .transformation(subscriber.rule)
                .ok_or_else(|| paro_error::internal("subscribed transformation disappeared"))?;
            let owner = self
                .memo
                .logical_owner(subscriber.expression)
                .ok_or_else(|| {
                    paro_error::internal("transformation subscriber has no owning group")
                })?;
            let context = RuleContext {
                memo: &self.memo,
                group: owner,
            };
            let promise = rule.promise(expression_ref, &context);
            agenda.push(
                TaskKey {
                    priority: promise.priority,
                    kind: TaskKind::Transform,
                    stable_id: subscriber.rule.0,
                    group: owner,
                    expression: subscriber.expression,
                    goal: None,
                },
                SearchTask::Transform {
                    group: owner,
                    expression: subscriber.expression,
                    rule: subscriber.rule,
                },
            );
        }
        Ok(())
    }

    /// Record the complete logical frontier reachable through the source
    /// expression's child group holes. This conservative closure covers
    /// legacy rules that still materialize a planner subtree while local
    /// Memo-native rules naturally subscribe only to their direct holes.
    fn observe_transformation_inputs(
        &mut self,
        task: TransformationTaskId,
    ) -> Result<Option<Fingerprint>> {
        let dependencies = transformation_dependency_versions(&self.memo, task.expression)?;
        if self
            .transformation_observations
            .get(&task)
            .is_some_and(|observed| observed.as_ref() == dependencies.as_slice())
        {
            return Ok(None);
        }

        if let Some(previous) = self.transformation_observations.get(&task) {
            for &(group, _) in previous {
                if let Some(subscribers) = self.transformation_subscribers.get_mut(&group) {
                    subscribers.remove(&task);
                }
            }
        }
        for (group, _) in dependencies.iter().copied() {
            self.transformation_subscribers
                .entry(group)
                .or_default()
                .insert(task);
        }
        let version = transformation_dependency_fingerprint(&dependencies);
        self.transformation_observations
            .insert(task, dependencies.into_boxed_slice());
        Ok(Some(version))
    }

    fn enumerate_implementations(&mut self, group: GroupId, goal: OptimizationGoal) -> Result<()> {
        let group = self.memo.canonical_group(group);
        if !self.implemented_goals.insert((group, goal)) {
            return Ok(());
        }
        let mut agenda = StableAgenda::default();
        let logical_exprs = self
            .memo
            .group(group)
            .ok_or_else(|| paro_error::internal("unknown group during implementation"))?
            .logical_exprs()
            .to_vec();
        for expression in logical_exprs {
            let expression_ref = self.memo.logical_expr(expression).unwrap();
            for implementation in self.registry.implementations() {
                let promise = implementation.promise(expression_ref, goal);
                let key = TaskKey {
                    priority: promise.priority,
                    kind: TaskKind::Implement,
                    stable_id: implementation.id().0,
                    group,
                    expression,
                    goal: Some(goal),
                };
                agenda.push(
                    key,
                    SearchTask::Implement {
                        group,
                        expression,
                        implementation: implementation.id(),
                        goal,
                    },
                );
            }
        }

        while let Some(task) = agenda.pop() {
            let SearchTask::Implement {
                group,
                expression,
                implementation,
                goal,
            } = task
            else {
                unreachable!("implementation agenda contains transformation task")
            };
            let mut candidates = {
                let implementation_ref = self
                    .registry
                    .implementation(implementation)
                    .ok_or_else(|| paro_error::internal("implementation disappeared"))?;
                let expression_ref = self.memo.logical_expr(expression).ok_or_else(|| {
                    paro_error::internal("implementation task references unknown expression")
                })?;
                let context = ImplementationContext {
                    memo: &self.memo,
                    group,
                };
                if !implementation_ref.matches(expression_ref, goal, &context) {
                    continue;
                }
                implementation_ref
                    .candidates(expression, goal, &context)?
                    .into_vec()
            };
            candidates.sort_by_key(|candidate| {
                (
                    !candidate.mandatory,
                    candidate.physical_fingerprint,
                    candidate.key.clone(),
                )
            });
            for candidate in candidates {
                self.admit_candidate(group, expression, implementation, goal, candidate)?;
            }
        }
        Ok(())
    }

    fn admit_candidate(
        &mut self,
        group: GroupId,
        expression: LogicalExprId,
        implementation: ImplementationId,
        goal: OptimizationGoal,
        mut candidate: PhysicalCandidate,
    ) -> Result<()> {
        candidate.local_cost.validate()?;
        candidate.provided.validate()?;
        if candidate.key.implementation != implementation || candidate.key.logical != expression {
            return Err(paro_error::internal(
                "implementation candidate key does not match its registry task",
            ));
        }
        for (child, child_goal) in candidate.child_goals.iter_mut() {
            child_goal.grant = self.normalized_child_grant(*child, goal.grant)?;
        }
        let canonical_key_children: Vec<_> = candidate
            .key
            .children
            .iter()
            .map(|child| self.memo.canonical_group(*child))
            .collect();
        let canonical_goal_children: Vec<_> = candidate
            .child_goals
            .iter()
            .map(|(child, _)| self.memo.canonical_group(*child))
            .collect();
        if canonical_key_children != canonical_goal_children {
            return Err(paro_error::internal(
                "physical candidate child groups and child goals disagree",
            ));
        }
        if !candidate.mandatory {
            if let Some(region) = &candidate.region {
                let admitted = self.region_candidates.entry(region.region).or_default();
                if !admitted.contains(&candidate.physical_fingerprint)
                    && admitted.len()
                        >= usize::from(self.memo.budget().max_composite_region_candidates)
                {
                    tracing::debug!(
                        target: "paro::optimizer",
                        memo_group = group.index(),
                        implementation = candidate.key.implementation.0,
                        region_candidate_count = admitted.len(),
                        "optional physical candidate rejected by its region budget"
                    );
                    return Ok(());
                }
                admitted.insert(candidate.physical_fingerprint);
            }
            let decision = self.memo.group_mut(group).unwrap().ledger.admit_optional(
                BudgetDimension::PhysicalExprPerGroup,
                candidate.stable_event(goal),
            );
            if decision == BudgetDecision::Exhausted {
                tracing::debug!(
                    target: "paro::optimizer",
                    memo_group = group.index(),
                    implementation = candidate.key.implementation.0,
                    "optional physical candidate rejected by its group budget"
                );
                return Ok(());
            }
        }
        let physical = self.memo.insert_physical(
            group,
            candidate.key,
            candidate.payload,
            candidate.provided,
        )?;
        let recipe_key = (physical, goal, candidate.physical_fingerprint);
        self.recipes.entry(recipe_key).or_insert(CostRecipe {
            child_goals: candidate.child_goals,
            local_cost: candidate.local_cost,
            source_filter_apply_cost: candidate.source_filter_apply_cost,
            cost_composition: candidate.cost_composition,
            spillable: candidate.spillable,
            enforcer_cost_input: candidate.enforcer_cost_input,
            physical_fingerprint: candidate.physical_fingerprint,
            region: candidate.region,
        });
        Ok(())
    }

    fn optimize_group(&mut self, group: GroupId, goal: OptimizationGoal) -> Result<()> {
        let group = self.memo.canonical_group(group);
        if self
            .memo
            .group(group)
            .and_then(|group| group.winner(goal))
            .is_some()
            || self.infeasible_goals.contains(&(group, goal))
        {
            return Ok(());
        }
        if !self.active_goals.insert((group, goal)) {
            return Err(paro_error::internal(
                "ordinary Memo group formed a recursive optimization cycle; use RecursiveRegion",
            ));
        }
        let result = self.optimize_group_inner(group, goal);
        self.active_goals.remove(&(group, goal));
        if result.is_ok()
            && self
                .memo
                .group(group)
                .and_then(|group| group.winner(goal))
                .is_none()
        {
            self.infeasible_goals.insert((group, goal));
        }
        result
    }

    fn optimize_group_inner(&mut self, group: GroupId, goal: OptimizationGoal) -> Result<()> {
        self.enumerate_implementations(group, goal)?;
        let required = self
            .memo
            .required(goal.required)
            .ok_or_else(|| paro_error::internal("optimization goal has unknown properties"))?
            .clone();
        // The recipe key is physical-expression first. Walk only the physical
        // expressions owned by this group and use bounded BTree ranges instead
        // of scanning the global recipe table for every (group, goal).
        let physical_exprs = self
            .memo
            .group(group)
            .ok_or_else(|| paro_error::internal("unknown group during recipe lookup"))?
            .physical_exprs()
            .to_vec();
        let mut recipes = Vec::new();
        for &physical in &physical_exprs {
            recipes.extend(
                self.recipes
                    .range(
                        (physical, goal, Fingerprint::default())
                            ..=(physical, goal, Fingerprint(u128::MAX)),
                    )
                    .map(|((physical, _, _), recipe)| (*physical, recipe.clone())),
            );
        }
        for (physical, recipe) in recipes {
            let mut child_costs = Vec::with_capacity(recipe.child_goals.len());
            let mut child_source_work = Vec::with_capacity(recipe.child_goals.len());
            let mut children_feasible = true;
            for (child, child_goal) in recipe.child_goals.iter().copied() {
                self.optimize_group(child, child_goal)?;
                let Some(child_winner) = self
                    .memo
                    .group(child)
                    .and_then(|group| group.winner(child_goal))
                else {
                    tracing::debug!(
                        target: "paro::optimizer",
                        parent_group = group.index(),
                        physical_expression = physical.index(),
                        child_group = child.index(),
                        ?child_goal,
                        "physical recipe rejected because a child goal is infeasible"
                    );
                    children_feasible = false;
                    break;
                };
                child_costs.push(child_winner.cost);
                child_source_work.push(child_winner.source_work.clone());
            }
            if !children_feasible {
                continue;
            }
            let Some(local_cost) = fit_local_retained_state_to_grant(
                recipe.local_cost,
                &child_costs,
                recipe.cost_composition.clone(),
                recipe.spillable,
                recipe.enforcer_cost_input,
            )?
            else {
                tracing::debug!(
                    target: "paro::optimizer",
                    memo_group = group.index(),
                    physical_expression = physical.index(),
                    local_peak_memory = recipe.local_cost.peak_memory_upper,
                    spillable = recipe.spillable,
                    hard_memory = recipe.enforcer_cost_input.hard_memory_bytes,
                    "physical recipe rejected by its resource grant"
                );
                continue;
            };
            let child_source_work_refs = child_source_work
                .iter()
                .map(|work| work.as_ref())
                .collect::<Vec<_>>();
            let composed = compose_candidate_cost_with_sources(
                local_cost,
                recipe.source_filter_apply_cost,
                &child_costs,
                &child_source_work_refs,
                recipe.cost_composition.clone(),
            )?;
            let source_work = composed.source_work;
            let Some(mut cost) =
                constrain_composed_cost_to_grant(composed.cost, recipe.enforcer_cost_input)?
            else {
                continue;
            };
            let physical_properties = self.memo.physical_expr(physical).unwrap().provided.clone();
            let Some(enforced) = self
                .enforcement
                .canonical_baseline(physical_properties, &required)?
            else {
                tracing::debug!(
                    target: "paro::optimizer",
                    memo_group = group.index(),
                    physical_expression = physical.index(),
                    "physical recipe rejected because its required enforcer is absent from the execution ABI"
                );
                continue;
            };
            let Some(enforcer_cost) = enforcer_cost(
                &enforced.steps,
                recipe.enforcer_cost_input,
                self.memo.calibration(),
            )?
            else {
                tracing::debug!(
                    target: "paro::optimizer",
                    memo_group = group.index(),
                    physical_expression = physical.index(),
                    ?enforced.steps,
                    "physical recipe rejected because its enforcer chain is infeasible"
                );
                continue;
            };
            let Some(constrained_cost) = constrain_composed_cost_to_grant(
                cost.sequential(enforcer_cost)?,
                recipe.enforcer_cost_input,
            )?
            else {
                continue;
            };
            cost = constrained_cost;
            let fingerprint = enforced_fingerprint(
                recipe.physical_fingerprint,
                &enforced.steps,
                recipe.child_goals.iter().filter_map(|(child, child_goal)| {
                    self.memo
                        .group(*child)
                        .and_then(|group| group.winner(*child_goal))
                        .map(|winner| winner.physical_fingerprint)
                }),
            );
            let joint_cost_proof = build_joint_cost_proof(&self.memo, group, &recipe, local_cost)?;
            if self
                .memo
                .group(group)
                .is_some_and(|group| group.logical_exprs().len() > 1)
            {
                let logical_expression = self
                    .memo
                    .physical_expr(physical)
                    .map(|physical| physical.key.logical);
                let origin_rule = logical_expression.and_then(|logical| {
                    self.memo.logical_expr(logical).and_then(|logical| {
                        logical.proofs.iter().find_map(|proof| match proof {
                            EquivalenceProof::Transformation { rule, .. }
                            | EquivalenceProof::SpecializedEnumerator { rule, .. } => Some(rule.0),
                            EquivalenceProof::Initial | EquivalenceProof::Normalization { .. } => {
                                None
                            }
                        })
                    })
                });
                tracing::debug!(
                    target: "paro::optimizer",
                    memo_group = group.index(),
                    logical_expression = logical_expression.map(LogicalExprId::index),
                    origin_rule,
                    physical_expression = physical.index(),
                    child_groups = ?recipe
                        .child_goals
                        .iter()
                        .map(|(child, _)| child.index())
                        .collect::<Vec<_>>(),
                    implementation = self
                        .memo
                        .physical_expr(physical)
                        .map(|physical| physical.key.implementation.0),
                    local_expected_cost = local_cost.score.range.expected,
                    local_risk_adjusted_cost = local_cost.score.risk_adjusted,
                    child_expected_costs = ?child_costs
                        .iter()
                        .map(|cost| cost.score.range.expected)
                        .collect::<Vec<_>>(),
                    child_risk_adjusted_costs = ?child_costs
                        .iter()
                        .map(|cost| cost.score.risk_adjusted)
                        .collect::<Vec<_>>(),
                    source_filter_apply_risk_adjusted_cost = recipe
                        .source_filter_apply_cost
                        .map(|cost| cost.score.risk_adjusted),
                    composition = ?recipe.cost_composition,
                    expected_cost = cost.score.range.expected,
                    risk_adjusted_cost = cost.score.risk_adjusted,
                    upper_cost = cost.score.range.upper,
                    "costed an equivalent physical candidate"
                );
            }
            self.memo.record_winner(
                group,
                goal,
                Winner {
                    expression: physical,
                    child_goals: recipe.child_goals.clone(),
                    enforcers: enforced.steps,
                    enforcer_cost_input: recipe.enforcer_cost_input,
                    provided: enforced.provided,
                    local_cost,
                    source_filter_apply_cost: recipe.source_filter_apply_cost,
                    cost_composition: recipe.cost_composition.clone(),
                    cost,
                    source_work,
                    physical_fingerprint: fingerprint,
                    joint_cost_proof,
                },
            )?;
        }
        Ok(())
    }

    fn infeasible_goal_error(
        &self,
        group: GroupId,
        goal: OptimizationGoal,
    ) -> paro_error::ParoError {
        let group_ref = self.memo.group(group);
        let logical = group_ref
            .map(|group| group.logical_exprs().to_vec())
            .unwrap_or_default();
        let physical = group_ref
            .map(|group| {
                group
                    .physical_exprs()
                    .iter()
                    .filter_map(|id| {
                        self.memo
                            .physical_expr(*id)
                            .map(|expr| (*id, expr.provided.clone()))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let required = self.memo.required(goal.required);
        paro_error::internal(format!(
            "no feasible physical plan exists for Memo group {group:?} with goal {goal:?}; required={required:?}, logical={logical:?}, physical={physical:?}"
        ))
    }
}

fn transformation_output_event(
    group: GroupId,
    expression: LogicalExprId,
    rule: RuleId,
    dependency_version: Fingerprint,
    ordinal: usize,
) -> Fingerprint {
    let mut event = StableFingerprintBuilder::default();
    event.write_bytes(b"paro.memo.transformation-output.v2");
    event.write_u64(group.0 as u64);
    event.write_u64(expression.0 as u64);
    event.write_u64(rule.0 as u64);
    event.write_fingerprint(dependency_version);
    event.write_u64(ordinal as u64);
    event.finish()
}

fn release_transformation_output_reservations(
    memo: &mut Memo,
    group: GroupId,
    events: &[Fingerprint],
) -> Result<()> {
    let ledger = &mut memo
        .group_mut(group)
        .ok_or_else(|| paro_error::internal("rule task references unknown group"))?
        .ledger;
    for event in events {
        ledger.release_optional_reservation(BudgetDimension::LogicalExprPerGroup, *event);
    }
    Ok(())
}

fn build_joint_cost_proof(
    memo: &Memo,
    owner_group: GroupId,
    recipe: &CostRecipe,
    local_cost: SearchCost,
) -> Result<Option<JointCostProof>> {
    let Some(region) = &recipe.region else {
        return Ok(None);
    };
    let owner_group = memo.canonical_group(owner_group);
    let boundary_goals = recipe
        .child_goals
        .iter()
        .map(|(child, goal)| (memo.canonical_group(*child), *goal))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let mut dependencies = boundary_goals
        .iter()
        .map(|(child, _)| RegionDependencyEdge {
            producer: *child,
            consumer: owner_group,
            kind: RegionDependencyKind::Data,
        })
        .collect::<Vec<_>>();
    let mut owned_artifacts = BTreeMap::new();
    for artifact in &region.artifacts {
        if owned_artifacts
            .insert(artifact.fingerprint, artifact.kind)
            .is_some()
        {
            return Err(paro_error::internal(
                "region candidate owns one artifact fingerprint more than once",
            ));
        }
    }
    let mut dependency_count_by_artifact = BTreeMap::<Fingerprint, usize>::new();
    for dependency in &region.artifact_dependencies {
        let Some(kind) = owned_artifacts.get(&dependency.artifact) else {
            return Err(paro_error::internal(
                "region candidate dependency references an unowned artifact",
            ));
        };
        if *kind == RegionArtifactKind::RuntimeFilter
            && dependency.kind != RegionDependencyKind::ControlWaitComplete
        {
            return Err(paro_error::internal(
                "runtime-filter artifact requires a wait-complete dependency",
            ));
        }
        let producer =
            resolve_region_boundary_endpoint(owner_group, &boundary_goals, dependency.producer)?;
        let consumer =
            resolve_region_boundary_endpoint(owner_group, &boundary_goals, dependency.consumer)?;
        if producer == consumer {
            return Err(paro_error::internal(
                "region artifact dependency resolves to a self-edge",
            ));
        }
        dependencies.push(RegionDependencyEdge {
            producer,
            consumer,
            kind: dependency.kind,
        });
        *dependency_count_by_artifact
            .entry(dependency.artifact)
            .or_default() += 1;
    }
    for artifact in &region.artifacts {
        if artifact.kind == RegionArtifactKind::RuntimeFilter
            && dependency_count_by_artifact
                .get(&artifact.fingerprint)
                .copied()
                != Some(1)
        {
            return Err(paro_error::internal(
                "runtime-filter artifact must declare exactly one candidate dependency",
            ));
        }
    }
    dependencies.sort_unstable();
    Ok(Some(JointCostProof {
        region: region.region,
        facets: region.facets.clone(),
        owner_group,
        boundary_goals,
        owned_artifacts: region.artifacts.clone(),
        artifact_dependencies: region.artifact_dependencies.clone(),
        dependencies: dependencies.into_boxed_slice(),
        local_cost,
        source_filter_apply_cost: recipe.source_filter_apply_cost,
        cost_composition: recipe.cost_composition.clone(),
    }))
}

fn resolve_region_boundary_endpoint(
    owner_group: GroupId,
    child_goals: &[(GroupId, OptimizationGoal)],
    endpoint: RegionBoundaryEndpoint,
) -> Result<GroupId> {
    match endpoint {
        RegionBoundaryEndpoint::Owner => Ok(owner_group),
        RegionBoundaryEndpoint::Input(ordinal) => child_goals
            .get(usize::from(ordinal))
            .map(|(child, _)| *child)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "region artifact dependency references missing candidate input {ordinal}"
                ))
            }),
    }
}

fn fit_local_retained_state_to_grant(
    mut local_cost: SearchCost,
    child_costs: &[SearchCost],
    composition: CostComposition,
    spillable: bool,
    grant: EnforcerCostInput,
) -> Result<Option<SearchCost>> {
    if grant.hard_memory_bytes == u64::MAX {
        return Ok(Some(local_cost));
    }
    let overlapping_children = composition.overlapping_children();
    let overlapping_minimum = if overlapping_children == 0 {
        0
    } else {
        child_costs
            .iter()
            .enumerate()
            .filter(|(index, _)| overlapping_children & (1_u64 << index) != 0)
            .map(|(_, child)| child.minimum_memory_bytes)
            .max()
            .unwrap_or(0)
    };
    let retained_minimum = local_cost
        .minimum_memory_bytes
        .saturating_add(overlapping_minimum);
    if retained_minimum > grant.hard_memory_bytes {
        return Ok(None);
    }
    if local_cost.peak_memory_upper == u64::MAX {
        if spillable && grant.spill_policy == SpillPolicy::Allowed {
            local_cost.peak_memory_upper = grant.hard_memory_bytes;
            local_cost.revocable_memory_target = local_cost
                .revocable_memory_target
                .min(grant.hard_memory_bytes - retained_minimum);
            return Ok(Some(local_cost));
        }
        if local_cost.memory_completion.is_runtime_capped() {
            local_cost.apply_runtime_cap(grant.hard_memory_bytes, retained_minimum)?;
            return Ok(Some(local_cost));
        }
        return Ok(None);
    }
    if local_cost.peak_memory_upper <= grant.hard_memory_bytes {
        return Ok(Some(local_cost));
    }
    if spillable && grant.spill_policy == SpillPolicy::Allowed {
        let spilled = local_cost
            .revocable_memory_target
            .saturating_add(retained_minimum)
            .saturating_sub(grant.hard_memory_bytes);
        local_cost.peak_memory_upper = grant.hard_memory_bytes;
        local_cost.revocable_memory_target = local_cost
            .revocable_memory_target
            .min(grant.hard_memory_bytes - retained_minimum);
        if spilled > 0 {
            add_composition_spill_cost(&mut local_cost, spilled)?;
        }
        return Ok(Some(local_cost));
    }
    if local_cost.memory_completion.is_runtime_capped() {
        local_cost.apply_runtime_cap(grant.hard_memory_bytes, retained_minimum)?;
        return Ok(Some(local_cost));
    }
    Ok(None)
}

fn add_composition_spill_cost(cost: &mut SearchCost, spilled: u64) -> Result<()> {
    cost.spill_bytes_expected = cost.spill_bytes_expected.saturating_add(spilled);
    let io_work = (spilled as f64 / 4096.0).max(1.0);
    let range = CompactRange::new(io_work, io_work * 2.0, io_work * 6.0)?;
    cost.score.range = cost.score.range.checked_add(range)?;
    cost.score.risk_adjusted += io_work * 3.0;
    cost.critical_path = cost.critical_path.checked_add(range)?;
    cost.resources_expected[ResourceDimension::SequentialIo as usize] += io_work * 2.0;
    cost.resources_risk_upper[ResourceDimension::SequentialIo as usize] += io_work * 6.0;
    cost.validate()
}

pub(crate) fn constrain_composed_cost_to_grant(
    mut cost: SearchCost,
    grant: EnforcerCostInput,
) -> Result<Option<SearchCost>> {
    if grant.hard_memory_bytes == u64::MAX {
        return Ok(Some(cost));
    }
    if cost.minimum_memory_bytes > grant.hard_memory_bytes {
        return Ok(None);
    }
    // Every child implementation has already proved its own resident state
    // against this class. Their revocable targets draw from the same query
    // pool and are therefore preferences, not additive reservations. Clamp
    // only that elastic portion after composing the mandatory floors.
    if cost.memory_completion.is_runtime_capped() {
        // A capped plan has no completion proof below its uncapped demand, so
        // its resident peak denotes the admitted ceiling itself rather than a
        // tighter estimate. The original demand remains in memory_completion.
        cost.apply_runtime_cap(grant.hard_memory_bytes, cost.minimum_memory_bytes)?;
    } else {
        cost.revocable_memory_target = cost.revocable_memory_target.min(
            grant
                .hard_memory_bytes
                .saturating_sub(cost.minimum_memory_bytes),
        );
        cost.peak_memory_upper = cost
            .peak_memory_upper
            .min(grant.hard_memory_bytes)
            .max(cost.minimum_memory_bytes);
    }
    cost.validate()?;
    Ok(Some(cost))
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ComposedCost {
    pub(crate) cost: SearchCost,
    pub(crate) source_work: Box<[SourceWork]>,
}

pub(crate) fn compose_candidate_cost_with_sources(
    local_cost: SearchCost,
    source_filter_apply_cost: Option<SearchCost>,
    child_costs: &[SearchCost],
    child_source_work: &[&[SourceWork]],
    composition: CostComposition,
) -> Result<ComposedCost> {
    if child_costs.len() != child_source_work.len() {
        return Err(paro_error::internal(
            "cost composition has no source-work evidence for one or more children",
        ));
    }
    let mut cost = local_cost;
    if composition == CostComposition::LocalOnly {
        cost.validate()?;
        return Ok(ComposedCost {
            cost,
            source_work: Box::new([]),
        });
    }
    if let CostComposition::Source { source } = &composition {
        if !child_costs.is_empty() {
            return Err(paro_error::internal(
                "a base source-work lane unexpectedly has child pipelines",
            ));
        }
        cost.validate()?;
        return Ok(ComposedCost {
            cost,
            source_work: Box::new([SourceWork {
                source: *source,
                cost: local_cost.work_only(),
                filters: Box::new([]),
                filter_apply_cost: SearchCost::ZERO,
            }]),
        });
    }
    let sideways_filter = composition.sideways_filter();
    let mut source_work = Vec::new();
    for (index, child) in child_costs.iter().copied().enumerate() {
        let mut child = child;
        let mut lanes = child_source_work[index].to_vec();
        if let Some((filtered_child, sources)) = sideways_filter {
            if index == filtered_child {
                let matching_lanes = lanes
                    .iter()
                    .filter(|lane| sources.iter().any(|source| source.source == lane.source))
                    .count();
                if matching_lanes != 0 {
                    let full_apply_cost = source_filter_apply_cost.ok_or_else(|| {
                        paro_error::internal(
                            "sideways-filter composition has no predicate-application cost",
                        )
                    })?;
                    cost = cost.replace_work(full_apply_cost, SearchCost::ZERO)?;
                    let matching_work = lanes
                        .iter()
                        .filter(|lane| sources.iter().any(|source| source.source == lane.source))
                        .map(|lane| lane.cost.score.range.expected.max(0.0))
                        .sum::<f64>();
                    // Predicate evaluation is one operator-local cost before it
                    // is attributed to source lanes. Allocate every ppm exactly
                    // once so splitting a UNION into more branches cannot create
                    // or discard work through independent rounding.
                    let mut apply_shares = Vec::with_capacity(matching_lanes);
                    let mut unallocated_ppm = 1_000_000_u32;
                    for lane in lanes
                        .iter()
                        .filter(|lane| sources.iter().any(|source| source.source == lane.source))
                    {
                        let remaining_lanes = matching_lanes - apply_shares.len();
                        let share = if remaining_lanes == 1 {
                            unallocated_ppm
                        } else if matching_work > 0.0 {
                            ((lane.cost.score.range.expected.max(0.0) / matching_work * 1_000_000.0)
                                .floor() as u32)
                                .min(unallocated_ppm)
                        } else {
                            unallocated_ppm
                                / u32::try_from(remaining_lanes).map_err(|_| {
                                    paro_error::internal(
                                        "runtime filter has too many source-work lanes",
                                    )
                                })?
                        };
                        apply_shares.push(share);
                        unallocated_ppm -= share;
                    }
                    debug_assert_eq!(unallocated_ppm, 0);
                    let mut apply_shares = apply_shares.into_iter();
                    for lane in &mut lanes {
                        if let Some(source) =
                            sources.iter().find(|source| source.source == lane.source)
                        {
                            let total_apply_cost = source_filter_apply_cost.expect(
                                "matching source-work lane established predicate application cost",
                            );
                            let share = apply_shares
                                .next()
                                .expect("one predicate-cost share per matching source lane");
                            let full_apply_cost = total_apply_cost.retain_work(share, 1_000_000)?;
                            // Speculative filters retain the complete risk
                            // ceiling. Exact membership over a declared-unique
                            // probe carries a proof-backed smaller ceiling.
                            let retained = lane.cost.retain_work(
                                source.expected_retained_ppm,
                                source.upper_retained_ppm,
                            )?;
                            child = child.replace_work(lane.cost, retained)?;
                            lane.cost = retained;
                            let old_apply_cost = lane.filter_apply_cost;
                            let mut filters = lane.filters.to_vec();
                            filters.push(SourceFilterWork {
                                expected_retained_ppm: source.expected_retained_ppm,
                                full_apply_cost: full_apply_cost.work_only(),
                            });
                            let new_apply_cost = ordered_source_filter_cost(&filters)?;
                            child = child.replace_work(old_apply_cost, new_apply_cost)?;
                            lane.filters = filters.into_boxed_slice();
                            lane.filter_apply_cost = new_apply_cost;
                        }
                    }
                }
                tracing::debug!(
                    target: "paro::optimizer",
                    declared_source_count = sources.len(),
                    matching_lanes,
                    source_retentions = ?sources,
                    child_expected_cost = child.score.range.expected,
                    "composed source-attributed sideways filter"
                );
            }
        }
        cost = child.sequential(cost)?;
        source_work.extend(lanes);
    }
    let overlapping_children = composition.overlapping_children();
    if overlapping_children != 0 {
        if child_costs.len() > u64::BITS as usize
            || (child_costs.len() < u64::BITS as usize
                && overlapping_children >> child_costs.len() != 0)
        {
            return Err(paro_error::internal(
                "cost composition references a missing child pipeline",
            ));
        }
        let overlapping_peak = child_costs
            .iter()
            .enumerate()
            .filter(|(index, _)| overlapping_children & (1_u64 << index) != 0)
            .map(|(_, child)| child.peak_memory_upper)
            .max()
            .unwrap_or(0);
        let (overlapping_completion, overlapping_completion_peak) = child_costs
            .iter()
            .enumerate()
            .filter(|(index, _)| overlapping_children & (1_u64 << index) != 0)
            .fold(
                (MemoryCompletion::Guaranteed, 0_u64),
                |(completion, peak), (_, child)| {
                    (
                        completion.sequential(
                            peak,
                            child.memory_completion,
                            child.peak_memory_upper,
                        ),
                        peak.max(child.peak_memory_upper),
                    )
                },
            );
        let retained_completion = local_cost.memory_completion.overlapping(
            local_cost.peak_memory_upper,
            overlapping_completion,
            overlapping_completion_peak,
        );
        let sequential_peak = cost.peak_memory_upper;
        cost.memory_completion = cost.memory_completion.sequential(
            sequential_peak,
            retained_completion,
            local_cost
                .peak_memory_upper
                .saturating_add(overlapping_completion_peak),
        );
        let overlapping_non_revocable = child_costs
            .iter()
            .enumerate()
            .filter(|(index, _)| overlapping_children & (1_u64 << index) != 0)
            .map(|(_, child)| child.non_revocable_memory_upper)
            .max()
            .unwrap_or(0);
        let retained_non_revocable = local_cost
            .non_revocable_memory_upper
            .saturating_add(overlapping_non_revocable);
        cost.non_revocable_memory_upper =
            cost.non_revocable_memory_upper.max(retained_non_revocable);
        let overlapping_minimum = child_costs
            .iter()
            .enumerate()
            .filter(|(index, _)| overlapping_children & (1_u64 << index) != 0)
            .map(|(_, child)| child.minimum_memory_bytes)
            .max()
            .unwrap_or(0);
        // Revocable targets compete inside one query pool, but allocations
        // required merely to make progress cannot be reclaimed from an
        // overlapping child. Compose those execution floors additively;
        // treating them as a shared maximum admitted plans that the
        // runtime could immediately disprove.
        let retained_minimum = local_cost
            .minimum_memory_bytes
            .saturating_add(overlapping_minimum);
        cost.minimum_memory_bytes = cost.minimum_memory_bytes.max(retained_minimum);
        cost.revocable_memory_target = cost
            .revocable_memory_target
            .max(local_cost.revocable_memory_target);
        // Revocable operator state is governed by one shared query pool.
        // Overlapping spillable working sets therefore compose by maximum;
        // only their non-revocable portions must be added.
        cost.peak_memory_upper = cost
            .peak_memory_upper
            .max(local_cost.peak_memory_upper)
            .max(overlapping_peak)
            .max(retained_non_revocable)
            .max(retained_minimum)
            .max(retained_minimum.saturating_add(cost.revocable_memory_target));
    }
    cost.validate()?;
    Ok(ComposedCost {
        cost,
        source_work: source_work.into_boxed_slice(),
    })
}

fn ordered_source_filter_cost(filters: &[SourceFilterWork]) -> Result<SearchCost> {
    const SCALE: u64 = 1_000_000;
    fn multiply_ppm(left: u32, right: u32) -> u32 {
        ((u64::from(left) * u64::from(right) + SCALE / 2) / SCALE) as u32
    }

    let mut ordered = filters.to_vec();
    ordered.sort_by_key(|filter| filter.expected_retained_ppm);
    let mut expected_prefix = SCALE as u32;
    let mut cost = SearchCost::ZERO;
    for filter in ordered {
        cost = cost.sequential(
            filter
                .full_apply_cost
                .retain_work(expected_prefix, SCALE as u32)?,
        )?;
        expected_prefix = multiply_ppm(expected_prefix, filter.expected_retained_ppm);
    }
    Ok(cost)
}

fn transformation_dependency_versions(
    memo: &Memo,
    source: LogicalExprId,
) -> Result<Vec<(GroupId, u64)>> {
    let source = memo
        .logical_expr(source)
        .ok_or_else(|| paro_error::internal("transformation dependency source disappeared"))?;
    let mut pending = source.key.children.to_vec();
    let mut visited = BTreeSet::new();
    let mut dependencies = BTreeMap::new();
    while let Some(group) = pending.pop() {
        let group = memo.canonical_group(group);
        if !visited.insert(group) {
            continue;
        }
        let group_ref = memo.group(group).ok_or_else(|| {
            paro_error::internal("transformation dependency references an unknown group")
        })?;
        dependencies.insert(group, group_ref.logical_expression_version());
        for expression in group_ref.logical_exprs() {
            let expression = memo.logical_expr(*expression).ok_or_else(|| {
                paro_error::internal("transformation dependency expression disappeared")
            })?;
            pending.extend(expression.key.children.iter().copied());
        }
    }
    Ok(dependencies.into_iter().collect())
}

fn transformation_dependency_fingerprint(dependencies: &[(GroupId, u64)]) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_bytes(b"paro.transformation-dependencies.v1");
    builder.write_u64(dependencies.len() as u64);
    for (group, version) in dependencies {
        builder.write_u64(group.0 as u64);
        builder.write_u64(*version);
    }
    builder.finish()
}

fn transformation_event(
    group: GroupId,
    expression: LogicalExprId,
    rule: RuleId,
    dependency_version: Fingerprint,
) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_bytes(b"paro.transformation-fire.v2");
    builder.write_u64(group.0 as u64);
    builder.write_u64(expression.0 as u64);
    builder.write_u64(rule.0 as u64);
    builder.write_fingerprint(dependency_version);
    builder.finish()
}

fn admit_transformation_work(
    memo: &mut Memo,
    target: GroupId,
    source: LogicalExprId,
    rule: RuleId,
    dependency_version: Fingerprint,
) -> Result<bool> {
    let mut pending = vec![source];
    let mut visited = BTreeSet::new();
    while let Some(expression) = pending.pop() {
        let children = memo
            .logical_expr(expression)
            .ok_or_else(|| {
                paro_error::internal("rule work accounting references a missing expression")
            })?
            .key
            .children
            .clone();
        for child in children.iter().copied() {
            let child = memo.canonical_group(child);
            if !visited.insert(child) {
                continue;
            }
            let mut event = StableFingerprintBuilder::default();
            event.write_bytes(b"paro.rule-work.v2");
            event.write_u64(target.0 as u64);
            event.write_u64(source.0 as u64);
            event.write_u64(rule.0 as u64);
            event.write_fingerprint(dependency_version);
            event.write_u64(child.0 as u64);
            if memo
                .group_mut(target)
                .ok_or_else(|| paro_error::internal("rule work target group disappeared"))?
                .ledger
                .admit_optional(BudgetDimension::RuleWorkPerGroup, event.finish())
                == BudgetDecision::Exhausted
            {
                return Ok(false);
            }
            let child_expression = memo
                .group(child)
                .and_then(|group| group.logical_exprs().first())
                .copied()
                .ok_or_else(|| {
                    paro_error::internal("rule work accounting found an empty child group")
                })?;
            pending.push(child_expression);
        }
    }
    Ok(true)
}

fn validate_transformation_proof(
    rule: RuleId,
    source: LogicalExprId,
    proof: &EquivalenceProof,
) -> Result<()> {
    match proof {
        EquivalenceProof::Transformation {
            rule: proof_rule,
            source: proof_source,
            ..
        } if *proof_rule == rule && *proof_source == source => Ok(()),
        EquivalenceProof::SpecializedEnumerator {
            rule: proof_rule, ..
        } if *proof_rule == rule => Ok(()),
        _ => Err(paro_error::internal(
            "transformation output did not carry a matching equivalence proof",
        )),
    }
}

pub(crate) fn enforcer_cost(
    steps: &[EnforcerStep],
    input: EnforcerCostInput,
    calibration: &MachineCalibrationBundle,
) -> Result<Option<SearchCost>> {
    if steps.is_empty() {
        return Ok(Some(SearchCost::ZERO));
    }
    input.rows.checked_add(super::cost::CompactRange::ZERO)?;
    let mut work = LocalOperatorWork::default();
    let mut profile = ParallelWorkProfile::Pipeline;
    let mut peak_memory_upper = 0_u64;
    let mut spill_bytes_expected = 0_u64;
    let row_bytes_upper = bytes_for_rows(input.rows.upper, input.row_width_bytes);
    for step in steps {
        match step {
            EnforcerStep::Sort(_) | EnforcerStep::LocalSort(_) => {
                profile = ParallelWorkProfile::BlockingMerge;
                work.add(OP_ENFORCER_SORT_COMPARE, sort_work(input.rows)?)?;
                if row_bytes_upper > input.hard_memory_bytes {
                    if input.spill_policy == SpillPolicy::Forbidden {
                        return Ok(None);
                    }
                    peak_memory_upper = peak_memory_upper.max(input.hard_memory_bytes);
                    let spill_bytes = row_bytes_upper.saturating_mul(2);
                    spill_bytes_expected = spill_bytes_expected.saturating_add(spill_bytes);
                    work.add(
                        OP_ENFORCER_SPILL_PAGE,
                        super::cost::CompactRange::point(pages(spill_bytes) as f64)?,
                    )?;
                } else {
                    peak_memory_upper = peak_memory_upper.max(row_bytes_upper);
                }
            }
            EnforcerStep::MutationInputSpool { .. } | EnforcerStep::Spool => {
                // The current immutable materialized-handle ABI owns chunks in
                // memory. Advertising spill here would violate the runtime
                // contract, so a class that cannot contain the upper bound is
                // infeasible rather than silently overcommitted.
                if row_bytes_upper > input.hard_memory_bytes {
                    return Ok(None);
                }
                peak_memory_upper = peak_memory_upper.max(row_bytes_upper);
                work.add(OP_ENFORCER_STREAM_ROW, input.rows)?;
            }
            EnforcerStep::Fetch { values } | EnforcerStep::FetchPreservingOrder { values, .. } => {
                work.add(
                    OP_ENFORCER_RANDOM_FETCH,
                    scale_range(input.rows, values.len().max(1) as f64)?,
                )?;
            }
            EnforcerStep::Gather
            | EnforcerStep::RepartitionHash { .. }
            | EnforcerStep::RepartitionRange { .. }
            | EnforcerStep::MergeGather(_)
            | EnforcerStep::PrepareOrderedFetch(_)
            | EnforcerStep::Flatten
            | EnforcerStep::Factorize(_) => {
                work.add(OP_ENFORCER_STREAM_ROW, input.rows)?;
            }
        }
    }
    let mut result = calibration.fold_for_tasks(&work, profile, input.max_parallel_tasks)?;
    result.peak_memory_upper = peak_memory_upper;
    result.spill_bytes_expected = spill_bytes_expected;
    result.validate()?;
    Ok(Some(result))
}

fn scale_range(range: super::cost::CompactRange, factor: f64) -> Result<super::cost::CompactRange> {
    super::cost::CompactRange::new(
        range.lower * factor,
        range.expected * factor,
        range.upper * factor,
    )
}

fn sort_work(rows: super::cost::CompactRange) -> Result<super::cost::CompactRange> {
    let comparisons = |rows: f64| {
        if rows <= 1.0 {
            rows
        } else {
            rows * rows.log2()
        }
    };
    super::cost::CompactRange::new(
        comparisons(rows.lower),
        comparisons(rows.expected),
        comparisons(rows.upper),
    )
}

fn bytes_for_rows(rows: f64, width: u64) -> u64 {
    if !rows.is_finite() || rows >= u64::MAX as f64 / width.max(1) as f64 {
        u64::MAX
    } else {
        rows.ceil().max(0.0) as u64 * width.max(1)
    }
}

fn pages(bytes: u64) -> u64 {
    bytes.saturating_add(4095) / 4096
}

fn enforced_fingerprint(
    base: Fingerprint,
    steps: &[EnforcerStep],
    children: impl IntoIterator<Item = Fingerprint>,
) -> Fingerprint {
    let children: Vec<_> = children.into_iter().collect();
    if steps.is_empty() && children.is_empty() {
        return base;
    }
    let mut builder = StableFingerprintBuilder::default();
    builder.write_fingerprint(base);
    for child in children {
        builder.write_fingerprint(child);
    }
    for step in steps {
        builder.write_fingerprint(step.stable_fingerprint());
    }
    builder.finish()
}

#[cfg(test)]
#[path = "engine/tests.rs"]
mod tests;
