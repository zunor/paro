// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Deterministic mandatory-baseline plus bounded optional Cascades search.

use std::collections::{BTreeMap, BTreeSet};

use paro_common::error::{self as paro_error, Result};

use super::budget::{BudgetDecision, BudgetDimension};
use super::calibration::{
    LocalOperatorWork, MachineCalibrationBundle, OP_ENFORCER_RANDOM_FETCH,
    OP_ENFORCER_SORT_COMPARE, OP_ENFORCER_SPILL_PAGE, OP_ENFORCER_STREAM_ROW,
};
use super::cost::{CompactRange, ResourceDimension, SearchCost};
use super::enforcer::{EnforcementPlanner, EnforcerStep};
use super::grant::{derive_grant_sensitivity, verify_grant_invariance, GrantSensitivitySummary};
use super::ids::{
    AdmissibleGrantSetId, Fingerprint, GroupId, ImplementationId, LogicalExprId, PhysicalExprId,
    ResourceGrantClassId, RuleId, StableFingerprintBuilder,
};
use super::memo::{EquivalenceProof, GrantGoalKey, Memo, OptimizationGoal, Winner};
use super::region::{
    JointCostProof, RegionArtifactKind, RegionCandidateContract, RegionDependencyEdge,
    RegionDependencyKind,
};
use super::rules::{
    CostComposition, ImplementationContext, ImplementationRegistry, PhysicalCandidate, RuleContext,
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
}

impl EnforcerCostInput {
    pub fn unbounded(rows: super::cost::CompactRange, row_width_bytes: u64) -> Self {
        Self {
            rows,
            row_width_bytes: row_width_bytes.max(1),
            hard_memory_bytes: u64::MAX,
            spill_policy: SpillPolicy::Allowed,
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
    effective_rule_insertions: BTreeMap<RuleId, u64>,
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
            effective_rule_insertions: BTreeMap::new(),
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
                for class in classes {
                    let goal = OptimizationGoal {
                        grant: GrantGoalKey::Class(class),
                        ..base_goal
                    };
                    winners.push(GrantWinner {
                        class,
                        goal,
                        winner: self.optimize(root, goal, SearchMode::Direct)?,
                    });
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
            if !self.memo.mark_rule_applied(expression, rule)? {
                continue;
            }
            let event = transformation_event(group, expression, rule);
            let admitted = self
                .memo
                .group_mut(group)
                .ok_or_else(|| paro_error::internal("rule task references unknown group"))?
                .ledger
                .admit_optional(BudgetDimension::RuleFirePerGroup, event);
            if admitted == BudgetDecision::Exhausted {
                continue;
            }
            let outputs = {
                let rule_impl = self
                    .registry
                    .transformation(rule)
                    .ok_or_else(|| paro_error::internal("rule disappeared from registry"))?;
                let context = RuleContext {
                    memo: &self.memo,
                    group,
                };
                rule_impl.apply(expression, &context)?
            };
            let mut inserted_groups = BTreeSet::new();
            for output in outputs {
                validate_transformation_proof(rule, expression, &output.proof)?;
                let target = self.memo.canonical_group(output.target_group);
                let output_event = output.key.stable_fingerprint();
                let admitted = self
                    .memo
                    .group_mut(target)
                    .ok_or_else(|| {
                        paro_error::internal("transformation targets an unknown Memo group")
                    })?
                    .ledger
                    .admit_optional(BudgetDimension::LogicalExprPerGroup, output_event);
                if admitted == BudgetDecision::Exhausted {
                    continue;
                }
                let before = self
                    .memo
                    .group(target)
                    .map(|group| group.logical_exprs().len())
                    .unwrap_or(0);
                self.memo
                    .insert_logical(target, output.key, output.payload, output.proof)?;
                let after = self.memo.group(target).unwrap().logical_exprs().len();
                if after > before {
                    *self.effective_rule_insertions.entry(rule).or_default() +=
                        u64::try_from(after - before).unwrap_or(u64::MAX);
                    inserted_groups.insert(target);
                }
            }
            for target in inserted_groups {
                self.schedule_transformations(target, &mut agenda)?;
            }
        }
        Ok(())
    }

    pub fn effective_rule_insertions(&self) -> &BTreeMap<RuleId, u64> {
        &self.effective_rule_insertions
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
                    return Ok(());
                }
                admitted.insert(candidate.physical_fingerprint);
            }
            let decision = self.memo.group_mut(group).unwrap().ledger.admit_optional(
                BudgetDimension::PhysicalExprPerGroup,
                candidate.stable_event(goal),
            );
            if decision == BudgetDecision::Exhausted {
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
            let mut children_feasible = true;
            for (child, child_goal) in recipe.child_goals.iter().copied() {
                self.optimize_group(child, child_goal)?;
                let Some(child_cost) = self
                    .memo
                    .group(child)
                    .and_then(|group| group.winner(child_goal))
                    .map(|winner| winner.cost)
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
                child_costs.push(child_cost);
            }
            if !children_feasible {
                continue;
            }
            let Some(local_cost) = fit_local_retained_state_to_grant(
                recipe.local_cost,
                &child_costs,
                recipe.cost_composition,
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
            let mut cost =
                compose_candidate_cost(local_cost, &child_costs, recipe.cost_composition)?;
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
            cost = cost.sequential(enforcer_cost)?;
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
            let joint_cost_proof = build_joint_cost_proof(group, &recipe, local_cost)?;
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
                    cost_composition: recipe.cost_composition,
                    cost,
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
            .map(|group| group.physical_exprs().to_vec())
            .unwrap_or_default();
        paro_error::internal(format!(
            "no feasible physical plan exists for Memo group {group:?} with goal {goal:?}; logical={logical:?}, physical={physical:?}"
        ))
    }
}

fn build_joint_cost_proof(
    owner_group: GroupId,
    recipe: &CostRecipe,
    local_cost: SearchCost,
) -> Result<Option<JointCostProof>> {
    let Some(region) = &recipe.region else {
        return Ok(None);
    };
    let mut dependencies = recipe
        .child_goals
        .iter()
        .map(|(child, _)| RegionDependencyEdge {
            producer: *child,
            consumer: owner_group,
            kind: RegionDependencyKind::Data,
        })
        .collect::<Vec<_>>();
    if region
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == RegionArtifactKind::RuntimeFilter)
    {
        let [(probe, _), (build, _)] = recipe.child_goals.as_ref() else {
            return Err(paro_error::internal(
                "runtime-filter region recipe is not a binary join",
            ));
        };
        dependencies.push(RegionDependencyEdge {
            producer: *build,
            consumer: *probe,
            kind: RegionDependencyKind::ControlWaitComplete,
        });
    }
    dependencies.sort_unstable();
    Ok(Some(JointCostProof {
        region: region.region,
        facets: region.facets.clone(),
        owner_group,
        boundary_goals: recipe.child_goals.clone(),
        owned_artifacts: region.artifacts.clone(),
        dependencies: dependencies.into_boxed_slice(),
        local_cost,
        cost_composition: recipe.cost_composition,
        candidate_fingerprint: recipe.physical_fingerprint,
    }))
}

fn fit_local_retained_state_to_grant(
    mut local_cost: SearchCost,
    child_costs: &[SearchCost],
    composition: CostComposition,
    spillable: bool,
    grant: EnforcerCostInput,
) -> Result<Option<SearchCost>> {
    let CostComposition::RetainedState {
        overlapping_children,
    } = composition
    else {
        return Ok(Some(local_cost));
    };
    if grant.hard_memory_bytes == u64::MAX {
        return Ok(Some(local_cost));
    }
    let overlapping_peak = child_costs
        .iter()
        .enumerate()
        .filter(|(index, _)| overlapping_children & (1_u64 << index) != 0)
        .map(|(_, child)| child.peak_memory_upper)
        .max()
        .unwrap_or(0);
    let available = grant.hard_memory_bytes.saturating_sub(overlapping_peak);
    if local_cost.peak_memory_upper <= available {
        return Ok(Some(local_cost));
    }
    if !spillable || grant.spill_policy == SpillPolicy::Forbidden {
        return Ok(None);
    }

    let spilled = local_cost.peak_memory_upper.saturating_sub(available);
    local_cost.peak_memory_upper = available;
    add_composition_spill_cost(&mut local_cost, spilled)?;
    Ok(Some(local_cost))
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

pub(crate) fn compose_candidate_cost(
    local_cost: SearchCost,
    child_costs: &[SearchCost],
    composition: CostComposition,
) -> Result<SearchCost> {
    let mut cost = local_cost;
    for child in child_costs {
        cost = child.sequential(cost)?;
    }
    match composition {
        CostComposition::Sequential => {}
        CostComposition::RetainedState {
            overlapping_children,
        } => {
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
            cost.peak_memory_upper = cost.peak_memory_upper.max(
                local_cost
                    .peak_memory_upper
                    .saturating_add(overlapping_peak),
            );
        }
    }
    cost.validate()?;
    Ok(cost)
}

fn transformation_event(group: GroupId, expression: LogicalExprId, rule: RuleId) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_u64(group.0 as u64);
    builder.write_u64(expression.0 as u64);
    builder.write_u64(rule.0 as u64);
    builder.finish()
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
    let mut peak_memory_upper = 0_u64;
    let mut spill_bytes_expected = 0_u64;
    let row_bytes_upper = bytes_for_rows(input.rows.upper, input.row_width_bytes);
    for step in steps {
        match step {
            EnforcerStep::Sort(_) | EnforcerStep::LocalSort(_) => {
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
    let mut result = calibration.fold(&work)?;
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
mod tests {
    use paro_common::types::LogicalType;

    use super::*;
    use crate::cascades::column::{ColumnDesc, ColumnOrigin, ColumnVisibility, GroupSchema};
    use crate::cascades::cost::{CompactRange, ScoreSummary};
    use crate::cascades::ids::{
        AdmissibleGrantSetId, ColumnId, LogicalPayloadId, ObjectiveProfileId,
        OptimizationContextId, PhysicalPayloadId,
    };
    use crate::cascades::memo::{
        GrantGoalKey, LogicalExprKey, LogicalProperties, PhysicalExprKey, RowGoal,
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
            ctx: &RuleContext<'_>,
        ) -> Result<Box<[EquivalentExpression]>> {
            Ok(vec![EquivalentExpression {
                target_group: ctx.group,
                key: LogicalExprKey {
                    operator: Fingerprint(11),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                payload: LogicalPayloadId(1),
                proof: EquivalenceProof::Transformation {
                    rule: self.id(),
                    source: expr,
                    premise: Fingerprint(77),
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
        let group = memo.create_group(schema(), LogicalProperties::default());
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
            ctx: &RuleContext<'_>,
        ) -> Result<Box<[EquivalentExpression]>> {
            Ok(vec![EquivalentExpression {
                target_group: ctx.group,
                key: LogicalExprKey {
                    operator: Fingerprint(31),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                payload: LogicalPayloadId(2),
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
        let child = memo.create_group(schema(), LogicalProperties::default());
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
        let root = memo.create_group(schema(), LogicalProperties::default());
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
            cost.resources_expected[super::super::cost::ResourceDimension::SequentialIo as usize]
                > 0.0
        );
    }

    #[test]
    fn retained_operator_state_overlaps_child_pipeline_memory() {
        let local = SearchCost {
            peak_memory_upper: 100,
            ..cost(1.0)
        };
        let child = SearchCost {
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
        let child = memo.create_group(schema(), LogicalProperties::default());
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
        let root = memo.create_group(schema(), LogicalProperties::default());
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
}
