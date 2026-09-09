// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Deterministic mandatory-baseline plus bounded optional Cascades search.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use paro_common::error::{self as paro_error, Result};

use super::budget::{BudgetDecision, BudgetDimension};
use super::calibration::{
    LocalOperatorWork, MachineCalibrationBundle, ParallelWorkProfile, OP_ENFORCER_RANDOM_FETCH,
    OP_ENFORCER_SORT_COMPARE, OP_ENFORCER_SPILL_PAGE, OP_ENFORCER_STREAM_ROW,
};
use super::cost::{CompactRange, MemoryCompletion, ResourceDimension, SearchCost};
use super::enforcer::{EnforcementPlanner, EnforcerStep};
use super::governor::{Governor, PlanMilestone, PlanningPolicy};
use super::grant::{derive_grant_sensitivity, verify_grant_sharing, GrantSensitivitySummary};
use super::ids::{
    AdmissibleGrantSetId, CandidateId, Fingerprint, GroupId, ImplementationId, LogicalExprId,
    PhysicalExprId, ResourceGrantClassId, RuleId, StableFingerprintBuilder,
};
use super::memo::{
    CandidatePreview, CandidateSummary, ChildWinnerRef, EquivalenceProof, GrantGoalKey,
    GroupCardinality, LogicalProperties, Memo, OptimizationGoal, Winner,
};
use super::quality::QualityBundleRegistry;
use super::region::{
    JointCostProof, RegionArtifactKind, RegionBoundaryEndpoint, RegionCandidateContract,
    RegionDependencyEdge, RegionDependencyKind,
};
#[cfg(test)]
use super::rules::WorkSourceId;
use super::rules::{
    CostComposition, ImplementationContext, ImplementationRegistry, PatternEnumerationCompletion,
    PatternRead, PhysicalCandidate, RuleContext, SourceFilterWork, SourceRetentionProof,
    SourceWork, SourceWorkData, TaskSupplyContract, TransformContext,
};
use super::tasks::{
    Cursor, ReadSet, StopReason, TaskId, TaskIntent, TaskOutcome, TaskRegistry, TaskRequest,
};
use crate::physical::{ResourceGrantClass, SpillPolicy};

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
    expressions: Vec<(GroupId, LogicalExprId)>,
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
        self.tasks.pop_first().map(|(_, task)| task)
    }
}

#[derive(Debug, Clone)]
struct CostRecipe {
    child_goals: Box<[(GroupId, OptimizationGoal)]>,
    local_cost: SearchCost,
    source_filter_apply_cost: Option<SearchCost>,
    task_supply: TaskSupplyContract,
    cost_composition: CostComposition,
    spillable: bool,
    enforcer_cost_input: EnforcerCostInput,
    physical_fingerprint: Fingerprint,
    region: Option<RegionCandidateContract>,
}

/// Exact query-local identity for one child-frontier combination. The budget
/// ledger still stores its compact event handle, but the handle is interned
/// from this full tuple instead of re-hashing the tuple on every hot-path
/// admission. Candidate IDs remain distinct across frontier pruning and
/// cost epochs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ChildCombinationIdentity {
    physical: PhysicalExprId,
    goal: OptimizationGoal,
    recipe: Fingerprint,
    children: Box<[CandidateId]>,
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

#[derive(Debug, Clone)]
struct BindingApplication {
    binding: super::rules::PatternBinding,
    reads: Box<[PatternRead]>,
    fact_value: Option<Fingerprint>,
}

type BindingApplications = BTreeMap<(TransformationTaskId, Fingerprint), Vec<BindingApplication>>;

/// Per-rule work phases used by the cold-search attribution report.  These
/// counters deliberately describe the existing single-worker engine; they do
/// not imply parallel width or a completed search frontier.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuleWorkProfile {
    /// Transformation tasks that reached the matcher.
    pub discovered: u64,
    /// Exact bindings returned by the matcher.
    pub matched: u64,
    /// Bindings for which the rule returned at least one output.
    pub applicable: u64,
    /// Output alternatives constructed by the rule before Memo duplicate
    /// elimination/publication.
    pub constructed: u64,
    /// Alternatives that became new Memo logical expressions.
    pub published: u64,
    /// Bindings rejected by an error, empty result, output-contract violation,
    /// or invalid/duplicate publication attempt.
    pub rejected: u64,
    /// Valid rule applications which produced no new expression because the
    /// result was already present.
    pub ineffective: u64,
    /// Diagnostic-only elapsed offsets from the start of the optimizer call.
    /// They are optional because the normal trace-off path does not maintain
    /// a timing clock or phase map.
    pub first_discovered_us: Option<u64>,
    pub first_matched_us: Option<u64>,
    pub first_applicable_us: Option<u64>,
    pub first_published_us: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchMilestones {
    /// First root candidate published by the protected mandatory search.
    pub first_safe_us: Option<u64>,
    pub safe_candidate: Option<CandidateId>,
    /// First root candidate published after optional search began. This is a
    /// physical readiness observation, not a proof that the candidate is the
    /// final winner or that optional search is complete.
    pub first_optional_ready_us: Option<u64>,
    pub optional_ready_candidate: Option<CandidateId>,
    /// First root candidate that changed the selected frontier entry after
    /// optional search began.
    pub first_optional_selected_us: Option<u64>,
    pub optional_selected_candidate: Option<CandidateId>,
    /// First logical publication made by optional transformation search.
    pub first_logical_publication_us: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
enum RuleWorkPhase {
    Discovered,
    Matched,
    Applicable,
    Published,
}

/// The engine is deliberately operator-agnostic. Domain implementations live
/// in the registry; this type owns stable scheduling, budgets, enforcement,
/// recursive goal optimization, and winner verification.
#[derive(Debug)]
pub struct CascadesEngine {
    mandatory_only: bool,
    memo: Memo,
    registry: ImplementationRegistry,
    enforcement: EnforcementPlanner,
    recipes: BTreeMap<(PhysicalExprId, OptimizationGoal, Fingerprint), Arc<CostRecipe>>,
    infeasible_goals: BTreeSet<(GroupId, OptimizationGoal)>,
    active_goals: BTreeSet<(GroupId, OptimizationGoal)>,
    grant_class_sets: BTreeMap<ResourceGrantClassId, AdmissibleGrantSetId>,
    grant_classes: BTreeMap<ResourceGrantClassId, ResourceGrantClass>,
    grant_sensitivity: BTreeMap<GroupId, GrantSensitivitySummary>,
    rule_attempts: BTreeMap<RuleId, u64>,
    effective_rule_insertions: BTreeMap<RuleId, u64>,
    rule_elapsed: BTreeMap<RuleId, Duration>,
    rule_allocated_bytes: BTreeMap<RuleId, u64>,
    rule_budget_exhaustions: BTreeMap<RuleId, u64>,
    rule_work_profile: BTreeMap<RuleId, RuleWorkProfile>,
    /// Rule phase counters are diagnostic-only.  The normal trace-off path
    /// must not pay a BTreeMap lookup for every transformation task.
    collect_rule_work_profile: bool,
    profile_started_at: Option<Instant>,
    search_milestones: SearchMilestones,
    milestone_root: Option<GroupId>,
    optional_search_started: bool,
    transformation_bindings: u64,
    fact_value_revalidation_hits: u64,
    fact_value_revalidation_misses: u64,
    /// Last child-expression frontier consumed by each transformation task.
    /// A task that declined a match is recorded as well: a later child
    /// alternative may make that same pattern applicable.
    transformation_observations: BTreeMap<TransformationTaskId, Box<[PatternRead]>>,
    /// Sorted unique group cursors, updated in place when another binding
    /// reads a previously known fact. Do not reconstruct a tree map per read.
    transformation_fact_observations: BTreeMap<TransformationTaskId, Vec<PatternRead>>,
    /// Collision-safe exact bindings already evaluated under these facts.
    /// Discovery can wake a task without invalidating its earlier bindings.
    transformation_applications: BindingApplications,
    /// Reverse index for incrementally closing transformation dependencies.
    /// Subscribers are woken only after a Memo transaction commits.
    transformation_subscribers: BTreeMap<GroupId, BTreeSet<TransformationTaskId>>,
    region_candidates: BTreeMap<super::ids::RegionId, BTreeSet<Fingerprint>>,
    /// Physical context work is keyed by the complete OptimizationGoal, not
    /// by group alone.  These counters make context reuse visible without
    /// retaining a second cache or changing the publication protocol.
    physical_subproblem_requests: u64,
    physical_subproblem_reuses: u64,
    physical_subproblem_evaluations: u64,
    physical_implementation_requests: u64,
    child_combination_events: BTreeMap<ChildCombinationIdentity, Fingerprint>,
    next_child_combination_event: u128,
    /// Shared task identity/progress protocol.  Memo remains the owner of
    /// expressions, candidates and facts; this registry only coordinates
    /// resumable work and publication state.
    task_registry: TaskRegistry,
    governor: Governor,
    quality_bundles: QualityBundleRegistry,
}

impl CascadesEngine {
    pub fn new(memo: Memo, registry: ImplementationRegistry) -> Self {
        let budget = memo.budget().clone();
        let mut quality_bundles = QualityBundleRegistry::default();
        quality_bundles
            .register_builtin_f1_f4()
            .expect("built-in quality bundles must have unique identities");
        Self {
            mandatory_only: false,
            memo,
            registry,
            enforcement: EnforcementPlanner::new(
                budget.max_optional_enforcer_depth,
                budget.max_optional_enforcer_chains_per_goal,
            ),
            recipes: BTreeMap::new(),
            infeasible_goals: BTreeSet::new(),
            active_goals: BTreeSet::new(),
            grant_class_sets: BTreeMap::new(),
            grant_classes: BTreeMap::new(),
            grant_sensitivity: BTreeMap::new(),
            rule_attempts: BTreeMap::new(),
            effective_rule_insertions: BTreeMap::new(),
            rule_elapsed: BTreeMap::new(),
            rule_allocated_bytes: BTreeMap::new(),
            rule_budget_exhaustions: BTreeMap::new(),
            rule_work_profile: BTreeMap::new(),
            collect_rule_work_profile: false,
            profile_started_at: None,
            search_milestones: SearchMilestones::default(),
            milestone_root: None,
            optional_search_started: false,
            transformation_bindings: 0,
            fact_value_revalidation_hits: 0,
            fact_value_revalidation_misses: 0,
            transformation_observations: BTreeMap::new(),
            transformation_fact_observations: BTreeMap::new(),
            transformation_applications: BTreeMap::new(),
            transformation_subscribers: BTreeMap::new(),
            region_candidates: BTreeMap::new(),
            physical_subproblem_requests: 0,
            physical_subproblem_reuses: 0,
            physical_subproblem_evaluations: 0,
            physical_implementation_requests: 0,
            child_combination_events: BTreeMap::new(),
            next_child_combination_event: 1,
            task_registry: TaskRegistry::default(),
            governor: Governor::new(PlanningPolicy::default())
                .expect("default planning policy must be valid"),
            quality_bundles,
        }
    }

    pub fn memo(&self) -> &Memo {
        &self.memo
    }

    pub fn memo_mut(&mut self) -> &mut Memo {
        &mut self.memo
    }

    pub fn task_registry(&self) -> &TaskRegistry {
        &self.task_registry
    }

    pub fn governor(&self) -> &Governor {
        &self.governor
    }

    pub fn quality_bundles(&self) -> &QualityBundleRegistry {
        &self.quality_bundles
    }

    /// Fact producers publish quality results through the same query-local
    /// registry used by the governor; callers must provide current ReadSet
    /// and native choice identities.
    pub fn quality_bundles_mut(&mut self) -> &mut QualityBundleRegistry {
        &mut self.quality_bundles
    }

    pub fn governor_mut(&mut self) -> &mut Governor {
        &mut self.governor
    }

    /// Enable the per-rule phase ledger only for an explicitly requested
    /// diagnostic cohort.  Normal C1 must remain trace-off and allocation-free
    /// with respect to this optional attribution.
    pub fn set_rule_work_profile_enabled(&mut self, enabled: bool) {
        self.collect_rule_work_profile = enabled;
    }

    fn begin_diagnostic_profile(&mut self, root: GroupId) {
        self.search_milestones = SearchMilestones::default();
        self.milestone_root = self
            .collect_rule_work_profile
            .then_some(self.memo.canonical_group(root));
        self.optional_search_started = false;
        self.profile_started_at = self.collect_rule_work_profile.then(Instant::now);
        if self.collect_rule_work_profile {
            self.rule_work_profile.clear();
        }
    }

    fn profile_elapsed_us(&self) -> Option<u64> {
        self.profile_started_at
            .map(|started| u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX))
    }

    fn note_rule_phase(&mut self, rule: RuleId, phase: RuleWorkPhase) {
        Self::note_rule_phase_at(
            &mut self.rule_work_profile,
            self.collect_rule_work_profile,
            self.profile_started_at,
            rule,
            phase,
        );
    }

    fn note_rule_phase_at(
        profiles: &mut BTreeMap<RuleId, RuleWorkProfile>,
        collect: bool,
        started_at: Option<Instant>,
        rule: RuleId,
        phase: RuleWorkPhase,
    ) {
        if !collect {
            return;
        }
        let Some(started_at) = started_at else {
            return;
        };
        let elapsed = u64::try_from(started_at.elapsed().as_micros()).unwrap_or(u64::MAX);
        let profile = profiles.entry(rule).or_default();
        let slot = match phase {
            RuleWorkPhase::Discovered => &mut profile.first_discovered_us,
            RuleWorkPhase::Matched => &mut profile.first_matched_us,
            RuleWorkPhase::Applicable => &mut profile.first_applicable_us,
            RuleWorkPhase::Published => &mut profile.first_published_us,
        };
        slot.get_or_insert(elapsed);
    }

    fn note_logical_publication(&mut self) {
        if self.collect_rule_work_profile
            && self
                .search_milestones
                .first_logical_publication_us
                .is_none()
        {
            self.search_milestones.first_logical_publication_us = self.profile_elapsed_us();
        }
    }

    fn note_safe_candidate(&mut self, candidate: CandidateId) {
        if !self.collect_rule_work_profile || self.search_milestones.first_safe_us.is_some() {
            return;
        }
        self.search_milestones.first_safe_us = self.profile_elapsed_us();
        self.search_milestones.safe_candidate = Some(candidate);
    }

    fn note_physical_candidate(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
        selected_changed: bool,
    ) {
        if !self.collect_rule_work_profile
            || self.optional_search_started
                && self.milestone_root != Some(self.memo.canonical_group(group))
        {
            return;
        }
        let Some(candidate) = self
            .memo
            .group(group)
            .and_then(|group| group.winner(goal))
            .map(|winner| winner.candidate)
        else {
            return;
        };
        if self.optional_search_started {
            if self.search_milestones.first_optional_ready_us.is_none() {
                self.search_milestones.first_optional_ready_us = self.profile_elapsed_us();
                self.search_milestones.optional_ready_candidate = Some(candidate);
            }
            if selected_changed && self.search_milestones.first_optional_selected_us.is_none() {
                self.search_milestones.first_optional_selected_us = self.profile_elapsed_us();
                self.search_milestones.optional_selected_candidate = Some(candidate);
            }
        }
    }

    pub fn optimize(
        &mut self,
        root: GroupId,
        goal: OptimizationGoal,
        mode: SearchMode,
    ) -> Result<Winner> {
        self.begin_diagnostic_profile(root);
        // CascadesEngine is also usable with a hand-built Memo. Seal at the
        // actual phase boundary rather than relying on one particular builder
        // to have done so: optional rules may only propagate expression-path
        // contexts admitted with the initial logical forest.
        self.memo.freeze_optimization_contexts()?;
        let root = self.memo.canonical_group(root);
        let mut incumbent = None;
        if mode == SearchMode::Memo {
            let phase = self.memo.control().incumbent_phase();
            self.mandatory_only = true;
            let baseline = self.optimize_group(root, goal);
            self.mandatory_only = false;
            drop(phase);
            baseline?;
            super::verifier::MemoVerifier::verify(&self.memo, None)?;
            incumbent = self
                .memo
                .group(root)
                .and_then(|group| group.winner(goal))
                .cloned();
            if let Some(incumbent) = &incumbent {
                self.governor.mark_safe(incumbent.candidate);
                self.note_safe_candidate(incumbent.candidate);
            }
            self.memo.control().begin_optional();
            if !self.memo.control().checkpoint()? {
                self.governor
                    .resource_stop(BudgetDimension::SearchCandidate);
                return incumbent.ok_or_else(|| self.infeasible_goal_error(root, goal));
            }
            self.reset_cost_epoch()?;
            self.optional_search_started = self.collect_rule_work_profile;
            // The archived mandatory incumbent remains the safe plan for this
            // new cost epoch.  As soon as optional work publishes enough new
            // logical alternatives, mandatory physical work is re-costed
            // incrementally below; this keeps the incumbent fallback semantics
            // intact when cancellation happens before the first publication.
            self.explore_transformations_with_interleave(Some((root, goal)))?;
        }
        self.optimize_group(root, goal)?;
        super::verifier::MemoVerifier::verify(&self.memo, None)?;
        self.memo
            .group(root)
            .and_then(|group| group.winner(goal))
            .cloned()
            .or(incumbent)
            .ok_or_else(|| self.infeasible_goal_error(root, goal))
    }

    fn reset_cost_epoch(&mut self) -> Result<()> {
        self.memo.clear_cost_frontiers()?;
        // Physical recipes are immutable descriptions of already-admitted
        // implementations. Keep them across a fact/cost epoch so only the
        // affected winner frontiers are recomposed; rebuilding every recipe
        // made a grant or logical refresh pay the same construction cost
        // again. New logical expressions still add recipes incrementally.
        self.infeasible_goals.clear();
        self.grant_sensitivity.clear();
        self.task_registry.invalidate_physical_tasks()?;
        Ok(())
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
        classes: impl IntoIterator<Item = ResourceGrantClass>,
        mode: SearchMode,
    ) -> Result<GrantOptimization> {
        let mut class_map = BTreeMap::new();
        for class in classes {
            if class.max_parallel_tasks == 0 {
                return Err(paro_error::internal("grant class has zero worker capacity"));
            }
            if class_map
                .insert(class.id, class)
                .is_some_and(|prior| prior != class)
            {
                return Err(paro_error::internal(
                    "grant class id has conflicting operating points",
                ));
            }
        }
        let classes = class_map;
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
        let root = self.memo.canonical_group(root);
        self.begin_diagnostic_profile(root);
        if mode == SearchMode::Memo {
            let phase = self.memo.control().incumbent_phase();
            self.mandatory_only = true;
            let incumbent = self.optimize_grant_classes(root, base_goal, admissible_set, &classes);
            self.mandatory_only = false;
            drop(phase);
            if incumbent
                .as_ref()
                .is_err_and(|error| error.is_query_canceled())
            {
                return incumbent;
            }
            // An infeasible initial implementation may become feasible under
            // an optional rewrite. Do not report a fabricated incumbent in
            // that case, but still permit the requested bounded search.
            if incumbent.is_ok() {
                super::verifier::MemoVerifier::verify(&self.memo, None)?;
                if let Some(incumbent) = incumbent
                    .as_ref()
                    .ok()
                    .and_then(|optimization| optimization.winners.first())
                {
                    self.governor.mark_safe(incumbent.winner.candidate);
                    self.note_safe_candidate(incumbent.winner.candidate);
                }
            }
            self.memo.control().begin_optional();
            if !self.memo.control().checkpoint()? {
                self.governor
                    .resource_stop(BudgetDimension::SearchCandidate);
                return incumbent;
            }
            self.reset_cost_epoch()?;
            self.optional_search_started = self.collect_rule_work_profile;
            self.explore_transformations()?;
            if !self.memo.control().checkpoint()? {
                return incumbent;
            }
            let result = self.optimize_grant_classes(root, base_goal, admissible_set, &classes);
            if self.memo.control().deadline_reached() && result.is_err() {
                return incumbent;
            }
            return result;
        }
        self.optimize_grant_classes(root, base_goal, admissible_set, &classes)
    }

    fn optimize_grant_classes(
        &mut self,
        root: GroupId,
        base_goal: OptimizationGoal,
        admissible_set: AdmissibleGrantSetId,
        classes: &BTreeMap<ResourceGrantClassId, ResourceGrantClass>,
    ) -> Result<GrantOptimization> {
        self.grant_class_sets.clear();
        self.grant_class_sets
            .extend(classes.keys().copied().map(|class| (class, admissible_set)));
        self.grant_classes.clone_from(classes);
        self.grant_sensitivity.clear();
        let root = self.memo.canonical_group(root);
        let sensitivity = self.goal_grant_sensitivity(root, base_goal.required)?;
        let mut winners = Vec::with_capacity(classes.len());
        let mut last_infeasible = None;
        for class in classes.values().copied() {
            let goal = OptimizationGoal {
                grant: sensitivity.goal_for(admissible_set, class),
                ..base_goal
            };
            self.optimize_group(root, goal)?;
            if let Some(winner) = self
                .memo
                .group(root)
                .and_then(|group| group.winner_frontier(goal))
                .and_then(|frontier| {
                    // Sharing removes redundant costing, not per-class
                    // feasibility. A memory-independent implementation can
                    // still have a finite, nonzero working set.
                    frontier.candidates().iter().find(|winner| {
                        winner.cost.peak_memory_upper <= class.hard_memory_bytes
                            && (winner.cost.spill_bytes_expected == 0
                                || class.spill_policy == SpillPolicy::Allowed)
                    })
                })
                .map(|winner| winner.as_ref().clone())
            {
                winners.push(GrantWinner {
                    class: class.id,
                    goal,
                    winner,
                });
            } else {
                last_infeasible = Some(goal);
            }
        }
        super::verifier::MemoVerifier::verify(&self.memo, None)?;
        if let GrantSensitivitySummary::Shared(proof) = &sensitivity {
            verify_grant_sharing(&self.memo, &self.registry, proof)?;
        }
        if winners.is_empty() {
            return Err(self.infeasible_goal_error(root, last_infeasible.unwrap_or(base_goal)));
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
        required: super::ids::PropertySetId,
        parent: GrantGoalKey,
    ) -> Result<GrantGoalKey> {
        let sensitivity = self.goal_grant_sensitivity(child, required)?;
        match parent {
            GrantGoalKey::Invariant(set) => match sensitivity {
                GrantSensitivitySummary::Shared(proof)
                    if proof.dependency == super::rules::GrantDependencyDescriptor::Invariant =>
                {
                    Ok(GrantGoalKey::Invariant(set))
                }
                _ => Err(paro_error::internal(
                    "invariant goal lost a child grant dependency",
                )),
            },
            GrantGoalKey::Parallelism { admissible, tasks } => match sensitivity {
                GrantSensitivitySummary::Shared(proof) => match proof.dependency {
                    super::rules::GrantDependencyDescriptor::Invariant => {
                        Ok(GrantGoalKey::Invariant(admissible))
                    }
                    super::rules::GrantDependencyDescriptor::Parallelism => Ok(parent),
                    super::rules::GrantDependencyDescriptor::Sensitive => {
                        Err(paro_error::internal("invalid grant-sharing proof"))
                    }
                },
                GrantSensitivitySummary::Sensitive { .. }
                | GrantSensitivitySummary::RequiredEnforcement { .. } => Err(paro_error::internal(
                    format!("capacity-only goal ({tasks} tasks) lost a child memory dependency"),
                )),
            },
            GrantGoalKey::Class(class) => {
                let set = self.grant_class_sets.get(&class).copied().ok_or_else(|| {
                    paro_error::internal(
                        "class goal was optimized outside a declared admissible grant set",
                    )
                })?;
                let operating_point = self.grant_classes.get(&class).copied().ok_or_else(|| {
                    paro_error::internal("class goal lost its worker-capacity contract")
                })?;
                Ok(sensitivity.goal_for(set, operating_point))
            }
        }
    }

    fn goal_grant_sensitivity(
        &mut self,
        group: GroupId,
        required: super::ids::PropertySetId,
    ) -> Result<GrantSensitivitySummary> {
        let properties = self.memo.required(required).ok_or_else(|| {
            paro_error::internal("grant dependency references unknown required properties")
        })?;
        if EnforcementPlanner::requires_memory_class(properties) {
            return Ok(GrantSensitivitySummary::RequiredEnforcement {
                group: self.memo.canonical_group(group),
                required,
            });
        }
        self.grant_sensitivity(group)
    }

    fn explore_transformations(&mut self) -> Result<()> {
        self.explore_transformations_with_interleave(None)
    }

    fn explore_transformations_with_interleave(
        &mut self,
        interleave: Option<(GroupId, OptimizationGoal)>,
    ) -> Result<()> {
        self.memo.control().begin_optional();
        self.memo.seal_optional_group_budget();
        let mut agenda = StableAgenda::default();
        let mut effective_insertions_since_recost = 0usize;
        const INTERLEAVE_BATCH: usize = 8;
        for group_index in 0..self.memo.group_count() {
            let group = GroupId::new(group_index);
            if self.memo.canonical_group(group) == group {
                self.schedule_transformations(group, &mut agenda)?;
            }
        }
        'tasks: while let Some(task) = agenda.pop() {
            if !self.memo.control().checkpoint()? {
                break;
            }
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
            self.note_rule_phase(rule, RuleWorkPhase::Discovered);
            if self.collect_rule_work_profile {
                let profile = self.rule_work_profile.entry(rule).or_default();
                profile.discovered = profile.discovered.saturating_add(1);
            }
            let budget_class = {
                let rule_impl = self
                    .registry
                    .transformation(rule)
                    .ok_or_else(|| paro_error::internal("rule disappeared from registry"))?;
                self.memo.logical_expr(expression).ok_or_else(|| {
                    paro_error::internal("rule task references unknown expression")
                })?;
                rule_impl.budget_class()
            };
            // A zero fire budget cannot admit any transformation. Avoid
            // constructing dependency closures for work the caller has
            // explicitly disabled. Likewise, the legacy region-work budget
            // cannot admit a non-leaf expression when it is zero.
            let budget = self.memo.budget();
            let fire_dimension = budget_class.fire_dimension();
            let work_dimension = budget_class.work_dimension();
            let output_dimension = budget_class.output_dimension();
            let expression_has_children = self
                .memo
                .logical_expr(expression)
                .is_some_and(|expression| !expression.key.children.is_empty());
            if budget.optional_limit(fire_dimension) == Some(0)
                || (budget.optional_limit(work_dimension) == Some(0) && expression_has_children)
            {
                let dimension = if budget.optional_limit(fire_dimension) == Some(0) {
                    fire_dimension
                } else {
                    work_dimension
                };
                let mut witness = StableFingerprintBuilder::default();
                witness.write_bytes(b"paro.unexamined-pattern.v1");
                witness.write_u64(expression.index() as u64);
                witness.write_u64(rule.0 as u64);
                self.memo
                    .group_ledger_mut(group)
                    .ok_or_else(|| paro_error::internal("pattern owner disappeared"))?
                    .record_budget_limited(dimension, witness.finish());
                continue;
            }
            // A task produced by this same rule inherits the exact read cursor
            // of the binding which produced it. Avoid rebuilding its pattern
            // closure until one of those reads advances. This is an
            // incremental-work cursor, not a provenance match guard: any
            // relevant child/fact revision invalidates it and makes the new
            // expression eligible for ordinary matching.
            if self.transformation_observation_is_current(task_id)? {
                continue;
            }
            let binding_started = Instant::now();
            let binding_allocated = paro_common::allocator::thread_allocated_bytes();
            let mut binding_set = {
                let rule_impl = self
                    .registry
                    .transformation(rule)
                    .ok_or_else(|| paro_error::internal("rule disappeared from registry"))?;
                let context = RuleContext {
                    memo: &self.memo,
                    group,
                };
                rule_impl.bindings(expression, &context)?
            };
            *self.rule_elapsed.entry(rule).or_default() += binding_started.elapsed();
            let allocated = paro_common::allocator::allocated_bytes_since(binding_allocated);
            let accumulated = self.rule_allocated_bytes.entry(rule).or_default();
            *accumulated = accumulated.saturating_add(allocated);
            if !self.memo.control().checkpoint()? {
                break;
            }
            if let Some(previous) = self.transformation_fact_observations.get(&task_id) {
                let mut reads = binding_set.reads.into_vec();
                for read in previous {
                    reads.push(if read.logical_frontier_revision.is_some() {
                        PatternRead::from_group(&self.memo, read.group)?
                    } else {
                        PatternRead::facts_from_group(&self.memo, read.group)?
                    });
                }
                binding_set.work_units = binding_set.work_units.saturating_add(previous.len());
                binding_set.reads = reads.into_boxed_slice();
            }
            let Some(read_version) =
                self.observe_transformation_inputs(task_id, &binding_set.reads)?
            else {
                continue;
            };
            if binding_set.work_dimension != work_dimension {
                return Err(paro_error::internal(
                    "transformation binding work dimension disagrees with rule contract",
                ));
            }
            if let PatternEnumerationCompletion::BudgetLimited {
                enumerated_bindings,
                omitted_at_least,
            } = binding_set.completion
            {
                *self.rule_budget_exhaustions.entry(rule).or_default() += 1;
                let mut witness = StableFingerprintBuilder::default();
                witness.write_bytes(b"paro.pattern-enumeration-limited.v1");
                witness.write_u64(group.0 as u64);
                witness.write_u64(expression.0 as u64);
                witness.write_u64(rule.0 as u64);
                witness.write_fingerprint(read_version);
                witness.write_u64(enumerated_bindings as u64);
                witness.write_u64(omitted_at_least as u64);
                self.memo
                    .group_ledger_mut(group)
                    .ok_or_else(|| paro_error::internal("pattern owner group disappeared"))?
                    .record_budget_limited(binding_set.work_dimension, witness.finish());
            }
            // Enumeration work belongs to the observed pattern frontier, not
            // to every binding produced from it. Charging the whole read and
            // construction cost once per binding would make the same search
            // exponentially more expensive as its bounded output frontier
            // grows, and a completed no-match would incorrectly be free.
            if !admit_transformation_work(
                &mut self.memo,
                group,
                expression,
                rule,
                read_version,
                binding_set.work_units,
                binding_set.work_dimension,
            )? {
                *self.rule_budget_exhaustions.entry(rule).or_default() += 1;
                continue;
            }
            if binding_set.bindings.is_empty() {
                continue;
            }
            if self.collect_rule_work_profile {
                self.note_rule_phase(rule, RuleWorkPhase::Matched);
                let profile = self.rule_work_profile.entry(rule).or_default();
                profile.matched = profile
                    .matched
                    .saturating_add(binding_set.bindings.len() as u64);
            }
            self.transformation_bindings = self
                .transformation_bindings
                .saturating_add(binding_set.bindings.len() as u64);
            for binding in binding_set.bindings.iter() {
                if !self.memo.control().checkpoint()? {
                    break 'tasks;
                }
                let application_key = (task_id, binding.fingerprint);
                let previous_application = self
                    .transformation_applications
                    .get(&application_key)
                    .and_then(|applications| {
                        applications
                            .iter()
                            .find(|application| application.binding == *binding)
                    })
                    .cloned();
                let previous_is_current = if let Some(previous) = &previous_application {
                    let mut current = true;
                    for read in &previous.reads {
                        if !read.is_current(&self.memo)? {
                            current = false;
                            break;
                        }
                    }
                    current
                } else {
                    false
                };
                if previous_is_current {
                    continue;
                }
                // A revision is only a wake-up cursor. If the resolved facts
                // retain the same canonical value, advance the cursor and keep
                // the previous result instead of executing the rule again.
                if let Some(previous_value) = previous_application
                    .as_ref()
                    .and_then(|previous| previous.fact_value)
                {
                    let mut validation = TransformContext::new(&mut self.memo, group);
                    let current_value = self
                        .registry
                        .transformation(rule)
                        .ok_or_else(|| paro_error::internal("transformation disappeared"))?
                        .binding_fact_value(binding, &mut validation)?;
                    let fact_reads = validation.take_fact_reads();
                    drop(validation);
                    if current_value == Some(previous_value) {
                        self.fact_value_revalidation_hits =
                            self.fact_value_revalidation_hits.saturating_add(1);
                        let mut reads = self
                            .registry
                            .transformation(rule)
                            .ok_or_else(|| paro_error::internal("transformation disappeared"))?
                            .binding_reads(
                                binding,
                                &binding_set.reads,
                                &RuleContext {
                                    memo: &self.memo,
                                    group,
                                },
                            )?
                            .into_vec();
                        reads.extend(fact_reads.iter().copied());
                        let reads = reads.into_boxed_slice();
                        Self::merge_transformation_fact_reads(
                            &self.memo,
                            &mut self.transformation_fact_observations,
                            task_id,
                            &fact_reads,
                        )?;
                        self.seed_transformation_observation(task_id, &reads)?;
                        if let Some(application) = self
                            .transformation_applications
                            .get_mut(&application_key)
                            .and_then(|applications| {
                                applications
                                    .iter_mut()
                                    .find(|application| application.binding == *binding)
                            })
                        {
                            application.reads = reads;
                        }
                        continue;
                    }
                    self.fact_value_revalidation_misses =
                        self.fact_value_revalidation_misses.saturating_add(1);
                }
                // Keep one current observation per exact binding, including
                // hash-collision peers. Obsolete fact versions are not search
                // candidates and must not accumulate on repeated wake-ups.
                if let Some(applications) =
                    self.transformation_applications.get_mut(&application_key)
                {
                    applications.retain(|application| application.binding != *binding);
                }
                let mut application_reads = self
                    .registry
                    .transformation(rule)
                    .ok_or_else(|| paro_error::internal("transformation disappeared"))?
                    .binding_reads(
                        binding,
                        &binding_set.reads,
                        &RuleContext {
                            memo: &self.memo,
                            group,
                        },
                    )?
                    .into_vec();
                let transformation_task = match self.task_registry.request_current(
                    TaskIntent::Transform {
                        expression,
                        rule,
                        binding: binding.clone(),
                    },
                    // Discovery and application have different scopes. The
                    // discovery task retains the frontier observation that
                    // produced this exact binding; the application task
                    // records only the facts it actually consumes. A child
                    // group may publish a new peer expression after the
                    // binding was discovered without invalidating the
                    // already-exact application.
                    ReadSet::new(application_reads.iter().copied()),
                    &self.memo,
                )? {
                    TaskRequest::Leader(task) => {
                        self.task_registry.start(task)?;
                        task
                    }
                    // The current engine is single-worker. An exact in-flight
                    // binding must not be evaluated twice; a future worker
                    // consumes the same registry wakeup instead.
                    TaskRequest::Subscriber { .. } | TaskRequest::Reused { .. } => continue,
                };
                let dependency_version =
                    transformation_binding_fingerprint(read_version, binding.fingerprint);
                // `applied_rules` remains an audit of whether this rule has ever
                // reached apply for the expression. Incremental idempotence is
                // governed by the dependency-version observation above.
                self.memo.mark_rule_applied(expression, rule)?;
                let event = transformation_event(group, expression, rule, dependency_version);
                let admitted = self
                    .memo
                    .group_ledger_mut(group)
                    .ok_or_else(|| paro_error::internal("rule task references unknown group"))?
                    .admit_optional(fire_dimension, event);
                if admitted == BudgetDecision::Exhausted {
                    *self.rule_budget_exhaustions.entry(rule).or_default() += 1;
                    self.complete_transformation_task(transformation_task)?;
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
                    rule_impl.output_bound(binding, &context)
                };
                let mut output_events = Vec::with_capacity(output_bound);
                let mut output_budget_limited = false;
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
                        .group_ledger_mut(group)
                        .ok_or_else(|| paro_error::internal("rule task references unknown group"))?
                        .admit_optional(output_dimension, event);
                    if admitted == BudgetDecision::Exhausted {
                        output_budget_limited = true;
                        break;
                    }
                    output_events.push(event);
                }
                if output_budget_limited {
                    *self.rule_budget_exhaustions.entry(rule).or_default() += 1;
                }
                if output_events.is_empty() {
                    self.complete_transformation_task(transformation_task)?;
                    continue;
                }
                *self.rule_attempts.entry(rule).or_default() += 1;
                // The context owns the complete attempt. Its Memo snapshot is
                // lazy, and rule-specific side state enlists in the same rollback
                // domain before its first write.
                let mut context = TransformContext::new(&mut self.memo, group);
                let apply_started = Instant::now();
                let apply_allocated = paro_common::allocator::thread_allocated_bytes();
                let outputs_result = {
                    let rule_impl = self
                        .registry
                        .transformation(rule)
                        .ok_or_else(|| paro_error::internal("rule disappeared from registry"))?;
                    rule_impl.apply_binding(binding, &mut context)
                };
                *self.rule_elapsed.entry(rule).or_default() += apply_started.elapsed();
                let allocated = paro_common::allocator::allocated_bytes_since(apply_allocated);
                let accumulated = self.rule_allocated_bytes.entry(rule).or_default();
                *accumulated = accumulated.saturating_add(allocated);
                if let Err(error) = &outputs_result {
                    if error.is_query_canceled() {
                        let error = error.clone();
                        context.rollback()?;
                        release_transformation_output_reservations(
                            &mut self.memo,
                            group,
                            &output_events,
                            output_dimension,
                        )?;
                        self.fail_transformation_task(transformation_task, error.to_string())?;
                        return Err(error);
                    }
                }
                match context.memo().control().checkpoint() {
                    Ok(true) => {}
                    stopped => {
                        context.rollback()?;
                        release_transformation_output_reservations(
                            &mut self.memo,
                            group,
                            &output_events,
                            output_dimension,
                        )?;
                        self.suspend_transformation_task(
                            transformation_task,
                            StopReason::Deadline,
                        )?;
                        stopped?;
                        break 'tasks;
                    }
                }
                let fact_value = context.fact_value_fingerprint();
                let fact_reads = context.take_fact_reads();
                application_reads.extend(fact_reads.iter().copied());
                let application_reads = application_reads.into_boxed_slice();
                Self::merge_transformation_fact_reads(
                    context.memo(),
                    &mut self.transformation_fact_observations,
                    task_id,
                    &fact_reads,
                )?;
                let outputs = match outputs_result {
                    Ok(outputs) => outputs,
                    Err(error) => {
                        if self.collect_rule_work_profile {
                            self.rule_work_profile.entry(rule).or_default().rejected = self
                                .rule_work_profile
                                .get(&rule)
                                .map_or(1, |profile| profile.rejected.saturating_add(1));
                        }
                        context.rollback()?;
                        self.seed_transformation_observation(task_id, &binding_set.reads)?;
                        release_transformation_output_reservations(
                            &mut self.memo,
                            group,
                            &output_events,
                            output_dimension,
                        )?;
                        tracing::debug!(
                            target: "paro::optimizer",
                            %error,
                            rule = rule.0,
                            group = group.index(),
                            "discarded failed optional transformation"
                        );
                        self.memo
                            .record_failed_rule(group, rule, event, error.to_string());
                        self.complete_transformation_task(transformation_task)?;
                        continue;
                    }
                };
                if outputs.is_empty() {
                    if self.collect_rule_work_profile {
                        self.rule_work_profile.entry(rule).or_default().rejected = self
                            .rule_work_profile
                            .get(&rule)
                            .map_or(1, |profile| profile.rejected.saturating_add(1));
                    }
                    context.rollback()?;
                    let mut observed = binding_set.reads.to_vec();
                    observed.extend(application_reads.iter().copied());
                    self.seed_transformation_observation(task_id, &observed)?;
                    self.transformation_applications
                        .entry(application_key)
                        .or_default()
                        .push(BindingApplication {
                            binding: binding.clone(),
                            reads: application_reads,
                            fact_value,
                        });
                    release_transformation_output_reservations(
                        &mut self.memo,
                        group,
                        &output_events,
                        output_dimension,
                    )?;
                    self.complete_transformation_task(transformation_task)?;
                    continue;
                }
                if outputs.len() > output_events.len() {
                    if self.collect_rule_work_profile {
                        self.rule_work_profile.entry(rule).or_default().rejected = self
                            .rule_work_profile
                            .get(&rule)
                            .map_or(1, |profile| profile.rejected.saturating_add(1));
                    }
                    context.rollback()?;
                    release_transformation_output_reservations(
                        &mut self.memo,
                        group,
                        &output_events,
                        output_dimension,
                    )?;
                    tracing::debug!(
                        target: "paro::optimizer",
                        rule = rule.0,
                        group = group.index(),
                        output_count = outputs.len(),
                        reserved_outputs = output_events.len(),
                        "discarded optional transformation whose frontier exceeded its declared bound"
                    );
                    if !output_budget_limited {
                        self.memo.record_failed_rule(
                            group,
                            rule,
                            event,
                            "rule exceeded its binding output contract",
                        );
                    }
                    self.complete_transformation_task(transformation_task)?;
                    continue;
                }
                if self.collect_rule_work_profile {
                    Self::note_rule_phase_at(
                        &mut self.rule_work_profile,
                        self.collect_rule_work_profile,
                        self.profile_started_at,
                        rule,
                        RuleWorkPhase::Applicable,
                    );
                    let profile = self.rule_work_profile.entry(rule).or_default();
                    profile.applicable = profile.applicable.saturating_add(1);
                    profile.constructed = profile.constructed.saturating_add(outputs.len() as u64);
                }
                let insertion = (|| -> Result<TransformationInsertion> {
                    let mut inserted_groups = BTreeSet::new();
                    let mut inserted_properties = Vec::new();
                    let mut inserted_expressions = Vec::new();
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
                        let duplicate = match output.operator_encoding.as_deref() {
                            Some(encoding) => context
                                .memo()
                                .logical_expr_for_structural_key(target, &output.key, encoding)
                                .is_some(),
                            None => context
                                .memo()
                                .logical_expr_for_key(target, &output.key)
                                .is_some(),
                        };
                        if duplicate {
                            continue;
                        }
                        let before = context
                            .memo()
                            .group(target)
                            .map(|group| group.logical_exprs().len())
                            .unwrap_or(0);
                        let inserted = if let Some(encoding) = output.operator_encoding {
                            context.memo_mut().insert_logical_with_operator_encoding(
                                target,
                                output.key,
                                output.payload,
                                output.proof,
                                encoding,
                            )?
                        } else {
                            context.memo_mut().insert_logical(
                                target,
                                output.key,
                                output.payload,
                                output.proof,
                            )?
                        };
                        let after = context
                            .memo()
                            .group(target)
                            .expect("target group was validated")
                            .logical_exprs()
                            .len();
                        if after > before {
                            inserted_groups.insert(target);
                            inserted_expressions.push((target, inserted));
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
                        if self.collect_rule_work_profile {
                            self.rule_work_profile.entry(rule).or_default().rejected = self
                                .rule_work_profile
                                .get(&rule)
                                .map_or(1, |profile| profile.rejected.saturating_add(1));
                        }
                        context.rollback()?;
                        release_transformation_output_reservations(
                            &mut self.memo,
                            group,
                            &output_events,
                            output_dimension,
                        )?;
                        tracing::debug!(
                            target: "paro::optimizer",
                            %error,
                            rule = rule.0,
                            group = group.index(),
                            "discarded invalid optional transformation output"
                        );
                        self.memo
                            .record_failed_rule(group, rule, event, error.to_string());
                        self.complete_transformation_task(transformation_task)?;
                        continue;
                    }
                };
                if inserted_groups.is_empty() {
                    // A duplicate root is not an effective transformation. Drop
                    // any staged child groups and planner payloads with it.
                    context.rollback()?;
                    release_transformation_output_reservations(
                        &mut self.memo,
                        group,
                        &output_events,
                        output_dimension,
                    )?;
                    if self.collect_rule_work_profile {
                        self.rule_work_profile.entry(rule).or_default().ineffective = self
                            .rule_work_profile
                            .get(&rule)
                            .map_or(1, |profile| profile.ineffective.saturating_add(1));
                    }
                    self.complete_transformation_task(transformation_task)?;
                } else {
                    let newly_inserted_expressions = inserted_expressions.clone();
                    let (appended_groups, locally_written_groups) = context.commit()?;
                    let changed_cte_readers = self.memo.take_changed_cte_readers();
                    self.note_logical_publication();
                    self.publish_transformation_task(
                        transformation_task,
                        locally_written_groups
                            .into_iter()
                            .chain(std::iter::once(group))
                            .chain(appended_groups.iter().copied())
                            .chain(changed_cte_readers.iter().copied()),
                    )?;
                    release_transformation_output_reservations(
                        &mut self.memo,
                        group,
                        &output_events[inserted_expressions.len().min(output_events.len())..],
                        output_dimension,
                    )?;
                    for (target, properties, cardinality) in inserted_properties {
                        let group = self.memo.group_mut(target).ok_or_else(|| {
                            paro_error::internal("committed transformation lost its target group")
                        })?;
                        group
                            .logical_properties
                            .merge_equivalent_facts(&properties)?;
                        group.cardinality =
                            std::mem::take(&mut group.cardinality).canonical_with(cardinality);
                    }
                    *self.effective_rule_insertions.entry(rule).or_default() +=
                        u64::try_from(inserted_expressions.len()).unwrap_or(u64::MAX);
                    effective_insertions_since_recost = effective_insertions_since_recost
                        .saturating_add(inserted_expressions.len());
                    if self.collect_rule_work_profile {
                        self.note_rule_phase(rule, RuleWorkPhase::Published);
                        self.rule_work_profile.entry(rule).or_default().published = self
                            .rule_work_profile
                            .get(&rule)
                            .map_or(inserted_expressions.len() as u64, |profile| {
                                profile
                                    .published
                                    .saturating_add(inserted_expressions.len() as u64)
                            });
                    }
                    let saturates_binding = self
                        .registry
                        .transformation(rule)
                        .ok_or_else(|| paro_error::internal("transformation disappeared"))?
                        .output_saturates_observed_binding();
                    if saturates_binding {
                        let mut inherited_reads = binding_set.reads.to_vec();
                        inherited_reads.extend(application_reads.iter().copied());
                        inherited_reads.extend(
                            appended_groups
                                .iter()
                                .copied()
                                .map(|group| PatternRead::from_group(&self.memo, group))
                                .collect::<Result<Vec<_>>>()?,
                        );
                        for (owner, inserted) in inserted_expressions.iter().copied() {
                            self.seed_transformation_observation(
                                TransformationTaskId {
                                    group: owner,
                                    expression: inserted,
                                    rule,
                                },
                                &inherited_reads,
                            )?;
                        }
                    }
                    inserted_groups.extend(appended_groups.iter().copied());
                    inserted_groups.extend(changed_cte_readers.iter().copied());
                    for (owner, inserted) in newly_inserted_expressions {
                        self.schedule_transformation_expression(owner, inserted, &mut agenda)?;
                    }
                    for appended in appended_groups.iter().copied() {
                        self.schedule_transformations(appended, &mut agenda)?;
                    }
                }
                let mut observed = binding_set.reads.to_vec();
                observed.extend(application_reads.iter().copied());
                self.seed_transformation_observation(task_id, &observed)?;
                self.transformation_applications
                    .entry(application_key)
                    .or_default()
                    .push(BindingApplication {
                        binding: binding.clone(),
                        reads: application_reads,
                        fact_value,
                    });
                for target in inserted_groups {
                    self.schedule_transformation_dependents(target, &mut agenda)?;
                }
            }
            if effective_insertions_since_recost >= INTERLEAVE_BATCH {
                if let Some((root, goal)) = interleave {
                    // Interleaving only the mandatory physical baseline keeps
                    // optional search budget accounting equivalent to the
                    // legacy final costing pass.  It still makes newly
                    // published logical alternatives executable and lets
                    // later rules observe their physical readiness.
                    let result = {
                        self.mandatory_only = true;
                        let result = self.optimize_group(root, goal);
                        self.mandatory_only = false;
                        result
                    };
                    result?;
                }
                effective_insertions_since_recost = 0;
            }
        }
        Ok(())
    }

    fn complete_transformation_task(&mut self, task: TaskId) -> Result<()> {
        let reads = self
            .task_registry
            .task_read_set(task)
            .ok_or_else(|| paro_error::internal("transformation task lost its read set"))?;
        self.task_registry
            .complete_current(task, &self.memo, TaskOutcome::NoChange { reads })?;
        Ok(())
    }

    fn publish_transformation_task(
        &mut self,
        task: TaskId,
        locally_written_groups: impl IntoIterator<Item = GroupId>,
    ) -> Result<()> {
        let cursor = self.task_registry.advance_cursor(
            task,
            Cursor {
                position: 1,
                complete: true,
            },
        )?;
        self.task_registry.publish_current_after_local_mutation(
            task,
            &self.memo,
            locally_written_groups,
            std::iter::empty(),
            TaskOutcome::Progress { cursor },
        )?;
        Ok(())
    }

    fn suspend_transformation_task(&mut self, task: TaskId, reason: StopReason) -> Result<()> {
        let cursor = self
            .task_registry
            .task(task)
            .map(|record| record.cursor)
            .ok_or_else(|| paro_error::internal("transformation task lost its cursor"))?;
        self.task_registry
            .suspend(task, TaskOutcome::Suspended { cursor, reason })?;
        Ok(())
    }

    fn fail_transformation_task(&mut self, task: TaskId, detail: impl Into<String>) -> Result<()> {
        self.task_registry.fail(task, detail)?;
        Ok(())
    }

    pub fn effective_rule_insertions(&self) -> &BTreeMap<RuleId, u64> {
        &self.effective_rule_insertions
    }

    pub fn rule_attempts(&self) -> &BTreeMap<RuleId, u64> {
        &self.rule_attempts
    }

    pub fn rule_elapsed(&self) -> &BTreeMap<RuleId, Duration> {
        &self.rule_elapsed
    }

    pub fn rule_allocated_bytes(&self) -> &BTreeMap<RuleId, u64> {
        &self.rule_allocated_bytes
    }

    pub fn rule_budget_exhaustions(&self) -> &BTreeMap<RuleId, u64> {
        &self.rule_budget_exhaustions
    }

    pub fn rule_work_profile(&self) -> &BTreeMap<RuleId, RuleWorkProfile> {
        &self.rule_work_profile
    }

    pub fn search_milestones(&self) -> &SearchMilestones {
        &self.search_milestones
    }

    pub fn search_work_counters(&self) -> BTreeMap<&'static str, u64> {
        let task_profile = self.task_registry.profile();
        BTreeMap::from([
            ("winner_proposal_count", self.memo.winner_proposal_count()),
            ("published_winner_count", self.memo.published_winner_count()),
            ("transformation_binding_count", self.transformation_bindings),
            (
                "fact_value_revalidation_hit_count",
                self.fact_value_revalidation_hits,
            ),
            (
                "fact_value_revalidation_miss_count",
                self.fact_value_revalidation_misses,
            ),
            (
                "physical_subproblem_request_count",
                self.physical_subproblem_requests,
            ),
            (
                "physical_subproblem_reuse_count",
                self.physical_subproblem_reuses,
            ),
            (
                "physical_subproblem_evaluation_count",
                self.physical_subproblem_evaluations,
            ),
            (
                "physical_implementation_request_count",
                self.physical_implementation_requests,
            ),
            (
                "optimization_context_count",
                self.memo.optimization_context_count() as u64,
            ),
            ("task_registry_request_count", task_profile.requests),
            (
                "task_registry_unique_intent_count",
                task_profile.unique_intents,
            ),
            (
                "task_registry_unique_evaluation_count",
                task_profile.unique_evaluations,
            ),
            (
                "task_registry_unique_subproblem_count",
                task_profile.unique_subproblems,
            ),
            ("task_registry_reuse_count", task_profile.reused_evaluations),
            (
                "task_registry_reopened_evaluation_count",
                task_profile.reopened_evaluations,
            ),
            (
                "task_registry_single_flight_subscription_count",
                task_profile.single_flight_subscriptions,
            ),
            ("task_registry_invalidation_count", task_profile.invalidated),
            ("task_registry_awaiting_count", task_profile.awaiting),
            (
                "child_combination_event_count",
                self.child_combination_events.len() as u64,
            ),
            (
                "governor_milestone",
                match self.governor.milestone() {
                    PlanMilestone::None => 0,
                    PlanMilestone::PSafe => 1,
                    PlanMilestone::PReady => 2,
                },
            ),
            (
                "governor_search_complete",
                u64::from(self.governor.is_search_complete()),
            ),
            (
                "governor_calibration_unavailable",
                u64::from(self.governor.last_calibration_status().is_some()),
            ),
        ])
    }

    fn schedule_transformations(&self, group: GroupId, agenda: &mut StableAgenda) -> Result<()> {
        let group_ref = self
            .memo
            .group(group)
            .ok_or_else(|| paro_error::internal("cannot schedule an unknown Memo group"))?;
        for &expression in group_ref.logical_exprs() {
            self.schedule_transformation_expression(group, expression, agenda)?;
        }
        Ok(())
    }

    fn schedule_transformation_expression(
        &self,
        group: GroupId,
        expression: LogicalExprId,
        agenda: &mut StableAgenda,
    ) -> Result<()> {
        let expression_ref = self
            .memo
            .logical_expr(expression)
            .ok_or_else(|| paro_error::internal("cannot schedule an unknown logical expression"))?;
        let context = RuleContext {
            memo: &self.memo,
            group,
        };
        for rule in self.registry.transformations() {
            if !self.memo.budget().transformation_enabled(rule.id())
                || !rule.matches_root(expression_ref)
            {
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
            .into_iter()
            .flat_map(|subscribers| subscribers.iter().copied());
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

    /// Publish precisely the frontier revisions read by the matcher. Reads
    /// from a completed no-match are retained, so a newly inserted alternative
    /// wakes the parent without subscribing to unrelated descendants.
    fn observe_transformation_inputs(
        &mut self,
        task: TransformationTaskId,
        reads: &[PatternRead],
    ) -> Result<Option<Fingerprint>> {
        let mut dependencies = reads.to_vec();
        dependencies.sort_unstable();
        dependencies.dedup();
        if self
            .transformation_observations
            .get(&task)
            .is_some_and(|observed| observed.as_ref() == dependencies.as_slice())
        {
            return Ok(None);
        }

        let version = transformation_dependency_fingerprint(&dependencies);
        self.seed_transformation_observation(task, &dependencies)?;
        Ok(Some(version))
    }

    fn transformation_observation_is_current(&self, task: TransformationTaskId) -> Result<bool> {
        let Some(observed) = self.transformation_observations.get(&task) else {
            return Ok(false);
        };
        for read in observed {
            if !read.is_current(&self.memo)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn seed_transformation_observation(
        &mut self,
        task: TransformationTaskId,
        reads: &[PatternRead],
    ) -> Result<()> {
        let mut dependencies = reads.to_vec();
        // Discovery is shared by all bindings of a task. Application-only
        // evidence remains subscribed even when a later binding reads a
        // disjoint subset, declines, or rolls back an optional rewrite.
        if let Some(actual) = self.transformation_fact_observations.get(&task) {
            dependencies.extend(actual.iter().copied());
        }
        dependencies.sort_unstable();
        dependencies.dedup();
        let previous = self
            .transformation_observations
            .get(&task)
            .map_or(&[][..], AsRef::as_ref);
        // A revision change updates the observation, not the subscription.
        // Only added/removed group memberships mutate the reverse index.
        // Shared discovery/application reads can contain several cursors for
        // one group; the subscription is still a single membership.
        visit_read_group_delta(previous, &dependencies, |group, subscribe| {
            if subscribe {
                self.transformation_subscribers
                    .entry(group)
                    .or_default()
                    .insert(task);
            } else if let Some(subscribers) = self.transformation_subscribers.get_mut(&group) {
                subscribers.remove(&task);
            }
        });
        self.transformation_observations
            .insert(task, dependencies.into_boxed_slice());
        Ok(())
    }

    fn merge_transformation_fact_reads(
        memo: &Memo,
        observations: &mut BTreeMap<TransformationTaskId, Vec<PatternRead>>,
        task: TransformationTaskId,
        fact_reads: &[PatternRead],
    ) -> Result<()> {
        if fact_reads.is_empty() {
            return Ok(());
        }
        let reads = observations.entry(task).or_default();
        for read in fact_reads.iter().copied() {
            // A facts-only access must not downgrade an earlier frontier
            // access made by another binding of this task.
            match reads.binary_search_by_key(&read.group, |previous| previous.group) {
                Ok(index) => {
                    reads[index] = if reads[index].logical_frontier_revision.is_some() {
                        PatternRead::from_group(memo, read.group)?
                    } else {
                        read
                    };
                }
                Err(index) => reads.insert(index, read),
            }
        }
        Ok(())
    }

    fn enumerate_implementations(&mut self, group: GroupId, goal: OptimizationGoal) -> Result<()> {
        let group = self.memo.canonical_group(group);
        self.physical_implementation_requests =
            self.physical_implementation_requests.saturating_add(1);
        let mut agenda = StableAgenda::default();
        let logical_exprs = self
            .memo
            .group(group)
            .ok_or_else(|| paro_error::internal("unknown group during implementation"))?
            .logical_exprs()
            .to_vec();
        for expression in logical_exprs {
            let expression_ref = self.memo.logical_expr(expression).ok_or_else(|| {
                paro_error::internal("unknown logical expression during implementation")
            })?;
            for implementation in self.registry.implementations() {
                let context = ImplementationContext {
                    memo: &self.memo,
                    group,
                };
                if !implementation.matches(expression_ref, goal, &context) {
                    continue;
                }
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
            if !self.memo.control().checkpoint()? {
                break;
            }
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
                self.memo.logical_expr(expression).ok_or_else(|| {
                    paro_error::internal("implementation task references unknown expression")
                })?;
                let context = ImplementationContext {
                    memo: &self.memo,
                    group,
                };
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
                if self.mandatory_only && !candidate.mandatory {
                    continue;
                }
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
        let inherited_sources = self
            .memo
            .optimization_context(goal.context)
            .ok_or_else(|| paro_error::internal("physical goal has no source-demand context"))?
            .filterable_sources()
            .clone();
        for (ordinal, (child, child_goal)) in candidate.child_goals.iter_mut().enumerate() {
            child_goal.grant =
                self.normalized_child_grant(*child, child_goal.required, goal.grant)?;
            let mut sources = inherited_sources.clone();
            if let Some((filtered_child, filters)) = candidate.cost_composition.sideways_filter() {
                if ordinal == filtered_child {
                    sources.extend(filters.iter().map(|filter| filter.source));
                }
            }
            child_goal.context = self
                .memo
                .intern_source_demand_context(child_goal.context, sources)?;
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
                    self.memo
                        .group_ledger_mut(group)
                        .ok_or_else(|| {
                            paro_error::internal("unknown group during region admission")
                        })?
                        .record_budget_limited(
                            BudgetDimension::CompositeRegionCandidate,
                            candidate.stable_event(goal),
                        );
                    return Ok(());
                }
            }
            let decision = self
                .memo
                .group_ledger_mut(group)
                .ok_or_else(|| paro_error::internal("unknown group during physical admission"))?
                .admit_optional(
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
        // Do not publish a region fingerprint or allocate a persistent cost
        // recipe until the candidate has crossed the Memo publication
        // boundary.  The previous order left region admission state behind
        // when the physical/group budget rejected the candidate, and built a
        // throwaway `CostRecipe` for duplicate physical keys.
        if let std::collections::btree_map::Entry::Vacant(entry) = self.recipes.entry(recipe_key) {
            if let Some(region) = &candidate.region {
                self.region_candidates
                    .entry(region.region)
                    .or_default()
                    .insert(candidate.physical_fingerprint);
            }
            entry.insert(Arc::new(CostRecipe {
                child_goals: candidate.child_goals,
                local_cost: candidate.local_cost,
                source_filter_apply_cost: candidate.source_filter_apply_cost,
                task_supply: candidate.task_supply,
                cost_composition: candidate.cost_composition,
                spillable: candidate.spillable,
                enforcer_cost_input: candidate.enforcer_cost_input,
                physical_fingerprint: candidate.physical_fingerprint,
                region: candidate.region,
            }));
        }
        Ok(())
    }

    fn optimize_group(&mut self, group: GroupId, goal: OptimizationGoal) -> Result<()> {
        if !self.memo.control().checkpoint()? {
            return Ok(());
        }
        let group = self.memo.canonical_group(group);
        self.physical_subproblem_requests = self.physical_subproblem_requests.saturating_add(1);
        let read_set = ReadSet::single(PatternRead::from_group(&self.memo, group)?);
        let task = match self.task_registry.request_current(
            TaskIntent::Optimize { group, goal },
            read_set,
            &self.memo,
        )? {
            TaskRequest::Leader(task) => task,
            TaskRequest::Reused { .. } => {
                self.physical_subproblem_reuses = self.physical_subproblem_reuses.saturating_add(1);
                return Ok(());
            }
            TaskRequest::Subscriber { task, .. } => {
                return Err(paro_error::internal(format!(
                    "recursive optimization request is already in flight for task {task:?}"
                )))
            }
        };
        self.task_registry.start(task)?;
        let task_has_residual_work = self
            .task_registry
            .task(task)
            .and_then(|record| self.task_registry.cursor(record.cursor))
            .is_some_and(|cursor| !cursor.complete);
        if !task_has_residual_work
            && (self
                .memo
                .group(group)
                .and_then(|group| group.winner(goal))
                .is_some()
                || self.infeasible_goals.contains(&(group, goal)))
        {
            self.physical_subproblem_reuses = self.physical_subproblem_reuses.saturating_add(1);
            let cursor = self.task_registry.advance_cursor(
                task,
                Cursor {
                    position: self.memo.physical_expr_count() as u64,
                    complete: true,
                },
            )?;
            let outcome = if self
                .memo
                .group(group)
                .and_then(|group| group.winner(goal))
                .is_some()
            {
                TaskOutcome::Progress { cursor }
            } else {
                TaskOutcome::Infeasible
            };
            self.task_registry
                .complete_current(task, &self.memo, outcome)?;
            return Ok(());
        }
        self.physical_subproblem_evaluations =
            self.physical_subproblem_evaluations.saturating_add(1);
        if !self.active_goals.insert((group, goal)) {
            let _ = self.task_registry.invalidate(task);
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
        match result {
            Ok(()) => {
                // A physical task's cursor describes its own recipe and
                // child-frontier domain. Global logical budget/rule
                // obligations are reported by Memo, but must not make every
                // completed physical task resumable: doing so re-enumerates
                // all implementations whenever an unrelated logical rule
                // leaves an obligation behind.
                let complete = !self.memo.control().deadline_reached();
                let cursor = self.task_registry.advance_cursor(
                    task,
                    Cursor {
                        position: self.memo.physical_expr_count() as u64,
                        complete,
                    },
                )?;
                let outcome = if self
                    .memo
                    .group(group)
                    .and_then(|group| group.winner(goal))
                    .is_some()
                {
                    TaskOutcome::Progress { cursor }
                } else {
                    TaskOutcome::Infeasible
                };
                self.task_registry.publish_current_after_local_mutation(
                    task,
                    &self.memo,
                    [group],
                    std::iter::empty(),
                    outcome,
                )?;
                Ok(())
            }
            Err(error) => {
                let _ = self.task_registry.invalidate(task);
                Err(error)
            }
        }
    }

    fn intern_child_combination_event(
        &mut self,
        physical: PhysicalExprId,
        goal: OptimizationGoal,
        recipe: Fingerprint,
        children: &[ChildWinnerRef],
    ) -> Result<Fingerprint> {
        let identity = ChildCombinationIdentity {
            physical,
            goal,
            recipe,
            children: children
                .iter()
                .map(|child| child.candidate)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        };
        if let Some(event) = self.child_combination_events.get(&identity).copied() {
            return Ok(event);
        }
        let event_id = self.next_child_combination_event;
        self.next_child_combination_event = event_id
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("child-combination event identity exhausted"))?;
        // The ChildFrontierCombination dimension is owned by this query's
        // engine, so a monotone interned handle is sufficient and avoids a
        // cryptographic digest for every repeated combination admission.
        let event = Fingerprint((1_u128 << 127) | event_id);
        self.child_combination_events.insert(identity, event);
        Ok(event)
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
                    .map(|((physical, _, fingerprint), recipe)| {
                        (*physical, *fingerprint, Arc::clone(recipe))
                    }),
            );
        }
        // Reuse the query-local frontier and cost scratch across recipes. The
        // vectors contain only immutable candidate handles; rebuilding their
        // backing allocations for every physical recipe made the same parent
        // pay an avoidable allocation cost before any candidate was compared.
        let mut child_frontiers = Vec::<Vec<ChildWinnerRef>>::new();
        let mut child_selections = Vec::<ChildWinnerRef>::new();
        let mut child_costs = Vec::<SearchCost>::new();
        let mut child_fingerprints = Vec::<Fingerprint>::new();
        for (physical, _recipe_fingerprint, recipe) in recipes {
            if !self.memo.control().checkpoint()? {
                break;
            }
            child_frontiers.resize_with(recipe.child_goals.len(), Vec::new);
            for frontier in child_frontiers.iter_mut() {
                frontier.clear();
            }
            let mut children_feasible = true;
            for ((child, child_goal), frontier_out) in recipe
                .child_goals
                .iter()
                .copied()
                .zip(child_frontiers.iter_mut())
            {
                self.optimize_group(child, child_goal)?;
                let Some(frontier) = self
                    .memo
                    .group(child)
                    .and_then(|group| group.winner_frontier(child_goal))
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
                frontier_out.reserve(frontier.candidates().len());
                frontier_out.extend(frontier.candidates().iter().map(|winner| ChildWinnerRef {
                    group: child,
                    goal: child_goal,
                    candidate: winner.candidate,
                }));
            }
            if !children_feasible {
                continue;
            }
            let admitted_combination_limit =
                (self.memo.budget().max_child_frontier_combinations_per_group as usize)
                    .saturating_sub(
                        self.memo
                            .group(group)
                            .ok_or_else(|| {
                                paro_error::internal("unknown group during frontier admission")
                            })?
                            .ledger
                            .consumed(BudgetDimension::ChildFrontierCombination),
                    )
                    .saturating_add(1);
            let mut combinations =
                child_winner_combinations(&child_frontiers, admitted_combination_limit);
            let (completion, first_omitted_ordinal, omitted_at_least) =
                match combinations.completion {
                    EnumerationCompletion::Complete => ("complete", None, 0),
                    EnumerationCompletion::BudgetLimited {
                        first_omitted_ordinal,
                        omitted_at_least,
                    } => (
                        "budget_limited",
                        Some(first_omitted_ordinal),
                        omitted_at_least,
                    ),
                };
            if let Some(first_omitted) = first_omitted_ordinal {
                let mut witness = StableFingerprintBuilder::default();
                witness.write_bytes(b"paro.child-product-omission.v1");
                witness.write_u64(physical.index() as u64);
                witness.write_fingerprint(recipe.physical_fingerprint);
                witness.write_u64(first_omitted as u64);
                witness.write_u64(omitted_at_least as u64);
                self.memo
                    .group_ledger_mut(group)
                    .ok_or_else(|| paro_error::internal("child-product owner disappeared"))?
                    .record_budget_limited(
                        BudgetDimension::ChildFrontierCombination,
                        witness.finish(),
                    );
            }
            tracing::debug!(
                target: "paro::optimizer",
                parent_group = group.index(),
                physical_expression = physical.index(),
                completion,
                first_omitted_ordinal,
                omitted_at_least,
                generated_combinations = combinations.combinations.len(),
                "enumerated bounded child frontier product"
            );
            let child_frontier_count = recipe.child_goals.len();
            child_selections.clear();
            child_selections.reserve(child_frontier_count);
            child_costs.clear();
            child_costs.reserve(child_frontier_count);
            child_fingerprints.clear();
            child_fingerprints.reserve(child_frontier_count);
            while let Some(ordinal) = combinations.combinations.next_into(&mut child_selections) {
                if !self.memo.control().checkpoint()? {
                    break;
                }
                if ordinal > 0 {
                    let event = self.intern_child_combination_event(
                        physical,
                        goal,
                        recipe.physical_fingerprint,
                        &child_selections,
                    )?;
                    if self
                        .memo
                        .group_ledger_mut(group)
                        .ok_or_else(|| paro_error::internal("child-combination owner disappeared"))?
                        .admit_optional(BudgetDimension::ChildFrontierCombination, event)
                        == BudgetDecision::Exhausted
                    {
                        continue;
                    }
                }
                child_costs.clear();
                child_fingerprints.clear();
                let (local_cost, source_work, mut cost) = {
                    let mut child_source_work_refs = Vec::with_capacity(child_frontier_count);
                    for child in &child_selections {
                        let winner = self.memo.resolve_child_winner(*child).ok_or_else(|| {
                            paro_error::internal("child product lost an immutable candidate")
                        })?;
                        child_costs.push(winner.cost);
                        child_source_work_refs.push(winner.source_work.as_ref());
                        child_fingerprints.push(winner.physical_fingerprint);
                    }
                    let Some(local_cost) = fit_local_retained_state_to_grant_ref(
                        recipe.local_cost,
                        &child_costs,
                        &recipe.cost_composition,
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
                    let local_without_source_filter = match recipe.source_filter_apply_cost {
                        Some(apply) => local_cost.replace_work(apply, SearchCost::ZERO)?,
                        None => local_cost,
                    };
                    let mut local_cost = resolve_task_supply(
                        local_without_source_filter,
                        &child_costs,
                        &recipe.task_supply,
                        self.memo.calibration(),
                    )?;
                    if let Some(apply) = recipe.source_filter_apply_cost {
                        local_cost = local_cost.replace_work(SearchCost::ZERO, apply)?;
                    }
                    let composed = compose_candidate_cost_with_sources_at_ref(
                        local_cost,
                        recipe.source_filter_apply_cost,
                        &child_costs,
                        &child_source_work_refs,
                        &recipe.cost_composition,
                        self.memo.calibration(),
                    )?;
                    let source_work = composed.source_work;
                    let Some(cost) = constrain_composed_cost_to_grant(
                        composed.cost,
                        recipe.enforcer_cost_input,
                    )?
                    else {
                        continue;
                    };
                    (local_cost, source_work, cost)
                };
                let physical_properties = self
                    .memo
                    .physical_expr(physical)
                    .ok_or_else(|| {
                        paro_error::internal("unknown physical expression during enforcement")
                    })?
                    .provided
                    .clone();
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
                let Some(enforcer_phase) = enforcer_cost(
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
                    enforcer_phase.compose_after(cost)?,
                    recipe.enforcer_cost_input,
                )?
                else {
                    continue;
                };
                cost = constrained_cost;
                let fingerprint = enforced_fingerprint(
                    recipe.physical_fingerprint,
                    &enforced.steps,
                    child_fingerprints.iter().copied(),
                );
                let summary = CandidateSummary {
                    expression: physical,
                    cost,
                    source_work: source_work.as_ref(),
                    physical_fingerprint: fingerprint,
                };
                match self.memo.candidate_preview(group, goal, summary)? {
                    CandidatePreview::Rejected => {
                        self.memo.record_rejected_winner_proposal(
                            group,
                            goal,
                            fingerprint,
                            false,
                        )?;
                        continue;
                    }
                    CandidatePreview::Truncated => {
                        self.memo.record_rejected_winner_proposal(
                            group,
                            goal,
                            fingerprint,
                            true,
                        )?;
                        continue;
                    }
                    CandidatePreview::Publish | CandidatePreview::MustMaterialize => {}
                }
                if tracing::enabled!(target: "paro::optimizer", tracing::Level::DEBUG)
                    && self
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
                                | EquivalenceProof::SpecializedEnumerator { rule, .. }
                                | EquivalenceProof::TransformationDescendant { rule } => {
                                    Some(rule.0)
                                }
                                EquivalenceProof::Initial
                                | EquivalenceProof::Normalization { .. } => None,
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
                        work_latency = cost.work_latency.expected,
                        critical_path = cost.critical_path.expected,
                        useful_parallel_tasks = cost.max_parallel_tasks,
                        "costed an equivalent physical candidate"
                    );
                }
                let joint_cost_proof =
                    build_joint_cost_proof(&self.memo, group, &recipe, local_cost)?;
                let winner = Winner {
                    candidate: super::ids::CandidateId::INVALID,
                    expression: physical,
                    children: child_selections.clone().into_boxed_slice(),
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
                };
                let selected_changed = self.memo.record_winner(group, goal, winner)?;
                self.note_physical_candidate(group, goal, selected_changed);
            }
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
    dimension: BudgetDimension,
) -> Result<()> {
    let ledger = memo
        .group_ledger_mut(group)
        .ok_or_else(|| paro_error::internal("rule task references unknown group"))?;
    for event in events {
        ledger.release_optional_reservation(dimension, *event);
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

#[cfg(test)]
fn fit_local_retained_state_to_grant(
    local_cost: SearchCost,
    child_costs: &[SearchCost],
    composition: CostComposition,
    spillable: bool,
    grant: EnforcerCostInput,
) -> Result<Option<SearchCost>> {
    fit_local_retained_state_to_grant_ref(local_cost, child_costs, &composition, spillable, grant)
}

fn fit_local_retained_state_to_grant_ref(
    mut local_cost: SearchCost,
    child_costs: &[SearchCost],
    composition: &CostComposition,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnumerationCompletion {
    Complete,
    BudgetLimited {
        first_omitted_ordinal: usize,
        omitted_at_least: usize,
    },
}

#[derive(Debug)]
struct ChildCombinationBatch<'a> {
    combinations: ChildWinnerCombinations<'a>,
    completion: EnumerationCompletion,
}

/// A lazy product of immutable candidate references, never copies of winner
/// trees/source-work histories. Storage is linear in the input frontier width
/// even if their Cartesian product overflows usize.
#[derive(Debug)]
struct ChildWinnerCombinations<'a> {
    frontiers: &'a [Vec<ChildWinnerRef>],
    next: usize,
    end: usize,
}

impl Iterator for ChildWinnerCombinations<'_> {
    type Item = Vec<ChildWinnerRef>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result = Vec::with_capacity(self.frontiers.len());
        self.next_into(&mut result)?;
        Some(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end - self.next;
        (remaining, Some(remaining))
    }
}

impl ChildWinnerCombinations<'_> {
    /// Fill a caller-owned selection buffer so repeated products do not
    /// allocate one Vec per candidate combination.
    fn next_into(&mut self, result: &mut Vec<ChildWinnerRef>) -> Option<usize> {
        if self.next == self.end {
            return None;
        }
        let mut ordinal = self.next;
        self.next += 1;
        result.clear();
        result.reserve(self.frontiers.len().saturating_sub(result.capacity()));
        for frontier in self.frontiers.iter().rev() {
            result.push(frontier[ordinal % frontier.len()]);
            ordinal /= frontier.len();
        }
        result.reverse();
        Some(self.next - 1)
    }
}

impl ExactSizeIterator for ChildWinnerCombinations<'_> {}

/// Admit only the remaining child-product credit plus a rejection witness.
fn child_winner_combinations(
    frontiers: &[Vec<ChildWinnerRef>],
    admitted_limit: usize,
) -> ChildCombinationBatch<'_> {
    let admitted_limit = admitted_limit.max(1);
    let witness_limit = admitted_limit.saturating_add(1);
    let total = frontiers.iter().fold(1_usize, |product, frontier| {
        product.saturating_mul(frontier.len())
    });
    ChildCombinationBatch {
        completion: if total <= admitted_limit {
            EnumerationCompletion::Complete
        } else {
            EnumerationCompletion::BudgetLimited {
                first_omitted_ordinal: admitted_limit,
                omitted_at_least: total.saturating_sub(admitted_limit),
            }
        },
        combinations: ChildWinnerCombinations {
            frontiers,
            next: 0,
            end: total.min(witness_limit),
        },
    }
}

fn resolve_task_supply(
    local_cost: SearchCost,
    child_costs: &[SearchCost],
    contract: &TaskSupplyContract,
    calibration: &MachineCalibrationBundle,
) -> Result<SearchCost> {
    let child_tasks = |index: u8| -> Result<u16> {
        child_costs
            .get(usize::from(index))
            .map(|cost| cost.output_pipeline_tasks)
            .ok_or_else(|| {
                paro_error::internal("task-supply contract references an absent child pipeline")
            })
    };
    match *contract {
        TaskSupplyContract::Serial => {
            calibration.rephase(local_cost, ParallelWorkProfile::Serial, 1, 1)
        }
        TaskSupplyContract::Source { tasks } => {
            calibration.rephase(local_cost, ParallelWorkProfile::Pipeline, tasks, tasks)
        }
        TaskSupplyContract::Streaming { input } => {
            let tasks = child_tasks(input)?;
            calibration.continue_pipeline(local_cost, tasks)
        }
        TaskSupplyContract::Breaker {
            input,
            output_tasks,
            profile,
        } => calibration.rephase(local_cost, profile, child_tasks(input)?, output_tasks),
        TaskSupplyContract::BuildProbe {
            build,
            probe,
            build_work_ppm,
        } => {
            if build_work_ppm > 1_000_000 {
                return Err(paro_error::internal(
                    "build/probe task-supply contract has an invalid work split",
                ));
            }
            let serial_work = local_cost.work_only();
            let build_work = serial_work.retain_work(build_work_ppm, build_work_ppm)?;
            let probe_work = serial_work.replace_work(build_work, SearchCost::ZERO)?;
            let build_tasks = child_tasks(build)?;
            let probe_tasks = child_tasks(probe)?;
            let build_cost = calibration.rephase(
                build_work,
                ParallelWorkProfile::Pipeline,
                build_tasks,
                build_tasks,
            )?;
            let probe_cost = calibration.rephase(
                probe_work,
                ParallelWorkProfile::Pipeline,
                probe_tasks,
                probe_tasks,
            )?;
            let phased_work = build_cost.sequential(probe_cost)?;
            let mut result = local_cost.replace_work(serial_work, phased_work)?;
            result.max_parallel_tasks = build_tasks.max(probe_tasks);
            result.output_pipeline_tasks = probe_tasks;
            result.validate()?;
            Ok(result)
        }
    }
}

fn serial_normalized_work(cost: SearchCost) -> SearchCost {
    let mut work = cost.work_only();
    work.critical_path = work.work_latency;
    work.max_parallel_tasks = 1;
    work.output_pipeline_tasks = 1;
    work
}

#[cfg(test)]
pub(crate) fn compose_candidate_cost_with_sources(
    local_cost: SearchCost,
    source_filter_apply_cost: Option<SearchCost>,
    child_costs: &[SearchCost],
    child_source_work: &[&[SourceWork]],
    composition: CostComposition,
) -> Result<ComposedCost> {
    compose_candidate_cost_with_sources_at(
        local_cost,
        source_filter_apply_cost,
        child_costs,
        child_source_work,
        composition,
        &MachineCalibrationBundle::default(),
    )
}

pub(crate) fn compose_candidate_cost_with_sources_at(
    local_cost: SearchCost,
    source_filter_apply_cost: Option<SearchCost>,
    child_costs: &[SearchCost],
    child_source_work: &[&[SourceWork]],
    composition: CostComposition,
    calibration: &MachineCalibrationBundle,
) -> Result<ComposedCost> {
    compose_candidate_cost_with_sources_at_ref(
        local_cost,
        source_filter_apply_cost,
        child_costs,
        child_source_work,
        &composition,
        calibration,
    )
}

pub(crate) fn compose_candidate_cost_with_sources_at_ref(
    local_cost: SearchCost,
    source_filter_apply_cost: Option<SearchCost>,
    child_costs: &[SearchCost],
    child_source_work: &[&[SourceWork]],
    composition: &CostComposition,
    calibration: &MachineCalibrationBundle,
) -> Result<ComposedCost> {
    if child_costs.len() != child_source_work.len() {
        return Err(paro_error::internal(
            "cost composition has no source-work evidence for one or more children",
        ));
    }
    let mut cost = local_cost;
    if matches!(composition, CostComposition::LocalOnly) {
        cost.validate()?;
        return Ok(ComposedCost {
            cost,
            source_work: Box::new([]),
        });
    }
    if let CostComposition::Source {
        source,
        source_rows,
    } = composition
    {
        if !child_costs.is_empty() {
            return Err(paro_error::internal(
                "a base source-work lane unexpectedly has child pipelines",
            ));
        }
        cost.validate()?;
        let serial_cost = serial_normalized_work(local_cost);
        return Ok(ComposedCost {
            cost,
            source_work: Box::new([SourceWorkData {
                source: *source,
                source_rows: *source_rows,
                base_cost: serial_cost,
                cost: serial_cost,
                retentions: Box::new([]),
                filters: Box::new([]),
                filter_apply_cost: SearchCost::ZERO,
                phased_cost: local_cost.work_only(),
                phase_tasks: local_cost.output_pipeline_tasks,
            }
            .into()]),
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
                    let total_rows = lanes
                        .iter()
                        .map(|lane| lane.source_rows)
                        .fold(0_u64, u64::saturating_add);
                    let matching_rows = lanes
                        .iter()
                        .filter(|lane| sources.iter().any(|source| source.source == lane.source))
                        .map(|lane| lane.source_rows)
                        .fold(0_u64, u64::saturating_add);
                    // Attribute the operator-local full-source term by the
                    // immutable row domain, not by `lane.cost` (which may
                    // already be reduced by a filter introduced by another
                    // join).  If lineage is incomplete, retain the
                    // unattributed fraction on the parent instead of turning
                    // a physical-source mismatch into a cost discount.
                    let matched_share = if total_rows == 0 {
                        1_000_000_u32
                    } else {
                        ((matching_rows as f64 / total_rows as f64 * 1_000_000.0).ceil() as u32)
                            .min(1_000_000)
                    };
                    let unmatched_share = 1_000_000_u32.saturating_sub(matched_share);
                    cost = cost.replace_work(
                        full_apply_cost,
                        full_apply_cost.retain_work(unmatched_share, unmatched_share)?,
                    )?;
                    // Predicate evaluation is one operator-local cost before it
                    // is attributed to source lanes. Allocate every ppm exactly
                    // once so splitting a UNION into more branches cannot create
                    // or discard work through independent rounding.
                    let mut apply_shares = Vec::with_capacity(matching_lanes);
                    let mut unallocated_ppm = matched_share;
                    for lane in lanes
                        .iter()
                        .filter(|lane| sources.iter().any(|source| source.source == lane.source))
                    {
                        let remaining_lanes = matching_lanes - apply_shares.len();
                        let share = if remaining_lanes == 1 {
                            unallocated_ppm
                        } else if matching_rows > 0 {
                            ((lane.source_rows as f64 / matching_rows as f64 * 1_000_000.0).floor()
                                as u32)
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
                            if lane
                                .retentions
                                .iter()
                                .any(|proof| proof.domain == source.domain)
                                && lane
                                    .filters
                                    .iter()
                                    .any(|filter| filter.evaluation == source.evaluation)
                            {
                                // The parent term was already attributed above.
                                // Re-publishing an existing proof/occurrence does
                                // not change the immutable source response.
                                continue;
                            }
                            let mut updated = lane.snapshot().clone();
                            let full_apply_cost = total_apply_cost.retain_work(share, share)?;
                            // Speculative filters retain the complete risk
                            // ceiling. Exact membership over a declared-unique
                            // probe carries a proof-backed smaller ceiling.
                            let mut retentions = std::mem::take(&mut updated.retentions).into_vec();
                            if !retentions
                                .iter()
                                .any(|retention| retention.domain == source.domain)
                            {
                                retentions.push(SourceRetentionProof {
                                    domain: source.domain,
                                    expected_retained_ppm: source.expected_retained_ppm,
                                    upper_retained_ppm: source.upper_retained_ppm,
                                });
                            }
                            retentions.sort_by_key(|retention| retention.domain);
                            let retained = retained_source_cost(lane.base_cost, &retentions)?;
                            updated.cost = retained;
                            updated.retentions = retentions.into_boxed_slice();
                            let mut filters = std::mem::take(&mut updated.filters).into_vec();
                            if !filters
                                .iter()
                                .any(|filter| filter.evaluation == source.evaluation)
                            {
                                filters.push(SourceFilterWork {
                                    domain: source.domain,
                                    evaluation: source.evaluation,
                                    evaluation_rows: lane.source_rows,
                                    expected_retained_ppm: source.expected_retained_ppm,
                                    upper_retained_ppm: source.upper_retained_ppm,
                                    full_apply_cost: full_apply_cost.work_only(),
                                });
                            }
                            filters.sort_by_key(|filter| filter.evaluation);
                            let new_apply_cost = ordered_source_filter_cost(&filters)?;
                            updated.filters = filters.into_boxed_slice();
                            updated.filter_apply_cost = new_apply_cost;
                            let serial_pipeline = retained.sequential(new_apply_cost)?;
                            let phased_pipeline = calibration.rephase(
                                serial_pipeline,
                                ParallelWorkProfile::Pipeline,
                                lane.phase_tasks,
                                lane.phase_tasks,
                            )?;
                            child = child.replace_work(lane.phased_cost, phased_pipeline)?;
                            updated.phased_cost = phased_pipeline;
                            *lane = updated.into();
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
        let overlapping_preferred = child_costs
            .iter()
            .enumerate()
            .filter(|(index, _)| overlapping_children & (1_u64 << index) != 0)
            .map(|(_, child)| child.preferred_memory_bytes())
            .max()
            .unwrap_or(0);
        // Price the actual overlap phase in absolute memory coordinates.
        // Adding its floor to the *global* sequential elastic delta let an
        // unrelated child's larger floor reduce this phase's preferred peak.
        // Only overlapping non-revocable floors add; elastic working sets
        // share the query pool. Both terms are monotone in floor/preferred.
        let retained_preferred = local_cost
            .preferred_memory_bytes()
            .saturating_add(overlapping_minimum)
            .max(
                local_cost
                    .minimum_memory_bytes
                    .saturating_add(overlapping_preferred),
            );
        let sequential_preferred = cost.preferred_memory_bytes();
        cost.minimum_memory_bytes = cost.minimum_memory_bytes.max(retained_minimum);
        cost.revocable_memory_target = sequential_preferred
            .max(retained_preferred)
            .max(cost.minimum_memory_bytes)
            .saturating_sub(cost.minimum_memory_bytes);
        // Revocable operator state is governed by one shared query pool.
        // Overlapping spillable working sets therefore compose by maximum;
        // only their non-revocable portions must be added.
        cost.peak_memory_upper = cost
            .peak_memory_upper
            .max(local_cost.peak_memory_upper)
            .max(overlapping_peak)
            .max(retained_non_revocable)
            .max(retained_minimum)
            .max(cost.preferred_memory_bytes());
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
    ordered.sort_by_key(|filter| {
        (
            filter.expected_retained_ppm,
            filter.domain,
            filter.evaluation,
        )
    });
    let mut expected_prefix = SCALE as u32;
    let mut upper_prefix = SCALE as u32;
    let mut applied_domains = BTreeSet::new();
    let mut cost = SearchCost::ZERO;
    for filter in ordered {
        cost = cost.sequential(
            filter
                .full_apply_cost
                .retain_work(expected_prefix.min(upper_prefix), upper_prefix)?,
        )?;
        if applied_domains.insert(filter.domain) {
            expected_prefix = multiply_ppm(expected_prefix, filter.expected_retained_ppm);
            upper_prefix = upper_prefix.min(filter.upper_retained_ppm);
        }
    }
    Ok(cost)
}

fn retained_source_cost(
    base_cost: SearchCost,
    retentions: &[SourceRetentionProof],
) -> Result<SearchCost> {
    const SCALE: u64 = 1_000_000;
    let expected = retentions.iter().fold(SCALE as u32, |prefix, proof| {
        ((u64::from(prefix) * u64::from(proof.expected_retained_ppm) + SCALE / 2) / SCALE) as u32
    });
    // Every upper bound is absolute in the immutable base-source domain. With
    // unknown correlation, intersection cardinality is bounded by the
    // smallest individual domain; multiplying those bounds would incorrectly
    // assume conditional independence. Repeated proof identities were removed
    // before this function is called, making the survivor contract idempotent.
    let upper = retentions
        .iter()
        .map(|proof| proof.upper_retained_ppm)
        .min()
        .unwrap_or(SCALE as u32);
    base_cost.retain_work(expected.min(upper), upper)
}

/// Set difference over sorted group cursors, without allocating temporary
/// group sets. The same group may have several distinct revision/facet reads.
fn visit_read_group_delta(
    previous: &[PatternRead],
    next: &[PatternRead],
    mut visit: impl FnMut(GroupId, bool),
) {
    debug_assert!(previous
        .windows(2)
        .all(|pair| pair[0].group <= pair[1].group));
    debug_assert!(next.windows(2).all(|pair| pair[0].group <= pair[1].group));
    let (mut left, mut right) = (0, 0);
    while left < previous.len() || right < next.len() {
        let before = previous.get(left).map(|read| read.group);
        let after = next.get(right).map(|read| read.group);
        let group = match (before, after) {
            (Some(a), Some(b)) => a.min(b),
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (None, None) => break,
        };
        if before != after {
            visit(group, after == Some(group));
        }
        while previous.get(left).is_some_and(|read| read.group == group) {
            left += 1;
        }
        while next.get(right).is_some_and(|read| read.group == group) {
            right += 1;
        }
    }
}

fn transformation_dependency_fingerprint(dependencies: &[PatternRead]) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_bytes(b"paro.transformation-dependencies.v2");
    builder.write_u64(dependencies.len() as u64);
    for read in dependencies {
        builder.write_u64(read.group.0 as u64);
        builder.write_u64(u64::from(read.logical_frontier_revision.is_some()));
        builder.write_u64(read.logical_frontier_revision.unwrap_or_default());
        builder.write_fingerprint(read.logical_fact_fingerprint);
        builder.write_fingerprint(read.statistics_snapshot_fingerprint);
    }
    builder.finish()
}

fn transformation_binding_fingerprint(
    read_version: Fingerprint,
    binding: Fingerprint,
) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_bytes(b"paro.transformation-binding.v1");
    builder.write_fingerprint(read_version);
    builder.write_fingerprint(binding);
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
    work_units: usize,
    work_dimension: BudgetDimension,
) -> Result<bool> {
    let units = u32::try_from(work_units).unwrap_or(u32::MAX);
    let mut event = StableFingerprintBuilder::default();
    event.write_bytes(b"paro.rule-work-batch.v1");
    event.write_u64(target.0 as u64);
    event.write_u64(source.0 as u64);
    event.write_u64(rule.0 as u64);
    event.write_fingerprint(dependency_version);
    Ok(memo
        .group_ledger_mut(target)
        .ok_or_else(|| paro_error::internal("rule work target group disappeared"))?
        .admit_optional_units(work_dimension, event.finish(), units)
        != BudgetDecision::Exhausted)
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct EnforcerPhaseCost {
    /// Keep the discriminant outside `SearchCost` without boxing it. This
    /// value is ignored when `present` is false; the explicit bit preserves
    /// the semantic difference between no phase and a zero-work phase while
    /// keeping candidate costing allocation-free.
    cost: SearchCost,
    present: bool,
}

impl EnforcerPhaseCost {
    pub(crate) fn compose_after(self, input: SearchCost) -> Result<SearchCost> {
        if self.present {
            input.sequential(self.cost)
        } else {
            Ok(input)
        }
    }

    #[cfg(test)]
    pub(crate) fn phase(self) -> Option<SearchCost> {
        self.present.then_some(self.cost)
    }
}

pub(crate) fn enforcer_cost(
    steps: &[EnforcerStep],
    input: EnforcerCostInput,
    calibration: &MachineCalibrationBundle,
) -> Result<Option<EnforcerPhaseCost>> {
    if steps.is_empty() {
        return Ok(Some(EnforcerPhaseCost {
            cost: SearchCost::ZERO,
            present: false,
        }));
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
    Ok(Some(EnforcerPhaseCost {
        cost: result,
        present: true,
    }))
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
