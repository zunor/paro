// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Deterministic mandatory-baseline plus bounded optional Cascades search.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use paro_common::error::{self as paro_error, Result};
use smallvec::SmallVec;

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
    CandidatePreview, CandidateSummary, ChildWinnerRef, EquivalenceProof, FrozenCandidate,
    GrantGoalKey, GroupCardinality, LogicalProperties, Memo, OptimizationGoal, Winner,
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
    PatternOperand, PatternRead, PhysicalCandidate, RuleContext, SourceFilterWork,
    SourceRetentionProof, SourceWork, SourceWorkData, TaskSupplyContract, TransformContext,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchStopReason {
    Complete,
    Deadline,
    BudgetLimited,
    RuleFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchStop {
    pub reason: SearchStopReason,
    /// A deadline can coincide with deterministic budget exhaustion. Keep
    /// both facts visible instead of collapsing them into one label.
    pub budget_limited: bool,
    pub configured_deadline_us: Option<u64>,
    /// Measured on SearchControl's clock, which includes mandatory incumbent
    /// construction and therefore matches the configured deadline semantics.
    pub actual_stop_us: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct GrantWinner {
    pub class: ResourceGrantClassId,
    pub goal: OptimizationGoal,
    /// Shared immutable root winner. The winner carries exact child choices,
    /// cost composition, source-work and proof evidence.
    pub winner: Arc<Winner>,
    /// Frozen selected DAG used to prove that extraction does not depend on a
    /// later search pass or a mutable frontier.
    pub frozen: Arc<FrozenCandidate>,
}

#[derive(Debug, Clone)]
pub struct GrantOptimization {
    pub sensitivity: GrantSensitivitySummary,
    pub winners: Box<[GrantWinner]>,
    pub stop: SearchStop,
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

/// Physical readiness work shared by the single-goal and grant-portfolio
/// entry points.  A logical publication adds only its owning group and the
/// registered physical ancestors; the final root pass remains the completion
/// boundary, not the mechanism used to rediscover every intermediate winner.
#[derive(Debug)]
struct PhysicalInterleave {
    root: GroupId,
    goals: Box<[OptimizationGoal]>,
    pending: BTreeSet<(GroupId, OptimizationGoal)>,
}

impl PhysicalInterleave {
    fn new(root: GroupId, goals: impl IntoIterator<Item = OptimizationGoal>) -> Self {
        let mut goals = goals.into_iter().collect::<Vec<_>>();
        goals.sort_unstable();
        goals.dedup();
        let pending = goals.iter().copied().map(|goal| (root, goal)).collect();
        Self {
            root,
            goals: goals.into_boxed_slice(),
            pending,
        }
    }
}

#[derive(Debug, Clone)]
struct CostRecipe {
    /// Monotone position within one `(group, goal)` recipe stream.  Physical
    /// expression IDs are not enough here: one expression can acquire a new
    /// child-goal recipe after an earlier readiness pass.
    sequence: u64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CombinationAdmission {
    /// The exact cost has not yet been compared with the parent frontier.
    Pending,
    /// The exact cost is retained, but the current parent frontier dominated
    /// the proposal. A published dominator remains a valid proof for this
    /// frozen cost/read context even when the bounded frontier later evicts
    /// it, so this combination never needs another admission scan.
    FrontierRejected { dominator: CandidateId },
    /// The exact cost is retained, but the bounded parent frontier did not
    /// retain the candidate.  This remains an incomplete frontier result,
    /// not a proof that the combination was never useful.
    FrontierTruncated,
    /// The candidate was published into the Memo winner archive.  Replaying
    /// it would allocate a second CandidateId, so it is never re-published.
    Published,
}

#[derive(Debug, Clone)]
struct CostedChildCombination {
    /// Exact immutable child choices.  CandidateId is the semantic identity;
    /// this copy is the payload needed to build a Winner without consulting a
    /// later frontier ordinal.
    children: Box<[ChildWinnerRef]>,
    local_cost: SearchCost,
    cost: SearchCost,
    source_work: Box<[SourceWork]>,
    physical_fingerprint: Fingerprint,
    admission: CombinationAdmission,
}

/// A resumable Cartesian-product cursor whose ordering is only a traversal
/// detail.  Combination identity is always the exact CandidateId tuple, so a
/// child frontier may reorder without invalidating progress.
#[derive(Debug, Clone)]
struct StableCombinationCursor {
    frontiers: Box<[Box<[CandidateId]>]>,
    last: Option<Box<[CandidateId]>>,
    complete: bool,
    mandatory_first: bool,
    mandatory_children: Option<Box<[CandidateId]>>,
    mandatory_emitted: bool,
}

impl StableCombinationCursor {
    fn new(
        frontiers: Box<[Box<[CandidateId]>]>,
        mandatory_first: bool,
        mandatory_children: Option<Box<[CandidateId]>>,
    ) -> Self {
        Self {
            frontiers,
            last: None,
            complete: false,
            mandatory_first,
            mandatory_children,
            mandatory_emitted: false,
        }
    }

    fn next(&mut self) -> Option<Box<[CandidateId]>> {
        if self.complete {
            return None;
        }
        let next = next_stable_combination(&self.frontiers, self.last.as_deref());
        let Some(next) = next else {
            self.complete = true;
            return None;
        };
        self.last = Some(next.clone());
        Some(next)
    }

    fn next_with_kind(&mut self) -> Option<(Box<[CandidateId]>, bool)> {
        if self.mandatory_first && !self.mandatory_emitted {
            self.mandatory_emitted = true;
            if let Some(children) = &self.mandatory_children {
                return Some((children.clone(), true));
            }
        }
        while let Some(children) = self.next() {
            // The mandatory baseline is emitted before the stable product
            // walk, but it must still count as covered by that walk. Skip its
            // lexicographic occurrence so a pause immediately after the
            // baseline cannot replay an old tuple.
            if self
                .mandatory_children
                .as_deref()
                .is_some_and(|mandatory| mandatory == children.as_ref())
            {
                continue;
            }
            return Some((children, false));
        }
        None
    }

    fn retain_active(&mut self, active: &[Box<[CandidateId]>]) {
        for (frontier, current) in self.frontiers.iter_mut().zip(active) {
            *frontier = frontier
                .iter()
                .copied()
                .filter(|candidate| current.binary_search(candidate).is_ok())
                .collect::<Vec<_>>()
                .into_boxed_slice();
        }
        if self.frontiers.iter().any(|frontier| frontier.is_empty()) {
            self.complete = true;
        }
    }
}

/// Query-local progress for one immutable physical recipe.  This is smaller
/// than a Memo/frontier clone: it stores only stable child-choice tuples that
/// were actually priced, plus cursors for the not-yet-priced product regions.
/// Admission status and budget rejection are deliberately separate from the
/// priced result so neither can accidentally suppress a later retry.
#[derive(Debug, Default)]
struct ChildCombinationState {
    cost_context: Option<Fingerprint>,
    active_frontiers: Box<[Box<[CandidateId]>]>,
    base_cursor: Option<StableCombinationCursor>,
    delta_cursors: Vec<StableCombinationCursor>,
    priced: BTreeMap<Box<[CandidateId]>, CostedChildCombination>,
    resource_rejected: BTreeSet<Box<[CandidateId]>>,
    budget_rejected: BTreeSet<Box<[CandidateId]>>,
    parent_frontier_revision: u64,
}

impl ChildCombinationState {
    fn reset_for_context(
        &mut self,
        frontiers: Box<[Box<[CandidateId]>]>,
        cost_context: Fingerprint,
        parent_frontier_revision: u64,
        mandatory_children: Option<Box<[CandidateId]>>,
    ) {
        self.cost_context = Some(cost_context);
        self.active_frontiers = frontiers.clone();
        self.base_cursor = Some(StableCombinationCursor::new(
            frontiers,
            true,
            mandatory_children,
        ));
        self.delta_cursors.clear();
        self.priced.clear();
        self.resource_rejected.clear();
        self.budget_rejected.clear();
        self.parent_frontier_revision = parent_frontier_revision;
    }

    /// Add only products containing a newly published candidate.  The first
    /// new position is the pivot, which makes the domains disjoint even when
    /// several child frontiers grow in one publication batch.
    fn observe_frontiers(&mut self, current: Box<[Box<[CandidateId]>]>) {
        let previous = self.active_frontiers.clone();
        let mut old_active = previous.clone();
        for (old, now) in old_active.iter_mut().zip(current.iter()) {
            *old = old
                .iter()
                .copied()
                .filter(|candidate| now.binary_search(candidate).is_ok())
                .collect::<Vec<_>>()
                .into_boxed_slice();
        }
        for pivot in 0..current.len() {
            let new_candidates = current[pivot]
                .iter()
                .copied()
                .filter(|candidate| previous[pivot].binary_search(candidate).is_err())
                .collect::<Vec<_>>()
                .into_boxed_slice();
            if new_candidates.is_empty() {
                continue;
            }
            let domains = current
                .iter()
                .enumerate()
                .map(|(index, frontier)| {
                    if index < pivot {
                        old_active[index].clone()
                    } else if index == pivot {
                        new_candidates.clone()
                    } else {
                        frontier.clone()
                    }
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            self.delta_cursors
                .push(StableCombinationCursor::new(domains, false, None));
        }
        if let Some(cursor) = &mut self.base_cursor {
            cursor.retain_active(&old_active);
        }
        for cursor in &mut self.delta_cursors {
            cursor.retain_active(&current);
        }
        self.delta_cursors.retain(|cursor| !cursor.complete);
        self.active_frontiers = current;
    }

    fn active(&self, children: &[ChildWinnerRef]) -> bool {
        children.iter().enumerate().all(|(index, child)| {
            self.active_frontiers
                .get(index)
                .is_some_and(|frontier| frontier.binary_search(&child.candidate).is_ok())
        })
    }

    fn next_unpriced_domain_tuple(&mut self) -> Option<(Box<[CandidateId]>, bool)> {
        if let Some(cursor) = &mut self.base_cursor {
            if let Some(next) = cursor.next_with_kind() {
                return Some(next);
            }
        }
        while let Some(cursor) = self.delta_cursors.first_mut() {
            if let Some(next) = cursor.next_with_kind() {
                return Some(next);
            }
            self.delta_cursors.remove(0);
        }
        None
    }
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

const SEARCH_CHECKPOINT_TARGETS_MS: [u64; 5] = [5, 10, 20, 50, 100];

/// A diagnostic-only snapshot of the currently selected root candidate at a
/// fixed search-time checkpoint. The timestamp is the first observation at or
/// after the target, so a long indivisible costing step is visible rather
/// than being presented as an exact stop point. A candidate is executable at
/// the Memo boundary, while `search_complete` separately records whether the
/// overall optional closure has finished.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SearchCheckpoint {
    pub target_ms: u64,
    pub observed_us: u64,
    pub goal: OptimizationGoal,
    pub candidate: Option<CandidateId>,
    pub expected_cost: Option<f64>,
    pub risk_adjusted_cost: Option<f64>,
    pub upper_cost: Option<f64>,
    pub search_complete: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
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
    /// Ordered by target checkpoint, then the exact root goal. This is
    /// populated only in diagnostic cohorts; normal trace-off C1 retains an
    /// empty allocation-free Vec.
    pub search_checkpoints: Vec<SearchCheckpoint>,
    /// Stop/handoff facts are diagnostic-only.  The actual result contract is
    /// carried by `GrantOptimization::stop` even when tracing is disabled.
    pub search_stop_reason: Option<SearchStopReason>,
    pub search_deadline_us: Option<u64>,
    pub search_stop_us: Option<u64>,
    pub search_stop_profile_us: Option<u64>,
    pub frozen_candidate_count: u64,
    pub freeze_elapsed_us: u64,
    pub search_return_profile_us: Option<u64>,
    pub timeout_tail_profile_us: Option<u64>,
    pub handoff_extraction_us: Option<u64>,
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
    preserve_incomplete_physical: bool,
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
    diagnostic_checkpoint_goals: Box<[OptimizationGoal]>,
    next_diagnostic_checkpoint: usize,
    diagnostic_search_complete: bool,
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
    region_candidates: BTreeMap<Box<[Fingerprint]>, BTreeSet<Fingerprint>>,
    /// Reverse physical recipe dependencies. A changed child frontier wakes
    /// only the parent goals whose recipes consumed that child, then walks the
    /// already registered ancestor chain. This index is query-local and is
    /// rebuilt lazily as new native/settled recipes are admitted.
    physical_parents:
        BTreeMap<GroupId, BTreeSet<(GroupId, OptimizationGoal, PhysicalExprId, Fingerprint)>>,
    /// Recipe identities dirtied by a changed child frontier.  A queued
    /// parent consumes only these old recipes plus any recipes appended after
    /// its cursor; a fact/statistics change still deliberately falls back to
    /// the complete local stream.
    physical_dirty_recipes:
        BTreeMap<(GroupId, OptimizationGoal), BTreeSet<(PhysicalExprId, Fingerprint)>>,
    /// Cost frontiers cleared by a new search epoch need one complete rebuild
    /// per physical subproblem. Once that rebuild has run, an incomplete
    /// readiness cursor is an append-only prefix and must not force another
    /// full scan.
    physical_full_recost: BTreeSet<(GroupId, OptimizationGoal)>,
    /// Exact goals under which a group has been observed as a physical
    /// dependency. A root goal must never be substituted for a child's
    /// required materialization, partitioning, or grant contract.
    physical_goals: BTreeMap<GroupId, BTreeSet<OptimizationGoal>>,
    /// Physical context work is keyed by the complete OptimizationGoal, not
    /// by group alone.  These counters make context reuse visible without
    /// retaining a second cache or changing the publication protocol.
    physical_subproblem_requests: u64,
    physical_subproblem_reuses: u64,
    physical_subproblem_evaluations: u64,
    physical_stale_retries: u64,
    physical_implementation_requests: u64,
    /// Logical frontier growth is append-only.  Keep the exact implementation
    /// visit identity so a later physical recost can discover only newly
    /// published logical expressions.  The mandatory/optional bit is part of
    /// the identity: mandatory baseline enumeration deliberately skips
    /// optional physical alternatives, which must be offered once when the
    /// optional phase begins.
    physical_implementation_seen: BTreeSet<(GroupId, OptimizationGoal, LogicalExprId, bool)>,
    physical_implementation_expression_evaluations: u64,
    physical_implementation_expression_skips: u64,
    child_combination_events: BTreeMap<ChildCombinationIdentity, Fingerprint>,
    next_child_combination_event: u128,
    child_combination_states:
        BTreeMap<(PhysicalExprId, OptimizationGoal, Fingerprint), ChildCombinationState>,
    child_combination_new_count: u64,
    child_combination_recompute_count: u64,
    child_combination_cost_synthesis_count: u64,
    child_combination_frontier_recheck_count: u64,
    child_combination_budget_rejection_count: u64,
    next_recipe_sequence: BTreeMap<(GroupId, OptimizationGoal), u64>,
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
            preserve_incomplete_physical: false,
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
            diagnostic_checkpoint_goals: Box::new([]),
            next_diagnostic_checkpoint: 0,
            diagnostic_search_complete: false,
            optional_search_started: false,
            transformation_bindings: 0,
            fact_value_revalidation_hits: 0,
            fact_value_revalidation_misses: 0,
            transformation_observations: BTreeMap::new(),
            transformation_fact_observations: BTreeMap::new(),
            transformation_applications: BTreeMap::new(),
            transformation_subscribers: BTreeMap::new(),
            region_candidates: BTreeMap::new(),
            physical_parents: BTreeMap::new(),
            physical_dirty_recipes: BTreeMap::new(),
            physical_full_recost: BTreeSet::new(),
            physical_goals: BTreeMap::new(),
            physical_subproblem_requests: 0,
            physical_subproblem_reuses: 0,
            physical_subproblem_evaluations: 0,
            physical_stale_retries: 0,
            physical_implementation_requests: 0,
            physical_implementation_seen: BTreeSet::new(),
            physical_implementation_expression_evaluations: 0,
            physical_implementation_expression_skips: 0,
            child_combination_events: BTreeMap::new(),
            next_child_combination_event: 1,
            child_combination_states: BTreeMap::new(),
            child_combination_new_count: 0,
            child_combination_recompute_count: 0,
            child_combination_cost_synthesis_count: 0,
            child_combination_frontier_recheck_count: 0,
            child_combination_budget_rejection_count: 0,
            next_recipe_sequence: BTreeMap::new(),
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

    /// Merge equivalent Memo groups through the same owner/redirect protocol
    /// used by resumable tasks. Callers that change group identity must use
    /// this entry point instead of mutating `Memo` directly: the Memo merge
    /// clears affected frontiers, while the registry invalidates stale task
    /// outcomes and the engine drops only transformation observations that
    /// read the merged equivalence class.
    pub fn merge_groups(&mut self, left: GroupId, right: GroupId) -> Result<GroupId> {
        let left = self.memo.canonical_group(left);
        let right = self.memo.canonical_group(right);
        if left == right {
            return Ok(left);
        }
        let (secondary, expected_canonical) = if left < right {
            (right, left)
        } else {
            (left, right)
        };
        // Preflight the task-side redirect while Memo still has both group
        // identities.  The actual registry mutation is deterministic after
        // this check; if Memo rejects the contract, no task is invalidated.
        self.task_registry
            .validate_group_redirect(secondary, expected_canonical)?;
        let canonical = self.memo.merge_groups(left, right)?;
        debug_assert_eq!(canonical, expected_canonical);
        // Memo::merge_groups validates the output contract and completes its
        // union-find update before this call. Redirecting afterward ensures a
        // failed Memo validation cannot invalidate a live task in advance.
        let _ = self.task_registry.redirect_group(secondary, canonical)?;
        self.recanonicalize_physical_parents();
        // Logical and physical expression ids from the two pre-merge groups
        // no longer describe an isolated implementation domain.  Revisit the
        // canonical group from its published expressions instead of allowing
        // a pre-merge visit marker to suppress a valid candidate.
        self.physical_implementation_seen.clear();
        self.discard_merged_transformation_state(secondary, canonical);
        Ok(canonical)
    }

    fn recanonicalize_physical_parents(&mut self) {
        let previous = std::mem::take(&mut self.physical_parents);
        for (child, parents) in previous {
            let child = self.memo.canonical_group(child);
            for (parent, goal, physical, recipe) in parents {
                self.physical_parents.entry(child).or_default().insert((
                    self.memo.canonical_group(parent),
                    goal,
                    physical,
                    recipe,
                ));
            }
        }
        let previous = std::mem::take(&mut self.physical_dirty_recipes);
        for ((group, goal), recipes) in previous {
            self.physical_dirty_recipes
                .entry((self.memo.canonical_group(group), goal))
                .or_default()
                .extend(recipes);
        }
        let previous = std::mem::take(&mut self.physical_full_recost);
        for (group, goal) in previous {
            self.physical_full_recost
                .insert((self.memo.canonical_group(group), goal));
        }
        let previous = std::mem::take(&mut self.physical_goals);
        for (group, goals) in previous {
            self.physical_goals
                .entry(self.memo.canonical_group(group))
                .or_default()
                .extend(goals);
        }
        let previous = std::mem::take(&mut self.next_recipe_sequence);
        for ((group, goal), sequence) in previous {
            self.next_recipe_sequence
                .entry((self.memo.canonical_group(group), goal))
                .and_modify(|current| *current = (*current).max(sequence))
                .or_insert(sequence);
        }
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

    fn begin_diagnostic_profile(
        &mut self,
        root: GroupId,
        checkpoint_goals: impl IntoIterator<Item = OptimizationGoal>,
    ) {
        self.search_milestones = SearchMilestones::default();
        self.milestone_root = self
            .collect_rule_work_profile
            .then_some(self.memo.canonical_group(root));
        if self.collect_rule_work_profile {
            let mut goals = checkpoint_goals.into_iter().collect::<Vec<_>>();
            goals.sort_unstable();
            goals.dedup();
            self.diagnostic_checkpoint_goals = goals.into_boxed_slice();
        } else {
            self.diagnostic_checkpoint_goals = Box::new([]);
        }
        self.next_diagnostic_checkpoint = 0;
        self.diagnostic_search_complete = false;
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

    fn search_stop(&self) -> SearchStop {
        let obligations = self.memo.search_obligations();
        let budget_limited = obligations.iter().any(|obligation| {
            matches!(
                obligation.reason,
                super::budget::SearchIncompleteReason::Budget(_)
            )
        });
        let reason = if self.memo.control().deadline_reached() {
            SearchStopReason::Deadline
        } else if budget_limited {
            SearchStopReason::BudgetLimited
        } else if !obligations.is_empty() {
            SearchStopReason::RuleFailure
        } else {
            SearchStopReason::Complete
        };
        SearchStop {
            reason,
            budget_limited,
            configured_deadline_us: self.memo.control().optional_time_limit_us(),
            actual_stop_us: match reason {
                SearchStopReason::Complete => None,
                SearchStopReason::Deadline => self
                    .memo
                    .control()
                    .deadline_elapsed_us()
                    .or_else(|| Some(self.memo.control().elapsed_us())),
                SearchStopReason::BudgetLimited | SearchStopReason::RuleFailure => {
                    Some(self.memo.control().elapsed_us())
                }
            },
        }
    }

    fn note_search_stop(&mut self, stop: SearchStop) {
        if !self.collect_rule_work_profile {
            return;
        }
        self.search_milestones.search_stop_reason = Some(stop.reason);
        self.search_milestones.search_deadline_us = stop.configured_deadline_us;
        self.search_milestones.search_stop_us = stop.actual_stop_us;
        self.search_milestones.search_stop_profile_us = self.profile_elapsed_us();
    }

    pub(crate) fn note_search_return(&mut self) {
        if !self.collect_rule_work_profile {
            return;
        }
        let Some(return_us) = self.profile_elapsed_us() else {
            return;
        };
        self.search_milestones.search_return_profile_us = Some(return_us);
        self.search_milestones.timeout_tail_profile_us = self
            .search_milestones
            .search_stop_profile_us
            .map(|stop_us| return_us.saturating_sub(stop_us));
    }

    fn freeze_grant_winner(
        &mut self,
        root: GroupId,
        class: ResourceGrantClassId,
        goal: OptimizationGoal,
        winner: Arc<Winner>,
    ) -> Result<GrantWinner> {
        let started = Instant::now();
        let reference = ChildWinnerRef {
            group: self.memo.canonical_group(root),
            goal,
            candidate: winner.candidate,
        };
        super::verifier::WinnerVerifier::verify_candidate_tree(&self.memo, reference)?;
        let frozen = self.memo.freeze_candidate_tree(reference)?;
        if frozen.winner.candidate != winner.candidate {
            return Err(paro_error::internal(
                "frozen winner identity disagrees with the selected grant winner",
            ));
        }
        if self.collect_rule_work_profile {
            self.search_milestones.frozen_candidate_count = self
                .search_milestones
                .frozen_candidate_count
                .saturating_add(1);
            self.search_milestones.freeze_elapsed_us = self
                .search_milestones
                .freeze_elapsed_us
                .saturating_add(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        }
        Ok(GrantWinner {
            class,
            goal,
            winner,
            frozen,
        })
    }

    fn record_search_checkpoints(&mut self, root: GroupId) {
        if !self.collect_rule_work_profile
            || self.diagnostic_checkpoint_goals.is_empty()
            || self.next_diagnostic_checkpoint >= SEARCH_CHECKPOINT_TARGETS_MS.len()
        {
            return;
        }
        let Some(observed_us) = self.profile_elapsed_us() else {
            return;
        };
        let root = self.memo.canonical_group(root);
        while self.next_diagnostic_checkpoint < SEARCH_CHECKPOINT_TARGETS_MS.len()
            && observed_us >= SEARCH_CHECKPOINT_TARGETS_MS[self.next_diagnostic_checkpoint] * 1_000
        {
            let target_ms = SEARCH_CHECKPOINT_TARGETS_MS[self.next_diagnostic_checkpoint];
            let search_complete = self.diagnostic_search_complete;
            let mut checkpoints = Vec::with_capacity(self.diagnostic_checkpoint_goals.len());
            for goal in self.diagnostic_checkpoint_goals.iter().copied() {
                let winner = self.memo.group(root).and_then(|group| group.winner(goal));
                checkpoints.push(SearchCheckpoint {
                    target_ms,
                    observed_us,
                    goal,
                    candidate: winner.map(|winner| winner.candidate),
                    expected_cost: winner.map(|winner| winner.cost.score.range.expected),
                    risk_adjusted_cost: winner.map(|winner| winner.cost.score.risk_adjusted),
                    upper_cost: winner.map(|winner| winner.cost.score.range.upper),
                    search_complete,
                });
            }
            self.search_milestones
                .search_checkpoints
                .extend(checkpoints);
            self.next_diagnostic_checkpoint += 1;
        }
    }

    fn record_diagnostic_checkpoints(&mut self) {
        if let Some(root) = self.milestone_root {
            self.record_search_checkpoints(root);
        }
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
        self.begin_diagnostic_profile(root, std::iter::once(goal));
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
            self.record_search_checkpoints(root);
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
                self.record_search_checkpoints(root);
                return incumbent.ok_or_else(|| self.infeasible_goal_error(root, goal));
            }
            self.reset_cost_epoch()?;
            self.optional_search_started = self.collect_rule_work_profile;
            // The archived mandatory incumbent remains the safe plan for this
            // new cost epoch.  As soon as optional work publishes enough new
            // logical alternatives, mandatory physical work is re-costed
            // incrementally below; this keeps the incumbent fallback semantics
            // intact when cancellation happens before the first publication.
            // Re-cost the protected baseline at logical publication batches.
            // This is the quality-first hand-off: a newly published narrow
            // aggregate or pushed domain must become executable before a
            // later parent transformation can decide that the broad plan is
            // its only available response.
            self.explore_transformations_with_interleave(Some(PhysicalInterleave::new(
                root,
                std::iter::once(goal),
            )))?;
            self.record_search_checkpoints(root);
        }
        self.optimize_group(root, goal)?;
        super::verifier::MemoVerifier::verify(&self.memo, None)?;
        self.diagnostic_search_complete =
            !self.memo.control().deadline_reached() && self.memo.search_obligations().is_empty();
        self.record_search_checkpoints(root);
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
        self.physical_full_recost = self.next_recipe_sequence.keys().copied().collect();
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
        let checkpoint_sensitivity = self.goal_grant_sensitivity(root, base_goal.required)?;
        let checkpoint_goals = classes
            .values()
            .copied()
            .map(|class| OptimizationGoal {
                grant: checkpoint_sensitivity.goal_for(admissible_set, class),
                ..base_goal
            })
            .collect::<Vec<_>>();
        self.begin_diagnostic_profile(root, checkpoint_goals.iter().copied());
        if mode == SearchMode::Memo {
            let phase = self.memo.control().incumbent_phase();
            self.mandatory_only = true;
            let incumbent = self.optimize_grant_classes(root, base_goal, admissible_set, &classes);
            self.mandatory_only = false;
            drop(phase);
            self.record_search_checkpoints(root);
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
                self.record_search_checkpoints(root);
                let stop = self.search_stop();
                self.note_search_stop(stop);
                return incumbent.map(|mut incumbent| {
                    incumbent.stop = stop;
                    incumbent
                });
            }
            self.reset_cost_epoch()?;
            self.optional_search_started = self.collect_rule_work_profile;
            let sensitivity = self.goal_grant_sensitivity(root, base_goal.required)?;
            let goals = classes.values().copied().map(|class| OptimizationGoal {
                grant: sensitivity.goal_for(admissible_set, class),
                ..base_goal
            });
            self.explore_transformations_with_interleave(Some(PhysicalInterleave::new(
                root, goals,
            )))?;
            self.record_search_checkpoints(root);
            if !self.memo.control().checkpoint()? {
                self.record_search_checkpoints(root);
                return self.stop_with_snapshot_or_fallback(
                    root,
                    base_goal,
                    admissible_set,
                    &classes,
                    incumbent,
                );
            }
            let result = self.optimize_grant_classes(root, base_goal, admissible_set, &classes);
            if self.memo.control().deadline_reached() {
                self.record_search_checkpoints(root);
                let fallback = result.or_else(|_| incumbent);
                return self.stop_with_snapshot_or_fallback(
                    root,
                    base_goal,
                    admissible_set,
                    &classes,
                    fallback,
                );
            }
            if result.is_ok() {
                self.diagnostic_search_complete = !self.memo.control().deadline_reached()
                    && self.memo.search_obligations().is_empty();
                self.note_search_stop(self.search_stop());
            }
            self.record_search_checkpoints(root);
            return result;
        }
        let result = self.optimize_grant_classes(root, base_goal, admissible_set, &classes);
        if result.is_ok() {
            self.diagnostic_search_complete = !self.memo.control().deadline_reached()
                && self.memo.search_obligations().is_empty();
            self.note_search_stop(self.search_stop());
        }
        self.record_search_checkpoints(root);
        result
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
                .cloned()
            {
                winners.push(self.freeze_grant_winner(root, class.id, goal, winner)?);
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
            stop: self.search_stop(),
        })
    }

    /// Capture the newest complete root response already published by the
    /// interleaved physical queue.  This function performs no optimization:
    /// it only selects, verifies, and freezes the current frontier entries.
    /// A class without a qualified post-reset entry inherits its immutable
    /// mandatory incumbent, if one exists.
    fn snapshot_grant_classes(
        &mut self,
        root: GroupId,
        base_goal: OptimizationGoal,
        admissible_set: AdmissibleGrantSetId,
        classes: &BTreeMap<ResourceGrantClassId, ResourceGrantClass>,
        fallback: Option<&GrantOptimization>,
    ) -> Result<Option<GrantOptimization>> {
        let root = self.memo.canonical_group(root);
        let sensitivity = self.goal_grant_sensitivity(root, base_goal.required)?;
        let mut winners = Vec::with_capacity(classes.len());
        let mut last_infeasible = None;
        for class in classes.values().copied() {
            let goal = OptimizationGoal {
                grant: sensitivity.goal_for(admissible_set, class),
                ..base_goal
            };
            let current = self
                .memo
                .group(root)
                .and_then(|group| group.winner_frontier(goal))
                .and_then(|frontier| {
                    frontier
                        .candidates()
                        .iter()
                        .find(|winner| {
                            winner.cost.peak_memory_upper <= class.hard_memory_bytes
                                && (winner.cost.spill_bytes_expected == 0
                                    || class.spill_policy == SpillPolicy::Allowed)
                        })
                        .cloned()
                });
            if let Some(winner) = current {
                winners.push(self.freeze_grant_winner(root, class.id, goal, winner)?);
                continue;
            }
            if let Some(incumbent) = fallback.and_then(|optimization| {
                optimization
                    .winners
                    .iter()
                    .find(|winner| winner.class == class.id)
                    .cloned()
            }) {
                winners.push(incumbent);
            } else {
                last_infeasible = Some(goal);
            }
        }
        if winners.is_empty() {
            // Keep the caller's original error when neither the current
            // frontier nor the baseline can satisfy any grant class.
            let _ = last_infeasible;
            return Ok(None);
        }
        Ok(Some(GrantOptimization {
            sensitivity,
            winners: winners.into_boxed_slice(),
            stop: self.search_stop(),
        }))
    }

    fn stop_with_snapshot_or_fallback(
        &mut self,
        root: GroupId,
        base_goal: OptimizationGoal,
        admissible_set: AdmissibleGrantSetId,
        classes: &BTreeMap<ResourceGrantClassId, ResourceGrantClass>,
        fallback: Result<GrantOptimization>,
    ) -> Result<GrantOptimization> {
        let stop = self.search_stop();
        // A frontier entry that cannot be verified/frozen is not a qualified
        // candidate.  Keep the already verified mandatory result in that
        // case; only return the snapshot when the whole grant portfolio can
        // be published from it.
        if let Some(mut snapshot) = self.snapshot_grant_classes(
            root,
            base_goal,
            admissible_set,
            classes,
            fallback.as_ref().ok(),
        )? {
            snapshot.stop = stop;
            self.note_search_stop(stop);
            return Ok(snapshot);
        }
        match fallback {
            Ok(mut fallback) => {
                fallback.stop = stop;
                self.note_search_stop(stop);
                Ok(fallback)
            }
            Err(error) => Err(error),
        }
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

    fn register_physical_dependency(
        &mut self,
        child: GroupId,
        child_goal: OptimizationGoal,
        parent: GroupId,
        parent_goal: OptimizationGoal,
        physical: PhysicalExprId,
        recipe: Fingerprint,
    ) {
        let child = self.memo.canonical_group(child);
        let parent = self.memo.canonical_group(parent);
        self.physical_goals
            .entry(child)
            .or_default()
            .insert(child_goal);
        self.physical_goals
            .entry(parent)
            .or_default()
            .insert(parent_goal);
        self.physical_parents.entry(child).or_default().insert((
            parent,
            parent_goal,
            physical,
            recipe,
        ));
    }

    /// Add a changed group's exact physical targets and walk only the reverse
    /// recipe edges already observed by this engine. Child and parent goals
    /// stay in the dependency index; a root goal is never substituted for a
    /// child's materialization, partitioning, or grant contract.
    fn enqueue_physical_ancestors(
        &mut self,
        changed_groups: impl IntoIterator<Item = GroupId>,
        interleave: &mut PhysicalInterleave,
    ) {
        let mut groups = VecDeque::new();
        let mut visited = BTreeSet::new();
        for group in changed_groups {
            let group = self.memo.canonical_group(group);
            let goals = if group == interleave.root {
                interleave.goals.iter().copied().collect::<Vec<_>>()
            } else {
                self.physical_goals
                    .get(&group)
                    .into_iter()
                    .flat_map(|goals| goals.iter().copied())
                    .collect::<Vec<_>>()
            };
            for goal in goals {
                interleave.pending.insert((group, goal));
                self.infeasible_goals.remove(&(group, goal));
            }
            groups.push_back(group);
        }
        while let Some(group) = groups.pop_front() {
            if !visited.insert(group) {
                continue;
            }
            let Some(parents) = self.physical_parents.get(&group) else {
                continue;
            };
            for &(parent, goal, physical, recipe) in parents {
                let parent = self.memo.canonical_group(parent);
                interleave.pending.insert((parent, goal));
                self.physical_dirty_recipes
                    .entry((parent, goal))
                    .or_default()
                    .insert((physical, recipe));
                self.infeasible_goals.remove(&(parent, goal));
                groups.push_back(parent);
            }
        }
    }

    fn drain_physical_interleave(&mut self, interleave: &mut PhysicalInterleave) -> Result<()> {
        while let Some((group, goal)) = interleave.pending.pop_first() {
            if !self.memo.control().checkpoint()? {
                break;
            }
            let previous_mandatory_only = self.mandatory_only;
            let previous_preserve_incomplete = self.preserve_incomplete_physical;
            // Interleave runs at the same policy level as the surrounding
            // optional search.  Restricting this queue to mandatory
            // implementations would make every early snapshot a baseline
            // plan and would exclude the alternative physical operators that
            // carry the recovered execution-quality chain.
            self.mandatory_only = previous_mandatory_only;
            self.preserve_incomplete_physical = true;
            let result = self.optimize_group(group, goal);
            self.mandatory_only = previous_mandatory_only;
            self.preserve_incomplete_physical = previous_preserve_incomplete;
            result?;
            self.record_diagnostic_checkpoints();
        }
        Ok(())
    }

    #[cfg(test)]
    fn explore_transformations(&mut self) -> Result<()> {
        self.explore_transformations_with_interleave(None)
    }

    fn explore_transformations_with_interleave(
        &mut self,
        mut interleave: Option<PhysicalInterleave>,
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
            self.record_diagnostic_checkpoints();
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
            // A binding set is a snapshot shared by all exact bindings of the
            // source expression. Applying one binding may refine a reused
            // Memo group's facts; the remaining bindings then belong to a new
            // discovery frontier and must be re-enumerated. Do not submit a
            // later application with the earlier shared snapshot and rely on
            // publication to discover the conflict after it has already
            // committed unrelated local work.
            let binding_read_set = ReadSet::new(binding_set.reads.iter().copied());
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
                if !binding_read_set.is_current(&self.memo)? {
                    // The successful predecessor publication schedules the
                    // subscribed source task through its locally written
                    // groups. Re-enter that task with a fresh binding set;
                    // the remaining bindings from this snapshot are not
                    // independently valid.
                    break;
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
                    // The application task consumes both the discovery
                    // frontier and the facts read while constructing this
                    // exact binding. Keeping both observations is important:
                    // a later logical alternative can change the binding
                    // even when the application-local facts are unchanged.
                    ReadSet::new(
                        binding_set
                            .reads
                            .iter()
                            .copied()
                            .chain(application_reads.iter().copied()),
                    ),
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
                    let locally_written_groups = locally_written_groups
                        .into_iter()
                        .map(|group| self.memo.canonical_group(group))
                        .collect::<BTreeSet<_>>();
                    let changed_cte_readers = self.memo.take_changed_cte_readers();
                    self.note_logical_publication();
                    self.publish_transformation_task(
                        transformation_task,
                        locally_written_groups
                            .iter()
                            .copied()
                            .chain(std::iter::once(group))
                            .chain(appended_groups.iter().copied())
                            .chain(changed_cte_readers.iter().copied())
                            .chain(fact_reads.iter().map(|read| read.group)),
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
                    inserted_groups.extend(locally_written_groups);
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
                if let Some(interleave) = interleave.as_mut() {
                    self.enqueue_physical_ancestors(inserted_groups.iter().copied(), interleave);
                }
                for target in inserted_groups {
                    self.schedule_transformation_dependents(target, &mut agenda)?;
                }
            }
            if effective_insertions_since_recost >= INTERLEAVE_BATCH {
                if let Some(interleave) = interleave.as_mut() {
                    // Drain only groups invalidated by the latest logical
                    // publication (plus their registered physical ancestors).
                    // The final root pass still owns completion; this queue is
                    // solely the early quality/readiness path.
                    self.drain_physical_interleave(interleave)?;
                }
                effective_insertions_since_recost = 0;
            }
        }
        if let Some(interleave) = interleave.as_mut() {
            // A tail smaller than INTERLEAVE_BATCH must still become visible
            // before the final grant extraction. This is an incremental drain,
            // not another whole-root exploration.
            self.drain_physical_interleave(interleave)?;
        }
        self.record_diagnostic_checkpoints();
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
            ("physical_stale_retry_count", self.physical_stale_retries),
            (
                "physical_implementation_request_count",
                self.physical_implementation_requests,
            ),
            (
                "physical_implementation_expression_evaluation_count",
                self.physical_implementation_expression_evaluations,
            ),
            (
                "physical_implementation_expression_skip_count",
                self.physical_implementation_expression_skips,
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
                "child_combination_new_count",
                self.child_combination_new_count,
            ),
            (
                "child_combination_recompute_count",
                self.child_combination_recompute_count,
            ),
            (
                "child_combination_cost_synthesis_count",
                self.child_combination_cost_synthesis_count,
            ),
            (
                "child_combination_frontier_recheck_count",
                self.child_combination_frontier_recheck_count,
            ),
            (
                "child_combination_budget_rejection_count",
                self.child_combination_budget_rejection_count,
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

    /// A group merge changes the semantic identity of every transformation
    /// observation that read either side. Those tasks have already been
    /// invalidated by `TaskRegistry::redirect_group`; removing only their
    /// reverse-index entries prevents a stale no-match/application payload
    /// from being resurrected when the exact task identity is reopened.
    fn discard_merged_transformation_state(&mut self, secondary: GroupId, canonical: GroupId) {
        let touches_group =
            |group: GroupId| group == secondary || self.memo.canonical_group(group) == canonical;
        let touches_reads =
            |reads: &[PatternRead]| reads.iter().any(|read| touches_group(read.group));
        let mut affected = BTreeSet::new();
        for (task, reads) in &self.transformation_observations {
            if task.group == secondary || touches_reads(reads) {
                affected.insert(*task);
            }
        }
        for (task, reads) in &self.transformation_fact_observations {
            if task.group == secondary || touches_reads(reads) {
                affected.insert(*task);
            }
        }
        for ((task, _), applications) in &self.transformation_applications {
            if task.group == secondary
                || applications.iter().any(|application| {
                    pattern_operand_touches_group(&application.binding.root, secondary, canonical)
                })
            {
                affected.insert(*task);
            }
        }
        self.transformation_observations
            .retain(|task, _| !affected.contains(task));
        self.transformation_fact_observations
            .retain(|task, _| !affected.contains(task));
        self.transformation_applications
            .retain(|(task, _), _| !affected.contains(task));
        self.transformation_subscribers.remove(&secondary);
        self.transformation_subscribers.remove(&canonical);
        for subscribers in self.transformation_subscribers.values_mut() {
            subscribers.retain(|task| !affected.contains(task));
        }
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
        let phase = self.mandatory_only;
        for expression in logical_exprs {
            let visit = (group, goal, expression, phase);
            if !self.physical_implementation_seen.insert(visit) {
                self.physical_implementation_expression_skips = self
                    .physical_implementation_expression_skips
                    .saturating_add(1);
                continue;
            }
            self.physical_implementation_expression_evaluations = self
                .physical_implementation_expression_evaluations
                .saturating_add(1);
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
        if let Some(region) = candidate.region.as_mut() {
            refresh_region_candidate_contract(&self.memo, region)?;
        }
        if let Some(region) = &candidate.region {
            let owner = self.memo.canonical_group(group);
            let owner_in_scope = self
                .memo
                .regions()
                .node(region.region)
                .is_some_and(|region| region.scope.contains(&owner));
            if !owner_in_scope {
                if candidate.mandatory {
                    return Err(paro_error::internal(
                        "mandatory physical candidate owner is outside its region scope",
                    ));
                }
                tracing::debug!(
                    target: "paro::optimizer",
                    memo_group = owner.index(),
                    region = region.region.0,
                    "optional physical candidate rejected outside its region scope"
                );
                return Ok(());
            }
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
                let admitted = self
                    .region_candidates
                    .entry(stable_region_candidate_key(region))
                    .or_default();
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
            let sequence = self
                .next_recipe_sequence
                .entry((self.memo.canonical_group(group), goal))
                .or_default();
            let recipe_sequence = *sequence;
            *sequence = sequence
                .checked_add(1)
                .ok_or_else(|| paro_error::internal("physical recipe sequence exhausted"))?;
            if let Some(region) = &candidate.region {
                self.region_candidates
                    .entry(stable_region_candidate_key(region))
                    .or_default()
                    .insert(candidate.physical_fingerprint);
            }
            entry.insert(Arc::new(CostRecipe {
                sequence: recipe_sequence,
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

    /// Capture the physical inputs that one group can observe through its
    /// currently published recipes.  The parent group is intentionally read
    /// only through its logical/fact frontier: the task owns its local
    /// physical writes.  Child physical frontiers are exact dependencies, so
    /// a newly published child winner creates a new evaluation without
    /// making every unchanged recursive visit resumable.
    fn physical_read_set(&self, group: GroupId, goal: OptimizationGoal) -> Result<ReadSet> {
        let group = self.memo.canonical_group(group);
        let physical_exprs = self
            .memo
            .group(group)
            .ok_or_else(|| paro_error::internal("unknown group during physical read capture"))?
            .physical_exprs()
            .to_vec();
        // The task is allowed to publish into its own group, but subsequent
        // requests still need to observe physical expressions/frontier
        // entries published by another owner.  Publication ignores this
        // local physical write; reuse does not.
        // The owner may append physical expressions while evaluating this
        // task. Those local writes are tracked by the per-(group, goal)
        // recipe sequence below; observing the owner's physical frontier here
        // would make every local publication invalidate the task that made
        // it. Child physical frontiers remain exact dependencies and are
        // still read with `physical_from_group`.
        let mut reads = vec![PatternRead::from_group(&self.memo, group)?];
        for physical in physical_exprs {
            for (_, recipe) in self.recipes.range(
                (physical, goal, Fingerprint::default())..=(physical, goal, Fingerprint(u128::MAX)),
            ) {
                for (child, _) in recipe.child_goals.iter().copied() {
                    reads.push(PatternRead::physical_from_group(&self.memo, child)?);
                }
            }
        }
        Ok(ReadSet::new(reads))
    }

    fn optimize_group(&mut self, group: GroupId, goal: OptimizationGoal) -> Result<()> {
        if !self.memo.control().checkpoint()? {
            return Ok(());
        }
        let group = self.memo.canonical_group(group);
        let mut dirty_recipes = self.physical_dirty_recipes.remove(&(group, goal));
        let force_full_recost = self.physical_full_recost.remove(&(group, goal));
        self.physical_subproblem_requests = self.physical_subproblem_requests.saturating_add(1);
        // Child frontiers are part of the exact parent response. Capture them
        // before requesting the task so a changed child selects a new
        // evaluation, while an unchanged incomplete task remains reusable.
        let read_set = self.physical_read_set(group, goal)?;
        let requested_read_set = read_set.clone();
        let mut new_evaluation = false;
        // An incomplete result from the readiness queue is a reusable prefix,
        // not an instruction to reopen the task on every recursive visit. We
        // decide whether the cursor has new recipe work only after capturing
        // the exact current frontier below.
        let mut resume_candidate = false;
        let mut resumed_incomplete = false;
        let task = match self.task_registry.request_current(
            TaskIntent::Optimize { group, goal },
            read_set,
            &self.memo,
        )? {
            TaskRequest::Leader(task) => {
                new_evaluation = true;
                task
            }
            TaskRequest::Reused { task, outcome } => {
                let incomplete = outcome.as_ref().is_some_and(|outcome| {
                    matches!(
                        outcome,
                        TaskOutcome::Progress { cursor }
                            if self
                                .task_registry
                                .cursor(*cursor)
                                .is_some_and(|cursor| !cursor.complete)
                    )
                });
                if !self.mandatory_only && !self.preserve_incomplete_physical && incomplete {
                    resume_candidate = true;
                    task
                } else {
                    self.physical_subproblem_reuses =
                        self.physical_subproblem_reuses.saturating_add(1);
                    return Ok(());
                }
            }
            TaskRequest::Subscriber { task, .. } => {
                return Err(paro_error::internal(format!(
                    "recursive optimization request is already in flight for task {task:?}"
                )))
            }
        };
        let current_cursor = self
            .task_registry
            .task(task)
            .and_then(|record| self.task_registry.cursor(record.cursor))
            .unwrap_or_default();
        let predecessor = self.task_registry.task_predecessor(task);
        let predecessor_cursor = predecessor
            .and_then(|task| self.task_registry.task(task))
            .and_then(|record| self.task_registry.cursor(record.cursor));
        let predecessor_reads = predecessor
            .and_then(|task| self.task_registry.task_read_set(task))
            .and_then(|read_set| self.task_registry.read_set(read_set))
            .cloned();
        let previous_reads = if resume_candidate {
            Some(requested_read_set.clone())
        } else {
            predecessor_reads.clone()
        };
        let mut full_recost = force_full_recost
            || physical_read_requires_full_recost(
                group,
                previous_reads.as_ref(),
                &requested_read_set,
            );
        if !full_recost {
            let changed_children =
                physical_changed_child_groups(group, previous_reads.as_ref(), &requested_read_set);
            if !changed_children.is_empty() {
                let mut dirty = dirty_recipes.take().unwrap_or_default();
                let mut all_indexed = true;
                for child in changed_children {
                    let mut indexed = false;
                    if let Some(parents) = self.physical_parents.get(&child) {
                        for &(parent, parent_goal, physical, recipe) in parents {
                            if parent == group && parent_goal == goal {
                                dirty.insert((physical, recipe));
                                indexed = true;
                            }
                        }
                    }
                    all_indexed &= indexed;
                }
                if all_indexed {
                    dirty_recipes = Some(dirty);
                } else {
                    // A missing reverse edge means the dependency was
                    // observed before the incremental index was populated.
                    // Re-cost the complete stream rather than risk reusing an
                    // old composed child frontier.
                    full_recost = true;
                    dirty_recipes = None;
                }
            }
        }
        // A changed child/fact read invalidates all old recipes because their
        // composed costs may have changed.  A local logical/physical frontier
        // is append-only, so a predecessor cursor is sufficient to visit only
        // newly published expressions. The cursor is task-owned progress;
        // the predecessor result itself is never reused as a winner.
        let recipe_cursor = predecessor_cursor
            .or_else(|| (!new_evaluation).then_some(current_cursor))
            .map(|cursor| cursor.position)
            .unwrap_or_default();
        let recipe_start = if full_recost { 0 } else { recipe_cursor };
        let local_logical_frontier_changed = physical_local_logical_frontier_changed(
            group,
            previous_reads.as_ref(),
            &requested_read_set,
        );
        let enumerate_local_implementations = local_logical_frontier_changed;
        let task_has_incomplete_cursor = self
            .task_registry
            .task(task)
            .and_then(|record| self.task_registry.cursor(record.cursor))
            .is_some_and(|cursor| !cursor.complete);
        let has_recipe_work = local_logical_frontier_changed
            || self.has_physical_recipe_work(
                group,
                goal,
                recipe_start,
                recipe_cursor,
                dirty_recipes.as_ref(),
            );
        if resume_candidate {
            if !has_recipe_work {
                // The readiness pass already consumed this task's current
                // recipe prefix. A later logical publication will enqueue the
                // task again when it appends a recipe; reopening it here would
                // repeatedly replay the same prefix for every parent recipe.
                self.physical_subproblem_reuses = self.physical_subproblem_reuses.saturating_add(1);
                return Ok(());
            }
            resumed_incomplete = self.task_registry.resume_incomplete(task)?;
        }
        self.task_registry.start(task)?;
        let task_has_residual_work =
            !new_evaluation && (resumed_incomplete || task_has_incomplete_cursor);
        if !resumed_incomplete
            && !task_has_residual_work
            && !has_recipe_work
            && (self
                .memo
                .group(group)
                .and_then(|group| group.winner(goal))
                .is_some()
                || self.infeasible_goals.contains(&(group, goal))
                || self.preserve_incomplete_physical)
        {
            self.physical_subproblem_reuses = self.physical_subproblem_reuses.saturating_add(1);
            let canonical_group = self.memo.canonical_group(group);
            let recipe_count = self
                .next_recipe_sequence
                .get(&(canonical_group, goal))
                .copied()
                .unwrap_or_default();
            let cursor = self.task_registry.advance_cursor(
                task,
                Cursor {
                    position: recipe_count,
                    complete: !self.preserve_incomplete_physical
                        && self.memo.search_obligations().is_empty(),
                },
            )?;
            let outcome = if self
                .memo
                .group(group)
                .and_then(|group| group.winner(goal))
                .is_some()
                || self.preserve_incomplete_physical
            {
                TaskOutcome::Progress { cursor }
            } else {
                TaskOutcome::Infeasible
            };
            self.task_registry
                .complete_current(task, &self.memo, outcome)?;
            return Ok(());
        }
        if !new_evaluation
            && !resumed_incomplete
            && !task_has_residual_work
            && (self
                .memo
                .group(group)
                .and_then(|group| group.winner(goal))
                .is_some()
                || self.infeasible_goals.contains(&(group, goal)))
        {
            self.physical_subproblem_reuses = self.physical_subproblem_reuses.saturating_add(1);
            let canonical_group = self.memo.canonical_group(group);
            let recipe_count = self
                .next_recipe_sequence
                .get(&(canonical_group, goal))
                .copied()
                .unwrap_or_default();
            let cursor = self.task_registry.advance_cursor(
                task,
                Cursor {
                    position: recipe_count,
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
        let result = self.optimize_group_inner(
            group,
            goal,
            recipe_start,
            recipe_cursor,
            enumerate_local_implementations,
            dirty_recipes.as_ref(),
        );
        self.active_goals.remove(&(group, goal));
        match result {
            Ok(()) => {
                let has_winner = self
                    .memo
                    .group(group)
                    .and_then(|group| group.winner(goal))
                    .is_some();
                if !has_winner && !self.preserve_incomplete_physical {
                    self.infeasible_goals.insert((group, goal));
                }
                // A physical task is complete only after its declared search
                // obligations have been discharged.  This preserves the
                // quality contract: a budget-limited pass remains resumable
                // so a later logical publication can re-enter the child
                // frontier and compete with the incumbent.  The restart
                // storm seen with this condition is a task lifecycle bug, not
                // a reason to report an incomplete search as complete.
                let complete =
                    self.memo.search_obligations().is_empty() && !self.preserve_incomplete_physical;
                // The recursive pass may have created new recipes and child
                // frontier dependencies. Rebind the running task to the
                // exact post-child ReadSet before publication; otherwise a
                // later parent could either miss a child change or retain a
                // provisional pre-child snapshot.
                let post_child_reads = self.physical_read_set(group, goal)?;
                self.task_registry
                    .replace_current_read_set(task, &self.memo, post_child_reads)?;
                let recipe_count = self
                    .next_recipe_sequence
                    .get(&(self.memo.canonical_group(group), goal))
                    .copied()
                    .unwrap_or_default();
                let cursor = self.task_registry.advance_cursor(
                    task,
                    Cursor {
                        position: recipe_count,
                        complete,
                    },
                )?;
                let outcome = if has_winner || self.preserve_incomplete_physical {
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
                if !has_winner && !self.preserve_incomplete_physical {
                    self.infeasible_goals.insert((group, goal));
                }
                Ok(())
            }
            Err(error) => {
                let _ = self.task_registry.invalidate(task);
                Err(error)
            }
        }
    }

    fn has_physical_recipe_work(
        &self,
        group: GroupId,
        goal: OptimizationGoal,
        recipe_start: u64,
        recipe_cursor: u64,
        dirty_recipes: Option<&BTreeSet<(PhysicalExprId, Fingerprint)>>,
    ) -> bool {
        let group = self.memo.canonical_group(group);
        let next_sequence = self
            .next_recipe_sequence
            .get(&(group, goal))
            .copied()
            .unwrap_or_default();
        match dirty_recipes {
            Some(dirty) => {
                next_sequence > recipe_cursor
                    || dirty.iter().any(|(physical, fingerprint)| {
                        self.recipes.contains_key(&(*physical, goal, *fingerprint))
                    })
            }
            None => next_sequence > recipe_start,
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

    fn admit_cached_child_combination(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
        physical: PhysicalExprId,
        recipe: &CostRecipe,
        enforced: &super::enforcer::EnforcedPlan,
        state: &mut ChildCombinationState,
        children: &[CandidateId],
        count_recheck: bool,
    ) -> Result<(bool, bool)> {
        let Some(cached) = state.priced.get_mut(children) else {
            return Err(paro_error::internal(
                "priced child combination disappeared before frontier admission",
            ));
        };
        if cached.admission == CombinationAdmission::Published {
            return Ok((false, false));
        }
        if count_recheck {
            self.child_combination_frontier_recheck_count = self
                .child_combination_frontier_recheck_count
                .saturating_add(1);
        }
        let preview = self.memo.candidate_preview(
            group,
            goal,
            CandidateSummary {
                expression: physical,
                cost: cached.cost,
                source_work: cached.source_work.as_ref(),
                physical_fingerprint: cached.physical_fingerprint,
            },
        )?;
        match preview {
            CandidatePreview::Rejected { dominator } => {
                self.memo.record_rejected_winner_proposal(
                    group,
                    goal,
                    cached.physical_fingerprint,
                    false,
                )?;
                cached.admission = CombinationAdmission::FrontierRejected { dominator };
                Ok((false, false))
            }
            CandidatePreview::Truncated => {
                self.memo.record_rejected_winner_proposal(
                    group,
                    goal,
                    cached.physical_fingerprint,
                    true,
                )?;
                cached.admission = CombinationAdmission::FrontierTruncated;
                Ok((false, false))
            }
            CandidatePreview::Publish | CandidatePreview::MustMaterialize => {
                let before = self
                    .memo
                    .group(group)
                    .map(|group| group.physical_frontier_version())
                    .unwrap_or_default();
                let joint_cost_proof =
                    build_joint_cost_proof(&self.memo, group, recipe, cached.local_cost)?;
                let winner = Winner {
                    candidate: CandidateId::INVALID,
                    expression: physical,
                    children: cached.children.clone(),
                    enforcers: enforced.steps.clone(),
                    enforcer_cost_input: recipe.enforcer_cost_input,
                    provided: enforced.provided.clone(),
                    local_cost: cached.local_cost,
                    source_filter_apply_cost: recipe.source_filter_apply_cost,
                    cost_composition: recipe.cost_composition.clone(),
                    cost: cached.cost,
                    source_work: cached.source_work.clone(),
                    physical_fingerprint: cached.physical_fingerprint,
                    joint_cost_proof,
                };
                let published_before = self.memo.published_winner_count();
                let selected_changed = self.memo.record_winner(group, goal, winner)?;
                cached.admission = if self.memo.published_winner_count() > published_before {
                    CombinationAdmission::Published
                } else {
                    CombinationAdmission::FrontierTruncated
                };
                let after = self
                    .memo
                    .group(group)
                    .map(|group| group.physical_frontier_version())
                    .unwrap_or(before);
                Ok((after != before, selected_changed))
            }
        }
    }

    fn optimize_group_inner(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
        recipe_start: u64,
        recipe_cursor: u64,
        enumerate_local_implementations: bool,
        dirty_recipes: Option<&BTreeSet<(PhysicalExprId, Fingerprint)>>,
    ) -> Result<()> {
        if enumerate_local_implementations {
            self.enumerate_implementations(group, goal)?;
        }
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
                        (recipe.sequence, *physical, *fingerprint, Arc::clone(recipe))
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
        let mut source_work_scratch = Vec::<SourceWork>::new();

        for (sequence, physical, recipe_fingerprint, recipe) in recipes {
            let recipe_is_dirty =
                dirty_recipes.is_some_and(|dirty| dirty.contains(&(physical, recipe_fingerprint)));
            if let Some(_) = dirty_recipes {
                if sequence < recipe_cursor && !recipe_is_dirty {
                    continue;
                }
            } else if sequence < recipe_start {
                continue;
            }
            if !self.memo.control().checkpoint()? {
                break;
            }
            child_frontiers.resize_with(recipe.child_goals.len(), Vec::new);
            for frontier in child_frontiers.iter_mut() {
                frontier.clear();
            }
            let mut baseline_child_selections = Vec::with_capacity(recipe.child_goals.len());
            let mut children_feasible = true;
            for ((child, child_goal), frontier_out) in recipe
                .child_goals
                .iter()
                .copied()
                .zip(child_frontiers.iter_mut())
            {
                self.register_physical_dependency(
                    child,
                    child_goal,
                    group,
                    goal,
                    physical,
                    recipe.physical_fingerprint,
                );
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
                if let Some(selected) = frontier.selected() {
                    baseline_child_selections.push(ChildWinnerRef {
                        group: child,
                        goal: child_goal,
                        candidate: selected.candidate,
                    });
                } else {
                    children_feasible = false;
                    break;
                }
                frontier_out.reserve(frontier.candidates().len());
                frontier_out.extend(frontier.candidates().iter().map(|winner| ChildWinnerRef {
                    group: child,
                    goal: child_goal,
                    candidate: winner.candidate,
                }));
                frontier_out.sort_unstable_by_key(|child| child.candidate);
            }
            if !children_feasible {
                continue;
            }
            // Enforcement depends only on the physical expression and the
            // parent requirement.  It is invariant across every child
            // frontier combination; compute it once per recipe instead of
            // cloning properties and rebuilding the baseline for each
            // proposal.
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
            let current_frontier_ids = child_frontiers
                .iter()
                .map(|frontier| {
                    frontier
                        .iter()
                        .map(|child| child.candidate)
                        .collect::<Vec<_>>()
                        .into_boxed_slice()
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            let cost_context =
                child_combination_cost_context_fingerprint(&self.memo, group, goal, &recipe)?;
            let recipe_key = (physical, goal, recipe_fingerprint);
            let parent_frontier_revision = self
                .memo
                .group(group)
                .map(|group| group.physical_frontier_version())
                .unwrap_or_default();
            let mut combination_state = self
                .child_combination_states
                .remove(&recipe_key)
                .unwrap_or_default();
            let context_changed = combination_state.cost_context != Some(cost_context);
            if context_changed {
                self.child_combination_recompute_count =
                    self.child_combination_recompute_count.saturating_add(
                        u64::try_from(
                            combination_state
                                .priced
                                .len()
                                .saturating_add(combination_state.resource_rejected.len()),
                        )
                        .unwrap_or(u64::MAX),
                    );
                combination_state.reset_for_context(
                    current_frontier_ids.clone(),
                    cost_context,
                    parent_frontier_revision,
                    Some(
                        baseline_child_selections
                            .iter()
                            .map(|child| child.candidate)
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                    ),
                );
            } else {
                combination_state.observe_frontiers(current_frontier_ids.clone());
            }

            // A parent frontier revision changes only admission. Recheck the
            // cached cost, never the child composition, so a previously
            // rejected/truncated tuple can become eligible without a second
            // cost synthesis.
            if !context_changed
                && combination_state.parent_frontier_revision != parent_frontier_revision
            {
                let rechecks = combination_state
                    .priced
                    .iter()
                    .filter(|(_, cached)| {
                        matches!(cached.admission, CombinationAdmission::FrontierTruncated)
                            && combination_state.active(&cached.children)
                    })
                    .map(|(children, _)| children.clone())
                    .collect::<Vec<_>>();
                for children in rechecks {
                    let (frontier_changed, selected_changed) = self
                        .admit_cached_child_combination(
                            group,
                            goal,
                            physical,
                            &recipe,
                            &enforced,
                            &mut combination_state,
                            &children,
                            true,
                        )?;
                    if frontier_changed {
                        self.note_physical_candidate(group, goal, selected_changed);
                        self.record_diagnostic_checkpoints();
                    }
                }
            }

            let child_frontier_count = recipe.child_goals.len();
            child_selections.clear();
            child_selections.reserve(child_frontier_count);
            child_costs.clear();
            child_costs.reserve(child_frontier_count);
            child_fingerprints.clear();
            child_fingerprints.reserve(child_frontier_count);
            let mut budget_blocked = false;
            while self.memo.control().checkpoint()? {
                let pending = combination_state
                    .budget_rejected
                    .iter()
                    .find(|children| {
                        child_combination_refs(children, &child_frontiers)
                            .is_ok_and(|children| combination_state.active(&children))
                    })
                    .cloned();
                let (child_ids, mandatory, pending_retry) = if let Some(children) = pending {
                    (children, false, true)
                } else if let Some((children, mandatory)) =
                    combination_state.next_unpriced_domain_tuple()
                {
                    (children, mandatory, false)
                } else {
                    break;
                };
                let Ok(children) = child_combination_refs(&child_ids, &child_frontiers) else {
                    continue;
                };
                if !combination_state.active(&children) {
                    continue;
                }
                if combination_state.resource_rejected.contains(&child_ids) {
                    continue;
                }
                if combination_state.priced.contains_key(&child_ids) {
                    let (frontier_changed, selected_changed) = self
                        .admit_cached_child_combination(
                            group,
                            goal,
                            physical,
                            &recipe,
                            &enforced,
                            &mut combination_state,
                            &child_ids,
                            false,
                        )?;
                    if frontier_changed {
                        self.note_physical_candidate(group, goal, selected_changed);
                        self.record_diagnostic_checkpoints();
                    }
                    continue;
                }
                if combination_state.budget_rejected.contains(&child_ids) {
                    continue;
                }
                if !pending_retry {
                    self.child_combination_new_count =
                        self.child_combination_new_count.saturating_add(1);
                }
                if !mandatory {
                    let event = self.intern_child_combination_event(
                        physical,
                        goal,
                        recipe.physical_fingerprint,
                        &children,
                    )?;
                    let decision = self
                        .memo
                        .group_ledger_mut(group)
                        .ok_or_else(|| paro_error::internal("child-combination owner disappeared"))?
                        .admit_optional(BudgetDimension::ChildFrontierCombination, event);
                    if decision == BudgetDecision::Exhausted {
                        combination_state.budget_rejected.insert(child_ids);
                        self.child_combination_budget_rejection_count = self
                            .child_combination_budget_rejection_count
                            .saturating_add(1);
                        budget_blocked = true;
                        break;
                    }
                }
                if pending_retry {
                    combination_state.budget_rejected.remove(&child_ids);
                }
                child_selections.clear();
                child_selections.extend(children.iter().copied());
                child_costs.clear();
                child_fingerprints.clear();
                source_work_scratch.clear();
                self.child_combination_cost_synthesis_count = self
                    .child_combination_cost_synthesis_count
                    .saturating_add(1);
                let (local_cost, mut cost) = {
                    // Keep the borrowed source-lane list in inline storage;
                    // the cached result below owns only immutable SourceWork
                    // handles, not a duplicate winner tree.
                    let mut child_source_work_refs =
                        SmallVec::<[&[SourceWork]; 8]>::with_capacity(child_frontier_count);
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
                        combination_state
                            .resource_rejected
                            .insert(child_ids.clone());
                        source_work_scratch.clear();
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
                    let composed_cost = compose_candidate_cost_with_sources_at_ref_scratch(
                        local_cost,
                        recipe.source_filter_apply_cost,
                        &child_costs,
                        &child_source_work_refs,
                        &recipe.cost_composition,
                        self.memo.calibration(),
                        &mut source_work_scratch,
                    )?;
                    let Some(cost) = constrain_composed_cost_to_grant(
                        composed_cost,
                        recipe.enforcer_cost_input,
                    )?
                    else {
                        combination_state
                            .resource_rejected
                            .insert(child_ids.clone());
                        source_work_scratch.clear();
                        continue;
                    };
                    (local_cost, cost)
                };
                let Some(constrained_cost) = constrain_composed_cost_to_grant(
                    enforcer_phase.compose_after(cost)?,
                    recipe.enforcer_cost_input,
                )?
                else {
                    combination_state
                        .resource_rejected
                        .insert(child_ids.clone());
                    source_work_scratch.clear();
                    continue;
                };
                cost = constrained_cost;
                let fingerprint = enforced_fingerprint(
                    recipe.physical_fingerprint,
                    &enforced.steps,
                    child_fingerprints.iter().copied(),
                );
                let source_work = std::mem::take(&mut source_work_scratch).into_boxed_slice();
                combination_state.priced.insert(
                    child_ids.clone(),
                    CostedChildCombination {
                        children,
                        local_cost,
                        cost,
                        source_work,
                        physical_fingerprint: fingerprint,
                        admission: CombinationAdmission::Pending,
                    },
                );
                let (frontier_changed, selected_changed) = self.admit_cached_child_combination(
                    group,
                    goal,
                    physical,
                    &recipe,
                    &enforced,
                    &mut combination_state,
                    &child_ids,
                    false,
                )?;
                if frontier_changed {
                    self.note_physical_candidate(group, goal, selected_changed);
                    self.record_diagnostic_checkpoints();
                }
            }
            if budget_blocked {
                tracing::debug!(
                    target: "paro::optimizer",
                    parent_group = group.index(),
                    physical_expression = physical.index(),
                    "child combination pricing paused by budget"
                );
            }
            let current_parent_frontier_revision = self
                .memo
                .group(group)
                .map(|group| group.physical_frontier_version())
                .unwrap_or(parent_frontier_revision);
            if current_parent_frontier_revision != combination_state.parent_frontier_revision {
                let rechecks = combination_state
                    .priced
                    .iter()
                    .filter(|(_, cached)| {
                        matches!(cached.admission, CombinationAdmission::FrontierTruncated)
                            && combination_state.active(&cached.children)
                    })
                    .map(|(children, _)| children.clone())
                    .collect::<Vec<_>>();
                for children in rechecks {
                    let (frontier_changed, selected_changed) = self
                        .admit_cached_child_combination(
                            group,
                            goal,
                            physical,
                            &recipe,
                            &enforced,
                            &mut combination_state,
                            &children,
                            true,
                        )?;
                    if frontier_changed {
                        self.note_physical_candidate(group, goal, selected_changed);
                        self.record_diagnostic_checkpoints();
                    }
                }
            }
            combination_state.parent_frontier_revision = self
                .memo
                .group(group)
                .map(|group| group.physical_frontier_version())
                .unwrap_or(current_parent_frontier_revision);
            self.child_combination_states
                .insert(recipe_key, combination_state);
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

fn physical_local_logical_frontier_changed(
    owner: GroupId,
    previous: Option<&ReadSet>,
    current: &ReadSet,
) -> bool {
    let Some(previous) = previous else {
        return true;
    };
    let Some(previous_owner) = previous.reads().iter().find(|read| read.group == owner) else {
        return true;
    };
    let Some(current_owner) = current.reads().iter().find(|read| read.group == owner) else {
        return true;
    };
    previous_owner.logical_frontier_revision != current_owner.logical_frontier_revision
}

fn physical_changed_child_groups(
    owner: GroupId,
    previous: Option<&ReadSet>,
    current: &ReadSet,
) -> BTreeSet<GroupId> {
    let Some(previous) = previous else {
        return BTreeSet::new();
    };
    previous
        .reads()
        .iter()
        .filter(|read| read.group != owner)
        .filter_map(|previous_read| {
            current
                .reads()
                .iter()
                .find(|read| read.group == previous_read.group)
                .filter(|current_read| *current_read != previous_read)
                .map(|_| previous_read.group)
        })
        .collect()
}

fn physical_read_requires_full_recost(
    owner: GroupId,
    previous: Option<&ReadSet>,
    current: &ReadSet,
) -> bool {
    let Some(previous) = previous else {
        return false;
    };

    let previous_owner = previous.reads().iter().find(|read| read.group == owner);
    let current_owner = current.reads().iter().find(|read| read.group == owner);
    match (previous_owner, current_owner) {
        (Some(previous), Some(current)) => {
            // The owner may append logical/physical alternatives while this
            // task is running. Those frontiers are handled by the recipe
            // cursor; fact and statistics changes alter every composed cost.
            if previous.group != current.group
                || previous.logical_fact_fingerprint != current.logical_fact_fingerprint
                || previous.statistics_snapshot_fingerprint
                    != current.statistics_snapshot_fingerprint
                || previous.logical_frontier_revision.is_some()
                    != current.logical_frontier_revision.is_some()
                || previous.physical_frontier_revision.is_some()
                    != current.physical_frontier_revision.is_some()
            {
                return true;
            }
        }
        _ => return true,
    }

    // For an existing child group, an exact read change is narrowed to the
    // recipes registered against that child by `physical_changed_child_groups`.
    // A missing group is a dependency-shape change and therefore falls back to
    // a complete recost. A group appearing only in the current ReadSet belongs
    // to a newly appended local recipe; the cursor visits it without
    // invalidating the processed prefix.
    for previous_read in previous.reads().iter().filter(|read| read.group != owner) {
        let current_group_count = current
            .reads()
            .iter()
            .filter(|read| read.group == previous_read.group)
            .count();
        if current_group_count != 1 {
            return true;
        }
    }
    false
}

fn pattern_operand_touches_group(
    operand: &PatternOperand,
    secondary: GroupId,
    canonical: GroupId,
) -> bool {
    match operand {
        PatternOperand::Group(group) => *group == secondary || *group == canonical,
        PatternOperand::Expression {
            group, children, ..
        } => {
            *group == secondary
                || *group == canonical
                || children
                    .iter()
                    .any(|child| pattern_operand_touches_group(child, secondary, canonical))
        }
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
    let mut region_id = None;
    for facet in region.facets.iter().copied() {
        let current = memo.regions().region_for_facet(facet).ok_or_else(|| {
            paro_error::internal("physical candidate references an unowned planning facet")
        })?;
        if region_id.is_some_and(|previous| previous != current) {
            return Err(paro_error::internal(
                "physical candidate facets do not share a planning region",
            ));
        }
        region_id = Some(current);
    }
    let region_id = region_id
        .ok_or_else(|| paro_error::internal("physical region candidate has no active facet"))?;
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
        region: region_id,
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

fn stable_region_candidate_key(region: &RegionCandidateContract) -> Box<[Fingerprint]> {
    let mut facets = region.facets.to_vec();
    facets.sort_unstable();
    facets.dedup();
    facets.into_boxed_slice()
}

fn refresh_region_candidate_contract(
    memo: &Memo,
    region: &mut RegionCandidateContract,
) -> Result<()> {
    let mut region_id = None;
    for facet in region.facets.iter().copied() {
        let current = memo.regions().region_for_facet(facet).ok_or_else(|| {
            paro_error::internal("physical candidate references an unowned planning facet")
        })?;
        if region_id.is_some_and(|previous| previous != current) {
            return Err(paro_error::internal(
                "physical candidate facets do not share a planning region",
            ));
        }
        region_id = Some(current);
    }
    region.region = region_id
        .ok_or_else(|| paro_error::internal("physical region candidate has no active facet"))?;
    Ok(())
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

/// Return the lexicographically first CandidateId tuple strictly after the
/// saved tuple.  The cursor is allowed to retain a tuple whose candidate was
/// later pruned; only the exact CandidateId ordering is used to find the next
/// live tuple.  No frontier ordinal participates in identity or progress.
fn next_stable_combination(
    frontiers: &[Box<[CandidateId]>],
    last: Option<&[CandidateId]>,
) -> Option<Box<[CandidateId]>> {
    if frontiers.is_empty() {
        return last
            .is_none()
            .then(|| Vec::<CandidateId>::new().into_boxed_slice());
    }
    if frontiers.iter().any(|frontier| frontier.is_empty()) {
        return None;
    }
    let first = || {
        frontiers
            .iter()
            .map(|frontier| frontier[0])
            .collect::<Vec<_>>()
            .into_boxed_slice()
    };
    let Some(last) = last else {
        return Some(first());
    };
    if last.len() != frontiers.len() {
        return Some(first());
    }

    // Find the rightmost position at which the prefix can remain equal and a
    // strictly larger live CandidateId exists.  Rebuilding the suffix from
    // its first IDs makes this a stable tuple successor even after pruning.
    for pivot in (0..frontiers.len()).rev() {
        if (0..pivot).any(|index| frontiers[index].binary_search(&last[index]).is_err()) {
            continue;
        }
        let position = frontiers[pivot]
            .iter()
            .position(|candidate| *candidate > last[pivot]);
        let Some(position) = position else {
            continue;
        };
        let mut next = last[..pivot].to_vec();
        next.push(frontiers[pivot][position]);
        next.extend(frontiers[pivot + 1..].iter().map(|frontier| frontier[0]));
        return Some(next.into_boxed_slice());
    }
    None
}

fn child_combination_refs(
    children: &[CandidateId],
    frontiers: &[Vec<ChildWinnerRef>],
) -> Result<Box<[ChildWinnerRef]>> {
    if children.len() != frontiers.len() {
        return Err(paro_error::internal(
            "child combination arity disagrees with its recipe",
        ));
    }
    children
        .iter()
        .zip(frontiers)
        .map(|(candidate, frontier)| {
            frontier
                .iter()
                .find(|child| child.candidate == *candidate)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal("child combination references a stale candidate")
                })
        })
        .collect::<Result<Vec<_>>>()
        .map(Vec::into_boxed_slice)
}

fn write_f64_fingerprint(builder: &mut StableFingerprintBuilder, value: f64) {
    builder.write_bytes(&value.to_bits().to_le_bytes());
}

fn write_search_cost_fingerprint(builder: &mut StableFingerprintBuilder, cost: SearchCost) {
    for value in [cost.score.range, cost.work_latency, cost.critical_path] {
        write_f64_fingerprint(builder, value.lower);
        write_f64_fingerprint(builder, value.expected);
        write_f64_fingerprint(builder, value.upper);
    }
    write_f64_fingerprint(builder, cost.score.risk_adjusted);
    for value in cost
        .resources_expected
        .into_iter()
        .chain(cost.resources_risk_upper)
    {
        write_f64_fingerprint(builder, value);
    }
    builder.write_u64(u64::from(cost.max_parallel_tasks));
    builder.write_u64(u64::from(cost.output_pipeline_tasks));
    builder.write_u64(cost.non_revocable_memory_upper);
    builder.write_u64(cost.minimum_memory_bytes);
    builder.write_u64(cost.revocable_memory_target);
    builder.write_u64(cost.peak_memory_upper);
    match cost.memory_completion {
        MemoryCompletion::Guaranteed => builder.write_u64(0),
        MemoryCompletion::RuntimeCapped {
            uncapped_memory_demand,
        } => {
            builder.write_u64(1);
            match uncapped_memory_demand {
                super::cost::UncappedMemoryDemand::KnownBytes(bytes) => {
                    builder.write_u64(0);
                    builder.write_u64(bytes);
                }
                super::cost::UncappedMemoryDemand::Unbounded => builder.write_u64(1),
            }
        }
    }
    builder.write_u64(cost.spill_bytes_expected);
    builder.write_u64(cost.external_workers.0 as u64);
    builder.write_u64(u64::from(cost.external_worker_slots_upper));
}

fn write_cost_composition_fingerprint(
    builder: &mut StableFingerprintBuilder,
    composition: &CostComposition,
) {
    match composition {
        CostComposition::LocalOnly => builder.write_u64(0),
        CostComposition::Source {
            source,
            source_rows,
        } => {
            builder.write_u64(1);
            builder.write_u64(source.0 as u64);
            builder.write_u64(*source_rows);
        }
        CostComposition::Sequential => builder.write_u64(2),
        CostComposition::RetainedState {
            overlapping_children,
        } => {
            builder.write_u64(3);
            builder.write_u64(*overlapping_children);
        }
        CostComposition::SidewaysFilter {
            overlapping_children,
            filtered_child,
            sources,
        } => {
            builder.write_u64(4);
            builder.write_u64(*overlapping_children);
            builder.write_u64(u64::from(*filtered_child));
            builder.write_u64(sources.len() as u64);
            for source in sources {
                builder.write_u64(source.source.0 as u64);
                builder.write_fingerprint(source.domain.0);
                builder.write_fingerprint(source.evaluation.0);
                builder.write_u64(u64::from(source.expected_retained_ppm));
                builder.write_u64(u64::from(source.upper_retained_ppm));
            }
        }
    }
}

fn write_task_supply_fingerprint(
    builder: &mut StableFingerprintBuilder,
    supply: &TaskSupplyContract,
) {
    match supply {
        TaskSupplyContract::Serial => builder.write_u64(0),
        TaskSupplyContract::Source { tasks } => {
            builder.write_u64(1);
            builder.write_u64(u64::from(*tasks));
        }
        TaskSupplyContract::Streaming { input } => {
            builder.write_u64(2);
            builder.write_u64(u64::from(*input));
        }
        TaskSupplyContract::Breaker {
            input,
            output_tasks,
            profile,
        } => {
            builder.write_u64(3);
            builder.write_u64(u64::from(*input));
            builder.write_u64(u64::from(*output_tasks));
            builder.write_u64(match profile {
                ParallelWorkProfile::Serial => 0,
                ParallelWorkProfile::Pipeline => 1,
                ParallelWorkProfile::BlockingMerge => 2,
            });
        }
        TaskSupplyContract::BuildProbe {
            build,
            probe,
            build_work_ppm,
        } => {
            builder.write_u64(4);
            builder.write_u64(u64::from(*build));
            builder.write_u64(u64::from(*probe));
            builder.write_u64(u64::from(*build_work_ppm));
        }
    }
}

fn child_combination_cost_context_fingerprint(
    memo: &Memo,
    owner: GroupId,
    goal: OptimizationGoal,
    recipe: &CostRecipe,
) -> Result<Fingerprint> {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_bytes(b"paro.child-combination-cost-context.v1");
    builder.write_u64(memo.cost_epoch_value());
    builder.write_u64(goal.required.0 as u64);
    builder.write_u64(goal.row_goal.stable_tag());
    builder.write_u64(goal.objective.stable_tag());
    builder.write_u64(goal.grant.stable_tag());
    builder.write_u64(goal.context.0 as u64);
    builder.write_u64(memo.calibration().revision.0 as u64);
    builder.write_fingerprint(recipe.physical_fingerprint);
    write_search_cost_fingerprint(&mut builder, recipe.local_cost);
    if let Some(cost) = recipe.source_filter_apply_cost {
        builder.write_u64(1);
        write_search_cost_fingerprint(&mut builder, cost);
    } else {
        builder.write_u64(0);
    }
    write_task_supply_fingerprint(&mut builder, &recipe.task_supply);
    write_cost_composition_fingerprint(&mut builder, &recipe.cost_composition);
    builder.write_u64(u64::from(recipe.spillable));
    write_f64_fingerprint(&mut builder, recipe.enforcer_cost_input.rows.lower);
    write_f64_fingerprint(&mut builder, recipe.enforcer_cost_input.rows.expected);
    write_f64_fingerprint(&mut builder, recipe.enforcer_cost_input.rows.upper);
    builder.write_u64(recipe.enforcer_cost_input.row_width_bytes);
    builder.write_u64(recipe.enforcer_cost_input.hard_memory_bytes);
    builder.write_u64(match recipe.enforcer_cost_input.spill_policy {
        SpillPolicy::Forbidden => 0,
        SpillPolicy::Allowed => 1,
    });
    builder.write_u64(u64::from(recipe.enforcer_cost_input.max_parallel_tasks));

    let mut groups = BTreeSet::from([memo.canonical_group(owner)]);
    groups.extend(
        recipe
            .child_goals
            .iter()
            .map(|(group, _)| memo.canonical_group(*group)),
    );
    builder.write_u64(groups.len() as u64);
    for group in groups {
        let group_ref = memo
            .group(group)
            .ok_or_else(|| paro_error::internal("cost context references an unknown group"))?;
        builder.write_u64(group.0 as u64);
        builder.write_fingerprint(group_ref.logical_fact_fingerprint());
        builder.write_fingerprint(memo.local_statistics_fingerprint(group));
    }
    Ok(builder.finish())
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnumerationCompletion {
    Complete,
    BudgetLimited {
        first_omitted_ordinal: usize,
        omitted_at_least: usize,
    },
}

#[cfg(test)]
#[derive(Debug)]
struct ChildCombinationBatch<'a> {
    combinations: ChildWinnerCombinations<'a>,
    completion: EnumerationCompletion,
}

/// A lazy product of immutable candidate references, never copies of winner
/// trees/source-work histories. Storage is linear in the input frontier width
/// even if their Cartesian product overflows usize.
#[cfg(test)]
#[derive(Debug)]
struct ChildWinnerCombinations<'a> {
    frontiers: &'a [Vec<ChildWinnerRef>],
    next: usize,
    end: usize,
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
impl ExactSizeIterator for ChildWinnerCombinations<'_> {}

/// Admit only the remaining child-product credit plus a rejection witness.
#[cfg(test)]
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
    let mut source_work = Vec::new();
    let cost = compose_candidate_cost_with_sources_at_ref_scratch(
        local_cost,
        source_filter_apply_cost,
        child_costs,
        child_source_work,
        composition,
        calibration,
        &mut source_work,
    )?;
    Ok(ComposedCost {
        cost,
        source_work: source_work.into_boxed_slice(),
    })
}

/// Compose a candidate into caller-owned source-work scratch.  The engine
/// uses this form while a CandidateSummary is still only a preview: rejected
/// proposals clear the Vec and pay no owned `Box<[SourceWork]>` allocation.
fn compose_candidate_cost_with_sources_at_ref_scratch(
    local_cost: SearchCost,
    source_filter_apply_cost: Option<SearchCost>,
    child_costs: &[SearchCost],
    child_source_work: &[&[SourceWork]],
    composition: &CostComposition,
    calibration: &MachineCalibrationBundle,
    source_work: &mut Vec<SourceWork>,
) -> Result<SearchCost> {
    source_work.clear();
    if child_costs.len() != child_source_work.len() {
        return Err(paro_error::internal(
            "cost composition has no source-work evidence for one or more children",
        ));
    }
    let mut cost = local_cost;
    if matches!(composition, CostComposition::LocalOnly) {
        cost.validate()?;
        return Ok(cost);
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
        source_work.push(
            SourceWorkData {
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
            .into(),
        );
        return Ok(cost);
    }
    let sideways_filter = composition.sideways_filter();
    let source_work_capacity = child_source_work.iter().map(|lanes| lanes.len()).sum();
    source_work.reserve(source_work_capacity);
    for (index, child) in child_costs.iter().copied().enumerate() {
        let mut child = child;
        let mut lanes: Option<Vec<SourceWork>> =
            sideways_filter.and_then(|(filtered_child, sources)| {
                (index == filtered_child
                    && child_source_work[index]
                        .iter()
                        .any(|lane| sources.iter().any(|source| source.source == lane.source)))
                .then(|| child_source_work[index].to_vec())
            });
        if let Some((filtered_child, sources)) = sideways_filter {
            if let Some(lanes) = lanes.as_mut() {
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
                            .filter(|lane| {
                                sources.iter().any(|source| source.source == lane.source)
                            })
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
                        for lane in lanes.iter().filter(|lane| {
                            sources.iter().any(|source| source.source == lane.source)
                        }) {
                            let remaining_lanes = matching_lanes - apply_shares.len();
                            let share = if remaining_lanes == 1 {
                                unallocated_ppm
                            } else if matching_rows > 0 {
                                ((lane.source_rows as f64 / matching_rows as f64 * 1_000_000.0)
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
                        for lane in lanes.iter_mut() {
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
                                let mut retentions =
                                    std::mem::take(&mut updated.retentions).into_vec();
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
            if let Some(lanes) = lanes {
                source_work.extend(lanes);
            } else {
                source_work.extend(child_source_work[index].iter().cloned());
            }
        } else {
            // SourceWork is an immutable Arc-backed snapshot. When no
            // sideways predicate changes a lane, append shallow handles
            // directly to the output buffer instead of allocating a
            // per-child temporary Vec for every Cartesian-product candidate.
            source_work.extend(child_source_work[index].iter().cloned());
        }
        cost = child.sequential(cost)?;
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
    Ok(cost)
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
        builder.write_u64(u64::from(read.physical_frontier_revision.is_some()));
        builder.write_u64(read.physical_frontier_revision.unwrap_or_default());
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
