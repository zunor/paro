// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Deterministic mandatory-baseline plus bounded optional Cascades search.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

// P1-only bounded timing. Owned guards survive error/continue paths without
// borrowing the engine; no per-candidate events or heap allocations.
#[derive(Default)]
struct CostPhaseTimes([std::sync::atomic::AtomicU64; 2]);
struct CostPhaseTimer {
    times: Arc<CostPhaseTimes>,
    phase: usize,
    started: Instant,
}
impl CostPhaseTimer {
    fn start(times: &Option<Arc<CostPhaseTimes>>, phase: usize) -> Option<Self> {
        times.as_ref().map(|times| Self {
            times: Arc::clone(times),
            phase,
            started: Instant::now(),
        })
    }
}
impl Drop for CostPhaseTimer {
    fn drop(&mut self) {
        self.times.0[self.phase].fetch_add(
            self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use smallvec::SmallVec;

use super::bounds::{CertifiedLocalWorkFloor, ProvenChildLatencyFloor, ProvenRecipeLatencyFloor};
use super::budget::{BudgetDecision, BudgetDimension};
use super::calibration::{
    LocalOperatorWork, MachineCalibrationBundle, ParallelWorkProfile, OP_ENFORCER_RANDOM_FETCH,
    OP_ENFORCER_SORT_COMPARE, OP_ENFORCER_SPILL_PAGE, OP_ENFORCER_STREAM_ROW,
};
use super::cost::{CompactRange, MemoryCompletion, ResourceDimension, SearchCost};
use super::enforcer::{replay_enforcer_chain, EnforcementPlanner, EnforcerStep};
use super::governor::{Governor, PlanMilestone, PlanningPolicy};
use super::grant::{derive_grant_sensitivity, verify_grant_sharing, GrantSensitivitySummary};
use super::ids::{
    AdmissibleGrantSetId, CandidateId, Fingerprint, GroupId, ImplementationId, LogicalExprId,
    PhysicalExprId, QualityPolicyId, ResourceGrantClassId, RuleId, StableFingerprintBuilder,
};
use super::memo::{
    CandidatePreview, CandidateSummary, ChildWinnerRef, EquivalenceProof, FrozenCandidate,
    GrantGoalKey, GroupCardinality, LogicalExpr, LogicalProperties, Memo, OptimizationGoal, Winner,
};
use super::properties::{
    MaterializationRequirement, MutationSafetyRequirement, OrderingRequirement, OrderingScope,
    PartitioningRequirement, ReplayabilityRequirement, RepresentationRequirement, ResultGuarantee,
};
use super::quality::{
    PReadyCertificate, QualityBundleRegistry, QualityEvaluationSummary, QualityEvidenceProvider,
    QualityPolicyStatus,
};
use super::region::{
    JointCostProof, RegionArtifactKind, RegionBoundaryEndpoint, RegionCandidateContract,
    RegionDependencyEdge, RegionDependencyKind,
};
#[cfg(test)]
use super::rules::WorkSourceId;
use super::rules::{
    CostComposition, ImplementationContext, ImplementationRegistry, PatternBinding,
    PatternBindingSet, PatternEnumerationCompletion, PatternOperand, PatternRead,
    PhysicalCandidate, RuleContext, SourceFilterWork, SourceRetentionProof, SourceWork,
    SourceWorkData, TaskSupplyContract, TransformContext, TransformationRule,
};
use super::tasks::{
    BoundContext, BoundProofId, BoundProofKind, Cursor, ReadSet, StopReason, TaskId, TaskIntent,
    TaskOutcome, TaskRegistry, TaskRequest, TaskState,
};
use crate::physical::{ObjectiveProfile, ResourceGrantClass, SpillPolicy};

mod quality_production;
use quality_production::QualityProductionRequest;

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
    /// Executable baseline available, but declared optional closure deferred.
    SearchIncomplete,
    Deadline,
    BudgetLimited,
    RuleFailure,
    /// A policy-certified executable candidate was handed to extraction.
    /// This is intentionally not search completeness: Memo obligations may
    /// remain and are still reported to the caller.
    QualityPolicySatisfied,
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

/// An immutable, executable plan snapshot.  This type deliberately contains
/// no model cost or cost-context fingerprint: a plan exported from one Memo
/// is only a seed until its selected DAG is priced for the destination
/// operating point.
#[derive(Debug, Clone)]
pub struct SeedPlan {
    group: GroupId,
    goal: OptimizationGoal,
    frozen: Arc<FrozenCandidate>,
    source_reads: ReadSet,
    identity: Fingerprint,
}

impl SeedPlan {
    pub fn group(&self) -> GroupId {
        self.group
    }

    pub fn goal(&self) -> OptimizationGoal {
        self.goal
    }

    pub fn frozen(&self) -> &Arc<FrozenCandidate> {
        &self.frozen
    }

    pub fn plan_identity(&self) -> Fingerprint {
        self.identity
    }

    pub fn candidate(&self) -> CandidateId {
        self.frozen.reference.candidate
    }
}

/// The exact facts and calibration context under which a plan cost was
/// produced.  ReadSet is the invalidation witness; the fingerprint also
/// covers the goal contract, calibration and grant operating point.  A
/// priced incumbent is never valid merely because its frozen plan is still
/// executable.
#[derive(Debug, Clone)]
pub struct CostContext {
    fingerprint: Fingerprint,
    reads: ReadSet,
}

impl CostContext {
    pub fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    pub fn reads(&self) -> &ReadSet {
        &self.reads
    }
}

/// A SeedPlan plus a cost re-evaluated in one declared destination context.
/// The fields are private so callers cannot pair a winner cost with an
/// unrelated pre-search fingerprint.
#[derive(Debug, Clone)]
pub struct PricedIncumbent {
    target_group: GroupId,
    goal: OptimizationGoal,
    plan: Arc<SeedPlan>,
    cost: SearchCost,
    context: CostContext,
}

#[derive(Debug)]
struct RepricedSeedNode {
    cost: SearchCost,
    source_work: Box<[SourceWork]>,
    physical_fingerprint: Fingerprint,
    reads: Vec<PatternRead>,
}

impl PricedIncumbent {
    pub fn target_group(&self) -> GroupId {
        self.target_group
    }

    pub fn goal(&self) -> OptimizationGoal {
        self.goal
    }

    pub fn plan(&self) -> &Arc<SeedPlan> {
        &self.plan
    }

    pub fn cost(&self) -> SearchCost {
        self.cost
    }

    pub fn context(&self) -> &CostContext {
        &self.context
    }
}

#[derive(Debug, Clone)]
pub struct GrantOptimization {
    pub sensitivity: GrantSensitivitySummary,
    pub winners: Box<[GrantWinner]>,
    pub stop: SearchStop,
    pub grant_search: Option<crate::physical::GrantSearchCoverage>,
    /// Exact mandatory DAGs, retained even if the expected class produces a
    /// better optional variant. Never re-price or re-tag these after reset.
    pub safe_winners: Box<[GrantWinner]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum TaskKind {
    Transform,
    Implement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct TaskKey {
    /// A physical parent demand is an exact causal request for the group's
    /// quality alternatives. Keep those tasks ahead of the broader quality
    /// lane without changing the latter's established ordering.
    demand_stage: u8,
    /// A promoted task is one whose observed dependency was just published.
    /// Keep that readiness lane before the ordinary promise so the direct
    /// successor can run promptly, but enqueue the initial agenda on the
    /// sentinel lane. This is a local dependency edge, not a global quality
    /// priority: unrelated groups retain the ordinary scheduler order.
    quality_stage: u8,
    priority: u16,
    kind: TaskKind,
    stable_id: u32,
    group: GroupId,
    expression: LogicalExprId,
    goal: Option<OptimizationGoal>,
}

/// Return the transformation proofs carried by one selected logical
/// expression.  This is deliberately derived from the selected expression's
/// proof set, not from `LogicalExpr::applied_rules`, which is an audit trail of
/// attempted work and may include rejected or unused applications.
pub(crate) fn selected_proof_rule_ids(logical: &LogicalExpr) -> Box<[RuleId]> {
    logical
        .proofs
        .iter()
        .filter_map(|proof| match proof {
            EquivalenceProof::Transformation { rule, .. }
            | EquivalenceProof::TransformationDescendant { rule }
            | EquivalenceProof::SpecializedEnumerator { rule, .. } => Some(*rule),
            EquivalenceProof::Initial | EquivalenceProof::Normalization { .. } => None,
        })
        .collect()
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
    /// `Some` identifies a direct quality-lane binding.  Keeping it outside
    /// `SearchTask` lets the ordinary task remain queued while one exact
    /// selected path is run first; the two observations and applications are
    /// still independent and cannot suppress the complete matcher later.
    binding: Option<Fingerprint>,
}

#[derive(Debug, Clone, Copy)]
enum TransformationTaskLifecyclePhase {
    Enqueued,
    FirstRun,
    DependenciesReady,
    Matched,
    NoMatch,
    Applicable,
    Published,
    NoOutput,
    BudgetRejected,
}

/// Diagnostic-only exact lifecycle for one quality-producing transformation
/// task.  Family-level first-run timestamps cannot tell whether a late
/// candidate waited in the agenda or waited for a particular observed
/// frontier.  This record is bounded and emitted only when the diagnostic
/// rule profile is enabled; normal trace-off searches keep no task entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformationTaskLifecycle {
    pub group: GroupId,
    pub expression: LogicalExprId,
    pub rule: RuleId,
    pub first_enqueued_us: Option<u64>,
    pub first_run_us: Option<u64>,
    pub first_dependencies_ready_us: Option<u64>,
    pub first_matched_us: Option<u64>,
    pub first_no_match_us: Option<u64>,
    pub first_applicable_us: Option<u64>,
    pub first_published_us: Option<u64>,
    pub first_no_output_us: Option<u64>,
    pub first_budget_rejected_us: Option<u64>,
    /// The most recent run/publication are needed when one exact task is
    /// reactivated by a changing Memo fact.  First-* alone would classify a
    /// late alternative as an early task that happened to publish once.
    pub last_enqueued_us: Option<u64>,
    pub last_dependencies_ready_us: Option<u64>,
    pub last_run_us: Option<u64>,
    pub last_published_us: Option<u64>,
    pub first_binding: Option<Fingerprint>,
    pub match_count: u64,
    pub no_match_count: u64,
    pub applicable_count: u64,
    pub no_output_count: u64,
    pub published_count: u64,
    pub budget_rejected_count: u64,
    pub last_reads: Box<[PatternRead]>,
}

impl TransformationTaskLifecycle {
    fn new(task: TransformationTaskId) -> Self {
        Self {
            group: task.group,
            expression: task.expression,
            rule: task.rule,
            first_enqueued_us: None,
            first_run_us: None,
            first_dependencies_ready_us: None,
            first_matched_us: None,
            first_no_match_us: None,
            first_applicable_us: None,
            first_published_us: None,
            first_no_output_us: None,
            first_budget_rejected_us: None,
            last_enqueued_us: None,
            last_dependencies_ready_us: None,
            last_run_us: None,
            last_published_us: None,
            first_binding: None,
            match_count: 0,
            no_match_count: 0,
            applicable_count: 0,
            no_output_count: 0,
            published_count: 0,
            budget_rejected_count: 0,
            last_reads: Box::new([]),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
    /// One pending key per exact task identity. A child publication may make
    /// an already queued dependent task ready for the promoted lane; replace
    /// its old key instead of paying a duplicate discovery visit.
    keys: BTreeMap<SearchTask, TaskKey>,
}

impl StableAgenda {
    /// Insert or promote one exact task.  Returning whether the agenda
    /// changed lets lifecycle diagnostics distinguish a real queue event from
    /// a notification which found an already-pending task.  Callers which do
    /// not need that distinction may continue to ignore the result.
    fn push(&mut self, key: TaskKey, task: SearchTask) -> bool {
        if let Some(previous) = self.keys.get(&task).copied() {
            if previous <= key {
                return false;
            }
            self.tasks.remove(&previous);
        }
        self.keys.insert(task, key);
        self.tasks.insert(key, task);
        true
    }

    fn pop(&mut self) -> Option<SearchTask> {
        let (_, task) = self.tasks.pop_first()?;
        self.keys.remove(&task);
        Some(task)
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

#[derive(Debug)]
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
    /// A proof-backed floor for the recipe's own work. Unknown/statistical
    /// ranges remain `None`; the floor never stands in for child or logical
    /// search completeness.
    certified_local_work: Option<CertifiedLocalWorkFloor>,
    /// These fields are immutable after recipe publication. Facts, statistics,
    /// grant and calibration remain separate live inputs to combination identity;
    /// resuming a recipe does not hash its unchanged source-work payload again.
    immutable_cost_identity: OnceLock<Fingerprint>,
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
    /// Guard witnesses for rejected bindings; not mutually exclusive.
    pub rejection_guards: crate::transformation_rejection::TransformationRejectionCounts,
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
    /// Diagnostic-only lifecycle offsets for the dependency scheduler.
    pub first_enqueued_us: Option<u64>,
    pub first_dependencies_ready_us: Option<u64>,
    pub first_run_us: Option<u64>,
    /// Number of times a rule's exact native proof was consumed by a frozen
    /// root candidate.  `applied_rules` is intentionally not used here.
    pub root_consumed: u64,
    /// Diagnostic-only elapsed offsets from the start of the optimizer call.
    /// They are optional because the normal trace-off path does not maintain
    /// a timing clock or phase map.
    pub first_discovered_us: Option<u64>,
    pub first_matched_us: Option<u64>,
    pub first_applicable_us: Option<u64>,
    pub first_published_us: Option<u64>,
}

/// Exact selected-DAG evidence captured at a diagnostic search checkpoint.
/// Candidate IDs alone are not sufficient because they do not identify the
/// child choices, payloads, or rule products that made a plan executable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenChoice {
    pub reference: ChildWinnerRef,
    pub logical: LogicalExprId,
    pub physical: PhysicalExprId,
    pub logical_payload: u32,
    pub physical_payload: u32,
    pub physical_fingerprint: Fingerprint,
    pub children: Box<[ChildWinnerRef]>,
    /// Audit-only rules which were attempted in the selected expression's
    /// Memo group.  This field is retained for backwards-compatible trace
    /// shape; it is never a quality proof.
    pub rules: Box<[RuleId]>,
    /// Proof-bearing rules attached to this exact selected logical
    /// expression.  These rules are the only rule attribution eligible for a
    /// quality certificate.
    pub selected_rules: Box<[RuleId]>,
}

const SEARCH_CHECKPOINT_TARGETS_MS: [u64; 5] = [5, 10, 20, 50, 100];
const MAX_CANDIDATE_LIFECYCLE_EVENTS: usize = 2_048;
// Independent bounded prefixes reserve room for late logical/root evidence.
// A dense publication stage cannot evict another stage's recorded history.
const CANDIDATE_LIFECYCLE_STAGE_LIMITS: [u64; 6] = [512, 256, 128, 128, 992, 32];
const MAX_TRANSFORMATION_TASK_LIFECYCLES: usize = 1_024;

/// A diagnostic-only snapshot of the currently selected root candidate at a
/// fixed search-time checkpoint. The timestamp is the first observation at or
/// after the target, so a long indivisible costing step is visible rather
/// than being presented as an exact stop point. A candidate is executable at
/// the Memo boundary, while `search_complete` separately records whether the
/// overall optional closure has finished.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchCheckpoint {
    pub target_ms: u64,
    pub observed_us: u64,
    pub goal: OptimizationGoal,
    pub candidate: Option<CandidateId>,
    pub expected_cost: Option<f64>,
    pub risk_adjusted_cost: Option<f64>,
    pub upper_cost: Option<f64>,
    pub choices: Box<[FrozenChoice]>,
    pub fact_reads: Box<[PatternRead]>,
    pub frozen: bool,
    pub search_complete: bool,
}

/// Diagnostic-only identity for one point in the real candidate production
/// chain.  The event is intentionally attached to the existing search
/// milestone snapshot instead of a second trace or replay store.  In
/// particular, a tuple-priced event has no parent CandidateId yet; its exact
/// child references are the identity that the later Memo admission consumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum CandidateLifecycleStage {
    LogicalPublished = 0,
    PhysicalRecipePublished = 1,
    ChildReady = 2,
    TuplePriced = 3,
    ParentPublished = 4,
    RootQualified = 5,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateLifecycleEvent {
    pub stage: CandidateLifecycleStage,
    pub elapsed_us: u64,
    pub group: GroupId,
    pub goal: Option<OptimizationGoal>,
    pub candidate: Option<CandidateId>,
    /// Source logical expression whose transformation produced `logical`.
    /// This is only populated for logical-publication events; it lets the
    /// diagnostic timeline follow the exact same-Memo dependency edge rather
    /// than attributing a late output to a rule family alone.
    pub source: Option<LogicalExprId>,
    /// Stable identity of the exact pattern binding consumed by the rule.
    /// This distinguishes multiple child-shell matches of one source
    /// expression without exposing a mutable frontier ordinal.
    pub binding: Option<Fingerprint>,
    /// Direct child expression selected by the binding, when the matched
    /// shell has one.  It is diagnostic provenance only; the Memo remains the
    /// source of truth for the candidate's actual choices.
    pub source_child: Option<LogicalExprId>,
    pub logical: Option<LogicalExprId>,
    pub physical: Option<PhysicalExprId>,
    pub recipe: Option<Fingerprint>,
    pub rule: Option<RuleId>,
    pub children: Box<[ChildWinnerRef]>,
    /// Facts are retained only when the producer already has a concrete
    /// fact-read set (logical publication/root qualification). Physical
    /// events use the exact child refs and recipe identity; their complete
    /// fact contract is replayed by the existing FrozenCandidate trace.
    pub facts: Box<[PatternRead]>,
    pub expected_cost_bits: Option<u64>,
    pub upper_cost_bits: Option<u64>,
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
    /// A quality policy can hand off an executable candidate while the Memo
    /// still has unexplored obligations. This is separate from ProofComplete.
    pub quality_policy_satisfied_us: Option<u64>,
    pub quality_policy_candidate: Option<CandidateId>,
    /// Bounded diagnostic timeline for the actual same-Memo production path.
    /// Normal trace-off searches keep this empty and allocation-free.
    pub candidate_lifecycle: Vec<CandidateLifecycleEvent>,
    pub candidate_lifecycle_dropped: u64,
    /// Per-stage accounting makes a bounded diagnostic cohort auditable: a
    /// dense child-ready stream must not evict the physical publications
    /// needed to locate a late producer/parent edge.
    pub candidate_lifecycle_stage_stored: [u64; 6],
    pub candidate_lifecycle_stage_dropped: [u64; 6],
    /// Exact quality-rule task lifecycle, bounded separately from the denser
    /// candidate timeline.  The task identity and reads make a late logical
    /// publication attributable to queueing versus an advancing child
    /// frontier without turning normal C1 into a tracing run.
    pub transformation_task_lifecycle: Vec<TransformationTaskLifecycle>,
    pub transformation_task_lifecycle_dropped: u64,
}

/// The last published physical response for one exact `(group, goal)` task.
/// TaskRegistry remains the source of lifecycle/audit truth; this is only a
/// read-only fast path for the overwhelmingly common recursive request that
/// arrives before any observed input or local recipe has changed.
#[derive(Debug, Clone)]
struct PhysicalTaskCacheEntry {
    reads: ReadSet,
    recipe_cursor: u64,
    /// Readiness passes may cache an incomplete prefix so an unchanged queue
    /// wake-up does not re-enter TaskRegistry.  A normal completion pass may
    /// use the same entry only when this bit is set; an incomplete prefix
    /// must remain resumable by the full search.
    complete: bool,
}

#[derive(Debug, Clone, Copy)]
struct PhysicalCompletionProof {
    proof: BoundProofId,
    domain: Fingerprint,
    candidate: CandidateId,
    threshold: u64,
}

/// Diagnostic attribution for certified-bound attempts.  These counters are
/// deliberately separate from candidate/pruning counts: a bound can be
/// unavailable because its evidence is incomplete, or available but too weak
/// to exclude a recipe.  Durations are measured on the single search worker;
/// they are CPU-wall proxies for this diagnostic cohort and are never used as
/// a correctness condition.
#[derive(Debug, Default, Clone, Copy)]
struct CertifiedBoundDiagnostics {
    no_incumbent: u64,
    local_interval_uncertain: u64,
    child_completion_missing: u64,
    source_response_unsupported: u64,
    phase_overlap_unsupported: u64,
    available_not_tight: u64,
    pruned_before_children: u64,
    pruned_after_children: u64,
    compute_us: u64,
    validation_us: u64,
    invalidation_count: u64,
}

impl CertifiedBoundDiagnostics {
    fn add_elapsed(target: &mut u64, started: Instant) {
        *target =
            target.saturating_add(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
}

/// A mandatory winner retained across a cost-epoch reset.  The winner is an
/// immutable executable upper bound; the fact reads are the validity witness
/// for the cost context that produced it.  We deliberately retain only the
/// winner DAG's group facts, not a second Memo or a copied physical frontier.
#[derive(Debug, Clone)]
struct ProtectedIncumbent {
    winner: Arc<Winner>,
    reads: ReadSet,
}

#[derive(Debug, Clone, Copy)]
enum RuleWorkPhase {
    Enqueued,
    DependenciesReady,
    FirstRun,
    Discovered,
    Matched,
    Applicable,
    Published,
}

#[derive(Debug, Clone, Copy)]
enum BoundCheckLocation {
    BeforeChildren,
    AfterChildren,
}

/// The engine is deliberately operator-agnostic. Domain implementations live
/// in the registry; this type owns stable scheduling, budgets, enforcement,
/// recursive goal optimization, and winner verification.
#[derive(Debug)]
pub struct CascadesEngine {
    active_optional_grant: Option<ResourceGrantClass>,
    engine_created_at: Instant,
    mandatory_only: bool,
    preserve_incomplete_physical: bool,
    /// The readiness queue may ask a physical task to yield after publishing
    /// one frontier delta.  This is a continuation mode, not a smaller
    /// search budget: the exact recipe/child cursor is published back to the
    /// TaskRegistry and the same task is queued again by the interleave.
    physical_interleave_step_mode: bool,
    physical_interleave_step_yielded: bool,
    physical_interleave_step_publications: u16,
    memo: Memo,
    registry: ImplementationRegistry,
    enforcement: EnforcementPlanner,
    recipes: BTreeMap<(PhysicalExprId, OptimizationGoal, Fingerprint), Arc<CostRecipe>>,
    infeasible_goals: BTreeSet<(GroupId, OptimizationGoal)>,
    /// Mandatory winners survive a cost-epoch reset in the immutable winner
    /// archive, but their frontier is intentionally cleared. Keep this small
    /// exact map so optional recipe bounds can compare against the protected
    /// incumbent without copying or rehydrating a Memo frontier.
    protected_incumbents: BTreeMap<(GroupId, OptimizationGoal), ProtectedIncumbent>,
    /// Immutable upper bounds supplied by a separate search/Memo.  Unlike
    /// `protected_incumbents`, these are never produced by the destination
    /// search epoch and therefore isolate incumbent quality from proof cost.
    strong_incumbents: BTreeMap<(GroupId, OptimizationGoal), PricedIncumbent>,
    /// Diagnostic switch used to establish whether certified pruning is
    /// blocked by the quality of the available incumbent.  The default keeps
    /// the verified mandatory candidate; disabling it never changes the
    /// returned fallback, only the optional proof experiment.
    protected_incumbent_enabled: bool,
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
    /// Exact selected-path bindings requested by the same-Memo quality lane.
    /// The goal is part of the key because grants can require different
    /// quality candidates; no binding is treated as complete search.
    quality_forced_transform_bindings:
        BTreeMap<(OptimizationGoal, TransformationTaskId), VecDeque<PatternBinding>>,
    quality_active_forced_transform_binding: Option<(TransformationTaskId, PatternBinding)>,
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
    /// Child groups observed by each physical subproblem.  Recipes are
    /// published incrementally, so maintaining this small deduplicated index
    /// at publication avoids rescanning the global recipe table every time a
    /// recursive task captures its exact child ReadSet.
    physical_read_dependencies:
        BTreeMap<(GroupId, OptimizationGoal), BTreeSet<(GroupId, OptimizationGoal)>>,
    physical_task_cache: BTreeMap<(GroupId, OptimizationGoal), PhysicalTaskCacheEntry>,
    /// A proof is retained only for the exact current physical domain. The
    /// TaskRegistry owns its lifecycle; this index avoids scanning all bound
    /// records when a parent asks whether a child may provide a lower bound.
    physical_completion_proofs: BTreeMap<(GroupId, OptimizationGoal), PhysicalCompletionProof>,
    /// Certified pruning is an explicit experimental policy.  The production
    /// path can execute the same proof protocol without making diagnostic
    /// tracing part of normal C1; callers opt in only after model/oracle
    /// admission.
    certified_group_pruning_enabled: bool,
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
    /// Physical child groups observed by the mandatory baseline.  The
    /// optional quality lane may promote only these proven demand edges; it
    /// must not walk every logical group merely because it is reachable from
    /// the root. New optional physical requests append to the same set.
    physical_quality_demanded_groups: BTreeSet<GroupId>,
    /// Prevent repeated physical drains from rescanning a demanded group's
    /// existing expressions. New expressions still use the ordinary local
    /// publication wake-up path.
    physical_quality_scheduled_groups: BTreeSet<GroupId>,
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
    diagnostic_cost_phase_times: Option<Arc<CostPhaseTimes>>,
    child_combination_frontier_recheck_count: u64,
    child_combination_budget_rejection_count: u64,
    certified_bound_check_count: u64,
    certified_recipe_prune_count: u64,
    certified_bound_diagnostics: CertifiedBoundDiagnostics,
    strong_incumbent_lookup_count: u64,
    strong_incumbent_bound_request_count: u64,
    strong_incumbent_valid_lookup_count: u64,
    strong_incumbent_invalid_lookup_count: u64,
    strong_incumbent_missing_key_count: u64,
    strong_incumbent_lookup_group_mismatch_count: u64,
    strong_incumbent_lookup_goal_mismatch_count: u64,
    strong_incumbent_lookup_unrelated_key_count: u64,
    strong_incumbent_fact_invalid_count: u64,
    strong_incumbent_context_invalid_count: u64,
    strong_incumbent_source_response_bypass_count: u64,
    strong_incumbent_phase_overlap_bypass_count: u64,
    strong_incumbent_selected_for_bound_count: u64,
    strong_incumbent_install_count: u64,
    strong_incumbent_installed_read_count: u64,
    strong_incumbent_installed_cost_expected_bits: Option<u64>,
    strong_incumbent_installed_cost_upper_bits: Option<u64>,
    strong_incumbent_merge_invalidation_count: u64,
    strong_incumbent_goal_mismatch_count: u64,
    strong_incumbent_reprice_rejection_count: u64,
    strong_incumbent_reprice_no_destination_recipe_count: u64,
    strong_incumbent_reprice_child_dag_count: u64,
    strong_incumbent_reprice_fingerprint_count: u64,
    strong_incumbent_reprice_property_count: u64,
    strong_incumbent_reprice_other_count: u64,
    strong_incumbent_first_reprice_failure_reason: Option<u64>,
    strong_incumbent_installed_at_us: Option<u64>,
    strong_incumbent_installed_context_fingerprint: Option<Fingerprint>,
    strong_incumbent_first_invalidation_at_us: Option<u64>,
    strong_incumbent_first_invalidation_reason: Option<u64>,
    strong_incumbent_first_invalidated_context_fingerprint: Option<Fingerprint>,
    next_recipe_sequence: BTreeMap<(GroupId, OptimizationGoal), u64>,
    /// Shared task identity/progress protocol.  Memo remains the owner of
    /// expressions, candidates and facts; this registry only coordinates
    /// resumable work and publication state.
    task_registry: TaskRegistry,
    governor: Governor,
    quality_bundles: QualityBundleRegistry,
    /// Opt-in production handoff. The default path never evaluates quality
    /// packages, so trace-off normal C1 pays no policy-discovery cost.
    quality_handoff_enabled: bool,
    quality_evidence_provider: Option<Arc<dyn QualityEvidenceProvider>>,
    quality_required_goals: BTreeSet<OptimizationGoal>,
    quality_ready_winners: BTreeMap<OptimizationGoal, GrantWinner>,
    quality_certificates: BTreeMap<OptimizationGoal, PReadyCertificate>,
    quality_handoff_reached: bool,
    quality_production_requests: BTreeMap<OptimizationGoal, QualityProductionRequest>,
    quality_last_production_obligation: Option<(super::quality::BundleFact, GroupId)>,
    quality_producer_dispatch_count: u64,
    quality_direct_binding_dispatch_count: u64,
    quality_direct_binding_first_us: Option<u64>,
    quality_direct_binding_last_us: Option<u64>,
    quality_direct_binding_work_units: u64,
    quality_candidate_evaluation_count: u64,
    quality_candidate_missing_evidence_count: u64,
    /// Number of root-frontier entries inspected by the quality policy. A
    /// quality handoff is allowed to select a published, non-leading frontier
    /// entry when it is the first exact candidate whose native contract is
    /// complete; the model-cost winner is not automatically a quality winner.
    quality_frontier_candidate_count: u64,
    quality_frontier_candidate_skip_count: u64,
    quality_frontier_certified_count: u64,
    quality_frontier_policy_rejection_count: u64,
    quality_frontier_fact_signatures: BTreeMap<u16, u64>,
    quality_evaluated_candidates:
        BTreeSet<(OptimizationGoal, CandidateId, super::tasks::ReadSetId)>,
    quality_frontier_max_aggregates: u32,
    quality_frontier_max_runtime_filters: u32,
    quality_frontier_max_aggregate_regions: u32,
    quality_frontier_max_covered_aggregate_regions: u32,
    quality_frontier_first_aggregate_region_us: Option<u64>,
    quality_frontier_first_aggregate_candidate: Option<CandidateId>,
    quality_frontier_first_incomplete_aggregate_region_us: Option<u64>,
    quality_frontier_first_incomplete_aggregate_candidate: Option<CandidateId>,
    quality_frontier_first_incomplete_aggregate_region: Option<Fingerprint>,
    quality_frontier_first_incomplete_aggregate_anchor: Option<CandidateId>,
    quality_frontier_first_incomplete_aggregate_covered: u32,
    quality_frontier_first_incomplete_aggregate_total: u32,
    quality_frontier_first_complete_aggregate_region_us: Option<u64>,
    quality_frontier_first_complete_aggregate_candidate: Option<CandidateId>,
    quality_frontier_max_aggregate_witnesses: u32,
    quality_frontier_max_join_witnesses: u32,
    quality_certified_max_aggregates: u32,
    quality_certified_max_runtime_filters: u32,
    quality_certified_max_aggregate_regions: u32,
    quality_certified_max_covered_aggregate_regions: u32,
    quality_certified_max_aggregate_witnesses: u32,
    quality_certified_max_join_witnesses: u32,
    quality_last_evaluation: QualityEvaluationSummary,
    /// The candidate/goal associated with the most recent quality attempt.
    /// A bundle summary without this identity cannot explain why a later
    /// frozen frontier entry was or was not PReady.
    quality_last_evaluation_candidate: Option<CandidateId>,
    quality_last_evaluation_goal: Option<OptimizationGoal>,
}

impl CascadesEngine {
    pub fn new(memo: Memo, registry: ImplementationRegistry) -> Self {
        let budget = memo.budget().clone();
        let mut quality_bundles = QualityBundleRegistry::default();
        quality_bundles
            .register_builtin_f1_f4()
            .expect("built-in quality bundles must have unique identities");
        Self {
            active_optional_grant: None,
            engine_created_at: Instant::now(),
            mandatory_only: false,
            preserve_incomplete_physical: false,
            physical_interleave_step_mode: false,
            physical_interleave_step_yielded: false,
            physical_interleave_step_publications: 0,
            memo,
            registry,
            enforcement: EnforcementPlanner::new(
                budget.max_optional_enforcer_depth,
                budget.max_optional_enforcer_chains_per_goal,
            ),
            recipes: BTreeMap::new(),
            infeasible_goals: BTreeSet::new(),
            protected_incumbents: BTreeMap::new(),
            strong_incumbents: BTreeMap::new(),
            protected_incumbent_enabled: true,
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
            quality_forced_transform_bindings: BTreeMap::new(),
            quality_active_forced_transform_binding: None,
            transformation_subscribers: BTreeMap::new(),
            region_candidates: BTreeMap::new(),
            physical_parents: BTreeMap::new(),
            physical_read_dependencies: BTreeMap::new(),
            physical_task_cache: BTreeMap::new(),
            physical_completion_proofs: BTreeMap::new(),
            certified_group_pruning_enabled: false,
            physical_dirty_recipes: BTreeMap::new(),
            physical_full_recost: BTreeSet::new(),
            physical_goals: BTreeMap::new(),
            physical_quality_demanded_groups: BTreeSet::new(),
            physical_quality_scheduled_groups: BTreeSet::new(),
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
            diagnostic_cost_phase_times: (std::env::var_os("PARO_DIAGNOSTIC_COST_PHASE_TIMES")
                .as_deref() == Some(std::ffi::OsStr::new("1")))
            .then(|| Arc::new(CostPhaseTimes::default())),
            child_combination_frontier_recheck_count: 0,
            child_combination_budget_rejection_count: 0,
            certified_bound_check_count: 0,
            certified_recipe_prune_count: 0,
            certified_bound_diagnostics: CertifiedBoundDiagnostics::default(),
            strong_incumbent_lookup_count: 0,
            strong_incumbent_bound_request_count: 0,
            strong_incumbent_valid_lookup_count: 0,
            strong_incumbent_invalid_lookup_count: 0,
            strong_incumbent_missing_key_count: 0,
            strong_incumbent_lookup_group_mismatch_count: 0,
            strong_incumbent_lookup_goal_mismatch_count: 0,
            strong_incumbent_lookup_unrelated_key_count: 0,
            strong_incumbent_fact_invalid_count: 0,
            strong_incumbent_context_invalid_count: 0,
            strong_incumbent_source_response_bypass_count: 0,
            strong_incumbent_phase_overlap_bypass_count: 0,
            strong_incumbent_selected_for_bound_count: 0,
            strong_incumbent_install_count: 0,
            strong_incumbent_installed_read_count: 0,
            strong_incumbent_installed_cost_expected_bits: None,
            strong_incumbent_installed_cost_upper_bits: None,
            strong_incumbent_merge_invalidation_count: 0,
            strong_incumbent_goal_mismatch_count: 0,
            strong_incumbent_reprice_rejection_count: 0,
            strong_incumbent_reprice_no_destination_recipe_count: 0,
            strong_incumbent_reprice_child_dag_count: 0,
            strong_incumbent_reprice_fingerprint_count: 0,
            strong_incumbent_reprice_property_count: 0,
            strong_incumbent_reprice_other_count: 0,
            strong_incumbent_first_reprice_failure_reason: None,
            strong_incumbent_installed_at_us: None,
            strong_incumbent_installed_context_fingerprint: None,
            strong_incumbent_first_invalidation_at_us: None,
            strong_incumbent_first_invalidation_reason: None,
            strong_incumbent_first_invalidated_context_fingerprint: None,
            next_recipe_sequence: BTreeMap::new(),
            task_registry: TaskRegistry::default(),
            governor: Governor::new(PlanningPolicy::default())
                .expect("default planning policy must be valid"),
            quality_bundles,
            quality_handoff_enabled: false,
            quality_evidence_provider: None,
            quality_required_goals: BTreeSet::new(),
            quality_ready_winners: BTreeMap::new(),
            quality_certificates: BTreeMap::new(),
            quality_handoff_reached: false,
            quality_production_requests: BTreeMap::new(),
            quality_last_production_obligation: None,
            quality_producer_dispatch_count: 0,
            quality_direct_binding_dispatch_count: 0,
            quality_direct_binding_first_us: None,
            quality_direct_binding_last_us: None,
            quality_direct_binding_work_units: 0,
            quality_candidate_evaluation_count: 0,
            quality_candidate_missing_evidence_count: 0,
            quality_frontier_candidate_count: 0,
            quality_frontier_candidate_skip_count: 0,
            quality_frontier_certified_count: 0,
            quality_frontier_policy_rejection_count: 0,
            quality_frontier_fact_signatures: BTreeMap::new(),
            quality_evaluated_candidates: BTreeSet::new(),
            quality_frontier_max_aggregates: 0,
            quality_frontier_max_runtime_filters: 0,
            quality_frontier_max_aggregate_regions: 0,
            quality_frontier_max_covered_aggregate_regions: 0,
            quality_frontier_first_aggregate_region_us: None,
            quality_frontier_first_aggregate_candidate: None,
            quality_frontier_first_incomplete_aggregate_region_us: None,
            quality_frontier_first_incomplete_aggregate_candidate: None,
            quality_frontier_first_incomplete_aggregate_region: None,
            quality_frontier_first_incomplete_aggregate_anchor: None,
            quality_frontier_first_incomplete_aggregate_covered: 0,
            quality_frontier_first_incomplete_aggregate_total: 0,
            quality_frontier_first_complete_aggregate_region_us: None,
            quality_frontier_first_complete_aggregate_candidate: None,
            quality_frontier_max_aggregate_witnesses: 0,
            quality_frontier_max_join_witnesses: 0,
            quality_certified_max_aggregates: 0,
            quality_certified_max_runtime_filters: 0,
            quality_certified_max_aggregate_regions: 0,
            quality_certified_max_covered_aggregate_regions: 0,
            quality_certified_max_aggregate_witnesses: 0,
            quality_certified_max_join_witnesses: 0,
            quality_last_evaluation: QualityEvaluationSummary::default(),
            quality_last_evaluation_candidate: None,
            quality_last_evaluation_goal: None,
        }
    }

    pub fn memo(&self) -> &Memo {
        &self.memo
    }

    pub fn memo_mut(&mut self) -> &mut Memo {
        &mut self.memo
    }

    /// Prime the exact grant operating points before a SeedPlan is re-priced.
    /// The normal grant-search entry repeats this validation and publication;
    /// this small pre-search hook only makes the same context available to
    /// destination seed pricing.
    pub(crate) fn prime_grant_context(
        &mut self,
        classes: impl IntoIterator<Item = ResourceGrantClass>,
    ) -> Result<()> {
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
        if class_map.is_empty() {
            return Err(paro_error::internal(
                "grant portfolio optimization requires at least one class",
            ));
        }
        self.grant_classes = class_map;
        self.grant_class_sets = self
            .grant_classes
            .keys()
            .copied()
            .map(|class| (class, AdmissibleGrantSetId(0)))
            .collect();
        Ok(())
    }

    /// Re-price one immutable seed for the exact grant goals that the real
    /// portfolio entry will request.  A source Memo may classify grants
    /// differently after the destination has rebuilt its logical shell, so
    /// reusing the source goal as a map key would silently make the upper
    /// bound unreachable.  This helper is intentionally on the same engine
    /// path as `optimize_for_grants`; it does not create a second search or
    /// grant scheduler.
    pub(crate) fn reprice_strong_incumbents_for_grants(
        &mut self,
        root: GroupId,
        base_goal: OptimizationGoal,
        admissible_set: AdmissibleGrantSetId,
        classes: impl IntoIterator<Item = ResourceGrantClass>,
        plans: &[SeedPlan],
    ) -> Result<Vec<PricedIncumbent>> {
        let classes = classes.into_iter().collect::<Vec<_>>();
        self.prime_grant_context(classes.iter().copied())?;
        let root = self.memo.canonical_group(root);
        let sensitivity = self.goal_grant_sensitivity(root, base_goal.required)?;
        if plans.is_empty() {
            return Ok(Vec::new());
        }
        let mut repriced = Vec::with_capacity(classes.len());
        for class in classes {
            let goal = OptimizationGoal {
                grant: sensitivity.goal_for(admissible_set, class),
                ..base_goal
            };
            // A SeedPlan is an executable shape, not a promise that it is
            // feasible under every grant operating point. Re-price every
            // source seed whose grant contract matches this target and keep
            // the best destination cost. A class-specific target must never
            // inherit a cost from a different source goal.
            let matching_plans = plans
                .iter()
                .filter(|plan| plan.goal().grant == goal.grant)
                .collect::<Vec<_>>();
            if matching_plans.is_empty() {
                self.strong_incumbent_goal_mismatch_count =
                    self.strong_incumbent_goal_mismatch_count.saturating_add(1);
                continue;
            }
            let mut best = None;
            for plan in matching_plans {
                match self.reprice_seed_plan_for_goal(root, goal, plan.clone()) {
                    Ok(priced) => {
                        if best.as_ref().is_none_or(|current: &PricedIncumbent| {
                            goal.objective.compare(&priced.cost, &current.cost)
                                == std::cmp::Ordering::Less
                        }) {
                            best = Some(priced);
                        }
                    }
                    Err(error) => {
                        // Re-pricing is the validity check. An executable
                        // source plan can still be infeasible in a rebuilt
                        // target Memo; reject it and let target search
                        // establish its own incumbent.
                        self.strong_incumbent_reprice_rejection_count = self
                            .strong_incumbent_reprice_rejection_count
                            .saturating_add(1);
                        self.note_strong_incumbent_reprice_failure(&error.to_string());
                    }
                }
            }
            if let Some(priced) = best {
                repriced.push(priced);
            }
        }
        Ok(repriced)
    }

    pub(crate) fn install_strong_incumbent_for_grants(
        &mut self,
        root: GroupId,
        base_goal: OptimizationGoal,
        admissible_set: AdmissibleGrantSetId,
        classes: impl IntoIterator<Item = ResourceGrantClass>,
        plans: &[SeedPlan],
    ) -> Result<u64> {
        let repriced = self.reprice_strong_incumbents_for_grants(
            root,
            base_goal,
            admissible_set,
            classes,
            plans,
        )?;
        let installed = repriced.len() as u64;
        for priced in repriced {
            self.install_priced_incumbent(priced)?;
        }
        Ok(installed)
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
        self.physical_task_cache.clear();
        self.revalidate_strong_incumbents_after_merge()?;
        // Logical and physical expression ids from the two pre-merge groups
        // no longer describe an isolated implementation domain.  Revisit the
        // canonical group from its published expressions instead of allowing
        // a pre-merge visit marker to suppress a valid candidate.
        self.physical_implementation_seen.clear();
        self.quality_production_requests.clear();
        self.quality_forced_transform_bindings.clear();
        self.quality_active_forced_transform_binding = None;
        // A group merge changes the declared physical search domain and
        // invalidates every completion certificate, even when a redirected
        // task happens to retain the same numeric winner.
        self.physical_completion_proofs.clear();
        self.discard_merged_transformation_state(secondary, canonical);
        Ok(canonical)
    }

    /// A group merge changes the identity of an equivalence class, but it is
    /// not by itself a cost-fact change. Rebuild the exact facts-only witness
    /// against canonical group IDs and retain the priced seed when its
    /// context digest is unchanged. If cardinality, logical facts, producer
    /// facts, statistics, grant, or calibration changed, the digest differs
    /// and the old upper bound is dropped fail-closed.
    fn revalidate_strong_incumbents_after_merge(&mut self) -> Result<()> {
        let incumbents = std::mem::take(&mut self.strong_incumbents);
        let mut retained = BTreeMap::new();
        for (_, mut incumbent) in incumbents {
            let target_group = self.memo.canonical_group(incumbent.target_group);
            let reads = incumbent
                .context
                .reads
                .reads()
                .iter()
                .map(|read| PatternRead::facts_from_group(&self.memo, read.group))
                .collect::<Result<Vec<_>>>()
                .map(ReadSet::new);
            let Ok(reads) = reads else {
                self.strong_incumbent_merge_invalidation_count = self
                    .strong_incumbent_merge_invalidation_count
                    .saturating_add(1);
                self.note_strong_incumbent_invalidation(4, Some(incumbent.context.fingerprint));
                continue;
            };
            let Ok(context) = self.priced_cost_context(
                target_group,
                incumbent.goal,
                incumbent.plan.identity,
                &reads,
            ) else {
                self.strong_incumbent_merge_invalidation_count = self
                    .strong_incumbent_merge_invalidation_count
                    .saturating_add(1);
                self.note_strong_incumbent_invalidation(4, Some(incumbent.context.fingerprint));
                continue;
            };
            let reads_current = context.reads.is_current(&self.memo)?;
            if !reads_current || context.fingerprint != incumbent.context.fingerprint {
                self.strong_incumbent_merge_invalidation_count = self
                    .strong_incumbent_merge_invalidation_count
                    .saturating_add(1);
                self.note_strong_incumbent_invalidation(
                    if !reads_current { 1 } else { 2 },
                    Some(incumbent.context.fingerprint),
                );
                continue;
            }
            incumbent.target_group = target_group;
            incumbent.context = context;
            let key = (target_group, incumbent.goal);
            match retained.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(incumbent);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let current = entry.get();
                    if incumbent
                        .goal
                        .objective
                        .compare(&incumbent.cost, &current.cost)
                        == std::cmp::Ordering::Less
                    {
                        entry.insert(incumbent);
                    }
                }
            }
        }
        self.strong_incumbents = retained;
        Ok(())
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
        let previous = std::mem::take(&mut self.physical_read_dependencies);
        for ((group, goal), children) in previous {
            let dependencies = self
                .physical_read_dependencies
                .entry((self.memo.canonical_group(group), goal))
                .or_default();
            dependencies.extend(
                children
                    .into_iter()
                    .map(|(child, child_goal)| (self.memo.canonical_group(child), child_goal)),
            );
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

    /// Install the planner-owned producer for the optional ready-to-execute
    /// policy. The producer receives only an exact frozen candidate and may
    /// return no evidence when a required dependency is not available.
    pub fn set_quality_evidence_provider(&mut self, provider: Arc<dyn QualityEvidenceProvider>) {
        self.quality_evidence_provider = Some(provider);
    }

    /// Enable the same-Memo PReady handoff for an explicitly requested
    /// diagnostic/experimental cohort. It never changes the default stop
    /// policy by itself.
    pub fn set_quality_policy_handoff_enabled(&mut self, enabled: bool) {
        self.quality_handoff_enabled = enabled;
    }

    pub fn quality_policy_status(&self) -> QualityPolicyStatus {
        if !self.quality_handoff_reached {
            return QualityPolicyStatus::NotSatisfied;
        }
        self.quality_certificates.values().next().cloned().map_or(
            QualityPolicyStatus::NotSatisfied,
            QualityPolicyStatus::Satisfied,
        )
    }

    pub fn quality_policy_certificate(&self) -> Option<&PReadyCertificate> {
        self.quality_handoff_reached
            .then(|| self.quality_certificates.values().next())
            .flatten()
    }

    pub fn quality_last_evaluation(&self) -> &QualityEvaluationSummary {
        &self.quality_last_evaluation
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

    /// Enable the sound, proof-backed group/recipe pruning experiment without
    /// enabling per-rule tracing.  This is intentionally an explicit policy
    /// knob: the proof path must pass the independent model gate before it is
    /// made the default production policy.
    pub fn set_certified_group_pruning_enabled(&mut self, enabled: bool) {
        self.certified_group_pruning_enabled = enabled;
    }

    /// Toggle the diagnostic incumbent archive independently from certified
    /// pruning.  A disabled archive is useful as the no-incumbent control;
    /// mandatory fallback and result correctness are unchanged.
    pub fn set_protected_incumbent_enabled(&mut self, enabled: bool) {
        self.protected_incumbent_enabled = enabled;
        if !enabled {
            self.protected_incumbents.clear();
        }
    }

    /// Export the selected winner as an immutable executable plan seed for a
    /// separate Memo.  A SeedPlan intentionally carries no cost: pricing is
    /// a distinct operation in the destination context.
    pub fn export_seed_plan(
        &self,
        group: GroupId,
        goal: OptimizationGoal,
    ) -> Result<Option<SeedPlan>> {
        let group = self.memo.canonical_group(group);
        let Some(winner) = self
            .memo
            .group(group)
            .and_then(|group| group.winner(goal))
            .cloned()
        else {
            return Ok(None);
        };
        winner.cost.validate()?;
        if winner.cost.memory_completion != MemoryCompletion::Guaranteed {
            return Err(paro_error::internal(
                "strong incumbent seed requires guaranteed memory completion",
            ));
        }
        let reference = ChildWinnerRef {
            group,
            goal,
            candidate: winner.candidate,
        };
        let frozen = self.memo.freeze_candidate_tree(reference)?;
        validate_frozen_seed_tree(&frozen)?;
        let source_reads = self.winner_fact_reads(group, &winner)?;
        let identity = frozen_seed_plan_identity(&frozen);
        Ok(Some(SeedPlan {
            group,
            goal,
            frozen,
            source_reads,
            identity,
        }))
    }

    /// Price an exported plan against the exact winner DAG that produced it.
    /// This is useful for source-side attestation and tests; a plan crossing
    /// Memo boundaries must use `reprice_seed_plan`, which reconstructs the
    /// selected physical recipe in the destination Memo.
    pub fn price_seed_plan(&self, plan: &SeedPlan) -> Result<PricedIncumbent> {
        self.validate_seed_plan(plan)?;
        if !plan.source_reads.is_current(&self.memo)? {
            return Err(paro_error::internal(
                "seed plan source facts are stale; re-export the executable plan",
            ));
        }
        let winner = self
            .memo
            .resolve_child_winner(plan.frozen.reference)
            .ok_or_else(|| paro_error::internal("seed plan winner disappeared from source Memo"))?;
        if winner.cost != plan.frozen.winner.cost {
            return Err(paro_error::internal(
                "seed plan was paired with a winner from a different cost epoch",
            ));
        }
        let reads = self.winner_fact_reads(plan.group, winner)?;
        let context = self.priced_cost_context(plan.group, plan.goal, plan.identity, &reads)?;
        Ok(PricedIncumbent {
            target_group: plan.group,
            goal: plan.goal,
            plan: Arc::new(plan.clone()),
            cost: winner.cost,
            context,
        })
    }

    /// Re-price the selected physical DAG in this Memo.  The source Winner's
    /// cost is never read as a destination upper bound; only its immutable
    /// shape/choices are used to find the corresponding destination recipes.
    pub fn reprice_seed_plan(
        &mut self,
        target_group: GroupId,
        plan: SeedPlan,
    ) -> Result<PricedIncumbent> {
        self.reprice_seed_plan_for_goal(target_group, plan.goal, plan)
    }

    /// Variant used by independent ID-renaming oracles: the destination may
    /// have interned the same semantic requirement/context under different
    /// Memo-local IDs.
    pub fn reprice_seed_plan_for_goal(
        &mut self,
        target_group: GroupId,
        target_goal: OptimizationGoal,
        plan: SeedPlan,
    ) -> Result<PricedIncumbent> {
        self.validate_seed_plan(&plan)?;
        let target_group = self.memo.canonical_group(target_group);
        let repriced = self.reprice_seed_node(target_group, target_goal, &plan.frozen)?;
        let reads = ReadSet::new(repriced.reads);
        let context = self.priced_cost_context(target_group, target_goal, plan.identity, &reads)?;
        repriced.cost.validate()?;
        if repriced.cost.memory_completion != MemoryCompletion::Guaranteed {
            return Err(paro_error::internal(
                "repriced seed requires guaranteed memory completion",
            ));
        }
        Ok(PricedIncumbent {
            target_group,
            goal: target_goal,
            plan: Arc::new(plan),
            cost: repriced.cost,
            context,
        })
    }

    /// Install only a fully priced incumbent.  The read witness and the
    /// context digest are checked again at the publication boundary so a
    /// caller cannot retain an old cost after facts, grant, calibration, or
    /// the cost epoch changed.
    pub fn install_priced_incumbent(&mut self, incumbent: PricedIncumbent) -> Result<()> {
        let group = self.memo.canonical_group(incumbent.target_group);
        if incumbent.target_group != group {
            return Err(paro_error::internal(
                "priced incumbent root does not match its destination contract",
            ));
        }
        // The immutable SeedPlan retains the source Memo's reference goal;
        // the priced wrapper may carry the semantically equivalent target
        // goal whose local PropertySet/Context IDs were interned separately.
        // Validate the source identity here, then validate the destination
        // goal through the priced context below.
        self.validate_seed_plan(incumbent.plan.as_ref())?;
        self.memo
            .group(group)
            .ok_or_else(|| paro_error::internal("priced incumbent targets an unknown group"))?;
        incumbent.cost.validate()?;
        if incumbent.cost.memory_completion != MemoryCompletion::Guaranteed {
            return Err(paro_error::internal(
                "priced incumbent requires guaranteed memory completion",
            ));
        }
        validate_frozen_seed_tree(&incumbent.plan.frozen)?;
        if !incumbent.context.reads.is_current(&self.memo)? {
            return Err(paro_error::internal(
                "priced incumbent read witness is stale in the destination Memo",
            ));
        }
        let current_context = self.priced_cost_context(
            group,
            incumbent.goal,
            incumbent.plan.identity,
            &incumbent.context.reads,
        )?;
        if incumbent.context.fingerprint != current_context.fingerprint {
            return Err(paro_error::internal(
                "priced incumbent cost context changed before installation",
            ));
        }
        let context_fingerprint = incumbent.context.fingerprint;
        let read_count = incumbent.context.reads.reads().len() as u64;
        let cost_expected_bits = incumbent.cost.score.range.expected.to_bits();
        let cost_upper_bits = incumbent.cost.score.range.upper.to_bits();
        self.strong_incumbents
            .insert((group, incumbent.goal), incumbent);
        self.strong_incumbent_install_count = self.strong_incumbent_install_count.saturating_add(1);
        if self.strong_incumbent_installed_at_us.is_none() {
            self.strong_incumbent_installed_at_us = Some(self.engine_elapsed_us());
            self.strong_incumbent_installed_context_fingerprint = Some(context_fingerprint);
            self.strong_incumbent_installed_read_count = read_count;
            self.strong_incumbent_installed_cost_expected_bits = Some(cost_expected_bits);
            self.strong_incumbent_installed_cost_upper_bits = Some(cost_upper_bits);
        }
        Ok(())
    }

    /// Rebind a priced witness produced by an independent pricing Memo to
    /// this destination Memo's current facts. The conservative read set is
    /// all destination groups, so a child fact/statistics change cannot leave
    /// a detached source-group cursor looking valid. This does not copy or
    /// publish the pricing Memo and is used only before target search starts.
    pub(crate) fn rebind_priced_incumbent_to_current_facts(
        &self,
        incumbent: PricedIncumbent,
        target_goal: OptimizationGoal,
    ) -> Result<PricedIncumbent> {
        let reads = self
            .memo
            .groups()
            .map(|group| PatternRead::facts_from_group(&self.memo, group.id))
            .collect::<Result<Vec<_>>>()
            .map(ReadSet::new)?;
        let context = self.priced_cost_context(
            incumbent.target_group,
            target_goal,
            incumbent.plan.identity,
            &reads,
        )?;
        Ok(PricedIncumbent {
            goal: target_goal,
            context,
            ..incumbent
        })
    }

    fn engine_elapsed_us(&self) -> u64 {
        u64::try_from(self.engine_created_at.elapsed().as_micros()).unwrap_or(u64::MAX)
    }

    fn note_strong_incumbent_invalidation(
        &mut self,
        reason: u64,
        context_fingerprint: Option<Fingerprint>,
    ) {
        if self.strong_incumbent_first_invalidation_at_us.is_none() {
            self.strong_incumbent_first_invalidation_at_us = Some(self.engine_elapsed_us());
            self.strong_incumbent_first_invalidation_reason = Some(reason);
            self.strong_incumbent_first_invalidated_context_fingerprint = context_fingerprint;
        }
    }

    fn note_strong_incumbent_reprice_failure(&mut self, error: &str) {
        let reason = if error.contains("no matching destination recipe") {
            self.strong_incumbent_reprice_no_destination_recipe_count = self
                .strong_incumbent_reprice_no_destination_recipe_count
                .saturating_add(1);
            1
        } else if error.contains("selected child DAG") {
            self.strong_incumbent_reprice_child_dag_count = self
                .strong_incumbent_reprice_child_dag_count
                .saturating_add(1);
            2
        } else if error.contains("physical fingerprint") {
            self.strong_incumbent_reprice_fingerprint_count = self
                .strong_incumbent_reprice_fingerprint_count
                .saturating_add(1);
            3
        } else if error.contains("properties") || error.contains("requirement") {
            self.strong_incumbent_reprice_property_count = self
                .strong_incumbent_reprice_property_count
                .saturating_add(1);
            4
        } else {
            self.strong_incumbent_reprice_other_count =
                self.strong_incumbent_reprice_other_count.saturating_add(1);
            255
        };
        if self.strong_incumbent_first_reprice_failure_reason.is_none() {
            self.strong_incumbent_first_reprice_failure_reason = Some(reason);
        }
    }

    fn validate_seed_plan(&self, plan: &SeedPlan) -> Result<()> {
        if plan.frozen.reference.group != plan.group
            || plan.frozen.reference.goal != plan.goal
            || plan.frozen.winner.candidate != plan.frozen.reference.candidate
            || plan.identity != frozen_seed_plan_identity(&plan.frozen)
        {
            return Err(paro_error::internal(
                "seed plan lost its immutable root or stable plan identity",
            ));
        }
        validate_frozen_seed_tree(&plan.frozen)
    }

    fn reprice_seed_node(
        &mut self,
        target_group: GroupId,
        target_goal: OptimizationGoal,
        source: &FrozenCandidate,
    ) -> Result<RepricedSeedNode> {
        let target_group = self.memo.canonical_group(target_group);
        let logical_ids = self
            .memo
            .group(target_group)
            .ok_or_else(|| paro_error::internal("seed plan targets an unknown destination group"))?
            .logical_exprs()
            .to_vec();
        let implementation_id = source.physical.key.implementation;
        let required = self
            .memo
            .required(target_goal.required)
            .cloned()
            .ok_or_else(|| {
                paro_error::internal("seed plan target has unknown required properties")
            })?;

        let mut logical_matches = 0_u64;
        let mut candidate_count = 0_u64;
        let mut child_error = None;
        let mut candidate_summary = Vec::new();
        let mut rejection_reasons = BTreeMap::<String, u64>::new();
        let target_logical_summary = logical_ids
            .iter()
            .filter_map(|logical_id| self.memo.logical_expr(*logical_id))
            .map(|logical| {
                let encoding = logical.operator_encoding.as_ref().map(|encoding| {
                    let mut builder = StableFingerprintBuilder::default();
                    builder.write_bytes(encoding);
                    builder.finish()
                });
                format!(
                    "expr={},op={:?},tag={:?},children={},encoding={encoding:?}",
                    logical.id.index(),
                    logical.key.operator,
                    logical.operator_tag,
                    logical.key.children.len(),
                )
            })
            .collect::<Vec<_>>()
            .join(";");
        let source_encoding = source.logical.operator_encoding.as_ref().map(|encoding| {
            let mut builder = StableFingerprintBuilder::default();
            builder.write_bytes(encoding);
            builder.finish()
        });
        for logical_id in logical_ids {
            let Some(logical) = self.memo.logical_expr(logical_id) else {
                continue;
            };
            if !seed_logical_shell_matches(&source.logical, logical) {
                continue;
            }
            logical_matches = logical_matches.saturating_add(1);
            let candidates = {
                let implementation = self
                    .registry
                    .implementation(implementation_id)
                    .ok_or_else(|| {
                        paro_error::internal(
                            "seed plan references an implementation absent from destination registry",
                        )
                    })?;
                let context = ImplementationContext {
                    memo: &self.memo,
                    group: target_group,
                };
                if !implementation.matches(logical, target_goal, &context) {
                    continue;
                }
                implementation
                    .candidates(logical_id, target_goal, &context)?
                    .into_vec()
            };
            candidate_count = candidate_count.saturating_add(candidates.len() as u64);

            for mut candidate in candidates {
                candidate_summary.push(format!(
                    "logical={} impl={} payload={:?} children={:?} provided={:?}",
                    logical_id.index(),
                    candidate.key.implementation.0,
                    candidate.key.payload_fingerprint,
                    candidate
                        .key
                        .children
                        .iter()
                        .map(|child| child.index())
                        .collect::<Vec<_>>(),
                    &candidate.provided,
                ));
                let identity_mismatch = candidate.key.implementation != implementation_id
                    || candidate.key.logical != logical_id
                    || candidate.key.payload_fingerprint != source.physical.key.payload_fingerprint
                    || candidate.key.children.len() != source.children.len();
                if identity_mismatch || !candidate.provided.satisfies(&required) {
                    let reason = if identity_mismatch {
                        format!(
                            "candidate identity: impl={} payload={:?} arity={}",
                            candidate.key.implementation.0,
                            candidate.key.payload_fingerprint,
                            candidate.key.children.len()
                        )
                    } else {
                        "candidate properties do not satisfy target requirement".to_owned()
                    };
                    *rejection_reasons.entry(reason).or_default() += 1;
                    continue;
                }
                if let Some(region) = candidate.region.as_mut() {
                    refresh_region_candidate_contract(&self.memo, region)?;
                }
                if let Some(region) = &candidate.region {
                    let owner_in_scope = self
                        .memo
                        .regions()
                        .node(region.region)
                        .is_some_and(|region| region.scope.contains(&target_group));
                    if !owner_in_scope {
                        *rejection_reasons
                            .entry("region owner is outside target scope".to_owned())
                            .or_default() += 1;
                        continue;
                    }
                }
                let inherited_sources = self
                    .memo
                    .optimization_context(target_goal.context)
                    .ok_or_else(|| {
                        paro_error::internal("seed plan target has no source-demand context")
                    })?
                    .filterable_sources()
                    .clone();
                for (ordinal, (child, child_goal)) in candidate.child_goals.iter_mut().enumerate() {
                    child_goal.grant = self.normalized_child_grant(
                        *child,
                        child_goal.required,
                        target_goal.grant,
                    )?;
                    let mut sources = inherited_sources.clone();
                    if let Some((filtered_child, filters)) =
                        candidate.cost_composition.sideways_filter()
                    {
                        if ordinal == filtered_child {
                            sources.extend(filters.iter().map(|filter| filter.source));
                        }
                    }
                    child_goal.context = self
                        .memo
                        .intern_source_demand_context(child_goal.context, sources)?;
                }
                let canonical_key_children = candidate
                    .key
                    .children
                    .iter()
                    .map(|child| self.memo.canonical_group(*child))
                    .collect::<Vec<_>>();
                let canonical_goal_children = candidate
                    .child_goals
                    .iter()
                    .map(|(child, _)| self.memo.canonical_group(*child))
                    .collect::<Vec<_>>();
                if canonical_key_children != canonical_goal_children {
                    *rejection_reasons
                        .entry("candidate child keys disagree with child goals".to_owned())
                        .or_default() += 1;
                    continue;
                }

                let mut child_nodes = Vec::with_capacity(candidate.child_goals.len());
                let mut child_costs = Vec::with_capacity(candidate.child_goals.len());
                let mut child_fingerprints = Vec::with_capacity(candidate.child_goals.len());
                let mut reads = vec![PatternRead::facts_from_group(&self.memo, target_group)?];
                let mut children_match = true;
                for ((child, child_goal), source_child) in candidate
                    .child_goals
                    .iter()
                    .copied()
                    .zip(source.children.iter())
                {
                    let child = self.memo.canonical_group(child);
                    let child_node = match self.reprice_seed_node(child, child_goal, source_child) {
                        Ok(child_node) => child_node,
                        Err(error) => {
                            child_error.get_or_insert_with(|| error.to_string());
                            children_match = false;
                            break;
                        }
                    };
                    child_costs.push(child_node.cost);
                    child_fingerprints.push(child_node.physical_fingerprint);
                    reads.extend(child_node.reads.iter().copied());
                    child_nodes.push(child_node);
                }
                if !children_match || child_nodes.len() != source.children.len() {
                    *rejection_reasons
                        .entry("selected child DAG has no destination match".to_owned())
                        .or_default() += 1;
                    continue;
                }

                let enforced = match replay_enforcer_chain(
                    candidate.provided.clone(),
                    &required,
                    &source.winner.enforcers,
                ) {
                    Ok(enforced) if enforced.satisfies(&required) => enforced,
                    _ => {
                        *rejection_reasons
                            .entry("enforcer chain cannot be replayed".to_owned())
                            .or_default() += 1;
                        continue;
                    }
                };
                drop(enforced);
                let physical_fingerprint = enforced_fingerprint(
                    candidate.physical_fingerprint,
                    &source.winner.enforcers,
                    child_fingerprints.iter().copied(),
                );
                if physical_fingerprint != source.winner.physical_fingerprint {
                    *rejection_reasons
                        .entry("physical fingerprint differs after child replay".to_owned())
                        .or_default() += 1;
                    continue;
                }
                let Some(local_cost) = fit_local_retained_state_to_grant_ref(
                    candidate.local_cost,
                    &child_costs,
                    &candidate.cost_composition,
                    candidate.spillable,
                    candidate.enforcer_cost_input,
                )?
                else {
                    *rejection_reasons
                        .entry("local retained-state cost is infeasible".to_owned())
                        .or_default() += 1;
                    continue;
                };
                let local_without_source_filter = match candidate.source_filter_apply_cost {
                    Some(apply) => local_cost.replace_work(apply, SearchCost::ZERO)?,
                    None => local_cost,
                };
                let mut local_cost = resolve_task_supply(
                    local_without_source_filter,
                    &child_costs,
                    &candidate.task_supply,
                    self.memo.calibration(),
                )?;
                if let Some(apply) = candidate.source_filter_apply_cost {
                    local_cost = local_cost.replace_work(SearchCost::ZERO, apply)?;
                }
                let child_source_work_refs = child_nodes
                    .iter()
                    .map(|child| child.source_work.as_ref())
                    .collect::<SmallVec<[&[SourceWork]; 8]>>();
                let composed = compose_candidate_cost_with_sources_at_ref(
                    local_cost,
                    candidate.source_filter_apply_cost,
                    &child_costs,
                    &child_source_work_refs,
                    &candidate.cost_composition,
                    self.memo.calibration(),
                )?;
                let Some(cost) =
                    constrain_composed_cost_to_grant(composed.cost, candidate.enforcer_cost_input)?
                else {
                    *rejection_reasons
                        .entry("composed cost is infeasible for target grant".to_owned())
                        .or_default() += 1;
                    continue;
                };
                let Some(enforcer_phase) = enforcer_cost(
                    &source.winner.enforcers,
                    candidate.enforcer_cost_input,
                    self.memo.calibration(),
                )?
                else {
                    *rejection_reasons
                        .entry("enforcer cost is infeasible for target grant".to_owned())
                        .or_default() += 1;
                    continue;
                };
                let Some(cost) = constrain_composed_cost_to_grant(
                    enforcer_phase.compose_after(cost)?,
                    candidate.enforcer_cost_input,
                )?
                else {
                    *rejection_reasons
                        .entry("enforcer phase cost is infeasible for target grant".to_owned())
                        .or_default() += 1;
                    continue;
                };
                return Ok(RepricedSeedNode {
                    cost,
                    source_work: composed.source_work,
                    physical_fingerprint,
                    reads,
                });
            }
        }
        Err(paro_error::internal(format!(
            "seed plan physical DAG has no matching destination recipe: target_group={}, logical_matches={}, candidates={}, implementation={}, payload={:?}, source_op={:?}, source_tag={:?}, source_encoding={source_encoding:?}, target_required={:?}, source_physical={:?}, source_winner_fp={:?}, source_enforcers={:?}, children={}, target_logicals=[{}], candidate_summary=[{}], rejections={:?}, child_error={}",
            target_group.index(),
            logical_matches,
            candidate_count,
            implementation_id.0,
            source.physical.key.payload_fingerprint,
            source.logical.key.operator,
            source.logical.operator_tag,
            required,
            source.physical.provided,
            source.winner.physical_fingerprint,
            source.winner.enforcers,
            source.children.len(),
            target_logical_summary,
            candidate_summary.join(";"),
            rejection_reasons,
            child_error.as_deref().unwrap_or("none"),
        )))
    }

    fn begin_diagnostic_profile(
        &mut self,
        root: GroupId,
        checkpoint_goals: impl IntoIterator<Item = OptimizationGoal>,
    ) {
        let checkpoint_goals = checkpoint_goals.into_iter().collect::<Vec<_>>();
        self.search_milestones = SearchMilestones::default();
        self.milestone_root = (self.collect_rule_work_profile || self.quality_handoff_enabled)
            .then_some(self.memo.canonical_group(root));
        self.quality_required_goals = self
            .quality_handoff_enabled
            .then(|| checkpoint_goals.iter().copied().collect())
            .unwrap_or_default();
        self.quality_ready_winners.clear();
        self.quality_certificates.clear();
        self.quality_handoff_reached = false;
        self.quality_production_requests.clear();
        self.quality_forced_transform_bindings.clear();
        self.quality_active_forced_transform_binding = None;
        self.quality_last_production_obligation = None;
        self.quality_producer_dispatch_count = 0;
        self.quality_direct_binding_dispatch_count = 0;
        self.quality_direct_binding_first_us = None;
        self.quality_direct_binding_last_us = None;
        self.quality_direct_binding_work_units = 0;
        self.quality_candidate_evaluation_count = 0;
        self.quality_candidate_missing_evidence_count = 0;
        self.quality_frontier_candidate_count = 0;
        self.quality_frontier_candidate_skip_count = 0;
        self.quality_frontier_certified_count = 0;
        self.quality_frontier_policy_rejection_count = 0;
        self.quality_frontier_fact_signatures.clear();
        self.quality_evaluated_candidates.clear();
        self.quality_frontier_max_aggregates = 0;
        self.quality_frontier_max_runtime_filters = 0;
        self.quality_frontier_max_aggregate_regions = 0;
        self.quality_frontier_max_covered_aggregate_regions = 0;
        self.quality_frontier_first_aggregate_region_us = None;
        self.quality_frontier_first_aggregate_candidate = None;
        self.quality_frontier_first_incomplete_aggregate_region_us = None;
        self.quality_frontier_first_incomplete_aggregate_candidate = None;
        self.quality_frontier_first_incomplete_aggregate_region = None;
        self.quality_frontier_first_incomplete_aggregate_anchor = None;
        self.quality_frontier_first_incomplete_aggregate_covered = 0;
        self.quality_frontier_first_incomplete_aggregate_total = 0;
        self.quality_frontier_first_complete_aggregate_region_us = None;
        self.quality_frontier_first_complete_aggregate_candidate = None;
        self.quality_frontier_max_aggregate_witnesses = 0;
        self.quality_frontier_max_join_witnesses = 0;
        self.quality_certified_max_aggregates = 0;
        self.quality_certified_max_runtime_filters = 0;
        self.quality_certified_max_aggregate_regions = 0;
        self.quality_certified_max_covered_aggregate_regions = 0;
        self.quality_certified_max_aggregate_witnesses = 0;
        self.quality_certified_max_join_witnesses = 0;
        self.quality_last_evaluation = QualityEvaluationSummary::default();
        self.quality_last_evaluation_candidate = None;
        self.quality_last_evaluation_goal = None;
        if self.collect_rule_work_profile {
            let mut goals = checkpoint_goals;
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
        let reason = if self.quality_handoff_reached {
            SearchStopReason::QualityPolicySatisfied
        } else if self.memo.control().deadline_reached() {
            SearchStopReason::Deadline
        } else if budget_limited {
            SearchStopReason::BudgetLimited
        } else if obligations.iter().any(|obligation| {
            matches!(
                obligation.reason,
                super::budget::SearchIncompleteReason::RuleFailure { .. }
            )
        }) {
            SearchStopReason::RuleFailure
        } else if !obligations.is_empty() {
            SearchStopReason::SearchIncomplete
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
                SearchStopReason::BudgetLimited
                | SearchStopReason::SearchIncomplete
                | SearchStopReason::RuleFailure
                | SearchStopReason::QualityPolicySatisfied => {
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
                let winner = self
                    .memo
                    .group(root)
                    .and_then(|group| group.winner(goal))
                    .cloned();
                let (choices, fact_reads, frozen) = winner
                    .as_ref()
                    .and_then(|winner| self.checkpoint_candidate_evidence(root, goal, winner).ok())
                    .map_or(
                        (
                            Box::<[FrozenChoice]>::default(),
                            Box::<[PatternRead]>::default(),
                            false,
                        ),
                        |evidence| evidence,
                    );
                checkpoints.push(SearchCheckpoint {
                    target_ms,
                    observed_us,
                    goal,
                    candidate: winner.as_ref().map(|winner| winner.candidate),
                    expected_cost: winner
                        .as_ref()
                        .map(|winner| winner.cost.score.range.expected),
                    risk_adjusted_cost: winner
                        .as_ref()
                        .map(|winner| winner.cost.score.risk_adjusted),
                    upper_cost: winner.as_ref().map(|winner| winner.cost.score.range.upper),
                    choices,
                    fact_reads,
                    frozen,
                    search_complete,
                });
            }
            self.search_milestones
                .search_checkpoints
                .extend(checkpoints);
            self.next_diagnostic_checkpoint += 1;
        }
    }

    fn checkpoint_candidate_evidence(
        &self,
        root: GroupId,
        goal: OptimizationGoal,
        winner: &Winner,
    ) -> Result<(Box<[FrozenChoice]>, Box<[PatternRead]>, bool)> {
        let reference = ChildWinnerRef {
            group: self.memo.canonical_group(root),
            goal,
            candidate: winner.candidate,
        };
        let frozen = self.memo.freeze_candidate_tree(reference)?;
        let mut choices = Vec::new();
        let mut visited = BTreeSet::new();
        fn visit(
            frozen: &FrozenCandidate,
            choices: &mut Vec<FrozenChoice>,
            visited: &mut BTreeSet<CandidateId>,
        ) {
            if !visited.insert(frozen.reference.candidate) {
                return;
            }
            choices.push(FrozenChoice {
                reference: frozen.reference,
                logical: frozen.logical.id,
                physical: frozen.physical.id,
                logical_payload: frozen.logical.payload.0,
                physical_payload: frozen.physical.payload.0,
                physical_fingerprint: frozen.winner.physical_fingerprint,
                children: frozen.winner.children.clone(),
                rules: frozen.logical.applied_rules.iter().copied().collect(),
                selected_rules: selected_proof_rule_ids(&frozen.logical),
            });
            for child in frozen.children.iter() {
                visit(child, choices, visited);
            }
        }
        visit(&frozen, &mut choices, &mut visited);
        let fact_reads = self.winner_fact_reads(root, winner)?.reads().to_vec();
        Ok((
            choices.into_boxed_slice(),
            fact_reads.into_boxed_slice(),
            true,
        ))
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

    fn note_transformation_task(
        &mut self,
        task: TransformationTaskId,
        phase: TransformationTaskLifecyclePhase,
        reads: Option<&[PatternRead]>,
        binding: Option<Fingerprint>,
        count: u64,
    ) {
        if !self.collect_rule_work_profile
            || self
                .registry
                .transformation(task.rule)
                .is_none_or(|rule| rule.quality_dependency().is_none())
        {
            return;
        }
        let elapsed = self.lifecycle_elapsed_us();
        Self::note_transformation_task_at(
            &mut self.search_milestones,
            task,
            phase,
            reads,
            binding,
            count,
            elapsed,
        );
    }

    fn note_transformation_task_at(
        milestones: &mut SearchMilestones,
        task: TransformationTaskId,
        phase: TransformationTaskLifecyclePhase,
        reads: Option<&[PatternRead]>,
        binding: Option<Fingerprint>,
        count: u64,
        elapsed: u64,
    ) {
        let index = if let Some(index) =
            milestones
                .transformation_task_lifecycle
                .iter()
                .position(|entry| {
                    entry.group == task.group
                        && entry.expression == task.expression
                        && entry.rule == task.rule
                }) {
            index
        } else {
            if milestones.transformation_task_lifecycle.len() >= MAX_TRANSFORMATION_TASK_LIFECYCLES
            {
                milestones.transformation_task_lifecycle_dropped = milestones
                    .transformation_task_lifecycle_dropped
                    .saturating_add(1);
                return;
            }
            milestones
                .transformation_task_lifecycle
                .push(TransformationTaskLifecycle::new(task));
            milestones
                .transformation_task_lifecycle
                .len()
                .saturating_sub(1)
        };
        let entry = &mut milestones.transformation_task_lifecycle[index];
        if let Some(reads) = reads {
            let mut reads = reads.to_vec();
            reads.sort_unstable();
            reads.dedup();
            entry.last_reads = reads.into_boxed_slice();
        }
        if entry.first_binding.is_none() {
            entry.first_binding = binding;
        }
        match phase {
            TransformationTaskLifecyclePhase::Enqueued => entry.last_enqueued_us = Some(elapsed),
            TransformationTaskLifecyclePhase::DependenciesReady => {
                entry.last_dependencies_ready_us = Some(elapsed)
            }
            TransformationTaskLifecyclePhase::FirstRun => entry.last_run_us = Some(elapsed),
            TransformationTaskLifecyclePhase::Published => entry.last_published_us = Some(elapsed),
            TransformationTaskLifecyclePhase::Matched
            | TransformationTaskLifecyclePhase::NoMatch
            | TransformationTaskLifecyclePhase::Applicable
            | TransformationTaskLifecyclePhase::NoOutput
            | TransformationTaskLifecyclePhase::BudgetRejected => {}
        }
        let first = match phase {
            TransformationTaskLifecyclePhase::Enqueued => &mut entry.first_enqueued_us,
            TransformationTaskLifecyclePhase::FirstRun => &mut entry.first_run_us,
            TransformationTaskLifecyclePhase::DependenciesReady => {
                &mut entry.first_dependencies_ready_us
            }
            TransformationTaskLifecyclePhase::Matched => &mut entry.first_matched_us,
            TransformationTaskLifecyclePhase::NoMatch => &mut entry.first_no_match_us,
            TransformationTaskLifecyclePhase::Applicable => &mut entry.first_applicable_us,
            TransformationTaskLifecyclePhase::Published => &mut entry.first_published_us,
            TransformationTaskLifecyclePhase::NoOutput => &mut entry.first_no_output_us,
            TransformationTaskLifecyclePhase::BudgetRejected => &mut entry.first_budget_rejected_us,
        };
        first.get_or_insert(elapsed);
        match phase {
            TransformationTaskLifecyclePhase::Matched => {
                entry.match_count = entry.match_count.saturating_add(count)
            }
            TransformationTaskLifecyclePhase::NoMatch => {
                entry.no_match_count = entry.no_match_count.saturating_add(count.max(1))
            }
            TransformationTaskLifecyclePhase::Applicable => {
                entry.applicable_count = entry.applicable_count.saturating_add(count.max(1))
            }
            TransformationTaskLifecyclePhase::NoOutput => {
                entry.no_output_count = entry.no_output_count.saturating_add(count.max(1))
            }
            TransformationTaskLifecyclePhase::Published => {
                entry.published_count = entry.published_count.saturating_add(count)
            }
            TransformationTaskLifecyclePhase::BudgetRejected => {
                entry.budget_rejected_count =
                    entry.budget_rejected_count.saturating_add(count.max(1))
            }
            TransformationTaskLifecyclePhase::Enqueued
            | TransformationTaskLifecyclePhase::FirstRun
            | TransformationTaskLifecyclePhase::DependenciesReady => {}
        }
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
            RuleWorkPhase::Enqueued => &mut profile.first_enqueued_us,
            RuleWorkPhase::DependenciesReady => &mut profile.first_dependencies_ready_us,
            RuleWorkPhase::FirstRun => &mut profile.first_run_us,
            RuleWorkPhase::Discovered => &mut profile.first_discovered_us,
            RuleWorkPhase::Matched => &mut profile.first_matched_us,
            RuleWorkPhase::Applicable => &mut profile.first_applicable_us,
            RuleWorkPhase::Published => &mut profile.first_published_us,
        };
        slot.get_or_insert(elapsed);
    }

    fn note_rule_root_consumed(&mut self, rules: &[RuleId]) {
        if !self.collect_rule_work_profile {
            return;
        }
        for rule in rules.iter().copied() {
            let count = self
                .rule_work_profile
                .get(&rule)
                .map_or(1, |profile| profile.root_consumed.saturating_add(1));
            self.rule_work_profile
                .entry(rule)
                .or_default()
                .root_consumed = count;
        }
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

    fn note_candidate_lifecycle(&mut self, event: CandidateLifecycleEvent) {
        if !self.collect_rule_work_profile {
            return;
        }
        let stage_index = event.stage as usize;
        if self.search_milestones.candidate_lifecycle_stage_stored[stage_index]
            >= CANDIDATE_LIFECYCLE_STAGE_LIMITS[stage_index]
            || self.search_milestones.candidate_lifecycle.len() >= MAX_CANDIDATE_LIFECYCLE_EVENTS
        {
            self.search_milestones.candidate_lifecycle_stage_dropped[stage_index] =
                self.search_milestones.candidate_lifecycle_stage_dropped[stage_index]
                    .saturating_add(1);
            self.search_milestones.candidate_lifecycle_dropped = self
                .search_milestones
                .candidate_lifecycle_dropped
                .saturating_add(1);
            return;
        }
        self.search_milestones.candidate_lifecycle_stage_stored[stage_index] =
            self.search_milestones.candidate_lifecycle_stage_stored[stage_index].saturating_add(1);
        self.search_milestones.candidate_lifecycle.push(event);
    }

    fn lifecycle_elapsed_us(&self) -> u64 {
        self.profile_elapsed_us()
            .unwrap_or_else(|| self.memo.control().elapsed_us())
    }

    fn lifecycle_elapsed_from(started_at: Option<Instant>) -> u64 {
        started_at
            .map(|started| u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX))
            .unwrap_or_default()
    }

    fn note_parent_publication(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
        physical: PhysicalExprId,
        recipe: Fingerprint,
        candidate: CandidateId,
        children: Box<[ChildWinnerRef]>,
        cost: SearchCost,
    ) {
        if !self.collect_rule_work_profile {
            return;
        }
        self.note_candidate_lifecycle(CandidateLifecycleEvent {
            stage: CandidateLifecycleStage::ParentPublished,
            elapsed_us: self.lifecycle_elapsed_us(),
            group,
            goal: Some(goal),
            candidate: Some(candidate),
            source: None,
            binding: None,
            source_child: None,
            logical: self
                .memo
                .physical_expr(physical)
                .map(|expression| expression.key.logical),
            physical: Some(physical),
            recipe: Some(recipe),
            rule: None,
            children,
            facts: Box::new([]),
            expected_cost_bits: Some(cost.score.range.expected.to_bits()),
            upper_cost_bits: Some(cost.score.range.upper.to_bits()),
        });
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
    ) -> Result<()> {
        let is_root = self.milestone_root == Some(self.memo.canonical_group(group));
        if !self.collect_rule_work_profile || self.optional_search_started && !is_root {
            if self.quality_handoff_enabled && self.optional_search_started && is_root {
                return self.try_quality_handoff_candidate(group, goal);
            }
            return Ok(());
        }
        let Some(candidate) = self
            .memo
            .group(group)
            .and_then(|group| group.winner(goal))
            .map(|winner| winner.candidate)
        else {
            return Ok(());
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
        if self.quality_handoff_enabled && self.optional_search_started {
            self.try_quality_handoff_candidate(group, goal)?;
        }
        Ok(())
    }

    /// Yield the readiness pass after a bounded batch of newly visible
    /// physical frontier entries.  The caller keeps the exact
    /// child-combination state and returns
    /// the current recipe cursor to the TaskRegistry, so this does not drop
    /// alternatives or turn a partial domain into a completion claim.
    fn should_yield_physical_interleave_step(&mut self) -> bool {
        const PUBLICATIONS_PER_STEP: u16 = 32;
        if self.physical_interleave_step_mode {
            self.physical_interleave_step_publications =
                self.physical_interleave_step_publications.saturating_add(1);
            if self.physical_interleave_step_publications >= PUBLICATIONS_PER_STEP {
                self.physical_interleave_step_yielded = true;
                return true;
            }
        }
        false
    }

    fn note_physical_frontier_change(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
        selected_changed: bool,
    ) -> Result<bool> {
        self.note_physical_candidate(group, goal, selected_changed)?;
        self.record_diagnostic_checkpoints();
        Ok(self.should_yield_physical_interleave_step())
    }

    fn try_quality_handoff_candidate(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
    ) -> Result<()> {
        if !self.quality_handoff_enabled
            || self.quality_handoff_reached
            || !self.quality_required_goals.contains(&goal)
            || self.quality_ready_winners.contains_key(&goal)
        {
            return Ok(());
        }
        let Some(provider) = self.quality_evidence_provider.clone() else {
            return Ok(());
        };
        let root = self.memo.canonical_group(group);
        let frontier = self
            .memo
            .group(root)
            .and_then(|group| group.winner_frontier(goal))
            .map(|frontier| frontier.candidates().to_vec())
            .unwrap_or_default();
        if frontier.is_empty() {
            return Ok(());
        }
        let leading_candidate = frontier.first().map(|winner| winner.candidate);
        let class = self.quality_class_for_goal(goal);
        for winner in frontier {
            // The same immutable CandidateId can remain in a frontier while
            // its group facts advance. Include the exact fact read-set in the
            // evaluation cursor so a stale quality result cannot suppress a
            // re-check after a branch/domain/statistics publication.
            let reads = self.winner_fact_reads(root, &winner)?;
            let diagnostic_facts = self
                .collect_rule_work_profile
                .then(|| reads.reads().to_vec().into_boxed_slice());
            let read_id = self.task_registry.intern_read_set(reads);
            let candidate_key = (goal, winner.candidate, read_id);
            if !self.quality_evaluated_candidates.insert(candidate_key) {
                self.quality_frontier_candidate_skip_count =
                    self.quality_frontier_candidate_skip_count.saturating_add(1);
                continue;
            }
            self.quality_frontier_candidate_count =
                self.quality_frontier_candidate_count.saturating_add(1);
            let frozen_winner = self.freeze_grant_winner(root, class, goal, winner)?;
            let reference = frozen_winner.frozen.reference;
            self.quality_last_evaluation_candidate = Some(reference.candidate);
            self.quality_last_evaluation_goal = Some(goal);
            let Some(evidence) =
                provider.evidence(&self.memo, reference, &frozen_winner.frozen, goal)?
            else {
                self.quality_candidate_missing_evidence_count = self
                    .quality_candidate_missing_evidence_count
                    .saturating_add(1);
                continue;
            };
            tracing::debug!(
                target: "paro::optimizer::quality_handoff",
                candidate = reference.candidate.index(),
                logical_payload = frozen_winner.frozen.logical.payload.0,
                physical_payload = frozen_winner.frozen.physical.payload.0,
                operator_tag = frozen_winner.frozen.logical.operator_tag,
                child_count = frozen_winner.frozen.children.len(),
                shape = ?evidence.shape,
                facts = ?evidence.facts,
                capabilities = ?evidence.capabilities,
                aggregate_regions = ?evidence.aggregate_regions,
                "evaluating exact quality handoff candidate"
            );
            self.quality_frontier_max_aggregates = self
                .quality_frontier_max_aggregates
                .max(evidence.shape.aggregates);
            self.quality_frontier_max_runtime_filters = self
                .quality_frontier_max_runtime_filters
                .max(evidence.shape.runtime_filter_joins);
            let aggregate_region_count = evidence.aggregate_regions.len() as u32;
            let covered_aggregate_region_count = evidence
                .aggregate_regions
                .iter()
                .filter(|witness| witness.covered)
                .count() as u32;
            let coverage_elapsed_us = self
                .profile_elapsed_us()
                .unwrap_or_else(|| self.memo.control().elapsed_us());
            if aggregate_region_count > 0 {
                self.quality_frontier_first_aggregate_region_us
                    .get_or_insert(coverage_elapsed_us);
                self.quality_frontier_first_aggregate_candidate
                    .get_or_insert(reference.candidate);
                if covered_aggregate_region_count < aggregate_region_count {
                    if self
                        .quality_frontier_first_incomplete_aggregate_region_us
                        .is_none()
                    {
                        let witness = evidence
                            .aggregate_regions
                            .iter()
                            .find(|witness| !witness.covered)
                            .or_else(|| evidence.aggregate_regions.first());
                        self.quality_frontier_first_incomplete_aggregate_region_us =
                            Some(coverage_elapsed_us);
                        self.quality_frontier_first_incomplete_aggregate_candidate =
                            Some(reference.candidate);
                        self.quality_frontier_first_incomplete_aggregate_region =
                            witness.map(|witness| witness.region);
                        self.quality_frontier_first_incomplete_aggregate_anchor =
                            witness.map(|witness| witness.anchor);
                        self.quality_frontier_first_incomplete_aggregate_covered =
                            covered_aggregate_region_count;
                        self.quality_frontier_first_incomplete_aggregate_total =
                            aggregate_region_count;
                    }
                } else {
                    self.quality_frontier_first_complete_aggregate_region_us
                        .get_or_insert(coverage_elapsed_us);
                    self.quality_frontier_first_complete_aggregate_candidate
                        .get_or_insert(reference.candidate);
                }
            }
            self.quality_frontier_max_aggregate_regions = self
                .quality_frontier_max_aggregate_regions
                .max(aggregate_region_count);
            self.quality_frontier_max_covered_aggregate_regions = self
                .quality_frontier_max_covered_aggregate_regions
                .max(covered_aggregate_region_count);
            self.quality_frontier_max_aggregate_witnesses = self
                .quality_frontier_max_aggregate_witnesses
                .max(evidence.shape.aggregate_witness_nodes);
            self.quality_frontier_max_join_witnesses = self
                .quality_frontier_max_join_witnesses
                .max(evidence.shape.join_region_witness_nodes);
            // This is the first point at which the exact selected proof-bearing
            // rules are consumed by the root-quality decision.  It is deliberately
            // fed by the provider's frozen-DAG evidence, never by Memo audit bits.
            let fact_signature = evidence.facts.iter().fold(0_u16, |signature, fact| {
                signature | (1_u16 << fact.stable_tag())
            });
            *self
                .quality_frontier_fact_signatures
                .entry(fact_signature)
                .or_default() += 1;
            self.note_rule_root_consumed(&evidence.selected_rules);
            self.quality_candidate_evaluation_count =
                self.quality_candidate_evaluation_count.saturating_add(1);
            let Some(certificate) = self.quality_bundles.evaluate_native_candidate(
                QualityPolicyId::new(1),
                reference.candidate,
                read_id,
                &evidence,
                1,
            )?
            else {
                self.quality_frontier_policy_rejection_count = self
                    .quality_frontier_policy_rejection_count
                    .saturating_add(1);
                self.quality_last_evaluation = self.quality_bundles.evaluation_summary();
                let missing = self.quality_last_evaluation.missing_fact_kinds.clone();
                self.record_quality_production_request(
                    goal,
                    &frozen_winner.frozen,
                    read_id,
                    &evidence,
                    &missing,
                )?;
                continue;
            };
            self.quality_last_evaluation = self.quality_bundles.evaluation_summary();
            self.quality_frontier_certified_count =
                self.quality_frontier_certified_count.saturating_add(1);
            self.quality_certified_max_aggregates = self
                .quality_certified_max_aggregates
                .max(evidence.shape.aggregates);
            self.quality_certified_max_runtime_filters = self
                .quality_certified_max_runtime_filters
                .max(evidence.shape.runtime_filter_joins);
            self.quality_certified_max_aggregate_regions = self
                .quality_certified_max_aggregate_regions
                .max(aggregate_region_count);
            self.quality_certified_max_covered_aggregate_regions = self
                .quality_certified_max_covered_aggregate_regions
                .max(covered_aggregate_region_count);
            self.quality_certified_max_aggregate_witnesses = self
                .quality_certified_max_aggregate_witnesses
                .max(evidence.shape.aggregate_witness_nodes);
            self.quality_certified_max_join_witnesses = self
                .quality_certified_max_join_witnesses
                .max(evidence.shape.join_region_witness_nodes);
            if Some(reference.candidate) != leading_candidate {
                // This is intentionally observable: selecting a quality-ready
                // candidate from the Pareto frontier is not the same operation
                // as claiming that the leading model-cost candidate was ready.
                tracing::debug!(
                    target: targets::OPTIMIZER,
                    leading_candidate =
                        leading_candidate.map_or(u64::MAX, |candidate| candidate.index() as u64),
                    selected_candidate = reference.candidate.index(),
                    goal = ?goal,
                    "quality policy selected a non-leading published root candidate"
                );
            }
            self.quality_certificates.insert(goal, certificate);
            if self.collect_rule_work_profile {
                let frozen = &frozen_winner.frozen;
                self.note_candidate_lifecycle(CandidateLifecycleEvent {
                    stage: CandidateLifecycleStage::RootQualified,
                    elapsed_us: self.lifecycle_elapsed_us(),
                    group: root,
                    goal: Some(goal),
                    candidate: Some(reference.candidate),
                    source: None,
                    binding: None,
                    source_child: None,
                    logical: Some(frozen.logical.id),
                    physical: Some(frozen.physical.id),
                    recipe: Some(frozen.winner.physical_fingerprint),
                    rule: None,
                    children: frozen.winner.children.clone(),
                    facts: diagnostic_facts.unwrap_or_default(),
                    expected_cost_bits: Some(frozen.winner.cost.score.range.expected.to_bits()),
                    upper_cost_bits: Some(frozen.winner.cost.score.range.upper.to_bits()),
                });
            }
            self.quality_ready_winners.insert(goal, frozen_winner);
            if self
                .quality_required_goals
                .iter()
                .all(|required| self.quality_ready_winners.contains_key(required))
            {
                self.quality_handoff_reached = true;
                self.search_milestones.quality_policy_satisfied_us = self
                    .profile_elapsed_us()
                    .or_else(|| Some(self.memo.control().elapsed_us()));
                self.search_milestones.quality_policy_candidate = Some(reference.candidate);
            }
            break;
        }
        Ok(())
    }

    fn quality_class_for_goal(&self, goal: OptimizationGoal) -> ResourceGrantClassId {
        if let Some(class) = self.active_optional_grant {
            // Shared goals still need the active operating point's identity
            // at the handoff boundary. The full sharing proof remains in the
            // registry; this does not relabel a class-specific winner.
            if match goal.grant {
                GrantGoalKey::Invariant(_) => true,
                GrantGoalKey::Parallelism { tasks, .. } => tasks == class.max_parallel_tasks,
                GrantGoalKey::Class(id) => id == class.id,
            } {
                return class.id;
            }
        }
        match goal.grant {
            GrantGoalKey::Class(class) => class,
            GrantGoalKey::Parallelism { tasks, .. } => self
                .grant_classes
                .values()
                .find(|class| class.max_parallel_tasks == tasks)
                .map(|class| class.id)
                .or_else(|| self.grant_classes.keys().next().copied())
                .unwrap_or(ResourceGrantClassId(0)),
            GrantGoalKey::Invariant(_) => self
                .grant_classes
                .keys()
                .next()
                .copied()
                .unwrap_or(ResourceGrantClassId(0)),
        }
    }

    /// Re-evaluate the candidates that are actually present at the stop
    /// boundary.  The normal interleave evaluates on root frontier updates,
    /// but a final child publication can become visible through the frontier
    /// snapshot without another root callback.  Evaluating here closes the
    /// PReady -> FrozenCandidate handoff without doing any more search.
    fn evaluate_current_quality_candidates(
        &mut self,
        root: GroupId,
        base_goal: OptimizationGoal,
        admissible_set: AdmissibleGrantSetId,
        classes: &BTreeMap<ResourceGrantClassId, ResourceGrantClass>,
    ) -> Result<()> {
        if !self.quality_handoff_enabled || self.quality_handoff_reached {
            return Ok(());
        }
        let root = self.memo.canonical_group(root);
        let sensitivity = self.goal_grant_sensitivity(root, base_goal.required)?;
        for class in classes.values().copied() {
            let goal = OptimizationGoal {
                grant: sensitivity.goal_for(admissible_set, class),
                ..base_goal
            };
            self.try_quality_handoff_candidate(root, goal)?;
            if self.quality_handoff_reached {
                break;
            }
        }
        Ok(())
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
                self.protect_incumbent(root, goal, incumbent.clone())?;
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
            self.optional_search_started =
                self.collect_rule_work_profile || self.quality_handoff_enabled;
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
            if self.quality_handoff_reached {
                if let Some(winner) = self.quality_ready_winners.get(&goal).cloned() {
                    let stop = self.search_stop();
                    self.note_search_stop(stop);
                    return Ok(winner.winner.as_ref().clone());
                }
            }
        }
        self.optimize_group(root, goal)?;
        super::verifier::MemoVerifier::verify(&self.memo, None)?;
        self.diagnostic_search_complete = self.memo.search_obligations_empty();
        self.record_search_checkpoints(root);
        self.memo
            .group(root)
            .and_then(|group| group.winner(goal))
            .cloned()
            .or(incumbent)
            .ok_or_else(|| self.infeasible_goal_error(root, goal))
    }

    fn reset_cost_epoch(&mut self) -> Result<()> {
        self.quality_production_requests.clear();
        self.quality_forced_transform_bindings.clear();
        self.quality_active_forced_transform_binding = None;
        self.protect_current_winners()?;
        self.memo.clear_cost_frontiers()?;
        // Physical recipes are immutable descriptions of already-admitted
        // implementations. Keep them across a fact/cost epoch so only the
        // affected winner frontiers are recomposed; rebuilding every recipe
        // made a grant or logical refresh pay the same construction cost
        // again. New logical expressions still add recipes incrementally.
        self.infeasible_goals.clear();
        self.grant_sensitivity.clear();
        // The cost epoch is part of every child-combination context. A
        // completion proof from the previous epoch is therefore never a
        // valid lower-bound source after frontiers are cleared.
        self.physical_completion_proofs.clear();
        self.task_registry.invalidate_physical_tasks()?;
        self.physical_full_recost = self.next_recipe_sequence.keys().copied().collect();
        self.physical_quality_demanded_groups = self
            .physical_parents
            .keys()
            .copied()
            .map(|group| self.memo.canonical_group(group))
            .collect();
        self.physical_quality_scheduled_groups.clear();
        Ok(())
    }

    /// Save every currently selected winner before clearing the cost
    /// frontiers.  The physical archive remains the source of exact child
    /// payloads; this map only retains the immutable roots that can serve as
    /// conservative upper bounds during the next epoch.
    fn protect_current_winners(&mut self) -> Result<()> {
        if !self.protected_incumbent_enabled {
            return Ok(());
        }
        let winners = self
            .memo
            .groups()
            .flat_map(|group| {
                group
                    .winners()
                    .map(move |(goal, winner)| (group.id, *goal, winner.clone()))
            })
            .collect::<Vec<_>>();
        for (group, goal, winner) in winners {
            self.protect_incumbent(group, goal, winner)?;
        }
        Ok(())
    }

    /// Retain a winner together with the exact group-fact reads that feed its
    /// local and descendant cost facts.  An old winner is usable only while
    /// all of those reads remain current; logical alternatives which do not
    /// change the observed facts therefore keep the incumbent, while a merge
    /// or statistics refresh invalidates it fail-closed.
    fn protect_incumbent(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
        winner: Winner,
    ) -> Result<()> {
        if !self.protected_incumbent_enabled {
            return Ok(());
        }
        let reads = self.winner_fact_reads(group, &winner)?;
        self.protected_incumbents.insert(
            (self.memo.canonical_group(group), goal),
            ProtectedIncumbent {
                winner: Arc::new(winner),
                reads,
            },
        );
        Ok(())
    }

    fn priced_cost_context(
        &self,
        group: GroupId,
        goal: OptimizationGoal,
        plan_identity: Fingerprint,
        reads: &ReadSet,
    ) -> Result<CostContext> {
        let group = self.memo.canonical_group(group);
        self.memo
            .group(group)
            .ok_or_else(|| paro_error::internal("unknown group in priced incumbent context"))?;
        let mut builder = StableFingerprintBuilder::default();
        builder.write_bytes(b"paro.priced-incumbent-context.v2");
        builder.write_fingerprint(plan_identity);
        write_semantic_goal_fingerprint(&mut builder, &self.memo, goal, &self.grant_classes)?;
        builder.write_fingerprint(self.memo.calibration().stable_fingerprint());
        builder.write_u64(reads.reads().len() as u64);
        for read in reads.reads() {
            builder.write_u64(u64::from(read.logical_frontier_revision.is_some()));
            builder.write_u64(u64::from(read.physical_frontier_revision.is_some()));
            builder.write_fingerprint(read.logical_fact_fingerprint);
            builder.write_fingerprint(read.statistics_snapshot_fingerprint);
        }
        Ok(CostContext {
            fingerprint: builder.finish(),
            reads: reads.clone(),
        })
    }

    fn winner_fact_reads(&self, group: GroupId, winner: &Winner) -> Result<ReadSet> {
        let mut pending = vec![(self.memo.canonical_group(group), winner)];
        let mut visited_candidates = BTreeSet::new();
        let mut groups = BTreeSet::new();
        while let Some((owner, winner)) = pending.pop() {
            if !visited_candidates.insert(winner.candidate) {
                continue;
            }
            groups.insert(self.memo.canonical_group(owner));
            for child in winner.children.iter().copied() {
                let child_owner = self.memo.canonical_group(child.group);
                let child_winner = self.memo.resolve_child_winner(child).ok_or_else(|| {
                    paro_error::internal("protected incumbent references an unknown child")
                })?;
                pending.push((child_owner, child_winner));
            }
        }
        groups
            .into_iter()
            .map(|group| PatternRead::facts_from_group(&self.memo, group))
            .collect::<Result<Vec<_>>>()
            .map(ReadSet::new)
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
        self.optimize_grant_portfolio(root, base_goal, admissible_set, classes, mode, None)
    }

    /// Production lazy entry point. None means that no declared operating
    /// point fits the frozen availability; all classes still get P_safe.
    pub fn optimize_for_expected_grant(
        &mut self,
        root: GroupId,
        base_goal: OptimizationGoal,
        admissible_set: AdmissibleGrantSetId,
        classes: impl IntoIterator<Item = ResourceGrantClass>,
        mode: SearchMode,
        expected_class: Option<ResourceGrantClassId>,
    ) -> Result<GrantOptimization> {
        self.optimize_grant_portfolio(
            root,
            base_goal,
            admissible_set,
            classes,
            mode,
            Some(expected_class),
        )
    }

    fn optimize_grant_portfolio(
        &mut self,
        root: GroupId,
        base_goal: OptimizationGoal,
        admissible_set: AdmissibleGrantSetId,
        classes: impl IntoIterator<Item = ResourceGrantClass>,
        mode: SearchMode,
        expected: Option<Option<ResourceGrantClassId>>,
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
        if expected
            .flatten()
            .is_some_and(|id| !classes.contains_key(&id))
        {
            return Err(paro_error::internal(
                "expected grant is not a declared class",
            ));
        }
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
        self.grant_classes.clone_from(&classes);
        // Make the actual multi-grant entry point own the same operating
        // context used by SeedPlan re-pricing.  The planner primes this
        // before installation as well, but direct engine callers must not
        // receive a different seed-validity contract.
        self.prime_grant_context(classes.values().copied())?;
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
                if let Some(optimization) = incumbent.as_ref().ok() {
                    for winner in &optimization.winners {
                        self.protect_incumbent(root, winner.goal, winner.winner.as_ref().clone())?;
                    }
                    if let Some(winner) = optimization.winners.first() {
                        self.governor.mark_safe(winner.winner.candidate);
                        self.note_safe_candidate(winner.winner.candidate);
                    }
                }
            }
            let safe = incumbent.as_ref().ok().cloned();
            if expected.is_some()
                && safe
                    .as_ref()
                    .is_none_or(|safe| safe.winners.len() != classes.len())
            {
                return Err(paro_error::internal(
                    "lazy grant portfolio requires an exact mandatory winner for every class",
                ));
            }
            if expected == Some(None) {
                let mut safe = incumbent?;
                for class in classes.keys().copied() {
                    self.memo.record_deferred_grant(class);
                }
                self.diagnostic_search_complete = false;
                safe.stop = self.search_stop();
                self.note_search_stop(safe.stop);
                self.record_search_checkpoints(root);
                safe.safe_winners = safe.winners.clone();
                safe.grant_search = Some(crate::physical::GrantSearchCoverage::new(
                    None,
                    classes.keys().copied(),
                    false,
                ));
                return Ok(safe);
            }
            let active_classes = classes
                .iter()
                .filter(|(id, _)| expected.is_none_or(|selected| selected == Some(**id)))
                .map(|(id, class)| (*id, *class))
                .collect::<BTreeMap<_, _>>();
            let prior_quality_goals = self.quality_required_goals.clone();
            if expected.flatten().is_some() {
                let sensitivity = self.goal_grant_sensitivity(root, base_goal.required)?;
                self.quality_required_goals = active_classes
                    .values()
                    .map(|class| OptimizationGoal {
                        grant: sensitivity.goal_for(admissible_set, *class),
                        ..base_goal
                    })
                    .collect();
            }
            // There must be no fallible early return between installing this
            // temporary restriction and clearing it below.
            self.active_optional_grant = expected.flatten().map(|id| classes[&id]);
            let mut optional_started = false;
            let result = self.optimize_optional_grants(
                root,
                base_goal,
                admissible_set,
                &active_classes,
                incumbent,
                &mut optional_started,
            );
            self.active_optional_grant = None;
            // Restore the full declared class registry even on cancellation or
            // failed optional staging; the immutable mandatory DAGs own fallback.
            self.grant_classes.clone_from(&classes);
            self.grant_class_sets = classes.keys().map(|id| (*id, admissible_set)).collect();
            if result.is_err() {
                self.quality_required_goals = prior_quality_goals;
                self.quality_production_requests.clear();
                self.quality_forced_transform_bindings.clear();
                self.quality_active_forced_transform_binding = None;
            }
            return result.map(|mut result| {
                if let Some(expected_class) = expected {
                    let safe = safe.expect("complete mandatory portfolio checked above");
                    let mut winners = result.winners.into_vec();
                    winners.retain(|winner| Some(winner.class) == expected_class);
                    for winner in &safe.winners {
                        if !winners
                            .iter()
                            .any(|selected| selected.class == winner.class)
                        {
                            winners.push(winner.clone());
                        }
                    }
                    winners.sort_by_key(|winner| winner.class);
                    result.winners = winners.into_boxed_slice();
                    result.safe_winners = safe.winners;
                    result.grant_search = Some(crate::physical::GrantSearchCoverage::new(
                        expected_class,
                        classes.keys().copied(),
                        optional_started,
                    ));
                    // Active-goal closure is not closure of the declared
                    // portfolio. Retaining a shared mandatory goal alone is
                    // not a proof that every class completed optional work.
                    for class in &result.grant_search.as_ref().unwrap().mandatory_only_classes {
                        self.memo.record_deferred_grant(*class);
                    }
                    self.diagnostic_search_complete = self.memo.search_obligations_empty();
                    result.stop = self.search_stop();
                    self.note_search_stop(result.stop);
                    self.record_search_checkpoints(root);
                }
                result
            });
        }
        let mut result = self.optimize_grant_classes(root, base_goal, admissible_set, &classes)?;
        if let Some(expected_class) = expected {
            if result.winners.len() != classes.len() {
                return Err(paro_error::internal(
                    "lazy grant portfolio requires all mandatory classes",
                ));
            }
            result.safe_winners = result.winners.clone();
            result.grant_search = Some(crate::physical::GrantSearchCoverage::new(
                expected_class,
                classes.keys().copied(),
                false,
            ));
            for class in classes.keys().copied() {
                self.memo.record_deferred_grant(class);
            }
            result.stop = self.search_stop();
        }
        self.diagnostic_search_complete = self.memo.search_obligations_empty();
        self.note_search_stop(self.search_stop());
        self.record_search_checkpoints(root);
        Ok(result)
    }

    fn optimize_optional_grants(
        &mut self,
        root: GroupId,
        base_goal: OptimizationGoal,
        admissible_set: AdmissibleGrantSetId,
        classes: &BTreeMap<ResourceGrantClassId, ResourceGrantClass>,
        incumbent: Result<GrantOptimization>,
        optional_started: &mut bool,
    ) -> Result<GrantOptimization> {
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
        *optional_started = true;
        self.optional_search_started =
            self.collect_rule_work_profile || self.quality_handoff_enabled;
        let sensitivity = self.goal_grant_sensitivity(root, base_goal.required)?;
        let goals = classes.values().copied().map(|class| OptimizationGoal {
            grant: sensitivity.goal_for(admissible_set, class),
            ..base_goal
        });
        self.explore_transformations_with_interleave(Some(PhysicalInterleave::new(root, goals)))?;
        self.record_search_checkpoints(root);
        if self.quality_handoff_reached {
            if let Some(mut snapshot) =
                self.quality_grant_snapshot(root, base_goal, admissible_set, classes)?
            {
                let stop = self.search_stop();
                snapshot.stop = stop;
                self.note_search_stop(stop);
                return Ok(snapshot);
            }
        }
        if !self.memo.control().checkpoint()? {
            self.record_search_checkpoints(root);
            return self.stop_with_snapshot_or_fallback(
                root,
                base_goal,
                admissible_set,
                classes,
                incumbent,
            );
        }
        let result = self.optimize_grant_classes(root, base_goal, admissible_set, classes);
        if result
            .as_ref()
            .is_err_and(|error| error.is_query_canceled())
        {
            return result;
        }
        if self.memo.control().deadline_reached() {
            self.record_search_checkpoints(root);
            let fallback = result.or_else(|_| incumbent);
            return self.stop_with_snapshot_or_fallback(
                root,
                base_goal,
                admissible_set,
                classes,
                fallback,
            );
        }
        if result.is_ok() {
            self.diagnostic_search_complete = self.memo.search_obligations_empty();
            self.note_search_stop(self.search_stop());
        }
        self.record_search_checkpoints(root);
        result
    }

    fn quality_grant_snapshot(
        &mut self,
        root: GroupId,
        base_goal: OptimizationGoal,
        admissible_set: AdmissibleGrantSetId,
        classes: &BTreeMap<ResourceGrantClassId, ResourceGrantClass>,
    ) -> Result<Option<GrantOptimization>> {
        let root = self.memo.canonical_group(root);
        let sensitivity = self.goal_grant_sensitivity(root, base_goal.required)?;
        let mut winners = Vec::with_capacity(classes.len());
        for class in classes.values().copied() {
            let goal = OptimizationGoal {
                grant: sensitivity.goal_for(admissible_set, class),
                ..base_goal
            };
            let Some(winner) = self.quality_ready_winners.get(&goal).cloned() else {
                return Ok(None);
            };
            if winner.class != class.id {
                return Ok(None);
            }
            winners.push(winner);
        }
        if winners.is_empty() {
            return Ok(None);
        }
        Ok(Some(GrantOptimization {
            grant_search: None,
            safe_winners: Box::new([]),
            sensitivity,
            winners: winners.into_boxed_slice(),
            stop: self.search_stop(),
        }))
    }

    fn optimize_grant_classes(
        &mut self,
        root: GroupId,
        base_goal: OptimizationGoal,
        admissible_set: AdmissibleGrantSetId,
        classes: &BTreeMap<ResourceGrantClassId, ResourceGrantClass>,
    ) -> Result<GrantOptimization> {
        // Optional scheduling narrows work, not the declared grant domain or
        // the context used to derive shared/RequiredEnforcement child goals.
        if self.active_optional_grant.is_none() {
            self.grant_class_sets.clear();
            self.grant_class_sets
                .extend(classes.keys().copied().map(|class| (class, admissible_set)));
            self.grant_classes.clone_from(classes);
        }
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
            grant_search: None,
            safe_winners: Box::new([]),
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
            grant_search: None,
            safe_winners: Box::new([]),
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
        self.evaluate_current_quality_candidates(root, base_goal, admissible_set, classes)?;
        // The stop reason must be sampled after the boundary evaluation. A
        // frontier publication can make all grants PReady without running
        // another search task; reporting the earlier Deadline here would
        // hide a real quality-policy handoff as a timeout.
        let stop = self.search_stop();
        // If the stop-boundary evaluation certified every required grant,
        // return those exact frozen winners. Re-freezing the frontier below
        // would preserve the candidate id in practice, but would weaken the
        // producer-to-consumer identity guarantee of this handoff.
        if self.quality_handoff_reached {
            if let Some(mut snapshot) =
                self.quality_grant_snapshot(root, base_goal, admissible_set, classes)?
            {
                snapshot.stop = stop;
                self.note_search_stop(stop);
                return Ok(snapshot);
            }
        }
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
        if self.quality_handoff_enabled && self.optional_search_started {
            self.physical_quality_demanded_groups.insert(child);
        }
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
            // A logical publication can change the local implementation set
            // without changing an already-indexed child frontier.  Queue the
            // exact physical goals that have actually been observed for this
            // group before walking its ancestors.  Relying on the root queue
            // alone delays a new branch implementation until a later parent
            // recursion, which is precisely the gap between a published
            // aggregate choice and a root-consumable quality candidate.
            if let Some(goals) = self.physical_goals.get(&group).cloned() {
                for goal in goals {
                    interleave.pending.insert((group, goal));
                    self.infeasible_goals.remove(&(group, goal));
                }
            }
            if group == interleave.root {
                let root_goals = interleave.goals.clone();
                for goal in root_goals {
                    interleave.pending.insert((group, goal));
                    self.infeasible_goals.remove(&(group, goal));
                }
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
                // The parent owns the observable response. Its recursive
                // physical pass will pull the changed child goal on demand;
                // queueing both sides made every publication pay a separate
                // readiness visit for a child that can never be returned by
                // this interleave. The root remains explicitly queued above.
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
            if self
                .active_optional_grant
                .is_some_and(|class| match goal.grant {
                    GrantGoalKey::Class(id) => id != class.id,
                    GrantGoalKey::Parallelism { tasks, .. } => tasks != class.max_parallel_tasks,
                    GrantGoalKey::Invariant(_) => false,
                })
            {
                continue;
            }
            if self.quality_handoff_reached {
                break;
            }
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
            self.physical_interleave_step_mode = true;
            self.physical_interleave_step_yielded = false;
            self.physical_interleave_step_publications = 0;
            let result = self.optimize_group(group, goal);
            let yielded = self.physical_interleave_step_yielded;
            self.physical_interleave_step_mode = false;
            self.mandatory_only = previous_mandatory_only;
            self.preserve_incomplete_physical = previous_preserve_incomplete;
            result?;
            if yielded {
                // A publication inside the task may have made a registered
                // parent recipe consumable. Requeue the exact task and walk
                // only its indexed physical ancestors; the next step resumes
                // from the recipe/combination cursor rather than rescanning
                // the whole child frontier.
                interleave.pending.insert((group, goal));
                self.enqueue_physical_ancestors([group], interleave);
            }
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
        if self.quality_handoff_enabled {
            if let Some(interleave) = interleave.as_ref() {
                self.schedule_quality_bootstrap(interleave.root, &mut agenda)?;
            }
        }
        self.schedule_demanded_physical_quality_groups(&mut agenda)?;
        'tasks: while let Some(task) = self.pop_transformation_task(&mut agenda)? {
            if self.quality_handoff_reached {
                break;
            }
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
            let normal_task_id = TransformationTaskId {
                group,
                expression,
                rule,
                binding: None,
            };
            let forced_binding = self
                .quality_active_forced_transform_binding
                .take()
                .map(|(_, binding)| binding);
            let task_id = forced_binding
                .as_ref()
                .map(|binding| TransformationTaskId {
                    binding: Some(binding.fingerprint),
                    ..normal_task_id
                })
                .unwrap_or(normal_task_id);
            self.note_transformation_task(
                task_id,
                TransformationTaskLifecyclePhase::FirstRun,
                None,
                None,
                0,
            );
            self.note_rule_phase(rule, RuleWorkPhase::FirstRun);
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
                self.note_transformation_task(
                    task_id,
                    TransformationTaskLifecyclePhase::BudgetRejected,
                    None,
                    None,
                    1,
                );
                continue;
            }
            // Rule implementations expose an allocation-free root dispatch
            // predicate.  Run it before scoped pattern enumeration: the
            // latter may walk every descendant frontier even when the
            // immutable root operator can never match.  A false result is a
            // proof that descendant changes cannot make this exact
            // expression applicable, so an empty observation is sufficient
            // and does not subscribe the task to unrelated child groups.
            let root_dispatch = {
                let rule_impl = self
                    .registry
                    .transformation(rule)
                    .ok_or_else(|| paro_error::internal("rule disappeared from registry"))?;
                let logical = self.memo.logical_expr(expression).ok_or_else(|| {
                    paro_error::internal("rule task references unknown expression")
                })?;
                let context = RuleContext {
                    memo: &self.memo,
                    group,
                };
                rule_impl.root_dispatch(logical, &context)?
            };
            if !root_dispatch.matches {
                self.seed_transformation_observation(task_id, &root_dispatch.reads)?;
                continue;
            }
            // A task produced by this same rule inherits the exact read cursor
            // of the binding which produced it. Avoid rebuilding its pattern
            // closure until one of those reads advances. This is an
            // incremental-work cursor, not a provenance match guard: any
            // relevant child/fact revision invalidates it and makes the new
            // expression eligible for ordinary matching.
            if forced_binding.is_none() && self.transformation_observation_is_current(task_id)? {
                continue;
            }
            let binding_started = Instant::now();
            let binding_allocated = paro_common::allocator::thread_allocated_bytes();
            let mut binding_set = if let Some(binding) = forced_binding {
                let reads = pattern_binding_fact_reads(&self.memo, &binding)?;
                self.quality_direct_binding_work_units = self
                    .quality_direct_binding_work_units
                    .saturating_add(pattern_operand_work_units(&binding.root) as u64);
                PatternBindingSet {
                    bindings: Box::new([binding.clone()]),
                    reads,
                    work_units: pattern_operand_work_units(&binding.root),
                    work_dimension,
                    completion: PatternEnumerationCompletion::Complete,
                }
            } else {
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
            // Binding construction has now observed the exact child/fact
            // reads required by this task.  Record this separately from
            // enqueue and first-run: a queued task can still wait on an
            // advancing child frontier, and a matcher can discover no binding.
            self.note_rule_phase(rule, RuleWorkPhase::DependenciesReady);
            self.note_transformation_task(
                task_id,
                TransformationTaskLifecyclePhase::DependenciesReady,
                Some(&binding_set.reads),
                None,
                0,
            );
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
                self.note_transformation_task(
                    task_id,
                    TransformationTaskLifecyclePhase::NoMatch,
                    Some(&binding_set.reads),
                    None,
                    1,
                );
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
            self.note_transformation_task(
                task_id,
                TransformationTaskLifecyclePhase::Matched,
                Some(&binding_set.reads),
                binding_set
                    .bindings
                    .first()
                    .map(|binding| binding.fingerprint),
                binding_set.bindings.len() as u64,
            );
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
                    self.note_transformation_task(
                        task_id,
                        TransformationTaskLifecyclePhase::BudgetRejected,
                        Some(&application_reads),
                        Some(binding.fingerprint),
                        1,
                    );
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
                let task_lifecycle_enabled = self.collect_rule_work_profile
                    && self
                        .registry
                        .transformation(rule)
                        .is_some_and(|rule| rule.quality_dependency().is_some());
                let task_lifecycle_started_at = self.profile_started_at;
                let mut context = TransformContext::new(&mut self.memo, group);
                context.rejection_reasons = self
                    .collect_rule_work_profile
                    .then(crate::transformation_rejection::RejectionReasons::default);
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
                // Lifecycle diagnostics must retain the discovery frontier as
                // well as application facts.  The frontier is the cursor
                // which explains a later reactivation; recording only the
                // narrower binding facts makes an early subscriber look like
                // an unrelated late task.  This allocation is diagnostic
                // only, just like the surrounding task profile.
                let lifecycle_reads = task_lifecycle_enabled.then(|| {
                    ReadSet::new(
                        binding_set
                            .reads
                            .iter()
                            .copied()
                            .chain(application_reads.iter().copied()),
                    )
                });
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
                            let mut reasons = context.rejection_reasons.unwrap_or_default();
                            reasons.record(crate::transformation_rejection::TransformationRejectionGuard::ApplicationError);
                            self.rule_work_profile
                                .entry(rule)
                                .or_default()
                                .rejection_guards
                                .record(reasons);
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
                        let mut reasons = context.rejection_reasons.unwrap_or_default();
                        reasons.record(
                            crate::transformation_rejection::TransformationRejectionGuard::NoOutput,
                        );
                        self.rule_work_profile
                            .entry(rule)
                            .or_default()
                            .rejection_guards
                            .record(reasons);
                        self.rule_work_profile.entry(rule).or_default().rejected = self
                            .rule_work_profile
                            .get(&rule)
                            .map_or(1, |profile| profile.rejected.saturating_add(1));
                    }
                    context.rollback()?;
                    let mut observed = binding_set.reads.to_vec();
                    observed.extend(application_reads.iter().copied());
                    self.seed_transformation_observation(task_id, &observed)?;
                    self.note_transformation_task(
                        task_id,
                        TransformationTaskLifecyclePhase::NoOutput,
                        lifecycle_reads.as_ref().map(ReadSet::reads),
                        Some(binding.fingerprint),
                        1,
                    );
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
                        let mut reasons =
                            crate::transformation_rejection::RejectionReasons::default();
                        reasons.record(crate::transformation_rejection::TransformationRejectionGuard::OutputContract);
                        self.rule_work_profile
                            .entry(rule)
                            .or_default()
                            .rejection_guards
                            .record(reasons);
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
                if task_lifecycle_enabled {
                    Self::note_transformation_task_at(
                        &mut self.search_milestones,
                        task_id,
                        TransformationTaskLifecyclePhase::Applicable,
                        lifecycle_reads.as_ref().map(ReadSet::reads),
                        Some(binding.fingerprint),
                        1,
                        Self::lifecycle_elapsed_from(task_lifecycle_started_at),
                    );
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
                            let mut reasons =
                                crate::transformation_rejection::RejectionReasons::default();
                            reasons.record(crate::transformation_rejection::TransformationRejectionGuard::PublicationError);
                            self.rule_work_profile
                                .entry(rule)
                                .or_default()
                                .rejection_guards
                                .record(reasons);
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
                    if task_lifecycle_enabled {
                        Self::note_transformation_task_at(
                            &mut self.search_milestones,
                            task_id,
                            TransformationTaskLifecyclePhase::Published,
                            lifecycle_reads.as_ref().map(ReadSet::reads),
                            Some(binding.fingerprint),
                            newly_inserted_expressions.len() as u64,
                            Self::lifecycle_elapsed_from(task_lifecycle_started_at),
                        );
                    }
                    let (appended_groups, locally_written_groups) = context.commit()?;
                    let locally_written_groups = locally_written_groups
                        .into_iter()
                        .map(|group| self.memo.canonical_group(group))
                        .collect::<BTreeSet<_>>();
                    let changed_cte_readers = self.memo.take_changed_cte_readers();
                    self.note_logical_publication();
                    if self.collect_rule_work_profile {
                        let facts = ReadSet::new(
                            binding_set
                                .reads
                                .iter()
                                .copied()
                                .chain(application_reads.iter().copied()),
                        )
                        .reads()
                        .to_vec()
                        .into_boxed_slice();
                        let elapsed_us = self.lifecycle_elapsed_us();
                        for (target, inserted) in newly_inserted_expressions.iter().copied() {
                            self.note_candidate_lifecycle(CandidateLifecycleEvent {
                                stage: CandidateLifecycleStage::LogicalPublished,
                                elapsed_us,
                                group: target,
                                goal: None,
                                candidate: None,
                                source: Some(expression),
                                binding: Some(binding.fingerprint),
                                source_child: match &binding.root {
                                    PatternOperand::Expression { children, .. } => {
                                        children.first().and_then(PatternOperand::expression)
                                    }
                                    PatternOperand::Group(_) => None,
                                },
                                logical: Some(inserted),
                                physical: None,
                                recipe: None,
                                rule: Some(rule),
                                children: Box::new([]),
                                facts: facts.clone(),
                                expected_cost_bits: None,
                                upper_cost_bits: None,
                            });
                        }
                    }
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
                                    binding: None,
                                },
                                &inherited_reads,
                            )?;
                        }
                    }
                    inserted_groups.extend(appended_groups.iter().copied());
                    inserted_groups.extend(changed_cte_readers.iter().copied());
                    let promote_quality_followups = self.quality_handoff_enabled
                        && self
                            .registry
                            .transformation(rule)
                            .is_some_and(|rule| rule.quality_dependency().is_some());
                    for (owner, inserted) in newly_inserted_expressions {
                        self.schedule_transformation_expression(
                            owner,
                            inserted,
                            &mut agenda,
                            promote_quality_followups,
                            false,
                        )?;
                    }
                    for appended in appended_groups.iter().copied() {
                        self.schedule_transformations_with_lane(
                            appended,
                            &mut agenda,
                            promote_quality_followups,
                        )?;
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
                    self.schedule_demanded_physical_quality_groups(&mut agenda)?;
                }
                effective_insertions_since_recost = 0;
            }
        }
        if !self.quality_handoff_reached {
            if let Some(interleave) = interleave.as_mut() {
                // A tail smaller than INTERLEAVE_BATCH must still become visible
                // before the final grant extraction. This is an incremental drain,
                // not another whole-root exploration.
                self.drain_physical_interleave(interleave)?;
                self.schedule_demanded_physical_quality_groups(&mut agenda)?;
            }
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

    pub fn search_work_counters(&mut self) -> BTreeMap<&'static str, u64> {
        let task_profile = self.task_registry.profile();
        let mut strong_incumbent_active_count = 0_u64;
        let mut first_invalid = None;
        for ((group, goal), incumbent) in &self.strong_incumbents {
            let reads_current = incumbent
                .context
                .reads
                .is_current(&self.memo)
                .is_ok_and(|current| current);
            let valid = reads_current
                && self
                    .priced_cost_context(
                        *group,
                        *goal,
                        incumbent.plan.identity,
                        &incumbent.context.reads,
                    )
                    .ok()
                    .is_some_and(|context| context.fingerprint == incumbent.context.fingerprint);
            if valid {
                strong_incumbent_active_count = strong_incumbent_active_count.saturating_add(1);
            } else if first_invalid.is_none() {
                first_invalid = Some((
                    if reads_current { 2 } else { 1 },
                    Some(incumbent.context.fingerprint),
                ));
            }
        }
        if let Some((reason, fingerprint)) = first_invalid {
            self.note_strong_incumbent_invalidation(reason, fingerprint);
        }
        let mut counters = BTreeMap::from([
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
            ("diagnostic_cost_kernel_ns", self.diagnostic_cost_phase_times.as_ref().map_or(0, |t| t.0[0].load(std::sync::atomic::Ordering::Relaxed))),
            ("diagnostic_candidate_admission_ns", self.diagnostic_cost_phase_times.as_ref().map_or(0, |t| t.0[1].load(std::sync::atomic::Ordering::Relaxed))),
            (
                "child_combination_frontier_recheck_count",
                self.child_combination_frontier_recheck_count,
            ),
            (
                "child_combination_budget_rejection_count",
                self.child_combination_budget_rejection_count,
            ),
            (
                "quality_policy_handoff_enabled",
                u64::from(self.quality_handoff_enabled),
            ),
            (
                "quality_producer_dispatch_count",
                self.quality_producer_dispatch_count,
            ),
            (
                "quality_direct_binding_dispatch_count",
                self.quality_direct_binding_dispatch_count,
            ),
            (
                "quality_direct_binding_first_us",
                self.quality_direct_binding_first_us.unwrap_or_default(),
            ),
            (
                "quality_direct_binding_last_us",
                self.quality_direct_binding_last_us.unwrap_or_default(),
            ),
            (
                "quality_direct_binding_work_units",
                self.quality_direct_binding_work_units,
            ),
            (
                "quality_policy_candidate_evaluation_count",
                self.quality_candidate_evaluation_count,
            ),
            (
                "quality_policy_ready_goal_count",
                self.quality_ready_winners.len() as u64,
            ),
            (
                "quality_policy_missing_evidence_count",
                self.quality_candidate_missing_evidence_count,
            ),
            (
                "quality_policy_frontier_candidate_count",
                self.quality_frontier_candidate_count,
            ),
            (
                "quality_policy_frontier_candidate_skip_count",
                self.quality_frontier_candidate_skip_count,
            ),
            (
                "quality_policy_frontier_certified_count",
                self.quality_frontier_certified_count,
            ),
            (
                "quality_policy_frontier_policy_rejection_count",
                self.quality_frontier_policy_rejection_count,
            ),
            (
                "quality_policy_frontier_max_aggregates",
                u64::from(self.quality_frontier_max_aggregates),
            ),
            (
                "quality_policy_frontier_max_runtime_filters",
                u64::from(self.quality_frontier_max_runtime_filters),
            ),
            (
                "quality_policy_frontier_max_aggregate_regions",
                u64::from(self.quality_frontier_max_aggregate_regions),
            ),
            (
                "quality_policy_frontier_max_covered_aggregate_regions",
                u64::from(self.quality_frontier_max_covered_aggregate_regions),
            ),
            (
                "quality_policy_frontier_first_aggregate_region_us",
                self.quality_frontier_first_aggregate_region_us
                    .unwrap_or(u64::MAX),
            ),
            (
                "quality_policy_frontier_first_aggregate_candidate",
                self.quality_frontier_first_aggregate_candidate
                    .map_or(u64::MAX, |candidate| candidate.index() as u64),
            ),
            (
                "quality_policy_frontier_first_incomplete_aggregate_region_us",
                self.quality_frontier_first_incomplete_aggregate_region_us
                    .unwrap_or(u64::MAX),
            ),
            (
                "quality_policy_frontier_first_incomplete_aggregate_candidate",
                self.quality_frontier_first_incomplete_aggregate_candidate
                    .map_or(u64::MAX, |candidate| candidate.index() as u64),
            ),
            (
                "quality_policy_frontier_first_incomplete_aggregate_anchor",
                self.quality_frontier_first_incomplete_aggregate_anchor
                    .map_or(u64::MAX, |candidate| candidate.index() as u64),
            ),
            (
                "quality_policy_frontier_first_incomplete_aggregate_covered",
                u64::from(self.quality_frontier_first_incomplete_aggregate_covered),
            ),
            (
                "quality_policy_frontier_first_incomplete_aggregate_total",
                u64::from(self.quality_frontier_first_incomplete_aggregate_total),
            ),
            (
                "quality_policy_frontier_first_complete_aggregate_region_us",
                self.quality_frontier_first_complete_aggregate_region_us
                    .unwrap_or(u64::MAX),
            ),
            (
                "quality_policy_frontier_first_complete_aggregate_candidate",
                self.quality_frontier_first_complete_aggregate_candidate
                    .map_or(u64::MAX, |candidate| candidate.index() as u64),
            ),
            (
                "quality_policy_frontier_max_aggregate_witnesses",
                u64::from(self.quality_frontier_max_aggregate_witnesses),
            ),
            (
                "quality_policy_frontier_max_join_witnesses",
                u64::from(self.quality_frontier_max_join_witnesses),
            ),
            (
                "quality_policy_certified_max_aggregates",
                u64::from(self.quality_certified_max_aggregates),
            ),
            (
                "quality_policy_certified_max_runtime_filters",
                u64::from(self.quality_certified_max_runtime_filters),
            ),
            (
                "quality_policy_certified_max_aggregate_regions",
                u64::from(self.quality_certified_max_aggregate_regions),
            ),
            (
                "quality_policy_certified_max_covered_aggregate_regions",
                u64::from(self.quality_certified_max_covered_aggregate_regions),
            ),
            (
                "quality_policy_certified_max_aggregate_witnesses",
                u64::from(self.quality_certified_max_aggregate_witnesses),
            ),
            (
                "quality_policy_certified_max_join_witnesses",
                u64::from(self.quality_certified_max_join_witnesses),
            ),
            (
                "quality_last_evaluation_candidate",
                self.quality_last_evaluation_candidate
                    .map_or(u64::MAX, |candidate| candidate.index() as u64),
            ),
            (
                "quality_last_evaluation_goal_required",
                self.quality_last_evaluation_goal
                    .map_or(u64::MAX, |goal| goal.required.0 as u64),
            ),
            (
                "quality_last_evaluation_goal_grant",
                self.quality_last_evaluation_goal
                    .map_or(u64::MAX, |goal| goal.grant.stable_tag()),
            ),
            (
                "quality_last_evaluation_goal_context",
                self.quality_last_evaluation_goal
                    .map_or(u64::MAX, |goal| goal.context.0 as u64),
            ),
            (
                "quality_last_completed_bundle_count",
                self.quality_last_evaluation.completed,
            ),
            (
                "quality_last_not_applicable_bundle_count",
                self.quality_last_evaluation.not_applicable,
            ),
            (
                "quality_last_missing_bundle_count",
                self.quality_last_evaluation.missing_evidence,
            ),
            (
                "quality_last_suspended_bundle_count",
                self.quality_last_evaluation.suspended,
            ),
            (
                "quality_last_missing_fact_count",
                self.quality_last_evaluation.missing_facts,
            ),
            (
                "quality_policy_satisfied",
                u64::from(self.quality_handoff_reached),
            ),
            (
                "certified_bound_check_count",
                self.certified_bound_check_count,
            ),
            (
                "certified_recipe_prune_count",
                self.certified_recipe_prune_count,
            ),
            (
                "certified_group_pruning_enabled",
                u64::from(self.certified_group_pruning_enabled),
            ),
            (
                "protected_incumbent_enabled",
                u64::from(self.protected_incumbent_enabled),
            ),
            (
                "strong_incumbent_seed_count",
                self.strong_incumbents.len() as u64,
            ),
            (
                "strong_incumbent_active_count",
                strong_incumbent_active_count,
            ),
            (
                "certified_bound_no_incumbent_count",
                self.certified_bound_diagnostics.no_incumbent,
            ),
            (
                "certified_bound_local_interval_uncertain_count",
                self.certified_bound_diagnostics.local_interval_uncertain,
            ),
            (
                "certified_bound_child_completion_missing_count",
                self.certified_bound_diagnostics.child_completion_missing,
            ),
            (
                "certified_bound_source_response_unsupported_count",
                self.certified_bound_diagnostics.source_response_unsupported,
            ),
            (
                "certified_bound_phase_overlap_unsupported_count",
                self.certified_bound_diagnostics.phase_overlap_unsupported,
            ),
            (
                "certified_bound_available_not_tight_count",
                self.certified_bound_diagnostics.available_not_tight,
            ),
            (
                "certified_bound_pruned_before_children_count",
                self.certified_bound_diagnostics.pruned_before_children,
            ),
            (
                "certified_bound_pruned_after_children_count",
                self.certified_bound_diagnostics.pruned_after_children,
            ),
            (
                "certified_bound_compute_us",
                self.certified_bound_diagnostics.compute_us,
            ),
            (
                "certified_bound_validation_us",
                self.certified_bound_diagnostics.validation_us,
            ),
            (
                "certified_bound_invalidation_count",
                self.certified_bound_diagnostics.invalidation_count,
            ),
            (
                "strong_incumbent_lookup_count",
                self.strong_incumbent_lookup_count,
            ),
            (
                "strong_incumbent_bound_request_count",
                self.strong_incumbent_bound_request_count,
            ),
            (
                "strong_incumbent_valid_lookup_count",
                self.strong_incumbent_valid_lookup_count,
            ),
            (
                "strong_incumbent_invalid_lookup_count",
                self.strong_incumbent_invalid_lookup_count,
            ),
            (
                "strong_incumbent_missing_key_count",
                self.strong_incumbent_missing_key_count,
            ),
            (
                "strong_incumbent_lookup_group_mismatch_count",
                self.strong_incumbent_lookup_group_mismatch_count,
            ),
            (
                "strong_incumbent_lookup_goal_mismatch_count",
                self.strong_incumbent_lookup_goal_mismatch_count,
            ),
            (
                "strong_incumbent_lookup_unrelated_key_count",
                self.strong_incumbent_lookup_unrelated_key_count,
            ),
            (
                "strong_incumbent_fact_invalid_count",
                self.strong_incumbent_fact_invalid_count,
            ),
            (
                "strong_incumbent_context_invalid_count",
                self.strong_incumbent_context_invalid_count,
            ),
            (
                "strong_incumbent_source_response_bypass_count",
                self.strong_incumbent_source_response_bypass_count,
            ),
            (
                "strong_incumbent_phase_overlap_bypass_count",
                self.strong_incumbent_phase_overlap_bypass_count,
            ),
            (
                "strong_incumbent_selected_for_bound_count",
                self.strong_incumbent_selected_for_bound_count,
            ),
            (
                "strong_incumbent_install_count",
                self.strong_incumbent_install_count,
            ),
            (
                "strong_incumbent_installed_read_count",
                self.strong_incumbent_installed_read_count,
            ),
            (
                "strong_incumbent_merge_invalidation_count",
                self.strong_incumbent_merge_invalidation_count,
            ),
            (
                "strong_incumbent_goal_mismatch_count",
                self.strong_incumbent_goal_mismatch_count,
            ),
            (
                "strong_incumbent_reprice_rejection_count",
                self.strong_incumbent_reprice_rejection_count,
            ),
            (
                "strong_incumbent_reprice_no_destination_recipe_count",
                self.strong_incumbent_reprice_no_destination_recipe_count,
            ),
            (
                "strong_incumbent_reprice_child_dag_count",
                self.strong_incumbent_reprice_child_dag_count,
            ),
            (
                "strong_incumbent_reprice_fingerprint_count",
                self.strong_incumbent_reprice_fingerprint_count,
            ),
            (
                "strong_incumbent_reprice_property_count",
                self.strong_incumbent_reprice_property_count,
            ),
            (
                "strong_incumbent_reprice_other_count",
                self.strong_incumbent_reprice_other_count,
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
        ]);
        if let Some(timestamp) = self.strong_incumbent_installed_at_us {
            counters.insert("strong_incumbent_installed_at_us", timestamp);
        }
        if let Some(cost) = self.strong_incumbent_installed_cost_expected_bits {
            counters.insert("strong_incumbent_installed_cost_expected_bits", cost);
        }
        if let Some(cost) = self.strong_incumbent_installed_cost_upper_bits {
            counters.insert("strong_incumbent_installed_cost_upper_bits", cost);
        }
        if let Some(reason) = self.strong_incumbent_first_reprice_failure_reason {
            counters.insert("strong_incumbent_first_reprice_failure_reason", reason);
        }
        if let Some(fingerprint) = self.strong_incumbent_installed_context_fingerprint {
            counters.insert(
                "strong_incumbent_installed_context_fingerprint_lo",
                fingerprint.0 as u64,
            );
            counters.insert(
                "strong_incumbent_installed_context_fingerprint_hi",
                (fingerprint.0 >> 64) as u64,
            );
        }
        if let Some(timestamp) = self.strong_incumbent_first_invalidation_at_us {
            counters.insert("strong_incumbent_first_invalidation_at_us", timestamp);
        }
        if let Some(region) = self.quality_frontier_first_incomplete_aggregate_region {
            counters.insert(
                "quality_policy_frontier_first_incomplete_aggregate_region_lo",
                region.0 as u64,
            );
            counters.insert(
                "quality_policy_frontier_first_incomplete_aggregate_region_hi",
                (region.0 >> 64) as u64,
            );
        }
        if let Some(reason) = self.strong_incumbent_first_invalidation_reason {
            counters.insert("strong_incumbent_first_invalidation_reason", reason);
        }
        if let Some(fingerprint) = self.strong_incumbent_first_invalidated_context_fingerprint {
            counters.insert(
                "strong_incumbent_first_invalidated_context_fingerprint_lo",
                fingerprint.0 as u64,
            );
            counters.insert(
                "strong_incumbent_first_invalidated_context_fingerprint_hi",
                (fingerprint.0 >> 64) as u64,
            );
        }
        let fact_bit = |fact: super::quality::BundleFact| 1_u16 << fact.stable_tag();
        let fact_count = |mask: u16| {
            self.quality_frontier_fact_signatures
                .iter()
                .filter(|(signature, _)| **signature & mask == mask)
                .map(|(_, count)| *count)
                .sum()
        };
        counters.insert(
            "quality_frontier_has_join_region_count",
            fact_count(fact_bit(super::quality::BundleFact::JoinRegion)),
        );
        counters.insert(
            "quality_frontier_has_aggregate_decomposition_count",
            fact_count(fact_bit(super::quality::BundleFact::AggregateDecomposition)),
        );
        counters.insert(
            "quality_frontier_has_join_and_aggregate_count",
            fact_count(
                fact_bit(super::quality::BundleFact::JoinRegion)
                    | fact_bit(super::quality::BundleFact::AggregateDecomposition),
            ),
        );
        // Keep budget evidence in the same diagnostic counter snapshot as
        // physical work.  Aggregate counts alone cannot tell whether a
        // local proof was blocked by logical closure, child products, or a
        // global envelope, and guessing that dimension would make any
        // follow-up pruning change unauditable.
        for (dimension, count) in self.memo.exhaustion_counts() {
            counters.insert(budget_exhaustion_counter_name(dimension), count);
        }
        counters
    }

    fn schedule_transformations(
        &mut self,
        group: GroupId,
        agenda: &mut StableAgenda,
    ) -> Result<()> {
        self.schedule_transformations_with_lane(group, agenda, false)
    }

    /// Seed the quality dependency lane from the root's current Memo
    /// subgraph. The lane is an ordering request over existing native rule
    /// tasks, not a second optimizer or a query-specific recipe: every
    /// logical expression remains scheduled, and undeclared rules stay on
    /// the ordinary agenda. This makes an already-visible producer/consumer
    /// chain runnable before unrelated optional exploration consumes the
    /// first interleave batch.
    fn schedule_quality_bootstrap(
        &mut self,
        root: GroupId,
        agenda: &mut StableAgenda,
    ) -> Result<()> {
        let mut pending = vec![self.memo.canonical_group(root)];
        let mut visited = BTreeSet::new();
        while let Some(group) = pending.pop() {
            let group = self.memo.canonical_group(group);
            if !visited.insert(group) {
                continue;
            }
            let expressions = self
                .memo
                .group(group)
                .ok_or_else(|| paro_error::internal("quality bootstrap lost Memo group"))?
                .logical_exprs()
                .to_vec();
            self.schedule_transformations_with_lane(group, agenda, true)?;
            for expression in expressions {
                let logical = self.memo.logical_expr(expression).ok_or_else(|| {
                    paro_error::internal("quality bootstrap lost logical expression")
                })?;
                pending.extend(logical.key.children.iter().copied());
            }
        }
        Ok(())
    }

    /// Promote only the quality rules owned by groups which the physical
    /// readiness path has actually demanded.  The mandatory dependency index
    /// supplies the initial set, while optional recursive costing can add a
    /// newly observed child.  Keeping this handoff local is important: a
    /// broad second walk of the root's selected tree previously spent most of
    /// its time on work that never reached a physical parent.
    fn schedule_demanded_physical_quality_groups(
        &mut self,
        agenda: &mut StableAgenda,
    ) -> Result<()> {
        if !self.quality_handoff_enabled {
            return Ok(());
        }
        let groups = self
            .physical_quality_demanded_groups
            .iter()
            .copied()
            .collect::<Vec<_>>();
        for group in groups {
            let group = self.memo.canonical_group(group);
            if self.physical_quality_scheduled_groups.insert(group) {
                self.schedule_transformations_with_lane_and_demand(group, agenda, true, true)?;
            }
        }
        Ok(())
    }

    /// Schedule the transformations owned by one newly visible group. A
    /// quality-producing publication may pass its lane to the exact output
    /// groups it created; this keeps the dependency chain local while making
    /// the next producer/consumer step run before unrelated initial work.
    fn schedule_transformations_with_lane(
        &mut self,
        group: GroupId,
        agenda: &mut StableAgenda,
        promoted: bool,
    ) -> Result<()> {
        self.schedule_transformations_with_lane_and_demand(group, agenda, promoted, false)
    }

    fn schedule_transformations_with_lane_and_demand(
        &mut self,
        group: GroupId,
        agenda: &mut StableAgenda,
        promoted: bool,
        demanded: bool,
    ) -> Result<()> {
        let expressions = self
            .memo
            .group(group)
            .ok_or_else(|| paro_error::internal("cannot schedule an unknown Memo group"))?
            .logical_exprs()
            .to_vec();
        for expression in expressions {
            self.schedule_transformation_expression(group, expression, agenda, promoted, demanded)?;
        }
        Ok(())
    }

    /// Return the local quality lane for one transformation task. A task is
    /// promoted only when the publication which woke it is one of its
    /// observed dependencies. A physical demand is a narrower lane than the
    /// ordinary quality bootstrap, but it still schedules every legal rule
    /// for the demanded group; it is not a selected-only shortcut or a global
    /// phase barrier.
    fn quality_stage_for_rule(
        &self,
        rule: &dyn TransformationRule,
        promoted: bool,
        demanded: bool,
    ) -> u8 {
        if !self.quality_handoff_enabled {
            return 0;
        }
        if (demanded || promoted) && rule.quality_dependency().is_some() {
            0
        } else {
            u8::MAX
        }
    }

    fn schedule_transformation_expression(
        &mut self,
        group: GroupId,
        expression: LogicalExprId,
        agenda: &mut StableAgenda,
        promoted: bool,
        demanded: bool,
    ) -> Result<()> {
        let expression_ref = self
            .memo
            .logical_expr(expression)
            .ok_or_else(|| paro_error::internal("cannot schedule an unknown logical expression"))?;
        let context = RuleContext {
            memo: &self.memo,
            group,
        };
        // Apply the same dispatch used by the task executor before putting a
        // task on the agenda.  The old scheduler only checked the immutable
        // shell and then paid one queue/checkpoint/cache visit for every
        // nested-path rule that was already known not to have a witness.
        // Negative dispatch is also an observation publication: cached path
        // reads must wake this exact expression when a child frontier later
        // acquires a qualifying alternative.
        let dispatches = self
            .registry
            .transformations()
            .filter(|rule| self.memo.budget().transformation_enabled(rule.id()))
            .filter(|rule| rule.root_operator_tag_may_match(expression_ref.operator_tag))
            .map(|rule| {
                let dispatch = rule.root_dispatch(expression_ref, &context)?;
                Ok((rule.id(), rule.promise(expression_ref, &context), dispatch))
            })
            .collect::<Result<Vec<_>>>()?;
        for (rule_id, promise, dispatch) in dispatches {
            let task_id = TransformationTaskId {
                group,
                expression,
                rule: rule_id,
                binding: None,
            };
            if !dispatch.matches {
                if !dispatch.reads.is_empty() {
                    self.seed_transformation_observation(task_id, &dispatch.reads)?;
                }
                continue;
            }
            // Scheduling is itself an incremental operation.  Do not create
            // a duplicate agenda item for an exact task whose observed
            // frontier and fact reads have not advanced since its last
            // completed application.
            if self.transformation_observation_is_current(task_id)? {
                continue;
            }
            let quality_stage = self.quality_stage_for_rule(
                self.registry
                    .transformation(rule_id)
                    .expect("transformation dispatch disappeared"),
                promoted,
                demanded,
            );
            let key = TaskKey {
                demand_stage: u8::from(!(demanded && quality_stage == 0)),
                quality_stage,
                priority: promise.priority,
                kind: TaskKind::Transform,
                stable_id: rule_id.0,
                group,
                expression,
                goal: None,
            };
            let queued = agenda.push(
                key,
                SearchTask::Transform {
                    group,
                    expression,
                    rule: rule_id,
                },
            );
            if queued {
                self.note_transformation_task(
                    task_id,
                    TransformationTaskLifecyclePhase::Enqueued,
                    None,
                    None,
                    0,
                );
                self.note_rule_phase(rule_id, RuleWorkPhase::Enqueued);
            }
        }
        Ok(())
    }

    fn schedule_transformation_dependents(
        &mut self,
        group: GroupId,
        agenda: &mut StableAgenda,
    ) -> Result<()> {
        let group = self.memo.canonical_group(group);
        let subscribers = self
            .transformation_subscribers
            .get(&group)
            .into_iter()
            .flat_map(|subscribers| subscribers.iter().copied());
        let subscribers = subscribers.collect::<Vec<_>>();
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
            if !rule.root_operator_tag_may_match(expression_ref.operator_tag) {
                continue;
            }
            // A child publication can wake a subscriber whose root operator
            // is immutable and can never match this rule.  Do the complete
            // dispatch before putting another task on the agenda, while
            // preserving the dependency reads of a cached negative path.
            let dispatch = rule.root_dispatch(expression_ref, &context)?;
            let task_id = TransformationTaskId {
                group: owner,
                expression: subscriber.expression,
                rule: subscriber.rule,
                binding: None,
            };
            if !dispatch.matches {
                if !dispatch.reads.is_empty() {
                    self.seed_transformation_observation(task_id, &dispatch.reads)?;
                }
                continue;
            }
            // A subscriber may be notified more than once while a sibling
            // publication is being committed.  The exact observation is the
            // wake-up cursor; if every read is still current, putting the
            // task back on the agenda only pays a discovery visit and then
            // immediately skips the same binding frontier.
            if self.transformation_observation_is_current(task_id)? {
                continue;
            }
            let promise = rule.promise(expression_ref, &context);
            let queued = agenda.push(
                TaskKey {
                    demand_stage: 1,
                    quality_stage: self.quality_stage_for_rule(rule, true, false),
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
            if queued {
                self.note_transformation_task(
                    task_id,
                    TransformationTaskLifecyclePhase::Enqueued,
                    None,
                    None,
                    0,
                );
                self.note_rule_phase(subscriber.rule, RuleWorkPhase::Enqueued);
            }
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
                    demand_stage: 1,
                    quality_stage: if self.quality_handoff_enabled {
                        u8::MAX
                    } else {
                        0
                    },
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
        let certified_local_work = if candidate.source_filter_apply_cost.is_none()
            && !matches!(
                candidate.cost_composition,
                CostComposition::SidewaysFilter { .. }
            )
            && candidate.cost_composition.overlapping_children() == 0
            && matches!(
                candidate.task_supply,
                TaskSupplyContract::Serial | TaskSupplyContract::Source { .. }
            ) {
            CertifiedLocalWorkFloor::from_exact_cost(
                candidate.local_cost,
                candidate.stable_event(goal),
            )
        } else {
            None
        };
        let physical = self.memo.insert_physical(
            group,
            candidate.key,
            candidate.payload,
            candidate.provided,
        )?;
        let recipe_key = (physical, goal, candidate.physical_fingerprint);
        let recipe_fingerprint = candidate.physical_fingerprint;
        let child_dependencies = candidate
            .child_goals
            .iter()
            .map(|(child, child_goal)| (self.memo.canonical_group(*child), *child_goal))
            .collect::<BTreeSet<_>>();
        // Do not publish a region fingerprint or allocate a persistent cost
        // recipe until the candidate has crossed the Memo publication
        // boundary.  The previous order left region admission state behind
        // when the physical/group budget rejected the candidate, and built a
        // throwaway `CostRecipe` for duplicate physical keys.
        let recipe_published = if let std::collections::btree_map::Entry::Vacant(entry) =
            self.recipes.entry(recipe_key)
        {
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
                certified_local_work,
                immutable_cost_identity: OnceLock::new(),
            }));
            true
        } else {
            false
        };
        if recipe_published && self.collect_rule_work_profile {
            let logical = self
                .memo
                .physical_expr(physical)
                .map(|expression| expression.key.logical);
            let facts =
                PatternRead::facts_from_group(&self.memo, group).map(|read| Box::new([read]))?;
            self.note_candidate_lifecycle(CandidateLifecycleEvent {
                stage: CandidateLifecycleStage::PhysicalRecipePublished,
                elapsed_us: self.lifecycle_elapsed_us(),
                group,
                goal: Some(goal),
                candidate: None,
                source: None,
                binding: None,
                source_child: None,
                logical,
                physical: Some(physical),
                recipe: Some(recipe_fingerprint),
                rule: None,
                children: Box::new([]),
                facts,
                expected_cost_bits: None,
                upper_cost_bits: None,
            });
        }
        let owner = self.memo.canonical_group(group);
        self.physical_read_dependencies
            .entry((owner, goal))
            .or_default()
            .extend(child_dependencies.iter().copied());
        // Publish the reverse edge together with the recipe.  Registering it
        // only when a parent later recurses into the child leaves a window in
        // which a newly published child frontier has no parent to wake.  The
        // edge is exact (child goal, parent goal, physical expression and
        // recipe identity), so an early wakeup still follows the normal
        // continuation/read-set invalidation path and cannot turn a partial
        // child frontier into a completion certificate.
        for &(child, child_goal) in &child_dependencies {
            self.register_physical_dependency(
                child,
                child_goal,
                owner,
                goal,
                physical,
                recipe_fingerprint,
            );
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
        self.memo
            .group(group)
            .ok_or_else(|| paro_error::internal("unknown group during physical read capture"))?;
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
        if let Some(children) = self.physical_read_dependencies.get(&(group, goal)) {
            for (child, _) in children.iter().copied() {
                reads.push(PatternRead::physical_from_group(&self.memo, child)?);
            }
        }
        Ok(ReadSet::new(reads))
    }

    fn optimize_group(&mut self, group: GroupId, goal: OptimizationGoal) -> Result<()> {
        if !self.memo.control().checkpoint()? {
            return Ok(());
        }
        let group = self.memo.canonical_group(group);
        let cache_key = (group, goal);
        let next_recipe_sequence = self
            .next_recipe_sequence
            .get(&cache_key)
            .copied()
            .unwrap_or_default();
        let fast_reuse = !self.physical_full_recost.contains(&cache_key)
            && !self.physical_dirty_recipes.contains_key(&cache_key)
            && self
                .physical_task_cache
                .get(&cache_key)
                .is_some_and(|entry| {
                    next_recipe_sequence <= entry.recipe_cursor
                        && (entry.complete
                            || (self.preserve_incomplete_physical
                                && !self.physical_interleave_step_mode))
                        && entry
                            .reads
                            .is_current(&self.memo)
                            .is_ok_and(|current| current)
                });
        if fast_reuse {
            self.physical_subproblem_reuses = self.physical_subproblem_reuses.saturating_add(1);
            return Ok(());
        }
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
                // A dirty notification can be redundant. Reused already
                // certifies the same child/fact ReadSet; only an extended local
                // recipe stream (or explicit full recost) reopens that proof.
                let recipe_domain_advanced = self
                    .physical_task_cache
                    .get(&cache_key)
                    .is_some_and(|cached| next_recipe_sequence > cached.recipe_cursor)
                    || force_full_recost;
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
                if !incomplete && recipe_domain_advanced {
                    // Own physical writes are deliberately absent from the
                    // task ReadSet. A completed prefix cannot certify recipes
                    // appended later under that same read context.
                    let cursor = self
                        .task_registry
                        .task(task)
                        .and_then(|record| self.task_registry.cursor(record.cursor))
                        .unwrap_or_default();
                    self.task_registry.advance_cursor(
                        task,
                        Cursor {
                            complete: false,
                            ..cursor
                        },
                    )?;
                    self.physical_completion_proofs.remove(&cache_key);
                    resume_candidate = true;
                    task
                } else if !self.mandatory_only
                    && (!self.preserve_incomplete_physical || self.physical_interleave_step_mode)
                    && incomplete
                {
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
        let recipe_cursor = if resume_candidate {
            // Resume the current evaluation. Its predecessor belongs to the
            // prior read-set epoch and may still point at the beginning of
            // the recipe stream even when this task yielded mid-recipe.
            current_cursor.position
        } else {
            predecessor_cursor
                .or_else(|| (!new_evaluation).then_some(current_cursor))
                .map(|cursor| cursor.position)
                .unwrap_or_default()
        };
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
                        && self.memo.search_obligations_empty(),
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
            self.physical_task_cache.insert(
                cache_key,
                PhysicalTaskCacheEntry {
                    reads: requested_read_set.clone(),
                    recipe_cursor: recipe_count,
                    complete: !self.preserve_incomplete_physical
                        && self.memo.search_obligations_empty(),
                },
            );
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
            self.physical_task_cache.insert(
                cache_key,
                PhysicalTaskCacheEntry {
                    reads: requested_read_set.clone(),
                    recipe_cursor: recipe_count,
                    complete: !self.preserve_incomplete_physical,
                },
            );
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
            task,
            recipe_start,
            recipe_cursor,
            enumerate_local_implementations,
            dirty_recipes.as_ref(),
        );
        self.active_goals.remove(&(group, goal));
        match result {
            Ok(resume_recipe_cursor) => {
                let yielded = resume_recipe_cursor.is_some();
                let has_winner = self
                    .memo
                    .group(group)
                    .and_then(|group| group.winner(goal))
                    .is_some();
                if !has_winner && !self.preserve_incomplete_physical {
                    self.infeasible_goals.insert((group, goal));
                }
                // A readiness pass may leave the query-wide logical search
                // open while this exact physical domain is already closed.
                // Keep that distinction explicit: the local task can publish
                // a versioned proof for parent bounds, while the root still
                // reports an incomplete search until the stronger global
                // obligation predicate succeeds.  A local budget/failure or
                // deadline remains a hard blocker for this proof.
                let complete = !yielded && self.memo.group_physical_obligations_empty(group);
                // The recursive pass may have created new recipes and child
                // frontier dependencies. Rebind the running task to the
                // exact post-child ReadSet before publication; otherwise a
                // later parent could either miss a child change or retain a
                // provisional pre-child snapshot.
                let post_child_reads = self.physical_read_set(group, goal)?;
                self.task_registry.replace_current_read_set(
                    task,
                    &self.memo,
                    post_child_reads.clone(),
                )?;
                let recipe_count = self
                    .next_recipe_sequence
                    .get(&(self.memo.canonical_group(group), goal))
                    .copied()
                    .unwrap_or_default();
                let cursor_position = resume_recipe_cursor.unwrap_or(recipe_count);
                let cursor = self.task_registry.advance_cursor(
                    task,
                    Cursor {
                        position: cursor_position,
                        complete,
                    },
                )?;
                let proof = if complete {
                    if let Some(candidate) = self
                        .memo
                        .group(group)
                        .and_then(|group| group.winner(goal))
                        .map(|winner| winner.candidate)
                    {
                        self.record_physical_completion_proof(task, group, goal, candidate)?
                    } else {
                        None
                    }
                } else {
                    None
                };
                let outcome = match (has_winner, proof) {
                    (true, Some(certificate)) if goal.objective == ObjectiveProfile::Latency => {
                        TaskOutcome::ProvenNoPlanBelow {
                            threshold: self
                                .physical_completion_proofs
                                .get(&cache_key)
                                .map(|proof| proof.threshold)
                                .ok_or_else(|| {
                                    paro_error::internal(
                                        "latency completion proof lost its threshold",
                                    )
                                })?,
                            certificate,
                        }
                    }
                    (true, Some(certificate)) => TaskOutcome::ProvenOptimal {
                        candidate: self
                            .memo
                            .group(group)
                            .and_then(|group| group.winner(goal))
                            .map(|winner| winner.candidate)
                            .ok_or_else(|| {
                                paro_error::internal("physical completion proof lost its winner")
                            })?,
                        certificate,
                    },
                    (true, None) => TaskOutcome::Progress { cursor },
                    // A readiness step is allowed to stop before the first
                    // feasible response is assembled (for example while a
                    // child is publishing the first frontier delta).  Keep
                    // that task resumable; marking it Infeasible would make
                    // TaskRegistry treat the partial prefix as a proof and
                    // the final full pass could never reopen it.
                    (false, _) if self.preserve_incomplete_physical => {
                        TaskOutcome::Progress { cursor }
                    }
                    (false, _) => TaskOutcome::Infeasible,
                };
                self.task_registry.publish_current_after_local_mutation(
                    task,
                    &self.memo,
                    [group],
                    std::iter::empty(),
                    outcome,
                )?;
                self.physical_task_cache.insert(
                    cache_key,
                    PhysicalTaskCacheEntry {
                        reads: post_child_reads,
                        recipe_cursor: cursor_position,
                        complete,
                    },
                );
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

    fn incumbent_for_goal(&self, group: GroupId, goal: OptimizationGoal) -> Option<&Winner> {
        if let Some(winner) = self.memo.group(group).and_then(|group| group.winner(goal)) {
            return Some(winner);
        }
        self.protected_incumbents
            .get(&(self.memo.canonical_group(group), goal))
            .filter(|incumbent| {
                incumbent
                    .reads
                    .is_current(&self.memo)
                    .is_ok_and(|current| current)
            })
            .map(|incumbent| incumbent.winner.as_ref())
    }

    fn incumbent_cost_for_goal(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
    ) -> Option<SearchCost> {
        let local = self
            .incumbent_for_goal(group, goal)
            .map(|winner| winner.cost);
        let strong = self.strong_incumbent_cost_for_goal(group, goal);
        match (local, strong) {
            (Some(local), Some(strong)) => {
                if goal.objective.compare(&strong, &local) == std::cmp::Ordering::Less {
                    self.strong_incumbent_selected_for_bound_count = self
                        .strong_incumbent_selected_for_bound_count
                        .saturating_add(1);
                    Some(strong)
                } else {
                    Some(local)
                }
            }
            (Some(local), None) => Some(local),
            (None, Some(strong)) => {
                self.strong_incumbent_selected_for_bound_count = self
                    .strong_incumbent_selected_for_bound_count
                    .saturating_add(1);
                Some(strong)
            }
            (None, None) => None,
        }
    }

    /// Return a strong incumbent only when the exact cost dependency witness
    /// is still current.  The counters intentionally distinguish a missing
    /// map entry from an entry that was rejected by ReadSet/context
    /// validation; otherwise a final `active_count` snapshot cannot explain
    /// whether the seed ever participated in a proof check.
    fn strong_incumbent_cost_for_goal(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
    ) -> Option<SearchCost> {
        if self.strong_incumbents.is_empty() {
            return None;
        }
        self.observe_strong_incumbent_lifecycle();
        let key = (self.memo.canonical_group(group), goal);
        self.strong_incumbent_lookup_count = self.strong_incumbent_lookup_count.saturating_add(1);
        let (identity, cost, context_fingerprint, reads) = {
            let Some(incumbent) = self.strong_incumbents.get(&key) else {
                self.strong_incumbent_missing_key_count =
                    self.strong_incumbent_missing_key_count.saturating_add(1);
                let has_same_group = self
                    .strong_incumbents
                    .keys()
                    .any(|(candidate_group, _)| *candidate_group == key.0);
                let has_same_goal = self
                    .strong_incumbents
                    .keys()
                    .any(|(_, candidate_goal)| *candidate_goal == key.1);
                if has_same_group {
                    self.strong_incumbent_lookup_goal_mismatch_count = self
                        .strong_incumbent_lookup_goal_mismatch_count
                        .saturating_add(1);
                } else if has_same_goal {
                    self.strong_incumbent_lookup_group_mismatch_count = self
                        .strong_incumbent_lookup_group_mismatch_count
                        .saturating_add(1);
                } else {
                    self.strong_incumbent_lookup_unrelated_key_count = self
                        .strong_incumbent_lookup_unrelated_key_count
                        .saturating_add(1);
                }
                return None;
            };
            (
                incumbent.plan.identity,
                incumbent.cost,
                incumbent.context.fingerprint,
                incumbent.context.reads.clone(),
            )
        };

        let reads_current = reads.is_current(&self.memo).is_ok_and(|current| current);
        if !reads_current {
            self.strong_incumbent_invalid_lookup_count =
                self.strong_incumbent_invalid_lookup_count.saturating_add(1);
            self.strong_incumbent_fact_invalid_count =
                self.strong_incumbent_fact_invalid_count.saturating_add(1);
            self.note_strong_incumbent_invalidation(1, Some(context_fingerprint));
            return None;
        }
        let current_context = match self.priced_cost_context(key.0, goal, identity, &reads) {
            Ok(context) => context,
            Err(_) => {
                self.strong_incumbent_invalid_lookup_count =
                    self.strong_incumbent_invalid_lookup_count.saturating_add(1);
                self.strong_incumbent_context_invalid_count = self
                    .strong_incumbent_context_invalid_count
                    .saturating_add(1);
                self.note_strong_incumbent_invalidation(2, Some(context_fingerprint));
                return None;
            }
        };
        if current_context.fingerprint != context_fingerprint {
            self.strong_incumbent_invalid_lookup_count =
                self.strong_incumbent_invalid_lookup_count.saturating_add(1);
            self.strong_incumbent_context_invalid_count = self
                .strong_incumbent_context_invalid_count
                .saturating_add(1);
            self.note_strong_incumbent_invalidation(2, Some(context_fingerprint));
            return None;
        }
        self.strong_incumbent_valid_lookup_count =
            self.strong_incumbent_valid_lookup_count.saturating_add(1);
        Some(cost)
    }

    /// Observe invalidation at the first bound lookup after the Memo's fact
    /// epoch has changed. A seed can become stale without an exact lookup for
    /// its root key (for example, when the search only asks child goals), so
    /// the final counter snapshot is not the first useful observation.
    fn observe_strong_incumbent_lifecycle(&mut self) {
        if self.strong_incumbent_first_invalidation_at_us.is_some() {
            return;
        }
        let mut first_invalid = None;
        for ((group, goal), incumbent) in &self.strong_incumbents {
            let reads_current = incumbent
                .context
                .reads
                .is_current(&self.memo)
                .is_ok_and(|current| current);
            let context_current = reads_current
                && self
                    .priced_cost_context(
                        *group,
                        *goal,
                        incumbent.plan.identity,
                        &incumbent.context.reads,
                    )
                    .ok()
                    .is_some_and(|context| context.fingerprint == incumbent.context.fingerprint);
            if !context_current {
                first_invalid = Some((
                    if reads_current { 2 } else { 1 },
                    Some(incumbent.context.fingerprint),
                ));
                break;
            }
        }
        if let Some((reason, fingerprint)) = first_invalid {
            self.note_strong_incumbent_invalidation(reason, fingerprint);
        }
    }

    fn max_parallel_tasks_for_goal(&self, goal: OptimizationGoal) -> u16 {
        match goal.grant {
            GrantGoalKey::Class(class) => self
                .grant_classes
                .get(&class)
                .map_or(1, |class| class.max_parallel_tasks.max(1)),
            GrantGoalKey::Parallelism { tasks, .. } => tasks.max(1),
            GrantGoalKey::Invariant(_) => 1,
        }
    }

    /// Encode the complete currently published physical domain of one
    /// subproblem. ReadSet cursors invalidate the proof when facts or logical
    /// frontiers change; this additional identity covers a recipe appended
    /// by an already-observed logical expression and makes the proof fail
    /// closed if the physical registry grows without a frontier mutation.
    fn physical_search_domain(
        &self,
        group: GroupId,
        goal: OptimizationGoal,
    ) -> Result<Fingerprint> {
        let group = self.memo.canonical_group(group);
        let group_ref = self
            .memo
            .group(group)
            .ok_or_else(|| paro_error::internal("unknown group in physical search domain"))?;
        let mut builder = StableFingerprintBuilder::default();
        builder.write_bytes(b"paro.physical-search-domain.v1");
        builder.write_u64(group.0 as u64);
        write_optimization_goal_fingerprint(&mut builder, goal);
        builder.write_u64(group_ref.logical_expression_version());
        builder.write_fingerprint(group_ref.logical_fact_fingerprint());
        builder.write_fingerprint(self.memo.local_statistics_fingerprint(group));
        for &physical in group_ref.physical_exprs() {
            builder.write_u64(physical.0 as u64);
            let Some(expression) = self.memo.physical_expr(physical) else {
                return Err(paro_error::internal(
                    "physical search domain references an unknown expression",
                ));
            };
            builder.write_fingerprint(expression.key.stable_fingerprint());
            for ((recipe_physical, recipe_goal, recipe_fingerprint), recipe) in self.recipes.range(
                (physical, goal, Fingerprint::default())..=(physical, goal, Fingerprint(u128::MAX)),
            ) {
                debug_assert_eq!(*recipe_physical, physical);
                debug_assert_eq!(*recipe_goal, goal);
                builder.write_u64(recipe.sequence);
                builder.write_fingerprint(*recipe_fingerprint);
                builder.write_fingerprint(recipe.physical_fingerprint);
                for (child, child_goal) in recipe.child_goals.iter().copied() {
                    builder.write_u64(self.memo.canonical_group(child).0 as u64);
                    write_optimization_goal_fingerprint(&mut builder, child_goal);
                }
                write_search_cost_fingerprint(&mut builder, recipe.local_cost);
                write_task_supply_fingerprint(&mut builder, &recipe.task_supply);
                write_cost_composition_fingerprint(&mut builder, &recipe.cost_composition);
            }
        }
        Ok(builder.finish())
    }

    /// Return the exact, current frontier of a child only when the child
    /// task completed with a named upper-bound certificate. A selected winner
    /// or a non-empty frontier alone is not enough: it may be an anytime
    /// prefix or may already have been invalidated by a fact change.
    fn proven_child_latency_floor(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
    ) -> Result<Option<ProvenChildLatencyFloor>> {
        let key = (self.memo.canonical_group(group), goal);
        let Some(record) = self.physical_completion_proofs.get(&key).copied() else {
            return Ok(None);
        };
        let Some(proof) = self.task_registry.bound(record.proof).cloned() else {
            self.certified_bound_diagnostics.invalidation_count = self
                .certified_bound_diagnostics
                .invalidation_count
                .saturating_add(1);
            return Ok(None);
        };
        // A live upper-bound record is useful to the task owner as a
        // provisional incumbent, but it is not evidence that every child
        // alternative has been enumerated.  This caller composes the result
        // as a *complete* child frontier, so require the named certificate to
        // have crossed the publication boundary and to be the task outcome.
        let Some(task) = self.task_registry.task(proof.task) else {
            self.certified_bound_diagnostics.invalidation_count = self
                .certified_bound_diagnostics
                .invalidation_count
                .saturating_add(1);
            return Ok(None);
        };
        let task_is_completed = task.state == TaskState::Completed;
        let task_names_proof = matches!(
            task.outcome,
            Some(TaskOutcome::ProvenNoPlanBelow { certificate, .. })
                if certificate == record.proof
        );
        if !task_is_completed || !task_names_proof {
            self.certified_bound_diagnostics.invalidation_count = self
                .certified_bound_diagnostics
                .invalidation_count
                .saturating_add(1);
            return Ok(None);
        }
        if !matches!(
            proof.kind,
            BoundProofKind::NoPlanBelow { threshold }
                if threshold == record.threshold
                    && self
                        .memo
                        .group(key.0)
                        .and_then(|group| group.winner(goal))
                        .is_some_and(|winner| winner.candidate == record.candidate)
        ) {
            return Ok(None);
        }
        let validation_started = Instant::now();
        let bound_current = self
            .task_registry
            .bound_is_current(record.proof, &self.memo)?;
        let domain_current = record.domain == self.physical_search_domain(key.0, goal)?;
        CertifiedBoundDiagnostics::add_elapsed(
            &mut self.certified_bound_diagnostics.validation_us,
            validation_started,
        );
        if !bound_current || !domain_current {
            self.certified_bound_diagnostics.invalidation_count = self
                .certified_bound_diagnostics
                .invalidation_count
                .saturating_add(1);
            return Ok(None);
        }
        let Some(frontier) = self
            .memo
            .group(key.0)
            .and_then(|group| group.winner_frontier(goal))
        else {
            self.certified_bound_diagnostics.invalidation_count = self
                .certified_bound_diagnostics
                .invalidation_count
                .saturating_add(1);
            return Ok(None);
        };
        Ok(ProvenChildLatencyFloor::from_frontier(
            frontier
                .candidates()
                .iter()
                .map(|winner| winner.cost.work_latency.expected),
            frontier
                .candidates()
                .iter()
                .map(|winner| winner.cost.critical_path.expected),
            frontier
                .candidates()
                .iter()
                .map(|winner| winner.cost.max_parallel_tasks),
        ))
    }

    /// Publish a completion certificate for one exact physical task. The
    /// certificate is deliberately created before the task outcome is
    /// published; `BoundProof::is_current` then requires that outcome to name
    /// the certificate, preventing a normal progress result from becoming a
    /// durable pruning proof by accident.
    fn record_physical_completion_proof(
        &mut self,
        task: TaskId,
        group: GroupId,
        goal: OptimizationGoal,
        candidate: CandidateId,
    ) -> Result<Option<BoundProofId>> {
        if !self.certified_group_pruning_enabled || self.mandatory_only {
            return Ok(None);
        }
        let reads = self
            .task_registry
            .task_read_set(task)
            .ok_or_else(|| paro_error::internal("physical task lost its completion read set"))?;
        let domain = self.physical_search_domain(group, goal)?;
        let winner = self
            .memo
            .group(group)
            .and_then(|group| group.winner(goal))
            .filter(|winner| winner.candidate == candidate)
            .ok_or_else(|| paro_error::internal("completion proof lost its selected winner"))?;
        let threshold = if goal.objective == ObjectiveProfile::Latency {
            super::bounds::expected_makespan(&winner.cost).to_bits()
        } else {
            winner.cost.score.range.expected.to_bits()
        };
        let context = BoundContext {
            group: self.memo.canonical_group(group),
            goal,
            reads,
            search_domain: domain,
        };
        // Latency tasks publish the stronger existential certificate.  It is
        // tied to the same exact selected candidate and domain as the normal
        // upper bound, so it cannot be consumed as a child result after a
        // later logical/fact revision. Other objectives retain the existing
        // verified-upper outcome until their objective-specific lower algebra
        // is admitted.
        let proof = if goal.objective == ObjectiveProfile::Latency {
            self.task_registry
                .record_no_plan_below(task, context, threshold)?
        } else {
            self.task_registry
                .record_verified_upper(task, context, threshold, candidate)?
        };
        self.physical_completion_proofs.insert(
            (self.memo.canonical_group(group), goal),
            PhysicalCompletionProof {
                proof,
                domain,
                candidate,
                threshold,
            },
        );
        Ok(Some(proof))
    }

    /// Prove a recipe cannot beat the incumbent from complete child
    /// subproblems. This intentionally covers only the additive, unfiltered
    /// latency contract. Sideways filters, source response and phase overlap
    /// remain open and therefore return `false` rather than guessing.
    fn recipe_is_provably_worse_with_children(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
        recipe: &CostRecipe,
        location: BoundCheckLocation,
    ) -> Result<Option<bool>> {
        if self.mandatory_only || !self.certified_group_pruning_enabled {
            return Ok(None);
        }
        self.strong_incumbent_bound_request_count =
            self.strong_incumbent_bound_request_count.saturating_add(1);
        let started = Instant::now();
        if goal.objective != ObjectiveProfile::Latency {
            CertifiedBoundDiagnostics::add_elapsed(
                &mut self.certified_bound_diagnostics.compute_us,
                started,
            );
            return Ok(None);
        }
        if recipe.source_filter_apply_cost.is_some()
            || recipe.cost_composition.sideways_filter().is_some()
        {
            self.strong_incumbent_source_response_bypass_count = self
                .strong_incumbent_source_response_bypass_count
                .saturating_add(1);
            self.certified_bound_diagnostics.source_response_unsupported = self
                .certified_bound_diagnostics
                .source_response_unsupported
                .saturating_add(1);
            CertifiedBoundDiagnostics::add_elapsed(
                &mut self.certified_bound_diagnostics.compute_us,
                started,
            );
            return Ok(None);
        }
        if recipe.cost_composition.overlapping_children() != 0 {
            self.strong_incumbent_phase_overlap_bypass_count = self
                .strong_incumbent_phase_overlap_bypass_count
                .saturating_add(1);
            self.certified_bound_diagnostics.phase_overlap_unsupported = self
                .certified_bound_diagnostics
                .phase_overlap_unsupported
                .saturating_add(1);
            CertifiedBoundDiagnostics::add_elapsed(
                &mut self.certified_bound_diagnostics.compute_us,
                started,
            );
            return Ok(None);
        }
        let Some(incumbent_cost) = self.incumbent_cost_for_goal(group, goal) else {
            self.certified_bound_diagnostics.no_incumbent = self
                .certified_bound_diagnostics
                .no_incumbent
                .saturating_add(1);
            CertifiedBoundDiagnostics::add_elapsed(
                &mut self.certified_bound_diagnostics.compute_us,
                started,
            );
            return Ok(None);
        };
        let mut children = Vec::with_capacity(recipe.child_goals.len());
        for (child, child_goal) in recipe.child_goals.iter().copied() {
            let Some(floor) = self.proven_child_latency_floor(child, child_goal)? else {
                // A missing child certificate is an unknown bound, not a
                // reason to reject the recipe. The caller must continue with
                // ordinary child-frontier enumeration and may retry this
                // proof after that child completes.
                self.certified_bound_diagnostics.child_completion_missing = self
                    .certified_bound_diagnostics
                    .child_completion_missing
                    .saturating_add(1);
                CertifiedBoundDiagnostics::add_elapsed(
                    &mut self.certified_bound_diagnostics.compute_us,
                    started,
                );
                return Ok(None);
            };
            children.push(floor);
        }
        // The requested grant is not the only source of execution capacity.
        // Source and streaming contracts can expose a larger pipeline, and a
        // breaker/build-probe can carry that capacity through a phase.  A
        // floor which divides by the grant alone would be too high and could
        // incorrectly prune a source-backed plan before it has a winner.
        let mut max_parallel_tasks = self.max_parallel_tasks_for_goal(goal);
        for child in &children {
            max_parallel_tasks = max_parallel_tasks.max(child.max_parallel_tasks);
        }
        let child_capacity = |index: u8| {
            children
                .get(usize::from(index))
                .map_or(1, |child| child.max_parallel_tasks.max(1))
        };
        match recipe.task_supply {
            TaskSupplyContract::Serial => {}
            TaskSupplyContract::Source { tasks } => {
                max_parallel_tasks = max_parallel_tasks.max(tasks.max(1));
            }
            TaskSupplyContract::Streaming { input } => {
                max_parallel_tasks = max_parallel_tasks.max(child_capacity(input));
            }
            TaskSupplyContract::Breaker {
                input,
                output_tasks,
                ..
            } => {
                max_parallel_tasks = max_parallel_tasks
                    .max(child_capacity(input))
                    .max(output_tasks.max(1));
            }
            TaskSupplyContract::BuildProbe { build, probe, .. } => {
                max_parallel_tasks = max_parallel_tasks
                    .max(child_capacity(build))
                    .max(child_capacity(probe));
            }
        }
        let Some(floor) = ProvenRecipeLatencyFloor::from_fixed_recipe(
            recipe.local_cost,
            children,
            max_parallel_tasks,
            recipe.physical_fingerprint,
        ) else {
            self.certified_bound_diagnostics.local_interval_uncertain = self
                .certified_bound_diagnostics
                .local_interval_uncertain
                .saturating_add(1);
            CertifiedBoundDiagnostics::add_elapsed(
                &mut self.certified_bound_diagnostics.compute_us,
                started,
            );
            return Ok(Some(false));
        };
        self.certified_bound_check_count = self.certified_bound_check_count.saturating_add(1);
        if !floor.proves_no_latency_improvement(&incumbent_cost) {
            self.certified_bound_diagnostics.available_not_tight = self
                .certified_bound_diagnostics
                .available_not_tight
                .saturating_add(1);
            CertifiedBoundDiagnostics::add_elapsed(
                &mut self.certified_bound_diagnostics.compute_us,
                started,
            );
            return Ok(Some(false));
        }
        self.certified_recipe_prune_count = self.certified_recipe_prune_count.saturating_add(1);
        match location {
            BoundCheckLocation::BeforeChildren => {
                self.certified_bound_diagnostics.pruned_before_children = self
                    .certified_bound_diagnostics
                    .pruned_before_children
                    .saturating_add(1);
            }
            BoundCheckLocation::AfterChildren => {
                self.certified_bound_diagnostics.pruned_after_children = self
                    .certified_bound_diagnostics
                    .pruned_after_children
                    .saturating_add(1);
            }
        }
        CertifiedBoundDiagnostics::add_elapsed(
            &mut self.certified_bound_diagnostics.compute_us,
            started,
        );
        tracing::debug!(
            target: "paro::optimizer",
            memo_group = group.index(),
            ?goal,
            witness = floor.witness.0,
            lower_makespan = floor.makespan(),
            incumbent_makespan = super::bounds::expected_makespan(&incumbent_cost),
            "pruned physical recipe by complete child latency bounds"
        );
        Ok(Some(true))
    }

    /// Apply only a recipe-local proof. This deliberately does not infer a
    /// lower bound from child frontiers, statistical interval endpoints, or
    /// the set of logical rules not yet applied. Those domains remain open
    /// until a later bound certificate covers them.
    fn recipe_is_provably_worse(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
        _task: TaskId,
        recipe: &CostRecipe,
    ) -> Result<bool> {
        if self.mandatory_only || !self.certified_group_pruning_enabled {
            return Ok(false);
        }
        self.strong_incumbent_bound_request_count =
            self.strong_incumbent_bound_request_count.saturating_add(1);
        let started = Instant::now();
        if recipe.source_filter_apply_cost.is_some()
            || recipe.cost_composition.sideways_filter().is_some()
        {
            self.strong_incumbent_source_response_bypass_count = self
                .strong_incumbent_source_response_bypass_count
                .saturating_add(1);
            self.certified_bound_diagnostics.source_response_unsupported = self
                .certified_bound_diagnostics
                .source_response_unsupported
                .saturating_add(1);
        }
        if recipe.cost_composition.overlapping_children() != 0 {
            self.strong_incumbent_phase_overlap_bypass_count = self
                .strong_incumbent_phase_overlap_bypass_count
                .saturating_add(1);
            self.certified_bound_diagnostics.phase_overlap_unsupported = self
                .certified_bound_diagnostics
                .phase_overlap_unsupported
                .saturating_add(1);
        }
        let Some(floor) = recipe.certified_local_work else {
            if recipe.source_filter_apply_cost.is_none()
                && recipe.cost_composition.sideways_filter().is_none()
                && recipe.cost_composition.overlapping_children() == 0
            {
                self.certified_bound_diagnostics.local_interval_uncertain = self
                    .certified_bound_diagnostics
                    .local_interval_uncertain
                    .saturating_add(1);
            }
            CertifiedBoundDiagnostics::add_elapsed(
                &mut self.certified_bound_diagnostics.compute_us,
                started,
            );
            return Ok(false);
        };
        let Some(incumbent_cost) = self.incumbent_cost_for_goal(group, goal) else {
            self.certified_bound_diagnostics.no_incumbent = self
                .certified_bound_diagnostics
                .no_incumbent
                .saturating_add(1);
            CertifiedBoundDiagnostics::add_elapsed(
                &mut self.certified_bound_diagnostics.compute_us,
                started,
            );
            return Ok(false);
        };
        self.certified_bound_check_count = self.certified_bound_check_count.saturating_add(1);
        let max_parallel_tasks =
            self.max_parallel_tasks_for_goal(goal)
                .max(match recipe.task_supply {
                    TaskSupplyContract::Serial => 1,
                    TaskSupplyContract::Source { tasks } => tasks.max(1),
                    TaskSupplyContract::Streaming { .. }
                    | TaskSupplyContract::Breaker { .. }
                    | TaskSupplyContract::BuildProbe { .. } => 1,
                });
        if !floor.proves_no_latency_improvement(goal.objective, &incumbent_cost, max_parallel_tasks)
        {
            self.certified_bound_diagnostics.available_not_tight = self
                .certified_bound_diagnostics
                .available_not_tight
                .saturating_add(1);
            CertifiedBoundDiagnostics::add_elapsed(
                &mut self.certified_bound_diagnostics.compute_us,
                started,
            );
            return Ok(false);
        }
        self.certified_recipe_prune_count = self.certified_recipe_prune_count.saturating_add(1);
        self.certified_bound_diagnostics.pruned_before_children = self
            .certified_bound_diagnostics
            .pruned_before_children
            .saturating_add(1);
        CertifiedBoundDiagnostics::add_elapsed(
            &mut self.certified_bound_diagnostics.compute_us,
            started,
        );
        tracing::debug!(
            target: "paro::optimizer",
            memo_group = group.index(),
            ?goal,
            witness = floor.witness.0,
            local_work = floor.work_latency,
            incumbent_makespan = super::bounds::expected_makespan(&incumbent_cost),
            max_parallel_tasks,
            "pruned physical recipe by certified local work floor"
        );
        Ok(true)
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
    ) -> Result<(bool, bool, Option<CandidateId>)> {
        let _admission_timer = CostPhaseTimer::start(&self.diagnostic_cost_phase_times, 1);
        let Some(cached) = state.priced.get_mut(children) else {
            return Err(paro_error::internal(
                "priced child combination disappeared before frontier admission",
            ));
        };
        if cached.admission == CombinationAdmission::Published {
            return Ok((false, false, None));
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
                Ok((false, false, None))
            }
            CandidatePreview::Truncated => {
                self.memo.record_rejected_winner_proposal(
                    group,
                    goal,
                    cached.physical_fingerprint,
                    true,
                )?;
                cached.admission = CombinationAdmission::FrontierTruncated;
                Ok((false, false, None))
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
                let published_candidate = (self.memo.published_winner_count() > published_before)
                    .then(|| CandidateId::new(published_before as usize));
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
                Ok((after != before, selected_changed, published_candidate))
            }
        }
    }

    fn optimize_group_inner(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
        task: TaskId,
        recipe_start: u64,
        recipe_cursor: u64,
        enumerate_local_implementations: bool,
        dirty_recipes: Option<&BTreeSet<(PhysicalExprId, Fingerprint)>>,
    ) -> Result<Option<u64>> {
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
        let mut optimized_children = BTreeSet::<(GroupId, OptimizationGoal)>::new();

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
            if self.recipe_is_provably_worse(group, goal, task, &recipe)? {
                continue;
            }
            // Once every child has a current completion certificate, the
            // parent recipe can be rejected before copying any child
            // frontier or registering another recursive response.  An
            // unknown child bound deliberately falls through to the normal
            // path; the post-child check below retries it after optimization.
            let child_bound = self.recipe_is_provably_worse_with_children(
                group,
                goal,
                &recipe,
                BoundCheckLocation::BeforeChildren,
            )?;
            if child_bound == Some(true) {
                continue;
            }
            child_frontiers.resize_with(recipe.child_goals.len(), Vec::new);
            for frontier in child_frontiers.iter_mut() {
                frontier.clear();
            }
            let mut baseline_child_selections = Vec::with_capacity(recipe.child_goals.len());
            let mut children_feasible = true;
            let mut child_yielded = false;
            for ((child, child_goal), frontier_out) in recipe
                .child_goals
                .iter()
                .copied()
                .zip(child_frontiers.iter_mut())
            {
                let child = self.memo.canonical_group(child);
                self.register_physical_dependency(
                    child,
                    child_goal,
                    group,
                    goal,
                    physical,
                    recipe.physical_fingerprint,
                );
                if optimized_children.insert((child, child_goal)) {
                    // A child yields a usable prefix, not a barrier requiring
                    // its entire domain to finish before a parent can respond.
                    // Once one child yields, consume only already-published
                    // siblings; do not start another recursive search here.
                    if !(self.physical_interleave_step_mode
                        && self.physical_interleave_step_yielded)
                    {
                        self.optimize_group(child, child_goal)?;
                    }
                }
                child_yielded |=
                    self.physical_interleave_step_mode && self.physical_interleave_step_yielded;
                let Some(frontier) = self
                    .memo
                    .group(child)
                    .and_then(|group| group.winner_frontier(child_goal))
                else {
                    if child_yielded {
                        return Ok(Some(sequence));
                    }
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
                let frontier_candidates = frontier
                    .candidates()
                    .iter()
                    .map(|winner| ChildWinnerRef {
                        group: child,
                        goal: child_goal,
                        candidate: winner.candidate,
                    })
                    .collect::<Vec<_>>();
                frontier_out.reserve(frontier_candidates.len());
                frontier_out.extend(frontier_candidates.iter().copied());
                frontier_out.sort_unstable_by_key(|child| child.candidate);
                if self.collect_rule_work_profile {
                    for child_reference in frontier_candidates {
                        self.note_candidate_lifecycle(CandidateLifecycleEvent {
                            stage: CandidateLifecycleStage::ChildReady,
                            elapsed_us: self.lifecycle_elapsed_us(),
                            group: child,
                            goal: Some(child_goal),
                            candidate: Some(child_reference.candidate),
                            source: None,
                            binding: None,
                            source_child: None,
                            logical: None,
                            physical: Some(physical),
                            recipe: Some(recipe.physical_fingerprint),
                            rule: None,
                            children: Box::new([child_reference]),
                            facts: Box::new([]),
                            expected_cost_bits: None,
                            upper_cost_bits: None,
                        });
                    }
                }
            }
            if !children_feasible {
                if child_yielded {
                    return Ok(Some(sequence));
                }
                continue;
            }
            if child_bound.is_none()
                && self.recipe_is_provably_worse_with_children(
                    group,
                    goal,
                    &recipe,
                    BoundCheckLocation::AfterChildren,
                )? == Some(true)
            {
                if child_yielded {
                    return Ok(Some(sequence));
                }
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
                if child_yielded {
                    return Ok(Some(sequence));
                }
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
                if child_yielded {
                    return Ok(Some(sequence));
                }
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
            let mut yield_after_publication = false;
            // At most one ordinary admission/composition attempt per waiting
            // parent frame after a child yield. The attempt uses the same
            // budget ledger and CandidateId cursor, including rejection. This
            // is an explicit response opportunity, not a free full Cartesian
            // product or a claim that the whole coordinator turn is bounded.
            let mut yielded_response_attempted = false;
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
                    .take(if child_yielded { 1 } else { usize::MAX })
                    .collect::<Vec<_>>();
                for children in rechecks {
                    yielded_response_attempted = true;
                    let (frontier_changed, selected_changed, published_candidate) = self
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
                    if let Some(candidate) = published_candidate {
                        let (child_refs, cost) = combination_state
                            .priced
                            .get(&children)
                            .map(|cached| (cached.children.clone(), cached.cost))
                            .ok_or_else(|| {
                                paro_error::internal(
                                    "published child combination disappeared before timeline recording",
                                )
                            })?;
                        self.note_parent_publication(
                            group,
                            goal,
                            physical,
                            recipe.physical_fingerprint,
                            candidate,
                            child_refs,
                            cost,
                        );
                    }
                    if frontier_changed {
                        if self.note_physical_frontier_change(group, goal, selected_changed)? {
                            yield_after_publication = true;
                            break;
                        }
                    }
                }
            }
            if yield_after_publication || (child_yielded && yielded_response_attempted) {
                self.child_combination_states
                    .insert(recipe_key, combination_state);
                return Ok(Some(sequence));
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
                if child_yielded && yielded_response_attempted {
                    break;
                }
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
                yielded_response_attempted = true;
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
                    let (frontier_changed, selected_changed, published_candidate) = self
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
                    if let Some(candidate) = published_candidate {
                        let (child_refs, cost) = combination_state
                            .priced
                            .get(&child_ids)
                            .map(|cached| (cached.children.clone(), cached.cost))
                            .ok_or_else(|| {
                                paro_error::internal(
                                    "published child combination disappeared before timeline recording",
                                )
                            })?;
                        self.note_parent_publication(
                            group,
                            goal,
                            physical,
                            recipe.physical_fingerprint,
                            candidate,
                            child_refs,
                            cost,
                        );
                    }
                    if frontier_changed {
                        if self.note_physical_frontier_change(group, goal, selected_changed)? {
                            yield_after_publication = true;
                            break;
                        }
                    }
                    if yield_after_publication {
                        break;
                    }
                    continue;
                }
                if !pending_retry && combination_state.budget_rejected.contains(&child_ids) {
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
                let kernel_timer = CostPhaseTimer::start(&self.diagnostic_cost_phase_times, 0);
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
                drop(kernel_timer);
                if self.collect_rule_work_profile {
                    self.note_candidate_lifecycle(CandidateLifecycleEvent {
                        stage: CandidateLifecycleStage::TuplePriced,
                        elapsed_us: self.lifecycle_elapsed_us(),
                        group,
                        goal: Some(goal),
                        candidate: None,
                        source: None,
                        binding: None,
                        source_child: None,
                        logical: None,
                        physical: Some(physical),
                        recipe: Some(recipe.physical_fingerprint),
                        rule: None,
                        children: children.clone(),
                        facts: Box::new([]),
                        expected_cost_bits: Some(cost.score.range.expected.to_bits()),
                        upper_cost_bits: Some(cost.score.range.upper.to_bits()),
                    });
                }
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
                let (frontier_changed, selected_changed, published_candidate) = self
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
                if let Some(candidate) = published_candidate {
                    let (child_refs, cost) = combination_state
                        .priced
                        .get(&child_ids)
                        .map(|cached| (cached.children.clone(), cached.cost))
                        .ok_or_else(|| {
                            paro_error::internal(
                                "published child combination disappeared before timeline recording",
                            )
                        })?;
                    self.note_parent_publication(
                        group,
                        goal,
                        physical,
                        recipe.physical_fingerprint,
                        candidate,
                        child_refs,
                        cost,
                    );
                }
                if frontier_changed {
                    if self.note_physical_frontier_change(group, goal, selected_changed)? {
                        yield_after_publication = true;
                        break;
                    }
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
            if yield_after_publication || child_yielded {
                self.child_combination_states
                    .insert(recipe_key, combination_state);
                return Ok(Some(sequence));
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
                    let (frontier_changed, selected_changed, published_candidate) = self
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
                    if let Some(candidate) = published_candidate {
                        let (child_refs, cost) = combination_state
                            .priced
                            .get(&children)
                            .map(|cached| (cached.children.clone(), cached.cost))
                            .ok_or_else(|| {
                                paro_error::internal(
                                    "published child combination disappeared before timeline recording",
                                )
                            })?;
                        self.note_parent_publication(
                            group,
                            goal,
                            physical,
                            recipe.physical_fingerprint,
                            candidate,
                            child_refs,
                            cost,
                        );
                    }
                    if frontier_changed {
                        if self.note_physical_frontier_change(group, goal, selected_changed)? {
                            yield_after_publication = true;
                            break;
                        }
                    }
                }
            }
            if yield_after_publication {
                self.child_combination_states
                    .insert(recipe_key, combination_state);
                return Ok(Some(sequence));
            }
            combination_state.parent_frontier_revision = self
                .memo
                .group(group)
                .map(|group| group.physical_frontier_version())
                .unwrap_or(current_parent_frontier_revision);
            self.child_combination_states
                .insert(recipe_key, combination_state);
        }
        Ok(None)
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

fn validate_frozen_seed_tree(seed: &FrozenCandidate) -> Result<()> {
    fn visit(
        node: &FrozenCandidate,
        active: &mut BTreeSet<CandidateId>,
        seen: &mut BTreeSet<CandidateId>,
    ) -> Result<()> {
        if !active.insert(node.reference.candidate) {
            return Err(paro_error::internal(
                "strong incumbent seed contains a candidate cycle",
            ));
        }
        if seen.contains(&node.reference.candidate) {
            active.remove(&node.reference.candidate);
            return Ok(());
        }
        if node.winner.candidate != node.reference.candidate
            || node.physical.id != node.winner.expression
            || node.logical.id != node.physical.key.logical
            || node.physical.key.children.len() != node.winner.children.len()
            || node.children.len() != node.winner.children.len()
            || node
                .physical
                .key
                .children
                .iter()
                .zip(node.winner.children.iter())
                .any(|(group, child)| *group != child.group)
            || node
                .children
                .iter()
                .zip(node.winner.children.iter())
                .any(|(child, reference)| child.reference != *reference)
        {
            return Err(paro_error::internal(
                "strong incumbent seed lost an exact physical child or payload identity",
            ));
        }
        node.winner.cost.validate()?;
        for child in node.children.iter() {
            visit(child, active, seen)?;
        }
        active.remove(&node.reference.candidate);
        seen.insert(node.reference.candidate);
        Ok(())
    }

    visit(seed, &mut BTreeSet::new(), &mut BTreeSet::new())
}

fn seed_logical_shell_matches(
    source: &super::memo::LogicalExpr,
    target: &super::memo::LogicalExpr,
) -> bool {
    source.key.operator == target.key.operator
        && source.operator_tag == target.operator_tag
        && source.key.children.len() == target.key.children.len()
        && source.operator_encoding.as_deref() == target.operator_encoding.as_deref()
        // A core-only expression has no canonical byte encoding.  In that
        // case scalar IDs are the only exact shell witness and must match;
        // planner expressions carry operator_encoding and remain independent
        // of Memo-local scalar numbering.
        && (source.operator_encoding.is_some() || source.key.scalars == target.key.scalars)
}

fn frozen_seed_plan_identity(seed: &FrozenCandidate) -> Fingerprint {
    fn visit(node: &FrozenCandidate, builder: &mut StableFingerprintBuilder) {
        builder.write_bytes(b"paro.seed-plan-node.v1");
        builder.write_fingerprint(node.logical.key.operator);
        builder.write_u64(node.logical.key.children.len() as u64);
        builder.write_u64(node.logical.key.scalars.len() as u64);
        if let Some(encoding) = &node.logical.operator_encoding {
            builder.write_u64(1);
            builder.write_bytes(encoding);
        } else {
            builder.write_u64(0);
            for scalar in &node.logical.key.scalars {
                builder.write_u64(scalar.0 as u64);
            }
        }
        match node.logical.operator_tag {
            Some(tag) => {
                builder.write_u64(1);
                builder.write_u64(tag);
            }
            None => builder.write_u64(0),
        }
        builder.write_u64(node.physical.key.implementation.0 as u64);
        builder.write_fingerprint(node.physical.key.payload_fingerprint);
        builder.write_fingerprint(node.winner.physical_fingerprint);
        builder.write_u64(node.winner.enforcers.len() as u64);
        for enforcer in &node.winner.enforcers {
            builder.write_fingerprint(enforcer.stable_fingerprint());
        }
        write_cost_composition_fingerprint(builder, &node.winner.cost_composition);
        builder.write_u64(node.children.len() as u64);
        for child in &node.children {
            visit(child, builder);
        }
    }

    let mut builder = StableFingerprintBuilder::default();
    builder.write_bytes(b"paro.seed-plan.v1");
    visit(seed, &mut builder);
    builder.finish()
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
            let index = frontier
                .binary_search_by_key(candidate, |child| child.candidate)
                .map_err(|_| {
                    paro_error::internal("child combination references a stale candidate")
                })?;
            Ok(frontier[index])
        })
        .collect::<Result<Vec<_>>>()
        .map(Vec::into_boxed_slice)
}

fn write_f64_fingerprint(builder: &mut StableFingerprintBuilder, value: f64) {
    builder.write_bytes(&value.to_bits().to_le_bytes());
}

fn write_optimization_goal_fingerprint(
    builder: &mut StableFingerprintBuilder,
    goal: OptimizationGoal,
) {
    builder.write_u64(goal.required.0 as u64);
    builder.write_u64(goal.row_goal.stable_tag());
    builder.write_u64(goal.objective.stable_tag());
    builder.write_u64(goal.grant.stable_tag());
    builder.write_u64(goal.context.0 as u64);
}

/// Fingerprint a goal without relying on Memo-local property/context IDs.
/// The exact IDs still select the destination objects during search, while
/// this digest is the cost-context attestation used across independently
/// constructed Memos.
fn write_semantic_goal_fingerprint(
    builder: &mut StableFingerprintBuilder,
    memo: &Memo,
    goal: OptimizationGoal,
    grant_classes: &BTreeMap<ResourceGrantClassId, ResourceGrantClass>,
) -> Result<()> {
    builder.write_bytes(b"paro.semantic-goal.v1");
    builder.write_u64(goal.row_goal.stable_tag());
    builder.write_u64(goal.objective.stable_tag());
    match goal.grant {
        GrantGoalKey::Invariant(set) => {
            builder.write_u64(0);
            builder.write_u64(set.0 as u64);
        }
        GrantGoalKey::Parallelism { admissible, tasks } => {
            builder.write_u64(1);
            builder.write_u64(admissible.0 as u64);
            builder.write_u64(u64::from(tasks));
        }
        GrantGoalKey::Class(class) => {
            builder.write_u64(2);
            if let Some(class) = grant_classes.get(&class) {
                builder.write_u64(class.hard_memory_bytes);
                builder.write_u64(match class.spill_policy {
                    SpillPolicy::Forbidden => 0,
                    SpillPolicy::Allowed => 1,
                });
                builder.write_u64(u64::from(class.max_parallel_tasks));
            } else {
                // A class not yet primed is not a usable priced context. The
                // ID is retained only in the error path to make the failure
                // actionable rather than silently weakening the attestation.
                return Err(paro_error::internal(format!(
                    "priced incumbent references an unprimed grant class: {class:?}"
                )));
            }
        }
    }
    let required = memo
        .required(goal.required)
        .ok_or_else(|| paro_error::internal("priced incumbent has unknown required properties"))?;
    write_required_properties_fingerprint(builder, required);
    let context = memo
        .optimization_context(goal.context)
        .ok_or_else(|| paro_error::internal("priced incumbent has unknown optimization context"))?;
    builder.write_bytes(b"paro.optimization-context.v1");
    builder.write_u64(context.required_region_facets().len() as u64);
    for facet in context.required_region_facets() {
        builder.write_fingerprint(*facet);
    }
    builder.write_u64(context.filterable_sources().len() as u64);
    for source in context.filterable_sources() {
        builder.write_u64(source.0 as u64);
    }
    builder.write_u64(match context.phase() {
        super::memo::OptimizationPhase::Physical => 0,
        super::memo::OptimizationPhase::Logical => 1,
        super::memo::OptimizationPhase::Cost => 2,
        super::memo::OptimizationPhase::Admission => 3,
        super::memo::OptimizationPhase::Execution => 4,
    });
    match context.ownership() {
        super::memo::SharedOwnership::Private => builder.write_u64(0),
        super::memo::SharedOwnership::Shared { owner } => {
            builder.write_u64(1);
            builder.write_fingerprint(owner);
        }
        super::memo::SharedOwnership::Cte { producer } => {
            builder.write_u64(2);
            builder.write_fingerprint(producer);
        }
    }
    match context.continuation() {
        super::memo::ContinuationContract::Complete => builder.write_u64(0),
        super::memo::ContinuationContract::Prefix { frontier } => {
            builder.write_u64(1);
            builder.write_fingerprint(frontier);
        }
        super::memo::ContinuationContract::ParentResponse { response } => {
            builder.write_u64(2);
            builder.write_fingerprint(response);
        }
    }
    Ok(())
}

fn write_required_properties_fingerprint(
    builder: &mut StableFingerprintBuilder,
    required: &super::properties::RequiredProperties,
) {
    builder.write_bytes(b"paro.required-properties.v1");
    match &required.ordering {
        OrderingRequirement::Any => builder.write_u64(0),
        OrderingRequirement::Ordered(ordering) => {
            builder.write_u64(1);
            builder.write_u64(match ordering.scope {
                OrderingScope::PartitionLocal => 0,
                OrderingScope::Global => 1,
            });
            builder.write_u64(ordering.keys.len() as u64);
            for key in &ordering.keys {
                builder.write_u64(key.column.0 as u64);
                builder.write_u64(match key.direction {
                    super::properties::SortDirection::Asc => 0,
                    super::properties::SortDirection::Desc => 1,
                });
                builder.write_u64(match key.nulls {
                    super::properties::NullOrder::First => 0,
                    super::properties::NullOrder::Last => 1,
                });
                match key.collation {
                    Some(collation) => {
                        builder.write_u64(1);
                        builder.write_u64(collation.0 as u64);
                    }
                    None => builder.write_u64(0),
                }
            }
        }
    }
    match &required.partitioning {
        PartitioningRequirement::Any => builder.write_u64(0),
        PartitioningRequirement::Singleton => builder.write_u64(1),
        PartitioningRequirement::Hash { keys, partitions } => {
            builder.write_u64(2);
            write_columns_fingerprint(builder, keys);
            write_optional_u16(builder, *partitions);
        }
        PartitioningRequirement::Range { keys, partitions } => {
            builder.write_u64(3);
            write_columns_fingerprint(builder, keys);
            write_optional_u16(builder, *partitions);
        }
    }
    write_materialization_fingerprint(builder, &required.materialization);
    match &required.mutation_safety {
        MutationSafetyRequirement::None => builder.write_u64(0),
        MutationSafetyRequirement::StableReadBeforeWrite { targets, snapshot } => {
            builder.write_u64(1);
            builder.write_u64(targets.len() as u64);
            for target in targets {
                builder.write_u64(target.0 as u64);
            }
            builder.write_u64(snapshot.0 as u64);
        }
    }
    match required.representation {
        RepresentationRequirement::Any => builder.write_u64(0),
        RepresentationRequirement::Flat => builder.write_u64(1),
        RepresentationRequirement::Factorized(spec) => {
            builder.write_u64(2);
            builder.write_u64(spec.0 as u64);
        }
    }
    builder.write_u64(match required.replayability {
        ReplayabilityRequirement::Any => 0,
        ReplayabilityRequirement::Rewindable => 1,
    });
    match required.result_guarantee {
        ResultGuarantee::Exact => builder.write_u64(0),
        ResultGuarantee::ApproximateAllowed(policy) => {
            builder.write_u64(1);
            builder.write_u64(policy.0 as u64);
        }
    }
}

fn write_columns_fingerprint<'a>(
    builder: &mut StableFingerprintBuilder,
    columns: impl IntoIterator<Item = &'a super::ids::ColumnId>,
) {
    let columns = columns.into_iter().collect::<Vec<_>>();
    builder.write_u64(columns.len() as u64);
    for column in columns {
        builder.write_u64(column.0 as u64);
    }
}

fn write_optional_u16(builder: &mut StableFingerprintBuilder, value: Option<u16>) {
    match value {
        Some(value) => {
            builder.write_u64(1);
            builder.write_u64(u64::from(value));
        }
        None => builder.write_u64(0),
    }
}

fn write_materialization_fingerprint(
    builder: &mut StableFingerprintBuilder,
    materialization: &MaterializationRequirement,
) {
    builder.write_u64(materialization.values.len() as u64);
    for value in &materialization.values {
        builder.write_u64(value.0 as u64);
    }
    builder.write_u64(materialization.locators.len() as u64);
    for (relation, requirement) in &materialization.locators {
        builder.write_u64(relation.0 as u64);
        match requirement.kind {
            Some(kind) => {
                builder.write_u64(1);
                builder.write_u64(kind.0 as u64);
            }
            None => builder.write_u64(0),
        }
        builder.write_u64(match requirement.use_kind {
            super::properties::LocatorUse::ReadStable => 0,
            super::properties::LocatorUse::WriteTarget => 1,
        });
    }
}

fn budget_exhaustion_counter_name(dimension: BudgetDimension) -> &'static str {
    match dimension {
        BudgetDimension::Group => "budget_exhaustion_group",
        BudgetDimension::CompositionGroup => "budget_exhaustion_composition_group",
        BudgetDimension::LogicalExprPerGroup => "budget_exhaustion_logical_expr_per_group",
        BudgetDimension::CompositionLogicalExprPerGroup => {
            "budget_exhaustion_composition_logical_expr_per_group"
        }
        BudgetDimension::PhysicalExprPerGroup => "budget_exhaustion_physical_expr_per_group",
        BudgetDimension::InterestingGoalPerGroup => "budget_exhaustion_interesting_goal_per_group",
        BudgetDimension::RuleFirePerGroup => "budget_exhaustion_rule_fire_per_group",
        BudgetDimension::CompositionRuleFirePerGroup => {
            "budget_exhaustion_composition_rule_fire_per_group"
        }
        BudgetDimension::RuleWorkPerGroup => "budget_exhaustion_rule_work_per_group",
        BudgetDimension::CompositionRuleWorkPerGroup => {
            "budget_exhaustion_composition_rule_work_per_group"
        }
        BudgetDimension::ChildFrontierCombination => "budget_exhaustion_child_frontier_combination",
        BudgetDimension::JoinConnectedPair => "budget_exhaustion_join_connected_pair",
        BudgetDimension::GraphFrontier => "budget_exhaustion_graph_frontier",
        BudgetDimension::FactorizationVariant => "budget_exhaustion_factorization_variant",
        BudgetDimension::MultiwayJoinCandidate => "budget_exhaustion_multiway_join_candidate",
        BudgetDimension::SearchCandidate => "budget_exhaustion_search_candidate",
        BudgetDimension::SearchFusion => "budget_exhaustion_search_fusion",
        BudgetDimension::ParameterContext => "budget_exhaustion_parameter_context",
        BudgetDimension::CompositeRegionCandidate => "budget_exhaustion_composite_region_candidate",
        BudgetDimension::RecursiveCandidate => "budget_exhaustion_recursive_candidate",
        BudgetDimension::EnforcerChain => "budget_exhaustion_enforcer_chain",
        BudgetDimension::WinnerFrontier => "budget_exhaustion_winner_frontier",
    }
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
    builder.write_bytes(b"paro.child-combination-cost-context.v2");
    builder.write_u64(memo.cost_epoch_value());
    builder.write_u64(goal.required.0 as u64);
    builder.write_u64(goal.row_goal.stable_tag());
    builder.write_u64(goal.objective.stable_tag());
    builder.write_u64(goal.grant.stable_tag());
    builder.write_u64(goal.context.0 as u64);
    builder.write_fingerprint(memo.calibration_fingerprint());
    builder.write_fingerprint(
        *recipe
            .immutable_cost_identity
            .get_or_init(|| immutable_recipe_cost_identity(recipe)),
    );

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

fn immutable_recipe_cost_identity(recipe: &CostRecipe) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_bytes(b"paro.immutable-recipe-cost.v1");
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

    builder.finish()
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

/// A selected-path binding observes facts/statistics of its exact operands,
/// not the complete logical frontiers below them.  The normal matcher keeps
/// its frontier reads separately, so this direct quality task cannot suppress
/// later peer exploration or be invalidated by an unrelated child alternative.
fn pattern_binding_fact_reads(memo: &Memo, binding: &PatternBinding) -> Result<Box<[PatternRead]>> {
    fn collect(operand: &PatternOperand, memo: &Memo, groups: &mut BTreeSet<GroupId>) {
        match operand {
            PatternOperand::Group(group) => {
                groups.insert(memo.canonical_group(*group));
            }
            PatternOperand::Expression {
                group, children, ..
            } => {
                groups.insert(memo.canonical_group(*group));
                for child in children {
                    collect(child, memo, groups);
                }
            }
        }
    }

    let mut groups = BTreeSet::new();
    collect(&binding.root, memo, &mut groups);
    groups
        .into_iter()
        .map(|group| PatternRead::facts_from_group(memo, group))
        .collect::<Result<Vec<_>>>()
        .map(Vec::into_boxed_slice)
}

fn pattern_operand_work_units(operand: &PatternOperand) -> usize {
    match operand {
        PatternOperand::Group(_) => 1,
        PatternOperand::Expression { children, .. } => {
            1 + children
                .iter()
                .map(pattern_operand_work_units)
                .sum::<usize>()
        }
    }
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
