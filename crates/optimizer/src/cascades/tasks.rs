// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query-local task identity and publication state for Cascades search.
//!
//! This module is deliberately a small state machine, not a second Memo or a
//! second winner cache.  The Memo remains the owner of logical/physical
//! expressions and candidate payloads; [`TaskRegistry`] only owns the
//! identity, dependency, progress and ownership bookkeeping needed to make a
//! search task resumable.  All keys are interned from exact structural values
//! so a digest collision can never turn two tasks into one task.

use std::collections::{BTreeMap, BTreeSet};

use paro_common::error::{self as paro_error, Result};

use super::budget::BudgetDimension;
use super::ids::{
    CalibrationRevisionId, CandidateId, Fingerprint, GroupId, ImplementationId, LogicalExprId,
    PhysicalExprId, RuleId,
};
use super::memo::{Memo, OptimizationGoal};
use super::rules::{PatternBinding, PatternOperand, PatternRead};

macro_rules! task_id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub u32);

        impl $name {
            pub const INVALID: Self = Self(u32::MAX);

            pub fn new(index: usize) -> Self {
                assert!(
                    index < u32::MAX as usize,
                    concat!(stringify!($name), " overflow")
                );
                Self(index as u32)
            }

            pub const fn index(self) -> usize {
                self.0 as usize
            }
        }
    };
}

task_id_type!(TaskIntentId);
task_id_type!(InputRevisionId);
task_id_type!(EvaluationId);
task_id_type!(TaskId);
task_id_type!(ReadSetId);
task_id_type!(CursorId);
task_id_type!(WaiterId);
task_id_type!(ReservationId);
task_id_type!(TaskObjectId);
task_id_type!(BoundProofId);
task_id_type!(CompletionObligationId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TaskKind {
    Discover,
    Transform,
    Implement,
    Optimize,
    Cost,
}

impl TaskKind {
    const ALL: [Self; 5] = [
        Self::Discover,
        Self::Transform,
        Self::Implement,
        Self::Optimize,
        Self::Cost,
    ];

    const fn index(self) -> usize {
        match self {
            Self::Discover => 0,
            Self::Transform => 1,
            Self::Implement => 2,
            Self::Optimize => 3,
            Self::Cost => 4,
        }
    }
}

/// The semantic work requested by a task.  Physical goals are deliberately
/// absent from discovery/transformation identities: a new grant does not
/// recreate a logical equivalence closure.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TaskIntent {
    Discover {
        expression: LogicalExprId,
        rule: RuleId,
    },
    Transform {
        expression: LogicalExprId,
        rule: RuleId,
        /// Exact structural binding identity. The digest remains inside the
        /// binding as a deterministic bucket, but is never the sole task
        /// identity because colliding bindings must remain independent.
        binding: PatternBinding,
    },
    Implement {
        expression: LogicalExprId,
        goal: OptimizationGoal,
        implementation: ImplementationId,
    },
    Optimize {
        group: GroupId,
        goal: OptimizationGoal,
    },
    Cost {
        physical: PhysicalExprId,
        goal: OptimizationGoal,
        children: Box<[CandidateId]>,
    },
}

impl TaskIntent {
    pub const fn kind(&self) -> TaskKind {
        match self {
            Self::Discover { .. } => TaskKind::Discover,
            Self::Transform { .. } => TaskKind::Transform,
            Self::Implement { .. } => TaskKind::Implement,
            Self::Optimize { .. } => TaskKind::Optimize,
            Self::Cost { .. } => TaskKind::Cost,
        }
    }

    pub fn subproblem_key(&self) -> Option<SubproblemKey> {
        match self {
            Self::Optimize { group, goal } => Some(SubproblemKey {
                group: *group,
                goal: *goal,
            }),
            _ => None,
        }
    }
}

/// Canonical physical subproblem identity.  The complete goal remains part
/// of the key: source response, memory completion, task supply and grant
/// differences are observable parent dimensions and cannot be collapsed to a
/// single winner per group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubproblemKey {
    pub group: GroupId,
    pub goal: OptimizationGoal,
}

/// Exact input revision of one task.  A revision is interned from the actual
/// ReadSet, not from a global Memo generation, so unrelated group changes do
/// not invalidate all work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EvaluationKey {
    pub intent: TaskIntentId,
    pub inputs: InputRevisionId,
}

/// A read cursor over semantic Memo facts/frontiers.  This is the same
/// PatternRead contract used by transformation matching; task publication
/// does not invent a weaker second versioning scheme.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReadSet {
    reads: Box<[PatternRead]>,
}

impl ReadSet {
    /// Build the overwhelmingly common one-group read without constructing a
    /// temporary ordered map.  A single read already satisfies the canonical
    /// one-read-per-group invariant, so this is the allocation-free part of
    /// the normalization performed by [`Self::new`].
    pub fn single(read: PatternRead) -> Self {
        Self {
            reads: Box::new([read]),
        }
    }

    pub fn new(reads: impl IntoIterator<Item = PatternRead>) -> Self {
        let mut reads = reads.into_iter().collect::<Vec<_>>();
        // Normalize to one cursor per group.  The cursor's scope is the union
        // of the categories actually observed by all callers, so this
        // reduces identity/storage without dropping a frontier or fact
        // dependency.  It also lets a structural-only reader avoid inheriting
        // a statistics subscription merely because another task reads the
        // same group at a different point in the Memo.
        reads.sort_unstable_by_key(|read| read.group);
        let mut normalized: Vec<PatternRead> = Vec::with_capacity(reads.len());
        for read in reads {
            let mut merged = false;
            for previous in normalized.iter_mut().rev() {
                if previous.group != read.group {
                    break;
                }
                // Two observations of the same category may have been taken
                // at different Memo revisions.  They are mergeable only when
                // their snapshots agree; otherwise retaining both cursors is
                // required to prevent a newer observation from hiding an
                // older stale dependency.
                if previous.can_union(read) {
                    *previous = previous.union(read);
                    merged = true;
                    break;
                }
            }
            if !merged {
                normalized.push(read);
            }
        }
        Self {
            reads: normalized.into_boxed_slice(),
        }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn reads(&self) -> &[PatternRead] {
        &self.reads
    }

    pub fn is_current(&self, memo: &Memo) -> Result<bool> {
        self.reads
            .iter()
            .try_fold(true, |current, read| Ok(current && read.is_current(memo)?))
    }

    fn is_current_except(
        &self,
        memo: &Memo,
        locally_written_groups: &BTreeSet<GroupId>,
    ) -> Result<bool> {
        self.reads.iter().try_fold(true, |current, read| {
            Ok(current
                && (locally_written_groups.contains(&read.group)
                    || read.is_current_for_publication(memo)?))
        })
    }
}

/// A task's resumable progress cursor.  `complete` means the cursor has
/// consumed its declared enumeration domain; it does not mean that a caller
/// has found a feasible plan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cursor {
    pub position: u64,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum StopReason {
    ProofStop { proof: BoundProofId },
    EconomicStop { calibration: CalibrationRevisionId },
    ResourceStop { dimension: BudgetDimension },
    CalibrationUnavailable,
    Deadline,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskOutcome {
    Progress {
        cursor: CursorId,
    },
    NoChange {
        reads: ReadSetId,
    },
    Awaiting {
        cursor: CursorId,
        dependencies: Box<[TaskId]>,
    },
    Invalidated {
        cursor: CursorId,
    },
    ProvenOptimal {
        candidate: CandidateId,
        certificate: BoundProofId,
    },
    ProvenNoPlanBelow {
        threshold: u64,
        certificate: BoundProofId,
    },
    /// No candidate in the consumed prefix. Even a complete local cursor
    /// does not prove infeasibility of future logical/implementation domains.
    /// The owning evaluation's goal, ReadSet and cursor delimit this result.
    NoCandidate {
        cursor: CursorId,
    },
    Failed {
        detail: String,
    },
    Suspended {
        cursor: CursorId,
        reason: StopReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskState {
    Runnable,
    Running,
    Awaiting,
    Suspended,
    Completed,
    Invalidated,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskRequest {
    /// This caller owns the first computation of the EvaluationKey.
    Leader(TaskId),
    /// A computation is already in flight.  The caller is a waiter, not a
    /// second evaluation of the same subproblem.
    Subscriber { task: TaskId, waiter: WaiterId },
    /// An exact completed/failed outcome can be consumed without recomputing.
    Reused {
        task: TaskId,
        outcome: Option<TaskOutcome>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskWakeup {
    pub task: TaskId,
    pub waiter: Option<WaiterId>,
    pub outcome: Option<TaskOutcome>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskKindProfile {
    pub requests: u64,
    pub unique_intents: u64,
    pub unique_evaluations: u64,
    pub reused_evaluations: u64,
    /// An existing exact evaluation reopened after an incomplete cursor,
    /// suspension, or invalidation. This is distinct from a first leader and
    /// from a completed evaluation consumed as a reuse.
    pub reopened_evaluations: u64,
    pub single_flight_subscriptions: u64,
    pub started: u64,
    pub completed: u64,
    pub suspended: u64,
    pub invalidated: u64,
    pub awaiting: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskRegistryProfile {
    pub requests: u64,
    pub unique_intents: u64,
    pub unique_evaluations: u64,
    /// Number of distinct physical `(group, goal)` subproblems represented by
    /// the exact task-intent catalog. This is diagnostic-only and is derived
    /// from the same identity table; it is not a second cache.
    pub unique_subproblems: u64,
    pub reused_evaluations: u64,
    pub reopened_evaluations: u64,
    pub single_flight_subscriptions: u64,
    pub started: u64,
    pub completed: u64,
    pub suspended: u64,
    pub invalidated: u64,
    pub awaiting: u64,
    pub reservation_attempts: u64,
    pub reservation_reuses: u64,
    pub reservation_rejections: u64,
    pub bound_proofs: u64,
    /// Diagnostic attribution by semantic task kind. The values are derived
    /// from the same exact TaskIntent state machine; no second task ledger is
    /// maintained for profiling.
    pub by_kind: BTreeMap<TaskKind, TaskKindProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundContext {
    pub group: GroupId,
    pub goal: OptimizationGoal,
    pub reads: ReadSetId,
    /// Identity of the declared search domain.  A numeric candidate estimate
    /// is never accepted as this proof-domain identity implicitly.
    pub search_domain: Fingerprint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundProofKind {
    Lower { value: u64 },
    Upper { value: u64, candidate: CandidateId },
    NoPlanBelow { threshold: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundProof {
    pub id: BoundProofId,
    pub task: TaskId,
    pub context: BoundContext,
    pub kind: BoundProofKind,
}

impl BoundProof {
    pub fn is_current(&self, registry: &TaskRegistry, memo: &Memo) -> Result<bool> {
        let Some(task) = registry.task(self.task) else {
            return Ok(false);
        };

        // A proof is scoped to the Optimize intent which produced it.  A
        // task id alone is not enough: group merges can redirect an old task
        // while leaving its record addressable in the registry.
        let context_matches = matches!(
            registry.intent(task.intent),
            Some(TaskIntent::Optimize { group, goal })
                if registry.canonical_group(*group)
                    == registry.canonical_group(self.context.group)
                    && *goal == self.context.goal
        );
        if !context_matches {
            return Ok(false);
        }

        // While an owner is still active, the proof is available to the
        // owner and its dependants. Once the owner completes, retain it only
        // when the completion outcome explicitly names this certificate.
        // A normal Progress/NoChange completion must not accidentally turn a
        // provisional bound into a durable pruning proof.
        let certified = match task.state {
            TaskState::Runnable | TaskState::Running | TaskState::Awaiting => true,
            TaskState::Completed => matches!(
                task.outcome,
                Some(
                    TaskOutcome::ProvenOptimal { certificate, .. }
                        | TaskOutcome::ProvenNoPlanBelow { certificate, .. }
                ) if certificate == self.id
            ),
            TaskState::Suspended | TaskState::Invalidated | TaskState::Failed => false,
        };
        if !certified {
            return Ok(false);
        }

        registry
            .read_set(self.context.reads)
            .ok_or_else(|| paro_error::internal("bound proof references unknown read set"))?
            .is_current(memo)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CompletionObligation {
    pub id: CompletionObligationId,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct TaskRecord {
    pub id: TaskId,
    pub intent: TaskIntentId,
    pub evaluation: EvaluationKey,
    pub read_set: ReadSetId,
    pub cursor: CursorId,
    /// Previous exact evaluation of the same semantic intent.  A changed
    /// ReadSet creates a new evaluation identity, but the task protocol still
    /// exposes the predecessor so an owner can carry a safe enumeration
    /// cursor across an append-only frontier.  The predecessor's outcome is
    /// never reused as a current result.
    predecessor: Option<TaskId>,
    pub state: TaskState,
    pub outcome: Option<TaskOutcome>,
    dependencies: BTreeSet<TaskId>,
    waiters: BTreeSet<WaiterId>,
    obligations: BTreeSet<CompletionObligationId>,
}

#[derive(Debug, Clone)]
struct Waiter {
    id: WaiterId,
    task: TaskId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JournalEntry {
    Reserved {
        reservation: ReservationId,
        units: u64,
    },
    Allocated {
        object: TaskObjectId,
    },
    Published {
        object: TaskObjectId,
    },
    RolledBack {
        reservation: ReservationId,
        object_count: usize,
    },
}

#[derive(Debug, Default)]
struct TaskSegment {
    reservations: BTreeMap<ReservationId, u64>,
    uncommitted_objects: BTreeSet<TaskObjectId>,
    journal: Vec<JournalEntry>,
}

#[derive(Debug, Clone, Copy)]
struct ReservationRecord {
    task: TaskId,
    units: u64,
    committed: bool,
}

/// Single-owner task registry.  It is intentionally `&mut` driven: the
/// publication boundary validates reads, changes state, registers
/// dependencies and emits wakeups in one linearizable operation for the
/// single-worker engine.  A future worker pool can put a lock around this
/// same protocol without changing task semantics.
#[derive(Debug, Default)]
pub struct TaskRegistry {
    intents: Vec<TaskIntent>,
    intent_index: BTreeMap<TaskIntent, TaskIntentId>,
    read_sets: Vec<ReadSet>,
    read_set_index: BTreeMap<ReadSet, ReadSetId>,
    input_revisions: Vec<ReadSetId>,
    input_revision_index: BTreeMap<ReadSetId, InputRevisionId>,
    evaluations: BTreeMap<EvaluationKey, TaskId>,
    latest_tasks_by_intent: BTreeMap<TaskIntentId, TaskId>,
    tasks: Vec<TaskRecord>,
    waiters: BTreeMap<WaiterId, Waiter>,
    dependents: BTreeMap<TaskId, BTreeSet<TaskId>>,
    cursors: Vec<Cursor>,
    cursor_index: BTreeMap<Cursor, CursorId>,
    bounds: Vec<BoundProof>,
    obligations: BTreeMap<CompletionObligationId, CompletionObligation>,
    next_waiter: u32,
    next_reservation: u32,
    next_object: u32,
    next_obligation: u32,
    segments: BTreeMap<TaskId, TaskSegment>,
    reservations: BTreeMap<ReservationId, ReservationRecord>,
    reservation_slots: BTreeMap<(TaskId, u64), ReservationId>,
    published_objects: BTreeMap<TaskObjectId, TaskId>,
    committed_units: u64,
    reserved_units: u64,
    reservation_limit: Option<u64>,
    profile: TaskRegistryProfile,
    // A task kind is a closed, five-value enum. Keep its diagnostic counters
    // in fixed slots so normal requests do not mutate a tree or allocate a
    // map entry on the first task of each kind.
    kind_profiles: [TaskKindProfile; 5],
    group_redirects: BTreeMap<GroupId, GroupId>,
    group_revisions: BTreeMap<GroupId, u64>,
}

impl TaskRegistry {
    pub fn set_reservation_limit(&mut self, limit: Option<u64>) {
        self.reservation_limit = limit;
    }

    pub fn profile(&self) -> TaskRegistryProfile {
        let mut profile = self.profile.clone();
        profile.unique_intents = self.intents.len() as u64;
        profile.unique_evaluations = self.evaluations.len() as u64;
        profile.unique_subproblems = self
            .intents
            .iter()
            .filter_map(TaskIntent::subproblem_key)
            .collect::<BTreeSet<_>>()
            .len() as u64;
        profile.by_kind = TaskKind::ALL
            .into_iter()
            .zip(self.kind_profiles.iter().cloned())
            .filter(|(_, profile)| profile != &TaskKindProfile::default())
            .collect();
        profile
    }

    fn kind_profile_mut(&mut self, kind: TaskKind) -> &mut TaskKindProfile {
        &mut self.kind_profiles[kind.index()]
    }

    pub fn intern_intent(&mut self, intent: TaskIntent) -> TaskIntentId {
        if let Some(id) = self.intent_index.get(&intent).copied() {
            return id;
        }
        let profile = self.kind_profile_mut(intent.kind());
        profile.unique_intents = profile.unique_intents.saturating_add(1);
        let id = TaskIntentId::new(self.intents.len());
        self.intents.push(intent.clone());
        self.intent_index.insert(intent, id);
        id
    }

    pub fn intent(&self, id: TaskIntentId) -> Option<&TaskIntent> {
        self.intents.get(id.index())
    }

    pub fn intern_read_set(&mut self, read_set: ReadSet) -> ReadSetId {
        if let Some(id) = self.read_set_index.get(&read_set).copied() {
            return id;
        }
        let id = ReadSetId::new(self.read_sets.len());
        self.read_sets.push(read_set.clone());
        self.read_set_index.insert(read_set, id);
        id
    }

    pub fn read_set(&self, id: ReadSetId) -> Option<&ReadSet> {
        self.read_sets.get(id.index())
    }

    pub fn intern_input_revision(&mut self, read_set: ReadSetId) -> Result<InputRevisionId> {
        if read_set.index() >= self.read_sets.len() {
            return Err(paro_error::internal(
                "input revision references unknown read set",
            ));
        }
        if let Some(id) = self.input_revision_index.get(&read_set).copied() {
            return Ok(id);
        }
        let id = InputRevisionId::new(self.input_revisions.len());
        self.input_revisions.push(read_set);
        self.input_revision_index.insert(read_set, id);
        Ok(id)
    }

    pub fn request(&mut self, intent: TaskIntent, reads: ReadSet) -> Result<TaskRequest> {
        self.request_with_current_reads(intent, reads, None)
    }

    /// Request a task while checking the actual Memo revisions represented by
    /// its ReadSet.  The plain `request` method remains useful to the small
    /// registry state-machine tests, but production callers must use this
    /// entry point so a completed outcome cannot be reused merely because the
    /// structural ReadSet has the same shape.
    pub fn request_current(
        &mut self,
        intent: TaskIntent,
        reads: ReadSet,
        memo: &Memo,
    ) -> Result<TaskRequest> {
        let intent = canonicalize_task_intent(memo, intent);
        let reads = canonicalize_read_set(memo, reads);
        self.request_with_current_reads(intent, reads, Some(memo))
    }

    fn request_with_current_reads(
        &mut self,
        intent: TaskIntent,
        reads: ReadSet,
        memo: Option<&Memo>,
    ) -> Result<TaskRequest> {
        self.profile.requests = self.profile.requests.saturating_add(1);
        let kind = intent.kind();
        let profile = self.kind_profile_mut(kind);
        profile.requests = profile.requests.saturating_add(1);
        let intent = self.intern_intent(intent);
        let read_set = self.intern_read_set(reads);
        let inputs = self.intern_input_revision(read_set)?;
        let evaluation = EvaluationKey { intent, inputs };
        if let Some(task) = self.evaluations.get(&evaluation).copied() {
            let state = self
                .task(task)
                .ok_or_else(|| paro_error::internal("task evaluation index is corrupt"))?
                .state;
            if !matches!(state, TaskState::Running)
                && !matches!(state, TaskState::Invalidated)
                && memo.is_some_and(|memo| {
                    self.read_set(read_set)
                        .is_none_or(|read_set| !read_set.is_current(memo).unwrap_or(false))
                })
            {
                // Keep the exact task/evaluation identity, but discard its
                // outcome and local segment before reopening it.  This is
                // selective invalidation: unrelated tasks retain their
                // completed results and the task's continuation cursor is
                // still available to the new leader.
                self.invalidate(task)?;
            }
            let state = self
                .task(task)
                .ok_or_else(|| paro_error::internal("task evaluation index is corrupt"))?
                .state;
            return match state {
                TaskState::Completed => {
                    let incomplete = self
                        .task(task)
                        .and_then(|record| record.outcome.as_ref())
                        .and_then(|outcome| match outcome {
                            TaskOutcome::Progress { cursor }
                            | TaskOutcome::NoCandidate { cursor } => self.cursor(*cursor),
                            _ => None,
                        })
                        .is_some_and(|cursor| !cursor.complete);
                    if incomplete {
                        if memo.is_some() {
                            // An incomplete cursor is resumable, not an
                            // instruction to restart on every recursive
                            // visit.  The current-read request already
                            // proved that this exact task domain has not
                            // changed; the engine will request a new exact
                            // evaluation when a logical or physical input
                            // frontier advances.  Keeping the completed
                            // prefix here prevents a child scan from
                            // repeatedly reopening the whole parent search.
                            self.profile.reused_evaluations =
                                self.profile.reused_evaluations.saturating_add(1);
                            let profile = self.kind_profile_mut(kind);
                            profile.reused_evaluations =
                                profile.reused_evaluations.saturating_add(1);
                            Ok(TaskRequest::Reused {
                                task,
                                outcome: self.task(task).and_then(|record| record.outcome.clone()),
                            })
                        } else {
                            // The legacy, non-Memo registry API has no input
                            // revision to tell it whether a continuation is
                            // still current. Preserve its explicit
                            // continuation behavior for the small state
                            // machine tests and callers.
                            self.task_mut(task)?.state = TaskState::Runnable;
                            self.task_mut(task)?.outcome = None;
                            self.record_reopened_evaluation(kind);
                            Ok(TaskRequest::Leader(task))
                        }
                    } else {
                        self.profile.reused_evaluations =
                            self.profile.reused_evaluations.saturating_add(1);
                        let profile = self.kind_profile_mut(kind);
                        profile.reused_evaluations = profile.reused_evaluations.saturating_add(1);
                        Ok(TaskRequest::Reused {
                            task,
                            outcome: self.task(task).and_then(|record| record.outcome.clone()),
                        })
                    }
                }
                TaskState::Failed => {
                    self.profile.reused_evaluations =
                        self.profile.reused_evaluations.saturating_add(1);
                    let profile = self.kind_profile_mut(kind);
                    profile.reused_evaluations = profile.reused_evaluations.saturating_add(1);
                    Ok(TaskRequest::Reused {
                        task,
                        outcome: self.task(task).and_then(|record| record.outcome.clone()),
                    })
                }
                TaskState::Invalidated | TaskState::Suspended => {
                    self.task_mut(task)?.state = TaskState::Runnable;
                    self.task_mut(task)?.outcome = None;
                    self.record_reopened_evaluation(kind);
                    Ok(TaskRequest::Leader(task))
                }
                TaskState::Runnable | TaskState::Running | TaskState::Awaiting => {
                    let waiter = self.add_waiter(task)?;
                    self.profile.single_flight_subscriptions =
                        self.profile.single_flight_subscriptions.saturating_add(1);
                    let profile = self.kind_profile_mut(kind);
                    profile.single_flight_subscriptions =
                        profile.single_flight_subscriptions.saturating_add(1);
                    Ok(TaskRequest::Subscriber { task, waiter })
                }
            };
        }
        let task = TaskId::new(self.tasks.len());
        let cursor = self.intern_cursor(Cursor::default());
        let predecessor = self.latest_tasks_by_intent.get(&intent).copied();
        self.tasks.push(TaskRecord {
            id: task,
            intent,
            evaluation,
            read_set,
            cursor,
            predecessor,
            state: TaskState::Runnable,
            outcome: None,
            dependencies: BTreeSet::new(),
            waiters: BTreeSet::new(),
            obligations: BTreeSet::new(),
        });
        self.evaluations.insert(evaluation, task);
        self.latest_tasks_by_intent.insert(intent, task);
        let profile = self.kind_profile_mut(kind);
        profile.unique_evaluations = profile.unique_evaluations.saturating_add(1);
        Ok(TaskRequest::Leader(task))
    }

    fn record_reopened_evaluation(&mut self, kind: TaskKind) {
        self.profile.reopened_evaluations = self.profile.reopened_evaluations.saturating_add(1);
        let profile = self.kind_profile_mut(kind);
        profile.reopened_evaluations = profile.reopened_evaluations.saturating_add(1);
    }

    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Start a new fact/cost epoch without deleting task identities.  Old
    /// evaluations remain inspectable, while a subsequent request is forced
    /// through the normal leader path and cannot consume an old outcome as a
    /// fresh proof.
    pub fn invalidate_all(&mut self) -> Result<usize> {
        let tasks = self
            .tasks
            .iter()
            .filter(|task| task.state != TaskState::Invalidated)
            .map(|task| task.id)
            .collect::<Vec<_>>();
        for task in tasks.iter().copied() {
            self.invalidate(task)?;
        }
        Ok(tasks.len())
    }

    /// Invalidate only tasks whose result is owned by the physical costing
    /// epoch. Logical discovery/transform tasks keep their exact cursor and
    /// observations across a cost-frontier reset; a later ReadSet change will
    /// wake them through the normal current-read request path.
    pub fn invalidate_physical_tasks(&mut self) -> Result<usize> {
        let tasks = self
            .tasks
            .iter()
            .filter(|task| {
                task.state != TaskState::Invalidated
                    && self.intent(task.intent).is_some_and(|intent| {
                        matches!(
                            intent,
                            TaskIntent::Implement { .. }
                                | TaskIntent::Optimize { .. }
                                | TaskIntent::Cost { .. }
                        )
                    })
            })
            .map(|task| task.id)
            .collect::<Vec<_>>();
        for task in tasks.iter().copied() {
            self.invalidate(task)?;
        }
        Ok(tasks.len())
    }

    pub fn task(&self, id: TaskId) -> Option<&TaskRecord> {
        self.tasks.get(id.index())
    }

    pub fn state(&self, id: TaskId) -> Option<TaskState> {
        self.task(id).map(|task| task.state)
    }

    pub fn canonical_group(&self, mut group: GroupId) -> GroupId {
        while let Some(parent) = self.group_redirects.get(&group).copied() {
            if parent == group {
                break;
            }
            group = parent;
        }
        group
    }

    pub fn group_revision(&self, group: GroupId) -> u64 {
        self.group_revisions
            .get(&self.canonical_group(group))
            .copied()
            .unwrap_or_default()
    }

    /// Publish a Memo group redirect into the same task protocol.  Existing
    /// evaluations that read either side are invalidated before a caller can
    /// request work against the canonical group, so stale proofs cannot be
    /// silently reused after merge/reinsert.
    pub fn validate_group_redirect(&self, from: GroupId, to: GroupId) -> Result<()> {
        let from = self.canonical_group(from);
        let to = self.canonical_group(to);
        if from == to {
            return Ok(());
        }

        // Redirects are expected to point at a canonical root.  Keep this
        // check explicit: callers performing a Memo merge can preflight the
        // task side before changing Memo's union-find, and a future reinsert
        // path cannot accidentally create a redirect cycle.
        let mut current = to;
        let mut visited = BTreeSet::new();
        while let Some(parent) = self.group_redirects.get(&current).copied() {
            if !visited.insert(current) {
                return Err(paro_error::internal(
                    "task group redirect table already contains a cycle",
                ));
            }
            if parent == from {
                return Err(paro_error::internal(
                    "task group redirect would create a cycle",
                ));
            }
            current = parent;
        }
        Ok(())
    }

    pub fn redirect_group(&mut self, from: GroupId, to: GroupId) -> Result<Vec<TaskWakeup>> {
        let from = self.canonical_group(from);
        let to = self.canonical_group(to);
        if from == to {
            return Ok(Vec::new());
        }
        self.validate_group_redirect(from, to)?;
        let next_revision = self
            .group_revisions
            .get(&to)
            .copied()
            .unwrap_or_default()
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("task group revision overflow"))?;
        self.group_redirects.insert(from, to);
        self.group_revisions.insert(to, next_revision);
        let affected = self
            .tasks
            .iter()
            .filter(|task| {
                let intent_group = self
                    .intent(task.intent)
                    .and_then(|intent| match intent {
                        TaskIntent::Optimize { group, .. } => Some(*group),
                        TaskIntent::Transform { binding, .. } => Some(binding.root_group()),
                        _ => None,
                    })
                    .is_some_and(|group| {
                        group == from || group == to || self.canonical_group(group) == to
                    });
                let read_group = self.read_set(task.read_set).is_some_and(|reads| {
                    reads.reads().iter().any(|read| {
                        read.group == from
                            || read.group == to
                            || self.canonical_group(read.group) == to
                    })
                });
                intent_group || read_group
            })
            .map(|task| task.id)
            .collect::<Vec<_>>();
        let mut wakeups = Vec::new();
        for task in affected {
            wakeups.extend(self.invalidate(task)?);
        }
        Ok(wakeups)
    }

    pub fn evaluation_key(&self, id: TaskId) -> Option<EvaluationKey> {
        self.task(id).map(|task| task.evaluation)
    }

    pub fn task_read_set(&self, id: TaskId) -> Option<ReadSetId> {
        self.task(id).map(|task| task.read_set)
    }

    pub fn task_predecessor(&self, id: TaskId) -> Option<TaskId> {
        self.task(id).and_then(|task| task.predecessor)
    }

    /// Refresh the exact input revision of a running task after it has
    /// recursively materialized its declared dependencies.  A physical
    /// parent normally captures child frontiers before entering the child
    /// task; those frontiers are expected to advance during the first
    /// construction, so treating the provisional snapshot as a failed
    /// publication would recursively restart the same search.  Rebinding the
    /// task to the post-child ReadSet keeps selective invalidation exact while
    /// making the publication point observe the work it actually consumed.
    pub fn replace_current_read_set(
        &mut self,
        task: TaskId,
        memo: &Memo,
        reads: ReadSet,
    ) -> Result<()> {
        if !matches!(self.state(task), Some(TaskState::Running)) {
            return Err(paro_error::internal(
                "only a running task may replace its read set",
            ));
        }
        let reads = canonicalize_read_set(memo, reads);
        if !reads.is_current(memo)? {
            return Err(paro_error::internal(
                "task read set replacement is already obsolete",
            ));
        }
        let (intent, old_evaluation) = {
            let record = self
                .task(task)
                .ok_or_else(|| paro_error::internal("unknown task read-set replacement"))?;
            (record.intent, record.evaluation)
        };
        let read_set = self.intern_read_set(reads);
        let inputs = self.intern_input_revision(read_set)?;
        let evaluation = EvaluationKey { intent, inputs };
        if let Some(existing) = self.evaluations.get(&evaluation).copied() {
            if existing != task {
                return Err(paro_error::internal(
                    "task read-set replacement collides with another evaluation",
                ));
            }
        }
        if self.evaluations.get(&old_evaluation).copied() == Some(task) {
            self.evaluations.remove(&old_evaluation);
        }
        self.evaluations.insert(evaluation, task);
        let record = self.task_mut(task)?;
        record.evaluation = evaluation;
        record.read_set = read_set;
        Ok(())
    }

    pub fn outcome(&self, id: TaskId) -> Option<&TaskOutcome> {
        self.task(id).and_then(|task| task.outcome.as_ref())
    }

    fn task_mut(&mut self, id: TaskId) -> Result<&mut TaskRecord> {
        self.tasks
            .get_mut(id.index())
            .ok_or_else(|| paro_error::internal("unknown planner task"))
    }

    fn intern_cursor(&mut self, cursor: Cursor) -> CursorId {
        if let Some(id) = self.cursor_index.get(&cursor).copied() {
            return id;
        }
        let id = CursorId::new(self.cursors.len());
        self.cursors.push(cursor);
        self.cursor_index.insert(cursor, id);
        id
    }

    pub fn cursor(&self, id: CursorId) -> Option<Cursor> {
        self.cursors.get(id.index()).copied()
    }

    pub fn start(&mut self, task: TaskId) -> Result<()> {
        let kind = self
            .task(task)
            .and_then(|record| self.intent(record.intent))
            .ok_or_else(|| paro_error::internal("task lost its intent"))?
            .kind();
        let record = self.task_mut(task)?;
        if record.state != TaskState::Runnable {
            return Err(paro_error::internal(format!(
                "task {:?} cannot start from {:?}",
                task, record.state
            )));
        }
        record.state = TaskState::Running;
        self.profile.started = self.profile.started.saturating_add(1);
        let profile = self.kind_profile_mut(kind);
        profile.started = profile.started.saturating_add(1);
        Ok(())
    }

    /// Reopen an exact physical evaluation whose cursor was deliberately kept
    /// incomplete by an incremental readiness pass. Recursive readiness calls
    /// continue to reuse that prefix; only the explicit full optional pass may
    /// consume the continuation.
    pub fn resume_incomplete(&mut self, task: TaskId) -> Result<bool> {
        let kind = self
            .task(task)
            .and_then(|record| self.intent(record.intent))
            .ok_or_else(|| paro_error::internal("task lost its intent"))?
            .kind();
        let incomplete = self
            .task(task)
            .is_some_and(|record| record.state == TaskState::Completed)
            && self
                .task(task)
                .and_then(|record| self.cursor(record.cursor))
                .is_some_and(|cursor| !cursor.complete);
        if !incomplete {
            return Ok(false);
        }
        let record = self.task_mut(task)?;
        record.state = TaskState::Runnable;
        record.outcome = None;
        self.record_reopened_evaluation(kind);
        Ok(true)
    }

    pub fn advance_cursor(&mut self, task: TaskId, cursor: Cursor) -> Result<CursorId> {
        let cursor_id = self.intern_cursor(cursor);
        self.task_mut(task)?.cursor = cursor_id;
        Ok(cursor_id)
    }

    pub fn await_dependencies(
        &mut self,
        task: TaskId,
        dependencies: impl IntoIterator<Item = TaskId>,
    ) -> Result<bool> {
        let kind = self
            .task(task)
            .and_then(|record| self.intent(record.intent))
            .ok_or_else(|| paro_error::internal("task lost its intent"))?
            .kind();
        let dependencies = dependencies
            .into_iter()
            .filter(|dependency| *dependency != task)
            .collect::<BTreeSet<_>>();
        for dependency in &dependencies {
            if self.task(*dependency).is_none() {
                return Err(paro_error::internal("task waits on an unknown dependency"));
            }
        }
        let unresolved = dependencies
            .iter()
            .copied()
            .filter(|dependency| {
                !matches!(
                    self.state(*dependency),
                    Some(TaskState::Completed | TaskState::Failed)
                )
            })
            .collect::<BTreeSet<_>>();
        {
            let record = self.task_mut(task)?;
            if record.state != TaskState::Running {
                return Err(paro_error::internal(
                    "only a running task may await dependencies",
                ));
            }
            record.dependencies = unresolved.clone();
            record.state = if unresolved.is_empty() {
                TaskState::Runnable
            } else {
                TaskState::Awaiting
            };
        }
        if !unresolved.is_empty() {
            self.profile.awaiting = self.profile.awaiting.saturating_add(1);
            let profile = self.kind_profile_mut(kind);
            profile.awaiting = profile.awaiting.saturating_add(1);
        }
        for dependency in unresolved {
            self.dependents.entry(dependency).or_default().insert(task);
        }
        Ok(self.state(task) == Some(TaskState::Runnable))
    }

    fn add_waiter(&mut self, task: TaskId) -> Result<WaiterId> {
        let id = WaiterId::new(self.next_waiter as usize);
        self.next_waiter = self
            .next_waiter
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("planner waiter identity exhausted"))?;
        let waiter = Waiter { id, task };
        self.waiters.insert(id, waiter);
        self.task_mut(task)?.waiters.insert(id);
        Ok(id)
    }

    fn wake_dependents(&mut self, dependency: TaskId) -> Vec<TaskWakeup> {
        let dependents = self.dependents.remove(&dependency).unwrap_or_default();
        let mut wakeups = Vec::new();
        for dependent in dependents {
            if let Some(record) = self.tasks.get_mut(dependent.index()) {
                record.dependencies.remove(&dependency);
                if record.dependencies.is_empty() && record.state == TaskState::Awaiting {
                    record.state = TaskState::Runnable;
                    wakeups.push(TaskWakeup {
                        task: dependent,
                        waiter: None,
                        outcome: self.task(dependency).and_then(|task| task.outcome.clone()),
                    });
                }
            }
        }
        wakeups
    }

    fn take_waiters(&mut self, task: TaskId, outcome: Option<TaskOutcome>) -> Vec<TaskWakeup> {
        let waiter_ids = self
            .task(task)
            .map(|record| record.waiters.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut wakeups = Vec::with_capacity(waiter_ids.len());
        for waiter_id in waiter_ids {
            if let Some(waiter) = self.waiters.remove(&waiter_id) {
                if let Some(record) = self.tasks.get_mut(task.index()) {
                    record.waiters.remove(&waiter_id);
                }
                wakeups.push(TaskWakeup {
                    task: waiter.task,
                    waiter: Some(waiter.id),
                    outcome: outcome.clone(),
                });
            }
        }
        wakeups
    }

    fn finish(
        &mut self,
        task: TaskId,
        state: TaskState,
        outcome: Option<TaskOutcome>,
    ) -> Result<Vec<TaskWakeup>> {
        let kind = self
            .task(task)
            .and_then(|record| self.intent(record.intent))
            .ok_or_else(|| paro_error::internal("task lost its intent"))?
            .kind();
        let obligations = self
            .task(task)
            .ok_or_else(|| paro_error::internal("unknown task completion"))?
            .obligations
            .len();
        if state == TaskState::Completed && obligations != 0 {
            return Err(paro_error::internal(
                "task cannot complete while completion obligations remain",
            ));
        }
        let old_dependencies = std::mem::take(&mut self.task_mut(task)?.dependencies);
        for dependency in old_dependencies {
            if let Some(dependents) = self.dependents.get_mut(&dependency) {
                dependents.remove(&task);
                if dependents.is_empty() {
                    self.dependents.remove(&dependency);
                }
            }
        }
        {
            let record = self.task_mut(task)?;
            record.state = state;
            record.outcome = outcome.clone();
        }
        match state {
            TaskState::Completed => {
                self.profile.completed = self.profile.completed.saturating_add(1);
                let profile = self.kind_profile_mut(kind);
                profile.completed = profile.completed.saturating_add(1);
            }
            TaskState::Suspended => {
                self.profile.suspended = self.profile.suspended.saturating_add(1);
                let profile = self.kind_profile_mut(kind);
                profile.suspended = profile.suspended.saturating_add(1);
            }
            TaskState::Invalidated => {
                self.profile.invalidated = self.profile.invalidated.saturating_add(1);
                let profile = self.kind_profile_mut(kind);
                profile.invalidated = profile.invalidated.saturating_add(1);
            }
            _ => {}
        }
        let mut wakeups = self.take_waiters(task, outcome.clone());
        wakeups.extend(self.wake_dependents(task));
        Ok(wakeups)
    }

    pub fn complete(&mut self, task: TaskId, outcome: TaskOutcome) -> Result<Vec<TaskWakeup>> {
        let state = self
            .state(task)
            .ok_or_else(|| paro_error::internal("unknown task completion"))?;
        if !matches!(state, TaskState::Runnable | TaskState::Running) {
            return Err(paro_error::internal("task is not runnable for completion"));
        }
        if let TaskOutcome::Progress { cursor } | TaskOutcome::NoCandidate { cursor } = &outcome {
            if self
                .task(task)
                .is_none_or(|record| record.cursor != *cursor)
            {
                return Err(paro_error::internal(
                    "task completion references a foreign cursor",
                ));
            }
        }
        if self.segments.contains_key(&task) {
            self.commit_segment(task)?;
        }
        self.finish(task, TaskState::Completed, Some(outcome))
    }

    pub fn complete_current(
        &mut self,
        task: TaskId,
        memo: &Memo,
        outcome: TaskOutcome,
    ) -> Result<Vec<TaskWakeup>> {
        let reads = self
            .task(task)
            .ok_or_else(|| paro_error::internal("unknown task publication"))?
            .read_set;
        if !self
            .read_set(reads)
            .ok_or_else(|| paro_error::internal("task publication lost its read set"))?
            .is_current(memo)?
        {
            // A task can finish with no local mutation after another task
            // has advanced one of its observations. That is ordinary
            // single-flight invalidation, not a query error: discard the
            // stale completion and let request_current reopen the same
            // semantic task against the new read set.
            return self.invalidate(task);
        }
        self.complete(task, outcome)
    }

    /// Publish a task's local objects and either complete it or register its
    /// unresolved dependencies in one linearizable single-worker operation.
    /// The read-set check happens before publication; a stale task rolls back
    /// only its own segment and cannot notify consumers with obsolete data.
    pub fn publish_current(
        &mut self,
        task: TaskId,
        memo: &Memo,
        dependencies: impl IntoIterator<Item = TaskId>,
        outcome: TaskOutcome,
    ) -> Result<Vec<TaskWakeup>> {
        self.publish_current_with_local_mutation(
            task,
            memo,
            std::iter::empty(),
            dependencies,
            outcome,
        )
    }

    /// Publish after the task has committed a local Memo mutation. The
    /// caller must name exactly the groups it wrote while owning this task;
    /// reads of every other group are still revalidated. This preserves the
    /// publication protocol for the common transformation shape where the
    /// output is inserted into the same group whose frontier was read, while
    /// retaining a fail-closed check for unrelated concurrent changes.
    pub fn publish_current_after_local_mutation(
        &mut self,
        task: TaskId,
        memo: &Memo,
        locally_written_groups: impl IntoIterator<Item = GroupId>,
        dependencies: impl IntoIterator<Item = TaskId>,
        outcome: TaskOutcome,
    ) -> Result<Vec<TaskWakeup>> {
        self.publish_current_with_local_mutation(
            task,
            memo,
            locally_written_groups,
            dependencies,
            outcome,
        )
    }

    fn publish_current_with_local_mutation(
        &mut self,
        task: TaskId,
        memo: &Memo,
        locally_written_groups: impl IntoIterator<Item = GroupId>,
        dependencies: impl IntoIterator<Item = TaskId>,
        outcome: TaskOutcome,
    ) -> Result<Vec<TaskWakeup>> {
        let dependencies = dependencies.into_iter().collect::<Vec<_>>();
        let locally_written_groups = locally_written_groups
            .into_iter()
            .map(|group| self.canonical_group(group))
            .collect::<BTreeSet<_>>();
        let reads = self
            .task(task)
            .ok_or_else(|| paro_error::internal("unknown task publication"))?
            .read_set;
        let current = self
            .read_set(reads)
            .ok_or_else(|| paro_error::internal("task publication lost its read set"))?
            .is_current_except(memo, &locally_written_groups)?;
        let intent = self
            .task(task)
            .and_then(|record| self.intent(record.intent))
            .ok_or_else(|| paro_error::internal("task publication lost its intent"))?;
        let canonical = match intent {
            TaskIntent::Optimize { group, .. } => self.canonical_group(*group) == *group,
            _ => true,
        };
        if !current || !canonical {
            self.rollback_segment(task)?;
            self.invalidate(task)?;
            let intent = self.intent(
                self.task(task)
                    .ok_or_else(|| paro_error::internal("task lost its intent"))?
                    .intent,
            );
            let stale = self.read_set(reads).and_then(|read_set| {
                read_set
                    .reads()
                    .iter()
                    .find(|read| {
                        !locally_written_groups.contains(&read.group)
                            && !read.is_current(memo).unwrap_or(false)
                    })
                    .copied()
            });
            let current =
                stale.and_then(|read| PatternRead::facts_from_group(memo, read.group).ok());
            return Err(paro_error::internal(
                format!(
                    "task publication rejected an obsolete read or group identity: task={task:?}, intent={intent:?}, written={locally_written_groups:?}, stale_read={stale:?}, current_read={current:?}, canonical={canonical}"
                ),
            ));
        }

        if self.segments.contains_key(&task) {
            self.commit_segment(task)?;
        }
        let ready = self.register_awaiting_after_publish(task, dependencies.clone())?;
        if ready {
            self.complete(task, outcome)
        } else {
            let cursor = self
                .task(task)
                .ok_or_else(|| paro_error::internal("task publication lost its cursor"))?
                .cursor;
            self.task_mut(task)?.outcome = Some(TaskOutcome::Awaiting {
                cursor,
                dependencies: dependencies.into_boxed_slice(),
            });
            Ok(Vec::new())
        }
    }

    pub fn suspend(&mut self, task: TaskId, outcome: TaskOutcome) -> Result<Vec<TaskWakeup>> {
        if !matches!(
            self.state(task),
            Some(TaskState::Runnable | TaskState::Running)
        ) {
            return Err(paro_error::internal("task is not runnable for suspension"));
        }
        self.rollback_segment(task)?;
        self.finish(task, TaskState::Suspended, Some(outcome))
    }

    pub fn fail(&mut self, task: TaskId, detail: impl Into<String>) -> Result<Vec<TaskWakeup>> {
        self.rollback_segment(task)?;
        self.finish(
            task,
            TaskState::Failed,
            Some(TaskOutcome::Failed {
                detail: detail.into(),
            }),
        )
    }

    pub fn invalidate(&mut self, task: TaskId) -> Result<Vec<TaskWakeup>> {
        if self.task(task).is_none() {
            return Err(paro_error::internal("unknown task invalidation"));
        }
        self.rollback_segment(task)?;
        let cursor = self
            .task(task)
            .map(|record| record.cursor)
            .unwrap_or(CursorId::INVALID);
        self.finish(
            task,
            TaskState::Invalidated,
            Some(TaskOutcome::Invalidated { cursor }),
        )
    }

    pub fn add_completion_obligation(
        &mut self,
        task: TaskId,
        detail: impl Into<String>,
    ) -> Result<CompletionObligationId> {
        let id = CompletionObligationId::new(self.next_obligation as usize);
        self.next_obligation = self
            .next_obligation
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("completion obligation identity exhausted"))?;
        self.obligations.insert(
            id,
            CompletionObligation {
                id,
                detail: detail.into(),
            },
        );
        self.task_mut(task)?.obligations.insert(id);
        Ok(id)
    }

    pub fn discharge_obligation(
        &mut self,
        task: TaskId,
        obligation: CompletionObligationId,
    ) -> Result<()> {
        if !self.task_mut(task)?.obligations.remove(&obligation) {
            return Err(paro_error::internal(
                "task does not own completion obligation",
            ));
        }
        self.obligations.remove(&obligation);
        Ok(())
    }

    pub fn has_completion_obligations(&self, task: TaskId) -> bool {
        self.task(task)
            .is_some_and(|record| !record.obligations.is_empty())
    }

    pub fn register_awaiting_after_publish(
        &mut self,
        task: TaskId,
        dependencies: impl IntoIterator<Item = TaskId>,
    ) -> Result<bool> {
        // The registry owns both sides of this operation.  A dependency which
        // completed just before registration is observed here and therefore
        // cannot leave the caller permanently asleep.
        self.await_dependencies(task, dependencies)
    }

    pub fn reserve_once(
        &mut self,
        task: TaskId,
        slot: u64,
        units: u64,
    ) -> Result<Option<ReservationId>> {
        self.profile.reservation_attempts = self.profile.reservation_attempts.saturating_add(1);
        if self.task(task).is_none() {
            return Err(paro_error::internal("reservation belongs to unknown task"));
        }
        if units == 0 {
            return Ok(None);
        }
        if let Some(reservation) = self.reservation_slots.get(&(task, slot)).copied() {
            self.profile.reservation_reuses = self.profile.reservation_reuses.saturating_add(1);
            return Ok(Some(reservation));
        }
        let used = self.committed_units.saturating_add(self.reserved_units);
        if self
            .reservation_limit
            .is_some_and(|limit| units > limit.saturating_sub(used))
        {
            self.profile.reservation_rejections =
                self.profile.reservation_rejections.saturating_add(1);
            return Ok(None);
        }
        let reservation = ReservationId::new(self.next_reservation as usize);
        self.next_reservation = self
            .next_reservation
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("reservation identity exhausted"))?;
        self.reservations.insert(
            reservation,
            ReservationRecord {
                task,
                units,
                committed: false,
            },
        );
        self.reservation_slots.insert((task, slot), reservation);
        self.reserved_units = self.reserved_units.saturating_add(units);
        self.segments
            .entry(task)
            .or_default()
            .reservations
            .insert(reservation, units);
        self.segments
            .get_mut(&task)
            .expect("task segment was just inserted")
            .journal
            .push(JournalEntry::Reserved { reservation, units });
        Ok(Some(reservation))
    }

    pub fn allocate_object(&mut self, task: TaskId) -> Result<TaskObjectId> {
        if self.task(task).is_none() {
            return Err(paro_error::internal("allocation belongs to unknown task"));
        }
        let object = TaskObjectId::new(self.next_object as usize);
        self.next_object = self
            .next_object
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("task object identity exhausted"))?;
        let segment = self.segments.entry(task).or_default();
        segment.uncommitted_objects.insert(object);
        segment.journal.push(JournalEntry::Allocated { object });
        Ok(object)
    }

    pub fn commit_segment(&mut self, task: TaskId) -> Result<()> {
        let mut segment = self
            .segments
            .remove(&task)
            .ok_or_else(|| paro_error::internal("task has no allocation segment"))?;
        for object in std::mem::take(&mut segment.uncommitted_objects) {
            self.published_objects.insert(object, task);
            segment.journal.push(JournalEntry::Published { object });
        }
        for (reservation, units) in std::mem::take(&mut segment.reservations) {
            let record = self
                .reservations
                .get_mut(&reservation)
                .ok_or_else(|| paro_error::internal("task reservation journal is corrupt"))?;
            if record.task != task || record.units != units {
                return Err(paro_error::internal(
                    "task reservation journal changed its owner or units",
                ));
            }
            record.committed = true;
            self.reserved_units = self.reserved_units.saturating_sub(units);
            self.committed_units = self.committed_units.saturating_add(units);
        }
        Ok(())
    }

    pub fn rollback_segment(&mut self, task: TaskId) -> Result<()> {
        let Some(segment) = self.segments.remove(&task) else {
            return Ok(());
        };
        let object_count = segment.uncommitted_objects.len();
        let mut last_reservation = ReservationId::INVALID;
        for (reservation, units) in segment.reservations {
            last_reservation = reservation;
            self.reservations.remove(&reservation);
            self.reservation_slots
                .retain(|_, value| *value != reservation);
            self.reserved_units = self.reserved_units.saturating_sub(units);
        }
        // Object IDs are never reused.  Only this task's unpublished object
        // set disappears; another task's segment is untouched.
        let mut journal = segment.journal;
        journal.push(JournalEntry::RolledBack {
            reservation: last_reservation,
            object_count,
        });
        Ok(())
    }

    pub fn is_published(&self, object: TaskObjectId) -> bool {
        self.published_objects.contains_key(&object)
    }

    pub fn published_owner(&self, object: TaskObjectId) -> Option<TaskId> {
        self.published_objects.get(&object).copied()
    }

    pub fn committed_units(&self) -> u64 {
        self.committed_units
    }

    pub fn reserved_units(&self) -> u64 {
        self.reserved_units
    }

    pub fn record_lower_bound(
        &mut self,
        task: TaskId,
        context: BoundContext,
        value: u64,
    ) -> Result<BoundProofId> {
        self.record_bound(task, context, BoundProofKind::Lower { value })
    }

    pub fn record_verified_upper(
        &mut self,
        task: TaskId,
        context: BoundContext,
        value: u64,
        candidate: CandidateId,
    ) -> Result<BoundProofId> {
        self.record_bound(task, context, BoundProofKind::Upper { value, candidate })
    }

    pub fn record_no_plan_below(
        &mut self,
        task: TaskId,
        context: BoundContext,
        threshold: u64,
    ) -> Result<BoundProofId> {
        self.record_bound(task, context, BoundProofKind::NoPlanBelow { threshold })
    }

    fn record_bound(
        &mut self,
        task: TaskId,
        context: BoundContext,
        kind: BoundProofKind,
    ) -> Result<BoundProofId> {
        let task_record = self
            .task(task)
            .ok_or_else(|| paro_error::internal("bound proof references an unknown task"))?;
        if self.read_set(context.reads).is_none() {
            return Err(paro_error::internal(
                "bound proof references an unknown read set",
            ));
        }
        if !matches!(
            task_record.state,
            TaskState::Runnable | TaskState::Running | TaskState::Awaiting
        ) {
            return Err(paro_error::internal(
                "bound proof must be recorded by an active task",
            ));
        }
        let intent = self
            .intent(task_record.intent)
            .ok_or_else(|| paro_error::internal("bound proof task lost its intent"))?;
        let context_matches = match intent {
            TaskIntent::Optimize { group, goal } => {
                self.canonical_group(*group) == self.canonical_group(context.group)
                    && *goal == context.goal
            }
            _ => false,
        };
        if !context_matches {
            return Err(paro_error::internal(
                "bound proof context does not match its Optimize task",
            ));
        }
        let id = BoundProofId::new(self.bounds.len());
        self.bounds.push(BoundProof {
            id,
            task,
            context,
            kind,
        });
        self.profile.bound_proofs = self.profile.bound_proofs.saturating_add(1);
        Ok(id)
    }

    pub fn bound(&self, id: BoundProofId) -> Option<&BoundProof> {
        self.bounds.get(id.index())
    }

    pub fn bound_is_current(&self, id: BoundProofId, memo: &Memo) -> Result<bool> {
        self.bound(id)
            .ok_or_else(|| paro_error::internal("unknown bound proof"))?
            .is_current(self, memo)
    }

    pub fn tasks_with_current_reads(&self, memo: &Memo) -> Result<usize> {
        Ok(self
            .tasks
            .iter()
            .filter(|task| {
                self.read_set(task.read_set)
                    .is_some_and(|reads| reads.is_current(memo).unwrap_or(false))
            })
            .count())
    }
}

fn canonicalize_task_intent(memo: &Memo, intent: TaskIntent) -> TaskIntent {
    match intent {
        TaskIntent::Optimize { group, goal } => TaskIntent::Optimize {
            group: memo.canonical_group(group),
            goal,
        },
        TaskIntent::Transform {
            expression,
            rule,
            binding,
        } => TaskIntent::Transform {
            expression,
            rule,
            binding: PatternBinding {
                root: canonicalize_pattern_operand(memo, binding.root),
                fingerprint: binding.fingerprint,
            },
        },
        other => other,
    }
}

fn canonicalize_pattern_operand(memo: &Memo, operand: PatternOperand) -> PatternOperand {
    match operand {
        PatternOperand::Expression {
            group,
            expression,
            children,
        } => PatternOperand::Expression {
            group: memo.canonical_group(group),
            expression,
            children: children
                .into_vec()
                .into_iter()
                .map(|child| canonicalize_pattern_operand(memo, child))
                .collect(),
        },
        PatternOperand::Group(group) => PatternOperand::Group(memo.canonical_group(group)),
    }
}

fn canonicalize_read_set(memo: &Memo, reads: ReadSet) -> ReadSet {
    ReadSet::new(reads.reads.into_vec().into_iter().map(|mut read| {
        read.group = memo.canonical_group(read.group);
        read
    }))
}

#[cfg(test)]
mod tests {
    use crate::cascades::rules::ReadScope;
    use super::*;

    #[test]
    fn failed_task_keeps_its_cause_instead_of_inventing_resource_exhaustion() {
        let mut registry = TaskRegistry::default();
        let intent = TaskIntent::Discover {
            expression: LogicalExprId(0),
            rule: RuleId(1),
        };
        let TaskRequest::Leader(task) = registry.request(intent.clone(), ReadSet::empty()).unwrap()
        else {
            panic!("new task must lead")
        };
        registry.start(task).unwrap();
        registry
            .fail(task, "native contract failure: missing column")
            .unwrap();
        assert_eq!(registry.state(task), Some(TaskState::Failed));
        assert_eq!(
            registry.request(intent, ReadSet::empty()).unwrap(),
            TaskRequest::Reused {
                task,
                outcome: Some(TaskOutcome::Failed {
                    detail: "native contract failure: missing column".into(),
                }),
            }
        );
    }
    use crate::cascades::column::GroupSchema;
    use crate::cascades::ids::{LogicalPayloadId, PropertySetId};
    use crate::cascades::memo::{
        EquivalenceProof, GrantGoalKey, GroupCardinality, LogicalExprKey, LogicalProperties,
        RowGoal,
    };
    use crate::cascades::rules::PatternOperand;
    use crate::physical::ObjectiveProfile;

    fn goal() -> OptimizationGoal {
        OptimizationGoal {
            required: PropertySetId::new(0),
            row_goal: RowGoal::All,
            objective: ObjectiveProfile::Latency,
            grant: GrantGoalKey::Invariant(Default::default()),
            context: Default::default(),
        }
    }

    fn registry_with_task() -> (TaskRegistry, TaskId) {
        let mut registry = TaskRegistry::default();
        let task = match registry
            .request(
                TaskIntent::Optimize {
                    group: GroupId::new(0),
                    goal: goal(),
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            request => panic!("unexpected request: {request:?}"),
        };
        (registry, task)
    }

    #[test]
    fn exact_intents_single_flight_without_hash_identity() {
        let mut registry = TaskRegistry::default();
        let intent = TaskIntent::Discover {
            expression: LogicalExprId::new(1),
            rule: RuleId::new(2),
        };
        let first = registry.request(intent.clone(), ReadSet::empty()).unwrap();
        let second = registry.request(intent, ReadSet::empty()).unwrap();
        let TaskRequest::Leader(first) = first else {
            panic!("first request did not lead")
        };
        assert!(matches!(
            second,
            TaskRequest::Subscriber { task, .. } if task == first
        ));
        assert_eq!(registry.task_count(), 1);
    }

    #[test]
    fn single_read_uses_the_same_canonical_shape_as_general_normalization() {
        let read = PatternRead {
            group: GroupId::new(7),
            scope: ReadScope::LOGICAL_FRONTIER.union(ReadScope::FACTS),
            logical_frontier_revision: Some(3),
            physical_goal: None,
            physical_implementation_revision: None,
            physical_frontier_revision: None,
            logical_fact_fingerprint: Fingerprint(11),
            statistics_snapshot_fingerprint: Fingerprint(13),
        };

        assert_eq!(ReadSet::single(read), ReadSet::new([read]));
    }

    #[test]
    fn normalization_does_not_hide_an_older_same_category_revision() {
        let first = PatternRead {
            group: GroupId::new(7),
            scope: ReadScope::LOGICAL_FRONTIER,
            logical_frontier_revision: Some(3),
            physical_goal: None,
            physical_implementation_revision: None,
            physical_frontier_revision: None,
            logical_fact_fingerprint: Fingerprint::default(),
            statistics_snapshot_fingerprint: Fingerprint::default(),
        };
        let second = PatternRead {
            logical_frontier_revision: Some(4),
            ..first
        };

        let normalized = ReadSet::new([first, second]);
        assert_eq!(normalized.reads(), &[first, second]);
        assert!(!first.can_union(second));
    }

    #[test]
    fn normalization_merges_disjoint_categories_for_one_group() {
        let structure = PatternRead {
            group: GroupId::new(7),
            scope: ReadScope::LOGICAL_FRONTIER,
            logical_frontier_revision: Some(3),
            physical_goal: None,
            physical_implementation_revision: None,
            physical_frontier_revision: None,
            logical_fact_fingerprint: Fingerprint::default(),
            statistics_snapshot_fingerprint: Fingerprint::default(),
        };
        let facts = PatternRead {
            group: structure.group,
            scope: ReadScope::FACTS,
            logical_frontier_revision: None,
            physical_goal: None,
            physical_implementation_revision: None,
            physical_frontier_revision: None,
            logical_fact_fingerprint: Fingerprint(11),
            statistics_snapshot_fingerprint: Fingerprint(13),
        };

        let normalized = ReadSet::new([structure, facts]);
        assert_eq!(normalized.reads(), &[structure.union(facts)]);
    }

    #[test]
    fn task_profile_attributes_requests_by_exact_semantic_kind() {
        let mut registry = TaskRegistry::default();
        let discover_intent = TaskIntent::Discover {
            expression: LogicalExprId::new(1),
            rule: RuleId::new(2),
        };
        let task = match registry
            .request(discover_intent.clone(), ReadSet::empty())
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        assert!(matches!(
            registry
                .request(discover_intent.clone(), ReadSet::empty())
                .unwrap(),
            TaskRequest::Subscriber { task: subscribed, .. } if subscribed == task
        ));
        registry.start(task).unwrap();
        registry
            .complete(
                task,
                TaskOutcome::NoChange {
                    reads: ReadSetId::new(0),
                },
            )
            .unwrap();
        assert!(matches!(
            registry.request(discover_intent, ReadSet::empty()).unwrap(),
            TaskRequest::Reused { task: reused, .. } if reused == task
        ));

        let transform = match registry
            .request(
                TaskIntent::Transform {
                    expression: LogicalExprId::new(3),
                    rule: RuleId::new(4),
                    binding: PatternBinding {
                        root: PatternOperand::Group(GroupId::new(5)),
                        fingerprint: Fingerprint(6),
                    },
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        registry.start(transform).unwrap();
        registry
            .complete(
                transform,
                TaskOutcome::NoChange {
                    reads: ReadSetId::new(0),
                },
            )
            .unwrap();

        let profile = registry.profile();
        let discover = profile.by_kind.get(&TaskKind::Discover).unwrap();
        assert_eq!(discover.requests, 3);
        assert_eq!(discover.unique_intents, 1);
        assert_eq!(discover.unique_evaluations, 1);
        assert_eq!(discover.single_flight_subscriptions, 1);
        assert_eq!(discover.reused_evaluations, 1);
        assert_eq!(discover.started, 1);
        assert_eq!(discover.completed, 1);
        let transform_profile = profile.by_kind.get(&TaskKind::Transform).unwrap();
        assert_eq!(transform_profile.requests, 1);
        assert_eq!(transform_profile.unique_intents, 1);
        assert_eq!(transform_profile.unique_evaluations, 1);
        assert_eq!(transform_profile.started, 1);
        assert_eq!(transform_profile.completed, 1);
    }

    #[test]
    fn completed_evaluation_is_reused_but_new_read_revision_is_not() {
        let mut registry = TaskRegistry::default();
        let intent = TaskIntent::Discover {
            expression: LogicalExprId::new(1),
            rule: RuleId::new(2),
        };
        let TaskRequest::Leader(task) = registry.request(intent.clone(), ReadSet::empty()).unwrap()
        else {
            panic!("request did not lead")
        };
        registry.start(task).unwrap();
        registry
            .complete(
                task,
                TaskOutcome::NoChange {
                    reads: ReadSetId::new(0),
                },
            )
            .unwrap();
        assert!(matches!(
            registry.request(intent.clone(), ReadSet::empty()).unwrap(),
            TaskRequest::Reused { task: reused, .. } if reused == task
        ));

        let read_set = registry.intern_read_set(ReadSet::new([]));
        assert_eq!(read_set, ReadSetId::new(0));
        let next = registry.request(intent, ReadSet::new([])).unwrap();
        assert!(matches!(next, TaskRequest::Reused { .. }));
    }

    #[test]
    fn current_request_reopens_a_completed_task_after_its_read_advances() {
        let mut memo = Memo::new(Default::default());
        let group = memo.create_group(
            GroupSchema::new([]).unwrap(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let read = PatternRead::from_group(&memo, group).unwrap();
        let mut registry = TaskRegistry::default();
        let intent = TaskIntent::Discover {
            expression: LogicalExprId::new(0),
            rule: RuleId::new(2),
        };
        let TaskRequest::Leader(task) = registry
            .request_current(intent.clone(), ReadSet::new([read]), &memo)
            .unwrap()
        else {
            panic!("initial request did not lead")
        };
        registry.start(task).unwrap();
        let read_id = registry.intern_read_set(ReadSet::new([read]));
        registry
            .complete(task, TaskOutcome::NoChange { reads: read_id })
            .unwrap();
        memo.insert_logical(
            group,
            LogicalExprKey {
                operator: Fingerprint(1),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(1),
            EquivalenceProof::Initial,
        )
        .unwrap();
        let next = registry
            .request_current(intent, ReadSet::new([read]), &memo)
            .unwrap();
        assert!(matches!(next, TaskRequest::Leader(reopened) if reopened == task));
        assert_eq!(registry.state(task), Some(TaskState::Runnable));
    }

    #[test]
    fn current_request_reuses_an_incomplete_prefix_until_its_read_advances() {
        let mut memo = Memo::new(Default::default());
        let group = memo.create_group(
            GroupSchema::new([]).unwrap(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let read = PatternRead::from_group(&memo, group).unwrap();
        let mut registry = TaskRegistry::default();
        let intent = TaskIntent::Optimize {
            group,
            goal: goal(),
        };
        let TaskRequest::Leader(task) = registry
            .request_current(intent.clone(), ReadSet::single(read), &memo)
            .unwrap()
        else {
            panic!("initial request did not lead")
        };
        registry.start(task).unwrap();
        let cursor = registry
            .advance_cursor(
                task,
                Cursor {
                    position: 7,
                    complete: false,
                },
            )
            .unwrap();
        registry
            .complete(task, TaskOutcome::Progress { cursor })
            .unwrap();

        assert!(matches!(
            registry
                .request_current(intent.clone(), ReadSet::single(read), &memo)
                .unwrap(),
            TaskRequest::Reused {
                task: reused,
                outcome: Some(TaskOutcome::Progress { .. })
            } if reused == task
        ));
        assert_eq!(registry.state(task), Some(TaskState::Completed));
        assert_eq!(registry.profile().reopened_evaluations, 0);

        memo.insert_logical(
            group,
            LogicalExprKey {
                operator: Fingerprint(2),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(2),
            EquivalenceProof::Initial,
        )
        .unwrap();
        let current_read = PatternRead::from_group(&memo, group).unwrap();
        let TaskRequest::Leader(next) = registry
            .request_current(intent, ReadSet::single(current_read), &memo)
            .unwrap()
        else {
            panic!("advanced read did not create a fresh physical evaluation")
        };
        assert_ne!(next, task);
    }

    #[test]
    fn stale_no_change_completion_is_invalidated_without_failing_query() {
        let mut memo = Memo::new(Default::default());
        let group = memo.create_group(
            GroupSchema::new([]).unwrap(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let read = PatternRead::from_group(&memo, group).unwrap();
        let mut registry = TaskRegistry::default();
        let intent = TaskIntent::Discover {
            expression: LogicalExprId::new(0),
            rule: RuleId::new(2),
        };
        let TaskRequest::Leader(task) = registry
            .request_current(intent.clone(), ReadSet::single(read), &memo)
            .unwrap()
        else {
            panic!("initial request did not lead")
        };
        registry.start(task).unwrap();
        let read_id = registry.intern_read_set(ReadSet::single(read));
        memo.insert_logical(
            group,
            LogicalExprKey {
                operator: Fingerprint(1),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(1),
            EquivalenceProof::Initial,
        )
        .unwrap();

        registry
            .complete_current(task, &memo, TaskOutcome::NoChange { reads: read_id })
            .unwrap();
        assert_eq!(registry.state(task), Some(TaskState::Invalidated));
        let current_read = PatternRead::from_group(&memo, group).unwrap();
        let TaskRequest::Leader(reopened) = registry
            .request_current(intent, ReadSet::single(current_read), &memo)
            .unwrap()
        else {
            panic!("stale task was not replaced by a current-read leader")
        };
        assert_ne!(reopened, task);
    }

    #[test]
    fn physical_epoch_invalidation_preserves_logical_tasks() {
        let mut registry = TaskRegistry::default();
        let physical = match registry
            .request(
                TaskIntent::Optimize {
                    group: GroupId::new(0),
                    goal: goal(),
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        let logical = match registry
            .request(
                TaskIntent::Discover {
                    expression: LogicalExprId::new(1),
                    rule: RuleId::new(2),
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        registry.start(physical).unwrap();
        registry.start(logical).unwrap();
        registry
            .complete(
                physical,
                TaskOutcome::NoCandidate {
                    cursor: registry.task(physical).unwrap().cursor,
                },
            )
            .unwrap();
        registry
            .complete(
                logical,
                TaskOutcome::NoChange {
                    reads: ReadSetId::new(0),
                },
            )
            .unwrap();
        assert_eq!(registry.invalidate_physical_tasks().unwrap(), 1);
        assert_eq!(registry.state(physical), Some(TaskState::Invalidated));
        assert_eq!(registry.state(logical), Some(TaskState::Completed));
    }

    #[test]
    fn incomplete_progress_reopens_the_same_task_for_continuation() {
        let mut registry = TaskRegistry::default();
        let intent = TaskIntent::Discover {
            expression: LogicalExprId::new(1),
            rule: RuleId::new(2),
        };
        let TaskRequest::Leader(task) = registry.request(intent.clone(), ReadSet::empty()).unwrap()
        else {
            panic!("request did not lead")
        };
        registry.start(task).unwrap();
        let cursor = registry
            .advance_cursor(
                task,
                Cursor {
                    position: 3,
                    complete: false,
                },
            )
            .unwrap();
        registry
            .complete(task, TaskOutcome::Progress { cursor })
            .unwrap();

        let continuation = registry.request(intent, ReadSet::empty()).unwrap();
        assert!(matches!(continuation, TaskRequest::Leader(next) if next == task));
        assert_eq!(registry.state(task), Some(TaskState::Runnable));
        assert_eq!(
            registry.cursor(cursor),
            Some(Cursor {
                position: 3,
                complete: false
            })
        );
        let profile = registry.profile();
        assert_eq!(profile.reopened_evaluations, 1);
        assert_eq!(
            profile
                .by_kind
                .get(&TaskKind::Discover)
                .unwrap()
                .reopened_evaluations,
            1
        );
    }

    #[test]
    fn changed_read_evaluation_keeps_an_append_only_predecessor_cursor() {
        let mut registry = TaskRegistry::default();
        let intent = TaskIntent::Optimize {
            group: GroupId::new(1),
            goal: goal(),
        };
        let first = match registry.request(intent.clone(), ReadSet::empty()).unwrap() {
            TaskRequest::Leader(task) => task,
            request => panic!("unexpected first request: {request:?}"),
        };
        registry.start(first).unwrap();
        let cursor = registry
            .advance_cursor(
                first,
                Cursor {
                    position: 17,
                    complete: false,
                },
            )
            .unwrap();
        registry
            .complete(first, TaskOutcome::Progress { cursor })
            .unwrap();

        let second = match registry
            .request(
                intent,
                ReadSet::new([PatternRead {
                    group: GroupId::new(1),
                    scope: ReadScope::LOGICAL_FRONTIER.union(ReadScope::FACTS),
                    logical_frontier_revision: Some(2),
                    physical_goal: None,
                    physical_implementation_revision: None,
                    physical_frontier_revision: None,
                    logical_fact_fingerprint: Fingerprint(3),
                    statistics_snapshot_fingerprint: Fingerprint(4),
                }]),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            request => panic!("unexpected changed-read request: {request:?}"),
        };
        assert_ne!(second, first);
        assert_eq!(registry.task_predecessor(second), Some(first));
        assert_eq!(registry.cursor(cursor).unwrap().position, 17);
    }

    #[test]
    fn profile_counts_exact_unique_physical_subproblems() {
        let mut registry = TaskRegistry::default();
        let first_goal = goal();
        let second_goal = OptimizationGoal {
            row_goal: RowGoal::AtMost(1),
            ..first_goal
        };
        for (group, requested_goal) in [
            (GroupId::new(0), first_goal),
            (GroupId::new(0), first_goal),
            (GroupId::new(1), first_goal),
            (GroupId::new(0), second_goal),
        ] {
            registry
                .request(
                    TaskIntent::Optimize {
                        group,
                        goal: requested_goal,
                    },
                    ReadSet::empty(),
                )
                .unwrap();
        }
        assert_eq!(registry.profile().unique_subproblems, 3);
    }

    #[test]
    fn dependency_completion_wakes_waiter_without_lost_wakeup() {
        let mut registry = TaskRegistry::default();
        let make = |registry: &mut TaskRegistry, expression| match registry
            .request(
                TaskIntent::Discover {
                    expression,
                    rule: RuleId::new(1),
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            request => panic!("unexpected request: {request:?}"),
        };
        let parent = make(&mut registry, LogicalExprId::new(0));
        let child = make(&mut registry, LogicalExprId::new(1));
        registry.start(parent).unwrap();
        registry.start(child).unwrap();
        assert!(!registry.await_dependencies(parent, [child]).unwrap());
        assert_eq!(registry.state(parent), Some(TaskState::Awaiting));
        let wakeups = registry
            .complete(
                child,
                TaskOutcome::Progress {
                    cursor: CursorId::new(0),
                },
            )
            .unwrap();
        assert_eq!(registry.state(parent), Some(TaskState::Runnable));
        assert!(wakeups.iter().any(|wakeup| wakeup.task == parent));
    }

    #[test]
    fn dependency_already_completed_is_observed_during_registration() {
        let mut registry = TaskRegistry::default();
        let parent = match registry
            .request(
                TaskIntent::Discover {
                    expression: LogicalExprId::new(0),
                    rule: RuleId::new(1),
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        let child = match registry
            .request(
                TaskIntent::Discover {
                    expression: LogicalExprId::new(1),
                    rule: RuleId::new(1),
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        registry.start(child).unwrap();
        registry
            .complete(
                child,
                TaskOutcome::NoCandidate {
                    cursor: registry.task(child).unwrap().cursor,
                },
            )
            .unwrap();
        registry.start(parent).unwrap();
        assert!(registry.await_dependencies(parent, [child]).unwrap());
        assert_eq!(registry.state(parent), Some(TaskState::Runnable));
    }

    #[test]
    fn publication_commits_local_segment_before_waiting_and_wakes_parent() {
        let mut registry = TaskRegistry::default();
        let parent = match registry
            .request(
                TaskIntent::Discover {
                    expression: LogicalExprId::new(0),
                    rule: RuleId::new(1),
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        let child = match registry
            .request(
                TaskIntent::Discover {
                    expression: LogicalExprId::new(1),
                    rule: RuleId::new(1),
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        registry.start(parent).unwrap();
        registry.start(child).unwrap();
        registry.reserve_once(parent, 0, 4).unwrap();
        let object = registry.allocate_object(parent).unwrap();
        let memo = Memo::new(Default::default());
        let wakeups = registry
            .publish_current(
                parent,
                &memo,
                [child],
                TaskOutcome::Progress {
                    cursor: CursorId::new(0),
                },
            )
            .unwrap();
        assert!(wakeups.is_empty());
        assert_eq!(registry.state(parent), Some(TaskState::Awaiting));
        assert_eq!(registry.published_owner(object), Some(parent));
        assert_eq!(registry.reserved_units(), 0);
        assert_eq!(registry.committed_units(), 4);

        let wakeups = registry
            .complete(
                child,
                TaskOutcome::NoCandidate {
                    cursor: registry.task(child).unwrap().cursor,
                },
            )
            .unwrap();
        assert!(wakeups.iter().any(|wakeup| wakeup.task == parent));
        assert_eq!(registry.state(parent), Some(TaskState::Runnable));
        registry.start(parent).unwrap();
        registry
            .publish_current(
                parent,
                &memo,
                [],
                TaskOutcome::Progress {
                    cursor: CursorId::new(0),
                },
            )
            .unwrap();
        assert_eq!(registry.state(parent), Some(TaskState::Completed));
    }

    #[test]
    fn local_rollback_does_not_delete_another_task_publication() {
        let (mut registry, a) = registry_with_task();
        let b = match registry
            .request(
                TaskIntent::Discover {
                    expression: LogicalExprId::new(2),
                    rule: RuleId::new(3),
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        registry.reserve_once(a, 1, 2).unwrap();
        registry.reserve_once(b, 1, 3).unwrap();
        let a_object = registry.allocate_object(a).unwrap();
        let b_object = registry.allocate_object(b).unwrap();
        registry.commit_segment(b).unwrap();
        registry.rollback_segment(a).unwrap();
        assert!(!registry.is_published(a_object));
        assert_eq!(registry.published_owner(b_object), Some(b));
        assert_eq!(registry.committed_units(), 3);
        assert_eq!(registry.reserved_units(), 0);
    }

    #[test]
    fn group_redirect_invalidates_old_subproblem_evaluations() {
        let mut registry = TaskRegistry::default();
        let goal = goal();
        let task = match registry
            .request(
                TaskIntent::Optimize {
                    group: GroupId::new(4),
                    goal,
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        registry.start(task).unwrap();
        registry
            .redirect_group(GroupId::new(4), GroupId::new(2))
            .unwrap();
        assert_eq!(registry.canonical_group(GroupId::new(4)), GroupId::new(2));
        assert_eq!(registry.state(task), Some(TaskState::Invalidated));
        assert_eq!(registry.profile().invalidated, 1);
    }

    #[test]
    fn group_redirect_invalidates_transform_intents_and_read_subscribers() {
        let mut registry = TaskRegistry::default();
        let transform = match registry
            .request(
                TaskIntent::Transform {
                    expression: LogicalExprId::new(7),
                    rule: RuleId::new(8),
                    binding: PatternBinding {
                        root: PatternOperand::Group(GroupId::new(4)),
                        fingerprint: Fingerprint(9),
                    },
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        let reader = match registry
            .request(
                TaskIntent::Discover {
                    expression: LogicalExprId::new(10),
                    rule: RuleId::new(11),
                },
                ReadSet::new([PatternRead {
                    group: GroupId::new(4),
                    scope: ReadScope::FACTS,
                    logical_frontier_revision: None,
                    physical_goal: None,
                    physical_implementation_revision: None,
                    physical_frontier_revision: None,
                    logical_fact_fingerprint: Fingerprint(12),
                    statistics_snapshot_fingerprint: Fingerprint(13),
                }]),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        registry.start(transform).unwrap();
        registry.start(reader).unwrap();

        registry
            .redirect_group(GroupId::new(4), GroupId::new(2))
            .unwrap();

        assert_eq!(registry.state(transform), Some(TaskState::Invalidated));
        assert_eq!(registry.state(reader), Some(TaskState::Invalidated));
        assert_eq!(registry.profile().invalidated, 2);
    }

    #[test]
    fn current_request_canonicalizes_merged_group_aliases_in_identity_and_reads() {
        let mut memo = Memo::new(Default::default());
        let canonical = memo.create_group(
            GroupSchema::new([]).unwrap(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let secondary = memo.create_group(
            GroupSchema::new([]).unwrap(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );

        let mut registry = TaskRegistry::default();
        let old = match registry
            .request(
                TaskIntent::Optimize {
                    group: secondary,
                    goal: goal(),
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        registry.start(old).unwrap();
        memo.merge_groups(canonical, secondary).unwrap();
        registry
            .redirect_group(secondary, canonical)
            .expect("registry redirect should invalidate the old alias");

        let mut aliased_read = PatternRead::from_group(&memo, canonical).unwrap();
        aliased_read.group = secondary;
        let request = registry
            .request_current(
                TaskIntent::Optimize {
                    group: secondary,
                    goal: goal(),
                },
                ReadSet::single(aliased_read),
                &memo,
            )
            .unwrap();
        let TaskRequest::Leader(current) = request else {
            panic!("merged alias unexpectedly reused an old task: {request:?}");
        };
        assert_ne!(current, old);
        assert!(matches!(
            registry.intent(registry.task(current).unwrap().intent),
            Some(TaskIntent::Optimize { group, .. }) if *group == canonical
        ));
        assert_eq!(
            registry
                .read_set(registry.task(current).unwrap().read_set)
                .unwrap()
                .reads()[0]
                .group,
            canonical
        );

        let binding = PatternBinding {
            root: PatternOperand::Expression {
                group: secondary,
                expression: LogicalExprId::new(41),
                children: Box::new([PatternOperand::Group(secondary)]),
            },
            fingerprint: Fingerprint(42),
        };
        let transform = match registry
            .request_current(
                TaskIntent::Transform {
                    expression: LogicalExprId::new(41),
                    rule: RuleId::new(43),
                    binding,
                },
                ReadSet::empty(),
                &memo,
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        assert!(matches!(
            registry.intent(registry.task(transform).unwrap().intent),
            Some(TaskIntent::Transform {
                binding: PatternBinding {
                    root: PatternOperand::Expression { group, children, .. },
                    ..
                },
                ..
            }) if *group == canonical
                && children.as_ref() == [PatternOperand::Group(canonical)]
        ));
    }

    #[test]
    fn invalidation_rolls_back_only_the_redirected_task_segment() {
        let mut registry = TaskRegistry::default();
        let task = match registry
            .request(
                TaskIntent::Optimize {
                    group: GroupId::new(4),
                    goal: goal(),
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            TaskRequest::Leader(task) => task,
            _ => unreachable!(),
        };
        registry.start(task).unwrap();
        registry.reserve_once(task, 0, 2).unwrap();
        let object = registry.allocate_object(task).unwrap();
        registry
            .redirect_group(GroupId::new(4), GroupId::new(2))
            .unwrap();
        assert_eq!(registry.state(task), Some(TaskState::Invalidated));
        assert!(!registry.is_published(object));
        assert_eq!(registry.reserved_units(), 0);
        assert_eq!(registry.committed_units(), 0);
    }

    #[test]
    fn completion_obligations_prevent_false_closure() {
        let (mut registry, task) = registry_with_task();
        registry.start(task).unwrap();
        let obligation = registry
            .add_completion_obligation(task, "unrun child combination")
            .unwrap();
        assert!(registry
            .complete(
                task,
                TaskOutcome::Progress {
                    cursor: CursorId::new(0)
                }
            )
            .is_err());
        registry.discharge_obligation(task, obligation).unwrap();
        registry
            .complete(
                task,
                TaskOutcome::Progress {
                    cursor: CursorId::new(0),
                },
            )
            .unwrap();
        assert_eq!(registry.state(task), Some(TaskState::Completed));
    }

    #[test]
    fn bound_requires_explicit_context_and_tracks_read_validity() {
        let (mut registry, task) = registry_with_task();
        let reads = registry.intern_read_set(ReadSet::empty());
        let proof = registry
            .record_lower_bound(
                task,
                BoundContext {
                    group: GroupId::new(0),
                    goal: goal(),
                    reads,
                    search_domain: Fingerprint(7),
                },
                10,
            )
            .unwrap();
        assert!(registry
            .bound_is_current(proof, &Memo::new(Default::default()))
            .unwrap());
        assert!(matches!(
            registry.bound(proof).unwrap().kind,
            BoundProofKind::Lower { value: 10 }
        ));

        assert!(registry
            .record_lower_bound(
                task,
                BoundContext {
                    group: GroupId::new(1),
                    goal: goal(),
                    reads,
                    search_domain: Fingerprint(8),
                },
                11,
            )
            .is_err());

        registry.invalidate(task).unwrap();
        assert!(!registry
            .bound_is_current(proof, &Memo::new(Default::default()))
            .unwrap());
    }

    #[test]
    fn completed_proof_remains_current_only_when_completion_names_it() {
        let (mut registry, task) = registry_with_task();
        let reads = registry.intern_read_set(ReadSet::empty());
        let context = BoundContext {
            group: GroupId::new(0),
            goal: goal(),
            reads,
            search_domain: Fingerprint(17),
        };
        let proof = registry
            .record_no_plan_below(task, context.clone(), 99)
            .unwrap();
        registry
            .complete(
                task,
                TaskOutcome::ProvenNoPlanBelow {
                    threshold: 99,
                    certificate: proof,
                },
            )
            .unwrap();
        assert!(registry
            .bound_is_current(proof, &Memo::new(Default::default()))
            .unwrap());

        let (mut ordinary_registry, ordinary_task) = registry_with_task();
        let ordinary_reads = ordinary_registry.intern_read_set(ReadSet::empty());
        let ordinary_proof = ordinary_registry
            .record_no_plan_below(
                ordinary_task,
                BoundContext {
                    reads: ordinary_reads,
                    ..context
                },
                99,
            )
            .unwrap();
        ordinary_registry
            .complete(
                ordinary_task,
                TaskOutcome::NoChange {
                    reads: ordinary_reads,
                },
            )
            .unwrap();
        assert!(!ordinary_registry
            .bound_is_current(ordinary_proof, &Memo::new(Default::default()))
            .unwrap());
    }
}
