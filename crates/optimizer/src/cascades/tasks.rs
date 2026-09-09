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
use super::rules::PatternRead;

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
        binding: Fingerprint,
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
    pub fn new(reads: impl IntoIterator<Item = PatternRead>) -> Self {
        let mut reads = reads.into_iter().collect::<Vec<_>>();
        reads.sort_unstable();
        reads.dedup();
        Self {
            reads: reads.into_boxed_slice(),
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
    Infeasible,
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
}

impl TaskRegistry {
    pub fn set_reservation_limit(&mut self, limit: Option<u64>) {
        self.reservation_limit = limit;
    }

    pub fn intern_intent(&mut self, intent: TaskIntent) -> TaskIntentId {
        if let Some(id) = self.intent_index.get(&intent).copied() {
            return id;
        }
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
        let intent = self.intern_intent(intent);
        let read_set = self.intern_read_set(reads);
        let inputs = self.intern_input_revision(read_set)?;
        let evaluation = EvaluationKey { intent, inputs };
        if let Some(task) = self.evaluations.get(&evaluation).copied() {
            let state = self
                .task(task)
                .ok_or_else(|| paro_error::internal("task evaluation index is corrupt"))?
                .state;
            return match state {
                TaskState::Completed | TaskState::Failed => Ok(TaskRequest::Reused {
                    task,
                    outcome: self.task(task).and_then(|record| record.outcome.clone()),
                }),
                TaskState::Invalidated | TaskState::Suspended => {
                    self.task_mut(task)?.state = TaskState::Runnable;
                    self.task_mut(task)?.outcome = None;
                    Ok(TaskRequest::Leader(task))
                }
                TaskState::Runnable | TaskState::Running | TaskState::Awaiting => {
                    let waiter = self.add_waiter(task)?;
                    Ok(TaskRequest::Subscriber { task, waiter })
                }
            };
        }
        let task = TaskId::new(self.tasks.len());
        let cursor = self.intern_cursor(Cursor::default());
        self.tasks.push(TaskRecord {
            id: task,
            intent,
            evaluation,
            read_set,
            cursor,
            state: TaskState::Runnable,
            outcome: None,
            dependencies: BTreeSet::new(),
            waiters: BTreeSet::new(),
            obligations: BTreeSet::new(),
        });
        self.evaluations.insert(evaluation, task);
        Ok(TaskRequest::Leader(task))
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

    pub fn task(&self, id: TaskId) -> Option<&TaskRecord> {
        self.tasks.get(id.index())
    }

    pub fn state(&self, id: TaskId) -> Option<TaskState> {
        self.task(id).map(|task| task.state)
    }

    pub fn evaluation_key(&self, id: TaskId) -> Option<EvaluationKey> {
        self.task(id).map(|task| task.evaluation)
    }

    pub fn task_read_set(&self, id: TaskId) -> Option<ReadSetId> {
        self.task(id).map(|task| task.read_set)
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
        let record = self.task_mut(task)?;
        if record.state != TaskState::Runnable {
            return Err(paro_error::internal(format!(
                "task {:?} cannot start from {:?}",
                task, record.state
            )));
        }
        record.state = TaskState::Running;
        Ok(())
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
        self.finish(task, TaskState::Completed, Some(outcome))
    }

    pub fn suspend(&mut self, task: TaskId, outcome: TaskOutcome) -> Result<Vec<TaskWakeup>> {
        if !matches!(
            self.state(task),
            Some(TaskState::Runnable | TaskState::Running)
        ) {
            return Err(paro_error::internal("task is not runnable for suspension"));
        }
        self.finish(task, TaskState::Suspended, Some(outcome))
    }

    pub fn fail(&mut self, task: TaskId, detail: impl Into<String>) -> Result<Vec<TaskWakeup>> {
        self.finish(
            task,
            TaskState::Failed,
            Some(TaskOutcome::Suspended {
                cursor: self
                    .task(task)
                    .map(|record| record.cursor)
                    .unwrap_or(CursorId::INVALID),
                reason: StopReason::ResourceStop {
                    dimension: BudgetDimension::SearchCandidate,
                },
            }),
        )
        .map(|wakeups| {
            let _ = detail.into();
            wakeups
        })
    }

    pub fn invalidate(&mut self, task: TaskId) -> Result<Vec<TaskWakeup>> {
        if self.task(task).is_none() {
            return Err(paro_error::internal("unknown task invalidation"));
        }
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
        if self.task(task).is_none() {
            return Err(paro_error::internal("reservation belongs to unknown task"));
        }
        if units == 0 {
            return Ok(None);
        }
        if let Some(reservation) = self.reservation_slots.get(&(task, slot)).copied() {
            return Ok(Some(reservation));
        }
        let used = self.committed_units.saturating_add(self.reserved_units);
        if self
            .reservation_limit
            .is_some_and(|limit| units > limit.saturating_sub(used))
        {
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
        if self.task(task).is_none() || self.read_set(context.reads).is_none() {
            return Err(paro_error::internal(
                "bound proof references unknown task/read set",
            ));
        }
        let id = BoundProofId::new(self.bounds.len());
        self.bounds.push(BoundProof {
            id,
            task,
            context,
            kind,
        });
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
        self.tasks
            .iter()
            .filter(|task| {
                self.read_set(task.read_set)
                    .is_some_and(|reads| reads.is_current(memo).unwrap_or(false))
            })
            .count()
            .try_into()
            .map_err(|_| paro_error::internal("task count overflow"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cascades::ids::PropertySetId;
    use crate::cascades::memo::{GrantGoalKey, RowGoal};
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
        registry.complete(child, TaskOutcome::Infeasible).unwrap();
        registry.start(parent).unwrap();
        assert!(registry.await_dependencies(parent, [child]).unwrap());
        assert_eq!(registry.state(parent), Some(TaskState::Runnable));
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
    }
}
