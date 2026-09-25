// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Session-owned diagnostic snapshots shared across statement contexts.

use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

use crate::compile_diagnostics::{
    AdmissionFallback, AdmissionReceiptId, AdmissionResult, ArtifactIdentity, ExecutionImageStatus,
    ExecutionReceipt, ExecutionReceiptId, ExecutionTerminal, LoweringStatus, Observation,
    PlanningStatus, ResourceReceipt, ResourceReservationStatus, SelectionIdentity,
    RECEIPT_SCHEMA_VERSION,
};
use crate::StatementTraceSnapshot;

pub const COMPILE_RECEIPT_SCHEMA_VERSION: u32 = crate::compile_diagnostics::SCHEMA_VERSION;
const MAX_ACTIVE_EXECUTION_RECEIPTS: usize = 256;
const MAX_ACTIVE_STATEMENT_DECISIONS: usize = 256;

/// Unit of the value exposed by one optimizer diagnostic row.
///
/// Keeping the unit in the session contract prevents counters, byte totals,
/// and invocation counts from being silently interpreted as one another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerMetricUnit {
    Invocations,
    Bytes,
    Count,
    Insertions,
    Attempts,
}

impl OptimizerMetricUnit {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Invocations => "invocations",
            Self::Bytes => "bytes",
            Self::Count => "count",
            Self::Insertions => "insertions",
            Self::Attempts => "attempts",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerDiagnostic {
    pub name: String,
    pub kind: String,
    pub last_elapsed_us: i64,
    /// The primary value represented by this row.  Its unit is explicit so
    /// bytes and budget events can never masquerade as invocation counts.
    pub metric_value: i64,
    pub metric_unit: OptimizerMetricUnit,
    /// Kept only for timing rows; non-timing metrics set this to zero.
    pub invocation_count: i64,
}

/// A lightweight, post-timer-readable decision for the instance plan cache.
/// It is deliberately separate from the full statement trace so a normal C1
/// sample can prove a miss without enabling per-event tracing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementCacheDecision {
    /// Session-monotonic identity of this cache decision.  `occurrence` is
    /// retained only as human-readable context and is not an association key.
    pub decision_id: u64,
    pub query_fingerprint: u64,
    pub occurrence: u64,
    pub cache_hit: bool,
    pub artifact_identity: Option<ArtifactIdentity>,
    pub compile_work: Option<CompileWork>,
    pub compile_receipt: Option<CompileReceiptSummary>,
}

/// Scalar work ledger read after the client timer. Collection is not free:
/// the clocks/counter copies still execute inside the original SELECT.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompileWork {
    pub compiler_elapsed_us: u64,
    pub optimizer_elapsed_us: u64,
    pub normalization_elapsed_us: u64,
    pub physical_alternatives: u64,
}

/// Immutable compiler-side context retained by a shareable artifact. This is
/// the source receipt for a cache hit; the hit still receives a fresh
/// execution receipt for its actual admission and terminal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompileReceiptSummary {
    pub schema_version: u32,
    pub artifact_identity: Option<ArtifactIdentity>,
    pub planning_status: Observation<PlanningStatus>,

    pub budget_limited: Observation<bool>,

    pub expected_class: Observation<u32>,
    pub variant_count: Observation<usize>,
    pub omitted_variants: u64,
    pub compile_work: Option<CompileWork>,
}

pub fn compile_work_evidence_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED
        .get_or_init(|| std::env::var("PARO_COMPILE_WORK_EVIDENCE").is_ok_and(|value| value == "1"))
}

#[derive(Debug, Default)]
pub struct SessionDiagnostics {
    optimizer: RwLock<Vec<OptimizerDiagnostic>>,
    statement_trace: RwLock<Option<StatementTraceSnapshot>>,
    statement_cache: RwLock<Vec<StatementCacheDecision>>,
    /// Decisions with a live producer/execution owner stay outside bounded
    /// terminal history so a live handle can never lose its update target.
    active_statement_cache: RwLock<std::collections::BTreeMap<u64, StatementCacheDecision>>,
    execution_work: RwLock<Vec<ExecutionWorkRecord>>,
    /// Completed/terminal receipts are retained in a bounded history.  An
    /// active handle is kept separately so history eviction can never make a
    /// live execution silently unupdatable.
    execution_receipts: RwLock<Vec<ExecutionReceipt>>,
    active_execution_receipts: RwLock<std::collections::BTreeMap<u64, ExecutionReceipt>>,
    /// Admission is a decision boundary, not an execution occurrence.  Keep
    /// its allocator independent so retries and rejected executions cannot
    /// accidentally make the two identities aliases.
    admission_sequence: std::sync::atomic::AtomicU64,
    execution_sequence: std::sync::atomic::AtomicU64,
    statement_sequence: std::sync::atomic::AtomicU64,
    statement_decision_capacity_exceeded: std::sync::atomic::AtomicU64,
    execution_receipt_capacity_exceeded: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone)]
pub struct ExecutionWorkRecord {
    pub query_fingerprint: u64,
    /// Session-monotonic actual execution identity, including prepared warm runs.
    /// It is intentionally not the plan-cache lookup occurrence.
    pub execution_id: u64,
    pub image_id: u64,
    pub snapshot: paro_common::cold_work::Snapshot,
}

/// Immutable admission facts supplied when an execution receipt is created.
/// Keeping these facts together prevents callers from accidentally pairing an
/// artifact with a selection or resource contract from another admission.
#[derive(Debug, Clone)]
pub struct ExecutionReceiptStart {
    pub statement_decision_id: Option<u64>,
    pub artifact_identity: ArtifactIdentity,
    pub expected_class: Option<u32>,
    pub actual_class: Option<u32>,
    pub actual_fingerprint: Option<[u64; 2]>,
    pub resources: Option<ResourceReceipt>,
    pub admission: AdmissionResult,
    pub fallback: Option<AdmissionFallback>,
}

/// A small capability held by a real result handler.  Admission is published
/// before the pipeline is built; the handler closes the same record when the
/// terminal execution state is known.  Dropping the handler therefore cannot
/// leave an apparently successful execution receipt behind.
#[derive(Debug, Clone)]
pub struct ExecutionReceiptHandle {
    lease: Arc<ExecutionReceiptLease>,
}

/// The terminal-drop fallback belongs to the shared receipt lease, not to
/// each clone of the capability.  The executor keeps one clone while the
/// result handler owns another; dropping the executor's local clone must not
/// turn a still-running execution into `Dropped`.
#[derive(Debug)]
struct ExecutionReceiptLease {
    diagnostics: Arc<SessionDiagnostics>,
    execution_id: u64,
    registered: bool,
}

impl ExecutionReceiptHandle {
    pub fn execution_id(&self) -> Option<u64> {
        self.lease.registered.then_some(self.lease.execution_id)
    }

    pub fn complete(&self) {
        self.finish(ExecutionTerminal::Completed, None);
    }

    pub fn fail(&self, error: impl Into<String>) {
        self.finish(ExecutionTerminal::Failed, Some(error.into()));
    }

    pub fn cancel(&self, error: impl Into<String>) {
        self.finish(ExecutionTerminal::Cancelled, Some(error.into()));
    }

    pub fn image_ready(&self) {
        if !self.lease.registered {
            return;
        }
        self.lease
            .diagnostics
            .mark_execution_image_ready(self.lease.execution_id);
    }

    pub fn lowering_ready(&self) {
        if !self.lease.registered {
            return;
        }
        self.lease.diagnostics.mark_execution_lowering(
            self.lease.execution_id,
            LoweringStatus::Ready,
            None,
        );
    }

    pub fn lowering_failed(&self, error: impl Into<String>) {
        if !self.lease.registered {
            return;
        }
        let error = error.into();
        self.lease.diagnostics.mark_execution_lowering(
            self.lease.execution_id,
            LoweringStatus::Failed,
            Some(error.clone()),
        );
        self.fail(error);
    }

    pub fn reservation_failed(&self, error: impl Into<String>) {
        if !self.lease.registered {
            return;
        }
        self.lease
            .diagnostics
            .mark_execution_reservation_failed(self.lease.execution_id, error.into());
    }

    /// Publish the exact selection only after the execution has crossed the
    /// admission boundary.  The executor may call this before a fallible
    /// reservation operation so a reservation error retains the selected
    /// variant instead of being misreported as a planning failure.
    pub fn selected(
        &self,
        actual_class: Option<u32>,
        actual_fingerprint: Option<[u64; 2]>,
        resources: Option<ResourceReceipt>,
        fallback: Option<AdmissionFallback>,
    ) {
        if !self.lease.registered {
            return;
        }
        self.lease.diagnostics.mark_execution_selected(
            self.lease.execution_id,
            actual_class,
            actual_fingerprint,
            resources,
            fallback,
        );
    }

    /// Preserve an admission failure without turning it into an execution
    /// terminal.  An infeasible or failed admission is deliberately
    /// `NotExecuted`; the original bounded error still belongs on the receipt.
    pub fn record_error(&self, error: impl Into<String>) {
        if !self.lease.registered {
            return;
        }
        self.lease
            .diagnostics
            .record_execution_receipt_error(self.lease.execution_id, error.into());
    }

    /// Record a verified resource/dependency infeasibility without claiming
    /// that the selected image failed.  The execution remains NotExecuted;
    /// the caller's normal error path retains the original SQL error.
    pub fn infeasible(&self, error: impl Into<String>) {
        if !self.lease.registered {
            return;
        }
        self.lease
            .diagnostics
            .mark_execution_infeasible(self.lease.execution_id, error.into());
    }

    fn finish(&self, terminal: ExecutionTerminal, error: Option<String>) {
        if !self.lease.registered {
            return;
        }
        self.lease
            .diagnostics
            .finish_execution_receipt(self.lease.execution_id, terminal, error);
    }
}

impl Drop for ExecutionReceiptLease {
    fn drop(&mut self) {
        if !self.registered {
            return;
        }
        self.diagnostics.finish_execution_receipt(
            self.execution_id,
            ExecutionTerminal::Dropped,
            None,
        );
    }
}

impl SessionDiagnostics {
    pub fn publish_execution_work(
        &self,
        execution_id: u64,
        query_fingerprint: u64,
        image_id: u64,
        snapshot: paro_common::cold_work::Snapshot,
    ) {
        let mut records = self.execution_work.write().unwrap();
        records.push(ExecutionWorkRecord {
            query_fingerprint,
            execution_id,
            image_id,
            snapshot,
        });
        if records.len() > 64 {
            records.remove(0);
        }
    }

    pub fn begin_execution_receipt(
        self: &Arc<Self>,
        start: ExecutionReceiptStart,
    ) -> ExecutionReceiptHandle {
        let execution_id = self
            .execution_sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let admission_id = self
            .admission_sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let statement_decision_id = start.statement_decision_id;
        let receipt = ExecutionReceipt {
            schema_version: RECEIPT_SCHEMA_VERSION,
            execution_id: ExecutionReceiptId(execution_id),
            admission_receipt_id: AdmissionReceiptId(admission_id),
            selection_identity: start.actual_class.zip(start.actual_fingerprint).map(
                |(grant_class, physical_fingerprint)| SelectionIdentity {
                    artifact: start.artifact_identity.artifact,
                    grant_class: Some(grant_class),
                    physical_fingerprint: Some(physical_fingerprint),
                },
            ),
            statement_decision_id,
            artifact_identity: start.artifact_identity,
            expected_class: start.expected_class,
            actual_class: start.actual_class,
            actual_fingerprint: start.actual_fingerprint,
            resources: start.resources,
            admission: start.admission,
            fallback: start.fallback,
            reservation: if start.admission == AdmissionResult::Selected {
                ResourceReservationStatus::Committed
            } else {
                ResourceReservationStatus::NotRequired
            },
            lowering: LoweringStatus::NotStarted,
            lowering_error: None,
            image: ExecutionImageStatus::NotReady,
            terminal: if start.admission == AdmissionResult::Selected {
                ExecutionTerminal::Running
            } else {
                ExecutionTerminal::NotExecuted
            },
            terminal_error: None,
        };
        let mut active = self.active_execution_receipts.write().unwrap();
        if active.len() >= MAX_ACTIVE_EXECUTION_RECEIPTS {
            self.execution_receipt_capacity_exceeded
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // The execution is still allowed to proceed without a retained
            // diagnostic receipt, but its statement decision must not remain
            // an unbounded live handle.  Preserve the decision in bounded
            // history and expose the missing execution receipt through the
            // capacity counter/table row.
            drop(active);
            if let Some(decision_id) = statement_decision_id {
                self.finish_statement_cache_decision(decision_id);
            }
            return ExecutionReceiptHandle {
                lease: Arc::new(ExecutionReceiptLease {
                    diagnostics: Arc::clone(self),
                    execution_id,
                    registered: false,
                }),
            };
        }
        active.insert(execution_id, receipt);
        ExecutionReceiptHandle {
            lease: Arc::new(ExecutionReceiptLease {
                diagnostics: Arc::clone(self),
                execution_id,
                registered: true,
            }),
        }
    }

    pub fn execution_receipt_capacity_exceeded(&self) -> u64 {
        self.execution_receipt_capacity_exceeded
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn mark_execution_image_ready(&self, execution_id: u64) {
        let mut receipts = self.active_execution_receipts.write().unwrap();
        let Some(receipt) = receipts.get_mut(&execution_id) else {
            return;
        };
        if receipt.admission == AdmissionResult::Selected
            && receipt.terminal == ExecutionTerminal::Running
        {
            receipt.image = ExecutionImageStatus::Ready;
        }
    }

    fn mark_execution_selected(
        &self,
        execution_id: u64,
        actual_class: Option<u32>,
        actual_fingerprint: Option<[u64; 2]>,
        resources: Option<ResourceReceipt>,
        fallback: Option<AdmissionFallback>,
    ) {
        let mut receipts = self.active_execution_receipts.write().unwrap();
        let Some(receipt) = receipts.get_mut(&execution_id) else {
            return;
        };
        if receipt.terminal != ExecutionTerminal::NotExecuted {
            return;
        }
        receipt.admission = AdmissionResult::Selected;
        receipt.actual_class = actual_class;
        receipt.actual_fingerprint = actual_fingerprint;
        receipt.selection_identity = Some(SelectionIdentity {
            artifact: receipt.artifact_identity.artifact,
            grant_class: actual_class,
            physical_fingerprint: actual_fingerprint,
        });
        receipt.resources = resources;
        receipt.fallback = fallback;
        receipt.reservation = if receipt.resources.is_some() {
            ResourceReservationStatus::Committed
        } else {
            ResourceReservationStatus::NotRequired
        };
        receipt.terminal = ExecutionTerminal::Running;
        receipt.terminal_error = None;
    }

    fn mark_execution_lowering(
        &self,
        execution_id: u64,
        status: LoweringStatus,
        error: Option<String>,
    ) {
        let mut receipts = self.active_execution_receipts.write().unwrap();
        let Some(receipt) = receipts.get_mut(&execution_id) else {
            return;
        };
        if receipt.terminal == ExecutionTerminal::Running {
            receipt.lowering = status;
            receipt.lowering_error = error.map(|value| value.chars().take(256).collect());
        }
    }

    fn mark_execution_reservation_failed(&self, execution_id: u64, error: String) {
        let mut receipts = self.active_execution_receipts.write().unwrap();
        let Some(receipt) = receipts.get_mut(&execution_id) else {
            return;
        };
        if receipt.terminal == ExecutionTerminal::Running {
            receipt.reservation = ResourceReservationStatus::Failed;
            receipt.terminal_error = Some(error.chars().take(256).collect());
        }
    }

    pub fn finish_execution_receipt(
        &self,
        execution_id: u64,
        terminal: ExecutionTerminal,
        error: Option<String>,
    ) {
        let mut active = self.active_execution_receipts.write().unwrap();
        let Some(receipt) = active.get_mut(&execution_id) else {
            return;
        };
        match receipt.terminal {
            ExecutionTerminal::Running => {
                receipt.terminal = terminal;
                receipt.terminal_error = error.map(|value| value.chars().take(256).collect());
            }
            ExecutionTerminal::NotExecuted => {
                if let Some(error) = error {
                    receipt.terminal_error = Some(error.chars().take(256).collect());
                }
                // Preserve the admission failure as NotExecuted. Dropping its
                // handle closes the record but must not relabel it as a query
                // cancellation or successful execution.
            }
            _ => return,
        }
        let decision_id = receipt.statement_decision_id;
        let receipt = active
            .remove(&execution_id)
            .expect("active receipt was just looked up");
        drop(active);
        let mut history = self.execution_receipts.write().unwrap();
        history.push(receipt);
        if history.len() > 64 {
            history.remove(0);
        }
        if let Some(decision_id) = decision_id {
            self.finish_statement_cache_decision(decision_id);
        }
    }

    fn record_execution_receipt_error(&self, execution_id: u64, error: String) {
        let mut receipts = self.active_execution_receipts.write().unwrap();
        let Some(receipt) = receipts.get_mut(&execution_id) else {
            return;
        };
        if receipt.terminal == ExecutionTerminal::NotExecuted {
            receipt.terminal_error = Some(error.chars().take(256).collect());
        }
    }

    fn mark_execution_infeasible(&self, execution_id: u64, error: String) {
        let mut receipts = self.active_execution_receipts.write().unwrap();
        let Some(receipt) = receipts.get_mut(&execution_id) else {
            return;
        };
        if receipt.terminal == ExecutionTerminal::NotExecuted {
            receipt.admission = AdmissionResult::Infeasible;
            receipt.terminal_error = Some(error.chars().take(256).collect());
        }
    }

    pub fn execution_receipts_snapshot(&self) -> Vec<ExecutionReceipt> {
        let mut result = self.execution_receipts.read().unwrap().clone();
        result.extend(
            self.active_execution_receipts
                .read()
                .unwrap()
                .values()
                .cloned(),
        );
        result.sort_by_key(|receipt| receipt.execution_id);
        result
    }

    pub fn execution_receipt(&self, execution_id: u64) -> Option<ExecutionReceipt> {
        if let Some(receipt) = self
            .active_execution_receipts
            .read()
            .unwrap()
            .get(&execution_id)
            .cloned()
        {
            return Some(receipt);
        }
        self.execution_receipts
            .read()
            .unwrap()
            .iter()
            .find(|receipt| receipt.execution_id == ExecutionReceiptId(execution_id))
            .cloned()
    }

    /// Close a decision when compilation itself fails before an execution
    /// receipt can be created.  This is the same terminal transition used by
    /// an execution receipt; it prevents a failed active decision from
    /// consuming unbounded live state while preserving it in history.
    pub fn finish_statement_cache_decision(&self, decision_id: u64) {
        let Some(decision) = self
            .active_statement_cache
            .write()
            .unwrap()
            .remove(&decision_id)
        else {
            return;
        };
        let mut history = self.statement_cache.write().unwrap();
        history.push(decision);
        if history.len() > 256 {
            history.remove(0);
        }
    }

    pub fn execution_work_snapshot(&self) -> Vec<ExecutionWorkRecord> {
        self.execution_work.read().unwrap().clone()
    }
    pub fn publish_optimizer(&self, entries: Vec<OptimizerDiagnostic>) {
        *self.optimizer.write().unwrap() = entries;
    }

    pub fn optimizer_snapshot(&self) -> Vec<OptimizerDiagnostic> {
        self.optimizer.read().unwrap().clone()
    }

    pub fn publish_statement_trace(&self, trace: StatementTraceSnapshot) {
        *self.statement_trace.write().unwrap() = Some(trace);
    }

    pub fn statement_trace_snapshot(&self) -> Option<StatementTraceSnapshot> {
        self.statement_trace.read().unwrap().clone()
    }

    pub fn publish_statement_cache_decision(
        &self,
        query_fingerprint: u64,
        cache_hit: bool,
    ) -> Option<u64> {
        let mut active = self.active_statement_cache.write().unwrap();
        if active.len() >= MAX_ACTIVE_STATEMENT_DECISIONS {
            self.statement_decision_capacity_exceeded
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return None;
        }
        let decision_id = self
            .statement_sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let occurrence = active
            .iter()
            .filter(|(_, decision)| decision.query_fingerprint == query_fingerprint)
            .count() as u64;
        active.insert(
            decision_id,
            StatementCacheDecision {
                decision_id,
                query_fingerprint,
                occurrence,
                cache_hit,
                artifact_identity: None,
                compile_work: None,
                compile_receipt: None,
            },
        );
        Some(decision_id)
    }

    pub fn statement_decision_capacity_exceeded(&self) -> u64 {
        self.statement_decision_capacity_exceeded
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn publish_statement_artifact(
        &self,
        decision_id: u64,
        artifact_identity: ArtifactIdentity,
    ) {
        let mut active = self.active_statement_cache.write().unwrap();
        if let Some(decision) = active.get_mut(&decision_id) {
            decision.artifact_identity = Some(artifact_identity);
        }
    }

    pub fn publish_compile_work(&self, decision_id: u64, work: CompileWork) {
        let mut active = self.active_statement_cache.write().unwrap();
        if let Some(decision) = active.get_mut(&decision_id) {
            if decision.compile_work.is_some() {
                return;
            }
            decision.compile_work = Some(work);
        }
    }

    pub fn publish_compile_receipt(&self, decision_id: u64, mut receipt: CompileReceiptSummary) {
        let mut active = self.active_statement_cache.write().unwrap();
        if let Some(decision) = active.get_mut(&decision_id) {
            if receipt.artifact_identity.is_none() {
                receipt.artifact_identity = decision.artifact_identity;
            }
            if decision.compile_receipt.is_none() {
                decision.compile_work = receipt.compile_work;
                decision.compile_receipt = Some(receipt);
            }
        }
    }

    pub fn statement_cache_snapshot(&self) -> Vec<StatementCacheDecision> {
        let mut result = self.statement_cache.read().unwrap().clone();
        result.extend(
            self.active_statement_cache
                .read()
                .unwrap()
                .values()
                .cloned(),
        );
        result.sort_by_key(|decision| decision.decision_id);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_work_is_bound_to_the_decision_id_not_occurrence_or_latest_query() {
        let diagnostics = SessionDiagnostics::default();
        let first = diagnostics
            .publish_statement_cache_decision(11, false)
            .unwrap();
        let second = diagnostics
            .publish_statement_cache_decision(11, true)
            .unwrap();
        diagnostics
            .publish_statement_cache_decision(22, false)
            .unwrap();
        let work = CompileWork {
            compiler_elapsed_us: 20,
            optimizer_elapsed_us: 10,
            normalization_elapsed_us: 3,
            physical_alternatives: 7,
        };
        diagnostics.publish_compile_work(first, work);
        diagnostics.publish_compile_work(first, CompileWork::default());
        diagnostics.publish_compile_work(second, work);
        diagnostics.publish_compile_work(33, work);
        let rows = diagnostics.statement_cache_snapshot();
        assert_eq!(rows[0].decision_id, first);
        assert_eq!(rows[1].decision_id, second);
        assert_eq!(rows[0].compile_work, Some(work));
        assert_eq!(rows[1].compile_work, Some(work));
        assert_eq!(rows[2].compile_work, None);
    }

    #[test]
    fn execution_receipt_only_changes_at_a_real_terminal() {
        let diagnostics = Arc::new(SessionDiagnostics::default());
        let identity = ArtifactIdentity {
            schema_version: crate::compile_diagnostics::IDENTITY_SCHEMA_VERSION,
            artifact: crate::compile_diagnostics::CompiledArtifactId([1, 2]),
            structure: crate::compile_diagnostics::PlanStructureId([3, 4]),
            dependencies: [5, 6],
        };
        let handle = diagnostics.begin_execution_receipt(ExecutionReceiptStart {
            statement_decision_id: None,
            artifact_identity: identity,
            expected_class: Some(2),
            actual_class: Some(2),
            actual_fingerprint: Some([7, 8]),
            resources: None,
            admission: AdmissionResult::Selected,
            fallback: None,
        });
        assert_eq!(
            diagnostics.execution_receipts_snapshot()[0].terminal,
            ExecutionTerminal::Running
        );
        assert_eq!(
            diagnostics.execution_receipts_snapshot()[0].image,
            ExecutionImageStatus::NotReady
        );
        handle.image_ready();
        assert_eq!(
            diagnostics.execution_receipts_snapshot()[0].image,
            ExecutionImageStatus::Ready
        );
        handle.complete();
        assert_eq!(
            diagnostics.execution_receipts_snapshot()[0].terminal,
            ExecutionTerminal::Completed
        );
    }

    #[test]
    fn selected_reservation_failure_keeps_the_real_selection() {
        let diagnostics = Arc::new(SessionDiagnostics::default());
        let identity = ArtifactIdentity {
            schema_version: crate::compile_diagnostics::IDENTITY_SCHEMA_VERSION,
            artifact: crate::compile_diagnostics::CompiledArtifactId([21, 22]),
            structure: crate::compile_diagnostics::PlanStructureId([23, 24]),
            dependencies: [25, 26],
        };
        let handle = diagnostics.begin_execution_receipt(ExecutionReceiptStart {
            statement_decision_id: None,
            artifact_identity: identity,
            expected_class: Some(2),
            actual_class: None,
            actual_fingerprint: None,
            resources: None,
            admission: AdmissionResult::Failed,
            fallback: None,
        });
        handle.selected(
            Some(2),
            Some([31, 32]),
            Some(ResourceReceipt {
                class: 2,
                minimum_memory_bytes: 100,
                working_set_memory_bytes: 200,
                memory_ceiling_bytes: 300,
                memory_completion: crate::compile_diagnostics::MemoryCompletionReceipt::Guaranteed,
                max_parallel_tasks: 4,
                external_worker_slots: 0,
            }),
            Some(AdmissionFallback::LowerResourceClass),
        );
        handle.reservation_failed("reservation race");
        handle.fail("reservation race");
        let receipt = diagnostics.execution_receipts_snapshot().pop().unwrap();
        assert_eq!(receipt.admission, AdmissionResult::Selected);
        assert_eq!(receipt.actual_class, Some(2));
        assert_eq!(receipt.actual_fingerprint, Some([31, 32]));
        assert_eq!(receipt.reservation, ResourceReservationStatus::Failed);
        assert_eq!(receipt.terminal, ExecutionTerminal::Failed);
        assert_eq!(receipt.terminal_error.as_deref(), Some("reservation race"));
    }

    #[test]
    fn infeasible_admission_is_not_execution_failure() {
        let diagnostics = Arc::new(SessionDiagnostics::default());
        let handle = diagnostics.begin_execution_receipt(ExecutionReceiptStart {
            statement_decision_id: None,
            artifact_identity: ArtifactIdentity {
                schema_version: crate::compile_diagnostics::IDENTITY_SCHEMA_VERSION,
                artifact: crate::compile_diagnostics::CompiledArtifactId([41, 42]),
                structure: crate::compile_diagnostics::PlanStructureId([43, 44]),
                dependencies: [45, 46],
            },
            expected_class: Some(2),
            actual_class: None,
            actual_fingerprint: None,
            resources: None,
            admission: AdmissionResult::Failed,
            fallback: None,
        });
        handle.infeasible("no verified variant fits the resource contract");
        drop(handle);
        let receipt = diagnostics.execution_receipts_snapshot().pop().unwrap();
        assert_eq!(receipt.admission, AdmissionResult::Infeasible);
        assert_eq!(receipt.terminal, ExecutionTerminal::NotExecuted);
        assert!(receipt.terminal_error.is_some());
    }

    #[test]
    fn active_receipt_capacity_is_explicit_and_does_not_grow_unboundedly() {
        let diagnostics = Arc::new(SessionDiagnostics::default());
        let identity = ArtifactIdentity {
            schema_version: crate::compile_diagnostics::IDENTITY_SCHEMA_VERSION,
            artifact: crate::compile_diagnostics::CompiledArtifactId([1, 2]),
            structure: crate::compile_diagnostics::PlanStructureId([3, 4]),
            dependencies: [5, 6],
        };
        let handles: Vec<_> = (0..(MAX_ACTIVE_EXECUTION_RECEIPTS + 1))
            .map(|_| {
                diagnostics.begin_execution_receipt(ExecutionReceiptStart {
                    statement_decision_id: None,
                    artifact_identity: identity,
                    expected_class: Some(2),
                    actual_class: Some(2),
                    actual_fingerprint: Some([7, 8]),
                    resources: None,
                    admission: AdmissionResult::Selected,
                    fallback: None,
                })
            })
            .collect();
        assert_eq!(
            diagnostics.execution_receipts_snapshot().len(),
            MAX_ACTIVE_EXECUTION_RECEIPTS
        );
        assert_eq!(diagnostics.execution_receipt_capacity_exceeded(), 1);
        assert!(handles.last().unwrap().execution_id().is_none());
    }

    #[test]
    fn receipt_capacity_closes_the_associated_statement_decision() {
        let diagnostics = Arc::new(SessionDiagnostics::default());
        let decision = diagnostics
            .publish_statement_cache_decision(77, false)
            .unwrap();
        let identity = ArtifactIdentity {
            schema_version: crate::compile_diagnostics::IDENTITY_SCHEMA_VERSION,
            artifact: crate::compile_diagnostics::CompiledArtifactId([1, 2]),
            structure: crate::compile_diagnostics::PlanStructureId([3, 4]),
            dependencies: [5, 6],
        };
        let handles: Vec<_> = (0..MAX_ACTIVE_EXECUTION_RECEIPTS)
            .map(|_| {
                diagnostics.begin_execution_receipt(ExecutionReceiptStart {
                    statement_decision_id: None,
                    artifact_identity: identity,
                    expected_class: Some(2),
                    actual_class: Some(2),
                    actual_fingerprint: Some([7, 8]),
                    resources: None,
                    admission: AdmissionResult::Selected,
                    fallback: None,
                })
            })
            .collect();

        let unregistered = diagnostics.begin_execution_receipt(ExecutionReceiptStart {
            statement_decision_id: Some(decision),
            artifact_identity: identity,
            expected_class: Some(2),
            actual_class: Some(2),
            actual_fingerprint: Some([7, 8]),
            resources: None,
            admission: AdmissionResult::Selected,
            fallback: None,
        });
        assert!(unregistered.execution_id().is_none());
        assert_eq!(diagnostics.execution_receipt_capacity_exceeded(), 1);
        assert!(diagnostics
            .statement_cache_snapshot()
            .iter()
            .any(|item| item.decision_id == decision));

        drop(handles);
    }

    #[test]
    fn active_statement_decision_capacity_is_explicit_and_releases_on_terminal() {
        let diagnostics = SessionDiagnostics::default();
        let decisions: Vec<_> = (0..MAX_ACTIVE_STATEMENT_DECISIONS)
            .map(|index| {
                diagnostics
                    .publish_statement_cache_decision(index as u64, false)
                    .unwrap()
            })
            .collect();
        assert!(diagnostics
            .publish_statement_cache_decision(999, false)
            .is_none());
        assert_eq!(diagnostics.statement_decision_capacity_exceeded(), 1);
        diagnostics.finish_statement_cache_decision(decisions[0]);
        let replacement = diagnostics
            .publish_statement_cache_decision(1000, true)
            .expect("terminal decision releases its active slot");
        assert_ne!(replacement, decisions[0]);
        // The terminal decision remains in bounded history while the
        // replacement occupies the newly freed live slot.  The contract is
        // on active decisions, not on the combined live-plus-history view.
        assert_eq!(
            diagnostics.statement_cache_snapshot().len(),
            MAX_ACTIVE_STATEMENT_DECISIONS + 1
        );
    }

    #[test]
    fn dropping_one_receipt_clone_does_not_close_running_execution() {
        let diagnostics = Arc::new(SessionDiagnostics::default());
        let identity = ArtifactIdentity {
            schema_version: crate::compile_diagnostics::IDENTITY_SCHEMA_VERSION,
            artifact: crate::compile_diagnostics::CompiledArtifactId([11, 12]),
            structure: crate::compile_diagnostics::PlanStructureId([13, 14]),
            dependencies: [15, 16],
        };
        let handle = diagnostics.begin_execution_receipt(ExecutionReceiptStart {
            statement_decision_id: None,
            artifact_identity: identity,
            expected_class: Some(2),
            actual_class: Some(2),
            actual_fingerprint: Some([17, 18]),
            resources: None,
            admission: AdmissionResult::Selected,
            fallback: None,
        });
        let handler_handle = handle.clone();
        drop(handle);
        assert_eq!(
            diagnostics.execution_receipts_snapshot()[0].terminal,
            ExecutionTerminal::Running
        );
        handler_handle.complete();
        assert_eq!(
            diagnostics.execution_receipts_snapshot()[0].terminal,
            ExecutionTerminal::Completed
        );
    }
}
