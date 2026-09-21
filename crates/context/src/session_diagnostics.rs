// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Session-owned diagnostic snapshots shared across statement contexts.

use std::sync::{Arc, RwLock};

use crate::StatementTraceSnapshot;
use crate::compile_diagnostics::{
    AdmissionFallback, AdmissionResult, ArtifactIdentity, ExecutionImageStatus, ExecutionReceipt, ExecutionTerminal,
    ResourceReceipt, RECEIPT_SCHEMA_VERSION,
};

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
    pub query_fingerprint: u64,
    pub occurrence: u64,
    pub cache_hit: bool,
    pub artifact_identity: Option<ArtifactIdentity>,
    pub compile_work: Option<CompileWork>,
}

/// Scalar work ledger read after the client timer. Collection is not free:
/// the clocks/counter copies still execute inside the original SELECT.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CompileWork {
    pub compiler_elapsed_us: u64,
    pub optimizer_elapsed_us: u64,
    pub rule_elapsed_us: u64,
    pub child_combination_cost_synthesis_count: u64,
}

pub fn compile_work_evidence_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("PARO_COMPILE_WORK_EVIDENCE")
        .is_ok_and(|value| value == "1"))
}

#[derive(Debug, Default)]
pub struct SessionDiagnostics {
    optimizer: RwLock<Vec<OptimizerDiagnostic>>,
    statement_trace: RwLock<Option<StatementTraceSnapshot>>,
    statement_cache: RwLock<Vec<StatementCacheDecision>>,
    execution_work: RwLock<Vec<ExecutionWorkRecord>>,
    execution_receipts: RwLock<Vec<ExecutionReceipt>>,
    execution_sequence: std::sync::atomic::AtomicU64,
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
}

impl ExecutionReceiptHandle {
    pub fn execution_id(&self) -> u64 {
        self.lease.execution_id
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
        self.lease
            .diagnostics
            .mark_execution_image_ready(self.lease.execution_id);
    }

    /// Preserve an admission failure without turning it into an execution
    /// terminal.  An infeasible or failed admission is deliberately
    /// `NotExecuted`; the original bounded error still belongs on the receipt.
    pub fn record_error(&self, error: impl Into<String>) {
        self.lease
            .diagnostics
            .record_execution_receipt_error(self.lease.execution_id, error.into());
    }

    fn finish(&self, terminal: ExecutionTerminal, error: Option<String>) {
        self.lease
            .diagnostics
            .finish_execution_receipt(self.lease.execution_id, terminal, error);
    }
}

impl Drop for ExecutionReceiptLease {
    fn drop(&mut self) {
        self.diagnostics.finish_execution_receipt(
            self.execution_id,
            ExecutionTerminal::Dropped,
            None,
        );
    }
}

impl SessionDiagnostics {
    pub fn publish_execution_work(&self, query_fingerprint: u64, image_id: u64, snapshot: paro_common::cold_work::Snapshot) {
        let execution_id = self.execution_sequence.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut records = self.execution_work.write().unwrap();
        records.push(ExecutionWorkRecord { query_fingerprint, execution_id, image_id, snapshot });
        if records.len() > 64 { records.remove(0); }
    }

    pub fn begin_execution_receipt(
        self: &Arc<Self>,
        start: ExecutionReceiptStart,
    ) -> ExecutionReceiptHandle {
        let execution_id = self.execution_sequence.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let receipt = ExecutionReceipt {
            schema_version: RECEIPT_SCHEMA_VERSION,
            execution_id,
            artifact_identity: start.artifact_identity,
            expected_class: start.expected_class,
            actual_class: start.actual_class,
            actual_fingerprint: start.actual_fingerprint,
            resources: start.resources,
            admission: start.admission,
            fallback: start.fallback,
            image: ExecutionImageStatus::NotReady,
            terminal: if start.admission == AdmissionResult::Selected {
                ExecutionTerminal::Running
            } else {
                ExecutionTerminal::NotExecuted
            },
            terminal_error: None,
        };
        let mut receipts = self.execution_receipts.write().unwrap();
        receipts.push(receipt);
        if receipts.len() > 64 {
            receipts.remove(0);
        }
        ExecutionReceiptHandle {
            lease: Arc::new(ExecutionReceiptLease {
                diagnostics: Arc::clone(self),
                execution_id,
            }),
        }
    }

    fn mark_execution_image_ready(&self, execution_id: u64) {
        let mut receipts = self.execution_receipts.write().unwrap();
        let Some(receipt) = receipts.iter_mut().find(|receipt| receipt.execution_id == execution_id) else {
            return;
        };
        if receipt.admission == AdmissionResult::Selected
            && receipt.terminal == ExecutionTerminal::Running
        {
            receipt.image = ExecutionImageStatus::Ready;
        }
    }

    pub fn finish_execution_receipt(
        &self,
        execution_id: u64,
        terminal: ExecutionTerminal,
        error: Option<String>,
    ) {
        let mut receipts = self.execution_receipts.write().unwrap();
        let Some(receipt) = receipts.iter_mut().find(|receipt| receipt.execution_id == execution_id) else {
            return;
        };
        if receipt.terminal != ExecutionTerminal::Running {
            return;
        }
        receipt.terminal = terminal;
        receipt.terminal_error = error.map(|value| value.chars().take(256).collect());
    }

    fn record_execution_receipt_error(&self, execution_id: u64, error: String) {
        let mut receipts = self.execution_receipts.write().unwrap();
        let Some(receipt) = receipts.iter_mut().find(|receipt| receipt.execution_id == execution_id) else {
            return;
        };
        if receipt.terminal == ExecutionTerminal::NotExecuted {
            receipt.terminal_error = Some(error.chars().take(256).collect());
        }
    }

    pub fn execution_receipts_snapshot(&self) -> Vec<ExecutionReceipt> {
        self.execution_receipts.read().unwrap().clone()
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

    pub fn publish_statement_cache_decision(&self, query_fingerprint: u64, cache_hit: bool) -> u64 {
        const MAX_DECISIONS: usize = 256;
        let mut decisions = self.statement_cache.write().unwrap();
        let occurrence = decisions
            .iter()
            .filter(|decision| decision.query_fingerprint == query_fingerprint)
            .count() as u64;
        decisions.push(StatementCacheDecision {
            query_fingerprint,
            occurrence,
            cache_hit,
            artifact_identity: None,
            compile_work: None,
        });
        if decisions.len() > MAX_DECISIONS {
            decisions.remove(0);
        }
        occurrence
    }

    pub fn publish_statement_artifact(
        &self,
        query: u64,
        occurrence: u64,
        artifact_identity: ArtifactIdentity,
    ) {
        let mut decisions = self.statement_cache.write().unwrap();
        if let Some(decision) = decisions.iter_mut().find(|decision|
            decision.query_fingerprint == query && decision.occurrence == occurrence
        ) {
            decision.artifact_identity = Some(artifact_identity);
        }
    }

    pub fn publish_compile_work(&self, query: u64, occurrence: u64, work: CompileWork) {
        let mut decisions = self.statement_cache.write().unwrap();
        if let Some(decision) = decisions.iter_mut().find(|decision|
            decision.query_fingerprint == query && decision.occurrence == occurrence
            && !decision.cache_hit && decision.compile_work.is_none()) {
            decision.compile_work = Some(work);
        }
    }

    pub fn statement_cache_snapshot(&self) -> Vec<StatementCacheDecision> {
        self.statement_cache.read().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_work_is_exact_occurrence_not_the_latest_query_or_cached_cost() {
        let diagnostics = SessionDiagnostics::default();
        let first = diagnostics.publish_statement_cache_decision(11, false);
        let second = diagnostics.publish_statement_cache_decision(11, true);
        diagnostics.publish_statement_cache_decision(22, false);
        let work = CompileWork { compiler_elapsed_us: 20, optimizer_elapsed_us: 10,
            rule_elapsed_us: 3, child_combination_cost_synthesis_count: 7 };
        diagnostics.publish_compile_work(11, first, work);
        diagnostics.publish_compile_work(11, first, CompileWork::default());
        diagnostics.publish_compile_work(11, second, work);
        diagnostics.publish_compile_work(33, first, work);
        let rows = diagnostics.statement_cache_snapshot();
        assert_eq!(rows[0].compile_work, Some(work));
        assert_eq!(rows[1].compile_work, None);
        assert_eq!(rows[2].compile_work, None);
    }

    #[test]
    fn execution_receipt_only_changes_at_a_real_terminal() {
        let diagnostics = Arc::new(SessionDiagnostics::default());
        let identity = ArtifactIdentity {
            schema_version: 1,
            artifact: [1, 2],
            structure: [3, 4],
            dependencies: [5, 6],
        };
        let handle = diagnostics.begin_execution_receipt(ExecutionReceiptStart {
            artifact_identity: identity,
            expected_class: Some(2),
            actual_class: Some(2),
            actual_fingerprint: Some([7, 8]),
            resources: None,
            admission: AdmissionResult::Selected,
            fallback: None,
        });
        assert_eq!(diagnostics.execution_receipts_snapshot()[0].terminal, ExecutionTerminal::Running);
        assert_eq!(diagnostics.execution_receipts_snapshot()[0].image, ExecutionImageStatus::NotReady);
        handle.image_ready();
        assert_eq!(diagnostics.execution_receipts_snapshot()[0].image, ExecutionImageStatus::Ready);
        handle.complete();
        assert_eq!(diagnostics.execution_receipts_snapshot()[0].terminal, ExecutionTerminal::Completed);
    }

    #[test]
    fn dropping_one_receipt_clone_does_not_close_running_execution() {
        let diagnostics = Arc::new(SessionDiagnostics::default());
        let identity = ArtifactIdentity {
            schema_version: 1,
            artifact: [11, 12],
            structure: [13, 14],
            dependencies: [15, 16],
        };
        let handle = diagnostics.begin_execution_receipt(ExecutionReceiptStart {
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
