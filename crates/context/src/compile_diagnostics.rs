// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bounded, request-owned compiler observations. No planner objects or event log.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub const SCHEMA_VERSION: u32 = 2;
pub const ENCODED_LIMIT: usize = 200_000;
pub const RETAINED_LIMIT: usize = 1 << 20;
pub const PROCESS_LIMIT: usize = 64 << 20;
pub const MAX_CAPTURES: usize = 8;
pub const MAX_RULES: usize = 64;
pub const MAX_VARIANTS: usize = 16;
pub const MAX_DETAIL_EVENTS: usize = 8_192;
pub const RECEIPT_SCHEMA_VERSION: u32 = 1;
// Includes fixed recorder, encoder workspace, vector and protocol copy headroom.
const RESERVATION: usize = 2 << 20;
static ACTIVE: AtomicUsize = AtomicUsize::new(0);
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Observation<T> {
    Observed(T),
    NotExecuted,
    NotApplicable,
    Uncovered(UncoveredReason),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum UncoveredReason {
    NotInstrumented,
    FutureBoundary,
    Capacity,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SearchStop {
    Complete,
    Incomplete,
    Deadline,
    BudgetLimited,
    RuleFailure,
    QualityPolicySatisfied,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleSummary {
    pub id: u32,
    pub binding_calls: u64,
    pub binding_ns: u64,
    pub attempts: u64,
    pub inserted: u64,
    /// A different projection of optimizer time; never add to phases.
    pub elapsed_ns: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VariantSummary {
    pub ordinal: u16,
    /// Two exact words; never a JSON floating-point identity.
    pub physical_fingerprint: [u64; 2],
    pub admissible_classes: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CacheObservation {
    ForcedCompile,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MeasurementMode {
    Diagnostic,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CaptureLevel {
    Summary,
    Detail,
}

/// Stable machine kinds for the bounded Detail stream.  The values are part
/// of the Detail schema; the payload fields remain fixed-width references into
/// the real Memo/TaskRegistry snapshot rather than copied planner objects.
pub mod detail_kind {
    pub const RULE: u16 = 1;
    pub const CANDIDATE: u16 = 2;
    pub const TASK: u16 = 3;
    pub const CANDIDATE_CHILD: u16 = 4;
    pub const QUALITY: u16 = 5;
    pub const FACT: u16 = 6;
    pub const GRANT: u16 = 7;
    pub const SEARCH: u16 = 8;
    /// A same-Memo publication edge before the output is consumed as a
    /// candidate. Consumers can distinguish production from later selection
    /// without replaying rules.
    pub const PROPOSAL: u16 = 9;
    pub const MAX: u16 = PROPOSAL;
}

/// A compact event copied from the real optimizer lifecycle.  IDs are opaque
/// to the renderer; the Memo/TaskRegistry remain the semantic authority. The
/// fixed-width shape is deliberate: admitting an event never allocates a
/// planner object or retains a subtree.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DetailEvent {
    pub sequence: u64,
    pub kind: u16,
    pub phase: u16,
    pub primary: u64,
    pub secondary: u64,
    pub tertiary: u64,
    /// An additional opaque reference.  Its meaning is fixed by `kind` and
    /// is never a pointer or a retained planner object.
    pub reference: u64,
    pub cause: u64,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimerBoundary {
    ParsedAstCompilerEntryToReturnV1,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ArtifactStatus {
    NotReady,
    CompiledArtifactReady,
}

/// Cross-process identity for an immutable compiled artifact.  The digest is
/// deliberately separate from the structure and dependency digests so a
/// consumer can reject a receipt whose plan shape or dependency contract was
/// produced by another schema revision.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct ArtifactIdentity {
    pub schema_version: u32,
    pub artifact: [u64; 2],
    pub structure: [u64; 2],
    pub dependencies: [u64; 2],
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum AdmissionResult {
    Selected,
    Infeasible,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum ExecutionTerminal {
    NotExecuted,
    Running,
    Completed,
    Failed,
    Cancelled,
    Dropped,
}

/// Admission selects a portfolio member before the executable image is
/// lowered.  Keep that lifecycle edge explicit instead of treating a
/// selected portfolio entry as an executable image.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum ExecutionImageStatus {
    NotReady,
    Ready,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum ResourceReservationStatus {
    NotRequired,
    Committed,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum LoweringStatus {
    NotStarted,
    Ready,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum AdmissionFallback {
    LowerResourceClass,
    ExternalCapacity,
    DependencyChanged,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum MemoryCompletionReceipt {
    Guaranteed,
    RuntimeCappedKnown { uncapped_memory_bytes: u64 },
    RuntimeCappedUnbounded,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceReceipt {
    pub class: u32,
    pub minimum_memory_bytes: u64,
    pub working_set_memory_bytes: u64,
    pub memory_ceiling_bytes: u64,
    pub memory_completion: MemoryCompletionReceipt,
    pub max_parallel_tasks: u16,
    pub external_worker_slots: u16,
}

/// The only record that may claim an actual portfolio choice.  It is created
/// at admission, not while a portfolio is compiled or rendered.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionReceipt {
    pub schema_version: u32,
    pub execution_id: u64,
    pub statement_decision_id: Option<u64>,
    pub artifact_identity: ArtifactIdentity,
    pub expected_class: Option<u32>,
    pub actual_class: Option<u32>,
    pub actual_fingerprint: Option<[u64; 2]>,
    pub resources: Option<ResourceReceipt>,
    pub admission: AdmissionResult,
    pub fallback: Option<AdmissionFallback>,
    pub reservation: ResourceReservationStatus,
    pub lowering: LoweringStatus,
    pub lowering_error: Option<String>,
    pub image: ExecutionImageStatus,
    pub terminal: ExecutionTerminal,
    pub terminal_error: Option<String>,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CompileOutcome {
    Incomplete,
    Success,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum UnavailableReason {
    Capacity,
    ProcessCapacity,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum UnavailableStatus {
    Unavailable,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnavailableDocument {
    pub schema_version: u32,
    pub diagnostic: UnavailableStatus,
    pub reason: UnavailableReason,
    pub target_compile: CompileOutcome,
    pub target_execution: Observation<u64>,
}

/// Both normal output and capacity refusal use this single wire document.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CompileDocument {
    Summary(Box<CompileRecord>),
    Unavailable(UnavailableDocument),
}

impl CompileDocument {
    pub fn unavailable(reason: UnavailableReason) -> Self {
        Self::Unavailable(UnavailableDocument {
            schema_version: SCHEMA_VERSION,
            diagnostic: UnavailableStatus::Unavailable,
            reason,
            target_compile: CompileOutcome::Success,
            target_execution: Observation::NotExecuted,
        })
    }
    pub fn summary(&self) -> Option<&CompileRecord> {
        match self {
            Self::Summary(r) => Some(r),
            Self::Unavailable(_) => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompileRecord {
    #[serde(flatten)]
    fields: CompileFields,
    pub rules: Vec<RuleSummary>,
    pub omitted_rules: u64,
    pub variants: Vec<VariantSummary>,
    pub omitted_variants: u64,
    pub retained_limit: usize,
    pub encoded_limit: usize,
    pub process_limit: usize,
    pub process_reservation: usize,
    pub capture_level: CaptureLevel,
    pub detail: Vec<DetailEvent>,
    pub omitted_detail: u64,
    pub detail_limit: usize,
    pub execution_receipt: Option<ExecutionReceipt>,
}

impl std::ops::Deref for CompileRecord {
    type Target = CompileFields;
    fn deref(&self) -> &CompileFields {
        &self.fields
    }
}

/// Fixed-size observations only. Producers cannot replace collections or the
/// capacity profile through this interface.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct CompileFields {
    pub schema_version: u32,
    pub invocation: u64,
    pub process: u32,
    pub cache: CacheObservation,
    pub input_fingerprint: Observation<u64>,
    pub identity_encoding: u32,
    pub source_build: Observation<u64>,
    pub catalog_facts: Observation<u64>,
    pub output_identity: Observation<u64>,
    pub artifact_identity: Observation<ArtifactIdentity>,
    pub planning_settings: Observation<u64>,
    pub available_memory_bytes: Observation<u64>,
    pub available_parallel_tasks: Observation<u16>,
    pub measurement_mode: MeasurementMode,
    pub timer_boundary: TimerBoundary,
    pub parse: Observation<u64>,
    pub bind_ns: Observation<u64>,
    pub optimizer_ns: Observation<u64>,
    pub verify_ns: Observation<u64>,
    pub finish_ns: Observation<u64>,
    pub compiler_ns: Observation<u64>,
    pub compiler_other_ns: Observation<u64>,
    pub safety_verified: Observation<bool>,
    pub search_stop: Observation<SearchStop>,
    pub search_complete: Observation<bool>,
    pub quality_policy_satisfied: Observation<bool>,
    pub budget_limited: Observation<bool>,
    pub obligations: Observation<u64>,
    pub groups: Observation<u64>,
    pub logical_expressions: Observation<u64>,
    pub physical_expressions: Observation<u64>,
    pub output_columns: Observation<usize>,
    pub artifact: ArtifactStatus,
    pub expected_class: Observation<u32>,
    pub variant_count: Observation<usize>,
    pub selected_fingerprint: Observation<[u64; 2]>,
    pub admission: Observation<u64>,
    pub execution: Observation<u64>,
    pub response_terminal: Observation<u64>,
    pub outcome: CompileOutcome,
}

#[derive(Debug)]
pub struct CompileCapture {
    record: Mutex<CompileRecord>,
    sealed: AtomicBool,
    level: CaptureLevel,
}

/// Immutable transport view. Its lease keeps the reservation alive after the
/// request returns, including while a collector retains the output vector.
#[derive(Debug)]
pub struct SealedCompileCapture(Arc<CompileCapture>);

impl SealedCompileCapture {
    pub fn read<T>(&self, f: impl FnOnce(&CompileRecord) -> T) -> T {
        self.0.read(f)
    }
}

impl CompileCapture {
    /// Reserve before allocating. Capacity refusal is diagnostic, not a search error.
    pub fn try_start() -> Option<Arc<Self>> {
        Self::try_start_with_level(CaptureLevel::Summary)
    }

    pub fn try_start_with_level(level: CaptureLevel) -> Option<Arc<Self>> {
        ACTIVE
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < MAX_CAPTURES && (n + 1) * RESERVATION <= PROCESS_LIMIT).then_some(n + 1)
            })
            .ok()?;
        let invocation = match NEXT_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        {
            Ok(id) => id,
            Err(_) => {
                ACTIVE.fetch_sub(1, Ordering::AcqRel);
                return None;
            }
        };
        let unknown = Observation::Uncovered(UncoveredReason::NotInstrumented);
        let unknown_artifact = Observation::Uncovered(UncoveredReason::NotInstrumented);
        Some(Arc::new(Self {
            sealed: AtomicBool::new(false),
            record: Mutex::new(CompileRecord {
                fields: CompileFields {
                    schema_version: SCHEMA_VERSION,
                    invocation,
                    process: std::process::id(),
                    cache: CacheObservation::ForcedCompile,
                    measurement_mode: MeasurementMode::Diagnostic,
                    input_fingerprint: unknown,
                    output_identity: unknown,
                    artifact_identity: unknown_artifact,
                    planning_settings: unknown,
                    identity_encoding: 1,
                    source_build: unknown,
                    catalog_facts: unknown,
                    available_memory_bytes: unknown,
                    available_parallel_tasks: Observation::Uncovered(
                        UncoveredReason::NotInstrumented,
                    ),
                    timer_boundary: TimerBoundary::ParsedAstCompilerEntryToReturnV1,
                    parse: unknown,
                    bind_ns: unknown,
                    optimizer_ns: unknown,
                    verify_ns: unknown,
                    finish_ns: unknown,
                    compiler_ns: unknown,
                    compiler_other_ns: unknown,
                    safety_verified: Observation::Uncovered(UncoveredReason::NotInstrumented),
                    search_stop: Observation::Uncovered(UncoveredReason::NotInstrumented),
                    search_complete: Observation::Uncovered(UncoveredReason::NotInstrumented),
                    quality_policy_satisfied: Observation::Uncovered(
                        UncoveredReason::NotInstrumented,
                    ),
                    budget_limited: Observation::Uncovered(UncoveredReason::NotInstrumented),
                    obligations: unknown,
                    groups: unknown,
                    logical_expressions: unknown,
                    physical_expressions: unknown,
                    output_columns: Observation::Uncovered(UncoveredReason::NotInstrumented),
                    artifact: ArtifactStatus::NotReady,
                    expected_class: Observation::Uncovered(UncoveredReason::NotInstrumented),
                    variant_count: Observation::Uncovered(UncoveredReason::NotInstrumented),
                    selected_fingerprint: Observation::Uncovered(UncoveredReason::NotInstrumented),
                    admission: Observation::NotExecuted,
                    execution: Observation::NotExecuted,
                    response_terminal: Observation::Uncovered(UncoveredReason::FutureBoundary),
                    outcome: CompileOutcome::Incomplete,
                },
                rules: Vec::new(),
                omitted_rules: 0,
                variants: Vec::new(),
                omitted_variants: 0,
                retained_limit: RETAINED_LIMIT,
                encoded_limit: ENCODED_LIMIT,
                process_limit: PROCESS_LIMIT,
                process_reservation: RESERVATION,
                capture_level: level,
                detail: Vec::new(),
                omitted_detail: 0,
                detail_limit: MAX_DETAIL_EVENTS,
                execution_receipt: None,
            }),
            level,
        }))
    }

    pub fn update(&self, f: impl FnOnce(&mut CompileFields)) {
        let mut record = self.record.lock().unwrap_or_else(|e| e.into_inner());
        if !self.sealed.load(Ordering::Acquire) {
            f(&mut record.fields);
        }
    }

    /// After compilation no producer may alter the transported snapshot.
    pub fn seal(self: &Arc<Self>) -> Arc<SealedCompileCapture> {
        let _record = self.record.lock().unwrap_or_else(|e| e.into_inner());
        self.sealed.store(true, Ordering::Release);
        Arc::new(SealedCompileCapture(self.clone()))
    }

    pub fn read<T>(&self, f: impl FnOnce(&CompileRecord) -> T) -> T {
        f(&self.record.lock().unwrap_or_else(|e| e.into_inner()))
    }

    pub fn rule(&self, rule: RuleSummary) {
        let mut r = self.record.lock().unwrap_or_else(|e| e.into_inner());
        if !self.sealed.load(Ordering::Acquire) {
            if r.rules.len() < MAX_RULES {
                let at = r.rules.partition_point(|existing| existing.id < rule.id);
                r.rules.insert(at, rule);
            } else {
                r.omitted_rules = r.omitted_rules.saturating_add(1);
                // Retain the same bounded subset regardless of map iteration order.
                if r.rules.last().is_some_and(|last| rule.id < last.id) {
                    r.rules.pop();
                    let at = r.rules.partition_point(|existing| existing.id < rule.id);
                    r.rules.insert(at, rule);
                }
            }
        }
    }

    pub fn level(&self) -> CaptureLevel {
        self.level
    }

    pub fn detail(&self, event: DetailEvent) {
        if self.level != CaptureLevel::Detail {
            return;
        }
        let mut record = self.record.lock().unwrap_or_else(|e| e.into_inner());
        if self.sealed.load(Ordering::Acquire) {
            return;
        }
        if record.detail.len() < MAX_DETAIL_EVENTS {
            record.detail.push(event);
        } else {
            record.omitted_detail = record.omitted_detail.saturating_add(1);
        }
    }

    /// Account for lifecycle records that the real optimizer intentionally
    /// dropped before this bounded capture was copied.  This is separate from
    /// `detail()`'s capture-capacity counter so a consumer can distinguish
    /// source retention loss from transport capacity loss without retaining
    /// an unbounded event log.
    pub fn detail_omitted(&self, count: u64) {
        if self.level != CaptureLevel::Detail || count == 0 {
            return;
        }
        let mut record = self.record.lock().unwrap_or_else(|e| e.into_inner());
        if !self.sealed.load(Ordering::Acquire) {
            record.omitted_detail = record.omitted_detail.saturating_add(count);
        }
    }

    pub fn variants(&self, count: usize, variants: impl Iterator<Item = VariantSummary>) {
        let mut r = self.record.lock().unwrap_or_else(|e| e.into_inner());
        if self.sealed.load(Ordering::Acquire) {
            return;
        }
        r.fields.variant_count = Observation::Observed(count);
        r.variants.clear();
        r.variants.extend(variants.take(MAX_VARIANTS));
        r.omitted_variants = count.saturating_sub(r.variants.len()) as u64;
    }
}

impl Drop for CompileCapture {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn capture_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn capacity_is_admitted_before_growth_and_retained_until_last_owner() {
        let _lock = capture_test_lock();
        let captures: Vec<_> = (0..MAX_CAPTURES)
            .map(|_| CompileCapture::try_start().unwrap())
            .collect();
        assert!(CompileCapture::try_start().is_none());
        let retained = captures[0].clone();
        for id in 0..809_720 {
            retained.rule(RuleSummary {
                id,
                binding_calls: 0,
                binding_ns: 0,
                attempts: 1,
                inserted: 0,
                elapsed_ns: 0,
            });
        }
        retained.read(|r| {
            assert_eq!(r.rules.len(), MAX_RULES);
            assert_eq!(r.omitted_rules, 809_720 - MAX_RULES as u64);
            assert!(
                std::mem::size_of::<CompileRecord>()
                    + r.rules.capacity() * std::mem::size_of::<RuleSummary>()
                    < RETAINED_LIMIT - 4096
            );
            assert_eq!(r.execution, Observation::NotExecuted);
        });
        retained.variants(
            70_000,
            (0..70_000).map(|ordinal| VariantSummary {
                ordinal: ordinal as u16,
                physical_fingerprint: [0, 0],
                admissible_classes: 1,
            }),
        );
        retained.read(|r| {
            assert_eq!(r.variants.len(), MAX_VARIANTS);
            assert_eq!(r.omitted_variants, 70_000 - MAX_VARIANTS as u64);
            assert!(
                std::mem::size_of::<CompileRecord>()
                    + r.rules.capacity() * std::mem::size_of::<RuleSummary>()
                    + r.variants.capacity() * std::mem::size_of::<VariantSummary>()
                    < RETAINED_LIMIT
            );
        });
        let sealed = retained.seal();
        retained.update(|r| r.execution = Observation::Observed(99));
        assert_eq!(retained.read(|r| r.execution), Observation::NotExecuted);
        drop(captures);
        assert_eq!(ACTIVE.load(Ordering::Acquire), 1);
        drop(retained);
        assert_eq!(ACTIVE.load(Ordering::Acquire), 1);
        drop(sealed);
        assert_eq!(ACTIVE.load(Ordering::Acquire), 0);
    }

    #[test]
    fn detail_is_opt_in_bounded_and_summary_has_no_event_buffer() {
        let _lock = capture_test_lock();
        let summary = CompileCapture::try_start().unwrap();
        summary.detail(DetailEvent {
            sequence: 0,
            kind: detail_kind::RULE,
            phase: 0,
            primary: 1,
            secondary: 2,
            tertiary: 3,
            reference: 4,
            cause: 5,
        });
        summary.read(|record| {
            assert_eq!(record.capture_level, CaptureLevel::Summary);
            assert!(record.detail.is_empty());
            assert_eq!(record.omitted_detail, 0);
        });

        let detail = CompileCapture::try_start_with_level(CaptureLevel::Detail).unwrap();
        const DETAIL_ATTEMPTS: usize = 809_720;
        for sequence in 0..DETAIL_ATTEMPTS {
            detail.detail(DetailEvent {
                sequence: sequence as u64,
                kind: detail_kind::TASK,
                phase: 0,
                primary: sequence as u64,
                secondary: 0,
                tertiary: 0,
                reference: 0,
                cause: 0,
            });
        }
        detail.read(|record| {
            assert_eq!(record.capture_level, CaptureLevel::Detail);
            assert_eq!(record.detail.len(), MAX_DETAIL_EVENTS);
            assert_eq!(
                record.omitted_detail,
                (DETAIL_ATTEMPTS - MAX_DETAIL_EVENTS) as u64
            );
            assert_eq!(record.detail.first().unwrap().sequence, 0);
        });
    }
}
