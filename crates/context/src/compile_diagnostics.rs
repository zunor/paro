// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bounded, request-owned compiler observations. No planner objects or event log.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// The only current Compile Evidence wire/schema version. Older documents are
/// historical artifacts and are intentionally rejected by every current
/// reader; there is no compatibility decoder in the producer path.
pub const SCHEMA_VERSION: u32 = 3;
pub const ENCODED_LIMIT: usize = 200_000;
pub const RETAINED_LIMIT: usize = 1 << 20;
pub const PROCESS_LIMIT: usize = 64 << 20;
pub const MAX_CAPTURES: usize = 8;
pub const MAX_RULES: usize = 64;
pub const MAX_SEARCH_COUNTERS: usize = 256;
pub const MAX_VARIANTS: usize = 16;
pub const MAX_DETAIL_EVENTS: usize = 2_048;
pub const RECEIPT_SCHEMA_VERSION: u32 = SCHEMA_VERSION;
pub const IDENTITY_SCHEMA_VERSION: u32 = SCHEMA_VERSION;
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

/// A bounded, typed search counter exported by the compiler producer.  The
/// name is an immutable producer-owned key from the planner's counter set;
/// it is not reconstructed by a consumer from a diagnostic side channel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SearchCounter {
    pub name: String,
    pub value: u64,
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

/// Typed, scoped references used by Detail.  Keeping these as distinct Rust
/// types prevents a logical expression, physical expression, group, and
/// fingerprint from silently occupying the same integer slot in a report.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct MemoGroupRef(pub u64);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct LogicalExprRef(pub u64);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct PhysicalExprRef(pub u64);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct CandidateRef(pub u64);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct RuleRef(pub u32);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct BindingRef(pub [u64; 2]);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct FingerprintRef(pub [u64; 2]);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct GoalRef {
    pub required: u64,
    pub grant: u64,
    pub context: u64,
}

/// A bounded lifecycle record. `source_sequence` is local to the producer
/// stream named by the variant; it is never the order in which the renderer
/// happened to visit categories. `event_time_us` is the actual monotonic
/// search timestamp when the source supplied one. Aggregate snapshots keep a
/// zero timestamp and are explicitly typed as such.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data", deny_unknown_fields)]
pub enum DetailEvent {
    RuleSummary {
        source_sequence: u64,
        rule: RuleRef,
        binding_calls: u64,
        binding_ns: u64,
        attempts: u64,
        inserted: u64,
        elapsed_ns: u64,
    },
    Proposal {
        source_sequence: u64,
        event_time_us: u64,
        stage: u8,
        group: MemoGroupRef,
        source: Option<LogicalExprRef>,
        logical: Option<LogicalExprRef>,
        physical: Option<PhysicalExprRef>,
        binding: Option<BindingRef>,
        rule: Option<RuleRef>,
    },
    Candidate {
        source_sequence: u64,
        event_time_us: u64,
        stage: u8,
        group: MemoGroupRef,
        goal: Option<GoalRef>,
        candidate: Option<CandidateRef>,
        source: Option<LogicalExprRef>,
        source_child: Option<LogicalExprRef>,
        logical: Option<LogicalExprRef>,
        physical: Option<PhysicalExprRef>,
        recipe: Option<FingerprintRef>,
        rule: Option<RuleRef>,
        expected_cost_bits: Option<u64>,
        upper_cost_bits: Option<u64>,
    },
    CandidateChild {
        source_sequence: u64,
        parent_event_id: u64,
        ordinal: u32,
        event_time_us: u64,
        stage: u8,
        candidate: Option<CandidateRef>,
        child_group: MemoGroupRef,
        child_candidate: CandidateRef,
        goal: GoalRef,
    },
    Fact {
        source_sequence: u64,
        parent_event_id: u64,
        ordinal: u32,
        event_time_us: u64,
        candidate: Option<CandidateRef>,
        group: MemoGroupRef,
        logical_fact: FingerprintRef,
        statistics_snapshot: FingerprintRef,
    },
    Task {
        source_sequence: u64,
        event_time_us: u64,
        group: MemoGroupRef,
        expression: LogicalExprRef,
        rule: RuleRef,
        first_binding: Option<BindingRef>,
        first_run_us: Option<u64>,
        first_published_us: Option<u64>,
        match_count: u64,
        applicable_count: u64,
        published_count: u64,
        no_match_count: u64,
        no_output_count: u64,
        budget_rejected_count: u64,
    },
    Grant {
        source_sequence: u64,
        class: u32,
        physical_fingerprint: FingerprintRef,
        expected_cost_bits: u64,
        max_parallel_tasks: u16,
    },
    /// A real quality-policy evaluation snapshot.  This is emitted only from
    /// the planner's quality decision point; a candidate carrying a goal is
    /// deliberately not treated as a quality decision.
    Quality {
        source_sequence: u64,
        event_time_us: u64,
        candidate: Option<CandidateRef>,
        goal: Option<GoalRef>,
        completed: u64,
        not_applicable: u64,
        missing_evidence: u64,
        suspended: u64,
        missing_facts: u64,
        missing_bundles: Box<[u32]>,
        missing_fact_kinds: Box<[u64]>,
        policy_satisfied: bool,
    },
    Search {
        source_sequence: u64,
        groups: u64,
        logical_expressions: u64,
        physical_expressions: u64,
        obligations: u64,
        stop: SearchStop,
    },
}

impl DetailEvent {
    pub fn source_sequence(&self) -> u64 {
        match self {
            Self::RuleSummary {
                source_sequence, ..
            }
            | Self::Proposal {
                source_sequence, ..
            }
            | Self::Candidate {
                source_sequence, ..
            }
            | Self::CandidateChild {
                source_sequence, ..
            }
            | Self::Fact {
                source_sequence, ..
            }
            | Self::Task {
                source_sequence, ..
            }
            | Self::Grant {
                source_sequence, ..
            }
            | Self::Quality {
                source_sequence, ..
            }
            | Self::Search {
                source_sequence, ..
            } => *source_sequence,
        }
    }

    pub fn stream_name(&self) -> &'static str {
        match self {
            Self::RuleSummary { .. } => "rule-summary",
            Self::Proposal { .. } => "proposal-lifecycle",
            Self::Candidate { .. } => "candidate-lifecycle",
            Self::CandidateChild { .. } => "candidate-child-lifecycle",
            Self::Fact { .. } => "fact-lifecycle",
            Self::Task { .. } => "task-lifecycle",
            Self::Grant { .. } => "grant-snapshot",
            Self::Quality { .. } => "quality-evaluation",
            Self::Search { .. } => "search-snapshot",
        }
    }
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
    pub artifact: CompiledArtifactId,
    pub structure: PlanStructureId,
    pub dependencies: [u64; 2],
}

/// Stable structural identity of a compiled physical plan. This is a typed
/// identity boundary; it is not a display fingerprint and never contains an
/// arena index, pointer, wall-clock value, cost or search-order coordinate.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct PlanStructureId(pub [u64; 2]);

/// Identity of the immutable compiled artifact, including its structure and
/// dependency contract. The value is separate from the execution/admission
/// identity of a later invocation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct CompiledArtifactId(pub [u64; 2]);

/// Session-monotonic identity of one actual admission/execution receipt.
/// This is deliberately distinct from cache occurrences and from the
/// artifact identity; its transparent wire representation remains a u64.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct ExecutionReceiptId(pub u64);

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
    pub execution_id: ExecutionReceiptId,
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
    pub search_counters: Vec<SearchCounter>,
    pub omitted_search_counters: u64,
    pub variants: Vec<VariantSummary>,
    pub omitted_variants: u64,
    pub retained_limit: usize,
    pub encoded_limit: usize,
    pub process_limit: usize,
    pub process_reservation: usize,
    pub capture_level: CaptureLevel,
    pub detail: Vec<DetailEvent>,
    /// Lifecycle records the optimizer could not retain before this capture
    /// was copied.  This is not the same failure as capture capacity.
    pub omitted_source_detail: u64,
    /// Records rejected by this bounded capture after the producer supplied
    /// them.  Encoding loss is tracked separately by the renderer.
    pub omitted_capture_detail: u64,
    pub omitted_encoding_detail: u64,
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
                search_counters: Vec::new(),
                omitted_search_counters: 0,
                variants: Vec::new(),
                omitted_variants: 0,
                retained_limit: RETAINED_LIMIT,
                encoded_limit: ENCODED_LIMIT,
                process_limit: PROCESS_LIMIT,
                process_reservation: RESERVATION,
                capture_level: level,
                detail: Vec::new(),
                omitted_source_detail: 0,
                omitted_capture_detail: 0,
                omitted_encoding_detail: 0,
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

    /// Replace the bounded search-counter snapshot before sealing.  The
    /// planner supplies an ordered map of static counter names, so sorting and
    /// truncation are deterministic and do not depend on hash/map iteration.
    pub fn search_counters(&self, counters: impl IntoIterator<Item = (&'static str, u64)>) {
        let mut values: Vec<_> = counters
            .into_iter()
            .map(|(name, value)| SearchCounter {
                name: name.to_string(),
                value,
            })
            .collect();
        values.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        let omitted = values.len().saturating_sub(MAX_SEARCH_COUNTERS);
        values.truncate(MAX_SEARCH_COUNTERS);
        let mut record = self.record.lock().unwrap_or_else(|e| e.into_inner());
        if !self.sealed.load(Ordering::Acquire) {
            record.search_counters = values;
            record.omitted_search_counters = omitted as u64;
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
            record.omitted_capture_detail = record.omitted_capture_detail.saturating_add(1);
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
            record.omitted_source_detail = record.omitted_source_detail.saturating_add(count);
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
        summary.detail(DetailEvent::RuleSummary {
            source_sequence: 1,
            rule: RuleRef(1),
            binding_calls: 2,
            binding_ns: 3,
            attempts: 4,
            inserted: 5,
            elapsed_ns: 6,
        });
        summary.read(|record| {
            assert_eq!(record.capture_level, CaptureLevel::Summary);
            assert!(record.detail.is_empty());
            assert_eq!(record.omitted_source_detail, 0);
            assert_eq!(record.omitted_capture_detail, 0);
        });

        let detail = CompileCapture::try_start_with_level(CaptureLevel::Detail).unwrap();
        const DETAIL_ATTEMPTS: usize = 809_720;
        for sequence in 0..DETAIL_ATTEMPTS {
            detail.detail(DetailEvent::Task {
                source_sequence: sequence as u64,
                event_time_us: sequence as u64,
                group: MemoGroupRef(sequence as u64),
                expression: LogicalExprRef(sequence as u64),
                rule: RuleRef(1),
                first_binding: None,
                first_run_us: None,
                first_published_us: None,
                match_count: 0,
                applicable_count: 0,
                published_count: 0,
                no_match_count: 0,
                no_output_count: 0,
                budget_rejected_count: 0,
            });
        }
        detail.read(|record| {
            assert_eq!(record.capture_level, CaptureLevel::Detail);
            assert_eq!(record.detail.len(), MAX_DETAIL_EVENTS);
            assert_eq!(
                record.omitted_capture_detail,
                (DETAIL_ATTEMPTS - MAX_DETAIL_EVENTS) as u64
            );
            assert_eq!(record.detail.first().unwrap().source_sequence(), 0);
        });
    }

    #[test]
    fn search_counter_snapshot_is_sorted_and_bounded() {
        let _lock = capture_test_lock();
        let capture = CompileCapture::try_start().unwrap();
        capture.search_counters((0..MAX_SEARCH_COUNTERS + 2).map(|index| {
            let name = Box::leak(format!("counter_{index:03}").into_boxed_str());
            (name as &'static str, index as u64)
        }));
        capture.read(|record| {
            assert_eq!(record.search_counters.len(), MAX_SEARCH_COUNTERS);
            assert_eq!(record.omitted_search_counters, 2);
            assert_eq!(record.search_counters[0].name, "counter_000");
            assert_eq!(record.search_counters[255].name, "counter_255");
        });
    }
}
