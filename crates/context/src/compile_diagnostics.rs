// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bounded, request-owned compiler observations. No planner objects or event log.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub const SCHEMA_VERSION: u32 = 1;
pub const ENCODED_LIMIT: usize = 200_000;
pub const RETAINED_LIMIT: usize = 1 << 20;
pub const PROCESS_LIMIT: usize = 64 << 20;
pub const MAX_CAPTURES: usize = 8;
pub const MAX_RULES: usize = 64;
pub const MAX_VARIANTS: usize = 16;
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
pub enum TimerBoundary {
    ParsedAstCompilerEntryToReturnV1,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ArtifactStatus {
    NotReady,
    CompiledArtifactReady,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CompileOutcome {
    Incomplete,
    Success,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompileRecord {
    pub schema_version: u32,
    pub invocation: u64,
    pub process: u32,
    pub cache: CacheObservation,
    pub input_fingerprint: Observation<u64>,
    pub identity_encoding: u32,
    pub source_build: Observation<u64>,
    pub catalog_facts: Observation<u64>,
    pub output_identity: Observation<u64>,
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
    pub rules: Vec<RuleSummary>,
    pub omitted_rules: u64,
    pub output_columns: Observation<usize>,
    pub artifact: ArtifactStatus,
    pub expected_class: Observation<u32>,
    pub variant_count: Observation<usize>,
    pub variants: Vec<VariantSummary>,
    pub omitted_variants: u64,
    pub selected_fingerprint: Observation<[u64; 2]>,
    pub admission: Observation<u64>,
    pub execution: Observation<u64>,
    pub response_terminal: Observation<u64>,
    pub retained_limit: usize,
    pub encoded_limit: usize,
    pub process_limit: usize,
    pub process_reservation: usize,
    pub outcome: CompileOutcome,
}

#[derive(Debug)]
pub struct CompileCapture {
    record: Mutex<CompileRecord>,
    sealed: AtomicBool,
}

impl CompileCapture {
    /// Reserve before allocating. Capacity refusal is diagnostic, not a search error.
    pub fn try_start() -> Option<Arc<Self>> {
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
        Some(Arc::new(Self {
            sealed: AtomicBool::new(false),
            record: Mutex::new(CompileRecord {
                schema_version: SCHEMA_VERSION,
                invocation,
                process: std::process::id(),
                cache: CacheObservation::ForcedCompile,
                measurement_mode: MeasurementMode::Diagnostic,
                input_fingerprint: unknown,
                output_identity: unknown,
                planning_settings: unknown,
                identity_encoding: 1,
                source_build: unknown,
                catalog_facts: unknown,
                available_memory_bytes: unknown,
                available_parallel_tasks: Observation::Uncovered(UncoveredReason::NotInstrumented),
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
                quality_policy_satisfied: Observation::Uncovered(UncoveredReason::NotInstrumented),
                budget_limited: Observation::Uncovered(UncoveredReason::NotInstrumented),
                obligations: unknown,
                groups: unknown,
                logical_expressions: unknown,
                physical_expressions: unknown,
                rules: Vec::new(),
                omitted_rules: 0,
                output_columns: Observation::Uncovered(UncoveredReason::NotInstrumented),
                artifact: ArtifactStatus::NotReady,
                expected_class: Observation::Uncovered(UncoveredReason::NotInstrumented),
                variant_count: Observation::Uncovered(UncoveredReason::NotInstrumented),
                variants: Vec::new(),
                omitted_variants: 0,
                selected_fingerprint: Observation::Uncovered(UncoveredReason::NotInstrumented),
                admission: Observation::NotExecuted,
                execution: Observation::NotExecuted,
                response_terminal: Observation::Uncovered(UncoveredReason::FutureBoundary),
                retained_limit: RETAINED_LIMIT,
                encoded_limit: ENCODED_LIMIT,
                process_limit: PROCESS_LIMIT,
                process_reservation: RESERVATION,
                outcome: CompileOutcome::Incomplete,
            }),
        }))
    }

    pub fn update(&self, f: impl FnOnce(&mut CompileRecord)) {
        let mut record = self.record.lock().unwrap_or_else(|e| e.into_inner());
        if !self.sealed.load(Ordering::Acquire) {
            f(&mut record);
        }
    }

    /// After compilation no producer may alter the transported snapshot.
    pub fn seal(&self) {
        let _record = self.record.lock().unwrap_or_else(|e| e.into_inner());
        self.sealed.store(true, Ordering::Release);
    }

    pub fn read<T>(&self, f: impl FnOnce(&CompileRecord) -> T) -> T {
        f(&self.record.lock().unwrap_or_else(|e| e.into_inner()))
    }

    pub fn rule(&self, rule: RuleSummary) {
        self.update(|r| {
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
        });
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
    #[test]
    fn capacity_is_admitted_before_growth_and_retained_until_last_owner() {
        let captures: Vec<_> = (0..MAX_CAPTURES)
            .map(|_| CompileCapture::try_start().unwrap())
            .collect();
        assert!(CompileCapture::try_start().is_none());
        let retained = captures[0].clone();
        for id in 0..809_720 {
            retained.rule(RuleSummary {
                id,
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
        retained.seal();
        retained.update(|r| r.execution = Observation::Observed(99));
        assert_eq!(retained.read(|r| r.execution), Observation::NotExecuted);
        drop(captures);
        assert_eq!(ACTIVE.load(Ordering::Acquire), 1);
        drop(retained);
        assert_eq!(ACTIVE.load(Ordering::Acquire), 0);
    }
}
