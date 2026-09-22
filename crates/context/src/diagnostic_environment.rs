// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable process-start configuration observed by the benchmark harness.

use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticEnvironmentSetting {
    pub name: &'static str,
    pub value: Option<String>,
}

// Keep this list aligned with the benchmark optimizer-evidence allow-list.
// Unset values remain in the snapshot so a report can distinguish an absent
// switch from a launcher that forgot to clear an inherited value.
const OBSERVED_ENVIRONMENT: &[&str] = &[
    "PARO_CERTIFIED_GROUP_PRUNING",
    "PARO_DISABLE_PROTECTED_INCUMBENT",
    "PARO_EXPORT_STRONG_INCUMBENT",
    "PARO_QUALITY_PREFLIGHT",
    "PARO_DIAGNOSTIC_CARDINALITY_AUDIT",
    "PARO_DIAGNOSTIC_COST_PHASE_TIMES",
    "PARO_DIAGNOSTIC_FRONTIER_SNAPSHOT",
    "PARO_DIAGNOSTIC_FRONTIER_WIDTH",
    "PARO_DIAGNOSTIC_NORMALIZE_CTE_DOMAIN",
    "PARO_DIAGNOSTIC_OBLIGATION_ONLY",
    "PARO_DIAGNOSTIC_SEARCH_STOP_MS",
    "PARO_DIAGNOSTIC_SHADOW_STRUCTURAL_QUALITY",
    "PARO_DIAGNOSTIC_SKIP_MEMO_VERIFIERS",
    "PARO_DIAGNOSTIC_STREAM_SEQUENTIAL",
    "PARO_DIAGNOSTIC_STRUCTURAL_QUALITY_EVIDENCE",
    "PARO_DIAGNOSTIC_WORK_PARTITION",
    "PARO_STRONG_INCUMBENT_EXPERIMENT",
    "PARO_STRONG_INCUMBENT_PROVIDE_BOUND",
    "PARO_STRONG_INCUMBENT_INJECT_LOGICAL",
    "PARO_STRONG_INCUMBENT_EXPORT_CANDIDATE",
    "PARO_DIAGNOSTIC_REPLAY_CANDIDATE",
    "PARO_DIAGNOSTIC_REPLAY_GRANT_CLASS",
    "PARO_STATEMENT_CACHE_EVIDENCE",
    "PARO_STATEMENT_TRACE",
    "PARO_STATEMENT_TRACE_SAMPLE",
    "PARO_COMPILE_WORK_EVIDENCE",
    "PARO_COLD_WORK_EVIDENCE",
];

static SNAPSHOT: OnceLock<Vec<DiagnosticEnvironmentSetting>> = OnceLock::new();

pub fn initialize_diagnostic_environment() {
    let _ = snapshot();
}

pub fn snapshot() -> &'static [DiagnosticEnvironmentSetting] {
    SNAPSHOT
        .get_or_init(|| {
            OBSERVED_ENVIRONMENT
                .iter()
                .map(|name| DiagnosticEnvironmentSetting {
                    name,
                    value: std::env::var(name).ok(),
                })
                .collect()
        })
        .as_slice()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn observation_has_unique_names_and_one_immutable_process_snapshot() {
        let names: BTreeSet<_> = OBSERVED_ENVIRONMENT.iter().collect();
        assert_eq!(names.len(), OBSERVED_ENVIRONMENT.len());
        initialize_diagnostic_environment();
        let first = snapshot();
        assert_eq!(first.len(), names.len());
        assert!(std::ptr::eq(first, snapshot()));
        assert!(first.iter().all(|setting| names.contains(&setting.name)));
    }
}
