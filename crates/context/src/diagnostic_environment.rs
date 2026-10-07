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
    "PARO_DIAGNOSTIC_STREAM_SEQUENTIAL",
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
