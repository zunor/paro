// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Optional, session-owned phase evidence for one front-end statement.
//!
//! The trace is deliberately relative to one monotonic `Instant`.  It never
//! stores wall-clock timestamps and it is disabled unless
//! `PARO_STATEMENT_TRACE` is explicitly enabled.  This keeps the normal query
//! path free of per-phase allocation while making cold-statement boundaries
//! auditable when a benchmark or a developer opts in.

use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const STATEMENT_TRACE_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementTraceEvent {
    pub sequence: u64,
    pub phase: String,
    pub event: String,
    pub elapsed_us: u64,
    pub duration_us: Option<u64>,
    pub value: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementTraceSnapshot {
    pub schema_version: u32,
    pub statement_id: u64,
    pub statement_index: usize,
    pub query_len: usize,
    pub query_fingerprint: u64,
    pub events: Vec<StatementTraceEvent>,
}

#[derive(Debug)]
struct TraceState {
    next_sequence: u64,
    events: Vec<StatementTraceEvent>,
}

/// A cheap-to-share recorder attached to the immutable statement context.
pub struct StatementTrace {
    started_at: Instant,
    statement_id: u64,
    statement_index: usize,
    query_len: usize,
    query_fingerprint: u64,
    state: Mutex<TraceState>,
}

impl std::fmt::Debug for StatementTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementTrace")
            .field("statement_id", &self.statement_id)
            .field("statement_index", &self.statement_index)
            .field("query_len", &self.query_len)
            .field("query_fingerprint", &self.query_fingerprint)
            .finish_non_exhaustive()
    }
}

impl StatementTrace {
    /// Return whether the optional diagnostic recorder was requested.
    pub fn enabled() -> bool {
        std::env::var("PARO_STATEMENT_TRACE")
            .map(|value| {
                !matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "" | "0" | "false" | "off" | "no"
                )
            })
            .unwrap_or(false)
    }

    pub fn new(
        statement_id: u64,
        statement_index: usize,
        query: &str,
        started_at: Instant,
    ) -> Self {
        Self {
            started_at,
            statement_id,
            statement_index,
            query_len: query.len(),
            query_fingerprint: fingerprint(query),
            state: Mutex::new(TraceState {
                next_sequence: 0,
                events: Vec::new(),
            }),
        }
    }

    pub fn record_event(&self, phase: &str, event: &str) {
        self.record(phase, event, None, None);
    }

    pub fn record_value(&self, phase: &str, event: &str, value: u64) {
        self.record(phase, event, None, Some(value));
    }

    pub fn record_duration(&self, phase: &str, event: &str, duration: Duration) {
        self.record(phase, event, Some(duration), None);
    }

    pub fn record_span(&self, phase: &str, event: &str, started_at: Instant) {
        self.record_duration(phase, event, started_at.elapsed());
    }

    pub fn snapshot(&self) -> StatementTraceSnapshot {
        let state = self.state.lock().unwrap();
        StatementTraceSnapshot {
            schema_version: STATEMENT_TRACE_SCHEMA_VERSION,
            statement_id: self.statement_id,
            statement_index: self.statement_index,
            query_len: self.query_len,
            query_fingerprint: self.query_fingerprint,
            events: state.events.clone(),
        }
    }

    fn record(&self, phase: &str, event: &str, duration: Option<Duration>, value: Option<u64>) {
        let mut state = self.state.lock().unwrap();
        let sequence = state.next_sequence;
        state.next_sequence = state.next_sequence.saturating_add(1);
        state.events.push(StatementTraceEvent {
            sequence,
            phase: phase.to_string(),
            event: event.to_string(),
            elapsed_us: self
                .started_at
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
            duration_us: duration
                .map(|duration| duration.as_micros().min(u128::from(u64::MAX)) as u64),
            value,
        });
    }
}

/// Stable within a source tree and independent of randomized hash seeds.
pub fn fingerprint(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3_u64);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::{fingerprint, StatementTrace};
    use std::time::{Duration, Instant};

    #[test]
    fn trace_snapshot_is_ordered_and_correlatable_without_query_text() {
        let trace = StatementTrace::new(7, 2, "SELECT 1", Instant::now());
        trace.record_event("frontend", "parse_exit");
        trace.record_value("execution", "rows", 3);
        trace.record_duration("protocol", "result_metadata", Duration::from_micros(4));

        let snapshot = trace.snapshot();
        assert_eq!(snapshot.schema_version, 2);
        assert_eq!(snapshot.statement_id, 7);
        assert_eq!(snapshot.statement_index, 2);
        assert_eq!(snapshot.query_len, 8);
        assert_eq!(snapshot.query_fingerprint, fingerprint("SELECT 1"));
        assert_eq!(
            snapshot
                .events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(snapshot.events[1].value, Some(3));
        assert_eq!(snapshot.events[2].duration_us, Some(4));
    }
}
