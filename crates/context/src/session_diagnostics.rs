// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Session-owned diagnostic snapshots shared across statement contexts.

use std::sync::RwLock;

use crate::StatementTraceSnapshot;

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
}

#[derive(Debug, Default)]
pub struct SessionDiagnostics {
    optimizer: RwLock<Vec<OptimizerDiagnostic>>,
    statement_trace: RwLock<Option<StatementTraceSnapshot>>,
    statement_cache: RwLock<Vec<StatementCacheDecision>>,
}

impl SessionDiagnostics {
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

    pub fn publish_statement_cache_decision(&self, query_fingerprint: u64, cache_hit: bool) {
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
        });
        if decisions.len() > MAX_DECISIONS {
            decisions.remove(0);
        }
    }

    pub fn statement_cache_snapshot(&self) -> Vec<StatementCacheDecision> {
        self.statement_cache.read().unwrap().clone()
    }
}
