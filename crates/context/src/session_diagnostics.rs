// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Session-owned diagnostic snapshots shared across statement contexts.

use std::sync::RwLock;

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

#[derive(Debug, Default)]
pub struct SessionDiagnostics {
    optimizer: RwLock<Vec<OptimizerDiagnostic>>,
}

impl SessionDiagnostics {
    pub fn publish_optimizer(&self, entries: Vec<OptimizerDiagnostic>) {
        *self.optimizer.write().unwrap() = entries;
    }

    pub fn optimizer_snapshot(&self) -> Vec<OptimizerDiagnostic> {
        self.optimizer.read().unwrap().clone()
    }
}
