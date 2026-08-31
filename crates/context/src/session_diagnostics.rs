// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Session-owned diagnostic snapshots shared across statement contexts.

use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerDiagnostic {
    pub name: String,
    pub kind: String,
    pub last_elapsed_us: i64,
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
