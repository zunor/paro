// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, RwLock};
use std::time::Duration;

use crate::optimizer_type::OptimizerType;

#[derive(Debug, Clone, Default)]
pub struct PipelineTimingEntry {
    pub last_elapsed: Duration,
    pub invocation_count: u64,
}

#[derive(Debug, Default)]
pub struct PipelineProfiler {
    entries: HashMap<OptimizerType, PipelineTimingEntry>,
}

#[derive(Debug, Clone)]
pub struct OptimizerProfileSnapshotEntry {
    pub optimizer_type: OptimizerType,
    pub enabled: bool,
    pub last_elapsed: Duration,
    pub invocation_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct OptimizerProfileSnapshot {
    pub entries: Vec<OptimizerProfileSnapshotEntry>,
}

static LAST_PROFILE_SNAPSHOT: LazyLock<RwLock<OptimizerProfileSnapshot>> =
    LazyLock::new(|| RwLock::new(OptimizerProfileSnapshot::default()));

impl PipelineProfiler {
    pub fn record(&mut self, optimizer_type: OptimizerType, elapsed: Duration) {
        let entry = self.entries.entry(optimizer_type).or_default();
        entry.last_elapsed = elapsed;
        entry.invocation_count += 1;
    }

    pub fn get(&self, optimizer_type: OptimizerType) -> Option<&PipelineTimingEntry> {
        self.entries.get(&optimizer_type)
    }

    pub fn snapshot(
        &self,
        pipeline: &[OptimizerType],
        disabled: &HashSet<OptimizerType>,
    ) -> OptimizerProfileSnapshot {
        let mut seen = HashSet::new();
        let mut entries = Vec::new();
        for optimizer_type in pipeline.iter().copied() {
            if !seen.insert(optimizer_type) {
                continue;
            }
            let profile = self.get(optimizer_type);
            entries.push(OptimizerProfileSnapshotEntry {
                optimizer_type,
                enabled: !disabled.contains(&optimizer_type),
                last_elapsed: profile.map(|entry| entry.last_elapsed).unwrap_or_default(),
                invocation_count: profile.map(|entry| entry.invocation_count).unwrap_or(0),
            });
        }
        OptimizerProfileSnapshot { entries }
    }
}

pub fn publish_optimizer_profile_snapshot(snapshot: OptimizerProfileSnapshot) {
    *LAST_PROFILE_SNAPSHOT.write().unwrap() = snapshot;
}

pub fn latest_optimizer_profile_snapshot() -> OptimizerProfileSnapshot {
    LAST_PROFILE_SNAPSHOT.read().unwrap().clone()
}
