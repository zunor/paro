// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use crate::explain::types::{ExplainActualStats, ExplainNodeId};

#[derive(Debug, Clone, Default)]
struct LocalOperatorStats {
    output_rows: u64,
    loops: u64,
    startup_time_ms: Option<f64>,
    total_time_ms: Option<f64>,
}

impl LocalOperatorStats {
    fn record(&mut self, startup_time_ms: f64, total_time_ms: f64, output_rows: u64) {
        self.output_rows = self.output_rows.saturating_add(output_rows);
        self.loops = self.loops.saturating_add(1);
        self.startup_time_ms = match self.startup_time_ms {
            Some(existing) => Some(existing.min(startup_time_ms)),
            None => Some(startup_time_ms),
        };
        self.total_time_ms = match self.total_time_ms {
            Some(existing) => Some(existing.max(total_time_ms)),
            None => Some(total_time_ms),
        };
    }

    fn merge_into(&self, target: &mut ExplainActualStats) {
        target.output_rows = target.output_rows.saturating_add(self.output_rows);
        target.loops = target.loops.saturating_add(self.loops);
        target.startup_time_ms = match (target.startup_time_ms, self.startup_time_ms) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        };
        target.total_time_ms = match (target.total_time_ms, self.total_time_ms) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        };
    }
}

#[derive(Debug)]
pub struct ExplainProfiler {
    start_time: Instant,
    stats: Mutex<HashMap<ExplainNodeId, ExplainActualStats>>,
}

impl ExplainProfiler {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            start_time: Instant::now(),
            stats: Mutex::new(HashMap::new()),
        })
    }

    fn elapsed_ms(&self, instant: Instant) -> f64 {
        instant.duration_since(self.start_time).as_secs_f64() * 1000.0
    }

    fn merge(&self, local: &HashMap<ExplainNodeId, LocalOperatorStats>) {
        let mut stats = self.stats.lock();
        for (node_id, local_stats) in local {
            let entry = stats.entry(*node_id).or_default();
            local_stats.merge_into(entry);
        }
    }

    pub fn node_stats(&self, node_id: ExplainNodeId) -> Option<ExplainActualStats> {
        self.stats.lock().get(&node_id).cloned()
    }
}

#[derive(Debug)]
pub struct OperatorProfiler {
    shared: Arc<ExplainProfiler>,
    active: HashMap<ExplainNodeId, Instant>,
    local: HashMap<ExplainNodeId, LocalOperatorStats>,
}

impl OperatorProfiler {
    pub fn new(shared: Arc<ExplainProfiler>) -> Self {
        Self {
            shared,
            active: HashMap::new(),
            local: HashMap::new(),
        }
    }

    pub fn start_operator(&mut self, node_id: ExplainNodeId) {
        self.active.insert(node_id, Instant::now());
    }

    pub fn end_operator(&mut self, node_id: ExplainNodeId, output_rows: u64) {
        let Some(started_at) = self.active.remove(&node_id) else {
            return;
        };
        let ended_at = Instant::now();
        let startup_time_ms = self.shared.elapsed_ms(started_at);
        let total_time_ms = self.shared.elapsed_ms(ended_at);
        self.local
            .entry(node_id)
            .or_default()
            .record(startup_time_ms, total_time_ms, output_rows);
    }

    pub fn flush(&mut self) {
        if self.local.is_empty() {
            return;
        }
        self.shared.merge(&self.local);
        self.local.clear();
    }
}
