// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use crate::explain::types::{
    ExplainActualStats, ExplainControlRegionStats, ExplainNodeId, ExplainRuntimeStats,
};

#[derive(Debug, Clone, Default)]
struct LocalOperatorStats {
    output_rows: u64,
    loops: u64,
    startup_time_ms: Option<f64>,
    total_time_ms: Option<f64>,
    runtime: ExplainRuntimeStats,
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
        merge_runtime_stats(&self.runtime, &mut target.runtime);
    }

    fn record_runtime(&mut self, runtime: ExplainRuntimeStats) {
        merge_runtime_stats(&runtime, &mut self.runtime);
    }
}

#[derive(Debug)]
pub struct ExplainProfiler {
    start_time: Instant,
    stats: Mutex<HashMap<ExplainNodeId, ExplainActualStats>>,
    control_regions: Mutex<Vec<ExplainControlRegionStats>>,
}

#[derive(Debug, Clone, Default)]
pub struct ExplainProfileSnapshot {
    pub operators: HashMap<ExplainNodeId, ExplainActualStats>,
    pub control_regions: Vec<ExplainControlRegionStats>,
}

impl ExplainProfiler {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            start_time: Instant::now(),
            stats: Mutex::new(HashMap::new()),
            control_regions: Mutex::new(Vec::new()),
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

    pub fn record_control_region(&self, stats: ExplainControlRegionStats) {
        self.control_regions.lock().push(stats);
    }

    pub fn snapshot(&self) -> ExplainProfileSnapshot {
        ExplainProfileSnapshot {
            operators: self.stats.lock().clone(),
            control_regions: self.control_regions.lock().clone(),
        }
    }
}

#[derive(Debug)]
pub struct OperatorProfiler {
    shared: Option<Arc<ExplainProfiler>>,
    active: Option<(ExplainNodeId, Instant)>,
    local: HashMap<ExplainNodeId, LocalOperatorStats>,
}

impl OperatorProfiler {
    pub fn new(shared: Arc<ExplainProfiler>) -> Self {
        Self {
            shared: Some(shared),
            active: None,
            local: HashMap::new(),
        }
    }

    pub fn disabled() -> Self {
        Self {
            shared: None,
            active: None,
            local: HashMap::new(),
        }
    }

    pub fn start_operator(&mut self, node_id: ExplainNodeId) {
        if self.shared.is_none() {
            return;
        }
        self.active = Some((node_id, Instant::now()));
    }

    pub fn end_operator(&mut self, node_id: ExplainNodeId, output_rows: u64) {
        let Some(shared) = &self.shared else {
            return;
        };
        let Some((active_node_id, started_at)) = self.active.take() else {
            return;
        };
        if active_node_id != node_id {
            self.active = Some((active_node_id, started_at));
            return;
        };
        let ended_at = Instant::now();
        let startup_time_ms = shared.elapsed_ms(started_at);
        let total_time_ms = shared.elapsed_ms(ended_at);
        self.local
            .entry(node_id)
            .or_default()
            .record(startup_time_ms, total_time_ms, output_rows);
    }

    pub fn cancel_operator(&mut self, node_id: ExplainNodeId) {
        if self.shared.is_none() {
            return;
        }
        if self
            .active
            .as_ref()
            .is_some_and(|(active_node_id, _)| *active_node_id == node_id)
        {
            self.active = None;
        }
    }

    pub fn record_runtime(&mut self, node_id: ExplainNodeId, runtime: ExplainRuntimeStats) {
        if self.shared.is_none() || !runtime.has_any() {
            return;
        }
        self.local
            .entry(node_id)
            .or_default()
            .record_runtime(runtime);
    }

    pub fn flush(&mut self) {
        let Some(shared) = &self.shared else {
            return;
        };
        if self.local.is_empty() {
            return;
        }
        shared.merge(&self.local);
        self.local.clear();
    }
}

fn merge_runtime_stats(source: &ExplainRuntimeStats, target: &mut ExplainRuntimeStats) {
    target.spilled = match (target.spilled, source.spilled) {
        (Some(left), Some(right)) => Some(left || right),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    };
    merge_max(&mut target.peak_memory_bytes, source.peak_memory_bytes);
    merge_sum(&mut target.spilled_bytes, source.spilled_bytes);
}

fn merge_max(target: &mut Option<u64>, value: Option<u64>) {
    if let Some(value) = value {
        *target = Some(target.map_or(value, |existing| existing.max(value)));
    }
}

fn merge_sum(target: &mut Option<u64>, value: Option<u64>) {
    if let Some(value) = value {
        *target = Some(target.unwrap_or(0).saturating_add(value));
    }
}
