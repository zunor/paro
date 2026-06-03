// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use paro_common::allocator::{allocator_tracking_audit_enabled, allocator_tracking_event_count};

use crate::explain::types::{
    ExplainActualStats, ExplainControlRegionStats, ExplainNodeId, ExplainRuntimeStats,
};
use crate::memory_runtime::MemoryRuntimeStats;
use crate::runtime::{BlockReason, Blocker};

pub const PROFILE_SCHEMA_VERSION: u64 = 1;

static NEXT_EXPLAIN_QUERY_ID: AtomicU64 = AtomicU64::new(1);

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
    query_id: u64,
    start_time: Instant,
    stats: Mutex<HashMap<ExplainNodeId, ExplainActualStats>>,
    events: Mutex<Vec<ExplainProfileEvent>>,
    control_regions: Mutex<Vec<ExplainControlRegionStats>>,
    query_memory: Mutex<Option<MemoryRuntimeStats>>,
}

#[derive(Debug, Clone, Default)]
pub struct ExplainProfileSnapshot {
    pub query_id: u64,
    pub operators: HashMap<ExplainNodeId, ExplainActualStats>,
    pub events: Vec<ExplainProfileEvent>,
    pub control_regions: Vec<ExplainControlRegionStats>,
    pub query_memory: Option<MemoryRuntimeStats>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProfileWorkerContext {
    pub pipeline_id: Option<u64>,
    pub work_unit_id: Option<u64>,
    pub thread_id: Option<u64>,
    pub total_threads: Option<u64>,
    pub morsel_range: Option<ProfileMorselRange>,
}

impl ProfileWorkerContext {
    pub fn new(
        pipeline_id: Option<u64>,
        work_unit_id: Option<u64>,
        thread_id: Option<u64>,
        total_threads: Option<u64>,
        morsel_range: Option<ProfileMorselRange>,
    ) -> Self {
        Self {
            pipeline_id,
            work_unit_id,
            thread_id,
            total_threads,
            morsel_range,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileMorselRange {
    pub kind: &'static str,
    pub start: u64,
    pub end: u64,
}

impl ProfileMorselRange {
    pub fn new(kind: &'static str, start: u64, end: u64) -> Self {
        Self { kind, start, end }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExplainProfileEvent {
    pub query_id: u64,
    pub sequence: u64,
    pub pipeline_id: Option<u64>,
    pub work_unit_id: Option<u64>,
    pub operator_id: ExplainNodeId,
    pub thread_id: Option<u64>,
    pub total_threads: Option<u64>,
    pub morsel_range: Option<ProfileMorselRange>,
    pub phase: &'static str,
    pub rows: u64,
    pub bytes: u64,
    pub start_time_ms: f64,
    pub end_time_ms: f64,
    pub wait_reason: Option<&'static str>,
    pub memory_class: Option<&'static str>,
}

impl ExplainProfiler {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            query_id: NEXT_EXPLAIN_QUERY_ID.fetch_add(1, Ordering::Relaxed),
            start_time: Instant::now(),
            stats: Mutex::new(HashMap::new()),
            events: Mutex::new(Vec::new()),
            control_regions: Mutex::new(Vec::new()),
            query_memory: Mutex::new(None),
        })
    }

    pub fn query_id(&self) -> u64 {
        self.query_id
    }

    fn elapsed_ms(&self, instant: Instant) -> f64 {
        instant.duration_since(self.start_time).as_secs_f64() * 1000.0
    }

    fn merge(
        &self,
        local: &HashMap<ExplainNodeId, LocalOperatorStats>,
        local_events: &[ExplainProfileEvent],
    ) {
        let mut stats = self.stats.lock();
        for (node_id, local_stats) in local {
            let entry = stats.entry(*node_id).or_default();
            local_stats.merge_into(entry);
        }
        drop(stats);

        if !local_events.is_empty() {
            self.events.lock().extend_from_slice(local_events);
        }
    }

    pub fn record_control_region(&self, stats: ExplainControlRegionStats) {
        self.control_regions.lock().push(stats);
    }

    pub fn record_query_memory_stats(&self, stats: MemoryRuntimeStats) {
        *self.query_memory.lock() = Some(stats);
    }

    pub fn snapshot(&self) -> ExplainProfileSnapshot {
        let mut events = self.events.lock().clone();
        events.sort_by_key(profile_event_sort_key);
        ExplainProfileSnapshot {
            query_id: self.query_id,
            operators: self.stats.lock().clone(),
            events,
            control_regions: self.control_regions.lock().clone(),
            query_memory: self.query_memory.lock().clone(),
        }
    }
}

#[derive(Debug)]
pub struct OperatorProfiler {
    shared: Option<Arc<ExplainProfiler>>,
    active: Option<(ExplainNodeId, Instant, u64)>,
    local: HashMap<ExplainNodeId, LocalOperatorStats>,
    local_events: Vec<ExplainProfileEvent>,
    worker: ProfileWorkerContext,
    sequence: u64,
}

impl OperatorProfiler {
    pub fn new(shared: Arc<ExplainProfiler>) -> Self {
        Self::new_with_context(shared, ProfileWorkerContext::default())
    }

    pub fn new_with_context(shared: Arc<ExplainProfiler>, worker: ProfileWorkerContext) -> Self {
        Self {
            shared: Some(shared),
            active: None,
            local: HashMap::new(),
            local_events: Vec::new(),
            worker,
            sequence: 0,
        }
    }

    pub fn disabled() -> Self {
        Self {
            shared: None,
            active: None,
            local: HashMap::new(),
            local_events: Vec::new(),
            worker: ProfileWorkerContext::default(),
            sequence: 0,
        }
    }

    pub fn start_operator(&mut self, node_id: ExplainNodeId) {
        if self.shared.is_none() {
            return;
        }
        let tracking_start = allocator_tracking_event_count();
        self.active = Some((node_id, Instant::now(), tracking_start));
    }

    pub fn end_operator(&mut self, node_id: ExplainNodeId, output_rows: u64) {
        let Some(shared) = self.shared.as_ref().cloned() else {
            return;
        };
        let Some((active_node_id, started_at, tracking_start)) = self.active.take() else {
            return;
        };
        if active_node_id != node_id {
            self.active = Some((active_node_id, started_at, tracking_start));
            return;
        };
        let ended_at = Instant::now();
        let startup_time_ms = shared.elapsed_ms(started_at);
        let total_time_ms = shared.elapsed_ms(ended_at);
        {
            let stats = self.local.entry(node_id).or_default();
            stats.record(startup_time_ms, total_time_ms, output_rows);
            if allocator_tracking_audit_enabled() {
                let tracking_delta =
                    allocator_tracking_event_count().saturating_sub(tracking_start);
                if tracking_delta > 0 {
                    stats.record_runtime(ExplainRuntimeStats {
                        allocator_tracking_event_count: Some(tracking_delta),
                        ..ExplainRuntimeStats::default()
                    });
                }
            }
        }
        self.push_event(
            &shared,
            ProfileEventInput {
                operator_id: node_id,
                phase: "operator",
                rows: output_rows,
                bytes: 0,
                started_at,
                ended_at,
                wait_reason: None,
                memory_class: None,
            },
        );
    }

    pub fn cancel_operator(&mut self, node_id: ExplainNodeId) {
        // Cancel is cleanup-only: the operator did not produce a completed
        // call boundary, so it should not create a profile event.
        if self.shared.is_none() {
            return;
        }
        if self
            .active
            .as_ref()
            .is_some_and(|(active_node_id, _, _)| *active_node_id == node_id)
        {
            self.active = None;
        }
    }

    pub fn record_runtime(&mut self, node_id: ExplainNodeId, runtime: ExplainRuntimeStats) {
        if self.shared.is_none() {
            return;
        };
        if !runtime.has_any() {
            return;
        }
        self.local
            .entry(node_id)
            .or_default()
            .record_runtime(runtime);
    }

    pub fn record_blocked(&mut self, node_id: ExplainNodeId, blocker: &Blocker) {
        let Some(shared) = self.shared.as_ref().cloned() else {
            return;
        };
        let reason = block_reason_name(&blocker.reason);
        let mut runtime = ExplainRuntimeStats {
            scheduler_blocked_count: Some(1),
            ..ExplainRuntimeStats::default()
        };
        if blocker.reason == BlockReason::OutputBackpressure {
            runtime.output_backpressure_count = Some(1);
        }
        self.local
            .entry(node_id)
            .or_default()
            .record_runtime(runtime);
        let now = Instant::now();
        self.push_event(
            &shared,
            ProfileEventInput {
                operator_id: node_id,
                phase: "wait",
                rows: 0,
                bytes: blocker.retained_memory.bytes as u64,
                started_at: now,
                ended_at: now,
                wait_reason: Some(reason),
                memory_class: Some(match blocker.reason {
                    BlockReason::Memory => "revocable",
                    BlockReason::Spill => "spill",
                    _ => "runtime",
                }),
            },
        );
    }

    pub fn record_wake(
        &mut self,
        node_id: ExplainNodeId,
        blocker: Option<&Blocker>,
        wait_time_us: u64,
    ) {
        let Some(shared) = self.shared.as_ref().cloned() else {
            return;
        };
        let reason = blocker.map(|blocker| block_reason_name(&blocker.reason));
        self.local
            .entry(node_id)
            .or_default()
            .record_runtime(ExplainRuntimeStats {
                scheduler_wake_count: Some(1),
                scheduler_wait_time_us: (wait_time_us > 0).then_some(wait_time_us),
                ..ExplainRuntimeStats::default()
            });
        let now = Instant::now();
        self.push_event(
            &shared,
            ProfileEventInput {
                operator_id: node_id,
                phase: "wake",
                rows: 0,
                bytes: 0,
                started_at: now,
                ended_at: now,
                wait_reason: reason,
                memory_class: None,
            },
        );
    }

    pub fn flush(&mut self) {
        let Some(shared) = &self.shared else {
            return;
        };
        if self.local.is_empty() && self.local_events.is_empty() {
            return;
        }
        shared.merge(&self.local, &self.local_events);
        self.local.clear();
        self.local_events.clear();
    }

    fn push_event(&mut self, shared: &ExplainProfiler, input: ProfileEventInput) {
        self.sequence = self.sequence.saturating_add(1);
        self.local_events.push(ExplainProfileEvent {
            query_id: shared.query_id(),
            sequence: self.sequence,
            pipeline_id: self.worker.pipeline_id,
            work_unit_id: self.worker.work_unit_id,
            operator_id: input.operator_id,
            thread_id: self.worker.thread_id,
            total_threads: self.worker.total_threads,
            morsel_range: self.worker.morsel_range,
            phase: input.phase,
            rows: input.rows,
            bytes: input.bytes,
            start_time_ms: shared.elapsed_ms(input.started_at),
            end_time_ms: shared.elapsed_ms(input.ended_at),
            wait_reason: input.wait_reason,
            memory_class: input.memory_class,
        });
    }
}

struct ProfileEventInput {
    operator_id: ExplainNodeId,
    phase: &'static str,
    rows: u64,
    bytes: u64,
    started_at: Instant,
    ended_at: Instant,
    wait_reason: Option<&'static str>,
    memory_class: Option<&'static str>,
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
    merge_sum(
        &mut target.allocator_tracking_event_count,
        source.allocator_tracking_event_count,
    );
    merge_sum(
        &mut target.selection_materialization_count,
        source.selection_materialization_count,
    );
    merge_sum(
        &mut target.scheduler_worker_count,
        source.scheduler_worker_count,
    );
    merge_sum(
        &mut target.scheduler_morsel_count,
        source.scheduler_morsel_count,
    );
    merge_sum(
        &mut target.scheduler_blocked_count,
        source.scheduler_blocked_count,
    );
    merge_sum(
        &mut target.scheduler_wake_count,
        source.scheduler_wake_count,
    );
    merge_sum(
        &mut target.scheduler_ready_time_us,
        source.scheduler_ready_time_us,
    );
    merge_sum(
        &mut target.scheduler_wait_time_us,
        source.scheduler_wait_time_us,
    );
    merge_sum(
        &mut target.scheduler_wake_coalesce_count,
        source.scheduler_wake_coalesce_count,
    );
    merge_sum(
        &mut target.output_backpressure_count,
        source.output_backpressure_count,
    );
    merge_sum(
        &mut target.runtime_filter_installed_count,
        source.runtime_filter_installed_count,
    );
    merge_sum(
        &mut target.runtime_filter_no_wait_count,
        source.runtime_filter_no_wait_count,
    );
    merge_sum(&mut target.grant_bytes, source.grant_bytes);
    merge_sum(&mut target.revoked_bytes, source.revoked_bytes);
    merge_sum(&mut target.yield_latency_us, source.yield_latency_us);
    merge_max(&mut target.repartition_depth, source.repartition_depth);
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

fn block_reason_name(reason: &BlockReason) -> &'static str {
    match reason {
        BlockReason::Memory => "memory",
        BlockReason::Spill => "spill",
        BlockReason::ExternalRuntime => "external_runtime",
        BlockReason::DerivedIndex => "derived_index",
        BlockReason::OutputBackpressure => "output_backpressure",
        BlockReason::CancelCheck => "cancel_check",
        BlockReason::Other(reason) => reason,
    }
}

fn profile_event_sort_key(
    event: &ExplainProfileEvent,
) -> (u64, u64, u64, u64, u64, &'static str, u64) {
    (
        event.pipeline_id.unwrap_or(u64::MAX),
        event.operator_id,
        event.work_unit_id.unwrap_or(u64::MAX),
        event.thread_id.unwrap_or(u64::MAX),
        event
            .morsel_range
            .map(|range| range.start)
            .unwrap_or(u64::MAX),
        event.phase,
        event.sequence,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::RetainedMemorySnapshot;

    #[test]
    fn profile_events_are_worker_local_until_flush() {
        let shared = ExplainProfiler::new();
        let mut profiler = OperatorProfiler::new_with_context(
            shared.clone(),
            ProfileWorkerContext::new(
                Some(3),
                Some(30),
                Some(2),
                Some(4),
                Some(ProfileMorselRange::new("chunk", 7, 8)),
            ),
        );

        profiler.start_operator(11);
        profiler.end_operator(11, 128);
        assert!(shared.snapshot().events.is_empty());

        profiler.flush();
        let snapshot = shared.snapshot();
        assert_eq!(snapshot.events.len(), 1);
        let event = &snapshot.events[0];
        assert_eq!(event.query_id, snapshot.query_id);
        assert_eq!(event.pipeline_id, Some(3));
        assert_eq!(event.work_unit_id, Some(30));
        assert_eq!(event.operator_id, 11);
        assert_eq!(event.thread_id, Some(2));
        assert_eq!(event.total_threads, Some(4));
        assert_eq!(
            event.morsel_range,
            Some(ProfileMorselRange::new("chunk", 7, 8))
        );
        assert_eq!(event.phase, "operator");
        assert_eq!(event.rows, 128);
    }

    #[test]
    fn profile_events_sort_by_context_instead_of_flush_order() {
        let shared = ExplainProfiler::new();
        let mut later_pipeline = OperatorProfiler::new_with_context(
            shared.clone(),
            ProfileWorkerContext::new(Some(9), Some(90), Some(1), Some(2), None),
        );
        let mut earlier_pipeline = OperatorProfiler::new_with_context(
            shared.clone(),
            ProfileWorkerContext::new(Some(1), Some(10), Some(0), Some(2), None),
        );

        later_pipeline.start_operator(90);
        later_pipeline.end_operator(90, 1);
        later_pipeline.flush();
        earlier_pipeline.start_operator(10);
        earlier_pipeline.end_operator(10, 1);
        earlier_pipeline.flush();

        let pipelines = shared
            .snapshot()
            .events
            .iter()
            .map(|event| event.pipeline_id)
            .collect::<Vec<_>>();
        assert_eq!(pipelines, vec![Some(1), Some(9)]);
    }

    #[test]
    fn wait_and_wake_runtime_stats_are_aggregated_locally() {
        let shared = ExplainProfiler::new();
        let mut profiler = OperatorProfiler::new_with_context(
            shared.clone(),
            ProfileWorkerContext::new(Some(4), Some(40), Some(1), Some(2), None),
        );
        let blocker = Blocker {
            reason: BlockReason::OutputBackpressure,
            wake: None,
            retained_memory: RetainedMemorySnapshot { bytes: 512 },
        };

        profiler.record_blocked(44, &blocker);
        profiler.record_wake(44, Some(&blocker), 17);
        profiler.flush();

        let snapshot = shared.snapshot();
        let actual = snapshot.operators.get(&44).expect("operator stats");
        assert_eq!(actual.runtime.scheduler_blocked_count, Some(1));
        assert_eq!(actual.runtime.scheduler_wake_count, Some(1));
        assert_eq!(actual.runtime.scheduler_wait_time_us, Some(17));
        assert_eq!(actual.runtime.output_backpressure_count, Some(1));
        assert_eq!(snapshot.events.len(), 2);
        assert_eq!(snapshot.events[0].phase, "wait");
        assert_eq!(snapshot.events[0].wait_reason, Some("output_backpressure"));
        assert_eq!(snapshot.events[0].memory_class, Some("runtime"));
        assert_eq!(snapshot.events[0].bytes, 512);
    }
}
