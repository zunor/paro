// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! EXPLAIN ANALYZE rendering for typed runtime programs.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write;

use paro_planner::operator::{ExplainFormat, ExplainSpec};

use crate::explain::profiler::{
    ExplainProfileEvent, ExplainProfileSnapshot, ExplainProfiler, ProfileMorselRange,
    PROFILE_SCHEMA_VERSION,
};
use crate::explain::types::{
    ExplainActualStats, ExplainControlRegionStats, ExplainNodeId, ExplainRecursiveCteStats,
    ExplainRuntimeStats, EXPLAIN_FORMAT_VERSION,
};
use crate::memory_runtime::MemoryRuntimeStats;
use crate::pipeline::StatementProgram;

pub(crate) fn render_explain_analyze(
    target: &StatementProgram,
    spec: ExplainSpec,
    profiler: &ExplainProfiler,
    elapsed_ms: f64,
    rows_returned: u64,
) -> Vec<String> {
    let stats = profiler.snapshot();
    match spec.format {
        ExplainFormat::Text => {
            let mut lines = render_explain_analyze_text(target, &stats, elapsed_ms);
            if spec.detail.summary {
                lines.push(format!("Execution Time: {elapsed_ms:.3} ms"));
                lines.push(format!("Rows Returned: {rows_returned}"));
            }
            lines
        }
        ExplainFormat::Json => vec![render_explain_analyze_json(
            target,
            &stats,
            elapsed_ms,
            rows_returned,
        )],
    }
}

fn render_explain_analyze_text(
    target: &StatementProgram,
    snapshot: &ExplainProfileSnapshot,
    elapsed_ms: f64,
) -> Vec<String> {
    let mut lines = Vec::new();
    match target {
        StatementProgram::Pipeline { programs, .. } => {
            for program in programs.pipelines.iter() {
                lines.push(format!("PIPELINE {}", program.id.index()));
                lines.push(format!(
                    "  SOURCE #{} {}{}",
                    program.source.operator_id.index(),
                    program.source.exec.name(),
                    actual_suffix(
                        &snapshot.operators,
                        program.source.operator_id.index() as u64
                    )
                ));
                for transform in program.transforms.iter() {
                    lines.push(format!(
                        "  TRANSFORM #{} {}{}",
                        transform.operator_id.index(),
                        transform.exec.name(),
                        actual_suffix(&snapshot.operators, transform.operator_id.index() as u64)
                    ));
                }
                lines.push(format!(
                    "  SINK #{} {}{}",
                    program.sink.operator_id.index(),
                    program.sink.exec.name(),
                    actual_suffix(&snapshot.operators, program.sink.operator_id.index() as u64)
                ));
            }
            render_control_regions_text(&snapshot.control_regions, &mut lines);
            render_profile_summary_text(snapshot, elapsed_ms, &mut lines);
        }
        StatementProgram::Utility(utility) => {
            lines.push(format!("UTILITY {:?}", utility.spec));
            render_profile_summary_text(snapshot, elapsed_ms, &mut lines);
        }
        StatementProgram::ExplainAnalyze { .. } => {
            unreachable!("nested EXPLAIN ANALYZE is rejected before rendering");
        }
    }
    lines
}

fn render_explain_analyze_json(
    target: &StatementProgram,
    snapshot: &ExplainProfileSnapshot,
    elapsed_ms: f64,
    rows_returned: u64,
) -> String {
    let operators = match target {
        StatementProgram::Pipeline { programs, .. } => {
            let capacity = programs
                .pipelines
                .iter()
                .map(|program| program.transforms.len() + 2)
                .sum();
            let mut operators = Vec::with_capacity(capacity);
            for program in programs.pipelines.iter() {
                let pipeline = program.id.index();
                operators.push(operator_json(
                    pipeline,
                    program.source.operator_id.index(),
                    "source",
                    program.source.exec.name(),
                    &snapshot.operators,
                ));
                for transform in program.transforms.iter() {
                    operators.push(operator_json(
                        pipeline,
                        transform.operator_id.index(),
                        "transform",
                        transform.exec.name(),
                        &snapshot.operators,
                    ));
                }
                operators.push(operator_json(
                    pipeline,
                    program.sink.operator_id.index(),
                    "sink",
                    program.sink.exec.name(),
                    &snapshot.operators,
                ));
            }
            operators
        }
        StatementProgram::Utility(utility) => vec![serde_json::json!({
            "role": "utility",
            "operator": format!("{:?}", utility.spec),
        })],
        StatementProgram::ExplainAnalyze { .. } => {
            unreachable!("nested EXPLAIN ANALYZE is rejected before rendering");
        }
    };
    let mut output = serde_json::json!({
        "format_version": EXPLAIN_FORMAT_VERSION,
        "mode": "analyze",
        "profile_schema_version": PROFILE_SCHEMA_VERSION,
        "query_id": snapshot.query_id,
        "profile_schema": PROFILE_SCHEMA_FIELDS,
        "operators": operators,
        "profile": profile_summary_json(snapshot, elapsed_ms),
        "profile_events": profile_events_json(&snapshot.events),
        "summary": {
            "Execution Time": format!("{elapsed_ms:.3} ms"),
            "Rows Returned": rows_returned,
        }
    });
    if !snapshot.control_regions.is_empty() {
        output["control_regions"] = control_regions_json(&snapshot.control_regions);
    }
    output.to_string()
}

const PROFILE_SCHEMA_FIELDS: &[&str] = &[
    "query_id",
    "pipeline_id",
    "work_unit_id",
    "operator_id",
    "thread_id",
    "morsel_range",
    "phase",
    "rows",
    "bytes",
    "time",
    "wait_reason",
    "memory_class",
];

fn render_profile_summary_text(
    snapshot: &ExplainProfileSnapshot,
    elapsed_ms: f64,
    lines: &mut Vec<String>,
) {
    let profile = profile_summary_for_elapsed(snapshot, elapsed_ms);
    lines.push(format!(
        "PROFILE schema_version={} query_id={} events={} parallelism={} workers={} worker_utilization={:.4} ready_time_us={} wait_time_us={} wake_coalesce={} backpressure={} runtime_filter_installed={} runtime_filter_no_wait={}",
        PROFILE_SCHEMA_VERSION,
        snapshot.query_id,
        snapshot.events.len(),
        profile.max_parallelism,
        profile.observed_workers,
        profile.worker_utilization,
        profile.ready_time_us,
        profile.wait_time_us,
        profile.wake_coalesce_count,
        profile.output_backpressure_count,
        profile.runtime_filter_installed_count,
        profile.runtime_filter_no_wait_count,
    ));
    lines.push(format!(
        "MEMORY_PROFILE grant_bytes={} revoked_bytes={} revocable_bytes={} spill_bytes={} spill_latency_us={} yield_latency_us={} repartition_depth={}",
        profile.memory_grant_bytes,
        profile.memory_revoked_bytes,
        profile.memory_revocable_bytes,
        profile.memory_spill_bytes,
        profile.memory_spill_latency_us,
        profile.memory_yield_latency_us,
        profile.repartition_depth,
    ));
}

fn profile_summary_json(snapshot: &ExplainProfileSnapshot, elapsed_ms: f64) -> serde_json::Value {
    let profile = profile_summary_for_elapsed(snapshot, elapsed_ms);
    serde_json::json!({
        "parallelism": {
            "max_threads": profile.max_parallelism,
            "observed_workers": profile.observed_workers,
            "worker_utilization": profile.worker_utilization,
            "operator_time_ms": profile.operator_time_ms,
            "ready_time_us": profile.ready_time_us,
            "wait_time_us": profile.wait_time_us,
            "wake_count": profile.wake_count,
            "wake_coalesce_count": profile.wake_coalesce_count,
            "backpressure_count": profile.output_backpressure_count,
        },
        "runtime_filters": {
            "installed_count": profile.runtime_filter_installed_count,
            "no_wait_fallback_count": profile.runtime_filter_no_wait_count,
        },
        "memory": {
            "grant_bytes": profile.memory_grant_bytes,
            "revoked_bytes": profile.memory_revoked_bytes,
            "revocable_bytes": profile.memory_revocable_bytes,
            "spill_bytes": profile.memory_spill_bytes,
            "spill_latency_us": profile.memory_spill_latency_us,
            "yield_latency_us": profile.memory_yield_latency_us,
            "repartition_depth": profile.repartition_depth,
        },
    })
}

fn profile_summary_for_elapsed(
    snapshot: &ExplainProfileSnapshot,
    elapsed_ms: f64,
) -> ProfileSummary {
    let mut profile = profile_summary(snapshot);
    if elapsed_ms > 0.0 && profile.max_parallelism > 0 {
        profile.worker_utilization =
            profile.operator_time_ms / (elapsed_ms * profile.max_parallelism as f64);
    }
    profile
}

fn profile_events_json(events: &[ExplainProfileEvent]) -> serde_json::Value {
    serde_json::Value::Array(events.iter().map(profile_event_json).collect())
}

fn profile_event_json(event: &ExplainProfileEvent) -> serde_json::Value {
    serde_json::json!({
        "query_id": event.query_id,
        "pipeline_id": event.pipeline_id,
        "work_unit_id": event.work_unit_id,
        "operator_id": event.operator_id,
        "thread_id": event.thread_id,
        "total_threads": event.total_threads,
        "morsel_range": event.morsel_range.map(morsel_range_json),
        "phase": event.phase,
        "rows": event.rows,
        "bytes": event.bytes,
        "time": {
            "start_ms": event.start_time_ms,
            "end_ms": event.end_time_ms,
            "duration_ms": (event.end_time_ms - event.start_time_ms).max(0.0),
        },
        "wait_reason": event.wait_reason,
        "memory_class": event.memory_class,
    })
}

fn morsel_range_json(range: ProfileMorselRange) -> serde_json::Value {
    serde_json::json!({
        "kind": range.kind,
        "start": range.start,
        "end": range.end,
    })
}

#[derive(Debug, Clone)]
struct ProfileSummary {
    max_parallelism: u64,
    observed_workers: u64,
    worker_utilization: f64,
    operator_time_ms: f64,
    ready_time_us: u64,
    wait_time_us: u64,
    wake_count: u64,
    wake_coalesce_count: u64,
    output_backpressure_count: u64,
    runtime_filter_installed_count: u64,
    runtime_filter_no_wait_count: u64,
    memory_grant_bytes: u64,
    memory_revoked_bytes: u64,
    memory_revocable_bytes: u64,
    memory_spill_bytes: u64,
    memory_spill_latency_us: u64,
    memory_yield_latency_us: u64,
    repartition_depth: u64,
}

fn profile_summary(snapshot: &ExplainProfileSnapshot) -> ProfileSummary {
    let mut thread_ids = BTreeSet::new();
    let mut max_parallelism = 1;
    let mut operator_time_ms = 0.0;
    for event in &snapshot.events {
        if let Some(thread_id) = event.thread_id {
            thread_ids.insert(thread_id);
        }
        if let Some(total_threads) = event.total_threads {
            max_parallelism = max_parallelism.max(total_threads);
        }
        if event.phase == "operator" {
            operator_time_ms += (event.end_time_ms - event.start_time_ms).max(0.0);
        }
    }
    let runtime = RuntimeTotals::from_operators(&snapshot.operators);
    let memory = MemoryTotals::from_snapshot(snapshot.query_memory.as_ref(), &runtime);
    ProfileSummary {
        max_parallelism,
        observed_workers: thread_ids.len() as u64,
        worker_utilization: 0.0,
        operator_time_ms,
        ready_time_us: runtime.scheduler_ready_time_us,
        wait_time_us: runtime.scheduler_wait_time_us,
        wake_count: runtime.scheduler_wake_count,
        wake_coalesce_count: runtime.scheduler_wake_coalesce_count,
        output_backpressure_count: runtime.output_backpressure_count,
        runtime_filter_installed_count: runtime.runtime_filter_installed_count,
        runtime_filter_no_wait_count: runtime.runtime_filter_no_wait_count,
        memory_grant_bytes: memory.grant_bytes,
        memory_revoked_bytes: memory.revoked_bytes,
        memory_revocable_bytes: memory.revocable_bytes,
        memory_spill_bytes: memory.spill_bytes,
        memory_spill_latency_us: memory.spill_latency_us,
        memory_yield_latency_us: memory.yield_latency_us,
        repartition_depth: runtime.repartition_depth,
    }
}

#[derive(Debug, Default)]
struct RuntimeTotals {
    scheduler_ready_time_us: u64,
    scheduler_wait_time_us: u64,
    scheduler_wake_count: u64,
    scheduler_wake_coalesce_count: u64,
    output_backpressure_count: u64,
    runtime_filter_installed_count: u64,
    runtime_filter_no_wait_count: u64,
    grant_bytes: u64,
    revoked_bytes: u64,
    yield_latency_us: u64,
    spilled_bytes: u64,
    spill_latency_us: u64,
    repartition_depth: u64,
}

impl RuntimeTotals {
    fn from_operators(operators: &HashMap<ExplainNodeId, ExplainActualStats>) -> Self {
        let mut totals = Self::default();
        for actual in operators.values() {
            let runtime = &actual.runtime;
            totals.scheduler_ready_time_us = totals
                .scheduler_ready_time_us
                .saturating_add(runtime.scheduler_ready_time_us.unwrap_or(0));
            totals.scheduler_wait_time_us = totals
                .scheduler_wait_time_us
                .saturating_add(runtime.scheduler_wait_time_us.unwrap_or(0));
            totals.scheduler_wake_count = totals
                .scheduler_wake_count
                .saturating_add(runtime.scheduler_wake_count.unwrap_or(0));
            totals.scheduler_wake_coalesce_count = totals
                .scheduler_wake_coalesce_count
                .saturating_add(runtime.scheduler_wake_coalesce_count.unwrap_or(0));
            totals.output_backpressure_count = totals
                .output_backpressure_count
                .saturating_add(runtime.output_backpressure_count.unwrap_or(0));
            totals.runtime_filter_installed_count = totals
                .runtime_filter_installed_count
                .saturating_add(runtime.runtime_filter_installed_count.unwrap_or(0));
            totals.runtime_filter_no_wait_count = totals
                .runtime_filter_no_wait_count
                .saturating_add(runtime.runtime_filter_no_wait_count.unwrap_or(0));
            totals.grant_bytes = totals
                .grant_bytes
                .saturating_add(runtime.grant_bytes.unwrap_or(0));
            totals.revoked_bytes = totals
                .revoked_bytes
                .saturating_add(runtime.revoked_bytes.unwrap_or(0));
            totals.yield_latency_us = totals
                .yield_latency_us
                .saturating_add(runtime.yield_latency_us.unwrap_or(0));
            totals.spilled_bytes = totals
                .spilled_bytes
                .saturating_add(runtime.spilled_bytes.unwrap_or(0));
            totals.spill_latency_us = totals
                .spill_latency_us
                .saturating_add(runtime.spill_latency_us.unwrap_or(0));
            totals.repartition_depth = totals
                .repartition_depth
                .max(runtime.repartition_depth.unwrap_or(0));
        }
        totals
    }
}

#[derive(Debug, Default)]
struct MemoryTotals {
    grant_bytes: u64,
    revoked_bytes: u64,
    revocable_bytes: u64,
    spill_bytes: u64,
    spill_latency_us: u64,
    yield_latency_us: u64,
}

impl MemoryTotals {
    fn from_snapshot(memory: Option<&MemoryRuntimeStats>, runtime: &RuntimeTotals) -> Self {
        let Some(memory) = memory else {
            return Self {
                grant_bytes: runtime.grant_bytes,
                revoked_bytes: runtime.revoked_bytes,
                revocable_bytes: 0,
                spill_bytes: runtime.spilled_bytes,
                spill_latency_us: runtime.spill_latency_us,
                yield_latency_us: runtime.yield_latency_us,
            };
        };
        Self {
            grant_bytes: memory.issued_bytes as u64,
            revoked_bytes: memory.reclaimed_bytes as u64,
            revocable_bytes: memory.revocable_bytes as u64,
            spill_bytes: memory.spilled_bytes.max(memory.spill_bytes) as u64,
            spill_latency_us: memory.spill_latency_us as u64,
            yield_latency_us: memory.reclaim_latency_us as u64,
        }
    }
}

fn render_control_regions_text(stats: &[ExplainControlRegionStats], lines: &mut Vec<String>) {
    for stats in sorted_control_region_stats(stats) {
        match stats {
            ExplainControlRegionStats::RecursiveCte(stats) => {
                render_recursive_cte_text(stats, lines);
            }
        }
    }
}

fn render_recursive_cte_text(stats: &ExplainRecursiveCteStats, lines: &mut Vec<String>) {
    lines.push(format!(
        "CONTROL_REGION {} RECURSIVE_CTE (iterations={} termination={})",
        stats.region_id,
        stats.iterations,
        stats.termination.as_str()
    ));
    for iteration in &stats.iteration_stats {
        lines.push(format!(
            "  ITERATION {} (delta_rows={} working_rows={})",
            iteration.iteration, iteration.delta_rows, iteration.working_rows
        ));
    }
}

fn control_regions_json(stats: &[ExplainControlRegionStats]) -> serde_json::Value {
    serde_json::Value::Array(
        sorted_control_region_stats(stats)
            .map(|stats| match stats {
                ExplainControlRegionStats::RecursiveCte(stats) => recursive_cte_json(stats),
            })
            .collect(),
    )
}

fn sorted_control_region_stats(
    stats: &[ExplainControlRegionStats],
) -> impl Iterator<Item = &ExplainControlRegionStats> {
    let mut sorted = stats.iter().collect::<Vec<_>>();
    sorted.sort_by_key(|stats| match stats {
        ExplainControlRegionStats::RecursiveCte(stats) => stats.region_id,
    });
    sorted.into_iter()
}

fn operator_json(
    pipeline: usize,
    runtime_id: usize,
    role: &'static str,
    operator: &str,
    stats: &HashMap<ExplainNodeId, ExplainActualStats>,
) -> serde_json::Value {
    serde_json::json!({
        "pipeline": pipeline,
        "runtime_id": runtime_id,
        "role": role,
        "operator": operator,
        "actual": actual_json(stats, runtime_id as u64),
    })
}

fn recursive_cte_json(stats: &ExplainRecursiveCteStats) -> serde_json::Value {
    serde_json::json!({
        "region": stats.region_id,
        "kind": "recursive_cte",
        "iterations": stats.iterations,
        "termination": stats.termination.as_str(),
        "iteration_stats": stats.iteration_stats.iter().map(|iteration| {
            serde_json::json!({
                "iteration": iteration.iteration,
                "delta_rows": iteration.delta_rows,
                "working_rows": iteration.working_rows,
            })
        }).collect::<Vec<_>>(),
    })
}

fn actual_suffix(stats: &HashMap<ExplainNodeId, ExplainActualStats>, node_id: u64) -> String {
    let Some(actual) = stats.get(&node_id) else {
        return " (actual time=0.000..0.000 rows=0 loops=0)".to_string();
    };
    let start = actual.startup_time_ms.unwrap_or(0.0);
    let end = actual.total_time_ms.unwrap_or(start);
    let mut suffix = format!(
        " (actual time={start:.3}..{end:.3} rows={} loops={}",
        actual.output_rows, actual.loops
    );
    write_runtime_suffix(&mut suffix, &actual.runtime);
    suffix.push(')');
    suffix
}

fn write_runtime_suffix(suffix: &mut String, runtime: &ExplainRuntimeStats) {
    if let Some(value) = runtime.spilled {
        write!(suffix, " spilled={value}").expect("write to String");
    }
    let fields = [
        ("peak_memory_bytes", runtime.peak_memory_bytes),
        ("spilled_bytes", runtime.spilled_bytes),
        (
            "allocator_tracking_event_count",
            runtime.allocator_tracking_event_count,
        ),
        ("scheduler_worker_count", runtime.scheduler_worker_count),
        ("scheduler_morsel_count", runtime.scheduler_morsel_count),
        ("scheduler_blocked_count", runtime.scheduler_blocked_count),
        ("scheduler_wake_count", runtime.scheduler_wake_count),
        ("scheduler_ready_time_us", runtime.scheduler_ready_time_us),
        ("scheduler_wait_time_us", runtime.scheduler_wait_time_us),
        (
            "scheduler_wake_coalesce_count",
            runtime.scheduler_wake_coalesce_count,
        ),
        (
            "output_backpressure_count",
            runtime.output_backpressure_count,
        ),
        (
            "runtime_filter_installed_count",
            runtime.runtime_filter_installed_count,
        ),
        (
            "runtime_filter_no_wait_count",
            runtime.runtime_filter_no_wait_count,
        ),
        ("grant_bytes", runtime.grant_bytes),
        ("revoked_bytes", runtime.revoked_bytes),
        ("yield_latency_us", runtime.yield_latency_us),
        ("spill_latency_us", runtime.spill_latency_us),
        ("repartition_depth", runtime.repartition_depth),
    ];
    for (name, value) in fields {
        if let Some(value) = value {
            write!(suffix, " {name}={value}").expect("write to String");
        }
    }
}

fn actual_json(
    stats: &HashMap<ExplainNodeId, ExplainActualStats>,
    node_id: u64,
) -> serde_json::Value {
    let Some(stats_actual) = stats.get(&node_id) else {
        return serde_json::json!({
            "startup_time_ms": 0.0,
            "total_time_ms": 0.0,
            "rows": 0,
            "loops": 0,
        });
    };
    let mut actual = serde_json::json!({
        "startup_time_ms": stats_actual.startup_time_ms.unwrap_or(0.0),
        "total_time_ms": stats_actual.total_time_ms.unwrap_or(0.0),
        "rows": stats_actual.output_rows,
        "loops": stats_actual.loops,
    });
    if let Some(value) = stats_actual.runtime.spilled {
        actual["spilled"] = serde_json::Value::Bool(value);
    }
    if let Some(value) = stats_actual.runtime.peak_memory_bytes {
        actual["peak_memory_bytes"] = serde_json::Value::from(value);
    }
    if let Some(value) = stats_actual.runtime.spilled_bytes {
        actual["spilled_bytes"] = serde_json::Value::from(value);
    }
    if let Some(value) = stats_actual.runtime.allocator_tracking_event_count {
        actual["allocator_tracking_event_count"] = serde_json::Value::from(value);
    }
    insert_actual_runtime_json(&mut actual, &stats_actual.runtime);
    actual
}

fn insert_actual_runtime_json(actual: &mut serde_json::Value, runtime: &ExplainRuntimeStats) {
    let Some(object) = actual.as_object_mut() else {
        return;
    };
    insert_optional_json_u64(object, "temp_storage_bytes", runtime.temp_storage_bytes);
    insert_optional_json_u64(object, "data_plane_bytes", runtime.data_plane_bytes);
    insert_optional_json_u64(object, "leaked_grant_bytes", runtime.leaked_grant_bytes);
    insert_optional_json_u64(object, "local_refill_count", runtime.local_refill_count);
    insert_optional_json_u64(object, "local_refill_bytes", runtime.local_refill_bytes);
    insert_optional_json_u64(
        object,
        "reclaim_attempt_count",
        runtime.reclaim_attempt_count,
    );
    insert_optional_json_u64(object, "reclaimed_bytes", runtime.reclaimed_bytes);
    insert_optional_json_u64(object, "reclaim_latency_us", runtime.reclaim_latency_us);
    insert_optional_json_u64(
        object,
        "selection_materialization_count",
        runtime.selection_materialization_count,
    );
    insert_optional_json_u64(
        object,
        "scheduler_worker_count",
        runtime.scheduler_worker_count,
    );
    insert_optional_json_u64(
        object,
        "scheduler_morsel_count",
        runtime.scheduler_morsel_count,
    );
    insert_optional_json_u64(
        object,
        "scheduler_blocked_count",
        runtime.scheduler_blocked_count,
    );
    insert_optional_json_u64(object, "scheduler_wake_count", runtime.scheduler_wake_count);
    insert_optional_json_u64(
        object,
        "scheduler_ready_time_us",
        runtime.scheduler_ready_time_us,
    );
    insert_optional_json_u64(
        object,
        "scheduler_wait_time_us",
        runtime.scheduler_wait_time_us,
    );
    insert_optional_json_u64(
        object,
        "scheduler_wake_coalesce_count",
        runtime.scheduler_wake_coalesce_count,
    );
    insert_optional_json_u64(
        object,
        "output_backpressure_count",
        runtime.output_backpressure_count,
    );
    insert_optional_json_u64(
        object,
        "runtime_filter_installed_count",
        runtime.runtime_filter_installed_count,
    );
    insert_optional_json_u64(
        object,
        "runtime_filter_no_wait_count",
        runtime.runtime_filter_no_wait_count,
    );
    insert_optional_json_u64(object, "grant_bytes", runtime.grant_bytes);
    insert_optional_json_u64(object, "revoked_bytes", runtime.revoked_bytes);
    insert_optional_json_u64(object, "yield_latency_us", runtime.yield_latency_us);
    insert_optional_json_u64(object, "spill_latency_us", runtime.spill_latency_us);
    insert_optional_json_u64(object, "repartition_depth", runtime.repartition_depth);
    insert_optional_json_u64(object, "peak_rss_bytes", runtime.peak_rss_bytes);
    insert_optional_json_u64(object, "output_buffer_bytes", runtime.output_buffer_bytes);
    insert_optional_json_u64(
        object,
        "session_retained_bytes",
        runtime.session_retained_bytes,
    );
}

fn insert_optional_json_u64(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: Option<u64>,
) {
    if let Some(value) = value {
        object.insert(key.to_string(), serde_json::Value::from(value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::explain::types::ExplainRuntimeStats;
    use crate::memory_runtime::MemoryRuntimeStats;

    #[test]
    fn actual_suffix_includes_runtime_observability() {
        let mut stats = HashMap::new();
        stats.insert(
            7,
            ExplainActualStats {
                output_rows: 42,
                loops: 3,
                startup_time_ms: Some(1.0),
                total_time_ms: Some(2.5),
                runtime: ExplainRuntimeStats {
                    spilled: Some(true),
                    peak_memory_bytes: Some(1024),
                    spilled_bytes: Some(2048),
                    allocator_tracking_event_count: Some(12),
                    ..ExplainRuntimeStats::default()
                },
            },
        );

        assert_eq!(
            actual_suffix(&stats, 7),
            " (actual time=1.000..2.500 rows=42 loops=3 spilled=true peak_memory_bytes=1024 spilled_bytes=2048 allocator_tracking_event_count=12)"
        );
    }

    #[test]
    fn actual_json_includes_runtime_observability() {
        let mut stats = HashMap::new();
        stats.insert(
            11,
            ExplainActualStats {
                output_rows: 5,
                loops: 1,
                startup_time_ms: Some(0.25),
                total_time_ms: Some(0.75),
                runtime: ExplainRuntimeStats {
                    spilled: Some(true),
                    peak_memory_bytes: Some(4096),
                    spilled_bytes: Some(8192),
                    scheduler_wait_time_us: Some(17),
                    runtime_filter_installed_count: Some(1),
                    grant_bytes: Some(64),
                    allocator_tracking_event_count: Some(4),
                    ..ExplainRuntimeStats::default()
                },
            },
        );

        let actual = actual_json(&stats, 11);
        assert_eq!(actual["spilled"], serde_json::Value::Bool(true));
        assert_eq!(actual["peak_memory_bytes"], serde_json::Value::from(4096));
        assert_eq!(actual["spilled_bytes"], serde_json::Value::from(8192));
        assert_eq!(
            actual["allocator_tracking_event_count"],
            serde_json::Value::from(4)
        );
        assert_eq!(
            actual["scheduler_wait_time_us"],
            serde_json::Value::from(17)
        );
        assert_eq!(
            actual["runtime_filter_installed_count"],
            serde_json::Value::from(1)
        );
        assert_eq!(actual["grant_bytes"], serde_json::Value::from(64));
    }

    #[test]
    fn profile_summary_json_includes_parallel_runtime_filter_and_memory_schema() {
        let mut operators = HashMap::new();
        operators.insert(
            2,
            ExplainActualStats {
                output_rows: 0,
                loops: 0,
                startup_time_ms: None,
                total_time_ms: None,
                runtime: ExplainRuntimeStats {
                    scheduler_ready_time_us: Some(5),
                    scheduler_wait_time_us: Some(11),
                    scheduler_wake_count: Some(2),
                    scheduler_wake_coalesce_count: Some(1),
                    output_backpressure_count: Some(1),
                    runtime_filter_installed_count: Some(1),
                    runtime_filter_no_wait_count: Some(3),
                    repartition_depth: Some(2),
                    ..ExplainRuntimeStats::default()
                },
            },
        );
        let snapshot = ExplainProfileSnapshot {
            query_id: 77,
            operators,
            events: vec![ExplainProfileEvent {
                query_id: 77,
                sequence: 1,
                pipeline_id: Some(4),
                work_unit_id: Some(40),
                operator_id: 2,
                thread_id: Some(1),
                total_threads: Some(4),
                morsel_range: Some(ProfileMorselRange::new("chunk", 3, 4)),
                phase: "operator",
                rows: 9,
                bytes: 0,
                start_time_ms: 1.0,
                end_time_ms: 3.0,
                wait_reason: None,
                memory_class: None,
            }],
            control_regions: Vec::new(),
            query_memory: Some(MemoryRuntimeStats {
                issued_bytes: 1024,
                revocable_bytes: 256,
                reclaimed_bytes: 128,
                spilled_bytes: 4096,
                spill_latency_us: 13,
                reclaim_latency_us: 7,
                ..MemoryRuntimeStats::default()
            }),
        };

        let summary = profile_summary_json(&snapshot, 10.0);
        assert_eq!(
            summary["parallelism"]["max_threads"],
            serde_json::Value::from(4)
        );
        assert_eq!(
            summary["parallelism"]["observed_workers"],
            serde_json::Value::from(1)
        );
        assert_eq!(
            summary["parallelism"]["ready_time_us"],
            serde_json::Value::from(5)
        );
        assert_eq!(
            summary["runtime_filters"]["installed_count"],
            serde_json::Value::from(1)
        );
        assert_eq!(
            summary["runtime_filters"]["no_wait_fallback_count"],
            serde_json::Value::from(3)
        );
        assert_eq!(
            summary["memory"]["grant_bytes"],
            serde_json::Value::from(1024)
        );
        assert_eq!(
            summary["memory"]["revoked_bytes"],
            serde_json::Value::from(128)
        );
        assert_eq!(
            summary["memory"]["spill_bytes"],
            serde_json::Value::from(4096)
        );

        let events = profile_events_json(&snapshot.events);
        assert_eq!(events[0]["query_id"], serde_json::Value::from(77));
        assert_eq!(events[0]["morsel_range"]["kind"], "chunk");
        assert_eq!(
            events[0]["time"]["duration_ms"],
            serde_json::Value::from(2.0)
        );
    }
}
