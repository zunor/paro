// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! EXPLAIN ANALYZE rendering for typed runtime programs.

use std::collections::HashMap;
use std::fmt::Write;

use paro_planner::operator::{ExplainFormat, ExplainSpec};

use crate::explain::profiler::{ExplainProfileSnapshot, ExplainProfiler};
use crate::explain::types::{
    ExplainActualStats, ExplainControlRegionStats, ExplainNodeId, ExplainRecursiveCteStats,
};
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
            let mut lines = render_explain_analyze_text(target, &stats);
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
        }
        StatementProgram::Utility(utility) => {
            lines.push(format!("UTILITY {:?}", utility.spec));
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
        "format_version": 1,
        "mode": "analyze",
        "operators": operators,
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
    if let Some(value) = actual.runtime.spilled {
        write!(&mut suffix, " spilled={value}").expect("write to String");
    }
    if let Some(value) = actual.runtime.peak_memory_bytes {
        write!(&mut suffix, " peak_memory_bytes={value}").expect("write to String");
    }
    if let Some(value) = actual.runtime.spilled_bytes {
        write!(&mut suffix, " spilled_bytes={value}").expect("write to String");
    }
    suffix.push(')');
    suffix
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
    actual
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::explain::types::ExplainRuntimeStats;

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
                    ..ExplainRuntimeStats::default()
                },
            },
        );

        assert_eq!(
            actual_suffix(&stats, 7),
            " (actual time=1.000..2.500 rows=42 loops=3 spilled=true peak_memory_bytes=1024 spilled_bytes=2048)"
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
                    ..ExplainRuntimeStats::default()
                },
            },
        );

        let actual = actual_json(&stats, 11);
        assert_eq!(actual["spilled"], serde_json::Value::Bool(true));
        assert_eq!(actual["peak_memory_bytes"], serde_json::Value::from(4096));
        assert_eq!(actual["spilled_bytes"], serde_json::Value::from(8192));
    }
}
