// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search maintenance scheduler priority micro benchmark.

use std::env;
use std::fs;
use std::path::Path;

use divan::{black_box, Bencher};
use paro_storage::search::bench_support::{
    run_required_scheduler_bench, RequiredSchedulerBenchConfig, RequiredSchedulerBenchSummary,
};

const STRUCTURED_SAMPLE_COUNT: usize = 50;
const OPPORTUNISTIC_COUNT: usize = 64;
const CASE_ID: &str = "required_catch_up_preempts_64_opportunistic";

fn main() {
    if let Ok(path) = env::var("PARO_DIVAN_JSON_OUT") {
        write_structured_results(Path::new(&path));
        return;
    }
    divan::main();
}

#[divan::bench(sample_count = 10)]
fn required_catch_up_preempts_64_opportunistic(bencher: Bencher) {
    let config = RequiredSchedulerBenchConfig {
        opportunistic_count: OPPORTUNISTIC_COUNT,
    };
    bencher.bench_local(|| {
        let summary = run_required_scheduler_bench(config);
        black_box(summary.opportunistic_tasks_before_required)
    });
}

fn write_structured_results(path: &Path) {
    let sample_count = structured_sample_count();
    for _ in 0..structured_warmup_count() {
        black_box(run_once());
    }

    let mut samples_ms = Vec::with_capacity(sample_count);
    let mut required_dispatch_delay_ms = Vec::with_capacity(sample_count);
    let mut opportunistic_tasks_before_required = Vec::with_capacity(sample_count);
    let mut queued_task_count = Vec::with_capacity(sample_count);
    let mut required_admitted = Vec::with_capacity(sample_count);

    for _ in 0..sample_count {
        let summary = run_once();
        samples_ms.push(summary.required_dispatch_delay_ms);
        required_dispatch_delay_ms.push(summary.required_dispatch_delay_ms);
        opportunistic_tasks_before_required
            .push(summary.opportunistic_tasks_before_required as u64);
        queued_task_count.push(summary.queued_task_count as u64);
        required_admitted.push(u64::from(summary.required_admitted));
    }

    let payload = serde_json::json!({
        "schema_version": 1,
        "kind": "divan_bench_result",
        "crate": "paro-storage",
        "bench": "search_maintenance_scheduler",
        "sample_count": sample_count,
        "benches": [{
            "id": CASE_ID,
            "items": OPPORTUNISTIC_COUNT + 1,
            "samples_ms": samples_ms,
            "audit": {
                "required_dispatch_delay_ms": required_dispatch_delay_ms,
                "opportunistic_tasks_before_required": opportunistic_tasks_before_required,
                "queued_task_count": queued_task_count,
                "required_admitted": required_admitted,
            },
        }],
    });
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("structured Divan result directory should be created");
    }
    let bytes =
        serde_json::to_vec_pretty(&payload).expect("structured Divan result should serialize");
    fs::write(path, bytes).expect("structured Divan result should be written");
}

fn run_once() -> RequiredSchedulerBenchSummary {
    run_required_scheduler_bench(RequiredSchedulerBenchConfig {
        opportunistic_count: OPPORTUNISTIC_COUNT,
    })
}

fn structured_sample_count() -> usize {
    env::var("PARO_DIVAN_SAMPLE_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(STRUCTURED_SAMPLE_COUNT)
}

fn structured_warmup_count() -> usize {
    env::var("PARO_DIVAN_WARMUP")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1)
}
