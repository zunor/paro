// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search row fetch late-materialization micro benchmark.

use std::env;
use std::fs;
use std::path::Path;

use divan::{black_box, Bencher};
use paro_storage::search::bench_support::{RowFetchBenchConfig, RowFetchBenchFixture};

const STRUCTURED_SAMPLE_COUNT: usize = 50;
const ROW_COUNT: usize = 65_536;
const CASES: [(&str, usize); 4] = [
    ("row_fetch_1_candidate_fixed_varlen", 1),
    ("row_fetch_8_candidates_fixed_varlen", 8),
    ("row_fetch_32_candidates_fixed_varlen", 32),
    ("row_fetch_128_candidates_fixed_varlen", 128),
];

fn main() {
    if let Ok(path) = env::var("PARO_DIVAN_JSON_OUT") {
        write_structured_results(Path::new(&path));
        return;
    }
    divan::main();
}

#[divan::bench(sample_count = 10)]
fn row_fetch_128_candidates_fixed_varlen(bencher: Bencher) {
    bench_case(bencher, 128);
}

#[divan::bench(sample_count = 10)]
fn row_fetch_32_candidates_fixed_varlen(bencher: Bencher) {
    bench_case(bencher, 32);
}

#[divan::bench(sample_count = 10)]
fn row_fetch_8_candidates_fixed_varlen(bencher: Bencher) {
    bench_case(bencher, 8);
}

#[divan::bench(sample_count = 10)]
fn row_fetch_1_candidate_fixed_varlen(bencher: Bencher) {
    bench_case(bencher, 1);
}

fn bench_case(bencher: Bencher, candidate_count: usize) {
    let fixture = fixture(candidate_count);
    bencher.bench_local(|| {
        let summary = fixture.run_once().expect("run row fetch bench");
        black_box(summary.projected_bytes)
    });
}

fn write_structured_results(path: &Path) {
    let benches = CASES
        .into_iter()
        .map(|(case_id, candidate_count)| measure_case(case_id, candidate_count))
        .collect::<Vec<_>>();
    let payload = serde_json::json!({
        "schema_version": 1,
        "kind": "divan_bench_result",
        "crate": "paro-storage",
        "bench": "search_row_fetch",
        "sample_count": structured_sample_count(),
        "benches": benches,
    });
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("structured Divan result directory should be created");
    }
    let bytes =
        serde_json::to_vec_pretty(&payload).expect("structured Divan result should serialize");
    fs::write(path, bytes).expect("structured Divan result should be written");
}

fn measure_case(case_id: &str, candidate_count: usize) -> serde_json::Value {
    let fixture = fixture(candidate_count);
    let sample_count = structured_sample_count();
    for _ in 0..structured_warmup_count() {
        black_box(fixture.run_once().expect("warm row fetch bench"));
    }

    let mut samples_ms = Vec::with_capacity(sample_count);
    let mut rows = Vec::with_capacity(sample_count);
    let mut projected_columns = Vec::with_capacity(sample_count);
    let mut segment_groups = Vec::with_capacity(sample_count);
    let mut column_batches = Vec::with_capacity(sample_count);
    let mut fixed_width_column_batches = Vec::with_capacity(sample_count);
    let mut varlen_column_batches = Vec::with_capacity(sample_count);
    let mut projected_bytes = Vec::with_capacity(sample_count);
    let mut page_run_seeks = Vec::with_capacity(sample_count);
    let mut search_layer_varlen_fallback_seek_count = Vec::with_capacity(sample_count);
    let mut decoded_page_cache_hits = Vec::with_capacity(sample_count);
    let mut decoded_page_cache_misses = Vec::with_capacity(sample_count);
    let mut decoded_page_cache_first_touch_admissions = Vec::with_capacity(sample_count);
    let mut decoded_page_cache_probation_promotions = Vec::with_capacity(sample_count);
    let mut decoded_page_cache_policy_rejections = Vec::with_capacity(sample_count);

    for _ in 0..sample_count {
        let summary = fixture.run_once().expect("run row fetch bench");
        samples_ms.push(summary.elapsed_ms);
        rows.push(summary.rows as u64);
        projected_columns.push(summary.projected_columns as u64);
        segment_groups.push(summary.segment_groups as u64);
        column_batches.push(summary.column_batches as u64);
        fixed_width_column_batches.push(summary.fixed_width_column_batches as u64);
        varlen_column_batches.push(summary.varlen_column_batches as u64);
        projected_bytes.push(summary.projected_bytes as u64);
        page_run_seeks.push(summary.column_read_by_rowids_page_run_seeks as u64);
        search_layer_varlen_fallback_seek_count
            .push(summary.search_layer_varlen_fallback_seek_count as u64);
        decoded_page_cache_hits.push(summary.decoded_page_cache_hits);
        decoded_page_cache_misses.push(summary.decoded_page_cache_misses);
        decoded_page_cache_first_touch_admissions
            .push(summary.decoded_page_cache_first_touch_admissions);
        decoded_page_cache_probation_promotions
            .push(summary.decoded_page_cache_probation_promotions);
        decoded_page_cache_policy_rejections.push(summary.decoded_page_cache_policy_rejections);
    }

    serde_json::json!({
        "id": case_id,
        "items": candidate_count,
        "samples_ms": samples_ms,
        "audit": {
            "rows": rows,
            "projected_columns": projected_columns,
            "segment_groups": segment_groups,
            "column_batches": column_batches,
            "fixed_width_column_batches": fixed_width_column_batches,
            "varlen_column_batches": varlen_column_batches,
            "projected_bytes": projected_bytes,
            "column_read_by_rowids_page_run_seeks": page_run_seeks,
            "search_layer_varlen_fallback_seek_count": search_layer_varlen_fallback_seek_count,
            "decoded_page_cache_hits": decoded_page_cache_hits,
            "decoded_page_cache_misses": decoded_page_cache_misses,
            "decoded_page_cache_first_touch_admissions": decoded_page_cache_first_touch_admissions,
            "decoded_page_cache_probation_promotions": decoded_page_cache_probation_promotions,
            "decoded_page_cache_policy_rejections": decoded_page_cache_policy_rejections,
        },
    })
}

fn fixture(candidate_count: usize) -> RowFetchBenchFixture {
    RowFetchBenchFixture::new(RowFetchBenchConfig {
        row_count: ROW_COUNT,
        candidate_count,
    })
    .expect("create row fetch bench fixture")
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
