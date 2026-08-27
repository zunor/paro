// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search manifest open/replay micro benchmarks.

use std::env;
use std::fs;
use std::path::Path;
use std::time::Instant;

use divan::{black_box, Bencher};
use paro_storage::search::bench_support::{
    open_manifest_bench_fixture, open_manifest_bench_fixture_with_manifest_bytes,
    prepare_manifest_open_bench_fixture, ManifestBenchCodec, ManifestOpenBenchConfig,
    ManifestOpenBenchSummary,
};
use tempfile::TempDir;

const STRUCTURED_SAMPLE_COUNT: usize = 50;
const CASES: &[ManifestOpenCase] = &[
    ManifestOpenCase::new(
        "manifest_open_json_delta_0",
        0,
        1,
        0,
        ManifestBenchCodec::JsonDebugV3,
    ),
    ManifestOpenCase::new(
        "manifest_open_json_delta_32",
        32,
        1,
        0,
        ManifestBenchCodec::JsonDebugV3,
    ),
    ManifestOpenCase::new(
        "manifest_open_json_delta_128",
        128,
        1,
        0,
        ManifestBenchCodec::JsonDebugV3,
    ),
    ManifestOpenCase::new(
        "manifest_open_json_32_shards_32_deltas",
        32,
        32,
        1,
        ManifestBenchCodec::JsonDebugV3,
    ),
    ManifestOpenCase::new(
        "manifest_open_binary_32_shards_32_deltas",
        32,
        32,
        1,
        ManifestBenchCodec::BinaryV3,
    ),
];

fn main() {
    if let Ok(path) = env::var("PARO_DIVAN_JSON_OUT") {
        write_structured_results(Path::new(&path));
        return;
    }
    divan::main();
}

#[derive(Debug, Clone, Copy)]
struct ManifestOpenCase {
    id: &'static str,
    delta_count: usize,
    shard_count: usize,
    entries_per_shard: usize,
    codec: ManifestBenchCodec,
}

impl ManifestOpenCase {
    const fn new(
        id: &'static str,
        delta_count: usize,
        shard_count: usize,
        entries_per_shard: usize,
        codec: ManifestBenchCodec,
    ) -> Self {
        Self {
            id,
            delta_count,
            shard_count,
            entries_per_shard,
            codec,
        }
    }

    const fn config(self) -> ManifestOpenBenchConfig {
        ManifestOpenBenchConfig::new(self.delta_count)
            .with_shards(self.shard_count, self.entries_per_shard)
            .with_codec(self.codec)
    }
}

struct ManifestOpenFixture {
    _dir: TempDir,
    config: ManifestOpenBenchConfig,
    manifest_bytes: u64,
}

impl ManifestOpenFixture {
    fn new(case: ManifestOpenCase) -> Self {
        let dir = TempDir::new().expect("create manifest open bench temp dir");
        let config = case.config();
        prepare_manifest_open_bench_fixture(dir.path(), config)
            .expect("prepare manifest open bench fixture");
        let manifest_bytes =
            open_manifest_bench_fixture(dir.path(), config.definition_id, config.codec)
                .expect("precompute manifest open bench bytes")
                .manifest_bytes;
        Self {
            _dir: dir,
            config,
            manifest_bytes,
        }
    }

    fn run_once(&self) -> ManifestOpenBenchSummary {
        open_manifest_bench_fixture_with_manifest_bytes(
            self._dir.path(),
            self.config.definition_id,
            self.config.codec,
            Some(self.manifest_bytes),
        )
        .expect("open manifest bench fixture")
    }

    fn run_once_checksum(&self) -> usize {
        let summary = self.run_once();
        summary.artifact_count
            ^ summary.tail_pending_count
            ^ summary.recent_delta_count
            ^ summary.shard_count
            ^ usize::try_from(summary.manifest_bytes).unwrap_or(usize::MAX)
    }
}

#[divan::bench(sample_count = 10)]
fn manifest_open_json_delta_0(bencher: Bencher) {
    let fixture = ManifestOpenFixture::new(CASES[0]);
    bencher.bench_local(|| black_box(fixture.run_once_checksum()));
}

#[divan::bench(sample_count = 10)]
fn manifest_open_json_delta_32(bencher: Bencher) {
    let fixture = ManifestOpenFixture::new(CASES[1]);
    bencher.bench_local(|| black_box(fixture.run_once_checksum()));
}

#[divan::bench(sample_count = 10)]
fn manifest_open_json_delta_128(bencher: Bencher) {
    let fixture = ManifestOpenFixture::new(CASES[2]);
    bencher.bench_local(|| black_box(fixture.run_once_checksum()));
}

#[divan::bench(sample_count = 10)]
fn manifest_open_json_32_shards_32_deltas(bencher: Bencher) {
    let fixture = ManifestOpenFixture::new(CASES[3]);
    bencher.bench_local(|| black_box(fixture.run_once_checksum()));
}

#[divan::bench(sample_count = 10)]
fn manifest_open_binary_32_shards_32_deltas(bencher: Bencher) {
    let fixture = ManifestOpenFixture::new(CASES[4]);
    bencher.bench_local(|| black_box(fixture.run_once_checksum()));
}

fn write_structured_results(path: &Path) {
    let sample_count = structured_sample_count();
    let selected = structured_bench_filter();
    let mut benches = Vec::new();

    for case in CASES {
        if !structured_bench_selected(&selected, case.id) {
            continue;
        }
        let fixture = ManifestOpenFixture::new(*case);
        benches.push(measure_structured_bench(case, sample_count, || {
            fixture.run_once()
        }));
    }

    let payload = serde_json::json!({
        "schema_version": 1,
        "kind": "divan_bench_result",
        "crate": "paro-storage",
        "bench": "search_manifest_open",
        "sample_count": sample_count,
        "benches": benches,
    });
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("structured Divan result directory should be created");
    }
    let bytes =
        serde_json::to_vec_pretty(&payload).expect("structured Divan result should serialize");
    fs::write(path, bytes).expect("structured Divan result should be written");
}

fn measure_structured_bench<F>(
    case: &ManifestOpenCase,
    sample_count: usize,
    mut run_once: F,
) -> serde_json::Value
where
    F: FnMut() -> ManifestOpenBenchSummary,
{
    for _ in 0..structured_warmup_count() {
        black_box(run_once().manifest_bytes);
    }

    let mut samples_ms = Vec::with_capacity(sample_count);
    let mut manifest_bytes = Vec::with_capacity(sample_count);
    let mut fragment_counts = Vec::with_capacity(sample_count);
    let mut shard_counts = Vec::with_capacity(sample_count);
    let mut delta_counts = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let start = Instant::now();
        let summary = run_once();
        black_box(summary.artifact_count);
        samples_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        manifest_bytes.push(summary.manifest_bytes);
        shard_counts.push(summary.shard_count as u64);
        delta_counts.push(summary.recent_delta_count as u64);
        fragment_counts.push(1 + summary.shard_count as u64 + summary.recent_delta_count as u64);
    }

    serde_json::json!({
        "id": case.id,
        "items": case.delta_count + case.shard_count,
        "samples_ms": samples_ms,
        "audit": {
            "chunk_count": (1 + case.shard_count + case.delta_count).max(1),
            "codec_label": case.codec.label(),
            "manifest_open_bytes_total": manifest_bytes,
            "manifest_open_fragment_count": fragment_counts,
            "manifest_open_shard_count": shard_counts,
            "manifest_open_delta_count": delta_counts,
        },
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

fn structured_bench_filter() -> Option<Vec<String>> {
    let items = env::var("PARO_DIVAN_BENCH_FILTER").ok()?;
    let selected = items
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    (!selected.is_empty()).then_some(selected)
}

fn structured_bench_selected(selected: &Option<Vec<String>>, id: &str) -> bool {
    match selected {
        Some(items) => items.iter().any(|item| item == id),
        None => true,
    }
}
