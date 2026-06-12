// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search sidecar reader open-count micro benchmark.

use std::env;
use std::fs;
use std::path::Path;
use std::time::Instant;

use divan::{black_box, Bencher};
use paro_storage::metrics::storage_metrics;
use paro_storage::search::{
    SearchIndexKind, SidecarArtifactStore, SidecarReaderCache, SidecarReaderRequest,
    SIDECAR_PACKAGE_CODEC,
};
use tempfile::TempDir;

const STRUCTURED_SAMPLE_COUNT: usize = 30;
const PACKAGE_COUNT: usize = 32;
const ARTIFACTS_PER_PACKAGE: usize = 4;
const CASE_ID: &str = "sidecar_reader_32_packages_4_artifacts";

fn main() {
    if let Ok(path) = env::var("PARO_DIVAN_JSON_OUT") {
        write_structured_results(Path::new(&path));
        return;
    }
    divan::main();
}

struct SidecarOpenFixture {
    _dir: TempDir,
    store: SidecarArtifactStore,
    artifacts: Vec<paro_storage::search::ArtifactLocation>,
}

#[derive(Debug, Clone, Copy)]
struct SidecarOpenSample {
    elapsed_ms: f64,
    open_count: u64,
    cache_misses: u64,
    format_dispatch_count: u64,
    active_shard_count: u64,
}

impl SidecarOpenFixture {
    fn new() -> Self {
        let dir = TempDir::new().expect("sidecar open bench temp dir");
        let store = SidecarArtifactStore::new(dir.path());
        let mut artifacts = Vec::with_capacity(PACKAGE_COUNT * ARTIFACTS_PER_PACKAGE);
        for package_index in 0..PACKAGE_COUNT {
            let file_id = paro_storage::search::ArtifactFileId {
                definition_id: 1,
                generation_id: 1,
                package_index: package_index as u32,
            };
            let mut writer = store
                .create_package_writer(file_id)
                .expect("create sidecar package");
            for artifact_index in 0..ARTIFACTS_PER_PACKAGE {
                let bytes = format!("package-{package_index}-artifact-{artifact_index}");
                artifacts.push(
                    writer
                        .append_artifact(bytes.as_bytes())
                        .expect("append sidecar artifact"),
                );
            }
            writer.finalize().expect("finalize sidecar package");
        }
        Self {
            _dir: dir,
            store,
            artifacts,
        }
    }

    fn run_once(&self) -> SidecarOpenSample {
        let before = storage_metrics().snapshot();
        let cache = SidecarReaderCache::new(self.store.clone());
        let started_at = Instant::now();
        let mut bytes = 0usize;
        for location in &self.artifacts {
            let artifact = cache
                .open(SidecarReaderRequest {
                    location,
                    artifact_format_version: 1,
                    provider: SearchIndexKind::FullText,
                    codec: SIDECAR_PACKAGE_CODEC,
                })
                .expect("open sidecar artifact");
            bytes ^= artifact.bytes().len();
        }
        black_box(bytes);
        let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
        let after = storage_metrics().snapshot();
        let before_sidecar = sidecar_counters(&before);
        let after_sidecar = sidecar_counters(&after);
        SidecarOpenSample {
            elapsed_ms,
            open_count: after_sidecar.0.saturating_sub(before_sidecar.0),
            cache_misses: after_sidecar.1.saturating_sub(before_sidecar.1),
            format_dispatch_count: after_sidecar.2.saturating_sub(before_sidecar.2),
            active_shard_count: PACKAGE_COUNT as u64,
        }
    }
}

#[divan::bench(sample_count = 10)]
fn sidecar_reader_32_packages_4_artifacts(bencher: Bencher) {
    let fixture = SidecarOpenFixture::new();
    bencher.bench_local(|| black_box(fixture.run_once().open_count));
}

fn write_structured_results(path: &Path) {
    let fixture = SidecarOpenFixture::new();
    let sample_count = structured_sample_count();
    for _ in 0..structured_warmup_count() {
        black_box(fixture.run_once());
    }

    let mut samples_ms = Vec::with_capacity(sample_count);
    let mut open_count = Vec::with_capacity(sample_count);
    let mut cache_misses = Vec::with_capacity(sample_count);
    let mut format_dispatch_count = Vec::with_capacity(sample_count);
    let mut active_shard_count = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let sample = fixture.run_once();
        samples_ms.push(sample.elapsed_ms);
        open_count.push(sample.open_count);
        cache_misses.push(sample.cache_misses);
        format_dispatch_count.push(sample.format_dispatch_count);
        active_shard_count.push(sample.active_shard_count);
    }

    let payload = serde_json::json!({
        "schema_version": 1,
        "kind": "divan_bench_result",
        "crate": "paro-storage",
        "bench": "search_sidecar_open",
        "sample_count": sample_count,
        "benches": [{
            "id": CASE_ID,
            "items": PACKAGE_COUNT * ARTIFACTS_PER_PACKAGE,
            "samples_ms": samples_ms,
            "audit": {
                "chunk_count": PACKAGE_COUNT * ARTIFACTS_PER_PACKAGE,
                "sidecar_open_count": open_count,
                "sidecar_cache_miss_count": cache_misses,
                "sidecar_format_dispatch_count": format_dispatch_count,
                "active_shard_count": active_shard_count,
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

fn sidecar_counters(snapshot: &paro_storage::metrics::StorageMetricsSnapshot) -> (u64, u64, u64) {
    snapshot
        .search_sidecar_reader_by_key
        .iter()
        .filter(|series| {
            series.key.provider == SearchIndexKind::FullText
                && series.key.codec == SIDECAR_PACKAGE_CODEC
        })
        .fold((0, 0, 0), |acc, series| {
            (
                acc.0.saturating_add(series.counters.open_count_total),
                acc.1.saturating_add(series.counters.cache_misses_total),
                acc.2.saturating_add(series.counters.format_dispatch_total),
            )
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
