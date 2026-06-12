// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Foreground query I/O under admission-bounded catch-up micro benchmark.

use std::env;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use divan::{black_box, Bencher};
use paro_storage::search::bench_support::run_foreground_io_admission_bench;
use paro_storage::search::{
    ArtifactFileId, ArtifactLocation, SearchIndexKind, SidecarArtifactStore, SidecarReaderCache,
    SidecarReaderRequest, SIDECAR_PACKAGE_CODEC,
};
use tempfile::TempDir;

const STRUCTURED_SAMPLE_COUNT: usize = 50;
const FOREGROUND_PACKAGE_COUNT: usize = 32;
const FOREGROUND_ARTIFACTS_PER_PACKAGE: usize = 4;
const FOREGROUND_ARTIFACT_BYTES: usize = 64 * 1024;
const BACKGROUND_PACKAGE_COUNT: usize = 8;
const BACKGROUND_ARTIFACTS_PER_PACKAGE: usize = 4;
const BACKGROUND_ARTIFACT_BYTES: usize = 4 * 1024;
const CASE_ID: &str = "foreground_sidecar_io_with_bounded_catch_up";

fn main() {
    if let Ok(path) = env::var("PARO_DIVAN_JSON_OUT") {
        write_structured_results(Path::new(&path));
        return;
    }
    divan::main();
}

struct ForegroundIoFixture {
    _dir: TempDir,
    store: SidecarArtifactStore,
    foreground_artifacts: Arc<Vec<ArtifactLocation>>,
    background_artifacts: Arc<Vec<ArtifactLocation>>,
    background_read_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct ForegroundIoSample {
    baseline_ms: f64,
    contended_ms: f64,
    degradation_percent: f64,
    background_read_bytes: u64,
    foreground_bytes: u64,
    foreground_reserved_bytes: u64,
    admitted_background_read_bytes: u64,
    oversized_background_deferred: u64,
}

impl ForegroundIoFixture {
    fn new() -> Self {
        let dir = TempDir::new().expect("foreground io bench temp dir");
        let store = SidecarArtifactStore::new(dir.path());
        let foreground_artifacts = create_sidecar_artifacts(
            &store,
            1,
            FOREGROUND_PACKAGE_COUNT,
            FOREGROUND_ARTIFACTS_PER_PACKAGE,
            FOREGROUND_ARTIFACT_BYTES,
        );
        let background_artifacts = create_sidecar_artifacts(
            &store,
            2,
            BACKGROUND_PACKAGE_COUNT,
            BACKGROUND_ARTIFACTS_PER_PACKAGE,
            BACKGROUND_ARTIFACT_BYTES,
        );
        let background_read_bytes = (BACKGROUND_PACKAGE_COUNT
            * BACKGROUND_ARTIFACTS_PER_PACKAGE
            * BACKGROUND_ARTIFACT_BYTES) as u64;
        Self {
            _dir: dir,
            store,
            foreground_artifacts: Arc::new(foreground_artifacts),
            background_artifacts: Arc::new(background_artifacts),
            background_read_bytes,
        }
    }

    fn run_once(&self) -> ForegroundIoSample {
        let admission = run_foreground_io_admission_bench();
        let baseline_ms = self.run_foreground_query();
        let contended_ms = self.run_foreground_query_with_background_catch_up();
        let degradation_percent = if baseline_ms > 0.0 {
            ((contended_ms / baseline_ms) - 1.0).max(0.0) * 100.0
        } else {
            0.0
        };
        ForegroundIoSample {
            baseline_ms,
            contended_ms,
            degradation_percent,
            background_read_bytes: self.background_read_bytes,
            foreground_bytes: foreground_bytes(),
            foreground_reserved_bytes: admission.foreground_reserved_bytes,
            admitted_background_read_bytes: admission.admitted_background_read_bytes,
            oversized_background_deferred: u64::from(admission.oversized_background_deferred),
        }
    }

    fn run_foreground_query(&self) -> f64 {
        let cache = SidecarReaderCache::new(self.store.clone());
        let started_at = Instant::now();
        touch_foreground_artifacts(&cache, &self.foreground_artifacts);
        started_at.elapsed().as_secs_f64() * 1000.0
    }

    fn run_foreground_query_with_background_catch_up(&self) -> f64 {
        let barrier = Arc::new(Barrier::new(2));
        let background_barrier = Arc::clone(&barrier);
        let background_store = self.store.clone();
        let background_artifacts = Arc::clone(&self.background_artifacts);
        let handle = thread::spawn(move || {
            background_barrier.wait();
            read_background_catch_up(&background_store, &background_artifacts)
        });

        let cache = SidecarReaderCache::new(self.store.clone());
        barrier.wait();
        let started_at = Instant::now();
        touch_foreground_artifacts(&cache, &self.foreground_artifacts);
        let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
        black_box(handle.join().expect("background catch-up thread"));
        elapsed_ms
    }
}

#[divan::bench(sample_count = 10)]
fn foreground_sidecar_io_with_bounded_catch_up(bencher: Bencher) {
    let fixture = ForegroundIoFixture::new();
    bencher.bench_local(|| black_box(fixture.run_once().degradation_percent));
}

fn write_structured_results(path: &Path) {
    let fixture = ForegroundIoFixture::new();
    let sample_count = structured_sample_count();
    for _ in 0..structured_warmup_count() {
        black_box(fixture.run_once());
    }

    let mut samples_ms = Vec::with_capacity(sample_count);
    let mut baseline_ms = Vec::with_capacity(sample_count);
    let mut contended_ms = Vec::with_capacity(sample_count);
    let mut degradation_percent = Vec::with_capacity(sample_count);
    let mut background_read_bytes = Vec::with_capacity(sample_count);
    let mut foreground_bytes = Vec::with_capacity(sample_count);
    let mut foreground_reserved_bytes = Vec::with_capacity(sample_count);
    let mut admitted_background_read_bytes = Vec::with_capacity(sample_count);
    let mut oversized_background_deferred = Vec::with_capacity(sample_count);

    for _ in 0..sample_count {
        let sample = fixture.run_once();
        samples_ms.push(sample.contended_ms);
        baseline_ms.push(sample.baseline_ms);
        contended_ms.push(sample.contended_ms);
        degradation_percent.push(sample.degradation_percent);
        background_read_bytes.push(sample.background_read_bytes);
        foreground_bytes.push(sample.foreground_bytes);
        foreground_reserved_bytes.push(sample.foreground_reserved_bytes);
        admitted_background_read_bytes.push(sample.admitted_background_read_bytes);
        oversized_background_deferred.push(sample.oversized_background_deferred);
    }

    let payload = serde_json::json!({
        "schema_version": 1,
        "kind": "divan_bench_result",
        "crate": "paro-storage",
        "bench": "search_foreground_io",
        "sample_count": sample_count,
        "benches": [{
            "id": CASE_ID,
            "items": FOREGROUND_PACKAGE_COUNT * FOREGROUND_ARTIFACTS_PER_PACKAGE,
            "samples_ms": samples_ms,
            "audit": {
                "foreground_baseline_ms": baseline_ms,
                "foreground_contended_ms": contended_ms,
                "foreground_io_degradation_percent": degradation_percent,
                "background_catch_up_read_bytes": background_read_bytes,
                "foreground_query_bytes": foreground_bytes,
                "foreground_reserved_bytes": foreground_reserved_bytes,
                "admitted_background_read_bytes": admitted_background_read_bytes,
                "oversized_background_deferred": oversized_background_deferred,
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

fn create_sidecar_artifacts(
    store: &SidecarArtifactStore,
    definition_id: u64,
    package_count: usize,
    artifacts_per_package: usize,
    artifact_bytes: usize,
) -> Vec<ArtifactLocation> {
    let mut artifacts = Vec::with_capacity(package_count * artifacts_per_package);
    for package_index in 0..package_count {
        let file_id = ArtifactFileId {
            definition_id,
            generation_id: 1,
            package_index: package_index as u32,
        };
        let mut writer = store
            .create_package_writer(file_id)
            .expect("create foreground io sidecar package");
        for artifact_index in 0..artifacts_per_package {
            let bytes = artifact_payload(package_index, artifact_index, artifact_bytes);
            artifacts.push(
                writer
                    .append_artifact(&bytes)
                    .expect("append foreground io sidecar artifact"),
            );
        }
        writer.finalize().expect("finalize foreground io package");
    }
    artifacts
}

fn artifact_payload(package_index: usize, artifact_index: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|offset| {
            package_index
                .wrapping_mul(31)
                .wrapping_add(artifact_index.wrapping_mul(17))
                .wrapping_add(offset)
                .to_le_bytes()[0]
        })
        .collect()
}

fn touch_foreground_artifacts(cache: &SidecarReaderCache, artifacts: &[ArtifactLocation]) {
    let mut checksum = 0usize;
    for location in artifacts {
        let artifact = cache
            .open(SidecarReaderRequest {
                location,
                artifact_format_version: 1,
                provider: SearchIndexKind::FullText,
                codec: SIDECAR_PACKAGE_CODEC,
            })
            .expect("open foreground sidecar artifact");
        for byte in artifact.bytes().iter().step_by(64) {
            checksum = checksum.wrapping_add(usize::from(*byte));
        }
    }
    black_box(checksum);
}

fn read_background_catch_up(store: &SidecarArtifactStore, artifacts: &[ArtifactLocation]) -> usize {
    let mut bytes_read = 0usize;
    for location in artifacts {
        let bytes = store
            .read_artifact(location)
            .expect("read background catch-up sidecar artifact");
        bytes_read = bytes_read.saturating_add(bytes.len());
    }
    black_box(bytes_read)
}

const fn foreground_bytes() -> u64 {
    (FOREGROUND_PACKAGE_COUNT * FOREGROUND_ARTIFACTS_PER_PACKAGE * FOREGROUND_ARTIFACT_BYTES) as u64
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
