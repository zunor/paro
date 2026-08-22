// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search writer-side derived-state write amplification benchmarks.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use divan::{black_box, Bencher};
use paro_common::chunk::Chunk;
use paro_common::test_utils::{test_allocator, test_embeddings_vector, test_string_vector};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_storage::index::hnsw::DistanceMetric;
use paro_storage::metrics::storage_metrics;
use paro_storage::rowset::SparseVector;
use paro_storage::search::bench_support::manifest_fragment_bytes;
use paro_storage::search::{SearchFreshnessPolicy, SearchIndexDefinition, SearchIndexKind};
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::TableHandle;
use tempfile::TempDir;

const STRUCTURED_SAMPLE_COUNT: usize = 30;
const SEARCH_ROWS: usize = 8192;
const HNSW_ROWS: usize = 4096;
const FULLTEXT_BYTES_PER_ROW: u64 = 64;
const SPARSE_NNZ: usize = 32;
const HNSW_DIMENSION: usize = 128;
const CASES: &[WriteCase] = &[
    WriteCase::new(
        "fulltext_inline_write_64b_text",
        SearchIndexKind::FullText,
        SEARCH_ROWS,
    ),
    WriteCase::new(
        "sparse_inline_write_32_nnz",
        SearchIndexKind::Sparse,
        SEARCH_ROWS,
    ),
    WriteCase::new(
        "hnsw_schema_seed_inline_write",
        SearchIndexKind::Hnsw,
        HNSW_ROWS,
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
struct WriteCase {
    id: &'static str,
    provider: SearchIndexKind,
    rows: usize,
}

impl WriteCase {
    const fn new(id: &'static str, provider: SearchIndexKind, rows: usize) -> Self {
        Self { id, provider, rows }
    }
}

#[derive(Debug, Clone, Copy)]
struct WriteSample {
    elapsed_ms: f64,
    rows_per_second: f64,
    artifact_build_cpu_us_per_row: f64,
    artifact_bytes: u64,
    manifest_publish_bytes: u64,
    write_amplification: f64,
    segment_file_open_count: u64,
    segment_file_count: u64,
}

#[divan::bench(sample_count = 10)]
fn fulltext_inline_write_64b_text(bencher: Bencher) {
    bench_case(bencher, CASES[0]);
}

#[divan::bench(sample_count = 10)]
fn sparse_inline_write_32_nnz(bencher: Bencher) {
    bench_case(bencher, CASES[1]);
}

#[divan::bench(sample_count = 10)]
fn hnsw_schema_seed_inline_write(bencher: Bencher) {
    bench_case(bencher, CASES[2]);
}

fn bench_case(bencher: Bencher, case: WriteCase) {
    bencher.bench_local(|| {
        let sample = run_case(case).expect("search write amplification bench case");
        black_box(sample.artifact_bytes ^ sample.manifest_publish_bytes)
    });
}

fn write_structured_results(path: &Path) {
    let sample_count = structured_sample_count();
    let selected = structured_bench_filter();
    let mut benches = Vec::new();

    for case in CASES {
        if !structured_bench_selected(&selected, case.id) {
            continue;
        }
        benches.push(measure_structured_bench(*case, sample_count));
    }

    let payload = serde_json::json!({
        "schema_version": 1,
        "kind": "divan_bench_result",
        "crate": "paro-storage",
        "bench": "search_write_amplification",
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

fn measure_structured_bench(case: WriteCase, sample_count: usize) -> serde_json::Value {
    let mut samples_ms = Vec::with_capacity(sample_count);
    let mut rows_per_second = Vec::with_capacity(sample_count);
    let mut cpu_us_per_row = Vec::with_capacity(sample_count);
    let mut artifact_bytes = Vec::with_capacity(sample_count);
    let mut manifest_publish_bytes = Vec::with_capacity(sample_count);
    let mut write_amplification = Vec::with_capacity(sample_count);
    let mut segment_file_open_count = Vec::with_capacity(sample_count);
    let mut segment_file_count = Vec::with_capacity(sample_count);

    for _ in 0..structured_warmup_count() {
        black_box(run_case(case).expect("warm search write amplification bench case"));
    }

    for _ in 0..sample_count {
        let sample = run_case(case).expect("search write amplification bench case");
        samples_ms.push(sample.elapsed_ms);
        rows_per_second.push(sample.rows_per_second);
        cpu_us_per_row.push(sample.artifact_build_cpu_us_per_row);
        artifact_bytes.push(sample.artifact_bytes);
        manifest_publish_bytes.push(sample.manifest_publish_bytes);
        write_amplification.push(sample.write_amplification);
        segment_file_open_count.push(sample.segment_file_open_count);
        segment_file_count.push(sample.segment_file_count);
    }

    serde_json::json!({
        "id": case.id,
        "items": case.rows,
        "samples_ms": samples_ms,
        "audit": {
            "chunk_count": case.rows,
            "rows_per_second": rows_per_second,
            "artifact_build_cpu_us_per_row": cpu_us_per_row,
            "artifact_bytes": artifact_bytes,
            "manifest_publish_bytes": manifest_publish_bytes,
            "write_amplification": write_amplification,
            "segment_file_open_count": segment_file_open_count,
            "segment_file_count": segment_file_count,
        },
    })
}

fn run_case(case: WriteCase) -> paro_common::error::Result<WriteSample> {
    let dir = TempDir::new()?;
    let table = build_table(dir.path(), case.provider)?;
    let (definition_id, input_bytes) = prepare_definition_and_chunk(&table, case.provider)?;
    let chunk = build_chunk(case)?;
    let metrics_before = storage_metrics().snapshot();

    let started_at = Instant::now();
    table.append(&chunk)?;
    let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;

    let snapshot = table
        .open_search_generation_snapshot(definition_id)?
        .expect("search generation snapshot should exist after append");
    let artifact_bytes = snapshot
        .artifacts
        .artifacts
        .iter()
        .map(|artifact| artifact.stats.bytes_on_disk)
        .sum::<u64>();
    let manifest_publish_bytes = manifest_fragment_bytes(table.tablet().data_dir(), definition_id)?;
    let metrics_after = storage_metrics().snapshot();
    let inline_cpu_us = inline_cpu_us_for_provider(&metrics_after, case.provider)
        .saturating_sub(inline_cpu_us_for_provider(&metrics_before, case.provider));
    let segment_file_open_count = metrics_after
        .segment_file_open_total
        .saturating_sub(metrics_before.segment_file_open_total);
    let elapsed_us = (elapsed_ms * 1000.0).max(1.0);
    let cpu_us = if inline_cpu_us == 0 {
        elapsed_us
    } else {
        inline_cpu_us as f64
    };
    let total_derived_bytes = artifact_bytes.saturating_add(manifest_publish_bytes);

    Ok(WriteSample {
        elapsed_ms,
        rows_per_second: (case.rows as f64) / (elapsed_ms / 1000.0).max(f64::EPSILON),
        artifact_build_cpu_us_per_row: cpu_us / case.rows as f64,
        artifact_bytes,
        manifest_publish_bytes,
        write_amplification: total_derived_bytes as f64 / input_bytes.max(1) as f64,
        segment_file_open_count,
        segment_file_count: count_files(table.tablet().data_dir()),
    })
}

fn inline_cpu_us_for_provider(
    metrics: &paro_storage::metrics::StorageMetricsSnapshot,
    provider: SearchIndexKind,
) -> u64 {
    metrics
        .search_inline_build_by_key
        .iter()
        .filter(|series| series.key.provider == provider)
        .map(|series| series.counters.cpu_us_total)
        .sum()
}

fn build_table(root: &Path, provider: SearchIndexKind) -> paro_common::error::Result<TableHandle> {
    let factory = TableFactory::default().with_storage_root(root);
    match provider {
        SearchIndexKind::FullText => factory.create_table(&[LogicalType::Varchar]),
        SearchIndexKind::Sparse => factory.create_table(&[LogicalType::Blob]),
        SearchIndexKind::Hnsw => factory.create_table(&[LogicalType::Array(
            Box::new(LogicalType::Float),
            HNSW_DIMENSION,
        )]),
    }
}

fn prepare_definition_and_chunk(
    table: &TableHandle,
    provider: SearchIndexKind,
) -> paro_common::error::Result<(u64, u64)> {
    match provider {
        SearchIndexKind::FullText => {
            let definition = search_definition(table, 10, provider, 0, Some("simple"), None);
            let definition_id = definition.definition_id;
            table.register_search_definition(definition)?;
            Ok((
                definition_id,
                table_row_count(provider) as u64 * FULLTEXT_BYTES_PER_ROW,
            ))
        }
        SearchIndexKind::Sparse => {
            let definition = search_definition(table, 11, provider, 0, None, None);
            let definition_id = definition.definition_id;
            table.register_search_definition(definition)?;
            let row_bytes = 8 + (SPARSE_NNZ as u64 * 8);
            Ok((definition_id, table_row_count(provider) as u64 * row_bytes))
        }
        SearchIndexKind::Hnsw => {
            let capability = table
                .vector_capability(0, DistanceMetric::Euclidean)
                .expect("array(float) column should seed HNSW definition");
            Ok((
                capability.definition_id,
                table_row_count(provider) as u64
                    * HNSW_DIMENSION as u64
                    * std::mem::size_of::<f32>() as u64,
            ))
        }
    }
}

fn table_row_count(provider: SearchIndexKind) -> usize {
    match provider {
        SearchIndexKind::Hnsw => HNSW_ROWS,
        SearchIndexKind::FullText | SearchIndexKind::Sparse => SEARCH_ROWS,
    }
}

fn search_definition(
    table: &TableHandle,
    definition_id: u64,
    provider: SearchIndexKind,
    column_id: u32,
    tokenizer: Option<&str>,
    dimension: Option<u64>,
) -> SearchIndexDefinition {
    let provider_config = match provider {
        SearchIndexKind::FullText => serde_json::json!({
            "version": paro_storage::search::FULLTEXT_PROVIDER_CONFIG_VERSION,
            "config": tokenizer.unwrap_or("simple")
        }),
        SearchIndexKind::Sparse => serde_json::json!({
            "version": paro_storage::search::SPARSE_PROVIDER_CONFIG_VERSION,
            "physical_encoding": "binary-v1"
        }),
        SearchIndexKind::Hnsw => paro_storage::search::HnswProviderConfig {
            version: paro_storage::search::HNSW_PROVIDER_CONFIG_VERSION,
            dimension: dimension.unwrap_or(HNSW_DIMENSION as u64) as u32,
            distance: paro_storage::index::hnsw::DistanceMetric::Euclidean,
            m: 16,
            ef_construct: 64,
            ef_search: 64,
            plain_scan_threshold: 10_000,
            filtered_plain_scan_threshold: 0,
            build_seed: paro_storage::search::DEFAULT_HNSW_BUILD_SEED,
            inline_threshold: paro_storage::search::HnswInlineConfig {
                enabled: true,
                max_vector_count: 4_096,
                max_graph_memory_bytes: 64 * 1024 * 1024,
                max_dimension: 1_536,
            },
        }
        .validated()
        .expect("valid benchmark HNSW config")
        .to_value()
        .expect("serialize benchmark HNSW config"),
    };
    let expression = tokenizer.map(|config| format!("to_tsvector('{config}', col_{column_id})"));
    SearchIndexDefinition {
        definition_id,
        table_id: table.tablet_id(),
        name: format!("bench_{provider:?}_{definition_id}"),
        kind: provider,
        column_ids: vec![column_id],
        expression: expression.clone(),
        freshness_policy: SearchFreshnessPolicy::default_for_kind(provider),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            provider,
            &[column_id],
            expression.as_deref(),
            &provider_config,
        ),
        provider_config,
    }
}

fn build_chunk(case: WriteCase) -> paro_common::error::Result<Chunk> {
    let vector = match case.provider {
        SearchIndexKind::FullText => fulltext_vector(case.rows),
        SearchIndexKind::Sparse => sparse_vector(case.rows)?,
        SearchIndexKind::Hnsw => hnsw_vector(case.rows),
    };
    Ok(Chunk::from_vectors(vec![vector], test_allocator()))
}

fn fulltext_vector(rows: usize) -> Vector {
    let values = (0..rows)
        .map(|idx| format!("search derived state benchmark text payload row {idx:04} alpha beta"))
        .collect::<Vec<_>>();
    let refs = values.iter().map(String::as_str).collect::<Vec<_>>();
    test_string_vector(&refs)
}

fn sparse_vector(rows: usize) -> paro_common::error::Result<Vector> {
    let allocator = test_allocator();
    let mut vector = Vector::try_new(LogicalType::Blob, rows, allocator)?;
    for row in 0..rows {
        let dims = (0..SPARSE_NNZ)
            .map(|idx| (idx as u32) * 2 + (row as u32 % 2))
            .collect::<Vec<_>>();
        let weights = (0..SPARSE_NNZ)
            .map(|idx| 1.0 / (idx as f32 + 1.0))
            .collect::<Vec<_>>();
        let sparse = SparseVector::new(dims, weights)?;
        vector.set_blob(row, &sparse.to_row_image_v1()?);
    }
    vector.set_count(rows);
    Ok(vector)
}

fn hnsw_vector(rows: usize) -> Vector {
    let values = (0..rows)
        .map(|row| {
            (0..HNSW_DIMENSION)
                .map(|dim| ((row + dim) % 17) as f32 / 17.0)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    test_embeddings_vector(&values, HNSW_DIMENSION)
}

fn count_files(root: &Path) -> u64 {
    let mut stack = vec![root.to_path_buf()];
    let mut files = 0u64;
    while let Some(path) = stack.pop() {
        let Ok(entries) = fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path: PathBuf = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file() {
                files = files.saturating_add(1);
            }
        }
    }
    files
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
