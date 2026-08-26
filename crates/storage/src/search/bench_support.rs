// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Narrow benchmark hooks for search internals.
//!
//! This module intentionally exposes only fixture-style helpers. It keeps the
//! manifest store itself crate-private while allowing Divan benchmarks to
//! measure bounded manifest replay costs without routing through SQL noise.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use paro_common::allocator::{default_allocator, Allocator};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::index::hnsw::SearchParams;
use crate::table::table_factory::TableFactory;
use crate::table::table_handle::TableHandle;

use super::artifact::SegmentPagePointer;
use super::budget::{ResourceBudget, SearchBatchConfig};
use super::capability::{ArtifactSegmentRef, CoverageState, SearchArtifactRef, SearchIndexKind};
use super::cursor::{SearchBatchState, SearchReadSnapshot};
use super::inline_sink::{
    CostEstimate, FullTextStatsDelta, MaintenanceBenefit, MaintenanceCost, SearchStatsDelta,
};
use super::maintenance::{
    MaintenanceAdmissionDecision, MaintenanceAdmissionPolicy, MaintenanceAdmissionReason,
    MaintenanceAdmissionRequest, MaintenanceFairnessKey, MaintenanceScheduler,
    SearchMaintenanceAction,
};
use super::manifest::{
    GenerationManifestRoot, ManifestCodecKind, ManifestDelta, ManifestDeltaEntry, ManifestShard,
    ManifestStore,
};
use super::row_fetch::{RowFetchMode, SearchRowFetcher};
use super::stats::{
    CatchUpBacklogTier, ExecutionModes, FullTextProviderStats, GenerationMaintenanceState,
    GenerationStats, MaintenancePriority, SearchArtifactStats, SearchProviderStats,
};
use super::tail::{TailEntryId, TailMutationKind, TailPendingEntry, TailRowImageRef};

#[derive(Debug, Clone, Copy)]
pub struct ManifestOpenBenchConfig {
    pub definition_id: u64,
    pub generation_id: u64,
    pub delta_count: usize,
    pub entries_per_delta: usize,
    pub shard_count: usize,
    pub entries_per_shard: usize,
    pub codec: ManifestBenchCodec,
}

impl ManifestOpenBenchConfig {
    pub const fn new(delta_count: usize) -> Self {
        Self {
            definition_id: 1,
            generation_id: 1,
            delta_count,
            entries_per_delta: 1,
            shard_count: 1,
            entries_per_shard: 0,
            codec: ManifestBenchCodec::JsonDebugV2,
        }
    }

    pub const fn with_shards(mut self, shard_count: usize, entries_per_shard: usize) -> Self {
        self.shard_count = shard_count;
        self.entries_per_shard = entries_per_shard;
        self
    }

    pub const fn with_codec(mut self, codec: ManifestBenchCodec) -> Self {
        self.codec = codec;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestBenchCodec {
    JsonDebugV2,
    BinaryV2,
}

impl ManifestBenchCodec {
    pub const fn label(self) -> &'static str {
        match self {
            Self::JsonDebugV2 => "json-debug-v2",
            Self::BinaryV2 => "binary-v2",
        }
    }

    const fn manifest_codec(self) -> ManifestCodecKind {
        match self {
            Self::JsonDebugV2 => ManifestCodecKind::JSON_DEBUG_V2,
            Self::BinaryV2 => ManifestCodecKind::BINARY_V2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestOpenBenchSummary {
    pub artifact_count: usize,
    pub tail_pending_count: usize,
    pub recent_delta_count: usize,
    pub shard_count: usize,
    pub manifest_bytes: u64,
}

pub fn prepare_manifest_open_bench_fixture(
    table_data_dir: &Path,
    config: ManifestOpenBenchConfig,
) -> Result<()> {
    let store = ManifestStore::new_with_codec(table_data_dir, config.codec.manifest_codec());
    let mut shard_refs = Vec::with_capacity(config.shard_count.max(1));
    for shard_idx in 0..config.shard_count.max(1) {
        let base = (shard_idx * config.entries_per_shard) as u64;
        let mut artifact_refs = Vec::with_capacity(config.entries_per_shard);
        let mut tail_pending_entries = Vec::with_capacity(config.entries_per_shard);
        for entry_offset in 0..config.entries_per_shard {
            let rowset_id = base + entry_offset as u64 + 1;
            artifact_refs.push(sample_artifact(rowset_id));
            tail_pending_entries.push(sample_tail_entry(rowset_id, TailEntryId(rowset_id)));
        }
        shard_refs.push(store.write_shard(
            config.definition_id,
            config.generation_id,
            shard_idx as u64 + 1,
            &ManifestShard {
                artifact_refs,
                tail_pending_entries,
            },
        )?);
    }

    let mut delta_refs = Vec::with_capacity(config.delta_count);
    for ordinal in 0..config.delta_count {
        let base = (ordinal * config.entries_per_delta) as u64;
        let mut entries = Vec::with_capacity(config.entries_per_delta * 3);
        for entry_offset in 0..config.entries_per_delta {
            let rowset_id = base + entry_offset as u64 + 1;
            entries.push(ManifestDeltaEntry::AddArtifact(sample_artifact(rowset_id)));
            entries.push(ManifestDeltaEntry::UpsertTail(sample_tail_entry(
                rowset_id,
                TailEntryId(rowset_id),
            )));
            entries.push(ManifestDeltaEntry::StatsDelta(SearchStatsDelta::FullText(
                FullTextStatsDelta {
                    stats: sample_fulltext_stats(1),
                },
            )));
        }
        delta_refs.push(store.write_delta(
            config.definition_id,
            config.generation_id,
            ordinal as u64 + 2,
            ordinal,
            &ManifestDelta::new(entries),
        )?);
    }

    let mut root = GenerationManifestRoot {
        definition_id: config.definition_id,
        generation_id: config.generation_id,
        build_epoch: 1,
        build_snapshot_version: 1,
        indexed_through_ts: 1,
        config_fingerprint: 1,
        coverage: CoverageState::Complete,
        generation_stats: GenerationStats::default(),
        next_tail_entry_id: TailEntryId((config.delta_count * config.entries_per_delta) as u64 + 1),
        execution_modes: ExecutionModes::default(),
        maintenance_state: GenerationMaintenanceState::default(),
        root_version: config.delta_count as u64 + 1,
        checksum: 0,
        shard_files: shard_refs,
        recent_delta_files: delta_refs,
        materialized_state_file: None,
    };
    root.recompute_checksum()?;
    store.write_root(config.definition_id, &root)?;
    Ok(())
}

pub fn open_manifest_bench_fixture(
    table_data_dir: &Path,
    definition_id: u64,
    codec: ManifestBenchCodec,
) -> Result<ManifestOpenBenchSummary> {
    open_manifest_bench_fixture_with_manifest_bytes(table_data_dir, definition_id, codec, None)
}

pub fn open_manifest_bench_fixture_with_manifest_bytes(
    table_data_dir: &Path,
    definition_id: u64,
    codec: ManifestBenchCodec,
    manifest_bytes: Option<u64>,
) -> Result<ManifestOpenBenchSummary> {
    let store = ManifestStore::new_with_codec(table_data_dir, codec.manifest_codec());
    let Some(manifest) = store.load_latest_manifest_for_private_workspace(definition_id)? else {
        return Ok(ManifestOpenBenchSummary {
            artifact_count: 0,
            tail_pending_count: 0,
            recent_delta_count: 0,
            shard_count: 0,
            manifest_bytes: 0,
        });
    };
    let manifest_bytes =
        manifest_bytes.unwrap_or_else(|| manifest_path_bytes(&manifest.opened_paths()));
    Ok(ManifestOpenBenchSummary {
        artifact_count: manifest.artifacts.artifacts.len(),
        tail_pending_count: manifest.tail_pending_entries.len(),
        recent_delta_count: manifest.root.recent_delta_files.len(),
        shard_count: manifest.root.shard_files.len(),
        manifest_bytes,
    })
}

pub fn manifest_fragment_bytes(table_data_dir: &Path, definition_id: u64) -> Result<u64> {
    let store = ManifestStore::new(table_data_dir);
    let Some(manifest) = store.load_latest_manifest_for_private_workspace(definition_id)? else {
        return Ok(0);
    };
    Ok(manifest_path_bytes(&manifest.all_paths()))
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RequiredSchedulerBenchConfig {
    pub opportunistic_count: usize,
}

impl Default for RequiredSchedulerBenchConfig {
    fn default() -> Self {
        Self {
            opportunistic_count: 64,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RequiredSchedulerBenchSummary {
    pub required_dispatch_delay_ms: f64,
    pub opportunistic_tasks_before_required: usize,
    pub queued_task_count: usize,
    pub required_admitted: bool,
}

pub fn run_required_scheduler_bench(
    config: RequiredSchedulerBenchConfig,
) -> RequiredSchedulerBenchSummary {
    let total_requests = config.opportunistic_count.saturating_add(1).max(1);
    let scheduler = MaintenanceScheduler::with_policy(MaintenanceAdmissionPolicy {
        fulltext_concurrency: total_requests,
        table_concurrency: total_requests,
        ..MaintenanceAdmissionPolicy::default()
    });

    let opportunistic = (0..config.opportunistic_count)
        .map(|offset| {
            scheduler_bench_request(
                1_000 + offset as u64,
                MaintenancePriority::Opportunistic,
                CatchUpBacklogTier::Healthy,
                MaintenanceBenefit {
                    expected_tail_rows_drained: 1_000_000 + offset as u64,
                    ..Default::default()
                },
            )
        })
        .collect::<Vec<_>>();
    let _ = scheduler.schedule_requests(&opportunistic);

    let required = scheduler_bench_request(
        1,
        MaintenancePriority::Critical,
        CatchUpBacklogTier::Degraded,
        MaintenanceBenefit {
            expected_tail_rows_drained: 1,
            ..Default::default()
        },
    );
    let required_admitted = scheduler.schedule_requests(&[required])[0].is_admitted();
    let queued_task_count = config
        .opportunistic_count
        .saturating_add(usize::from(required_admitted));
    let started_at = Instant::now();
    let mut opportunistic_tasks_before_required = 0usize;
    while let Some(task) = scheduler.pop_next_task() {
        if task.request.definition_id == 1 {
            break;
        }
        opportunistic_tasks_before_required = opportunistic_tasks_before_required.saturating_add(1);
    }
    RequiredSchedulerBenchSummary {
        required_dispatch_delay_ms: started_at.elapsed().as_secs_f64() * 1000.0,
        opportunistic_tasks_before_required,
        queued_task_count,
        required_admitted,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForegroundIoAdmissionBenchSummary {
    pub foreground_reserved_bytes: u64,
    pub remaining_background_read_budget: u64,
    pub admitted_background_read_bytes: u64,
    pub oversized_background_deferred: bool,
}

pub fn run_foreground_io_admission_bench() -> ForegroundIoAdmissionBenchSummary {
    const IO_READ_BUDGET: u64 = 1_048_576;
    const FOREGROUND_RESERVED: u64 = 786_432;
    const OVERSIZED_BACKGROUND_READ: u64 = 524_288;
    const ADMITTED_BACKGROUND_READ: u64 = 131_072;

    let scheduler = MaintenanceScheduler::with_policy(MaintenanceAdmissionPolicy {
        io_read_bytes_budget: IO_READ_BUDGET,
        foreground_io_read_bytes_reserved: FOREGROUND_RESERVED,
        fulltext_concurrency: 2,
        table_concurrency: 2,
        ..MaintenanceAdmissionPolicy::default()
    });
    let oversized = scheduler_bench_request_with_cost(
        21,
        MaintenancePriority::Opportunistic,
        CatchUpBacklogTier::Healthy,
        MaintenanceCost {
            io_read_bytes: OVERSIZED_BACKGROUND_READ,
            ..Default::default()
        },
        MaintenanceBenefit::default(),
    );
    let admitted = scheduler_bench_request_with_cost(
        22,
        MaintenancePriority::Opportunistic,
        CatchUpBacklogTier::Healthy,
        MaintenanceCost {
            io_read_bytes: ADMITTED_BACKGROUND_READ,
            ..Default::default()
        },
        MaintenanceBenefit::default(),
    );
    let decisions = scheduler.admit_requests(&[oversized, admitted]);
    let oversized_background_deferred = matches!(
        decisions.first(),
        Some(MaintenanceAdmissionDecision::Deferred {
            reason: MaintenanceAdmissionReason::IoReadBudget
        })
    );
    let admitted_background_read_bytes = decisions
        .get(1)
        .filter(|decision| decision.is_admitted())
        .map(|_| ADMITTED_BACKGROUND_READ)
        .unwrap_or_default();

    ForegroundIoAdmissionBenchSummary {
        foreground_reserved_bytes: FOREGROUND_RESERVED,
        remaining_background_read_budget: IO_READ_BUDGET.saturating_sub(FOREGROUND_RESERVED),
        admitted_background_read_bytes,
        oversized_background_deferred,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RowFetchBenchConfig {
    pub row_count: usize,
    pub candidate_count: usize,
}

impl Default for RowFetchBenchConfig {
    fn default() -> Self {
        Self {
            row_count: 4_096,
            candidate_count: 128,
        }
    }
}

#[derive(Debug)]
pub struct RowFetchBenchFixture {
    table: TableHandle,
    snapshot: SearchReadSnapshot,
    rows: Vec<super::cursor::PhysicalRowRef>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RowFetchBenchSummary {
    pub elapsed_ms: f64,
    pub rows: usize,
    pub projected_columns: usize,
    pub segment_groups: usize,
    pub column_batches: usize,
    pub fixed_width_column_batches: usize,
    pub varlen_column_batches: usize,
    pub projected_bytes: usize,
    pub column_read_by_rowids_page_run_seeks: usize,
    pub search_layer_varlen_fallback_seek_count: usize,
}

impl RowFetchBenchFixture {
    pub fn new(config: RowFetchBenchConfig) -> Result<Self> {
        let table = TableFactory::default().create_table(&[
            LogicalType::Array(Box::new(LogicalType::Float), 2),
            LogicalType::Varchar,
            LogicalType::BigInt,
        ])?;
        let allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
        table.append(&row_fetch_bench_chunk(config.row_count, allocator)?)?;
        let opened = table.open_vector_search_cursor(
            0,
            &[0.0, 0.0],
            crate::index::hnsw::DistanceMetric::Euclidean,
            config.candidate_count,
            SearchParams {
                ef: Some(128),
                ..Default::default()
            },
            None,
            table.max_version(),
            &crate::search::SearchReadOptions::ungoverned(),
        )?;
        let mut cursor = opened.cursor;
        let snapshot = opened.snapshot;
        let mut budget = ResourceBudget::standalone(64 * 1024 * 1024, config.candidate_count, 1);
        let rows = loop {
            match cursor.next_batch(
                &SearchBatchConfig {
                    row_limit: config.candidate_count,
                    preferred_bytes: 1 << 20,
                },
                &mut budget,
            )? {
                SearchBatchState::Ready(batch) if batch.is_empty() => continue,
                SearchBatchState::Ready(batch) => break batch.rows,
                SearchBatchState::Exhausted => break Vec::new(),
            }
        };
        Ok(Self {
            table,
            snapshot,
            rows,
        })
    }

    pub fn run_once(&self) -> Result<RowFetchBenchSummary> {
        let projected = SearchRowFetcher::new(&self.snapshot, self.table.types()).fetch_batch(
            &self.rows,
            &[2, 1],
            RowFetchMode::materialize(self.rows.len()),
        )?;
        Ok(RowFetchBenchSummary {
            elapsed_ms: projected.stats.elapsed_micros as f64 / 1000.0,
            rows: projected.stats.rows,
            projected_columns: projected.stats.projected_columns,
            segment_groups: projected.stats.segment_groups,
            column_batches: projected.stats.column_batches,
            fixed_width_column_batches: projected.stats.fixed_width_column_batches,
            varlen_column_batches: projected.stats.varlen_column_batches,
            projected_bytes: projected.stats.projected_bytes,
            column_read_by_rowids_page_run_seeks: projected
                .stats
                .column_read_by_rowids_page_run_seeks,
            search_layer_varlen_fallback_seek_count: 0,
        })
    }
}

fn manifest_path_bytes(paths: &[std::path::PathBuf]) -> u64 {
    paths
        .iter()
        .filter_map(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn row_fetch_bench_chunk(row_count: usize, allocator: Arc<dyn Allocator>) -> Result<Chunk> {
    let embeddings = (0..row_count)
        .map(|idx| vec![idx as f32, 0.0])
        .collect::<Vec<_>>();
    let labels = (0..row_count)
        .map(|idx| format!("row-fetch-payload-{idx:05}"))
        .collect::<Vec<_>>();
    let label_refs = labels.iter().map(String::as_str).collect::<Vec<_>>();
    let values = (0..row_count).map(|idx| idx as i64).collect::<Vec<_>>();
    Ok(Chunk::from_vectors(
        vec![
            Vector::try_from_embeddings(&embeddings, 2, allocator.clone())?,
            Vector::try_from_strings(&label_refs, allocator.clone())?,
            Vector::try_from_i64(&values, allocator.clone())?,
        ],
        allocator,
    ))
}

fn sample_artifact(rowset_id: u64) -> SearchArtifactRef {
    SearchArtifactRef {
        definition_id: 1,
        generation_id: 1,
        coverage: super::capability::SearchPartitionCoverage::singleton(
            ArtifactSegmentRef {
                rowset_id,
                segment_id: 0,
            },
            1,
        )
        .expect("sample partition coverage"),
        column_id: 0,
        kind: SearchIndexKind::FullText,
        provider_variant: 1,
        artifact_format_version: 1,
        location: super::artifact::ArtifactLocation::Inline {
            page: SegmentPagePointer {
                rowset_id,
                segment_id: 0,
                column_id: 0,
                page_offset: rowset_id * 64,
                page_len: 64,
                checksum: rowset_id,
            },
        },
        stats: SearchArtifactStats {
            row_count: 1,
            bytes_on_disk: 64,
            provider_stats: Some(SearchProviderStats::FullText(sample_fulltext_stats(1))),
        },
        checksum: rowset_id,
    }
}

fn scheduler_bench_request(
    definition_id: u64,
    priority: MaintenancePriority,
    backlog_tier: CatchUpBacklogTier,
    benefit: MaintenanceBenefit,
) -> MaintenanceAdmissionRequest {
    scheduler_bench_request_with_cost(
        definition_id,
        priority,
        backlog_tier,
        MaintenanceCost::default(),
        benefit,
    )
}

fn scheduler_bench_request_with_cost(
    definition_id: u64,
    priority: MaintenancePriority,
    backlog_tier: CatchUpBacklogTier,
    cost: MaintenanceCost,
    benefit: MaintenanceBenefit,
) -> MaintenanceAdmissionRequest {
    MaintenanceAdmissionRequest {
        definition_id,
        action: SearchMaintenanceAction::CatchUp,
        fairness_key: MaintenanceFairnessKey {
            database_id: 0,
            table_id: 1,
            provider: SearchIndexKind::FullText,
        },
        priority,
        backlog_tier,
        estimate: CostEstimate { cost, benefit },
    }
}

fn sample_tail_entry(rowset_id: u64, entry_id: TailEntryId) -> TailPendingEntry {
    TailPendingEntry {
        entry_id,
        rowset_id,
        segment_ids: vec![0],
        mutation: TailMutationKind::Append,
        row_count: 1,
        byte_count: 128,
        row_image_ref: Some(TailRowImageRef::WholeRowset),
    }
}

fn sample_fulltext_stats(rows: u64) -> FullTextProviderStats {
    FullTextProviderStats {
        total_docs: rows as u32,
        total_terms: rows * 4,
        avg_doc_length: 4.0,
        unique_terms: rows as u32,
        total_postings: rows * 4,
        max_posting_list_len: rows as u32,
        min_posting_list_len: 1,
        bm25_k1: 1.2,
        bm25_b: 0.75,
        tokenizer: "simple".to_string(),
    }
}
