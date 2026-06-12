// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Storage-level lightweight metrics (counters + gauges) for observability.
//!
//! Requirement: expose PrimaryIndex hit/conflict counters, L0→L1
//! flush count, DeleteVector entry count, and PrimaryIndex memory usage.
//!
//! The implementation is intentionally simple: a global, lock-free set of
//! atomics accessible via [`storage_metrics()`]. It is header-only and
//! avoids external dependencies to stay embeddable in storage-only builds.

use paro_common::allocator::{MemoryUsageSnapshot, MEMORY_TAG_COUNT};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use crate::search::{SearchIndexKind, SEARCH_BUILD_LATENCY_BUCKETS_US, SEARCH_LATENCY_BUCKETS_US};

pub const SEARCH_ROW_FETCH_LATENCY_BUCKET_COUNT: usize = SEARCH_LATENCY_BUCKETS_US.len() + 1;
pub const SEARCH_BUILD_LATENCY_BUCKET_COUNT: usize = SEARCH_BUILD_LATENCY_BUCKETS_US.len() + 1;
pub const SEARCH_INLINE_BUILD_LATENCY_BUCKET_COUNT: usize = SEARCH_BUILD_LATENCY_BUCKET_COUNT;
pub const SEARCH_SIDECAR_BUILD_LATENCY_BUCKET_COUNT: usize = SEARCH_BUILD_LATENCY_BUCKET_COUNT;
pub const SEARCH_GENERATION_LATENCY_BUCKET_COUNT: usize = SEARCH_BUILD_LATENCY_BUCKETS_US.len() + 1;
pub const SEARCH_MANIFEST_LATENCY_BUCKET_COUNT: usize = SEARCH_LATENCY_BUCKETS_US.len() + 1;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchManifestMetricKey {
    pub codec: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchManifestMetricCounters {
    pub publish_latency_us_total: u64,
    pub publish_latency_us_buckets: [u64; SEARCH_MANIFEST_LATENCY_BUCKET_COUNT],
    pub publish_cas_retries_total: u64,
    pub open_latency_us_total: u64,
    pub open_latency_us_buckets: [u64; SEARCH_MANIFEST_LATENCY_BUCKET_COUNT],
    pub delta_count: u64,
    pub open_bytes_total: u64,
}

impl SearchManifestMetricCounters {
    fn record_publish_latency(&mut self, elapsed_micros: u64) {
        self.publish_latency_us_total =
            self.publish_latency_us_total.saturating_add(elapsed_micros);
        let bucket_idx = latency_bucket_index(
            SEARCH_LATENCY_BUCKETS_US,
            SEARCH_MANIFEST_LATENCY_BUCKET_COUNT,
            elapsed_micros,
        );
        self.publish_latency_us_buckets[bucket_idx] =
            self.publish_latency_us_buckets[bucket_idx].saturating_add(1);
    }

    fn record_open_latency(&mut self, elapsed_micros: u64) {
        self.open_latency_us_total = self.open_latency_us_total.saturating_add(elapsed_micros);
        let bucket_idx = latency_bucket_index(
            SEARCH_LATENCY_BUCKETS_US,
            SEARCH_MANIFEST_LATENCY_BUCKET_COUNT,
            elapsed_micros,
        );
        self.open_latency_us_buckets[bucket_idx] =
            self.open_latency_us_buckets[bucket_idx].saturating_add(1);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchManifestMetricsByKey {
    pub key: SearchManifestMetricKey,
    pub counters: SearchManifestMetricCounters,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchTailMetricKey {
    pub provider: SearchIndexKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchTailMetricCounters {
    pub tail_rows: u64,
    pub tail_bytes: u64,
    pub tail_backlog_tier: u64,
    pub exact_merge_rows_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchTailMetricsByKey {
    pub key: SearchTailMetricKey,
    pub counters: SearchTailMetricCounters,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchTailRejectedMetricKey {
    pub provider: SearchIndexKind,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchTailRejectedMetricsByKey {
    pub key: SearchTailRejectedMetricKey,
    pub rejected_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchFullTextDegradedScoreMetricKey {
    pub table_id: u64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchFullTextDegradedScoreMetricsByKey {
    pub key: SearchFullTextDegradedScoreMetricKey,
    pub degraded_queries: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchGenerationMetricKey {
    pub provider: SearchIndexKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchGenerationMetricCounters {
    pub retired_total: u64,
    pub retired_bytes_total: u64,
    pub lease_hold_time_us_buckets: [u64; SEARCH_GENERATION_LATENCY_BUCKET_COUNT],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchGenerationMetricsByKey {
    pub key: SearchGenerationMetricKey,
    pub counters: SearchGenerationMetricCounters,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchArtifactGcDelayMetricKey {
    pub provider: SearchIndexKind,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchArtifactGcDelayMetricCounters {
    pub delay_us_buckets: [u64; SEARCH_GENERATION_LATENCY_BUCKET_COUNT],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchArtifactGcDelayMetricsByKey {
    pub key: SearchArtifactGcDelayMetricKey,
    pub counters: SearchArtifactGcDelayMetricCounters,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchInlineBuildMetricKey {
    pub definition_id: u64,
    pub provider: SearchIndexKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchInlineBuildMetricCounters {
    pub rows_total: u64,
    pub bytes_total: u64,
    pub latency_us_total: u64,
    pub latency_us_buckets: [u64; SEARCH_INLINE_BUILD_LATENCY_BUCKET_COUNT],
    pub cpu_us_total: u64,
}

impl SearchInlineBuildMetricCounters {
    fn record(&mut self, rows: u64, bytes: u64, elapsed_micros: u64, cpu_micros: u64) {
        self.rows_total = self.rows_total.saturating_add(rows);
        self.bytes_total = self.bytes_total.saturating_add(bytes);
        self.latency_us_total = self.latency_us_total.saturating_add(elapsed_micros);
        self.cpu_us_total = self.cpu_us_total.saturating_add(cpu_micros);
        let bucket_idx = latency_bucket_index(
            SEARCH_BUILD_LATENCY_BUCKETS_US,
            SEARCH_INLINE_BUILD_LATENCY_BUCKET_COUNT,
            elapsed_micros,
        );
        self.latency_us_buckets[bucket_idx] = self.latency_us_buckets[bucket_idx].saturating_add(1);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchInlineBuildMetricsByKey {
    pub key: SearchInlineBuildMetricKey,
    pub counters: SearchInlineBuildMetricCounters,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchInlineBuildFailureKey {
    pub provider: SearchIndexKind,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchInlineBuildFailureMetricsByKey {
    pub key: SearchInlineBuildFailureKey,
    pub failures_total: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchSidecarBuildMetricKey {
    pub definition_id: u64,
    pub provider: SearchIndexKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchSidecarBuildMetricCounters {
    pub rows_total: u64,
    pub read_bytes_total: u64,
    pub write_bytes_total: u64,
    pub artifact_bytes_total: u64,
    pub latency_us_total: u64,
    pub latency_us_buckets: [u64; SEARCH_SIDECAR_BUILD_LATENCY_BUCKET_COUNT],
}

impl SearchSidecarBuildMetricCounters {
    fn record(
        &mut self,
        rows: u64,
        read_bytes: u64,
        write_bytes: u64,
        artifact_bytes: u64,
        elapsed_micros: u64,
    ) {
        self.rows_total = self.rows_total.saturating_add(rows);
        self.read_bytes_total = self.read_bytes_total.saturating_add(read_bytes);
        self.write_bytes_total = self.write_bytes_total.saturating_add(write_bytes);
        self.artifact_bytes_total = self.artifact_bytes_total.saturating_add(artifact_bytes);
        self.latency_us_total = self.latency_us_total.saturating_add(elapsed_micros);
        let bucket_idx = latency_bucket_index(
            SEARCH_BUILD_LATENCY_BUCKETS_US,
            SEARCH_SIDECAR_BUILD_LATENCY_BUCKET_COUNT,
            elapsed_micros,
        );
        self.latency_us_buckets[bucket_idx] = self.latency_us_buckets[bucket_idx].saturating_add(1);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchSidecarBuildMetricsByKey {
    pub key: SearchSidecarBuildMetricKey,
    pub counters: SearchSidecarBuildMetricCounters,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchSidecarReaderMetricKey {
    pub provider: SearchIndexKind,
    pub codec: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchSidecarReaderMetricCounters {
    pub open_count_total: u64,
    pub cache_hits_total: u64,
    pub cache_misses_total: u64,
    pub mmap_bytes: u64,
    pub format_dispatch_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchSidecarReaderMetricsByKey {
    pub key: SearchSidecarReaderMetricKey,
    pub counters: SearchSidecarReaderMetricCounters,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchRowFetchMetricKey {
    pub table_id: u64,
    pub provider: SearchIndexKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchRowFetchMetricCounters {
    pub batches_total: u64,
    pub rows_total: u64,
    pub projected_columns_total: u64,
    pub segment_groups_total: u64,
    pub column_batches_total: u64,
    pub fixed_width_column_batches_total: u64,
    pub varlen_column_batches_total: u64,
    pub projected_bytes_total: u64,
    pub latency_us_total: u64,
    pub latency_us_buckets: [u64; SEARCH_ROW_FETCH_LATENCY_BUCKET_COUNT],
    pub column_read_by_rowids_page_run_seeks_total: u64,
}

impl SearchRowFetchMetricCounters {
    #[allow(clippy::too_many_arguments)]
    fn record(
        &mut self,
        rows: usize,
        projected_columns: usize,
        segment_groups: usize,
        column_batches: usize,
        fixed_width_column_batches: usize,
        varlen_column_batches: usize,
        projected_bytes: usize,
        column_read_by_rowids_page_run_seeks: usize,
        elapsed_micros: u64,
    ) {
        self.batches_total = self.batches_total.saturating_add(1);
        self.rows_total = self.rows_total.saturating_add(rows as u64);
        self.projected_columns_total = self
            .projected_columns_total
            .saturating_add(projected_columns as u64);
        self.segment_groups_total = self
            .segment_groups_total
            .saturating_add(segment_groups as u64);
        self.column_batches_total = self
            .column_batches_total
            .saturating_add(column_batches as u64);
        self.fixed_width_column_batches_total = self
            .fixed_width_column_batches_total
            .saturating_add(fixed_width_column_batches as u64);
        self.varlen_column_batches_total = self
            .varlen_column_batches_total
            .saturating_add(varlen_column_batches as u64);
        self.projected_bytes_total = self
            .projected_bytes_total
            .saturating_add(projected_bytes as u64);
        self.latency_us_total = self.latency_us_total.saturating_add(elapsed_micros);
        let bucket_idx = latency_bucket_index(
            SEARCH_LATENCY_BUCKETS_US,
            SEARCH_ROW_FETCH_LATENCY_BUCKET_COUNT,
            elapsed_micros,
        );
        self.latency_us_buckets[bucket_idx] = self.latency_us_buckets[bucket_idx].saturating_add(1);
        self.column_read_by_rowids_page_run_seeks_total = self
            .column_read_by_rowids_page_run_seeks_total
            .saturating_add(column_read_by_rowids_page_run_seeks as u64);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRowFetchMetricsByKey {
    pub key: SearchRowFetchMetricKey,
    pub counters: SearchRowFetchMetricCounters,
}

/// Snapshot of storage metrics (read-only view).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMetricsSnapshot {
    pub memory_tag_bytes: [i64; MEMORY_TAG_COUNT],
    pub memory_tag_total: i64,
    pub page_cache_hits: u64,
    pub page_cache_misses: u64,
    pub page_cache_evictions: u64,
    pub page_cache_entries: usize,
    pub primary_index_hits: u64,
    pub primary_index_misses: u64,
    pub primary_index_conflicts: u64,
    pub primary_index_memory_bytes: usize,
    pub persistent_index_flushes: u64,
    pub delete_vector_entries: u64,
    pub memtable_flush_count: u64,
    pub memtable_backpressure_count: u64,
    pub memtable_backpressure_ns: u64,
    pub delta_writer_commit_ns: u64,
    pub delta_writer_commit_count: u64,
    pub delta_writer_flush_ns: u64,
    pub delta_writer_flush_count: u64,
    pub compaction_tasks_total: u64,
    pub compaction_tasks_success: u64,
    pub compaction_tasks_failed: u64,
    pub compaction_duration_ns: u64,
    pub compaction_input_bytes: u64,
    pub compaction_output_bytes: u64,
    pub compaction_queue_len: usize,
    pub compaction_running_tablets: usize,
    pub prefetch_hits: u64,
    pub prefetch_waits: u64,
    pub prefetch_wastes: u64,
    pub decompress_parallel_batches: u64,
    pub decompress_parallel_tasks: u64,
    pub decompress_parallelism_last: u64,
    pub decompress_parallelism_peak: u64,
    pub checkpoint_capture_optimistic_total: u64,
    pub checkpoint_capture_meta_lock_total: u64,
    pub checkpoint_capture_retry_total: u64,
    pub graph_expand_rows: u64,
    pub graph_frontier_size: usize,
    pub graph_frontier_size_peak: usize,
    pub graph_delta_lookups: u64,
    pub graph_delta_hits: u64,
    pub graph_rebuild_latency_ns: u64,
    pub graph_rebuild_count: u64,
    pub derived_index_lag_ts: u64,
    pub tail_exact_merge_cost: u64,
    pub segment_file_open_total: u64,
    pub search_manifest_by_key: Vec<SearchManifestMetricsByKey>,
    pub search_tail_by_key: Vec<SearchTailMetricsByKey>,
    pub search_tail_rejected_by_key: Vec<SearchTailRejectedMetricsByKey>,
    pub search_fulltext_degraded_score_by_key: Vec<SearchFullTextDegradedScoreMetricsByKey>,
    pub search_generation_by_key: Vec<SearchGenerationMetricsByKey>,
    pub search_artifact_gc_delay_by_key: Vec<SearchArtifactGcDelayMetricsByKey>,
    pub search_inline_build_by_key: Vec<SearchInlineBuildMetricsByKey>,
    pub search_inline_build_failures_by_key: Vec<SearchInlineBuildFailureMetricsByKey>,
    pub search_sidecar_build_by_key: Vec<SearchSidecarBuildMetricsByKey>,
    pub search_sidecar_reader_by_key: Vec<SearchSidecarReaderMetricsByKey>,
    pub search_row_fetch_batches_total: u64,
    pub search_row_fetch_rows_total: u64,
    pub search_row_fetch_projected_columns_total: u64,
    pub search_row_fetch_segment_groups_total: u64,
    pub search_row_fetch_column_batches_total: u64,
    pub search_row_fetch_fixed_width_column_batches_total: u64,
    pub search_row_fetch_varlen_column_batches_total: u64,
    pub search_row_fetch_projected_bytes_total: u64,
    pub search_row_fetch_latency_us_total: u64,
    pub search_row_fetch_latency_us_buckets: [u64; SEARCH_ROW_FETCH_LATENCY_BUCKET_COUNT],
    pub search_row_fetch_by_key: Vec<SearchRowFetchMetricsByKey>,
    pub column_read_by_rowids_page_run_seeks_total: u64,
    pub txn_spill_bytes: u64,
    pub txn_spill_artifacts: u64,
    pub txn_spill_wait_us: u64,
    pub txn_spill_admission_rejects: u64,
    pub txn_spill_device_pressure_rejects: u64,
}

impl StorageMetricsSnapshot {
    pub fn graph_delta_hit_ratio(&self) -> f64 {
        if self.graph_delta_lookups == 0 {
            0.0
        } else {
            self.graph_delta_hits as f64 / self.graph_delta_lookups as f64
        }
    }

    pub fn graph_rebuild_latency_avg_ns(&self) -> u64 {
        if self.graph_rebuild_count == 0 {
            0
        } else {
            self.graph_rebuild_latency_ns / self.graph_rebuild_count
        }
    }
}

/// Global storage metrics container.
#[derive(Debug, Default)]
pub struct StorageMetrics {
    memory_tag_bytes: [AtomicI64; MEMORY_TAG_COUNT],
    memory_tag_total: AtomicI64,
    page_cache_hits: AtomicU64,
    page_cache_misses: AtomicU64,
    page_cache_evictions: AtomicU64,
    page_cache_entries: AtomicUsize,
    primary_index_hits: AtomicU64,
    primary_index_misses: AtomicU64,
    primary_index_conflicts: AtomicU64,
    primary_index_memory_bytes: AtomicUsize,
    persistent_index_flushes: AtomicU64,
    delete_vector_entries: AtomicU64,
    memtable_flush_count: AtomicU64,
    memtable_backpressure_count: AtomicU64,
    memtable_backpressure_ns: AtomicU64,
    delta_writer_commit_ns: AtomicU64,
    delta_writer_commit_count: AtomicU64,
    delta_writer_flush_ns: AtomicU64,
    delta_writer_flush_count: AtomicU64,
    compaction_tasks_total: AtomicU64,
    compaction_tasks_success: AtomicU64,
    compaction_tasks_failed: AtomicU64,
    compaction_duration_ns: AtomicU64,
    compaction_input_bytes: AtomicU64,
    compaction_output_bytes: AtomicU64,
    compaction_queue_len: AtomicUsize,
    compaction_running_tablets: AtomicUsize,
    prefetch_hits: AtomicU64,
    prefetch_waits: AtomicU64,
    prefetch_wastes: AtomicU64,
    decompress_parallel_batches: AtomicU64,
    decompress_parallel_tasks: AtomicU64,
    decompress_parallelism_last: AtomicU64,
    decompress_parallelism_peak: AtomicU64,
    checkpoint_capture_optimistic_total: AtomicU64,
    checkpoint_capture_meta_lock_total: AtomicU64,
    checkpoint_capture_retry_total: AtomicU64,
    graph_expand_rows: AtomicU64,
    graph_frontier_size: AtomicUsize,
    graph_frontier_size_peak: AtomicUsize,
    graph_delta_lookups: AtomicU64,
    graph_delta_hits: AtomicU64,
    graph_rebuild_latency_ns: AtomicU64,
    graph_rebuild_count: AtomicU64,
    derived_index_lag_ts: AtomicU64,
    tail_exact_merge_cost: AtomicU64,
    segment_file_open_total: AtomicU64,
    search_manifest_by_key: Mutex<BTreeMap<SearchManifestMetricKey, SearchManifestMetricCounters>>,
    search_tail_by_key: Mutex<BTreeMap<SearchTailMetricKey, SearchTailMetricCounters>>,
    search_tail_rejected_by_key: Mutex<BTreeMap<SearchTailRejectedMetricKey, u64>>,
    search_fulltext_degraded_score_by_key:
        Mutex<BTreeMap<SearchFullTextDegradedScoreMetricKey, u64>>,
    search_generation_by_key:
        Mutex<BTreeMap<SearchGenerationMetricKey, SearchGenerationMetricCounters>>,
    search_artifact_gc_delay_by_key:
        Mutex<BTreeMap<SearchArtifactGcDelayMetricKey, SearchArtifactGcDelayMetricCounters>>,
    search_inline_build_by_key:
        Mutex<BTreeMap<SearchInlineBuildMetricKey, SearchInlineBuildMetricCounters>>,
    search_inline_build_failures_by_key: Mutex<BTreeMap<SearchInlineBuildFailureKey, u64>>,
    search_sidecar_build_by_key:
        Mutex<BTreeMap<SearchSidecarBuildMetricKey, SearchSidecarBuildMetricCounters>>,
    search_sidecar_reader_by_key:
        Mutex<BTreeMap<SearchSidecarReaderMetricKey, SearchSidecarReaderMetricCounters>>,
    search_row_fetch_batches_total: AtomicU64,
    search_row_fetch_rows_total: AtomicU64,
    search_row_fetch_projected_columns_total: AtomicU64,
    search_row_fetch_segment_groups_total: AtomicU64,
    search_row_fetch_column_batches_total: AtomicU64,
    search_row_fetch_fixed_width_column_batches_total: AtomicU64,
    search_row_fetch_varlen_column_batches_total: AtomicU64,
    search_row_fetch_projected_bytes_total: AtomicU64,
    search_row_fetch_latency_us_total: AtomicU64,
    search_row_fetch_latency_us_buckets: [AtomicU64; SEARCH_ROW_FETCH_LATENCY_BUCKET_COUNT],
    search_row_fetch_by_key: Mutex<BTreeMap<SearchRowFetchMetricKey, SearchRowFetchMetricCounters>>,
    column_read_by_rowids_page_run_seeks_total: AtomicU64,
    txn_spill_bytes: AtomicU64,
    txn_spill_artifacts: AtomicU64,
    txn_spill_wait_us: AtomicU64,
    txn_spill_admission_rejects: AtomicU64,
    txn_spill_device_pressure_rejects: AtomicU64,
}

impl StorageMetrics {
    fn global() -> &'static StorageMetrics {
        static INSTANCE: OnceLock<StorageMetrics> = OnceLock::new();
        INSTANCE.get_or_init(StorageMetrics::default)
    }

    /// Update memory usage snapshot for per-tag metrics.
    pub fn set_memory_usage_snapshot(&self, snapshot: &MemoryUsageSnapshot) {
        for (idx, value) in snapshot.usage_per_tag.iter().enumerate() {
            self.memory_tag_bytes[idx].store(*value, Ordering::Relaxed);
        }
        self.memory_tag_total
            .store(snapshot.total_usage, Ordering::Relaxed);
    }

    /// Record a PageCache hit.
    pub fn inc_page_cache_hit(&self) {
        self.page_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a PageCache miss.
    pub fn inc_page_cache_miss(&self) {
        self.page_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a PageCache eviction.
    pub fn inc_page_cache_eviction(&self) {
        self.page_cache_evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Update PageCache entry count gauge.
    pub fn set_page_cache_entries(&self, entries: usize) {
        self.page_cache_entries.store(entries, Ordering::Relaxed);
    }

    /// Record a PrimaryIndex lookup hit.
    pub fn inc_primary_index_hit(&self) {
        self.primary_index_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a PrimaryIndex lookup miss.
    pub fn inc_primary_index_miss(&self) {
        self.primary_index_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record PrimaryIndex conflicts (upsert replaced an existing key).
    pub fn inc_primary_index_conflicts(&self, delta: u64) {
        if delta > 0 {
            self.primary_index_conflicts
                .fetch_add(delta, Ordering::Relaxed);
        }
    }

    /// Update PrimaryIndex memory usage gauge.
    pub fn set_primary_index_memory(&self, bytes: usize) {
        self.primary_index_memory_bytes
            .store(bytes, Ordering::Relaxed);
    }

    /// Record an L0→L1 flush in PersistentIndex.
    pub fn inc_persistent_index_flushes(&self) {
        self.persistent_index_flushes
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record added DeleteVector entries (unique row ids).
    pub fn inc_delete_vector_entries(&self, delta: u64) {
        if delta > 0 {
            self.delete_vector_entries
                .fetch_add(delta, Ordering::Relaxed);
        }
    }

    /// Record a MemTable flush.
    pub fn inc_memtable_flush(&self) {
        self.memtable_flush_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a MemTable backpressure event.
    pub fn inc_memtable_backpressure(&self) {
        self.memtable_backpressure_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record MemTable backpressure duration.
    pub fn add_memtable_backpressure_time(&self, duration: std::time::Duration) {
        self.memtable_backpressure_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Record DeltaWriter commit duration.
    pub fn add_delta_writer_commit_time(&self, duration: std::time::Duration) {
        self.delta_writer_commit_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.delta_writer_commit_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record DeltaWriter flush duration.
    pub fn add_delta_writer_flush_time(&self, duration: std::time::Duration) {
        self.delta_writer_flush_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.delta_writer_flush_count
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_compaction_tasks(&self) {
        self.compaction_tasks_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_compaction_success(&self) {
        self.compaction_tasks_success
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_compaction_failed(&self) {
        self.compaction_tasks_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_compaction_duration(&self, duration: std::time::Duration) {
        self.compaction_duration_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }

    pub fn add_compaction_bytes(&self, input: u64, output: u64) {
        self.compaction_input_bytes
            .fetch_add(input, Ordering::Relaxed);
        self.compaction_output_bytes
            .fetch_add(output, Ordering::Relaxed);
    }

    /// Update compaction candidate queue length gauge.
    pub fn set_compaction_queue_len(&self, len: usize) {
        self.compaction_queue_len.store(len, Ordering::Relaxed);
    }

    /// Update currently running compaction tablets gauge.
    pub fn set_compaction_running_tablets(&self, count: usize) {
        self.compaction_running_tablets
            .store(count, Ordering::Relaxed);
    }

    /// Record a prefetch hit (page consumed after prefetch ready).
    pub fn inc_prefetch_hit(&self) {
        self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a prefetch wait (consumer waited on in-flight prefetch).
    pub fn inc_prefetch_wait(&self) {
        self.prefetch_waits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record wasted prefetch pages (prefetched but unused).
    pub fn add_prefetch_waste(&self, delta: u64) {
        if delta > 0 {
            self.prefetch_wastes.fetch_add(delta, Ordering::Relaxed);
        }
    }

    /// Record one decompression batch execution and effective parallelism.
    pub fn record_parallel_decompress(&self, parallelism: usize, tasks: usize) {
        if tasks == 0 {
            return;
        }
        let workers = parallelism.max(1) as u64;
        self.decompress_parallel_batches
            .fetch_add(1, Ordering::Relaxed);
        self.decompress_parallel_tasks
            .fetch_add(tasks as u64, Ordering::Relaxed);
        self.decompress_parallelism_last
            .store(workers, Ordering::Relaxed);
        self.decompress_parallelism_peak
            .fetch_max(workers, Ordering::Relaxed);
    }

    /// Record one checkpoint tablet freeze capture and any optimistic retries
    /// that were invalidated before the final snapshot mode succeeded.
    pub fn record_checkpoint_capture(&self, used_meta_lock: bool, retries: usize) {
        if used_meta_lock {
            self.checkpoint_capture_meta_lock_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.checkpoint_capture_optimistic_total
                .fetch_add(1, Ordering::Relaxed);
        }
        if retries > 0 {
            self.checkpoint_capture_retry_total
                .fetch_add(retries as u64, Ordering::Relaxed);
        }
    }

    /// Record graph rows emitted by expand / shortest-path operators.
    pub fn add_graph_expand_rows(&self, rows: usize) {
        if rows > 0 {
            self.graph_expand_rows
                .fetch_add(rows as u64, Ordering::Relaxed);
        }
    }

    /// Update the latest observed graph frontier size and keep a peak gauge.
    pub fn set_graph_frontier_size(&self, size: usize) {
        self.graph_frontier_size.store(size, Ordering::Relaxed);
        self.graph_frontier_size_peak
            .fetch_max(size, Ordering::Relaxed);
    }

    /// Record whether a graph neighbor lookup had to merge committed deltas.
    pub fn record_graph_delta_lookup(&self, hit: bool) {
        self.graph_delta_lookups.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.graph_delta_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record full rebuild latency for REFRESH / recovery rebuild paths.
    pub fn add_graph_rebuild_latency(&self, duration: std::time::Duration) {
        self.graph_rebuild_latency_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.graph_rebuild_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the highest commit timestamp whose derived search/graph publish
    /// still needs retry or catch-up. Core storage/catalog visibility is not
    /// gated by this gauge.
    pub fn record_derived_index_lag_ts(&self, commit_ts: u64) {
        self.derived_index_lag_ts
            .fetch_max(commit_ts, Ordering::Relaxed);
    }

    /// Record the highest observed exact tail merge cost on read
    /// paths. The value is the planner's composite latency weight.
    pub fn record_tail_exact_merge_cost(&self, cost: u64) {
        self.tail_exact_merge_cost
            .fetch_max(cost, Ordering::Relaxed);
    }

    /// Record a successful segment file open on a read/metadata path.
    pub fn record_segment_file_open(&self) {
        self.segment_file_open_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record successful search manifest publish latency.
    pub fn record_search_manifest_publish(&self, codec: &'static str, elapsed_micros: u64) {
        self.search_manifest_by_key
            .lock()
            .expect("search manifest metrics lock poisoned")
            .entry(SearchManifestMetricKey {
                codec: codec.to_string(),
            })
            .or_default()
            .record_publish_latency(elapsed_micros);
    }

    /// Record successful search manifest open latency.
    pub fn record_search_manifest_open(&self, codec: &'static str, elapsed_micros: u64) {
        self.search_manifest_by_key
            .lock()
            .expect("search manifest metrics lock poisoned")
            .entry(SearchManifestMetricKey {
                codec: codec.to_string(),
            })
            .or_default()
            .record_open_latency(elapsed_micros);
    }

    /// Add bytes read while opening or replaying search manifest files.
    pub fn add_search_manifest_open_bytes(&self, codec: &'static str, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut metrics = self
            .search_manifest_by_key
            .lock()
            .expect("search manifest metrics lock poisoned");
        let counters = metrics
            .entry(SearchManifestMetricKey {
                codec: codec.to_string(),
            })
            .or_default();
        counters.open_bytes_total = counters.open_bytes_total.saturating_add(bytes);
    }

    /// Set the latest observed search manifest recent-delta count.
    pub fn set_search_manifest_delta_count(&self, codec: &'static str, delta_count: usize) {
        let mut metrics = self
            .search_manifest_by_key
            .lock()
            .expect("search manifest metrics lock poisoned");
        metrics
            .entry(SearchManifestMetricKey {
                codec: codec.to_string(),
            })
            .or_default()
            .delta_count = delta_count as u64;
    }

    /// Set latest provider tail gauges from manifest coverage.
    pub fn set_search_tail_gauges(
        &self,
        provider: SearchIndexKind,
        tail_rows: u64,
        tail_bytes: u64,
        backlog_tier: u64,
    ) {
        let mut metrics = self
            .search_tail_by_key
            .lock()
            .expect("search tail metrics lock poisoned");
        let counters = metrics.entry(SearchTailMetricKey { provider }).or_default();
        counters.tail_rows = tail_rows;
        counters.tail_bytes = tail_bytes;
        counters.tail_backlog_tier = backlog_tier;
    }

    /// Add rows admitted into query-time exact tail merge.
    pub fn add_search_tail_exact_merge_rows(&self, provider: SearchIndexKind, rows: u64) {
        if rows == 0 {
            return;
        }
        let mut metrics = self
            .search_tail_by_key
            .lock()
            .expect("search tail metrics lock poisoned");
        let counters = metrics.entry(SearchTailMetricKey { provider }).or_default();
        counters.exact_merge_rows_total = counters.exact_merge_rows_total.saturating_add(rows);
    }

    /// Record a query-time exact tail merge rejection.
    pub fn record_search_tail_exact_merge_rejected(
        &self,
        provider: SearchIndexKind,
        reason: &'static str,
    ) {
        let key = SearchTailRejectedMetricKey {
            provider,
            reason: reason.to_string(),
        };
        let mut rejected = self
            .search_tail_rejected_by_key
            .lock()
            .expect("search tail rejection metrics lock poisoned");
        let counter = rejected.entry(key).or_default();
        *counter = counter.saturating_add(1);
    }

    /// Record a FullText query whose final scorer had to use a degraded stats path.
    pub fn record_search_fulltext_degraded_score(&self, table_id: u64, reason: &'static str) {
        let key = SearchFullTextDegradedScoreMetricKey {
            table_id,
            reason: reason.to_string(),
        };
        let mut metrics = self
            .search_fulltext_degraded_score_by_key
            .lock()
            .expect("search fulltext degraded score metrics lock poisoned");
        let counter = metrics.entry(key).or_default();
        *counter = counter.saturating_add(1);
    }

    /// Record a retired search generation or manifest path set.
    pub fn record_search_generation_retired(&self, provider: SearchIndexKind, bytes: u64) {
        let mut metrics = self
            .search_generation_by_key
            .lock()
            .expect("search generation metrics lock poisoned");
        let counters = metrics
            .entry(SearchGenerationMetricKey { provider })
            .or_default();
        counters.retired_total = counters.retired_total.saturating_add(1);
        counters.retired_bytes_total = counters.retired_bytes_total.saturating_add(bytes);
    }

    /// Record how long a retired generation remained pinned by active leases.
    pub fn record_search_generation_lease_hold(
        &self,
        provider: SearchIndexKind,
        elapsed_micros: u64,
    ) {
        let mut metrics = self
            .search_generation_by_key
            .lock()
            .expect("search generation metrics lock poisoned");
        let counters = metrics
            .entry(SearchGenerationMetricKey { provider })
            .or_default();
        let bucket_idx = latency_bucket_index(
            SEARCH_BUILD_LATENCY_BUCKETS_US,
            SEARCH_GENERATION_LATENCY_BUCKET_COUNT,
            elapsed_micros,
        );
        counters.lease_hold_time_us_buckets[bucket_idx] =
            counters.lease_hold_time_us_buckets[bucket_idx].saturating_add(1);
    }

    /// Record delay before artifact/manifest GC could reclaim retired paths.
    pub fn record_search_artifact_gc_delay(
        &self,
        provider: SearchIndexKind,
        reason: &'static str,
        elapsed_micros: u64,
    ) {
        let key = SearchArtifactGcDelayMetricKey {
            provider,
            reason: reason.to_string(),
        };
        let mut metrics = self
            .search_artifact_gc_delay_by_key
            .lock()
            .expect("search artifact gc delay metrics lock poisoned");
        let counters = metrics.entry(key).or_default();
        let bucket_idx = latency_bucket_index(
            SEARCH_BUILD_LATENCY_BUCKETS_US,
            SEARCH_GENERATION_LATENCY_BUCKET_COUNT,
            elapsed_micros,
        );
        counters.delay_us_buckets[bucket_idx] =
            counters.delay_us_buckets[bucket_idx].saturating_add(1);
    }

    /// Record one successful writer-side inline search artifact build.
    pub fn record_search_inline_build(
        &self,
        key: SearchInlineBuildMetricKey,
        rows: u64,
        bytes: u64,
        elapsed_micros: u64,
        cpu_micros: u64,
    ) {
        self.search_inline_build_by_key
            .lock()
            .expect("search inline build metrics lock poisoned")
            .entry(key)
            .or_default()
            .record(rows, bytes, elapsed_micros, cpu_micros);
    }

    /// Record a writer-side inline search build failure.
    pub fn record_search_inline_build_failure(
        &self,
        provider: SearchIndexKind,
        reason: &'static str,
    ) {
        let key = SearchInlineBuildFailureKey {
            provider,
            reason: reason.to_string(),
        };
        let mut failures = self
            .search_inline_build_failures_by_key
            .lock()
            .expect("search inline build failure metrics lock poisoned");
        let counter = failures.entry(key).or_default();
        *counter = counter.saturating_add(1);
    }

    /// Record one successful sidecar search artifact build.
    pub fn record_search_sidecar_build(
        &self,
        key: SearchSidecarBuildMetricKey,
        rows: u64,
        read_bytes: u64,
        write_bytes: u64,
        artifact_bytes: u64,
        elapsed_micros: u64,
    ) {
        self.search_sidecar_build_by_key
            .lock()
            .expect("search sidecar build metrics lock poisoned")
            .entry(key)
            .or_default()
            .record(
                rows,
                read_bytes,
                write_bytes,
                artifact_bytes,
                elapsed_micros,
            );
    }

    /// Record a sidecar package open on a reader cache miss.
    pub fn record_search_sidecar_reader_open(
        &self,
        provider: SearchIndexKind,
        codec: &'static str,
    ) {
        let mut metrics = self
            .search_sidecar_reader_by_key
            .lock()
            .expect("search sidecar reader metrics lock poisoned");
        let counters = metrics
            .entry(SearchSidecarReaderMetricKey {
                provider,
                codec: codec.to_string(),
            })
            .or_default();
        counters.open_count_total = counters.open_count_total.saturating_add(1);
    }

    /// Record a sidecar artifact reader cache hit.
    pub fn record_search_sidecar_reader_cache_hit(
        &self,
        provider: SearchIndexKind,
        codec: &'static str,
    ) {
        let mut metrics = self
            .search_sidecar_reader_by_key
            .lock()
            .expect("search sidecar reader metrics lock poisoned");
        let counters = metrics
            .entry(SearchSidecarReaderMetricKey {
                provider,
                codec: codec.to_string(),
            })
            .or_default();
        counters.cache_hits_total = counters.cache_hits_total.saturating_add(1);
    }

    /// Record a sidecar artifact reader cache miss.
    pub fn record_search_sidecar_reader_cache_miss(
        &self,
        provider: SearchIndexKind,
        codec: &'static str,
    ) {
        let mut metrics = self
            .search_sidecar_reader_by_key
            .lock()
            .expect("search sidecar reader metrics lock poisoned");
        let counters = metrics
            .entry(SearchSidecarReaderMetricKey {
                provider,
                codec: codec.to_string(),
            })
            .or_default();
        counters.cache_misses_total = counters.cache_misses_total.saturating_add(1);
    }

    /// Add bytes pinned by the sidecar reader cache.
    pub fn add_search_sidecar_reader_mmap_bytes(
        &self,
        provider: SearchIndexKind,
        codec: &'static str,
        bytes: u64,
    ) {
        if bytes == 0 {
            return;
        }
        let mut metrics = self
            .search_sidecar_reader_by_key
            .lock()
            .expect("search sidecar reader metrics lock poisoned");
        let counters = metrics
            .entry(SearchSidecarReaderMetricKey {
                provider,
                codec: codec.to_string(),
            })
            .or_default();
        counters.mmap_bytes = counters.mmap_bytes.saturating_add(bytes);
    }

    /// Record a reader dispatch by artifact format version.
    pub fn record_search_sidecar_reader_format_dispatch(
        &self,
        provider: SearchIndexKind,
        codec: &'static str,
    ) {
        let mut metrics = self
            .search_sidecar_reader_by_key
            .lock()
            .expect("search sidecar reader metrics lock poisoned");
        let counters = metrics
            .entry(SearchSidecarReaderMetricKey {
                provider,
                codec: codec.to_string(),
            })
            .or_default();
        counters.format_dispatch_total = counters.format_dispatch_total.saturating_add(1);
    }

    /// Record one successful search late-materialization row fetch batch.
    #[allow(clippy::too_many_arguments)]
    pub fn record_search_row_fetch(
        &self,
        key: SearchRowFetchMetricKey,
        rows: usize,
        projected_columns: usize,
        segment_groups: usize,
        column_batches: usize,
        fixed_width_column_batches: usize,
        varlen_column_batches: usize,
        projected_bytes: usize,
        column_read_by_rowids_page_run_seeks: usize,
        elapsed_micros: u64,
    ) {
        self.search_row_fetch_batches_total
            .fetch_add(1, Ordering::Relaxed);
        add_usize_counter(&self.search_row_fetch_rows_total, rows);
        add_usize_counter(
            &self.search_row_fetch_projected_columns_total,
            projected_columns,
        );
        add_usize_counter(&self.search_row_fetch_segment_groups_total, segment_groups);
        add_usize_counter(&self.search_row_fetch_column_batches_total, column_batches);
        add_usize_counter(
            &self.search_row_fetch_fixed_width_column_batches_total,
            fixed_width_column_batches,
        );
        add_usize_counter(
            &self.search_row_fetch_varlen_column_batches_total,
            varlen_column_batches,
        );
        add_usize_counter(
            &self.search_row_fetch_projected_bytes_total,
            projected_bytes,
        );
        if elapsed_micros > 0 {
            self.search_row_fetch_latency_us_total
                .fetch_add(elapsed_micros, Ordering::Relaxed);
        }
        self.search_row_fetch_latency_us_buckets[latency_bucket_index(
            SEARCH_LATENCY_BUCKETS_US,
            SEARCH_ROW_FETCH_LATENCY_BUCKET_COUNT,
            elapsed_micros,
        )]
        .fetch_add(1, Ordering::Relaxed);
        self.search_row_fetch_by_key
            .lock()
            .expect("search row fetch metrics lock poisoned")
            .entry(key)
            .or_default()
            .record(
                rows,
                projected_columns,
                segment_groups,
                column_batches,
                fixed_width_column_batches,
                varlen_column_batches,
                projected_bytes,
                column_read_by_rowids_page_run_seeks,
                elapsed_micros,
            );
    }

    /// Record page-local span seeks performed by the column-layer random rowid path.
    pub fn add_column_read_by_rowids_page_run_seeks(&self, delta: usize) {
        add_usize_counter(&self.column_read_by_rowids_page_run_seeks_total, delta);
    }

    pub fn add_txn_spill_bytes(&self, bytes: u64) {
        if bytes > 0 {
            self.txn_spill_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub fn inc_txn_spill_artifacts(&self) {
        self.txn_spill_artifacts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_txn_spill_wait_time(&self, duration: std::time::Duration) {
        self.txn_spill_wait_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn inc_txn_spill_admission_rejects(&self) {
        self.txn_spill_admission_rejects
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_txn_spill_device_pressure_rejects(&self) {
        self.txn_spill_device_pressure_rejects
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent snapshot of all metrics.
    pub fn snapshot(&self) -> StorageMetricsSnapshot {
        let mut tag_bytes = [0i64; MEMORY_TAG_COUNT];
        for (idx, counter) in self.memory_tag_bytes.iter().enumerate() {
            tag_bytes[idx] = counter.load(Ordering::Relaxed);
        }
        let mut search_row_fetch_latency_us_buckets = [0u64; SEARCH_ROW_FETCH_LATENCY_BUCKET_COUNT];
        for (idx, counter) in self.search_row_fetch_latency_us_buckets.iter().enumerate() {
            search_row_fetch_latency_us_buckets[idx] = counter.load(Ordering::Relaxed);
        }
        let search_row_fetch_by_key = self
            .search_row_fetch_by_key
            .lock()
            .expect("search row fetch metrics lock poisoned")
            .iter()
            .map(|(key, counters)| SearchRowFetchMetricsByKey {
                key: *key,
                counters: counters.clone(),
            })
            .collect();
        let search_inline_build_by_key = self
            .search_inline_build_by_key
            .lock()
            .expect("search inline build metrics lock poisoned")
            .iter()
            .map(|(key, counters)| SearchInlineBuildMetricsByKey {
                key: *key,
                counters: counters.clone(),
            })
            .collect();
        let search_inline_build_failures_by_key = self
            .search_inline_build_failures_by_key
            .lock()
            .expect("search inline build failure metrics lock poisoned")
            .iter()
            .map(
                |(key, failures_total)| SearchInlineBuildFailureMetricsByKey {
                    key: key.clone(),
                    failures_total: *failures_total,
                },
            )
            .collect();
        let search_sidecar_build_by_key = self
            .search_sidecar_build_by_key
            .lock()
            .expect("search sidecar build metrics lock poisoned")
            .iter()
            .map(|(key, counters)| SearchSidecarBuildMetricsByKey {
                key: *key,
                counters: counters.clone(),
            })
            .collect();
        let search_manifest_by_key = self
            .search_manifest_by_key
            .lock()
            .expect("search manifest metrics lock poisoned")
            .iter()
            .map(|(key, counters)| SearchManifestMetricsByKey {
                key: key.clone(),
                counters: counters.clone(),
            })
            .collect();
        let search_tail_by_key = self
            .search_tail_by_key
            .lock()
            .expect("search tail metrics lock poisoned")
            .iter()
            .map(|(key, counters)| SearchTailMetricsByKey {
                key: *key,
                counters: counters.clone(),
            })
            .collect();
        let search_tail_rejected_by_key = self
            .search_tail_rejected_by_key
            .lock()
            .expect("search tail rejection metrics lock poisoned")
            .iter()
            .map(|(key, rejected_total)| SearchTailRejectedMetricsByKey {
                key: key.clone(),
                rejected_total: *rejected_total,
            })
            .collect();
        let search_fulltext_degraded_score_by_key = self
            .search_fulltext_degraded_score_by_key
            .lock()
            .expect("search fulltext degraded score metrics lock poisoned")
            .iter()
            .map(
                |(key, degraded_queries)| SearchFullTextDegradedScoreMetricsByKey {
                    key: key.clone(),
                    degraded_queries: *degraded_queries,
                },
            )
            .collect();
        let search_generation_by_key = self
            .search_generation_by_key
            .lock()
            .expect("search generation metrics lock poisoned")
            .iter()
            .map(|(key, counters)| SearchGenerationMetricsByKey {
                key: *key,
                counters: counters.clone(),
            })
            .collect();
        let search_artifact_gc_delay_by_key = self
            .search_artifact_gc_delay_by_key
            .lock()
            .expect("search artifact gc delay metrics lock poisoned")
            .iter()
            .map(|(key, counters)| SearchArtifactGcDelayMetricsByKey {
                key: key.clone(),
                counters: counters.clone(),
            })
            .collect();
        let search_sidecar_reader_by_key = self
            .search_sidecar_reader_by_key
            .lock()
            .expect("search sidecar reader metrics lock poisoned")
            .iter()
            .map(|(key, counters)| SearchSidecarReaderMetricsByKey {
                key: key.clone(),
                counters: counters.clone(),
            })
            .collect();

        StorageMetricsSnapshot {
            memory_tag_bytes: tag_bytes,
            memory_tag_total: self.memory_tag_total.load(Ordering::Relaxed),
            page_cache_hits: self.page_cache_hits.load(Ordering::Relaxed),
            page_cache_misses: self.page_cache_misses.load(Ordering::Relaxed),
            page_cache_evictions: self.page_cache_evictions.load(Ordering::Relaxed),
            page_cache_entries: self.page_cache_entries.load(Ordering::Relaxed),
            primary_index_hits: self.primary_index_hits.load(Ordering::Relaxed),
            primary_index_misses: self.primary_index_misses.load(Ordering::Relaxed),
            primary_index_conflicts: self.primary_index_conflicts.load(Ordering::Relaxed),
            primary_index_memory_bytes: self.primary_index_memory_bytes.load(Ordering::Relaxed),
            persistent_index_flushes: self.persistent_index_flushes.load(Ordering::Relaxed),
            delete_vector_entries: self.delete_vector_entries.load(Ordering::Relaxed),
            memtable_flush_count: self.memtable_flush_count.load(Ordering::Relaxed),
            memtable_backpressure_count: self.memtable_backpressure_count.load(Ordering::Relaxed),
            memtable_backpressure_ns: self.memtable_backpressure_ns.load(Ordering::Relaxed),
            delta_writer_commit_ns: self.delta_writer_commit_ns.load(Ordering::Relaxed),
            delta_writer_commit_count: self.delta_writer_commit_count.load(Ordering::Relaxed),
            delta_writer_flush_ns: self.delta_writer_flush_ns.load(Ordering::Relaxed),
            delta_writer_flush_count: self.delta_writer_flush_count.load(Ordering::Relaxed),
            compaction_tasks_total: self.compaction_tasks_total.load(Ordering::Relaxed),
            compaction_tasks_success: self.compaction_tasks_success.load(Ordering::Relaxed),
            compaction_tasks_failed: self.compaction_tasks_failed.load(Ordering::Relaxed),
            compaction_duration_ns: self.compaction_duration_ns.load(Ordering::Relaxed),
            compaction_input_bytes: self.compaction_input_bytes.load(Ordering::Relaxed),
            compaction_output_bytes: self.compaction_output_bytes.load(Ordering::Relaxed),
            compaction_queue_len: self.compaction_queue_len.load(Ordering::Relaxed),
            compaction_running_tablets: self.compaction_running_tablets.load(Ordering::Relaxed),
            prefetch_hits: self.prefetch_hits.load(Ordering::Relaxed),
            prefetch_waits: self.prefetch_waits.load(Ordering::Relaxed),
            prefetch_wastes: self.prefetch_wastes.load(Ordering::Relaxed),
            decompress_parallel_batches: self.decompress_parallel_batches.load(Ordering::Relaxed),
            decompress_parallel_tasks: self.decompress_parallel_tasks.load(Ordering::Relaxed),
            decompress_parallelism_last: self.decompress_parallelism_last.load(Ordering::Relaxed),
            decompress_parallelism_peak: self.decompress_parallelism_peak.load(Ordering::Relaxed),
            checkpoint_capture_optimistic_total: self
                .checkpoint_capture_optimistic_total
                .load(Ordering::Relaxed),
            checkpoint_capture_meta_lock_total: self
                .checkpoint_capture_meta_lock_total
                .load(Ordering::Relaxed),
            checkpoint_capture_retry_total: self
                .checkpoint_capture_retry_total
                .load(Ordering::Relaxed),
            graph_expand_rows: self.graph_expand_rows.load(Ordering::Relaxed),
            graph_frontier_size: self.graph_frontier_size.load(Ordering::Relaxed),
            graph_frontier_size_peak: self.graph_frontier_size_peak.load(Ordering::Relaxed),
            graph_delta_lookups: self.graph_delta_lookups.load(Ordering::Relaxed),
            graph_delta_hits: self.graph_delta_hits.load(Ordering::Relaxed),
            graph_rebuild_latency_ns: self.graph_rebuild_latency_ns.load(Ordering::Relaxed),
            graph_rebuild_count: self.graph_rebuild_count.load(Ordering::Relaxed),
            derived_index_lag_ts: self.derived_index_lag_ts.load(Ordering::Relaxed),
            tail_exact_merge_cost: self.tail_exact_merge_cost.load(Ordering::Relaxed),
            segment_file_open_total: self.segment_file_open_total.load(Ordering::Relaxed),
            search_manifest_by_key,
            search_tail_by_key,
            search_tail_rejected_by_key,
            search_fulltext_degraded_score_by_key,
            search_generation_by_key,
            search_artifact_gc_delay_by_key,
            search_inline_build_by_key,
            search_inline_build_failures_by_key,
            search_sidecar_build_by_key,
            search_sidecar_reader_by_key,
            search_row_fetch_batches_total: self
                .search_row_fetch_batches_total
                .load(Ordering::Relaxed),
            search_row_fetch_rows_total: self.search_row_fetch_rows_total.load(Ordering::Relaxed),
            search_row_fetch_projected_columns_total: self
                .search_row_fetch_projected_columns_total
                .load(Ordering::Relaxed),
            search_row_fetch_segment_groups_total: self
                .search_row_fetch_segment_groups_total
                .load(Ordering::Relaxed),
            search_row_fetch_column_batches_total: self
                .search_row_fetch_column_batches_total
                .load(Ordering::Relaxed),
            search_row_fetch_fixed_width_column_batches_total: self
                .search_row_fetch_fixed_width_column_batches_total
                .load(Ordering::Relaxed),
            search_row_fetch_varlen_column_batches_total: self
                .search_row_fetch_varlen_column_batches_total
                .load(Ordering::Relaxed),
            search_row_fetch_projected_bytes_total: self
                .search_row_fetch_projected_bytes_total
                .load(Ordering::Relaxed),
            search_row_fetch_latency_us_total: self
                .search_row_fetch_latency_us_total
                .load(Ordering::Relaxed),
            search_row_fetch_latency_us_buckets,
            search_row_fetch_by_key,
            column_read_by_rowids_page_run_seeks_total: self
                .column_read_by_rowids_page_run_seeks_total
                .load(Ordering::Relaxed),
            txn_spill_bytes: self.txn_spill_bytes.load(Ordering::Relaxed),
            txn_spill_artifacts: self.txn_spill_artifacts.load(Ordering::Relaxed),
            txn_spill_wait_us: self.txn_spill_wait_us.load(Ordering::Relaxed),
            txn_spill_admission_rejects: self.txn_spill_admission_rejects.load(Ordering::Relaxed),
            txn_spill_device_pressure_rejects: self
                .txn_spill_device_pressure_rejects
                .load(Ordering::Relaxed),
        }
    }

    /// Test-only helper to clear all counters.
    #[cfg(test)]
    pub fn reset_for_tests(&self) {
        for counter in &self.memory_tag_bytes {
            counter.store(0, Ordering::Relaxed);
        }
        self.memory_tag_total.store(0, Ordering::Relaxed);
        self.page_cache_hits.store(0, Ordering::Relaxed);
        self.page_cache_misses.store(0, Ordering::Relaxed);
        self.page_cache_evictions.store(0, Ordering::Relaxed);
        self.page_cache_entries.store(0, Ordering::Relaxed);
        self.primary_index_hits.store(0, Ordering::Relaxed);
        self.primary_index_misses.store(0, Ordering::Relaxed);
        self.primary_index_conflicts.store(0, Ordering::Relaxed);
        self.primary_index_memory_bytes.store(0, Ordering::Relaxed);
        self.persistent_index_flushes.store(0, Ordering::Relaxed);
        self.delete_vector_entries.store(0, Ordering::Relaxed);
        self.memtable_flush_count.store(0, Ordering::Relaxed);
        self.memtable_backpressure_count.store(0, Ordering::Relaxed);
        self.memtable_backpressure_ns.store(0, Ordering::Relaxed);
        self.delta_writer_commit_ns.store(0, Ordering::Relaxed);
        self.delta_writer_commit_count.store(0, Ordering::Relaxed);
        self.delta_writer_flush_ns.store(0, Ordering::Relaxed);
        self.delta_writer_flush_count.store(0, Ordering::Relaxed);
        self.compaction_tasks_total.store(0, Ordering::Relaxed);
        self.compaction_tasks_success.store(0, Ordering::Relaxed);
        self.compaction_tasks_failed.store(0, Ordering::Relaxed);
        self.compaction_duration_ns.store(0, Ordering::Relaxed);
        self.compaction_input_bytes.store(0, Ordering::Relaxed);
        self.compaction_output_bytes.store(0, Ordering::Relaxed);
        self.compaction_queue_len.store(0, Ordering::Relaxed);
        self.compaction_running_tablets.store(0, Ordering::Relaxed);
        self.prefetch_hits.store(0, Ordering::Relaxed);
        self.prefetch_waits.store(0, Ordering::Relaxed);
        self.prefetch_wastes.store(0, Ordering::Relaxed);
        self.decompress_parallel_batches.store(0, Ordering::Relaxed);
        self.decompress_parallel_tasks.store(0, Ordering::Relaxed);
        self.decompress_parallelism_last.store(0, Ordering::Relaxed);
        self.decompress_parallelism_peak.store(0, Ordering::Relaxed);
        self.checkpoint_capture_optimistic_total
            .store(0, Ordering::Relaxed);
        self.checkpoint_capture_meta_lock_total
            .store(0, Ordering::Relaxed);
        self.checkpoint_capture_retry_total
            .store(0, Ordering::Relaxed);
        self.graph_expand_rows.store(0, Ordering::Relaxed);
        self.graph_frontier_size.store(0, Ordering::Relaxed);
        self.graph_frontier_size_peak.store(0, Ordering::Relaxed);
        self.graph_delta_lookups.store(0, Ordering::Relaxed);
        self.graph_delta_hits.store(0, Ordering::Relaxed);
        self.graph_rebuild_latency_ns.store(0, Ordering::Relaxed);
        self.graph_rebuild_count.store(0, Ordering::Relaxed);
        self.derived_index_lag_ts.store(0, Ordering::Relaxed);
        self.tail_exact_merge_cost.store(0, Ordering::Relaxed);
        self.search_manifest_by_key
            .lock()
            .expect("search manifest metrics lock poisoned")
            .clear();
        self.search_tail_by_key
            .lock()
            .expect("search tail metrics lock poisoned")
            .clear();
        self.search_tail_rejected_by_key
            .lock()
            .expect("search tail rejection metrics lock poisoned")
            .clear();
        self.search_fulltext_degraded_score_by_key
            .lock()
            .expect("search fulltext degraded score metrics lock poisoned")
            .clear();
        self.search_generation_by_key
            .lock()
            .expect("search generation metrics lock poisoned")
            .clear();
        self.search_artifact_gc_delay_by_key
            .lock()
            .expect("search artifact gc delay metrics lock poisoned")
            .clear();
        self.search_inline_build_by_key
            .lock()
            .expect("search inline build metrics lock poisoned")
            .clear();
        self.search_inline_build_failures_by_key
            .lock()
            .expect("search inline build failure metrics lock poisoned")
            .clear();
        self.search_sidecar_build_by_key
            .lock()
            .expect("search sidecar build metrics lock poisoned")
            .clear();
        self.search_sidecar_reader_by_key
            .lock()
            .expect("search sidecar reader metrics lock poisoned")
            .clear();
        self.search_row_fetch_batches_total
            .store(0, Ordering::Relaxed);
        self.search_row_fetch_rows_total.store(0, Ordering::Relaxed);
        self.search_row_fetch_projected_columns_total
            .store(0, Ordering::Relaxed);
        self.search_row_fetch_segment_groups_total
            .store(0, Ordering::Relaxed);
        self.search_row_fetch_column_batches_total
            .store(0, Ordering::Relaxed);
        self.search_row_fetch_fixed_width_column_batches_total
            .store(0, Ordering::Relaxed);
        self.search_row_fetch_varlen_column_batches_total
            .store(0, Ordering::Relaxed);
        self.search_row_fetch_projected_bytes_total
            .store(0, Ordering::Relaxed);
        self.search_row_fetch_latency_us_total
            .store(0, Ordering::Relaxed);
        for counter in &self.search_row_fetch_latency_us_buckets {
            counter.store(0, Ordering::Relaxed);
        }
        self.search_row_fetch_by_key
            .lock()
            .expect("search row fetch metrics lock poisoned")
            .clear();
        self.column_read_by_rowids_page_run_seeks_total
            .store(0, Ordering::Relaxed);
        self.txn_spill_bytes.store(0, Ordering::Relaxed);
        self.txn_spill_artifacts.store(0, Ordering::Relaxed);
        self.txn_spill_wait_us.store(0, Ordering::Relaxed);
        self.txn_spill_admission_rejects.store(0, Ordering::Relaxed);
        self.txn_spill_device_pressure_rejects
            .store(0, Ordering::Relaxed);
    }
}

/// Get global storage metrics singleton.
pub fn storage_metrics() -> &'static StorageMetrics {
    StorageMetrics::global()
}

#[cfg(test)]
pub(crate) fn storage_metrics_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static STORAGE_METRICS_TEST_GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    STORAGE_METRICS_TEST_GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("storage metrics test guard poisoned")
}

fn add_usize_counter(counter: &AtomicU64, delta: usize) {
    if delta > 0 {
        counter.fetch_add(delta as u64, Ordering::Relaxed);
    }
}

fn latency_bucket_index(buckets: &[u64], bucket_count: usize, elapsed_micros: u64) -> usize {
    buckets
        .iter()
        .position(|upper_bound| elapsed_micros <= *upper_bound)
        .unwrap_or(bucket_count - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::MemoryTag;

    #[test]
    fn snapshot_updates() {
        let m = StorageMetrics::default();

        let mut tag_usage = [0i64; MEMORY_TAG_COUNT];
        tag_usage[MemoryTag::Allocator.as_index()] = 1024;
        let mem_snapshot = MemoryUsageSnapshot {
            usage_per_tag: tag_usage,
            total_usage: 1024,
        };
        m.set_memory_usage_snapshot(&mem_snapshot);
        m.inc_page_cache_hit();
        m.inc_page_cache_miss();
        m.inc_page_cache_eviction();
        m.set_page_cache_entries(3);

        m.inc_primary_index_hit();
        m.inc_primary_index_miss();
        m.inc_primary_index_conflicts(2);
        m.set_primary_index_memory(1024);
        m.inc_persistent_index_flushes();
        m.inc_delete_vector_entries(5);
        m.inc_memtable_flush();
        m.inc_memtable_backpressure();
        m.add_memtable_backpressure_time(std::time::Duration::from_nanos(30));
        m.add_delta_writer_commit_time(std::time::Duration::from_nanos(10));
        m.add_delta_writer_flush_time(std::time::Duration::from_nanos(20));
        m.record_parallel_decompress(4, 16);
        m.record_checkpoint_capture(false, 2);
        m.record_checkpoint_capture(true, 1);
        m.add_graph_expand_rows(7);
        m.set_graph_frontier_size(5);
        m.set_graph_frontier_size(3);
        m.record_graph_delta_lookup(true);
        m.record_graph_delta_lookup(false);
        m.add_graph_rebuild_latency(std::time::Duration::from_nanos(80));
        m.record_derived_index_lag_ts(42);
        m.record_tail_exact_merge_cost(128);
        m.record_tail_exact_merge_cost(64);
        m.record_segment_file_open();
        m.record_segment_file_open();
        m.record_search_manifest_publish("json", 8);
        m.record_search_manifest_open("json", 12);
        m.add_search_manifest_open_bytes("json", 4096);
        m.set_search_manifest_delta_count("json", 2);
        m.set_search_tail_gauges(SearchIndexKind::Hnsw, 17, 0, 2);
        m.add_search_tail_exact_merge_rows(SearchIndexKind::Hnsw, 13);
        m.record_search_tail_exact_merge_rejected(SearchIndexKind::Sparse, "hard_limit");
        m.record_search_fulltext_degraded_score(123, "missing_generation_stats");
        m.record_search_generation_retired(SearchIndexKind::FullText, 8192);
        m.record_search_generation_lease_hold(SearchIndexKind::FullText, 19);
        m.record_search_artifact_gc_delay(SearchIndexKind::FullText, "lease_released", 23);
        let inline_key = SearchInlineBuildMetricKey {
            definition_id: 77,
            provider: SearchIndexKind::Sparse,
        };
        m.record_search_inline_build(inline_key, 5, 2048, 15, 11);
        m.record_search_inline_build_failure(SearchIndexKind::FullText, "finish_error");
        let sidecar_build_key = SearchSidecarBuildMetricKey {
            definition_id: 88,
            provider: SearchIndexKind::Hnsw,
        };
        m.record_search_sidecar_build(sidecar_build_key, 21, 4096, 2048, 1536, 17);
        m.record_search_sidecar_reader_open(SearchIndexKind::Hnsw, "scar-v1");
        m.record_search_sidecar_reader_cache_hit(SearchIndexKind::Hnsw, "scar-v1");
        m.record_search_sidecar_reader_cache_miss(SearchIndexKind::Hnsw, "scar-v1");
        m.add_search_sidecar_reader_mmap_bytes(SearchIndexKind::Hnsw, "scar-v1", 1024);
        m.record_search_sidecar_reader_format_dispatch(SearchIndexKind::Hnsw, "scar-v1");
        let row_fetch_key = SearchRowFetchMetricKey {
            table_id: 99,
            provider: SearchIndexKind::FullText,
        };
        m.record_search_row_fetch(row_fetch_key, 3, 2, 1, 2, 1, 1, 96, 4, 7);
        m.add_column_read_by_rowids_page_run_seeks(3);

        let snap = m.snapshot();
        assert_eq!(snap.memory_tag_total, 1024);
        assert_eq!(snap.memory_tag_bytes[MemoryTag::Allocator.as_index()], 1024);
        assert_eq!(snap.page_cache_hits, 1);
        assert_eq!(snap.page_cache_misses, 1);
        assert_eq!(snap.page_cache_evictions, 1);
        assert_eq!(snap.page_cache_entries, 3);
        assert_eq!(snap.primary_index_hits, 1);
        assert_eq!(snap.primary_index_misses, 1);
        assert_eq!(snap.primary_index_conflicts, 2);
        assert_eq!(snap.primary_index_memory_bytes, 1024);
        assert_eq!(snap.persistent_index_flushes, 1);
        assert_eq!(snap.delete_vector_entries, 5);
        assert_eq!(snap.memtable_flush_count, 1);
        assert_eq!(snap.memtable_backpressure_count, 1);
        assert_eq!(snap.memtable_backpressure_ns, 30);
        assert_eq!(snap.delta_writer_commit_ns, 10);
        assert_eq!(snap.delta_writer_commit_count, 1);
        assert_eq!(snap.delta_writer_flush_ns, 20);
        assert_eq!(snap.delta_writer_flush_count, 1);
        assert_eq!(snap.decompress_parallel_batches, 1);
        assert_eq!(snap.decompress_parallel_tasks, 16);
        assert_eq!(snap.decompress_parallelism_last, 4);
        assert_eq!(snap.decompress_parallelism_peak, 4);
        assert_eq!(snap.checkpoint_capture_optimistic_total, 1);
        assert_eq!(snap.checkpoint_capture_meta_lock_total, 1);
        assert_eq!(snap.checkpoint_capture_retry_total, 3);
        assert_eq!(snap.graph_expand_rows, 7);
        assert_eq!(snap.graph_frontier_size, 3);
        assert_eq!(snap.graph_frontier_size_peak, 5);
        assert_eq!(snap.graph_delta_lookups, 2);
        assert_eq!(snap.graph_delta_hits, 1);
        assert_eq!(snap.graph_rebuild_latency_ns, 80);
        assert_eq!(snap.graph_rebuild_count, 1);
        assert_eq!(snap.derived_index_lag_ts, 42);
        assert_eq!(snap.tail_exact_merge_cost, 128);
        assert_eq!(snap.segment_file_open_total, 2);
        assert_eq!(snap.search_manifest_by_key.len(), 1);
        assert_eq!(snap.search_manifest_by_key[0].key.codec, "json");
        assert_eq!(
            snap.search_manifest_by_key[0]
                .counters
                .publish_latency_us_buckets[1],
            1
        );
        assert_eq!(
            snap.search_manifest_by_key[0]
                .counters
                .open_latency_us_buckets[2],
            1
        );
        assert_eq!(
            snap.search_manifest_by_key[0].counters.open_bytes_total,
            4096
        );
        assert_eq!(snap.search_manifest_by_key[0].counters.delta_count, 2);
        assert_eq!(snap.search_tail_by_key.len(), 1);
        assert_eq!(
            snap.search_tail_by_key[0].key.provider,
            SearchIndexKind::Hnsw
        );
        assert_eq!(snap.search_tail_by_key[0].counters.tail_rows, 17);
        assert_eq!(snap.search_tail_by_key[0].counters.tail_backlog_tier, 2);
        assert_eq!(
            snap.search_tail_by_key[0].counters.exact_merge_rows_total,
            13
        );
        assert_eq!(snap.search_tail_rejected_by_key.len(), 1);
        assert_eq!(
            snap.search_tail_rejected_by_key[0].key.provider,
            SearchIndexKind::Sparse
        );
        assert_eq!(snap.search_tail_rejected_by_key[0].key.reason, "hard_limit");
        assert_eq!(snap.search_tail_rejected_by_key[0].rejected_total, 1);
        assert_eq!(snap.search_fulltext_degraded_score_by_key.len(), 1);
        assert_eq!(
            snap.search_fulltext_degraded_score_by_key[0].key.table_id,
            123
        );
        assert_eq!(
            snap.search_fulltext_degraded_score_by_key[0].key.reason,
            "missing_generation_stats"
        );
        assert_eq!(
            snap.search_fulltext_degraded_score_by_key[0].degraded_queries,
            1
        );
        assert_eq!(snap.search_generation_by_key.len(), 1);
        assert_eq!(
            snap.search_generation_by_key[0].key.provider,
            SearchIndexKind::FullText
        );
        assert_eq!(snap.search_generation_by_key[0].counters.retired_total, 1);
        assert_eq!(
            snap.search_generation_by_key[0]
                .counters
                .retired_bytes_total,
            8192
        );
        assert_eq!(
            snap.search_generation_by_key[0]
                .counters
                .lease_hold_time_us_buckets[2],
            1
        );
        assert_eq!(snap.search_artifact_gc_delay_by_key.len(), 1);
        assert_eq!(
            snap.search_artifact_gc_delay_by_key[0].key.provider,
            SearchIndexKind::FullText
        );
        assert_eq!(
            snap.search_artifact_gc_delay_by_key[0].key.reason,
            "lease_released"
        );
        assert_eq!(
            snap.search_artifact_gc_delay_by_key[0]
                .counters
                .delay_us_buckets[2],
            1
        );
        assert_eq!(snap.search_inline_build_by_key.len(), 1);
        assert_eq!(snap.search_inline_build_by_key[0].key, inline_key);
        assert_eq!(snap.search_inline_build_by_key[0].counters.rows_total, 5);
        assert_eq!(
            snap.search_inline_build_by_key[0].counters.bytes_total,
            2048
        );
        assert_eq!(snap.search_inline_build_by_key[0].counters.cpu_us_total, 11);
        assert_eq!(
            snap.search_inline_build_by_key[0]
                .counters
                .latency_us_buckets[2],
            1
        );
        assert_eq!(snap.search_inline_build_failures_by_key.len(), 1);
        assert_eq!(
            snap.search_inline_build_failures_by_key[0].key.provider,
            SearchIndexKind::FullText
        );
        assert_eq!(
            snap.search_inline_build_failures_by_key[0].key.reason,
            "finish_error"
        );
        assert_eq!(
            snap.search_inline_build_failures_by_key[0].failures_total,
            1
        );
        assert_eq!(snap.search_sidecar_build_by_key.len(), 1);
        assert_eq!(snap.search_sidecar_build_by_key[0].key, sidecar_build_key);
        assert_eq!(snap.search_sidecar_build_by_key[0].counters.rows_total, 21);
        assert_eq!(
            snap.search_sidecar_build_by_key[0]
                .counters
                .read_bytes_total,
            4096
        );
        assert_eq!(
            snap.search_sidecar_build_by_key[0]
                .counters
                .write_bytes_total,
            2048
        );
        assert_eq!(
            snap.search_sidecar_build_by_key[0]
                .counters
                .artifact_bytes_total,
            1536
        );
        assert_eq!(
            snap.search_sidecar_build_by_key[0]
                .counters
                .latency_us_buckets[2],
            1
        );
        assert_eq!(snap.search_sidecar_reader_by_key.len(), 1);
        assert_eq!(
            snap.search_sidecar_reader_by_key[0].key.provider,
            SearchIndexKind::Hnsw
        );
        assert_eq!(snap.search_sidecar_reader_by_key[0].key.codec, "scar-v1");
        assert_eq!(
            snap.search_sidecar_reader_by_key[0]
                .counters
                .open_count_total,
            1
        );
        assert_eq!(
            snap.search_sidecar_reader_by_key[0]
                .counters
                .cache_hits_total,
            1
        );
        assert_eq!(
            snap.search_sidecar_reader_by_key[0]
                .counters
                .cache_misses_total,
            1
        );
        assert_eq!(
            snap.search_sidecar_reader_by_key[0].counters.mmap_bytes,
            1024
        );
        assert_eq!(
            snap.search_sidecar_reader_by_key[0]
                .counters
                .format_dispatch_total,
            1
        );
        assert_eq!(snap.search_row_fetch_batches_total, 1);
        assert_eq!(snap.search_row_fetch_rows_total, 3);
        assert_eq!(snap.search_row_fetch_projected_columns_total, 2);
        assert_eq!(snap.search_row_fetch_segment_groups_total, 1);
        assert_eq!(snap.search_row_fetch_column_batches_total, 2);
        assert_eq!(snap.search_row_fetch_fixed_width_column_batches_total, 1);
        assert_eq!(snap.search_row_fetch_varlen_column_batches_total, 1);
        assert_eq!(snap.search_row_fetch_projected_bytes_total, 96);
        assert_eq!(snap.search_row_fetch_latency_us_total, 7);
        assert_eq!(snap.search_row_fetch_latency_us_buckets[1], 1);
        assert_eq!(
            snap.search_row_fetch_latency_us_buckets.iter().sum::<u64>(),
            1
        );
        assert_eq!(snap.search_row_fetch_by_key.len(), 1);
        assert_eq!(snap.search_row_fetch_by_key[0].key, row_fetch_key);
        assert_eq!(snap.search_row_fetch_by_key[0].counters.rows_total, 3);
        assert_eq!(
            snap.search_row_fetch_by_key[0]
                .counters
                .column_read_by_rowids_page_run_seeks_total,
            4
        );
        assert_eq!(
            snap.search_row_fetch_by_key[0].counters.latency_us_buckets[1],
            1
        );
        assert_eq!(snap.column_read_by_rowids_page_run_seeks_total, 3);
        assert!((snap.graph_delta_hit_ratio() - 0.5).abs() < f64::EPSILON);
        assert_eq!(snap.graph_rebuild_latency_avg_ns(), 80);
    }
}
