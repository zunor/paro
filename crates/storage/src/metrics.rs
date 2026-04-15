//! Storage-level lightweight metrics (counters + gauges) for observability.
//!
//! Requirement: expose PrimaryIndex hit/conflict counters, L0→L1
//! flush count, DeleteVector entry count, and PrimaryIndex memory usage.
//!
//! The implementation is intentionally simple: a global, lock-free set of
//! atomics accessible via [`storage_metrics()`]. It is header-only and
//! avoids external dependencies to stay embeddable in storage-only builds.

use paro_common::allocator::{MemoryUsageSnapshot, MEMORY_TAG_COUNT};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;

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
    pub wal_replay_entries: u64,
    pub wal_replay_bytes: u64,
    pub wal_truncate_bytes: u64,
    pub wal_checkpoint_merges: u64,
    pub wal_recovery_mode: u64,
    pub graph_expand_rows: u64,
    pub graph_frontier_size: usize,
    pub graph_frontier_size_peak: usize,
    pub graph_delta_lookups: u64,
    pub graph_delta_hits: u64,
    pub graph_rebuild_latency_ns: u64,
    pub graph_rebuild_count: u64,
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
    wal_replay_entries: AtomicU64,
    wal_replay_bytes: AtomicU64,
    wal_truncate_bytes: AtomicU64,
    wal_checkpoint_merges: AtomicU64,
    wal_recovery_mode: AtomicU64,
    graph_expand_rows: AtomicU64,
    graph_frontier_size: AtomicUsize,
    graph_frontier_size_peak: AtomicUsize,
    graph_delta_lookups: AtomicU64,
    graph_delta_hits: AtomicU64,
    graph_rebuild_latency_ns: AtomicU64,
    graph_rebuild_count: AtomicU64,
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

    /// Record WAL replay progress in entries and bytes.
    pub fn add_wal_replay(&self, entries: u64, bytes: u64) {
        if entries > 0 {
            self.wal_replay_entries
                .fetch_add(entries, Ordering::Relaxed);
        }
        if bytes > 0 {
            self.wal_replay_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record physically reclaimed WAL bytes due to truncation.
    pub fn add_wal_truncate_bytes(&self, bytes: u64) {
        if bytes > 0 {
            self.wal_truncate_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record one main+checkpoint WAL merge.
    pub fn inc_wal_checkpoint_merge(&self) {
        self.wal_checkpoint_merges.fetch_add(1, Ordering::Relaxed);
    }

    /// Set the last observed WAL recovery mode as an integer gauge.
    ///
    /// Mode values are defined by `wal::recovery::WalRecoveryMode::as_metric_value()`.
    pub fn set_wal_recovery_mode(&self, mode: u64) {
        self.wal_recovery_mode.store(mode, Ordering::Relaxed);
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

    /// Take a consistent snapshot of all metrics.
    pub fn snapshot(&self) -> StorageMetricsSnapshot {
        let mut tag_bytes = [0i64; MEMORY_TAG_COUNT];
        for (idx, counter) in self.memory_tag_bytes.iter().enumerate() {
            tag_bytes[idx] = counter.load(Ordering::Relaxed);
        }

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
            wal_replay_entries: self.wal_replay_entries.load(Ordering::Relaxed),
            wal_replay_bytes: self.wal_replay_bytes.load(Ordering::Relaxed),
            wal_truncate_bytes: self.wal_truncate_bytes.load(Ordering::Relaxed),
            wal_checkpoint_merges: self.wal_checkpoint_merges.load(Ordering::Relaxed),
            wal_recovery_mode: self.wal_recovery_mode.load(Ordering::Relaxed),
            graph_expand_rows: self.graph_expand_rows.load(Ordering::Relaxed),
            graph_frontier_size: self.graph_frontier_size.load(Ordering::Relaxed),
            graph_frontier_size_peak: self.graph_frontier_size_peak.load(Ordering::Relaxed),
            graph_delta_lookups: self.graph_delta_lookups.load(Ordering::Relaxed),
            graph_delta_hits: self.graph_delta_hits.load(Ordering::Relaxed),
            graph_rebuild_latency_ns: self.graph_rebuild_latency_ns.load(Ordering::Relaxed),
            graph_rebuild_count: self.graph_rebuild_count.load(Ordering::Relaxed),
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
        self.wal_replay_entries.store(0, Ordering::Relaxed);
        self.wal_replay_bytes.store(0, Ordering::Relaxed);
        self.wal_truncate_bytes.store(0, Ordering::Relaxed);
        self.wal_checkpoint_merges.store(0, Ordering::Relaxed);
        self.wal_recovery_mode.store(0, Ordering::Relaxed);
        self.graph_expand_rows.store(0, Ordering::Relaxed);
        self.graph_frontier_size.store(0, Ordering::Relaxed);
        self.graph_frontier_size_peak.store(0, Ordering::Relaxed);
        self.graph_delta_lookups.store(0, Ordering::Relaxed);
        self.graph_delta_hits.store(0, Ordering::Relaxed);
        self.graph_rebuild_latency_ns.store(0, Ordering::Relaxed);
        self.graph_rebuild_count.store(0, Ordering::Relaxed);
    }
}

/// Get global storage metrics singleton.
pub fn storage_metrics() -> &'static StorageMetrics {
    StorageMetrics::global()
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::MemoryTag;

    #[test]
    #[serial_test::serial]
    fn snapshot_updates() {
        let m = storage_metrics();
        m.reset_for_tests();

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
        m.add_wal_replay(3, 512);
        m.add_wal_truncate_bytes(128);
        m.inc_wal_checkpoint_merge();
        m.set_wal_recovery_mode(2);
        m.add_graph_expand_rows(7);
        m.set_graph_frontier_size(5);
        m.set_graph_frontier_size(3);
        m.record_graph_delta_lookup(true);
        m.record_graph_delta_lookup(false);
        m.add_graph_rebuild_latency(std::time::Duration::from_nanos(80));

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
        assert_eq!(snap.wal_replay_entries, 3);
        assert_eq!(snap.wal_replay_bytes, 512);
        assert_eq!(snap.wal_truncate_bytes, 128);
        assert_eq!(snap.wal_checkpoint_merges, 1);
        assert_eq!(snap.wal_recovery_mode, 2);
        assert_eq!(snap.graph_expand_rows, 7);
        assert_eq!(snap.graph_frontier_size, 3);
        assert_eq!(snap.graph_frontier_size_peak, 5);
        assert_eq!(snap.graph_delta_lookups, 2);
        assert_eq!(snap.graph_delta_hits, 1);
        assert_eq!(snap.graph_rebuild_latency_ns, 80);
        assert_eq!(snap.graph_rebuild_count, 1);
        assert!((snap.graph_delta_hit_ratio() - 0.5).abs() < f64::EPSILON);
        assert_eq!(snap.graph_rebuild_latency_avg_ns(), 80);
    }
}
