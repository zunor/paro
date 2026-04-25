// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Scan-side memory budgeting and adaptive tuning.
//!
//! This module derives operator-level budgets from query-level limits,
//! and uses the resulting reservation to tune:
//! - prefetch target bytes/window
//! - decompression parallelism
//! - scan source parallelism (backpressure)

use paro_storage::buffer::{PrefetchOptions, DEFAULT_BLOCK_ALLOC_SIZE};

/// Default prefetch window used by scan planning.
const DEFAULT_PREFETCH_WINDOW_PAGES: usize = 8;
/// Upper bound for prefetch window adaptation.
const MAX_PREFETCH_WINDOW_PAGES: usize = 16;
/// Default prefetch batch size.
const DEFAULT_PREFETCH_BATCH_PAGES: usize = 4;
/// Per-operator budget is capped to a fraction of query memory.
const OPERATOR_QUERY_CAP_DIVISOR: usize = 4;
/// Ratio of reservation that can be used for prefetch target.
const PREFETCH_TARGET_NUM: usize = 3;
/// Ratio of reservation that can be used for prefetch target.
const PREFETCH_TARGET_DEN: usize = 5;
/// Heuristic bytes required for one decompression worker.
const DECOMPRESS_BYTES_PER_THREAD: usize = DEFAULT_BLOCK_ALLOC_SIZE * 2;
/// Heuristic bytes required for one scan source worker.
const SCAN_THREAD_BYTES_BASE: usize = DEFAULT_BLOCK_ALLOC_SIZE * 4;
/// Max number of concurrent scans to model in demand estimation.
const MAX_MODELED_CONCURRENT_SCANS: usize = 4;

/// Inputs for scan memory budget planning.
#[derive(Debug, Clone)]
pub struct ScanMemoryBudgetConfig {
    /// Number of projected columns in the scan.
    pub projected_columns: usize,
    /// Remaining (or total) segment count for this scan.
    pub segment_count: usize,
    /// Planned batch size for tablet reader.
    pub batch_size: usize,
    /// Query-level thread budget.
    pub query_max_threads: usize,
    /// Query-level memory limit from TMM configuration.
    pub query_max_memory: usize,
    /// Force external mode from session settings.
    pub force_external: bool,
}

impl ScanMemoryBudgetConfig {
    pub fn new(
        projected_columns: usize,
        segment_count: usize,
        batch_size: usize,
        query_max_threads: usize,
        query_max_memory: usize,
        force_external: bool,
    ) -> Self {
        Self {
            projected_columns,
            segment_count,
            batch_size,
            query_max_threads,
            query_max_memory,
            force_external,
        }
    }

    pub fn with_segment_count(mut self, segment_count: usize) -> Self {
        self.segment_count = segment_count;
        self
    }
}

/// Planned scan budget and derived tuning knobs.
#[derive(Debug, Clone)]
pub struct ScanMemoryBudget {
    /// Demand estimate before operator cap.
    pub demanded_bytes: usize,
    /// Requested bytes after operator cap.
    pub requested_bytes: usize,
    /// Effective reservation used by this operator (after cap).
    pub reservation_bytes: usize,
    /// Operator-level cap derived from query-level budget.
    pub operator_cap_bytes: usize,

    /// Prefetch target bytes.
    pub prefetch_target_bytes: usize,
    /// Prefetch window pages.
    pub prefetch_window_pages: usize,
    /// Prefetch batch pages.
    pub prefetch_batch_pages: usize,
    /// Suggested max in-flight prefetch tasks.
    pub prefetch_max_concurrent_tasks: usize,

    /// Suggested max decompression workers.
    pub decompress_max_threads: usize,
    /// Suggested max scan source workers.
    pub max_scan_threads: usize,

    /// Whether to keep prefetch active for this phase.
    pub use_prefetch: bool,
    /// Whether to externalize scan behavior (disable aggressive buffering).
    pub externalize: bool,
    /// Whether to apply backpressure (reduce scan parallelism).
    pub backpressure: bool,
}

impl ScanMemoryBudget {
    /// Convert budget to prefetch options.
    pub fn prefetch_options(&self) -> PrefetchOptions {
        PrefetchOptions {
            window_pages: self.prefetch_window_pages.max(1),
            batch_pages: self.prefetch_batch_pages.max(1),
            // PrefetchLease enforces live in-flight bytes.
            max_inflight_bytes: 0,
            // Let prefetcher derive concurrency from the lease target dynamically.
            max_concurrent_tasks: 0,
        }
    }
}

/// Compute scan memory budget. In-flight prefetch is enforced by PrefetchLease.
pub fn plan_scan_memory_budget(config: &ScanMemoryBudgetConfig) -> ScanMemoryBudget {
    let projected_columns = config.projected_columns.max(1);
    let query_threads = config.query_max_threads.max(1);
    let modeled_concurrency = std::cmp::min(
        std::cmp::min(config.segment_count.max(1), query_threads),
        MAX_MODELED_CONCURRENT_SCANS,
    );

    let per_scan_prefetch = projected_columns
        .saturating_mul(DEFAULT_PREFETCH_WINDOW_PAGES)
        .saturating_mul(DEFAULT_BLOCK_ALLOC_SIZE);
    let per_scan_decode = estimate_decode_bytes(projected_columns, config.batch_size);

    let demanded_bytes = per_scan_prefetch
        .saturating_mul(modeled_concurrency)
        .saturating_add(per_scan_decode.saturating_mul(modeled_concurrency))
        .max(DEFAULT_BLOCK_ALLOC_SIZE);

    let operator_cap_bytes = if config.query_max_memory == 0 {
        demanded_bytes
    } else {
        std::cmp::max(
            DEFAULT_BLOCK_ALLOC_SIZE,
            config.query_max_memory / OPERATOR_QUERY_CAP_DIVISOR,
        )
    };

    let requested_bytes = std::cmp::min(demanded_bytes, operator_cap_bytes).max(1);
    let reservation_bytes = requested_bytes;

    let externalize =
        config.force_external || reservation_bytes.saturating_mul(4) < demanded_bytes.max(1);
    let backpressure = reservation_bytes.saturating_mul(5) < demanded_bytes.saturating_mul(4);

    let prefetch_target_bytes = if externalize {
        0
    } else {
        let target_cap =
            reservation_bytes.saturating_mul(PREFETCH_TARGET_NUM) / PREFETCH_TARGET_DEN;
        std::cmp::min(
            per_scan_prefetch.saturating_mul(modeled_concurrency),
            target_cap,
        )
        .max(DEFAULT_BLOCK_ALLOC_SIZE)
    };

    let prefetch_window_pages = if prefetch_target_bytes == 0 {
        1
    } else {
        let per_window_bytes = projected_columns
            .saturating_mul(DEFAULT_BLOCK_ALLOC_SIZE)
            .max(1);
        let pages = prefetch_target_bytes / per_window_bytes;
        pages.clamp(1, MAX_PREFETCH_WINDOW_PAGES)
    };
    let prefetch_batch_pages =
        std::cmp::min(DEFAULT_PREFETCH_BATCH_PAGES, prefetch_window_pages).max(1);
    let prefetch_max_concurrent_tasks = if prefetch_target_bytes == 0 {
        1
    } else {
        let bytes_per_task = prefetch_batch_pages
            .saturating_mul(DEFAULT_BLOCK_ALLOC_SIZE)
            .max(1);
        std::cmp::max(1, prefetch_target_bytes / bytes_per_task)
    };

    let decompress_max_threads = if externalize {
        1
    } else {
        let by_memory = std::cmp::max(1, reservation_bytes / DECOMPRESS_BYTES_PER_THREAD);
        std::cmp::min(query_threads, by_memory)
    };

    let bytes_per_scan_thread = projected_columns
        .saturating_mul(SCAN_THREAD_BYTES_BASE)
        .max(DEFAULT_BLOCK_ALLOC_SIZE);
    let by_memory_threads = std::cmp::max(1, reservation_bytes / bytes_per_scan_thread);
    let mut max_scan_threads = std::cmp::min(
        std::cmp::min(config.segment_count.max(1), query_threads),
        by_memory_threads,
    )
    .max(1);

    if externalize {
        max_scan_threads = 1;
    } else if backpressure {
        max_scan_threads = std::cmp::max(1, max_scan_threads / 2);
    }

    ScanMemoryBudget {
        demanded_bytes,
        requested_bytes,
        reservation_bytes,
        operator_cap_bytes,
        prefetch_target_bytes,
        prefetch_window_pages,
        prefetch_batch_pages,
        prefetch_max_concurrent_tasks,
        decompress_max_threads,
        max_scan_threads,
        use_prefetch: prefetch_target_bytes > 0 && !externalize,
        externalize,
        backpressure,
    }
}

fn estimate_decode_bytes(projected_columns: usize, batch_size: usize) -> usize {
    let rows = batch_size.max(1);
    let bytes_per_row = projected_columns.saturating_mul(16).max(8);
    rows.saturating_mul(bytes_per_row)
        .max(DEFAULT_BLOCK_ALLOC_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_budget_prefetch_enabled_when_memory_is_sufficient() {
        let cfg = ScanMemoryBudgetConfig::new(4, 16, 4096, 8, 512 * 1024 * 1024, false);
        let budget = plan_scan_memory_budget(&cfg);

        assert!(!budget.externalize);
        assert!(budget.use_prefetch);
        assert!(budget.prefetch_target_bytes > 0);
        assert!(budget.decompress_max_threads >= 2);
        assert!(budget.max_scan_threads >= 2);
    }

    #[test]
    fn scan_budget_externalizes_when_force_external_enabled() {
        let cfg = ScanMemoryBudgetConfig::new(4, 16, 4096, 8, 512 * 1024 * 1024, true);
        let budget = plan_scan_memory_budget(&cfg);

        assert!(budget.externalize);
        assert!(!budget.use_prefetch);
        assert_eq!(budget.prefetch_target_bytes, 0);
        assert_eq!(budget.decompress_max_threads, 1);
        assert_eq!(budget.max_scan_threads, 1);
    }

    #[test]
    fn scan_budget_triggers_backpressure_under_low_query_cap() {
        let query_cap = 2 * 1024 * 1024; // 2MB
        let cfg = ScanMemoryBudgetConfig::new(8, 32, 4096, 8, query_cap, false);
        let budget = plan_scan_memory_budget(&cfg);

        assert!(budget.backpressure);
        assert!(budget.externalize);
        assert_eq!(budget.max_scan_threads, 1);
        assert_eq!(budget.prefetch_target_bytes, 0);
        assert!(budget.requested_bytes <= budget.operator_cap_bytes);
    }
}
