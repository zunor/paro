// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Search Telemetry
//!
//! Runtime search statistics (non-persistent).

use crate::index::hnsw::{
    HnswExactScanKind, HnswPredicateAdmissionMode, HnswSearchOutcome, HnswSearchPath,
};

const SELECTIVITY_BUCKETS: [f64; 7] = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9];
const BATCH_SIZE_BUCKETS: [usize; 6] = [1, 2, 4, 8, 16, 32];

/// Runtime telemetry for search paths.
#[derive(Debug, Clone)]
pub struct SearchTelemetry {
    /// Number of searches recorded.
    pub search_count: u64,
    /// Average latency in microseconds.
    pub avg_latency_us: f64,
    /// Total pre-filter candidate count.
    pub pre_filter_count: u64,
    /// Total post-filter candidate count.
    pub post_filter_count: u64,
    /// Optional selectivity histogram buckets.
    pub filter_selectivity_histogram: Option<Vec<u64>>,
    /// Total vector distance evaluations performed by HNSW and exact fallback
    /// paths. Non-HNSW users leave this counter at zero.
    pub hnsw_scored_points: u64,
    pub hnsw_exact_scan_count: u64,
    pub hnsw_predicate_covering_scan_count: u64,
    pub hnsw_deferred_beam_admission_count: u64,
    pub hnsw_unfiltered_graph_count: u64,
    pub hnsw_masked_graph_count: u64,
    pub hnsw_adaptive_graph_count: u64,
    pub hnsw_predicate_topology_count: u64,
    pub hnsw_predicate_refinement_count: u64,
    pub hnsw_exact_fallback_count: u64,
}

impl Default for SearchTelemetry {
    fn default() -> Self {
        Self {
            search_count: 0,
            avg_latency_us: 0.0,
            pre_filter_count: 0,
            post_filter_count: 0,
            filter_selectivity_histogram: None,
            hnsw_scored_points: 0,
            hnsw_exact_scan_count: 0,
            hnsw_predicate_covering_scan_count: 0,
            hnsw_deferred_beam_admission_count: 0,
            hnsw_unfiltered_graph_count: 0,
            hnsw_masked_graph_count: 0,
            hnsw_adaptive_graph_count: 0,
            hnsw_predicate_topology_count: 0,
            hnsw_predicate_refinement_count: 0,
            hnsw_exact_fallback_count: 0,
        }
    }
}

impl SearchTelemetry {
    /// Create a telemetry instance with selectivity histogram enabled.
    pub fn with_histogram() -> Self {
        Self {
            filter_selectivity_histogram: Some(vec![0; SELECTIVITY_BUCKETS.len() + 1]),
            ..Self::default()
        }
    }

    /// Enable selectivity histogram on an existing telemetry instance.
    pub fn enable_histogram(&mut self) {
        if self.filter_selectivity_histogram.is_none() {
            self.filter_selectivity_histogram = Some(vec![0; SELECTIVITY_BUCKETS.len() + 1]);
        }
    }

    /// Record a search with latency and filter counts.
    pub fn record(&mut self, latency_us: u64, pre_filter_count: u64, post_filter_count: u64) {
        self.search_count = self.search_count.saturating_add(1);
        let count = self.search_count as f64;
        let latency = latency_us as f64;
        self.avg_latency_us += (latency - self.avg_latency_us) / count;
        self.pre_filter_count = self.pre_filter_count.saturating_add(pre_filter_count);
        self.post_filter_count = self.post_filter_count.saturating_add(post_filter_count);

        if let Some(hist) = self.filter_selectivity_histogram.as_mut() {
            let selectivity = if pre_filter_count == 0 {
                0.0
            } else {
                post_filter_count as f64 / pre_filter_count as f64
            };
            let mut idx = SELECTIVITY_BUCKETS.len();
            for (i, bucket) in SELECTIVITY_BUCKETS.iter().enumerate() {
                if selectivity <= *bucket {
                    idx = i;
                    break;
                }
            }
            if let Some(slot) = hist.get_mut(idx) {
                *slot = slot.saturating_add(1);
            }
        }
    }

    pub fn record_hnsw_work(&mut self, scored_points: u64, outcome: HnswSearchOutcome) {
        self.hnsw_scored_points = self.hnsw_scored_points.saturating_add(scored_points);
        match outcome.path {
            HnswSearchPath::ExactScan(kind) => {
                self.hnsw_exact_scan_count = self.hnsw_exact_scan_count.saturating_add(1);
                self.hnsw_predicate_covering_scan_count = self
                    .hnsw_predicate_covering_scan_count
                    .saturating_add(u64::from(kind.uses_predicate_covering()));
            }
            HnswSearchPath::UnfilteredGraph => {
                self.hnsw_unfiltered_graph_count =
                    self.hnsw_unfiltered_graph_count.saturating_add(1)
            }
            HnswSearchPath::MaskedGraph => {
                self.hnsw_masked_graph_count = self.hnsw_masked_graph_count.saturating_add(1)
            }
            HnswSearchPath::AdaptiveGraph => {
                self.hnsw_adaptive_graph_count = self.hnsw_adaptive_graph_count.saturating_add(1)
            }
        }
        self.hnsw_deferred_beam_admission_count = self
            .hnsw_deferred_beam_admission_count
            .saturating_add(u64::from(
                outcome.predicate_admission == HnswPredicateAdmissionMode::DeferredGlobalBeam,
            ));
        self.hnsw_predicate_refinement_count = self
            .hnsw_predicate_refinement_count
            .saturating_add(u64::from(outcome.predicate_refined));
        self.hnsw_predicate_topology_count = self
            .hnsw_predicate_topology_count
            .saturating_add(u64::from(outcome.predicate_topology_used));
        if outcome
            .exact_fallback
            .is_some_and(HnswExactScanKind::uses_predicate_covering)
        {
            self.hnsw_predicate_covering_scan_count =
                self.hnsw_predicate_covering_scan_count.saturating_add(1);
        }
        self.hnsw_exact_fallback_count = self
            .hnsw_exact_fallback_count
            .saturating_add(u64::from(outcome.exact_fallback.is_some()));
    }
}

/// Runtime telemetry for batched HNSW search paths.
#[derive(Debug, Clone)]
pub struct HnswBatchTelemetry {
    /// Number of batch searches recorded.
    pub batch_search_count: u64,
    /// Average end-to-end latency per batch in microseconds.
    pub batch_avg_latency_us: f64,
    /// Total number of queries processed through batched search.
    pub batched_query_count: u64,
    /// Histogram of observed batch sizes.
    ///
    /// Buckets represent batch sizes `1/2/4/8/16/32/64+`.
    pub batch_size_histogram: Vec<u64>,
}

impl Default for HnswBatchTelemetry {
    fn default() -> Self {
        Self {
            batch_search_count: 0,
            batch_avg_latency_us: 0.0,
            batched_query_count: 0,
            batch_size_histogram: vec![0; BATCH_SIZE_BUCKETS.len() + 1],
        }
    }
}

impl HnswBatchTelemetry {
    /// Record one batched search.
    pub fn record_batch(&mut self, latency_us: u64, num_queries: usize) {
        self.batch_search_count = self.batch_search_count.saturating_add(1);
        let count = self.batch_search_count as f64;
        let latency = latency_us as f64;
        self.batch_avg_latency_us += (latency - self.batch_avg_latency_us) / count;
        self.batched_query_count = self.batched_query_count.saturating_add(num_queries as u64);

        let mut bucket_idx = BATCH_SIZE_BUCKETS.len();
        for (idx, bucket) in BATCH_SIZE_BUCKETS.iter().enumerate() {
            if num_queries <= *bucket {
                bucket_idx = idx;
                break;
            }
        }

        if let Some(slot) = self.batch_size_histogram.get_mut(bucket_idx) {
            *slot = slot.saturating_add(1);
        }
    }
}

/// Full-text search specific telemetry.
#[derive(Debug, Clone)]
pub struct FullTextSearchTelemetry {
    /// Generic search telemetry.
    pub base: SearchTelemetry,
    /// Count of filter-mode calls.
    pub fulltext_filter_count: u64,
    /// Count of BM25 search calls.
    pub fulltext_search_count: u64,
    /// Average match bitmap cardinality.
    pub avg_match_bitmap_cardinality: f64,
    /// Average BM25 candidate count.
    pub avg_bm25_candidate_count: f64,
}

impl Default for FullTextSearchTelemetry {
    fn default() -> Self {
        Self {
            base: SearchTelemetry::default(),
            fulltext_filter_count: 0,
            fulltext_search_count: 0,
            avg_match_bitmap_cardinality: 0.0,
            avg_bm25_candidate_count: 0.0,
        }
    }
}

impl FullTextSearchTelemetry {
    /// Record a filter-mode call.
    pub fn record_filter(
        &mut self,
        latency_us: u64,
        pre_filter_count: u64,
        post_filter_count: u64,
        match_cardinality: u64,
    ) {
        self.base
            .record(latency_us, pre_filter_count, post_filter_count);
        self.fulltext_filter_count = self.fulltext_filter_count.saturating_add(1);
        let total = (self.fulltext_filter_count + self.fulltext_search_count) as f64;
        let value = match_cardinality as f64;
        self.avg_match_bitmap_cardinality += (value - self.avg_match_bitmap_cardinality) / total;
    }

    /// Record a BM25 search-mode call.
    pub fn record_search(
        &mut self,
        latency_us: u64,
        pre_filter_count: u64,
        post_filter_count: u64,
        match_cardinality: u64,
        bm25_candidates: u64,
    ) {
        self.base
            .record(latency_us, pre_filter_count, post_filter_count);
        self.fulltext_search_count = self.fulltext_search_count.saturating_add(1);
        let total = (self.fulltext_filter_count + self.fulltext_search_count) as f64;
        let match_value = match_cardinality as f64;
        self.avg_match_bitmap_cardinality +=
            (match_value - self.avg_match_bitmap_cardinality) / total;

        let search_count = self.fulltext_search_count as f64;
        let candidate_value = bm25_candidates as f64;
        self.avg_bm25_candidate_count +=
            (candidate_value - self.avg_bm25_candidate_count) / search_count;
    }
}
