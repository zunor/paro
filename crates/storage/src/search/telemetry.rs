// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use crate::rowset::RowsetId;

use super::capability::CoverageState;
use super::capability::SearchIndexKind;
use super::stats::{BuildEpoch, SearchDefinitionId, SearchGenerationId, SegmentId};

pub const SEARCH_LATENCY_BUCKETS_US: &[u64] =
    &[1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000];

pub const SEARCH_BUILD_LATENCY_BUCKETS_US: &[u64] = &[
    1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 30_000_000, 60_000_000,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMetricType {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMetricUnit {
    Count,
    Rows,
    Bytes,
    Microseconds,
    Percent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMetricDimension {
    Global,
    Table,
    Definition,
    Provider,
    Reason,
    Codec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchMetricDescriptor {
    pub name: &'static str,
    pub metric_type: SearchMetricType,
    pub unit: SearchMetricUnit,
    pub dimensions: &'static [SearchMetricDimension],
    pub buckets_us: &'static [u64],
}

impl SearchMetricDescriptor {
    pub const fn counter(
        name: &'static str,
        unit: SearchMetricUnit,
        dimensions: &'static [SearchMetricDimension],
    ) -> Self {
        Self {
            name,
            metric_type: SearchMetricType::Counter,
            unit,
            dimensions,
            buckets_us: &[],
        }
    }

    pub const fn gauge(
        name: &'static str,
        unit: SearchMetricUnit,
        dimensions: &'static [SearchMetricDimension],
    ) -> Self {
        Self {
            name,
            metric_type: SearchMetricType::Gauge,
            unit,
            dimensions,
            buckets_us: &[],
        }
    }

    pub const fn histogram(
        name: &'static str,
        unit: SearchMetricUnit,
        dimensions: &'static [SearchMetricDimension],
        buckets_us: &'static [u64],
    ) -> Self {
        Self {
            name,
            metric_type: SearchMetricType::Histogram,
            unit,
            dimensions,
            buckets_us,
        }
    }
}

const PROVIDER: &[SearchMetricDimension] = &[SearchMetricDimension::Provider];
const GLOBAL: &[SearchMetricDimension] = &[SearchMetricDimension::Global];
const TABLE_PROVIDER: &[SearchMetricDimension] = &[
    SearchMetricDimension::Table,
    SearchMetricDimension::Provider,
];
const TABLE_REASON: &[SearchMetricDimension] =
    &[SearchMetricDimension::Table, SearchMetricDimension::Reason];
const DEFINITION_PROVIDER: &[SearchMetricDimension] = &[
    SearchMetricDimension::Definition,
    SearchMetricDimension::Provider,
];
const PROVIDER_REASON: &[SearchMetricDimension] = &[
    SearchMetricDimension::Provider,
    SearchMetricDimension::Reason,
];
const PROVIDER_CODEC: &[SearchMetricDimension] = &[
    SearchMetricDimension::Provider,
    SearchMetricDimension::Codec,
];
const GLOBAL_CODEC: &[SearchMetricDimension] =
    &[SearchMetricDimension::Global, SearchMetricDimension::Codec];

pub const SEARCH_METRIC_DESCRIPTORS: &[SearchMetricDescriptor] = &[
    SearchMetricDescriptor::counter(
        "search_hnsw_scored_points_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_exact_segment_searches_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_predicate_covering_segment_scans_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_deferred_beam_admission_segment_searches_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_unfiltered_graph_segment_searches_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_masked_graph_segment_searches_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_adaptive_graph_segment_searches_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_predicate_refined_segment_searches_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_exact_fallback_segment_searches_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_predicate_topology_segment_searches_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_integrity_scheduled_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_integrity_completed_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_integrity_failed_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_integrity_stale_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_integrity_deferred_total",
        SearchMetricUnit::Count,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_hnsw_integrity_verified_bytes_total",
        SearchMetricUnit::Bytes,
        GLOBAL,
    ),
    SearchMetricDescriptor::counter(
        "search_inline_build_rows_total",
        SearchMetricUnit::Rows,
        DEFINITION_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_inline_build_bytes_total",
        SearchMetricUnit::Bytes,
        DEFINITION_PROVIDER,
    ),
    SearchMetricDescriptor::histogram(
        "search_inline_build_latency_us",
        SearchMetricUnit::Microseconds,
        DEFINITION_PROVIDER,
        SEARCH_BUILD_LATENCY_BUCKETS_US,
    ),
    SearchMetricDescriptor::counter(
        "search_inline_build_cpu_us_total",
        SearchMetricUnit::Microseconds,
        DEFINITION_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_inline_build_failures_total",
        SearchMetricUnit::Count,
        PROVIDER_REASON,
    ),
    SearchMetricDescriptor::counter(
        "search_sidecar_build_rows_total",
        SearchMetricUnit::Rows,
        DEFINITION_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_sidecar_build_read_bytes_total",
        SearchMetricUnit::Bytes,
        DEFINITION_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_sidecar_build_write_bytes_total",
        SearchMetricUnit::Bytes,
        DEFINITION_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_sidecar_build_artifact_bytes_total",
        SearchMetricUnit::Bytes,
        DEFINITION_PROVIDER,
    ),
    SearchMetricDescriptor::histogram(
        "search_sidecar_build_latency_us",
        SearchMetricUnit::Microseconds,
        DEFINITION_PROVIDER,
        SEARCH_BUILD_LATENCY_BUCKETS_US,
    ),
    SearchMetricDescriptor::histogram(
        "search_manifest_publish_latency_us",
        SearchMetricUnit::Microseconds,
        GLOBAL_CODEC,
        SEARCH_LATENCY_BUCKETS_US,
    ),
    SearchMetricDescriptor::counter(
        "search_manifest_publish_cas_retries_total",
        SearchMetricUnit::Count,
        GLOBAL_CODEC,
    ),
    SearchMetricDescriptor::histogram(
        "search_manifest_open_latency_us",
        SearchMetricUnit::Microseconds,
        GLOBAL_CODEC,
        SEARCH_LATENCY_BUCKETS_US,
    ),
    SearchMetricDescriptor::gauge(
        "search_manifest_delta_count",
        SearchMetricUnit::Count,
        GLOBAL_CODEC,
    ),
    SearchMetricDescriptor::counter(
        "search_manifest_open_bytes_total",
        SearchMetricUnit::Bytes,
        GLOBAL_CODEC,
    ),
    SearchMetricDescriptor::gauge("search_tail_rows", SearchMetricUnit::Rows, PROVIDER),
    SearchMetricDescriptor::gauge("search_tail_bytes", SearchMetricUnit::Bytes, PROVIDER),
    SearchMetricDescriptor::gauge(
        "search_tail_backlog_tier",
        SearchMetricUnit::Count,
        PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_tail_exact_merge_rows_total",
        SearchMetricUnit::Rows,
        PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_tail_exact_merge_rejected_total",
        SearchMetricUnit::Count,
        PROVIDER_REASON,
    ),
    SearchMetricDescriptor::counter(
        "search_fulltext_degraded_score_queries",
        SearchMetricUnit::Count,
        TABLE_REASON,
    ),
    SearchMetricDescriptor::counter(
        "search_sidecar_reader_open_count_total",
        SearchMetricUnit::Count,
        PROVIDER_CODEC,
    ),
    SearchMetricDescriptor::counter(
        "search_sidecar_reader_cache_hits_total",
        SearchMetricUnit::Count,
        PROVIDER_CODEC,
    ),
    SearchMetricDescriptor::counter(
        "search_sidecar_reader_cache_misses_total",
        SearchMetricUnit::Count,
        PROVIDER_CODEC,
    ),
    SearchMetricDescriptor::gauge(
        "search_sidecar_reader_mmap_bytes",
        SearchMetricUnit::Bytes,
        PROVIDER_CODEC,
    ),
    SearchMetricDescriptor::counter(
        "search_sidecar_reader_format_dispatch_total",
        SearchMetricUnit::Count,
        PROVIDER_CODEC,
    ),
    SearchMetricDescriptor::counter(
        "search_row_fetch_batches_total",
        SearchMetricUnit::Count,
        TABLE_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_row_fetch_rows_total",
        SearchMetricUnit::Rows,
        TABLE_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_row_fetch_projected_columns_total",
        SearchMetricUnit::Count,
        TABLE_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_row_fetch_segment_groups_total",
        SearchMetricUnit::Count,
        TABLE_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_row_fetch_column_batches_total",
        SearchMetricUnit::Count,
        TABLE_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_row_fetch_fixed_width_column_batches_total",
        SearchMetricUnit::Count,
        TABLE_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_row_fetch_varlen_column_batches_total",
        SearchMetricUnit::Count,
        TABLE_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_row_fetch_projected_bytes_total",
        SearchMetricUnit::Bytes,
        TABLE_PROVIDER,
    ),
    SearchMetricDescriptor::histogram(
        "search_row_fetch_latency_us",
        SearchMetricUnit::Microseconds,
        TABLE_PROVIDER,
        SEARCH_LATENCY_BUCKETS_US,
    ),
    SearchMetricDescriptor::counter(
        "search_row_fetch_latency_us_total",
        SearchMetricUnit::Microseconds,
        TABLE_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "column_read_by_rowids_page_run_seeks_total",
        SearchMetricUnit::Count,
        TABLE_PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_generation_retired_total",
        SearchMetricUnit::Count,
        PROVIDER,
    ),
    SearchMetricDescriptor::counter(
        "search_generation_retired_bytes_total",
        SearchMetricUnit::Bytes,
        PROVIDER,
    ),
    SearchMetricDescriptor::histogram(
        "search_generation_lease_hold_time_us",
        SearchMetricUnit::Microseconds,
        PROVIDER,
        SEARCH_BUILD_LATENCY_BUCKETS_US,
    ),
    SearchMetricDescriptor::histogram(
        "search_artifact_gc_delay_us",
        SearchMetricUnit::Microseconds,
        PROVIDER_REASON,
        SEARCH_BUILD_LATENCY_BUCKETS_US,
    ),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTelemetryEvent {
    pub kind: SearchIndexKind,
    pub segments_searched: usize,
    pub candidates_produced: usize,
    pub rows_returned: usize,
    pub peak_heap_items: usize,
    pub degraded_segments: usize,
    pub degraded_score_reasons: Vec<String>,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentTelemetryEvent {
    pub kind: SearchIndexKind,
    pub rowset_id: RowsetId,
    pub segment_id: SegmentId,
    pub candidates_produced: usize,
    pub degraded: bool,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationTelemetryEvent {
    pub kind: SearchIndexKind,
    pub definition_id: SearchDefinitionId,
    pub generation_id: SearchGenerationId,
    pub build_epoch: BuildEpoch,
    pub coverage: CoverageState,
    pub artifact_count: usize,
}

pub trait SearchTelemetryCollector: Send + Sync {
    fn record_query(&self, _event: QueryTelemetryEvent) {}

    fn record_segment_search(&self, _event: SegmentTelemetryEvent) {}

    fn record_generation(&self, _event: GenerationTelemetryEvent) {}
}

#[derive(Debug, Default)]
pub struct NoopSearchTelemetryCollector;

impl SearchTelemetryCollector for NoopSearchTelemetryCollector {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn search_metric_descriptors_have_unique_names() {
        let mut names = BTreeSet::new();
        for descriptor in SEARCH_METRIC_DESCRIPTORS {
            assert!(
                names.insert(descriptor.name),
                "duplicate search metric descriptor `{}`",
                descriptor.name
            );
            assert!(
                descriptor
                    .dimensions
                    .windows(2)
                    .all(|pair| pair[0] != pair[1]),
                "metric `{}` repeats adjacent dimensions",
                descriptor.name
            );
        }
    }

    #[test]
    fn latency_histograms_keep_required_low_latency_buckets() {
        let required = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000];
        for descriptor in SEARCH_METRIC_DESCRIPTORS {
            if descriptor.metric_type != SearchMetricType::Histogram {
                assert!(descriptor.buckets_us.is_empty());
                continue;
            }
            for bucket in required {
                assert!(
                    descriptor.buckets_us.contains(&bucket),
                    "histogram `{}` missing required {}us bucket",
                    descriptor.name,
                    bucket
                );
            }
        }
    }

    #[test]
    fn row_fetch_metrics_are_declared_with_table_provider_dimensions() {
        for metric_name in [
            "search_row_fetch_batches_total",
            "search_row_fetch_rows_total",
            "search_row_fetch_projected_columns_total",
            "search_row_fetch_segment_groups_total",
            "search_row_fetch_column_batches_total",
            "search_row_fetch_fixed_width_column_batches_total",
            "search_row_fetch_varlen_column_batches_total",
            "search_row_fetch_projected_bytes_total",
            "search_row_fetch_latency_us",
            "column_read_by_rowids_page_run_seeks_total",
        ] {
            let descriptor = SEARCH_METRIC_DESCRIPTORS
                .iter()
                .find(|descriptor| descriptor.name == metric_name)
                .unwrap_or_else(|| panic!("missing row fetch metric `{}`", metric_name));
            assert_eq!(descriptor.dimensions, TABLE_PROVIDER);
        }
    }
}
