// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! paro_search_metrics() table function.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_storage::metrics::{storage_metrics, StorageMetricsSnapshot};
use paro_storage::search::{
    SearchIndexKind, SearchMetricDescriptor, SearchMetricDimension, SearchMetricType,
    SearchMetricUnit, SEARCH_BUILD_LATENCY_BUCKETS_US, SEARCH_LATENCY_BUCKETS_US,
    SEARCH_METRIC_DESCRIPTORS,
};

use crate::table::{
    GlobalTableFunctionState, TableFunction, TableFunctionBindData, TableFunctionBindInput,
    TableFunctionInitInput, TableFunctionInput, TableFunctionResult, TableFunctionSet,
};

#[derive(Clone)]
pub struct ParoSearchMetricsBindData;

impl TableFunctionBindData for ParoSearchMetricsBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn cardinality(&self) -> Option<usize> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct SearchMetricData {
    pub metric_name: String,
    pub metric_type: String,
    pub unit: String,
    pub dimensions: String,
    pub table_id: i64,
    pub definition_id: i64,
    pub provider: String,
    pub reason: String,
    pub codec: String,
    pub bucket_le_us: i64,
    pub bucket_label: String,
    pub value: i64,
}

pub struct ParoSearchMetricsGlobalState {
    pub entries: Vec<SearchMetricData>,
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoSearchMetricsGlobalState {
    fn max_threads(&self) -> usize {
        1
    }

    fn get_progress(&self) -> f64 {
        if self.entries.is_empty() {
            return 100.0;
        }
        let offset = self.offset.load(Ordering::Relaxed);
        (offset as f64 / self.entries.len() as f64) * 100.0
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

fn paro_search_metrics_bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    for (name, ty) in [
        ("metric_name", LogicalType::Varchar),
        ("metric_type", LogicalType::Varchar),
        ("unit", LogicalType::Varchar),
        ("dimensions", LogicalType::Varchar),
        ("table_id", LogicalType::BigInt),
        ("definition_id", LogicalType::BigInt),
        ("provider", LogicalType::Varchar),
        ("reason", LogicalType::Varchar),
        ("codec", LogicalType::Varchar),
        ("bucket_le_us", LogicalType::BigInt),
        ("bucket_label", LogicalType::Varchar),
        ("value", LogicalType::BigInt),
    ] {
        names.push(name.to_string());
        return_types.push(ty);
    }

    Ok(Some(Box::new(ParoSearchMetricsBindData)))
}

fn paro_search_metrics_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    Ok(Some(Box::new(ParoSearchMetricsGlobalState {
        entries: collect_search_metric_data(),
        offset: AtomicUsize::new(0),
    })))
}

fn paro_search_metrics_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let output_allocator = output.allocator().clone();
    let gstate = input.global_state.and_then(|state| {
        state
            .as_any()
            .downcast_ref::<ParoSearchMetricsGlobalState>()
    });
    let Some(gstate) = gstate else {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    };

    let offset = gstate.offset.load(Ordering::Relaxed);
    if offset >= gstate.entries.len() {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    let batch_size = 2048.min(gstate.entries.len() - offset);
    let mut metric_names = Vec::with_capacity(batch_size);
    let mut metric_types = Vec::with_capacity(batch_size);
    let mut units = Vec::with_capacity(batch_size);
    let mut dimensions = Vec::with_capacity(batch_size);
    let mut table_ids = Vec::with_capacity(batch_size);
    let mut definition_ids = Vec::with_capacity(batch_size);
    let mut providers = Vec::with_capacity(batch_size);
    let mut reasons = Vec::with_capacity(batch_size);
    let mut codecs = Vec::with_capacity(batch_size);
    let mut bucket_le_us = Vec::with_capacity(batch_size);
    let mut bucket_labels = Vec::with_capacity(batch_size);
    let mut values = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        metric_names.push(entry.metric_name.clone());
        metric_types.push(entry.metric_type.clone());
        units.push(entry.unit.clone());
        dimensions.push(entry.dimensions.clone());
        table_ids.push(entry.table_id);
        definition_ids.push(entry.definition_id);
        providers.push(entry.provider.clone());
        reasons.push(entry.reason.clone());
        codecs.push(entry.codec.clone());
        bucket_le_us.push(entry.bucket_le_us);
        bucket_labels.push(entry.bucket_label.clone());
        values.push(entry.value);
    }

    gstate.offset.fetch_add(batch_size, Ordering::Relaxed);

    let metric_name_refs = metric_names
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();
    let metric_type_refs = metric_types
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();
    let unit_refs = units.iter().map(|value| value.as_str()).collect::<Vec<_>>();
    let dimension_refs = dimensions
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();
    let provider_refs = providers
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();
    let reason_refs = reasons
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();
    let codec_refs = codecs
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();
    let bucket_label_refs = bucket_labels
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();

    if let Some(col) = output.column_mut(0) {
        *col = Vector::try_from_strings(&metric_name_refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(1) {
        *col = Vector::try_from_strings(&metric_type_refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(2) {
        *col = Vector::try_from_strings(&unit_refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(3) {
        *col = Vector::try_from_strings(&dimension_refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(4) {
        *col = Vector::try_from_i64(&table_ids, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(5) {
        *col = Vector::try_from_i64(&definition_ids, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(6) {
        *col = Vector::try_from_strings(&provider_refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(7) {
        *col = Vector::try_from_strings(&reason_refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(8) {
        *col = Vector::try_from_strings(&codec_refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(9) {
        *col = Vector::try_from_i64(&bucket_le_us, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(10) {
        *col = Vector::try_from_strings(&bucket_label_refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(11) {
        *col = Vector::try_from_i64(&values, output_allocator.clone())?;
    }
    output.set_cardinality(batch_size);

    if gstate.offset.load(Ordering::Relaxed) >= gstate.entries.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn paro_search_metrics_progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state
        .map(|state| state.get_progress())
        .unwrap_or(-1.0)
}

pub fn create_paro_search_metrics_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_search_metrics", vec![]);
    func.bind = Some(paro_search_metrics_bind);
    func.init_global = Some(paro_search_metrics_init_global);
    func.function = Some(paro_search_metrics_function);
    func.table_scan_progress = Some(paro_search_metrics_progress);

    let mut set = TableFunctionSet::new("paro_search_metrics");
    set.add_function(func);
    set
}

pub fn populate_search_metric_data(
    state: &mut ParoSearchMetricsGlobalState,
    entries: Vec<SearchMetricData>,
) {
    state.entries = entries;
    state.offset.store(0, Ordering::Relaxed);
}

#[derive(Debug, Clone, Copy, Default)]
struct MetricDimensions<'a> {
    table_id: u64,
    definition_id: u64,
    provider: &'a str,
    reason: &'a str,
    codec: &'a str,
}

fn collect_search_metric_data() -> Vec<SearchMetricData> {
    let snapshot = storage_metrics().snapshot();
    search_metric_data_from_snapshot(&snapshot)
}

fn search_metric_data_from_snapshot(snapshot: &StorageMetricsSnapshot) -> Vec<SearchMetricData> {
    let mut entries = Vec::new();

    for series in &snapshot.search_inline_build_by_key {
        let dims = MetricDimensions {
            definition_id: series.key.definition_id,
            provider: provider_label(series.key.provider),
            ..MetricDimensions::default()
        };
        push_value(
            &mut entries,
            "search_inline_build_rows_total",
            dims,
            series.counters.rows_total,
        );
        push_value(
            &mut entries,
            "search_inline_build_bytes_total",
            dims,
            series.counters.bytes_total,
        );
        push_histogram(
            &mut entries,
            "search_inline_build_latency_us",
            dims,
            SEARCH_BUILD_LATENCY_BUCKETS_US,
            &series.counters.latency_us_buckets,
        );
        push_value(
            &mut entries,
            "search_inline_build_cpu_us_total",
            dims,
            series.counters.cpu_us_total,
        );
    }
    for series in &snapshot.search_inline_build_failures_by_key {
        push_value(
            &mut entries,
            "search_inline_build_failures_total",
            MetricDimensions {
                provider: provider_label(series.key.provider),
                reason: &series.key.reason,
                ..MetricDimensions::default()
            },
            series.failures_total,
        );
    }

    for series in &snapshot.search_sidecar_build_by_key {
        let dims = MetricDimensions {
            definition_id: series.key.definition_id,
            provider: provider_label(series.key.provider),
            ..MetricDimensions::default()
        };
        push_value(
            &mut entries,
            "search_sidecar_build_rows_total",
            dims,
            series.counters.rows_total,
        );
        push_value(
            &mut entries,
            "search_sidecar_build_read_bytes_total",
            dims,
            series.counters.read_bytes_total,
        );
        push_value(
            &mut entries,
            "search_sidecar_build_write_bytes_total",
            dims,
            series.counters.write_bytes_total,
        );
        push_value(
            &mut entries,
            "search_sidecar_build_artifact_bytes_total",
            dims,
            series.counters.artifact_bytes_total,
        );
        push_histogram(
            &mut entries,
            "search_sidecar_build_latency_us",
            dims,
            SEARCH_BUILD_LATENCY_BUCKETS_US,
            &series.counters.latency_us_buckets,
        );
    }

    for series in &snapshot.search_manifest_by_key {
        let dims = MetricDimensions {
            codec: &series.key.codec,
            ..MetricDimensions::default()
        };
        push_histogram(
            &mut entries,
            "search_manifest_publish_latency_us",
            dims,
            SEARCH_LATENCY_BUCKETS_US,
            &series.counters.publish_latency_us_buckets,
        );
        push_value(
            &mut entries,
            "search_manifest_publish_cas_retries_total",
            dims,
            series.counters.publish_cas_retries_total,
        );
        push_histogram(
            &mut entries,
            "search_manifest_open_latency_us",
            dims,
            SEARCH_LATENCY_BUCKETS_US,
            &series.counters.open_latency_us_buckets,
        );
        push_value(
            &mut entries,
            "search_manifest_delta_count",
            dims,
            series.counters.delta_count,
        );
        push_value(
            &mut entries,
            "search_manifest_open_bytes_total",
            dims,
            series.counters.open_bytes_total,
        );
    }

    for series in &snapshot.search_tail_by_key {
        let dims = MetricDimensions {
            provider: provider_label(series.key.provider),
            ..MetricDimensions::default()
        };
        push_value(
            &mut entries,
            "search_tail_rows",
            dims,
            series.counters.tail_rows,
        );
        push_value(
            &mut entries,
            "search_tail_bytes",
            dims,
            series.counters.tail_bytes,
        );
        push_value(
            &mut entries,
            "search_tail_backlog_tier",
            dims,
            series.counters.tail_backlog_tier,
        );
        push_value(
            &mut entries,
            "search_tail_exact_merge_rows_total",
            dims,
            series.counters.exact_merge_rows_total,
        );
    }
    for series in &snapshot.search_tail_rejected_by_key {
        push_value(
            &mut entries,
            "search_tail_exact_merge_rejected_total",
            MetricDimensions {
                provider: provider_label(series.key.provider),
                reason: &series.key.reason,
                ..MetricDimensions::default()
            },
            series.rejected_total,
        );
    }
    for series in &snapshot.search_fulltext_degraded_score_by_key {
        push_value(
            &mut entries,
            "search_fulltext_degraded_score_queries",
            MetricDimensions {
                table_id: series.key.table_id,
                reason: &series.key.reason,
                ..MetricDimensions::default()
            },
            series.degraded_queries,
        );
    }

    for series in &snapshot.search_sidecar_reader_by_key {
        let dims = MetricDimensions {
            provider: provider_label(series.key.provider),
            codec: &series.key.codec,
            ..MetricDimensions::default()
        };
        push_value(
            &mut entries,
            "search_sidecar_reader_open_count_total",
            dims,
            series.counters.open_count_total,
        );
        push_value(
            &mut entries,
            "search_sidecar_reader_cache_hits_total",
            dims,
            series.counters.cache_hits_total,
        );
        push_value(
            &mut entries,
            "search_sidecar_reader_cache_misses_total",
            dims,
            series.counters.cache_misses_total,
        );
        push_value(
            &mut entries,
            "search_sidecar_reader_mmap_bytes",
            dims,
            series.counters.mmap_bytes,
        );
        push_value(
            &mut entries,
            "search_sidecar_reader_format_dispatch_total",
            dims,
            series.counters.format_dispatch_total,
        );
    }

    for series in &snapshot.search_row_fetch_by_key {
        let dims = MetricDimensions {
            table_id: series.key.table_id,
            provider: provider_label(series.key.provider),
            ..MetricDimensions::default()
        };
        push_value(
            &mut entries,
            "search_row_fetch_batches_total",
            dims,
            series.counters.batches_total,
        );
        push_value(
            &mut entries,
            "search_row_fetch_rows_total",
            dims,
            series.counters.rows_total,
        );
        push_value(
            &mut entries,
            "search_row_fetch_projected_columns_total",
            dims,
            series.counters.projected_columns_total,
        );
        push_value(
            &mut entries,
            "search_row_fetch_segment_groups_total",
            dims,
            series.counters.segment_groups_total,
        );
        push_value(
            &mut entries,
            "search_row_fetch_column_batches_total",
            dims,
            series.counters.column_batches_total,
        );
        push_value(
            &mut entries,
            "search_row_fetch_fixed_width_column_batches_total",
            dims,
            series.counters.fixed_width_column_batches_total,
        );
        push_value(
            &mut entries,
            "search_row_fetch_varlen_column_batches_total",
            dims,
            series.counters.varlen_column_batches_total,
        );
        push_value(
            &mut entries,
            "search_row_fetch_projected_bytes_total",
            dims,
            series.counters.projected_bytes_total,
        );
        push_histogram(
            &mut entries,
            "search_row_fetch_latency_us",
            dims,
            SEARCH_LATENCY_BUCKETS_US,
            &series.counters.latency_us_buckets,
        );
        push_value(
            &mut entries,
            "search_row_fetch_latency_us_total",
            dims,
            series.counters.latency_us_total,
        );
        push_value(
            &mut entries,
            "column_read_by_rowids_page_run_seeks_total",
            dims,
            series.counters.column_read_by_rowids_page_run_seeks_total,
        );
    }

    for series in &snapshot.search_generation_by_key {
        let dims = MetricDimensions {
            provider: provider_label(series.key.provider),
            ..MetricDimensions::default()
        };
        push_value(
            &mut entries,
            "search_generation_retired_total",
            dims,
            series.counters.retired_total,
        );
        push_value(
            &mut entries,
            "search_generation_retired_bytes_total",
            dims,
            series.counters.retired_bytes_total,
        );
        push_histogram(
            &mut entries,
            "search_generation_lease_hold_time_us",
            dims,
            SEARCH_BUILD_LATENCY_BUCKETS_US,
            &series.counters.lease_hold_time_us_buckets,
        );
    }
    for series in &snapshot.search_artifact_gc_delay_by_key {
        push_histogram(
            &mut entries,
            "search_artifact_gc_delay_us",
            MetricDimensions {
                provider: provider_label(series.key.provider),
                reason: &series.key.reason,
                ..MetricDimensions::default()
            },
            SEARCH_BUILD_LATENCY_BUCKETS_US,
            &series.counters.delay_us_buckets,
        );
    }

    entries
}

fn push_value(
    entries: &mut Vec<SearchMetricData>,
    metric_name: &'static str,
    dims: MetricDimensions<'_>,
    value: u64,
) {
    let descriptor = descriptor(metric_name);
    if value == 0 && descriptor.metric_type != SearchMetricType::Gauge {
        return;
    }
    entries.push(metric_row(descriptor, dims, 0, String::new(), value));
}

fn push_histogram(
    entries: &mut Vec<SearchMetricData>,
    metric_name: &'static str,
    dims: MetricDimensions<'_>,
    buckets_us: &[u64],
    counts: &[u64],
) {
    if counts.iter().all(|count| *count == 0) {
        return;
    }

    let descriptor = descriptor(metric_name);
    for (idx, value) in counts.iter().enumerate() {
        let (bucket_le_us, bucket_label) = bucket_metadata(buckets_us, idx);
        entries.push(metric_row(
            descriptor,
            dims,
            bucket_le_us,
            bucket_label,
            *value,
        ));
    }
}

fn metric_row(
    descriptor: &SearchMetricDescriptor,
    dims: MetricDimensions<'_>,
    bucket_le_us: i64,
    bucket_label: String,
    value: u64,
) -> SearchMetricData {
    SearchMetricData {
        metric_name: descriptor.name.to_string(),
        metric_type: metric_type_label(descriptor.metric_type).to_string(),
        unit: metric_unit_label(descriptor.unit).to_string(),
        dimensions: dimension_label_list(descriptor.dimensions),
        table_id: dims.table_id as i64,
        definition_id: dims.definition_id as i64,
        provider: dims.provider.to_string(),
        reason: dims.reason.to_string(),
        codec: dims.codec.to_string(),
        bucket_le_us,
        bucket_label,
        value: value.min(i64::MAX as u64) as i64,
    }
}

fn descriptor(metric_name: &str) -> &'static SearchMetricDescriptor {
    SEARCH_METRIC_DESCRIPTORS
        .iter()
        .find(|descriptor| descriptor.name == metric_name)
        .unwrap_or_else(|| panic!("missing search metric descriptor `{}`", metric_name))
}

fn bucket_metadata(buckets_us: &[u64], idx: usize) -> (i64, String) {
    if let Some(bucket) = buckets_us.get(idx) {
        let bucket_le_us = (*bucket).min(i64::MAX as u64) as i64;
        (bucket_le_us, format!("<={}us", bucket))
    } else {
        (i64::MAX, "+Inf".to_string())
    }
}

fn provider_label(provider: SearchIndexKind) -> &'static str {
    match provider {
        SearchIndexKind::Hnsw => "hnsw",
        SearchIndexKind::Sparse => "sparse",
        SearchIndexKind::FullText => "fulltext",
    }
}

fn metric_type_label(metric_type: SearchMetricType) -> &'static str {
    match metric_type {
        SearchMetricType::Counter => "counter",
        SearchMetricType::Gauge => "gauge",
        SearchMetricType::Histogram => "histogram",
    }
}

fn metric_unit_label(unit: SearchMetricUnit) -> &'static str {
    match unit {
        SearchMetricUnit::Count => "count",
        SearchMetricUnit::Rows => "rows",
        SearchMetricUnit::Bytes => "bytes",
        SearchMetricUnit::Microseconds => "microseconds",
        SearchMetricUnit::Percent => "percent",
    }
}

fn dimension_label_list(dimensions: &[SearchMetricDimension]) -> String {
    dimensions
        .iter()
        .map(|dimension| match dimension {
            SearchMetricDimension::Global => "global",
            SearchMetricDimension::Table => "table",
            SearchMetricDimension::Definition => "definition",
            SearchMetricDimension::Provider => "provider",
            SearchMetricDimension::Reason => "reason",
            SearchMetricDimension::Codec => "codec",
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use paro_common::runtime_value::Value;
    use paro_storage::metrics::SearchSidecarBuildMetricKey;

    #[test]
    fn test_paro_search_metrics_bind() {
        let mut return_types = Vec::new();
        let mut names = Vec::new();

        let bind = paro_search_metrics_bind(
            &TableFunctionBindInput::new(&[], &HashMap::new()),
            &mut return_types,
            &mut names,
        )
        .unwrap();

        assert!(bind.is_some());
        assert_eq!(
            names,
            [
                "metric_name",
                "metric_type",
                "unit",
                "dimensions",
                "table_id",
                "definition_id",
                "provider",
                "reason",
                "codec",
                "bucket_le_us",
                "bucket_label",
                "value"
            ]
        );
        assert_eq!(return_types.len(), 12);
    }

    #[test]
    fn test_paro_search_metrics_function_with_data() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let mut state_box = paro_search_metrics_init_global(&input).unwrap().unwrap();
        let state = state_box
            .as_any_mut()
            .downcast_mut::<ParoSearchMetricsGlobalState>()
            .unwrap();
        populate_search_metric_data(
            state,
            vec![SearchMetricData {
                metric_name: "search_row_fetch_latency_us".to_string(),
                metric_type: "histogram".to_string(),
                unit: "microseconds".to_string(),
                dimensions: "table,provider".to_string(),
                table_id: 7,
                definition_id: 0,
                provider: "fulltext".to_string(),
                reason: String::new(),
                codec: String::new(),
                bucket_le_us: 10,
                bucket_label: "<=10us".to_string(),
                value: 3,
            }],
        );

        let state_ref = state_box
            .as_any()
            .downcast_ref::<ParoSearchMetricsGlobalState>()
            .unwrap();
        let mut input = TableFunctionInput {
            bind_data: None,
            local_state: None,
            global_state: Some(state_ref),
        };
        let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
            &[
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::Varchar,
                LogicalType::BigInt,
            ],
            8,
        );

        let result = paro_search_metrics_function(&mut input, &mut chunk).unwrap();
        assert_eq!(result, TableFunctionResult::Finished);
        assert_eq!(
            chunk.column(0).unwrap().get_string(0),
            Some("search_row_fetch_latency_us")
        );
        assert_eq!(chunk.column(4).unwrap().get_value(0), Value::BigInt(7));
        assert_eq!(chunk.column(6).unwrap().get_string(0), Some("fulltext"));
        assert_eq!(chunk.column(9).unwrap().get_value(0), Value::BigInt(10));
        assert_eq!(chunk.column(11).unwrap().get_value(0), Value::BigInt(3));
    }

    #[test]
    fn test_paro_search_metrics_init_reads_storage_snapshot() {
        let definition_id = 987_654_321;
        storage_metrics().record_search_sidecar_build(
            SearchSidecarBuildMetricKey {
                definition_id,
                provider: SearchIndexKind::FullText,
            },
            1234,
            2048,
            1024,
            512,
            17,
        );

        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let state_box = paro_search_metrics_init_global(&input).unwrap().unwrap();
        let state = state_box
            .as_any()
            .downcast_ref::<ParoSearchMetricsGlobalState>()
            .unwrap();

        assert!(
            state.entries.iter().any(|entry| {
                entry.metric_name == "search_sidecar_build_rows_total"
                    && entry.definition_id == definition_id as i64
                    && entry.provider == "fulltext"
                    && entry.value >= 1234
            }),
            "paro_search_metrics() should expose storage_metrics() snapshot rows"
        );
        assert!(
            state.entries.iter().any(|entry| {
                entry.metric_name == "search_sidecar_build_latency_us"
                    && entry.definition_id == definition_id as i64
                    && entry.provider == "fulltext"
                    && entry.bucket_label == "<=100us"
                    && entry.value >= 1
            }),
            "histogram buckets should be expanded into table rows"
        );
    }
}
