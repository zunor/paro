// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # RowsetWriter
//!
//! Rowset writer that coordinates multiple Segment writes.
//!
//! ## Key Design
//!
//! - Coordinates writing data across multiple Segments
//! - Automatically creates new Segments when size threshold is reached
//! - Tracks statistics (rows, data size, index size) across all Segments
//! - Produces a complete Rowset with all Segments on finalization
//!
//! ## Usage
//!
//! ```ignore
//! let context = RowsetWriterContext::new(schema, tablet_id, version, rowset_path);
//! let mut writer = RowsetWriter::create(context)?;
//!
//! // Add data chunks
//! writer.add_chunk(&column_data_vec)?;
//!
//! // Optionally flush current segment
//! writer.flush_segment()?;
//!
//! // Build the final Rowset
//! let rowset = writer.build()?;
//! ```

use super::rowset::{Rowset, RowsetSharedPtr};
use super::rowset_meta::{generate_rowset_id, RowsetId, RowsetMeta, RowsetState, SegmentsOverlap};
use super::segment::{
    ColumnData, HnswColumnBuildOptions, Segment, SegmentInlineIndexKind, SegmentInlineIndexPage,
    SegmentWriter, SegmentWriterOptions,
};
use crate::metrics::{storage_metrics, SearchInlineBuildMetricKey};
use crate::search::{
    AdmissionDecision, AdmissionGrant, FlushSearchMode, HnswInlineBuildEstimate,
    InlineAdmissionRequest, InlineArtifactBuildResult, MaintenanceCost, SearchAdmission,
    SearchIndexKind, SearchInlineBuilderEntry, SearchInlineBuilderSet, SegmentChunkInput,
    SegmentChunkSink, SegmentFlushCtx, SegmentSinkSavepoint,
};
use crate::tablet::{ColumnId, TabletSchemaRef, Version};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default segment size threshold (256 MB)
const DEFAULT_SEGMENT_SIZE_THRESHOLD: u64 = 256 * 1024 * 1024;

/// Default maximum rows per segment (1 million)
const DEFAULT_MAX_ROWS_PER_SEGMENT: u64 = 1_000_000;

/// Source of the physical segment row boundary. The ordinary fallback keeps
/// non-search tables at a conservative size, while a durable HNSW placement
/// contract may raise or lower the adaptive boundary. An explicit caller cap
/// is always an upper bound and is never enlarged by a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentRowLimit {
    Adaptive { fallback_rows: u64 },
    Explicit { max_rows: u64 },
}

impl SegmentRowLimit {
    fn effective(self, provider_rows: Option<u64>) -> u64 {
        match self {
            Self::Adaptive { fallback_rows } => provider_rows.unwrap_or(fallback_rows),
            Self::Explicit { max_rows } => {
                provider_rows.map_or(max_rows, |rows| rows.min(max_rows))
            }
        }
        .max(1)
    }
}

/// Required freshness must block the current segment instead of silently
/// falling back to tail-only. Keep the retry bounded so a broken admission
/// implementation cannot wedge the writer forever.
const REQUIRED_INLINE_ADMISSION_MAX_ATTEMPTS: usize = 32;

/// Context for creating a RowsetWriter
#[derive(Debug, Clone)]
pub struct RowsetWriterContext {
    /// Rowset ID (auto-generated if not specified)
    pub rowset_id: RowsetId,
    /// Tablet ID
    pub tablet_id: u64,
    /// Version for this rowset
    pub version: Version,
    /// Tablet schema
    pub schema: TabletSchemaRef,
    /// Rowset data directory path
    pub rowset_path: PathBuf,
    /// Segment size threshold in bytes
    pub segment_size_threshold: u64,
    /// Physical row-boundary policy.
    pub segment_row_limit: SegmentRowLimit,
    /// Compression type for segments
    pub compression: super::page::CompressionType,
    /// Whether to build short key index
    pub build_short_key_index: bool,
    /// Number of short key columns
    pub num_short_key_columns: usize,
    /// Whether to build HNSW index pages during segment write.
    pub build_hnsw_indexes: bool,
    /// Immutable writer-side search builder snapshot for the rowset.
    pub search_inline_builders: SearchInlineBuilderSet,
    /// Optional subset of columns to write for partial-row rowsets.
    pub write_column_ids: Option<Vec<ColumnId>>,
}

impl RowsetWriterContext {
    /// Create a new RowsetWriterContext
    pub fn new(
        schema: TabletSchemaRef,
        tablet_id: u64,
        version: Version,
        rowset_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            rowset_id: generate_rowset_id(),
            tablet_id,
            version,
            schema,
            rowset_path: rowset_path.into(),
            segment_size_threshold: DEFAULT_SEGMENT_SIZE_THRESHOLD,
            segment_row_limit: SegmentRowLimit::Adaptive {
                fallback_rows: DEFAULT_MAX_ROWS_PER_SEGMENT,
            },
            compression: super::page::CompressionType::Lz4,
            build_short_key_index: true,
            num_short_key_columns: 3,
            build_hnsw_indexes: true,
            search_inline_builders: SearchInlineBuilderSet::default(),
            write_column_ids: None,
        }
    }

    /// Set rowset ID
    pub fn with_rowset_id(mut self, id: RowsetId) -> Self {
        self.rowset_id = id;
        self
    }

    /// Set segment size threshold
    pub fn with_segment_size_threshold(mut self, threshold: u64) -> Self {
        self.segment_size_threshold = threshold;
        self
    }

    /// Set maximum rows per segment
    pub fn with_max_rows_per_segment(mut self, max_rows: u64) -> Self {
        self.segment_row_limit = SegmentRowLimit::Explicit { max_rows };
        self
    }

    /// Set compression type
    pub fn with_compression(mut self, compression: super::page::CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Set whether to build short key index
    pub fn with_short_key_index(mut self, build: bool) -> Self {
        self.build_short_key_index = build;
        self
    }

    /// Set number of short key columns
    pub fn with_num_short_key_columns(mut self, num: usize) -> Self {
        self.num_short_key_columns = num;
        self
    }

    /// Set whether to build HNSW index pages while writing segments.
    pub fn with_build_hnsw_indexes(mut self, build: bool) -> Self {
        self.build_hnsw_indexes = build;
        self
    }

    pub fn with_search_inline_builders(mut self, builders: SearchInlineBuilderSet) -> Self {
        self.search_inline_builders = builders;
        self
    }

    pub fn with_write_column_ids(mut self, column_ids: Vec<ColumnId>) -> Self {
        self.write_column_ids = Some(column_ids);
        self
    }
}

/// Statistics for a completed segment
#[derive(Debug, Clone, Default)]
struct SegmentStats {
    /// Number of rows
    num_rows: u64,
    /// Data size in bytes
    data_size: u64,
    /// Index size in bytes
    index_size: u64,
}

struct ActiveSegmentSearchSink {
    definition_id: u64,
    provider: SearchIndexKind,
    sink: Box<dyn SegmentChunkSink>,
    _admission_lease: Option<AdmissionLease>,
    elapsed_micros: u64,
    cpu_micros: u64,
}

impl ActiveSegmentSearchSink {
    fn new(
        definition_id: u64,
        provider: SearchIndexKind,
        sink: Box<dyn SegmentChunkSink>,
        admission_lease: Option<AdmissionLease>,
    ) -> Self {
        Self {
            definition_id,
            provider,
            sink,
            _admission_lease: admission_lease,
            elapsed_micros: 0,
            cpu_micros: 0,
        }
    }

    fn metric_key(&self) -> SearchInlineBuildMetricKey {
        SearchInlineBuildMetricKey {
            definition_id: self.definition_id,
            provider: self.provider,
        }
    }

    fn append_chunk(&mut self, input: SegmentChunkInput<'_>) -> Result<()> {
        let started_at = Instant::now();
        let cpu_started_at = thread_cpu_micros();
        match self.sink.append_chunk(input) {
            Ok(()) => {
                self.elapsed_micros = self
                    .elapsed_micros
                    .saturating_add(elapsed_micros_since(started_at));
                self.cpu_micros = self
                    .cpu_micros
                    .saturating_add(cpu_micros_since(cpu_started_at));
                Ok(())
            }
            Err(err) => {
                self.elapsed_micros = self
                    .elapsed_micros
                    .saturating_add(elapsed_micros_since(started_at));
                self.cpu_micros = self
                    .cpu_micros
                    .saturating_add(cpu_micros_since(cpu_started_at));
                storage_metrics().record_search_inline_build_failure(self.provider, "append_error");
                Err(err)
            }
        }
    }

    fn mark_savepoint(&mut self) -> Result<SegmentSinkSavepoint> {
        self.sink.mark_savepoint()
    }

    fn rollback_to_savepoint(&mut self, savepoint: &SegmentSinkSavepoint) -> Result<()> {
        self.sink.rollback_to_savepoint(savepoint)
    }
}

struct AdmissionLease {
    admission: Arc<dyn SearchAdmission>,
    grant_id: u64,
}

impl AdmissionLease {
    fn new(admission: Arc<dyn SearchAdmission>, grant: AdmissionGrant) -> Self {
        Self {
            admission,
            grant_id: grant.grant_id,
        }
    }
}

impl Drop for AdmissionLease {
    fn drop(&mut self) {
        self.admission.release(self.grant_id);
    }
}

fn abort_search_sinks(sinks: Vec<ActiveSegmentSearchSink>) {
    for search_sink in sinks {
        let _ = search_sink.sink.abort();
    }
}

fn finish_search_sinks(
    sinks: Vec<ActiveSegmentSearchSink>,
) -> Result<Vec<InlineArtifactBuildResult>> {
    let mut results = Vec::with_capacity(sinks.len());
    let mut iter = sinks.into_iter();
    while let Some(search_sink) = iter.next() {
        let metric_key = search_sink.metric_key();
        let provider = search_sink.provider;
        let mut elapsed_micros = search_sink.elapsed_micros;
        let mut cpu_micros = search_sink.cpu_micros;
        let started_at = Instant::now();
        let cpu_started_at = thread_cpu_micros();
        match search_sink.sink.finish() {
            Ok(result) => {
                elapsed_micros = elapsed_micros.saturating_add(elapsed_micros_since(started_at));
                cpu_micros = cpu_micros.saturating_add(cpu_micros_since(cpu_started_at));
                let (rows, bytes) = inline_build_rows_bytes(&result);
                storage_metrics().record_search_inline_build(
                    metric_key,
                    rows,
                    bytes,
                    elapsed_micros,
                    cpu_micros,
                );
                results.push(result);
            }
            Err(err) => {
                storage_metrics().record_search_inline_build_failure(provider, "finish_error");
                abort_search_sinks(iter.collect());
                return Err(err);
            }
        }
    }
    Ok(results)
}

struct AdmittedInlineSink<'a> {
    entry: &'a SearchInlineBuilderEntry,
    grant: Option<AdmissionGrant>,
    admission: Option<Arc<dyn SearchAdmission>>,
    hnsw_inline: Option<HnswInlineBuildEstimate>,
}

fn release_admitted_inline_sinks(admitted: Vec<AdmittedInlineSink<'_>>) {
    for admitted_sink in admitted {
        if let (Some(admission), Some(grant)) = (admitted_sink.admission, admitted_sink.grant) {
            admission.release(grant.grant_id);
        }
    }
}

fn wait_until(deadline: Instant) {
    let delay = deadline.saturating_duration_since(Instant::now());
    if delay > Duration::ZERO {
        std::thread::sleep(delay);
    }
}

fn chunk_row_count(columns: &[ColumnData]) -> u64 {
    columns
        .first()
        .map(|column| u64::from(column.num_values))
        .unwrap_or(0)
}

fn split_hnsw_segment_admissions<'a>(
    admitted: Vec<AdmittedInlineSink<'a>>,
) -> Result<(
    Vec<AdmittedInlineSink<'a>>,
    Vec<AdmissionLease>,
    Option<u64>,
    BTreeMap<ColumnId, HnswColumnBuildOptions>,
)> {
    // Validate the complete physical index set before transferring any raw
    // admission grant into an RAII lease. A malformed or conflicting catalog
    // definition must release every grant acquired for this segment.
    let validated = (|| {
        let mut hnsw_inline_row_limit: Option<u64> = None;
        let mut hnsw_indexes = BTreeMap::new();
        for admitted_sink in &admitted {
            if admitted_sink.entry.definition.kind != SearchIndexKind::Hnsw {
                continue;
            }
            if let Some(estimate) = admitted_sink.hnsw_inline {
                let limit = estimate.max_segment_vector_count();
                hnsw_inline_row_limit = Some(
                    hnsw_inline_row_limit
                        .map(|existing| existing.min(limit))
                        .unwrap_or(limit),
                );
            }
            let (column_id, options) = hnsw_column_build_options(&admitted_sink.entry.definition)?;
            if hnsw_indexes.insert(column_id, options).is_some() {
                return Err(paro_error::invalid_input(format!(
                    "multiple active HNSW definitions target column {column_id}"
                )));
            }
        }
        Ok((hnsw_inline_row_limit, hnsw_indexes))
    })();
    let (hnsw_inline_row_limit, hnsw_indexes) = match validated {
        Ok(validated) => validated,
        Err(err) => {
            release_admitted_inline_sinks(admitted);
            return Err(err);
        }
    };

    let mut search_sinks = Vec::new();
    let mut hnsw_leases = Vec::new();
    for mut admitted_sink in admitted {
        if admitted_sink.entry.definition.kind == SearchIndexKind::Hnsw {
            if let (Some(admission), Some(grant)) =
                (admitted_sink.admission.take(), admitted_sink.grant.take())
            {
                hnsw_leases.push(AdmissionLease::new(admission, grant));
            }
        } else {
            search_sinks.push(admitted_sink);
        }
    }
    Ok((
        search_sinks,
        hnsw_leases,
        hnsw_inline_row_limit,
        hnsw_indexes,
    ))
}

fn hnsw_column_build_options(
    definition: &crate::search::SearchIndexDefinition,
) -> Result<(ColumnId, HnswColumnBuildOptions)> {
    let [column_id] = definition.column_ids.as_slice() else {
        return Err(paro_error::invalid_input(
            "HNSW definition requires exactly one vector column",
        ));
    };
    let provider = definition.hnsw_provider_config()?;
    Ok((
        *column_id,
        HnswColumnBuildOptions {
            build_contract: provider.build_contract(),
        },
    ))
}

fn estimate_inline_build_cost(
    entry: &SearchInlineBuilderEntry,
    max_rows_per_segment: u64,
) -> MaintenanceCost {
    let row_count = max_rows_per_segment.max(1);
    match entry.definition.kind {
        SearchIndexKind::FullText => MaintenanceCost {
            cpu_ns: row_count.saturating_mul(5_000),
            memory_peak_bytes: row_count.saturating_mul(128),
            publish_bytes: 256,
            ..Default::default()
        },
        SearchIndexKind::Sparse => MaintenanceCost {
            cpu_ns: row_count.saturating_mul(3_000),
            memory_peak_bytes: row_count.saturating_mul(96),
            publish_bytes: 256,
            ..Default::default()
        },
        SearchIndexKind::Hnsw => MaintenanceCost {
            cpu_ns: row_count.saturating_mul(50_000),
            memory_peak_bytes: row_count.saturating_mul(1_024),
            publish_bytes: 256,
            ..Default::default()
        },
    }
}

fn hnsw_inline_build_estimate(
    entry: &SearchInlineBuilderEntry,
    max_rows_per_segment: u64,
    column_schema: &[crate::tablet::TabletColumn],
) -> Result<Option<HnswInlineBuildEstimate>> {
    if entry.definition.kind != SearchIndexKind::Hnsw {
        return Ok(None);
    }
    let column_id = *entry.definition.column_ids.first().ok_or_else(|| {
        paro_error::invalid_input("HNSW definition requires exactly one vector column")
    })?;
    let dimension = column_schema
        .iter()
        .find(|column| column.id == column_id)
        .and_then(|column| match &column.logical_type {
            LogicalType::Array(_, dimension) => u32::try_from(*dimension).ok(),
            _ => None,
        })
        .ok_or_else(|| {
            paro_error::invalid_input(format!(
                "HNSW definition column {column_id} is not a VECTOR(N) column"
            ))
        })?;
    HnswInlineBuildEstimate::from_definition(
        &entry.definition,
        max_rows_per_segment.max(1),
        dimension,
    )
}

fn inline_build_rows_bytes(result: &InlineArtifactBuildResult) -> (u64, u64) {
    let rows = result
        .blobs
        .iter()
        .map(|blob| blob.stats.row_count)
        .max()
        .unwrap_or(0);
    let bytes = result
        .blobs
        .iter()
        .map(|blob| blob.stats.bytes_on_disk)
        .sum();
    (rows, bytes)
}

fn elapsed_micros_since(started_at: Instant) -> u64 {
    let micros = started_at.elapsed().as_micros();
    micros.min(u128::from(u64::MAX)) as u64
}

fn cpu_micros_since(started_at: Option<u64>) -> u64 {
    let Some(started_at) = started_at else {
        return 0;
    };
    thread_cpu_micros()
        .map(|finished_at| finished_at.saturating_sub(started_at))
        .unwrap_or(0)
}

#[cfg(target_os = "linux")]
fn thread_cpu_micros() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_THREAD, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
    timeval_to_micros(usage.ru_utime).checked_add(timeval_to_micros(usage.ru_stime))
}

#[cfg(target_os = "linux")]
fn timeval_to_micros(value: libc::timeval) -> u64 {
    let seconds = u64::try_from(value.tv_sec).unwrap_or_default();
    let micros = u64::try_from(value.tv_usec).unwrap_or_default();
    seconds.saturating_mul(1_000_000).saturating_add(micros)
}

#[cfg(target_vendor = "apple")]
fn thread_cpu_micros() -> Option<u64> {
    let thread = unsafe { libc::pthread_mach_thread_np(libc::pthread_self()) };
    if thread == 0 {
        return None;
    }
    let mut info = std::mem::MaybeUninit::<libc::thread_basic_info>::uninit();
    let mut count = libc::THREAD_BASIC_INFO_COUNT;
    let rc = unsafe {
        libc::thread_info(
            thread,
            libc::THREAD_BASIC_INFO as libc::thread_flavor_t,
            info.as_mut_ptr().cast::<libc::integer_t>(),
            &mut count,
        )
    };
    if rc != libc::KERN_SUCCESS {
        return None;
    }
    let info = unsafe { info.assume_init() };
    apple_time_value_to_micros(info.user_time)
        .checked_add(apple_time_value_to_micros(info.system_time))
}

#[cfg(target_vendor = "apple")]
fn apple_time_value_to_micros(value: libc::time_value_t) -> u64 {
    let seconds = u64::try_from(value.seconds).unwrap_or_default();
    let micros = u64::try_from(value.microseconds).unwrap_or_default();
    seconds.saturating_mul(1_000_000).saturating_add(micros)
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
fn thread_cpu_micros() -> Option<u64> {
    None
}

fn inline_index_pages_from_search_results<'a>(
    results: &'a [InlineArtifactBuildResult],
) -> Result<Vec<SegmentInlineIndexPage<'a>>> {
    let mut pages = Vec::new();
    for result in results {
        for blob in &result.blobs {
            if blob.bytes.is_empty() {
                continue;
            }
            let kind = match blob.kind {
                SearchIndexKind::FullText => SegmentInlineIndexKind::FullText,
                SearchIndexKind::Sparse => SegmentInlineIndexKind::Sparse,
                SearchIndexKind::Hnsw => SegmentInlineIndexKind::Hnsw,
            };
            let actual_checksum = seahash::hash(&blob.bytes);
            if actual_checksum != blob.checksum {
                return Err(paro_error::data_corrupted(format!(
                    "inline search artifact checksum mismatch for definition {} generation {} column {}",
                    blob.definition_id, blob.generation_id, blob.column_id
                )));
            }
            pages.push(SegmentInlineIndexPage {
                column_id: blob.column_id,
                kind,
                bytes: &blob.bytes,
            });
        }
    }
    Ok(pages)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowsetWriterSavepoint {
    completed_segments_len: usize,
    total_rows: u64,
    total_data_size: u64,
    total_index_size: u64,
    current_segment_id: u32,
    had_active_segment: bool,
    current_segment_chunks_len: usize,
    active_sink_savepoints: Vec<SegmentSinkSavepoint>,
}

/// RowsetWriter coordinates writing data across multiple Segments
///
/// ## Lifecycle
///
/// 1. Create with `RowsetWriter::create(context)`
/// 2. Add data with `add_chunk()`
/// 3. Optionally flush segments with `flush_segment()`
/// 4. Finalize with `build()` to get the Rowset
///
/// ## Automatic Segment Management
///
/// The writer automatically creates new Segments when:
/// - Current segment exceeds size threshold
/// - Current segment exceeds row count threshold
///
pub struct RowsetWriter {
    /// Writer context
    context: RowsetWriterContext,
    /// Rowset metadata (updated during writing)
    rowset_meta: RowsetMeta,
    /// Current segment writer (None if no active segment)
    current_segment_writer: Option<SegmentWriter>,
    /// Admission leases for SegmentWriter-managed HNSW inline build on the current segment.
    current_hnsw_admission_leases: Vec<AdmissionLease>,
    /// Per-column physical HNSW contract admitted for the current segment.
    current_hnsw_indexes: BTreeMap<ColumnId, HnswColumnBuildOptions>,
    /// Maximum rows allowed in the current HNSW-admitted segment.
    current_hnsw_inline_row_limit: Option<u64>,
    /// Search sinks consuming the current segment writer stream.
    current_search_sinks: Vec<ActiveSegmentSearchSink>,
    /// Writer-side input chunks for the current unfinalized segment.
    current_segment_chunks: Vec<Vec<ColumnData>>,
    /// Completed segments
    completed_segments: Vec<Segment>,
    /// Search artifact build results aligned with completed segments.
    completed_search_artifacts: Vec<Vec<InlineArtifactBuildResult>>,
    /// Statistics for completed segments
    segment_stats: Vec<SegmentStats>,
    /// Total rows written across all segments
    total_rows: u64,
    /// Total data size across all segments
    total_data_size: u64,
    /// Total index size across all segments
    total_index_size: u64,
    /// Current segment ID
    current_segment_id: u32,
    /// Whether the writer has been finalized
    finalized: bool,
}

impl RowsetWriter {
    /// Create a new RowsetWriter
    ///
    /// # Arguments
    /// * `context` - Writer context with configuration
    ///
    /// # Returns
    /// A new RowsetWriter instance
    pub fn create(context: RowsetWriterContext) -> Result<Self> {
        // Create rowset directory
        std::fs::create_dir_all(&context.rowset_path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to create rowset directory {:?}: {}",
                context.rowset_path, e
            ))
        })?;

        // Initialize rowset metadata
        let rowset_meta = RowsetMeta::new(context.rowset_id, context.tablet_id, context.version);

        Ok(Self {
            context,
            rowset_meta,
            current_segment_writer: None,
            current_hnsw_admission_leases: Vec::new(),
            current_hnsw_indexes: BTreeMap::new(),
            current_hnsw_inline_row_limit: None,
            current_search_sinks: Vec::new(),
            current_segment_chunks: Vec::new(),
            completed_segments: Vec::new(),
            completed_search_artifacts: Vec::new(),
            segment_stats: Vec::new(),
            total_rows: 0,
            total_data_size: 0,
            total_index_size: 0,
            current_segment_id: 0,
            finalized: false,
        })
    }

    /// Get the rowset ID
    pub fn rowset_id(&self) -> RowsetId {
        self.context.rowset_id
    }

    /// Get the tablet ID
    pub fn tablet_id(&self) -> u64 {
        self.context.tablet_id
    }

    /// Get the version
    pub fn version(&self) -> Version {
        self.context.version
    }

    /// Get total rows written
    pub fn num_rows(&self) -> u64 {
        self.total_rows + self.current_segment_rows()
    }

    /// Get number of segments (completed + current)
    pub fn num_segments(&self) -> u32 {
        let current = if self.current_segment_writer.is_some() {
            1
        } else {
            0
        };
        self.completed_segments.len() as u32 + current
    }

    /// Get total data size
    pub fn total_data_size(&self) -> u64 {
        self.total_data_size
    }

    /// Get total index size
    pub fn total_index_size(&self) -> u64 {
        self.total_index_size
    }

    /// Input bytes retained to support statement savepoint rollback for the
    /// active segment. Completed segments do not retain their source batches.
    pub fn retained_input_bytes(&self) -> u64 {
        self.current_segment_chunks
            .iter()
            .flatten()
            .fold(0_u64, |bytes, column| {
                bytes
                    .saturating_add(column.data.len() as u64)
                    .saturating_add(
                        column
                            .null_flags
                            .as_ref()
                            .map_or(0_u64, |nulls| nulls.len() as u64),
                    )
            })
    }

    /// Check if the writer has been finalized
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    /// Get rows in current segment
    fn current_segment_rows(&self) -> u64 {
        self.current_segment_writer
            .as_ref()
            .map(|w| w.num_rows())
            .unwrap_or(0)
    }

    fn effective_segment_row_limit(&self) -> u64 {
        self.context
            .segment_row_limit
            .effective(self.current_hnsw_inline_row_limit)
    }

    /// Check if current segment should be flushed
    fn should_flush_segment(&self) -> bool {
        if let Some(writer) = &self.current_segment_writer {
            let rows = writer.num_rows();
            // Check row count threshold
            if rows >= self.effective_segment_row_limit() {
                return true;
            }
            // Note: Size threshold check would require tracking estimated size
            // For now, we rely on row count
        }
        false
    }

    fn create_raw_segment_writer(
        &self,
        segment_id: u32,
        hnsw_indexes: BTreeMap<ColumnId, HnswColumnBuildOptions>,
    ) -> Result<SegmentWriter> {
        let segment_path = self.segment_path(segment_id);
        let rowset_gen = self.rowset_meta.rowset_gen();

        let mut options = SegmentWriterOptions::new(segment_id)
            .with_rowset_context(self.context.tablet_id, self.context.rowset_id, rowset_gen)
            .with_compression(self.context.compression)
            .with_short_key_index(self.context.build_short_key_index)
            .with_num_short_key_columns(self.context.num_short_key_columns);

        options = options.with_build_hnsw_indexes(self.context.build_hnsw_indexes);
        for (column_id, hnsw) in hnsw_indexes {
            options = options.with_hnsw_build_options(column_id, hnsw);
        }

        let mut writer = SegmentWriter::create(self.context.schema.clone(), segment_path, options)?;
        if let Some(column_ids) = &self.context.write_column_ids {
            writer.init_vertical(column_ids.clone(), true)?;
        }
        Ok(writer)
    }

    /// Create a new segment writer
    fn create_segment_writer(&mut self, row_count_estimate: u64) -> Result<()> {
        let segment_id = self.current_segment_id;
        let admitted = self.admitted_inline_sinks(row_count_estimate)?;
        let (admitted_search_sinks, hnsw_admission_leases, hnsw_inline_row_limit, hnsw_indexes) =
            split_hnsw_segment_admissions(admitted)?;
        let writer = self.create_raw_segment_writer(segment_id, hnsw_indexes.clone())?;
        let search_sinks = match self.open_segment_search_sinks(segment_id, admitted_search_sinks) {
            Ok(search_sinks) => search_sinks,
            Err(err) => {
                drop(writer);
                drop(hnsw_admission_leases);
                let _ = self.cleanup_search_staging_dir();
                self.remove_segment_outputs_from(segment_id)?;
                return Err(err);
            }
        };
        self.current_segment_writer = Some(writer);
        self.current_hnsw_admission_leases = hnsw_admission_leases;
        self.current_hnsw_indexes = hnsw_indexes;
        self.current_hnsw_inline_row_limit = hnsw_inline_row_limit;
        self.current_search_sinks = search_sinks;
        self.current_segment_chunks.clear();

        Ok(())
    }

    fn open_segment_search_sinks(
        &self,
        segment_id: u32,
        admitted: Vec<AdmittedInlineSink<'_>>,
    ) -> Result<Vec<ActiveSegmentSearchSink>> {
        if admitted.is_empty() {
            return Ok(Vec::new());
        }

        let staging_dir = self.search_staging_dir();
        let mut sinks = Vec::new();
        for admitted_sink in admitted {
            let entry = admitted_sink.entry;
            let flush_mode = entry.flush_mode();
            let admission_lease = admitted_sink
                .grant
                .map(|grant| AdmissionLease::new(admitted_sink.admission.unwrap(), grant));
            let ctx = SegmentFlushCtx {
                rowset_id: self.context.rowset_id,
                segment_id,
                definition: &entry.definition,
                generation_id: entry.generation_id,
                flush_mode,
                admission: admitted_sink.grant,
                staging_dir: &staging_dir,
                column_schema: self.context.schema.columns(),
            };
            match entry.builder.open_sink(&ctx) {
                Ok(sink) => sinks.push(ActiveSegmentSearchSink::new(
                    entry.definition.definition_id,
                    entry.definition.kind,
                    sink,
                    admission_lease,
                )),
                Err(err) => {
                    storage_metrics()
                        .record_search_inline_build_failure(entry.definition.kind, "open_error");
                    if matches!(flush_mode, FlushSearchMode::InlineIfAdmitted) {
                        continue;
                    }
                    abort_search_sinks(sinks);
                    let _ = self.cleanup_search_staging_dir();
                    return Err(err);
                }
            }
        }
        Ok(sinks)
    }

    fn admitted_inline_sinks(
        &self,
        row_count_estimate: u64,
    ) -> Result<Vec<AdmittedInlineSink<'_>>> {
        let entries = self
            .context
            .search_inline_builders
            .entries()
            .iter()
            .filter(|entry| {
                !matches!(entry.flush_mode(), FlushSearchMode::TailOnly)
                    && self.search_entry_touches_written_columns(entry)
            })
            .collect::<Vec<_>>();
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let Some(admission) = self.context.search_inline_builders.admission() else {
            let mut admitted = Vec::with_capacity(entries.len());
            for entry in entries {
                if entry.definition.kind == SearchIndexKind::Hnsw {
                    storage_metrics()
                        .record_search_inline_build_failure(entry.definition.kind, "no_admission");
                    if matches!(entry.flush_mode(), FlushSearchMode::InlineRequired) {
                        return Err(paro_error::invalid_input(format!(
                            "required HNSW inline build for definition {} requires admission",
                            entry.definition.definition_id
                        )));
                    }
                    continue;
                }
                admitted.push(AdmittedInlineSink {
                    entry,
                    grant: None,
                    admission: None,
                    hnsw_inline: None,
                });
            }
            return Ok(admitted);
        };

        let requests = entries
            .iter()
            .map(|entry| -> Result<InlineAdmissionRequest> {
                let initial_hnsw = hnsw_inline_build_estimate(
                    entry,
                    row_count_estimate,
                    self.context.schema.columns(),
                )?;
                let admitted_rows = initial_hnsw.map_or(row_count_estimate, |estimate| {
                    // RowsetWriter seals only between chunks; it never splits
                    // one logical input batch. Admission must therefore cover
                    // at least the current indivisible chunk even when the
                    // derived segment target is smaller.
                    self.context
                        .segment_row_limit
                        .effective(Some(estimate.max_segment_vector_count()))
                        .max(row_count_estimate)
                });
                let hnsw_inline = if initial_hnsw.is_some() {
                    hnsw_inline_build_estimate(entry, admitted_rows, self.context.schema.columns())?
                } else {
                    None
                };
                let mut estimated_cost = estimate_inline_build_cost(entry, admitted_rows);
                if let Some(estimate) = hnsw_inline {
                    estimated_cost.memory_peak_bytes = estimate.estimated_build_peak_memory_bytes;
                }
                Ok(InlineAdmissionRequest {
                    table_id: entry.definition.table_id,
                    definition_id: entry.definition.definition_id,
                    provider: entry.definition.kind,
                    flush_mode: entry.flush_mode(),
                    estimated_cost,
                    row_count: admitted_rows.max(1),
                    hnsw_inline,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for attempt in 0..REQUIRED_INLINE_ADMISSION_MAX_ATTEMPTS {
            let decisions = admission.request_inline_batch(&requests)?;
            if decisions.len() != entries.len() {
                return Err(paro_error::internal(format!(
                    "search inline admission returned {} decisions for {} requests",
                    decisions.len(),
                    entries.len()
                )));
            }

            let mut admitted = Vec::with_capacity(entries.len());
            let mut required_wait = None;
            let mut required_reject = None;
            for ((entry, request), decision) in
                entries.iter().copied().zip(requests.iter()).zip(decisions)
            {
                match decision {
                    AdmissionDecision::Proceed(grant) => admitted.push(AdmittedInlineSink {
                        entry,
                        grant: Some(grant),
                        admission: Some(Arc::clone(admission)),
                        hnsw_inline: request.hnsw_inline,
                    }),
                    AdmissionDecision::Wait { deadline, reason } => {
                        storage_metrics().record_search_inline_build_failure(
                            entry.definition.kind,
                            "admission_wait",
                        );
                        if matches!(entry.flush_mode(), FlushSearchMode::InlineRequired) {
                            required_wait =
                                Some((entry.definition.definition_id, deadline, reason));
                        }
                    }
                    AdmissionDecision::Reject { reason } => {
                        storage_metrics().record_search_inline_build_failure(
                            entry.definition.kind,
                            "admission_reject",
                        );
                        if matches!(entry.flush_mode(), FlushSearchMode::InlineRequired) {
                            required_reject = Some((entry.definition.definition_id, reason));
                        }
                    }
                }
            }

            if let Some((definition_id, reason)) = required_reject {
                release_admitted_inline_sinks(admitted);
                return Err(paro_error::invalid_input(format!(
                    "required search inline build for definition {} was rejected by admission: {:?}",
                    definition_id, reason
                )));
            }
            if let Some((definition_id, deadline, reason)) = required_wait {
                release_admitted_inline_sinks(admitted);
                if attempt + 1 == REQUIRED_INLINE_ADMISSION_MAX_ATTEMPTS {
                    return Err(paro_error::invalid_input(format!(
                        "required search inline build for definition {} still waiting for admission after {} attempts: {:?}",
                        definition_id, REQUIRED_INLINE_ADMISSION_MAX_ATTEMPTS, reason
                    )));
                }
                wait_until(deadline);
                continue;
            }
            return Ok(admitted);
        }

        Err(paro_error::internal(
            "required search inline admission retry loop exhausted unexpectedly",
        ))
    }

    fn search_entry_touches_written_columns(&self, entry: &SearchInlineBuilderEntry) -> bool {
        let Some(write_column_ids) = &self.context.write_column_ids else {
            return true;
        };
        entry
            .definition
            .column_ids
            .iter()
            .any(|column_id| write_column_ids.contains(column_id))
    }

    /// Get segment file path
    fn segment_path(&self, segment_id: u32) -> PathBuf {
        self.context.rowset_path.join(format!("{}.dat", segment_id))
    }

    /// Add a chunk of data
    ///
    /// The data is written to the current segment. If the segment exceeds
    /// thresholds, it is automatically flushed and a new segment is created.
    ///
    /// # Arguments
    /// * `columns` - Vector of column data, one per schema column
    ///
    /// # Returns
    /// Number of rows added
    pub fn add_chunk(&mut self, columns: &[ColumnData]) -> Result<u64> {
        if self.finalized {
            return Err(paro_error::internal("RowsetWriter already finalized"));
        }

        let incoming_rows = chunk_row_count(columns);
        // Create segment writer if needed
        if self.current_segment_writer.is_none() {
            self.create_segment_writer(incoming_rows)?;
        } else {
            self.flush_hnsw_segment_before_limit(incoming_rows)?;
        }

        let writer = self.current_segment_writer.as_mut().unwrap();
        let base_row_id = u32::try_from(writer.num_rows()).map_err(|_| {
            paro_error::invalid_input("segment row id exceeds inline search sink row id range")
        })?;
        let rows_added = writer.append_chunk(columns)?;
        if let Err(err) = self.append_chunk_to_search_sinks(base_row_id, columns) {
            self.abort_current_segment_after_search_error()?;
            return Err(err);
        }
        self.current_segment_chunks.push(columns.to_vec());

        // Check if we should flush
        if self.should_flush_segment() {
            self.flush_segment()?;
        }

        Ok(rows_added)
    }

    fn flush_hnsw_segment_before_limit(&mut self, incoming_rows: u64) -> Result<()> {
        let limit = self.effective_segment_row_limit();
        let Some(writer) = self.current_segment_writer.as_ref() else {
            return Ok(());
        };
        let current_rows = writer.num_rows();
        if current_rows == 0 || incoming_rows == 0 {
            return Ok(());
        }
        if current_rows.saturating_add(incoming_rows) <= limit {
            return Ok(());
        }
        self.flush_segment()?;
        self.create_segment_writer(incoming_rows)
    }

    fn append_chunk_to_search_sinks(
        &mut self,
        base_row_id: u32,
        columns: &[ColumnData],
    ) -> Result<()> {
        if self.current_search_sinks.is_empty() {
            return Ok(());
        }

        let input = SegmentChunkInput {
            base_row_id,
            columns,
            column_ids: self.context.write_column_ids.as_deref(),
        };
        for search_sink in &mut self.current_search_sinks {
            search_sink.append_chunk(input)?;
        }
        Ok(())
    }

    fn abort_current_segment_after_search_error(&mut self) -> Result<()> {
        let sinks = std::mem::take(&mut self.current_search_sinks);
        abort_search_sinks(sinks);
        self.clear_current_hnsw_admission();
        self.current_segment_writer.take();
        self.current_segment_chunks.clear();
        self.cleanup_search_staging_dir()?;
        self.remove_segment_outputs_from(self.current_segment_id)
    }

    fn clear_current_hnsw_admission(&mut self) {
        self.current_hnsw_admission_leases.clear();
        self.current_hnsw_indexes.clear();
        self.current_hnsw_inline_row_limit = None;
    }

    /// Flush the current segment
    ///
    /// Finalizes the current segment and prepares for a new one.
    /// This is called automatically when thresholds are exceeded,
    /// but can also be called manually.
    pub fn flush_segment(&mut self) -> Result<()> {
        if self.finalized {
            return Err(paro_error::internal("RowsetWriter already finalized"));
        }

        if let Some(writer) = self.current_segment_writer.take() {
            let num_rows = writer.num_rows();
            let search_sinks = std::mem::take(&mut self.current_search_sinks);

            // Skip empty segments
            if num_rows == 0 {
                abort_search_sinks(search_sinks);
                self.clear_current_hnsw_admission();
                return Ok(());
            }

            let search_results = match finish_search_sinks(search_sinks) {
                Ok(search_results) => search_results,
                Err(err) => {
                    self.clear_current_hnsw_admission();
                    self.cleanup_search_staging_dir()?;
                    return Err(err);
                }
            };
            let inline_index_pages = match inline_index_pages_from_search_results(&search_results) {
                Ok(inline_index_pages) => inline_index_pages,
                Err(err) => {
                    self.clear_current_hnsw_admission();
                    self.cleanup_search_staging_dir()?;
                    return Err(err);
                }
            };

            // Finalize the segment
            let segment = match writer.finalize_with_inline_index_pages(&inline_index_pages) {
                Ok(segment) => segment,
                Err(err) => {
                    self.clear_current_hnsw_admission();
                    self.cleanup_search_staging_dir()?;
                    return Err(err);
                }
            };
            self.clear_current_hnsw_admission();
            self.cleanup_search_staging_dir()?;

            // Collect statistics
            let stats = SegmentStats {
                num_rows,
                data_size: segment.data_size(),
                index_size: segment.index_size(),
            };

            self.total_rows += stats.num_rows;
            self.total_data_size += stats.data_size;
            self.total_index_size += stats.index_size;

            self.completed_segments.push(segment);
            self.completed_search_artifacts.push(search_results);
            self.segment_stats.push(stats);
            self.current_segment_chunks.clear();

            // Increment segment ID for next segment
            self.current_segment_id += 1;
        }

        Ok(())
    }

    pub fn mark_savepoint(&mut self) -> Result<RowsetWriterSavepoint> {
        let active_sink_savepoints = self
            .current_search_sinks
            .iter_mut()
            .map(ActiveSegmentSearchSink::mark_savepoint)
            .collect::<Result<Vec<_>>>()?;
        Ok(RowsetWriterSavepoint {
            completed_segments_len: self.completed_segments.len(),
            total_rows: self.total_rows,
            total_data_size: self.total_data_size,
            total_index_size: self.total_index_size,
            current_segment_id: self.current_segment_id,
            had_active_segment: self.current_segment_writer.is_some(),
            current_segment_chunks_len: self.current_segment_chunks.len(),
            active_sink_savepoints,
        })
    }

    pub fn rollback_to_savepoint(&mut self, mark: &RowsetWriterSavepoint) -> Result<()> {
        if self.finalized {
            return Err(paro_error::internal("RowsetWriter already finalized"));
        }

        let same_active_segment = self.current_segment_writer.is_some()
            && mark.had_active_segment
            && mark.current_segment_id == self.current_segment_id
            && mark.completed_segments_len == self.completed_segments.len();

        if same_active_segment {
            self.rollback_active_segment_to_savepoint(mark)?;
        } else {
            self.current_segment_writer.take();
            abort_search_sinks(std::mem::take(&mut self.current_search_sinks));
            self.clear_current_hnsw_admission();
            self.current_segment_chunks.clear();
            self.cleanup_search_staging_dir()?;
            self.remove_segment_outputs_from(mark.current_segment_id)?;
            self.completed_segments
                .truncate(mark.completed_segments_len);
            self.completed_search_artifacts
                .truncate(mark.completed_segments_len);
            self.segment_stats.truncate(mark.completed_segments_len);
            self.current_segment_id = mark.current_segment_id;
        }
        self.total_rows = mark.total_rows;
        self.total_data_size = mark.total_data_size;
        self.total_index_size = mark.total_index_size;
        Ok(())
    }

    fn rollback_active_segment_to_savepoint(&mut self, mark: &RowsetWriterSavepoint) -> Result<()> {
        if mark.current_segment_chunks_len > self.current_segment_chunks.len() {
            return Err(paro_error::invalid_input(
                "rowset writer savepoint is ahead of current segment state",
            ));
        }
        if mark.active_sink_savepoints.len() != self.current_search_sinks.len() {
            return Err(paro_error::invalid_input(
                "rowset writer savepoint does not match active search sinks",
            ));
        }

        for (sink, savepoint) in self
            .current_search_sinks
            .iter_mut()
            .zip(&mark.active_sink_savepoints)
        {
            if let Err(err) = sink.rollback_to_savepoint(savepoint) {
                self.abort_current_segment_after_search_error()?;
                return Err(err);
            }
        }
        self.cleanup_search_staging_dir()?;

        let retained_chunks = self.current_segment_chunks[..mark.current_segment_chunks_len]
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        self.current_segment_writer.take();
        self.remove_segment_outputs_from(mark.current_segment_id)?;
        let mut writer = self.create_raw_segment_writer(
            mark.current_segment_id,
            self.current_hnsw_indexes.clone(),
        )?;
        for columns in &retained_chunks {
            writer.append_chunk(columns)?;
        }
        self.current_segment_writer = Some(writer);
        self.current_segment_chunks = retained_chunks;
        Ok(())
    }

    /// Build the final Rowset
    ///
    /// Finalizes any remaining segment and creates the Rowset with all segments.
    ///
    /// # Returns
    /// The completed Rowset
    pub fn build(mut self) -> Result<Rowset> {
        if self.finalized {
            return Err(paro_error::internal("RowsetWriter already finalized"));
        }

        // Flush any remaining segment
        self.flush_segment()?;
        self.cleanup_search_staging_dir()?;
        self.finalized = true;

        // Update rowset metadata
        self.rowset_meta.set_num_rows(self.total_rows);
        self.rowset_meta
            .set_num_segments(self.completed_segments.len() as u32);
        self.rowset_meta
            .set_disk_sizes(self.total_data_size, self.total_index_size);
        self.rowset_meta.set_rowset_state(RowsetState::Committed);
        self.rowset_meta
            .set_rowset_path(self.context.rowset_path.to_string_lossy().to_string());

        // Determine segments overlap
        // For now, assume non-overlapping if we have proper ordering
        let overlap = if self.completed_segments.len() <= 1 {
            SegmentsOverlap::NonOverlapping
        } else {
            SegmentsOverlap::Unknown
        };
        self.rowset_meta.set_segments_overlap(overlap);

        // Create the Rowset
        let rowset = Rowset::create_with_segments(
            self.context.schema.clone(),
            self.rowset_meta,
            &self.context.rowset_path,
            self.completed_segments.into_iter().map(Arc::new).collect(),
        )?;

        Ok(rowset)
    }

    /// Build and return a shared pointer to the Rowset
    pub fn build_shared(self) -> Result<RowsetSharedPtr> {
        Ok(Arc::new(self.build()?))
    }

    /// Get the rowset path
    pub fn rowset_path(&self) -> &Path {
        &self.context.rowset_path
    }

    fn search_staging_dir(&self) -> PathBuf {
        self.context.rowset_path.join("_search_staging")
    }

    fn cleanup_search_staging_dir(&self) -> Result<()> {
        let staging_dir = self.search_staging_dir();
        if !staging_dir.exists() {
            return Ok(());
        }
        fs::remove_dir_all(&staging_dir).map_err(|err| {
            paro_error::io_error(format!(
                "remove search staging dir {}: {}",
                staging_dir.display(),
                err
            ))
        })
    }

    fn remove_segment_outputs_from(&self, first_segment_id: u32) -> Result<()> {
        if !self.context.rowset_path.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(&self.context.rowset_path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to read rowset directory {:?}: {}",
                self.context.rowset_path, e
            ))
        })? {
            let entry = entry
                .map_err(|e| paro_error::io_error(format!("Failed to read rowset entry: {}", e)))?;
            let path = entry.path();
            let Some(segment_id) = Self::segment_artifact_id(&path) else {
                continue;
            };
            if segment_id < first_segment_id {
                continue;
            }

            let file_type = entry.file_type().map_err(|e| {
                paro_error::io_error(format!(
                    "Failed to inspect rowset artifact {:?}: {}",
                    path, e
                ))
            })?;
            if file_type.is_dir() {
                fs::remove_dir_all(&path).map_err(|e| {
                    paro_error::io_error(format!(
                        "Failed to remove rowset artifact directory {:?}: {}",
                        path, e
                    ))
                })?;
            } else {
                fs::remove_file(&path).map_err(|e| {
                    paro_error::io_error(format!(
                        "Failed to remove rowset artifact {:?}: {}",
                        path, e
                    ))
                })?;
            }
        }

        Ok(())
    }

    fn segment_artifact_id(path: &Path) -> Option<u32> {
        let file_name = path.file_name()?.to_str()?;
        let prefix = file_name.split('.').next()?;
        prefix.parse().ok()
    }
}

/// Builder for creating RowsetWriter with fluent API
pub struct RowsetWriterBuilder {
    context: RowsetWriterContext,
}

impl RowsetWriterBuilder {
    /// Create a new builder
    pub fn new(
        schema: TabletSchemaRef,
        tablet_id: u64,
        version: Version,
        rowset_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            context: RowsetWriterContext::new(schema, tablet_id, version, rowset_path),
        }
    }

    /// Set rowset ID
    pub fn rowset_id(mut self, id: RowsetId) -> Self {
        self.context.rowset_id = id;
        self
    }

    /// Set segment size threshold
    pub fn segment_size_threshold(mut self, threshold: u64) -> Self {
        self.context.segment_size_threshold = threshold;
        self
    }

    /// Set maximum rows per segment
    pub fn max_rows_per_segment(mut self, max_rows: u64) -> Self {
        self.context.segment_row_limit = SegmentRowLimit::Explicit { max_rows };
        self
    }

    /// Set compression type
    pub fn compression(mut self, compression: super::page::CompressionType) -> Self {
        self.context.compression = compression;
        self
    }

    /// Set whether to build short key index
    pub fn short_key_index(mut self, build: bool) -> Self {
        self.context.build_short_key_index = build;
        self
    }

    /// Set number of short key columns
    pub fn num_short_key_columns(mut self, num: usize) -> Self {
        self.context.num_short_key_columns = num;
        self
    }

    /// Set whether to build HNSW index pages while writing segments.
    pub fn build_hnsw_indexes(mut self, build: bool) -> Self {
        self.context.build_hnsw_indexes = build;
        self
    }

    pub fn search_inline_builders(mut self, builders: SearchInlineBuilderSet) -> Self {
        self.context.search_inline_builders = builders;
        self
    }

    pub fn write_column_ids(mut self, column_ids: Vec<ColumnId>) -> Self {
        self.context.write_column_ids = Some(column_ids);
        self
    }

    /// Build the RowsetWriter
    pub fn build(self) -> Result<RowsetWriter> {
        RowsetWriter::create(self.context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowset::page::CompressionType;
    use crate::rowset::SparseVector;
    use crate::search::maintenance::{InlineSearchAdmission, MaintenanceAdmissionPolicy};
    use crate::search::{
        AdmissionDecision, AdmissionGrant, AdmissionRejectReason, AdmissionWaitReason,
        FlushSearchMode, FullTextInlineArtifactBuilder, InlineAdmissionRequest,
        InlineArtifactBuildResult, InlineArtifactBuilder, MaintenanceCost, SearchAdmission,
        SearchFreshnessPolicy, SearchIndexDefinition, SearchIndexKind, SearchInlineBuilderEntry,
        SearchInlineBuilderSet, SegmentChunkInput, SegmentChunkSink, SegmentFlushCtx,
        SegmentSinkSavepoint, SparseInlineArtifactBuilder, HNSW_PROVIDER_CONFIG_VERSION,
    };
    use crate::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};
    use paro_common::types::LogicalType;
    use serde_json::json;
    use std::sync::Mutex;
    use std::time::Instant;
    use tempfile::TempDir;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum SearchSinkEvent {
        Open {
            rowset_id: RowsetId,
            segment_id: u32,
            flush_mode: FlushSearchMode,
            grant_id: Option<u64>,
        },
        Append {
            base_row_id: u32,
            rows: u32,
            column_ids: Option<Vec<ColumnId>>,
        },
        Mark {
            rows_seen: u64,
        },
        Rollback {
            rows_seen: u64,
        },
        Finish,
        Abort,
    }

    #[derive(Clone)]
    struct RecordingInlineBuilder {
        events: Arc<Mutex<Vec<SearchSinkEvent>>>,
    }

    impl InlineArtifactBuilder for RecordingInlineBuilder {
        fn open_sink(&self, ctx: &SegmentFlushCtx<'_>) -> Result<Box<dyn SegmentChunkSink>> {
            self.events.lock().unwrap().push(SearchSinkEvent::Open {
                rowset_id: ctx.rowset_id,
                segment_id: ctx.segment_id,
                flush_mode: ctx.flush_mode,
                grant_id: ctx.admission.map(|grant| grant.grant_id),
            });
            Ok(Box::new(RecordingSegmentSink {
                events: Arc::clone(&self.events),
                rows_seen: 0,
            }))
        }
    }

    struct RecordingSegmentSink {
        events: Arc<Mutex<Vec<SearchSinkEvent>>>,
        rows_seen: u64,
    }

    impl SegmentChunkSink for RecordingSegmentSink {
        fn append_chunk(&mut self, input: SegmentChunkInput<'_>) -> Result<()> {
            let rows = input
                .columns
                .first()
                .map(|column| column.num_values)
                .unwrap_or(0);
            self.rows_seen += u64::from(rows);
            self.events.lock().unwrap().push(SearchSinkEvent::Append {
                base_row_id: input.base_row_id,
                rows,
                column_ids: input.column_ids.map(<[ColumnId]>::to_vec),
            });
            Ok(())
        }

        fn mark_savepoint(&mut self) -> Result<SegmentSinkSavepoint> {
            self.events.lock().unwrap().push(SearchSinkEvent::Mark {
                rows_seen: self.rows_seen,
            });
            Ok(SegmentSinkSavepoint {
                rows_seen: self.rows_seen,
                bytes_buffered: 0,
                entries_seen: self.rows_seen,
                state_id: self.rows_seen,
            })
        }

        fn rollback_to_savepoint(&mut self, savepoint: &SegmentSinkSavepoint) -> Result<()> {
            self.rows_seen = savepoint.rows_seen;
            self.events.lock().unwrap().push(SearchSinkEvent::Rollback {
                rows_seen: self.rows_seen,
            });
            Ok(())
        }

        fn finish(self: Box<Self>) -> Result<InlineArtifactBuildResult> {
            self.events.lock().unwrap().push(SearchSinkEvent::Finish);
            Ok(InlineArtifactBuildResult {
                blobs: Vec::new(),
                stats_delta: None,
            })
        }

        fn abort(self: Box<Self>) -> Result<()> {
            self.events.lock().unwrap().push(SearchSinkEvent::Abort);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct StagingInlineBuilder {
        fail_finish: bool,
    }

    impl InlineArtifactBuilder for StagingInlineBuilder {
        fn open_sink(&self, ctx: &SegmentFlushCtx<'_>) -> Result<Box<dyn SegmentChunkSink>> {
            fs::create_dir_all(ctx.staging_dir).map_err(paro_error::io)?;
            let path = ctx.staging_dir.join(format!(
                "d{}_s{}.tmp",
                ctx.definition.definition_id, ctx.segment_id
            ));
            fs::write(&path, b"staged search payload").map_err(paro_error::io)?;
            Ok(Box::new(StagingSegmentSink {
                path,
                fail_finish: self.fail_finish,
            }))
        }
    }

    struct StagingSegmentSink {
        path: PathBuf,
        fail_finish: bool,
    }

    impl SegmentChunkSink for StagingSegmentSink {
        fn append_chunk(&mut self, _input: SegmentChunkInput<'_>) -> Result<()> {
            if let Some(parent) = self.path.parent() {
                fs::create_dir_all(parent).map_err(paro_error::io)?;
            }
            fs::write(&self.path, b"staged search payload with rows").map_err(paro_error::io)
        }

        fn mark_savepoint(&mut self) -> Result<SegmentSinkSavepoint> {
            Ok(SegmentSinkSavepoint {
                rows_seen: 0,
                bytes_buffered: 0,
                entries_seen: 0,
                state_id: 0,
            })
        }

        fn rollback_to_savepoint(&mut self, _savepoint: &SegmentSinkSavepoint) -> Result<()> {
            Ok(())
        }

        fn finish(self: Box<Self>) -> Result<InlineArtifactBuildResult> {
            if self.fail_finish {
                return Err(paro_error::internal("staging finish failure"));
            }
            Ok(InlineArtifactBuildResult {
                blobs: Vec::new(),
                stats_delta: None,
            })
        }

        fn abort(self: Box<Self>) -> Result<()> {
            Ok(())
        }
    }

    fn create_test_schema() -> TabletSchemaRef {
        let columns = vec![
            TabletColumn::key(0, "id", LogicalType::BigInt),
            TabletColumn::new(1, "name", LogicalType::Varchar),
            TabletColumn::new(2, "value", LogicalType::Integer),
        ];
        Arc::new(TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap())
    }

    fn create_simple_schema() -> TabletSchemaRef {
        let columns = vec![
            TabletColumn::new(0, "col0", LogicalType::Integer),
            TabletColumn::new(1, "col1", LogicalType::Integer),
        ];
        Arc::new(TabletSchema::new(1, columns, KeysType::DuplicateKeys).unwrap())
    }

    fn create_text_search_schema() -> TabletSchemaRef {
        let columns = vec![
            TabletColumn::new(0, "id", LogicalType::Integer),
            TabletColumn::new(1, "body", LogicalType::Varchar),
        ];
        Arc::new(TabletSchema::new(1, columns, KeysType::DuplicateKeys).unwrap())
    }

    fn create_sparse_search_schema() -> TabletSchemaRef {
        let columns = vec![
            TabletColumn::new(0, "id", LogicalType::Integer),
            TabletColumn::new(1, "sparse_vec", LogicalType::Blob),
        ];
        Arc::new(TabletSchema::new(1, columns, KeysType::DuplicateKeys).unwrap())
    }

    fn create_hnsw_search_schema() -> TabletSchemaRef {
        let columns = vec![
            TabletColumn::new(0, "id", LogicalType::Integer),
            TabletColumn::new(
                1,
                "vec",
                LogicalType::Array(Box::new(LogicalType::Float), 2),
            )
            .with_hnsw_index(8, 64, 0),
        ];
        Arc::new(TabletSchema::new(1, columns, KeysType::DuplicateKeys).unwrap())
    }

    fn int_columns(start: i32, rows: u32) -> Vec<ColumnData> {
        let end = start + rows as i32;
        let col0_data: Vec<u8> = (start..end).flat_map(|v| v.to_le_bytes()).collect();
        let col1_data: Vec<u8> = (start + 100..end + 100)
            .flat_map(|v| v.to_le_bytes())
            .collect();
        vec![
            ColumnData::new(col0_data, rows),
            ColumnData::new(col1_data, rows),
        ]
    }

    fn text_columns(values: &[&str]) -> Vec<ColumnData> {
        let rows = values.len() as u32;
        let id_data: Vec<u8> = (0..rows as i32).flat_map(|v| v.to_le_bytes()).collect();
        vec![
            ColumnData::new(id_data, rows),
            ColumnData::new(encode_varlen(values), rows),
        ]
    }

    fn sparse_columns(values: &[SparseVector]) -> Vec<ColumnData> {
        let rows = values.len() as u32;
        let id_data: Vec<u8> = (0..rows as i32).flat_map(|v| v.to_le_bytes()).collect();
        let row_images = values
            .iter()
            .map(|value| value.to_row_image_v1().expect("sparse row image"))
            .collect::<Vec<_>>();
        let row_image_refs = row_images.iter().map(Vec::as_slice).collect::<Vec<&[u8]>>();
        vec![
            ColumnData::new(id_data, rows),
            ColumnData::new(encode_varlen_bytes(&row_image_refs), rows),
        ]
    }

    fn hnsw_columns(values: &[[f32; 2]]) -> Vec<ColumnData> {
        let rows = values.len() as u32;
        let id_data: Vec<u8> = (0..rows as i32).flat_map(|v| v.to_le_bytes()).collect();
        let vector_data = values
            .iter()
            .flat_map(|vector| vector.iter().flat_map(|value| value.to_le_bytes()))
            .collect::<Vec<_>>();
        vec![
            ColumnData::new(id_data, rows),
            ColumnData::new(vector_data, rows),
        ]
    }

    fn encode_varlen(values: &[&str]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        bytes
    }

    fn encode_varlen_bytes(values: &[&[u8]]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value);
        }
        bytes
    }

    fn recording_builder_set(events: Arc<Mutex<Vec<SearchSinkEvent>>>) -> SearchInlineBuilderSet {
        recording_builder_set_with_policy(events, SearchFreshnessPolicy::Required)
    }

    fn recording_builder_set_with_policy(
        events: Arc<Mutex<Vec<SearchSinkEvent>>>,
        freshness_policy: SearchFreshnessPolicy,
    ) -> SearchInlineBuilderSet {
        recording_builder_set_with_policy_and_admission(events, freshness_policy, None)
    }

    fn recording_builder_set_with_policy_and_admission(
        events: Arc<Mutex<Vec<SearchSinkEvent>>>,
        freshness_policy: SearchFreshnessPolicy,
        admission: Option<Arc<dyn SearchAdmission>>,
    ) -> SearchInlineBuilderSet {
        let definition = SearchIndexDefinition {
            definition_id: 7,
            table_id: 11,
            name: "col1_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![1],
            expression: None,
            provider_config: json!({"version": 1, "config": "simple"}),
            freshness_policy,
            config_fingerprint: 99,
        };
        SearchInlineBuilderSet::new(
            vec![SearchInlineBuilderEntry::new(
                definition,
                13,
                freshness_policy,
                Arc::new(RecordingInlineBuilder { events }),
            )],
            admission,
        )
    }

    fn staging_builder_set(fail_finish: bool) -> SearchInlineBuilderSet {
        let freshness_policy = SearchFreshnessPolicy::Required;
        let definition = SearchIndexDefinition {
            definition_id: 9,
            table_id: 11,
            name: "staging_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: None,
            provider_config: json!({"version": 1, "config": "simple"}),
            freshness_policy,
            config_fingerprint: 101,
        };
        SearchInlineBuilderSet::new(
            vec![SearchInlineBuilderEntry::new(
                definition,
                13,
                freshness_policy,
                Arc::new(StagingInlineBuilder { fail_finish }),
            )],
            None,
        )
    }

    fn recording_and_staging_builder_set(
        events: Arc<Mutex<Vec<SearchSinkEvent>>>,
    ) -> SearchInlineBuilderSet {
        let freshness_policy = SearchFreshnessPolicy::Required;
        let recording_definition = SearchIndexDefinition {
            definition_id: 10,
            table_id: 11,
            name: "recording_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: None,
            provider_config: json!({"version": 1, "config": "simple"}),
            freshness_policy,
            config_fingerprint: 102,
        };
        let staging_definition = SearchIndexDefinition {
            definition_id: 11,
            table_id: 11,
            name: "staging_sparse".to_string(),
            kind: SearchIndexKind::Sparse,
            column_ids: vec![1],
            expression: None,
            provider_config: json!({"version": 1, "physical_encoding": "binary-v1"}),
            freshness_policy,
            config_fingerprint: 103,
        };
        SearchInlineBuilderSet::new(
            vec![
                SearchInlineBuilderEntry::new(
                    recording_definition,
                    13,
                    freshness_policy,
                    Arc::new(RecordingInlineBuilder { events }),
                ),
                SearchInlineBuilderEntry::new(
                    staging_definition,
                    13,
                    freshness_policy,
                    Arc::new(StagingInlineBuilder { fail_finish: false }),
                ),
            ],
            None,
        )
    }

    fn two_recording_builder_set_with_admission(
        events: Arc<Mutex<Vec<SearchSinkEvent>>>,
        admission: Arc<dyn SearchAdmission>,
    ) -> SearchInlineBuilderSet {
        let freshness_policy = SearchFreshnessPolicy::Required;
        let first_definition = SearchIndexDefinition {
            definition_id: 21,
            table_id: 11,
            name: "col0_required".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: None,
            provider_config: json!({"version": 1, "config": "simple"}),
            freshness_policy,
            config_fingerprint: 121,
        };
        let second_definition = SearchIndexDefinition {
            definition_id: 22,
            table_id: 11,
            name: "col1_required".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![1],
            expression: None,
            provider_config: json!({"version": 1, "config": "simple"}),
            freshness_policy,
            config_fingerprint: 122,
        };
        SearchInlineBuilderSet::new(
            vec![
                SearchInlineBuilderEntry::new(
                    first_definition,
                    13,
                    freshness_policy,
                    Arc::new(RecordingInlineBuilder {
                        events: Arc::clone(&events),
                    }),
                ),
                SearchInlineBuilderEntry::new(
                    second_definition,
                    13,
                    freshness_policy,
                    Arc::new(RecordingInlineBuilder { events }),
                ),
            ],
            Some(admission),
        )
    }

    fn hnsw_recording_builder_set(
        events: Arc<Mutex<Vec<SearchSinkEvent>>>,
        freshness_policy: SearchFreshnessPolicy,
        admission: Option<Arc<dyn SearchAdmission>>,
    ) -> SearchInlineBuilderSet {
        hnsw_recording_builder_set_with_max_vectors(events, freshness_policy, admission, 1024)
    }

    fn hnsw_recording_builder_set_with_max_vectors(
        events: Arc<Mutex<Vec<SearchSinkEvent>>>,
        freshness_policy: SearchFreshnessPolicy,
        admission: Option<Arc<dyn SearchAdmission>>,
        max_vector_count: u64,
    ) -> SearchInlineBuilderSet {
        let definition = SearchIndexDefinition {
            definition_id: 8,
            table_id: 11,
            name: "vec_hnsw".to_string(),
            kind: SearchIndexKind::Hnsw,
            column_ids: vec![1],
            expression: None,
            provider_config: json!({
                "version": HNSW_PROVIDER_CONFIG_VERSION,
                "dimension": 2,
                "distance": "euclidean",
                "build_vector_encoding": "exact_f32",
                "m": 8,
                "ef_construct": 64,
                "ef_search": 64,
                "rerank_policy": "top_k",
                "distance_cost": {
                    "source": {
                        "kind": "built_in",
                        "revision": crate::index::hnsw::HNSW_BUILT_IN_DISTANCE_COST_REVISION
                    },
                    "random_access_cost_units": crate::search::DEFAULT_HNSW_RANDOM_ACCESS_COST_UNITS,
                    "exact_f32_dimension_cost_units": crate::search::DEFAULT_HNSW_EXACT_F32_DIMENSION_COST_UNITS,
                    "sequential_dimension_cost_units": crate::search::DEFAULT_HNSW_SEQUENTIAL_DIMENSION_COST_UNITS,
                    "symmetric_i16_dimension_cost_units": crate::search::DEFAULT_HNSW_SYMMETRIC_I16_DIMENSION_COST_UNITS,
                    "graph_scored_points_per_ef":
                        crate::search::DEFAULT_HNSW_GRAPH_SCORED_POINTS_PER_EF
                },
                "generation_layout": {
                    "target_graph_rows": crate::search::DEFAULT_HNSW_GENERATION_TARGET_GRAPH_ROWS
                },
                "maintenance": {
                    "target_vector_bytes": crate::search::DEFAULT_HNSW_MAINTENANCE_TARGET_VECTOR_BYTES,
                    "max_pending_vector_bytes": crate::search::DEFAULT_HNSW_MAINTENANCE_MAX_PENDING_VECTOR_BYTES,
                    "compaction_fanout": crate::search::DEFAULT_HNSW_MAINTENANCE_COMPACTION_FANOUT
                },
                "build_seed": 1,
                "proposal_wave_max_size": crate::search::DEFAULT_HNSW_PROPOSAL_WAVE_MAX_SIZE,
                "warmup_point_count": crate::search::DEFAULT_HNSW_WARMUP_POINT_COUNT,
                "filter_columns": [],
                "filter_block_rows": crate::search::DEFAULT_HNSW_FILTER_BLOCK_ROWS,
                "filter_m": crate::search::DEFAULT_HNSW_FILTER_M,
                "inline_threshold": {
                    "enabled": true,
                    "max_vector_count": max_vector_count,
                    "max_graph_memory_bytes": 64 * 1024 * 1024u64,
                    "max_dimension": 128
                }
            }),
            freshness_policy,
            config_fingerprint: 100,
        };
        SearchInlineBuilderSet::new(
            vec![SearchInlineBuilderEntry::new(
                definition,
                13,
                freshness_policy,
                Arc::new(RecordingInlineBuilder { events }),
            )],
            admission,
        )
    }

    struct FixedAdmission {
        decision: AdmissionDecision,
        releases: Arc<Mutex<Vec<u64>>>,
        requests: Arc<Mutex<Vec<InlineAdmissionRequest>>>,
    }

    impl FixedAdmission {
        fn new(decision: AdmissionDecision, releases: Arc<Mutex<Vec<u64>>>) -> Self {
            Self {
                decision,
                releases,
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl SearchAdmission for FixedAdmission {
        fn request_inline_batch(
            &self,
            reqs: &[InlineAdmissionRequest],
        ) -> Result<Vec<AdmissionDecision>> {
            self.requests.lock().unwrap().extend_from_slice(reqs);
            Ok(vec![self.decision.clone(); reqs.len()])
        }

        fn release(&self, grant_id: u64) {
            self.releases.lock().unwrap().push(grant_id);
        }
    }

    struct SequencedAdmission {
        decisions: Mutex<Vec<Vec<AdmissionDecision>>>,
        releases: Arc<Mutex<Vec<u64>>>,
        requests: Arc<Mutex<Vec<InlineAdmissionRequest>>>,
    }

    impl SequencedAdmission {
        fn new(decisions: Vec<Vec<AdmissionDecision>>, releases: Arc<Mutex<Vec<u64>>>) -> Self {
            Self {
                decisions: Mutex::new(decisions),
                releases,
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl SearchAdmission for SequencedAdmission {
        fn request_inline_batch(
            &self,
            reqs: &[InlineAdmissionRequest],
        ) -> Result<Vec<AdmissionDecision>> {
            self.requests.lock().unwrap().extend_from_slice(reqs);
            let mut decisions = self.decisions.lock().unwrap();
            assert!(
                !decisions.is_empty(),
                "sequenced admission exhausted decisions"
            );
            let next = decisions.remove(0);
            assert_eq!(next.len(), reqs.len());
            Ok(next)
        }

        fn release(&self, grant_id: u64) {
            self.releases.lock().unwrap().push(grant_id);
        }
    }

    #[test]
    fn test_rowset_writer_context() {
        let schema = create_test_schema();
        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), "/tmp/rowset")
            .with_segment_size_threshold(128 * 1024 * 1024)
            .with_max_rows_per_segment(500_000)
            .with_compression(CompressionType::Zstd)
            .with_short_key_index(false);

        assert_eq!(context.tablet_id, 100);
        assert_eq!(context.segment_size_threshold, 128 * 1024 * 1024);
        assert_eq!(
            context.segment_row_limit,
            SegmentRowLimit::Explicit { max_rows: 500_000 }
        );
        assert_eq!(context.compression, CompressionType::Zstd);
        assert!(!context.build_short_key_index);
    }

    #[test]
    fn test_rowset_writer_create() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path);
        let writer = RowsetWriter::create(context).unwrap();

        assert_eq!(writer.tablet_id(), 100);
        assert_eq!(writer.num_rows(), 0);
        assert_eq!(writer.num_segments(), 0);
        assert!(!writer.is_finalized());
    }

    #[test]
    fn test_rowset_writer_builder() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let writer = RowsetWriterBuilder::new(schema, 100, Version::singleton(0), &rowset_path)
            .rowset_id(42)
            .max_rows_per_segment(100)
            .compression(CompressionType::None)
            .short_key_index(false)
            .build()
            .unwrap();

        assert_eq!(writer.rowset_id(), 42);
    }

    #[test]
    fn test_rowset_writer_empty_build() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false);
        let writer = RowsetWriter::create(context).unwrap();

        let rowset = writer.build().unwrap();

        assert_eq!(rowset.num_rows(), 0);
        assert_eq!(rowset.num_segments(), 0);
        assert_eq!(rowset.rowset_state(), RowsetState::Committed);
    }

    #[test]
    fn test_rowset_writer_add_chunk() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = RowsetWriter::create(context).unwrap();

        // Create test data: 10 rows of two i32 columns
        let col0_data: Vec<u8> = (0i32..10).flat_map(|v| v.to_le_bytes()).collect();
        let col1_data: Vec<u8> = (100i32..110).flat_map(|v| v.to_le_bytes()).collect();

        let columns = vec![
            ColumnData::new(col0_data, 10),
            ColumnData::new(col1_data, 10),
        ];

        let rows_added = writer.add_chunk(&columns).unwrap();
        assert_eq!(rows_added, 10);
        assert_eq!(writer.num_rows(), 10);
        assert_eq!(writer.num_segments(), 1); // One active segment

        let rowset = writer.build().unwrap();
        assert_eq!(rowset.num_rows(), 10);
        assert_eq!(rowset.num_segments(), 1);
    }

    #[test]
    fn rowset_writer_feeds_inline_sink_after_segment_append() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();
        let events = Arc::new(Mutex::new(Vec::new()));

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(recording_builder_set(Arc::clone(&events)));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer.add_chunk(&int_columns(0, 10)).unwrap();
        writer.add_chunk(&int_columns(10, 5)).unwrap();
        writer.flush_segment().unwrap();

        assert_eq!(writer.completed_search_artifacts.len(), 1);
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                SearchSinkEvent::Open {
                    rowset_id: 42,
                    segment_id: 0,
                    flush_mode: FlushSearchMode::InlineRequired,
                    grant_id: None,
                },
                SearchSinkEvent::Append {
                    base_row_id: 0,
                    rows: 10,
                    column_ids: None,
                },
                SearchSinkEvent::Append {
                    base_row_id: 10,
                    rows: 5,
                    column_ids: None,
                },
                SearchSinkEvent::Finish,
            ]
        );
    }

    #[test]
    fn rowset_writer_removes_search_staging_after_successful_flush() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let staging_dir = rowset_path.join("_search_staging");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(staging_builder_set(false));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer.add_chunk(&int_columns(0, 10)).unwrap();
        assert!(staging_dir.exists());
        writer.flush_segment().unwrap();

        assert!(
            !staging_dir.exists(),
            "inline search staging must not survive successful segment flush"
        );
    }

    #[test]
    fn rowset_writer_removes_search_staging_after_finish_failure() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let staging_dir = rowset_path.join("_search_staging");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(staging_builder_set(true));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer.add_chunk(&int_columns(0, 10)).unwrap();
        assert!(staging_dir.exists());
        let err = writer
            .flush_segment()
            .expect_err("staging finish should fail");

        assert!(err.to_string().contains("staging finish failure"));
        assert!(
            !staging_dir.exists(),
            "inline search staging must be removed when sink finish fails"
        );
    }

    #[test]
    fn rowset_writer_rolls_back_active_segment_and_inline_sink_to_savepoint() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();
        let events = Arc::new(Mutex::new(Vec::new()));

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(recording_builder_set(Arc::clone(&events)));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer.add_chunk(&int_columns(0, 10)).unwrap();
        let mark = writer.mark_savepoint().unwrap();
        writer.add_chunk(&int_columns(10, 5)).unwrap();
        assert_eq!(writer.num_rows(), 15);

        writer.rollback_to_savepoint(&mark).unwrap();
        assert_eq!(writer.num_rows(), 10);
        assert_eq!(writer.num_segments(), 1);
        writer.add_chunk(&int_columns(20, 3)).unwrap();
        let rowset = writer.build().unwrap();

        assert_eq!(rowset.num_rows(), 13);
        assert_eq!(rowset.num_segments(), 1);
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                SearchSinkEvent::Open {
                    rowset_id: 42,
                    segment_id: 0,
                    flush_mode: FlushSearchMode::InlineRequired,
                    grant_id: None,
                },
                SearchSinkEvent::Append {
                    base_row_id: 0,
                    rows: 10,
                    column_ids: None,
                },
                SearchSinkEvent::Mark { rows_seen: 10 },
                SearchSinkEvent::Append {
                    base_row_id: 10,
                    rows: 5,
                    column_ids: None,
                },
                SearchSinkEvent::Rollback { rows_seen: 10 },
                SearchSinkEvent::Append {
                    base_row_id: 10,
                    rows: 3,
                    column_ids: None,
                },
                SearchSinkEvent::Finish,
            ]
        );
    }

    #[test]
    fn rowset_writer_multi_sink_rollback_cleans_staging_and_continues() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let staging_dir = rowset_path.join("_search_staging");
        let schema = create_simple_schema();
        let events = Arc::new(Mutex::new(Vec::new()));

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(recording_and_staging_builder_set(Arc::clone(&events)));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer.add_chunk(&int_columns(0, 10)).unwrap();
        assert!(staging_dir.exists());
        let mark = writer.mark_savepoint().unwrap();
        writer.add_chunk(&int_columns(10, 5)).unwrap();
        assert!(staging_dir.exists());

        writer.rollback_to_savepoint(&mark).unwrap();
        assert!(
            !staging_dir.exists(),
            "rollback must remove staging files for all active sinks"
        );
        writer.add_chunk(&int_columns(20, 3)).unwrap();
        assert!(staging_dir.exists());
        let rowset = writer.build().unwrap();

        assert_eq!(rowset.num_rows(), 13);
        assert_eq!(rowset.num_segments(), 1);
        assert!(
            !staging_dir.exists(),
            "successful build must remove staging recreated after rollback"
        );
        assert!(events
            .lock()
            .unwrap()
            .contains(&SearchSinkEvent::Rollback { rows_seen: 10 }));
    }

    #[test]
    fn rowset_writer_rolls_back_hnsw_buffered_vectors_to_savepoint() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_hnsw_search_schema();
        let events = Arc::new(Mutex::new(Vec::new()));
        let releases = Arc::new(Mutex::new(Vec::new()));
        let admission = Arc::new(FixedAdmission::new(
            AdmissionDecision::Proceed(AdmissionGrant {
                budget: MaintenanceCost {
                    memory_peak_bytes: 1024,
                    ..Default::default()
                },
                valid_until: Instant::now(),
                grant_id: 99,
            }),
            Arc::clone(&releases),
        ));
        let admission_trait: Arc<dyn SearchAdmission> = admission;

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(hnsw_recording_builder_set(
                Arc::clone(&events),
                SearchFreshnessPolicy::Required,
                Some(admission_trait),
            ));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer
            .add_chunk(&hnsw_columns(&[
                [0.0, 0.0],
                [1.0, 1.0],
                [2.0, 2.0],
                [3.0, 3.0],
                [4.0, 4.0],
                [5.0, 5.0],
                [6.0, 6.0],
                [7.0, 7.0],
                [8.0, 8.0],
                [9.0, 9.0],
            ]))
            .unwrap();
        let mark = writer.mark_savepoint().unwrap();
        writer
            .add_chunk(&hnsw_columns(&[
                [10.0, 10.0],
                [11.0, 11.0],
                [12.0, 12.0],
                [13.0, 13.0],
                [14.0, 14.0],
            ]))
            .unwrap();

        writer.rollback_to_savepoint(&mark).unwrap();
        writer
            .add_chunk(&hnsw_columns(&[[20.0, 20.0], [21.0, 21.0], [22.0, 22.0]]))
            .unwrap();
        let rowset = writer.build().unwrap();

        assert_eq!(rowset.num_rows(), 13);
        let segment = rowset.get_segment(0).expect("segment");
        let hnsw = segment.hnsw_index(1).expect("hnsw index");
        assert_eq!(hnsw.graph.links.num_points(), 13);
        assert_eq!(
            *releases.lock().unwrap(),
            vec![99],
            "HNSW segment admission grant should release after flush"
        );
        assert!(
            events.lock().unwrap().is_empty(),
            "HNSW SegmentWriter inline build should not open a provider sink"
        );
    }

    #[test]
    fn rowset_writer_aborts_active_inline_sink_on_rollback() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();
        let events = Arc::new(Mutex::new(Vec::new()));

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(recording_builder_set(Arc::clone(&events)));
        let mut writer = RowsetWriter::create(context).unwrap();
        let mark = writer.mark_savepoint().unwrap();

        writer.add_chunk(&int_columns(0, 10)).unwrap();
        writer.rollback_to_savepoint(&mark).unwrap();

        assert_eq!(writer.num_rows(), 0);
        assert_eq!(writer.completed_search_artifacts.len(), 0);
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                SearchSinkEvent::Open {
                    rowset_id: 42,
                    segment_id: 0,
                    flush_mode: FlushSearchMode::InlineRequired,
                    grant_id: None,
                },
                SearchSinkEvent::Append {
                    base_row_id: 0,
                    rows: 10,
                    column_ids: None,
                },
                SearchSinkEvent::Abort,
            ]
        );
    }

    #[test]
    fn rowset_writer_skips_tail_only_inline_sink() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();
        let events = Arc::new(Mutex::new(Vec::new()));

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(recording_builder_set_with_policy(
                Arc::clone(&events),
                SearchFreshnessPolicy::Opportunistic,
            ));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer.add_chunk(&int_columns(0, 10)).unwrap();
        writer.flush_segment().unwrap();

        assert!(events.lock().unwrap().is_empty());
        assert_eq!(writer.completed_search_artifacts.len(), 1);
        assert!(writer.completed_search_artifacts[0].is_empty());
    }

    #[test]
    fn rowset_writer_skips_hnsw_inline_when_bounded_lag_has_no_admission() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_hnsw_search_schema();
        let events = Arc::new(Mutex::new(Vec::new()));

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(hnsw_recording_builder_set(
                Arc::clone(&events),
                SearchFreshnessPolicy::BoundedLag {
                    max_tail_rows: 64,
                    max_lag_millis: 250,
                },
                None,
            ));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer
            .add_chunk(&hnsw_columns(&[[0.0, 0.0], [1.0, 1.0]]))
            .unwrap();
        writer.flush_segment().unwrap();

        assert!(
            events.lock().unwrap().is_empty(),
            "HNSW inline builder must not open without admission"
        );
        let segment = writer
            .completed_segments
            .first()
            .expect("completed segment");
        assert!(
            segment.hnsw_index(1).is_none(),
            "HNSW segment page must not be built without admission"
        );
    }

    #[test]
    fn rowset_writer_skips_hnsw_inline_when_chunk_exceeds_inline_threshold() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_hnsw_search_schema();
        let events = Arc::new(Mutex::new(Vec::new()));
        let admission: Arc<dyn SearchAdmission> = Arc::new(InlineSearchAdmission::with_policy(
            MaintenanceAdmissionPolicy {
                memory_peak_bytes_budget: u64::MAX,
                cpu_ns_budget: u64::MAX,
                ..MaintenanceAdmissionPolicy::default()
            },
        ));

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(hnsw_recording_builder_set_with_max_vectors(
                Arc::clone(&events),
                SearchFreshnessPolicy::BoundedLag {
                    max_tail_rows: 64,
                    max_lag_millis: 250,
                },
                Some(admission),
                2,
            ));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer
            .add_chunk(&hnsw_columns(&[[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]]))
            .unwrap();
        writer.flush_segment().unwrap();

        let segment = writer
            .completed_segments
            .first()
            .expect("completed segment");
        assert!(
            segment.hnsw_index(1).is_none(),
            "oversized HNSW chunks must go tail-only instead of building inline"
        );
        assert!(
            events.lock().unwrap().is_empty(),
            "HNSW SegmentWriter inline build should not open a provider sink"
        );
    }

    #[test]
    fn rowset_writer_rejects_required_hnsw_inline_without_admission() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_hnsw_search_schema();
        let events = Arc::new(Mutex::new(Vec::new()));

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(hnsw_recording_builder_set(
                Arc::clone(&events),
                SearchFreshnessPolicy::Required,
                None,
            ));
        let mut writer = RowsetWriter::create(context).unwrap();

        let err = writer
            .add_chunk(&hnsw_columns(&[[0.0, 0.0], [1.0, 1.0]]))
            .expect_err("required HNSW inline build should require admission");

        assert!(
            err.to_string().contains("requires admission"),
            "unexpected error: {err}"
        );
        assert!(events.lock().unwrap().is_empty());
    }

    #[test]
    fn rowset_writer_passes_hnsw_inline_estimate_to_admission() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_hnsw_search_schema();
        let events = Arc::new(Mutex::new(Vec::new()));
        let releases = Arc::new(Mutex::new(Vec::new()));
        let admission = Arc::new(FixedAdmission::new(
            AdmissionDecision::Proceed(AdmissionGrant {
                budget: MaintenanceCost {
                    memory_peak_bytes: 1024,
                    ..Default::default()
                },
                valid_until: Instant::now(),
                grant_id: 88,
            }),
            Arc::clone(&releases),
        ));
        let requests = Arc::clone(&admission.requests);
        let admission_trait: Arc<dyn SearchAdmission> = admission;

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_max_rows_per_segment(128)
            .with_search_inline_builders(hnsw_recording_builder_set(
                Arc::clone(&events),
                SearchFreshnessPolicy::Required,
                Some(admission_trait),
            ));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer
            .add_chunk(&hnsw_columns(&[[0.0, 0.0], [1.0, 1.0]]))
            .unwrap();
        writer.flush_segment().unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let estimate = requests[0].hnsw_inline.expect("hnsw inline estimate");
        assert_eq!(estimate.vector_count, 128);
        assert_eq!(requests[0].row_count, 128);
        assert_eq!(estimate.dimension, 2);
        assert_eq!(estimate.threshold.max_vector_count, 1024);
        assert!(estimate.allows_inline());
        assert_eq!(
            requests[0].estimated_cost.memory_peak_bytes,
            estimate.estimated_build_peak_memory_bytes
        );
        assert_eq!(*releases.lock().unwrap(), vec![88]);
    }

    #[test]
    fn rowset_writer_passes_inline_admission_grant_and_releases_after_finish() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();
        let events = Arc::new(Mutex::new(Vec::new()));
        let releases = Arc::new(Mutex::new(Vec::new()));
        let admission = Arc::new(FixedAdmission::new(
            AdmissionDecision::Proceed(AdmissionGrant {
                budget: MaintenanceCost {
                    cpu_ns: 10,
                    ..Default::default()
                },
                valid_until: Instant::now(),
                grant_id: 77,
            }),
            Arc::clone(&releases),
        ));
        let requests = Arc::clone(&admission.requests);
        let admission_trait: Arc<dyn SearchAdmission> = admission;

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(recording_builder_set_with_policy_and_admission(
                Arc::clone(&events),
                SearchFreshnessPolicy::Required,
                Some(admission_trait),
            ));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer.add_chunk(&int_columns(0, 10)).unwrap();
        writer.flush_segment().unwrap();

        assert_eq!(
            *events.lock().unwrap(),
            vec![
                SearchSinkEvent::Open {
                    rowset_id: 42,
                    segment_id: 0,
                    flush_mode: FlushSearchMode::InlineRequired,
                    grant_id: Some(77),
                },
                SearchSinkEvent::Append {
                    base_row_id: 0,
                    rows: 10,
                    column_ids: None,
                },
                SearchSinkEvent::Finish,
            ]
        );
        assert_eq!(*releases.lock().unwrap(), vec![77]);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].flush_mode, FlushSearchMode::InlineRequired);
        assert!(requests[0].estimated_cost.cpu_ns > 0);
    }

    #[test]
    fn rowset_writer_falls_back_to_tail_only_when_bounded_lag_waits_for_admission() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();
        let events = Arc::new(Mutex::new(Vec::new()));
        let releases = Arc::new(Mutex::new(Vec::new()));
        let admission = Arc::new(FixedAdmission::new(
            AdmissionDecision::Wait {
                deadline: Instant::now(),
                reason: AdmissionWaitReason::MemoryBudget,
            },
            releases,
        ));
        let requests = Arc::clone(&admission.requests);
        let admission_trait: Arc<dyn SearchAdmission> = admission;

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(recording_builder_set_with_policy_and_admission(
                Arc::clone(&events),
                SearchFreshnessPolicy::BoundedLag {
                    max_tail_rows: 64,
                    max_lag_millis: 250,
                },
                Some(admission_trait),
            ));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer.add_chunk(&int_columns(0, 10)).unwrap();
        writer.flush_segment().unwrap();

        assert!(events.lock().unwrap().is_empty());
        assert_eq!(writer.completed_search_artifacts.len(), 1);
        assert!(writer.completed_search_artifacts[0].is_empty());
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].flush_mode, FlushSearchMode::InlineIfAdmitted);
    }

    #[test]
    fn rowset_writer_retries_required_inline_when_admission_waits() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();
        let events = Arc::new(Mutex::new(Vec::new()));
        let releases = Arc::new(Mutex::new(Vec::new()));
        let admission = Arc::new(SequencedAdmission::new(
            vec![
                vec![AdmissionDecision::Wait {
                    deadline: Instant::now(),
                    reason: AdmissionWaitReason::MemoryBudget,
                }],
                vec![AdmissionDecision::Proceed(AdmissionGrant {
                    budget: MaintenanceCost::default(),
                    valid_until: Instant::now(),
                    grant_id: 88,
                })],
            ],
            Arc::clone(&releases),
        ));
        let requests = Arc::clone(&admission.requests);
        let admission_trait: Arc<dyn SearchAdmission> = admission;

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(recording_builder_set_with_policy_and_admission(
                Arc::clone(&events),
                SearchFreshnessPolicy::Required,
                Some(admission_trait),
            ));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer.add_chunk(&int_columns(0, 10)).unwrap();
        writer.flush_segment().unwrap();

        assert_eq!(
            *events.lock().unwrap(),
            vec![
                SearchSinkEvent::Open {
                    rowset_id: 42,
                    segment_id: 0,
                    flush_mode: FlushSearchMode::InlineRequired,
                    grant_id: Some(88),
                },
                SearchSinkEvent::Append {
                    base_row_id: 0,
                    rows: 10,
                    column_ids: None,
                },
                SearchSinkEvent::Finish,
            ]
        );
        assert_eq!(*releases.lock().unwrap(), vec![88]);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests
            .iter()
            .all(|request| request.flush_mode == FlushSearchMode::InlineRequired));
    }

    #[test]
    fn rowset_writer_releases_partial_grants_before_required_wait_retry() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();
        let events = Arc::new(Mutex::new(Vec::new()));
        let releases = Arc::new(Mutex::new(Vec::new()));
        let admission = Arc::new(SequencedAdmission::new(
            vec![
                vec![
                    AdmissionDecision::Proceed(AdmissionGrant {
                        budget: MaintenanceCost::default(),
                        valid_until: Instant::now(),
                        grant_id: 10,
                    }),
                    AdmissionDecision::Wait {
                        deadline: Instant::now(),
                        reason: AdmissionWaitReason::ProviderConcurrency,
                    },
                ],
                vec![
                    AdmissionDecision::Proceed(AdmissionGrant {
                        budget: MaintenanceCost::default(),
                        valid_until: Instant::now(),
                        grant_id: 11,
                    }),
                    AdmissionDecision::Proceed(AdmissionGrant {
                        budget: MaintenanceCost::default(),
                        valid_until: Instant::now(),
                        grant_id: 12,
                    }),
                ],
            ],
            Arc::clone(&releases),
        ));
        let requests = Arc::clone(&admission.requests);
        let admission_trait: Arc<dyn SearchAdmission> = admission;

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(two_recording_builder_set_with_admission(
                Arc::clone(&events),
                admission_trait,
            ));
        let mut writer = RowsetWriter::create(context).unwrap();

        writer.add_chunk(&int_columns(0, 10)).unwrap();
        writer.flush_segment().unwrap();

        let events = events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SearchSinkEvent::Open { .. }))
                .count(),
            2
        );
        assert_eq!(*releases.lock().unwrap(), vec![10, 11, 12]);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        assert!(requests
            .iter()
            .all(|request| request.flush_mode == FlushSearchMode::InlineRequired));
    }

    #[test]
    fn rowset_writer_rejects_required_inline_when_admission_rejects() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();
        let events = Arc::new(Mutex::new(Vec::new()));
        let releases = Arc::new(Mutex::new(Vec::new()));
        let admission = Arc::new(FixedAdmission::new(
            AdmissionDecision::Reject {
                reason: AdmissionRejectReason::RequiredBudgetUnavailable,
            },
            releases,
        ));
        let admission_trait: Arc<dyn SearchAdmission> = admission;

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(recording_builder_set_with_policy_and_admission(
                Arc::clone(&events),
                SearchFreshnessPolicy::Required,
                Some(admission_trait),
            ));
        let mut writer = RowsetWriter::create(context).unwrap();

        let err = writer.add_chunk(&int_columns(0, 10)).unwrap_err();

        assert!(err
            .to_string()
            .contains("required search inline build for definition 7 was rejected"));
        assert!(events.lock().unwrap().is_empty());
        assert!(!rowset_path.join("0.dat").exists());
    }

    #[test]
    fn rowset_writer_skips_inline_sink_for_untouched_partial_columns() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();
        let events = Arc::new(Mutex::new(Vec::new()));

        let mut writer = RowsetWriterBuilder::new(schema, 100, Version::singleton(0), &rowset_path)
            .rowset_id(42)
            .short_key_index(false)
            .compression(CompressionType::None)
            .write_column_ids(vec![0])
            .search_inline_builders(recording_builder_set(Arc::clone(&events)))
            .build()
            .unwrap();

        let col0_data: Vec<u8> = (0i32..3).flat_map(|v| v.to_le_bytes()).collect();
        writer.add_chunk(&[ColumnData::new(col0_data, 3)]).unwrap();
        writer.flush_segment().unwrap();

        assert!(events.lock().unwrap().is_empty());
        assert_eq!(writer.completed_search_artifacts.len(), 1);
        assert!(writer.completed_search_artifacts[0].is_empty());
    }

    #[test]
    fn rowset_writer_publishes_inline_fulltext_blob_in_segment_footer() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_text_search_schema();
        let definition = SearchIndexDefinition {
            definition_id: 70,
            table_id: 100,
            name: "body_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![1],
            expression: None,
            provider_config: json!({"version": 1, "config": "simple"}),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
            config_fingerprint: 7,
        };
        let builders = SearchInlineBuilderSet::new(
            vec![SearchInlineBuilderEntry::new(
                definition,
                3,
                SearchFreshnessPolicy::Required,
                Arc::new(FullTextInlineArtifactBuilder),
            )],
            None,
        );
        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(builders);
        let mut writer = RowsetWriter::create(context).unwrap();

        writer
            .add_chunk(&text_columns(&["alpha beta", "beta gamma"]))
            .unwrap();
        let rowset = writer.build().unwrap();

        let segments = rowset.segments();
        assert_eq!(segments.len(), 1);
        let segment = &segments[0];
        let body_meta = segment
            .column_metas()
            .iter()
            .find(|meta| meta.column_id == 1)
            .expect("body column meta");
        assert!(body_meta.fulltext_index_pointer.is_some());
        assert!(segment.fulltext_index(1).is_some());
    }

    #[test]
    fn rowset_writer_publishes_inline_sparse_blob_in_segment_footer() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_sparse_search_schema();
        let definition = SearchIndexDefinition {
            definition_id: 71,
            table_id: 100,
            name: "body_sparse".to_string(),
            kind: SearchIndexKind::Sparse,
            column_ids: vec![1],
            expression: None,
            provider_config: json!({"version": 1, "physical_encoding": "binary-v1"}),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
            config_fingerprint: 8,
        };
        let builders = SearchInlineBuilderSet::new(
            vec![SearchInlineBuilderEntry::new(
                definition,
                3,
                SearchFreshnessPolicy::Required,
                Arc::new(SparseInlineArtifactBuilder),
            )],
            None,
        );
        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_rowset_id(42)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_search_inline_builders(builders);
        let mut writer = RowsetWriter::create(context).unwrap();

        writer
            .add_chunk(&sparse_columns(&[
                SparseVector::new(vec![1, 2], vec![1.0, 0.5]).unwrap(),
                SparseVector::new(vec![2], vec![1.0]).unwrap(),
            ]))
            .unwrap();
        let rowset = writer.build().unwrap();

        let segments = rowset.segments();
        assert_eq!(segments.len(), 1);
        let segment = &segments[0];
        let body_meta = segment
            .column_metas()
            .iter()
            .find(|meta| meta.column_id == 1)
            .expect("body column meta");
        assert!(body_meta.sparse_index_pointer.is_some());
        assert!(segment.sparse_index(1).is_some());
    }

    #[test]
    fn test_rowset_writer_multiple_chunks() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = RowsetWriter::create(context).unwrap();

        // Add multiple chunks
        for batch in 0..5 {
            let col0_data: Vec<u8> = (0i32..100)
                .flat_map(|v| (v + batch * 100).to_le_bytes())
                .collect();
            let col1_data: Vec<u8> = (0i32..100)
                .flat_map(|v| (v + batch * 1000).to_le_bytes())
                .collect();

            let columns = vec![
                ColumnData::new(col0_data, 100),
                ColumnData::new(col1_data, 100),
            ];

            writer.add_chunk(&columns).unwrap();
        }

        assert_eq!(writer.num_rows(), 500);

        let rowset = writer.build().unwrap();
        assert_eq!(rowset.num_rows(), 500);
    }

    #[test]
    fn test_rowset_writer_auto_flush() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        // Set low threshold to trigger auto-flush
        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_max_rows_per_segment(50); // Very low threshold
        let mut writer = RowsetWriter::create(context).unwrap();

        // Add multiple small chunks to trigger auto-flush
        for _ in 0..4 {
            let col0_data: Vec<u8> = (0i32..30).flat_map(|v| v.to_le_bytes()).collect();
            let col1_data: Vec<u8> = (0i32..30).flat_map(|v| v.to_le_bytes()).collect();

            let columns = vec![
                ColumnData::new(col0_data, 30),
                ColumnData::new(col1_data, 30),
            ];

            writer.add_chunk(&columns).unwrap();
        }

        // Should have created multiple segments due to auto-flush
        let rowset = writer.build().unwrap();
        assert_eq!(rowset.num_rows(), 120);
        // At least 2 segments due to 50 row threshold (120 rows / 50 = 2.4)
        assert!(
            rowset.num_segments() >= 2,
            "Expected at least 2 segments, got {}",
            rowset.num_segments()
        );
    }

    #[test]
    fn test_rowset_writer_manual_flush() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = RowsetWriter::create(context).unwrap();

        // Add first chunk
        let col0_data: Vec<u8> = (0i32..50).flat_map(|v| v.to_le_bytes()).collect();
        let col1_data: Vec<u8> = (0i32..50).flat_map(|v| v.to_le_bytes()).collect();
        let columns = vec![
            ColumnData::new(col0_data, 50),
            ColumnData::new(col1_data, 50),
        ];
        writer.add_chunk(&columns).unwrap();

        // Manual flush
        writer.flush_segment().unwrap();

        // Add second chunk
        let col0_data: Vec<u8> = (50i32..100).flat_map(|v| v.to_le_bytes()).collect();
        let col1_data: Vec<u8> = (50i32..100).flat_map(|v| v.to_le_bytes()).collect();
        let columns = vec![
            ColumnData::new(col0_data, 50),
            ColumnData::new(col1_data, 50),
        ];
        writer.add_chunk(&columns).unwrap();

        let rowset = writer.build().unwrap();
        assert_eq!(rowset.num_rows(), 100);
        assert_eq!(rowset.num_segments(), 2);
    }

    #[test]
    fn test_rowset_writer_savepoint_discards_new_segment_outputs() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_savepoint");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = RowsetWriter::create(context).unwrap();

        let first_columns = vec![
            ColumnData::new(
                (0i32..10).flat_map(|v| v.to_le_bytes()).collect::<Vec<_>>(),
                10,
            ),
            ColumnData::new(
                (100i32..110)
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
                10,
            ),
        ];
        writer.add_chunk(&first_columns).unwrap();

        let mark = writer.mark_savepoint().unwrap();
        assert_eq!(writer.num_segments(), 1);
        assert!(
            rowset_path.join("0.dat").exists(),
            "active segment file exists while the segment writer is open"
        );

        let second_columns = vec![
            ColumnData::new(
                (10i32..15)
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
                5,
            ),
            ColumnData::new(
                (110i32..115)
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
                5,
            ),
        ];
        writer.add_chunk(&second_columns).unwrap();
        assert_eq!(writer.num_rows(), 15);
        assert_eq!(writer.num_segments(), 1);

        writer.rollback_to_savepoint(&mark).unwrap();

        assert_eq!(writer.num_rows(), 10);
        assert_eq!(writer.num_segments(), 1);
        assert!(rowset_path.join("0.dat").exists());
        assert!(!rowset_path.join("1.dat").exists());

        let third_columns = vec![
            ColumnData::new(
                (20i32..24)
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
                4,
            ),
            ColumnData::new(
                (120i32..124)
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
                4,
            ),
        ];
        writer.add_chunk(&third_columns).unwrap();

        let rowset = writer.build().unwrap();
        assert_eq!(rowset.num_rows(), 14);
        assert_eq!(rowset.num_segments(), 1);
    }

    #[test]
    fn test_rowset_writer_version() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        // Test with range version
        let context = RowsetWriterContext::new(schema, 100, Version::new(5, 10), &rowset_path)
            .with_short_key_index(false);
        let writer = RowsetWriter::create(context).unwrap();

        assert_eq!(writer.version().start, 5);
        assert_eq!(writer.version().end, 10);

        let rowset = writer.build().unwrap();
        assert_eq!(rowset.start_version(), 5);
        assert_eq!(rowset.end_version(), 10);
    }

    #[test]
    fn test_rowset_writer_statistics() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = RowsetWriter::create(context).unwrap();

        // Add data
        let col0_data: Vec<u8> = (0i32..100).flat_map(|v| v.to_le_bytes()).collect();
        let col1_data: Vec<u8> = (0i32..100).flat_map(|v| v.to_le_bytes()).collect();
        let columns = vec![
            ColumnData::new(col0_data, 100),
            ColumnData::new(col1_data, 100),
        ];
        writer.add_chunk(&columns).unwrap();

        let rowset = writer.build().unwrap();

        // Check that statistics are populated
        assert_eq!(rowset.num_rows(), 100);
        assert!(rowset.total_disk_size() > 0);
    }

    #[test]
    fn test_rowset_writer_double_build_fails() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false);
        let writer = RowsetWriter::create(context).unwrap();

        // First build succeeds
        let _rowset = writer.build().unwrap();

        // Can't build twice because build() consumes self
        // This is enforced by Rust's ownership system
    }

    #[test]
    fn test_rowset_writer_path() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false);
        let writer = RowsetWriter::create(context).unwrap();

        assert_eq!(writer.rowset_path(), rowset_path);
    }

    #[test]
    fn test_rowset_writer_with_primary_key_schema() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_test_schema(); // Has primary key

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = RowsetWriter::create(context).unwrap();

        // Create test data for 3 columns
        let col0_data: Vec<u8> = (0i64..10).flat_map(|v| v.to_le_bytes()).collect(); // BigInt
        let col1_data: Vec<u8> = vec![0u8; 40]; // Varchar placeholder
        let col2_data: Vec<u8> = (0i32..10).flat_map(|v| v.to_le_bytes()).collect(); // Integer

        let columns = vec![
            ColumnData::new(col0_data, 10),
            ColumnData::new(col1_data, 10),
            ColumnData::new(col2_data, 10),
        ];

        writer.add_chunk(&columns).unwrap();

        let rowset = writer.build().unwrap();
        assert_eq!(rowset.num_rows(), 10);
    }

    #[test]
    fn adaptive_segment_limit_uses_provider_placement_but_explicit_limit_caps_it() {
        let adaptive = SegmentRowLimit::Adaptive {
            fallback_rows: 1_000_000,
        };
        assert_eq!(adaptive.effective(None), 1_000_000);
        assert_eq!(adaptive.effective(Some(2_000_000)), 2_000_000);

        let explicit = SegmentRowLimit::Explicit { max_rows: 500_000 };
        assert_eq!(explicit.effective(None), 500_000);
        assert_eq!(explicit.effective(Some(2_000_000)), 500_000);
        assert_eq!(explicit.effective(Some(250_000)), 250_000);
    }
}
