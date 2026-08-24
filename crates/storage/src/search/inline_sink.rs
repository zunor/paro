// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Writer-side search artifact contracts.
//!
//! These types are intentionally storage-owned. `RowsetWriter` can feed the
//! same logical chunk it is already writing into provider sinks without making
//! providers reopen freshly-written segments.

use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::rowset::{ColumnData, RowsetId, RowsetSharedPtr};
use crate::tablet::{ColumnId, TabletColumn};

use super::artifact::ArtifactFileId;
use super::capability::{
    SearchArtifactRef, SearchFreshnessPolicy, SearchIndexDefinition, SearchIndexKind,
};
use super::stats::{
    ConfigFingerprint, FullTextProviderStats, HnswProviderStats, ProviderVariantId,
    SearchArtifactStats, SearchDefinitionId, SearchGenerationId, SearchProviderStats, SegmentId,
    SparseProviderStats, TableId,
};
use super::tail::TailPendingEntry;

/// Logical chunk image consumed by an inline search sink.
///
/// The chunk is append-only from the sink's point of view. Replace/delete
/// semantics belong to tail metadata, not to provider-specific chunk parsing.
#[derive(Clone, Copy)]
pub struct SegmentChunkInput<'a> {
    pub base_row_id: u32,
    pub columns: &'a [ColumnData],
    pub column_ids: Option<&'a [ColumnId]>,
}

/// Provider sink opened for a single segment.
pub trait SegmentChunkSink: Send {
    fn append_chunk(&mut self, input: SegmentChunkInput<'_>) -> Result<()>;

    fn mark_savepoint(&mut self) -> Result<SegmentSinkSavepoint>;

    fn rollback_to_savepoint(&mut self, savepoint: &SegmentSinkSavepoint) -> Result<()>;

    fn finish(self: Box<Self>) -> Result<InlineArtifactBuildResult>;

    fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentSinkSavepoint {
    pub rows_seen: u64,
    pub bytes_buffered: u64,
    pub entries_seen: u64,
    pub state_id: u64,
}

pub trait InlineArtifactBuilder: Send + Sync {
    fn open_sink(&self, ctx: &SegmentFlushCtx<'_>) -> Result<Box<dyn SegmentChunkSink>>;
}

#[derive(Debug, Default)]
pub struct HnswSegmentInlineArtifactBuilder;

impl InlineArtifactBuilder for HnswSegmentInlineArtifactBuilder {
    fn open_sink(&self, _ctx: &SegmentFlushCtx<'_>) -> Result<Box<dyn SegmentChunkSink>> {
        Err(paro_error::internal(
            "HNSW inline artifacts are built by SegmentWriter after RowsetWriter admission",
        ))
    }
}

#[derive(Clone)]
pub struct SearchInlineBuilderEntry {
    pub definition: SearchIndexDefinition,
    pub generation_id: SearchGenerationId,
    pub freshness_policy: SearchFreshnessPolicy,
    pub builder: Arc<dyn InlineArtifactBuilder>,
}

impl SearchInlineBuilderEntry {
    pub fn new(
        definition: SearchIndexDefinition,
        generation_id: SearchGenerationId,
        freshness_policy: SearchFreshnessPolicy,
        builder: Arc<dyn InlineArtifactBuilder>,
    ) -> Self {
        Self {
            definition,
            generation_id,
            freshness_policy,
            builder,
        }
    }

    pub const fn flush_mode(&self) -> FlushSearchMode {
        self.freshness_policy.default_flush_mode()
    }
}

impl fmt::Debug for SearchInlineBuilderEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SearchInlineBuilderEntry")
            .field("definition_id", &self.definition.definition_id)
            .field("generation_id", &self.generation_id)
            .field("generation_kind", &self.definition.kind)
            .field("column_ids", &self.definition.column_ids)
            .field("freshness_policy", &self.freshness_policy)
            .field("flush_mode", &self.flush_mode())
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct SearchInlineBuilderSet {
    entries: Arc<[SearchInlineBuilderEntry]>,
    admission: Option<Arc<dyn SearchAdmission>>,
}

impl SearchInlineBuilderSet {
    pub fn new(
        entries: Vec<SearchInlineBuilderEntry>,
        admission: Option<Arc<dyn SearchAdmission>>,
    ) -> Self {
        Self {
            entries: entries.into_boxed_slice().into(),
            admission,
        }
    }

    pub fn empty() -> Self {
        Self::new(Vec::new(), None)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn entries(&self) -> &[SearchInlineBuilderEntry] {
        &self.entries
    }

    pub fn admission(&self) -> Option<&Arc<dyn SearchAdmission>> {
        self.admission.as_ref()
    }
}

impl Default for SearchInlineBuilderSet {
    fn default() -> Self {
        Self::empty()
    }
}

impl fmt::Debug for SearchInlineBuilderSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SearchInlineBuilderSet")
            .field("entries", &self.entries)
            .field("has_admission", &self.admission.is_some())
            .finish()
    }
}

pub trait SidecarArtifactBuilder: Send + Sync {
    fn estimate_cost(&self, input: &SidecarBuildInput) -> CostEstimate;

    fn build(
        &self,
        input: SidecarBuildInput,
        budget: &BuildBudget,
    ) -> Result<SidecarArtifactBuildResult>;
}

pub struct SegmentFlushCtx<'a> {
    pub rowset_id: RowsetId,
    pub segment_id: SegmentId,
    pub definition: &'a SearchIndexDefinition,
    pub generation_id: SearchGenerationId,
    pub flush_mode: FlushSearchMode,
    pub admission: Option<AdmissionGrant>,
    pub staging_dir: &'a Path,
    pub column_schema: &'a [TabletColumn],
}

pub struct SidecarBuildInput {
    pub definition: SearchIndexDefinition,
    pub generation_id: SearchGenerationId,
    pub tail_window: Vec<TailPendingEntry>,
    pub rowset_refs: Vec<RowsetSharedPtr>,
    pub snapshot_version: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildBudget {
    pub cost_envelope: MaintenanceCost,
    pub deadline: Option<Instant>,
    pub grant_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MaintenanceCost {
    pub cpu_ns: u64,
    pub io_read_bytes: u64,
    pub io_write_bytes: u64,
    pub memory_peak_bytes: u64,
    pub publish_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MaintenanceBenefit {
    pub expected_open_cost_saved_us: u64,
    pub expected_tail_rows_drained: u64,
    pub expected_artifact_bytes_reclaimed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CostEstimate {
    pub cost: MaintenanceCost,
    pub benefit: MaintenanceBenefit,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InlineArtifactBuildResult {
    pub blobs: Vec<InlineArtifactBlob>,
    pub stats_delta: Option<SearchStatsDelta>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InlineArtifactBlob {
    pub definition_id: SearchDefinitionId,
    pub generation_id: SearchGenerationId,
    pub column_id: ColumnId,
    pub kind: SearchIndexKind,
    pub bytes: Vec<u8>,
    pub stats: SearchArtifactStats,
    pub checksum: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SidecarArtifactBuildResult {
    pub artifact_refs: Vec<SearchArtifactRef>,
    pub stats_delta: Option<SearchStatsDelta>,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushSearchMode {
    InlineRequired,
    InlineIfAdmitted,
    TailOnly,
}

impl SearchFreshnessPolicy {
    pub const fn default_flush_mode(self) -> FlushSearchMode {
        match self {
            Self::Required => FlushSearchMode::InlineRequired,
            Self::BoundedLag { .. } => FlushSearchMode::InlineIfAdmitted,
            Self::Opportunistic => FlushSearchMode::TailOnly,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineAdmissionRequest {
    pub table_id: TableId,
    pub definition_id: SearchDefinitionId,
    pub provider: SearchIndexKind,
    pub flush_mode: FlushSearchMode,
    pub estimated_cost: MaintenanceCost,
    pub row_count: u64,
    pub hnsw_inline: Option<HnswInlineBuildEstimate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionDecision {
    Proceed(AdmissionGrant),
    Wait {
        deadline: Instant,
        reason: AdmissionWaitReason,
    },
    Reject {
        reason: AdmissionRejectReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionGrant {
    pub budget: MaintenanceCost,
    pub valid_until: Instant,
    pub grant_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionWaitReason {
    CpuBudget,
    IoBudget,
    MemoryBudget,
    ProviderConcurrency,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionRejectReason {
    RequiredBudgetUnavailable,
    InvalidRequest,
    InlineThresholdExceeded,
    ProviderDisabled,
}

pub trait SearchAdmission: Send + Sync {
    fn request_inline_batch(
        &self,
        reqs: &[InlineAdmissionRequest],
    ) -> Result<Vec<AdmissionDecision>>;

    fn release(&self, grant_id: u64);
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderConfig {
    pub kind: SearchIndexKind,
    pub raw_config: Value,
    pub config_fingerprint: ConfigFingerprint,
}

impl ProviderConfig {
    pub fn from_definition(definition: &SearchIndexDefinition) -> Self {
        Self {
            kind: definition.kind,
            raw_config: definition.provider_config.clone(),
            config_fingerprint: definition.config_fingerprint,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderStatsProfile {
    pub provider_kind: SearchIndexKind,
    pub provider_variant: ProviderVariantId,
    pub artifact_format_version: u32,
    pub scoring_fingerprint: u64,
    pub config_fingerprint: ConfigFingerprint,
}

impl ProviderStatsProfile {
    pub fn compatible_with_config(
        &self,
        config: &ProviderConfig,
        provider_variant: ProviderVariantId,
        artifact_format_version: u32,
        scoring_fingerprint: u64,
    ) -> bool {
        self.provider_kind == config.kind
            && self.provider_variant == provider_variant
            && self.artifact_format_version == artifact_format_version
            && self.scoring_fingerprint == scoring_fingerprint
            && self.config_fingerprint == config.config_fingerprint
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SearchStatsDelta {
    FullText(FullTextStatsDelta),
    Sparse(SparseStatsDelta),
    Hnsw(HnswStatsDelta),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FullTextStatsDelta {
    pub stats: FullTextProviderStats,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SparseStatsDelta {
    pub row_count: u64,
    pub nnz: u64,
    pub posting_fanout: u64,
    pub unique_dimensions: u64,
    pub l2_norm_sum: f64,
    pub max_l2_norm: f32,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct HnswStatsDelta {
    pub vector_count: u64,
    pub dimension: u32,
    pub max_level: u32,
    pub m: u32,
    pub ef_construction: u32,
    pub graph_memory_bytes: u64,
    pub vector_storage_bytes: u64,
    pub total_graph_links: u64,
    pub level0_graph_links: u64,
    pub avg_level0_degree: f32,
    pub max_level0_degree: u32,
}

impl SearchStatsDelta {
    pub fn provider_stats(&self) -> Option<SearchProviderStats> {
        match self {
            Self::FullText(delta) => Some(SearchProviderStats::FullText(delta.stats.clone())),
            Self::Sparse(delta) => Some(SearchProviderStats::Sparse(SparseProviderStats {
                row_count: delta.row_count,
                nnz: delta.nnz,
                posting_fanout: delta.posting_fanout,
                unique_dimensions: delta.unique_dimensions,
                avg_vector_nnz: if delta.row_count == 0 {
                    0.0
                } else {
                    delta.nnz as f32 / delta.row_count as f32
                },
                l2_norm_sum: delta.l2_norm_sum,
                max_l2_norm: delta.max_l2_norm,
            })),
            Self::Hnsw(delta) => Some(SearchProviderStats::Hnsw(HnswProviderStats {
                vector_count: delta.vector_count,
                dimension: delta.dimension,
                max_level: delta.max_level,
                m: delta.m,
                ef_construction: delta.ef_construction,
                graph_memory_bytes: delta.graph_memory_bytes,
                vector_storage_bytes: delta.vector_storage_bytes,
                total_graph_links: delta.total_graph_links,
                level0_graph_links: delta.level0_graph_links,
                avg_level0_degree: delta.avg_level0_degree,
                max_level0_degree: delta.max_level0_degree,
            })),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswInlineThreshold {
    pub max_vector_count: u64,
    pub max_graph_memory_bytes: u64,
    pub max_dimension: u32,
}

impl HnswInlineThreshold {
    pub const DEFAULT: Self = Self {
        max_vector_count: 4_096,
        max_graph_memory_bytes: 64 * 1024 * 1024,
        max_dimension: 1_536,
    };

    pub const fn allows(
        self,
        vector_count: u64,
        estimated_graph_memory_bytes: u64,
        dimension: u32,
    ) -> bool {
        vector_count <= self.max_vector_count
            && estimated_graph_memory_bytes <= self.max_graph_memory_bytes
            && dimension <= self.max_dimension
    }

    /// Machine-independent resident graph estimate used by the durable inline
    /// threshold. This models the published hybrid-CSR artifact, not the
    /// mutable builder object graph. Mixing builder containers into this
    /// estimate makes storage segmentation depend on transient implementation
    /// details and multiplies query-time graph work. Runtime worker width must
    /// never affect this value: identical definitions and segment contents
    /// must make the same inline/sidecar placement decision on every node.
    pub fn estimate_graph_memory_bytes(vector_count: u64, m: u32) -> u64 {
        let m = u64::from(m.max(1));
        let point_bytes = std::mem::size_of::<u32>() as u64;
        let offset_bytes = std::mem::size_of::<u64>() as u64;
        let header_bytes = 64_u64;
        let offset_tables = vector_count
            .saturating_add(1)
            .saturating_mul(offset_bytes)
            .saturating_mul(2);
        let level0_links = vector_count
            .saturating_mul(m.saturating_mul(2))
            .saturating_mul(point_bytes);
        // Upper levels are delta-varint encoded. The standard level
        // distribution has 1/(m-1) upper point-levels per point. Five bytes
        // per u32 target plus level/count tags is a conservative durable bound
        // without importing mutable builder container sizes.
        let upper_denominator = m.max(2) - 1;
        let upper_payload_per_point = 1_u64.saturating_add(
            m.saturating_mul(5)
                .saturating_add(1)
                .div_ceil(upper_denominator),
        );
        header_bytes
            .saturating_add(offset_tables)
            .saturating_add(level0_links)
            .saturating_add(vector_count.saturating_mul(upper_payload_per_point))
    }

    /// Durable footprint of the predicate-local hierarchical topology. Each
    /// configured scalar column independently partitions the full point
    /// domain and contributes at most `2 * filter_m` local links plus
    /// `filter_m` cross-block routing links per point. The published graph
    /// merges those links but remains separate from the base graph so
    /// unfiltered queries never pay this degree.
    pub fn estimate_filter_graph_memory_bytes(
        vector_count: u64,
        filter_columns: usize,
        target_block_rows: u32,
        filter_m: u32,
    ) -> u64 {
        if filter_columns == 0 {
            return 0;
        }
        let columns = filter_columns as u64;
        let offset_tables = vector_count
            .saturating_add(1)
            .saturating_mul(std::mem::size_of::<u64>() as u64)
            .saturating_mul(2);
        let links = vector_count
            .saturating_mul(columns)
            .saturating_mul(u64::from(filter_m).saturating_mul(3))
            .saturating_mul(std::mem::size_of::<u32>() as u64);
        let m = u64::from(filter_m.max(2));
        let upper_payload_per_point = 1_u64.saturating_add(
            m.saturating_mul(5)
                .saturating_add(1)
                .div_ceil(m - 1)
                .saturating_mul(columns),
        );
        let block_count = vector_count
            .div_ceil(u64::from(target_block_rows.max(1)))
            .saturating_add(1)
            .saturating_mul(columns);
        let entry_points = block_count.saturating_mul(12);
        64_u64
            .saturating_add(offset_tables)
            .saturating_add(links)
            .saturating_add(vector_count.saturating_mul(upper_payload_per_point))
            .saturating_add(entry_points)
    }

    /// Durable covering layout for exact filtered distance scans. Each
    /// configured filter column stores one row-id and one vector copy per
    /// point in scalar-block order; ordinal/block metadata is bounded by one
    /// additional u32 per point.
    pub fn estimate_filter_scan_layout_bytes(
        vector_count: u64,
        dimension: u32,
        filter_columns: usize,
    ) -> u64 {
        let columns = filter_columns as u64;
        if columns == 0 {
            return 0;
        }
        let vector_bytes = vector_count
            .saturating_mul(u64::from(dimension.max(1)))
            .saturating_mul(std::mem::size_of::<f32>() as u64);
        let row_and_ordinal_bytes = vector_count
            .saturating_mul(2)
            .saturating_mul(std::mem::size_of::<u32>() as u64);
        columns.saturating_mul(
            vector_bytes
                .saturating_add(row_and_ordinal_bytes)
                .saturating_add(64),
        )
    }

    /// Mutable graph-builder resident estimate. This belongs only to runtime
    /// admission/accounting and must not determine the durable segment shape.
    fn estimate_builder_graph_memory_bytes(vector_count: u64, m: u32) -> u64 {
        let m = u64::from(m.max(1));
        let link_bytes = std::mem::size_of::<u32>() as u64;
        let graph_links = vector_count
            .saturating_mul(m.saturating_mul(3))
            .saturating_mul(link_bytes);
        let outer_point_vectors =
            vector_count.saturating_mul(std::mem::size_of::<Vec<()>>() as u64);
        // With the standard HNSW level distribution, the expected number of
        // materialized levels is m/(m-1), including level zero.
        let expected_level_numerator = m.max(2);
        let expected_level_denominator = expected_level_numerator - 1;
        let expected_level_count = vector_count
            .saturating_mul(expected_level_numerator)
            .div_ceil(expected_level_denominator);
        let level_container_bytes =
            (std::mem::size_of::<std::sync::RwLock<crate::index::hnsw::LinksContainer>>() as u64)
                .saturating_add(16);
        let level_containers = expected_level_count.saturating_mul(level_container_bytes);
        graph_links
            .saturating_add(outer_point_vectors)
            .saturating_add(level_containers)
    }

    /// Runtime peak estimate used by admission and write-buffer governance.
    /// Unlike the placement estimate, this intentionally includes the actual
    /// process build width and transient decoded-vector frontier.
    pub fn estimate_build_peak_memory_bytes(
        vector_count: u64,
        dimension: u32,
        m: u32,
        build_width: usize,
    ) -> u64 {
        let visited_lists = vector_count.saturating_mul(build_width.max(1) as u64);
        let build_frontier = vector_count
            .saturating_mul(u64::from(dimension.max(1)))
            .saturating_mul(std::mem::size_of::<f32>() as u64);
        Self::estimate_builder_graph_memory_bytes(vector_count, m)
            .saturating_add(visited_lists)
            .saturating_add(build_frontier)
    }

    fn estimate_filter_build_peak_memory_bytes(
        vector_count: u64,
        dimension: u32,
        filter_columns: usize,
        filter_m: u32,
        build_width: usize,
    ) -> u64 {
        if filter_columns == 0 {
            return 0;
        }
        // A single equality posting may be larger than the target block size,
        // because equal values are never split. Use the full segment as the
        // local-builder upper bound; runtime admission must remain safe for a
        // constant-valued filter column.
        let max_block_rows = vector_count;
        let merged_containers = vector_count.saturating_mul(std::mem::size_of::<Vec<u32>>() as u64);
        let merged_links = vector_count
            .saturating_mul(filter_columns as u64)
            .saturating_mul(u64::from(filter_m).saturating_mul(3))
            .saturating_mul(std::mem::size_of::<u32>() as u64);
        let upper_level_containers = vector_count
            .saturating_mul(filter_columns as u64)
            .div_ceil(u64::from(filter_m.max(2)) - 1)
            .saturating_mul(std::mem::size_of::<Vec<u32>>() as u64);
        let block_membership = vector_count.saturating_mul(std::mem::size_of::<u32>() as u64);
        // The retained covering layout owns one vector copy for every point.
        // Building the current predicate-local graph also needs a temporary
        // row-id-ordered block copy because scan order must not perturb graph
        // topology. Both coexist at the block build peak.
        let graph_order_block_vectors = max_block_rows
            .saturating_mul(u64::from(dimension.max(1)))
            .saturating_mul(std::mem::size_of::<f32>() as u64);
        let local_visited = max_block_rows.saturating_mul(build_width.max(1) as u64);
        merged_containers
            .saturating_add(merged_links)
            .saturating_add(upper_level_containers)
            .saturating_add(block_membership)
            .saturating_add(Self::estimate_builder_graph_memory_bytes(
                max_block_rows,
                filter_m,
            ))
            .saturating_add(Self::estimate_filter_scan_layout_bytes(
                vector_count,
                dimension,
                filter_columns,
            ))
            .saturating_add(graph_order_block_vectors)
            .saturating_add(local_visited)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswInlineBuildEstimate {
    pub vector_count: u64,
    pub dimension: u32,
    pub estimated_graph_memory_bytes: u64,
    pub estimated_build_peak_memory_bytes: u64,
    pub threshold: HnswInlineThreshold,
}

impl HnswInlineBuildEstimate {
    pub fn from_definition(
        definition: &SearchIndexDefinition,
        vector_count: u64,
        dimension: u32,
    ) -> Result<Option<Self>> {
        if definition.kind != SearchIndexKind::Hnsw {
            return Ok(None);
        }
        let config = definition.hnsw_provider_config()?;
        if dimension != config.dimension {
            return Err(paro_error::invalid_input(format!(
                "HNSW inline dimension mismatch: definition={}, segment={dimension}",
                config.dimension
            )));
        }
        let threshold = HnswInlineThreshold {
            max_vector_count: config.inline_threshold.max_vector_count,
            max_graph_memory_bytes: config.inline_threshold.max_graph_memory_bytes,
            max_dimension: config.inline_threshold.max_dimension,
        };
        let metric_preprocessing_bytes =
            if config.distance == crate::index::hnsw::DistanceMetric::Cosine {
                vector_count.saturating_mul(std::mem::size_of::<f32>() as u64)
            } else {
                0
            };
        let estimated_graph_memory_bytes =
            HnswInlineThreshold::estimate_graph_memory_bytes(vector_count, config.m)
                .saturating_add(HnswInlineThreshold::estimate_filter_graph_memory_bytes(
                    vector_count,
                    config.filter_columns.len(),
                    config.filter_block_rows,
                    config.filter_m,
                ))
                .saturating_add(HnswInlineThreshold::estimate_filter_scan_layout_bytes(
                    vector_count,
                    dimension,
                    config.filter_columns.len(),
                ))
                .saturating_add(metric_preprocessing_bytes);
        let estimated_build_peak_memory_bytes =
            HnswInlineThreshold::estimate_build_peak_memory_bytes(
                vector_count,
                dimension,
                config.m,
                crate::index::hnsw::hnsw_build_thread_count(),
            )
            .saturating_add(
                HnswInlineThreshold::estimate_filter_build_peak_memory_bytes(
                    vector_count,
                    dimension,
                    config.filter_columns.len(),
                    config.filter_m,
                    crate::index::hnsw::hnsw_build_thread_count(),
                ),
            )
            .saturating_add(metric_preprocessing_bytes);
        Ok(Some(Self {
            vector_count,
            dimension,
            estimated_graph_memory_bytes,
            estimated_build_peak_memory_bytes,
            threshold,
        }))
    }

    pub const fn allows_inline(self) -> bool {
        self.threshold.allows(
            self.vector_count,
            self.estimated_graph_memory_bytes,
            self.dimension,
        )
    }

    /// Largest safe segment envelope for this vector shape.
    ///
    /// HNSW query quality and latency depend on graph locality, so the writer
    /// should not inherit the executor's 4K chunk size as a graph boundary.
    /// Derive a segment limit from both the configured vector-count ceiling
    /// and the graph-memory ceiling instead.
    pub fn max_segment_vector_count(self) -> u64 {
        let bytes_per_vector = self
            .estimated_graph_memory_bytes
            .saturating_add(self.vector_count.saturating_sub(1))
            / self.vector_count.max(1);
        let memory_bound = self.threshold.max_graph_memory_bytes / bytes_per_vector.max(1);
        self.threshold.max_vector_count.min(memory_bound).max(1)
    }
}

pub type SidecarArtifactFileId = ArtifactFileId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidecarArtifactLocation {
    pub file_id: SidecarArtifactFileId,
    pub offset: u64,
    pub len: u64,
    pub checksum: u64,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use paro_common::error::{self as paro_error, Result};
    use serde_json::json;

    use super::{
        AdmissionDecision, AdmissionGrant, AdmissionRejectReason, AdmissionWaitReason,
        FlushSearchMode, FullTextStatsDelta, HnswInlineBuildEstimate, HnswInlineThreshold,
        HnswStatsDelta, InlineAdmissionRequest, InlineArtifactBuilder, MaintenanceCost,
        ProviderConfig, ProviderStatsProfile, SearchAdmission, SearchFreshnessPolicy,
        SearchInlineBuilderEntry, SearchInlineBuilderSet, SearchStatsDelta, SegmentChunkSink,
        SegmentFlushCtx, SparseStatsDelta,
    };
    use crate::search::{SearchIndexDefinition, SearchIndexKind, HNSW_PROVIDER_CONFIG_VERSION};

    struct NoopInlineBuilder;

    impl InlineArtifactBuilder for NoopInlineBuilder {
        fn open_sink(&self, _ctx: &SegmentFlushCtx<'_>) -> Result<Box<dyn SegmentChunkSink>> {
            Err(paro_error::internal(
                "noop inline builder is only used as a typed test fixture",
            ))
        }
    }

    #[test]
    fn freshness_policy_maps_to_write_mode_without_provider_config_fingerprint() {
        assert_eq!(
            SearchFreshnessPolicy::Required.default_flush_mode(),
            FlushSearchMode::InlineRequired
        );
        assert_eq!(
            SearchFreshnessPolicy::BoundedLag {
                max_tail_rows: 128,
                max_lag_millis: 1_000,
            }
            .default_flush_mode(),
            FlushSearchMode::InlineIfAdmitted
        );
        assert_eq!(
            SearchFreshnessPolicy::Opportunistic.default_flush_mode(),
            FlushSearchMode::TailOnly
        );
    }

    #[test]
    fn hnsw_inline_threshold_uses_all_three_dimensions() {
        let threshold = HnswInlineThreshold::DEFAULT;
        assert!(threshold.allows(4_096, 64 * 1024 * 1024, 1_536));
        assert!(!threshold.allows(4_097, 64 * 1024 * 1024, 1_536));
        assert!(!threshold.allows(4_096, 64 * 1024 * 1024 + 1, 1_536));
        assert!(!threshold.allows(4_096, 64 * 1024 * 1024, 1_537));
    }

    #[test]
    fn hnsw_runtime_peak_accounts_for_workers_without_changing_graph_placement() {
        let points = 1_000;
        let graph = HnswInlineThreshold::estimate_graph_memory_bytes(points, 16);
        let builder_graph = HnswInlineThreshold::estimate_builder_graph_memory_bytes(points, 16);
        let serial = HnswInlineThreshold::estimate_build_peak_memory_bytes(points, 128, 16, 1);
        let width_32 = HnswInlineThreshold::estimate_build_peak_memory_bytes(points, 128, 16, 32);

        assert_eq!(width_32 - serial, points * 31);
        assert!(builder_graph > graph);
        assert_eq!(
            serial - builder_graph,
            points + points * 128 * std::mem::size_of::<f32>() as u64
        );
    }

    #[test]
    fn persisted_csr_budget_does_not_inherit_builder_container_overhead() {
        let points = 1_000_000;
        let resident = HnswInlineThreshold::estimate_graph_memory_bytes(points, 16);
        let builder = HnswInlineThreshold::estimate_build_peak_memory_bytes(
            points,
            32,
            16,
            crate::index::hnsw::hnsw_build_thread_count(),
        );
        let estimate = HnswInlineBuildEstimate {
            vector_count: 8_192,
            dimension: 32,
            estimated_graph_memory_bytes: HnswInlineThreshold::estimate_graph_memory_bytes(
                8_192, 16,
            ),
            estimated_build_peak_memory_bytes: 0,
            threshold: HnswInlineThreshold {
                max_vector_count: points,
                max_graph_memory_bytes: 512 * 1024 * 1024,
                max_dimension: 32,
            },
        };

        assert!(resident < 512 * 1024 * 1024);
        assert!(builder > resident);
        assert_eq!(estimate.max_segment_vector_count(), points);
    }

    #[test]
    fn hnsw_inline_threshold_comes_from_provider_config() {
        let definition = SearchIndexDefinition {
            definition_id: 1,
            table_id: 2,
            name: "vec_hnsw".to_string(),
            kind: SearchIndexKind::Hnsw,
            column_ids: vec![3],
            expression: None,
            provider_config: json!({
                "version": HNSW_PROVIDER_CONFIG_VERSION,
                "dimension": 4,
                "distance": "euclidean",
                "m": 8,
                "ef_construct": 64,
                "ef_search": 64,
                "plain_scan_threshold": 10_000,
                "filtered_plain_scan_threshold": 0,
                "build_seed": 1,
                "proposal_wave_size": crate::search::DEFAULT_HNSW_PROPOSAL_WAVE_SIZE,
                "warmup_point_count": crate::search::DEFAULT_HNSW_WARMUP_POINT_COUNT,
                "inline_threshold": {
                    "enabled": true,
                    "max_vector_count": 128,
                    "max_graph_memory_bytes": 64,
                    "max_dimension": 4
                }
            }),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
            config_fingerprint: 99,
        };

        let provider = definition.hnsw_provider_config().unwrap();
        let threshold = HnswInlineThreshold {
            max_vector_count: provider.inline_threshold.max_vector_count,
            max_graph_memory_bytes: provider.inline_threshold.max_graph_memory_bytes,
            max_dimension: provider.inline_threshold.max_dimension,
        };
        assert_eq!(threshold.max_vector_count, 128);
        assert_eq!(threshold.max_graph_memory_bytes, 64);
        assert_eq!(threshold.max_dimension, 4);

        let estimate = HnswInlineBuildEstimate::from_definition(&definition, 16, 4)
            .expect("valid hnsw config")
            .expect("hnsw estimate");
        assert_eq!(estimate.threshold, threshold);
        assert!(!estimate.allows_inline());

        let roomy = HnswInlineBuildEstimate {
            threshold: HnswInlineThreshold {
                max_vector_count: 16,
                max_graph_memory_bytes: estimate.estimated_graph_memory_bytes,
                max_dimension: 4,
            },
            ..estimate
        };
        assert!(roomy.allows_inline());
        assert_eq!(roomy.max_segment_vector_count(), 16);
        assert!(HnswInlineBuildEstimate {
            estimated_build_peak_memory_bytes: u64::MAX,
            ..roomy
        }
        .allows_inline());
    }

    #[test]
    fn builder_set_is_an_immutable_rowset_writer_contract() {
        let definition = SearchIndexDefinition {
            definition_id: 7,
            table_id: 11,
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![3],
            expression: None,
            provider_config: json!({"version": 1, "config": "simple"}),
            freshness_policy: SearchFreshnessPolicy::BoundedLag {
                max_tail_rows: 64,
                max_lag_millis: 250,
            },
            config_fingerprint: 99,
        };
        let entry = SearchInlineBuilderEntry::new(
            definition,
            13,
            SearchFreshnessPolicy::BoundedLag {
                max_tail_rows: 64,
                max_lag_millis: 250,
            },
            Arc::new(NoopInlineBuilder),
        );

        let set = SearchInlineBuilderSet::new(vec![entry], None);

        assert_eq!(set.len(), 1);
        assert!(!set.is_empty());
        assert!(set.admission().is_none());
        assert_eq!(
            set.entries()[0].flush_mode(),
            FlushSearchMode::InlineIfAdmitted
        );
    }

    #[test]
    fn provider_profile_compatibility_uses_all_stable_keys() {
        let definition = SearchIndexDefinition {
            definition_id: 7,
            table_id: 11,
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![3],
            expression: None,
            provider_config: json!({"tokenizer": "simple"}),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
            config_fingerprint: 99,
        };
        let config = ProviderConfig::from_definition(&definition);
        let profile = ProviderStatsProfile {
            provider_kind: SearchIndexKind::FullText,
            provider_variant: 1,
            artifact_format_version: 2,
            scoring_fingerprint: 3,
            config_fingerprint: 99,
        };

        assert!(profile.compatible_with_config(&config, 1, 2, 3));
        assert!(!profile.compatible_with_config(&config, 2, 2, 3));
        assert!(!profile.compatible_with_config(&config, 1, 3, 3));
        assert!(!profile.compatible_with_config(&config, 1, 2, 4));
        assert!(!profile.compatible_with_config(
            &ProviderConfig {
                config_fingerprint: 100,
                ..config
            },
            1,
            2,
            3
        ));
    }

    #[test]
    fn search_stats_delta_variants_keep_provider_stats_boundaries() {
        let fulltext = SearchStatsDelta::FullText(FullTextStatsDelta {
            stats: Default::default(),
        });
        let sparse = SearchStatsDelta::Sparse(SparseStatsDelta {
            row_count: 10,
            nnz: 32,
            posting_fanout: 7,
            unique_dimensions: 5,
            l2_norm_sum: 12.5,
            max_l2_norm: 3.0,
        });
        let hnsw = SearchStatsDelta::Hnsw(HnswStatsDelta {
            vector_count: 4,
            dimension: 128,
            max_level: 2,
            m: 16,
            ef_construction: 100,
            graph_memory_bytes: 4096,
            vector_storage_bytes: 2048,
            total_graph_links: 96,
            level0_graph_links: 64,
            avg_level0_degree: 16.0,
            max_level0_degree: 32,
        });

        assert!(fulltext.provider_stats().is_some());
        assert!(sparse.provider_stats().is_some());
        assert!(hnsw.provider_stats().is_some());

        let encoded = serde_json::to_value(&sparse).unwrap();
        let decoded: SearchStatsDelta = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, sparse);
    }

    struct RecordingAdmission;

    impl SearchAdmission for RecordingAdmission {
        fn request_inline_batch(
            &self,
            reqs: &[InlineAdmissionRequest],
        ) -> Result<Vec<AdmissionDecision>> {
            Ok(reqs
                .iter()
                .map(|req| match req.flush_mode {
                    FlushSearchMode::InlineRequired => AdmissionDecision::Proceed(AdmissionGrant {
                        budget: req.estimated_cost,
                        valid_until: Instant::now(),
                        grant_id: req.definition_id,
                    }),
                    FlushSearchMode::InlineIfAdmitted => AdmissionDecision::Wait {
                        deadline: Instant::now(),
                        reason: AdmissionWaitReason::MemoryBudget,
                    },
                    FlushSearchMode::TailOnly => AdmissionDecision::Reject {
                        reason: AdmissionRejectReason::InvalidRequest,
                    },
                })
                .collect())
        }

        fn release(&self, _grant_id: u64) {}
    }

    #[test]
    fn search_admission_batch_decisions_match_request_order() {
        let admission = RecordingAdmission;
        let reqs = vec![
            InlineAdmissionRequest {
                table_id: 1,
                definition_id: 11,
                provider: SearchIndexKind::FullText,
                flush_mode: FlushSearchMode::InlineRequired,
                estimated_cost: MaintenanceCost {
                    cpu_ns: 100,
                    ..Default::default()
                },
                row_count: 10,
                hnsw_inline: None,
            },
            InlineAdmissionRequest {
                table_id: 1,
                definition_id: 12,
                provider: SearchIndexKind::Sparse,
                flush_mode: FlushSearchMode::InlineIfAdmitted,
                estimated_cost: Default::default(),
                row_count: 10,
                hnsw_inline: None,
            },
            InlineAdmissionRequest {
                table_id: 1,
                definition_id: 13,
                provider: SearchIndexKind::Hnsw,
                flush_mode: FlushSearchMode::TailOnly,
                estimated_cost: Default::default(),
                row_count: 10,
                hnsw_inline: None,
            },
        ];

        let decisions = admission.request_inline_batch(&reqs).unwrap();

        assert_eq!(decisions.len(), reqs.len());
        assert!(matches!(
            decisions[0],
            AdmissionDecision::Proceed(AdmissionGrant { grant_id: 11, .. })
        ));
        assert!(matches!(
            decisions[1],
            AdmissionDecision::Wait {
                reason: AdmissionWaitReason::MemoryBudget,
                ..
            }
        ));
        assert!(matches!(
            decisions[2],
            AdmissionDecision::Reject {
                reason: AdmissionRejectReason::InvalidRequest,
            }
        ));
    }
}
