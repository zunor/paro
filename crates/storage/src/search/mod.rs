// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared search contracts.
//!
//! Phase 0 establishes a single storage-owned contract surface that later
//! planner/executor/storage refactors can converge on. Provider implementations
//! live under `providers/`; new cross-layer work should build on the typed
//! contracts re-exported below rather than on legacy runtime/declared helpers.

pub mod artifact;
#[doc(hidden)]
pub mod bench_support;
pub mod budget;
pub mod capability;
pub mod cursor;
pub(crate) mod definition;
pub(crate) mod generation;
pub mod hnsw_config;
pub mod inline_sink;
pub(crate) mod lifecycle;
pub mod maintenance;
pub mod posting_stream;
pub mod provider_config;
pub(crate) mod providers;
pub mod qualification;
pub mod request;
pub mod sidecar;
pub(crate) mod sidecar_builder;
mod staged_generation;
pub mod stats;
pub mod tail;
pub mod telemetry;

pub use artifact::{
    ArtifactFileId, ArtifactGcContext, ArtifactGcPolicy, ArtifactLocation, GcDecision,
    SegmentPagePointer,
};
pub use budget::{
    ResourceBudget, ResourceContext, SearchBatchConfig, SearchCancellation, SearchMemoryAccountant,
    SearchMemoryReservation, SearchWorkBudget,
};
pub use capability::{
    ArtifactPointRef, ArtifactSegmentRef, ArtifactSegmentSpan, CapabilityToken, CoverageState,
    SearchArtifactRef, SearchCapability, SearchCapabilityState, SearchDefinitionOrigin,
    SearchFreshnessPolicy, SearchGeneration, SearchIndexDefinition, SearchIndexKind,
    SearchNotQueryableReason, SearchPartitionCoverage, SearchPlanCandidate, SearchTailSummary,
    SequentialCapability,
};
pub use cursor::{
    CandidateBatch, GenerationArtifactSet, GenerationReadLease, GenerationReadSnapshot,
    OpenSearchCursorResult, OpenedSearchCursor, PhysicalRowRef, SearchBatchState, SearchCursor,
    SearchProvider, SearchReadOptions, SearchReadSnapshot, SearchRowHandle, TableReadLease,
    TableReadSnapshot,
};
pub use generation::coverage::SearchGenerationCoverage;

/// Search-generation objects reclaimed after durable recovery replay.
///
/// Workspaces are private pre-commit build directories. Manifest fragments
/// are immutable installed revisions that are not reachable from any durable
/// generation head. Keeping the counts separate makes recovery telemetry
/// distinguish abandoned work from normal revision reclamation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchGenerationOrphanSweepReport {
    pub staging_workspaces: usize,
    pub manifest_fragments: usize,
}

impl SearchGenerationOrphanSweepReport {
    pub fn total_removed(self) -> usize {
        self.staging_workspaces
            .saturating_add(self.manifest_fragments)
    }
}
pub use hnsw_config::{
    HnswInlineConfig, HnswProviderConfig, DEFAULT_HNSW_BUILD_SEED, DEFAULT_HNSW_EF_CONSTRUCT,
    DEFAULT_HNSW_EF_SEARCH, DEFAULT_HNSW_EXACT_F32_DIMENSION_COST_UNITS,
    DEFAULT_HNSW_FILTER_BLOCK_ROWS, DEFAULT_HNSW_FILTER_M, DEFAULT_HNSW_GRAPH_SCORED_POINTS_PER_EF,
    DEFAULT_HNSW_M, DEFAULT_HNSW_PROPOSAL_WAVE_SIZE, DEFAULT_HNSW_RANDOM_ACCESS_COST_UNITS,
    DEFAULT_HNSW_SEQUENTIAL_DIMENSION_COST_UNITS, DEFAULT_HNSW_SYMMETRIC_I16_DIMENSION_COST_UNITS,
    DEFAULT_HNSW_WARMUP_POINT_COUNT, HNSW_PROVIDER_CONFIG_VERSION,
};
pub use inline_sink::{
    AdmissionDecision, AdmissionGrant, AdmissionRejectReason, AdmissionWaitReason, BuildBudget,
    CostEstimate, FlushSearchMode, FullTextStatsDelta, HnswInlineBuildEstimate,
    HnswInlineThreshold, HnswStatsDelta, InlineAdmissionRequest, InlineArtifactBlob,
    InlineArtifactBuildResult, InlineArtifactBuilder, MaintenanceBenefit, MaintenanceCost,
    ProviderConfig, ProviderStatsProfile, SearchAdmission, SearchBuildStopCheck,
    SearchInlineBuilderEntry, SearchInlineBuilderSet, SearchStatsDelta, SegmentChunkInput,
    SegmentChunkSink, SegmentFlushCtx, SegmentSinkSavepoint, SidecarArtifactBuildResult,
    SidecarArtifactBuilder, SidecarArtifactFileId, SidecarArtifactLocation, SidecarBuildInput,
    SparseStatsDelta,
};
pub use lifecycle::bootstrap::SearchBootstrapReport;
pub use maintenance::{
    DefinitionMaintenanceReport, HnswMaintenanceRequest, HnswMaintenanceRowsetRef,
    MaintenanceAdmissionDecision, MaintenanceAdmissionReason, MaintenanceFairnessKey,
    ProviderMaintenanceRequest, SearchMaintenanceAction, SearchMaintenanceFailure,
    SearchMaintenanceReport, SearchMaintenanceUrgency,
};
pub use posting_stream::{
    CandidateStreamStep, PostingCandidateStream, PostingPruningHint, SearchScore,
};
pub use provider_config::{
    FullTextProviderConfig, SparsePhysicalEncoding, SparseProviderConfig,
    FULLTEXT_PROVIDER_CONFIG_VERSION, SPARSE_PROVIDER_CONFIG_VERSION,
};
pub use providers::fulltext::inline::FullTextInlineArtifactBuilder;
pub use providers::sparse::inline::SparseInlineArtifactBuilder;
pub use qualification::{qualify_hnsw_generation, HnswGenerationQualification};
pub use request::{
    analyze_fulltext_query_stats, build_fulltext_query_stats, normalize_fulltext_config,
    DenseVectorQuery, ExactFilterMaterialization, FullTextIntent, FullTextQueryKind,
    FullTextQueryStats, FullTextScoreMode, FusionStrategy, HnswIntent, NormalizedSearchRequest,
    ProjectionSpec, SearchIntent, SearchRequestMode, SparseIntent,
};
pub use sidecar::{
    DecodedSidecarArtifactKey, SearchReaderRuntime, SidecarArtifactStore, SidecarCachedArtifact,
    SidecarIntegrityPolicy, SidecarMappedPackage, SidecarPackageWriter, SidecarReaderCache,
    SidecarReaderCacheKey, SidecarReaderRequest, SIDECAR_PACKAGE_CODEC,
};
pub use staged_generation::StagedSearchGeneration;
pub use stats::{
    BuildEpoch, BuildWatermarks, CatchUpBacklogTier, ConfigFingerprint, ExecutionModes,
    FullTextProviderStats, GenerationMaintenanceState, GenerationRecoveryState, GenerationStats,
    MaintenancePriority, PreferHint, ProviderVariantId, SearchArtifactStats, SearchCostEstimate,
    SearchDefinitionId, SearchExecutionMode, SearchGenerationId, SearchProviderStats,
    SearchSourceId, SegmentId, TableId,
};
pub use tail::exact_merge::{
    TailExactMergeBudget, TailExactMergeCost, TailExactMergeQueryShape, TailWindow,
};
pub use tail::{
    provider_tail_exact_merge_policy, TailEntryId, TailExactMergePolicy, TailMutationKind,
    TailPendingEntry, TailPendingSet, TailRowImageRef,
};
pub use telemetry::{
    SearchMetricDescriptor, SearchMetricDimension, SearchMetricType, SearchMetricUnit,
    SEARCH_BUILD_LATENCY_BUCKETS_US, SEARCH_LATENCY_BUCKETS_US, SEARCH_METRIC_DESCRIPTORS,
};

pub(crate) mod manifest;
pub(crate) mod registry;
pub(crate) mod row_fetch;
pub(crate) mod segment_dispatch;
pub(crate) mod tail_merge;
pub(crate) mod topk_merge;
pub(crate) mod write_path;
