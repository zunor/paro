// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared search contracts.
//!
//! Phase 0 establishes a single storage-owned contract surface that later
//! planner/executor/storage refactors can converge on. The provider-specific
//! batch implementations still live here temporarily as crate-private modules,
//! but new cross-layer work should build on the typed contracts re-exported
//! below rather than on the legacy runtime/declared helpers.

pub mod artifact;
pub mod budget;
pub mod capability;
pub mod cursor;
pub mod request;
pub mod stats;
pub mod tail;
pub mod telemetry;

pub use artifact::{ArtifactGcContext, ArtifactGcPolicy, ArtifactLocation, GcDecision};
pub use budget::{ResourceBudget, ResourceContext, SearchBatchConfig};
pub use capability::{
    ArtifactSegmentRef, CoverageState, SearchArtifactRef, SearchCapability, SearchGeneration,
    SearchIndexDefinition, SearchIndexKind, SearchPlanCandidate, SequentialCapability,
};
pub use cursor::{
    CandidateBatch, GenerationArtifactSet, GenerationReadLease, GenerationReadSnapshot,
    OpenedSearchCursor, PhysicalRowRef, SearchBatchState, SearchCursor, SearchProvider,
    SearchReadSnapshot, SearchRowHandle, TableReadLease, TableReadSnapshot,
};
pub use registry::{
    DefinitionMaintenanceReport, SearchBootstrapReport, SearchGenerationCoverage,
    SearchMaintenanceAction, SearchMaintenanceReport,
};
pub use request::{
    analyze_fulltext_query_stats, build_fulltext_query_stats, normalize_fulltext_config,
    FullTextIntent, FullTextQueryKind, FullTextQueryStats, FullTextScoreMode, FusionStrategy,
    HnswIntent, NormalizedSearchRequest, ProjectionSpec, SearchIntent, SearchRequestMode,
    SparseIntent,
};
pub use stats::{
    BuildEpoch, BuildWatermarks, CatchUpBacklogTier, ConfigFingerprint, ExecutionModes,
    FullTextProviderStats, GenerationMaintenanceState, GenerationRecoveryState, GenerationStats,
    MaintenancePriority, PreferHint, ProviderVariantId, SearchArtifactStats, SearchCostEstimate,
    SearchDefinitionId, SearchExecutionMode, SearchGenerationId, SearchProviderStats,
    SearchSourceId, SegmentId, TableId,
};
pub use tail::{
    provider_tail_merge_policy, ExactTailMergePolicy, TailMutationKind, TailPendingEntry,
    TailPendingSet, TailRowImageRef,
};

pub(crate) mod fulltext_search;
pub(crate) mod manifest;
pub(crate) mod registry;
pub(crate) mod row_fetch;
pub(crate) mod segment_dispatch;
pub(crate) mod sparse_search;
pub(crate) mod tail_merge;
pub(crate) mod topk_merge;
pub(crate) mod vector_search;
pub(crate) mod write_path;
