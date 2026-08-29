// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Index
//!
//! Hierarchical Navigable Small World (HNSW) index for dense vector search.
//!
//! Adapted for Paro's storage engine.

mod artifact_integrity;
pub mod batch_scorer;
pub mod build_cache;
pub mod build_task;
pub mod builder;
pub mod compaction;
pub mod diagnostics;
pub mod distance;
pub mod entry_points;
pub mod graph;
pub mod graph_links;
pub mod healer;
pub mod hnsw_builder;
mod integrity_scheduler;
pub mod links_container;
pub mod persistence;
mod predicate_scan;
pub mod scorer;
pub mod search_context;
pub mod types;
pub mod vector_storage;
pub mod visited_pool;

pub use batch_scorer::{BatchScoredResult, BatchScorer};
pub use build_cache::DistanceCache;
pub use build_task::{
    build_missing_hnsw_indexes_with_scheduler,
    build_missing_hnsw_indexes_with_scheduler_and_stop_check, HnswBuildSummary,
    HnswColumnBuildConfig,
};
pub use builder::GraphLayersBuilder;
pub use diagnostics::{
    HnswDegreeSampleSummary, HnswGraphDiagnostics, HnswGraphQualityReport,
    HnswTruthIndegreeComparison,
};
pub use distance::DistanceMetric;
pub use entry_points::{EntryPoint, EntryPoints, PredicateEntryPoint};
pub use graph::GraphLayers;
pub use graph_links::{GraphLinks, GraphLinksData};
pub use healer::GraphLayersHealer;
pub use hnsw_builder::{
    configure_hnsw_build_threads, HnswBuildExecutionPolicy, HnswBuildStopCheck, HnswBuilder,
};
pub(crate) use hnsw_builder::{
    hnsw_build_thread_count, hnsw_foreground_pressure_active, HnswQueryActivity,
};
pub use integrity_scheduler::HnswIntegrityScheduler;
pub use links_container::{ItemsBuffer, LinksContainer};
pub(crate) use persistence::{
    hnsw_artifact_build_contract, hnsw_artifact_uses_external_vectors, HnswExternalVectorBinding,
    HnswExternalVectorSource, HnswExternalVectorSpan, HNSW_ARTIFACT_HEADER_LEN,
};
pub use persistence::{
    hnsw_artifact_compatibility, HnswArtifactCompatibility, HnswFilterBlock, HnswFilterBlocks,
    HnswFilterColumnBlocks, HnswIndex, HNSW_ARTIFACT_ALIGNMENT, HNSW_ARTIFACT_FORMAT_VERSION,
};
pub use predicate_scan::PredicateScanLayout;
pub(crate) use predicate_scan::PREDICATE_SCAN_BUILD_STREAM_BYTES;
pub(crate) use scorer::GraphVectorScorer;
pub use scorer::VectorScorer;
pub use search_context::{FixedLengthPriorityQueue, SearchContext};
pub use types::*;
pub(crate) use vector_storage::{open_plain_vector_column, PartitionedVectorStorage};
pub use vector_storage::{
    InMemoryVectorStorage, IndexedVectorStorage, MmapVectorStorage, SharedVectorStorage,
    VectorStorage,
};
pub(crate) use visited_pool::build_visited_workspace_bytes;
pub use visited_pool::{VisitedListHandle, VisitedPool};
