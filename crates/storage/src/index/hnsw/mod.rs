// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Index
//!
//! Hierarchical Navigable Small World (HNSW) index for dense vector search.
//!
//! Adapted for Paro's storage engine.

pub mod batch_scorer;
pub mod build_cache;
pub mod build_task;
pub mod builder;
pub mod compaction;
pub mod distance;
pub mod entry_points;
pub mod graph;
pub mod graph_links;
pub mod healer;
pub mod hnsw_builder;
pub mod links_container;
pub mod persistence;
pub mod scorer;
pub mod search_context;
pub mod types;
pub mod vector_storage;
pub mod visited_pool;

pub use batch_scorer::BatchScorer;
pub use build_cache::DistanceCache;
pub use build_task::{
    build_missing_hnsw_indexes_with_scheduler,
    build_missing_hnsw_indexes_with_scheduler_and_stop_check, HnswBuildSummary,
    HnswColumnBuildConfig,
};
pub use builder::GraphLayersBuilder;
pub use distance::DistanceMetric;
pub use entry_points::{EntryPoint, EntryPoints};
pub use graph::GraphLayers;
pub use graph_links::{GraphLinks, GraphLinksData};
pub use healer::GraphLayersHealer;
pub use hnsw_builder::{
    configure_hnsw_build_threads, HnswBuildExecutionPolicy, HnswBuildStopCheck, HnswBuilder,
};
pub use links_container::{ItemsBuffer, LinksContainer};
pub use persistence::{hnsw_artifact_compatibility, HnswArtifactCompatibility, HnswIndex};
pub use scorer::VectorScorer;
pub use search_context::{FixedLengthPriorityQueue, SearchContext};
pub use types::*;
pub use vector_storage::{
    InMemoryVectorStorage, IndexedVectorStorage, MmapVectorStorage, SharedVectorStorage,
    VectorStorage,
};
pub use visited_pool::{VisitedListHandle, VisitedPool};
