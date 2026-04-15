// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Graph index primitives for SQL/PGQ support.

pub mod adjacency_csr;
pub mod delta_adjacency;
pub mod graph_projection_index;
pub mod graph_snapshot;
pub mod graph_statistics;
pub mod index_manager;
pub mod vertex_id_map;

pub use adjacency_csr::{AdjacencyCSR, CSRData};
pub use delta_adjacency::{DeltaAdjacency, DeltaEdge};
pub use graph_projection_index::{
    EdgeBuildInput, GraphBuildInput, GraphManifest, GraphProjectionIndex, GraphState,
    VertexBuildInput,
};
pub use graph_snapshot::{
    GraphReadSnapshot, GraphRuntimeHandle, GraphStorageGeneration, NeighborView,
};
pub use graph_statistics::{
    DegreeHistogram, GraphStatistics, GraphStatsProvider, PatternStepStatistic,
};
pub use index_manager::{
    GraphProjectionIndexManager, GraphRecoveryCatalogEntry, GraphRecoveryResult,
};
pub use vertex_id_map::{LocalVertexId, VertexIdMap, VertexKey};
