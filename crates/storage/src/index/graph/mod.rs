// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Graph index primitives for SQL/PGQ support.

use std::sync::{LazyLock, Mutex, MutexGuard};

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

static GRAPH_ARTIFACT_IO_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Serialize graph artifact publication, replacement, and removal.
///
/// Background rebuilds and transactional DDL both replace the stable graph
/// directory. They must share one ownership boundary so a stale rebuild cannot
/// race a committed DROP or CREATE.
pub fn lock_graph_artifact_io() -> MutexGuard<'static, ()> {
    GRAPH_ARTIFACT_IO_GUARD
        .lock()
        .expect("graph artifact I/O mutex poisoned")
}
