// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable graph runtime generations and read snapshots.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::metrics::storage_metrics;

use super::{DeltaAdjacency, GraphManifest, GraphProjectionIndex, GraphStatistics};

/// Zero-allocation neighbor view when no delta is present; scratch-backed otherwise.
#[derive(Debug, Clone, Copy)]
pub enum NeighborView<'a> {
    Base {
        neighbors: &'a [u32],
        edge_rowids: &'a [u64],
    },
    Merged(&'a [(u32, u64)]),
}

impl<'a> NeighborView<'a> {
    pub fn len(&self) -> usize {
        match self {
            NeighborView::Base { neighbors, .. } => neighbors.len(),
            NeighborView::Merged(entries) => entries.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn pair_at(&self, idx: usize) -> Option<(u32, u64)> {
        match self {
            NeighborView::Base {
                neighbors,
                edge_rowids,
            } => neighbors
                .get(idx)
                .zip(edge_rowids.get(idx))
                .map(|(&neighbor, &edge_rowid)| (neighbor, edge_rowid)),
            NeighborView::Merged(entries) => entries.get(idx).copied(),
        }
    }
}

/// Immutable graph storage generation published by the runtime handle.
#[derive(Debug)]
pub struct GraphStorageGeneration {
    pub base: Arc<GraphProjectionIndex>,
    pub committed_edge_deltas: HashMap<String, Arc<DeltaAdjacency>>,
    pub manifest: GraphManifest,
    pub statistics: Arc<GraphStatistics>,
    pub generation: u64,
}

impl GraphStorageGeneration {
    pub fn new(
        base: Arc<GraphProjectionIndex>,
        manifest: GraphManifest,
        generation: u64,
        committed_edge_deltas: HashMap<String, Arc<DeltaAdjacency>>,
        statistics: Arc<GraphStatistics>,
    ) -> Self {
        Self {
            base,
            committed_edge_deltas,
            manifest,
            statistics,
            generation,
        }
    }

    pub fn from_index(
        index: GraphProjectionIndex,
        manifest: GraphManifest,
        generation: u64,
    ) -> Self {
        let statistics = manifest
            .statistics()
            .cloned()
            .unwrap_or_else(|| GraphStatistics::from_index(&index));
        Self::new(
            Arc::new(index),
            manifest,
            generation,
            HashMap::new(),
            Arc::new(statistics),
        )
    }

    pub fn neighbors_forward<'a>(
        &'a self,
        edge_label: &str,
        v: u32,
        scratch: &'a mut Vec<(u32, u64)>,
    ) -> Option<NeighborView<'a>> {
        let base = self.base.forward_csr(edge_label)?;
        let delta_hit = self
            .committed_edge_deltas
            .get(edge_label)
            .map(|delta| !delta.is_empty())
            .unwrap_or(false);
        storage_metrics().record_graph_delta_lookup(delta_hit);
        match self.committed_edge_deltas.get(edge_label) {
            Some(delta) if delta_hit => {
                delta.fill_neighbors_merged_forward(v, base, scratch);
                Some(NeighborView::Merged(scratch.as_slice()))
            }
            _ => Some(NeighborView::Base {
                neighbors: base.neighbors(v),
                edge_rowids: base.edge_rowids_for(v),
            }),
        }
    }

    pub fn neighbors_backward<'a>(
        &'a self,
        edge_label: &str,
        v: u32,
        scratch: &'a mut Vec<(u32, u64)>,
    ) -> Option<NeighborView<'a>> {
        let base = self.base.backward_csr(edge_label)?;
        let delta_hit = self
            .committed_edge_deltas
            .get(edge_label)
            .map(|delta| !delta.is_empty())
            .unwrap_or(false);
        storage_metrics().record_graph_delta_lookup(delta_hit);
        match self.committed_edge_deltas.get(edge_label) {
            Some(delta) if delta_hit => {
                delta.fill_neighbors_merged_backward(v, base, scratch);
                Some(NeighborView::Merged(scratch.as_slice()))
            }
            _ => Some(NeighborView::Base {
                neighbors: base.neighbors(v),
                edge_rowids: base.edge_rowids_for(v),
            }),
        }
    }
}

/// Per-graph runtime handle that atomically swaps immutable generations.
#[derive(Debug)]
pub struct GraphRuntimeHandle {
    current: RwLock<Arc<GraphStorageGeneration>>,
}

impl GraphRuntimeHandle {
    pub fn new(initial: GraphStorageGeneration) -> Self {
        Self {
            current: RwLock::new(Arc::new(initial)),
        }
    }

    pub fn current_generation(&self) -> Arc<GraphStorageGeneration> {
        self.current.read().unwrap().clone()
    }

    pub fn snapshot(&self) -> GraphReadSnapshot {
        GraphReadSnapshot {
            generation: self.current_generation(),
        }
    }

    pub fn publish(&self, next: GraphStorageGeneration) -> Arc<GraphStorageGeneration> {
        let next = Arc::new(next);
        *self.current.write().unwrap() = next.clone();
        next
    }
}

/// Immutable query-time pin of a graph storage generation.
#[derive(Debug, Clone)]
pub struct GraphReadSnapshot {
    generation: Arc<GraphStorageGeneration>,
}

impl GraphReadSnapshot {
    pub fn generation_id(&self) -> u64 {
        self.generation.generation
    }

    pub fn base(&self) -> &Arc<GraphProjectionIndex> {
        &self.generation.base
    }

    pub fn manifest(&self) -> &GraphManifest {
        &self.generation.manifest
    }

    pub fn statistics(&self) -> &Arc<GraphStatistics> {
        &self.generation.statistics
    }

    pub fn generation(&self) -> &Arc<GraphStorageGeneration> {
        &self.generation
    }

    pub fn delta_size(&self) -> usize {
        self.generation
            .committed_edge_deltas
            .values()
            .map(|delta| delta.added_count() + delta.deleted_count() as usize)
            .sum()
    }

    pub fn neighbors_forward<'a>(
        &'a self,
        edge_label: &str,
        v: u32,
        scratch: &'a mut Vec<(u32, u64)>,
    ) -> Option<NeighborView<'a>> {
        self.generation.neighbors_forward(edge_label, v, scratch)
    }

    pub fn neighbors_backward<'a>(
        &'a self,
        edge_label: &str,
        v: u32,
        scratch: &'a mut Vec<(u32, u64)>,
    ) -> Option<NeighborView<'a>> {
        self.generation.neighbors_backward(edge_label, v, scratch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::graph::{
        EdgeBuildInput, GraphBuildInput, GraphState, VertexBuildInput, VertexKey,
    };
    use crate::metrics::storage_metrics;

    fn build_index() -> GraphProjectionIndex {
        GraphProjectionIndex::build(&GraphBuildInput {
            graph_name: "g".to_string(),
            vertex_tables: vec![VertexBuildInput {
                label: "Person".to_string(),
                keys_and_rowids: vec![
                    (VertexKey::Int64(1), 101),
                    (VertexKey::Int64(2), 102),
                    (VertexKey::Int64(3), 103),
                ],
            }],
            edge_tables: vec![EdgeBuildInput {
                label: "Knows".to_string(),
                source_vertex_label: "Person".to_string(),
                destination_vertex_label: "Person".to_string(),
                edges: vec![
                    (VertexKey::Int64(1), VertexKey::Int64(2), 9001),
                    (VertexKey::Int64(2), VertexKey::Int64(3), 9002),
                ],
            }],
            build_backward_adjacency: true,
        })
        .unwrap()
    }

    #[test]
    #[serial_test::serial]
    fn neighbor_view_uses_base_slices_when_delta_is_empty() {
        let generation = GraphStorageGeneration::from_index(
            build_index(),
            GraphManifest::new("g".to_string(), GraphState::Ready, "fp:test".to_string()),
            1,
        );
        let snapshot = GraphRuntimeHandle::new(generation).snapshot();
        let mut scratch = Vec::new();

        let view = snapshot
            .neighbors_forward("Knows", 0, &mut scratch)
            .unwrap();
        match view {
            NeighborView::Base {
                neighbors,
                edge_rowids,
            } => {
                assert_eq!(neighbors, &[1]);
                assert_eq!(edge_rowids, &[9001]);
            }
            NeighborView::Merged(_) => panic!("expected base neighbor view"),
        }
    }

    #[test]
    fn snapshot_pins_generation_until_query_finishes() {
        let handle = GraphRuntimeHandle::new(GraphStorageGeneration::from_index(
            build_index(),
            GraphManifest::new("g".to_string(), GraphState::Ready, "fp:test".to_string()),
            1,
        ));
        let snapshot = handle.snapshot();

        let mut next_generation = GraphStorageGeneration::from_index(
            build_index(),
            GraphManifest::new("g".to_string(), GraphState::Ready, "fp:test".to_string()),
            2,
        );
        let mut delta = DeltaAdjacency::new();
        delta.add_edge_by_local_id(0, 2, 9003);
        next_generation
            .committed_edge_deltas
            .insert("Knows".to_string(), Arc::new(delta));
        handle.publish(next_generation);

        assert_eq!(snapshot.generation_id(), 1);
        assert_eq!(handle.snapshot().generation_id(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn delta_lookup_metrics_track_hit_ratio() {
        storage_metrics().reset_for_tests();

        let handle = GraphRuntimeHandle::new(GraphStorageGeneration::from_index(
            build_index(),
            GraphManifest::new("g".to_string(), GraphState::Ready, "fp:test".to_string()),
            1,
        ));

        let mut next_generation = GraphStorageGeneration::from_index(
            build_index(),
            GraphManifest::new("g".to_string(), GraphState::Ready, "fp:test".to_string()),
            2,
        );
        let mut delta = DeltaAdjacency::new();
        delta.add_edge_by_local_id(0, 2, 9003);
        next_generation
            .committed_edge_deltas
            .insert("Knows".to_string(), Arc::new(delta));
        handle.publish(next_generation);

        let snapshot = handle.snapshot();
        let mut scratch = Vec::new();
        let _ = snapshot.neighbors_forward("Knows", 0, &mut scratch);

        let metrics = storage_metrics().snapshot();
        assert_eq!(metrics.graph_delta_lookups, 1);
        assert_eq!(metrics.graph_delta_hits, 1);
        assert!((metrics.graph_delta_hit_ratio() - 1.0).abs() < f64::EPSILON);
        assert_eq!(snapshot.delta_size(), 1);
    }
}
