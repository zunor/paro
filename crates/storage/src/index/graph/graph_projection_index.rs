// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Graph projection index combines vertex id maps and edge adjacency.

use super::{AdjacencyCSR, GraphStatistics, VertexIdMap, VertexKey};
use paro_common::error as paro_error;
use paro_common::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const GRAPH_PROJECTION_META_FILE: &str = "meta.json";
const GRAPH_PROJECTION_META_VERSION_V1: u32 = 1;
const GRAPH_PROJECTION_META_VERSION_V2: u32 = 2;

/// Input for building a graph projection index.
#[derive(Debug, Clone, Default)]
pub struct GraphBuildInput {
    pub graph_name: String,
    pub vertex_tables: Vec<VertexBuildInput>,
    pub edge_tables: Vec<EdgeBuildInput>,
    pub build_backward_adjacency: bool,
}

/// Input for one vertex label map.
#[derive(Debug, Clone)]
pub struct VertexBuildInput {
    pub label: String,
    pub keys_and_rowids: Vec<(VertexKey, u64)>,
}

/// Input for one edge label adjacency.
#[derive(Debug, Clone)]
pub struct EdgeBuildInput {
    pub label: String,
    pub source_vertex_label: String,
    pub destination_vertex_label: String,
    pub edges: Vec<(VertexKey, VertexKey, u64)>,
}

#[derive(Debug, Clone)]
struct EdgeEndpoints {
    source_vertex_label: String,
    destination_vertex_label: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GraphState {
    Building,
    Ready,
    Rebuilding,
    Stale,
    Failed,
}

/// In-memory graph projection index.
#[derive(Debug, Default)]
pub struct GraphProjectionIndex {
    graph_name: String,
    /// per vertex label -> id map
    vertex_maps: HashMap<String, VertexIdMap>,
    /// per edge label -> forward adjacency
    forward_adjacency: HashMap<String, AdjacencyCSR>,
    /// per edge label -> backward adjacency
    backward_adjacency: HashMap<String, AdjacencyCSR>,
    edge_endpoints: HashMap<String, EdgeEndpoints>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphProjectionMeta {
    version: u32,
    graph_name: String,
    vertices: Vec<VertexFileMeta>,
    edges: Vec<EdgeFileMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct VertexFileMeta {
    label: String,
    file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct EdgeFileMeta {
    label: String,
    source_vertex_label: String,
    destination_vertex_label: String,
    forward_file: String,
    backward_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphManifest {
    version: u32,
    graph_name: String,
    state: GraphState,
    schema_fingerprint: String,
    created_at_epoch_ms: u64,
    updated_at_epoch_ms: u64,
    #[serde(default)]
    last_rebuild_epoch_ms: u64,
    vertices: Vec<VertexFileMeta>,
    edges: Vec<EdgeFileMeta>,
    #[serde(default)]
    statistics: Option<GraphStatistics>,
}

impl GraphProjectionIndex {
    /// Build index from vertex/edge data.
    pub fn build(info: &GraphBuildInput) -> Result<Self> {
        if info.graph_name.trim().is_empty() {
            return Err(paro_error::invalid_input(
                "GraphProjectionIndex: graph_name is empty",
            ));
        }

        let mut vertex_maps = HashMap::with_capacity(info.vertex_tables.len());
        for vertex in &info.vertex_tables {
            validate_label(&vertex.label, "vertex label")?;
            if vertex_maps.contains_key(&vertex.label) {
                return Err(paro_error::invalid_input(format!(
                    "GraphProjectionIndex: duplicate vertex label '{}'",
                    vertex.label
                )));
            }
            let map = VertexIdMap::build(vertex.keys_and_rowids.clone());
            vertex_maps.insert(vertex.label.clone(), map);
        }

        let mut forward_adjacency = HashMap::with_capacity(info.edge_tables.len());
        let mut backward_adjacency = HashMap::with_capacity(info.edge_tables.len());
        let mut edge_endpoints = HashMap::with_capacity(info.edge_tables.len());

        for edge in &info.edge_tables {
            validate_label(&edge.label, "edge label")?;
            validate_label(&edge.source_vertex_label, "source vertex label")?;
            validate_label(&edge.destination_vertex_label, "destination vertex label")?;

            if forward_adjacency.contains_key(&edge.label) {
                return Err(paro_error::invalid_input(format!(
                    "GraphProjectionIndex: duplicate edge label '{}'",
                    edge.label
                )));
            }

            let source_map = vertex_maps.get(&edge.source_vertex_label).ok_or_else(|| {
                paro_error::invalid_input(format!(
                    "GraphProjectionIndex: source vertex label '{}' not found for edge '{}'",
                    edge.source_vertex_label, edge.label
                ))
            })?;
            let destination_map =
                vertex_maps
                    .get(&edge.destination_vertex_label)
                    .ok_or_else(|| {
                        paro_error::invalid_input(format!(
                    "GraphProjectionIndex: destination vertex label '{}' not found for edge '{}'",
                    edge.destination_vertex_label, edge.label
                ))
                    })?;

            let num_vertices = source_map
                .num_vertices()
                .max(destination_map.num_vertices());
            let mut forward_edges = Vec::with_capacity(edge.edges.len());
            for (source_key, destination_key, edge_rowid) in &edge.edges {
                let source_local = source_map.key_to_local(source_key).ok_or_else(|| {
                    paro_error::invalid_input(format!(
                        "GraphProjectionIndex: edge '{}' source key not found in vertex label '{}': {:?}",
                        edge.label, edge.source_vertex_label, source_key
                    ))
                })?;
                let destination_local = destination_map.key_to_local(destination_key).ok_or_else(|| {
                    paro_error::invalid_input(format!(
                        "GraphProjectionIndex: edge '{}' destination key not found in vertex label '{}': {:?}",
                        edge.label, edge.destination_vertex_label, destination_key
                    ))
                })?;
                forward_edges.push((source_local, destination_local, *edge_rowid));
            }

            let forward = AdjacencyCSR::build(&mut forward_edges, num_vertices);
            forward_adjacency.insert(edge.label.clone(), forward);

            if info.build_backward_adjacency {
                let backward = AdjacencyCSR::build_reverse(&forward_edges, num_vertices);
                backward_adjacency.insert(edge.label.clone(), backward);
            }

            edge_endpoints.insert(
                edge.label.clone(),
                EdgeEndpoints {
                    source_vertex_label: edge.source_vertex_label.clone(),
                    destination_vertex_label: edge.destination_vertex_label.clone(),
                },
            );
        }

        Ok(Self {
            graph_name: info.graph_name.clone(),
            vertex_maps,
            forward_adjacency,
            backward_adjacency,
            edge_endpoints,
        })
    }

    /// Get forward CSR by edge label.
    pub fn forward_csr(&self, edge_label: &str) -> Option<&AdjacencyCSR> {
        self.forward_adjacency.get(edge_label)
    }

    /// Get backward CSR by edge label.
    pub fn backward_csr(&self, edge_label: &str) -> Option<&AdjacencyCSR> {
        self.backward_adjacency.get(edge_label)
    }

    /// Get vertex id map by label.
    pub fn vertex_map(&self, vertex_label: &str) -> Option<&VertexIdMap> {
        self.vertex_maps.get(vertex_label)
    }

    pub fn graph_name(&self) -> &str {
        &self.graph_name
    }

    /// Save index files under a directory.
    pub fn save(&self, dir: &Path) -> Result<()> {
        let manifest =
            GraphManifest::new(self.graph_name.clone(), GraphState::Ready, String::new());
        self.save_with_manifest(dir, manifest)
    }

    pub fn save_with_manifest(&self, dir: &Path, manifest: GraphManifest) -> Result<()> {
        fs::create_dir_all(dir)?;
        let (vertices_meta, edges_meta) = self.write_index_files(dir)?;
        let manifest = manifest
            .with_files(self.graph_name.clone(), vertices_meta, edges_meta)
            .ensure_statistics(|| GraphStatistics::from_index(self));
        Self::write_manifest(dir, &manifest)?;
        Ok(())
    }

    /// Load index from directory. CSR files are mmap-backed when possible.
    pub fn load(dir: &Path) -> Result<Self> {
        match Self::read_meta_file(dir)? {
            GraphMetaFile::V1(meta) => Self::load_from_v1_meta(dir, meta),
            GraphMetaFile::V2(manifest) => Self::load_from_manifest(dir, manifest),
        }
    }

    pub fn manifest_version(dir: &Path) -> Result<u32> {
        Self::read_meta_version(dir)?.ok_or_else(|| {
            paro_error::invalid_input("GraphProjectionIndex: meta.json is missing version field")
        })
    }

    pub fn load_manifest(dir: &Path) -> Result<GraphManifest> {
        match Self::read_meta_file(dir)? {
            GraphMetaFile::V2(manifest) => Ok(manifest),
            GraphMetaFile::V1(_) => Err(paro_error::invalid_input(
                "GraphProjectionIndex: legacy v1 meta requires upgrade before recovery",
            )),
        }
    }

    pub fn write_manifest(dir: &Path, manifest: &GraphManifest) -> Result<()> {
        fs::create_dir_all(dir)?;
        let meta_path = dir.join(GRAPH_PROJECTION_META_FILE);
        let payload = serde_json::to_vec_pretty(manifest).map_err(|e| {
            paro_error::internal(format!(
                "GraphProjectionIndex: failed to serialize meta.json: {}",
                e
            ))
        })?;
        let mut meta_file = File::create(meta_path)?;
        meta_file.write_all(&payload)?;
        meta_file.flush()?;
        Ok(())
    }

    fn write_index_files(&self, dir: &Path) -> Result<(Vec<VertexFileMeta>, Vec<EdgeFileMeta>)> {
        let mut vertex_items = self.vertex_maps.iter().collect::<Vec<_>>();
        vertex_items.sort_unstable_by(|lhs, rhs| lhs.0.cmp(rhs.0));

        let mut edge_items = self.forward_adjacency.iter().collect::<Vec<_>>();
        edge_items.sort_unstable_by(|lhs, rhs| lhs.0.cmp(rhs.0));

        let mut vertices_meta = Vec::with_capacity(vertex_items.len());
        for (idx, (label, map)) in vertex_items.iter().enumerate() {
            let file_name = format!("vertex_{}.vmap", idx);
            let path = dir.join(&file_name);
            let mut file = File::create(&path)?;
            map.serialize(&mut file)?;
            file.flush()?;
            vertices_meta.push(VertexFileMeta {
                label: (*label).clone(),
                file: file_name,
            });
        }

        let mut edges_meta = Vec::with_capacity(edge_items.len());
        for (idx, (label, forward)) in edge_items.iter().enumerate() {
            let endpoints = self.edge_endpoints.get(*label).ok_or_else(|| {
                paro_error::data_corrupted(format!(
                    "GraphProjectionIndex: missing edge endpoints for '{}'",
                    label
                ))
            })?;

            let forward_file = format!("edge_{}.fwd", idx);
            let forward_path = dir.join(&forward_file);
            let mut forward_writer = File::create(&forward_path)?;
            forward.serialize(&mut forward_writer)?;
            forward_writer.flush()?;

            let backward_file = if let Some(backward) = self.backward_adjacency.get(*label) {
                let file_name = format!("edge_{}.bwd", idx);
                let path = dir.join(&file_name);
                let mut backward_writer = File::create(&path)?;
                backward.serialize(&mut backward_writer)?;
                backward_writer.flush()?;
                Some(file_name)
            } else {
                None
            };

            edges_meta.push(EdgeFileMeta {
                label: (*label).clone(),
                source_vertex_label: endpoints.source_vertex_label.clone(),
                destination_vertex_label: endpoints.destination_vertex_label.clone(),
                forward_file,
                backward_file,
            });
        }

        Ok((vertices_meta, edges_meta))
    }

    fn load_from_v1_meta(dir: &Path, meta: GraphProjectionMeta) -> Result<Self> {
        if meta.version != GRAPH_PROJECTION_META_VERSION_V1 {
            return Err(paro_error::invalid_input(format!(
                "GraphProjectionIndex: unsupported meta version {} (expected {})",
                meta.version, GRAPH_PROJECTION_META_VERSION_V1
            )));
        }
        if meta.graph_name.trim().is_empty() {
            return Err(paro_error::invalid_input(
                "GraphProjectionIndex: graph_name in meta is empty",
            ));
        }
        Self::load_from_parts(dir, meta.graph_name, &meta.vertices, &meta.edges)
    }

    fn load_from_manifest(dir: &Path, manifest: GraphManifest) -> Result<Self> {
        if manifest.version != GRAPH_PROJECTION_META_VERSION_V2 {
            return Err(paro_error::invalid_input(format!(
                "GraphProjectionIndex: unsupported manifest version {} (expected {})",
                manifest.version, GRAPH_PROJECTION_META_VERSION_V2
            )));
        }
        if manifest.graph_name.trim().is_empty() {
            return Err(paro_error::invalid_input(
                "GraphProjectionIndex: graph_name in manifest is empty",
            ));
        }
        if manifest.state != GraphState::Ready {
            return Err(paro_error::invalid_input(format!(
                "GraphProjectionIndex: manifest state {:?} is not queryable",
                manifest.state
            )));
        }
        Self::load_from_parts(
            dir,
            manifest.graph_name,
            &manifest.vertices,
            &manifest.edges,
        )
    }

    fn load_from_parts(
        dir: &Path,
        graph_name: String,
        vertices: &[VertexFileMeta],
        edges: &[EdgeFileMeta],
    ) -> Result<Self> {
        let meta = GraphProjectionMeta {
            version: GRAPH_PROJECTION_META_VERSION_V1,
            graph_name,
            vertices: vertices.to_vec(),
            edges: edges.to_vec(),
        };

        let mut vertex_maps = HashMap::with_capacity(meta.vertices.len());
        for vertex_meta in &meta.vertices {
            validate_label(&vertex_meta.label, "vertex label")?;
            if vertex_maps.contains_key(&vertex_meta.label) {
                return Err(paro_error::invalid_input(format!(
                    "GraphProjectionIndex: duplicate vertex label '{}' in meta",
                    vertex_meta.label
                )));
            }
            let path = dir.join(&vertex_meta.file);
            let mut file = File::open(&path).map_err(|e| {
                paro_error::io_error(format!(
                    "GraphProjectionIndex: failed to open vertex map {:?}: {}",
                    path, e
                ))
            })?;
            let map = VertexIdMap::deserialize(&mut file)?;
            vertex_maps.insert(vertex_meta.label.clone(), map);
        }

        let mut forward_adjacency = HashMap::with_capacity(meta.edges.len());
        let mut backward_adjacency = HashMap::with_capacity(meta.edges.len());
        let mut edge_endpoints = HashMap::with_capacity(meta.edges.len());

        for edge_meta in &meta.edges {
            validate_label(&edge_meta.label, "edge label")?;
            if forward_adjacency.contains_key(&edge_meta.label) {
                return Err(paro_error::invalid_input(format!(
                    "GraphProjectionIndex: duplicate edge label '{}' in meta",
                    edge_meta.label
                )));
            }
            if !vertex_maps.contains_key(&edge_meta.source_vertex_label) {
                return Err(paro_error::invalid_input(format!(
                    "GraphProjectionIndex: edge '{}' source vertex label '{}' not found in meta vertex maps",
                    edge_meta.label, edge_meta.source_vertex_label
                )));
            }
            if !vertex_maps.contains_key(&edge_meta.destination_vertex_label) {
                return Err(paro_error::invalid_input(format!(
                    "GraphProjectionIndex: edge '{}' destination vertex label '{}' not found in meta vertex maps",
                    edge_meta.label, edge_meta.destination_vertex_label
                )));
            }

            let forward_path = dir.join(&edge_meta.forward_file);
            let forward = load_csr_with_mmap_fallback(&forward_path)?;
            forward_adjacency.insert(edge_meta.label.clone(), forward);

            if let Some(backward_file) = &edge_meta.backward_file {
                let backward_path = dir.join(backward_file);
                let backward = load_csr_with_mmap_fallback(&backward_path)?;
                backward_adjacency.insert(edge_meta.label.clone(), backward);
            }

            edge_endpoints.insert(
                edge_meta.label.clone(),
                EdgeEndpoints {
                    source_vertex_label: edge_meta.source_vertex_label.clone(),
                    destination_vertex_label: edge_meta.destination_vertex_label.clone(),
                },
            );
        }

        Ok(Self {
            graph_name: meta.graph_name,
            vertex_maps,
            forward_adjacency,
            backward_adjacency,
            edge_endpoints,
        })
    }

    fn read_meta_file(dir: &Path) -> Result<GraphMetaFile> {
        let meta_path = dir.join(GRAPH_PROJECTION_META_FILE);
        let mut meta_file = File::open(&meta_path).map_err(|e| {
            paro_error::io_error(format!(
                "GraphProjectionIndex: failed to open meta file {:?}: {}",
                meta_path, e
            ))
        })?;
        let mut meta_bytes = Vec::new();
        meta_file.read_to_end(&mut meta_bytes)?;
        let version = serde_json::from_slice::<serde_json::Value>(&meta_bytes)
            .ok()
            .and_then(|value| value.get("version").and_then(|v| v.as_u64()))
            .ok_or_else(|| {
                paro_error::invalid_input(
                    "GraphProjectionIndex: invalid meta.json content: missing version",
                )
            })? as u32;

        match version {
            GRAPH_PROJECTION_META_VERSION_V1 => {
                let meta: GraphProjectionMeta =
                    serde_json::from_slice(&meta_bytes).map_err(|e| {
                        paro_error::invalid_input(format!(
                            "GraphProjectionIndex: invalid meta.json content: {}",
                            e
                        ))
                    })?;
                Ok(GraphMetaFile::V1(meta))
            }
            GRAPH_PROJECTION_META_VERSION_V2 => {
                let manifest: GraphManifest = serde_json::from_slice(&meta_bytes).map_err(|e| {
                    paro_error::invalid_input(format!(
                        "GraphProjectionIndex: invalid manifest content: {}",
                        e
                    ))
                })?;
                Ok(GraphMetaFile::V2(manifest))
            }
            other => Err(paro_error::invalid_input(format!(
                "GraphProjectionIndex: unsupported meta version {}",
                other
            ))),
        }
    }

    fn read_meta_version(dir: &Path) -> Result<Option<u32>> {
        let meta_path = dir.join(GRAPH_PROJECTION_META_FILE);
        let mut meta_file = File::open(&meta_path).map_err(|e| {
            paro_error::io_error(format!(
                "GraphProjectionIndex: failed to open meta file {:?}: {}",
                meta_path, e
            ))
        })?;
        let mut meta_bytes = Vec::new();
        meta_file.read_to_end(&mut meta_bytes)?;
        Ok(serde_json::from_slice::<serde_json::Value>(&meta_bytes)
            .ok()
            .and_then(|value| value.get("version").and_then(|v| v.as_u64()))
            .map(|value| value as u32))
    }

    /// Get all vertex labels.
    pub fn vertex_labels(&self) -> Vec<String> {
        let mut labels: Vec<String> = self.vertex_maps.keys().cloned().collect();
        labels.sort();
        labels
    }

    /// Get all edge labels.
    pub fn edge_labels(&self) -> Vec<String> {
        let mut labels: Vec<String> = self.forward_adjacency.keys().cloned().collect();
        labels.sort();
        labels
    }

    /// Get edge endpoints for a given edge label.
    pub fn edge_endpoints(&self, edge_label: &str) -> Option<(&str, &str)> {
        self.edge_endpoints.get(edge_label).map(|ep| {
            (
                ep.source_vertex_label.as_str(),
                ep.destination_vertex_label.as_str(),
            )
        })
    }

    /// Estimated memory footprint in bytes.
    pub fn memory_usage(&self) -> usize {
        let mut usage = self.graph_name.len();
        for (label, map) in &self.vertex_maps {
            usage = usage
                .saturating_add(label.len())
                .saturating_add(map.memory_usage());
        }
        for (label, csr) in &self.forward_adjacency {
            usage = usage
                .saturating_add(label.len())
                .saturating_add(csr.memory_usage());
        }
        for (label, csr) in &self.backward_adjacency {
            usage = usage
                .saturating_add(label.len())
                .saturating_add(csr.memory_usage());
        }
        for endpoints in self.edge_endpoints.values() {
            usage = usage
                .saturating_add(endpoints.source_vertex_label.len())
                .saturating_add(endpoints.destination_vertex_label.len());
        }
        usage
    }
}

fn load_csr_with_mmap_fallback(path: &Path) -> Result<AdjacencyCSR> {
    AdjacencyCSR::load_mmap(path).or_else(|_| {
        let mut reader = File::open(path)?;
        AdjacencyCSR::deserialize(&mut reader)
    })
}

fn validate_label(label: &str, context: &str) -> Result<()> {
    if label.trim().is_empty() {
        return Err(paro_error::invalid_input(format!(
            "GraphProjectionIndex: {} is empty",
            context
        )));
    }
    Ok(())
}

enum GraphMetaFile {
    V1(GraphProjectionMeta),
    V2(GraphManifest),
}

impl GraphManifest {
    pub fn new(graph_name: String, state: GraphState, schema_fingerprint: String) -> Self {
        let now = current_time_millis();
        Self {
            version: GRAPH_PROJECTION_META_VERSION_V2,
            graph_name,
            state,
            schema_fingerprint,
            created_at_epoch_ms: now,
            updated_at_epoch_ms: now,
            last_rebuild_epoch_ms: now,
            vertices: Vec::new(),
            edges: Vec::new(),
            statistics: None,
        }
    }

    fn with_files(
        mut self,
        graph_name: String,
        vertices: Vec<VertexFileMeta>,
        edges: Vec<EdgeFileMeta>,
    ) -> Self {
        self.version = GRAPH_PROJECTION_META_VERSION_V2;
        self.graph_name = graph_name;
        self.updated_at_epoch_ms = current_time_millis();
        self.last_rebuild_epoch_ms = self.updated_at_epoch_ms;
        self.vertices = vertices;
        self.edges = edges;
        self
    }

    fn ensure_statistics<F>(self, build: F) -> Self
    where
        F: FnOnce() -> GraphStatistics,
    {
        if self.statistics.is_some() {
            self
        } else {
            self.with_statistics(build())
        }
    }

    pub fn with_statistics(mut self, statistics: GraphStatistics) -> Self {
        self.updated_at_epoch_ms = current_time_millis();
        self.statistics = Some(statistics);
        self
    }

    pub fn with_state(mut self, state: GraphState) -> Self {
        self.updated_at_epoch_ms = current_time_millis();
        self.state = state;
        self
    }

    pub fn graph_name(&self) -> &str {
        &self.graph_name
    }

    pub fn state(&self) -> GraphState {
        self.state
    }

    pub fn schema_fingerprint(&self) -> &str {
        &self.schema_fingerprint
    }

    pub fn updated_at_epoch_ms(&self) -> u64 {
        self.updated_at_epoch_ms
    }

    pub fn last_rebuild_epoch_ms(&self) -> u64 {
        if self.last_rebuild_epoch_ms == 0 {
            self.updated_at_epoch_ms
        } else {
            self.last_rebuild_epoch_ms
        }
    }

    pub fn statistics(&self) -> Option<&GraphStatistics> {
        self.statistics.as_ref()
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::graph::GraphStatsProvider;
    use tempfile::TempDir;

    fn social_graph_build_input(build_backward_adjacency: bool) -> GraphBuildInput {
        GraphBuildInput {
            graph_name: "social_network".to_string(),
            vertex_tables: vec![
                VertexBuildInput {
                    label: "Person".to_string(),
                    keys_and_rowids: vec![
                        (VertexKey::Int64(1), 101),
                        (VertexKey::Int64(2), 102),
                        (VertexKey::Int64(3), 103),
                    ],
                },
                VertexBuildInput {
                    label: "Company".to_string(),
                    keys_and_rowids: vec![
                        (VertexKey::Int64(100), 501),
                        (VertexKey::Int64(200), 502),
                    ],
                },
            ],
            edge_tables: vec![
                EdgeBuildInput {
                    label: "Knows".to_string(),
                    source_vertex_label: "Person".to_string(),
                    destination_vertex_label: "Person".to_string(),
                    edges: vec![
                        (VertexKey::Int64(1), VertexKey::Int64(2), 9001),
                        (VertexKey::Int64(2), VertexKey::Int64(3), 9002),
                        (VertexKey::Int64(1), VertexKey::Int64(3), 9003),
                    ],
                },
                EdgeBuildInput {
                    label: "WorksAt".to_string(),
                    source_vertex_label: "Person".to_string(),
                    destination_vertex_label: "Company".to_string(),
                    edges: vec![
                        (VertexKey::Int64(1), VertexKey::Int64(100), 9101),
                        (VertexKey::Int64(2), VertexKey::Int64(200), 9102),
                        (VertexKey::Int64(3), VertexKey::Int64(100), 9103),
                    ],
                },
            ],
            build_backward_adjacency,
        }
    }

    #[test]
    fn build_multi_label_graph_projection() {
        let input = social_graph_build_input(true);
        let index = GraphProjectionIndex::build(&input).unwrap();

        let person_map = index.vertex_map("Person").unwrap();
        assert_eq!(person_map.key_to_local(&VertexKey::Int64(1)), Some(0));
        assert_eq!(person_map.key_to_local(&VertexKey::Int64(2)), Some(1));
        assert_eq!(person_map.key_to_local(&VertexKey::Int64(3)), Some(2));
        assert_eq!(person_map.local_to_rowid(1), 102);
        assert_eq!(person_map.rowid_to_local(103), Some(2));

        let company_map = index.vertex_map("Company").unwrap();
        assert_eq!(company_map.key_to_local(&VertexKey::Int64(100)), Some(0));
        assert_eq!(company_map.key_to_local(&VertexKey::Int64(200)), Some(1));

        let knows = index.forward_csr("Knows").unwrap();
        assert_eq!(knows.neighbors(0), &[1, 2]);
        assert_eq!(knows.edge_rowids_for(0), &[9001, 9003]);
        assert_eq!(knows.neighbors(1), &[2]);
        assert_eq!(knows.edge_rowids_for(1), &[9002]);

        let works_at = index.forward_csr("WorksAt").unwrap();
        assert_eq!(works_at.neighbors(0), &[0]);
        assert_eq!(works_at.neighbors(1), &[1]);
        assert_eq!(works_at.neighbors(2), &[0]);
        assert_eq!(works_at.edge_rowids_for(0), &[9101]);
        assert_eq!(works_at.edge_rowids_for(1), &[9102]);
        assert_eq!(works_at.edge_rowids_for(2), &[9103]);

        let works_at_backward = index.backward_csr("WorksAt").unwrap();
        assert_eq!(works_at_backward.neighbors(0), &[0, 2]);
        assert_eq!(works_at_backward.edge_rowids_for(0), &[9101, 9103]);
        assert_eq!(works_at_backward.neighbors(1), &[1]);
        assert_eq!(works_at_backward.edge_rowids_for(1), &[9102]);
    }

    #[test]
    fn build_fails_when_edge_key_not_found() {
        let mut input = social_graph_build_input(false);
        input.edge_tables[0]
            .edges
            .push((VertexKey::Int64(999), VertexKey::Int64(1), 9999));

        let err = GraphProjectionIndex::build(&input).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("source key not found"),
            "unexpected error: {}",
            message
        );
    }

    #[test]
    fn save_load_roundtrip_is_consistent() {
        let input = social_graph_build_input(true);
        let index = GraphProjectionIndex::build(&input).unwrap();

        let temp_dir = TempDir::new().unwrap();
        index.save(temp_dir.path()).unwrap();

        let loaded = GraphProjectionIndex::load(temp_dir.path()).unwrap();
        assert!(loaded.memory_usage() > 0);

        let person = loaded.vertex_map("Person").unwrap();
        assert_eq!(person.key_to_local(&VertexKey::Int64(1)), Some(0));
        assert_eq!(person.local_to_rowid(2), 103);

        let knows = loaded.forward_csr("Knows").unwrap();
        assert_eq!(knows.neighbors(0), &[1, 2]);
        assert_eq!(knows.edge_rowids_for(0), &[9001, 9003]);

        let works_at_backward = loaded.backward_csr("WorksAt").unwrap();
        assert_eq!(works_at_backward.neighbors(0), &[0, 2]);
        assert_eq!(works_at_backward.edge_rowids_for(0), &[9101, 9103]);
    }

    #[test]
    fn save_load_without_backward_edges() {
        let input = social_graph_build_input(false);
        let index = GraphProjectionIndex::build(&input).unwrap();

        let temp_dir = TempDir::new().unwrap();
        index.save(temp_dir.path()).unwrap();

        let loaded = GraphProjectionIndex::load(temp_dir.path()).unwrap();
        assert!(loaded.backward_csr("Knows").is_none());
        assert!(loaded.backward_csr("WorksAt").is_none());
        assert!(loaded.forward_csr("Knows").is_some());
        assert!(loaded.forward_csr("WorksAt").is_some());
    }

    #[test]
    fn load_rejects_non_ready_manifest_state() {
        let input = social_graph_build_input(true);
        let index = GraphProjectionIndex::build(&input).unwrap();

        let temp_dir = TempDir::new().unwrap();
        let manifest = GraphManifest::new(
            "social_network".to_string(),
            GraphState::Building,
            "fp:test".to_string(),
        );
        index.save_with_manifest(temp_dir.path(), manifest).unwrap();

        let manifest = GraphProjectionIndex::load_manifest(temp_dir.path()).unwrap();
        assert_eq!(manifest.state(), GraphState::Building);

        let err = GraphProjectionIndex::load(temp_dir.path()).unwrap_err();
        assert!(
            err.to_string().contains("not queryable"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn manifest_statistics_roundtrip_is_consistent() {
        let input = social_graph_build_input(true);
        let index = GraphProjectionIndex::build(&input).unwrap();

        let temp_dir = TempDir::new().unwrap();
        let manifest = GraphManifest::new(
            "social_network".to_string(),
            GraphState::Ready,
            "fp:test".to_string(),
        )
        .with_statistics(GraphStatistics::from_build_input(&input));
        index.save_with_manifest(temp_dir.path(), manifest).unwrap();

        let manifest = GraphProjectionIndex::load_manifest(temp_dir.path()).unwrap();
        let stats = manifest.statistics().expect("statistics should exist");
        assert_eq!(stats.vertex_count("Person"), Some(3));
        assert_eq!(stats.edge_count("WorksAt"), Some(3));
        assert_eq!(
            stats.pattern_step_count("Person", "WorksAt", "Company"),
            Some(3)
        );
    }

    #[test]
    fn manifest_last_rebuild_survives_state_transition() {
        let manifest = GraphManifest::new(
            "social_network".to_string(),
            GraphState::Ready,
            "fp:test".into(),
        );
        let last_rebuild = manifest.last_rebuild_epoch_ms();
        let stale = manifest.clone().with_state(GraphState::Stale);

        assert_eq!(stale.last_rebuild_epoch_ms(), last_rebuild);
        assert_eq!(stale.state(), GraphState::Stale);
        assert!(stale.updated_at_epoch_ms() >= last_rebuild);
    }
}
