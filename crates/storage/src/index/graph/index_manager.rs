// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Graph projection index manager.
//!
//! Manages in-memory references to all loaded graph projection indexes.

use super::{
    GraphManifest, GraphProjectionIndex, GraphReadSnapshot, GraphRuntimeHandle, GraphState,
    GraphStatistics, GraphStorageGeneration,
};
use paro_common::error::Result;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tracing::warn;

/// Manages all loaded graph projection indexes.
///
/// Provides thread-safe access to graph indexes by name.
/// Used by the execution layer to look up indexes during query execution
/// and by DDL operators to register/unregister indexes.
pub struct GraphProjectionIndexManager {
    handles: RwLock<HashMap<String, Arc<GraphRuntimeHandle>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRecoveryCatalogEntry {
    pub graph_name: String,
    pub schema_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphRecoveryResult {
    Loaded { graph_name: String },
    Missing { graph_name: String },
    Stale { graph_name: String, reason: String },
    NeedsUpgrade { graph_name: String },
}

impl GraphProjectionIndexManager {
    /// Create a new empty manager.
    pub fn new() -> Self {
        Self {
            handles: RwLock::new(HashMap::new()),
        }
    }

    /// Get the runtime handle for a graph.
    pub fn get_handle(&self, graph_name: &str) -> Option<Arc<GraphRuntimeHandle>> {
        self.handles.read().unwrap().get(graph_name).cloned()
    }

    /// Create a read snapshot pinned to the current immutable generation.
    pub fn snapshot(&self, graph_name: &str) -> Option<GraphReadSnapshot> {
        self.get_handle(graph_name).map(|handle| handle.snapshot())
    }

    /// Get a graph projection index by name.
    pub fn get(&self, graph_name: &str) -> Option<Arc<GraphProjectionIndex>> {
        self.snapshot(graph_name)
            .map(|snapshot| snapshot.base().clone())
    }

    pub fn statistics(&self, graph_name: &str) -> Option<Arc<GraphStatistics>> {
        self.snapshot(graph_name)
            .map(|snapshot| snapshot.statistics().clone())
    }

    /// Register a pre-built immutable generation for a graph.
    pub fn register_generation(&self, graph_name: &str, generation: GraphStorageGeneration) {
        let mut handles = self.handles.write().unwrap();
        handles.insert(
            graph_name.to_string(),
            Arc::new(GraphRuntimeHandle::new(generation)),
        );
    }

    /// Register a graph projection index.
    pub fn register(&self, graph_name: &str, index: GraphProjectionIndex) {
        let manifest = GraphManifest::new(graph_name.to_string(), GraphState::Ready, String::new());
        self.register_generation(
            graph_name,
            GraphStorageGeneration::from_index(index, manifest, 0),
        );
    }

    /// Publish a new immutable generation into an existing runtime handle.
    pub fn publish_generation(
        &self,
        graph_name: &str,
        generation: GraphStorageGeneration,
    ) -> Option<Arc<GraphStorageGeneration>> {
        self.get_handle(graph_name)
            .map(|handle| handle.publish(generation))
    }

    /// Unregister a graph projection index.
    pub fn unregister(&self, graph_name: &str) -> Option<Arc<GraphRuntimeHandle>> {
        self.handles.write().unwrap().remove(graph_name)
    }

    /// List all registered graph names.
    pub fn list_all(&self) -> Vec<String> {
        let mut names: Vec<String> = self.handles.read().unwrap().keys().cloned().collect();
        names.sort();
        names
    }

    /// Load all graph projection indexes from a directory.
    ///
    /// Scans `base_dir` for subdirectories, each representing a graph.
    /// Each subdirectory should contain a `meta.json` file.
    pub fn load_all(base_dir: &Path) -> Result<Self> {
        let manager = Self::new();

        if !base_dir.exists() {
            return Ok(manager);
        }

        let entries = std::fs::read_dir(base_dir).map_err(|e| {
            paro_common::error::io_error(format!(
                "GraphProjectionIndexManager: failed to read directory {:?}: {}",
                base_dir, e
            ))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                paro_common::error::io_error(format!(
                    "GraphProjectionIndexManager: failed to read dir entry: {}",
                    e
                ))
            })?;
            let path = entry.path();
            if path.is_dir() {
                let meta_path = path.join("meta.json");
                if meta_path.exists() {
                    match GraphProjectionIndex::load(&path) {
                        Ok(index) => {
                            let graph_name = path
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("")
                                .to_string();
                            let manifest = GraphProjectionIndex::load_manifest(&path)
                                .unwrap_or_else(|_| {
                                    GraphManifest::new(
                                        graph_name.clone(),
                                        GraphState::Ready,
                                        String::new(),
                                    )
                                });
                            manager.register_generation(
                                &graph_name,
                                GraphStorageGeneration::from_index(index, manifest, 0),
                            );
                        }
                        Err(e) => {
                            warn!(
                                "GraphProjectionIndexManager: failed to load graph index from {:?}: {}",
                                path, e
                            );
                        }
                    }
                }
            }
        }

        Ok(manager)
    }

    pub fn recover_from_catalog_entries(
        &self,
        base_dir: &Path,
        entries: &[GraphRecoveryCatalogEntry],
    ) -> Vec<GraphRecoveryResult> {
        let mut results = Vec::with_capacity(entries.len());

        for entry in entries {
            let path = base_dir.join(&entry.graph_name);
            if !path.exists() {
                results.push(GraphRecoveryResult::Missing {
                    graph_name: entry.graph_name.clone(),
                });
                continue;
            }

            let version = match GraphProjectionIndex::manifest_version(&path) {
                Ok(version) => version,
                Err(error) => {
                    results.push(GraphRecoveryResult::Stale {
                        graph_name: entry.graph_name.clone(),
                        reason: error.to_string(),
                    });
                    continue;
                }
            };

            if version == 1 {
                results.push(GraphRecoveryResult::NeedsUpgrade {
                    graph_name: entry.graph_name.clone(),
                });
                continue;
            }

            let manifest = match GraphProjectionIndex::load_manifest(&path) {
                Ok(manifest) => manifest,
                Err(error) => {
                    results.push(GraphRecoveryResult::Stale {
                        graph_name: entry.graph_name.clone(),
                        reason: error.to_string(),
                    });
                    continue;
                }
            };

            if manifest.graph_name() != entry.graph_name {
                results.push(GraphRecoveryResult::Stale {
                    graph_name: entry.graph_name.clone(),
                    reason: format!(
                        "manifest graph name mismatch: expected {}, got {}",
                        entry.graph_name,
                        manifest.graph_name()
                    ),
                });
                continue;
            }

            if manifest.schema_fingerprint() != entry.schema_fingerprint {
                results.push(GraphRecoveryResult::Stale {
                    graph_name: entry.graph_name.clone(),
                    reason: "schema fingerprint mismatch".to_string(),
                });
                continue;
            }

            if manifest.state() != GraphState::Ready {
                if matches!(
                    manifest.state(),
                    GraphState::Building | GraphState::Rebuilding
                ) {
                    let _ = fs::remove_dir_all(&path);
                }
                results.push(GraphRecoveryResult::Stale {
                    graph_name: entry.graph_name.clone(),
                    reason: format!("manifest state is {:?}", manifest.state()),
                });
                continue;
            }

            match GraphProjectionIndex::load(&path) {
                Ok(index) => {
                    self.register_generation(
                        &entry.graph_name,
                        GraphStorageGeneration::from_index(index, manifest.clone(), 0),
                    );
                    results.push(GraphRecoveryResult::Loaded {
                        graph_name: entry.graph_name.clone(),
                    });
                }
                Err(error) => {
                    warn!(
                        "GraphProjectionIndexManager: failed to recover graph {} from {:?}: {}",
                        entry.graph_name, path, error
                    );
                    results.push(GraphRecoveryResult::Stale {
                        graph_name: entry.graph_name.clone(),
                        reason: error.to_string(),
                    });
                }
            }
        }

        results
    }
}

impl Default for GraphProjectionIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for GraphProjectionIndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let handles = self.handles.read().unwrap();
        f.debug_struct("GraphProjectionIndexManager")
            .field("graph_count", &handles.len())
            .field("graphs", &handles.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_manager() {
        let manager = GraphProjectionIndexManager::new();
        assert!(manager.get("nonexistent").is_none());
    }

    #[test]
    fn test_load_all_nonexistent_dir() {
        let manager =
            GraphProjectionIndexManager::load_all(Path::new("/tmp/nonexistent_graph_dir_12345"))
                .unwrap();
        assert!(manager.get("any").is_none());
    }

    #[test]
    fn test_register_generation_exposes_snapshot() {
        let manager = GraphProjectionIndexManager::new();
        let manifest =
            GraphManifest::new("g".to_string(), GraphState::Ready, "fp:test".to_string());
        let generation =
            GraphStorageGeneration::from_index(GraphProjectionIndex::default(), manifest, 7);

        manager.register_generation("g", generation);

        let snapshot = manager.snapshot("g").expect("snapshot should exist");
        assert_eq!(snapshot.generation_id(), 7);
    }
}
