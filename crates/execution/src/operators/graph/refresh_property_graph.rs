// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Refresh Property Graph Operator
//!
//! Executes REFRESH PROPERTY GRAPH DDL:
//! 1. Scans the current vertex/edge tables
//! 2. Chooses edge-delta publish or full rebuild
//! 3. Publishes a new immutable generation

use super::property_graph_support::{
    build_graph_index_from_scans, graph_data_dir, graph_statistics_from_scans,
    scan_graph_inputs_with_catalog, ScannedGraphInputs,
};
use paro_catalog::catalog::Catalog;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{graph_schema_fingerprint, CatalogEntry, PropertyGraphCatalogEntry};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::error::{self as paro_error, Result};
use paro_common::identity::GraphId;
use paro_context::{GraphIndexProvider, GraphRegistry};
use paro_storage::index::graph::{
    lock_graph_artifact_io, DeltaAdjacency, GraphManifest, GraphProjectionIndex, GraphState,
    GraphStatistics, GraphStorageGeneration, VertexBuildInput,
};
use paro_storage::metrics::storage_metrics;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;
use std::time::Instant;

const GRAPH_DELTA_COMPACTION_NUMERATOR: usize = 1;
const GRAPH_DELTA_COMPACTION_DENOMINATOR: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GraphRefreshPolicy {
    Synchronous,
    BackgroundCompaction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GraphMaintenanceState {
    running: bool,
    requested_visible_start_time: u64,
}

static GRAPH_BACKGROUND_MAINTENANCE: LazyLock<Mutex<HashMap<String, GraphMaintenanceState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static GRAPH_REBUILD_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
enum DeltaPlan {
    Publish {
        committed_edge_deltas: HashMap<String, Arc<DeltaAdjacency>>,
        delta_edges: usize,
    },
    Rebuild,
}

fn vertex_inputs_match_base(
    base: &GraphProjectionIndex,
    vertex_inputs: &[VertexBuildInput],
) -> bool {
    for vertex_input in vertex_inputs {
        let Some(base_map) = base.vertex_map(&vertex_input.label) else {
            return false;
        };
        if base_map.num_vertices() as usize != vertex_input.keys_and_rowids.len() {
            return false;
        }
        for (key, rowid) in &vertex_input.keys_and_rowids {
            let Some(local_id) = base_map.key_to_local(key) else {
                return false;
            };
            if base_map.local_to_rowid(local_id) != *rowid {
                return false;
            }
        }
    }
    true
}

fn build_delta_plan(
    base: &GraphProjectionIndex,
    scanned: &ScannedGraphInputs,
) -> Result<DeltaPlan> {
    let mut committed_edge_deltas = HashMap::new();
    let mut total_delta_edges = 0usize;

    for edge_input in &scanned.edge_inputs {
        let (src_label, dst_label) = base.edge_endpoints(&edge_input.label).ok_or_else(|| {
            paro_error::internal(format!(
                "Missing edge endpoints for label \"{}\" in graph \"{}\"",
                edge_input.label,
                base.graph_name()
            ))
        })?;
        let src_map = base.vertex_map(src_label).ok_or_else(|| {
            paro_error::internal(format!(
                "Missing source vertex map \"{}\" for edge label \"{}\"",
                src_label, edge_input.label
            ))
        })?;
        let dst_map = base.vertex_map(dst_label).ok_or_else(|| {
            paro_error::internal(format!(
                "Missing destination vertex map \"{}\" for edge label \"{}\"",
                dst_label, edge_input.label
            ))
        })?;
        let base_csr = base.forward_csr(&edge_input.label).ok_or_else(|| {
            paro_error::internal(format!(
                "Missing forward CSR for edge label \"{}\" in graph \"{}\"",
                edge_input.label,
                base.graph_name()
            ))
        })?;

        let mut base_edges_by_rowid =
            HashMap::with_capacity(base_csr.num_edges().try_into().unwrap_or(0));
        for src in 0..base_csr.num_vertices() {
            let nbrs = base_csr.neighbors(src);
            let edge_rowids = base_csr.edge_rowids_for(src);
            for (idx, &dst) in nbrs.iter().enumerate() {
                base_edges_by_rowid.insert(edge_rowids[idx], (src, dst));
            }
        }

        let mut seen_base_rowids = HashSet::with_capacity(base_edges_by_rowid.len());
        let mut delta = DeltaAdjacency::new();

        for (src_key, dst_key, rowid) in &edge_input.edges {
            let Some(src_local) = src_map.key_to_local(src_key) else {
                return Ok(DeltaPlan::Rebuild);
            };
            let Some(dst_local) = dst_map.key_to_local(dst_key) else {
                return Ok(DeltaPlan::Rebuild);
            };

            if let Some(&(base_src, base_dst)) = base_edges_by_rowid.get(rowid) {
                if base_src != src_local || base_dst != dst_local {
                    return Ok(DeltaPlan::Rebuild);
                }
                seen_base_rowids.insert(*rowid);
            } else {
                delta.add_edge_by_local_id(src_local, dst_local, *rowid);
            }
        }

        for rowid in base_edges_by_rowid.keys() {
            if !seen_base_rowids.contains(rowid) {
                delta.delete_edge(*rowid);
            }
        }

        let label_delta_edges = delta.added_count() + delta.deleted_count() as usize;
        if label_delta_edges > 0 {
            total_delta_edges += label_delta_edges;
            committed_edge_deltas.insert(edge_input.label.clone(), Arc::new(delta));
        }
    }

    Ok(DeltaPlan::Publish {
        committed_edge_deltas,
        delta_edges: total_delta_edges,
    })
}

fn publish_generation(
    graph_id: &GraphId,
    graph_index: &dyn GraphIndexProvider,
    graph_registry: &dyn GraphRegistry,
    generation: GraphStorageGeneration,
) -> Result<()> {
    if graph_index.snapshot(graph_id).is_some() {
        graph_registry.publish_generation(graph_id, generation)?;
    } else {
        graph_registry.register_generation(graph_id, generation);
    }
    Ok(())
}

fn publish_delta_generation(
    graph_id: &GraphId,
    graph_index: &dyn GraphIndexProvider,
    graph_registry: &dyn GraphRegistry,
    snapshot: &paro_storage::index::graph::GraphReadSnapshot,
    committed_edge_deltas: HashMap<String, Arc<DeltaAdjacency>>,
    graph_stats: Arc<GraphStatistics>,
    next_generation_id: u64,
    indexed_through_ts: u64,
) -> Result<()> {
    publish_generation(
        graph_id,
        graph_index,
        graph_registry,
        GraphStorageGeneration::new(
            snapshot.base().clone(),
            snapshot
                .manifest()
                .clone()
                .with_indexed_through_ts(indexed_through_ts)
                .with_statistics(graph_stats.as_ref().clone()),
            next_generation_id,
            committed_edge_deltas,
            graph_stats,
        ),
    )
}

fn publish_metadata_generation(
    graph_id: &GraphId,
    graph_index: &dyn GraphIndexProvider,
    graph_registry: &dyn GraphRegistry,
    snapshot: &paro_storage::index::graph::GraphReadSnapshot,
    next_generation_id: u64,
    indexed_through_ts: u64,
) -> Result<()> {
    publish_generation(
        graph_id,
        graph_index,
        graph_registry,
        GraphStorageGeneration::new(
            snapshot.base().clone(),
            snapshot
                .manifest()
                .clone()
                .with_indexed_through_ts(indexed_through_ts),
            next_generation_id,
            snapshot.generation().committed_edge_deltas.clone(),
            snapshot.statistics().clone(),
        ),
    )
}

fn graph_rebuild_staging_dir(
    db_path: &str,
    graph_name: &str,
    generation_id: u64,
) -> std::path::PathBuf {
    let sequence = GRAPH_REBUILD_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let base = if db_path.is_empty() {
        std::path::PathBuf::from("data")
    } else {
        std::path::PathBuf::from(db_path)
    };
    base.join(".graph-rebuild-staging")
        .join(format!("{graph_name}-{generation_id}-{sequence}"))
}

fn replace_graph_dir_atomically(
    graph_dir: &std::path::Path,
    staging_dir: &std::path::Path,
) -> Result<()> {
    let Some(parent) = graph_dir.parent() else {
        return Err(paro_error::internal(format!(
            "graph dir {} has no parent",
            graph_dir.display()
        )));
    };
    fs::create_dir_all(parent)?;

    if !graph_dir.exists() {
        fs::rename(staging_dir, graph_dir)?;
        return Ok(());
    }

    let backup_dir = staging_dir.with_extension("old");
    if backup_dir.exists() {
        let _ = fs::remove_dir_all(&backup_dir);
    }

    fs::rename(graph_dir, &backup_dir)?;
    if let Err(err) = fs::rename(staging_dir, graph_dir) {
        let _ = fs::rename(&backup_dir, graph_dir);
        return Err(err.into());
    }
    let _ = fs::remove_dir_all(&backup_dir);
    Ok(())
}

fn persist_and_publish_rebuild(
    graph_id: &GraphId,
    graph_index: &dyn GraphIndexProvider,
    graph_registry: &dyn GraphRegistry,
    db_path: &str,
    graph_name: &str,
    scanned: &ScannedGraphInputs,
    next_generation_id: u64,
    schema_fingerprint: &str,
    indexed_through_ts: u64,
) -> Result<()> {
    let rebuild_start = Instant::now();
    let index = build_graph_index_from_scans(graph_name, scanned)?;
    let graph_stats = GraphStatistics::from_index(&index);
    let manifest = GraphManifest::new(
        graph_name.to_string(),
        GraphState::Ready,
        schema_fingerprint.to_string(),
    )
    .with_indexed_through_ts(indexed_through_ts)
    .with_statistics(graph_stats);

    let graph_dir = graph_data_dir(db_path, graph_name);
    let staging_dir = graph_rebuild_staging_dir(db_path, graph_name, next_generation_id);
    if staging_dir.exists() {
        let _ = fs::remove_dir_all(&staging_dir);
    }
    if let Some(parent) = staging_dir.parent() {
        fs::create_dir_all(parent)?;
    }
    index.save_with_manifest(&staging_dir, manifest.clone())?;
    replace_graph_dir_atomically(&graph_dir, &staging_dir)?;

    publish_generation(
        graph_id,
        graph_index,
        graph_registry,
        GraphStorageGeneration::new(
            Arc::new(index),
            manifest,
            next_generation_id,
            HashMap::new(),
            Arc::new(graph_statistics_from_scans(graph_name, scanned)),
        ),
    )?;
    storage_metrics().add_graph_rebuild_latency(rebuild_start.elapsed());
    Ok(())
}

pub fn schedule_property_graph_background_rebuild(
    catalog: Arc<ParoCatalog>,
    graph_index: Arc<dyn GraphIndexProvider>,
    graph_registry: Arc<dyn GraphRegistry>,
    graph_entry: Arc<PropertyGraphCatalogEntry>,
    visible_start_time: u64,
) {
    let graph_id = GraphId::new(
        &graph_entry.info.catalog,
        &graph_entry.info.schema,
        &graph_entry.info.graph_name,
    );
    let graph_name = graph_entry.info.graph_name.clone();
    let maintenance_key = graph_id.runtime_key();
    let should_spawn = {
        let mut state = GRAPH_BACKGROUND_MAINTENANCE
            .lock()
            .expect("graph background maintenance mutex poisoned");
        let entry = state
            .entry(maintenance_key.clone())
            .or_insert(GraphMaintenanceState {
                running: false,
                requested_visible_start_time: visible_start_time,
            });
        if entry.requested_visible_start_time < visible_start_time {
            entry.requested_visible_start_time = visible_start_time;
        }
        if entry.running {
            false
        } else {
            entry.running = true;
            true
        }
    };

    if !should_spawn {
        return;
    }

    thread::spawn(move || loop {
        let requested_visible_start_time = {
            let state = GRAPH_BACKGROUND_MAINTENANCE
                .lock()
                .expect("graph background maintenance mutex poisoned");
            state
                .get(&maintenance_key)
                .map(|entry| entry.requested_visible_start_time)
                .unwrap_or(visible_start_time)
        };

        let result = rebuild_property_graph_committed(
            catalog.clone(),
            graph_index.clone(),
            graph_registry.clone(),
            graph_entry.clone(),
            requested_visible_start_time,
        );
        if let Err(err) = result {
            tracing::warn!(
                graph = %graph_name,
                error = %err,
                "Background property-graph rebuild failed"
            );
        }

        let should_continue = {
            let mut state = GRAPH_BACKGROUND_MAINTENANCE
                .lock()
                .expect("graph background maintenance mutex poisoned");
            if let Some(entry) = state.get_mut(&maintenance_key) {
                if entry.requested_visible_start_time > requested_visible_start_time {
                    true
                } else {
                    state.remove(&maintenance_key);
                    false
                }
            } else {
                false
            }
        };

        if !should_continue {
            break;
        }
    });
}

pub(crate) fn refresh_scanned_graph(
    graph_id: &GraphId,
    graph_index: &dyn GraphIndexProvider,
    graph_registry: &dyn GraphRegistry,
    db_path: &str,
    graph_name: &str,
    schema_fingerprint: &str,
    scanned: &ScannedGraphInputs,
    valid_through_ts: u64,
    refresh_policy: GraphRefreshPolicy,
    background_catalog: Option<Arc<ParoCatalog>>,
    background_graph_index: Option<Arc<dyn GraphIndexProvider>>,
    background_graph_registry: Option<Arc<dyn GraphRegistry>>,
    background_graph_entry: Option<Arc<PropertyGraphCatalogEntry>>,
    background_visible_start_time: u64,
) -> Result<()> {
    let _graph_artifact_guard = lock_graph_artifact_io();
    refresh_scanned_graph_locked(
        graph_id,
        graph_index,
        graph_registry,
        db_path,
        graph_name,
        schema_fingerprint,
        scanned,
        valid_through_ts,
        refresh_policy,
        background_catalog,
        background_graph_index,
        background_graph_registry,
        background_graph_entry,
        background_visible_start_time,
    )
}

fn refresh_scanned_graph_locked(
    graph_id: &GraphId,
    graph_index: &dyn GraphIndexProvider,
    graph_registry: &dyn GraphRegistry,
    db_path: &str,
    graph_name: &str,
    schema_fingerprint: &str,
    scanned: &ScannedGraphInputs,
    valid_through_ts: u64,
    refresh_policy: GraphRefreshPolicy,
    background_catalog: Option<Arc<ParoCatalog>>,
    background_graph_index: Option<Arc<dyn GraphIndexProvider>>,
    background_graph_registry: Option<Arc<dyn GraphRegistry>>,
    background_graph_entry: Option<Arc<PropertyGraphCatalogEntry>>,
    background_visible_start_time: u64,
) -> Result<()> {
    let Some(snapshot) = graph_index.snapshot(graph_id) else {
        persist_and_publish_rebuild(
            graph_id,
            graph_index,
            graph_registry,
            db_path,
            graph_name,
            scanned,
            0,
            schema_fingerprint,
            scanned.indexed_through_ts.max(valid_through_ts),
        )?;
        return Ok(());
    };

    let next_generation_id = snapshot.generation_id().saturating_add(1);
    let base = snapshot.base().as_ref();
    let indexed_through_ts = scanned.indexed_through_ts.max(valid_through_ts);

    if !vertex_inputs_match_base(base, &scanned.vertex_inputs) {
        persist_and_publish_rebuild(
            graph_id,
            graph_index,
            graph_registry,
            db_path,
            graph_name,
            scanned,
            next_generation_id,
            schema_fingerprint,
            indexed_through_ts,
        )?;
        return Ok(());
    }

    match build_delta_plan(base, scanned)? {
        DeltaPlan::Rebuild => {
            persist_and_publish_rebuild(
                graph_id,
                graph_index,
                graph_registry,
                db_path,
                graph_name,
                scanned,
                next_generation_id,
                schema_fingerprint,
                indexed_through_ts,
            )?;
        }
        DeltaPlan::Publish {
            committed_edge_deltas,
            delta_edges,
        } => {
            if delta_edges == 0 {
                if indexed_through_ts > snapshot.indexed_through_ts() {
                    publish_metadata_generation(
                        graph_id,
                        graph_index,
                        graph_registry,
                        &snapshot,
                        next_generation_id,
                        indexed_through_ts,
                    )?;
                }
                return Ok(());
            }

            let base_edge_count: usize = base
                .edge_labels()
                .into_iter()
                .filter_map(|label| base.forward_csr(&label).map(|csr| csr.num_edges() as usize))
                .sum();

            let should_compact = base_edge_count == 0
                || delta_edges * GRAPH_DELTA_COMPACTION_DENOMINATOR
                    > base_edge_count * GRAPH_DELTA_COMPACTION_NUMERATOR;

            if should_compact {
                match refresh_policy {
                    GraphRefreshPolicy::Synchronous => {
                        persist_and_publish_rebuild(
                            graph_id,
                            graph_index,
                            graph_registry,
                            db_path,
                            graph_name,
                            scanned,
                            next_generation_id,
                            schema_fingerprint,
                            indexed_through_ts,
                        )?;
                    }
                    GraphRefreshPolicy::BackgroundCompaction => {
                        let graph_stats =
                            Arc::new(graph_statistics_from_scans(graph_name, scanned));
                        publish_delta_generation(
                            graph_id,
                            graph_index,
                            graph_registry,
                            &snapshot,
                            committed_edge_deltas,
                            graph_stats,
                            next_generation_id,
                            indexed_through_ts,
                        )?;
                        if let (
                            Some(catalog),
                            Some(background_graph_index),
                            Some(background_graph_registry),
                            Some(graph_entry),
                        ) = (
                            background_catalog,
                            background_graph_index,
                            background_graph_registry,
                            background_graph_entry,
                        ) {
                            schedule_property_graph_background_rebuild(
                                catalog,
                                background_graph_index,
                                background_graph_registry,
                                graph_entry,
                                background_visible_start_time,
                            );
                        }
                    }
                }
            } else {
                let graph_stats = Arc::new(graph_statistics_from_scans(graph_name, scanned));
                publish_delta_generation(
                    graph_id,
                    graph_index,
                    graph_registry,
                    &snapshot,
                    committed_edge_deltas,
                    graph_stats,
                    next_generation_id,
                    indexed_through_ts,
                )?;
            }
        }
    }

    Ok(())
}

pub fn refresh_property_graph_committed(
    catalog: Arc<ParoCatalog>,
    graph_index: Arc<dyn GraphIndexProvider>,
    graph_registry: Arc<dyn GraphRegistry>,
    graph_entry: Arc<PropertyGraphCatalogEntry>,
    visible_start_time: u64,
) -> Result<()> {
    let graph_info = graph_entry.info.clone();
    let schema_fingerprint = graph_schema_fingerprint(&graph_info);
    let visible_txn = CatalogSnapshot::read_only(visible_start_time);
    let scanned = scan_graph_inputs_with_catalog(catalog.as_ref(), &visible_txn, &graph_info)?;
    let graph_id = GraphId::new(
        &graph_info.catalog,
        &graph_info.schema,
        &graph_info.graph_name,
    );
    let _graph_artifact_guard = lock_graph_artifact_io();
    if !property_graph_entry_is_current(catalog.as_ref(), graph_entry.as_ref())
        || graph_index.snapshot(&graph_id).is_none()
    {
        return Ok(());
    }
    refresh_scanned_graph_locked(
        &graph_id,
        graph_index.as_ref(),
        graph_registry.as_ref(),
        &catalog.get_db_path(),
        &graph_info.graph_name,
        &schema_fingerprint,
        &scanned,
        visible_start_time,
        GraphRefreshPolicy::BackgroundCompaction,
        Some(catalog),
        Some(graph_index.clone()),
        Some(graph_registry.clone()),
        Some(graph_entry),
        visible_start_time,
    )
}

pub fn rebuild_property_graph_committed(
    catalog: Arc<ParoCatalog>,
    graph_index: Arc<dyn GraphIndexProvider>,
    graph_registry: Arc<dyn GraphRegistry>,
    graph_entry: Arc<PropertyGraphCatalogEntry>,
    visible_start_time: u64,
) -> Result<()> {
    let _graph_artifact_guard = lock_graph_artifact_io();
    let graph_info = graph_entry.info.clone();
    if !property_graph_entry_is_current(catalog.as_ref(), graph_entry.as_ref()) {
        return Ok(());
    }
    let graph_id = GraphId::new(
        &graph_info.catalog,
        &graph_info.schema,
        &graph_info.graph_name,
    );
    let Some(snapshot) = graph_index.snapshot(&graph_id) else {
        return Ok(());
    };
    let schema_fingerprint = graph_schema_fingerprint(&graph_info);
    let visible_txn = CatalogSnapshot::read_only(visible_start_time);
    let scanned = scan_graph_inputs_with_catalog(catalog.as_ref(), &visible_txn, &graph_info)?;
    let next_generation_id = snapshot.generation_id().saturating_add(1);
    persist_and_publish_rebuild(
        &graph_id,
        graph_index.as_ref(),
        graph_registry.as_ref(),
        &catalog.get_db_path(),
        &graph_info.graph_name,
        &scanned,
        next_generation_id,
        &schema_fingerprint,
        scanned.indexed_through_ts.max(visible_start_time),
    )
}

pub fn mark_property_graph_stale(
    catalog: &ParoCatalog,
    graph_index: &dyn GraphIndexProvider,
    graph_registry: &dyn GraphRegistry,
    graph_entry: &PropertyGraphCatalogEntry,
) -> Result<()> {
    let _graph_artifact_guard = lock_graph_artifact_io();
    if !property_graph_entry_is_current(catalog, graph_entry) {
        return Ok(());
    }
    let graph_name = &graph_entry.info.graph_name;
    let graph_id = GraphId::new(
        &graph_entry.info.catalog,
        &graph_entry.info.schema,
        graph_name,
    );
    let graph_dir = graph_data_dir(&catalog.get_db_path(), graph_name);
    let snapshot = graph_index.snapshot(&graph_id);
    let next_generation_id = snapshot
        .as_ref()
        .map(|current| current.generation_id().saturating_add(1))
        .unwrap_or(0);
    let manifest = match &snapshot {
        Some(current) => current.manifest().clone().with_state(GraphState::Stale),
        None => GraphProjectionIndex::load_manifest(&graph_dir)
            .unwrap_or_else(|_| {
                GraphManifest::new(
                    graph_name.clone(),
                    GraphState::Stale,
                    graph_schema_fingerprint(&graph_entry.info),
                )
            })
            .with_state(GraphState::Stale),
    };

    if graph_dir.exists() {
        GraphProjectionIndex::write_manifest(&graph_dir, &manifest)?;
    }

    if let Some(current) = snapshot {
        publish_generation(
            &graph_id,
            graph_index,
            graph_registry,
            GraphStorageGeneration::new(
                current.base().clone(),
                manifest,
                next_generation_id,
                current.generation().committed_edge_deltas.clone(),
                current.statistics().clone(),
            ),
        )?;
    }

    Ok(())
}

fn property_graph_entry_is_current(
    catalog: &ParoCatalog,
    graph_entry: &PropertyGraphCatalogEntry,
) -> bool {
    let committed_txn = CatalogSnapshot::default();
    let Ok(schema) = catalog.get_schema(&committed_txn, &graph_entry.info.schema) else {
        return false;
    };
    schema
        .get_property_graph(&committed_txn, &graph_entry.info.graph_name)
        .is_ok_and(|current| current.object_id() == graph_entry.object_id())
}
