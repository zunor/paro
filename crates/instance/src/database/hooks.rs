// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Post-replay recovery hooks for secondary runtime artifacts.
//!
//! Hooks run after single-database WAL recovery but before the handle is
//! published into the runtime registry.

use crate::database::handle::DatabaseHandle;
use crate::lifecycle::startup_report::StartupPolicy;
use paro_catalog::entry::{graph_schema_fingerprint, CatalogEntryEnum, CreatePropertyGraphInfo};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::chunk::Chunk;
use paro_common::error as paro_error;
use paro_common::identity::GraphId;
use paro_common::logging::targets;
use paro_common::runtime_value::Value;
use paro_storage::index::graph::{
    EdgeBuildInput, GraphBuildInput, GraphManifest, GraphProjectionIndex,
    GraphProjectionIndexManager, GraphState, GraphStatistics, GraphStorageGeneration,
    VertexBuildInput, VertexKey,
};
use paro_storage::tablet::TabletReaderParams;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryHookIssueKind {
    ManifestMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryHookIssue {
    pub kind: RecoveryHookIssueKind,
    pub object_name: Option<String>,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryHookResult {
    Reused,
    Skipped {
        reason: String,
    },
    Rebuilt {
        detail: Option<String>,
        issues: Vec<RecoveryHookIssue>,
    },
    Failed {
        error: String,
        issues: Vec<RecoveryHookIssue>,
    },
}

#[derive(Debug, Clone)]
pub struct RecoveryHookContext {
    pub database_root: PathBuf,
    pub recovery_report: crate::recovery::consistency_report::RecoveryConsistencyReport,
    pub startup_policy: StartupPolicy,
    pub graph_registry: Arc<GraphProjectionIndexManager>,
}

/// Post-WAL-replay recovery hook executed before a database is published to the runtime registry.
///
/// Hooks must be idempotent: if a prior startup crashed after leaving partial artifacts behind,
/// rerunning the hook must converge the database back to the same final state without requiring
/// manual cleanup of the partially-written runtime artifacts.
pub trait RecoveryHook: Send + Sync {
    /// Stable hook name for logs/reporting.
    fn name(&self) -> &'static str;

    /// Run post-replay artifact recovery.
    ///
    /// Implementations must be idempotent: startup may retry a hook after the
    /// previous attempt left behind partial artifacts, and repair/best-effort
    /// policies may observe the same durable state multiple times.
    fn run(
        &self,
        db: &Arc<DatabaseHandle>,
        ctx: &RecoveryHookContext,
    ) -> anyhow::Result<RecoveryHookResult>;
}

#[derive(Debug, Default)]
pub struct GraphProjectionRecoveryHook;

#[derive(Debug, Default)]
pub struct FullTextRecoveryHook;

impl RecoveryHook for FullTextRecoveryHook {
    fn name(&self) -> &'static str {
        "fulltext"
    }

    fn run(
        &self,
        _db: &Arc<DatabaseHandle>,
        _ctx: &RecoveryHookContext,
    ) -> anyhow::Result<RecoveryHookResult> {
        Ok(RecoveryHookResult::Skipped {
            reason: "fulltext runtime recovery is already reconciled during WAL replay".to_string(),
        })
    }
}

#[derive(Debug, Default)]
pub struct VectorIndexRecoveryHook;

impl RecoveryHook for VectorIndexRecoveryHook {
    fn name(&self) -> &'static str {
        "vector_index"
    }

    fn run(
        &self,
        _db: &Arc<DatabaseHandle>,
        _ctx: &RecoveryHookContext,
    ) -> anyhow::Result<RecoveryHookResult> {
        Ok(RecoveryHookResult::Skipped {
            reason: "vector index runtime state is persisted in storage and needs no extra startup rebuild".to_string(),
        })
    }
}

impl RecoveryHook for GraphProjectionRecoveryHook {
    fn name(&self) -> &'static str {
        "graph_projection"
    }

    fn run(
        &self,
        db: &Arc<DatabaseHandle>,
        ctx: &RecoveryHookContext,
    ) -> anyhow::Result<RecoveryHookResult> {
        let txn = CatalogSnapshot::default();
        let graph_entries = db.catalog().scan_property_graphs(&txn);
        if graph_entries.is_empty() {
            return Ok(RecoveryHookResult::Skipped {
                reason: "no property graphs in catalog".to_string(),
            });
        }

        let base_dir = ctx.database_root.join("graph");
        let mut reused_count = 0usize;
        let mut rebuilt_count = 0usize;
        let mut rebuild_reasons = Vec::new();
        let mut issues = Vec::new();

        for graph in graph_entries {
            let schema_fingerprint = graph_schema_fingerprint(&graph.info);
            let graph_name = graph.info.graph_name.clone();
            let graph_dir = base_dir.join(&graph_name);
            let runtime_key =
                GraphId::new(db.name(), &graph.info.schema, &graph_name).runtime_key();

            match recover_existing_graph(&graph_dir, &graph_name, &schema_fingerprint)? {
                ExistingGraphRecovery::Reuse {
                    index,
                    manifest,
                    reason,
                } => {
                    ctx.graph_registry.register_generation(
                        &runtime_key,
                        GraphStorageGeneration::from_index(index, manifest, 0),
                    );
                    tracing::info!(
                        target: targets::INSTANCE,
                        hook = self.name(),
                        db = %db.name(),
                        graph = %graph_name,
                        reason = %reason,
                        "Graph projection reused during startup"
                    );
                    reused_count += 1;
                }
                ExistingGraphRecovery::Rebuild { reason, issue } => {
                    let index = build_graph_index_from_catalog(db, &txn, &graph.info)?;
                    if graph_dir.exists() {
                        let _ = std::fs::remove_dir_all(&graph_dir);
                    }

                    let graph_stats = GraphStatistics::from_index(&index);
                    let manifest = GraphManifest::new(
                        graph_name.clone(),
                        GraphState::Ready,
                        schema_fingerprint.clone(),
                    )
                    .with_statistics(graph_stats);
                    index.save_with_manifest(&graph_dir, manifest.clone())?;
                    ctx.graph_registry.register_generation(
                        &runtime_key,
                        GraphStorageGeneration::from_index(index, manifest, 0),
                    );

                    tracing::info!(
                        target: targets::INSTANCE,
                        hook = self.name(),
                        db = %db.name(),
                        graph = %graph_name,
                        reason = %reason,
                        "Graph projection rebuilt during startup"
                    );
                    rebuilt_count += 1;
                    rebuild_reasons.push(format!("{graph_name}: {reason}"));
                    if let Some(kind) = issue {
                        issues.push(RecoveryHookIssue {
                            kind,
                            object_name: Some(graph_name.clone()),
                            detail: reason,
                        });
                    }
                }
            }
        }

        if rebuilt_count == 0 {
            return Ok(RecoveryHookResult::Reused);
        }

        Ok(RecoveryHookResult::Rebuilt {
            detail: Some(format!(
                "reused {} graph projection(s), rebuilt {} graph projection(s): {}",
                reused_count,
                rebuilt_count,
                rebuild_reasons.join("; ")
            )),
            issues,
        })
    }
}

#[allow(clippy::large_enum_variant)]
enum ExistingGraphRecovery {
    Reuse {
        index: GraphProjectionIndex,
        manifest: GraphManifest,
        reason: String,
    },
    Rebuild {
        reason: String,
        issue: Option<RecoveryHookIssueKind>,
    },
}

fn recover_existing_graph(
    graph_dir: &Path,
    graph_name: &str,
    schema_fingerprint: &str,
) -> anyhow::Result<ExistingGraphRecovery> {
    if !graph_dir.exists() {
        return Ok(ExistingGraphRecovery::Rebuild {
            reason: "graph directory missing; rebuilding from catalog".to_string(),
            issue: None,
        });
    }

    let version = match GraphProjectionIndex::manifest_version(graph_dir) {
        Ok(version) => version,
        Err(error) => {
            return Ok(ExistingGraphRecovery::Rebuild {
                reason: format!("manifest unreadable ({}); rebuilding from catalog", error),
                issue: Some(RecoveryHookIssueKind::ManifestMismatch),
            });
        }
    };
    if version == 1 {
        return Ok(ExistingGraphRecovery::Rebuild {
            reason: "legacy manifest version; rebuilding from catalog".to_string(),
            issue: Some(RecoveryHookIssueKind::ManifestMismatch),
        });
    }

    let manifest = match GraphProjectionIndex::load_manifest(graph_dir) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Ok(ExistingGraphRecovery::Rebuild {
                reason: format!("manifest invalid ({}); rebuilding from catalog", error),
                issue: Some(RecoveryHookIssueKind::ManifestMismatch),
            });
        }
    };

    if manifest.graph_name() != graph_name {
        return Ok(ExistingGraphRecovery::Rebuild {
            reason: format!(
                "manifest graph name mismatch (expected {}, got {}); rebuilding from catalog",
                graph_name,
                manifest.graph_name()
            ),
            issue: Some(RecoveryHookIssueKind::ManifestMismatch),
        });
    }
    if manifest.schema_fingerprint() != schema_fingerprint {
        return Ok(ExistingGraphRecovery::Rebuild {
            reason: "schema fingerprint changed; rebuilding from catalog".to_string(),
            issue: Some(RecoveryHookIssueKind::ManifestMismatch),
        });
    }
    if manifest.state() != GraphState::Ready {
        return Ok(ExistingGraphRecovery::Rebuild {
            reason: format!(
                "manifest state {:?}; rebuilding from catalog",
                manifest.state()
            ),
            issue: Some(RecoveryHookIssueKind::ManifestMismatch),
        });
    }

    let index = match GraphProjectionIndex::load(graph_dir) {
        Ok(index) => index,
        Err(error) => {
            return Ok(ExistingGraphRecovery::Rebuild {
                reason: format!(
                    "graph files unreadable ({}); rebuilding from catalog",
                    error
                ),
                issue: None,
            });
        }
    };

    Ok(ExistingGraphRecovery::Reuse {
        index,
        manifest,
        reason: "manifest validated; reusing persisted graph projection".to_string(),
    })
}

fn build_graph_index_from_catalog(
    db: &Arc<DatabaseHandle>,
    txn: &CatalogSnapshot,
    pg_info: &CreatePropertyGraphInfo,
) -> paro_common::error::Result<GraphProjectionIndex> {
    let mut vertex_inputs = Vec::with_capacity(pg_info.vertex_tables.len());
    for vt in &pg_info.vertex_tables {
        let table_entry = db
            .catalog()
            .get_table(txn, &pg_info.schema, &vt.table_name)?;
        let table = match table_entry.as_ref() {
            CatalogEntryEnum::Table(table) => table,
            _ => {
                return Err(paro_error::wrong_object_type("table", &vt.table_name));
            }
        };
        let storage = table.get_storage().ok_or_else(|| {
            paro_error::internal(format!("Vertex table \"{}\" has no storage", vt.table_name))
        })?;

        let (projected_columns, key_positions) =
            prepare_graph_key_projection(&[vt.key_column_ids.as_slice()]);
        let params = TabletReaderParams::with_version(storage.max_version())
            .with_columns(projected_columns)
            .with_emit_row_id(true);
        let mut reader = storage.create_reader(params)?;
        reader.prepare()?;

        let mut keys_and_rowids = Vec::new();
        while let Some(chunk) = reader.get_next_chunk()? {
            let rowid_col = chunk.column(chunk.column_count() - 1).ok_or_else(|| {
                paro_error::internal("Missing rowid column in vertex recovery scan")
            })?;
            for idx in 0..chunk.size() {
                let key = graph_vertex_key_from_chunk(
                    &chunk,
                    &key_positions[0],
                    idx,
                    &format!("vertex table \"{}\" key at row {}", vt.table_name, idx),
                )?;
                let rowid = graph_rowid_from_value(
                    &rowid_col.get_value(idx),
                    &format!("vertex table \"{}\" rowid at row {}", vt.table_name, idx),
                )?;
                keys_and_rowids.push((key, rowid));
            }
        }

        vertex_inputs.push(VertexBuildInput {
            label: vt.label.clone(),
            keys_and_rowids,
        });
    }

    let mut edge_inputs = Vec::with_capacity(pg_info.edge_tables.len());
    for et in &pg_info.edge_tables {
        let table_entry = db
            .catalog()
            .get_table(txn, &pg_info.schema, &et.table_name)?;
        let table = match table_entry.as_ref() {
            CatalogEntryEnum::Table(table) => table,
            _ => {
                return Err(paro_error::wrong_object_type("table", &et.table_name));
            }
        };
        let storage = table.get_storage().ok_or_else(|| {
            paro_error::internal(format!("Edge table \"{}\" has no storage", et.table_name))
        })?;

        let (projected_columns, key_positions) = prepare_graph_key_projection(&[
            et.source_key_column_ids.as_slice(),
            et.destination_key_column_ids.as_slice(),
        ]);
        let params = TabletReaderParams::with_version(storage.max_version())
            .with_columns(projected_columns)
            .with_emit_row_id(true);
        let mut reader = storage.create_reader(params)?;
        reader.prepare()?;

        let mut edges = Vec::new();
        while let Some(chunk) = reader.get_next_chunk()? {
            let rowid_col = chunk.column(chunk.column_count() - 1).ok_or_else(|| {
                paro_error::internal("Missing rowid column in edge recovery scan")
            })?;
            for idx in 0..chunk.size() {
                let src_key = graph_vertex_key_from_chunk(
                    &chunk,
                    &key_positions[0],
                    idx,
                    &format!("edge table \"{}\" source key at row {}", et.table_name, idx),
                )?;
                let dst_key = graph_vertex_key_from_chunk(
                    &chunk,
                    &key_positions[1],
                    idx,
                    &format!(
                        "edge table \"{}\" destination key at row {}",
                        et.table_name, idx
                    ),
                )?;
                let rowid = graph_rowid_from_value(
                    &rowid_col.get_value(idx),
                    &format!("edge table \"{}\" rowid at row {}", et.table_name, idx),
                )?;
                edges.push((src_key, dst_key, rowid));
            }
        }

        edge_inputs.push(EdgeBuildInput {
            label: et.label.clone(),
            source_vertex_label: et.source_vertex_table.clone(),
            destination_vertex_label: et.destination_vertex_table.clone(),
            edges,
        });
    }

    let table_to_label = pg_info
        .vertex_tables
        .iter()
        .map(|vt| (vt.table_name.as_str(), vt.label.as_str()))
        .collect::<std::collections::HashMap<_, _>>();
    for edge in &mut edge_inputs {
        if let Some(label) = table_to_label.get(edge.source_vertex_label.as_str()) {
            edge.source_vertex_label = (*label).to_string();
        }
        if let Some(label) = table_to_label.get(edge.destination_vertex_label.as_str()) {
            edge.destination_vertex_label = (*label).to_string();
        }
    }

    GraphProjectionIndex::build(&GraphBuildInput {
        graph_name: pg_info.graph_name.clone(),
        vertex_tables: vertex_inputs,
        edge_tables: edge_inputs,
        build_backward_adjacency: true,
    })
}

fn prepare_graph_key_projection(column_groups: &[&[u32]]) -> (Vec<usize>, Vec<Vec<usize>>) {
    let mut projected = Vec::new();
    let mut index_by_column = std::collections::HashMap::new();
    let mut positions = Vec::with_capacity(column_groups.len());
    for group in column_groups {
        let mut group_positions = Vec::with_capacity(group.len());
        for &column_id in *group {
            let position = if let Some(existing) = index_by_column.get(&column_id) {
                *existing
            } else {
                let next = projected.len();
                projected.push(column_id as usize);
                index_by_column.insert(column_id, next);
                next
            };
            group_positions.push(position);
        }
        positions.push(group_positions);
    }
    (projected, positions)
}

fn graph_vertex_key_from_chunk(
    chunk: &Chunk,
    positions: &[usize],
    row_idx: usize,
    context: &str,
) -> paro_common::error::Result<VertexKey> {
    if positions.len() == 1 {
        let value = chunk
            .column(positions[0])
            .ok_or_else(|| paro_error::internal(format!("Missing key column for {}", context)))?
            .get_value(row_idx);
        return graph_vertex_key_from_value(&value, context);
    }

    let mut encoded = Vec::new();
    let count = u32::try_from(positions.len())
        .map_err(|_| paro_error::out_of_range("Composite graph key column count overflow"))?;
    encoded.extend_from_slice(&count.to_le_bytes());
    for &position in positions {
        let value = chunk
            .column(position)
            .ok_or_else(|| paro_error::internal(format!("Missing key column for {}", context)))?
            .get_value(row_idx);
        encode_composite_vertex_key_part(&value, context, &mut encoded)?;
    }
    Ok(VertexKey::Composite(encoded.into_boxed_slice()))
}

fn graph_vertex_key_from_value(
    value: &Value,
    context: &str,
) -> paro_common::error::Result<VertexKey> {
    match value {
        Value::BigInt(v) => Ok(VertexKey::Int64(*v)),
        Value::Varchar(v) => Ok(VertexKey::String(v.clone().into_boxed_str())),
        Value::Null(_) => Err(paro_error::internal(format!("Missing key for {}", context))),
        _ => Err(paro_error::internal(format!(
            "Unsupported graph key type {} for {}",
            value.logical_type(),
            context
        ))),
    }
}

fn encode_composite_vertex_key_part(
    value: &Value,
    context: &str,
    encoded: &mut Vec<u8>,
) -> paro_common::error::Result<()> {
    match value {
        Value::BigInt(v) => {
            encoded.push(1);
            encoded.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        Value::Varchar(v) => {
            encoded.push(2);
            let len = u32::try_from(v.len()).map_err(|_| {
                paro_error::out_of_range("Composite graph key string length overflow")
            })?;
            encoded.extend_from_slice(&len.to_le_bytes());
            encoded.extend_from_slice(v.as_bytes());
            Ok(())
        }
        Value::Null(_) => Err(paro_error::internal(format!("Missing key for {}", context))),
        _ => Err(paro_error::internal(format!(
            "Unsupported graph key type {} for {}",
            value.logical_type(),
            context
        ))),
    }
}

fn graph_rowid_from_value(value: &Value, context: &str) -> paro_common::error::Result<u64> {
    value
        .as_i64()
        .and_then(|v| u64::try_from(v).ok())
        .ok_or_else(|| paro_error::internal(format!("Missing or invalid rowid for {}", context)))
}
