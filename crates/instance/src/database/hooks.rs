// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Post-replay recovery hooks for secondary runtime artifacts.
//!
//! Hooks run after single-database WAL recovery but before the handle is
//! published into the runtime registry.

use crate::database::handle::DatabaseHandle;
use crate::lifecycle::startup_report::StartupPolicy;
use crate::search_registry::register_search_definition;
use paro_catalog::entry::{
    graph_schema_fingerprint, CatalogEntry, CatalogEntryEnum, CreatePropertyGraphInfo,
    IndexCatalogEntry, IndexType as CatalogIndexType, PropertyGraphCatalogEntry,
};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::chunk::Chunk;
use paro_common::effect::DeferredTask;
use paro_common::error as paro_error;
use paro_common::identity::GraphId;
use paro_common::logging::targets;
use paro_common::runtime_value::Value;
use paro_execution::operators::graph::refresh_property_graph::{
    mark_property_graph_stale, refresh_property_graph_committed,
    schedule_property_graph_background_rebuild,
};
use paro_scheduler::scheduler::TaskScheduler;
use paro_storage::index::graph::{
    EdgeBuildInput, GraphBuildInput, GraphManifest, GraphProjectionIndex,
    GraphProjectionIndexManager, GraphState, GraphStatistics, GraphStorageGeneration,
    VertexBuildInput, VertexKey,
};
use paro_storage::tablet::TabletReaderParams;
use std::collections::HashMap;
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
    pub scheduler: Arc<TaskScheduler>,
    pub replayed_deferred_tasks: Vec<DeferredTask>,
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

#[derive(Debug, Default)]
pub struct DeferredTaskRecoveryHook;

#[derive(Debug, Clone, PartialEq, Eq)]
enum DeferredTaskDelivery {
    Executed(String),
    Skipped(String),
}

impl RecoveryHook for DeferredTaskRecoveryHook {
    fn name(&self) -> &'static str {
        "deferred_task"
    }

    fn run(
        &self,
        db: &Arc<DatabaseHandle>,
        ctx: &RecoveryHookContext,
    ) -> anyhow::Result<RecoveryHookResult> {
        if ctx.replayed_deferred_tasks.is_empty() {
            return Ok(RecoveryHookResult::Skipped {
                reason: "no deferred tasks recovered from journal".to_string(),
            });
        }

        let mut executed = 0usize;
        let mut skipped = 0usize;
        let mut failed = 0usize;
        let mut details = Vec::new();

        for task in &ctx.replayed_deferred_tasks {
            let delivery = match task {
                DeferredTask::FinalizeIndexState {
                    index,
                    table_name,
                    index_type,
                    column_ids,
                    fulltext_config,
                } => replay_finalize_index_state_task(
                    db,
                    ctx,
                    index,
                    table_name,
                    index_type,
                    column_ids,
                    fulltext_config.as_deref(),
                ),
                DeferredTask::GraphDmlMaintenance { deltas } => {
                    replay_graph_dml_maintenance_task(db, ctx, deltas)
                }
            };

            match delivery {
                Ok(DeferredTaskDelivery::Executed(detail)) => {
                    executed += 1;
                    details.push(detail);
                }
                Ok(DeferredTaskDelivery::Skipped(reason)) => {
                    skipped += 1;
                    details.push(reason);
                }
                Err(err) => {
                    failed += 1;
                    details.push(err.to_string());
                }
            }
        }

        if failed > 0 {
            return Ok(RecoveryHookResult::Failed {
                error: format!(
                    "{} recovered deferred task(s) failed after durable replay: {}",
                    failed,
                    details.join("; ")
                ),
                issues: Vec::new(),
            });
        }

        if executed == 0 {
            return Ok(RecoveryHookResult::Skipped {
                reason: format!(
                    "{} recovered deferred task(s) were already converged or no longer applicable",
                    skipped
                ),
            });
        }

        Ok(RecoveryHookResult::Rebuilt {
            detail: Some(format!(
                "redelivered {} deferred task(s), skipped {}: {}",
                executed,
                skipped,
                details.join("; ")
            )),
            issues: Vec::new(),
        })
    }
}

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
            reason: "search generation recovery already restores fulltext queryability".to_string(),
        })
    }
}

fn replay_finalize_index_state_task(
    db: &Arc<DatabaseHandle>,
    _ctx: &RecoveryHookContext,
    index: &paro_common::ddl::DdlObjectKey,
    table_name: &str,
    index_type: &str,
    column_ids: &[u32],
    _fulltext_config: Option<&str>,
) -> anyhow::Result<DeferredTaskDelivery> {
    let txn = CatalogSnapshot::default();
    let Some(schema_name) = index.schema.as_deref() else {
        return Err(anyhow::anyhow!(
            "deferred task for index {} is missing schema name",
            index.name
        ));
    };
    let schema = match db.catalog().get_schema(&txn, schema_name) {
        Ok(schema) => schema,
        Err(_) => {
            return Ok(DeferredTaskDelivery::Skipped(format!(
                "index {} skipped: schema {} disappeared after replay",
                index.name, schema_name
            )))
        }
    };
    let Some(table_entry) = schema.get_table(txn.transaction_id, txn.start_time, table_name) else {
        return Ok(DeferredTaskDelivery::Skipped(format!(
            "index {} skipped: table {}.{} disappeared after replay",
            index.name, schema_name, table_name
        )));
    };
    let Some(table) = table_entry.as_ref().as_table() else {
        return Ok(DeferredTaskDelivery::Skipped(format!(
            "index {} skipped: {}.{} is no longer a table",
            index.name, schema_name, table_name
        )));
    };
    let Some(storage) = table.get_storage() else {
        return Ok(DeferredTaskDelivery::Skipped(format!(
            "index {} skipped: table {}.{} has no storage",
            index.name, schema_name, table_name
        )));
    };
    let index_entry = schema
        .get_index(txn.transaction_id, txn.start_time, &index.name)
        .and_then(|entry| match &*entry {
            CatalogEntryEnum::Index(index) => Some(index.clone()),
            _ => None,
        });

    match CatalogIndexType::from_str(index_type) {
        CatalogIndexType::HNSW | CatalogIndexType::Sparse | CatalogIndexType::FullText => {
            let index_entry = index_entry.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "search index {} disappeared before recovery hook registry install",
                    index.name
                )
            })?;
            if storage
                .search_generation_coverage(index_entry.base.base.object_id.raw())
                .map_err(|err| anyhow::anyhow!(err.to_string()))?
                .is_some()
            {
                return Ok(DeferredTaskDelivery::Skipped(format!(
                    "index {} on {}.{} already restored",
                    index.name, schema_name, table_name
                )));
            }
            register_search_definition(storage.as_ref(), index_entry.as_ref())
                .map_err(|err| anyhow::anyhow!(err.to_string()))?;
            return Ok(DeferredTaskDelivery::Executed(format!(
                "index {} on {}.{} restored via search generation registry",
                index.name, schema_name, table_name
            )));
        }
        _ => {}
    }

    match CatalogIndexType::from_str(index_type) {
        CatalogIndexType::ART => {
            let [column_id] = column_ids else {
                return Err(anyhow::anyhow!(
                    "ART deferred task for index {} requires exactly one column",
                    index.name
                ));
            };
            if let Err(err) = storage.rebuild_art_index(*column_id) {
                let _ = storage.release_art_index(&index.name, *column_id);
                mark_index_state_failed(
                    index_entry.as_ref(),
                    format!("ART runtime restore failed: {}", err),
                );
                return Err(anyhow::anyhow!(err.to_string()));
            }
        }
        CatalogIndexType::FullText | CatalogIndexType::HNSW | CatalogIndexType::Sparse => {
            unreachable!("search indexes return early")
        }
        _ => {
            return Ok(DeferredTaskDelivery::Skipped(format!(
                "index {} skipped: {} requires no deferred runtime rebuild",
                index.name, index_type
            )))
        }
    }

    if let Some(index_entry) = index_entry.as_ref() {
        register_search_definition(storage.as_ref(), index_entry.as_ref())
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    }

    Ok(DeferredTaskDelivery::Executed(format!(
        "index {} on {}.{} redelivered as {} runtime build",
        index.name, schema_name, table_name, index_type
    )))
}

fn mark_index_state_failed(index_entry: Option<&Arc<IndexCatalogEntry>>, reason: String) {
    if let Some(index_entry) = index_entry {
        index_entry.mark_failed(Some(reason));
    }
}

fn replay_graph_dml_maintenance_task(
    db: &Arc<DatabaseHandle>,
    ctx: &RecoveryHookContext,
    dml_deltas: &[paro_common::effect::GraphDmlTableDelta],
) -> anyhow::Result<DeferredTaskDelivery> {
    if dml_deltas.is_empty() {
        return Ok(DeferredTaskDelivery::Skipped(
            "graph maintenance skipped: empty delta batch".to_string(),
        ));
    }

    let catalog = db.catalog().clone();
    let visible_start_time = db.transaction_manager().published_commit_id();
    let visible_txn = CatalogSnapshot::read_only(visible_start_time.saturating_add(1));
    let visible_graphs = catalog.scan_property_graphs(&visible_txn);
    if visible_graphs.is_empty() {
        return Ok(DeferredTaskDelivery::Skipped(
            "graph maintenance skipped: no property graphs in visible catalog".to_string(),
        ));
    }

    let mut graphs_to_stale: HashMap<String, Arc<PropertyGraphCatalogEntry>> = HashMap::new();
    let mut graphs_to_refresh: HashMap<String, Arc<PropertyGraphCatalogEntry>> = HashMap::new();

    for delta in dml_deltas {
        let table_oid = delta.table_oid;
        let updated_columns = delta.updated_columns.iter().copied().collect();
        for graph_entry in &visible_graphs {
            let graph_name = graph_entry.info.graph_name.clone();
            let vertex_structural = graph_entry
                .info
                .vertex_tables
                .iter()
                .find(|vertex| vertex.table_oid == table_oid)
                .map(|vertex| {
                    delta.inserted > 0
                        || delta.deleted > 0
                        || graph_update_hits_columns(&updated_columns, &vertex.key_column_ids)
                })
                .unwrap_or(false);
            if vertex_structural {
                graphs_to_refresh.remove(&graph_name);
                graphs_to_stale.insert(graph_name, Arc::clone(graph_entry));
                continue;
            }

            let edge_structural = graph_entry
                .info
                .edge_tables
                .iter()
                .find(|edge| edge.table_oid == table_oid)
                .map(|edge| {
                    delta.inserted > 0
                        || delta.deleted > 0
                        || graph_update_hits_columns(&updated_columns, &edge.source_key_column_ids)
                        || graph_update_hits_columns(
                            &updated_columns,
                            &edge.destination_key_column_ids,
                        )
                })
                .unwrap_or(false);
            if edge_structural && !graphs_to_stale.contains_key(&graph_name) {
                graphs_to_refresh.insert(graph_name, Arc::clone(graph_entry));
            }
        }
    }

    let graph_index = ctx.graph_registry.clone();
    let graph_registry = ctx.graph_registry.clone();
    let source_txn = CatalogSnapshot::read_only(u64::MAX);
    let mut touched = 0usize;
    for graph_entry in graphs_to_stale.values() {
        let graph_visible_start_time = graph_recovery_visible_start_time(
            db,
            &source_txn,
            &graph_entry.info,
            visible_start_time,
        )?;
        mark_property_graph_stale(
            catalog.as_ref(),
            graph_index.as_ref(),
            graph_registry.as_ref(),
            graph_entry,
        )
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        schedule_property_graph_background_rebuild(
            catalog.clone(),
            graph_index.clone(),
            graph_registry.clone(),
            Arc::clone(graph_entry),
            graph_visible_start_time,
        );
        touched += 1;
    }

    for graph_entry in graphs_to_refresh.values() {
        let graph_visible_start_time = graph_recovery_visible_start_time(
            db,
            &source_txn,
            &graph_entry.info,
            visible_start_time,
        )?;
        if let Err(err) = refresh_property_graph_committed(
            catalog.clone(),
            graph_index.clone(),
            graph_registry.clone(),
            Arc::clone(graph_entry),
            graph_visible_start_time,
        ) {
            tracing::warn!(
                target: targets::INSTANCE,
                hook = "deferred_task",
                graph = %graph_entry.info.graph_name,
                error = %err,
                "Deferred graph maintenance refresh failed; falling back to stale+background rebuild"
            );
            mark_property_graph_stale(
                catalog.as_ref(),
                graph_index.as_ref(),
                graph_registry.as_ref(),
                graph_entry,
            )
            .map_err(|stale_err| anyhow::anyhow!(stale_err.to_string()))?;
            schedule_property_graph_background_rebuild(
                catalog.clone(),
                graph_index.clone(),
                graph_registry.clone(),
                Arc::clone(graph_entry),
                graph_visible_start_time,
            );
        }
        touched += 1;
    }

    if touched == 0 {
        return Ok(DeferredTaskDelivery::Skipped(
            "graph maintenance skipped: recovered deltas no longer touch visible graphs"
                .to_string(),
        ));
    }

    Ok(DeferredTaskDelivery::Executed(format!(
        "graph maintenance redelivered for {} graph(s)",
        touched
    )))
}

fn graph_update_hits_columns(
    updated_columns: &std::collections::BTreeSet<u32>,
    graph_columns: &[u32],
) -> bool {
    graph_columns
        .iter()
        .any(|column_id| updated_columns.contains(column_id))
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
            let published_commit_id = db.transaction_manager().published_commit_id();
            let source_max_version = property_graph_source_max_version(db, &txn, &graph.info)?;
            let graph_valid_through_ts = published_commit_id
                .max(graph.timestamp())
                .max(source_max_version.saturating_add(1));

            match recover_existing_graph(&graph_dir, &graph_name, &schema_fingerprint)? {
                ExistingGraphRecovery::Reuse {
                    index,
                    mut manifest,
                    reason,
                } => {
                    if manifest.indexed_through_ts() < source_max_version {
                        let reason = format!(
                            "manifest indexed through {} but source tables have version {}; rebuilding from catalog",
                            manifest.indexed_through_ts(),
                            source_max_version
                        );
                        let (index, manifest) = rebuild_graph_projection(
                            db,
                            &txn,
                            &graph.info,
                            &graph_dir,
                            &graph_name,
                            &schema_fingerprint,
                            graph_valid_through_ts,
                        )?;
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
                        issues.push(RecoveryHookIssue {
                            kind: RecoveryHookIssueKind::ManifestMismatch,
                            object_name: Some(graph_name.clone()),
                            detail: reason,
                        });
                        continue;
                    }

                    if manifest.indexed_through_ts() < graph_valid_through_ts {
                        manifest = manifest.with_indexed_through_ts(graph_valid_through_ts);
                        GraphProjectionIndex::write_manifest(&graph_dir, &manifest)?;
                    }

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
                    let (index, manifest) = rebuild_graph_projection(
                        db,
                        &txn,
                        &graph.info,
                        &graph_dir,
                        &graph_name,
                        &schema_fingerprint,
                        graph_valid_through_ts,
                    )?;
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

fn rebuild_graph_projection(
    db: &Arc<DatabaseHandle>,
    txn: &CatalogSnapshot,
    graph_info: &CreatePropertyGraphInfo,
    graph_dir: &Path,
    graph_name: &str,
    schema_fingerprint: &str,
    indexed_through_ts: u64,
) -> anyhow::Result<(GraphProjectionIndex, GraphManifest)> {
    let index = build_graph_index_from_catalog(db, txn, graph_info)?;
    if graph_dir.exists() {
        let _ = std::fs::remove_dir_all(graph_dir);
    }

    let graph_stats = GraphStatistics::from_index(&index);
    let manifest = GraphManifest::new(
        graph_name.to_string(),
        GraphState::Ready,
        schema_fingerprint.to_string(),
    )
    .with_indexed_through_ts(indexed_through_ts)
    .with_statistics(graph_stats);
    index.save_with_manifest(graph_dir, manifest.clone())?;
    Ok((index, manifest))
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
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

fn property_graph_source_max_version(
    db: &Arc<DatabaseHandle>,
    txn: &CatalogSnapshot,
    pg_info: &CreatePropertyGraphInfo,
) -> paro_common::error::Result<u64> {
    let mut max_version = 0u64;
    for table_name in pg_info
        .vertex_tables
        .iter()
        .map(|vertex| vertex.table_name.as_str())
        .chain(
            pg_info
                .edge_tables
                .iter()
                .map(|edge| edge.table_name.as_str()),
        )
    {
        let table_entry = db.catalog().get_table(txn, &pg_info.schema, table_name)?;
        let table = match table_entry.as_ref() {
            CatalogEntryEnum::Table(table) => table,
            _ => return Err(paro_error::wrong_object_type("table", table_name)),
        };
        let storage = table.get_storage().ok_or_else(|| {
            paro_error::internal(format!(
                "Graph source table \"{}\" has no storage",
                table_name
            ))
        })?;
        max_version = max_version.max(storage.max_version().max(0) as u64);
    }
    Ok(max_version)
}

fn graph_recovery_visible_start_time(
    db: &Arc<DatabaseHandle>,
    txn: &CatalogSnapshot,
    pg_info: &CreatePropertyGraphInfo,
    base_visible_start_time: u64,
) -> anyhow::Result<u64> {
    let source_max_version = property_graph_source_max_version(db, txn, pg_info)?;
    Ok(base_visible_start_time.max(source_max_version.saturating_add(1)))
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

#[cfg(test)]
mod tests {
    use super::{recover_existing_graph, ExistingGraphRecovery, RecoveryHookIssueKind};
    use paro_storage::index::graph::{
        GraphBuildInput, GraphManifest, GraphProjectionIndex, GraphState, VertexBuildInput,
        VertexKey,
    };
    use tempfile::tempdir;

    fn save_graph(dir: &std::path::Path, fingerprint: &str) {
        let index = GraphProjectionIndex::build(&GraphBuildInput {
            graph_name: "g".to_string(),
            vertex_tables: vec![VertexBuildInput {
                label: "Person".to_string(),
                keys_and_rowids: vec![(VertexKey::Int64(1), 42)],
            }],
            edge_tables: Vec::new(),
            build_backward_adjacency: false,
        })
        .expect("build graph index");
        index
            .save_with_manifest(
                dir,
                GraphManifest::new("g".to_string(), GraphState::Ready, fingerprint.to_string()),
            )
            .expect("save graph index");
    }

    #[test]
    fn recover_existing_graph_reuses_matching_ready_manifest() {
        let dir = tempdir().expect("tempdir");
        save_graph(dir.path(), "fp:test");

        let recovery =
            recover_existing_graph(dir.path(), "g", "fp:test").expect("recover existing graph");
        match recovery {
            ExistingGraphRecovery::Reuse { reason, .. } => {
                assert!(reason.contains("reusing persisted graph projection"));
            }
            other => panic!("expected graph reuse, got {other:?}"),
        }
    }

    #[test]
    fn recover_existing_graph_rebuilds_on_schema_fingerprint_change() {
        let dir = tempdir().expect("tempdir");
        save_graph(dir.path(), "fp:old");

        let recovery =
            recover_existing_graph(dir.path(), "g", "fp:new").expect("recover existing graph");
        match recovery {
            ExistingGraphRecovery::Rebuild { reason, issue } => {
                assert!(reason.contains("schema fingerprint changed"));
                assert_eq!(issue, Some(RecoveryHookIssueKind::ManifestMismatch));
            }
            other => panic!("expected graph rebuild, got {other:?}"),
        }
    }

    #[test]
    fn recover_existing_graph_rebuilds_on_unsupported_legacy_manifest_format() {
        let dir = tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("meta.json"),
            r#"{
  "version": 1,
  "graph_name": "g",
  "vertices": [],
  "edges": []
}"#,
        )
        .expect("write legacy meta");

        let recovery =
            recover_existing_graph(dir.path(), "g", "fp:test").expect("recover existing graph");
        match recovery {
            ExistingGraphRecovery::Rebuild { reason, issue } => {
                assert!(
                    reason.contains("unsupported current meta.json format version 1"),
                    "unexpected reason: {reason}"
                );
                assert_eq!(issue, Some(RecoveryHookIssueKind::ManifestMismatch));
            }
            other => panic!("expected graph rebuild, got {other:?}"),
        }
    }
}
