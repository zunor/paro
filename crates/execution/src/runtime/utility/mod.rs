// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! One-shot runtime for metadata-only utility statements.

use std::path::{Component, Path};

use paro_catalog::catalog::Catalog;
use paro_catalog::entry::{
    graph_schema_fingerprint, CatalogEntryEnum, CatalogType, CreateSchemaInfo, CreateTableInfo,
    DropEntryInfo, OnCreateConflict,
};
use paro_common::effect::StagingArtifactId;
use paro_common::error::{self as paro_error, Result};
use paro_common::identity::GraphId;
use paro_context::PreparedIndexArtifact;
use paro_planner::binder::ir::statement::{
    BoundCreatePropertyGraphInfo, BoundRefreshPropertyGraphInfo, DropType,
};
use paro_storage::index::graph::{
    GraphManifest, GraphProjectionIndex, GraphState, GraphStatistics,
};

use crate::operators::graph::property_graph_support::{
    build_graph_index_from_scans, graph_staging_dir, scan_graph_inputs_with_catalog,
};
use crate::operators::graph::refresh_property_graph::{refresh_scanned_graph, GraphRefreshPolicy};
use crate::physical::specs::UtilitySpec;
use crate::runtime::context::UtilityContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UtilityRunResult {
    Done,
}

pub fn run_once(spec: &UtilitySpec, ctx: &mut UtilityContext<'_>) -> Result<UtilityRunResult> {
    match spec {
        UtilitySpec::CreateTable(info) => {
            let _ = ctx.session.database(&info.database_name).ok_or_else(|| {
                paro_error::catalog(format!("Database not found: {}", info.database_name))
            })?;
            let on_conflict = if info.if_not_exists {
                OnCreateConflict::IgnoreOnConflict
            } else {
                OnCreateConflict::ErrorOnConflict
            };
            let create = CreateTableInfo::new(
                info.database_name.clone(),
                info.schema_name.clone(),
                info.table_name.clone(),
                info.columns.clone(),
            )
            .with_constraints(info.constraints.clone())
            .with_on_conflict(on_conflict);
            ctx.session
                .ddl()
                .expect("ddl context must exist inside transactions")
                .apply_create_table(create)?;
            Ok(UtilityRunResult::Done)
        }
        UtilitySpec::CreateView(info) => {
            ctx.session
                .ddl()
                .expect("ddl context must exist inside transactions")
                .apply_create_view(info.clone().to_create_view_info())?;
            Ok(UtilityRunResult::Done)
        }
        UtilitySpec::CreateSchema(info) => {
            let _ = ctx.session.database(&info.database_name).ok_or_else(|| {
                paro_error::catalog(format!("Database not found: {}", info.database_name))
            })?;
            ctx.session
                .ddl()
                .expect("ddl context must exist inside transactions")
                .apply_create_schema(CreateSchemaInfo::new(
                    info.database_name.clone(),
                    info.schema_name.clone(),
                ))?;
            Ok(UtilityRunResult::Done)
        }
        UtilitySpec::CreateSequence(info) => {
            let _ = ctx.session.database(&info.database_name).ok_or_else(|| {
                paro_error::catalog(format!("Database not found: {}", info.database_name))
            })?;
            ctx.session
                .ddl()
                .expect("ddl context must exist inside transactions")
                .apply_create_sequence(info.clone().to_create_sequence_info())?;
            Ok(UtilityRunResult::Done)
        }
        UtilitySpec::CreateIndex(spec) => {
            if !spec.info.index_type.supports_metadata_only_build() {
                return Err(paro_error::not_implemented(format!(
                    "CREATE INDEX type '{}' requires runtime index backfill, which is not migrated to typed pipelines yet",
                    spec.info.index_type.as_str()
                )));
            }
            let ddl = ctx
                .session
                .ddl()
                .expect("ddl context must exist inside transactions");
            let handle =
                ddl.prepare_index_build(spec.info.clone(), spec.table.clone(), ctx.cancel.clone())?;
            if !handle.skip_build() {
                ddl.commit_index_build(
                    handle,
                    PreparedIndexArtifact::MetadataOnly { coverage: None },
                )?;
            }
            Ok(UtilityRunResult::Done)
        }
        UtilitySpec::CreateRoutine(info) => {
            ctx.session
                .ddl()
                .expect("ddl context must exist inside transactions")
                .apply_create_routine(info.to_create_routine_info())?;
            Ok(UtilityRunResult::Done)
        }
        UtilitySpec::CreatePropertyGraph(info) => {
            run_create_property_graph(info, ctx)?;
            Ok(UtilityRunResult::Done)
        }
        UtilitySpec::Alter(info) => {
            ctx.session
                .ddl()
                .expect("ddl context must exist inside transactions")
                .apply_alter_entry(
                    info.schema_name.clone(),
                    info.info.clone(),
                    info.sql.clone(),
                )?;
            Ok(UtilityRunResult::Done)
        }
        UtilitySpec::Drop(info) => {
            let catalog = ctx.session.catalog();
            let ddl = ctx
                .session
                .ddl()
                .expect("ddl context must exist inside transactions");
            if catalog.name() != info.database_name {
                return Err(paro_error::catalog(format!(
                    "Database mismatch: expected {}, got {}",
                    info.database_name,
                    catalog.name()
                )));
            }
            match info.drop_type {
                DropType::Table => {
                    ddl.apply_drop(
                        info.schema_name.clone(),
                        drop_info(CatalogType::Table, info),
                    )?;
                }
                DropType::Schema => {
                    if info.if_exists && catalog.get_schema(ctx.catalog, &info.object_name).is_err()
                    {
                        return Ok(UtilityRunResult::Done);
                    }
                    let mut entry =
                        DropEntryInfo::new(CatalogType::Schema, info.object_name.clone());
                    if info.if_exists {
                        entry = entry.with_if_exists();
                    }
                    if info.cascade {
                        entry = entry.with_cascade();
                    }
                    ddl.apply_drop(info.object_name.clone(), entry)?;
                }
                DropType::Index => {
                    let schema = catalog.get_schema(ctx.catalog, &info.schema_name)?;
                    let existing = schema.get_index(
                        ctx.catalog.transaction_id,
                        ctx.catalog.start_time,
                        &info.object_name,
                    );
                    let Some(existing_entry) = existing else {
                        if info.if_exists {
                            return Ok(UtilityRunResult::Done);
                        }
                        return Err(paro_error::object_not_found("index", &info.object_name));
                    };
                    let CatalogEntryEnum::Index(_) = existing_entry.as_ref() else {
                        return Err(paro_error::wrong_object_type("index", &info.object_name));
                    };
                    ddl.apply_drop(
                        info.schema_name.clone(),
                        drop_info(CatalogType::Index, info),
                    )?;
                }
                DropType::View => {
                    ddl.apply_drop(info.schema_name.clone(), drop_info(CatalogType::View, info))?;
                }
                DropType::Sequence => {
                    ddl.apply_drop(
                        info.schema_name.clone(),
                        drop_info(CatalogType::Sequence, info),
                    )?;
                }
                DropType::Routine => {
                    ddl.apply_drop_routine(
                        info.schema_name.clone(),
                        info.object_name.clone(),
                        paro_catalog::entry::DropRoutineInfo {
                            arg_types: info.routine_arg_types.clone(),
                            if_exists: info.if_exists,
                        },
                    )?;
                }
            }
            Ok(UtilityRunResult::Done)
        }
        UtilitySpec::DropPropertyGraph(info) => {
            ctx.session
                .ddl()
                .expect("ddl context must exist inside transactions")
                .apply_drop_property_graph(
                    info.catalog_name.clone(),
                    info.schema_name.clone(),
                    info.graph_name.clone(),
                    info.if_exists,
                )?;
            Ok(UtilityRunResult::Done)
        }
        UtilitySpec::RefreshPropertyGraph(info) => {
            run_refresh_property_graph(info, ctx)?;
            Ok(UtilityRunResult::Done)
        }
        UtilitySpec::Unsupported(spec) => Err(paro_error::not_supported(format!(
            "utility runtime for {} is not available",
            spec.name
        ))),
    }
}

fn run_create_property_graph(
    info: &BoundCreatePropertyGraphInfo,
    ctx: &UtilityContext<'_>,
) -> Result<()> {
    let pg_info = &info.info;
    if pg_info.if_not_exists {
        let catalog = ctx.session.catalog();
        let txn = ctx.session.catalog_txn_view();
        let schema = catalog.get_schema(&txn, &pg_info.schema)?;
        if schema.get_property_graph(&txn, &pg_info.graph_name).is_ok() {
            return Ok(());
        }
    }

    let db_path = ctx.session.catalog().get_db_path();
    let txn_id = ctx
        .session
        .active_transaction()
        .ok_or_else(|| {
            paro_error::internal("CREATE PROPERTY GRAPH requires an active transaction")
        })?
        .id;
    let graph_dir = graph_staging_dir(&db_path, txn_id, &pg_info.graph_name);
    if graph_dir.exists() {
        let _ = std::fs::remove_dir_all(&graph_dir);
    }

    let schema_fingerprint = graph_schema_fingerprint(pg_info);
    write_graph_manifest(
        &graph_dir,
        &pg_info.graph_name,
        GraphState::Building,
        &schema_fingerprint,
    )?;

    let result = (|| {
        let catalog = ctx.session.catalog();
        let txn = ctx.session.catalog_txn_view();
        let scanned = scan_graph_inputs_with_catalog(catalog.as_ref(), &txn, pg_info)?;
        let index = build_graph_index_from_scans(&pg_info.graph_name, &scanned)?;
        let graph_stats = GraphStatistics::from_index(&index);
        let manifest = GraphManifest::new(
            pg_info.graph_name.clone(),
            GraphState::Ready,
            schema_fingerprint.clone(),
        )
        .with_indexed_through_ts(ctx.session.txn.transaction.visible_version())
        .with_statistics(graph_stats);
        index.save_with_manifest(&graph_dir, manifest)?;

        ctx.session
            .ddl()
            .ok_or_else(|| {
                paro_error::internal("property graph DDL requires transaction DDL context")
            })?
            .apply_create_property_graph(
                pg_info.clone(),
                staging_artifact_id(txn_id, &graph_dir),
                schema_fingerprint.clone(),
            )
    })();

    if let Err(error) = &result {
        let _ = write_graph_manifest(
            &graph_dir,
            &pg_info.graph_name,
            GraphState::Failed,
            &schema_fingerprint,
        );
        return Err(error.clone());
    }

    result
}

fn run_refresh_property_graph(
    info: &BoundRefreshPropertyGraphInfo,
    ctx: &UtilityContext<'_>,
) -> Result<()> {
    let catalog = ctx.session.catalog();
    let txn = ctx.session.catalog_txn_view();
    let schema = catalog.get_schema(&txn, &info.schema_name)?;
    let graph_entry = schema.get_property_graph(&txn, &info.graph_name)?;
    let graph_info = graph_entry.info.clone();
    let schema_fingerprint = graph_schema_fingerprint(&graph_info);
    let scanned = scan_graph_inputs_with_catalog(catalog.as_ref(), &txn, &graph_info)?;
    let graph_id = GraphId::new(&info.catalog_name, &info.schema_name, &info.graph_name);
    refresh_scanned_graph(
        &graph_id,
        ctx.session.services.graph_index.as_ref(),
        ctx.session.graph_registry.as_ref(),
        &ctx.session.catalog().get_db_path(),
        &info.graph_name,
        &schema_fingerprint,
        &scanned,
        ctx.session
            .active_transaction()
            .map(|txn| txn.id)
            .unwrap_or(scanned.indexed_through_ts),
        GraphRefreshPolicy::Synchronous,
        None,
        None,
        None,
        None,
        0,
    )
}

fn drop_info(
    catalog_type: CatalogType,
    info: &paro_planner::binder::ir::statement::BoundDropInfo,
) -> DropEntryInfo {
    let entry = DropEntryInfo::new(catalog_type, info.object_name.clone());
    if info.if_exists {
        entry.with_if_exists()
    } else {
        entry
    }
}

fn write_graph_manifest(
    graph_dir: &Path,
    graph_name: &str,
    state: GraphState,
    schema_fingerprint: &str,
) -> Result<()> {
    let manifest = GraphManifest::new(
        graph_name.to_string(),
        state,
        schema_fingerprint.to_string(),
    );
    GraphProjectionIndex::write_manifest(graph_dir, &manifest)
}

fn staging_artifact_id(txn_id: u64, graph_dir: &Path) -> StagingArtifactId {
    StagingArtifactId::new(
        txn_id,
        graph_dir
            .components()
            .filter_map(|component| match component {
                Component::Normal(value) => Some(value.to_string_lossy().to_string()),
                Component::RootDir => Some("/".to_string()),
                _ => None,
            })
            .collect(),
    )
}
