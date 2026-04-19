// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::commit::CommitOutcome;
use super::ddl_changes::TransientCatalogRuntime;
use crate::session::Session;
use paro_catalog::entry::{
    CatalogEntryEnum, IndexCatalogEntry, IndexCoverage, IndexType as CatalogIndexType, LogicalIndex,
};
use paro_common::effect::{
    CleanupDescriptor, DeferredTask, PostCommitHookDescriptor, RuntimeTransitionDescriptor,
    StagedArtifactDescriptor,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::identity::GraphId;
use paro_common::logging::targets;
use paro_common::types::LogicalType;
use paro_storage::index::graph::{GraphProjectionIndex, GraphStorageGeneration};
use paro_storage::index::hnsw::{
    build_missing_hnsw_indexes_with_scheduler, DistanceMetric, HnswColumnBuildConfig, HnswConfig,
};
use paro_storage::table::table_handle::TableHandle;
use paro_storage::transaction::descriptor_cleanup::apply_cleanup_descriptor as run_cleanup_descriptor;
use std::path::{Path, PathBuf};

pub struct PostCommitActions;

impl PostCommitActions {
    pub fn execute(session: &mut Session, outcome: CommitOutcome) -> Result<()> {
        session.on_transaction_commit_prepared();
        Self::execute_catalog_effects(session, &outcome);
        if !outcome.catalog_ops.is_empty() {
            if let Err(err) = session.current_database.sync_compaction_tablets() {
                tracing::warn!(
                    target: targets::TRANSACTION,
                    commit_id = outcome.commit_id,
                    error = %err,
                    "compaction tablet registry sync failed after durable catalog commit"
                );
            }
        }
        Self::execute_deferred_tasks(session, &outcome)?;
        session.notify_transaction_commit();
        session.current_database.maybe_gc_catalog();
        crate::utility::settings::reconcile_effective_settings(session)?;
        session.refresh_session_metadata();
        Ok(())
    }

    fn execute_catalog_effects(session: &Session, outcome: &CommitOutcome) {
        for op in &outcome.catalog_ops {
            for artifact in &op.staged_artifacts {
                if let Err(err) = Self::publish_staged_artifact(session, artifact) {
                    tracing::warn!(
                        target: targets::TRANSACTION,
                        commit_id = outcome.commit_id,
                        artifact = ?artifact,
                        error = %err,
                        "catalog staged artifact post-commit publish failed after durable commit"
                    );
                }
            }

            for transition in &op.runtime_transitions {
                if let Err(err) = Self::apply_runtime_transition(session, transition) {
                    tracing::warn!(
                        target: targets::TRANSACTION,
                        commit_id = outcome.commit_id,
                        transition = ?transition,
                        error = %err,
                        "catalog runtime transition failed after durable commit"
                    );
                }
            }

            for cleanup in &op.cleanups {
                if let Err(err) = Self::apply_cleanup_descriptor(session, cleanup) {
                    tracing::warn!(
                        target: targets::TRANSACTION,
                        commit_id = outcome.commit_id,
                        cleanup = ?cleanup,
                        error = %err,
                        "catalog cleanup failed after durable commit"
                    );
                }
            }
        }
    }

    fn execute_deferred_tasks(session: &Session, outcome: &CommitOutcome) -> Result<()> {
        for task in &outcome.deferred_tasks {
            let deferred_task_lag_micros = outcome
                .published_at
                .elapsed()
                .as_micros()
                .min(u64::MAX as u128) as u64;
            match task {
                DeferredTask::GraphDmlMaintenance { deltas } => {
                    tracing::info!(
                        target: targets::WAL,
                        commit_id = outcome.commit_id,
                        deferred_task_lag_micros,
                        task_kind = "graph_dml_maintenance",
                        "Dispatching deferred task after durable commit"
                    );
                    session.apply_post_commit_hooks(
                        &[PostCommitHookDescriptor::GraphDmlMaintenance {
                            deltas: deltas.clone(),
                        }],
                        outcome.commit_id,
                    );
                }
                DeferredTask::BuildIndexRuntime {
                    index,
                    table_name,
                    index_type,
                    column_ids,
                    fulltext_config,
                } => {
                    tracing::info!(
                        target: targets::WAL,
                        commit_id = outcome.commit_id,
                        deferred_task_lag_micros,
                        task_kind = "build_index_runtime",
                        index = %index.name,
                        table = %table_name,
                        "Dispatching deferred task after durable commit"
                    );
                    Self::build_index_runtime_task(
                        session,
                        outcome,
                        &index,
                        &table_name,
                        &index_type,
                        &column_ids,
                        fulltext_config.as_deref(),
                    )
                    .unwrap_or_else(|err| {
                        tracing::warn!(
                            target: targets::TRANSACTION,
                            commit_id = outcome.commit_id,
                            index = %index.name,
                            table = %table_name,
                            index_type = %index_type,
                            error = %err,
                            "deferred index runtime task failed after durable commit; keeping commit durable"
                        );
                    });
                }
            }
        }
        Ok(())
    }

    fn build_index_runtime_task(
        session: &Session,
        outcome: &CommitOutcome,
        index: &paro_common::ddl::DdlObjectKey,
        table_name: &str,
        index_type: &str,
        column_ids: &[u32],
        fulltext_config: Option<&str>,
    ) -> Result<()> {
        for op in &outcome.catalog_ops {
            let matches_transition = op.runtime_transitions.iter().any(|transition| {
                matches!(
                    transition,
                    RuntimeTransitionDescriptor::AttachIndexRuntime {
                        index: transition_index,
                        ..
                    } if transition_index == index
                )
            });
            if !matches_transition {
                continue;
            }

            let Some(TransientCatalogRuntime::CreateIndex(action)) = op.transient_runtime.as_ref()
            else {
                return Self::apply_descriptor_only_index_runtime(
                    session,
                    index,
                    table_name,
                    index_type,
                    column_ids,
                    fulltext_config,
                );
            };

            let apply_result = (|| -> Result<()> {
                let storage = action.table.get_storage().ok_or_else(|| {
                    paro_error::internal(format!(
                        "table '{}' has no storage for CREATE INDEX deferred build",
                        action.table.base.base.name
                    ))
                })?;

                if let Some(built_index) = action.built_index.as_ref() {
                    if storage.has_index(&action.info.name) {
                        let _ = storage.remove_index(&action.info.name);
                    }
                    storage.add_index(built_index.clone())?;
                } else {
                    Self::attach_metadata_only_index_runtime(session, action)?;
                }

                let coverage = Self::recompute_index_coverage(action)?;
                action.entry.mark_ready_with_coverage(coverage);
                Ok(())
            })();

            if let Err(err) = apply_result {
                Self::cleanup_failed_runtime_from_action(action);
                action.entry.mark_failed(Some(err.to_string()));
                return Err(err);
            }

            return Ok(());
        }

        Self::apply_descriptor_only_index_runtime(
            session,
            index,
            table_name,
            index_type,
            column_ids,
            fulltext_config,
        )
    }

    fn attach_metadata_only_index_runtime(
        session: &Session,
        action: &crate::transaction::ddl_changes::IndexPostCommitAction,
    ) -> Result<()> {
        let Some(storage) = action.table.get_storage() else {
            if action.info.index_type == CatalogIndexType::ART
                || action.info.index_type == CatalogIndexType::HNSW
                || action.info.index_type == CatalogIndexType::FullText
            {
                return Err(paro_error::internal(format!(
                    "table '{}' has no storage. cannot finalize {} index",
                    action.table.base.base.name,
                    action.info.index_type.as_str()
                )));
            }
            return Ok(());
        };

        Self::mark_declared_runtime_indexes(storage.as_ref(), &action.info);

        if action.info.index_type == CatalogIndexType::HNSW {
            Self::build_hnsw_indexes_parallel(session, action)?;
        }
        if action.info.index_type == CatalogIndexType::FullText {
            for col in &action.info.column_ids {
                let config = action
                    .info
                    .fulltext
                    .as_ref()
                    .and_then(|meta| {
                        if meta.column_id.index == col.index {
                            Some(meta.config.as_str())
                        } else {
                            None
                        }
                    })
                    .unwrap_or("simple");
                storage.build_runtime_fulltext_index_with_config(col.index, config)?;
            }
        }
        if action.info.index_type == CatalogIndexType::ART {
            let [column_id] = action.info.column_ids.as_slice() else {
                return Err(paro_error::not_supported(
                    "ART indexes currently require exactly one column",
                ));
            };
            storage.build_runtime_art_index(column_id.index)?;
        }

        Ok(())
    }

    fn apply_descriptor_only_index_runtime(
        session: &Session,
        index: &paro_common::ddl::DdlObjectKey,
        table_name: &str,
        index_type: &str,
        column_ids: &[u32],
        fulltext_config: Option<&str>,
    ) -> Result<()> {
        let txn = session.catalog_txn_view();
        let schema_name = index.schema.as_deref().ok_or_else(|| {
            paro_error::serialization_error("CREATE INDEX runtime transition missing schema name")
        })?;
        let schema = session
            .current_database
            .catalog()
            .get_schema(&txn, schema_name)?;
        let Some(table_entry) = schema.get_table(txn.transaction_id, txn.start_time, table_name)
        else {
            tracing::debug!(
                target: targets::TRANSACTION,
                table = %table_name,
                index = %index.name,
                "index runtime transition skipped: table disappeared before post-commit"
            );
            return Ok(());
        };
        let Some(table) = table_entry.as_ref().as_table() else {
            return Ok(());
        };
        let Some(storage) = table.get_storage() else {
            return Ok(());
        };

        let apply_result = (|| -> Result<()> {
            if CatalogIndexType::from_str(index_type) == CatalogIndexType::ART {
                let [column_id] = column_ids else {
                    return Err(paro_error::not_supported(
                        "ART indexes currently require exactly one column",
                    ));
                };
                storage.mark_declared_art_index(*column_id);
                if let Err(err) = storage.build_runtime_art_index(*column_id) {
                    storage.unmark_declared_art_index(*column_id);
                    let _ = storage.remove_runtime_art_index(*column_id);
                    return Err(err);
                }
                return Ok(());
            }

            Self::mark_declared_runtime_indexes_from_descriptor(
                storage.as_ref(),
                index_type,
                column_ids,
                fulltext_config,
            );
            Ok(())
        })();

        if let Err(err) = apply_result {
            Self::cleanup_failed_runtime_from_descriptor(storage.as_ref(), index_type, column_ids);
            if let Some(entry) = Self::resolve_index_entry(session, index) {
                entry.mark_failed(Some(err.to_string()));
            }
            return Err(err);
        }

        Ok(())
    }

    fn publish_staged_artifact(
        session: &Session,
        artifact: &StagedArtifactDescriptor,
    ) -> Result<()> {
        match artifact {
            StagedArtifactDescriptor::PropertyGraphBuild {
                object, staging, ..
            } => {
                let staging_path = Self::path_from_components(&staging.path_components);
                let final_path =
                    Self::graph_dir(Path::new(session.current_database.path()), &object.name);

                if !staging_path.exists() {
                    if final_path.exists() {
                        return Ok(());
                    }
                    return Err(paro_error::internal(format!(
                        "missing staged property graph artifact during post-commit publish: {}",
                        staging_path.display()
                    )));
                }

                if let Some(parent) = final_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|err| {
                        paro_error::internal(format!(
                            "post-commit create property graph parent dir {}: {}",
                            parent.display(),
                            err
                        ))
                    })?;
                }

                if final_path.exists() {
                    std::fs::remove_dir_all(&final_path).map_err(|err| {
                        paro_error::internal(format!(
                            "post-commit remove stale property graph dir {}: {}",
                            final_path.display(),
                            err
                        ))
                    })?;
                }

                std::fs::rename(&staging_path, &final_path).map_err(|err| {
                    paro_error::internal(format!(
                        "post-commit publish property graph staging {} -> {}: {}",
                        staging_path.display(),
                        final_path.display(),
                        err
                    ))
                })
            }
        }
    }

    fn apply_runtime_transition(
        session: &Session,
        transition: &RuntimeTransitionDescriptor,
    ) -> Result<()> {
        match transition {
            RuntimeTransitionDescriptor::AttachIndexRuntime { .. } => Ok(()),
            RuntimeTransitionDescriptor::DetachIndexRuntime {
                index,
                table_name,
                index_type,
                column_ids,
                ..
            } => {
                let Some(storage) =
                    Self::table_storage(session, index.schema.as_deref(), table_name)?
                else {
                    return Ok(());
                };
                let _ = storage.remove_index(&index.name);
                Self::unmark_declared_runtime_indexes(storage.as_ref(), index_type, column_ids)
            }
            RuntimeTransitionDescriptor::RegisterGraphRuntime { graph } => {
                let schema_name = graph.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error(
                        "CREATE PROPERTY GRAPH runtime transition missing schema name",
                    )
                })?;
                let graph_dir =
                    Self::graph_dir(Path::new(session.current_database.path()), &graph.name);
                let index = GraphProjectionIndex::load(&graph_dir)?;
                let manifest = GraphProjectionIndex::load_manifest(&graph_dir)?;
                let runtime_key =
                    GraphId::new(&graph.database, schema_name, &graph.name).runtime_key();
                session.instance.graph_manager().register_generation(
                    &runtime_key,
                    GraphStorageGeneration::from_index(index, manifest, 0),
                );
                Ok(())
            }
            RuntimeTransitionDescriptor::UnregisterGraphRuntime { graph } => {
                let schema_name = graph.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error(
                        "DROP PROPERTY GRAPH runtime transition missing schema name",
                    )
                })?;
                let runtime_key =
                    GraphId::new(&graph.database, schema_name, &graph.name).runtime_key();
                session.instance.graph_manager().unregister(&runtime_key);
                Ok(())
            }
        }
    }

    fn apply_cleanup_descriptor(session: &Session, cleanup: &CleanupDescriptor) -> Result<()> {
        let tablet_meta_manager = session.current_database.tablet_meta_manager();
        run_cleanup_descriptor(cleanup, tablet_meta_manager.as_deref())
    }

    fn table_storage(
        session: &Session,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<Option<std::sync::Arc<TableHandle>>> {
        let Some(schema_name) = schema_name else {
            return Err(paro_error::serialization_error(
                "runtime transition missing schema name",
            ));
        };
        let txn = session.catalog_txn_view();
        let schema = session
            .current_database
            .catalog()
            .get_schema(&txn, schema_name)?;
        let Some(table_entry) = schema.get_table(txn.transaction_id, txn.start_time, table_name)
        else {
            return Ok(None);
        };
        let Some(table) = table_entry.as_ref().as_table() else {
            return Ok(None);
        };
        Ok(table.get_storage().cloned())
    }

    fn graph_dir(database_root: &Path, graph_name: &str) -> PathBuf {
        database_root.join("graph").join(graph_name)
    }

    fn path_from_components(components: &[String]) -> PathBuf {
        let mut iter = components.iter();
        let Some(first) = iter.next() else {
            return PathBuf::new();
        };
        let mut path = PathBuf::from(first);
        for component in iter {
            path.push(component);
        }
        path
    }

    fn resolve_index_entry(
        session: &Session,
        index: &paro_common::ddl::DdlObjectKey,
    ) -> Option<std::sync::Arc<IndexCatalogEntry>> {
        let txn = session.catalog_txn_view();
        let schema_name = index.schema.as_deref()?;
        let schema = session
            .current_database
            .catalog()
            .get_schema(&txn, schema_name)
            .ok()?;
        let entry = schema.get_index(txn.transaction_id, txn.start_time, &index.name)?;
        match &*entry {
            CatalogEntryEnum::Index(index_entry) => Some(index_entry.clone()),
            _ => None,
        }
    }

    fn cleanup_failed_runtime_from_action(
        action: &crate::transaction::ddl_changes::IndexPostCommitAction,
    ) {
        let Some(storage) = action.table.get_storage() else {
            return;
        };
        let column_ids = action
            .info
            .column_ids
            .iter()
            .map(|column| column.index)
            .collect::<Vec<_>>();
        Self::cleanup_failed_runtime_from_descriptor(
            storage.as_ref(),
            action.info.index_type.as_str(),
            &column_ids,
        );
    }

    fn cleanup_failed_runtime_from_descriptor(
        storage: &paro_storage::table::table_handle::TableHandle,
        index_type: &str,
        column_ids: &[u32],
    ) {
        match CatalogIndexType::from_str(index_type) {
            CatalogIndexType::ART => {
                for column_id in column_ids {
                    storage.unmark_declared_art_index(*column_id);
                    let _ = storage.remove_runtime_art_index(*column_id);
                }
            }
            CatalogIndexType::HNSW => {
                for column_id in column_ids {
                    storage.unmark_declared_vector_index(*column_id);
                }
            }
            CatalogIndexType::Sparse => {
                for column_id in column_ids {
                    storage.unmark_declared_sparse_index(*column_id);
                }
            }
            CatalogIndexType::FullText => {
                for column_id in column_ids {
                    storage.unmark_declared_fulltext_index(*column_id);
                }
            }
            _ => {}
        }
    }

    fn unmark_declared_runtime_indexes(
        storage: &TableHandle,
        index_type: &str,
        column_ids: &[u32],
    ) -> Result<()> {
        match CatalogIndexType::from_str(index_type) {
            CatalogIndexType::ART => {
                for column_id in column_ids {
                    storage.unmark_declared_art_index(*column_id);
                    storage.remove_runtime_art_index(*column_id)?;
                }
            }
            CatalogIndexType::HNSW => {
                for column_id in column_ids {
                    storage.unmark_declared_vector_index(*column_id);
                }
            }
            CatalogIndexType::Sparse => {
                for column_id in column_ids {
                    storage.unmark_declared_sparse_index(*column_id);
                }
            }
            CatalogIndexType::FullText => {
                for column_id in column_ids {
                    storage.unmark_declared_fulltext_index(*column_id);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn build_hnsw_indexes_parallel(
        session: &Session,
        action: &crate::transaction::ddl_changes::IndexPostCommitAction,
    ) -> Result<()> {
        let storage = action.table.get_storage().ok_or_else(|| {
            paro_error::internal(format!(
                "table '{}' has no storage. cannot build HNSW index",
                action.table.base.base.name
            ))
        })?;
        let tablet = storage.tablet();
        let schema = tablet.schema().ok_or_else(|| {
            paro_error::internal(format!(
                "table '{}' has no schema. cannot build HNSW index",
                action.table.base.base.name
            ))
        })?;

        let mut columns = Vec::new();
        for column in &action.info.column_ids {
            let column_id = column.index;
            let schema_col = schema.column_by_id(column_id).ok_or_else(|| {
                paro_error::column_not_found(format!(
                    "HNSW index column {} not found in table schema",
                    column_id
                ))
            })?;
            match &schema_col.logical_type {
                LogicalType::Array(inner, _dim) if matches!(inner.as_ref(), LogicalType::Float) => {
                }
                other => {
                    return Err(paro_error::not_supported(format!(
                        "HNSW index requires Array(Float, N), got {:?} for column {}",
                        other, column_id
                    )));
                }
            }

            columns.push(HnswColumnBuildConfig::new(
                column_id,
                HnswConfig::new(schema_col.hnsw_m, schema_col.hnsw_ef_construct),
                DistanceMetric::from_u8(schema_col.hnsw_distance),
            ));
        }

        if columns.is_empty() {
            return Ok(());
        }

        let visible_version = tablet.max_version();
        let rowsets = tablet.capture_consistent_rowsets(visible_version)?;
        if rowsets.is_empty() {
            return Ok(());
        }

        build_missing_hnsw_indexes_with_scheduler(
            &rowsets,
            &columns,
            session.instance.get_scheduler().clone(),
        )?;
        Ok(())
    }

    fn mark_declared_runtime_indexes(
        storage: &paro_storage::table::table_handle::TableHandle,
        info: &paro_catalog::entry::CreateIndexInfo,
    ) {
        for LogicalIndex { index, .. } in &info.column_ids {
            match info.index_type {
                CatalogIndexType::ART => storage.mark_declared_art_index(*index),
                CatalogIndexType::HNSW => storage.mark_declared_vector_index(*index),
                CatalogIndexType::Sparse => storage.mark_declared_sparse_index(*index),
                CatalogIndexType::FullText => {
                    let config = info
                        .fulltext
                        .as_ref()
                        .and_then(|meta| {
                            if meta.column_id.index == *index {
                                Some(meta.config.as_str())
                            } else {
                                None
                            }
                        })
                        .unwrap_or("simple");
                    storage.mark_declared_fulltext_index_with_config(*index, config);
                }
                _ => {}
            }
        }
    }

    fn recompute_index_coverage(
        action: &crate::transaction::ddl_changes::IndexPostCommitAction,
    ) -> Result<Option<IndexCoverage>> {
        if action.info.index_type != CatalogIndexType::FullText {
            return Ok(action.coverage.clone());
        }

        let Some(binding) = action.info.fulltext.as_ref() else {
            return Ok(action.coverage.clone());
        };
        let Some(storage) = action.table.get_storage() else {
            return Ok(action.coverage.clone());
        };

        let coverage = storage.fulltext_index_coverage(binding.column_id.index)?;
        Ok(Some(IndexCoverage::from_counts(
            coverage.visible_version,
            coverage.visible_segment_count,
            coverage.indexed_segment_count,
        )))
    }

    fn mark_declared_runtime_indexes_from_descriptor(
        storage: &paro_storage::table::table_handle::TableHandle,
        index_type: &str,
        column_ids: &[u32],
        fulltext_config: Option<&str>,
    ) {
        let index_type = CatalogIndexType::from_str(index_type);
        for column_id in column_ids {
            match index_type {
                CatalogIndexType::ART => storage.mark_declared_art_index(*column_id),
                CatalogIndexType::HNSW => storage.mark_declared_vector_index(*column_id),
                CatalogIndexType::Sparse => storage.mark_declared_sparse_index(*column_id),
                CatalogIndexType::FullText => storage.mark_declared_fulltext_index_with_config(
                    *column_id,
                    fulltext_config.unwrap_or("simple"),
                ),
                _ => {}
            }
        }
    }
}
