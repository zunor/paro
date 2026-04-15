// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use super::commit::CommitOutcome;
use super::ddl_changes::TransientCatalogRuntime;
use crate::session::Session;
use paro_catalog::entry::{IndexCoverage, IndexType as CatalogIndexType, LogicalIndex};
use paro_common::effect::{
    CleanupDescriptor, RuntimeTransitionDescriptor, StagedArtifactDescriptor,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::identity::GraphId;
use paro_common::logging::targets;
use paro_common::types::LogicalType;
use paro_storage::index::graph::{GraphProjectionIndex, GraphStorageGeneration};
use paro_storage::index::hnsw::{
    build_missing_hnsw_indexes_with_scheduler, DistanceMetric, HnswColumnBuildConfig, HnswConfig,
};
use paro_storage::transaction::descriptor_cleanup::{
    apply_cleanup_descriptor, path_from_components as cleanup_path_from_components,
    DescriptorCleanupQueue,
};

pub struct PostCommitActions;

impl PostCommitActions {
    pub fn execute(session: &mut Session, outcome: CommitOutcome) -> Result<()> {
        session.on_transaction_commit_prepared();
        Self::publish_staged_artifacts(session, &outcome)?;
        Self::apply_runtime_transitions(session, &outcome)?;
        session.apply_post_commit_hooks(&outcome.post_commit_hooks, outcome.commit_id);
        Self::sync_compaction(session, &outcome)?;
        Self::apply_cleanups(session, &outcome);
        session.notify_transaction_commit();
        session.current_database.maybe_gc_catalog();
        crate::utility::settings::reconcile_effective_settings(session)?;
        session.refresh_session_metadata();
        Ok(())
    }

    fn publish_staged_artifacts(session: &Session, outcome: &CommitOutcome) -> Result<()> {
        for descriptor in outcome
            .catalog_ops
            .iter()
            .flat_map(|op| op.staged_artifacts.iter())
        {
            match descriptor {
                StagedArtifactDescriptor::PropertyGraphBuild {
                    object, staging, ..
                } => {
                    let staging_path = Self::path_from_components(&staging.path_components);
                    let final_path = PathBuf::from(session.current_database.path())
                        .join("graph")
                        .join(&object.name);

                    if !staging_path.exists() {
                        if final_path.exists() {
                            continue;
                        }
                        return Err(paro_error::internal(format!(
                            "missing staged property graph artifact: {}",
                            staging_path.display()
                        )));
                    }

                    if let Some(parent) = final_path.parent() {
                        std::fs::create_dir_all(parent).map_err(|err| {
                            paro_error::internal(format!(
                                "create property graph parent dir {}: {}",
                                parent.display(),
                                err
                            ))
                        })?;
                    }

                    if final_path.exists() {
                        std::fs::remove_dir_all(&final_path).map_err(|err| {
                            paro_error::internal(format!(
                                "remove stale property graph dir {}: {}",
                                final_path.display(),
                                err
                            ))
                        })?;
                    }

                    std::fs::rename(&staging_path, &final_path).map_err(|err| {
                        paro_error::internal(format!(
                            "publish property graph staging {} -> {}: {}",
                            staging_path.display(),
                            final_path.display(),
                            err
                        ))
                    })?;
                }
            }
        }

        Ok(())
    }

    fn apply_runtime_transitions(session: &Session, outcome: &CommitOutcome) -> Result<()> {
        for op in &outcome.catalog_ops {
            for transition in &op.runtime_transitions {
                match transition {
                    RuntimeTransitionDescriptor::RegisterGraphRuntime { graph } => {
                        let graph_dir = PathBuf::from(session.current_database.path())
                            .join("graph")
                            .join(&graph.name);
                        let index = GraphProjectionIndex::load(&graph_dir)?;
                        let manifest = GraphProjectionIndex::load_manifest(&graph_dir)?;
                        let generation =
                            GraphStorageGeneration::from_index(index, manifest, outcome.commit_id);
                        let runtime_key = GraphId::new(
                            graph.database.clone(),
                            graph.schema.clone().unwrap_or_default(),
                            graph.name.clone(),
                        )
                        .runtime_key();
                        session
                            .instance
                            .graph_manager()
                            .register_generation(&runtime_key, generation);
                    }
                    RuntimeTransitionDescriptor::UnregisterGraphRuntime { graph } => {
                        let runtime_key = GraphId::new(
                            graph.database.clone(),
                            graph.schema.clone().unwrap_or_default(),
                            graph.name.clone(),
                        )
                        .runtime_key();
                        session.instance.graph_manager().unregister(&runtime_key);
                    }
                    RuntimeTransitionDescriptor::AttachIndexRuntime {
                        index,
                        table_name,
                        index_type,
                        column_ids,
                        fulltext_config,
                    } => {
                        let Some(TransientCatalogRuntime::CreateIndex(action)) =
                            op.transient_runtime.as_ref()
                        else {
                            Self::apply_descriptor_only_index_runtime(
                                session,
                                index,
                                table_name,
                                index_type,
                                column_ids,
                                fulltext_config.as_deref(),
                            )?;
                            continue;
                        };

                        let storage = action.table.get_storage().ok_or_else(|| {
                            paro_error::internal(format!(
                                "table '{}' has no storage for CREATE INDEX post-commit",
                                action.table.base.base.name
                            ))
                        })?;

                        if let Some(built_index) = action.built_index.as_ref() {
                            if storage.has_index(&action.info.name) {
                                let _ = storage.remove_index(&action.info.name);
                            }
                            storage.add_index(built_index.clone())?;
                        } else if let Err(err) =
                            Self::attach_metadata_only_index_runtime(session, action)
                        {
                            if action.info.index_type == CatalogIndexType::ART {
                                Self::cleanup_failed_art_runtime(action);
                                action.entry.mark_failed(Some(err.to_string()));
                            }
                            return Err(err);
                        }

                        let coverage = Self::recompute_index_coverage(action)?;
                        action.entry.mark_ready_with_coverage(coverage);
                    }
                    RuntimeTransitionDescriptor::DetachIndexRuntime {
                        index,
                        table_name,
                        index_type,
                        column_ids,
                        fulltext_config,
                    } => {
                        let txn = session.catalog_txn_view();
                        let schema_name = index.schema.as_deref().ok_or_else(|| {
                            paro_error::serialization_error(
                                "DROP INDEX runtime transition missing schema name",
                            )
                        })?;
                        let schema = session
                            .current_database
                            .catalog()
                            .get_schema(&txn, schema_name)?;
                        if let Some(table_entry) =
                            schema.get_table(txn.transaction_id, txn.start_time, table_name)
                        {
                            if let Some(table) = table_entry.as_ref().as_table() {
                                if let Some(storage) = table.get_storage() {
                                    let _ = storage.remove_index(&index.name);
                                    Self::detach_runtime_indexes(
                                        storage.as_ref(),
                                        index_type,
                                        column_ids,
                                        fulltext_config.as_deref(),
                                    )?;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
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
                LogicalType::Array(inner, _dim) if matches!(**inner, LogicalType::Float) => {}
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

    fn detach_runtime_indexes(
        storage: &paro_storage::table::table_handle::TableHandle,
        index_type: &str,
        column_ids: &[u32],
        _fulltext_config: Option<&str>,
    ) -> Result<()> {
        let index_type = CatalogIndexType::from_str(index_type);
        for column_id in column_ids {
            match index_type {
                CatalogIndexType::ART => {
                    storage.unmark_declared_art_index(*column_id);
                    storage.remove_runtime_art_index(*column_id)?;
                }
                CatalogIndexType::HNSW => storage.unmark_declared_vector_index(*column_id),
                CatalogIndexType::Sparse => storage.unmark_declared_sparse_index(*column_id),
                CatalogIndexType::FullText => storage.unmark_declared_fulltext_index(*column_id),
                _ => {}
            }
        }
        Ok(())
    }

    fn cleanup_failed_art_runtime(action: &crate::transaction::ddl_changes::IndexPostCommitAction) {
        let Some(storage) = action.table.get_storage() else {
            return;
        };

        for column in &action.info.column_ids {
            storage.unmark_declared_art_index(column.index);
            let _ = storage.remove_runtime_art_index(column.index);
        }
    }

    fn sync_compaction(session: &Session, outcome: &CommitOutcome) -> Result<()> {
        if outcome.catalog_ops.is_empty() {
            return Ok(());
        }

        session
            .current_database
            .sync_compaction_tablets()
            .map_err(|err| {
                paro_error::internal(format!(
                    "post-commit compaction sync failed before cleanup: {}",
                    err
                ))
            })?;
        Ok(())
    }

    fn apply_cleanups(session: &Session, outcome: &CommitOutcome) {
        let tablet_meta_manager = session.current_database.tablet_meta_manager();
        let mut queue = DescriptorCleanupQueue::default();
        for op in &outcome.catalog_ops {
            queue.enqueue(outcome.commit_id, op.cleanups.clone());
        }

        for batch in queue.drain() {
            for cleanup in &batch.descriptors {
                if let Err(err) = apply_cleanup_descriptor(cleanup, tablet_meta_manager.as_deref())
                {
                    match cleanup {
                        CleanupDescriptor::RemoveDirectory {
                            path_components, ..
                        } => {
                            let path = cleanup_path_from_components(path_components);
                            if path.exists() {
                                tracing::warn!(
                                    target: targets::TRANSACTION,
                                    epoch = batch.epoch,
                                    path = %path.display(),
                                    error = %err,
                                    "post-commit cleanup failed"
                                );
                            }
                        }
                        CleanupDescriptor::ShutdownTablet {
                            tablet_id,
                            data_dir_components,
                            ..
                        } => {
                            let path = cleanup_path_from_components(data_dir_components);
                            tracing::warn!(
                                target: targets::TRANSACTION,
                                epoch = batch.epoch,
                                tablet_id = *tablet_id,
                                path = %path.display(),
                                error = %err,
                                "post-commit tablet shutdown cleanup failed"
                            );
                        }
                    }
                }
            }
        }
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
}
