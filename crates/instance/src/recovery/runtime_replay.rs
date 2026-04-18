// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::replay_handler::CatalogReplayHandler;
use paro_catalog::entry::IndexType;
use paro_common::effect::{
    ApplyDescriptor, CleanupDescriptor, RuntimeTransitionDescriptor, StagedArtifactDescriptor,
};
use paro_common::error as paro_error;
use paro_common::identity::GraphId;
use paro_storage::index::graph::{GraphProjectionIndex, GraphStorageGeneration};
use paro_storage::table::table_handle::TableHandle;
use paro_storage::transaction::descriptor_cleanup::apply_cleanup_descriptor as run_cleanup_descriptor;
use std::path::PathBuf;

impl<'a> CatalogReplayHandler<'a> {
    pub(super) fn apply_cleanup_descriptor(
        &self,
        cleanup: &CleanupDescriptor,
    ) -> paro_common::error::Result<()> {
        run_cleanup_descriptor(cleanup, self.tablet_meta_manager.as_deref())
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

    fn apply_staged_artifact_descriptor(
        &self,
        descriptor: &StagedArtifactDescriptor,
    ) -> paro_common::error::Result<()> {
        match descriptor {
            StagedArtifactDescriptor::PropertyGraphBuild {
                object, staging, ..
            } => {
                let staging_path = Self::path_from_components(&staging.path_components);
                let final_path = self.database_root.join("graph").join(&object.name);

                if !staging_path.exists() {
                    if final_path.exists() {
                        return Ok(());
                    }
                    return Err(paro_error::internal(format!(
                        "missing staged property graph artifact during recovery: {}",
                        staging_path.display()
                    )));
                }

                if let Some(parent) = final_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|err| {
                        paro_error::internal(format!(
                            "recovery create property graph parent dir {}: {}",
                            parent.display(),
                            err
                        ))
                    })?;
                }

                if final_path.exists() {
                    std::fs::remove_dir_all(&final_path).map_err(|err| {
                        paro_error::internal(format!(
                            "recovery remove stale property graph dir {}: {}",
                            final_path.display(),
                            err
                        ))
                    })?;
                }

                std::fs::rename(&staging_path, &final_path).map_err(|err| {
                    paro_error::internal(format!(
                        "recovery publish property graph staging {} -> {}: {}",
                        staging_path.display(),
                        final_path.display(),
                        err
                    ))
                })
            }
        }
    }

    fn apply_runtime_transition(
        &mut self,
        transition: &RuntimeTransitionDescriptor,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        match transition {
            RuntimeTransitionDescriptor::RegisterGraphRuntime { graph } => {
                let _ = commit_id;
                let Some(graph_registry) = self.graph_registry.as_ref() else {
                    return Ok(());
                };
                let schema_name = graph.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error(
                        "CREATE PROPERTY GRAPH runtime transition missing schema name",
                    )
                })?;
                let graph_dir = self.database_root.join("graph").join(&graph.name);
                let index = GraphProjectionIndex::load(&graph_dir)?;
                let manifest = GraphProjectionIndex::load_manifest(&graph_dir)?;
                let runtime_key =
                    GraphId::new(&graph.database, schema_name, &graph.name).runtime_key();
                graph_registry.register_generation(
                    &runtime_key,
                    GraphStorageGeneration::from_index(index, manifest, 0),
                );
                Ok(())
            }
            RuntimeTransitionDescriptor::UnregisterGraphRuntime { graph } => {
                let _ = commit_id;
                let Some(graph_registry) = self.graph_registry.as_ref() else {
                    return Ok(());
                };
                let schema_name = graph.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error(
                        "DROP PROPERTY GRAPH runtime transition missing schema name",
                    )
                })?;
                let runtime_key =
                    GraphId::new(&graph.database, schema_name, &graph.name).runtime_key();
                graph_registry.unregister(&runtime_key);
                Ok(())
            }
            RuntimeTransitionDescriptor::AttachIndexRuntime {
                index,
                table_name,
                index_type,
                column_ids,
                fulltext_config,
            } => {
                let schema_name = index.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error(
                        "CREATE INDEX runtime transition missing schema name",
                    )
                })?;
                let schema = self.ensure_schema(schema_name, commit_id)?;
                if let Some(table_entry) = schema.get_table(
                    self.transaction.transaction_id,
                    self.transaction.start_time,
                    table_name,
                ) {
                    if let Some(table) = table_entry.as_ref().as_table() {
                        if let Some(storage) = table.get_storage() {
                            Self::mark_declared_runtime_indexes(
                                storage.as_ref(),
                                index_type,
                                column_ids,
                                fulltext_config.as_deref(),
                            );
                        }
                    }
                }
                Ok(())
            }
            RuntimeTransitionDescriptor::DetachIndexRuntime {
                index,
                table_name,
                index_type,
                column_ids,
                fulltext_config,
            } => {
                let schema_name = index.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error(
                        "DROP INDEX runtime transition missing schema name",
                    )
                })?;
                let schema = self.ensure_schema(schema_name, commit_id)?;
                if let Some(table_entry) = schema.get_table(
                    self.transaction.transaction_id,
                    self.transaction.start_time,
                    table_name,
                ) {
                    if let Some(table) = table_entry.as_ref().as_table() {
                        if let Some(storage) = table.get_storage() {
                            let _ = storage.remove_index(&index.name);
                            let _ = Self::unmark_declared_runtime_indexes(
                                storage.as_ref(),
                                index_type,
                                column_ids,
                                fulltext_config.as_deref(),
                            );
                        }
                    }
                }
                Ok(())
            }
        }
    }

    pub fn apply_descriptors(
        &mut self,
        descriptors: &[ApplyDescriptor],
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        for descriptor in descriptors {
            match descriptor {
                ApplyDescriptor::PublishStagedArtifact(artifact) => {
                    self.apply_staged_artifact_descriptor(artifact)?;
                }
                ApplyDescriptor::RuntimeTransition(transition) => {
                    self.apply_runtime_transition(transition, commit_id)?;
                }
                ApplyDescriptor::Cleanup(cleanup) => {
                    self.apply_cleanup_descriptor(cleanup)?;
                }
            }
        }
        Ok(())
    }

    fn mark_declared_runtime_indexes(
        storage: &TableHandle,
        index_type: &str,
        column_ids: &[u32],
        fulltext_config: Option<&str>,
    ) {
        let index_type = IndexType::from_str(index_type);
        for column_id in column_ids {
            match index_type {
                IndexType::ART => storage.mark_declared_art_index(*column_id),
                IndexType::HNSW => storage.mark_declared_vector_index(*column_id),
                IndexType::Sparse => storage.mark_declared_sparse_index(*column_id),
                IndexType::FullText => storage.mark_declared_fulltext_index_with_config(
                    *column_id,
                    fulltext_config.unwrap_or("simple"),
                ),
                _ => {}
            }
        }
    }

    fn unmark_declared_runtime_indexes(
        storage: &TableHandle,
        index_type: &str,
        column_ids: &[u32],
        _fulltext_config: Option<&str>,
    ) -> paro_common::error::Result<()> {
        let index_type = IndexType::from_str(index_type);
        for column_id in column_ids {
            match index_type {
                IndexType::ART => {
                    storage.unmark_declared_art_index(*column_id);
                    storage.remove_runtime_art_index(*column_id)?;
                }
                IndexType::HNSW => storage.unmark_declared_vector_index(*column_id),
                IndexType::Sparse => storage.unmark_declared_sparse_index(*column_id),
                IndexType::FullText => storage.unmark_declared_fulltext_index(*column_id),
                _ => {}
            }
        }
        Ok(())
    }
}
