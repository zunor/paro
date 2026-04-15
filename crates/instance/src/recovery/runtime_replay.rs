use super::replay_handler::CatalogReplayHandler;
use paro_catalog::entry::IndexType;
use paro_common::effect::{
    CatalogTxnOp, CleanupDescriptor, PostCommitHookDescriptor, RuntimeTransitionDescriptor,
    StagedArtifactDescriptor,
};
use paro_common::error as paro_error;
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

    pub(super) fn replay_post_commit_hooks(
        &mut self,
        hooks: &[PostCommitHookDescriptor],
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let _ = (hooks, commit_id);
        Ok(())
    }

    fn replay_runtime_transition(
        &mut self,
        transition: &RuntimeTransitionDescriptor,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        match transition {
            RuntimeTransitionDescriptor::RegisterGraphRuntime { graph } => {
                let _ = (graph, commit_id);
                Ok(())
            }
            RuntimeTransitionDescriptor::UnregisterGraphRuntime { graph } => {
                let _ = (graph, commit_id);
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
                            Self::mark_declared_search_indexes(
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
                            Self::unmark_declared_search_indexes(
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

    pub(super) fn replay_runtime_transitions(
        &mut self,
        ops: &[CatalogTxnOp],
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        for op in ops {
            for artifact in &op.staged_artifacts {
                self.apply_staged_artifact_descriptor(artifact)?;
            }
        }
        for op in ops {
            for transition in &op.runtime_transitions {
                self.replay_runtime_transition(transition, commit_id)?;
            }
        }
        Ok(())
    }

    fn mark_declared_search_indexes(
        storage: &TableHandle,
        index_type: &str,
        column_ids: &[u32],
        fulltext_config: Option<&str>,
    ) {
        let index_type = IndexType::from_str(index_type);
        for column_id in column_ids {
            match index_type {
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

    fn unmark_declared_search_indexes(
        storage: &TableHandle,
        index_type: &str,
        column_ids: &[u32],
        _fulltext_config: Option<&str>,
    ) {
        let index_type = IndexType::from_str(index_type);
        for column_id in column_ids {
            match index_type {
                IndexType::HNSW => storage.unmark_declared_vector_index(*column_id),
                IndexType::Sparse => storage.unmark_declared_sparse_index(*column_id),
                IndexType::FullText => storage.unmark_declared_fulltext_index(*column_id),
                _ => {}
            }
        }
    }
}
