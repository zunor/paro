// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! DDL publish helpers used by the SQL commit pipeline.

use super::super::ddl_changes::IndexPostCommitAction;
use super::super::index_backfill::IndexBackfillPlan;
use paro_catalog::entry::{
    IndexCatalogEntry, IndexCoverage, IndexType as CatalogIndexType, TableCatalogEntry,
};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::effect::{
    ApplyDescriptor, CleanupDescriptor, RuntimeTransitionDescriptor, StagedArtifactDescriptor,
};
use paro_common::error::Result;
use paro_common::identity::GraphId;
use paro_common::logging::targets;
use paro_instance::{DatabaseHandle, Instance};
use paro_storage::{
    index::{
        graph::{lock_graph_artifact_io, GraphProjectionIndex, GraphStorageGeneration},
        BoundIndex,
    },
    search::{SearchFreshnessPolicy, SearchIndexDefinition, SearchIndexKind},
    table::table_handle::TableHandle,
    transaction::descriptor_cleanup::apply_cleanup_descriptor as run_cleanup_descriptor,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

type CommitApplyWork = Box<dyn FnOnce(u64) -> Result<()> + Send + 'static>;

pub(super) fn build_apply_descriptor_phase(
    instance: Arc<Instance>,
    database: Arc<DatabaseHandle>,
    descriptors: Vec<ApplyDescriptor>,
) -> CommitApplyWork {
    let coordinates_graph_artifacts = descriptors.iter().any(|descriptor| {
        matches!(
            descriptor,
            ApplyDescriptor::PublishStagedArtifact(
                StagedArtifactDescriptor::PropertyGraphBuild { .. }
            ) | ApplyDescriptor::RuntimeTransition(
                RuntimeTransitionDescriptor::RegisterGraphRuntime { .. }
                    | RuntimeTransitionDescriptor::UnregisterGraphRuntime { .. }
            )
        )
    });
    Box::new(move |commit_id| {
        let _graph_artifact_guard = coordinates_graph_artifacts.then(lock_graph_artifact_io);
        for descriptor in descriptors {
            match descriptor {
                ApplyDescriptor::PublishStagedArtifact(artifact) => {
                    publish_staged_artifact(database.as_ref(), &artifact)?;
                }
                ApplyDescriptor::RuntimeTransition(transition) => {
                    apply_runtime_transition(
                        instance.as_ref(),
                        database.as_ref(),
                        &transition,
                        commit_id,
                    )?;
                }
                ApplyDescriptor::Cleanup(cleanup) => {
                    apply_cleanup_descriptor(database.as_ref(), &cleanup)?;
                }
            }
        }
        Ok(())
    })
}

fn publish_staged_artifact(
    database: &DatabaseHandle,
    artifact: &StagedArtifactDescriptor,
) -> Result<()> {
    match artifact {
        StagedArtifactDescriptor::PropertyGraphBuild {
            object, staging, ..
        } => {
            let staging_path = path_from_components(&staging.path_components);
            let final_path = graph_dir(Path::new(database.path()), &object.name);

            if !staging_path.exists() {
                if final_path.exists() {
                    return Ok(());
                }
                return Err(paro_common::error::internal(format!(
                    "missing staged property graph artifact during required publish: {}",
                    staging_path.display()
                )));
            }

            if let Some(parent) = final_path.parent() {
                std::fs::create_dir_all(parent).map_err(|err| {
                    paro_common::error::internal(format!(
                        "required publish create property graph parent dir {}: {}",
                        parent.display(),
                        err
                    ))
                })?;
            }

            if final_path.exists() {
                std::fs::remove_dir_all(&final_path).map_err(|err| {
                    paro_common::error::internal(format!(
                        "required publish remove stale property graph dir {}: {}",
                        final_path.display(),
                        err
                    ))
                })?;
            }

            std::fs::rename(&staging_path, &final_path).map_err(|err| {
                paro_common::error::internal(format!(
                    "required publish property graph staging {} -> {}: {}",
                    staging_path.display(),
                    final_path.display(),
                    err
                ))
            })
        }
        StagedArtifactDescriptor::BulkLoadRowset(_artifact) => Ok(()),
    }
}

fn apply_runtime_transition(
    instance: &Instance,
    database: &DatabaseHandle,
    transition: &RuntimeTransitionDescriptor,
    commit_id: u64,
) -> Result<()> {
    match transition {
        RuntimeTransitionDescriptor::AttachIndexState { .. } => Ok(()),
        RuntimeTransitionDescriptor::DetachIndexState {
            index,
            table_name,
            index_type,
            column_ids,
            ..
        } => {
            let Some(storage) = table_storage(database, index.schema.as_deref(), table_name)?
            else {
                return Ok(());
            };
            let _ = storage.remove_index(&index.name);
            detach_index_state(storage.as_ref(), &index.name, index_type, column_ids)
        }
        RuntimeTransitionDescriptor::RegisterGraphRuntime { graph } => {
            let schema_name = graph.schema.as_deref().ok_or_else(|| {
                paro_common::error::serialization_error(
                    "CREATE PROPERTY GRAPH runtime transition missing schema name",
                )
            })?;
            let graph_dir = graph_dir(Path::new(database.path()), &graph.name);
            let index = GraphProjectionIndex::load(&graph_dir)?;
            let mut manifest = GraphProjectionIndex::load_manifest(&graph_dir)?;
            if manifest.indexed_through_ts() < commit_id {
                manifest = manifest.with_indexed_through_ts(commit_id);
                GraphProjectionIndex::write_manifest(&graph_dir, &manifest)?;
            }
            let runtime_key = GraphId::new(&graph.database, schema_name, &graph.name).runtime_key();
            instance.graph_manager().register_generation(
                &runtime_key,
                GraphStorageGeneration::from_index(index, manifest, 0),
            );
            Ok(())
        }
        RuntimeTransitionDescriptor::UnregisterGraphRuntime { graph } => {
            let schema_name = graph.schema.as_deref().ok_or_else(|| {
                paro_common::error::serialization_error(
                    "DROP PROPERTY GRAPH runtime transition missing schema name",
                )
            })?;
            let runtime_key = GraphId::new(&graph.database, schema_name, &graph.name).runtime_key();
            instance.graph_manager().unregister(&runtime_key);
            Ok(())
        }
    }
}

fn apply_cleanup_descriptor(database: &DatabaseHandle, cleanup: &CleanupDescriptor) -> Result<()> {
    let tablet_meta_manager = database.tablet_meta_manager();
    run_cleanup_descriptor(cleanup, tablet_meta_manager.as_deref())
}

fn table_storage(
    database: &DatabaseHandle,
    schema_name: Option<&str>,
    table_name: &str,
) -> Result<Option<Arc<TableHandle>>> {
    let Some(schema_name) = schema_name else {
        return Err(paro_common::error::serialization_error(
            "runtime transition missing schema name",
        ));
    };
    let txn = CatalogSnapshot::read_only(database.transaction_manager().published_commit_id());
    let schema = database.catalog().get_schema(&txn, schema_name)?;
    let Some(table_entry) = schema.get_table(txn.transaction_id, txn.start_time, table_name) else {
        return Ok(None);
    };
    let Some(table) = table_entry.as_ref().as_table() else {
        return Ok(None);
    };
    Ok(table.get_storage().cloned())
}

fn detach_index_state(
    storage: &TableHandle,
    index_name: &str,
    index_type: &str,
    column_ids: &[u32],
) -> Result<()> {
    match CatalogIndexType::from_str(index_type) {
        CatalogIndexType::ART => {
            for column_id in column_ids {
                storage.forget_art_index(*column_id);
                storage.drop_art_index(*column_id)?;
            }
        }
        CatalogIndexType::HNSW | CatalogIndexType::Sparse | CatalogIndexType::FullText => {
            storage.unregister_search_definition_by_name(index_name)?;
        }
        _ => {}
    }
    Ok(())
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

#[derive(Clone)]
pub(super) struct IndexBackfillPublishTask {
    entry: Arc<IndexCatalogEntry>,
    table: Arc<TableCatalogEntry>,
    info: paro_catalog::entry::CreateIndexInfo,
    built_index: Option<Arc<dyn BoundIndex>>,
    coverage: Option<IndexCoverage>,
    backfill: Option<IndexBackfillPlan>,
}

impl IndexBackfillPublishTask {
    pub(super) fn from_action(action: &IndexPostCommitAction) -> Self {
        Self {
            entry: Arc::clone(&action.entry),
            table: Arc::clone(&action.table),
            info: action.info.clone(),
            built_index: action.built_index.clone(),
            coverage: action.coverage.clone(),
            backfill: action.backfill.clone(),
        }
    }

    pub(super) fn execute(&self, publish_ts: u64) -> Result<()> {
        if let Some(backfill) = &self.backfill {
            let report = backfill.bounded_final_catch_up(
                publish_ts,
                self.table.as_ref(),
                self.entry.as_ref(),
            )?;
            tracing::debug!(
                target: targets::TRANSACTION,
                index = %self.info.name,
                table = %self.info.table_name,
                from_ts = report.from_ts,
                to_ts = report.to_ts,
                consumed_commits = report.consumed_commits,
                "CREATE INDEX bounded final catch-up completed"
            );
        }

        let Some(storage) = self.table.get_storage() else {
            if matches!(
                self.info.index_type,
                CatalogIndexType::ART
                    | CatalogIndexType::HNSW
                    | CatalogIndexType::Sparse
                    | CatalogIndexType::FullText
            ) {
                return Err(paro_common::error::internal(format!(
                    "table '{}' has no storage for CREATE INDEX publish",
                    self.table.base.base.name
                )));
            }
            self.entry.mark_ready_with_coverage(self.coverage.clone());
            return Ok(());
        };

        if let Some(built_index) = self.built_index.as_ref() {
            if storage.has_index(&self.info.name) {
                let _ = storage.remove_index(&self.info.name);
            }
            storage.add_index(Arc::clone(built_index))?;
        } else if self.info.index_type == CatalogIndexType::ART {
            let [column_id] = self.info.column_ids.as_slice() else {
                return Err(paro_common::error::not_supported(
                    "ART indexes currently require exactly one column",
                ));
            };
            storage.install_art_index(column_id.index)?;
        }

        Self::register_search_definition(storage.as_ref(), self.entry.as_ref())?;
        let coverage = self.recompute_coverage(storage.as_ref())?;
        self.entry.mark_ready_with_coverage(coverage);
        Ok(())
    }

    fn recompute_coverage(&self, storage: &TableHandle) -> Result<Option<IndexCoverage>> {
        match self.info.index_type {
            CatalogIndexType::HNSW | CatalogIndexType::Sparse | CatalogIndexType::FullText => {
                let definition_id = self.entry.base.base.object_id.raw();
                let Some(coverage) = storage.search_generation_coverage(definition_id)? else {
                    return Ok(self.coverage.clone());
                };
                Ok(Some(IndexCoverage::from_counts(
                    coverage.visible_version,
                    coverage.visible_segment_count,
                    coverage.indexed_segment_count,
                )))
            }
            _ => Ok(self.coverage.clone()),
        }
    }

    fn register_search_definition(storage: &TableHandle, entry: &IndexCatalogEntry) -> Result<()> {
        let Some(kind) = Self::search_kind(entry.index_type) else {
            return Ok(());
        };
        let definition = SearchIndexDefinition {
            definition_id: entry.base.base.object_id.raw(),
            table_id: storage.tablet().table_id(),
            name: entry.base.base.name.clone(),
            kind,
            column_ids: entry
                .get_column_ids()
                .iter()
                .map(|column| column.index)
                .collect(),
            expression: Self::search_expression(entry),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(kind),
            provider_config: entry.provider_config.clone(),
            config_fingerprint: 0,
        };
        let expression = definition.expression.clone();
        let provider_config = definition.provider_config.clone();
        let column_ids = definition.column_ids.clone();
        let definition = SearchIndexDefinition {
            config_fingerprint: SearchIndexDefinition::try_compute_config_fingerprint(
                kind,
                &column_ids,
                expression.as_deref(),
                &provider_config,
            )?,
            ..definition
        };
        storage.register_search_definition(definition)
    }

    fn search_kind(index_type: CatalogIndexType) -> Option<SearchIndexKind> {
        match index_type {
            CatalogIndexType::HNSW => Some(SearchIndexKind::Hnsw),
            CatalogIndexType::Sparse => Some(SearchIndexKind::Sparse),
            CatalogIndexType::FullText => Some(SearchIndexKind::FullText),
            _ => None,
        }
    }

    fn search_expression(entry: &IndexCatalogEntry) -> Option<String> {
        if entry.index_type != CatalogIndexType::FullText {
            return None;
        }
        let binding = entry.fulltext_binding()?;
        Some(format!(
            "to_tsvector('{}', col_{})",
            binding.config, binding.column_id.index
        ))
    }
}
