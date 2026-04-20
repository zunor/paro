// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::checkpoint::view::CheckpointView;
use crate::storage_manager::StorageManager;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogEntry, CatalogEntryEnum, CatalogType, IndexBuildState};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::checkpoint::{
    DeferredTaskKey, DeferredTaskKind, DeferredTaskProgress, DeferredTaskScope,
    DerivedProgressBundle, GraphManifestProgressEntry, PrimaryIndexProgressEntry,
};
use paro_storage::meta::TabletMetaManager;
use std::path::{Path, PathBuf};

#[derive(Debug, Default)]
pub struct DerivedProgressWriter;

impl DerivedProgressWriter {
    pub fn serialize_view(
        catalog: &ParoCatalog,
        storage: &dyn StorageManager,
        view: &CheckpointView,
    ) -> anyhow::Result<DerivedProgressBundle> {
        Ok(DerivedProgressBundle {
            primary_indexes: Self::primary_indexes(storage)?,
            graph_manifests: Self::graph_manifests(storage)?,
            deferred_tasks: Self::deferred_tasks(catalog, view),
        })
    }

    fn primary_indexes(
        storage: &dyn StorageManager,
    ) -> anyhow::Result<Vec<PrimaryIndexProgressEntry>> {
        let Some(metadata_store) = storage.get_metadata_store() else {
            return Ok(Vec::new());
        };

        let mut entries = Vec::new();
        for (key, payload) in metadata_store
            .scan_prefix("tablet/")
            .map_err(|e| anyhow::anyhow!(e))?
        {
            let Some(tablet_id) = TabletMetaManager::decode_persistent_index_key(&key) else {
                continue;
            };
            entries.push(PrimaryIndexProgressEntry { tablet_id, payload });
        }
        entries.sort_by_key(|entry| entry.tablet_id);
        Ok(entries)
    }

    fn graph_manifests(
        storage: &dyn StorageManager,
    ) -> anyhow::Result<Vec<GraphManifestProgressEntry>> {
        let Some(root) = storage_root_from_path(storage.get_path()) else {
            return Ok(Vec::new());
        };
        let graph_root = root.join("graph");
        if !graph_root.exists() {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();
        for dir_entry in std::fs::read_dir(&graph_root)? {
            let dir_entry = dir_entry?;
            if !dir_entry.file_type()?.is_dir() {
                continue;
            }
            let graph_name = dir_entry.file_name().to_string_lossy().to_string();
            let manifest_path = dir_entry.path().join("meta.json");
            if !manifest_path.exists() {
                continue;
            }
            entries.push(GraphManifestProgressEntry {
                graph_name: graph_name.clone(),
                locator: relative_locator(&root, &manifest_path)?,
                payload: std::fs::read(&manifest_path)?,
            });
        }
        entries.sort_by(|left, right| left.graph_name.cmp(&right.graph_name));
        Ok(entries)
    }

    fn deferred_tasks(catalog: &ParoCatalog, view: &CheckpointView) -> Vec<DeferredTaskProgress> {
        let txn = CatalogSnapshot::read_only(view.catalog_snapshot_ts);
        let mut tasks = Vec::new();

        for schema_entry in catalog
            .get_schema_collection()
            .scan(txn.transaction_id, txn.start_time)
        {
            let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
                continue;
            };
            let Some(indexes) = schema.collection(CatalogType::Index) else {
                continue;
            };
            for index_entry in indexes.scan(txn.transaction_id, txn.start_time) {
                let CatalogEntryEnum::Index(index) = index_entry.as_ref() else {
                    continue;
                };
                match index.build_state() {
                    IndexBuildState::Ready => {}
                    IndexBuildState::Building => tasks.push(DeferredTaskProgress {
                        task_key: DeferredTaskKey {
                            task_kind: DeferredTaskKind::FinalizeIndexState,
                            scope: DeferredTaskScope::Object(index.object_id().raw()),
                        },
                        visible_lsn: view.frontier.checkpoint_lsn,
                        completed_lsn: None,
                        failed_lsn: None,
                        last_error: None,
                    }),
                    IndexBuildState::Failed => tasks.push(DeferredTaskProgress {
                        task_key: DeferredTaskKey {
                            task_kind: DeferredTaskKind::FinalizeIndexState,
                            scope: DeferredTaskScope::Object(index.object_id().raw()),
                        },
                        visible_lsn: view.frontier.checkpoint_lsn,
                        completed_lsn: None,
                        failed_lsn: Some(view.frontier.checkpoint_lsn),
                        last_error: Some(format!(
                            "index {} remains FAILED in checkpoint-visible catalog",
                            index.base.base.name
                        )),
                    }),
                }
            }
        }

        tasks.sort_by(|left, right| {
            deferred_scope_sort_key(&left.task_key.scope)
                .cmp(&deferred_scope_sort_key(&right.task_key.scope))
                .then(left.visible_lsn.cmp(&right.visible_lsn))
        });
        tasks
    }
}

fn deferred_scope_sort_key(scope: &DeferredTaskScope) -> (u8, String) {
    match scope {
        DeferredTaskScope::Object(id) => (0, id.to_string()),
        DeferredTaskScope::Tablet(id) => (1, id.to_string()),
        DeferredTaskScope::Global(name) => (2, name.clone()),
    }
}

fn storage_root_from_path(path: &str) -> Option<PathBuf> {
    let base_path = path.split('?').next().unwrap_or(path);
    if base_path.is_empty() || base_path == ":memory:" {
        None
    } else {
        Some(PathBuf::from(base_path))
    }
}

fn relative_locator(root: &Path, path: &Path) -> anyhow::Result<String> {
    let relative = path.strip_prefix(root).map_err(|err| {
        anyhow::anyhow!(
            "path {} is not under database root {}: {}",
            path.display(),
            root.display(),
            err
        )
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}
