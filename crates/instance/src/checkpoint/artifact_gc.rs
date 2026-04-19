// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint-owned tablet artifact reachability and orphan cleanup entry point.

use crate::storage_manager::StorageManager;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::checkpoint::{ArtifactRootsBundle, CheckpointArtifactRef};
use paro_storage::meta::TabletMetaManager;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const STAGED_ARTIFACT_NAMESPACE: &str = "staged";
const DELETE_PATCH_NAMESPACE: &str = "delete_patch";

#[derive(Debug, Default)]
pub struct ArtifactGc;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactGcReport {
    pub removed_graph_dirs: usize,
    pub removed_staging_entries: usize,
    pub removed_compaction_dirs: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactSweepMode {
    Startup,
    Retention,
}

impl ArtifactGc {
    pub fn checkpoint_roots(_storage: &dyn StorageManager) -> ArtifactRootsBundle {
        ArtifactRootsBundle {
            roots: Self::staged_and_delete_patch_roots(),
        }
    }

    pub fn sweep_startup_orphans(
        catalog: &ParoCatalog,
        storage: &dyn StorageManager,
        tablet_meta_manager: Option<Arc<TabletMetaManager>>,
        batch_size: usize,
        delete_budget: usize,
    ) -> anyhow::Result<ArtifactGcReport> {
        Self::sweep_orphans(
            catalog,
            storage,
            tablet_meta_manager,
            batch_size,
            delete_budget,
            ArtifactSweepMode::Startup,
        )
    }

    pub fn sweep_retention_orphans(
        catalog: &ParoCatalog,
        storage: &dyn StorageManager,
        tablet_meta_manager: Option<Arc<TabletMetaManager>>,
        batch_size: usize,
        delete_budget: usize,
    ) -> anyhow::Result<ArtifactGcReport> {
        Self::sweep_orphans(
            catalog,
            storage,
            tablet_meta_manager,
            batch_size,
            delete_budget,
            ArtifactSweepMode::Retention,
        )
    }

    fn sweep_orphans(
        catalog: &ParoCatalog,
        storage: &dyn StorageManager,
        tablet_meta_manager: Option<Arc<TabletMetaManager>>,
        batch_size: usize,
        delete_budget: usize,
        mode: ArtifactSweepMode,
    ) -> anyhow::Result<ArtifactGcReport> {
        if batch_size == 0 || delete_budget == 0 {
            return Ok(ArtifactGcReport::default());
        }

        let Some(root) = storage_root_from_path(storage.get_path()) else {
            return Ok(ArtifactGcReport::default());
        };

        let mut remaining_budget = delete_budget;
        let mut report = ArtifactGcReport::default();
        report.removed_staging_entries = Self::remove_path_contents(
            &root.join(".txn-staging"),
            batch_size.min(remaining_budget),
        )?;
        remaining_budget = remaining_budget.saturating_sub(report.removed_staging_entries);

        report.removed_graph_dirs =
            Self::sweep_graph_root(catalog, &root, batch_size.min(remaining_budget))?;
        remaining_budget = remaining_budget.saturating_sub(report.removed_graph_dirs);

        if mode == ArtifactSweepMode::Startup {
            if let Some(tablet_meta_manager) = tablet_meta_manager {
                report.removed_compaction_dirs = Self::sweep_compaction_roots(
                    &tablet_meta_manager,
                    &root,
                    batch_size.min(remaining_budget),
                )?;
            }
        }
        Ok(report)
    }

    fn staged_and_delete_patch_roots() -> Vec<CheckpointArtifactRef> {
        // Current checkpoint publish only durably materializes canonical final namespaces.
        // Staged / delete-patch ownership is not persisted yet, so the retained root set is
        // intentionally empty until those artifact classes become durable first-class state.
        let _ = (STAGED_ARTIFACT_NAMESPACE, DELETE_PATCH_NAMESPACE);
        Vec::new()
    }

    fn sweep_graph_root(catalog: &ParoCatalog, root: &Path, limit: usize) -> anyhow::Result<usize> {
        if limit == 0 {
            return Ok(0);
        }

        let graph_root = root.join("graph");
        if !graph_root.exists() {
            return Ok(0);
        }

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let live_graphs: HashSet<String> = catalog
            .scan_property_graphs(&txn)
            .into_iter()
            .map(|graph| graph.info.graph_name.clone())
            .collect();

        let mut removed = 0usize;
        for entry in fs::read_dir(&graph_root)? {
            if removed >= limit {
                break;
            }
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if live_graphs.contains(&name) {
                continue;
            }

            Self::remove_path(&path)?;
            removed += 1;
        }
        Ok(removed)
    }

    fn sweep_compaction_roots(
        tablet_meta_manager: &Arc<TabletMetaManager>,
        storage_root: &Path,
        limit: usize,
    ) -> anyhow::Result<usize> {
        if limit == 0 {
            return Ok(0);
        }

        let mut removed = 0usize;
        let mut visited = HashSet::new();

        for tablet_meta in tablet_meta_manager.scan_all_tablets()? {
            if removed >= limit {
                break;
            }
            let compaction_root = PathBuf::from(tablet_meta.data_dir()).join("_compaction");
            if visited.insert(compaction_root.clone()) {
                removed +=
                    Self::remove_path_contents(&compaction_root, limit.saturating_sub(removed))?;
            }
        }

        if removed < limit {
            if let Some(data_root) = tablet_meta_manager.data_root_dir() {
                if data_root.starts_with(storage_root) {
                    removed += Self::sweep_named_dirs(
                        data_root,
                        "_compaction",
                        &mut visited,
                        limit.saturating_sub(removed),
                    )?;
                }
            }
        }

        Ok(removed)
    }

    fn sweep_named_dirs(
        root: &Path,
        dir_name: &str,
        visited: &mut HashSet<PathBuf>,
        limit: usize,
    ) -> anyhow::Result<usize> {
        if limit == 0 || !root.exists() {
            return Ok(0);
        }

        let mut removed = 0usize;
        let mut stack = vec![root.to_path_buf()];
        while let Some(path) = stack.pop() {
            if removed >= limit {
                break;
            }
            let Ok(entries) = fs::read_dir(&path) else {
                continue;
            };
            for entry in entries {
                let entry = entry?;
                let child = entry.path();
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                if entry.file_name() == dir_name {
                    if visited.insert(child.clone()) {
                        removed +=
                            Self::remove_path_contents(&child, limit.saturating_sub(removed))?;
                    }
                    continue;
                }
                stack.push(child);
            }
        }

        Ok(removed)
    }

    fn remove_path_contents(path: &Path, limit: usize) -> anyhow::Result<usize> {
        if limit == 0 || !path.exists() {
            return Ok(0);
        }

        let mut removed = 0usize;
        for entry in fs::read_dir(path)? {
            if removed >= limit {
                break;
            }
            let entry = entry?;
            Self::remove_path(&entry.path())?;
            removed += 1;
        }
        Ok(removed)
    }

    fn remove_path(path: &Path) -> anyhow::Result<()> {
        if !path.exists() {
            return Ok(());
        }
        if path.is_dir() {
            fs::remove_dir_all(path)?;
        } else {
            fs::remove_file(path)?;
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_manager::{
        DatabaseSize, MetadataBlockInfo, SingleFileStorageCommitState, StorageCommitState,
    };
    use paro_catalog::database_catalog::ParoCatalog;
    use paro_storage::meta::{FileMetadataStore, MetadataStore};
    use paro_storage::wal::wal_entry::WalHeaderMetadata;
    use paro_storage::wal::write_ahead_log::WriteAheadLog;
    use tempfile::tempdir;

    #[derive(Debug)]
    struct TestStorageManager {
        path: String,
    }

    impl TestStorageManager {
        fn new(path: impl Into<String>) -> Self {
            Self { path: path.into() }
        }
    }

    impl StorageManager for TestStorageManager {
        fn get_path(&self) -> &str {
            &self.path
        }

        fn in_memory(&self) -> bool {
            false
        }

        fn is_read_only(&self) -> bool {
            false
        }

        fn is_loaded(&self) -> bool {
            true
        }

        fn get_wal(&self) -> Option<&WriteAheadLog> {
            None
        }

        fn get_wal_mut(&mut self) -> Option<&mut WriteAheadLog> {
            None
        }

        fn get_wal_arc(&self) -> Option<Arc<WriteAheadLog>> {
            None
        }

        fn wal_header_metadata(&self) -> Option<WalHeaderMetadata> {
            None
        }

        fn replace_wal(&mut self, _wal: Arc<WriteAheadLog>) -> paro_common::error::Result<()> {
            Ok(())
        }

        fn wal_size(&self) -> u64 {
            0
        }

        fn add_wal_size(&self, _size: u64) {}

        fn set_wal_size(&self, _size: u64) {}

        fn get_metadata_store(&self) -> Option<&dyn MetadataStore> {
            None
        }

        fn get_metadata_store_arc(&self) -> Option<Arc<dyn MetadataStore>> {
            None
        }

        fn get_tablet_meta_manager(&self) -> Option<Arc<TabletMetaManager>> {
            None
        }

        fn gen_storage_commit_state(&self, _transaction_id: u64) -> Box<dyn StorageCommitState> {
            Box::new(SingleFileStorageCommitState::new(None, 0))
        }

        fn get_database_size(&self) -> DatabaseSize {
            DatabaseSize::default()
        }

        fn get_metadata_info(&self) -> Vec<MetadataBlockInfo> {
            Vec::new()
        }

        fn initialize(&mut self) -> paro_common::error::Result<()> {
            Ok(())
        }

        fn destroy(&mut self) {}
    }

    #[test]
    fn checkpoint_roots_are_empty_under_sync_promote() {
        let temp = tempdir().expect("tempdir");
        let storage = TestStorageManager::new(temp.path().display().to_string());
        let roots = ArtifactGc::checkpoint_roots(&storage);
        assert!(
            roots.roots.is_empty(),
            "sync-promote checkpoint publish should not pin staged/delete-patch roots yet"
        );
    }

    #[test]
    fn sweep_startup_orphans_removes_txn_staging_graph_and_compaction_dirs() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        fs::create_dir_all(root.join(".txn-staging").join("txn-1")).expect("create txn staging");
        fs::create_dir_all(root.join("graph").join("orphan_graph")).expect("create graph root");
        let data_root = root.join("data");
        let compaction_root = data_root.join("tablet-1").join("_compaction").join("job-1");
        fs::create_dir_all(&compaction_root).expect("create compaction staging");

        let catalog = ParoCatalog::new("postgres".to_string());
        let meta_store: Arc<dyn MetadataStore> =
            Arc::new(FileMetadataStore::new(root.join("meta")).expect("metadata store"));
        let tablet_meta_manager = Arc::new(TabletMetaManager::with_store_and_data_root(
            meta_store, &data_root,
        ));
        let storage = TestStorageManager::new(root.display().to_string());

        let report = ArtifactGc::sweep_startup_orphans(
            &catalog,
            &storage,
            Some(tablet_meta_manager),
            usize::MAX,
            usize::MAX,
        )
        .expect("startup sweep");
        assert_eq!(report.removed_staging_entries, 1);
        assert_eq!(report.removed_graph_dirs, 1);
        assert_eq!(report.removed_compaction_dirs, 1);
        assert!(fs::read_dir(root.join(".txn-staging"))
            .expect("read txn staging")
            .next()
            .is_none());
        assert!(fs::read_dir(root.join("graph"))
            .expect("read graph root")
            .next()
            .is_none());
        assert!(fs::read_dir(data_root.join("tablet-1").join("_compaction"))
            .expect("read compaction root")
            .next()
            .is_none());
    }

    #[test]
    fn sweep_startup_orphans_respects_delete_budget() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        fs::create_dir_all(root.join(".txn-staging").join("txn-1")).expect("create txn one");
        fs::create_dir_all(root.join(".txn-staging").join("txn-2")).expect("create txn two");
        fs::create_dir_all(root.join("graph").join("orphan_graph")).expect("create graph root");

        let catalog = ParoCatalog::new("postgres".to_string());
        let storage = TestStorageManager::new(root.display().to_string());

        let report = ArtifactGc::sweep_startup_orphans(&catalog, &storage, None, 1, 1)
            .expect("startup sweep");
        assert_eq!(report.removed_staging_entries, 1);
        assert_eq!(report.removed_graph_dirs, 0);
        assert_eq!(
            fs::read_dir(root.join(".txn-staging"))
                .expect("read txn staging")
                .count(),
            1
        );
        assert!(
            root.join("graph").join("orphan_graph").exists(),
            "graph orphan should remain once delete budget is exhausted"
        );
    }

    #[test]
    fn sweep_startup_orphans_leaves_live_tablet_data_intact() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        let data_root = root.join("data");
        let live_rowset = data_root
            .join("tablet-7")
            .join("rowset-1")
            .join("segment.bin");
        fs::create_dir_all(live_rowset.parent().expect("rowset parent"))
            .expect("create live rowset dir");
        fs::write(&live_rowset, b"live-rowset").expect("write live rowset file");

        let catalog = ParoCatalog::new("postgres".to_string());
        let meta_store: Arc<dyn MetadataStore> =
            Arc::new(FileMetadataStore::new(root.join("meta")).expect("metadata store"));
        let tablet_meta_manager = Arc::new(TabletMetaManager::with_store_and_data_root(
            meta_store, &data_root,
        ));
        let storage = TestStorageManager::new(root.display().to_string());

        let report = ArtifactGc::sweep_startup_orphans(
            &catalog,
            &storage,
            Some(tablet_meta_manager),
            usize::MAX,
            usize::MAX,
        )
        .expect("startup sweep");

        assert_eq!(report, ArtifactGcReport::default());
        assert!(
            live_rowset.exists(),
            "artifact GC must not delete canonical tablet data during orphan cleanup"
        );
    }

    #[test]
    fn sweep_retention_orphans_leaves_compaction_workspaces_intact() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        fs::create_dir_all(root.join(".txn-staging").join("txn-1")).expect("create txn staging");
        fs::create_dir_all(root.join("graph").join("orphan_graph")).expect("create graph root");
        let data_root = root.join("data");
        let compaction_job = data_root.join("tablet-1").join("_compaction").join("job-1");
        fs::create_dir_all(&compaction_job).expect("create compaction staging");

        let catalog = ParoCatalog::new("postgres".to_string());
        let meta_store: Arc<dyn MetadataStore> =
            Arc::new(FileMetadataStore::new(root.join("meta")).expect("metadata store"));
        let tablet_meta_manager = Arc::new(TabletMetaManager::with_store_and_data_root(
            meta_store, &data_root,
        ));
        let storage = TestStorageManager::new(root.display().to_string());

        let report = ArtifactGc::sweep_retention_orphans(
            &catalog,
            &storage,
            Some(tablet_meta_manager),
            usize::MAX,
            usize::MAX,
        )
        .expect("retention sweep");
        assert_eq!(report.removed_staging_entries, 1);
        assert_eq!(report.removed_graph_dirs, 1);
        assert_eq!(report.removed_compaction_dirs, 0);
        assert!(
            compaction_job.exists(),
            "live retention must not delete compaction workspaces"
        );
    }
}
