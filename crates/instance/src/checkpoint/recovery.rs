// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::manifest_store::ManifestStore;
use super::writers::CatalogWriter;
use crate::recovery::{restore_runtime_art_indexes, restore_search_registry_definitions};
use crate::storage_manager::StorageManager;
use bincode::Options;
use parking_lot::RwLock;
use paro_catalog::catalog::Catalog;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogEntry, CatalogEntryEnum, CatalogType, IndexType};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::checkpoint::{
    BundleKind, CheckpointFrontier, CheckpointManifest, CheckpointTabletBundle, DeferredTaskKind,
    DeferredTaskProgress, DeferredTaskScope, DerivedProgressBundle, JournalTailRef,
    RecoverySummary, RouteRegistryBundle, SnapshotBundleRef, TabletShardBundle,
};
use paro_common::logging::targets;
use paro_storage::meta::{MetadataOp, MetadataStore, TabletMetaManager};
use paro_storage::primary_key::DeleteVector;
use paro_storage::rowset::RowsetMeta;
use paro_storage::tablet::TabletMeta;
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

/// Checkpoint-owned recovery helpers for loading snapshot base state before WAL
/// replay advances runtime state.
pub struct CheckpointRecovery;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckpointBaseState {
    pub checkpoint_id: Option<u64>,
    pub frontier: Option<CheckpointFrontier>,
    pub journal_tail: Option<JournalTailRef>,
    pub bootstrap: RecoverySummary,
    pub deferred_tasks: Vec<DeferredTaskProgress>,
}

#[derive(Debug, Default)]
struct LoadedCheckpointBundles {
    catalog_bytes: Vec<u8>,
    route_registry: Option<RouteRegistryBundle>,
    tablet_shards: Vec<TabletShardBundle>,
    derived_progress: DerivedProgressBundle,
}

fn checkpoint_bincode() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

impl CheckpointRecovery {
    pub fn load_base_from_storage(
        catalog: &ParoCatalog,
        storage: &dyn StorageManager,
        tablet_meta: Option<Arc<TabletMetaManager>>,
    ) -> anyhow::Result<CheckpointBaseState> {
        let Some(manifest_store) = ManifestStore::open_for_storage(storage)? else {
            return Ok(CheckpointBaseState::default());
        };
        let removed = manifest_store.sweep_orphan_staging_dirs()?;
        if !removed.is_empty() {
            tracing::info!(
                target: targets::CHECKPOINT,
                removed = removed.len(),
                "Removed orphan checkpoint staging directories during startup"
            );
        }

        let Some(manifest) = manifest_store.read_current_manifest()? else {
            return Ok(CheckpointBaseState::default());
        };
        Self::validate_manifest_identity(storage, &manifest)?;
        let bundles = Self::load_bundles(&manifest_store, &manifest.bundle_refs)?;
        Self::preflight_snapshot(catalog, tablet_meta.clone(), &bundles)?;

        if let Some(tablet_meta_manager) = tablet_meta.as_ref() {
            Self::apply_tablet_state(tablet_meta_manager, &bundles.tablet_shards)?;
        }
        CatalogWriter::deserialize(&bundles.catalog_bytes, catalog, tablet_meta.clone())?;
        Self::apply_derived_progress(
            tablet_meta.as_ref(),
            storage_root_from_path(storage.get_path()),
            &bundles.derived_progress,
        )?;

        tracing::info!(
            target: targets::CHECKPOINT,
            checkpoint_id = manifest.checkpoint_id,
            checkpoint_lsn = manifest.frontier.checkpoint_lsn,
            restored_primary_indexes = bundles.derived_progress.primary_indexes.len(),
            restored_graph_manifests = bundles.derived_progress.graph_manifests.len(),
            deferred_tasks = bundles.derived_progress.deferred_tasks.len(),
            "Loaded checkpoint base state from committed manifest"
        );

        Ok(CheckpointBaseState {
            checkpoint_id: Some(manifest.checkpoint_id),
            frontier: Some(manifest.frontier),
            journal_tail: Some(manifest.journal),
            bootstrap: manifest.bootstrap,
            deferred_tasks: bundles.derived_progress.deferred_tasks,
        })
    }

    pub fn redeliver_deferred_tasks(
        catalog: &Arc<ParoCatalog>,
        deferred_tasks: &[DeferredTaskProgress],
    ) -> usize {
        if deferred_tasks.is_empty() {
            return 0;
        }

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let mut actionable = 0usize;
        let mut skipped_missing = 0usize;
        let mut skipped_unsupported = 0usize;
        let mut needs_art_restore = false;
        for task in deferred_tasks {
            if task.completed_lsn.unwrap_or(0) >= task.visible_lsn {
                continue;
            }

            match (&task.task_key.task_kind, &task.task_key.scope) {
                (DeferredTaskKind::FinalizeIndexState, DeferredTaskScope::Object(object_id)) => {
                    match Self::find_index_type(catalog.as_ref(), &txn, *object_id) {
                        Some(IndexType::ART) => {
                            needs_art_restore = true;
                            actionable += 1;
                        }
                        Some(IndexType::FullText)
                        | Some(IndexType::Sparse)
                        | Some(IndexType::HNSW) => actionable += 1,
                        Some(_) => {
                            skipped_unsupported += 1;
                        }
                        None if Self::catalog_contains_object(
                            catalog.as_ref(),
                            &txn,
                            *object_id,
                        ) =>
                        {
                            skipped_unsupported += 1;
                        }
                        None => {
                            skipped_missing += 1;
                        }
                    }
                }
                (DeferredTaskKind::FinalizeIndexState, _) => {
                    skipped_unsupported += 1;
                }
            }
        }

        if needs_art_restore {
            restore_runtime_art_indexes(catalog);
        }
        restore_search_registry_definitions(catalog);

        if actionable > 0 {
            tracing::info!(
                target: targets::CHECKPOINT,
                actionable,
                needs_art_restore,
                skipped_missing,
                skipped_unsupported,
                "Checkpoint deferred task progress executed startup redelivery"
            );
        } else if skipped_missing > 0 || skipped_unsupported > 0 {
            tracing::info!(
                target: targets::CHECKPOINT,
                skipped_missing,
                skipped_unsupported,
                "Checkpoint deferred task progress had no actionable startup work"
            );
        }
        actionable
    }

    fn validate_manifest_identity(
        storage: &dyn StorageManager,
        manifest: &CheckpointManifest,
    ) -> anyhow::Result<()> {
        let expected = ManifestStore::load_database_identity(storage)?;
        if manifest.database_identity == expected {
            return Ok(());
        }

        anyhow::bail!(
            "checkpoint manifest {} identity mismatch: manifest database_id={} db_identifier={} created_at_ms={} but storage database_id={} db_identifier={} created_at_ms={}",
            manifest.checkpoint_id,
            manifest.database_identity.database_id,
            format_db_identifier(&manifest.database_identity.db_identifier),
            manifest.database_identity.created_at_ms,
            expected.database_id,
            format_db_identifier(&expected.db_identifier),
            expected.created_at_ms
        )
    }

    fn load_bundles(
        manifest_store: &ManifestStore,
        bundle_refs: &[SnapshotBundleRef],
    ) -> anyhow::Result<LoadedCheckpointBundles> {
        let catalog_bundle = bundle_refs
            .iter()
            .find(|bundle| matches!(bundle.kind, BundleKind::Catalog))
            .ok_or_else(|| anyhow::anyhow!("checkpoint manifest is missing catalog bundle"))?;
        let catalog_bytes = manifest_store.read_bundle_payload(catalog_bundle)?;

        let route_registry = bundle_refs
            .iter()
            .find(|bundle| matches!(bundle.kind, BundleKind::RouteRegistry))
            .map(|bundle| manifest_store.read_bundle_payload(bundle))
            .transpose()?
            .map(|payload| checkpoint_bincode().deserialize(&payload))
            .transpose()?;

        let mut tablet_shards = Vec::new();
        for bundle in bundle_refs {
            if !matches!(bundle.kind, BundleKind::TabletShard { .. }) {
                continue;
            }
            let payload = manifest_store.read_bundle_payload(bundle)?;
            let shard: TabletShardBundle = checkpoint_bincode().deserialize(&payload)?;
            tablet_shards.push(shard);
        }

        let derived_progress = bundle_refs
            .iter()
            .find(|bundle| matches!(bundle.kind, BundleKind::DerivedProgress))
            .map(|bundle| manifest_store.read_bundle_payload(bundle))
            .transpose()?
            .map(|payload| checkpoint_bincode().deserialize(&payload))
            .transpose()?
            .unwrap_or_default();

        Ok(LoadedCheckpointBundles {
            catalog_bytes,
            route_registry,
            tablet_shards,
            derived_progress,
        })
    }

    fn preflight_snapshot(
        catalog: &ParoCatalog,
        tablet_meta: Option<Arc<TabletMetaManager>>,
        bundles: &LoadedCheckpointBundles,
    ) -> anyhow::Result<()> {
        let scratch_catalog = ParoCatalog::new(catalog.name().to_string());
        scratch_catalog.initialize(false);

        let scratch_tablet_meta = if tablet_meta.is_some() {
            Some(Arc::new(TabletMetaManager::with_store(Arc::new(
                InMemoryMetadataStore::default(),
            ))))
        } else {
            None
        };

        if let Some(tablet_meta_manager) = scratch_tablet_meta.as_ref() {
            Self::apply_tablet_state(tablet_meta_manager, &bundles.tablet_shards)?;
        }
        CatalogWriter::deserialize(
            &bundles.catalog_bytes,
            &scratch_catalog,
            scratch_tablet_meta,
        )?;
        if let Some(route_registry) = bundles.route_registry.as_ref() {
            Self::validate_route_registry_bundle(route_registry, &scratch_catalog)?;
        }
        Ok(())
    }

    fn apply_tablet_state(
        tablet_meta_manager: &Arc<TabletMetaManager>,
        shards: &[TabletShardBundle],
    ) -> anyhow::Result<()> {
        if shards.is_empty() {
            let existing = tablet_meta_manager.scan_all_tablets()?;
            for meta in existing {
                tablet_meta_manager.remove_tablet_meta(meta.tablet_id())?;
            }
            return Ok(());
        }

        let snapshot_tablets: HashSet<u64> = shards
            .iter()
            .flat_map(|shard| shard.tablets.iter().map(|tablet| tablet.identity.tablet_id))
            .collect();
        let existing = tablet_meta_manager.scan_all_tablets()?;
        for meta in existing {
            if !snapshot_tablets.contains(&meta.tablet_id()) {
                tablet_meta_manager.remove_tablet_meta(meta.tablet_id())?;
            }
        }

        for shard in shards {
            for tablet in &shard.tablets {
                Self::restore_one_tablet(tablet_meta_manager, tablet)?;
            }
        }

        tablet_meta_manager.rebuild_storage_manifest()?;
        Ok(())
    }

    fn restore_one_tablet(
        tablet_meta_manager: &Arc<TabletMetaManager>,
        tablet: &CheckpointTabletBundle,
    ) -> anyhow::Result<()> {
        let tablet_id = tablet.identity.tablet_id;
        if tablet_meta_manager.load_tablet_meta(tablet_id)?.is_some() {
            tablet_meta_manager.remove_tablet_meta(tablet_id)?;
        }

        let tablet_meta = TabletMeta::deserialize(&tablet.meta_bytes)?;
        tablet_meta_manager.save_tablet_meta(&tablet_meta)?;
        for rowset in &tablet.rowsets {
            let rowset_meta = RowsetMeta::deserialize(&rowset.meta_bytes)?;
            tablet_meta_manager.save_rowset_meta(tablet_id, &rowset_meta)?;
            for delete_vector in &rowset.delete_vectors {
                let payload = DeleteVector::from_bytes(&delete_vector.payload)?;
                tablet_meta_manager.save_del_vector(
                    tablet_id,
                    delete_vector.segment_id,
                    delete_vector.version,
                    &payload,
                )?;
            }
        }
        Ok(())
    }

    fn apply_derived_progress(
        tablet_meta_manager: Option<&Arc<TabletMetaManager>>,
        storage_root: Option<PathBuf>,
        derived_progress: &DerivedProgressBundle,
    ) -> anyhow::Result<()> {
        if let Some(tablet_meta_manager) = tablet_meta_manager {
            for entry in &derived_progress.primary_indexes {
                tablet_meta_manager.save_persistent_index(entry.tablet_id, &entry.payload)?;
            }
        }

        if let Some(root) = storage_root {
            for entry in &derived_progress.graph_manifests {
                let target_path = root.join(&entry.locator);
                if let Some(parent) = target_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&target_path, &entry.payload)?;
            }
        }

        Ok(())
    }

    fn validate_route_registry_bundle(
        route_registry: &RouteRegistryBundle,
        catalog: &ParoCatalog,
    ) -> anyhow::Result<()> {
        let txn = CatalogSnapshot::read_only(u64::MAX);

        for entry in &route_registry.entries {
            let schema = catalog
                .get_schema(&txn, &entry.schema_name)
                .map_err(|err| {
                    anyhow::anyhow!(
                        "checkpoint route registry references missing schema {}: {}",
                        entry.schema_name,
                        err
                    )
                })?;
            let table_entry = schema
                .collection(CatalogType::Table)
                .expect("table collection")
                .get_entry(txn.transaction_id, txn.start_time, &entry.table_name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "checkpoint route registry references missing table {}.{}",
                        entry.schema_name,
                        entry.table_name
                    )
                })?;
            let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
                anyhow::bail!(
                    "checkpoint route registry entry {}.{} does not resolve to a table",
                    entry.schema_name,
                    entry.table_name
                );
            };
            let descriptor = table
                .get_storage_descriptor()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "checkpoint route registry table {}.{} has no storage descriptor",
                        entry.schema_name,
                        entry.table_name
                    )
                })?
                .serialize()
                .map_err(anyhow::Error::from)?;
            if descriptor != entry.storage_descriptor {
                anyhow::bail!(
                    "checkpoint route registry descriptor mismatch for {}.{}",
                    entry.schema_name,
                    entry.table_name
                );
            }
        }

        Ok(())
    }

    fn catalog_contains_object(
        catalog: &ParoCatalog,
        txn: &CatalogSnapshot,
        object_id: u64,
    ) -> bool {
        for schema_entry in catalog
            .get_schema_collection()
            .scan(txn.transaction_id, txn.start_time)
        {
            let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
                continue;
            };
            for entry_type in [
                CatalogType::Table,
                CatalogType::Index,
                CatalogType::View,
                CatalogType::PropertyGraph,
                CatalogType::Sequence,
            ] {
                let Some(collection) = schema.collection(entry_type) else {
                    continue;
                };
                for entry in collection.scan(txn.transaction_id, txn.start_time) {
                    if entry.object_id().raw() == object_id {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn find_index_type(
        catalog: &ParoCatalog,
        txn: &CatalogSnapshot,
        object_id: u64,
    ) -> Option<IndexType> {
        for schema_entry in catalog
            .get_schema_collection()
            .scan(txn.transaction_id, txn.start_time)
        {
            let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
                continue;
            };
            let Some(collection) = schema.collection(CatalogType::Index) else {
                continue;
            };
            for entry in collection.scan(txn.transaction_id, txn.start_time) {
                let Some(index) = entry.as_ref().as_index() else {
                    continue;
                };
                if index.object_id().raw() == object_id {
                    return Some(index.index_type);
                }
            }
        }
        None
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

fn format_db_identifier(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, Default)]
struct InMemoryMetadataStore {
    entries: RwLock<BTreeMap<String, Vec<u8>>>,
}

impl MetadataStore for InMemoryMetadataStore {
    fn put(&self, key: &str, value: &[u8]) -> paro_common::error::Result<()> {
        self.entries.write().insert(key.to_string(), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &str) -> paro_common::error::Result<Option<Vec<u8>>> {
        Ok(self.entries.read().get(key).cloned())
    }

    fn delete(&self, key: &str) -> paro_common::error::Result<()> {
        self.entries.write().remove(key);
        Ok(())
    }

    fn write_batch(&self, ops: &[MetadataOp]) -> paro_common::error::Result<()> {
        let mut entries = self.entries.write();
        for op in ops {
            match op {
                MetadataOp::Put { key, value } => {
                    entries.insert(key.clone(), value.clone());
                }
                MetadataOp::Delete { key } => {
                    entries.remove(key);
                }
            }
        }
        Ok(())
    }

    fn scan_prefix(&self, prefix: &str) -> paro_common::error::Result<Vec<(String, Vec<u8>)>> {
        Ok(self
            .entries
            .read()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn exists(&self, key: &str) -> paro_common::error::Result<bool> {
        Ok(self.entries.read().contains_key(key))
    }
}

#[cfg(test)]
mod tests {
    use super::CheckpointRecovery;
    use paro_catalog::catalog::Catalog;
    use paro_catalog::collection::InstallMode;
    use paro_catalog::entry::{
        CatalogEntry, CatalogEntryEnum, ColumnDefinition, CreateIndexInfo, IndexBuildState,
        IndexCatalogEntry, IndexType, LogicalIndex,
    };
    use paro_catalog::mvcc::CatalogSnapshot;
    use paro_common::checkpoint::{
        DeferredTaskKey, DeferredTaskKind, DeferredTaskProgress, DeferredTaskScope,
    };
    use paro_common::types::LogicalType;
    use paro_storage::table::table_factory::TableFactory;
    use std::sync::Arc;

    #[test]
    fn redeliver_deferred_tasks_executes_supported_runtime_repairs() {
        let catalog = Arc::new(paro_catalog::database_catalog::ParoCatalog::new(
            "test_db".to_string(),
        ));
        catalog.initialize(false);

        let art_storage = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Integer])
                .expect("create table"),
        );
        let write_txn = CatalogSnapshot::permanent_writer(u64::MAX);
        catalog
            .create_table_in_snapshot(
                &write_txn,
                "public",
                "users",
                vec![ColumnDefinition::new(
                    "id".to_string(),
                    LogicalType::Integer,
                )],
                Arc::clone(&art_storage),
            )
            .expect("create table in catalog");
        art_storage
            .append(&paro_common::test_utils::test_chunk_from_vectors(vec![
                paro_common::test_utils::test_i32_vector(&[1, 2, 3]),
            ]))
            .expect("append users");

        let fulltext_storage = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Varchar])
                .expect("create fulltext table"),
        );
        catalog
            .create_table_in_snapshot(
                &write_txn,
                "public",
                "docs",
                vec![ColumnDefinition::new(
                    "content".to_string(),
                    LogicalType::Varchar,
                )],
                Arc::clone(&fulltext_storage),
            )
            .expect("create docs table in catalog");
        fulltext_storage
            .append(&paro_common::test_utils::test_chunk_from_vectors(vec![
                paro_common::test_utils::test_string_vector(&["vector db"]),
            ]))
            .expect("append docs");
        let read_txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog
            .get_schema(&read_txn, "public")
            .expect("public schema");

        let users_table = schema
            .get_table(read_txn.transaction_id, read_txn.start_time, "users")
            .expect("users table should exist");
        let CatalogEntryEnum::Table(users_table) = users_table.as_ref() else {
            panic!("expected table entry");
        };
        let users_object_id = users_table.object_id().raw();

        let docs_table = schema
            .get_table(read_txn.transaction_id, read_txn.start_time, "docs")
            .expect("docs table should exist");
        let CatalogEntryEnum::Table(docs_table) = docs_table.as_ref() else {
            panic!("expected table entry");
        };

        let art_entry = Arc::new(CatalogEntryEnum::Index(Arc::new(IndexCatalogEntry::new(
            CreateIndexInfo::new(
                "public".to_string(),
                "users".to_string(),
                "idx_users_art".to_string(),
                vec![LogicalIndex::new(0)],
                vec![LogicalType::Integer],
            )
            .with_index_type(IndexType::ART)
            .with_build_state(IndexBuildState::Building),
            users_table.object_id().raw(),
            0,
            catalog.name().to_string(),
            catalog.object_id_allocator().allocate(),
        ))));
        let art_object_id = art_entry.object_id().raw();
        schema
            .collection(paro_catalog::entry::CatalogType::Index)
            .expect("index collection")
            .install_committed(Arc::clone(&art_entry), InstallMode::RejectExisting)
            .expect("install art index");

        let fulltext_entry = Arc::new(CatalogEntryEnum::Index(Arc::new(IndexCatalogEntry::new(
            CreateIndexInfo::new(
                "public".to_string(),
                "docs".to_string(),
                "idx_docs_fts".to_string(),
                vec![LogicalIndex::new(0)],
                vec![LogicalType::Varchar],
            )
            .with_index_type(IndexType::FullText)
            .with_fulltext_options(LogicalIndex::new(0), "simple")
            .with_build_state(IndexBuildState::Building),
            docs_table.object_id().raw(),
            0,
            catalog.name().to_string(),
            catalog.object_id_allocator().allocate(),
        ))));
        let fulltext_object_id = fulltext_entry.object_id().raw();
        schema
            .collection(paro_catalog::entry::CatalogType::Index)
            .expect("index collection")
            .install_committed(Arc::clone(&fulltext_entry), InstallMode::RejectExisting)
            .expect("install fulltext index");

        let redelivered = CheckpointRecovery::redeliver_deferred_tasks(
            &catalog,
            &[
                DeferredTaskProgress {
                    task_key: DeferredTaskKey {
                        task_kind: DeferredTaskKind::FinalizeIndexState,
                        scope: DeferredTaskScope::Object(art_object_id),
                    },
                    visible_lsn: 10,
                    completed_lsn: None,
                    failed_lsn: None,
                    last_error: None,
                },
                DeferredTaskProgress {
                    task_key: DeferredTaskKey {
                        task_kind: DeferredTaskKind::FinalizeIndexState,
                        scope: DeferredTaskScope::Object(fulltext_object_id),
                    },
                    visible_lsn: 10,
                    completed_lsn: None,
                    failed_lsn: None,
                    last_error: None,
                },
                DeferredTaskProgress {
                    task_key: DeferredTaskKey {
                        task_kind: DeferredTaskKind::FinalizeIndexState,
                        scope: DeferredTaskScope::Object(users_object_id),
                    },
                    visible_lsn: 11,
                    completed_lsn: None,
                    failed_lsn: None,
                    last_error: None,
                },
                DeferredTaskProgress {
                    task_key: DeferredTaskKey {
                        task_kind: DeferredTaskKind::FinalizeIndexState,
                        scope: DeferredTaskScope::Object(art_object_id),
                    },
                    visible_lsn: 13,
                    completed_lsn: Some(13),
                    failed_lsn: None,
                    last_error: None,
                },
            ],
        );

        assert_eq!(redelivered, 2);

        let art_index = schema
            .get_index(
                read_txn.transaction_id,
                read_txn.start_time,
                "idx_users_art",
            )
            .expect("art index should exist");
        let CatalogEntryEnum::Index(art_index) = art_index.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(art_index.build_state(), IndexBuildState::Ready);
        assert!(art_index.coverage().expect("art coverage").is_complete());
        assert_eq!(art_storage.tablet().declared_art_columns(), vec![0]);

        let fulltext_index = schema
            .get_index(read_txn.transaction_id, read_txn.start_time, "idx_docs_fts")
            .expect("fulltext index should exist");
        let CatalogEntryEnum::Index(fulltext_index) = fulltext_index.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(fulltext_index.build_state(), IndexBuildState::Ready);
        let coverage = fulltext_index.coverage().expect("fulltext coverage");
        assert_eq!(coverage.visible_segment_count, 1);
        assert_eq!(coverage.indexed_segment_count, 0);
        assert!(!coverage.is_complete());
        assert!(fulltext_storage.fulltext_capability(0, "simple").is_some());
    }
}
