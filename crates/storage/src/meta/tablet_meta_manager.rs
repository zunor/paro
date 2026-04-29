// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{GlobalSchemaMap, MetadataOp, MetadataStore, StorageManifest};
use crate::primary_key::DeleteVector;
use crate::rowset::{RowsetId, RowsetMeta};
use crate::tablet::{TabletId, TabletMeta, TabletState};
use paro_common::error::{self as paro_error, Result};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Centralized metadata entry point for tablet/rowset/delete-vector persistence.
#[derive(Clone)]
pub struct TabletMetaManager {
    store: Arc<dyn MetadataStore>,
    schema_map: Arc<GlobalSchemaMap>,
    data_root_dir: Option<PathBuf>,
}

impl std::fmt::Debug for TabletMetaManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TabletMetaManager")
            .field("store", &"dyn MetadataStore")
            .field("schema_map_size", &self.schema_map.len())
            .finish()
    }
}

impl TabletMetaManager {
    pub fn new(store: Arc<dyn MetadataStore>, schema_map: Arc<GlobalSchemaMap>) -> Self {
        Self {
            store,
            schema_map,
            data_root_dir: None,
        }
    }

    pub fn with_store(store: Arc<dyn MetadataStore>) -> Self {
        Self {
            store,
            schema_map: Arc::new(GlobalSchemaMap::new()),
            data_root_dir: None,
        }
    }

    pub fn with_store_and_data_root(
        store: Arc<dyn MetadataStore>,
        data_root_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            store,
            schema_map: Arc::new(GlobalSchemaMap::new()),
            data_root_dir: Some(data_root_dir.into()),
        }
    }

    pub fn store(&self) -> &Arc<dyn MetadataStore> {
        &self.store
    }

    pub fn schema_map(&self) -> &Arc<GlobalSchemaMap> {
        &self.schema_map
    }

    pub fn data_root_dir(&self) -> Option<&Path> {
        self.data_root_dir.as_deref()
    }

    // ==================== Key Encoding ====================

    pub fn encode_tablet_meta_key(tablet_id: TabletId) -> String {
        format!("tablet/{tablet_id}/meta")
    }

    pub fn decode_tablet_meta_key(key: &str) -> Option<TabletId> {
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() != 3 || parts[0] != "tablet" || parts[2] != "meta" {
            return None;
        }
        parts[1].parse().ok()
    }

    pub fn encode_rowset_meta_key(tablet_id: TabletId, rowset_id: RowsetId) -> String {
        format!("tablet/{tablet_id}/rowset/{rowset_id}")
    }

    pub fn decode_rowset_meta_key(key: &str) -> Option<(TabletId, RowsetId)> {
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() != 4 || parts[0] != "tablet" || parts[2] != "rowset" {
            return None;
        }
        Some((parts[1].parse().ok()?, parts[3].parse().ok()?))
    }

    pub fn encode_del_vector_key(tablet_id: TabletId, segment_id: u32, version: i64) -> String {
        format!("tablet/{tablet_id}/delvec/{segment_id}/{version}")
    }

    pub fn decode_del_vector_key(key: &str) -> Option<(TabletId, u32, i64)> {
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() != 5 || parts[0] != "tablet" || parts[2] != "delvec" {
            return None;
        }
        Some((
            parts[1].parse().ok()?,
            parts[3].parse().ok()?,
            parts[4].parse().ok()?,
        ))
    }

    pub fn encode_persistent_index_key(tablet_id: TabletId) -> String {
        format!("tablet/{tablet_id}/primary_index")
    }

    pub fn decode_persistent_index_key(key: &str) -> Option<TabletId> {
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() != 3 || parts[0] != "tablet" || parts[2] != "primary_index" {
            return None;
        }
        parts[1].parse().ok()
    }

    pub fn manifest_key() -> &'static str {
        "manifest/tablets"
    }

    // ==================== Tablet CRUD ====================

    pub fn save_tablet_meta(&self, meta: &TabletMeta) -> Result<()> {
        let schema = meta
            .schema()
            .cloned()
            .ok_or_else(|| paro_error::invalid_input("TabletMeta has no schema"))?;

        let schema_id = schema.schema_id();
        let schema_version = schema.schema_version();
        self.schema_map
            .get_or_insert(schema_id, schema_version, Arc::clone(&schema))?;

        let mut manifest = self.load_manifest_for_update()?;
        manifest.add_tablet_meta(meta);

        let ops = vec![
            MetadataOp::Put {
                key: GlobalSchemaMap::schema_store_key(schema_id, schema_version),
                value: schema.serialize()?,
            },
            MetadataOp::Put {
                key: Self::encode_tablet_meta_key(meta.tablet_id()),
                value: meta.serialize()?,
            },
            MetadataOp::Put {
                key: Self::manifest_key().to_string(),
                value: manifest.to_json_bytes()?,
            },
        ];
        self.store.write_batch(&ops)
    }

    pub fn load_tablet_meta(&self, tablet_id: TabletId) -> Result<Option<TabletMeta>> {
        let key = Self::encode_tablet_meta_key(tablet_id);
        let Some(bytes) = self.store.get(&key)? else {
            return Ok(None);
        };
        let meta = TabletMeta::deserialize(&bytes)?;
        if let Some(schema) = meta.schema().cloned() {
            self.schema_map
                .get_or_insert(schema.schema_id(), schema.schema_version(), schema)?;
        }
        Ok(Some(meta))
    }

    pub fn update_tablet_state(&self, tablet_id: TabletId, state: TabletState) -> Result<()> {
        let Some(mut tablet_meta) = self.load_tablet_meta(tablet_id)? else {
            return Err(paro_error::invalid_input(format!(
                "Tablet {} not found for state update",
                tablet_id
            )));
        };

        tablet_meta.set_tablet_state(state);

        let mut manifest = self.load_manifest_for_update()?;
        if !manifest.update_tablet_state(tablet_id, state) {
            manifest.add_tablet_meta(&tablet_meta);
        }

        let ops = vec![
            MetadataOp::Put {
                key: Self::encode_tablet_meta_key(tablet_id),
                value: tablet_meta.serialize()?,
            },
            MetadataOp::Put {
                key: Self::manifest_key().to_string(),
                value: manifest.to_json_bytes()?,
            },
        ];
        self.store.write_batch(&ops)
    }

    pub fn remove_tablet_meta(&self, tablet_id: TabletId) -> Result<()> {
        let schema_to_remove = self
            .load_tablet_meta(tablet_id)?
            .and_then(|meta| meta.schema().cloned())
            .map(|schema| (schema.schema_id(), schema.schema_version()));
        let mut manifest = self.load_manifest_for_update()?;
        manifest.remove_tablet(tablet_id);

        let rowset_prefix = format!("tablet/{tablet_id}/rowset/");
        let delvec_prefix = format!("tablet/{tablet_id}/delvec/");

        let rowset_keys: Vec<String> = self
            .store
            .scan_prefix(&rowset_prefix)?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        let delvec_keys: Vec<String> = self
            .store
            .scan_prefix(&delvec_prefix)?
            .into_iter()
            .map(|(k, _)| k)
            .collect();

        let mut ops = Vec::with_capacity(4 + rowset_keys.len() + delvec_keys.len());
        ops.push(MetadataOp::Delete {
            key: Self::encode_tablet_meta_key(tablet_id),
        });
        ops.push(MetadataOp::Delete {
            key: Self::encode_persistent_index_key(tablet_id),
        });
        for key in rowset_keys {
            ops.push(MetadataOp::Delete { key });
        }
        for key in delvec_keys {
            ops.push(MetadataOp::Delete { key });
        }
        ops.push(MetadataOp::Put {
            key: Self::manifest_key().to_string(),
            value: manifest.to_json_bytes()?,
        });
        self.store.write_batch(&ops)?;

        if let Some((schema_id, schema_version)) = schema_to_remove {
            self.schema_map.remove(schema_id, schema_version);
        }
        Ok(())
    }

    // ==================== Rowset CRUD ====================

    pub fn save_rowset_meta(&self, tablet_id: TabletId, meta: &RowsetMeta) -> Result<()> {
        if meta.tablet_id() != tablet_id {
            return Err(paro_error::invalid_input(format!(
                "RowsetMeta tablet_id mismatch: expected {}, got {}",
                tablet_id,
                meta.tablet_id()
            )));
        }
        let key = Self::encode_rowset_meta_key(tablet_id, meta.rowset_id());
        self.store.put(&key, &meta.serialize()?)
    }

    pub fn load_rowset_metas(&self, tablet_id: TabletId) -> Result<Vec<RowsetMeta>> {
        let prefix = format!("tablet/{tablet_id}/rowset/");
        let mut items: Vec<(RowsetId, RowsetMeta)> = Vec::new();

        for (key, value) in self.store.scan_prefix(&prefix)? {
            let Some((decoded_tablet_id, rowset_id)) = Self::decode_rowset_meta_key(&key) else {
                continue;
            };
            if decoded_tablet_id != tablet_id {
                continue;
            }
            items.push((rowset_id, RowsetMeta::deserialize(&value)?));
        }

        items.sort_by_key(|(rowset_id, _)| *rowset_id);
        Ok(items.into_iter().map(|(_, meta)| meta).collect())
    }

    pub fn remove_rowset_meta(&self, tablet_id: TabletId, rowset_id: RowsetId) -> Result<()> {
        let mut ops = vec![MetadataOp::Delete {
            key: Self::encode_rowset_meta_key(tablet_id, rowset_id),
        }];

        if let Some(mut tablet_meta) = self.load_tablet_meta(tablet_id)? {
            if tablet_meta.delete_rowset_meta(rowset_id).is_some() {
                ops.push(MetadataOp::Put {
                    key: Self::encode_tablet_meta_key(tablet_id),
                    value: tablet_meta.serialize()?,
                });
            }
        }
        self.store.write_batch(&ops)
    }

    // ==================== DeleteVector CRUD ====================

    pub fn save_del_vector(
        &self,
        tablet_id: TabletId,
        segment_id: u32,
        version: i64,
        dv: &DeleteVector,
    ) -> Result<()> {
        let key = Self::encode_del_vector_key(tablet_id, segment_id, version);
        let bytes = dv.to_bytes()?;
        self.store.put(&key, &bytes)
    }

    pub fn load_del_vector(
        &self,
        tablet_id: TabletId,
        segment_id: u32,
        version: i64,
    ) -> Result<Option<DeleteVector>> {
        let key = Self::encode_del_vector_key(tablet_id, segment_id, version);
        let Some(bytes) = self.store.get(&key)? else {
            return Ok(None);
        };
        Ok(Some(DeleteVector::from_bytes(&bytes)?))
    }

    pub fn clear_del_vectors(&self, tablet_id: TabletId) -> Result<()> {
        let prefix = format!("tablet/{tablet_id}/delvec/");
        let keys: Vec<String> = self
            .store
            .scan_prefix(&prefix)?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        if keys.is_empty() {
            return Ok(());
        }
        let ops: Vec<MetadataOp> = keys
            .into_iter()
            .map(|key| MetadataOp::Delete { key })
            .collect();
        self.store.write_batch(&ops)
    }

    // ==================== Persistent Index CRUD ====================

    /// Saves persistent primary-index payload bytes.
    pub fn save_persistent_index(&self, tablet_id: TabletId, payload: &[u8]) -> Result<()> {
        let key = Self::encode_persistent_index_key(tablet_id);
        self.store.put(&key, payload)
    }

    /// Loads persistent primary-index payload bytes.
    pub fn load_persistent_index(&self, tablet_id: TabletId) -> Result<Option<Vec<u8>>> {
        let key = Self::encode_persistent_index_key(tablet_id);
        self.store.get(&key)
    }

    // ==================== Rowset Commit ====================

    /// Atomically commits a rowset meta and updates tablet meta.
    pub fn commit_rowset(&self, tablet_id: TabletId, meta: &RowsetMeta) -> Result<()> {
        if meta.tablet_id() != tablet_id {
            return Err(paro_error::invalid_input(format!(
                "RowsetMeta tablet_id mismatch: expected {}, got {}",
                tablet_id,
                meta.tablet_id()
            )));
        }

        let Some(mut tablet_meta) = self.load_tablet_meta(tablet_id)? else {
            return Err(paro_error::invalid_input(format!(
                "Tablet {} not found for rowset commit",
                tablet_id
            )));
        };

        let mut rowset_meta = meta.clone();
        if rowset_meta.schema_id() == 0 {
            if let Some(schema) = tablet_meta.schema().cloned() {
                rowset_meta.set_schema_id(schema.schema_id());
            }
        }

        tablet_meta.delete_rowset_meta(rowset_meta.rowset_id());
        tablet_meta.add_rowset_meta(rowset_meta.clone());

        let ops = vec![
            MetadataOp::Put {
                key: Self::encode_rowset_meta_key(tablet_id, rowset_meta.rowset_id()),
                value: rowset_meta.serialize()?,
            },
            MetadataOp::Put {
                key: Self::encode_tablet_meta_key(tablet_id),
                value: tablet_meta.serialize()?,
            },
        ];
        self.store.write_batch(&ops)
    }

    // ==================== Scan / Walk ====================

    pub fn scan_all_tablets(&self) -> Result<Vec<TabletMeta>> {
        let mut metas = Vec::new();

        for (key, value) in self.store.scan_prefix("tablet/")? {
            let Some(tablet_id) = Self::decode_tablet_meta_key(&key) else {
                continue;
            };
            let meta = TabletMeta::deserialize(&value)?;
            if meta.tablet_id() != tablet_id {
                return Err(paro_error::internal(format!(
                    "Tablet meta key/data mismatch: key={}, payload={}",
                    tablet_id,
                    meta.tablet_id()
                )));
            }

            if let Some(schema) = meta.schema().cloned() {
                self.schema_map.get_or_insert(
                    schema.schema_id(),
                    schema.schema_version(),
                    schema,
                )?;
            }

            metas.push(meta);
        }

        metas.sort_by_key(|meta| meta.tablet_id());
        Ok(metas)
    }

    /// Load startup tablets from manifest first, and fallback to full scan when manifest is
    /// missing/corrupted.
    pub fn load_startup_tablets(&self, parallel_tablet_load: usize) -> Result<Vec<TabletMeta>> {
        if parallel_tablet_load == 0 {
            return Err(paro_error::invalid_input(
                "parallel_tablet_load must be greater than 0",
            ));
        }

        match self.load_storage_manifest() {
            Ok(Some(manifest)) => {
                match self.load_tablets_from_manifest(&manifest, parallel_tablet_load) {
                    Ok(metas) => Ok(metas),
                    Err(_manifest_load_err) => self.scan_tablets_and_rebuild_manifest(),
                }
            }
            Ok(None) | Err(_) => self.scan_tablets_and_rebuild_manifest(),
        }
    }

    /// Reads StorageManifest from metadata store.
    pub fn load_storage_manifest(&self) -> Result<Option<StorageManifest>> {
        let Some(bytes) = self.store.get(Self::manifest_key())? else {
            return Ok(None);
        };
        Ok(Some(StorageManifest::from_json_bytes(&bytes)?))
    }

    pub fn rebuild_storage_manifest(&self) -> Result<StorageManifest> {
        let metas = self.scan_all_tablets()?;
        let manifest = StorageManifest::from_tablet_metas(&metas);
        self.save_storage_manifest(&manifest)?;
        Ok(manifest)
    }

    fn save_storage_manifest(&self, manifest: &StorageManifest) -> Result<()> {
        let bytes = manifest.to_json_bytes()?;
        self.store.put(Self::manifest_key(), &bytes)
    }

    fn scan_tablets_and_rebuild_manifest(&self) -> Result<Vec<TabletMeta>> {
        let metas = self.scan_all_tablets()?;
        let startup_metas = Self::startup_metas(metas);
        let manifest = StorageManifest::from_tablet_metas(&startup_metas);
        self.save_storage_manifest(&manifest)?;
        Ok(startup_metas)
    }

    fn load_manifest_for_update(&self) -> Result<StorageManifest> {
        match self.load_storage_manifest() {
            Ok(Some(manifest)) => Ok(manifest),
            Ok(None) => self.rebuild_storage_manifest(),
            Err(_err) => self.rebuild_storage_manifest(),
        }
    }

    fn load_tablets_from_manifest(
        &self,
        manifest: &StorageManifest,
        parallel_tablet_load: usize,
    ) -> Result<Vec<TabletMeta>> {
        if manifest.tablets.is_empty() {
            return Ok(Vec::new());
        }

        let mut tablet_ids: Vec<TabletId> = manifest
            .tablets
            .iter()
            .map(|entry| entry.tablet_id)
            .collect();
        tablet_ids.sort_unstable();
        tablet_ids.dedup();

        let metas = if tablet_ids.len() <= 1 || parallel_tablet_load <= 1 {
            let mut loaded = Vec::with_capacity(tablet_ids.len());
            for tablet_id in tablet_ids {
                if let Some(meta) = self.load_tablet_meta(tablet_id)? {
                    loaded.push(meta);
                }
            }
            loaded
        } else {
            self.load_tablets_in_parallel(tablet_ids, parallel_tablet_load)?
        };

        let startup_metas = Self::startup_metas(metas);

        let mut should_rebuild_manifest = startup_metas.len() != manifest.tablets.len();
        if !should_rebuild_manifest {
            for meta in &startup_metas {
                match manifest.get_tablet(meta.tablet_id()) {
                    Some(entry) if entry.matches_meta(meta) => {}
                    _ => {
                        should_rebuild_manifest = true;
                        break;
                    }
                }
            }
        }

        if should_rebuild_manifest {
            let rebuilt = StorageManifest::from_tablet_metas(&startup_metas);
            self.save_storage_manifest(&rebuilt)?;
        }

        Ok(startup_metas)
    }

    fn startup_metas(mut metas: Vec<TabletMeta>) -> Vec<TabletMeta> {
        metas.retain(|meta| meta.tablet_state() != TabletState::Shutdown);
        metas.sort_by_key(|meta| meta.tablet_id());
        metas
    }

    fn load_tablets_in_parallel(
        &self,
        tablet_ids: Vec<TabletId>,
        parallel_tablet_load: usize,
    ) -> Result<Vec<TabletMeta>> {
        let worker_count = parallel_tablet_load.min(tablet_ids.len()).max(1);
        let queue = Mutex::new(VecDeque::from(tablet_ids));
        let loaded = Mutex::new(Vec::<TabletMeta>::new());
        let errors = Mutex::new(Vec::<String>::new());

        std::thread::scope(|scope| {
            for _ in 0..worker_count {
                scope.spawn(|| loop {
                    let tablet_id = {
                        let mut guard = queue.lock().expect("queue lock poisoned");
                        guard.pop_front()
                    };

                    let Some(tablet_id) = tablet_id else {
                        break;
                    };

                    match self.load_tablet_meta(tablet_id) {
                        Ok(Some(meta)) => {
                            loaded.lock().expect("loaded lock poisoned").push(meta);
                        }
                        Ok(None) => {}
                        Err(err) => {
                            errors.lock().expect("errors lock poisoned").push(format!(
                                "Failed to load tablet {} from manifest: {}",
                                tablet_id, err
                            ));
                            break;
                        }
                    }
                });
            }
        });

        let mut error_messages = errors
            .lock()
            .map_err(|_| paro_error::internal("errors lock poisoned"))?;
        if let Some(message) = error_messages.pop() {
            return Err(paro_error::internal(message));
        }
        drop(error_messages);

        let mut metas = loaded
            .lock()
            .map_err(|_| paro_error::internal("loaded lock poisoned"))?
            .clone();
        metas.sort_by_key(|meta| meta.tablet_id());
        Ok(metas)
    }

    pub fn walk<F>(&self, callback: F) -> Result<usize>
    where
        F: FnMut(TabletId, &TabletMeta) -> Result<bool>,
    {
        self.walk_internal(None, callback)
    }

    pub fn walk_with_timeout<F>(&self, timeout: Duration, callback: F) -> Result<usize>
    where
        F: FnMut(TabletId, &TabletMeta) -> Result<bool>,
    {
        self.walk_internal(Some(timeout), callback)
    }

    fn walk_internal<F>(&self, timeout: Option<Duration>, mut callback: F) -> Result<usize>
    where
        F: FnMut(TabletId, &TabletMeta) -> Result<bool>,
    {
        let started_at = Instant::now();
        let mut visited = 0usize;
        let metas = self.scan_all_tablets()?;

        for meta in metas {
            Self::check_walk_timeout(timeout, started_at, visited)?;
            let cont = callback(meta.tablet_id(), &meta)?;
            visited += 1;
            if !cont {
                break;
            }
            Self::check_walk_timeout(timeout, started_at, visited)?;
        }

        Ok(visited)
    }

    fn check_walk_timeout(
        timeout: Option<Duration>,
        started_at: Instant,
        visited: usize,
    ) -> Result<()> {
        if let Some(timeout) = timeout {
            if started_at.elapsed() > timeout {
                return Err(paro_error::internal(format!(
                    "TabletMetaManager walk timeout after {:?}, visited {} tablet(s)",
                    timeout, visited
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::FileMetadataStore;
    use crate::rowset::RowsetMetaBuilder;
    use crate::tablet::{KeysType, TabletColumn, TabletSchema, TabletState, Version};
    use paro_common::types::LogicalType;
    use std::thread;
    use tempfile::tempdir;

    fn build_schema(schema_id: u64, schema_version: u32) -> Arc<TabletSchema> {
        let columns = vec![
            TabletColumn::key(0, "id", LogicalType::BigInt),
            TabletColumn::new(1, "value", LogicalType::Varchar),
        ];
        Arc::new(
            TabletSchema::with_version(schema_id, schema_version, columns, KeysType::PrimaryKeys)
                .unwrap(),
        )
    }

    fn build_tablet_meta(tablet_id: TabletId, schema_id: u64) -> TabletMeta {
        TabletMeta::new(
            tablet_id,
            1000 + tablet_id,
            2000 + tablet_id,
            build_schema(schema_id, 1),
            format!("/tmp/tablet-{tablet_id}"),
        )
        .unwrap()
    }

    fn build_rowset_meta(tablet_id: TabletId, rowset_id: RowsetId, version: i64) -> RowsetMeta {
        RowsetMetaBuilder::with_id(rowset_id, tablet_id, Version::singleton(version))
            .path(format!("/tmp/tablet-{tablet_id}/rowset-{rowset_id}"))
            .build()
    }

    fn create_manager() -> (tempfile::TempDir, TabletMetaManager) {
        let dir = tempdir().unwrap();
        let store: Arc<dyn MetadataStore> = Arc::new(FileMetadataStore::new(dir.path()).unwrap());
        let schema_map = Arc::new(GlobalSchemaMap::new());
        let manager = TabletMetaManager::new(store, schema_map);
        (dir, manager)
    }

    #[test]
    fn key_codec_roundtrip_test() {
        let tk = TabletMetaManager::encode_tablet_meta_key(7);
        assert_eq!(TabletMetaManager::decode_tablet_meta_key(&tk), Some(7));

        let rk = TabletMetaManager::encode_rowset_meta_key(7, 99);
        assert_eq!(
            TabletMetaManager::decode_rowset_meta_key(&rk),
            Some((7, 99))
        );

        let dk = TabletMetaManager::encode_del_vector_key(7, 3, 42);
        assert_eq!(
            TabletMetaManager::decode_del_vector_key(&dk),
            Some((7, 3, 42))
        );

        let pk = TabletMetaManager::encode_persistent_index_key(7);
        assert_eq!(TabletMetaManager::decode_persistent_index_key(&pk), Some(7));
    }

    #[test]
    fn tablet_meta_manager_roundtrip_test() {
        let (_dir, manager) = create_manager();

        let tablet_meta = build_tablet_meta(10, 9001);
        manager.save_tablet_meta(&tablet_meta).unwrap();

        let loaded = manager.load_tablet_meta(10).unwrap().unwrap();
        assert_eq!(loaded.tablet_id(), 10);
        assert_eq!(loaded.table_id(), 1010);
        assert!(manager.schema_map().contains(9001, 1));
        let manifest = manager.load_storage_manifest().unwrap().unwrap();
        assert!(manifest.get_tablet(10).is_some());

        let rowset_1 = build_rowset_meta(10, 1, 0);
        let rowset_2 = build_rowset_meta(10, 2, 1);
        manager.save_rowset_meta(10, &rowset_2).unwrap();
        manager.save_rowset_meta(10, &rowset_1).unwrap();

        let rowsets = manager.load_rowset_metas(10).unwrap();
        assert_eq!(rowsets.len(), 2);
        assert_eq!(rowsets[0].rowset_id(), 1);
        assert_eq!(rowsets[1].rowset_id(), 2);

        manager.remove_rowset_meta(10, 2).unwrap();
        let rowsets = manager.load_rowset_metas(10).unwrap();
        assert_eq!(rowsets.len(), 1);
        assert_eq!(rowsets[0].rowset_id(), 1);

        let mut dv = DeleteVector::new();
        dv.mark_deleted(3);
        dv.mark_deleted(9);
        manager.save_del_vector(10, 0, 7, &dv).unwrap();
        let loaded_dv = manager.load_del_vector(10, 0, 7).unwrap().unwrap();
        assert!(loaded_dv.is_deleted(3));
        assert!(loaded_dv.is_deleted(9));

        manager.clear_del_vectors(10).unwrap();
        assert!(manager.load_del_vector(10, 0, 7).unwrap().is_none());

        manager
            .save_persistent_index(10, b"persistent-index-bytes")
            .unwrap();
        assert_eq!(
            manager.load_persistent_index(10).unwrap().unwrap(),
            b"persistent-index-bytes".to_vec()
        );

        manager.remove_tablet_meta(10).unwrap();
        assert!(manager.load_tablet_meta(10).unwrap().is_none());
        assert!(manager.load_rowset_metas(10).unwrap().is_empty());
        assert!(manager.load_persistent_index(10).unwrap().is_none());
        let manifest = manager.load_storage_manifest().unwrap().unwrap();
        assert!(manifest.get_tablet(10).is_none());
    }

    #[test]
    fn tablet_meta_manager_commit_atomicity_test() {
        let (_dir, manager) = create_manager();
        manager
            .save_tablet_meta(&build_tablet_meta(20, 500))
            .unwrap();

        let rowset = build_rowset_meta(20, 77, 7);
        manager.commit_rowset(20, &rowset).unwrap();

        let tablet = manager.load_tablet_meta(20).unwrap().unwrap();
        assert!(tablet.find_rowset_meta(77).is_some());

        let rowsets = manager.load_rowset_metas(20).unwrap();
        assert_eq!(rowsets.len(), 1);
        assert_eq!(rowsets[0].rowset_id(), 77);
        assert_eq!(rowsets[0].schema_id(), 500);
    }

    #[test]
    fn tablet_meta_manager_scan_and_walk_test() {
        let (_dir, manager) = create_manager();
        manager.save_tablet_meta(&build_tablet_meta(2, 11)).unwrap();
        manager.save_tablet_meta(&build_tablet_meta(1, 12)).unwrap();

        let scanned = manager.scan_all_tablets().unwrap();
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].tablet_id(), 1);
        assert_eq!(scanned[1].tablet_id(), 2);

        let mut visited = Vec::new();
        let walked = manager
            .walk(|tablet_id, _| {
                visited.push(tablet_id);
                Ok(true)
            })
            .unwrap();
        assert_eq!(walked, 2);
        assert_eq!(visited, vec![1, 2]);

        let timeout_err = manager.walk_with_timeout(Duration::from_millis(1), |_tablet_id, _| {
            thread::sleep(Duration::from_millis(3));
            Ok(true)
        });
        assert!(timeout_err.is_err());
    }

    #[test]
    fn manifest_startup_test() {
        let (_dir, manager) = create_manager();

        manager
            .save_tablet_meta(&build_tablet_meta(1, 100))
            .unwrap();
        manager
            .save_tablet_meta(&build_tablet_meta(2, 200))
            .unwrap();
        manager
            .save_tablet_meta(&build_tablet_meta(3, 300))
            .unwrap();

        let startup = manager.load_startup_tablets(4).unwrap();
        let tablet_ids: Vec<TabletId> = startup.iter().map(|meta| meta.tablet_id()).collect();
        assert_eq!(tablet_ids, vec![1, 2, 3]);

        let manifest = manager.load_storage_manifest().unwrap().unwrap();
        assert_eq!(manifest.tablets.len(), 3);
    }

    #[test]
    fn load_startup_tablets_filters_shutdown_entries_and_repairs_manifest_test() {
        let (_dir, manager) = create_manager();

        manager
            .save_tablet_meta(&build_tablet_meta(10, 700))
            .unwrap();
        manager
            .save_tablet_meta(&build_tablet_meta(20, 800))
            .unwrap();
        manager
            .update_tablet_state(20, TabletState::Shutdown)
            .unwrap();

        manager
            .store()
            .delete(TabletMetaManager::manifest_key())
            .unwrap();

        let loaded = manager.load_startup_tablets(2).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].tablet_id(), 10);

        let repaired_manifest = manager.load_storage_manifest().unwrap().unwrap();
        assert_eq!(repaired_manifest.tablets.len(), 1);
        assert_eq!(repaired_manifest.tablets[0].tablet_id, 10);
    }

    #[test]
    fn manifest_corruption_recovery_test() {
        let (_dir, manager) = create_manager();
        manager
            .save_tablet_meta(&build_tablet_meta(10, 700))
            .unwrap();
        manager
            .save_tablet_meta(&build_tablet_meta(20, 800))
            .unwrap();

        manager
            .store()
            .put(TabletMetaManager::manifest_key(), b"{broken-json")
            .unwrap();

        let loaded = manager.load_startup_tablets(2).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].tablet_id(), 10);
        assert_eq!(loaded[1].tablet_id(), 20);

        let repaired_manifest = manager.load_storage_manifest().unwrap().unwrap();
        assert_eq!(repaired_manifest.tablets.len(), 2);
        assert_eq!(repaired_manifest.tablets[0].tablet_id, 10);
        assert_eq!(repaired_manifest.tablets[1].tablet_id, 20);
    }

    #[test]
    fn tablet_state_update_updates_manifest_test() {
        let (_dir, manager) = create_manager();
        manager
            .save_tablet_meta(&build_tablet_meta(30, 1000))
            .unwrap();

        manager
            .update_tablet_state(30, TabletState::Running)
            .unwrap();

        let loaded = manager.load_tablet_meta(30).unwrap().unwrap();
        assert_eq!(loaded.tablet_state(), TabletState::Running);

        let manifest = manager.load_storage_manifest().unwrap().unwrap();
        assert_eq!(manifest.get_tablet(30).unwrap().state, TabletState::Running);

        manager
            .update_tablet_state(30, TabletState::Shutdown)
            .unwrap();

        let manifest = manager.load_storage_manifest().unwrap().unwrap();
        assert!(manifest.get_tablet(30).is_none());
    }
}
