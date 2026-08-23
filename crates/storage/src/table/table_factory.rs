// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::storage_descriptor::TableStorageDescriptor;
use super::table_handle::{TableColumnSpec, TableHandle};
use crate::meta::TabletMetaManager;
use crate::tablet::{KeysType, Tablet, TabletColumn, TabletIdentity, TabletSchema};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_transaction::{DatabaseId, LockNamespace, ShardedLockManager};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub trait TabletIdAllocator: Send + Sync {
    fn next_tablet_id(&self) -> u64;
}

#[derive(Debug)]
struct LocalTabletIdAllocator;

impl TabletIdAllocator for LocalTabletIdAllocator {
    fn next_tablet_id(&self) -> u64 {
        NEXT_TABLET_ID.fetch_add(1, AtomicOrdering::Relaxed) + 1
    }
}

#[derive(Clone)]
pub struct TableFactory {
    meta_manager: Option<Arc<TabletMetaManager>>,
    storage_root: PathBuf,
    id_allocator: Arc<dyn TabletIdAllocator>,
    lock_manager: Arc<ShardedLockManager>,
    lock_namespace: LockNamespace,
}

impl Default for TableFactory {
    fn default() -> Self {
        Self::new(None)
    }
}

impl TableFactory {
    pub fn new(meta_manager: Option<Arc<TabletMetaManager>>) -> Self {
        let storage_root = default_storage_root(meta_manager.as_deref());
        Self {
            meta_manager,
            storage_root,
            id_allocator: Arc::new(LocalTabletIdAllocator),
            lock_manager: Arc::new(ShardedLockManager::default()),
            lock_namespace: LockNamespace::single_tenant(DatabaseId::new(0)),
        }
    }

    pub fn with_storage_root(mut self, storage_root: impl Into<PathBuf>) -> Self {
        self.storage_root = storage_root.into();
        self
    }

    pub fn with_id_allocator(mut self, id_allocator: Arc<dyn TabletIdAllocator>) -> Self {
        self.id_allocator = id_allocator;
        self
    }

    pub fn with_transaction_locks(
        mut self,
        lock_manager: Arc<ShardedLockManager>,
        lock_namespace: LockNamespace,
    ) -> Self {
        self.lock_manager = lock_manager;
        self.lock_namespace = lock_namespace;
        self
    }

    pub fn create_table(&self, types: &[LogicalType]) -> Result<TableHandle> {
        self.create_table_with_keys(types, KeysType::DuplicateKeys)
    }

    pub fn create_table_with_keys(
        &self,
        types: &[LogicalType],
        keys_type: KeysType,
    ) -> Result<TableHandle> {
        let identity = self.allocate_local_identity();
        self.create_from_types(types, keys_type, identity)
    }

    pub fn create_table_from_specs(&self, specs: &[TableColumnSpec]) -> Result<TableHandle> {
        let identity = self.allocate_local_identity();
        self.create_from_specs(specs, identity)
    }

    pub fn create_from_specs(
        &self,
        specs: &[TableColumnSpec],
        identity: TabletIdentity,
    ) -> Result<TableHandle> {
        let schema = build_schema_from_specs(specs, identity.schema_id, identity.schema_version)?;
        let tablet = self.bootstrap_tablet(identity, schema)?;
        Ok(TableHandle::from_runtime_tablet(
            tablet,
            specs.iter().map(|spec| spec.logical_type.clone()).collect(),
        ))
    }

    pub fn create_from_types(
        &self,
        types: &[LogicalType],
        keys_type: KeysType,
        identity: TabletIdentity,
    ) -> Result<TableHandle> {
        let schema = build_schema_from_types(
            types,
            keys_type,
            identity.schema_id,
            identity.schema_version,
        )?;
        let tablet = self.bootstrap_tablet(identity, schema)?;
        Ok(TableHandle::from_runtime_tablet(tablet, types.to_vec()))
    }

    pub fn open_from_descriptor(
        &self,
        types: &[LogicalType],
        descriptor: &TableStorageDescriptor,
    ) -> Result<TableHandle> {
        descriptor.validate()?;
        let data_dir = PathBuf::from(&descriptor.data_dir);
        let meta_manager = self.meta_manager.clone().ok_or_else(|| {
            paro_error::invalid_input(
                "TableFactory::open_from_descriptor requires an explicit TabletMetaManager",
            )
        })?;
        let tablet = Tablet::open_with_lock_manager(
            descriptor.tablet_id,
            &data_dir,
            meta_manager,
            Arc::clone(&self.lock_manager),
            self.lock_namespace,
        )?;

        validate_descriptor_match(&tablet, descriptor)?;

        let runtime_schema = tablet
            .schema()
            .ok_or_else(|| paro_error::internal("Tablet schema missing"))?;
        let runtime_types = runtime_schema.logical_types();
        if !types.is_empty() && runtime_types != types {
            return Err(paro_error::invalid_input(
                "column types mismatch between catalog and descriptor runtime schema",
            ));
        }

        Ok(TableHandle::from_runtime_tablet(tablet, runtime_types))
    }

    fn allocate_local_identity(&self) -> TabletIdentity {
        let tablet_id = self.id_allocator.next_tablet_id();
        TabletIdentity {
            table_id: tablet_id,
            partition_id: 0,
            tablet_id,
            schema_id: tablet_id,
            schema_version: 1,
        }
    }

    fn bootstrap_tablet(
        &self,
        identity: TabletIdentity,
        schema: Arc<TabletSchema>,
    ) -> Result<Tablet> {
        let data_dir = stable_data_dir(
            &self.storage_root,
            identity.table_id,
            identity.partition_id,
            identity.tablet_id,
        );

        let tablet = Tablet::new_with_lock_manager(
            identity.tablet_id,
            identity.table_id,
            identity.partition_id,
            schema,
            &data_dir,
            self.meta_manager.clone(),
            Arc::clone(&self.lock_manager),
            self.lock_namespace,
        )?;
        tablet.init()?;
        tablet.save_meta()?;
        Ok(tablet)
    }
}

fn validate_descriptor_match(tablet: &Tablet, descriptor: &TableStorageDescriptor) -> Result<()> {
    if tablet.tablet_id() != descriptor.tablet_id {
        return Err(paro_error::invalid_input(format!(
            "tablet_id mismatch: descriptor={} runtime={}",
            descriptor.tablet_id,
            tablet.tablet_id()
        )));
    }
    if tablet.table_id() != descriptor.table_id {
        return Err(paro_error::invalid_input(format!(
            "table_id mismatch: descriptor={} runtime={}",
            descriptor.table_id,
            tablet.table_id()
        )));
    }
    if tablet.partition_id() != descriptor.partition_id {
        return Err(paro_error::invalid_input(format!(
            "partition_id mismatch: descriptor={} runtime={}",
            descriptor.partition_id,
            tablet.partition_id()
        )));
    }
    if tablet.schema_hash() != descriptor.schema_hash {
        return Err(paro_error::invalid_input(format!(
            "schema_hash mismatch: descriptor={} runtime={}",
            descriptor.schema_hash,
            tablet.schema_hash()
        )));
    }

    let runtime_schema = tablet
        .schema()
        .ok_or_else(|| paro_error::internal("Tablet schema missing"))?;
    if runtime_schema.schema_id() != descriptor.schema_id {
        return Err(paro_error::invalid_input(format!(
            "schema_id mismatch: descriptor={} runtime={}",
            descriptor.schema_id,
            runtime_schema.schema_id()
        )));
    }
    if runtime_schema.schema_version() != descriptor.schema_version {
        return Err(paro_error::invalid_input(format!(
            "schema_version mismatch: descriptor={} runtime={}",
            descriptor.schema_version,
            runtime_schema.schema_version()
        )));
    }

    let runtime_data_dir = tablet.data_dir().to_string_lossy();
    if runtime_data_dir.as_ref() != descriptor.data_dir {
        return Err(paro_error::invalid_input(format!(
            "data_dir mismatch: descriptor={} runtime={}",
            descriptor.data_dir, runtime_data_dir
        )));
    }

    let descriptor_keys_type = descriptor.keys_type_enum()?;
    if runtime_schema.keys_type() != descriptor_keys_type {
        return Err(paro_error::invalid_input(format!(
            "keys_type mismatch: descriptor={descriptor_keys_type:?} runtime={:?}",
            runtime_schema.keys_type()
        )));
    }

    Ok(())
}

fn build_schema_from_types(
    types: &[LogicalType],
    keys_type: KeysType,
    schema_id: u64,
    schema_version: u32,
) -> Result<Arc<TabletSchema>> {
    let mut columns: Vec<TabletColumn> = Vec::with_capacity(types.len());
    for (i, logical_type) in types.iter().enumerate() {
        columns.push(TabletColumn::new(
            i as u32,
            format!("col_{i}"),
            logical_type.clone(),
        ));
    }
    if keys_type == KeysType::PrimaryKeys && !columns.is_empty() {
        columns[0].is_key = true;
        columns[0].is_nullable = false;
    }
    Ok(Arc::new(TabletSchema::with_version(
        schema_id,
        schema_version,
        columns,
        keys_type,
    )?))
}

fn build_schema_from_specs(
    specs: &[TableColumnSpec],
    schema_id: u64,
    schema_version: u32,
) -> Result<Arc<TabletSchema>> {
    let keys_type = if specs.iter().any(|spec| spec.is_key) {
        KeysType::PrimaryKeys
    } else {
        KeysType::DuplicateKeys
    };

    let mut columns: Vec<TabletColumn> = Vec::with_capacity(specs.len());
    for (i, spec) in specs.iter().enumerate() {
        let mut column = TabletColumn::new(i as u32, spec.name.clone(), spec.logical_type.clone());
        column.is_key = spec.is_key;
        column.is_nullable = !spec.not_null && !spec.is_key;
        columns.push(column);
    }

    Ok(Arc::new(TabletSchema::with_version(
        schema_id,
        schema_version,
        columns,
        keys_type,
    )?))
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

static NEXT_TABLET_ID: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(now_micros()));

fn workspace_root_from_manifest() -> Option<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let crates_dir = manifest_dir.parent()?;
    if crates_dir.file_name()?.to_str()? != "crates" {
        return None;
    }
    Some(crates_dir.parent()?.to_path_buf())
}

fn explicit_table_data_root_from_env() -> Option<PathBuf> {
    std::env::var_os("PARO_TABLE_DATA_DIR")
        .or_else(|| std::env::var_os("PARO_TEST_DATA_DIR"))
        .map(PathBuf::from)
}

fn default_storage_root(tablet_meta_manager: Option<&TabletMetaManager>) -> PathBuf {
    if let Some(custom) = explicit_table_data_root_from_env() {
        return custom;
    }
    if let Some(root) = tablet_meta_manager
        .and_then(|manager| manager.data_root_dir())
        .map(PathBuf::from)
    {
        return root;
    }
    if let Some(workspace_root) = workspace_root_from_manifest() {
        return workspace_root.join("target").join("paro_data");
    }
    if let Ok(cwd) = std::env::current_dir() {
        return cwd.join(".paro_data");
    }
    PathBuf::from(".paro_data")
}

pub(crate) fn stable_data_dir(
    storage_root: &std::path::Path,
    table_id: u64,
    partition_id: u64,
    tablet_id: u64,
) -> PathBuf {
    storage_root
        .join("catalog_main")
        .join("schema_public")
        .join(format!("table_{table_id}"))
        .join(format!("partition_{partition_id}"))
        .join(format!("tablet_{tablet_id}"))
}
