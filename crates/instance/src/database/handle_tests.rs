// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::checkpoint::manifest_store::ManifestStore;
use crate::checkpoint::writers::CatalogWriter;
use crate::database::identity::DatabaseType;
use crate::database::storage::DatabaseStorage;
use crate::InMemoryDatabaseStorage;
use paro_catalog::entry::{
    CatalogObjectIdAllocator, CatalogObjectRef, CatalogType, ColumnDefinition, CreateSequenceInfo,
    CreateViewInfo, DependencyList, OnCreateConflict, TableCatalogEntry,
};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::types::LogicalType;
use paro_parser::ast::Statement;
use paro_parser::parse_one;
use paro_storage::buffer::{BufferManager, StandardBufferManager, DEFAULT_BLOCK_ALLOC_SIZE};
use paro_storage::meta::{FileMetadataStore, MetadataStore, TabletMetaManager};
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::TableHandle;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

fn create_table(types: &[LogicalType]) -> TableHandle {
    TableFactory::default().create_table(types).unwrap()
}

fn object_ids() -> Arc<CatalogObjectIdAllocator> {
    Arc::new(CatalogObjectIdAllocator::default())
}

fn create_test_meta_manager(path: &str) -> Arc<TabletMetaManager> {
    let store: Arc<dyn MetadataStore> =
        Arc::new(FileMetadataStore::new(Path::new(path).join("tablet_meta")).unwrap());
    Arc::new(TabletMetaManager::with_store_and_data_root(
        store,
        Path::new(path),
    ))
}

fn create_table_with_meta_manager(
    types: &[LogicalType],
    meta_manager: Arc<TabletMetaManager>,
) -> TableHandle {
    TableFactory::new(Some(meta_manager))
        .create_table(types)
        .unwrap()
}

fn parse_query(sql: &str) -> Box<paro_parser::ast::Query> {
    match parse_one(sql).unwrap().stmt {
        Statement::Query(query) => query,
        other => panic!("expected query statement, got {:?}", other),
    }
}

fn create_checkpointable_db(path: &str) -> (DatabaseHandle, Arc<TabletMetaManager>) {
    let _ = std::fs::remove_dir_all(path);
    let buffer_pool = Arc::new(BufferPool::new(1024));
    let db = DatabaseHandle::new(1, "test_db".into(), path.into(), buffer_pool, object_ids());
    db.initialize().unwrap();

    let buffer_manager: Arc<dyn BufferManager> = Arc::new(StandardBufferManager::new(
        8 * 1024 * 1024,
        DEFAULT_BLOCK_ALLOC_SIZE,
        8,
    ));
    let mut storage = DatabaseStorage::new(path.to_string(), buffer_manager);
    storage.create_new().unwrap();
    storage.bootstrap_storage_identity(db.id()).unwrap();
    storage.initialize_wal().unwrap();
    db.attach_storage(Box::new(storage));
    db.finalize_load().unwrap();
    let tablet_meta_manager = create_test_meta_manager(path);

    let txn = CatalogSnapshot::permanent_writer(u64::MAX);
    let schema = db.catalog().get_schema(&txn, "public").unwrap();
    let storage = Arc::new(create_table_with_meta_manager(
        &[LogicalType::Integer],
        tablet_meta_manager.clone(),
    ));
    let table_entry = Arc::new(TableCatalogEntry::new(
        "test_db".to_string(),
        "public".to_string(),
        "checkpoint_table".to_string(),
        vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )],
        storage,
        db.catalog().object_id_allocator().allocate(),
        0,
    ));
    schema
        .create_table(&txn, table_entry, OnCreateConflict::ErrorOnConflict)
        .unwrap();

    (db, tablet_meta_manager)
}

#[test]
fn test_db_state_transitions() {
    let buffer_pool = Arc::new(BufferPool::new(1024));
    let db = DatabaseHandle::new(
        1,
        "test".into(),
        "/tmp/test".into(),
        buffer_pool,
        object_ids(),
    );

    assert!(!db.is_ready());
    assert_eq!(db.state(), DbState::Opening);

    db.state_handle().set_ready();
    assert!(db.is_ready());
    assert_eq!(db.state(), DbState::Ready);

    assert!(db.try_mark_dropping());
    assert!(!db.is_ready());
    assert_eq!(db.state(), DbState::Dropping);

    assert!(!db.try_mark_dropping()); // already dropping
}

#[test]
fn test_attached_database_type() {
    let buffer_pool = Arc::new(BufferPool::new(1024));

    // Test system database
    let system_db = DatabaseHandle::new_system(0, buffer_pool.clone(), object_ids());
    assert!(system_db.is_system());
    assert!(!system_db.is_temporary());
    assert!(!system_db.is_read_only());
    assert_eq!(system_db.db_type(), DatabaseType::System);

    // Test temp database
    let temp_db = DatabaseHandle::new_temp(1, buffer_pool.clone(), object_ids());
    assert!(!temp_db.is_system());
    assert!(temp_db.is_temporary());
    assert!(!temp_db.is_read_only());
    assert_eq!(temp_db.db_type(), DatabaseType::Temp);

    // Test read-write database
    let rw_db = DatabaseHandle::new(
        2,
        "test".into(),
        "/tmp/test".into(),
        buffer_pool.clone(),
        object_ids(),
    );
    assert!(!rw_db.is_system());
    assert!(!rw_db.is_temporary());
    assert!(!rw_db.is_read_only());
    assert_eq!(rw_db.db_type(), DatabaseType::ReadWrite);

    // Test read-only database via options
    let ro_options = AttachOptions::read_only();
    let ro_db = DatabaseHandle::with_options(
        3,
        "readonly".into(),
        "/tmp/readonly".into(),
        buffer_pool.clone(),
        object_ids(),
        ro_options,
    );
    assert!(ro_db.is_read_only());
    assert_eq!(ro_db.db_type(), DatabaseType::ReadOnly);
}

#[test]
fn test_attach_options() {
    // Test default options
    let default_opts = AttachOptions::default();
    assert_eq!(default_opts.access_mode, AccessMode::Automatic);
    assert_eq!(default_opts.recovery_mode, RecoveryMode::Default);
    assert_eq!(default_opts.db_type, "paro");
    assert!(!default_opts.is_main_database);
    assert_eq!(default_opts.visibility, AttachVisibility::Shown);

    // Test builder pattern
    let custom_opts = AttachOptions::new()
        .with_access_mode(AccessMode::ReadOnly)
        .with_recovery_mode(RecoveryMode::NoWalWrites)
        .with_db_type("custom")
        .as_main_database()
        .with_visibility(AttachVisibility::Hidden)
        .with_option("key", "value");

    assert_eq!(custom_opts.access_mode, AccessMode::ReadOnly);
    assert_eq!(custom_opts.recovery_mode, RecoveryMode::NoWalWrites);
    assert_eq!(custom_opts.db_type, "custom");
    assert!(custom_opts.is_main_database);
    assert_eq!(custom_opts.visibility, AttachVisibility::Hidden);
    assert_eq!(custom_opts.options.get("key"), Some(&"value".to_string()));
}

#[test]
fn test_attach_options_from_map() {
    let mut options = HashMap::new();
    options.insert("readonly".to_string(), "true".to_string());
    options.insert("recovery_mode".to_string(), "no_wal_writes".to_string());
    options.insert("type".to_string(), "sqlite".to_string());
    options.insert("custom_key".to_string(), "custom_value".to_string());

    let parsed = AttachOptions::from_options(options, AccessMode::Automatic);

    assert_eq!(parsed.access_mode, AccessMode::ReadOnly);
    assert_eq!(parsed.recovery_mode, RecoveryMode::NoWalWrites);
    assert_eq!(parsed.db_type, "sqlite");
    assert_eq!(
        parsed.options.get("custom_key"),
        Some(&"custom_value".to_string())
    );
}

#[test]
fn test_should_checkpoint_no_wal() {
    let buffer_pool = Arc::new(BufferPool::new(1024));
    let db = DatabaseHandle::new(
        1,
        "test".into(),
        "/tmp/test".into(),
        buffer_pool,
        object_ids(),
    );

    // No WAL, should not checkpoint
    assert!(!db.should_checkpoint(0));
    assert!(!db.should_checkpoint(1024 * 1024 * 100));
}

#[test]
fn test_checkpoint_in_progress_flag() {
    let buffer_pool = Arc::new(BufferPool::new(1024));
    let db = DatabaseHandle::new(
        1,
        "test".into(),
        "/tmp/test".into(),
        buffer_pool,
        object_ids(),
    );

    assert!(!db.is_checkpoint_in_progress());

    let guard = db
        .checkpoint_coordinator()
        .try_acquire_in_progress()
        .expect("first acquire should succeed");
    assert!(db.is_checkpoint_in_progress());

    assert!(db
        .checkpoint_coordinator()
        .try_acquire_in_progress()
        .is_none());

    drop(guard);
    assert!(!db.is_checkpoint_in_progress());
    assert!(db
        .checkpoint_coordinator()
        .try_acquire_in_progress()
        .is_some());
}

#[test]
fn test_wal_keep_from_threshold() {
    let buffer_pool = Arc::new(BufferPool::new(1024));
    let db = DatabaseHandle::new(
        1,
        "test".into(),
        "/tmp/test".into(),
        buffer_pool,
        object_ids(),
    );

    assert_eq!(db.wal_keep_from(), u64::MAX);

    db.set_wal_keep_from(0);
    assert_eq!(db.wal_keep_from(), 0);

    db.set_wal_keep_from(128);
    assert_eq!(db.wal_keep_from(), 128);
}

#[test]
fn test_check_wal_health_without_wal() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test_db");
    let buffer_pool = Arc::new(BufferPool::new(1024));
    let db = DatabaseHandle::new(
        1,
        "test".into(),
        db_path.to_string_lossy().to_string(),
        buffer_pool,
        object_ids(),
    );

    let report = db.check_wal_health().unwrap();
    assert!(report.healthy);
    assert_eq!(
        report.recovery_mode,
        paro_journal::wal::recovery::WalRecoveryMode::NoWal
    );
}

#[test]
fn test_wal_lifecycle_metrics_tracks_wal_health_check() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test_db");
    let buffer_pool = Arc::new(BufferPool::new(1024));
    let db = DatabaseHandle::new(
        1,
        "test".into(),
        db_path.to_string_lossy().to_string(),
        buffer_pool,
        object_ids(),
    );

    db.check_wal_health().unwrap();

    let metrics = db.wal_lifecycle_metrics();
    assert_eq!(metrics.wal_health_check_total, 1);
    assert_eq!(
        metrics.recovery_mode,
        paro_journal::wal::recovery::WalRecoveryMode::NoWal
    );
    assert!(!metrics.main_wal_needs_truncation);
}

#[test]
fn test_recovery_consistency_report_cached_for_management_query() {
    let (db, _tablet_meta_manager) = create_checkpointable_db("/tmp/recovery_consistency_cached");

    let report = db.recovery_consistency_report();
    let cached = db.last_recovery_consistency_report();

    assert!(cached.is_some());
    assert_eq!(cached.unwrap(), report);
}

#[test]
fn test_name_is_reserved() {
    assert!(DatabaseHandle::name_is_reserved("system"));
    assert!(DatabaseHandle::name_is_reserved("SYSTEM"));
    assert!(DatabaseHandle::name_is_reserved("temp"));
    assert!(DatabaseHandle::name_is_reserved("TEMP"));
    assert!(DatabaseHandle::name_is_reserved("main"));
    assert!(DatabaseHandle::name_is_reserved("MAIN"));
    assert!(!DatabaseHandle::name_is_reserved("mydb"));
    assert!(!DatabaseHandle::name_is_reserved("test"));
}

#[test]
fn test_extract_database_name() {
    // Empty or memory path
    assert_eq!(DatabaseHandle::extract_database_name(""), "memory");
    assert_eq!(DatabaseHandle::extract_database_name(":memory:"), "memory");

    // Normal paths
    assert_eq!(
        DatabaseHandle::extract_database_name("/path/to/mydb.db"),
        "mydb"
    );
    assert_eq!(
        DatabaseHandle::extract_database_name("/path/to/test.db"),
        "test"
    );

    // Path with query parameters
    assert_eq!(
        DatabaseHandle::extract_database_name("/path/to/mydb.db?readonly=true"),
        "mydb"
    );

    // Reserved names get suffix
    assert_eq!(
        DatabaseHandle::extract_database_name("/path/to/system.db"),
        "system_db"
    );
    assert_eq!(
        DatabaseHandle::extract_database_name("/path/to/temp.db"),
        "temp_db"
    );
}

#[test]
fn test_initial_database_flag() {
    let buffer_pool = Arc::new(BufferPool::new(1024));
    let db = DatabaseHandle::new(
        1,
        "test".into(),
        "/tmp/test".into(),
        buffer_pool,
        object_ids(),
    );

    assert!(!db.is_initial_database());
    db.set_initial_database();
    assert!(db.is_initial_database());
}

#[test]
fn test_recovery_mode() {
    let buffer_pool = Arc::new(BufferPool::new(1024));

    // Default recovery mode
    let db1 = DatabaseHandle::new(
        1,
        "test".into(),
        "/tmp/test".into(),
        buffer_pool.clone(),
        object_ids(),
    );
    assert_eq!(db1.recovery_mode(), RecoveryMode::Default);

    // NoWalWrites recovery mode
    let options = AttachOptions::new().with_recovery_mode(RecoveryMode::NoWalWrites);
    let db2 = DatabaseHandle::with_options(
        2,
        "test2".into(),
        "/tmp/test2".into(),
        buffer_pool.clone(),
        object_ids(),
        options,
    );
    assert_eq!(db2.recovery_mode(), RecoveryMode::NoWalWrites);
}

#[test]
fn test_visibility() {
    let buffer_pool = Arc::new(BufferPool::new(1024));

    // Default visibility
    let db1 = DatabaseHandle::new(
        1,
        "test".into(),
        "/tmp/test".into(),
        buffer_pool.clone(),
        object_ids(),
    );
    assert_eq!(db1.visibility(), AttachVisibility::Shown);

    // Hidden visibility
    let options = AttachOptions::new().with_visibility(AttachVisibility::Hidden);
    let db2 = DatabaseHandle::with_options(
        2,
        "test2".into(),
        "/tmp/test2".into(),
        buffer_pool.clone(),
        object_ids(),
        options,
    );
    assert_eq!(db2.visibility(), AttachVisibility::Hidden);
}

#[test]
fn test_has_storage_manager() {
    let buffer_pool = Arc::new(BufferPool::new(1024));
    let db = DatabaseHandle::new(
        1,
        "test".into(),
        "/tmp/test".into(),
        buffer_pool,
        object_ids(),
    );

    assert!(!db.has_storage_manager());
    assert!(db.storage_manager().is_none());
}

#[test]
fn test_compaction_tablet_sync_create_drop() {
    let dir = tempfile::tempdir().unwrap();
    let buffer_pool = Arc::new(BufferPool::new(1024));
    let db = DatabaseHandle::new(
        1,
        "test".into(),
        dir.path().to_string_lossy().into_owned(),
        buffer_pool,
        object_ids(),
    );
    db.initialize().unwrap();
    let mut storage = InMemoryDatabaseStorage::new();
    storage.initialize().unwrap();
    db.attach_storage(Box::new(storage));
    db.finalize_load().unwrap();

    let txn = CatalogSnapshot::permanent_writer(u64::MAX);
    let schema = db.catalog().get_schema(&txn, "public").unwrap();
    let storage = Arc::new(create_table(&[LogicalType::Integer]));
    let table_entry = Arc::new(TableCatalogEntry::new(
        "test".to_string(),
        "public".to_string(),
        "sync_items".to_string(),
        vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )],
        storage,
        db.catalog().object_id_allocator().allocate(),
        0,
    ));
    schema
        .create_table(&txn, table_entry, OnCreateConflict::ErrorOnConflict)
        .unwrap();

    db.sync_compaction_tablets().unwrap();
    let obs = db.compaction_observability().unwrap();
    assert_eq!(obs.registered_tablets, 1);

    let drop_txn = CatalogSnapshot::permanent_writer(u64::MAX);
    db.catalog()
        .drop_table_with_snapshot(&drop_txn, "public", "sync_items", false)
        .unwrap();
    db.sync_compaction_tablets().unwrap();
    let obs = db.compaction_observability().unwrap();
    assert_eq!(obs.registered_tablets, 0);
}

#[test]
fn test_on_detach_shutdowns_compaction_manager() {
    let buffer_pool = Arc::new(BufferPool::new(1024));
    let db = DatabaseHandle::new(
        1,
        "test".into(),
        "/tmp/test_detach_compaction".into(),
        buffer_pool,
        object_ids(),
    );
    db.initialize().unwrap();
    let mut storage = InMemoryDatabaseStorage::new();
    storage.initialize().unwrap();
    db.attach_storage(Box::new(storage));
    db.finalize_load().unwrap();

    assert!(db.has_compaction_manager());
    db.on_detach().unwrap();
    assert!(!db.has_compaction_manager());
}

#[test]
fn test_close_idempotent() {
    let buffer_pool = Arc::new(BufferPool::new(1024));
    let db = DatabaseHandle::new(
        1,
        "test".into(),
        "/tmp/test".into(),
        buffer_pool,
        object_ids(),
    );

    // First close should succeed
    assert!(db.close(DatabaseCloseAction::TryCheckpoint).is_ok());

    // Second close should also succeed (idempotent)
    assert!(db.close(DatabaseCloseAction::TryCheckpoint).is_ok());
}

#[test]
fn test_checkpoint_serializes_catalog_metadata() {
    let (db, _tablet_meta_manager) = create_checkpointable_db("/tmp/checkpoint_serialization");

    db.checkpoint().unwrap();

    let storage = db.storage_lock().read();
    let sm = storage
        .as_ref()
        .expect("storage manager should be attached");
    let manifest_store = ManifestStore::open_for_storage(sm.as_ref())
        .unwrap()
        .expect("manifest store should exist");
    let manifest = manifest_store
        .read_current_manifest()
        .unwrap()
        .expect("current manifest should exist");
    let catalog_bundle = manifest
        .bundle_refs
        .iter()
        .find(|bundle| matches!(bundle.kind, paro_common::checkpoint::BundleKind::Catalog))
        .expect("catalog bundle should exist");
    let metadata = manifest_store.read_bundle_payload(catalog_bundle).unwrap();
    assert!(!metadata.is_empty());
    assert!(
        metadata
            .windows(b"checkpoint_table".len())
            .any(|window| window == b"checkpoint_table"),
        "serialized catalog metadata should contain table name"
    );
}

#[test]
fn test_checkpoint_roundtrip_preserves_object_ids() {
    let (db, tablet_meta_manager) = create_checkpointable_db("/tmp/checkpoint_oid_roundtrip");
    let txn = CatalogSnapshot::permanent_writer(u64::MAX);
    let schema = db.catalog().get_schema(&txn, "public").unwrap();

    let view_info = CreateViewInfo::new(
        "public".to_string(),
        "checkpoint_view".to_string(),
        parse_query("SELECT 1 AS id"),
    )
    .with_dependencies({
        let mut dependencies = DependencyList::new();
        let table = schema
            .get_table(txn.transaction_id, txn.start_time, "checkpoint_table")
            .unwrap();
        dependencies.add_regular(CatalogObjectRef::in_schema(
            table.object_id(),
            CatalogType::Table,
            db.catalog().name().to_string(),
            Some(schema.base.object_id),
            "public".to_string(),
            "checkpoint_table".to_string(),
        ));
        dependencies
    })
    .with_sql("CREATE VIEW public.checkpoint_view AS SELECT 1 AS id".to_string());
    schema
        .create_view(&txn, view_info, OnCreateConflict::ErrorOnConflict)
        .unwrap();

    let sequence_info = CreateSequenceInfo::new("public".to_string(), "checkpoint_seq".to_string())
        .with_start_value(10)
        .with_increment(2);
    schema
        .create_sequence(&txn, sequence_info, OnCreateConflict::ErrorOnConflict)
        .unwrap();

    let original_schema_oid = schema.base.object_id;
    let original_table_oid = schema
        .get_table(txn.transaction_id, txn.start_time, "checkpoint_table")
        .unwrap()
        .object_id();
    let original_view_oid = schema
        .get_view(txn.transaction_id, txn.start_time, "checkpoint_view")
        .unwrap()
        .object_id();
    let original_sequence_oid = schema
        .get_sequence(txn.transaction_id, txn.start_time, "checkpoint_seq")
        .unwrap()
        .object_id();
    let original_allocator_watermark = db.catalog().current_object_id();

    let checkpoint_bytes = CatalogWriter::serialize(db.catalog().as_ref()).unwrap();

    let restored = ParoCatalog::new("test_db".to_string());
    CatalogWriter::deserialize(&checkpoint_bytes, &restored, Some(tablet_meta_manager)).unwrap();

    let restored_txn = CatalogSnapshot::read_only(u64::MAX);
    let restored_schema = restored.get_schema(&restored_txn, "public").unwrap();
    assert_eq!(restored_schema.base.object_id, original_schema_oid);
    assert_eq!(
        restored_schema
            .get_table(
                restored_txn.transaction_id,
                restored_txn.start_time,
                "checkpoint_table"
            )
            .unwrap()
            .object_id(),
        original_table_oid
    );
    assert_eq!(
        restored_schema
            .get_view(
                restored_txn.transaction_id,
                restored_txn.start_time,
                "checkpoint_view"
            )
            .unwrap()
            .object_id(),
        original_view_oid
    );
    let restored_view = restored_schema
        .get_view(
            restored_txn.transaction_id,
            restored_txn.start_time,
            "checkpoint_view",
        )
        .unwrap();
    assert_eq!(restored_view.dependency_list().len(), 1);
    let dependency_error = restored
        .dependency_graph()
        .plan_drop(original_table_oid, false)
        .unwrap_err();
    assert!(dependency_error.to_string().contains("checkpoint_view"));
    assert_eq!(
        restored_schema
            .get_sequence(
                restored_txn.transaction_id,
                restored_txn.start_time,
                "checkpoint_seq"
            )
            .unwrap()
            .object_id(),
        original_sequence_oid
    );

    let restored_allocator_watermark = restored.current_object_id();
    assert!(restored_allocator_watermark >= original_allocator_watermark);

    let write_txn = CatalogSnapshot::permanent_writer(u64::MAX);
    let new_view_info = CreateViewInfo::new(
        "public".to_string(),
        "after_checkpoint_view".to_string(),
        parse_query("SELECT 2 AS id"),
    )
    .with_sql("CREATE VIEW public.after_checkpoint_view AS SELECT 2 AS id".to_string());
    restored_schema
        .create_view(&write_txn, new_view_info, OnCreateConflict::ErrorOnConflict)
        .unwrap();
    let allocated_view = restored_schema
        .get_view(
            restored_txn.transaction_id,
            restored_txn.start_time,
            "after_checkpoint_view",
        )
        .unwrap()
        .object_id();
    // The recovered catalog owns this allocator exclusively, so the first new identity starts
    // exactly at the persisted watermark.
    assert_eq!(allocated_view.raw(), restored_allocator_watermark);
}

#[test]
fn test_create_assigns_stable_unique_object_ids() {
    let (db, _tablet_meta_manager) = create_checkpointable_db("/tmp/create_object_id_identity");
    let txn = CatalogSnapshot::permanent_writer(u64::MAX);
    let schema = db.catalog().get_schema(&txn, "public").unwrap();

    let table_object_id = schema
        .get_table(txn.transaction_id, txn.start_time, "checkpoint_table")
        .unwrap()
        .object_id();
    let view_info = CreateViewInfo::new(
        "public".to_string(),
        "identity_view".to_string(),
        parse_query("SELECT id FROM public.checkpoint_table"),
    )
    .with_sql(
        "CREATE VIEW public.identity_view AS SELECT id FROM public.checkpoint_table".to_string(),
    );
    schema
        .create_view(&txn, view_info, OnCreateConflict::ErrorOnConflict)
        .unwrap();
    let sequence_info = CreateSequenceInfo::new("public".to_string(), "identity_seq".to_string());
    schema
        .create_sequence(&txn, sequence_info, OnCreateConflict::ErrorOnConflict)
        .unwrap();

    let schema_object_id = schema.base.object_id.raw();
    let view_object_id = schema
        .get_view(txn.transaction_id, txn.start_time, "identity_view")
        .unwrap()
        .object_id();
    let sequence_object_id = schema
        .get_sequence(txn.transaction_id, txn.start_time, "identity_seq")
        .unwrap()
        .object_id();

    assert_eq!(
        schema
            .get_view(txn.transaction_id, txn.start_time, "identity_view")
            .unwrap()
            .object_id(),
        view_object_id
    );

    let unique_ids = HashSet::from([
        schema_object_id,
        table_object_id.raw(),
        view_object_id.raw(),
        sequence_object_id.raw(),
    ]);
    assert_eq!(unique_ids.len(), 4);
}

#[test]
fn test_close_uses_wal_coordinated_checkpoint_path() {
    let (db, _tablet_meta_manager) = create_checkpointable_db("/tmp/close_checkpoint");

    db.close(DatabaseCloseAction::Checkpoint).unwrap();

    assert!(db.is_closed());
    let storage = db.storage_lock().read();
    let sm = storage
        .as_ref()
        .expect("storage manager should be attached");
    let manifest_store = ManifestStore::open_for_storage(sm.as_ref())
        .unwrap()
        .expect("manifest store should exist");
    let manifest = manifest_store
        .read_current_manifest()
        .unwrap()
        .expect("current manifest should exist");
    assert!(manifest.checkpoint_id > 0);
    assert!(manifest
        .bundle_refs
        .iter()
        .any(|bundle| matches!(bundle.kind, paro_common::checkpoint::BundleKind::Catalog)));
}

#[test]
fn test_checkpoint_updates_wal_lifecycle_metrics() {
    let (db, _tablet_meta_manager) = create_checkpointable_db("/tmp/checkpoint_metrics");

    db.checkpoint().unwrap();

    let metrics = db.wal_lifecycle_metrics();
    assert_eq!(metrics.checkpoint_success_total, 1);
    assert_eq!(metrics.checkpoint_failure_total, 0);
}

#[test]
fn test_finalize_load_marks_database_ready_without_recovery() {
    let buffer_pool = Arc::new(BufferPool::new(1024));
    let db = DatabaseHandle::new(
        7,
        "ready_db".into(),
        "/tmp/ready_db".into(),
        buffer_pool,
        object_ids(),
    );

    assert_eq!(db.state(), DbState::Opening);
    db.initialize().unwrap();

    let mut storage = InMemoryDatabaseStorage::new();
    storage.initialize().unwrap();
    db.attach_storage(Box::new(storage));

    db.finalize_load().unwrap();

    assert!(db.is_ready());
    assert_eq!(db.state(), DbState::Ready);
    assert!(db.has_storage_manager());
}

#[test]
fn continuous_search_maintenance_requests_cannot_extend_max_delay() {
    let now = Instant::now();
    let pending = SearchMaintenancePending {
        requested_epoch: 10_000,
        completed_epoch: 0,
        first_request: Some(now - SEARCH_MAINTENANCE_MAX_DELAY),
        // Model a writer that reset the trailing edge immediately before this
        // decision. The anchored first request still makes the pass runnable.
        last_request: Some(now),
        urgency: SearchMaintenanceUrgency::Deadline,
    };
    assert_eq!(pending.wait_before_run(now), None);
}

#[test]
fn elevated_search_maintenance_bypasses_debounce() {
    let now = Instant::now();
    let pending = SearchMaintenancePending {
        requested_epoch: 1,
        completed_epoch: 0,
        first_request: Some(now),
        last_request: Some(now),
        urgency: SearchMaintenanceUrgency::Immediate,
    };
    assert_eq!(pending.wait_before_run(now), None);
}

#[test]
fn opportunistic_search_maintenance_waits_for_quiescence_without_deadline_fragmentation() {
    let now = Instant::now();
    let pending = SearchMaintenancePending {
        requested_epoch: 10_000,
        completed_epoch: 0,
        first_request: Some(now - SEARCH_MAINTENANCE_MAX_DELAY * 4),
        last_request: Some(now),
        urgency: SearchMaintenanceUrgency::Quiescent,
    };
    assert_eq!(
        pending.wait_before_run(now),
        Some(SEARCH_MAINTENANCE_QUIESCENCE)
    );
}
