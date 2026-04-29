// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::catalog::Catalog;
use paro_catalog::entry::{CatalogEntryEnum, ColumnDefinition};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::checkpoint::BundleKind;
use paro_common::ddl::{DdlObjectKey, DdlObjectKind};
use paro_common::effect::{ApplyDescriptor, StagedArtifactDescriptor, StagingArtifactId};
use paro_common::journal::{
    CommitRecord, JournalRecord, JournalRecordMetadata, COMMIT_RECORD_VERSION,
};
use paro_common::types::LogicalType;
use paro_instance::checkpoint::manifest_store::testing::arm_manifest_rename_failure_for_path_on_nth_call;
use paro_instance::checkpoint::{manifest_store::ManifestStore, CheckpointRecovery};
use paro_instance::storage_manager::{wal_path_with_suffix, StorageManager};
use paro_instance::{
    recover_database_with_checkpoint, DatabaseCloseAction, DatabaseHandle, DatabaseRecordState,
    DatabaseStartupStatus, DatabaseStorage, DatabaseStorageIdentity, Instance,
    InstanceCatalogStore, InstanceConfig, InstanceLayout, InstanceLifecycleState, InstanceRunState,
    InstanceRunStateStore, InstanceShutdownDisposition, InstanceShutdownMode,
    InstanceStartupDisposition, RecoveryHook, RecoveryHookContext, RecoveryHookResult,
    StartupIssueKind, StartupPolicy, INSTANCE_RUN_STATE_FORMAT_VERSION,
};
use paro_journal::segments::SegmentCatalogStore;
use paro_journal::segments::DEFAULT_SEGMENT_ROTATION_BYTES;
use paro_journal::wal::test_support::{
    write_flushed_create_schema_txn, write_flushed_create_schema_txn_with_lsn,
};
use paro_journal::wal::wal_entry::{WalEntry, WalHeaderMetadata};
use paro_journal::wal::write_ahead_log::WriteAheadLog;
use paro_storage::buffer::StandardBufferManager;
use paro_storage::meta::metadata_store::testing::{
    arm_metadata_parent_sync_failure_for_path_on_nth_call,
    arm_metadata_rename_failure_for_path_on_nth_call,
};
use paro_storage::meta::{FileMetadataStore, MetadataStore};
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::TableHandle;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn create_table(types: &[LogicalType]) -> TableHandle {
    TableFactory::default().create_table(types).unwrap()
}

fn open_instance(base_dir: &Path) -> Arc<Instance> {
    let config = InstanceConfig::new().with_instance_root(base_dir.to_string_lossy().to_string());
    Instance::new(config).expect("instance should open")
}

fn open_instance_with_config(config: InstanceConfig) -> Arc<Instance> {
    Instance::new(config).expect("instance should open")
}

fn default_db(instance: &Arc<Instance>) -> Arc<DatabaseHandle> {
    instance
        .database_registry()
        .get_database("postgres")
        .expect("default database should exist")
}

fn load_instance_catalog(base_dir: &Path) -> paro_instance::InstanceCatalog {
    let layout = InstanceLayout::new(base_dir);
    let meta_store: Arc<dyn MetadataStore> =
        Arc::new(FileMetadataStore::new(layout.meta_dir()).expect("open instance meta store"));
    let store = InstanceCatalogStore::with_store(meta_store);
    store
        .load()
        .expect("load instance catalog")
        .expect("instance catalog should exist")
}

fn save_instance_catalog(base_dir: &Path, catalog: &mut paro_instance::InstanceCatalog) {
    let layout = InstanceLayout::new(base_dir);
    let meta_store: Arc<dyn MetadataStore> =
        Arc::new(FileMetadataStore::new(layout.meta_dir()).expect("open instance meta store"));
    let store = InstanceCatalogStore::with_store(meta_store);
    store.save(catalog).expect("persist instance catalog");
}

fn load_instance_run_state(base_dir: &Path) -> InstanceRunState {
    let layout = InstanceLayout::new(base_dir);
    let meta_store: Arc<dyn MetadataStore> =
        Arc::new(FileMetadataStore::new(layout.meta_dir()).expect("open instance meta store"));
    let store = InstanceRunStateStore::with_store(meta_store);
    store
        .load()
        .expect("load run state")
        .expect("run state should exist")
}

fn storage_identity_path(storage_dir: &Path) -> PathBuf {
    storage_dir
        .join("meta")
        .join("config")
        .join("storage_identity.json")
}

fn checkpoint_current_path(storage_dir: &Path) -> PathBuf {
    storage_dir
        .join("checkpoints")
        .join("manifests")
        .join("CURRENT")
}

fn load_storage_identity(storage_dir: &Path) -> DatabaseStorageIdentity {
    let path = storage_identity_path(storage_dir);
    let payload = std::fs::read(&path).expect("read storage identity");
    serde_json::from_slice(&payload).expect("deserialize storage identity")
}

fn save_storage_identity(storage_dir: &Path, identity: &DatabaseStorageIdentity) {
    let path = storage_identity_path(storage_dir);
    let payload = serde_json::to_vec_pretty(identity).expect("serialize storage identity");
    std::fs::write(path, payload).expect("write storage identity");
}

fn owner_lock_path(base_dir: &Path) -> PathBuf {
    InstanceLayout::new(base_dir).owner_lock_path()
}

fn run_state_path(base_dir: &Path) -> PathBuf {
    InstanceLayout::new(base_dir).run_state_path()
}

fn hook_marker_path(storage_dir: &Path) -> PathBuf {
    storage_dir.join("hook").join("marker.txt")
}

#[derive(Debug)]
struct FailingMarkerRecoveryHook {
    target_database: &'static str,
}

impl RecoveryHook for FailingMarkerRecoveryHook {
    fn name(&self) -> &'static str {
        "failing_marker"
    }

    fn run(
        &self,
        db: &Arc<DatabaseHandle>,
        ctx: &RecoveryHookContext,
    ) -> anyhow::Result<RecoveryHookResult> {
        if db.name() != self.target_database {
            return Ok(RecoveryHookResult::Skipped {
                reason: "non-target database".to_string(),
            });
        }

        let marker_path = ctx.database_root.join("hook").join("marker.txt");
        std::fs::create_dir_all(marker_path.parent().expect("marker parent"))?;
        std::fs::write(&marker_path, b"partial")?;
        anyhow::bail!(
            "simulated hook crash after writing {}",
            marker_path.display()
        );
    }
}

#[derive(Debug)]
struct IdempotentMarkerRecoveryHook {
    target_database: &'static str,
}

impl RecoveryHook for IdempotentMarkerRecoveryHook {
    fn name(&self) -> &'static str {
        "idempotent_marker"
    }

    fn run(
        &self,
        db: &Arc<DatabaseHandle>,
        ctx: &RecoveryHookContext,
    ) -> anyhow::Result<RecoveryHookResult> {
        if db.name() != self.target_database {
            return Ok(RecoveryHookResult::Skipped {
                reason: "non-target database".to_string(),
            });
        }

        let marker_path = ctx.database_root.join("hook").join("marker.txt");
        if let Ok(payload) = std::fs::read_to_string(&marker_path) {
            if payload == "ready" {
                return Ok(RecoveryHookResult::Reused);
            }
        }

        std::fs::create_dir_all(marker_path.parent().expect("marker parent"))?;
        std::fs::write(&marker_path, b"ready")?;
        Ok(RecoveryHookResult::Rebuilt {
            detail: Some(format!(
                "normalized recovery hook marker at {}",
                marker_path.display()
            )),
            issues: Vec::new(),
        })
    }
}

fn capture_wal_identity(db: &Arc<DatabaseHandle>) -> (String, WalHeaderMetadata) {
    let db_path = db.path().to_string();
    let storage_guard = db
        .storage_manager()
        .expect("storage manager should be available");
    let storage = storage_guard
        .as_ref()
        .expect("storage manager should contain backend");
    let metadata = storage
        .get_wal_arc()
        .map(|wal| wal.header_metadata())
        .unwrap_or_default();

    (db_path, metadata)
}

fn wal_probe_paths(db_path: &str) -> (PathBuf, PathBuf, PathBuf) {
    let root = PathBuf::from(db_path.split('?').next().unwrap_or(db_path));
    let wal_dir = root.join("wal");
    let wal_basename = root
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "db".to_string());

    (
        wal_dir.join(format!("{wal_basename}.wal")),
        wal_dir.join(format!("{wal_basename}.checkpoint.wal")),
        wal_dir.join(format!("{wal_basename}.recovery.wal")),
    )
}

fn active_segment_path(db_path: &str) -> PathBuf {
    let (main_wal, _, _) = wal_probe_paths(db_path);
    let store = SegmentCatalogStore::from_seed_path(&main_wal);
    let catalog = store
        .load()
        .expect("load segment catalog")
        .expect("segment catalog should exist");
    store.layout().segment_path(catalog.active_segment_id)
}

fn open_loaded_storage(path: &str) -> DatabaseStorage {
    let mut storage = DatabaseStorage::new(
        path.to_string(),
        Arc::new(StandardBufferManager::with_defaults(8 * 1024 * 1024)),
    );
    storage.load_existing().expect("load existing storage");
    storage.load_wal().expect("load WAL state");
    storage
}

fn write_flushed_missing_graph_publish_record(
    writer: &paro_journal::wal::wal_writer::WalWriter,
    lsn: u64,
    commit_id: u64,
    graph_name: &str,
) {
    let staging_root = std::env::temp_dir()
        .join("paro-missing-staged-artifact")
        .join(graph_name);
    let apply_descriptors = vec![ApplyDescriptor::PublishStagedArtifact(
        StagedArtifactDescriptor::PropertyGraphBuild {
            object: DdlObjectKey::new(
                "postgres",
                Some("public"),
                graph_name,
                DdlObjectKind::PropertyGraph,
            ),
            staging: StagingArtifactId::new(
                lsn,
                vec![
                    staging_root
                        .parent()
                        .expect("staging root parent")
                        .to_string_lossy()
                        .to_string(),
                    staging_root
                        .file_name()
                        .expect("staging root leaf")
                        .to_string_lossy()
                        .to_string(),
                ],
            ),
            schema_fingerprint: "fp:missing".to_string(),
        },
    )];
    let record = CommitRecord {
        record_version: COMMIT_RECORD_VERSION,
        metadata: JournalRecordMetadata::transaction(&[], &[], &apply_descriptors, &[]),
        txn_id: lsn,
        start_time: 0,
        commit_id,
        catalog_ops: Vec::new(),
        storage_ops: Vec::new(),
        apply_descriptors,
        deferred_tasks: Vec::new(),
    };
    let entry = WalEntry::JournalRecord {
        lsn,
        record: JournalRecord::Commit(record),
    };
    writer
        .write_entry(entry.wal_type(), &entry.serialize_data())
        .expect("write failing journal record");
    writer.flush().expect("flush failing journal record");
}

fn collect_single_int_rows(table: &TableHandle) -> Vec<i32> {
    let mut values = Vec::new();
    for chunk in table.scan_chunks().expect("scan chunks") {
        let ids = chunk.column(0).expect("column 0");
        for idx in 0..chunk.size() {
            values.push(ids.get_i32(idx).expect("value as i32"));
        }
    }
    values.sort_unstable();
    values
}

fn legacy_wal_checksum(buffer: &[u8]) -> u64 {
    const HASH_MULTIPLIER: u64 = 0xbf58476d1ce4e5b9;
    const MURMUR_M: u64 = 0xc6a4a7935bd1e995;
    const MURMUR_SEED: u64 = 0xe17a1465;
    const MURMUR_R: u32 = 47;

    fn checksum_u64(x: u64) -> u64 {
        x.wrapping_mul(HASH_MULTIPLIER)
    }

    fn checksum_remainder(data: &[u8]) -> u64 {
        let len = data.len();
        let mut h = MURMUR_SEED ^ ((len as u64).wrapping_mul(MURMUR_M));

        let n_blocks = len / 8;
        for i in 0..n_blocks {
            let offset = i * 8;
            let k = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());

            let k = k.wrapping_mul(MURMUR_M);
            let k = k ^ (k >> MURMUR_R);
            let k = k.wrapping_mul(MURMUR_M);

            h ^= k;
            h = h.wrapping_mul(MURMUR_M);
        }

        let remainder = &data[n_blocks * 8..];
        match remainder.len() {
            7 => {
                h ^= (remainder[6] as u64) << 48;
                h ^= (remainder[5] as u64) << 40;
                h ^= (remainder[4] as u64) << 32;
                h ^= (remainder[3] as u64) << 24;
                h ^= (remainder[2] as u64) << 16;
                h ^= (remainder[1] as u64) << 8;
                h ^= remainder[0] as u64;
                h = h.wrapping_mul(MURMUR_M);
            }
            6 => {
                h ^= (remainder[5] as u64) << 40;
                h ^= (remainder[4] as u64) << 32;
                h ^= (remainder[3] as u64) << 24;
                h ^= (remainder[2] as u64) << 16;
                h ^= (remainder[1] as u64) << 8;
                h ^= remainder[0] as u64;
                h = h.wrapping_mul(MURMUR_M);
            }
            5 => {
                h ^= (remainder[4] as u64) << 32;
                h ^= (remainder[3] as u64) << 24;
                h ^= (remainder[2] as u64) << 16;
                h ^= (remainder[1] as u64) << 8;
                h ^= remainder[0] as u64;
                h = h.wrapping_mul(MURMUR_M);
            }
            4 => {
                h ^= (remainder[3] as u64) << 24;
                h ^= (remainder[2] as u64) << 16;
                h ^= (remainder[1] as u64) << 8;
                h ^= remainder[0] as u64;
                h = h.wrapping_mul(MURMUR_M);
            }
            3 => {
                h ^= (remainder[2] as u64) << 16;
                h ^= (remainder[1] as u64) << 8;
                h ^= remainder[0] as u64;
                h = h.wrapping_mul(MURMUR_M);
            }
            2 => {
                h ^= (remainder[1] as u64) << 8;
                h ^= remainder[0] as u64;
                h = h.wrapping_mul(MURMUR_M);
            }
            1 => {
                h ^= remainder[0] as u64;
                h = h.wrapping_mul(MURMUR_M);
            }
            _ => {}
        }

        h ^= h >> MURMUR_R;
        h = h.wrapping_mul(MURMUR_M);
        h ^= h >> MURMUR_R;
        h
    }

    let mut result = 5381;
    let n_chunks = buffer.len() / 8;
    for i in 0..n_chunks {
        let offset = i * 8;
        let value = u64::from_le_bytes(buffer[offset..offset + 8].try_into().unwrap());
        result ^= checksum_u64(value);
    }

    let remainder = &buffer[n_chunks * 8..];
    if !remainder.is_empty() {
        result ^= checksum_remainder(remainder);
    }

    result
}

fn assert_legacy_wal_opcode_rejected(
    wal_name: &str,
    opcode: u8,
    payload: &[u8],
    expected_message: &str,
) {
    let dir = tempdir().expect("tempdir");
    let wal_path = dir.path().join(wal_name);
    let wal = WriteAheadLog::new(&wal_path).expect("initialize segment-backed wal");
    wal.flush().expect("materialize active segment header");
    let store = SegmentCatalogStore::from_seed_path(&wal_path);
    let catalog = store
        .load()
        .expect("load segment catalog")
        .expect("segment catalog should exist");
    let active_segment_path = store.layout().segment_path(catalog.active_segment_id);

    let mut entry_data = Vec::with_capacity(1 + payload.len());
    entry_data.push(opcode);
    entry_data.extend_from_slice(payload);

    let mut file = OpenOptions::new()
        .append(true)
        .open(&active_segment_path)
        .expect("open wal for raw append");
    file.write_all(&(entry_data.len() as u64).to_le_bytes())
        .expect("write entry size");
    file.write_all(&legacy_wal_checksum(&entry_data).to_le_bytes())
        .expect("write checksum");
    file.write_all(&entry_data).expect("write legacy entry");
    file.sync_all().expect("sync wal");

    let catalog = Arc::new(paro_catalog::database_catalog::ParoCatalog::new(
        "test".to_string(),
    ));
    catalog.initialize(false);
    let err = paro_instance::recover_database(&wal_path, &catalog, None)
        .expect_err("legacy opcode should fail recovery");
    assert!(err.to_string().contains(expected_message));
}

fn schema_exists(db: &Arc<DatabaseHandle>, schema_name: &str) -> bool {
    let txn = CatalogSnapshot::read_only(u64::MAX);
    db.catalog().get_schema(&txn, schema_name).is_ok()
}

fn create_single_int_table(
    db: &Arc<DatabaseHandle>,
    table_name: &str,
    values: &[i32],
) -> Arc<TableHandle> {
    let storage = Arc::new(create_table(&[LogicalType::Integer]));
    let write_txn = CatalogSnapshot::permanent_writer(u64::MAX);
    db.catalog()
        .create_table_in_snapshot(
            &write_txn,
            "public",
            table_name,
            vec![ColumnDefinition::new(
                "id".to_string(),
                LogicalType::Integer,
            )],
            Arc::clone(&storage),
        )
        .expect("create table should succeed");
    db.sync_compaction_tablets()
        .expect("compaction registry should reflect test catalog create");

    let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![
        paro_common::test_utils::test_i32_vector(values),
    ]);
    storage.append(&chunk).expect("append table data");
    storage
}

fn wait_for(description: &str, timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for {description}");
}

#[test]
fn checkpoint_manifest_records_segment_tail_without_sidecars() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());
    let db = default_db(&instance);

    create_single_int_table(&db, "manifest_tail_items", &[1, 2, 3]);
    db.force_checkpoint().expect("checkpoint should succeed");

    let (db_path, _) = capture_wal_identity(&db);
    let (main_wal, checkpoint_wal, recovery_wal) = wal_probe_paths(&db_path);
    let manifest_store = ManifestStore::open_database_root(&db_path).expect("open manifest store");
    let manifest = manifest_store
        .read_current_manifest()
        .expect("read current manifest")
        .expect("checkpoint manifest should exist");

    assert_eq!(
        main_wal.extension().and_then(|ext| ext.to_str()),
        Some("wal"),
        "storage should still expose a main WAL seed path"
    );
    assert!(
        manifest.journal.replay_from_segment_id > 0,
        "committed checkpoint should point at a concrete journal segment"
    );
    assert!(
        SegmentCatalogStore::from_seed_path(&main_wal)
            .load()
            .expect("load segment catalog")
            .is_some(),
        "segment catalog should exist for checkpoint manifest replay"
    );
    assert!(
        !checkpoint_wal.exists(),
        "legacy checkpoint sidecar WAL must not exist"
    );
    assert!(
        !recovery_wal.exists(),
        "legacy recovery sidecar WAL must not exist"
    );
}

#[test]
fn automatic_checkpoint_bytes_trigger_coalesces_into_committed_manifest_publish() {
    let dir = tempdir().expect("tempdir");
    let mut config =
        InstanceConfig::new().with_instance_root(dir.path().to_string_lossy().to_string());
    config.options.checkpoint.trigger_bytes = 1;
    config.options.checkpoint.trigger_interval = Duration::from_secs(3600);
    let instance = open_instance_with_config(config);
    let db = default_db(&instance);

    let (_summary, lsn) = db.publish_checkpoint_transaction(1, 0, 0);
    if let Some(wal) = db.wal() {
        wal.note_flushed_lsn(lsn)
            .expect("checkpoint transaction lsn should flush");
    }
    {
        let storage = db
            .storage_manager()
            .expect("storage manager should be available");
        storage
            .as_ref()
            .expect("storage should be attached")
            .set_wal_size(1);
    }

    db.schedule_auto_checkpoint_if_needed();
    db.schedule_auto_checkpoint_if_needed();

    let current_path = checkpoint_current_path(Path::new(db.path()));
    wait_for(
        "automatic bytes-trigger checkpoint publish",
        Duration::from_secs(5),
        || current_path.exists(),
    );

    let manifest_store = ManifestStore::open_database_root(db.path()).expect("open manifest store");
    let manifest = manifest_store
        .read_current_manifest()
        .expect("read current manifest")
        .expect("checkpoint manifest should exist");
    assert_eq!(manifest.frontier.checkpoint_lsn, 1);
    assert_eq!(manifest.checkpoint_id, 1);
}

#[test]
fn automatic_checkpoint_interval_trigger_runs_after_elapsed_interval() {
    let dir = tempdir().expect("tempdir");
    let mut config =
        InstanceConfig::new().with_instance_root(dir.path().to_string_lossy().to_string());
    config.options.checkpoint.trigger_bytes = u64::MAX;
    config.options.checkpoint.trigger_interval = Duration::from_millis(50);
    let instance = open_instance_with_config(config);
    let db = default_db(&instance);

    let (_summary, lsn) = db.publish_checkpoint_transaction(1, 0, 0);
    if let Some(wal) = db.wal() {
        wal.note_flushed_lsn(lsn)
            .expect("checkpoint transaction lsn should flush");
    }
    {
        let storage = db
            .storage_manager()
            .expect("storage manager should be available");
        storage
            .as_ref()
            .expect("storage should be attached")
            .set_wal_size(1);
    }

    thread::sleep(Duration::from_millis(80));
    db.schedule_auto_checkpoint_if_needed();

    let current_path = checkpoint_current_path(Path::new(db.path()));
    wait_for(
        "automatic interval-trigger checkpoint publish",
        Duration::from_secs(5),
        || current_path.exists(),
    );

    let manifest_store = ManifestStore::open_database_root(db.path()).expect("open manifest store");
    let manifest = manifest_store
        .read_current_manifest()
        .expect("read current manifest")
        .expect("checkpoint manifest should exist");
    assert_eq!(manifest.frontier.checkpoint_lsn, 1);
}

#[test]
fn checkpoint_manifest_bootstrap_restores_allocator_and_frontier_watermarks() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());
    let db = default_db(&instance);

    let (_summary, lsn) = db.publish_checkpoint_transaction(7, 7, 99);
    if let Some(wal) = db.wal() {
        wal.note_flushed_lsn(lsn)
            .expect("checkpoint transaction lsn should flush");
    }
    db.force_checkpoint().expect("checkpoint should succeed");
    drop(instance);

    let reopened = open_instance(dir.path());
    let db = default_db(&reopened);
    let (summary, lsn) = db.publish_checkpoint_transaction(8, 0, 0);
    if let Some(wal) = db.wal() {
        wal.note_flushed_lsn(lsn)
            .expect("checkpoint transaction lsn should flush after restart");
    }

    assert_eq!(summary.max_lsn, 2);
    assert_eq!(summary.max_commit_id, 8);
    assert_eq!(
        summary.max_catalog_commit_id, 7,
        "manifest bootstrap should restore catalog watermark without replaying trimmed prefix"
    );
    assert_eq!(
        summary.max_seen_object_id, 99,
        "manifest bootstrap should restore object-id allocator floor"
    );
}

#[test]
fn checkpoint_base_restore_reloads_tablet_state_from_snapshot_bundles() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());
    let db = default_db(&instance);

    create_single_int_table(&db, "checkpoint_base_items", &[10, 20, 30]);
    db.force_checkpoint().expect("checkpoint should succeed");

    let storage = open_loaded_storage(db.path());
    let restored_catalog = Arc::new(paro_catalog::database_catalog::ParoCatalog::new(
        "postgres".to_string(),
    ));
    let checkpoint_base = CheckpointRecovery::load_base_from_storage(
        restored_catalog.as_ref(),
        &storage,
        storage.get_tablet_meta_manager(),
    )
    .expect("load checkpoint base");

    assert!(checkpoint_base.checkpoint_id.is_some());
    assert!(checkpoint_base.frontier.is_some());
    assert!(checkpoint_base.journal_tail.is_some());

    let txn = CatalogSnapshot::read_only(u64::MAX);
    let table = restored_catalog
        .get_table(&txn, "public", "checkpoint_base_items")
        .expect("checkpointed table should restore");
    let CatalogEntryEnum::Table(table) = table.as_ref() else {
        panic!("expected table entry");
    };
    let storage = table.get_storage().expect("table storage should restore");
    assert_eq!(collect_single_int_rows(storage.as_ref()), vec![10, 20, 30]);
    assert!(
        storage.rowset_count() > 0,
        "tablet rowsets should restore from bundles"
    );
}

#[test]
fn checkpoint_base_restore_rejects_foreign_checkpoint_identity() {
    fn copy_dir_all(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).expect("create destination directory");
        for entry in std::fs::read_dir(src).expect("read source directory") {
            let entry = entry.expect("directory entry");
            let file_type = entry.file_type().expect("entry file type");
            let target = dst.join(entry.file_name());
            if file_type.is_dir() {
                copy_dir_all(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).expect("copy file");
            }
        }
    }

    let source_dir = tempdir().expect("tempdir");
    let source_instance = open_instance(source_dir.path());
    let source_db = default_db(&source_instance);
    source_db
        .force_checkpoint()
        .expect("checkpoint should succeed");

    let target_dir = tempdir().expect("tempdir");
    let target_storage_dir = target_dir.path().join("target_db");
    let mut target_storage = DatabaseStorage::new(
        target_storage_dir.to_string_lossy().to_string(),
        Arc::new(StandardBufferManager::with_defaults(8 * 1024 * 1024)),
    );
    target_storage.create_new().expect("create target storage");
    target_storage
        .bootstrap_storage_identity(99)
        .expect("bootstrap target storage identity");

    copy_dir_all(
        &PathBuf::from(source_db.path()).join("checkpoints"),
        &target_storage_dir.join("checkpoints"),
    );

    let target_storage = open_loaded_storage(target_storage_dir.to_string_lossy().as_ref());
    let restored_catalog = Arc::new(paro_catalog::database_catalog::ParoCatalog::new(
        "postgres".to_string(),
    ));
    let err = CheckpointRecovery::load_base_from_storage(
        restored_catalog.as_ref(),
        &target_storage,
        target_storage.get_tablet_meta_manager(),
    )
    .expect_err("foreign checkpoint identity should be rejected");

    assert!(err.to_string().contains("identity mismatch"));
}

#[test]
fn checkpoint_base_restore_preflight_does_not_mutate_live_tablets_on_invalid_catalog() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());
    let db = default_db(&instance);

    let durable_storage = Arc::new(
        TableFactory::new(db.tablet_meta_manager())
            .create_table(&[LogicalType::Integer])
            .expect("create durable test table"),
    );
    let write_txn = CatalogSnapshot::permanent_writer(u64::MAX);
    db.catalog()
        .create_table_in_snapshot(
            &write_txn,
            "public",
            "checkpoint_preflight_items",
            vec![ColumnDefinition::new(
                "id".to_string(),
                LogicalType::Integer,
            )],
            Arc::clone(&durable_storage),
        )
        .expect("install durable test table");
    db.sync_compaction_tablets()
        .expect("sync compaction registry for durable table");
    durable_storage
        .append(&paro_common::test_utils::test_chunk_from_vectors(vec![
            paro_common::test_utils::test_i32_vector(&[1, 2, 3]),
        ]))
        .expect("append durable test data");
    db.force_checkpoint().expect("checkpoint should succeed");

    let storage = open_loaded_storage(db.path());
    let tablet_meta_manager = storage
        .get_tablet_meta_manager()
        .expect("tablet meta manager should exist");
    let before_tablets: Vec<u64> = tablet_meta_manager
        .scan_all_tablets()
        .expect("scan tablets before corrupt checkpoint")
        .into_iter()
        .map(|meta| meta.tablet_id())
        .collect();
    assert!(
        !before_tablets.is_empty(),
        "checkpointed database should have durable tablet metadata"
    );

    let manifest_store = ManifestStore::open_database_root(db.path()).expect("open manifest store");
    let manifest = manifest_store
        .read_current_manifest()
        .expect("read current manifest")
        .expect("current manifest should exist");
    let identity =
        ManifestStore::load_database_identity(&storage).expect("load checkpoint identity");
    let mut staged = manifest_store
        .begin_publish(identity)
        .expect("begin corrupt publish");

    for bundle in &manifest.bundle_refs {
        let file_name = Path::new(&bundle.locator)
            .file_name()
            .expect("bundle file name")
            .to_string_lossy()
            .to_string();
        match &bundle.kind {
            BundleKind::TabletShard { .. } => {}
            BundleKind::Catalog => manifest_store
                .stage_raw_bundle(
                    &mut staged,
                    &file_name,
                    bundle.kind.clone(),
                    bundle.format_version,
                    b"corrupt-catalog",
                    bundle.base_checkpoint_id,
                )
                .expect("stage corrupt catalog bundle"),
            _ => {
                let payload = manifest_store
                    .read_bundle_payload(bundle)
                    .expect("read original bundle payload");
                manifest_store
                    .stage_raw_bundle(
                        &mut staged,
                        &file_name,
                        bundle.kind.clone(),
                        bundle.format_version,
                        &payload,
                        bundle.base_checkpoint_id,
                    )
                    .expect("stage copied bundle");
            }
        }
    }

    manifest_store
        .publish_manifest(
            staged,
            manifest.frontier.clone(),
            manifest.bootstrap.clone(),
            manifest.journal.clone(),
            manifest.retention_floor.clone(),
        )
        .expect("publish corrupt manifest");

    let restored_catalog = Arc::new(paro_catalog::database_catalog::ParoCatalog::new(
        "postgres".to_string(),
    ));
    let err = CheckpointRecovery::load_base_from_storage(
        restored_catalog.as_ref(),
        &storage,
        Some(tablet_meta_manager.clone()),
    )
    .expect_err("corrupt catalog bundle should fail preflight");
    assert!(err.to_string().contains("Invalid catalog snapshot magic"));

    let after_tablets: Vec<u64> = tablet_meta_manager
        .scan_all_tablets()
        .expect("scan tablets after corrupt checkpoint")
        .into_iter()
        .map(|meta| meta.tablet_id())
        .collect();
    assert_eq!(
        after_tablets, before_tablets,
        "failed checkpoint preflight must not rewrite live tablet metadata"
    );
}

#[test]
fn recover_database_with_checkpoint_replays_only_tail_journal() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());
    let db = default_db(&instance);

    create_single_int_table(&db, "tail_replay_base_items", &[7, 8, 9]);
    db.force_checkpoint().expect("checkpoint should succeed");

    let (db_path, metadata) = capture_wal_identity(&db);
    let wal = db.wal().expect("persistent db should have WAL");
    let tail_lsn = 211;
    write_flushed_create_schema_txn_with_lsn(
        wal.writer().as_ref(),
        "postgres",
        "tail_only_schema",
        11,
        tail_lsn,
        tail_lsn,
    )
    .expect("append tail schema txn");

    let storage = open_loaded_storage(&db_path);
    let restored_catalog = Arc::new(paro_catalog::database_catalog::ParoCatalog::new(
        "postgres".to_string(),
    ));
    let checkpoint_base = CheckpointRecovery::load_base_from_storage(
        restored_catalog.as_ref(),
        &storage,
        storage.get_tablet_meta_manager(),
    )
    .expect("load checkpoint base");

    let (main_wal, checkpoint_wal, recovery_wal) = wal_probe_paths(&db_path);
    let (_wal, replay, summary) = recover_database_with_checkpoint(
        &main_wal,
        &restored_catalog,
        storage.get_tablet_meta_manager(),
        checkpoint_base.journal_tail.clone(),
        Some(metadata),
        Some(storage.wal_keep_from()),
    )
    .expect("recover from checkpoint base + tail");

    assert_eq!(
        replay.entries_replayed, 2,
        "tail replay should apply one committed txn plus its flush boundary"
    );
    assert_eq!(
        summary.max_lsn, tail_lsn,
        "tail replay should surface the highest durable logical LSN from the replayed journal tail"
    );
    assert!(schema_exists(&db, "public"));
    let txn = CatalogSnapshot::read_only(u64::MAX);
    assert!(restored_catalog
        .get_table(&txn, "public", "tail_replay_base_items")
        .is_ok());
    assert!(restored_catalog
        .get_schema(&txn, "tail_only_schema")
        .is_ok());
    assert!(
        !checkpoint_wal.exists(),
        "legacy checkpoint sidecar WAL must not exist"
    );
    assert!(
        !recovery_wal.exists(),
        "legacy recovery sidecar WAL must not exist"
    );
}

#[test]
fn checkpoint_tail_replay_spans_multiple_rotated_segments() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());
    let db = default_db(&instance);

    let wal = db.wal().expect("persistent db should have WAL");
    write_flushed_create_schema_txn(
        wal.writer().as_ref(),
        "postgres",
        "before_checkpoint_segment_one",
        1,
        100,
    )
    .expect("write first schema txn");
    wal.writer()
        .truncate(DEFAULT_SEGMENT_ROTATION_BYTES)
        .expect("inflate active segment to rotation threshold");
    wal.note_flushed_lsn(1)
        .expect("rotate after sealing first segment");

    write_flushed_create_schema_txn(
        wal.writer().as_ref(),
        "postgres",
        "before_checkpoint_segment_two",
        2,
        101,
    )
    .expect("write second schema txn");
    wal.note_flushed_lsn(2).expect("record second schema flush");
    db.force_checkpoint().expect("checkpoint should succeed");

    write_flushed_create_schema_txn(
        wal.writer().as_ref(),
        "postgres",
        "tail_segment_two",
        3,
        102,
    )
    .expect("write tail schema in second segment");
    wal.writer()
        .truncate(DEFAULT_SEGMENT_ROTATION_BYTES)
        .expect("inflate second segment to rotation threshold");
    wal.note_flushed_lsn(3).expect("rotate into third segment");

    write_flushed_create_schema_txn(
        wal.writer().as_ref(),
        "postgres",
        "tail_segment_three",
        4,
        103,
    )
    .expect("write tail schema in third segment");
    wal.note_flushed_lsn(4).expect("record third segment flush");

    let segment_catalog = wal.segment_catalog_snapshot();
    assert!(
        segment_catalog.segments.len() >= 3,
        "setup should produce at least three physical WAL segments"
    );

    drop(instance);

    let restarted = open_instance(dir.path());
    let restarted_db = default_db(&restarted);
    assert!(schema_exists(
        &restarted_db,
        "before_checkpoint_segment_one"
    ));
    assert!(schema_exists(
        &restarted_db,
        "before_checkpoint_segment_two"
    ));
    assert!(schema_exists(&restarted_db, "tail_segment_two"));
    assert!(schema_exists(&restarted_db, "tail_segment_three"));
}

#[test]
fn checkpoint_tail_replay_stops_after_first_partial_segment() {
    let dir = tempdir().expect("tempdir");
    let wal_path = dir.path().join("partial_tail_stop.wal");
    let wal = WriteAheadLog::new(&wal_path).expect("initialize segment-backed wal");

    write_flushed_create_schema_txn(wal.writer().as_ref(), "postgres", "segment_one_ok", 1, 100)
        .expect("write first segment schema");
    wal.writer()
        .truncate(DEFAULT_SEGMENT_ROTATION_BYTES)
        .expect("inflate first segment to rotation threshold");
    wal.note_flushed_lsn(1).expect("rotate into second segment");

    write_flushed_missing_graph_publish_record(
        wal.writer().as_ref(),
        2,
        101,
        "missing_graph_segment_two",
    );
    wal.writer()
        .truncate(DEFAULT_SEGMENT_ROTATION_BYTES)
        .expect("inflate second segment to rotation threshold");
    wal.note_flushed_lsn(2).expect("rotate into third segment");

    write_flushed_create_schema_txn(
        wal.writer().as_ref(),
        "postgres",
        "segment_three_must_not_apply",
        3,
        102,
    )
    .expect("write third segment schema");

    let restored_catalog = Arc::new(paro_catalog::database_catalog::ParoCatalog::new(
        "postgres".to_string(),
    ));
    restored_catalog.initialize(false);

    let (_wal, replay, summary) =
        recover_database_with_checkpoint(&wal_path, &restored_catalog, None, None, None, None)
            .expect("recover from multi-segment tail");

    assert!(!replay.all_succeeded);
    assert!(replay
        .error
        .as_deref()
        .is_some_and(|msg| msg.contains("missing staged property graph artifact")));
    assert_eq!(
        summary.max_lsn, 1,
        "recovery summary must stop at the last exact-prefix segment"
    );

    let txn = CatalogSnapshot::read_only(u64::MAX);
    assert!(restored_catalog.get_schema(&txn, "segment_one_ok").is_ok());
    assert!(restored_catalog
        .get_schema(&txn, "segment_three_must_not_apply")
        .is_err());
}

#[test]
fn close_path_checkpoint_has_no_data_loss_window() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());
    let db = default_db(&instance);
    let (db_path, _) = capture_wal_identity(&db);
    let (_, checkpoint_wal, recovery_wal) = wal_probe_paths(&db_path);

    let wal = db.wal().expect("persistent db should have WAL");
    write_flushed_create_schema_txn(
        wal.writer().as_ref(),
        "postgres",
        "close_path_checkpoint_window",
        1,
        100,
    )
    .expect("write schema txn");

    db.close(DatabaseCloseAction::Checkpoint)
        .expect("close checkpoint should succeed");
    assert!(
        !checkpoint_wal.exists(),
        "close-path checkpoint should not leave legacy checkpoint sidecar WAL behind"
    );
    assert!(
        !recovery_wal.exists(),
        "close-path checkpoint should not leave legacy recovery sidecar WAL behind"
    );

    let report = db.check_wal_health().expect("wal health check");
    assert!(
        report.healthy,
        "segment journal should remain healthy after close"
    );
}

#[test]
fn torn_write_tail_is_repaired_on_instance_startup() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());
    let db = default_db(&instance);

    let wal = db.wal().expect("persistent db should have WAL");
    write_flushed_create_schema_txn(
        wal.writer().as_ref(),
        "postgres",
        "torn_before_restart",
        1,
        100,
    )
    .expect("write schema txn");

    db.close(DatabaseCloseAction::Checkpoint)
        .expect("close with checkpoint should succeed");
    let (db_path, _) = capture_wal_identity(&db);
    let active_segment = active_segment_path(&db_path);
    let size_before_torn = std::fs::metadata(&active_segment)
        .expect("active segment metadata before torn write")
        .len();

    drop(instance);

    {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&active_segment)
            .expect("open active segment for torn write injection");
        file.write_all(&[0xAA, 0xBB, 0xCC, 0xDD, 0xEE])
            .expect("append torn tail");
        file.sync_all().expect("sync torn tail");
    }
    let size_with_torn = std::fs::metadata(&active_segment)
        .expect("active segment metadata after torn write")
        .len();
    assert!(
        size_with_torn > size_before_torn,
        "torn write injection should increase active segment size"
    );

    let restarted = open_instance(dir.path());
    let restarted_db = default_db(&restarted);
    assert!(schema_exists(&restarted_db, "torn_before_restart"));

    let report = restarted_db
        .check_wal_health()
        .expect("wal health check after torn-write recovery");
    assert!(report.healthy, "recovery should repair torn WAL tail");

    let size_after_recovery = std::fs::metadata(&active_segment)
        .expect("active segment metadata after recovery")
        .len();
    assert!(
        size_after_recovery < size_with_torn,
        "recovery should truncate torn tail bytes from the active segment"
    );
}

#[test]
fn wal_path_construction_supports_query_parameters() {
    let db_path = "/tmp/paro_instance_test.db?token=abc&readonly=true";
    assert_eq!(
        wal_path_with_suffix(db_path, ".wal"),
        "/tmp/paro_instance_test.db.wal?token=abc&readonly=true"
    );
}

#[test]
fn legacy_row_tuple_update_wal_rejects_recovery() {
    assert_legacy_wal_opcode_rejected(
        "legacy_update_replay.wal",
        28,
        &[0u8; 4],
        "unsupported historical WAL opcode 28",
    );
}

#[test]
fn legacy_row_tuple_delete_wal_rejects_recovery() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&0u32.to_le_bytes());
    assert_legacy_wal_opcode_rejected(
        "legacy_delete_replay.wal",
        27,
        &payload,
        "unsupported historical WAL opcode 27",
    );
}

#[test]
fn legacy_segment_wal_version_rejects_recovery() {
    let dir = tempdir().expect("tempdir");
    let wal_path = dir.path().join("legacy_segment_version.wal");
    let active_segment_path = {
        let wal = WriteAheadLog::new(&wal_path).expect("initialize segment-backed wal");
        wal.flush().expect("materialize active segment header");

        let store = SegmentCatalogStore::from_seed_path(&wal_path);
        let catalog = store
            .load()
            .expect("load segment catalog")
            .expect("segment catalog should exist");
        store.layout().segment_path(catalog.active_segment_id)
    };

    let mut file = OpenOptions::new()
        .write(true)
        .open(&active_segment_path)
        .expect("open active segment for header rewrite");
    file.seek(SeekFrom::Start(1))
        .expect("seek to WAL version field");
    file.write_all(&2u64.to_le_bytes())
        .expect("overwrite WAL version");
    file.sync_all().expect("sync rewritten WAL header");

    let catalog = Arc::new(paro_catalog::database_catalog::ParoCatalog::new(
        "test".to_string(),
    ));
    catalog.initialize(false);
    let err = paro_instance::recover_database(&wal_path, &catalog, None)
        .expect_err("legacy segment WAL version should fail recovery");
    assert!(err.is_feature_not_supported());
    assert!(err.to_string().contains("unsupported WAL version 2"));
}

// --- Instance acceptance tests (startup, checkpoint, compaction) ---

/// Instance startup path: storage load + WAL recovery.
///
/// Verifies that a fresh instance → write data → close → reopen follows the
/// unified startup path and recovers all state correctly.
#[test]
fn instance_startup_single_path_storage_load_and_wal_recovery() {
    let dir = tempdir().expect("tempdir");

    // First startup writes durable state and closes through the checkpoint path.
    {
        let instance = open_instance(dir.path());
        let db = default_db(&instance);
        create_single_int_table(&db, "startup_path_items", &[1, 2, 3]);

        let wal = db.wal().expect("persistent db should have WAL");
        write_flushed_create_schema_txn(
            wal.writer().as_ref(),
            "postgres",
            "startup_test_schema",
            1,
            100,
        )
        .expect("write schema to WAL");

        db.close(DatabaseCloseAction::Checkpoint)
            .expect("close with checkpoint");
    }

    // Restart should load storage and replay WAL through the same startup path.
    let instance = open_instance(dir.path());
    let db = default_db(&instance);

    // Verify storage was loaded (table exists)
    let txn = CatalogSnapshot::read_only(u64::MAX);
    assert!(
        db.catalog()
            .get_table(&txn, "public", "startup_path_items")
            .is_ok()
            || schema_exists(&db, "startup_test_schema"),
        "startup should recover state via unified storage load + WAL recovery path"
    );

    // Verify WAL health after startup
    let report = db
        .check_wal_health()
        .expect("WAL health check should succeed after startup");
    assert!(report.healthy, "WAL should be healthy after startup");
}

/// All checkpoint entries go through the WAL coordination protocol.
///
/// Verifies that both `force_checkpoint()` and `close(Checkpoint)` use the
/// same WAL-coordinated checkpoint path.
#[test]
fn all_checkpoint_entries_use_wal_coordination() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());
    let db = default_db(&instance);

    create_single_int_table(&db, "checkpoint_coord_items", &[100, 200]);

    // force_checkpoint goes through WAL coordination
    db.force_checkpoint()
        .expect("force_checkpoint should succeed via WAL coordination");

    let (db_path, _) = capture_wal_identity(&db);
    let (_, checkpoint_wal, recovery_wal) = wal_probe_paths(&db_path);

    // After a successful checkpoint, no stale checkpoint/recovery WAL should remain
    assert!(
        !checkpoint_wal.exists(),
        "legacy checkpoint sidecar WAL should not exist after force_checkpoint"
    );
    assert!(
        !recovery_wal.exists(),
        "legacy recovery sidecar WAL should not exist after force_checkpoint"
    );

    let wal = db.wal().expect("WAL should exist");
    write_flushed_create_schema_txn(
        wal.writer().as_ref(),
        "postgres",
        "post_checkpoint_schema",
        2,
        101,
    )
    .expect("write schema");

    db.close(DatabaseCloseAction::Checkpoint)
        .expect("close with checkpoint should use WAL coordination");

    // After close-path checkpoint, no stale WAL files should remain
    assert!(
        !checkpoint_wal.exists(),
        "close-path checkpoint should not leave checkpoint WAL behind"
    );
    assert!(
        !recovery_wal.exists(),
        "close-path checkpoint should not leave recovery WAL behind"
    );

    drop(instance);

    // Verify restart recovers everything
    let restarted = open_instance(dir.path());
    let restarted_db = default_db(&restarted);
    let report = restarted_db
        .check_wal_health()
        .expect("WAL health check after restart");
    assert!(report.healthy, "WAL should be healthy after restart");
}

/// Compaction scheduling integrates with instance lifecycle.
///
/// Verifies:
/// - Compaction manager is created on startup
/// - Tablets are synced after recovery
/// - Compaction is not suspended after normal startup
/// - Checkpoint does not leave compaction suspended
#[test]
fn compaction_scheduling_integrates_with_instance_lifecycle() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());
    let db = default_db(&instance);

    // Create tables so compaction has tablets to manage
    create_single_int_table(&db, "lifecycle_t1", &[1, 2, 3]);
    create_single_int_table(&db, "lifecycle_t2", &[4, 5, 6]);

    // Sync tablets with compaction manager
    db.sync_compaction_tablets()
        .expect("compaction tablet sync should succeed");

    let obs = db
        .compaction_observability()
        .expect("compaction manager should exist for persistent db");
    assert!(
        obs.registered_tablets >= 2,
        "at least 2 tablets should be registered"
    );
    assert!(
        !obs.suspended,
        "compaction should not be suspended during normal operation"
    );

    // Checkpoint should complete without leaving compaction suspended
    db.force_checkpoint().expect("checkpoint should succeed");

    let obs_after = db
        .compaction_observability()
        .expect("compaction manager should still exist");
    assert!(
        !obs_after.suspended,
        "compaction should resume after checkpoint completes"
    );

    // Close and reopen — compaction should be re-initialized
    db.close(DatabaseCloseAction::Checkpoint)
        .expect("close with checkpoint");
    drop(instance);

    let restarted = open_instance(dir.path());
    let restarted_db = default_db(&restarted);

    restarted_db
        .sync_compaction_tablets()
        .expect("compaction sync after restart");

    let restarted_obs = restarted_db
        .compaction_observability()
        .expect("compaction manager should exist after restart");
    assert!(
        !restarted_obs.suspended,
        "compaction should not be suspended after restart"
    );
}

#[test]
fn bootstrap_persists_instance_catalog_and_stable_default_database_id() {
    let dir = tempdir().expect("tempdir");
    let expected_path = dir.path().join("databases").join("db-1");

    {
        let instance = open_instance(dir.path());
        let db = default_db(&instance);
        assert_eq!(db.id(), 1, "default database id should start at 1");
        assert_eq!(PathBuf::from(db.path()), expected_path);
    }

    let catalog = load_instance_catalog(dir.path());
    assert_eq!(catalog.default_database_id, Some(1));
    assert_eq!(catalog.next_database_id, 2);
    assert_eq!(catalog.databases.len(), 1);
    assert_eq!(catalog.databases[0].database_id, 1);
    assert_eq!(catalog.databases[0].name, "postgres");
    assert_eq!(
        PathBuf::from(&catalog.databases[0].storage_dir),
        expected_path
    );

    let restarted = open_instance(dir.path());
    let db = default_db(&restarted);
    assert_eq!(db.id(), 1, "restart should preserve default database id");
    assert_eq!(PathBuf::from(db.path()), expected_path);
}

#[test]
fn create_database_persists_database_id_and_catalog_record() {
    let dir = tempdir().expect("tempdir");
    let expected_path = dir.path().join("databases").join("db-2");

    {
        let instance = open_instance(dir.path());
        let analytics = instance
            .create_database("analytics")
            .expect("create database should succeed");
        assert_eq!(analytics.id(), 2);
        assert_eq!(PathBuf::from(analytics.path()), expected_path);
    }

    let catalog = load_instance_catalog(dir.path());
    let analytics = catalog
        .find_database_by_name("analytics")
        .expect("analytics record should be durable");
    assert_eq!(analytics.database_id, 2);
    assert_eq!(PathBuf::from(&analytics.storage_dir), expected_path);
    assert_eq!(catalog.next_database_id, 3);

    let restarted = open_instance(dir.path());
    let analytics = restarted
        .database_registry()
        .get_database("analytics")
        .expect("analytics should reload on restart");
    assert_eq!(analytics.id(), 2);
    assert_eq!(PathBuf::from(analytics.path()), expected_path);
}

#[test]
fn drop_database_removes_catalog_record_and_storage_dir() {
    let dir = tempdir().expect("tempdir");

    let storage_dir = {
        let instance = open_instance(dir.path());
        let drop_me = instance
            .create_database("drop_me")
            .expect("create database should succeed");
        let storage_dir = PathBuf::from(drop_me.path());
        assert!(storage_dir.exists(), "database storage dir should exist");

        instance
            .drop_database("drop_me")
            .expect("drop database should succeed");
        assert!(
            !storage_dir.exists(),
            "drop should remove the managed database directory"
        );
        storage_dir
    };

    let catalog = load_instance_catalog(dir.path());
    assert!(
        catalog.find_database_by_name("drop_me").is_none(),
        "dropped database should be removed from instance catalog"
    );

    let restarted = open_instance(dir.path());
    assert!(
        restarted
            .database_registry()
            .get_database("drop_me")
            .is_none(),
        "dropped database should not be published after restart"
    );
    assert!(
        !storage_dir.exists(),
        "managed storage directory should stay removed after restart"
    );
}

#[test]
fn rename_database_updates_catalog_and_persists_across_restart() {
    let dir = tempdir().expect("tempdir");

    let storage_dir = {
        let instance = open_instance(dir.path());
        let analytics = instance
            .create_database("analytics")
            .expect("create database should succeed");
        let storage_dir = PathBuf::from(analytics.path());

        instance
            .rename_database("analytics", "warehouse")
            .expect("rename database should succeed");

        assert!(
            instance
                .database_registry()
                .get_database("analytics")
                .is_none(),
            "old runtime name should be unpublished after rename"
        );
        let runtime_db = instance
            .database_registry()
            .get_database("warehouse")
            .expect("new runtime name should be published");
        assert_eq!(runtime_db.id(), analytics.id());
        assert_eq!(PathBuf::from(runtime_db.path()), storage_dir);
        storage_dir
    };

    let catalog = load_instance_catalog(dir.path());
    assert!(
        catalog.find_database_by_name("analytics").is_none(),
        "old name should be removed from durable catalog"
    );
    let warehouse = catalog
        .find_database_by_name("warehouse")
        .expect("renamed database should persist in durable catalog");
    assert_eq!(warehouse.database_id, 2);
    assert_eq!(PathBuf::from(&warehouse.storage_dir), storage_dir);

    let restarted = open_instance(dir.path());
    assert!(
        restarted
            .database_registry()
            .get_database("analytics")
            .is_none(),
        "old database name should not be restored after restart"
    );
    let warehouse = restarted
        .database_registry()
        .get_database("warehouse")
        .expect("renamed database should reload after restart");
    assert_eq!(warehouse.id(), 2);
    assert_eq!(PathBuf::from(warehouse.path()), storage_dir);
}

#[test]
fn rename_default_database_keeps_default_pointer_by_database_id() {
    let dir = tempdir().expect("tempdir");

    {
        let instance = open_instance_with_config(
            InstanceConfig::new()
                .with_instance_root(dir.path().to_string_lossy().to_string())
                .with_default_database("appdb"),
        );

        assert_eq!(
            instance
                .database_registry()
                .default_database_name()
                .as_deref(),
            Some("appdb")
        );

        instance
            .rename_database("appdb", "warehouse")
            .expect("rename default database should succeed");

        assert!(
            instance.database_registry().get_database("appdb").is_none(),
            "old runtime name should be unpublished after default rename"
        );
        assert!(
            instance
                .database_registry()
                .get_database("warehouse")
                .is_some(),
            "new runtime name should be published after default rename"
        );
        assert_eq!(
            instance.database_registry().default_database_id(),
            Some(1),
            "default pointer should stay anchored on the durable database id"
        );
        assert_eq!(
            instance
                .database_registry()
                .default_database_name()
                .as_deref(),
            Some("warehouse")
        );
    }

    let catalog = load_instance_catalog(dir.path());
    assert_eq!(catalog.default_database_id, Some(1));
    assert!(
        catalog.find_database_by_name("appdb").is_none(),
        "old default name should be removed from durable catalog"
    );
    assert!(
        catalog.find_database_by_name("warehouse").is_some(),
        "renamed default database should persist in durable catalog"
    );

    let restarted = open_instance(dir.path());
    assert_eq!(
        restarted.database_registry().default_database_id(),
        Some(1),
        "restart should preserve durable default database id"
    );
    assert_eq!(
        restarted
            .database_registry()
            .default_database_name()
            .as_deref(),
        Some("warehouse")
    );
    assert!(
        restarted
            .database_registry()
            .get_database("warehouse")
            .is_some(),
        "renamed default database should recover with the new name"
    );
}

#[test]
fn create_database_keeps_provisioning_record_when_cleanup_fails() {
    let dir = tempdir().expect("tempdir");
    let failed_storage_dir = dir.path().join("databases").join("db-2");

    let instance = open_instance(dir.path());
    std::fs::create_dir_all(failed_storage_dir.parent().expect("db parent"))
        .expect("create managed db parent");
    std::fs::write(&failed_storage_dir, b"block create_new path").expect("inject conflicting file");

    let error = instance
        .create_database("analytics")
        .expect_err("create database should fail when managed path is blocked");
    assert!(
        error
            .to_string()
            .contains("cleanup for provisioning database"),
        "create error should surface cleanup failure"
    );
    assert!(
        instance
            .database_registry()
            .get_database("analytics")
            .is_none(),
        "failed create should not publish runtime database"
    );

    let catalog = load_instance_catalog(dir.path());
    let analytics = catalog
        .find_database_by_name("analytics")
        .expect("failed create should keep provisioning record");
    assert_eq!(analytics.state, DatabaseRecordState::Provisioning);
    assert_eq!(PathBuf::from(&analytics.storage_dir), failed_storage_dir);
    assert!(
        analytics
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("cleanup for provisioning database"),
        "provisioning record should retain cleanup failure"
    );
}

#[test]
fn drop_database_keeps_dropping_record_when_cleanup_fails() {
    let dir = tempdir().expect("tempdir");
    let cleanup_blocker = dir.path().join("drop-blocker");

    let instance = open_instance(dir.path());
    instance
        .create_database("drop_me")
        .expect("create database should succeed");
    std::fs::write(&cleanup_blocker, b"block remove_dir_all").expect("create cleanup blocker");

    let mut catalog = load_instance_catalog(dir.path());
    let drop_me = catalog
        .find_database_mut_by_name("drop_me")
        .expect("drop_me record should exist");
    drop_me.storage_dir = cleanup_blocker.to_string_lossy().to_string();
    save_instance_catalog(dir.path(), &mut catalog);

    let error = instance
        .drop_database("drop_me")
        .expect_err("drop should fail when cleanup cannot remove storage path");
    assert!(
        error
            .to_string()
            .contains("Failed to remove database storage directory"),
        "drop error should surface cleanup failure"
    );
    assert!(
        instance
            .database_registry()
            .get_database("drop_me")
            .is_none(),
        "dropping database should be unpublished from runtime registry"
    );

    let catalog = load_instance_catalog(dir.path());
    let drop_me = catalog
        .find_database_by_name("drop_me")
        .expect("failed drop should keep dropping record");
    assert_eq!(drop_me.state, DatabaseRecordState::Dropping);
    assert_eq!(PathBuf::from(&drop_me.storage_dir), cleanup_blocker);
    assert!(
        drop_me
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("Failed to remove database storage directory"),
        "dropping record should retain cleanup failure"
    );

    drop(instance);
    let restarted = open_instance(dir.path());
    assert!(
        restarted
            .database_registry()
            .get_database("drop_me")
            .is_none(),
        "dropping record should stay out of runtime registry after restart"
    );
}

#[test]
fn recovery_rolls_back_provisioning_records_without_republishing() {
    let dir = tempdir().expect("tempdir");

    let provisioning_dir = {
        let instance = open_instance(dir.path());
        let analytics = instance
            .create_database("analytics")
            .expect("create database should succeed");
        let provisioning_dir = PathBuf::from(analytics.path());

        let mut catalog = load_instance_catalog(dir.path());
        let record = catalog
            .find_database_mut_by_id(analytics.id())
            .expect("analytics record should exist");
        record.state = DatabaseRecordState::Provisioning;
        record.last_error = Some("simulated interrupted create".to_string());

        save_instance_catalog(dir.path(), &mut catalog);
        provisioning_dir
    };

    let restarted = open_instance(dir.path());
    assert!(
        restarted
            .database_registry()
            .get_database("analytics")
            .is_none(),
        "provisioning database should not be republished on restart"
    );

    let catalog = load_instance_catalog(dir.path());
    assert!(
        catalog.find_database_by_name("analytics").is_none(),
        "provisioning record should be rolled back during recovery"
    );
    assert!(
        !provisioning_dir.exists(),
        "provisioning storage directory should be cleaned up during recovery"
    );
}

#[test]
fn recovery_rolls_back_provisioning_record_when_storage_dir_was_never_created() {
    let dir = tempdir().expect("tempdir");

    {
        let _instance = open_instance(dir.path());
        let mut catalog = load_instance_catalog(dir.path());
        let storage_dir =
            InstanceLayout::new(dir.path()).managed_database_dir(catalog.next_database_id);
        catalog
            .allocate_database(
                "analytics".to_string(),
                storage_dir.to_string_lossy().to_string(),
            )
            .expect("allocate provisioning record");
        save_instance_catalog(dir.path(), &mut catalog);
        assert!(
            !storage_dir.exists(),
            "simulated crash point should leave the managed directory absent"
        );
    }

    let restarted = open_instance(dir.path());
    assert!(
        restarted
            .database_registry()
            .get_database("analytics")
            .is_none(),
        "provisioning record should not be published when storage creation never started"
    );

    let catalog = load_instance_catalog(dir.path());
    assert!(
        catalog.find_database_by_name("analytics").is_none(),
        "startup should roll back a provisioning record even when the storage dir was never created"
    );
}

#[test]
fn ready_database_open_failure_marks_record_broken() {
    let dir = tempdir().expect("tempdir");

    let broken_dir = {
        let instance = open_instance(dir.path());
        let analytics = instance
            .create_database("analytics")
            .expect("create database should succeed");
        PathBuf::from(analytics.path())
    };

    std::fs::remove_dir_all(&broken_dir).expect("remove managed database directory");

    let config = InstanceConfig::new().with_instance_root(dir.path().to_string_lossy().to_string());
    let startup = Instance::new(config);
    assert!(
        startup.is_err(),
        "startup should fail when a ready database cannot be reopened"
    );

    let catalog = load_instance_catalog(dir.path());
    let analytics = catalog
        .find_database_by_name("analytics")
        .expect("analytics record should remain in catalog");
    assert_eq!(analytics.state, DatabaseRecordState::Broken);
    assert!(
        analytics
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("Storage directory does not exist"),
        "broken record should retain the reopen failure"
    );
}

#[test]
fn repair_policy_continues_startup_when_non_default_database_is_broken() {
    let dir = tempdir().expect("tempdir");

    let broken_dir = {
        let instance = open_instance(dir.path());
        let analytics = instance
            .create_database("analytics")
            .expect("create database should succeed");
        PathBuf::from(analytics.path())
    };

    std::fs::remove_dir_all(&broken_dir).expect("remove managed database directory");

    let config = InstanceConfig::new()
        .with_instance_root(dir.path().to_string_lossy().to_string())
        .with_startup_policy(StartupPolicy::Repair);
    let restarted = Instance::new(config).expect("repair policy should keep startup alive");

    assert!(
        restarted
            .database_registry()
            .get_database("postgres")
            .is_some(),
        "default database should still be recovered"
    );
    assert!(
        restarted
            .database_registry()
            .get_database("analytics")
            .is_none(),
        "broken non-default database should stay unpublished under repair policy"
    );

    let startup_report = restarted.startup_report();
    let postgres = startup_report
        .databases
        .iter()
        .find(|entry| entry.name == "postgres")
        .expect("startup report should include default database");
    assert_eq!(postgres.status, DatabaseStartupStatus::Recovered);

    let analytics = startup_report
        .databases
        .iter()
        .find(|entry| entry.name == "analytics")
        .expect("startup report should include failed database");
    assert_eq!(analytics.status, DatabaseStartupStatus::Failed);
    assert_eq!(analytics.durable_state, DatabaseRecordState::Broken);
    assert!(
        analytics
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("Storage directory does not exist"),
        "startup report should retain the recovery failure detail"
    );
}

#[test]
fn repair_policy_reports_orphan_managed_directory_without_reviving_it() {
    let dir = tempdir().expect("tempdir");

    let orphan_storage_dir = {
        let instance = open_instance(dir.path());
        let analytics = instance
            .create_database("analytics")
            .expect("create database should succeed");
        let storage_dir = PathBuf::from(analytics.path());

        let mut catalog = load_instance_catalog(dir.path());
        catalog.remove_database_by_id(analytics.id());
        save_instance_catalog(dir.path(), &mut catalog);
        storage_dir
    };

    let restarted = Instance::new(
        InstanceConfig::new()
            .with_instance_root(dir.path().to_string_lossy().to_string())
            .with_startup_policy(StartupPolicy::Repair),
    )
    .expect("repair policy should tolerate orphan directory");

    assert!(
        restarted
            .database_registry()
            .get_database("analytics")
            .is_none(),
        "orphan storage directory should not be revived into runtime registry"
    );

    let report = restarted.startup_report();
    let orphan_issue = report
        .issues
        .iter()
        .find(|issue| issue.kind == StartupIssueKind::OrphanDirectory)
        .expect("startup report should include orphan directory");
    assert_eq!(
        orphan_issue.path.as_deref(),
        Some(orphan_storage_dir.to_string_lossy().as_ref())
    );
    assert!(
        orphan_issue
            .detail
            .contains("storage identity database_id=2"),
        "orphan detail should surface the durable storage identity"
    );
}

#[test]
fn repair_policy_marks_identity_mismatch_broken_and_reports_issue() {
    let dir = tempdir().expect("tempdir");

    let analytics_dir = {
        let instance = open_instance(dir.path());
        let analytics = instance
            .create_database("analytics")
            .expect("create database should succeed");
        PathBuf::from(analytics.path())
    };

    let mut identity = load_storage_identity(&analytics_dir);
    identity.database_id = 999;
    save_storage_identity(&analytics_dir, &identity);

    let restarted = Instance::new(
        InstanceConfig::new()
            .with_instance_root(dir.path().to_string_lossy().to_string())
            .with_startup_policy(StartupPolicy::Repair),
    )
    .expect("repair policy should continue startup on identity mismatch");

    assert!(
        restarted
            .database_registry()
            .get_database("postgres")
            .is_some(),
        "default database should still come online"
    );
    assert!(
        restarted
            .database_registry()
            .get_database("analytics")
            .is_none(),
        "identity mismatch database should stay unpublished"
    );

    let catalog = load_instance_catalog(dir.path());
    let analytics = catalog
        .find_database_by_name("analytics")
        .expect("analytics record should remain in durable catalog");
    assert_eq!(analytics.state, DatabaseRecordState::Broken);
    assert!(
        analytics
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("Storage identity mismatch"),
        "broken record should preserve the identity mismatch detail"
    );

    let report = restarted.startup_report();
    let identity_issue = report
        .issues
        .iter()
        .find(|issue| issue.kind == StartupIssueKind::StorageIdentityMismatch)
        .expect("startup report should include storage identity mismatch");
    assert_eq!(identity_issue.database_id, Some(2));
    assert_eq!(identity_issue.name.as_deref(), Some("analytics"));
    assert_eq!(
        identity_issue.path.as_deref(),
        Some(analytics_dir.to_string_lossy().as_ref())
    );
    assert!(
        identity_issue.detail.contains("expects database_id 2")
            && identity_issue.detail.contains("belongs to database_id 999"),
        "startup issue should explain the mismatch clearly"
    );
}

#[test]
fn fresh_bootstrap_creates_default_database_once_and_ignores_later_default_name_changes() {
    let dir = tempdir().expect("tempdir");
    let bootstrap_config = InstanceConfig::new()
        .with_instance_root(dir.path().to_string_lossy().to_string())
        .with_default_database("appdb");

    {
        let instance = open_instance_with_config(bootstrap_config);
        assert!(
            instance.database_registry().get_database("appdb").is_some(),
            "fresh bootstrap should honor config.default_database"
        );
        assert!(
            instance
                .database_registry()
                .get_database("postgres")
                .is_none(),
            "fresh bootstrap should not create an extra postgres database"
        );
        assert_eq!(
            instance
                .database_registry()
                .default_database_name()
                .as_deref(),
            Some("appdb")
        );
    }

    let catalog = load_instance_catalog(dir.path());
    assert_eq!(catalog.databases.len(), 1);
    assert_eq!(catalog.default_database_id, Some(1));
    assert_eq!(catalog.next_database_id, 2);
    assert_eq!(catalog.databases[0].name, "appdb");
    assert!(
        !dir.path().join("bootstrap.json").exists(),
        "bootstrap should not depend on bootstrap.json"
    );

    let restarted = open_instance_with_config(
        InstanceConfig::new()
            .with_instance_root(dir.path().to_string_lossy().to_string())
            .with_default_database("ignored_on_restart"),
    );
    assert!(
        restarted
            .database_registry()
            .get_database("ignored_on_restart")
            .is_none(),
        "subsequent startups should not recreate the default database with a new name"
    );
    assert!(
        restarted
            .database_registry()
            .get_database("appdb")
            .is_some(),
        "durable default database should survive restart"
    );
    assert_eq!(
        restarted
            .database_registry()
            .default_database_name()
            .as_deref(),
        Some("appdb")
    );

    let catalog = load_instance_catalog(dir.path());
    assert_eq!(catalog.databases.len(), 1);
    assert_eq!(catalog.default_database_id, Some(1));
    assert_eq!(catalog.next_database_id, 2);
}

#[test]
fn restart_recovers_all_ready_databases_serially() {
    let dir = tempdir().expect("tempdir");

    {
        let instance = open_instance(dir.path());
        instance
            .create_database("analytics")
            .expect("create analytics");
        instance
            .create_database("warehouse")
            .expect("create warehouse");
    }

    let restarted = open_instance(dir.path());
    let report = restarted.startup_report();

    assert!(
        restarted
            .database_registry()
            .get_database("postgres")
            .is_some(),
        "default database should recover"
    );
    assert!(
        restarted
            .database_registry()
            .get_database("analytics")
            .is_some(),
        "analytics should recover"
    );
    assert!(
        restarted
            .database_registry()
            .get_database("warehouse")
            .is_some(),
        "warehouse should recover"
    );
    assert_eq!(
        restarted
            .database_registry()
            .default_database_name()
            .as_deref(),
        Some("postgres")
    );

    let recovered_names = report
        .databases
        .iter()
        .filter(|entry| entry.status == DatabaseStartupStatus::Recovered)
        .map(|entry| entry.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(recovered_names, vec!["postgres", "analytics", "warehouse"]);
}

#[test]
fn restart_retries_and_finishes_dropping_records() {
    let dir = tempdir().expect("tempdir");

    let drop_storage_dir = {
        let instance = open_instance(dir.path());
        let drop_me = instance
            .create_database("drop_me")
            .expect("create database should succeed");
        let storage_dir = PathBuf::from(drop_me.path());

        let mut catalog = load_instance_catalog(dir.path());
        let record = catalog
            .find_database_mut_by_id(drop_me.id())
            .expect("drop_me record should exist");
        record.state = DatabaseRecordState::Dropping;
        record.last_error = Some("simulated crash during drop".to_string());
        save_instance_catalog(dir.path(), &mut catalog);
        storage_dir
    };

    assert!(
        drop_storage_dir.exists(),
        "dropping storage dir should still exist before recovery retry"
    );

    let restarted = open_instance(dir.path());
    assert!(
        restarted
            .database_registry()
            .get_database("drop_me")
            .is_none(),
        "dropping database should not be republished during retry"
    );
    assert!(
        !drop_storage_dir.exists(),
        "startup retry should remove the interrupted dropping directory"
    );

    let catalog = load_instance_catalog(dir.path());
    assert!(
        catalog.find_database_by_name("drop_me").is_none(),
        "startup retry should remove the dropping catalog record"
    );
}

#[test]
fn restart_finishes_dropping_record_when_storage_dir_was_already_removed() {
    let dir = tempdir().expect("tempdir");

    {
        let instance = open_instance(dir.path());
        let drop_me = instance
            .create_database("drop_me")
            .expect("create database should succeed");
        let drop_storage_dir = PathBuf::from(drop_me.path());

        let mut catalog = load_instance_catalog(dir.path());
        let record = catalog
            .find_database_mut_by_id(drop_me.id())
            .expect("drop_me record should exist");
        record.state = DatabaseRecordState::Dropping;
        record.last_error = Some("simulated crash after storage removal".to_string());
        save_instance_catalog(dir.path(), &mut catalog);

        std::fs::remove_dir_all(&drop_storage_dir).expect("remove managed storage dir");
        assert!(
            !drop_storage_dir.exists(),
            "simulated crash point should leave the dropping directory absent"
        );
    }

    let restarted = open_instance(dir.path());
    assert!(
        restarted
            .database_registry()
            .get_database("drop_me")
            .is_none(),
        "dropping database should stay unpublished when only catalog cleanup is left"
    );

    let catalog = load_instance_catalog(dir.path());
    assert!(
        catalog.find_database_by_name("drop_me").is_none(),
        "startup should clear a dropping record once the storage dir is already gone"
    );
}

#[test]
fn dropping_cleanup_failure_is_retried_on_next_startup_after_unblock() {
    let dir = tempdir().expect("tempdir");
    let cleanup_blocker = dir.path().join("drop-blocker");

    let instance = open_instance(dir.path());
    instance
        .create_database("drop_me")
        .expect("create database should succeed");
    std::fs::write(&cleanup_blocker, b"block remove_dir_all").expect("create cleanup blocker");

    let mut catalog = load_instance_catalog(dir.path());
    let drop_me = catalog
        .find_database_mut_by_name("drop_me")
        .expect("drop_me record should exist");
    drop_me.storage_dir = cleanup_blocker.to_string_lossy().to_string();
    save_instance_catalog(dir.path(), &mut catalog);

    instance
        .drop_database("drop_me")
        .expect_err("drop should fail when cleanup cannot remove storage path");
    std::fs::remove_file(&cleanup_blocker).expect("remove cleanup blocker");

    drop(instance);
    let restarted = open_instance(dir.path());
    assert!(
        restarted
            .database_registry()
            .get_database("drop_me")
            .is_none(),
        "dropping database should stay unpublished after retry"
    );

    let catalog = load_instance_catalog(dir.path());
    assert!(
        catalog.find_database_by_name("drop_me").is_none(),
        "startup retry should clear dropping record once cleanup is unblocked"
    );
}

#[test]
fn persistent_instance_enforces_single_owner_lock() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());

    assert!(
        owner_lock_path(dir.path()).exists(),
        "persistent instance should materialize an owner lock file"
    );

    let err = match Instance::new(
        InstanceConfig::new().with_instance_root(dir.path().to_string_lossy().to_string()),
    ) {
        Ok(_) => panic!("second owner should be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("not yet accepting connections")
            || err.to_string().contains("already owned"),
        "owner lock error should explain the contention"
    );

    drop(instance);

    let reopened = open_instance(dir.path());
    assert!(
        reopened
            .database_registry()
            .get_database("postgres")
            .is_some(),
        "instance root should be reusable after the first owner exits"
    );
    assert!(
        owner_lock_path(dir.path()).exists(),
        "owner lock file should remain as the reusable advisory lock target"
    );
}

#[test]
fn persistent_instance_persists_running_run_state_json() {
    let dir = tempdir().expect("tempdir");

    let _instance = open_instance(dir.path());

    assert!(
        run_state_path(dir.path()).exists(),
        "persistent startup should materialize run_state.json"
    );

    let run_state = load_instance_run_state(dir.path());
    assert_eq!(run_state.state, InstanceLifecycleState::Running);
    assert_ne!(run_state.boot_id, 0, "boot id should be initialized");
}

#[test]
fn clean_restart_uses_conservative_fast_path_and_still_recovers_ready_databases() {
    let dir = tempdir().expect("tempdir");

    {
        let instance = open_instance(dir.path());
        instance
            .create_database("analytics")
            .expect("create database should succeed");
        let proof = instance
            .verify_quiesced_for_clean_shutdown()
            .expect("fresh instance should already be quiesced");
        instance
            .shutdown_clean(InstanceShutdownMode::TryCheckpoint, proof)
            .expect("clean shutdown should succeed");
    }

    let restarted = open_instance(dir.path());
    let report = restarted.startup_report();
    assert_eq!(
        report.disposition,
        InstanceStartupDisposition::CleanFastPath
    );
    assert!(
        restarted
            .database_registry()
            .get_database("postgres")
            .is_some(),
        "default database should still reopen on the clean fast path"
    );
    assert!(
        restarted
            .database_registry()
            .get_database("analytics")
            .is_some(),
        "managed ready databases should still reopen through open_existing"
    );
    assert!(
        report
            .databases
            .iter()
            .filter(|entry| entry.status == DatabaseStartupStatus::Recovered)
            .any(|entry| entry.name == "analytics"),
        "clean fast path should still drive the normal per-database recovery publish path"
    );
}

#[test]
fn startup_fails_when_starting_run_state_cannot_be_persisted() {
    let dir = tempdir().expect("tempdir");
    arm_metadata_rename_failure_for_path_on_nth_call(run_state_path(dir.path()), 1);

    let err = match Instance::new(
        InstanceConfig::new().with_instance_root(dir.path().to_string_lossy().to_string()),
    ) {
        Ok(_) => panic!("startup should fail when Starting cannot be persisted"),
        Err(err) => err,
    };

    assert!(
        err.to_string()
            .contains("Failed to persist instance run state Starting"),
        "startup error should surface the failed Starting write"
    );
    assert!(
        !run_state_path(dir.path()).exists(),
        "rename failure before commit must not leave a run_state file behind"
    );
}

#[test]
fn startup_overwrites_previous_clean_state_with_starting_before_recovery_failure() {
    let dir = tempdir().expect("tempdir");

    {
        let instance = open_instance(dir.path());
        instance
            .create_database("analytics")
            .expect("create database should succeed");
        let proof = instance
            .verify_quiesced_for_clean_shutdown()
            .expect("fresh instance should already be quiesced");
        instance
            .shutdown_clean(InstanceShutdownMode::TryCheckpoint, proof)
            .expect("clean shutdown should succeed");
    }

    let clean_state = load_instance_run_state(dir.path());
    assert_eq!(clean_state.state, InstanceLifecycleState::Clean);

    let err = match Instance::new(
        InstanceConfig::new()
            .with_instance_root(dir.path().to_string_lossy().to_string())
            .with_recovery_hooks(vec![Arc::new(FailingMarkerRecoveryHook {
                target_database: "analytics",
            })]),
    ) {
        Ok(_) => panic!("recovery hook failure should abort startup after Starting is persisted"),
        Err(err) => err,
    };

    assert!(
        err.to_string().contains("simulated hook crash"),
        "startup should surface the recovery hook failure"
    );

    let run_state = load_instance_run_state(dir.path());
    assert_eq!(
        run_state.state,
        InstanceLifecycleState::Starting,
        "startup must overwrite the previous Clean state before recovery work begins"
    );
    assert_ne!(
        run_state.boot_id, clean_state.boot_id,
        "failed restart should still publish a new boot id with Starting"
    );

    let restarted = open_instance(dir.path());
    assert_eq!(
        restarted.startup_report().disposition,
        InstanceStartupDisposition::FullRecovery,
        "a crash while the durable run state is Starting must fall back to full recovery"
    );
}

#[test]
fn shutdown_dirty_keeps_run_state_dirty() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());

    let report = instance
        .shutdown_dirty(InstanceShutdownMode::TryCheckpoint)
        .expect("dirty shutdown should succeed");
    assert_eq!(report.disposition, InstanceShutdownDisposition::Dirty);
    assert!(!report.clean_shutdown_persisted);
    assert!(
        instance.is_invalidated(),
        "dirty shutdown should invalidate the instance after completion"
    );

    drop(instance);

    let run_state = load_instance_run_state(dir.path());
    assert_eq!(run_state.state, InstanceLifecycleState::ShuttingDown);
    assert_eq!(run_state.last_clean_shutdown_ms, None);
    assert_eq!(run_state.last_clean_database_count, None);

    let restarted = open_instance(dir.path());
    assert_eq!(
        restarted.startup_report().disposition,
        InstanceStartupDisposition::FullRecovery,
        "a restart after ShuttingDown must not take the clean fast path"
    );
}

#[test]
fn shutdown_clean_persists_clean_run_state_summary() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());

    let proof = instance
        .verify_quiesced_for_clean_shutdown()
        .expect("tracked work should already be drained");
    let report = instance
        .shutdown_clean(InstanceShutdownMode::TryCheckpoint, proof)
        .expect("clean shutdown should succeed");
    assert_eq!(report.disposition, InstanceShutdownDisposition::Clean);
    assert!(report.clean_shutdown_persisted);
    assert_eq!(report.databases_failed, 0);
    assert!(
        instance.is_invalidated(),
        "clean shutdown should invalidate the instance after completion"
    );
    assert!(
        instance.create_database("after_shutdown").is_err(),
        "terminal shutdown state should reject new DDL"
    );

    drop(instance);

    let run_state = load_instance_run_state(dir.path());
    assert_eq!(run_state.state, InstanceLifecycleState::Clean);
    assert_eq!(run_state.last_clean_database_count, Some(1));
    assert_eq!(run_state.last_clean_default_database_id, Some(1));
    assert!(run_state.last_clean_shutdown_ms.is_some());
}

#[test]
fn shutdown_clean_returns_error_and_keeps_run_state_dirty_when_checkpoint_close_fails() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());

    let proof = instance
        .verify_quiesced_for_clean_shutdown()
        .expect("tracked work should already be drained");

    let postgres_storage_dir = PathBuf::from(default_db(&instance).path());
    arm_manifest_rename_failure_for_path_on_nth_call(
        checkpoint_current_path(&postgres_storage_dir),
        1,
    );
    let err = instance
        .shutdown_clean(InstanceShutdownMode::Checkpoint, proof)
        .expect_err("checkpoint close failure should make shutdown return an error");

    assert!(
        err.data()
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("databases_failed=1"),
        "shutdown error detail should include the failed managed database count"
    );
    assert!(
        instance.is_invalidated(),
        "failed shutdown should still invalidate the instance once shutdown completes"
    );
    assert!(
        instance.create_database("after_failed_shutdown").is_err(),
        "failed shutdown should still leave the instance in terminal state"
    );

    drop(instance);

    let run_state = load_instance_run_state(dir.path());
    assert_eq!(
        run_state.state,
        InstanceLifecycleState::ShuttingDown,
        "checkpoint failure must leave the instance run state dirty"
    );
    assert_eq!(run_state.last_clean_shutdown_ms, None);
}

#[test]
fn shutdown_dirty_returns_error_when_dirty_run_state_persist_fails() {
    let dir = tempdir().expect("tempdir");
    let instance = open_instance(dir.path());

    arm_metadata_parent_sync_failure_for_path_on_nth_call(run_state_path(dir.path()), 1);
    let err = instance
        .shutdown_dirty(InstanceShutdownMode::TryCheckpoint)
        .expect_err("dirty run-state persist failure should be surfaced to the caller");

    assert!(
        err.data()
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("persist dirty run state"),
        "shutdown error detail should include the dirty run-state persist failure"
    );
    assert!(
        instance.is_invalidated(),
        "shutdown failure should still invalidate the instance once shutdown completes"
    );

    drop(instance);

    let run_state = load_instance_run_state(dir.path());
    assert_eq!(
        run_state.state,
        InstanceLifecycleState::ShuttingDown,
        "a post-rename fsync failure still leaves the durable run state dirty"
    );
    assert_eq!(run_state.last_clean_shutdown_ms, None);
}

#[test]
fn restart_tolerates_corrupted_run_state_payloads_and_falls_back_to_full_recovery() {
    for overwrite_payload in [b"{ not valid json".to_vec(), {
        let dir = tempdir().expect("tempdir");
        let _instance = open_instance(dir.path());
        let mut run_state = load_instance_run_state(dir.path());
        run_state.format_version = INSTANCE_RUN_STATE_FORMAT_VERSION.saturating_add(1);
        serde_json::to_vec_pretty(&run_state).expect("serialize unsupported run_state")
    }] {
        let dir = tempdir().expect("tempdir");

        {
            let instance = open_instance(dir.path());
            let proof = instance
                .verify_quiesced_for_clean_shutdown()
                .expect("fresh instance should already be quiesced");
            instance
                .shutdown_clean(InstanceShutdownMode::TryCheckpoint, proof)
                .expect("clean shutdown should succeed");
        }

        std::fs::write(run_state_path(dir.path()), overwrite_payload).expect("overwrite run_state");

        let restarted = open_instance(dir.path());
        assert_eq!(
            restarted.startup_report().disposition,
            InstanceStartupDisposition::FullRecovery,
            "corrupted run_state payloads must degrade to full recovery instead of aborting startup"
        );

        let repaired_state = load_instance_run_state(dir.path());
        assert_eq!(repaired_state.state, InstanceLifecycleState::Running);
        assert_eq!(
            repaired_state.format_version, INSTANCE_RUN_STATE_FORMAT_VERSION,
            "successful startup should overwrite the corrupted run_state payload"
        );
    }
}

#[test]
fn clean_run_state_with_transient_catalog_record_forces_full_recovery() {
    let dir = tempdir().expect("tempdir");

    {
        let instance = open_instance(dir.path());
        let analytics = instance
            .create_database("analytics")
            .expect("create database should succeed");
        let proof = instance
            .verify_quiesced_for_clean_shutdown()
            .expect("fresh instance should already be quiesced");
        instance
            .shutdown_clean(InstanceShutdownMode::TryCheckpoint, proof)
            .expect("clean shutdown should succeed");

        let mut catalog = load_instance_catalog(dir.path());
        let analytics = catalog
            .find_database_mut_by_id(analytics.id())
            .expect("analytics record should exist");
        analytics.state = DatabaseRecordState::Provisioning;
        analytics.last_error = Some("simulated transient state after clean shutdown".to_string());
        save_instance_catalog(dir.path(), &mut catalog);
    }

    let restarted = open_instance(dir.path());
    assert_eq!(
        restarted.startup_report().disposition,
        InstanceStartupDisposition::FullRecovery,
        "transient durable catalog state must override a stale Clean run_state"
    );
    assert!(
        restarted
            .database_registry()
            .get_database("analytics")
            .is_none(),
        "provisioning database should still be rolled back during full recovery"
    );

    let report = restarted.startup_report();
    let invariant_issue = report
        .issues
        .iter()
        .find(|issue| issue.kind == StartupIssueKind::CleanStateInvariantViolation)
        .expect("startup report should record the clean-state invariant violation");
    assert!(
        invariant_issue.detail.contains("analytics(Provisioning)"),
        "startup issue should name the transient record that forced the fallback"
    );

    let reconciled_entry = report
        .databases
        .into_iter()
        .find(|entry| entry.name == "analytics")
        .expect("startup report should describe the reconciled transient database");
    assert_eq!(reconciled_entry.status, DatabaseStartupStatus::Reconciled);
    assert_eq!(
        reconciled_entry.durable_state,
        DatabaseRecordState::Provisioning
    );
}

#[test]
fn clean_fast_path_keeps_orphan_scan_for_repair_policies() {
    for policy in [StartupPolicy::Repair, StartupPolicy::BestEffort] {
        let dir = tempdir().expect("tempdir");

        let orphan_storage_dir = {
            let instance = open_instance(dir.path());
            let analytics = instance
                .create_database("analytics")
                .expect("create database should succeed");
            let storage_dir = PathBuf::from(analytics.path());
            let proof = instance
                .verify_quiesced_for_clean_shutdown()
                .expect("fresh instance should already be quiesced");
            instance
                .shutdown_clean(InstanceShutdownMode::TryCheckpoint, proof)
                .expect("clean shutdown should succeed");

            let mut catalog = load_instance_catalog(dir.path());
            catalog.remove_database_by_id(analytics.id());
            save_instance_catalog(dir.path(), &mut catalog);
            storage_dir
        };

        let restarted = open_instance_with_config(
            InstanceConfig::new()
                .with_instance_root(dir.path().to_string_lossy().to_string())
                .with_startup_policy(policy),
        );
        assert_eq!(
            restarted.startup_report().disposition,
            InstanceStartupDisposition::CleanFastPath,
            "repair policies should still be eligible for the conservative clean fast path"
        );
        let orphan_issue = restarted
            .startup_report()
            .issues
            .into_iter()
            .find(|issue| issue.kind == StartupIssueKind::OrphanDirectory)
            .expect("repair policy should preserve orphan scan even on a clean restart");
        assert_eq!(
            orphan_issue.path.as_deref(),
            Some(orphan_storage_dir.to_string_lossy().as_ref())
        );
    }
}

#[test]
fn repair_policy_reports_failed_recovery_hook_results() {
    let dir = tempdir().expect("tempdir");

    let analytics_dir = {
        let instance = open_instance(dir.path());
        let analytics = instance
            .create_database("analytics")
            .expect("create database should succeed");
        PathBuf::from(analytics.path())
    };

    let restarted = open_instance_with_config(
        InstanceConfig::new()
            .with_instance_root(dir.path().to_string_lossy().to_string())
            .with_startup_policy(StartupPolicy::Repair)
            .with_recovery_hooks(vec![Arc::new(FailingMarkerRecoveryHook {
                target_database: "analytics",
            })]),
    );

    assert!(
        restarted
            .database_registry()
            .get_database("postgres")
            .is_some(),
        "default database should still recover under repair policy"
    );
    assert!(
        restarted
            .database_registry()
            .get_database("analytics")
            .is_none(),
        "hook failure database should stay unpublished"
    );

    let analytics_entry = restarted
        .startup_report()
        .databases
        .into_iter()
        .find(|entry| entry.name == "analytics")
        .expect("startup report should include analytics");
    assert_eq!(analytics_entry.status, DatabaseStartupStatus::Failed);
    let hook_results = analytics_entry
        .recovery_report
        .as_ref()
        .expect("failed hook should retain a partial recovery report")
        .hook_results
        .clone();
    assert_eq!(hook_results.len(), 1);
    match &hook_results[0] {
        RecoveryHookResult::Failed { error, .. } => {
            assert!(
                error.contains("failing_marker"),
                "hook failure should preserve the hook name"
            );
        }
        other => panic!("expected hook failure result, got {:?}", other),
    }

    let report = restarted.startup_report();
    let hook_issue = report
        .issues
        .iter()
        .find(|issue| issue.kind == StartupIssueKind::RecoveryHookFailure)
        .expect("startup report should record recovery hook failure");
    assert_eq!(hook_issue.database_id, Some(2));
    assert_eq!(hook_issue.name.as_deref(), Some("analytics"));
    assert_eq!(
        hook_issue.path.as_deref(),
        Some(analytics_dir.to_string_lossy().as_ref())
    );
    assert!(
        hook_issue.detail.contains("failing_marker"),
        "hook failure issue should include the failing hook name"
    );
    assert_eq!(
        std::fs::read_to_string(hook_marker_path(&analytics_dir)).expect("read hook marker"),
        "partial"
    );
}

#[test]
fn recovery_hook_reexecution_is_safe_after_partial_artifact_is_left_behind() {
    let dir = tempdir().expect("tempdir");

    let analytics_dir = {
        let instance = open_instance(dir.path());
        let analytics = instance
            .create_database("analytics")
            .expect("create database should succeed");
        PathBuf::from(analytics.path())
    };

    let first_restart = open_instance_with_config(
        InstanceConfig::new()
            .with_instance_root(dir.path().to_string_lossy().to_string())
            .with_startup_policy(StartupPolicy::Repair)
            .with_recovery_hooks(vec![Arc::new(FailingMarkerRecoveryHook {
                target_database: "analytics",
            })]),
    );
    assert!(
        first_restart
            .database_registry()
            .get_database("analytics")
            .is_none(),
        "first restart should fail analytics hook recovery"
    );
    drop(first_restart);

    let mut catalog = load_instance_catalog(dir.path());
    let analytics = catalog
        .find_database_mut_by_name("analytics")
        .expect("analytics record should exist");
    analytics.state = DatabaseRecordState::Ready;
    analytics.last_error = None;
    save_instance_catalog(dir.path(), &mut catalog);

    let repaired_restart = open_instance_with_config(
        InstanceConfig::new()
            .with_instance_root(dir.path().to_string_lossy().to_string())
            .with_recovery_hooks(vec![Arc::new(IdempotentMarkerRecoveryHook {
                target_database: "analytics",
            })]),
    );
    assert!(
        repaired_restart
            .database_registry()
            .get_database("analytics")
            .is_some(),
        "retry startup should recover analytics after normalizing the partial hook artifact"
    );
    assert_eq!(
        std::fs::read_to_string(hook_marker_path(&analytics_dir)).expect("read hook marker"),
        "ready"
    );

    let analytics_entry = repaired_restart
        .startup_report()
        .databases
        .into_iter()
        .find(|entry| entry.name == "analytics")
        .expect("startup report should include analytics");
    assert_eq!(analytics_entry.status, DatabaseStartupStatus::Recovered);
    match analytics_entry
        .recovery_report
        .as_ref()
        .expect("analytics should have recovery report")
        .hook_results
        .as_slice()
    {
        [RecoveryHookResult::Rebuilt { detail, .. }] => {
            assert!(
                detail
                    .as_deref()
                    .unwrap_or_default()
                    .contains("normalized recovery hook marker"),
                "retry should report that it normalized the partial hook artifact"
            );
        }
        other => panic!("expected rebuilt hook result, got {:?}", other),
    }
    drop(repaired_restart);

    let final_restart = open_instance_with_config(
        InstanceConfig::new()
            .with_instance_root(dir.path().to_string_lossy().to_string())
            .with_recovery_hooks(vec![Arc::new(IdempotentMarkerRecoveryHook {
                target_database: "analytics",
            })]),
    );
    let analytics_entry = final_restart
        .startup_report()
        .databases
        .into_iter()
        .find(|entry| entry.name == "analytics")
        .expect("startup report should include analytics");
    assert_eq!(
        analytics_entry
            .recovery_report
            .as_ref()
            .expect("analytics should have recovery report")
            .hook_results,
        vec![RecoveryHookResult::Reused],
        "once the hook artifact is normalized, subsequent startups should reuse it safely"
    );
}
