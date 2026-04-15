// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bridges storage-level WAL replay with catalog and runtime recovery.

use super::consistency_report::{
    build_recovery_consistency_report, log_recovery_consistency_report,
};
use super::index_restore::{reconcile_fulltext_index_coverage, restore_runtime_art_indexes};
use paro_catalog::collection::{CatalogReplaySummary, InstallMode, StagedCatalogMutation};
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogEntryEnum, CreateSchemaInfo, OnCreateConflict};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_catalog::mvcc::REPLAY_WRITER_ID;
use paro_common::effect::{CatalogTxnOp, PostCommitHookDescriptor, PreparedDataOp};
use paro_common::error as paro_error;
use paro_common::logging::targets;
use paro_storage::meta::TabletMetaManager;
use paro_storage::transaction::descriptor_cleanup::DescriptorCleanupQueue;
use paro_storage::wal::recovery::{ReplayHandler, WalRecovery};
use paro_storage::wal::replay_state::ReplayResult;
use paro_storage::wal::wal_entry::WalHeaderMetadata;
use paro_storage::wal::write_ahead_log::WriteAheadLog;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Replay handler that applies WAL entries to a Catalog.
///
/// This handler is used during database startup to replay WAL entries
/// and restore the catalog to a consistent state.
pub struct CatalogReplayHandler<'a> {
    /// The catalog to apply entries to
    pub(super) catalog: &'a Arc<ParoCatalog>,
    /// Transaction for replay operations
    pub(super) transaction: CatalogSnapshot,
    /// Database root used for staged-artifact publish and cleanup descriptors.
    pub(super) database_root: PathBuf,
    /// Persistent tablet metadata state used to hide shutdown tablets from startup manifest.
    pub(super) tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    /// Highest object id observed in replayed WAL create payloads.
    pub(super) max_seen_object_id: u64,
    /// Highest committed catalog timestamp installed during replay.
    pub(super) max_catalog_commit_id: u64,
}

impl<'a> CatalogReplayHandler<'a> {
    /// Create a new catalog replay handler.
    pub fn new(catalog: &'a Arc<ParoCatalog>, txn_id: u64, commit_ts: u64) -> Self {
        let transaction = if txn_id >= REPLAY_WRITER_ID {
            CatalogSnapshot::writer(txn_id, commit_ts)
        } else {
            CatalogSnapshot::replay_writer(commit_ts)
        };
        Self {
            catalog,
            transaction,
            database_root: PathBuf::new(),
            tablet_meta_manager: None,
            max_seen_object_id: 0,
            max_catalog_commit_id: 0,
        }
    }

    pub fn with_database_root(mut self, database_root: PathBuf) -> Self {
        self.database_root = database_root;
        self
    }

    pub fn with_tablet_meta_manager(
        mut self,
        tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    ) -> Self {
        self.tablet_meta_manager = tablet_meta_manager;
        self
    }

    pub(super) fn observe_object_id(&mut self, object_id: u64) {
        self.max_seen_object_id = self.max_seen_object_id.max(object_id);
    }

    pub(super) fn observe_catalog_commit_id(
        &mut self,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        if commit_id == 0 || commit_id >= paro_storage::transaction::manager::TRANSACTION_ID_START {
            return Err(paro_error::serialization_error(format!(
                "replayed catalog commit timestamp must be in committed range, got {}",
                commit_id
            )));
        }
        self.max_catalog_commit_id = self.max_catalog_commit_id.max(commit_id);
        Ok(())
    }

    pub(super) fn install_replayed_entry(
        &mut self,
        collection: &paro_catalog::collection::CatalogCollection,
        commit_id: u64,
        entry: Arc<CatalogEntryEnum>,
        mode: InstallMode,
    ) -> paro_common::error::Result<()> {
        collection.install_replayed(commit_id, entry, mode)?;
        self.observe_catalog_commit_id(commit_id)
    }

    pub(super) fn publish_catalog_handle(
        &mut self,
        handle: StagedCatalogMutation,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        handle.publish(commit_id)?;
        self.observe_catalog_commit_id(commit_id)
    }

    pub fn summary(&self) -> CatalogReplaySummary {
        CatalogReplaySummary {
            max_catalog_commit_id: self.max_catalog_commit_id,
            max_seen_object_id: self.max_seen_object_id,
        }
    }

    fn finalize_object_id_allocator(&self) -> paro_common::error::Result<()> {
        if self.max_seen_object_id == 0 {
            return Ok(());
        }
        let next_object_id = self.max_seen_object_id.checked_add(1).ok_or_else(|| {
            paro_error::serialization_error(format!(
                "replayed object id {} overflowed allocator watermark",
                self.max_seen_object_id
            ))
        })?;
        self.catalog.bump_object_id_allocator(next_object_id);
        Ok(())
    }

    pub(super) fn ensure_schema(
        &mut self,
        schema_name: &str,
        commit_id: u64,
    ) -> paro_common::error::Result<Arc<paro_catalog::entry::SchemaEntry>> {
        match self.catalog.get_schema(&self.transaction, schema_name) {
            Ok(schema) => Ok(schema),
            Err(_) => {
                let info = CreateSchemaInfo {
                    catalog: self.catalog.name().to_string(),
                    name: schema_name.to_string(),
                    internal: false,
                    on_conflict: OnCreateConflict::IgnoreOnConflict,
                };
                let entry = Arc::new(CatalogEntryEnum::Schema(Arc::new(
                    paro_catalog::entry::SchemaEntry::from_info(
                        &info,
                        self.catalog.gc_epoch_handle(),
                        0,
                    ),
                )));
                self.install_replayed_entry(
                    self.catalog.get_schema_collection(),
                    commit_id,
                    entry,
                    InstallMode::RejectExisting,
                )?;
                self.catalog.get_schema(&self.transaction, schema_name)
            }
        }
    }
}

impl<'a> ReplayHandler for CatalogReplayHandler<'a> {
    fn replay_transaction(
        &mut self,
        catalog_ops: &[CatalogTxnOp],
        data_ops: &[PreparedDataOp],
        post_commit_hooks: &[PostCommitHookDescriptor],
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        self.replay_catalog_ops_in_commit_order(catalog_ops, commit_id)?;
        for op in data_ops {
            match op {
                PreparedDataOp::RowsetCommit {
                    locator,
                    start_version,
                    end_version,
                } => {
                    let rowset_path = locator.path_components.join("/");
                    self.replay_rowset_commit(
                        locator.tablet_id,
                        locator.rowset_id,
                        *start_version,
                        *end_version,
                        &rowset_path,
                    )?;
                }
                PreparedDataOp::PrimaryDelete { keys, .. } => {
                    self.replay_primary_delete(keys)?;
                }
                PreparedDataOp::RowIdDelete { locations, .. } => {
                    self.replay_row_id_delete(locations)?;
                }
            }
        }
        self.replay_catalog_drop_ops(catalog_ops, commit_id)?;
        self.replay_runtime_transitions(catalog_ops, commit_id)?;
        self.replay_post_commit_hooks(post_commit_hooks, commit_id)?;
        let mut cleanup_queue = DescriptorCleanupQueue::default();
        for op in catalog_ops {
            cleanup_queue.enqueue(commit_id, op.cleanups.clone());
        }
        for batch in cleanup_queue.drain() {
            for cleanup in &batch.descriptors {
                self.apply_cleanup_descriptor(cleanup)?;
            }
        }
        Ok(())
    }

    fn on_checkpoint(&mut self, checkpoint_marker: u64) -> paro_common::error::Result<()> {
        tracing::info!(
            target: targets::INSTANCE,
            checkpoint_marker = checkpoint_marker,
            "Checkpoint marker found during replay"
        );
        Ok(())
    }
}

/// Recover a database from its WAL.
///
/// This function:
/// 1. Opens the WAL file for the database
/// 2. Replays all entries to restore the catalog
/// 3. Returns the WAL for continued use
///
/// # Arguments
/// * `wal_path` - Path to the WAL file
/// * `catalog` - The catalog to restore
///
/// # Returns
/// * `Ok((wal, result, summary))` - Recovery completed successfully
/// * `Err(...)` - Fatal error during recovery
pub fn recover_database(
    wal_path: &Path,
    catalog: &Arc<ParoCatalog>,
    tablet_meta_manager: Option<Arc<TabletMetaManager>>,
) -> paro_common::error::Result<(WriteAheadLog, ReplayResult, CatalogReplaySummary)> {
    let recovery = WalRecovery::new(wal_path);

    // Use a dedicated replay writer identity and a maximally-open snapshot so
    // replay can stage mutations while still honoring committed visibility
    // boundaries when publishing.
    let mut handler = CatalogReplayHandler::new(catalog, 0, u64::MAX)
        .with_database_root(
            wal_path
                .parent()
                .unwrap_or_else(|| Path::new(""))
                .to_path_buf(),
        )
        .with_tablet_meta_manager(tablet_meta_manager);
    let recovered = recovery.recover(&mut handler)?;
    let summary = handler.summary();
    handler.finalize_object_id_allocator()?;
    catalog.rebuild_dependency_graph()?;
    restore_runtime_art_indexes(catalog);
    reconcile_fulltext_index_coverage(catalog);
    let report = build_recovery_consistency_report(catalog);
    log_recovery_consistency_report(&report);
    Ok((recovered.0, recovered.1, summary))
}

/// Recover a database from its WAL with checkpoint coordination.
///
/// This function:
/// 1. Checks if checkpoint marker matches WAL checkpoint marker
/// 2. If they match, skips WAL replay (checkpoint was successful)
/// 3. Otherwise, replays WAL entries to restore the catalog
///
/// # Arguments
/// * `wal_path` - Path to the WAL file
/// * `catalog` - The catalog to restore
/// * `checkpoint_marker` - Optional checkpoint marker from metadata store
///
/// # Returns
/// * `Ok((wal, result))` - Recovery completed successfully
/// * `Err(...)` - Fatal error during recovery
pub fn recover_database_with_checkpoint(
    wal_path: &Path,
    catalog: &Arc<ParoCatalog>,
    tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    checkpoint_marker: Option<u64>,
    wal_header_metadata: Option<WalHeaderMetadata>,
    wal_keep_from: Option<u64>,
) -> paro_common::error::Result<(WriteAheadLog, ReplayResult, CatalogReplaySummary)> {
    let mut recovery = WalRecovery::new(wal_path);

    // If we have a checkpoint marker, use it for verification.
    if let Some(marker) = checkpoint_marker {
        recovery = recovery.with_checkpoint_marker(marker);
    }

    if let Some(metadata) = wal_header_metadata {
        recovery = recovery
            .with_wal_header_metadata(metadata.db_identifier, metadata.checkpoint_iteration);
    }

    if let Some(keep_from) = wal_keep_from {
        recovery = recovery.with_wal_keep_from(keep_from);
    }

    // Use a dedicated replay writer identity and a maximally-open snapshot.
    let mut handler = CatalogReplayHandler::new(catalog, 0, u64::MAX)
        .with_database_root(
            wal_path
                .parent()
                .unwrap_or_else(|| Path::new(""))
                .to_path_buf(),
        )
        .with_tablet_meta_manager(tablet_meta_manager);
    let recovered = recovery.recover(&mut handler)?;
    let summary = handler.summary();
    handler.finalize_object_id_allocator()?;
    catalog.rebuild_dependency_graph()?;
    restore_runtime_art_indexes(catalog);
    reconcile_fulltext_index_coverage(catalog);
    let report = build_recovery_consistency_report(catalog);
    log_recovery_consistency_report(&report);
    Ok((recovered.0, recovered.1, summary))
}

/// Check if a WAL file exists and needs recovery.
pub fn needs_recovery(wal_path: &Path) -> bool {
    let report = paro_storage::wal::recovery::wal_health_check_read_only(wal_path);

    // Recover whenever any WAL stream exists so startup can consume checkpoint/recovery
    // artifacts and clean up stale files, even when main WAL is empty or absent.
    if report.main_wal.exists && report.main_wal.size_bytes > 0 {
        return true;
    }
    if report.checkpoint_wal.exists || report.recovery_wal.exists {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_catalog::catalog::Catalog;
    use paro_catalog::collection::InstallMode;
    use paro_catalog::database_catalog::ParoCatalog;
    use paro_catalog::entry::CatalogObjectId;
    use paro_catalog::entry::{
        CatalogEntryEnum, CatalogType, ColumnDefinition, CreateIndexInfo, CreateSchemaInfo,
        CreateSequenceInfo, CreateTableInfo, IndexBuildState, IndexCatalogEntry, IndexType,
        LogicalIndex, OnCreateConflict, SequenceCatalogEntry, TableCatalogEntry,
    };
    use paro_catalog::mvcc::CatalogSnapshot;
    use paro_common::chunk::Chunk;
    use paro_common::ddl::{
        CreateIndexPayload, CreatePropertyGraphPayload, CreateSchemaPayload, CreateSequencePayload,
        CreateTablePayload, CreateViewPayload, DdlChange, DdlChangeRecord, DdlDependencyObjectRef,
        DdlDependencyRef, DdlObjectKey, DdlObjectKind, DdlStorageDescriptor, DdlWalColumnInfo,
        PropertyGraphVertexPayload,
    };
    use paro_common::effect::CatalogTxnOp;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_storage::table::table_factory::TableFactory;
    use paro_storage::table::table_handle::TableHandle;
    use paro_storage::wal::wal_entry::{ColumnInfo, WalEntry};
    use paro_storage::wal::wal_type::WalType;
    use paro_storage::wal::wal_writer::{WalInitState, WalWriter};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn create_table(types: &[LogicalType]) -> TableHandle {
        TableFactory::default().create_table(types).unwrap()
    }

    fn find_first_segment_dir(root: &Path) -> Option<PathBuf> {
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = fs::read_dir(&dir) else {
                continue;
            };

            let mut has_segment = false;
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|ext| ext.to_str()) == Some("dat") {
                    has_segment = true;
                }
            }

            if has_segment {
                return Some(dir);
            }
        }
        None
    }

    fn ensure_main_schema(catalog: &Arc<ParoCatalog>) {
        let info = CreateSchemaInfo {
            catalog: catalog.name().to_string(),
            name: "main".to_string(),
            internal: false,
            on_conflict: OnCreateConflict::IgnoreOnConflict,
        };
        let entry = Arc::new(CatalogEntryEnum::Schema(Arc::new(
            paro_catalog::entry::SchemaEntry::from_info(&info, catalog.gc_epoch_handle(), 0),
        )));
        catalog
            .get_schema_collection()
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();
    }

    fn install_committed_table(
        catalog: &Arc<ParoCatalog>,
        schema_name: &str,
        table_name: &str,
        columns: Vec<ColumnDefinition>,
        storage: Arc<TableHandle>,
    ) {
        let info = CreateTableInfo::new(
            catalog.name().to_string(),
            schema_name.to_string(),
            table_name.to_string(),
            columns,
        );
        let entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
            TableCatalogEntry::from_info(info, storage, 0),
        )));
        let schema = catalog
            .get_schema(&CatalogSnapshot::read_only(u64::MAX), schema_name)
            .unwrap();
        schema
            .collection(CatalogType::Table)
            .expect("table collection")
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();
    }

    fn write_flushed_catalog_txn(
        writer: &WalWriter,
        txn_id: u64,
        commit_id: u64,
        changes: Vec<DdlChangeRecord>,
    ) {
        let begin = WalEntry::TxnBegin {
            txn_id,
            start_time: 0,
        };
        writer
            .write_entry(WalType::TxnBegin, &begin.serialize_data())
            .unwrap();
        for (seq, change) in changes.into_iter().enumerate() {
            let op = CatalogTxnOp {
                change,
                staged_artifacts: vec![],
                runtime_transitions: vec![],
                cleanups: vec![],
            };
            let entry = WalEntry::TxnCatalogOp {
                seq: seq as u32,
                op,
            };
            writer
                .write_entry(WalType::TxnCatalogOp, &entry.serialize_data())
                .unwrap();
        }
        let commit = WalEntry::TxnCommit { txn_id, commit_id };
        writer
            .write_entry(WalType::TxnCommit, &commit.serialize_data())
            .unwrap();
        writer.flush().unwrap();
    }

    #[test]
    fn test_catalog_replay_create_schema() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);

        let payload = CreateSchemaPayload {
            object_id: 42,
            if_not_exists: false,
        };

        handler
            .replay_create_schema("test_schema", &payload, 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "test_schema").unwrap();
        assert_eq!(schema.base.object_id.raw(), 42);
    }

    #[test]
    fn test_catalog_replay_create_table() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);

        let columns = [
            ColumnInfo::new("id".to_string(), LogicalType::Integer, false),
            ColumnInfo::new("name".to_string(), LogicalType::Varchar, true),
        ];
        let seed_storage = create_table(&[LogicalType::Integer, LogicalType::Varchar]);
        let descriptor = seed_storage.to_descriptor().unwrap();
        let payload = CreateTablePayload {
            object_id: 99,
            columns: columns
                .iter()
                .map(|column| DdlWalColumnInfo {
                    name: column.name.clone(),
                    logical_type: column.logical_type.clone(),
                    nullable: column.nullable,
                })
                .collect(),
            constraints: Vec::new(),
            if_not_exists: false,
            storage: Some(DdlStorageDescriptor {
                format_version: descriptor.format_version,
                tablet_id: descriptor.tablet_id,
                table_id: descriptor.table_id,
                partition_id: descriptor.partition_id,
                schema_id: descriptor.schema_id,
                schema_version: descriptor.schema_version,
                schema_hash: descriptor.schema_hash,
                data_dir: descriptor.data_dir.clone(),
                keys_type: descriptor.keys_type,
            }),
        };

        handler
            .replay_create_table("main", "users", &payload, 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let table = catalog.get_table(&txn, "main", "users").unwrap();
        let CatalogEntryEnum::Table(table) = table.as_ref() else {
            panic!("expected table entry");
        };
        assert_eq!(table.get_storage_descriptor(), Some(&descriptor));
        assert_eq!(table.base.base.object_id.raw(), 99);
    }

    #[test]
    fn test_catalog_replay_create_index_metadata_marks_art_ready() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "users", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        let payload = CreateIndexPayload {
            object_id: 42,
            table_name: "users".to_string(),
            column_ids: vec![0],
            column_types: vec![LogicalType::Integer],
            index_type: "ART".to_string(),
            is_unique: false,
            if_not_exists: false,
            fulltext_config: None,
        };
        handler
            .replay_create_index("main", "idx_users_id", &payload, 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let entry = schema.get_index(0, u64::MAX, "idx_users_id").unwrap();
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(index.build_state(), IndexBuildState::Ready);
        assert_eq!(index.base.base.object_id.raw(), 42);
        assert_eq!(index.failure_reason(), None);
    }

    #[test]
    fn test_catalog_replay_create_index_metadata_only_ready() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "users", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        let payload = CreateIndexPayload {
            object_id: 43,
            table_name: "users".to_string(),
            column_ids: vec![0],
            column_types: vec![LogicalType::Integer],
            index_type: "HNSW".to_string(),
            is_unique: false,
            if_not_exists: false,
            fulltext_config: None,
        };
        handler
            .replay_create_index("main", "idx_users_hnsw", &payload, 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let entry = schema.get_index(0, u64::MAX, "idx_users_hnsw").unwrap();
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(index.build_state(), IndexBuildState::Ready);
        assert_eq!(index.base.base.object_id.raw(), 43);
        assert_eq!(index.failure_reason(), None);
    }

    #[test]
    fn test_reconcile_fulltext_index_coverage_marks_failed_on_incomplete() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Varchar]));
        let columns = vec![ColumnDefinition::new(
            "content".to_string(),
            LogicalType::Varchar,
        )];
        install_committed_table(&catalog, "main", "docs", columns, Arc::clone(&storage));

        let insert = Chunk::from_vectors(vec![Vector::from_strings(&["vector db"])]);
        storage.append(&insert).unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let table_entry = schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .expect("docs table should exist");
        let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
            panic!("expected table entry");
        };
        let info = CreateIndexInfo::new(
            "main".to_string(),
            "docs".to_string(),
            "idx_docs_fts".to_string(),
            vec![LogicalIndex::new(0)],
            vec![LogicalType::Varchar],
        )
        .with_index_type(IndexType::FullText)
        .with_fulltext_options(LogicalIndex::new(0), "simple")
        .with_build_state(IndexBuildState::Building);
        let entry = Arc::new(CatalogEntryEnum::Index(Arc::new(IndexCatalogEntry::new(
            info,
            table.base.base.object_id.raw(),
            0,
            catalog.name().to_string(),
        ))));
        schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        reconcile_fulltext_index_coverage(&catalog);

        let entry = schema
            .get_index(txn.transaction_id, txn.start_time, "idx_docs_fts")
            .expect("index should exist");
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(index.build_state(), IndexBuildState::Failed);
        assert!(
            index
                .failure_reason()
                .unwrap_or_default()
                .contains("coverage incomplete"),
            "unexpected failure reason: {:?}",
            index.failure_reason()
        );
    }

    #[test]
    fn test_reconcile_fulltext_index_coverage_marks_ready_when_complete() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Varchar]));
        let columns = vec![ColumnDefinition::new(
            "content".to_string(),
            LogicalType::Varchar,
        )];
        install_committed_table(&catalog, "main", "docs", columns, Arc::clone(&storage));

        let insert = Chunk::from_vectors(vec![Vector::from_strings(&["vector db"])]);
        storage.append(&insert).unwrap();
        storage.build_runtime_fulltext_index(0).unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let table_entry = schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .expect("docs table should exist");
        let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
            panic!("expected table entry");
        };
        let info = CreateIndexInfo::new(
            "main".to_string(),
            "docs".to_string(),
            "idx_docs_fts".to_string(),
            vec![LogicalIndex::new(0)],
            vec![LogicalType::Varchar],
        )
        .with_index_type(IndexType::FullText)
        .with_fulltext_options(LogicalIndex::new(0), "simple")
        .with_build_state(IndexBuildState::Building);
        let entry = Arc::new(CatalogEntryEnum::Index(Arc::new(IndexCatalogEntry::new(
            info,
            table.base.base.object_id.raw(),
            0,
            catalog.name().to_string(),
        ))));
        schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        reconcile_fulltext_index_coverage(&catalog);

        let entry = schema
            .get_index(txn.transaction_id, txn.start_time, "idx_docs_fts")
            .expect("index should exist");
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(index.build_state(), IndexBuildState::Ready);
        let coverage = index.coverage().expect("coverage should be populated");
        assert!(coverage.is_complete());
        assert!(storage.has_fulltext_index_with_config(0, "simple"));
    }

    #[test]
    fn test_restore_runtime_art_indexes_marks_ready_when_complete() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "users", columns, Arc::clone(&storage));

        let insert = Chunk::from_vectors(vec![Vector::from_i32(&[1, 2, 3])]);
        storage.append(&insert).unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let table_entry = schema
            .get_table(txn.transaction_id, txn.start_time, "users")
            .expect("users table should exist");
        let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
            panic!("expected table entry");
        };
        let info = CreateIndexInfo::new(
            "main".to_string(),
            "users".to_string(),
            "idx_users_art".to_string(),
            vec![LogicalIndex::new(0)],
            vec![LogicalType::Integer],
        )
        .with_index_type(IndexType::ART)
        .with_build_state(IndexBuildState::Building);
        let entry = Arc::new(CatalogEntryEnum::Index(Arc::new(IndexCatalogEntry::new(
            info,
            table.base.base.object_id.raw(),
            0,
            catalog.name().to_string(),
        ))));
        schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        restore_runtime_art_indexes(&catalog);

        let entry = schema
            .get_index(txn.transaction_id, txn.start_time, "idx_users_art")
            .expect("index should exist");
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(index.build_state(), IndexBuildState::Ready);
        let coverage = index.coverage().expect("coverage should be populated");
        assert!(coverage.is_complete());
        assert_eq!(storage.tablet().declared_art_columns(), vec![0]);
        assert!(storage
            .collect_segments(storage.max_version())
            .unwrap()
            .iter()
            .all(|(_, segment)| segment.art_index(0).is_some()));

        let report = build_recovery_consistency_report(&catalog);
        assert!(
            report.all_consistent,
            "report should be consistent: {report:?}"
        );
    }

    #[test]
    fn test_restore_runtime_art_indexes_marks_failed_on_missing_column() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "users", columns, Arc::clone(&storage));

        let insert = Chunk::from_vectors(vec![Vector::from_i32(&[1, 2, 3])]);
        storage.append(&insert).unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let table_entry = schema
            .get_table(txn.transaction_id, txn.start_time, "users")
            .expect("users table should exist");
        let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
            panic!("expected table entry");
        };
        let info = CreateIndexInfo::new(
            "main".to_string(),
            "users".to_string(),
            "idx_users_art_missing".to_string(),
            vec![LogicalIndex::new(99)],
            vec![LogicalType::Integer],
        )
        .with_index_type(IndexType::ART)
        .with_build_state(IndexBuildState::Building);
        let entry = Arc::new(CatalogEntryEnum::Index(Arc::new(IndexCatalogEntry::new(
            info,
            table.base.base.object_id.raw(),
            0,
            catalog.name().to_string(),
        ))));
        schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        restore_runtime_art_indexes(&catalog);

        let entry = schema
            .get_index(txn.transaction_id, txn.start_time, "idx_users_art_missing")
            .expect("index should exist");
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(index.build_state(), IndexBuildState::Failed);
        assert!(
            index
                .failure_reason()
                .unwrap_or_default()
                .contains("column 99"),
            "unexpected failure reason: {:?}",
            index.failure_reason()
        );
        assert!(storage.tablet().declared_art_columns().is_empty());
        assert!(storage
            .collect_segments(storage.max_version())
            .unwrap()
            .iter()
            .all(|(_, segment)| segment.art_index(99).is_none()));
    }

    #[test]
    fn test_catalog_replay_create_sequence_applies_payload() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);

        let payload = CreateSequencePayload {
            object_id: 123,
            if_not_exists: false,
            increment: 3,
            min_value: 5,
            max_value: 99,
            start_value: 7,
            cycle: true,
        };

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_create_sequence("main", "seq_replayed", &payload, 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let entry = schema
            .get_sequence(txn.transaction_id, txn.start_time, "seq_replayed")
            .expect("sequence should exist after replay");
        let CatalogEntryEnum::Sequence(sequence) = entry.as_ref() else {
            panic!("expected sequence entry");
        };
        let data = sequence.get_data();
        assert_eq!(sequence.base.base.object_id.raw(), 123);
        assert_eq!(data.start_value, 7);
        assert_eq!(data.increment, 3);
        assert_eq!(data.min_value, 5);
        assert_eq!(data.max_value, 99);
        assert!(data.cycle);
    }

    #[test]
    fn test_catalog_replay_drop_schema_is_idempotent() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);
        let entry = Arc::new(CatalogEntryEnum::Schema(Arc::new(
            paro_catalog::entry::SchemaEntry::from_info(
                &CreateSchemaInfo {
                    catalog: catalog.name().to_string(),
                    name: "drop_me".to_string(),
                    internal: false,
                    on_conflict: OnCreateConflict::IgnoreOnConflict,
                },
                catalog.gc_epoch_handle(),
                0,
            ),
        )));
        catalog
            .get_schema_collection()
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler.replay_drop_schema("drop_me", 42).unwrap();
        handler.replay_drop_schema("drop_me", 42).unwrap();

        assert!(catalog
            .get_schema(&CatalogSnapshot::read_only(u64::MAX), "drop_me")
            .is_err());
    }

    #[test]
    fn test_catalog_replay_drop_sequence_is_idempotent() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let schema = catalog
            .get_schema(&CatalogSnapshot::read_only(u64::MAX), "main")
            .unwrap();
        let entry = Arc::new(CatalogEntryEnum::Sequence(Arc::new(
            SequenceCatalogEntry::new(
                CreateSequenceInfo::new("main".to_string(), "seq_to_drop".to_string())
                    .with_catalog(catalog.name().to_string()),
                0,
                catalog.name().to_string(),
            )
            .unwrap(),
        )));
        schema
            .collection(CatalogType::Sequence)
            .expect("sequence collection")
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_drop_sequence("main", "seq_to_drop", 42)
            .unwrap();
        handler
            .replay_drop_sequence("main", "seq_to_drop", 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        assert!(schema
            .get_sequence(txn.transaction_id, txn.start_time, "seq_to_drop")
            .is_none());
    }

    #[test]
    fn test_catalog_replay_alter_entry_updates_table_comment() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_alter_entry("COMMENT ON TABLE main.docs IS 'replayed comment'", 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let entry = schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .expect("docs table should exist");
        let CatalogEntryEnum::Table(table) = entry.as_ref() else {
            panic!("expected table entry");
        };
        assert_eq!(
            table.base.base.comment(),
            Some("replayed comment".to_string())
        );
    }

    #[test]
    fn test_catalog_replay_alter_entry_updates_column_comment() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer, LogicalType::Varchar]));
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("note".to_string(), LogicalType::Varchar),
        ];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_alter_entry(
                "COMMENT ON COLUMN main.docs.note IS 'replayed column comment'",
                42,
            )
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let entry = schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .expect("docs table should exist");
        let CatalogEntryEnum::Table(table) = entry.as_ref() else {
            panic!("expected table entry");
        };
        assert_eq!(
            table
                .get_column("note")
                .and_then(|column| column.comment.clone()),
            Some("replayed column comment".to_string())
        );
    }

    #[test]
    fn test_catalog_replay_alter_entry_renames_table() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_alter_entry("ALTER TABLE main.docs RENAME TO docs_v2", 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        assert!(schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .is_none());
        let entry = schema
            .get_table(txn.transaction_id, txn.start_time, "docs_v2")
            .expect("renamed table should exist");
        assert_eq!(entry.name(), "docs_v2");
    }

    #[test]
    fn test_catalog_replay_rename_uses_commit_id_visibility_boundary() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let commit_id = 77;
        let mut handler = CatalogReplayHandler::new(&catalog, 0, commit_id);
        handler
            .replay_alter_entry("ALTER TABLE main.docs RENAME TO docs_v2", commit_id)
            .unwrap();

        let at_commit = CatalogSnapshot::read_only(commit_id);
        let schema_at_commit = catalog.get_schema(&at_commit, "main").unwrap();
        assert!(schema_at_commit
            .get_table(at_commit.transaction_id, at_commit.start_time, "docs_v2")
            .is_none());
        assert!(schema_at_commit
            .get_table(at_commit.transaction_id, at_commit.start_time, "docs")
            .is_some());

        let after_commit = CatalogSnapshot::read_only(commit_id + 1);
        let schema_after_commit = catalog.get_schema(&after_commit, "main").unwrap();
        assert!(schema_after_commit
            .get_table(after_commit.transaction_id, after_commit.start_time, "docs")
            .is_none());
        assert!(schema_after_commit
            .get_table(
                after_commit.transaction_id,
                after_commit.start_time,
                "docs_v2"
            )
            .is_some());
    }

    #[test]
    fn test_catalog_replay_rename_table_across_schema() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);
        let info = CreateSchemaInfo {
            catalog: catalog.name().to_string(),
            name: "archive".to_string(),
            internal: false,
            on_conflict: OnCreateConflict::IgnoreOnConflict,
        };
        let entry = Arc::new(CatalogEntryEnum::Schema(Arc::new(
            paro_catalog::entry::SchemaEntry::from_info(&info, catalog.gc_epoch_handle(), 0),
        )));
        catalog
            .get_schema_collection()
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_alter_entry("RENAME TABLE main.docs TO archive.docs_v2", 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let main_schema = catalog.get_schema(&txn, "main").unwrap();
        let archive_schema = catalog.get_schema(&txn, "archive").unwrap();
        assert!(main_schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .is_none());
        let entry = archive_schema
            .get_table(txn.transaction_id, txn.start_time, "docs_v2")
            .expect("moved table should exist");
        let CatalogEntryEnum::Table(table) = entry.as_ref() else {
            panic!("expected table entry");
        };
        assert_eq!(table.base.schema_name, "archive");
        assert_eq!(table.base.base.name, "docs_v2");
    }

    #[test]
    fn test_catalog_replay_rename_table_commit_timestamp_baseline() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let replay_writer_id = 7;
        let replay_commit_ts = 42;
        let mut handler = CatalogReplayHandler::new(&catalog, replay_writer_id, replay_commit_ts);
        handler
            .replay_alter_entry("ALTER TABLE main.docs RENAME TO docs_v2", replay_commit_ts)
            .unwrap();

        let snapshot_at_commit = CatalogSnapshot::read_only(replay_commit_ts);
        let schema = catalog.get_schema(&snapshot_at_commit, "main").unwrap();
        assert!(
            schema
                .get_table(
                    snapshot_at_commit.transaction_id,
                    snapshot_at_commit.start_time,
                    "docs_v2",
                )
                .is_none(),
            "replay rename became visible at commit_ts, which means replay writer id is still leaking into publish visibility"
        );
        assert!(schema
            .get_table(
                snapshot_at_commit.transaction_id,
                snapshot_at_commit.start_time,
                "docs",
            )
            .is_some());

        let snapshot_after_commit = CatalogSnapshot::read_only(replay_commit_ts + 1);
        let schema_after_commit = catalog.get_schema(&snapshot_after_commit, "main").unwrap();
        assert!(schema_after_commit
            .get_table(
                snapshot_after_commit.transaction_id,
                snapshot_after_commit.start_time,
                "docs_v2",
            )
            .is_some());
    }

    #[test]
    fn test_catalog_replay_alter_entry_renames_column() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_alter_entry("ALTER TABLE main.docs RENAME COLUMN id TO doc_id", 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let entry = schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .expect("docs table should exist");
        let CatalogEntryEnum::Table(table) = entry.as_ref() else {
            panic!("expected table entry");
        };
        assert_eq!(table.columns[0].name, "doc_id");
    }

    #[test]
    fn test_recover_database_no_wal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("nonexistent.wal");
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);

        let (wal, result, _summary) = recover_database(&wal_path, &catalog, None).unwrap();

        assert!(result.all_succeeded, "replay error: {:?}", result.error);
        assert_eq!(result.entries_replayed, 0);
        assert!(!wal.is_initialized());
    }

    #[test]
    fn test_recover_database_with_entries() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        let replayed_schema_oid = catalog.current_object_id().saturating_add(1_000);

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            paro_storage::wal::test_support::write_flushed_create_schema_txn_with_object_id(
                &writer,
                "test",
                "test_schema",
                replayed_schema_oid,
                1,
                100,
            )
            .unwrap();
        }

        let (_wal, result, _summary) = recover_database(&wal_path, &catalog, None).unwrap();

        assert!(result.all_succeeded, "replay error: {:?}", result.error);
        assert!(result.entries_replayed > 0);

        // Verify catalog was restored
        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "test_schema").unwrap();
        assert_eq!(schema.base.object_id.raw(), replayed_schema_oid);
        let replay_watermark = catalog.current_object_id();
        assert!(replay_watermark > replayed_schema_oid);

        let write_txn = CatalogSnapshot::permanent_writer(u64::MAX);
        catalog
            .create_schema_with_snapshot(&write_txn, "post_recovery_schema")
            .unwrap();
        let created = catalog.get_schema(&txn, "post_recovery_schema").unwrap();
        assert_eq!(created.base.object_id.raw(), replay_watermark);
    }

    #[test]
    fn test_recover_database_restores_schema_table_view_index_and_property_graph() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("combo.wal");
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);

        let seed_storage = create_table(&[LogicalType::Integer]);
        let descriptor = seed_storage.to_descriptor().unwrap();
        let schema_oid = 7_001;
        let table_oid = 7_002;
        let view_oid = 7_003;
        let index_oid = 7_004;
        let graph_oid = 7_005;

        let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
        write_flushed_catalog_txn(
            &writer,
            1,
            100,
            vec![
                DdlChangeRecord {
                    key: DdlObjectKey::new(
                        "test",
                        None::<String>,
                        "replay_combo",
                        DdlObjectKind::Schema,
                    ),
                    change: DdlChange::CreateSchema(CreateSchemaPayload {
                        object_id: schema_oid,
                        if_not_exists: false,
                    }),
                },
                DdlChangeRecord {
                    key: DdlObjectKey::new(
                        "test",
                        Some("replay_combo"),
                        "items",
                        DdlObjectKind::Table,
                    ),
                    change: DdlChange::CreateTable(CreateTablePayload {
                        object_id: table_oid,
                        columns: vec![DdlWalColumnInfo {
                            name: "id".to_string(),
                            logical_type: LogicalType::Integer,
                            nullable: false,
                        }],
                        constraints: Vec::new(),
                        if_not_exists: false,
                        storage: Some(DdlStorageDescriptor {
                            format_version: descriptor.format_version,
                            tablet_id: descriptor.tablet_id,
                            table_id: descriptor.table_id,
                            partition_id: descriptor.partition_id,
                            schema_id: descriptor.schema_id,
                            schema_version: descriptor.schema_version,
                            schema_hash: descriptor.schema_hash,
                            data_dir: descriptor.data_dir.clone(),
                            keys_type: descriptor.keys_type,
                        }),
                    }),
                },
                DdlChangeRecord {
                    key: DdlObjectKey::new(
                        "test",
                        Some("replay_combo"),
                        "items_view",
                        DdlObjectKind::View,
                    ),
                    change: DdlChange::CreateView(CreateViewPayload {
                        object_id: view_oid,
                        sql: "CREATE VIEW replay_combo.items_view AS SELECT id FROM replay_combo.items"
                            .to_string(),
                        column_aliases: vec![],
                        dependencies: vec![DdlDependencyRef {
                            object: DdlDependencyObjectRef {
                                object_id: table_oid,
                                kind: "TABLE".to_string(),
                                catalog_name: "test".to_string(),
                                schema_id: Some(schema_oid),
                                schema_name: Some("replay_combo".to_string()),
                                name: "items".to_string(),
                            },
                            dependency_type: "regular".to_string(),
                        }],
                        if_not_exists: false,
                    }),
                },
                DdlChangeRecord {
                    key: DdlObjectKey::new(
                        "test",
                        Some("replay_combo"),
                        "idx_items_id",
                        DdlObjectKind::Index,
                    ),
                    change: DdlChange::CreateIndex(CreateIndexPayload {
                        object_id: index_oid,
                        table_name: "items".to_string(),
                        column_ids: vec![0],
                        column_types: vec![LogicalType::Integer],
                        index_type: "ART".to_string(),
                        is_unique: false,
                        if_not_exists: false,
                        fulltext_config: None,
                    }),
                },
                DdlChangeRecord {
                    key: DdlObjectKey::new(
                        "test",
                        Some("replay_combo"),
                        "items_graph",
                        DdlObjectKind::PropertyGraph,
                    ),
                    change: DdlChange::CreatePropertyGraph(CreatePropertyGraphPayload {
                        object_id: graph_oid,
                        schema: "replay_combo".to_string(),
                        graph_name: "items_graph".to_string(),
                        if_not_exists: false,
                        vertex_tables: vec![PropertyGraphVertexPayload {
                            table_name: "items".to_string(),
                            table_oid,
                            key_column_ids: vec![0],
                            label: "Item".to_string(),
                            property_column_ids: vec![],
                        }],
                        edge_tables: vec![],
                    }),
                },
            ],
        );

        let (_wal, result, _summary) = recover_database(&wal_path, &catalog, None).unwrap();
        assert!(result.all_succeeded, "replay error: {:?}", result.error);
        assert!(result.entries_replayed >= 5);

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "replay_combo").unwrap();
        assert_eq!(schema.base.object_id.raw(), schema_oid);
        assert!(schema
            .get_table(txn.transaction_id, txn.start_time, "items")
            .is_some());
        assert!(schema
            .get_view(txn.transaction_id, txn.start_time, "items_view")
            .is_some());
        assert!(schema
            .get_index(txn.transaction_id, txn.start_time, "idx_items_id")
            .is_some());
        assert!(schema.get_property_graph(&txn, "items_graph").is_ok());

        let dependency_error = catalog
            .dependency_graph()
            .plan_drop(CatalogObjectId::from_raw(table_oid), false)
            .unwrap_err();
        assert!(dependency_error.to_string().contains("items_view"));
    }

    #[test]
    fn test_catalog_replay_finalize_allocator_tracks_dropped_objects() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        let replayed_object_id = catalog.current_object_id().saturating_add(1_000);

        let payload = CreateSchemaPayload {
            object_id: replayed_object_id,
            if_not_exists: false,
        };

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_create_schema("ephemeral_schema", &payload, 42)
            .unwrap();
        handler.replay_drop_schema("ephemeral_schema", 43).unwrap();
        handler.finalize_object_id_allocator().unwrap();

        let read_txn = CatalogSnapshot::read_only(u64::MAX);
        assert!(catalog.get_schema(&read_txn, "ephemeral_schema").is_err());
        let next_object_id = catalog.current_object_id();
        assert!(next_object_id > replayed_object_id);

        let write_txn = CatalogSnapshot::permanent_writer(u64::MAX);
        catalog
            .create_schema_with_snapshot(&write_txn, "after_drop_replay")
            .unwrap();
        let created = catalog.get_schema(&read_txn, "after_drop_replay").unwrap();
        assert_eq!(created.base.object_id.raw(), next_object_id);
    }

    #[test]
    fn test_catalog_replay_drop_table_idempotent() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);
        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "to_drop", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler.replay_drop_table("main", "to_drop", 42).unwrap();
        handler.replay_drop_table("main", "to_drop", 42).unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        assert!(schema.get_table(0, u64::MAX, "to_drop").is_none());
    }

    #[test]
    fn test_catalog_replay_rowset_commit_applies_when_table_mapped() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let target_storage = Arc::new(create_table(&[LogicalType::Integer]));
        let target_columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(
            &catalog,
            "main",
            "target_table",
            target_columns,
            Arc::clone(&target_storage),
        );
        assert_eq!(target_storage.rowset_count(), 0);

        let source_storage = create_table(&[LogicalType::Integer]);
        let source_chunk = Chunk::from_vectors(vec![Vector::from_i32(&[1, 2, 3])]);
        source_storage.append(&source_chunk).unwrap();

        let source_descriptor = source_storage.to_descriptor().unwrap();
        let rowset_dir = find_first_segment_dir(Path::new(&source_descriptor.data_dir))
            .expect("expected source rowset directory with segment files");

        let target_descriptor = target_storage.to_descriptor().unwrap();
        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_rowset_commit(
                target_descriptor.tablet_id,
                9_999,
                1,
                1,
                rowset_dir.to_string_lossy().as_ref(),
            )
            .unwrap();

        assert_eq!(target_storage.rowset_count(), 1);
        assert_eq!(target_storage.total_rows(), 3);

        // Rowset commit replay is idempotent for the same rowset_id.
        handler
            .replay_rowset_commit(
                target_descriptor.tablet_id,
                9_999,
                1,
                1,
                rowset_dir.to_string_lossy().as_ref(),
            )
            .unwrap();
        assert_eq!(target_storage.rowset_count(), 1);
        assert_eq!(target_storage.total_rows(), 3);
    }

    #[test]
    fn test_recovery_consistency_report_marks_healthy_table() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "users", columns, Arc::clone(&storage));

        let report = build_recovery_consistency_report(&catalog);
        assert!(report.all_consistent);
        assert!(report.schema_count >= 1);
        assert!(report.table_count >= 1);

        let table_report = report
            .tables
            .iter()
            .find(|entry| entry.schema_name == "main" && entry.table_name == "users")
            .expect("expected report entry for main.users");
        assert!(table_report.has_storage);
        assert!(table_report.version_graph_ok);
        assert!(table_report.primary_index_reconciled);
        assert!(table_report.errors.is_empty());
    }

    #[test]
    fn test_recovery_consistency_report_detects_catalog_runtime_index_mismatch() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "users", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        let payload = CreateIndexPayload {
            object_id: 77,
            table_name: "users".to_string(),
            column_ids: vec![0],
            column_types: vec![LogicalType::Integer],
            index_type: "ART".to_string(),
            is_unique: false,
            if_not_exists: false,
            fulltext_config: None,
        };
        handler
            .replay_create_index("main", "idx_users_id", &payload, 42)
            .unwrap();

        let report = build_recovery_consistency_report(&catalog);
        assert!(!report.all_consistent);

        let table_report = report
            .tables
            .iter()
            .find(|entry| entry.schema_name == "main" && entry.table_name == "users")
            .expect("expected report entry for main.users");
        assert_eq!(table_report.catalog_index_count, 1);
        assert_eq!(table_report.runtime_index_count, Some(0));
        assert!(table_report
            .errors
            .iter()
            .any(|error| error.contains("index count mismatch")));
    }

    #[test]
    fn test_needs_recovery() {
        let dir = tempdir().unwrap();

        // Non-existent file
        let path = dir.path().join("nonexistent.wal");
        assert!(!needs_recovery(&path));

        // Empty file
        let empty_path = dir.path().join("empty.wal");
        std::fs::write(&empty_path, &[]).unwrap();
        assert!(!needs_recovery(&empty_path));

        // File with content
        let content_path = dir.path().join("content.wal");
        std::fs::write(&content_path, b"some content").unwrap();
        assert!(needs_recovery(&content_path));

        // Checkpoint WAL without main WAL should still trigger recovery.
        let checkpoint_only_main = dir.path().join("checkpoint_only.wal");
        let checkpoint_only_cp = dir.path().join("checkpoint_only.checkpoint.wal");
        std::fs::write(&checkpoint_only_cp, b"checkpoint content").unwrap();
        assert!(needs_recovery(&checkpoint_only_main));

        // Recovery WAL artifact should also trigger recovery for cleanup.
        let recovery_only_main = dir.path().join("recovery_only.wal");
        let recovery_only_rc = dir.path().join("recovery_only.recovery.wal");
        std::fs::write(&recovery_only_rc, b"recovery content").unwrap();
        assert!(needs_recovery(&recovery_only_main));
    }
}
