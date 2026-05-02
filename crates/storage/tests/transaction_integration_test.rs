// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::test_utils::test_allocator;
use paro_common::types::LogicalType;
use paro_storage::meta::{FileMetadataStore, MetadataStore, TabletMetaManager};
use paro_storage::metrics::storage_metrics;
use paro_storage::primary_key::{DeleteVector, PrimaryKeySerializer};
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::{InsertOnConflictAction, TableHandle};
use paro_storage::tablet::{
    KeysType, Tablet, TabletColumn, TabletReader, TabletReaderParams, TabletRef, TabletSchema,
};
use paro_storage::transaction::manager::TransactionManager;
use paro_storage::transaction::participant::{
    StorageCommitParticipant, StorageCommittedRecordApplier,
};
use paro_storage::transaction::txn::Transaction;
use paro_storage::write::DeltaWriter;
use paro_transaction::{
    CommandId, CommitAckPolicy, CommitParticipant, CommitRequest, CommittedRecordApplier,
    DatabaseId, IsolationLevel, ParticipantStateSet, ReadSnapshot, ReadTrackerHandle, ReadTs,
    TransactionView,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

fn create_table(types: &[LogicalType]) -> TableHandle {
    TableFactory::default().create_table(types).unwrap()
}

fn create_pk_table() -> TableHandle {
    TableFactory::default()
        .create_table_with_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            KeysType::PrimaryKeys,
        )
        .unwrap()
}

fn create_test_meta_manager(temp_dir: &TempDir) -> Arc<TabletMetaManager> {
    let store: Arc<dyn MetadataStore> =
        Arc::new(FileMetadataStore::new(temp_dir.path().join("meta")).unwrap());
    Arc::new(TabletMetaManager::with_store_and_data_root(
        store,
        temp_dir.path(),
    ))
}

fn read_rows(table: &TableHandle, visible_version: u64) -> usize {
    let mut reader = table
        .create_reader(TabletReaderParams::with_version(visible_version as i64))
        .unwrap();
    reader.prepare().unwrap();

    let mut total = 0usize;
    while let Some(chunk) = reader.get_next_chunk().unwrap() {
        total += chunk.size();
    }
    total
}

fn view_for_txn(txn: &Transaction) -> TransactionView {
    TransactionView::autocommit(ReadTs::new(txn.visible_version()))
}

fn commit_transaction(txn: &Transaction, commit_id: u64) -> paro_common::error::Result<()> {
    let apply_result = txn.apply_prepared_storage_for_commit(commit_id);
    txn.release_transaction_locks();
    apply_result?;
    txn.finalize_applied_commit(commit_id)
}

fn commit_storage_transaction(manager: &Arc<TransactionManager>, txn: Arc<Transaction>) -> u64 {
    let database_id = DatabaseId::new(0);
    let participant = StorageCommitParticipant::new(database_id, Arc::clone(&txn));
    let read_ts = ReadTs::new(txn.visible_commit_ts().into_raw());
    let view = TransactionView::new(
        txn.writer_id(),
        read_ts,
        ReadSnapshot::without_lease(read_ts),
        IsolationLevel::Snapshot,
        CommandId::new(0),
        ReadTrackerHandle::noop(),
        ParticipantStateSet::from_vec(vec![txn.storage_participant_state()]),
    );
    let prepared = participant.prepare(&view).unwrap();
    let descriptor = CommitParticipant::descriptor(&participant, &prepared).unwrap();
    let request = CommitRequest::new(
        database_id,
        txn.txn_id(),
        view,
        CommitAckPolicy::RequiredPublished,
        txn.frozen_lock_set(),
        vec![descriptor.clone()],
    );
    let plan = request.commit_plan();
    let ctx = request.validation_context();
    participant.validate(&plan, &ctx).unwrap();

    let applier = StorageCommittedRecordApplier::new(Arc::clone(manager), Arc::clone(&txn));
    let commit_ts = manager.commit_sequencer().next_commit_ts();
    manager
        .commit_sequencer()
        .sync_next_commit_ts_with(commit_ts);
    manager
        .register_committed_write_set(commit_ts, &[])
        .unwrap();
    let record = request.committed_record(commit_ts);
    applier.apply_required(&record, &descriptor).unwrap();
    manager
        .commit_frontier()
        .sync_commit_ids(commit_ts, commit_ts);
    commit_ts.into_raw()
}

fn storage_view_for_txn_command(txn: &Transaction, command_id: u32) -> TransactionView {
    TransactionView::new(
        txn.writer_id(),
        txn.read_ts(),
        ReadSnapshot::without_lease(txn.read_ts()),
        IsolationLevel::Snapshot,
        CommandId::new(command_id),
        ReadTrackerHandle::noop(),
        ParticipantStateSet::from_vec(vec![txn.storage_participant_state()]),
    )
}

fn primary_key_bytes(table: &TableHandle, id: i32) -> Vec<u8> {
    let schema = table.tablet().schema().unwrap();
    let serializer = PrimaryKeySerializer::from_schema_ref(&schema).unwrap();
    serializer
        .encode_chunk(&chunk_with_pairs(&[id], &[0]))
        .unwrap()
        .pop()
        .unwrap()
}

fn create_pk_tablet() -> (TabletRef, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let schema = {
        let cols = vec![
            TabletColumn::key(0, "id", LogicalType::Integer),
            TabletColumn::new(1, "v", LogicalType::Integer),
        ];
        Arc::new(TabletSchema::new(1, cols, KeysType::PrimaryKeys).unwrap())
    };
    let tablet = Tablet::new(1, 10, 100, schema, temp_dir.path(), None).unwrap();
    tablet.init().unwrap();
    (Arc::new(tablet), temp_dir)
}

fn create_managed_pk_tablet() -> (TabletRef, TempDir, Arc<TabletMetaManager>) {
    let temp_dir = TempDir::new().unwrap();
    let manager = create_test_meta_manager(&temp_dir);
    let schema = {
        let cols = vec![
            TabletColumn::key(0, "id", LogicalType::Integer),
            TabletColumn::new(1, "v", LogicalType::Integer),
        ];
        Arc::new(TabletSchema::new(1, cols, KeysType::PrimaryKeys).unwrap())
    };
    let tablet = Tablet::new(1, 10, 100, schema, temp_dir.path(), Some(manager.clone())).unwrap();
    tablet.init().unwrap();
    (Arc::new(tablet), temp_dir, manager)
}

fn chunk_with_pairs(ids: &[i32], vals: &[i32]) -> Chunk {
    let allocator = test_allocator();
    let v0 = paro_common::test_utils::test_i32_vector_with_allocator(ids, allocator.clone());
    let v1 = paro_common::test_utils::test_i32_vector_with_allocator(vals, allocator.clone());
    paro_common::test_utils::test_chunk_from_vectors(vec![v0, v1])
}

fn chunk_with_i32(values: &[i32]) -> Chunk {
    let allocator = test_allocator();
    let vector = paro_common::test_utils::test_i32_vector_with_allocator(values, allocator.clone());
    paro_common::test_utils::test_chunk_from_vectors(vec![vector])
}

fn read_rows_from_tablet(tablet: &TabletRef, visible_version: i64) -> usize {
    let mut reader = TabletReader::new(
        tablet.clone(),
        TabletReaderParams::with_version(visible_version),
    )
    .unwrap();
    reader.prepare().unwrap();
    let mut total = 0usize;
    while let Some(chunk) = reader.get_next_chunk().unwrap() {
        total += chunk.size();
    }
    total
}

fn read_row_map_from_tablet(tablet: &TabletRef, visible_version: i64) -> BTreeMap<i32, i32> {
    let mut reader = TabletReader::new(
        tablet.clone(),
        TabletReaderParams::with_version(visible_version),
    )
    .unwrap();
    reader.prepare().unwrap();

    let mut rows = BTreeMap::new();
    while let Some(chunk) = reader.get_next_chunk().unwrap() {
        for row in 0..chunk.size() {
            let id = chunk.column(0).unwrap().get_i32(row).unwrap();
            let value = chunk.column(1).unwrap().get_i32(row).unwrap();
            rows.insert(id, value);
        }
    }
    rows
}

#[test]
fn test_transaction_visibility() {
    let types = vec![LogicalType::Integer];
    let table = create_table(&types);
    let tm = Arc::new(TransactionManager::new());

    let t1 = tm.begin_transaction().unwrap();
    let t2 = tm.begin_transaction().unwrap();

    let chunk = chunk_with_i32(&[1, 2, 3]);
    table
        .append_with_transaction(&view_for_txn(&t1), &chunk, t1.clone())
        .unwrap();

    // Uncommitted rowset should not be visible.
    let count = read_rows(&table, t2.visible_version());
    assert_eq!(count, 0);

    commit_storage_transaction(&tm, t1);

    // Snapshot fixed at start_time: still not visible to t2.
    let count = read_rows(&table, t2.visible_version());
    assert_eq!(count, 0);

    let t3 = tm.begin_transaction().unwrap();
    let count = read_rows(&table, t3.visible_version());
    assert_eq!(count, 3);

    tm.rollback_transaction(t2).unwrap();
}

#[test]
fn test_transaction_rollback_hides_rowset() {
    let types = vec![LogicalType::Integer];
    let table = create_table(&types);
    let tm = Arc::new(TransactionManager::new());

    let t1 = tm.begin_transaction().unwrap();
    let chunk = chunk_with_i32(&[10, 20]);
    table
        .append_with_transaction(&view_for_txn(&t1), &chunk, t1.clone())
        .unwrap();

    tm.rollback_transaction(t1).unwrap();

    let t2 = tm.begin_transaction().unwrap();
    let count = read_rows(&table, t2.visible_version());
    assert_eq!(count, 0);
}

#[test]
fn test_upsert() {
    let (tablet, _tmp) = create_pk_tablet();
    let tm = Arc::new(TransactionManager::new());

    let t1 = tm.begin_transaction().unwrap();
    let mut writer = DeltaWriter::open(tablet.clone(), t1.id).unwrap();
    writer
        .write_chunk(&chunk_with_pairs(&[1, 2, 3], &[10, 20, 30]))
        .unwrap();
    let _rowset1 = writer
        .commit_in_transaction(t1.clone(), view_for_txn(&t1).command_id())
        .unwrap();
    commit_storage_transaction(&tm, t1);
    assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 3);

    let t2 = tm.begin_transaction().unwrap();
    let mut writer2 = DeltaWriter::open(tablet.clone(), t2.id).unwrap();
    writer2
        .write_chunk(&chunk_with_pairs(&[2, 3, 4], &[200, 300, 400]))
        .unwrap();
    let _rowset2 = writer2
        .commit_in_transaction(t2.clone(), view_for_txn(&t2).command_id())
        .unwrap();
    commit_storage_transaction(&tm, t2);

    // Keys: 1,2,3,4 (3 updated)
    assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 4);
    let rows = read_row_map_from_tablet(&tablet, tablet.max_version());
    let expected: BTreeMap<i32, i32> = vec![(1, 10), (2, 200), (3, 300), (4, 400)]
        .into_iter()
        .collect();
    assert_eq!(rows, expected);
}

#[test]
fn test_primary_key_lookup_respects_read_timestamp() {
    let table = create_pk_table();
    table.append(&chunk_with_pairs(&[1], &[10])).unwrap();
    let key = primary_key_bytes(&table, 1);
    let v1 = table.max_version() as u64;
    let first_row_id = table
        .tablet()
        .lookup_primary_key_at(&key, v1)
        .unwrap()
        .unwrap();

    table.append(&chunk_with_pairs(&[1], &[20])).unwrap();
    let v2 = table.max_version() as u64;
    let latest_row_id = table.tablet().lookup_primary_key(&key).unwrap().unwrap();

    assert_ne!(first_row_id, latest_row_id);
    assert_eq!(
        table.tablet().lookup_primary_key_at(&key, v1).unwrap(),
        Some(first_row_id)
    );
    assert_eq!(
        table.tablet().lookup_primary_key_at(&key, v2).unwrap(),
        Some(latest_row_id)
    );
}

#[test]
fn test_transaction_primary_key_overlay_drives_own_conflict_lookup() {
    let table = create_pk_table();
    let txn = Arc::new(Transaction::new(70_001, table.max_version().max(0) as u64));

    let view0 = storage_view_for_txn_command(&txn, 0);
    table
        .append_with_transaction(&view0, &chunk_with_pairs(&[1], &[10]), txn.clone())
        .unwrap();
    txn.publish_command_boundary(CommandId::new(1));

    let view1 = storage_view_for_txn_command(&txn, 1);
    let affected = table
        .insert_on_conflict(
            &view1,
            &chunk_with_pairs(&[1], &[20]),
            &InsertOnConflictAction::DoNothing,
            txn.clone(),
        )
        .unwrap();
    assert_eq!(affected, 0);

    commit_transaction(&txn, 1).unwrap();
    let rows = read_row_map_from_tablet(&table.tablet(), table.max_version());
    assert_eq!(rows, BTreeMap::from([(1, 10)]));
}

#[test]
fn test_transaction_delete_sees_own_pending_primary_key_insert() {
    let table = create_pk_table();
    let txn = Arc::new(Transaction::new(70_101, table.max_version().max(0) as u64));

    let view0 = storage_view_for_txn_command(&txn, 0);
    table
        .append_with_transaction(&view0, &chunk_with_pairs(&[1], &[10]), txn.clone())
        .unwrap();
    txn.publish_command_boundary(CommandId::new(1));

    let view1 = storage_view_for_txn_command(&txn, 1);
    let removed = table
        .delete_by_primary_keys(&view1, &chunk_with_i32(&[1]), txn.clone())
        .unwrap();
    assert_eq!(removed, 1);

    commit_transaction(&txn, 1).unwrap();
    assert!(read_row_map_from_tablet(&table.tablet(), table.max_version()).is_empty());
    assert!(table
        .tablet()
        .lookup_primary_key(&primary_key_bytes(&table, 1))
        .unwrap()
        .is_none());
}

#[test]
fn test_transaction_on_conflict_update_reads_own_pending_primary_key_insert() {
    let table = create_pk_table();
    let txn = Arc::new(Transaction::new(70_102, table.max_version().max(0) as u64));

    let view0 = storage_view_for_txn_command(&txn, 0);
    table
        .append_with_transaction(&view0, &chunk_with_pairs(&[1], &[10]), txn.clone())
        .unwrap();
    txn.publish_command_boundary(CommandId::new(1));

    let view1 = storage_view_for_txn_command(&txn, 1);
    let affected = table
        .insert_on_conflict(
            &view1,
            &chunk_with_pairs(&[1], &[20]),
            &InsertOnConflictAction::DoUpdate {
                target_columns: vec![1],
                source_columns: vec![1],
            },
            txn.clone(),
        )
        .unwrap();
    assert_eq!(affected, 1);

    commit_transaction(&txn, 1).unwrap();
    let rows = read_row_map_from_tablet(&table.tablet(), table.max_version());
    assert_eq!(rows, BTreeMap::from([(1, 20)]));
}

#[test]
fn test_primary_key_commit_validation_rejects_write_after_read_ts() {
    let table = create_pk_table();
    let stale_txn = Arc::new(Transaction::new(71_001, table.max_version().max(0) as u64));
    let winner = Arc::new(Transaction::new(71_002, table.max_version().max(0) as u64));

    let winner_view = storage_view_for_txn_command(&winner, 0);
    table
        .append_with_transaction(&winner_view, &chunk_with_pairs(&[1], &[20]), winner.clone())
        .unwrap();
    commit_transaction(&winner, 1).unwrap();

    let stale_view = storage_view_for_txn_command(&stale_txn, 0);
    table
        .append_with_transaction(
            &stale_view,
            &chunk_with_pairs(&[1], &[10]),
            stale_txn.clone(),
        )
        .unwrap();
    let err = commit_transaction(&stale_txn, 2).unwrap_err();
    assert!(
        err.to_string().contains("write-write conflict"),
        "expected primary-key validation conflict, got: {err}"
    );
}

#[test]
fn test_delete_then_reinsert_same_primary_key_becomes_visible_again() {
    let (tablet, _tmp) = create_pk_tablet();

    let mut writer = DeltaWriter::open(tablet.clone(), 21).unwrap();
    writer
        .write_chunk(&chunk_with_pairs(&[1, 2], &[10, 20]))
        .unwrap();
    writer.commit().unwrap();

    let delete_writer = DeltaWriter::open(tablet.clone(), 22).unwrap();
    let deleted = delete_writer
        .delete_keys(&chunk_with_pairs(&[1], &[0]))
        .unwrap();
    assert_eq!(deleted, 1);

    let after_delete = read_row_map_from_tablet(&tablet, tablet.max_version());
    let expected_after_delete: BTreeMap<i32, i32> = vec![(2, 20)].into_iter().collect();
    assert_eq!(after_delete, expected_after_delete);

    let mut reinsert_writer = DeltaWriter::open(tablet.clone(), 23).unwrap();
    reinsert_writer
        .write_chunk(&chunk_with_pairs(&[1], &[999]))
        .unwrap();
    reinsert_writer.commit().unwrap();

    let rows = read_row_map_from_tablet(&tablet, tablet.max_version());
    let expected: BTreeMap<i32, i32> = vec![(1, 999), (2, 20)].into_iter().collect();
    assert_eq!(rows, expected);
    assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 2);
}

#[test]
fn test_delete_vector() {
    let (tablet, _tmp) = create_pk_tablet();
    let tm = Arc::new(TransactionManager::new());

    let t1 = tm.begin_transaction().unwrap();
    let mut writer = DeltaWriter::open(tablet.clone(), t1.id).unwrap();
    writer.write_chunk(&chunk_with_pairs(&[1], &[10])).unwrap();
    let rowset1 = writer
        .commit_in_transaction(t1.clone(), view_for_txn(&t1).command_id())
        .unwrap();
    commit_storage_transaction(&tm, t1);

    let t2 = tm.begin_transaction().unwrap();
    let mut writer2 = DeltaWriter::open(tablet.clone(), t2.id).unwrap();
    writer2.write_chunk(&chunk_with_pairs(&[1], &[20])).unwrap();
    let _rowset2 = writer2
        .commit_in_transaction(t2.clone(), view_for_txn(&t2).command_id())
        .unwrap();
    commit_storage_transaction(&tm, t2);

    let committed_rowset1 = tablet.find_rowset_by_id(rowset1.rowset_id()).unwrap();
    let dv = DeleteVector::load_from_dir(committed_rowset1.rowset_path(), 0)
        .unwrap()
        .unwrap();
    assert_eq!(dv.cardinality(), 1);
}

#[test]
fn test_restart_reloads_committed_rowset_from_tablet_meta() {
    let (tablet, tmp, manager) = create_managed_pk_tablet();

    let mut writer = DeltaWriter::open(tablet.clone(), 1).unwrap();
    writer
        .write_chunk(&chunk_with_pairs(&[1, 2], &[10, 20]))
        .unwrap();
    let rowset = writer.commit().unwrap();
    let rowset_path = rowset.rowset_path().to_string_lossy().to_string();
    assert_eq!(rowset.rowset_meta().rowset_path(), rowset_path);
    assert!(rowset.rowset_path().join("0.dat").exists());
    let mut reader = TabletReader::new(
        tablet.clone(),
        TabletReaderParams::with_version(tablet.max_version()),
    )
    .unwrap();
    reader.prepare().unwrap();
    let mut pre_rows = 0usize;
    while let Some(chunk) = reader.get_next_chunk().unwrap() {
        pre_rows += chunk.size();
    }
    assert_eq!(pre_rows, 2);

    tablet.save_meta().unwrap();
    drop(tablet);

    let reloaded = Arc::new(Tablet::open(1, tmp.path(), manager).unwrap());
    assert_eq!(reloaded.num_rowsets(), 1);

    let mut reader = TabletReader::new(
        reloaded.clone(),
        TabletReaderParams::with_version(reloaded.max_version()),
    )
    .unwrap();
    reader.prepare().unwrap();
    let mut total = 0usize;
    while let Some(chunk) = reader.get_next_chunk().unwrap() {
        total += chunk.size();
    }
    assert_eq!(total, 2);
}

#[test]
fn test_restart_reloads_delete_then_reinsert_for_primary_key() {
    let (tablet, tmp, manager) = create_managed_pk_tablet();

    let mut writer = DeltaWriter::open(tablet.clone(), 31).unwrap();
    writer
        .write_chunk(&chunk_with_pairs(&[1, 2], &[10, 20]))
        .unwrap();
    writer.commit().unwrap();

    let delete_writer = DeltaWriter::open(tablet.clone(), 32).unwrap();
    let deleted = delete_writer
        .delete_keys(&chunk_with_pairs(&[1], &[0]))
        .unwrap();
    assert_eq!(deleted, 1);

    let mut reinsert_writer = DeltaWriter::open(tablet.clone(), 33).unwrap();
    reinsert_writer
        .write_chunk(&chunk_with_pairs(&[1], &[111]))
        .unwrap();
    reinsert_writer.commit().unwrap();

    tablet.save_meta().unwrap();
    drop(tablet);

    let reloaded = Arc::new(Tablet::open(1, tmp.path(), manager).unwrap());
    let rows = read_row_map_from_tablet(&reloaded, reloaded.max_version());
    let expected: BTreeMap<i32, i32> = vec![(1, 111), (2, 20)].into_iter().collect();
    assert_eq!(rows, expected);
    assert_eq!(reloaded.snapshot_primary_index_entries().unwrap().len(), 2);
}

#[test]
fn test_concurrent_read_write_rowset_scan() {
    let table = Arc::new(create_table(&[LogicalType::Integer]));
    let tm = Arc::new(TransactionManager::new());

    let t_read = tm.begin_transaction().unwrap();
    let visible = t_read.visible_version();
    let table_for_read = table.clone();
    let handle = thread::spawn(move || read_rows(&table_for_read, visible));

    let t_write = tm.begin_transaction().unwrap();
    let chunk = chunk_with_i32(&[7, 8, 9]);
    table
        .append_with_transaction(&view_for_txn(&t_write), &chunk, t_write.clone())
        .unwrap();
    commit_storage_transaction(&tm, t_write);

    let read_count = handle.join().unwrap();
    assert_eq!(read_count, 0);
    tm.rollback_transaction(t_read).unwrap();

    let t_after = tm.begin_transaction().unwrap();
    let count = read_rows(&table, t_after.visible_version());
    assert_eq!(count, 3);
    tm.rollback_transaction(t_after).unwrap();
}

#[test]
fn test_concurrent_upsert_same_primary_key_keeps_latest_committed_value_visible() {
    let (tablet, _tmp) = create_pk_tablet();
    let start = Arc::new(Barrier::new(5));

    let handles: Vec<_> = (0..4)
        .map(|idx| {
            let tablet = tablet.clone();
            let start = start.clone();
            thread::spawn(move || {
                let txn_id = 40 + idx as u64;
                let value = 1000 + idx as i32;
                start.wait();
                let mut writer = DeltaWriter::open(tablet, txn_id).unwrap();
                writer
                    .write_chunk(&chunk_with_pairs(&[7], &[value]))
                    .unwrap();
                let rowset = writer.commit().unwrap();
                (value, rowset.end_version())
            })
        })
        .collect();

    start.wait();
    let committed: Vec<(i32, i64)> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();

    let (expected_value, expected_version) = committed
        .iter()
        .max_by_key(|(_, version)| *version)
        .copied()
        .unwrap();

    let rows = read_row_map_from_tablet(&tablet, tablet.max_version());
    let expected: BTreeMap<i32, i32> = vec![(7, expected_value)].into_iter().collect();
    assert_eq!(rows, expected);
    assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 1);
    assert_eq!(tablet.max_version(), expected_version);
}

#[test]
fn test_transaction_memtable_reuse_single_rowset() {
    let metrics_before = storage_metrics().snapshot();
    let table = create_pk_table();
    let tablet = table.tablet();
    let tm = Arc::new(TransactionManager::new());

    let t1 = tm.begin_transaction().unwrap();
    table
        .append_with_transaction(
            &view_for_txn(&t1),
            &chunk_with_pairs(&[1], &[10]),
            t1.clone(),
        )
        .unwrap();
    table
        .append_with_transaction(
            &view_for_txn(&t1),
            &chunk_with_pairs(&[2], &[20]),
            t1.clone(),
        )
        .unwrap();

    // Before commit, writes are still transaction-local.
    assert_eq!(tablet.num_rowsets(), 0);

    commit_storage_transaction(&tm, t1);
    assert_eq!(tablet.num_rowsets(), 1);
    assert_eq!(read_rows_from_tablet(&tablet, tablet.max_version()), 2);

    // No threshold reached, flush happens during commit finalization.
    let metrics_after = storage_metrics().snapshot();
    assert!(
        metrics_after.memtable_flush_count > metrics_before.memtable_flush_count,
        "commit should flush transaction-local memtable"
    );
}

#[test]
fn test_transaction_memtable_flush_on_threshold_then_commit() {
    let metrics_before = storage_metrics().snapshot();
    let table = create_pk_table();
    let tablet = table.tablet();
    let tm = Arc::new(TransactionManager::new());

    let t1 = tm.begin_transaction().unwrap();
    let rows = 70_000i32;
    let ids: Vec<i32> = (0..rows).collect();
    let vals: Vec<i32> = (0..rows).map(|v| v * 10).collect();

    table
        .append_with_transaction(
            &view_for_txn(&t1),
            &chunk_with_pairs(&ids, &vals),
            t1.clone(),
        )
        .unwrap();

    // Threshold flush happens inside writer, but data is still uncommitted.
    let mid = storage_metrics().snapshot();
    assert!(
        mid.memtable_flush_count > metrics_before.memtable_flush_count,
        "expected threshold-driven memtable flush"
    );
    assert_eq!(tablet.num_rowsets(), 0);

    commit_storage_transaction(&tm, t1);
    assert_eq!(tablet.num_rowsets(), 1);
    assert_eq!(
        read_rows_from_tablet(&tablet, tablet.max_version()),
        rows as usize
    );
}
