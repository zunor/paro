// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::test_utils::test_allocator;
use paro_common::types::LogicalType;
use paro_storage::compaction::compaction_manager::CompactionManager;
use paro_storage::compaction::compaction_task::{CompactionTask, HorizontalCompactionTask};
use paro_storage::compaction::execution::rowset_merger::RowsetMerger;
use paro_storage::compaction::execution::workspace::{CompactionBuildOutput, CompactionWorkspace};
use paro_storage::compaction::plan::types::CompactionJobId;
use paro_storage::compaction::plan::CompactionPlanner;
use paro_storage::compaction::publish::{CompactionPublisher, CompactionValidator};
use paro_storage::meta::{FileMetadataStore, MetadataStore, TabletMetaManager};
use paro_storage::tablet::{
    KeysType, RetiredGcBarrier, Tablet, TabletColumn, TabletReader, TabletReaderParams,
    TabletSchema,
};
use paro_storage::write::DeltaWriter;
use std::collections::{BTreeMap, HashMap, HashSet};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

fn compaction_allocator() -> Arc<dyn Allocator> {
    test_allocator()
}

fn create_test_schema() -> Arc<TabletSchema> {
    let mut columns = Vec::new();
    columns.push(TabletColumn::new(0, "pk".to_string(), LogicalType::Integer));
    columns[0].is_key = true;
    columns.push(TabletColumn::new(1, "v".to_string(), LogicalType::Integer));
    Arc::new(TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap())
}

fn create_duplicate_schema() -> Arc<TabletSchema> {
    let mut columns = Vec::new();
    columns.push(TabletColumn::new(0, "k".to_string(), LogicalType::Integer));
    columns[0].is_key = true;
    columns.push(TabletColumn::new(1, "v".to_string(), LogicalType::Integer));
    Arc::new(TabletSchema::new(2, columns, KeysType::DuplicateKeys).unwrap())
}

fn create_tablet(dir: &TempDir) -> Arc<Tablet> {
    let schema = create_test_schema();
    let tablet = Tablet::new(101, 100, 0, schema, dir.path(), None).unwrap();
    tablet.init().unwrap();
    Arc::new(tablet)
}

fn create_duplicate_tablet(dir: &TempDir, tablet_id: u64) -> Arc<Tablet> {
    let schema = create_duplicate_schema();
    let tablet = Tablet::new(tablet_id, 200, 0, schema, dir.path(), None).unwrap();
    tablet.init().unwrap();
    Arc::new(tablet)
}

fn create_test_meta_manager(dir: &TempDir) -> Arc<TabletMetaManager> {
    let store: Arc<dyn MetadataStore> =
        Arc::new(FileMetadataStore::new(dir.path().join("meta")).unwrap());
    Arc::new(TabletMetaManager::with_store_and_data_root(
        store,
        dir.path(),
    ))
}

fn create_managed_duplicate_tablet(
    dir: &TempDir,
    tablet_id: u64,
) -> (Arc<Tablet>, Arc<TabletMetaManager>) {
    let manager = create_test_meta_manager(dir);
    let schema = create_duplicate_schema();
    let tablet = Tablet::new(tablet_id, 200, 0, schema, dir.path(), Some(manager.clone())).unwrap();
    tablet.init().unwrap();
    (Arc::new(tablet), manager)
}

fn append_data_with_txn(tablet: &Arc<Tablet>, txn_id: u64, keys: Vec<i32>, values: Vec<i32>) {
    assert_eq!(keys.len(), values.len());
    let mut writer = DeltaWriter::open(tablet.clone(), txn_id).unwrap();

    let allocator = test_allocator();
    let chunk = Chunk::from_vectors(
        vec![
            paro_common::test_utils::test_i32_vector_with_allocator(&keys, allocator.clone()),
            paro_common::test_utils::test_i32_vector_with_allocator(&values, allocator.clone()),
        ],
        allocator,
    );

    writer.write_chunk(&chunk).unwrap();
    writer.commit().unwrap();
}

fn append_data(tablet: &Arc<Tablet>, keys: Vec<i32>, values: Vec<i32>) {
    append_data_with_txn(tablet, 100, keys, values)
}

fn append_range(tablet: &Arc<Tablet>, txn_id: u64, start: i32, end: i32, offset: i32) {
    let keys: Vec<i32> = (start..end).collect();
    let values: Vec<i32> = keys.iter().map(|k| *k + offset).collect();
    append_data_with_txn(tablet, txn_id, keys, values);
}

fn delete_data_with_txn(tablet: &Arc<Tablet>, txn_id: u64, keys: Vec<i32>) {
    let writer = DeltaWriter::open(tablet.clone(), txn_id).unwrap();

    // Delete chunk only needs key columns: pk (i32)
    let allocator = test_allocator();
    let chunk = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i32_vector_with_allocator(
            &keys,
            allocator.clone(),
        )],
        allocator,
    );

    let count = writer.delete_keys(&chunk).unwrap();
    assert_eq!(count, keys.len(), "Failed to delete all requested keys");
}

fn delete_data(tablet: &Arc<Tablet>, keys: Vec<i32>) {
    delete_data_with_txn(tablet, 101, keys)
}

fn read_rows(tablet: Arc<Tablet>) -> Vec<(i32, i32)> {
    let mut reader = TabletReader::new(
        tablet.clone(),
        TabletReaderParams::with_version(tablet.max_version()),
    )
    .expect("create tablet reader");
    reader.prepare().expect("prepare tablet reader");

    let mut rows = Vec::new();
    while let Some(chunk) = reader.get_next_chunk().expect("read chunk") {
        for row in 0..chunk.size() {
            let k = chunk
                .column(0)
                .expect("pk column")
                .get_i32(row)
                .expect("pk value");
            let v = chunk
                .column(1)
                .expect("value column")
                .get_i32(row)
                .expect("value");
            rows.push((k, v));
        }
    }

    rows.sort_by_key(|(k, _)| *k);
    rows
}

fn read_row_map(tablet: Arc<Tablet>) -> BTreeMap<i32, i32> {
    read_rows(tablet).into_iter().collect()
}

fn read_count_and_samples(tablet: Arc<Tablet>, sample_keys: &[i32]) -> (usize, BTreeMap<i32, i32>) {
    let mut reader = TabletReader::new(
        tablet.clone(),
        TabletReaderParams::with_version(tablet.max_version()),
    )
    .expect("create tablet reader");
    reader.prepare().expect("prepare tablet reader");

    let wanted: HashSet<i32> = sample_keys.iter().copied().collect();
    let mut count = 0usize;
    let mut samples = BTreeMap::new();
    while let Some(chunk) = reader.get_next_chunk().expect("read chunk") {
        count += chunk.size();
        for row in 0..chunk.size() {
            let k = chunk
                .column(0)
                .expect("pk column")
                .get_i32(row)
                .expect("pk value");
            if wanted.contains(&k) {
                let v = chunk
                    .column(1)
                    .expect("value column")
                    .get_i32(row)
                    .expect("value");
                samples.insert(k, v);
            }
        }
    }

    (count, samples)
}

fn run_size_tiered_until_stable(tablet: Arc<Tablet>, max_rounds: usize) -> usize {
    let mut rounds = 0usize;

    while tablet.num_rowsets() > 1 {
        let Some(plan) = CompactionPlanner::plan(tablet.as_ref()).unwrap() else {
            break;
        };

        let mut task = HorizontalCompactionTask::new(tablet.clone(), plan, compaction_allocator());
        task.run().unwrap();
        rounds += 1;

        assert!(
            rounds <= max_rounds,
            "size-tiered did not converge within {} rounds, current_rowsets={}",
            max_rounds,
            tablet.num_rowsets()
        );
    }

    rounds
}

#[tokio::test]
async fn test_compaction_correctness() {
    let dir = TempDir::new().unwrap();
    let tablet = create_tablet(&dir);

    append_data(&tablet, vec![1, 2], vec![10, 20]);
    append_data(&tablet, vec![3, 4], vec![30, 40]);
    append_data(&tablet, vec![2, 5], vec![22, 50]);

    delete_data(&tablet, vec![1]);

    // Deletion is in-mem/WAL, no new rowset until flush.
    // Compaction should handle it via delete vector.
    assert_eq!(tablet.num_rowsets(), 3);

    let context = CompactionPlanner::plan(&tablet)
        .unwrap()
        .expect("Should have a compaction plan");
    assert_eq!(context.input_rowsets.len(), 3);

    let manager = CompactionManager::new(1);
    manager.register_tablet(tablet.clone());

    manager.schedule().await;

    // Poll for success
    let start = std::time::Instant::now();
    loop {
        let n = tablet.num_rowsets();
        if n == 1 {
            // Success!
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Timeout waiting for compaction. num_rowsets={}", n);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Check one more time to ensure stability
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(tablet.num_rowsets(), 1, "Unstable rowset count");

    let rowset = tablet.get_rowset_by_version(1).unwrap();
    // After delete(1): (2, 22), (3, 30), (4, 40), (5, 50) => 4 rows.
    // Current PK compaction rewrites only the latest visible rows into the output
    // rowset instead of preserving superseded duplicates as dead physical rows.
    assert_eq!(
        rowset.num_rows(),
        4,
        "Physical row count should match the surviving visible rows"
    );
    assert_eq!(
        rowset.rowset_meta().effective_rows(),
        4,
        "Effective row count should be 4"
    );
    assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 4);
}

#[test]
fn test_compaction_tablet_reader_before_after_consistent_with_pk_dedup() {
    let dir = TempDir::new().unwrap();
    let tablet = create_tablet(&dir);

    append_data_with_txn(&tablet, 201, vec![1, 2, 3], vec![10, 20, 30]);
    append_data_with_txn(&tablet, 202, vec![2, 4], vec![22, 40]);
    append_data_with_txn(&tablet, 203, vec![3, 5], vec![33, 50]);
    delete_data_with_txn(&tablet, 204, vec![1, 5]);

    let before_raw = read_rows(tablet.clone());
    let before: BTreeMap<i32, i32> = before_raw.into_iter().collect();
    let expected: BTreeMap<i32, i32> = vec![(2, 22), (3, 33), (4, 40)].into_iter().collect();
    assert_eq!(before, expected);

    let context = CompactionPlanner::plan(&tablet)
        .unwrap()
        .expect("plan compaction");
    let mut task = HorizontalCompactionTask::new(tablet.clone(), context, compaction_allocator());
    task.run().unwrap();

    assert_eq!(tablet.num_rowsets(), 1);

    let after_raw = read_rows(tablet.clone());
    let after: BTreeMap<i32, i32> = after_raw.into_iter().collect();
    assert_eq!(
        after, before,
        "TabletReader logical result changed after compaction"
    );

    let unique_keys: HashSet<i32> = after.keys().copied().collect();
    assert_eq!(
        unique_keys.len(),
        after.len(),
        "PK dedup should keep unique keys"
    );
    assert_eq!(
        tablet.snapshot_primary_index_entries().unwrap().len(),
        after.len()
    );
}

#[test]
fn test_compaction_preserves_delete_then_reinsert_latest_primary_key_rows() {
    let dir = TempDir::new().unwrap();
    let tablet = create_tablet(&dir);

    append_data_with_txn(&tablet, 211, vec![1, 2], vec![10, 20]);
    delete_data_with_txn(&tablet, 212, vec![1]);
    append_data_with_txn(&tablet, 213, vec![1, 3], vec![111, 30]);

    let before = read_row_map(tablet.clone());
    let expected: BTreeMap<i32, i32> = vec![(1, 111), (2, 20), (3, 30)].into_iter().collect();
    assert_eq!(before, expected);

    let context = CompactionPlanner::plan(&tablet)
        .unwrap()
        .expect("plan compaction for delete/reinsert");
    let mut task = HorizontalCompactionTask::new(tablet.clone(), context, compaction_allocator());
    task.run().unwrap();

    let after = read_row_map(tablet.clone());
    assert_eq!(after, expected);
    assert_eq!(tablet.num_rowsets(), 1);
    assert_eq!(
        tablet.snapshot_primary_index_entries().unwrap().len(),
        expected.len()
    );
}

#[test]
fn test_compaction_with_concurrent_write() {
    let dir = TempDir::new().unwrap();
    let tablet = create_tablet(&dir);

    append_data(&tablet, vec![1, 2], vec![10, 20]);
    append_data(&tablet, vec![3, 4], vec![30, 40]);

    let context = CompactionPlanner::plan(&tablet)
        .unwrap()
        .expect("Plan v1-v2");

    let mut task = HorizontalCompactionTask::new(tablet.clone(), context, compaction_allocator());

    task.run().unwrap();

    append_data(&tablet, vec![5], vec![50]);

    assert_eq!(tablet.num_rowsets(), 2);
    assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 5);
}

#[test]
fn test_compaction_keeps_old_snapshot_rowids_readable_until_gc() {
    let dir = TempDir::new().unwrap();
    let tablet = create_tablet(&dir);

    append_data_with_txn(&tablet, 601, vec![1, 2], vec![10, 20]);
    append_data_with_txn(&tablet, 602, vec![3, 4], vec![30, 40]);

    let snapshot_version = tablet.max_version();
    let mut old_reader = TabletReader::new(
        tablet.clone(),
        TabletReaderParams::with_version(snapshot_version)
            .with_columns(vec![0, 1])
            .with_emit_row_id(true),
    )
    .expect("create old snapshot reader");
    old_reader.prepare().expect("prepare old snapshot reader");

    let mut snapshot_rowids = Vec::new();
    let mut snapshot_rows = Vec::new();
    while let Some(chunk) = old_reader
        .get_next_chunk()
        .expect("read old snapshot chunk")
    {
        let ids = chunk.column(0).expect("id column");
        let vals = chunk.column(1).expect("value column");
        let row_ids = chunk.column(2).expect("row_id column");
        for row in 0..chunk.size() {
            snapshot_rows.push((
                ids.get_i32(row).expect("id"),
                vals.get_i32(row).expect("value"),
            ));
            snapshot_rowids.push(row_ids.get_i64(row).expect("row id") as u64);
        }
    }

    let context = CompactionPlanner::plan(&tablet)
        .unwrap()
        .expect("plan compaction");
    let mut task = HorizontalCompactionTask::new(tablet.clone(), context, compaction_allocator());
    task.run().unwrap();

    assert_eq!(tablet.num_rowsets(), 1);
    assert_eq!(tablet.min_active_visible_version(), Some(snapshot_version));
    let retired = tablet.retired_pending_gc_statuses();
    assert_eq!(retired.len(), 2);
    assert!(retired
        .iter()
        .all(|status| status.barrier != RetiredGcBarrier::Eligible));
    assert!(retired
        .iter()
        .any(|status| { status.barrier == RetiredGcBarrier::PendingSnapshotBarrier }));

    let fetched = old_reader
        .get_by_rowids(&snapshot_rowids, &[0, 1])
        .expect("late fetch by old rowids after compaction");
    let mut fetched_rows = Vec::new();
    for row in 0..fetched.size() {
        fetched_rows.push((
            fetched
                .column(0)
                .expect("id column")
                .get_i32(row)
                .expect("id"),
            fetched
                .column(1)
                .expect("value column")
                .get_i32(row)
                .expect("value"),
        ));
    }
    assert_eq!(fetched_rows, snapshot_rows);

    drop(old_reader);
    assert_eq!(tablet.min_active_visible_version(), None);
    let retired = tablet.retired_pending_gc_statuses();
    assert!(retired.is_empty());
}

#[test]
fn test_compaction_interleaved_with_concurrent_write_and_delete() {
    let dir = TempDir::new().unwrap();
    let tablet = create_tablet(&dir);

    // Build enough data so compaction has real merge work.
    append_range(&tablet, 301, 0, 20_000, 10);
    append_range(&tablet, 302, 10_000, 30_000, 1000);
    append_range(&tablet, 303, 30_000, 35_000, 2000);

    let context = CompactionPlanner::plan(&tablet)
        .unwrap()
        .expect("plan before concurrent mutation");

    let tablet_for_compaction = tablet.clone();
    let handle = std::thread::spawn(move || {
        let mut task =
            HorizontalCompactionTask::new(tablet_for_compaction, context, compaction_allocator());
        task.run().unwrap();
    });

    // Interleave concurrent write/delete while compaction uses an old snapshot plan.
    append_data_with_txn(
        &tablet,
        304,
        vec![15_000, 32_000, 45_000],
        vec![9_999, 8_888, 7_777],
    );
    delete_data_with_txn(&tablet, 305, vec![42, 15_001, 45_000]);

    handle.join().unwrap();

    let rows = read_row_map(tablet.clone());
    assert_eq!(rows.get(&15_000), Some(&9_999));
    assert_eq!(rows.get(&32_000), Some(&8_888));
    assert!(!rows.contains_key(&42));
    assert!(!rows.contains_key(&15_001));
    assert!(!rows.contains_key(&45_000));
    assert_eq!(
        tablet.snapshot_primary_index_entries().unwrap().len(),
        rows.len()
    );
}

#[test]
fn test_compaction_large_primary_key_bulk_insert_keeps_query_results_stable() {
    let dir = TempDir::new().unwrap();
    let tablet = create_tablet(&dir);
    let sample_keys = [0, 2_500, 4_999];

    append_range(&tablet, 701, 0, 1_250, 10);
    append_range(&tablet, 702, 1_250, 2_500, 10);
    append_range(&tablet, 703, 2_500, 3_750, 10);
    append_range(&tablet, 704, 3_750, 5_000, 10);

    let (before_count, before_samples) = read_count_and_samples(tablet.clone(), &sample_keys);
    let expected_samples: BTreeMap<i32, i32> = vec![(0, 10), (2_500, 2_510), (4_999, 5_009)]
        .into_iter()
        .collect();
    assert_eq!(before_count, 5_000);
    assert_eq!(before_samples, expected_samples);

    let context = CompactionPlanner::plan(&tablet)
        .unwrap()
        .expect("plan compaction for bulk insert");
    let mut task = HorizontalCompactionTask::new(tablet.clone(), context, compaction_allocator());
    task.run().unwrap();

    let (after_count, after_samples) = read_count_and_samples(tablet.clone(), &sample_keys);
    assert_eq!(after_count, before_count);
    assert_eq!(after_samples, expected_samples);
    assert_eq!(tablet.num_rowsets(), 1);
    assert_eq!(
        tablet.snapshot_primary_index_entries().unwrap().len(),
        5_000
    );
}

#[test]
fn test_compaction_crash_before_replace_restarts_from_persisted_meta() {
    let dir = TempDir::new().unwrap();
    let (tablet, manager) = create_managed_duplicate_tablet(&dir, 202);

    // Persist an initial meta snapshot; an unpublished staged output must not
    // become visible after restart.
    tablet.save_meta().unwrap();

    append_range(&tablet, 401, 0, 200, 0);
    append_range(&tablet, 402, 200, 400, 100);

    let before = read_rows(tablet.clone());

    let plan = CompactionPlanner::plan(&tablet)
        .unwrap()
        .expect("duplicate-key compaction plan");

    // Simulate crash in the middle of compaction: build completes in staging, but publish never runs.
    let workspace =
        CompactionWorkspace::create(&tablet, CompactionJobId(9001), plan.output_rowset_id).unwrap();
    let output = RowsetMerger::build(&tablet, Arc::new(plan), workspace, compaction_allocator())
        .unwrap()
        .expect("merge should produce staged output");
    let staged_rowset_dir = match output {
        CompactionBuildOutput::Rowset(artifact) => artifact.workspace.rowset_dir.clone(),
        CompactionBuildOutput::PrimaryKey { .. } => panic!("expected duplicate-key output"),
    };
    assert!(staged_rowset_dir.exists());
    assert_eq!(
        tablet.num_rowsets(),
        2,
        "inputs should remain visible before replace"
    );

    drop(tablet);

    let reloaded = Arc::new(Tablet::open(202, dir.path(), manager).unwrap());
    let after = read_rows(reloaded.clone());

    assert_eq!(
        after, before,
        "recovery should not expose partial compaction result"
    );
    assert_eq!(
        reloaded.num_rowsets(),
        2,
        "restart should keep original committed rowsets"
    );
}

#[cfg(unix)]
#[test]
fn test_compaction_build_failure_does_not_pollute_visible_query_results() {
    let dir = TempDir::new().unwrap();
    let tablet = create_duplicate_tablet(&dir, 203);

    append_range(&tablet, 501, 0, 2_000, 0);
    append_range(&tablet, 502, 2_000, 4_000, 100);
    let before = read_rows(tablet.clone());

    let plan = CompactionPlanner::plan(&tablet)
        .unwrap()
        .expect("duplicate-key compaction plan");
    let final_rowset_path = tablet
        .data_dir()
        .join("rowsets")
        .join(format!("rowset_{}", plan.output_rowset_id));
    let cancel_token = CancellationToken::new();
    let workspace = CompactionWorkspace::create_with_cancel_token(
        &tablet,
        CompactionJobId(9_003),
        plan.output_rowset_id,
        cancel_token,
    )
    .unwrap();
    let staging_rowset_dir = workspace.rowset_dir.clone();
    let mut permissions = std::fs::metadata(&workspace.rowset_dir)
        .unwrap()
        .permissions();
    permissions.set_mode(0o555);
    std::fs::set_permissions(&workspace.rowset_dir, permissions.clone()).unwrap();

    let build = RowsetMerger::build(&tablet, Arc::new(plan), workspace, compaction_allocator());
    assert!(
        build.is_err(),
        "expected compaction build to fail on read-only staging dir"
    );

    permissions.set_mode(0o755);
    if staging_rowset_dir.exists() {
        let _ = std::fs::set_permissions(&staging_rowset_dir, permissions);
    }
    paro_storage::compaction::cleanup::sweep_staging_root(tablet.data_dir().join("_compaction"));
    std::thread::sleep(Duration::from_millis(50));

    assert_eq!(read_rows(tablet.clone()), before);
    assert_eq!(tablet.num_rowsets(), 2);
    assert!(
        !final_rowset_path.exists(),
        "failed build must not leak into final rowset namespace"
    );
}

#[test]
fn test_compaction_final_dir_without_publish_record_is_not_recovered_visible() {
    let dir = TempDir::new().unwrap();
    let (tablet, manager) = create_managed_duplicate_tablet(&dir, 204);

    tablet.save_meta().unwrap();
    append_range(&tablet, 601, 0, 500, 0);
    append_range(&tablet, 602, 500, 1_000, 100);
    let before = read_rows(tablet.clone());

    let plan = CompactionPlanner::plan(&tablet)
        .unwrap()
        .expect("duplicate-key compaction plan");
    let workspace =
        CompactionWorkspace::create(&tablet, CompactionJobId(9_004), plan.output_rowset_id)
            .unwrap();
    let output = RowsetMerger::build(&tablet, Arc::new(plan), workspace, compaction_allocator())
        .unwrap()
        .expect("staged compaction output");
    CompactionValidator::validate_artifact(&tablet, &output).unwrap();
    let request =
        CompactionPublisher::prepare_request(&tablet, output, CompactionJobId(9_004)).unwrap();

    let staged_rowset_dir = match &request.output {
        CompactionBuildOutput::Rowset(artifact) => artifact.workspace.rowset_dir.clone(),
        CompactionBuildOutput::PrimaryKey { .. } => panic!("expected duplicate-key output"),
    };
    let final_rowset_path = PathBuf::from(&request.record.output_rowset_path);
    std::fs::rename(&staged_rowset_dir, &final_rowset_path).unwrap();
    assert!(final_rowset_path.exists());

    drop(tablet);

    let reopened = Arc::new(Tablet::open(204, dir.path(), manager).unwrap());
    assert_eq!(read_rows(reopened.clone()), before);
    assert_eq!(reopened.num_rowsets(), 2);
    assert!(reopened
        .find_rowset_by_id(request.record.output_rowset_id)
        .is_none());
    assert!(
        final_rowset_path.exists(),
        "journal-tail recovery must preserve canonical rowset artifacts until replay or retention decides ownership"
    );
}

#[test]
fn test_duplicate_key_compaction_keeps_output_visible() {
    let dir = TempDir::new().unwrap();
    let tablet = create_duplicate_tablet(&dir, 303);

    append_range(&tablet, 801, 0, 1_000, 0);
    append_range(&tablet, 802, 1_000, 2_000, 0);
    append_range(&tablet, 803, 2_000, 3_000, 0);
    append_range(&tablet, 804, 3_000, 4_000, 0);

    let before = read_rows(tablet.clone());
    assert_eq!(before.len(), 4_000);

    let context = CompactionPlanner::plan(&tablet)
        .unwrap()
        .expect("duplicate-key compaction plan");
    let mut task = HorizontalCompactionTask::new(tablet.clone(), context, compaction_allocator());
    task.run().unwrap();

    let after = read_rows(tablet.clone());
    assert_eq!(
        after, before,
        "duplicate-key compaction changed visible rows"
    );
    assert_eq!(
        tablet.num_rowsets(),
        1,
        "compaction should converge to one rowset"
    );
}

#[tokio::test]
async fn test_compaction_sync_drains_before_shutdown_sweep() {
    let dir = TempDir::new().unwrap();
    let tablet = create_duplicate_tablet(&dir, 401);

    append_range(&tablet, 1_001, 0, 5_000, 0);
    append_range(&tablet, 1_002, 5_000, 10_000, 10);
    append_range(&tablet, 1_003, 10_000, 15_000, 20);
    append_range(&tablet, 1_004, 15_000, 20_000, 30);

    let manager = Arc::new(CompactionManager::new(1));
    manager.register_tablet(tablet.clone());
    manager.schedule().await;

    let start = std::time::Instant::now();
    while !manager
        .observability()
        .running_tablets
        .contains(&tablet.tablet_id())
        && tablet.num_rowsets() > 1
    {
        if start.elapsed() > Duration::from_secs(10) {
            panic!("timed out waiting for compaction to start");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let manager_for_sync = manager.clone();
    let stats =
        tokio::task::spawn_blocking(move || manager_for_sync.sync_tablets(HashMap::new()).unwrap())
            .await
            .unwrap();
    assert_eq!(stats.unregistered, 1);
    assert_eq!(manager.running_task_count(), 0);
    assert!(manager.observability().failed_tablets.is_empty());

    let data_dir = tablet.data_dir().to_path_buf();
    Tablet::mark_shutdown_and_schedule_sweep_by_data_dir(&data_dir, false).unwrap();

    let start = std::time::Instant::now();
    while data_dir.exists() {
        if start.elapsed() > Duration::from_secs(10) {
            panic!(
                "timed out waiting for shutdown sweep to remove {}",
                data_dir.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[test]
fn test_compaction_size_tiered_convergence_speed_under_different_write_patterns() {
    let steady_dir = TempDir::new().unwrap();
    let steady = create_duplicate_tablet(&steady_dir, 301);

    // Pattern A: steady uniform writes (similar-size rowsets).
    for i in 0..8 {
        let start = i * 200;
        append_range(&steady, 500 + i as u64, start, start + 200, 0);
    }

    let steady_rounds = run_size_tiered_until_stable(steady.clone(), 10);
    assert_eq!(
        steady.num_rowsets(),
        1,
        "steady pattern should converge to one rowset"
    );

    let bursty_dir = TempDir::new().unwrap();
    let bursty = create_duplicate_tablet(&bursty_dir, 302);

    // Pattern B: trickle + bursts (different-size rowsets), should need more rounds.
    append_range(&bursty, 600, 0, 100, 0);
    append_range(&bursty, 601, 100, 200, 0);
    append_range(&bursty, 602, 200, 1_000, 0);
    append_range(&bursty, 603, 1_000, 1_800, 0);
    append_range(&bursty, 604, 1_800, 2_600, 0);

    let bursty_rounds = run_size_tiered_until_stable(bursty.clone(), 10);
    assert_eq!(
        bursty.num_rowsets(),
        1,
        "bursty pattern should also converge to one rowset"
    );

    assert!(
        bursty_rounds >= steady_rounds,
        "expected bursty pattern to be no faster than steady pattern: bursty_rounds={} steady_rounds={}",
        bursty_rounds,
        steady_rounds
    );
}
