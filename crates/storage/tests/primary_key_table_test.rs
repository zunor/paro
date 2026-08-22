// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use tempfile::TempDir;

use paro_common::chunk::Chunk;
use paro_common::test_utils::test_allocator;
use paro_common::types::LogicalType;
use paro_storage::meta::{FileMetadataStore, MetadataStore, TabletMetaManager};
use paro_storage::{
    compaction::compaction_task::{CompactionTask, HorizontalCompactionTask},
    compaction::plan::CompactionPlanner,
    index::hnsw::{HnswSearchPolicy, SearchParams},
    primary_key::{DeleteVector, PersistentIndex, PrimaryIndex, PrimaryKeySerializer},
    tablet::{
        tablet_schema::{KeysType, TabletColumn, TabletSchema},
        Tablet, TabletRef,
    },
    write::DeltaWriter,
};

fn create_test_schema() -> Arc<TabletSchema> {
    let cols = vec![
        TabletColumn::key(0, "id", LogicalType::Integer),
        TabletColumn::new(1, "v", LogicalType::Integer),
    ];
    Arc::new(TabletSchema::new(1, cols, KeysType::PrimaryKeys).unwrap())
}

fn create_test_meta_manager(tmp: &TempDir) -> Arc<TabletMetaManager> {
    let store: Arc<dyn MetadataStore> =
        Arc::new(FileMetadataStore::new(tmp.path().join("meta")).unwrap());
    Arc::new(TabletMetaManager::with_store_and_data_root(
        store,
        tmp.path(),
    ))
}

fn create_managed_test_tablet() -> (TabletRef, TempDir, Arc<TabletMetaManager>) {
    let tmp = TempDir::new().unwrap();
    let manager = create_test_meta_manager(&tmp);
    let schema = create_test_schema();
    let tablet = Tablet::new(1, 10, 100, schema, tmp.path(), Some(manager.clone())).unwrap();
    tablet.init().unwrap();
    (Arc::new(tablet), tmp, manager)
}

fn create_test_tablet() -> (TabletRef, TempDir) {
    let tmp = TempDir::new().unwrap();
    let schema = create_test_schema();
    let tablet = Tablet::new(1, 10, 100, schema, tmp.path(), None).unwrap();
    tablet.init().unwrap();
    (Arc::new(tablet), tmp)
}

fn create_vector_test_schema() -> Arc<TabletSchema> {
    let cols = vec![
        TabletColumn::key(0, "id", LogicalType::Integer),
        TabletColumn::new(
            1,
            "vec",
            LogicalType::Array(Box::new(LogicalType::Float), 2),
        )
        .with_hnsw_index(8, 64, 1), // cosine
    ];
    Arc::new(TabletSchema::new(2, cols, KeysType::PrimaryKeys).unwrap())
}

fn create_vector_test_tablet() -> (TabletRef, TempDir) {
    let tmp = TempDir::new().unwrap();
    let schema = create_vector_test_schema();
    let tablet = Tablet::new(2, 20, 200, schema, tmp.path(), None).unwrap();
    tablet.init().unwrap();
    (Arc::new(tablet), tmp)
}

fn chunk_with_vectors(ids: &[i32], vectors: &[[f32; 2]]) -> Chunk {
    assert_eq!(ids.len(), vectors.len());
    let embeddings: Vec<Vec<f32>> = vectors.iter().map(|v| v.to_vec()).collect();
    let allocator = test_allocator();
    let id_vec = paro_common::test_utils::test_i32_vector_with_allocator(ids, allocator.clone());
    let vec_vec = paro_common::test_utils::test_embeddings_vector_with_allocator(
        &embeddings,
        2,
        allocator.clone(),
    );
    paro_common::test_utils::test_chunk_from_vectors(vec![id_vec, vec_vec])
}

fn chunk_with_range(start: i32, end: i32) -> Chunk {
    let ids: Vec<i32> = (start..end).collect();
    // vals should have the same length as ids - map each id to id * 10
    let vals: Vec<i32> = ids.iter().map(|id| id * 10).collect();
    let allocator = test_allocator();
    let v0 = paro_common::test_utils::test_i32_vector_with_allocator(&ids, allocator.clone());
    let v1 = paro_common::test_utils::test_i32_vector_with_allocator(&vals, allocator.clone());
    paro_common::test_utils::test_chunk_from_vectors(vec![v0, v1])
}

fn key_chunk_i32(values: &[i32]) -> Chunk {
    let allocator = test_allocator();
    let vector = paro_common::test_utils::test_i32_vector_with_allocator(values, allocator.clone());
    paro_common::test_utils::test_chunk_from_vectors(vec![vector])
}

fn run_compaction(tablet: &TabletRef) {
    let plan = CompactionPlanner::plan(tablet.as_ref())
        .unwrap()
        .expect("compaction plan should exist");
    let mut task = HorizontalCompactionTask::new(tablet.clone(), plan, test_allocator());
    task.run().unwrap();
}

#[test]
fn primary_index_basic() {
    let idx = PrimaryIndex::new();
    let k = b"k1".to_vec();
    let loc =
        paro_storage::primary_key::RowID::new(1, paro_storage::rowset::SegmentRowId::from_raw(5));
    assert!(idx.get(&k).is_none());
    assert_eq!(idx.upsert(k.clone(), loc), None);
    assert_eq!(idx.get(&k), Some(loc));
    assert_eq!(idx.remove(&k), Some(loc));
    assert!(idx.get(&k).is_none());
}

#[test]
fn delete_vector_roundtrip() {
    let mut dv = DeleteVector::new();
    dv.mark_deleted(paro_storage::rowset::SegmentRowId::from_raw(1));
    dv.mark_deleted(paro_storage::rowset::SegmentRowId::from_raw(100));
    let bytes = dv.to_bytes().unwrap();
    let restored = DeleteVector::from_bytes(&bytes).unwrap();
    assert!(restored.is_deleted(paro_storage::rowset::SegmentRowId::from_raw(1)));
    assert!(restored.is_deleted(paro_storage::rowset::SegmentRowId::from_raw(100)));
    assert_eq!(restored.cardinality(), 2);
}

#[test]
fn delta_writer_upsert_dedup_across_batches() {
    let (tablet, _tmp) = create_test_tablet();
    let mut writer = DeltaWriter::open(tablet.clone(), 7).unwrap();
    writer.write_chunk(&chunk_with_range(0, 100)).unwrap();
    writer.write_chunk(&chunk_with_range(50, 150)).unwrap(); // overlaps with first
    let rowset = writer.commit().unwrap();

    // Unique keys should be 150 (0..150)
    assert_eq!(rowset.num_rows(), 150);
    assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 150);
}

#[test]
fn delta_writer_delete_keys_persists_delete_vector() {
    let (tablet, tmp) = create_test_tablet();
    // seed
    let mut writer = DeltaWriter::open(tablet.clone(), 8).unwrap();
    writer.write_chunk(&chunk_with_range(0, 20)).unwrap();
    let rowset = writer.commit().unwrap();
    assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 20);

    // delete first 5
    let del = DeltaWriter::open(tablet.clone(), 9).unwrap();
    let removed = del.delete_keys(&chunk_with_range(0, 5)).unwrap();
    assert_eq!(removed, 5);
    assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 15);

    // DeleteVector persisted
    let dv = DeleteVector::load_from_dir(rowset.rowset_path(), 0)
        .unwrap()
        .unwrap();
    assert_eq!(dv.cardinality(), 5);
    assert!(dv.is_deleted(paro_storage::rowset::SegmentRowId::from_raw(0)));

    drop(tmp);
}

#[test]
fn compaction_plan_picks_candidates_with_high_delete_ratio() {
    let (tablet, _tmp) = create_test_tablet();

    // Rowset 1: write 10 rows
    let mut w1 = DeltaWriter::open(tablet.clone(), 20).unwrap();
    w1.write_chunk(&chunk_with_range(0, 10)).unwrap();
    w1.commit().unwrap();

    // Delete 8 of the 10 rows to create high delete ratio (80%)
    let del = DeltaWriter::open(tablet.clone(), 22).unwrap();
    let _ = del.delete_keys(&chunk_with_range(0, 8)).unwrap();

    // Rowset 2: write 5 more rows (non-overlapping)
    let mut w2 = DeltaWriter::open(tablet.clone(), 21).unwrap();
    w2.write_chunk(&chunk_with_range(10, 15)).unwrap();
    w2.commit().unwrap();

    let plan = CompactionPlanner::plan(tablet.as_ref())
        .unwrap()
        .expect("primary-key compaction plan");
    assert!(!plan.input_rowsets.is_empty());
}

#[test]
fn compaction_execute_merges_rowsets_and_updates_index() {
    let (tablet, _tmp) = create_test_tablet();

    let mut w1 = DeltaWriter::open(tablet.clone(), 30).unwrap();
    w1.write_chunk(&chunk_with_range(0, 10)).unwrap();
    w1.commit().unwrap();

    let mut w2 = DeltaWriter::open(tablet.clone(), 31).unwrap();
    w2.write_chunk(&chunk_with_range(5, 15)).unwrap();
    w2.commit().unwrap();

    assert_eq!(
        tablet
            .capture_consistent_rowsets(tablet.max_version())
            .unwrap()
            .len(),
        2
    );
    run_compaction(&tablet);

    assert_eq!(
        tablet
            .capture_consistent_rowsets(tablet.max_version())
            .unwrap()
            .len(),
        1
    );
    assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 15);
}

#[test]
fn compaction_rebuilds_hnsw_index_and_preserves_cosine_semantics() {
    let (tablet, _tmp) = create_vector_test_tablet();

    let mut w1 = DeltaWriter::open(tablet.clone(), 40).unwrap();
    w1.write_chunk(&chunk_with_vectors(&[0, 1], &[[100.0, 0.0], [1.0, 1.0]]))
        .unwrap();
    w1.commit().unwrap();

    let mut w2 = DeltaWriter::open(tablet.clone(), 41).unwrap();
    w2.write_chunk(&chunk_with_vectors(&[2, 3], &[[0.0, 1.0], [-1.0, 0.0]]))
        .unwrap();
    w2.commit().unwrap();

    assert_eq!(
        tablet
            .capture_consistent_rowsets(tablet.max_version())
            .unwrap()
            .len(),
        2
    );
    run_compaction(&tablet);

    assert_eq!(tablet.num_rowsets(), 1);
    let rowset = tablet.get_rowset_by_version(tablet.max_version()).unwrap();
    rowset.reload().unwrap();

    let segment = rowset.get_segment(0).expect("compaction output segment");
    let hnsw = segment.hnsw_index(1).expect("HNSW index should exist");
    assert_eq!(hnsw.graph.links.num_points(), rowset.num_rows() as usize);

    let stored = segment
        .read_by_rowids(&[1], &[0])
        .expect("read original vector value");
    let bytes = &stored[0].1.data;
    assert_eq!(bytes.len(), 2 * std::mem::size_of::<f32>());
    assert_eq!(f32::from_le_bytes(bytes[0..4].try_into().unwrap()), 100.0);
    assert_eq!(f32::from_le_bytes(bytes[4..8].try_into().unwrap()), 0.0);

    // Cosine query [1,1]: best match should be row idx=1 ([1,1]),
    // not row idx=0 ([100,0]) which would win under plain dot product.
    let params = SearchParams {
        ef: Some(128),
        ..Default::default()
    };
    let results = rowset
        .vector_search(
            1,
            &[1.0, 1.0],
            2,
            &params,
            &HnswSearchPolicy::default(),
            None,
        )
        .unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].idx, 1);
}

#[test]
fn compaction_persists_rowid_mapping_across_restart() {
    let (tablet, tmp, manager) = create_managed_test_tablet();

    let mut w1 = DeltaWriter::open(tablet.clone(), 50).unwrap();
    w1.write_chunk(&chunk_with_range(0, 10)).unwrap();
    w1.commit().unwrap();

    let mut w2 = DeltaWriter::open(tablet.clone(), 51).unwrap();
    w2.write_chunk(&chunk_with_range(5, 15)).unwrap();
    w2.commit().unwrap();

    run_compaction(&tablet);

    assert_eq!(tablet.num_rowsets(), 1);
    let output = tablet
        .rowset_with_max_version()
        .expect("compaction output rowset");
    let output_rowset_id = output.rowset_id();

    let serializer =
        PrimaryKeySerializer::from_schema_ref(&tablet.schema().expect("tablet schema")).unwrap();
    let key_chunk = key_chunk_i32(&[7]);
    let key = serializer.encode_row(&key_chunk, 0).unwrap();
    let before_restart = tablet
        .lookup_primary_key(&key)
        .unwrap()
        .expect("row id before restart");
    assert_eq!(
        tablet.decode_row_id(before_restart).unwrap().rowset_id,
        output_rowset_id
    );

    drop(tablet);
    let reopened = Tablet::open(1, tmp.path(), manager).unwrap();

    assert_eq!(
        reopened
            .capture_consistent_rowsets(reopened.max_version())
            .unwrap()
            .len(),
        1
    );
    let after_restart = reopened
        .lookup_primary_key(&key)
        .unwrap()
        .expect("row id after restart");
    let location = reopened.decode_row_id(after_restart).unwrap();
    assert_eq!(location.rowset_id, output_rowset_id);
    assert!(reopened.find_rowset_by_id(output_rowset_id).is_some());
    assert_eq!(reopened.snapshot_primary_index_entries().unwrap().len(), 15);
}

#[test]
fn compaction_validation_repairs_primary_index_and_persistent_index() {
    let (tablet, _tmp) = create_test_tablet();

    let mut w1 = DeltaWriter::open(tablet.clone(), 60).unwrap();
    w1.write_chunk(&chunk_with_range(0, 10)).unwrap();
    w1.commit().unwrap();

    let mut w2 = DeltaWriter::open(tablet.clone(), 61).unwrap();
    w2.write_chunk(&chunk_with_range(5, 15)).unwrap();
    w2.commit().unwrap();

    run_compaction(&tablet);

    let output = tablet
        .rowset_with_max_version()
        .expect("compaction output rowset");
    let serializer =
        PrimaryKeySerializer::from_schema_ref(&tablet.schema().expect("tablet schema")).unwrap();
    let key_chunk = key_chunk_i32(&[7]);
    let key = serializer.encode_row(&key_chunk, 0).unwrap();

    tablet.remove_primary_index_entry_for_test(&key);
    let persistent = PersistentIndex::new(tablet.data_dir().join("primary_index")).unwrap();
    persistent
        .apply_deletes(std::slice::from_ref(&key))
        .unwrap();

    tablet
        .validate_primary_index_consistency_after_compaction(output.as_ref())
        .unwrap();

    let repaired = tablet
        .lookup_primary_key(&key)
        .unwrap()
        .expect("repaired row id");
    assert_eq!(
        tablet.decode_row_id(repaired).unwrap().rowset_id,
        output.rowset_id()
    );

    let persistent = PersistentIndex::new(tablet.data_dir().join("primary_index")).unwrap();
    assert_eq!(persistent.get(&key).unwrap(), Some(repaired));
}
