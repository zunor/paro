// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::test_utils::test_allocator;
use paro_common::types::LogicalType;
use paro_storage::compaction::compaction_task::{CompactionTask, HorizontalCompactionTask};
use paro_storage::compaction::execution::statistics_merge::merge_rowset_statistics;
use paro_storage::compaction::plan::CompactionPlanner;
use paro_storage::index::fulltext::text_index::FullTextIndex;
use paro_storage::index::hnsw::{
    DistanceMetric, HnswBuildContract, DEFAULT_HNSW_BUILD_SEED, HNSW_BUILD_CONTRACT_VERSION,
};
use paro_storage::primary_key::DeleteVector;
use paro_storage::rowset::{
    ColumnData, Segment, SegmentOptions, SegmentWriter, SegmentWriterOptions,
};
use paro_storage::statistics::{
    ColumnStatistics, FullTextIndexStatistics, IndexType as StatsIndexType, NumericStats,
};
use paro_storage::tablet::{KeysType, Tablet, TabletColumn, TabletRef, TabletSchema};
use paro_storage::write::DeltaWriter;
use tempfile::TempDir;

fn create_pk_schema() -> Arc<TabletSchema> {
    let columns = vec![
        TabletColumn::key(0, "id", LogicalType::Integer),
        TabletColumn::new(1, "v", LogicalType::Integer),
    ];
    Arc::new(TabletSchema::new(9001, columns, KeysType::PrimaryKeys).unwrap())
}

fn create_duplicate_schema() -> Arc<TabletSchema> {
    let mut columns = vec![
        TabletColumn::new(0, "k", LogicalType::Integer),
        TabletColumn::new(1, "v", LogicalType::Integer),
    ];
    columns[0].is_key = true;
    Arc::new(TabletSchema::new(9002, columns, KeysType::DuplicateKeys).unwrap())
}

fn create_tablet(tablet_id: u64, schema: Arc<TabletSchema>) -> (TabletRef, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let tablet = Tablet::new(tablet_id, tablet_id + 10, 0, schema, temp_dir.path(), None).unwrap();
    tablet.init().unwrap();
    (Arc::new(tablet), temp_dir)
}

fn append_rows(tablet: &TabletRef, txn_id: u64, keys: &[i32], values: &[i32]) {
    assert_eq!(keys.len(), values.len());
    let mut writer = DeltaWriter::open(tablet.clone(), txn_id).unwrap();
    let allocator = test_allocator();
    let chunk = Chunk::from_vectors(
        vec![
            paro_common::test_utils::test_i32_vector_with_allocator(keys, allocator.clone()),
            paro_common::test_utils::test_i32_vector_with_allocator(values, allocator.clone()),
        ],
        allocator,
    );
    writer.write_chunk(&chunk).unwrap();
    writer.commit().unwrap();
}

fn assert_i32_min_max(stats: &ColumnStatistics, min: i32, max: i32) {
    let base = stats.statistics();
    assert_eq!(NumericStats::get_min_i32(base), Some(min));
    assert_eq!(NumericStats::get_max_i32(base), Some(max));
}

#[test]
fn test_segment_statistics() {
    let (tablet, _tmp) = create_tablet(9101, create_pk_schema());
    append_rows(&tablet, 1001, &[1, 2, 3, 4, 5], &[10, 20, 30, 40, 50]);

    let rowsets = tablet
        .capture_consistent_rowsets(tablet.max_version())
        .unwrap();
    assert_eq!(rowsets.len(), 1);
    let rowset = rowsets[0].clone();

    let stats = rowset.statistics().unwrap();
    assert_eq!(stats.num_rows, 5);
    assert_eq!(stats.num_segments, 1);
    assert_eq!(stats.delete_stats.num_deleted_rows, 0);
    assert_eq!(stats.delete_stats.effective_row_count, 5);

    let col0 = stats.column(0).expect("column 0 statistics");
    let col1 = stats.column(1).expect("column 1 statistics");
    assert_i32_min_max(&col0.stats, 1, 5);
    assert_i32_min_max(&col1.stats, 10, 50);
    assert_eq!(col0.null_count, 0);
    assert_eq!(col1.null_count, 0);

    let allocator = test_allocator();
    let delete_chunk = Chunk::from_vectors(
        vec![paro_common::test_utils::test_i32_vector_with_allocator(
            &[2, 4],
            allocator.clone(),
        )],
        allocator,
    );
    let writer = DeltaWriter::open(tablet.clone(), 1002).unwrap();
    let deleted = writer.delete_keys(&delete_chunk).unwrap();
    assert_eq!(deleted, 2);

    let stats_after_delete = rowset.statistics().unwrap();
    assert_eq!(stats_after_delete.delete_stats.num_deleted_rows, 2);
    assert_eq!(stats_after_delete.delete_stats.effective_row_count, 3);
    assert!((stats_after_delete.delete_stats.delete_ratio - 0.4).abs() < 1e-9);

    let dv = DeleteVector::load_from_dir(rowset.rowset_path(), 0)
        .unwrap()
        .expect("delete vector should exist");
    assert_eq!(dv.cardinality(), 2);

    let tablet_stats = tablet.statistics().unwrap();
    assert_eq!(tablet_stats.delete_stats.num_deleted_rows, 2);
    assert_eq!(tablet_stats.delete_stats.effective_row_count, 3);
}

#[test]
fn test_statistics_merge() {
    let (tablet, _tmp) = create_tablet(9201, create_duplicate_schema());
    append_rows(&tablet, 2001, &[1, 2, 3], &[10, 20, 30]);
    append_rows(&tablet, 2002, &[4, 5], &[40, 50]);

    let input_rowsets = tablet
        .capture_consistent_rowsets(tablet.max_version())
        .unwrap();
    assert_eq!(input_rowsets.len(), 2);
    let merged_expected = merge_rowset_statistics(&input_rowsets).unwrap();
    assert_eq!(merged_expected.num_rows, 5);
    assert_i32_min_max(&merged_expected.column(0).unwrap().stats, 1, 5);
    assert_i32_min_max(&merged_expected.column(1).unwrap().stats, 10, 50);

    let context = CompactionPlanner::plan(&tablet)
        .unwrap()
        .expect("compaction plan should exist");
    let mut task = HorizontalCompactionTask::new(tablet.clone(), context, test_allocator());
    task.run().unwrap();

    assert_eq!(tablet.num_rowsets(), 1);
    let output_rowset = tablet
        .get_rowset_by_version(tablet.max_version())
        .expect("compaction output rowset should exist");
    let output_stats = output_rowset.statistics().unwrap();

    assert_eq!(output_stats.num_rows, merged_expected.num_rows);
    assert_eq!(
        output_stats.delete_stats.num_deleted_rows,
        merged_expected.delete_stats.num_deleted_rows
    );
    assert_i32_min_max(&output_stats.column(0).unwrap().stats, 1, 5);
    assert_i32_min_max(&output_stats.column(1).unwrap().stats, 10, 50);
}

#[test]
fn test_index_statistics() {
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("segment_with_hnsw.dat");

    let dim = 2usize;
    let schema = Arc::new(
        TabletSchema::new(
            9301,
            vec![
                TabletColumn::new(0, "id", LogicalType::BigInt),
                TabletColumn::new(
                    1,
                    "vec",
                    LogicalType::Array(Box::new(LogicalType::Float), dim),
                ),
            ],
            KeysType::DuplicateKeys,
        )
        .unwrap(),
    );

    let opts = SegmentWriterOptions::new(0)
        .with_short_key_index(false)
        .with_hnsw_build_contract(
            1,
            HnswBuildContract {
                version: HNSW_BUILD_CONTRACT_VERSION,
                m: 8,
                m0: 16,
                ef_construct: 50,
                distance: DistanceMetric::Cosine,
                build_seed: DEFAULT_HNSW_BUILD_SEED,
            },
        );
    let mut writer = SegmentWriter::create(schema.clone(), &segment_path, opts).unwrap();

    let ids = [0_i64, 1, 2, 3];
    let vectors = [[1.0_f32, 0.0_f32], [0.0, 1.0], [1.0, 1.0], [-1.0, 0.5]];

    let mut id_data = Vec::new();
    for id in ids {
        id_data.extend_from_slice(&id.to_le_bytes());
    }

    let mut vec_data = Vec::new();
    for vector in vectors {
        for v in vector {
            vec_data.extend_from_slice(&v.to_le_bytes());
        }
    }

    writer
        .append_chunk(&[
            ColumnData::new(id_data, ids.len() as u32),
            ColumnData::new(vec_data, vectors.len() as u32),
        ])
        .unwrap();
    writer.finalize().unwrap();

    let segment = Segment::open(
        0,
        &segment_path,
        schema,
        SegmentOptions::default().with_verify_checksum(false),
        0,
        0,
        0,
    )
    .unwrap();

    let index_stats = segment.index_statistics();
    let col0 = index_stats.column(0).expect("column 0 index stats");
    let col1 = index_stats.column(1).expect("column 1 index stats");
    assert!(col0
        .iter()
        .any(|s| { s.index_type == StatsIndexType::ZoneMap && s.index_size_bytes > 0 }));
    assert!(col1
        .iter()
        .any(|s| { s.index_type == StatsIndexType::HNSW && s.index_size_bytes > 0 }));

    let hnsw_stats = segment
        .hnsw_index_statistics(1)
        .expect("hnsw stats should exist");
    assert_eq!(hnsw_stats.num_indexed_vectors, 4);
    assert_eq!(hnsw_stats.dimension, dim);
    assert!(hnsw_stats.graph_size_bytes > 0);

    let mut fulltext = FullTextIndex::new_default();
    fulltext.add_document(1, "hello world hello").unwrap();
    fulltext.add_document(2, "world search").unwrap();
    let fulltext_stats = FullTextIndexStatistics::collect(&fulltext);
    assert_eq!(fulltext_stats.total_docs, 2);
    assert_eq!(fulltext_stats.unique_terms, 3);
    assert!((fulltext_stats.avg_doc_length - 2.5).abs() < 1e-6);
}
