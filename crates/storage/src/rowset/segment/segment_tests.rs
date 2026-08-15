// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::segment::PositionedFile;
use super::*;
use crate::index::{
    FixedMembership, FixedMembershipBuildPolicy, Predicate, PredicateResult, PredicateTree,
};
use crate::rowset::page::CompressionType;
use crate::rowset::{BatchRowOrdinal, SegmentRowId};
use crate::table::runtime_indexes::RuntimeIndexes;
use crate::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};
use crate::tablet::TabletSchemaRef;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use std::sync::Arc;
use tempfile::TempDir;

fn create_int_schema() -> TabletSchemaRef {
    let columns = vec![
        TabletColumn::key(0, "id", LogicalType::Integer),
        TabletColumn::new(1, "value", LogicalType::Integer),
    ];
    Arc::new(TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap())
}

#[test]
fn positioned_segment_file_cursors_do_not_share_seek_state() {
    use std::io::{Read, Seek, SeekFrom, Write};

    let mut file = tempfile::tempfile().unwrap();
    file.write_all(b"0123456789").unwrap();
    let mut left = PositionedFile::new(Arc::new(file));
    let mut right = left.clone();

    left.seek(SeekFrom::Start(2)).unwrap();
    right.seek(SeekFrom::Start(7)).unwrap();
    let mut left_bytes = [0u8; 2];
    let mut right_bytes = [0u8; 2];
    left.read_exact(&mut left_bytes).unwrap();
    right.read_exact(&mut right_bytes).unwrap();

    assert_eq!(&left_bytes, b"23");
    assert_eq!(&right_bytes, b"78");
    assert_eq!(left.stream_position().unwrap(), 4);
    assert_eq!(right.stream_position().unwrap(), 9);
}

#[test]
fn segment_open_and_iterate_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("roundtrip.seg");
    let schema = create_int_schema();

    let opts = SegmentWriterOptions::new(0).with_compression(CompressionType::None);
    let mut writer = SegmentWriter::create(schema.clone(), &file_path, opts).unwrap();

    let col0_data: Vec<u8> = (0i32..10).flat_map(|v| v.to_le_bytes()).collect();
    let col1_data: Vec<u8> = (100i32..110).flat_map(|v| v.to_le_bytes()).collect();
    writer
        .append_chunk(&[
            ColumnData::new(col0_data, 10),
            ColumnData::new(col1_data, 10),
        ])
        .unwrap();
    writer.finalize().unwrap();

    let segment = Arc::new(
        Segment::open(
            0,
            &file_path,
            schema,
            SegmentOptions::default().with_verify_checksum(false),
            0,
            0,
            0,
        )
        .unwrap(),
    );

    let mut iter = segment.new_iterator().unwrap();
    let (rowids, batch) = iter.next_batch(4).unwrap();
    assert_eq!(rowids, vec![0, 1, 2, 3]);
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].0, 0);
    assert_eq!(batch[1].0, 1);
    assert_eq!(batch[0].1.data.len(), 4 * std::mem::size_of::<i32>());
}

#[test]
fn segment_varlen_column_keeps_rows_across_pages_and_appends() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("varlen-multipage.seg");
    let schema = Arc::new(
        TabletSchema::new(
            1,
            vec![
                TabletColumn::new(0, "id", LogicalType::Integer),
                TabletColumn::new(1, "flag", LogicalType::Varchar),
            ],
            KeysType::DuplicateKeys,
        )
        .unwrap(),
    );
    let opts = SegmentWriterOptions::new(0)
        .with_compression(CompressionType::None)
        .with_page_size(16 * 1024);
    let mut writer = SegmentWriter::create(schema.clone(), &file_path, opts).unwrap();

    let encode_strings = |value: &str, count: usize| {
        let mut data = Vec::with_capacity(count * (value.len() + 4));
        for _ in 0..count {
            data.extend_from_slice(&(value.len() as u32).to_le_bytes());
            data.extend_from_slice(value.as_bytes());
        }
        data
    };
    for (start, count, flag) in [(0_i32, 4096_usize, "N"), (4096, 1909, "R")] {
        let ids = (start..start + count as i32)
            .flat_map(i32::to_le_bytes)
            .collect::<Vec<_>>();
        writer
            .append_chunk(&[
                ColumnData::new(ids, count as u32),
                ColumnData::new(encode_strings(flag, count), count as u32),
            ])
            .unwrap();
    }

    let segment = writer.finalize().unwrap();
    assert_eq!(segment.num_rows(), 6005);
    assert_eq!(segment.get_column_meta(0).unwrap().num_rows, 6005);
    assert_eq!(segment.get_column_meta(1).unwrap().num_rows, 6005);

    let mut iter = segment.new_iterator().unwrap();
    let mut rows = 0;
    while iter.has_next() {
        let batch = iter.next_batch_with_rowid_policy(4096, false).unwrap();
        assert!(batch.rows > 0);
        rows += batch.rows;
    }
    assert_eq!(rows, 6005);
}

#[test]
fn segment_materialized_batch_api_forces_late_materialization() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("predicate.seg");
    let schema = create_int_schema();

    let opts = SegmentWriterOptions::new(0).with_compression(CompressionType::None);
    let mut writer = SegmentWriter::create(schema.clone(), &file_path, opts).unwrap();

    let col0_data: Vec<u8> = (0i32..20).flat_map(|v| v.to_le_bytes()).collect();
    let col1_data: Vec<u8> = (100i32..120).flat_map(|v| v.to_le_bytes()).collect();
    writer
        .append_chunk(&[
            ColumnData::new(col0_data, 20),
            ColumnData::new(col1_data, 20),
        ])
        .unwrap();
    writer.finalize().unwrap();

    let segment = Arc::new(
        Segment::open(
            0,
            &file_path,
            schema,
            SegmentOptions::default().with_verify_checksum(false),
            0,
            0,
            0,
        )
        .unwrap(),
    );

    let adaptive_predicate = PredicateTree::leaf(Predicate::In {
        column_id: 0,
        values: vec![Value::Integer(7), Value::Integer(17)],
    });

    let mut eager = SegmentIterator::new_with_delete_vector_predicate_and_prefetcher(
        &segment,
        vec![0, 1],
        None,
        Some(adaptive_predicate),
        None,
    )
    .unwrap();
    let eager_batch = eager.next_batch_with_rowid_policy(10, false).unwrap();
    assert_eq!(eager_batch.rows, 1);
    assert_eq!(eager_batch.physical_rows, 10);
    assert_eq!(
        eager_batch.selection.as_deref(),
        Some([BatchRowOrdinal::from_index(7)].as_slice())
    );
    assert_eq!(
        i32::from_le_bytes(eager_batch.columns[0].1.data[..4].try_into().unwrap()),
        0
    );
    assert_eq!(
        i32::from_le_bytes(eager_batch.columns[1].1.data[..4].try_into().unwrap()),
        100
    );
    assert!(!eager.uses_late_materialize());

    let second_eager_batch = eager.next_batch_with_rowid_policy(10, false).unwrap();
    assert_eq!(second_eager_batch.rows, 1);
    assert_eq!(second_eager_batch.physical_rows, 10);
    assert_eq!(
        second_eager_batch.selection.as_deref(),
        Some([BatchRowOrdinal::from_index(7)].as_slice())
    );
    assert!(eager.uses_late_materialize());

    let predicate = PredicateTree::leaf(Predicate::Eq {
        column_id: 0,
        value: Value::Integer(7),
    });
    let mut iter = SegmentIterator::new_with_delete_vector_predicate_and_prefetcher(
        &segment,
        vec![0, 1],
        None,
        Some(predicate),
        None,
    )
    .unwrap();

    assert!(!iter.uses_late_materialize());
    assert!(matches!(iter.evaluated_selection, PredicateResult::Unknown));

    let (rowids, batch) = iter.next_batch(32).unwrap();
    assert_eq!(rowids, vec![7]);
    assert!(iter.uses_late_materialize());
    let predicate_value = i32::from_le_bytes(batch[0].1.data[0..4].try_into().unwrap());
    let projected_value = i32::from_le_bytes(batch[1].1.data[0..4].try_into().unwrap());
    assert_eq!(predicate_value, 7);
    assert_eq!(projected_value, 107);
}

#[test]
fn late_materialization_adapts_to_observed_batch_density() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("adaptive_predicate_density.seg");
    let schema = create_int_schema();
    let opts = SegmentWriterOptions::new(0).with_compression(CompressionType::None);
    let mut writer = SegmentWriter::create(schema.clone(), &file_path, opts).unwrap();
    writer
        .append_chunk(&[
            ColumnData::new(
                (0i32..20).flat_map(i32::to_le_bytes).collect::<Vec<_>>(),
                20,
            ),
            ColumnData::new(
                (100i32..120).flat_map(i32::to_le_bytes).collect::<Vec<_>>(),
                20,
            ),
        ])
        .unwrap();
    writer.finalize().unwrap();
    let segment = Arc::new(
        Segment::open(
            0,
            &file_path,
            schema,
            SegmentOptions::default().with_verify_checksum(false),
            0,
            0,
            0,
        )
        .unwrap(),
    );

    let make_iter = |predicate| {
        SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
            &segment,
            vec![1],
            vec![0],
            None,
            Some(predicate),
            None,
        )
        .unwrap()
    };

    let mut dense = make_iter(PredicateTree::leaf(Predicate::Lt {
        column_id: 0,
        value: Value::Integer(18),
    }));
    let first_dense_batch = dense.next_batch_with_rowid_policy(10, false).unwrap();
    assert_eq!(first_dense_batch.rows, 10);
    assert_eq!(first_dense_batch.physical_rows, 10);
    assert!(first_dense_batch.selection.is_none());
    assert!(dense.uses_late_materialize());

    let dense_batch = dense.next_batch_with_rowid_policy(10, false).unwrap();
    assert_eq!(dense_batch.rows, 8);
    assert_eq!(dense_batch.physical_rows, 10);
    let expected_selection = (0..8).map(BatchRowOrdinal::from_index).collect::<Vec<_>>();
    assert_eq!(
        dense_batch.selection.as_deref(),
        Some(expected_selection.as_slice())
    );
    assert!(dense_batch.rowids.is_empty());
    assert_eq!(
        dense_batch.columns[0].1.data.len(),
        10 * std::mem::size_of::<i32>()
    );
    assert!(!dense.uses_late_materialize());

    let mut sparse = make_iter(PredicateTree::leaf(Predicate::Eq {
        column_id: 0,
        value: Value::Integer(7),
    }));
    let sparse_batch = sparse.next_batch_with_rowid_policy(20, false).unwrap();
    assert_eq!(sparse_batch.rows, 1);
    assert_eq!(sparse_batch.physical_rows, 1);
    assert!(sparse_batch.selection.is_none());
    assert!(sparse_batch.rowids.is_empty());
    assert_eq!(
        i32::from_le_bytes(sparse_batch.columns[0].1.data[..4].try_into().unwrap()),
        107
    );
}

#[test]
fn staged_and_gathers_later_predicate_columns_and_preserves_order() {
    const ROWS: usize = 5000;
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("staged-predicate.seg");
    let schema = Arc::new(
        TabletSchema::new(
            1,
            vec![
                TabletColumn::new(0, "size", LogicalType::Integer).with_nullable(true),
                TabletColumn::new(1, "partkey", LogicalType::Integer),
                TabletColumn::new(2, "type", LogicalType::Varchar).with_nullable(true),
                TabletColumn::new(3, "payload", LogicalType::Integer),
            ],
            KeysType::DuplicateKeys,
        )
        .unwrap(),
    );
    let opts = SegmentWriterOptions::new(0)
        .with_compression(CompressionType::None)
        .with_page_size(1024);
    let mut writer = SegmentWriter::create(schema.clone(), &file_path, opts).unwrap();

    let sizes = (0..ROWS)
        .map(|row| if row % 50 == 0 { 15_i32 } else { 7_i32 })
        .flat_map(i32::to_le_bytes)
        .collect::<Vec<_>>();
    let mut size_nulls = vec![0_u8; ROWS.div_ceil(8)];
    // A null in the equality column must never survive, including across a
    // physical page boundary.
    size_nulls[1000 / 8] |= 1 << (1000 % 8);
    size_nulls[1024 / 8] |= 1 << (1024 % 8);
    let partkeys = (0..ROWS as i32)
        .flat_map(i32::to_le_bytes)
        .collect::<Vec<_>>();
    let mut types = Vec::new();
    for row in 0..ROWS {
        let value = if row % 100 == 0 {
            "POLISHED BRASS"
        } else {
            "STEEL"
        };
        types.extend_from_slice(&(value.len() as u32).to_le_bytes());
        types.extend_from_slice(value.as_bytes());
    }
    let mut type_nulls = vec![0_u8; ROWS.div_ceil(8)];
    type_nulls[200 / 8] |= 1 << (200 % 8);
    let payload = (10_000_i32..10_000 + ROWS as i32)
        .flat_map(i32::to_le_bytes)
        .collect::<Vec<_>>();
    writer
        .append_chunk(&[
            ColumnData::with_nulls(sizes, size_nulls, ROWS as u32),
            ColumnData::new(partkeys, ROWS as u32),
            ColumnData::with_nulls(types, type_nulls, ROWS as u32),
            ColumnData::new(payload, ROWS as u32),
        ])
        .unwrap();
    writer.finalize().unwrap();

    let segment = Arc::new(
        Segment::open(
            0,
            &file_path,
            schema,
            SegmentOptions::default().with_verify_checksum(false),
            0,
            0,
            0,
        )
        .unwrap(),
    );
    let membership = (0_i32..ROWS as i32).step_by(100).collect::<Vec<_>>();
    let predicate = PredicateTree::And(vec![
        PredicateTree::leaf(Predicate::Eq {
            column_id: 0,
            value: Value::Integer(15),
        }),
        PredicateTree::leaf(Predicate::FixedIn {
            column_id: 1,
            values: FixedMembership::i32_with_policy(
                membership,
                FixedMembershipBuildPolicy::new(1 << 20, 256),
            ),
        }),
        PredicateTree::leaf(Predicate::StringLike {
            column_id: 2,
            pattern: "%BRASS".to_string(),
            negated: false,
        }),
    ]);
    let mut iter =
        SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
            &segment,
            vec![0, 1, 2, 3],
            vec![0, 1, 2],
            None,
            Some(predicate),
            None,
        )
        .unwrap();

    let mut rowids = Vec::new();
    let mut projected_sizes = Vec::new();
    let mut projected_keys = Vec::new();
    let mut payloads = Vec::new();
    while iter.has_next() {
        let (batch_rowids, columns) = iter.next_batch(777).unwrap();
        if batch_rowids.is_empty() {
            break;
        }
        projected_sizes.extend(
            columns[0]
                .1
                .data
                .chunks_exact(4)
                .map(|value| i32::from_le_bytes(value.try_into().unwrap())),
        );
        projected_keys.extend(
            columns[1]
                .1
                .data
                .chunks_exact(4)
                .map(|value| i32::from_le_bytes(value.try_into().unwrap())),
        );
        rowids.extend(batch_rowids);
        payloads.extend(
            columns[3]
                .1
                .data
                .chunks_exact(4)
                .map(|value| i32::from_le_bytes(value.try_into().unwrap())),
        );
    }

    let expected = (0_u32..ROWS as u32)
        .step_by(100)
        .filter(|row| *row != 200 && *row != 1000)
        .collect::<Vec<_>>();
    assert_eq!(rowids, expected);
    assert_eq!(projected_sizes, vec![15; rowids.len()]);
    assert_eq!(
        projected_keys,
        rowids
            .iter()
            .map(|row| row.get() as i32)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        payloads,
        rowids
            .iter()
            .map(|row| 10_000 + row.get() as i32)
            .collect::<Vec<_>>()
    );
    let stats = iter.predicate_stage_read_stats();
    assert_eq!(stats.stages.len(), 3);
    assert_eq!(stats.stages[0].sequential_rows, ROWS as u64);
    assert_eq!(stats.stages[1].sequential_rows, 0);
    assert!(stats.stages[1].gathered_rows <= (ROWS / 50 + 1) as u64);
    assert_eq!(stats.stages[2].sequential_rows, 0);
    assert!(stats.stages[2].gathered_rows <= (ROWS / 100 + 1) as u64);
}

#[test]
fn staged_and_switches_access_modes_groups_same_column_and_accepts_sorted_membership() {
    const ROWS: usize = 32;
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("staged-access-switch.seg");
    let schema = Arc::new(
        TabletSchema::new(
            1,
            vec![
                TabletColumn::new(0, "gate", LogicalType::Integer),
                TabletColumn::new(1, "key", LogicalType::Integer),
                TabletColumn::new(2, "label", LogicalType::Varchar).with_nullable(true),
                TabletColumn::new(3, "payload", LogicalType::Integer),
            ],
            KeysType::DuplicateKeys,
        )
        .unwrap(),
    );
    let mut writer = SegmentWriter::create(
        schema.clone(),
        &file_path,
        SegmentWriterOptions::new(0)
            .with_compression(CompressionType::None)
            .with_page_size(64),
    )
    .unwrap();
    let gate_matches = |row: usize| {
        let offset = row % 8;
        if row / 8 % 2 == 0 {
            offset < 6
        } else {
            offset == 0 || offset == 7
        }
    };
    let gates = (0..ROWS)
        .map(|row| if gate_matches(row) { 7_i32 } else { 0_i32 })
        .flat_map(i32::to_le_bytes)
        .collect::<Vec<_>>();
    let keys = (0..ROWS as i32)
        .flat_map(i32::to_le_bytes)
        .collect::<Vec<_>>();
    let mut labels = Vec::new();
    for row in 0..ROWS {
        let value = if row % 3 == 0 { "ABZ" } else { "ABX" };
        labels.extend_from_slice(&(value.len() as u32).to_le_bytes());
        labels.extend_from_slice(value.as_bytes());
    }
    let mut label_nulls = vec![0_u8; ROWS.div_ceil(8)];
    label_nulls[15 / 8] |= 1 << (15 % 8);
    let payload = (100_i32..100 + ROWS as i32)
        .flat_map(i32::to_le_bytes)
        .collect::<Vec<_>>();
    writer
        .append_chunk(&[
            ColumnData::new(gates, ROWS as u32),
            ColumnData::new(keys, ROWS as u32),
            ColumnData::with_nulls(labels, label_nulls, ROWS as u32),
            ColumnData::new(payload, ROWS as u32),
        ])
        .unwrap();
    writer.finalize().unwrap();

    let segment = Arc::new(
        Segment::open(
            0,
            &file_path,
            schema,
            SegmentOptions::default().with_verify_checksum(false),
            0,
            0,
            0,
        )
        .unwrap(),
    );
    let membership = (0..ROWS)
        .filter(|row| gate_matches(*row))
        .map(|row| row as i32)
        .collect::<Vec<_>>();
    let predicate = PredicateTree::And(vec![
        PredicateTree::leaf(Predicate::Eq {
            column_id: 0,
            value: Value::Integer(7),
        }),
        PredicateTree::leaf(Predicate::FixedIn {
            column_id: 1,
            // A zero dense budget deliberately retains the sorted runtime
            // representation; the other staged test covers dense membership.
            values: FixedMembership::i32_with_policy(
                membership,
                FixedMembershipBuildPolicy::new(0, 0),
            ),
        }),
        PredicateTree::leaf(Predicate::StringPrefix {
            column_id: 2,
            prefix: "A".to_string(),
            negated: false,
        }),
        PredicateTree::leaf(Predicate::StringLike {
            column_id: 2,
            pattern: "%Z".to_string(),
            negated: false,
        }),
    ]);
    let mut iter =
        SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
            &segment,
            vec![0, 1, 2, 3],
            vec![0, 1, 2],
            None,
            Some(predicate),
            None,
        )
        .unwrap();

    let mut rowids = Vec::new();
    let mut payloads = Vec::new();
    while iter.has_next() {
        let (batch_rowids, columns) = iter.next_batch(8).unwrap();
        if batch_rowids.is_empty() {
            break;
        }
        rowids.extend(batch_rowids);
        payloads.extend(
            columns[3]
                .1
                .data
                .chunks_exact(4)
                .map(|value| i32::from_le_bytes(value.try_into().unwrap())),
        );
    }

    let expected = (0_u32..ROWS as u32)
        .filter(|row| gate_matches(*row as usize) && row % 3 == 0 && *row != 15)
        .collect::<Vec<_>>();
    assert_eq!(rowids, expected);
    assert_eq!(
        payloads,
        rowids
            .iter()
            .map(|row| 100 + row.get() as i32)
            .collect::<Vec<_>>()
    );
    let stats = iter.predicate_stage_read_stats();
    // Prefix and suffix LIKE share one physical column stage.
    assert_eq!(stats.stages.len(), 3);
    for stage in &stats.stages[1..] {
        assert!(stage.sequential_rows > 0);
        assert!(stage.gathered_rows > 0);
    }
}

#[test]
fn staged_predicates_fall_back_for_or() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("staged-or-fallback.seg");
    let schema = create_int_schema();
    let mut writer = SegmentWriter::create(
        schema.clone(),
        &file_path,
        SegmentWriterOptions::new(0).with_compression(CompressionType::None),
    )
    .unwrap();
    writer
        .append_chunk(&[
            ColumnData::new(
                (0_i32..10).flat_map(i32::to_le_bytes).collect::<Vec<_>>(),
                10,
            ),
            ColumnData::new(
                (10_i32..20).flat_map(i32::to_le_bytes).collect::<Vec<_>>(),
                10,
            ),
        ])
        .unwrap();
    writer.finalize().unwrap();
    let segment = Arc::new(
        Segment::open(
            0,
            &file_path,
            schema,
            SegmentOptions::default().with_verify_checksum(false),
            0,
            0,
            0,
        )
        .unwrap(),
    );
    let predicate = PredicateTree::Or(vec![
        PredicateTree::leaf(Predicate::Eq {
            column_id: 0,
            value: Value::Integer(2),
        }),
        PredicateTree::leaf(Predicate::Eq {
            column_id: 1,
            value: Value::Integer(17),
        }),
    ]);
    let mut iter =
        SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
            &segment,
            vec![0],
            vec![0, 1],
            None,
            Some(predicate),
            None,
        )
        .unwrap();
    assert_eq!(iter.next_batch(10).unwrap().0, [2, 7]);
    assert!(iter.predicate_stage_read_stats().stages.is_empty());
}

#[test]
fn segment_reused_predicate_column_preserves_nulls_from_or_matches() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("predicate_reuse_nulls.seg");
    let schema = Arc::new(
        TabletSchema::new(
            1,
            vec![
                TabletColumn::new(0, "nullable", LogicalType::Integer).with_nullable(true),
                TabletColumn::new(1, "fallback", LogicalType::Integer),
            ],
            KeysType::DuplicateKeys,
        )
        .unwrap(),
    );
    let opts = SegmentWriterOptions::new(0).with_compression(CompressionType::None);
    let mut writer = SegmentWriter::create(schema.clone(), &file_path, opts).unwrap();
    let nullable = [1_i32, 0, 3]
        .into_iter()
        .flat_map(i32::to_le_bytes)
        .collect::<Vec<_>>();
    let fallback = [0_i32, 1, 0]
        .into_iter()
        .flat_map(i32::to_le_bytes)
        .collect::<Vec<_>>();
    writer
        .append_chunk(&[
            ColumnData::with_nulls(nullable, vec![0b0000_0010_u8], 3),
            ColumnData::new(fallback, 3),
        ])
        .unwrap();
    writer.finalize().unwrap();

    let segment = Arc::new(
        Segment::open(
            0,
            &file_path,
            schema,
            SegmentOptions::default().with_verify_checksum(false),
            0,
            0,
            0,
        )
        .unwrap(),
    );
    let predicate = PredicateTree::Or(vec![
        PredicateTree::leaf(Predicate::Eq {
            column_id: 0,
            value: Value::Integer(1),
        }),
        PredicateTree::leaf(Predicate::Eq {
            column_id: 1,
            value: Value::Integer(1),
        }),
    ]);
    let mut iter = SegmentIterator::new_with_delete_vector_predicate_and_prefetcher(
        &segment,
        vec![0],
        None,
        Some(predicate),
        None,
    )
    .unwrap();

    let (rowids, columns) = iter.next_batch(32).unwrap();

    assert_eq!(rowids, [0, 1]);
    assert_eq!(columns[0].1.nulls.as_deref(), Some([0_u8, 1].as_slice()));
}

#[test]
fn segment_bitmap_predicate_skips_late_materialize_even_with_explicit_columns() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("bitmap_predicate.seg");
    let schema = create_int_schema();

    let opts = SegmentWriterOptions::new(0)
        .with_compression(CompressionType::None)
        .with_bitmap_index_columns(vec![0]);
    let mut writer = SegmentWriter::create(schema.clone(), &file_path, opts).unwrap();

    let ids = [1i32, 2, 1, 3];
    let values = [100i32, 200, 300, 400];
    writer
        .append_chunk(&[
            ColumnData::new(
                ids.into_iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<u8>>(),
                4,
            ),
            ColumnData::new(
                values
                    .into_iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<u8>>(),
                4,
            ),
        ])
        .unwrap();
    writer.finalize().unwrap();

    let segment = Arc::new(
        Segment::open(
            0,
            &file_path,
            schema,
            SegmentOptions::default().with_verify_checksum(false),
            0,
            0,
            0,
        )
        .unwrap(),
    );

    let predicate = PredicateTree::leaf(Predicate::Eq {
        column_id: 0,
        value: Value::Integer(1),
    });

    let mut iter =
        SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
            &segment,
            vec![1],
            vec![0],
            None,
            Some(predicate),
            None,
        )
        .unwrap();

    assert!(!iter.uses_late_materialize());
    assert!(matches!(
        iter.evaluated_selection,
        PredicateResult::Bitmap(_)
    ));

    let (rowids, batch) = iter.next_batch(8).unwrap();
    assert_eq!(rowids, vec![0, 2]);
    let values: Vec<i32> = batch[0]
        .1
        .data
        .chunks_exact(4)
        .map(|chunk| i32::from_le_bytes(chunk.try_into().unwrap()))
        .collect();
    assert_eq!(values, vec![100, 300]);
}

#[test]
fn segment_bitmap_range_predicate_verifies_rows_after_index_pruning() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("bitmap_range_predicate.seg");
    let schema = create_int_schema();

    let opts = SegmentWriterOptions::new(0)
        .with_compression(CompressionType::None)
        .with_bitmap_index_columns(vec![0]);
    let mut writer = SegmentWriter::create(schema.clone(), &file_path, opts).unwrap();

    let ids: Vec<i32> = (0..2000).map(|v| v % 1000).collect();
    let values: Vec<i32> = ids.iter().map(|v| v * 10).collect();
    writer
        .append_chunk(&[
            ColumnData::new(
                ids.iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<u8>>(),
                ids.len() as u32,
            ),
            ColumnData::new(
                values
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<u8>>(),
                values.len() as u32,
            ),
        ])
        .unwrap();
    writer.finalize().unwrap();

    let segment = Arc::new(
        Segment::open(
            0,
            &file_path,
            schema,
            SegmentOptions::default().with_verify_checksum(false),
            0,
            0,
            0,
        )
        .unwrap(),
    );

    let predicate = PredicateTree::leaf(Predicate::Lt {
        column_id: 0,
        value: Value::Integer(900),
    });

    let mut iter =
        SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
            &segment,
            vec![1],
            vec![0],
            None,
            Some(predicate),
            None,
        )
        .unwrap();

    assert!(iter.uses_late_materialize());
    assert!(matches!(iter.evaluated_selection, PredicateResult::Unknown));

    let mut matched_rowids = Vec::new();
    let mut matched_values = Vec::new();
    while iter.has_next() {
        let (rowids, batch) = iter.next_batch(1024).unwrap();
        if rowids.is_empty() {
            break;
        }
        matched_rowids.extend(rowids);
        matched_values.extend(
            batch[0]
                .1
                .data
                .chunks_exact(4)
                .map(|chunk| i32::from_le_bytes(chunk.try_into().unwrap())),
        );
    }
    assert_eq!(matched_rowids.len(), 1800);
    assert_eq!(
        matched_rowids.first().copied(),
        Some(SegmentRowId::from_raw(0))
    );
    assert_eq!(
        matched_rowids.last().copied(),
        Some(SegmentRowId::from_raw(1899))
    );
    assert_eq!(matched_values.len(), 1800);
    assert_eq!(matched_values[0], 0);
    assert_eq!(matched_values[899], 8990);
    assert_eq!(matched_values[900], 0);
    assert_eq!(matched_values[1799], 8990);
}

#[test]
fn segment_runtime_art_predicate_switches_between_bitmap_and_fallback() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("runtime_art.seg");
    let schema = create_int_schema();

    let opts = SegmentWriterOptions::new(0).with_compression(CompressionType::None);
    let mut writer = SegmentWriter::create(schema.clone(), &file_path, opts).unwrap();

    let col0_data: Vec<u8> = (0i32..20).flat_map(|v| v.to_le_bytes()).collect();
    let col1_data: Vec<u8> = (100i32..120).flat_map(|v| v.to_le_bytes()).collect();
    writer
        .append_chunk(&[
            ColumnData::new(col0_data, 20),
            ColumnData::new(col1_data, 20),
        ])
        .unwrap();
    writer.finalize().unwrap();

    let segment = Arc::new(
        Segment::open(
            0,
            &file_path,
            schema,
            SegmentOptions::default().with_verify_checksum(false),
            0,
            0,
            0,
        )
        .unwrap(),
    );

    let predicate = PredicateTree::leaf(Predicate::Eq {
        column_id: 0,
        value: Value::Integer(7),
    });

    let fallback_iter =
        SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
            &segment,
            vec![1],
            vec![0],
            None,
            Some(predicate.clone()),
            None,
        )
        .unwrap();
    assert!(fallback_iter.uses_late_materialize());
    assert!(matches!(
        fallback_iter.evaluated_selection,
        PredicateResult::Unknown
    ));

    RuntimeIndexes::rebuild_art_index_for_segment(&segment, 0).unwrap();
    assert!(segment.art_index(0).is_some());

    let mut art_iter =
        SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
            &segment,
            vec![1],
            vec![0],
            None,
            Some(predicate.clone()),
            None,
        )
        .unwrap();
    assert!(!art_iter.uses_late_materialize());
    assert!(matches!(
        art_iter.evaluated_selection,
        PredicateResult::Bitmap(_)
    ));

    let (rowids, batch) = art_iter.next_batch(8).unwrap();
    assert_eq!(rowids, vec![7]);
    let value = i32::from_le_bytes(batch[0].1.data[0..4].try_into().unwrap());
    assert_eq!(value, 107);

    segment.drop_art_index(0);
    assert!(segment.art_index(0).is_none());

    let removed_iter =
        SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
            &segment,
            vec![1],
            vec![0],
            None,
            Some(predicate),
            None,
        )
        .unwrap();
    assert!(removed_iter.uses_late_materialize());
    assert!(matches!(
        removed_iter.evaluated_selection,
        PredicateResult::Unknown
    ));
}

#[test]
fn segment_short_key_index_uses_canonical_decoder() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("short_key.seg");
    let schema = create_int_schema();

    let opts = SegmentWriterOptions::new(0)
        .with_compression(CompressionType::None)
        .with_short_key_index(true);
    let mut writer = SegmentWriter::create(schema.clone(), &file_path, opts).unwrap();

    for chunk_start in [0i32, 10, 20] {
        let col0_data: Vec<u8> = (chunk_start..chunk_start + 10)
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let col1_data: Vec<u8> = (100 + chunk_start..110 + chunk_start)
            .flat_map(|v| v.to_le_bytes())
            .collect();
        writer
            .append_chunk(&[
                ColumnData::new(col0_data, 10),
                ColumnData::new(col1_data, 10),
            ])
            .unwrap();
    }
    writer.finalize().unwrap();

    let segment = Segment::open(
        0,
        &file_path,
        schema,
        SegmentOptions::default().with_verify_checksum(false),
        0,
        0,
        0,
    )
    .unwrap();

    let decoder = segment.short_key_index().unwrap().unwrap();
    assert_eq!(decoder.num_items(), 3);
    assert_eq!(decoder.footer().num_segment_rows, 30);

    let key_15 = 15i32.to_le_bytes();
    let iter = decoder.lower_bound(&key_15);
    assert!(iter.valid());
    assert_eq!(iter.ordinal(), 2);
    assert_eq!(iter.key(), 20i32.to_le_bytes());
}
