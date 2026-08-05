// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::index::{Predicate, PredicateResult, PredicateTree};
use crate::rowset::page::CompressionType;
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
fn segment_predicate_fallback_uses_late_materialize() {
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

    assert!(iter.uses_late_materialize());
    assert!(matches!(iter.evaluated_selection, PredicateResult::Unknown));

    let (rowids, batch) = iter.next_batch(32).unwrap();
    assert_eq!(rowids, vec![7]);
    let predicate_value = i32::from_le_bytes(batch[0].1.data[0..4].try_into().unwrap());
    let projected_value = i32::from_le_bytes(batch[1].1.data[0..4].try_into().unwrap());
    assert_eq!(predicate_value, 7);
    assert_eq!(projected_value, 107);
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
    assert_eq!(matched_rowids.first().copied(), Some(0));
    assert_eq!(matched_rowids.last().copied(), Some(1899));
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
