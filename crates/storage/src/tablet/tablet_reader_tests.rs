// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::codec::vector_decoder;
use crate::index::{Predicate, PredicateTree};
use crate::primary_key::{DeleteVector, RowID};
use crate::rowset::column::ColumnBatch;
use crate::rowset::encoding::BinaryPlainPageBuilder;
use crate::rowset::rowset_meta::{RowsetMetaBuilder, RowsetState};
use crate::rowset::segment::{
    ColumnData, Segment, SegmentIterator, SegmentOptions, SegmentSharedPtr, SegmentWriter,
    SegmentWriterOptions,
};
use crate::rowset::CompressionType;
use crate::rowset::{Rowset, RowsetMeta, RowsetSharedPtr};
use crate::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};
use crate::tablet::{Tablet, Version};
use paro_common::vector::DictionarySource;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

fn create_test_schema() -> TabletSchemaRef {
    let columns = vec![
        TabletColumn::key(0, "id", LogicalType::BigInt),
        TabletColumn::new(1, "name", LogicalType::Varchar),
        TabletColumn::new(2, "value", LogicalType::Integer),
    ];
    Arc::new(TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap())
}

fn create_old_id_only_schema() -> TabletSchemaRef {
    let columns = vec![TabletColumn::key(0, "id", LogicalType::BigInt)];
    Arc::new(TabletSchema::with_version(1, 1, columns, KeysType::PrimaryKeys).unwrap())
}

fn create_schema_with_default_value_column(default_value: i32) -> TabletSchemaRef {
    let columns = vec![
        TabletColumn::key(0, "id", LogicalType::BigInt),
        TabletColumn::new(1, "name", LogicalType::Varchar),
        TabletColumn::new(2, "value", LogicalType::Integer)
            .with_nullable(false)
            .with_default(default_value.to_le_bytes().to_vec()),
    ];
    Arc::new(TabletSchema::with_version(1, 2, columns, KeysType::PrimaryKeys).unwrap())
}

fn create_extended_type_schema() -> TabletSchemaRef {
    let mut columns = vec![
        TabletColumn::new(0, "tiny", LogicalType::TinyInt),
        TabletColumn::new(1, "utiny", LogicalType::UTinyInt),
        TabletColumn::new(2, "small", LogicalType::SmallInt),
        TabletColumn::new(3, "usmall", LogicalType::USmallInt),
        TabletColumn::new(4, "int", LogicalType::Integer),
        TabletColumn::new(5, "uint", LogicalType::UInteger),
        TabletColumn::new(6, "big", LogicalType::BigInt),
        TabletColumn::new(7, "ubig", LogicalType::UBigInt),
        TabletColumn::new(8, "huge", LogicalType::HugeInt),
        TabletColumn::new(9, "uhuge", LogicalType::UHugeInt),
        TabletColumn::new(10, "flag", LogicalType::Boolean),
        TabletColumn::new(11, "flt", LogicalType::Float),
        TabletColumn::new(12, "dbl", LogicalType::Double),
        TabletColumn::new(13, "date_col", LogicalType::Date),
        TabletColumn::new(14, "time_col", LogicalType::Time),
        TabletColumn::new(15, "ts_col", LogicalType::Timestamp),
        TabletColumn::new(16, "interval_col", LogicalType::Interval),
        TabletColumn::new(
            17,
            "decimal_col",
            LogicalType::Decimal {
                precision: 38,
                scale: 6,
            },
        ),
        TabletColumn::new(18, "name", LogicalType::Varchar),
        TabletColumn::new(19, "blob_col", LogicalType::Blob),
        TabletColumn::new(
            20,
            "emb",
            LogicalType::Array(Box::new(LogicalType::Float), 2),
        ),
    ];
    columns[0].is_key = true;
    Arc::new(TabletSchema::new(2, columns, KeysType::DuplicateKeys).unwrap())
}

fn create_test_rowset(id: u64, version: Version, tablet_id: u64) -> RowsetSharedPtr {
    let schema = create_test_schema();
    let meta = RowsetMeta::new(id, tablet_id, version);
    let rowset = Rowset::create(schema, meta, "/tmp/test").unwrap();
    Arc::new(rowset)
}

fn create_test_tablet() -> TabletRef {
    let schema = create_test_schema();
    let tablet = Tablet::new(1, 100, 1000, schema, "/tmp/test", None).unwrap();

    // Add some rowsets
    tablet
        .add_rowset(create_test_rowset(1, Version::singleton(0), 1))
        .unwrap();
    tablet
        .add_rowset(create_test_rowset(2, Version::singleton(1), 1))
        .unwrap();

    Arc::new(tablet)
}

#[test]
fn test_tablet_reader_params_default() {
    let params = TabletReaderParams::default();
    assert_eq!(params.version, i64::MAX);
    assert!(params.columns.is_none());
    assert_eq!(params.batch_size, 4096);
}

#[test]
fn test_tablet_reader_params_builder() {
    let params = TabletReaderParams::with_version(5)
        .with_columns(vec![0, 2])
        .with_batch_size(1024);

    assert_eq!(params.version, 5);
    assert_eq!(params.columns, Some(vec![0, 2]));
    assert_eq!(params.batch_size, 1024);
}

#[test]
fn test_tablet_reader_new() {
    let tablet = create_test_tablet();
    let params = TabletReaderParams::with_version(1);
    let reader = TabletReader::new(tablet, params).unwrap();

    assert_eq!(reader.output_types().len(), 3);
    assert!(!reader.is_prepared);
}

#[test]
fn test_tablet_reader_with_column_projection() {
    let tablet = create_test_tablet();
    let params = TabletReaderParams::with_version(1).with_columns(vec![0, 2]);
    let reader = TabletReader::new(tablet, params).unwrap();

    assert_eq!(reader.output_types().len(), 2);
}

#[test]
fn test_tablet_reader_prepare() {
    let tablet = create_test_tablet();
    let params = TabletReaderParams::with_version(1);
    let mut reader = TabletReader::new(tablet.clone(), params).unwrap();

    reader.prepare().unwrap();
    assert!(reader.is_prepared);
    assert_eq!(reader.num_rowsets(), 2);
}

#[test]
fn test_tablet_reader_version_visibility() {
    let tablet = create_test_tablet();

    // Read at version 0 - should see 1 rowset
    let params = TabletReaderParams::with_version(0);
    let mut reader = TabletReader::new(tablet.clone(), params).unwrap();
    reader.prepare().unwrap();
    assert_eq!(reader.num_rowsets(), 1);

    // Read at version 1 - should see 2 rowsets
    let params = TabletReaderParams::with_version(1);
    let mut reader = TabletReader::new(tablet.clone(), params).unwrap();
    reader.prepare().unwrap();
    assert_eq!(reader.num_rowsets(), 2);
}

#[test]
fn test_tablet_reader_builder() {
    let tablet = create_test_tablet();
    let reader = TabletReaderBuilder::new(tablet)
        .version(1)
        .columns(vec![0, 1])
        .batch_size(2048)
        .build()
        .unwrap();

    assert_eq!(reader.version(), 1);
    assert_eq!(reader.output_types().len(), 2);
}

#[test]
fn test_tablet_reader_get_next_chunk_not_prepared() {
    let tablet = create_test_tablet();
    let params = TabletReaderParams::default();
    let mut reader = TabletReader::new(tablet.clone(), params).unwrap();

    let result = reader.get_next_chunk();
    assert!(result.is_err());
}

#[test]
fn test_build_chunk_supports_extended_types() {
    let schema = create_extended_type_schema();
    let tmp = TempDir::new().unwrap();
    let tablet = Arc::new(Tablet::new(77, 770, 7700, schema, tmp.path(), None).unwrap());
    let params = TabletReaderParams::with_version(0).with_columns((0..21).collect());
    let reader = TabletReader::new(tablet, params).unwrap();

    let rows = 2usize;
    let batch: Vec<(ColumnId, ColumnBatch)> = vec![
        (
            0,
            ColumnBatch::new(bytes::Bytes::from(vec![(-7_i8) as u8, 42_u8]), None),
        ),
        (
            1,
            ColumnBatch::new(bytes::Bytes::from(vec![3_u8, 250_u8]), None),
        ),
        (
            2,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [(-1234_i16).to_le_bytes(), 2345_i16.to_le_bytes()]
                        .concat()
                        .to_vec(),
                ),
                None,
            ),
        ),
        (
            3,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [1234_u16.to_le_bytes(), 5432_u16.to_le_bytes()]
                        .concat()
                        .to_vec(),
                ),
                None,
            ),
        ),
        (
            4,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [(-100_i32).to_le_bytes(), 200_i32.to_le_bytes()]
                        .concat()
                        .to_vec(),
                ),
                None,
            ),
        ),
        (
            5,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [100_u32.to_le_bytes(), 200_u32.to_le_bytes()]
                        .concat()
                        .to_vec(),
                ),
                None,
            ),
        ),
        (
            6,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [(-1000_i64).to_le_bytes(), 2000_i64.to_le_bytes()]
                        .concat()
                        .to_vec(),
                ),
                None,
            ),
        ),
        (
            7,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [1000_u64.to_le_bytes(), 2000_u64.to_le_bytes()]
                        .concat()
                        .to_vec(),
                ),
                None,
            ),
        ),
        (
            8,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [
                        (-1234567890123456789_i128).to_le_bytes(),
                        987654321012345678_i128.to_le_bytes(),
                    ]
                    .concat()
                    .to_vec(),
                ),
                None,
            ),
        ),
        (
            9,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [
                        12345678901234567890_u128.to_le_bytes(),
                        22345678901234567890_u128.to_le_bytes(),
                    ]
                    .concat()
                    .to_vec(),
                ),
                None,
            ),
        ),
        (
            10,
            ColumnBatch::new(bytes::Bytes::from(vec![1_u8, 0_u8]), None),
        ),
        (
            11,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [1.5_f32.to_le_bytes(), 2.5_f32.to_le_bytes()]
                        .concat()
                        .to_vec(),
                ),
                None,
            ),
        ),
        (
            12,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [10.25_f64.to_le_bytes(), (-3.5_f64).to_le_bytes()]
                        .concat()
                        .to_vec(),
                ),
                None,
            ),
        ),
        (
            13,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [100_i32.to_le_bytes(), 200_i32.to_le_bytes()]
                        .concat()
                        .to_vec(),
                ),
                None,
            ),
        ),
        (
            14,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [12345_i64.to_le_bytes(), 54321_i64.to_le_bytes()]
                        .concat()
                        .to_vec(),
                ),
                None,
            ),
        ),
        (
            15,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [777_i64.to_le_bytes(), 888_i64.to_le_bytes()]
                        .concat()
                        .to_vec(),
                ),
                None,
            ),
        ),
        (
            16,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [
                        1111222233334444_i128.to_le_bytes(),
                        (-555566667777888_i128).to_le_bytes(),
                    ]
                    .concat()
                    .to_vec(),
                ),
                None,
            ),
        ),
        (
            17,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [
                        123456789012345678_i128.to_le_bytes(),
                        (-223456789012345678_i128).to_le_bytes(),
                    ]
                    .concat()
                    .to_vec(),
                ),
                None,
            ),
        ),
        (
            18,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [
                        (5_u32).to_le_bytes().to_vec(),
                        b"alpha".to_vec(),
                        (4_u32).to_le_bytes().to_vec(),
                        b"beta".to_vec(),
                    ]
                    .concat(),
                ),
                None,
            ),
        ),
        (
            19,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [
                        (2_u32).to_le_bytes().to_vec(),
                        vec![0xAA_u8, 0xBB_u8],
                        (3_u32).to_le_bytes().to_vec(),
                        vec![0x01_u8, 0x02_u8, 0x03_u8],
                    ]
                    .concat(),
                ),
                None,
            ),
        ),
        (
            20,
            ColumnBatch::new(
                bytes::Bytes::from(
                    [
                        1.0_f32.to_le_bytes(),
                        2.0_f32.to_le_bytes(),
                        3.0_f32.to_le_bytes(),
                        4.0_f32.to_le_bytes(),
                    ]
                    .concat()
                    .to_vec(),
                ),
                None,
            ),
        ),
    ];

    assert_eq!(reader.infer_row_count(&batch, rows).unwrap(), rows);
    let chunk = reader.build_chunk(&batch, rows, &[], 0, 0).unwrap();
    assert_eq!(chunk.column_count(), 21);
    assert_eq!(chunk.len(), rows);

    assert_eq!(chunk.column(0).unwrap().get_i8(0), Some(-7));
    assert_eq!(chunk.column(1).unwrap().get_u8(1), Some(250));
    assert_eq!(chunk.column(2).unwrap().get_i16(0), Some(-1234));
    assert_eq!(chunk.column(3).unwrap().get_u16(1), Some(5432));
    assert_eq!(chunk.column(4).unwrap().get_i32(1), Some(200));
    assert_eq!(chunk.column(5).unwrap().get_u32(0), Some(100));
    assert_eq!(chunk.column(6).unwrap().get_i64(0), Some(-1000));
    assert_eq!(chunk.column(7).unwrap().get_u64(1), Some(2000));
    assert_eq!(
        chunk.column(8).unwrap().get_i128(1),
        Some(987654321012345678_i128)
    );
    assert_eq!(
        chunk.column(9).unwrap().get_u128(0),
        Some(12345678901234567890_u128)
    );
    assert_eq!(chunk.column(10).unwrap().get_bool(0), Some(true));
    assert_eq!(chunk.column(10).unwrap().get_bool(1), Some(false));
    assert_eq!(chunk.column(11).unwrap().get_f32(1), Some(2.5));
    assert_eq!(chunk.column(12).unwrap().get_f64(0), Some(10.25));
    assert_eq!(chunk.column(13).unwrap().get_i32(0), Some(100));
    assert_eq!(chunk.column(14).unwrap().get_i64(1), Some(54321));
    assert_eq!(chunk.column(15).unwrap().get_i64(0), Some(777));
    assert_eq!(
        chunk.column(16).unwrap().get_i128(1),
        Some(-555566667777888_i128)
    );
    assert_eq!(
        chunk.column(17).unwrap().get_i128(0),
        Some(123456789012345678_i128)
    );
    assert_eq!(chunk.column(18).unwrap().get_string(0), Some("alpha"));
    assert_eq!(chunk.column(18).unwrap().get_string(1), Some("beta"));
    assert_eq!(
        chunk.column(19).unwrap().get_blob(0),
        Some([0xAA_u8, 0xBB_u8].as_slice())
    );
    assert_eq!(
        chunk.column(19).unwrap().get_blob(1),
        Some([0x01_u8, 0x02_u8, 0x03_u8].as_slice())
    );
    let arr = chunk.column(20).unwrap();
    let child = arr.child().unwrap();
    assert_eq!(child.get_f32(0), Some(1.0));
    assert_eq!(child.get_f32(1), Some(2.0));
    assert_eq!(child.get_f32(2), Some(3.0));
    assert_eq!(child.get_f32(3), Some(4.0));
}

#[test]
fn test_build_chunk_preserves_storage_dictionary_provenance() {
    let tablet = create_test_tablet();
    let params = TabletReaderParams::with_version(1).with_columns(vec![1]);
    let reader = TabletReader::new(tablet, params).unwrap();

    let mut dictionary = BinaryPlainPageBuilder::new(1024);
    dictionary.add_slice(b"apple");
    dictionary.add_slice(b"banana");
    let dictionary = dictionary.finish().unwrap();

    let codes = bytes::Bytes::from(
        [
            1_u32.to_le_bytes(),
            0_u32.to_le_bytes(),
            1_u32.to_le_bytes(),
        ]
        .concat()
        .to_vec(),
    );
    let batch = vec![(
        1,
        ColumnBatch::with_storage_dictionary(dictionary, codes, None),
    )];

    let chunk = reader.build_chunk(&batch, 3, &[0, 1, 2], 17, 23).unwrap();
    let vector = chunk.column(0).expect("name column should exist");

    assert_eq!(vector.get_string(0), Some("banana"));
    assert_eq!(vector.get_string(1), Some("apple"));
    assert_eq!(vector.get_string(2), Some("banana"));
    let info = vector
        .dictionary_info()
        .expect("storage dictionary info should exist");
    assert_eq!(
        info.provenance_id,
        Some(vector_decoder::storage_dictionary_provenance_id(17, 23, 1))
    );
    assert_eq!(info.source, DictionarySource::Storage);
}

#[test]
fn test_tablet_reader_close() {
    let tablet = create_test_tablet();
    let params = TabletReaderParams::with_version(1);
    let mut reader = TabletReader::new(tablet.clone(), params).unwrap();

    reader.prepare().unwrap();
    assert_eq!(reader.num_rowsets(), 2);

    reader.close();
    assert!(reader.is_finished());
    assert_eq!(reader.num_rowsets(), 0);
}

fn create_segment_with_values(
    schema: &TabletSchemaRef,
    segment_id: u32,
    values: &[i64],
    path: &Path,
) -> SegmentSharedPtr {
    let opts = SegmentWriterOptions::new(segment_id)
        .with_short_key_index(false)
        .with_compression(CompressionType::None);
    let mut writer = SegmentWriter::create(schema.clone(), path, opts).unwrap();

    let num_rows = values.len() as u32;
    let col0_data: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    // varchar column with empty strings
    let col1_data: Vec<u8> = (0..num_rows).flat_map(|_| 0u32.to_le_bytes()).collect();
    // value column simple incremental i32
    let col2_data: Vec<u8> = (0..num_rows)
        .flat_map(|v| (v as i32).to_le_bytes())
        .collect();

    let columns = vec![
        ColumnData::new(col0_data, num_rows),
        ColumnData::new(col1_data, num_rows),
        ColumnData::new(col2_data, num_rows),
    ];
    writer.append_chunk(&columns).unwrap();
    writer.finalize().unwrap();

    Arc::new(
        Segment::open(
            segment_id,
            path,
            schema.clone(),
            SegmentOptions::default().with_verify_checksum(false),
            0,
            0,
            0,
        )
        .unwrap(),
    )
}

fn create_segment_with_names(
    schema: &TabletSchemaRef,
    names: &[&str],
    path: &Path,
) -> SegmentSharedPtr {
    let opts = SegmentWriterOptions::new(0)
        .with_short_key_index(false)
        .with_compression(CompressionType::None);
    let mut writer = SegmentWriter::create(schema.clone(), path, opts).unwrap();
    let rows = names.len() as u32;
    let ids: Vec<u8> = (0..rows)
        .flat_map(|value| i64::from(value).to_le_bytes())
        .collect();
    let mut encoded_names = Vec::new();
    for name in names {
        encoded_names.extend_from_slice(&(name.len() as u32).to_le_bytes());
        encoded_names.extend_from_slice(name.as_bytes());
    }
    let values: Vec<u8> = (0..rows)
        .flat_map(|value| (value as i32).to_le_bytes())
        .collect();
    writer
        .append_chunk(&[
            ColumnData::new(ids, rows),
            ColumnData::new(encoded_names, rows),
            ColumnData::new(values, rows),
        ])
        .unwrap();
    writer.finalize().unwrap();
    Arc::new(
        Segment::open(
            0,
            path,
            schema.clone(),
            SegmentOptions::default().with_verify_checksum(false),
            0,
            0,
            0,
        )
        .unwrap(),
    )
}

fn create_segment_with_values_column0_only(
    schema: &TabletSchemaRef,
    segment_id: u32,
    values: &[i64],
    path: &Path,
) -> SegmentSharedPtr {
    let opts = SegmentWriterOptions::new(segment_id)
        .with_short_key_index(false)
        .with_compression(CompressionType::None);
    let mut writer = SegmentWriter::create(schema.clone(), path, opts).unwrap();
    writer.init_vertical(vec![0], true).unwrap();

    let num_rows = values.len() as u32;
    let col0_data: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    let columns = vec![ColumnData::new(col0_data, num_rows)];
    writer.append_chunk(&columns).unwrap();
    writer.finalize_columns().unwrap();
    let segment = writer.finalize_footer().unwrap();

    Arc::new(segment)
}

fn create_rowset_with_delete_vector(
    base_dir: &Path,
    values: &[i64],
    deleted: &[u32],
) -> RowsetSharedPtr {
    let schema = create_test_schema();
    let rowset_dir = base_dir.join("rowset");
    std::fs::create_dir_all(&rowset_dir).unwrap();

    let segment_path = rowset_dir.join("0.dat");
    let segment = create_segment_with_values(&schema, 0, values, &segment_path);

    let mut dv = DeleteVector::new();
    for d in deleted {
        dv.mark_deleted(crate::rowset::SegmentRowId::from_raw(*d));
    }
    dv.save_to_dir(&rowset_dir, 0).unwrap();

    let meta = RowsetMetaBuilder::with_id(1, 1, Version::singleton(0))
        .num_rows(values.len() as u64)
        .num_segments(1)
        .state(RowsetState::Visible)
        .build();

    Arc::new(Rowset::create_with_segments(schema, meta, &rowset_dir, vec![segment]).unwrap())
}

#[test]
fn test_tablet_reader_applies_delete_vector() {
    let tmp = TempDir::new().unwrap();
    let base = tmp.path();

    let rowset = create_rowset_with_delete_vector(base, &[1, 2, 3], &[1]);

    let schema = create_test_schema();
    let tablet = Arc::new(Tablet::new(1, 100, 1000, schema, base, None).unwrap());
    tablet.add_rowset(rowset).unwrap();

    let params = TabletReaderParams::with_version(0)
        .with_columns(vec![0])
        .with_batch_size(4);
    let mut reader = TabletReader::new(tablet.clone(), params).unwrap();
    reader.prepare().unwrap();

    let chunk = reader
        .get_next_chunk()
        .unwrap()
        .expect("chunk should exist");
    assert_eq!(chunk.len(), 2);
    let col = &chunk.data[0];
    assert_eq!(col.get_i64(0), Some(1));
    assert_eq!(col.get_i64(1), Some(3));
    assert!(reader.get_next_chunk().unwrap().is_none());
}

#[test]
fn test_tablet_reader_duplicate_projection() {
    let tmp = TempDir::new().unwrap();
    let base = tmp.path();

    let schema = create_test_schema();
    let rowset_dir = base.join("rowset");
    std::fs::create_dir_all(&rowset_dir).unwrap();
    let segment_path = rowset_dir.join("0.dat");
    let segment = create_segment_with_values(&schema, 0, &[10, 20, 30], &segment_path);

    let meta = RowsetMetaBuilder::with_id(1, 1, Version::singleton(0))
        .num_rows(3)
        .num_segments(1)
        .state(RowsetState::Visible)
        .build();
    let rowset = Arc::new(
        Rowset::create_with_segments(schema.clone(), meta, &rowset_dir, vec![segment]).unwrap(),
    );

    let tablet = Arc::new(Tablet::new(1, 100, 1000, schema, base, None).unwrap());
    tablet.add_rowset(rowset).unwrap();

    let projection = ColumnProjection::new(vec![0, 0, 2]);
    let params = TabletReaderParams::with_version(0)
        .with_projection(projection)
        .with_batch_size(10);
    let mut reader = TabletReader::new(tablet.clone(), params).unwrap();
    reader.prepare().unwrap();

    let chunk = reader
        .get_next_chunk()
        .unwrap()
        .expect("chunk should exist");
    assert_eq!(chunk.column_count(), 3);
    assert_eq!(chunk.len(), 3);
    assert!(Arc::ptr_eq(&chunk.data[0], &chunk.data[1]));

    let col0 = &chunk.data[0];
    let col1 = &chunk.data[1];
    let col2 = &chunk.data[2];
    assert_eq!(col0.get_i64(0), Some(10));
    assert_eq!(col0.get_i64(1), Some(20));
    assert_eq!(col0.get_i64(2), Some(30));
    assert_eq!(col1.get_i64(0), Some(10));
    assert_eq!(col1.get_i64(1), Some(20));
    assert_eq!(col1.get_i64(2), Some(30));
    assert_eq!(col2.get_i32(0), Some(0));
    assert_eq!(col2.get_i32(1), Some(1));
    assert_eq!(col2.get_i32(2), Some(2));
    assert!(reader.get_next_chunk().unwrap().is_none());
}

#[test]
fn matched_prefix_projection_keeps_stored_sibling_and_emits_compact_value() {
    let tmp = TempDir::new().unwrap();
    let schema = create_test_schema();
    let rowset_dir = tmp.path().join("rowset");
    std::fs::create_dir_all(&rowset_dir).unwrap();
    let segment = create_segment_with_names(
        &schema,
        &["13alice", "31bob", "99drop"],
        &rowset_dir.join("0.dat"),
    );
    let meta = RowsetMetaBuilder::with_id(1, 1, Version::singleton(0))
        .num_rows(3)
        .num_segments(1)
        .state(RowsetState::Visible)
        .build();
    let rowset = Arc::new(
        Rowset::create_with_segments(schema.clone(), meta, &rowset_dir, vec![segment]).unwrap(),
    );
    let tablet = Arc::new(Tablet::new(1, 100, 1000, schema, tmp.path(), None).unwrap());
    tablet.add_rowset(rowset).unwrap();

    let projection = ColumnProjection::try_with_value_projections(
        vec![1, 1],
        vec![
            ColumnValueProjection::Stored,
            ColumnValueProjection::MatchedUtf8Prefix { byte_width: 2 },
        ],
    )
    .unwrap();
    let params = TabletReaderParams::with_version(0)
        .with_projection(projection)
        .with_predicates(PredicateTree::leaf(Predicate::StringPrefixIn {
            column_id: 1,
            prefixes: vec!["13".to_string(), "31".to_string()],
        }))
        .with_late_materialize(vec![1]);
    let mut reader = TabletReader::new(tablet, params).unwrap();
    reader.prepare().unwrap();
    let chunk = reader.get_next_chunk().unwrap().expect("matching rows");
    assert_eq!(chunk.len(), 2);
    assert_eq!(chunk.column(0).unwrap().get_string(0), Some("13alice"));
    assert_eq!(chunk.column(0).unwrap().get_string(1), Some("31bob"));
    assert_eq!(chunk.column(1).unwrap().get_string(0), Some("13"));
    assert_eq!(chunk.column(1).unwrap().get_string(1), Some("31"));
}

#[test]
fn matched_prefix_projection_requires_exact_predicate_witness() {
    let projection = ColumnProjection::try_with_value_projections(
        vec![1],
        vec![ColumnValueProjection::MatchedUtf8Prefix { byte_width: 2 }],
    )
    .unwrap();
    let params = TabletReaderParams::with_version(0).with_projection(projection);
    let error = TabletReader::new(create_test_tablet(), params)
        .expect_err("prefix projection without its predicate witness must be rejected");
    assert!(
        error
            .to_string()
            .contains("lacks its exact VARCHAR predicate witness"),
        "unexpected error: {error}"
    );
}

#[test]
fn test_segment_iterator_skips_sequential_rowids_for_plain_scan() {
    let tmp = TempDir::new().unwrap();
    let schema = create_test_schema();
    let segment_path = tmp.path().join("plain_scan.dat");
    let segment = create_segment_with_values(&schema, 0, &[10, 20, 30, 40], &segment_path);

    let mut iter = SegmentIterator::new_with_delete_vector(&segment, vec![0], None).unwrap();
    let batch = iter.next_batch_with_rowid_policy(2, false).unwrap();

    assert_eq!(batch.rows, 2);
    assert!(batch.rowids.is_empty());
    assert_eq!(batch.columns.len(), 1);
}

#[test]
fn test_segment_iterator_ordinal_range_is_end_exclusive() {
    let tmp = TempDir::new().unwrap();
    let schema = create_test_schema();
    let segment_path = tmp.path().join("ordinal_range.dat");
    let segment = create_segment_with_values(&schema, 0, &[10, 20, 30, 40, 50], &segment_path);

    let mut iter = SegmentIterator::new_with_delete_vector(&segment, vec![0], None).unwrap();
    iter.set_ordinal_range(1, 4).unwrap();
    let batch = iter.next_batch_with_rowid_policy(10, true).unwrap();

    assert_eq!(batch.rows, 3);
    assert_eq!(batch.rowids, vec![1, 2, 3]);
    let values = &batch.columns[0].1.data;
    let decoded = values
        .chunks_exact(std::mem::size_of::<i64>())
        .map(|bytes| i64::from_le_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(decoded, vec![20, 30, 40]);
    assert!(!iter.has_next());
}

#[test]
fn test_tablet_reader_emits_row_id_column() {
    let tmp = TempDir::new().unwrap();
    let base = tmp.path();

    let schema = create_test_schema();
    let rowset_dir = base.join("rowset_rowid");
    std::fs::create_dir_all(&rowset_dir).unwrap();
    let segment_path = rowset_dir.join("0.dat");
    let segment = create_segment_with_values(&schema, 0, &[10, 20, 30], &segment_path);

    let rowset_id = 7u64;
    let meta = RowsetMetaBuilder::with_id(rowset_id, 1, Version::singleton(0))
        .num_rows(3)
        .num_segments(1)
        .state(RowsetState::Visible)
        .build();
    let rowset = Arc::new(
        Rowset::create_with_segments(schema.clone(), meta, &rowset_dir, vec![segment]).unwrap(),
    );

    let tablet = Arc::new(Tablet::new(1, 100, 1000, schema, base, None).unwrap());
    tablet.add_rowset(rowset).unwrap();

    let params = TabletReaderParams::with_version(0)
        .with_columns(vec![0])
        .with_emit_row_id(true)
        .with_batch_size(10);
    let mut reader = TabletReader::new(tablet.clone(), params).unwrap();
    reader.prepare().unwrap();

    let chunk = reader
        .get_next_chunk()
        .unwrap()
        .expect("chunk should exist");
    assert_eq!(chunk.column_count(), 2);
    assert_eq!(chunk.len(), 3);

    let values = &chunk.data[0];
    assert_eq!(values.get_i64(0), Some(10));
    assert_eq!(values.get_i64(1), Some(20));
    assert_eq!(values.get_i64(2), Some(30));

    let rowids = &chunk.data[1];
    for idx in 0..chunk.len() {
        let raw = rowids.get_i64(idx).expect("row_id should exist") as u64;
        let row_id = RowID::from_raw(raw);
        let location = tablet.decode_row_id(row_id).expect("decode row id");
        assert_eq!(location.rowset_id, rowset_id);
        assert_eq!(location.segment_id, 0);
        assert_eq!(location.row_offset, idx as u32);
    }
    assert!(reader.get_next_chunk().unwrap().is_none());
}

#[test]
fn test_rowid_lookup_single_segment_restores_requested_order_without_flattening() {
    let tmp = TempDir::new().unwrap();
    let base = tmp.path();
    let schema = create_test_schema();
    let rowset_dir = base.join("rowset_lookup_permutation");
    std::fs::create_dir_all(&rowset_dir).unwrap();
    let segment_path = rowset_dir.join("0.dat");
    let segment = create_segment_with_values(&schema, 0, &[10, 20, 30], &segment_path);
    let rowset_id = 17u64;
    let meta = RowsetMetaBuilder::with_id(rowset_id, 1, Version::singleton(0))
        .num_rows(3)
        .num_segments(1)
        .state(RowsetState::Visible)
        .build();
    let rowset = Arc::new(
        Rowset::create_with_segments(schema.clone(), meta, &rowset_dir, vec![segment]).unwrap(),
    );
    let tablet = Arc::new(Tablet::new(1, 100, 1000, schema, base, None).unwrap());
    tablet.add_rowset(rowset).unwrap();

    let mut scan = TabletReader::new(
        tablet.clone(),
        TabletReaderParams::with_version(0)
            .with_columns(vec![0])
            .with_emit_row_id(true),
    )
    .unwrap();
    scan.prepare().unwrap();
    let scanned = scan.get_next_chunk().unwrap().unwrap();
    let raw_rowids = (0..scanned.size())
        .map(|index| scanned.column(1).unwrap().get_i64(index).unwrap() as u64)
        .collect::<Vec<_>>();

    let requested = [raw_rowids[2], raw_rowids[0], raw_rowids[2], raw_rowids[1]];
    let fetched = scan.get_by_rowids(&requested, &[2, 0]).unwrap();
    assert_eq!(fetched.size(), 4);
    assert_eq!(fetched.column_count(), 2);
    assert_eq!(fetched.column(0).unwrap().get_i32(0), Some(2));
    assert_eq!(fetched.column(0).unwrap().get_i32(1), Some(0));
    assert_eq!(fetched.column(0).unwrap().get_i32(2), Some(2));
    assert_eq!(fetched.column(0).unwrap().get_i32(3), Some(1));
    assert_eq!(fetched.column(1).unwrap().get_i64(0), Some(30));
    assert_eq!(fetched.column(1).unwrap().get_i64(1), Some(10));
    assert_eq!(fetched.column(1).unwrap().get_i64(2), Some(30));
    assert_eq!(fetched.column(1).unwrap().get_i64(3), Some(20));
    assert_eq!(
        fetched.column(0).unwrap().vector_type(),
        paro_common::vector::VectorType::Dictionary
    );

    let point_reader = crate::tablet::TabletRowIdReader::new(
        tablet.clone(),
        tablet.capture_consistent_rowsets(0).unwrap(),
        &[2, 0],
        Arc::new(paro_common::allocator::default_allocator()),
    )
    .unwrap();
    let fetched = point_reader.get_by_rowids(&requested, &[2, 0]).unwrap();
    assert_eq!(fetched.size(), 4);
    assert_eq!(fetched.column_count(), 2);
    assert_eq!(fetched.column(0).unwrap().get_i32(0), Some(2));
    assert_eq!(fetched.column(0).unwrap().get_i32(1), Some(0));
    assert_eq!(fetched.column(0).unwrap().get_i32(2), Some(2));
    assert_eq!(fetched.column(0).unwrap().get_i32(3), Some(1));
    assert_eq!(fetched.column(1).unwrap().get_i64(0), Some(30));
    assert_eq!(fetched.column(1).unwrap().get_i64(1), Some(10));
    assert_eq!(fetched.column(1).unwrap().get_i64(2), Some(30));
    assert_eq!(fetched.column(1).unwrap().get_i64(3), Some(20));
    assert_eq!(
        fetched.column(0).unwrap().vector_type(),
        paro_common::vector::VectorType::Dictionary
    );
}

#[test]
fn test_tablet_reader_row_id_only_projection() {
    let tmp = TempDir::new().unwrap();
    let base = tmp.path();

    let schema = create_test_schema();
    let rowset_dir = base.join("rowset_rowid_only");
    std::fs::create_dir_all(&rowset_dir).unwrap();
    let segment_path = rowset_dir.join("0.dat");
    let segment = create_segment_with_values(&schema, 0, &[1, 2, 3], &segment_path);

    let rowset_id = 9u64;
    let meta = RowsetMetaBuilder::with_id(rowset_id, 1, Version::singleton(0))
        .num_rows(3)
        .num_segments(1)
        .state(RowsetState::Visible)
        .build();
    let rowset = Arc::new(
        Rowset::create_with_segments(schema.clone(), meta, &rowset_dir, vec![segment]).unwrap(),
    );

    let tablet = Arc::new(Tablet::new(1, 100, 1000, schema, base, None).unwrap());
    tablet.add_rowset(rowset).unwrap();

    let params = TabletReaderParams::with_version(0)
        .with_projection(ColumnProjection::new(Vec::new()))
        .with_emit_row_id(true)
        .with_batch_size(10);
    let mut reader = TabletReader::new(tablet.clone(), params).unwrap();
    reader.prepare().unwrap();

    let chunk = reader
        .get_next_chunk()
        .unwrap()
        .expect("chunk should exist");
    assert_eq!(chunk.column_count(), 1);
    assert_eq!(chunk.len(), 3);
    let rowids = &chunk.data[0];
    for idx in 0..chunk.len() {
        let raw = rowids.get_i64(idx).expect("row_id should exist") as u64;
        let row_id = RowID::from_raw(raw);
        let location = tablet.decode_row_id(row_id).expect("decode row id");
        assert_eq!(location.rowset_id, rowset_id);
        assert_eq!(location.segment_id, 0);
        assert_eq!(location.row_offset, idx as u32);
    }
    assert!(reader.get_next_chunk().unwrap().is_none());
}

#[test]
fn test_tablet_reader_cardinality_only_projection() {
    let tmp = TempDir::new().unwrap();
    let base = tmp.path();

    let schema = create_test_schema();
    let rowset_dir = base.join("rowset_cardinality_only");
    std::fs::create_dir_all(&rowset_dir).unwrap();
    let segment_path = rowset_dir.join("0.dat");
    let segment = create_segment_with_values(&schema, 0, &[1, 2, 3], &segment_path);

    let meta = RowsetMetaBuilder::with_id(10, 1, Version::singleton(0))
        .num_rows(3)
        .num_segments(1)
        .state(RowsetState::Visible)
        .build();
    let rowset = Arc::new(
        Rowset::create_with_segments(schema.clone(), meta, &rowset_dir, vec![segment]).unwrap(),
    );

    let tablet = Arc::new(Tablet::new(1, 100, 1000, schema, base, None).unwrap());
    tablet.add_rowset(rowset).unwrap();

    let params = TabletReaderParams::with_version(0)
        .with_projection(ColumnProjection::new(Vec::new()))
        .with_batch_size(10);
    let mut reader = TabletReader::new(tablet, params).unwrap();
    reader.prepare().unwrap();

    let chunk = reader
        .get_next_chunk()
        .unwrap()
        .expect("cardinality-only chunk should exist");
    assert_eq!(chunk.column_count(), 0);
    assert_eq!(chunk.len(), 3);
    assert!(reader.get_next_chunk().unwrap().is_none());
}

#[test]
fn test_tablet_reader_missing_column_projection() {
    let tmp = TempDir::new().unwrap();
    let base = tmp.path();

    let schema = create_test_schema();
    let rowset_dir = base.join("rowset_missing");
    std::fs::create_dir_all(&rowset_dir).unwrap();
    let segment_path = rowset_dir.join("0.dat");
    let segment = create_segment_with_values_column0_only(&schema, 0, &[1, 2, 3], &segment_path);

    let meta = RowsetMetaBuilder::with_id(1, 1, Version::singleton(0))
        .num_rows(3)
        .num_segments(1)
        .state(RowsetState::Visible)
        .build();
    let rowset = Arc::new(
        Rowset::create_with_segments(schema.clone(), meta, &rowset_dir, vec![segment]).unwrap(),
    );

    let tablet = Arc::new(Tablet::new(1, 100, 1000, schema, base, None).unwrap());
    tablet.add_rowset(rowset).unwrap();

    let projection = ColumnProjection::new(vec![0, 1]);
    let params = TabletReaderParams::with_version(0)
        .with_projection(projection)
        .with_batch_size(10);
    let mut reader = TabletReader::new(tablet, params).unwrap();
    reader.prepare().unwrap();

    let chunk = reader
        .get_next_chunk()
        .unwrap()
        .expect("chunk should exist");
    assert_eq!(chunk.column_count(), 2);
    assert_eq!(chunk.len(), 3);

    let col0 = &chunk.data[0];
    let col1 = &chunk.data[1];
    assert_eq!(col0.get_i64(0), Some(1));
    assert_eq!(col0.get_i64(1), Some(2));
    assert_eq!(col0.get_i64(2), Some(3));
    assert!(col1.is_null(0));
    assert!(col1.is_null(1));
    assert!(col1.is_null(2));
    assert!(reader.get_next_chunk().unwrap().is_none());
}

#[test]
fn test_tablet_reader_adapts_added_nullable_column_from_old_rowset() {
    let tmp = TempDir::new().unwrap();
    let base = tmp.path();

    let old_schema = create_old_id_only_schema();
    let current_schema = create_test_schema();
    let rowset_dir = base.join("rowset_old_nullable");
    std::fs::create_dir_all(&rowset_dir).unwrap();
    let segment_path = rowset_dir.join("0.dat");
    let segment =
        create_segment_with_values_column0_only(&old_schema, 0, &[1, 2, 3], &segment_path);

    let meta = RowsetMetaBuilder::with_id(1, 1, Version::singleton(0))
        .num_rows(3)
        .num_segments(1)
        .schema_hash(11)
        .state(RowsetState::Visible)
        .build();
    let rowset = Arc::new(
        Rowset::create_with_segments(old_schema, meta, &rowset_dir, vec![segment]).unwrap(),
    );

    let tablet = Arc::new(Tablet::new(1, 100, 1000, current_schema, base, None).unwrap());
    tablet.add_rowset(rowset).unwrap();

    let params = TabletReaderParams::with_version(0)
        .with_columns(vec![2])
        .with_batch_size(10);
    let mut reader = TabletReader::new(tablet, params).unwrap();
    reader.prepare().unwrap();

    let chunk = reader
        .get_next_chunk()
        .unwrap()
        .expect("chunk should exist");
    assert_eq!(chunk.column_count(), 1);
    assert_eq!(chunk.len(), 3);
    let col = &chunk.data[0];
    assert!(col.is_null(0));
    assert!(col.is_null(1));
    assert!(col.is_null(2));
    assert!(reader.get_next_chunk().unwrap().is_none());
}

#[test]
fn test_tablet_reader_adapts_added_default_column_from_old_rowset() {
    let tmp = TempDir::new().unwrap();
    let base = tmp.path();

    let old_schema = create_old_id_only_schema();
    let current_schema = create_schema_with_default_value_column(7);
    let rowset_dir = base.join("rowset_old_default");
    std::fs::create_dir_all(&rowset_dir).unwrap();
    let segment_path = rowset_dir.join("0.dat");
    let segment =
        create_segment_with_values_column0_only(&old_schema, 0, &[1, 2, 3], &segment_path);

    let meta = RowsetMetaBuilder::with_id(1, 1, Version::singleton(0))
        .num_rows(3)
        .num_segments(1)
        .schema_hash(11)
        .state(RowsetState::Visible)
        .build();
    let rowset = Arc::new(
        Rowset::create_with_segments(old_schema, meta, &rowset_dir, vec![segment]).unwrap(),
    );

    let tablet = Arc::new(Tablet::new(1, 100, 1000, current_schema, base, None).unwrap());
    tablet.add_rowset(rowset).unwrap();

    let params = TabletReaderParams::with_version(0)
        .with_columns(vec![2])
        .with_batch_size(10);
    let mut reader = TabletReader::new(tablet, params).unwrap();
    reader.prepare().unwrap();

    let chunk = reader
        .get_next_chunk()
        .unwrap()
        .expect("chunk should exist");
    assert_eq!(chunk.column_count(), 1);
    assert_eq!(chunk.len(), 3);
    let col = &chunk.data[0];
    assert_eq!(col.get_i32(0), Some(7));
    assert_eq!(col.get_i32(1), Some(7));
    assert_eq!(col.get_i32(2), Some(7));
    assert!(reader.get_next_chunk().unwrap().is_none());
}
