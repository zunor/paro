// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Index Builder
//!
//! Trait and implementations for building indexes from table data.
//!
//! ## Design Notes
//!
//! The `IndexBuilder` trait provides a standardized interface for building indexes
//! from table data. Each index type registers its own build functions via callbacks.
//!
//! The build process consists of:
//! 1. **Bind**: Gather metadata and validate the index creation request
//! 2. **Init Global**: Create shared state for parallel building
//! 3. **Init Local**: Create per-thread state
//! 4. **Sink**: Process data chunks and insert into the index
//! 5. **Combine**: Merge local states into global state
//! 6. **Finalize**: Complete the index and return the bound index

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

use super::bitmap::{BitmapIndex, BitmapIndexWriter};
use super::bloom::{BloomFilterIndex, BloomFilterIndexWriter, BloomFilterOptions};
use super::{
    BoundIndex, ColumnId, CreateIndexInput, IndexBuildBindData, IndexBuildBindInput,
    IndexBuildCombineInput, IndexBuildFinalizeInput, IndexBuildGlobalState,
    IndexBuildInitGlobalStateInput, IndexBuildInitLocalStateInput, IndexBuildLocalState,
    IndexBuildSinkInput, IndexBuildSortInput, IndexType,
};
use crate::index::predicate_result::PageRange;
use crate::index::value_to_bytes;

// =============================================================================
// Bloom Filter Index Builder
// =============================================================================

#[derive(Debug)]
pub struct BloomBuildBindData {
    pub index_name: String,
    pub column_ids: Vec<ColumnId>,
    pub logical_types: Vec<LogicalType>,
    pub options: HashMap<String, Value>,
}

impl IndexBuildBindData for BloomBuildBindData {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct BloomBuildGlobalState {
    pub writer: Mutex<BloomFilterIndexWriter>,
    pub page_ranges: Mutex<Vec<PageRange>>,
    pub rows_indexed: Mutex<u64>,
    pub logical_type: LogicalType,
}

impl std::fmt::Debug for BloomBuildGlobalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomBuildGlobalState")
            .field(
                "rows_indexed",
                &self.rows_indexed.lock().map(|v| *v).unwrap_or(0),
            )
            .finish()
    }
}

impl IndexBuildGlobalState for BloomBuildGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct BloomBuildLocalState;

impl IndexBuildLocalState for BloomBuildLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub fn bloom_build_bind(input: &IndexBuildBindInput) -> Result<Box<dyn IndexBuildBindData>> {
    if input.logical_types.len() != 1 {
        return Err(paro_error::invalid_input(
            "Bloom filter index only supports single-column indexes",
        ));
    }

    let col_type = &input.logical_types[0];
    if !is_scalar_supported_type(col_type) {
        return Err(paro_error::type_mismatch(format!(
            "Column type {:?} is not supported by Bloom filter index",
            col_type
        )));
    }

    Ok(Box::new(BloomBuildBindData {
        index_name: input.index_name.to_string(),
        column_ids: input.column_ids.to_vec(),
        logical_types: input.logical_types.to_vec(),
        options: input.options.clone(),
    }))
}

pub fn bloom_build_sort(_input: &IndexBuildSortInput) -> bool {
    false
}

pub fn bloom_build_global_init(
    input: &IndexBuildInitGlobalStateInput,
) -> Result<Box<dyn IndexBuildGlobalState>> {
    let opts = if let Some(bind_data) = input.bind_data {
        let bind = bind_data
            .as_any()
            .downcast_ref::<BloomBuildBindData>()
            .ok_or_else(|| paro_error::internal("Invalid bind data for bloom index"))?;
        parse_bloom_options(&bind.options)
    } else {
        BloomFilterOptions::default()
    };

    let logical_type = input
        .logical_types
        .first()
        .cloned()
        .unwrap_or(LogicalType::Unknown);

    Ok(Box::new(BloomBuildGlobalState {
        writer: Mutex::new(BloomFilterIndexWriter::new(opts)),
        page_ranges: Mutex::new(Vec::new()),
        rows_indexed: Mutex::new(0),
        logical_type,
    }))
}

pub fn bloom_build_local_init(
    _input: &IndexBuildInitLocalStateInput,
) -> Result<Box<dyn IndexBuildLocalState>> {
    Ok(Box::new(BloomBuildLocalState))
}

pub fn bloom_build_sink(
    input: &mut IndexBuildSinkInput,
    key_chunk: &Chunk,
    _row_ids: &[u64],
) -> Result<()> {
    let gstate = input
        .global_state
        .as_any()
        .downcast_ref::<BloomBuildGlobalState>()
        .ok_or_else(|| paro_error::internal("Invalid global state type"))?;

    let row_count = key_chunk.size();
    if row_count == 0 {
        return Ok(());
    }

    let vector = key_chunk
        .column(0)
        .ok_or_else(|| paro_error::internal("Missing index column"))?;

    let mut writer = gstate
        .writer
        .lock()
        .map_err(|_| paro_error::internal("Failed to lock bloom writer"))?;
    let mut rows_indexed = gstate
        .rows_indexed
        .lock()
        .map_err(|_| paro_error::internal("Failed to lock bloom rows counter"))?;

    let start = *rows_indexed;
    for row_idx in 0..row_count {
        if vector.is_null(row_idx) {
            writer.add_nulls(1);
            *rows_indexed += 1;
            continue;
        }
        let value = vector.get_value(row_idx);
        let bytes = value_to_bytes(&value, &gstate.logical_type)?;
        writer.add_value(&bytes);
        *rows_indexed += 1;
    }
    let end = *rows_indexed;
    writer.flush();

    if end > start {
        let start_u32 = u32::try_from(start)
            .map_err(|_| paro_error::out_of_range("Bloom filter row id exceeds u32 range"))?;
        let end_u32 = u32::try_from(end)
            .map_err(|_| paro_error::out_of_range("Bloom filter row id exceeds u32 range"))?;
        let mut ranges = gstate
            .page_ranges
            .lock()
            .map_err(|_| paro_error::internal("Failed to lock bloom ranges"))?;
        ranges.push(PageRange::new(start_u32, end_u32));
    }

    Ok(())
}

pub fn bloom_build_combine(_input: &mut IndexBuildCombineInput) -> Result<()> {
    Ok(())
}

pub fn bloom_build_finalize(input: IndexBuildFinalizeInput) -> Result<Arc<dyn BoundIndex>> {
    let gstate = input
        .global_state
        .as_any()
        .downcast_ref::<BloomBuildGlobalState>()
        .ok_or_else(|| paro_error::internal("Invalid global state type"))?;

    let mut writer = gstate
        .writer
        .lock()
        .map_err(|_| paro_error::internal("Failed to lock bloom writer"))?;
    let ranges = gstate
        .page_ranges
        .lock()
        .map_err(|_| paro_error::internal("Failed to lock bloom ranges"))?;

    let index = BloomFilterIndex::from_writer(
        input.name,
        input.constraint_type,
        input.column_ids.to_vec(),
        input.logical_types.to_vec(),
        &mut writer,
        ranges.clone(),
    )?;

    Ok(Arc::new(index))
}

fn parse_bloom_options(options: &HashMap<String, Value>) -> BloomFilterOptions {
    let mut opts = BloomFilterOptions::default();

    if let Some(value) = options.get("fpp") {
        match value {
            Value::Double(v) => opts = opts.with_fpp(*v),
            Value::Float(v) => opts = opts.with_fpp(*v as f64),
            _ => {}
        }
    }

    if let Some(value) = options.get("expected_entries") {
        match value {
            Value::Integer(v) => opts = opts.with_expected_entries(*v as usize),
            Value::BigInt(v) => opts = opts.with_expected_entries(*v as usize),
            Value::UInteger(v) => opts = opts.with_expected_entries(*v as usize),
            Value::UBigInt(v) => opts = opts.with_expected_entries(*v as usize),
            _ => {}
        }
    }

    opts
}

pub fn bloom_create_instance(input: &CreateIndexInput) -> Result<Arc<dyn BoundIndex>> {
    BloomFilterIndex::from_storage_info(input)
}

pub fn get_bloom_index_type() -> IndexType {
    IndexType::new(BloomFilterIndex::TYPE_NAME)
        .with_build_bind(bloom_build_bind)
        .with_build_sort(bloom_build_sort)
        .with_build_global_init(bloom_build_global_init)
        .with_build_local_init(bloom_build_local_init)
        .with_build_sink(bloom_build_sink)
        .with_build_combine(bloom_build_combine)
        .with_build_finalize(bloom_build_finalize)
        .with_create_instance(bloom_create_instance)
}

// =============================================================================
// Bitmap Index Builder
// =============================================================================

#[derive(Debug)]
pub struct BitmapBuildBindData {
    pub index_name: String,
    pub column_ids: Vec<ColumnId>,
    pub logical_types: Vec<LogicalType>,
    pub options: HashMap<String, Value>,
}

impl IndexBuildBindData for BitmapBuildBindData {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct BitmapBuildGlobalState {
    pub writer: Mutex<BitmapIndexWriter>,
    pub logical_type: LogicalType,
}

impl std::fmt::Debug for BitmapBuildGlobalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitmapBuildGlobalState").finish()
    }
}

impl IndexBuildGlobalState for BitmapBuildGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct BitmapBuildLocalState;

impl IndexBuildLocalState for BitmapBuildLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub fn bitmap_build_bind(input: &IndexBuildBindInput) -> Result<Box<dyn IndexBuildBindData>> {
    if input.logical_types.len() != 1 {
        return Err(paro_error::invalid_input(
            "Bitmap index only supports single-column indexes",
        ));
    }

    let col_type = &input.logical_types[0];
    if !is_scalar_supported_type(col_type) {
        return Err(paro_error::type_mismatch(format!(
            "Column type {:?} is not supported by Bitmap index",
            col_type
        )));
    }

    Ok(Box::new(BitmapBuildBindData {
        index_name: input.index_name.to_string(),
        column_ids: input.column_ids.to_vec(),
        logical_types: input.logical_types.to_vec(),
        options: input.options.clone(),
    }))
}

pub fn bitmap_build_sort(_input: &IndexBuildSortInput) -> bool {
    false
}

pub fn bitmap_build_global_init(
    input: &IndexBuildInitGlobalStateInput,
) -> Result<Box<dyn IndexBuildGlobalState>> {
    let logical_type = input
        .logical_types
        .first()
        .cloned()
        .unwrap_or(LogicalType::Unknown);

    Ok(Box::new(BitmapBuildGlobalState {
        writer: Mutex::new(BitmapIndexWriter::new()),
        logical_type,
    }))
}

pub fn bitmap_build_local_init(
    _input: &IndexBuildInitLocalStateInput,
) -> Result<Box<dyn IndexBuildLocalState>> {
    Ok(Box::new(BitmapBuildLocalState))
}

pub fn bitmap_build_sink(
    input: &mut IndexBuildSinkInput,
    key_chunk: &Chunk,
    _row_ids: &[u64],
) -> Result<()> {
    let gstate = input
        .global_state
        .as_any()
        .downcast_ref::<BitmapBuildGlobalState>()
        .ok_or_else(|| paro_error::internal("Invalid global state type"))?;

    let row_count = key_chunk.size();
    if row_count == 0 {
        return Ok(());
    }

    let vector = key_chunk
        .column(0)
        .ok_or_else(|| paro_error::internal("Missing index column"))?;

    let mut writer = gstate
        .writer
        .lock()
        .map_err(|_| paro_error::internal("Failed to lock bitmap writer"))?;

    for row_idx in 0..row_count {
        if vector.is_null(row_idx) {
            writer.add_nulls(1);
            continue;
        }
        let value = vector.get_value(row_idx);
        let bytes = value_to_bytes(&value, &gstate.logical_type)?;
        writer.add_value(&bytes);
    }

    Ok(())
}

pub fn bitmap_build_combine(_input: &mut IndexBuildCombineInput) -> Result<()> {
    Ok(())
}

pub fn bitmap_build_finalize(input: IndexBuildFinalizeInput) -> Result<Arc<dyn BoundIndex>> {
    let gstate = input
        .global_state
        .as_any()
        .downcast_ref::<BitmapBuildGlobalState>()
        .ok_or_else(|| paro_error::internal("Invalid global state type"))?;

    let writer = gstate
        .writer
        .lock()
        .map_err(|_| paro_error::internal("Failed to lock bitmap writer"))?;

    let index = BitmapIndex::from_writer(
        input.name,
        input.constraint_type,
        input.column_ids.to_vec(),
        input.logical_types.to_vec(),
        &writer,
    )?;

    Ok(Arc::new(index))
}

pub fn bitmap_create_instance(input: &CreateIndexInput) -> Result<Arc<dyn BoundIndex>> {
    BitmapIndex::from_storage_info(input)
}

pub fn get_bitmap_index_type() -> IndexType {
    IndexType::new(BitmapIndex::TYPE_NAME)
        .with_build_bind(bitmap_build_bind)
        .with_build_sort(bitmap_build_sort)
        .with_build_global_init(bitmap_build_global_init)
        .with_build_local_init(bitmap_build_local_init)
        .with_build_sink(bitmap_build_sink)
        .with_build_combine(bitmap_build_combine)
        .with_build_finalize(bitmap_build_finalize)
        .with_create_instance(bitmap_create_instance)
}

// =============================================================================
// Helper Functions (shared)
// =============================================================================

fn is_scalar_supported_type(col_type: &LogicalType) -> bool {
    matches!(
        col_type,
        LogicalType::Boolean
            | LogicalType::TinyInt
            | LogicalType::SmallInt
            | LogicalType::Integer
            | LogicalType::BigInt
            | LogicalType::HugeInt
            | LogicalType::UTinyInt
            | LogicalType::USmallInt
            | LogicalType::UInteger
            | LogicalType::UBigInt
            | LogicalType::UHugeInt
            | LogicalType::Uuid
            | LogicalType::Float
            | LogicalType::Double
            | LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob
    )
}
