// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! DeltaWriter - coordinates MemTable buffering and Rowset flush.
//!
//! Minimal implementation:
//! - Open writer for a tablet/txn, assign new version
//! - Accept column data and forward to RowsetWriter
//! - Flush/close/commit lifecycle, add rowset to tablet
//!
//! NOTE: MemTable now deduplicates PRIMARY_KEYS rows on insert and still owns
//! threshold-based flushing / backpressure decisions.

use std::path::{Path, PathBuf};

use crate::metrics::storage_metrics;
use crate::primary_key::delete_vector::DeleteVector;
use crate::primary_key::{PrimaryKeySerializer, RowID};
use crate::rowset::{
    rowset_writer::RowsetWriterSavepoint, save_base_rowids, ColumnData, RowsetSharedPtr,
    RowsetWriter, RowsetWriterBuilder, RowsetWriterContext,
};
use crate::tablet::{
    KeysType, PrimaryIndexUpdate, TabletRef, TabletSchemaRef, TabletState, Version,
};
use crate::transaction::txn::Transaction;
use crate::write::{memtable::MemTableSavepoint, MemTable, MemTableDecision};
use paro_common::allocator::{default_allocator, Allocator};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct DeltaWriterSavepoint {
    memtable_mark: MemTableSavepoint,
    rowset_writer_mark: RowsetWriterSavepoint,
    written: Vec<(Vec<u8>, Option<RowID>)>,
    pending_delete_vectors: HashMap<(u64, u32), DeleteVector>,
    partial_base_rowids_by_key: HashMap<Vec<u8>, RowID>,
    partial_base_rowids: Vec<RowID>,
}

/// DeltaWriter coordinates write path:
/// MemTable → RowsetWriter → Tablet publish
pub struct DeltaWriter {
    tablet: TabletRef,
    #[allow(dead_code)]
    memtable: MemTable,
    rowset_writer: Option<RowsetWriter>,
    txn_id: u64,
    version: Version,
    rowset_path: PathBuf,
    closed: bool,
    prepared: bool,
    schema: TabletSchemaRef,
    serializer: Option<PrimaryKeySerializer>,
    /// Written keys and their previous row ids (if existed).
    written: Vec<(Vec<u8>, Option<RowID>)>,
    /// DeleteVectors to persist: (rowset_id, segment_id) -> DV
    pending_delete_vectors: HashMap<(u64, u32), DeleteVector>,
    /// Optional subset of table column indices written for partial update rowsets.
    partial_update_columns: Option<Vec<usize>>,
    /// Latest base row-id per key for the current memtable contents.
    partial_base_rowids_by_key: HashMap<Vec<u8>, RowID>,
    /// Base row-id chain for all partial rows flushed to the current rowset.
    partial_base_rowids: Vec<RowID>,
}

impl std::fmt::Debug for DeltaWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaWriter")
            .field("txn_id", &self.txn_id)
            .field("version", &self.version)
            .field("rowset_path", &self.rowset_path)
            .field("closed", &self.closed)
            .field("num_written", &self.written.len())
            .finish_non_exhaustive()
    }
}

impl DeltaWriter {
    /// Open a new DeltaWriter for the given tablet/transaction.
    pub fn open(tablet: TabletRef, txn_id: u64) -> Result<Self> {
        Self::open_with_allocator(tablet, txn_id, Arc::new(default_allocator()))
    }

    pub fn open_partial(
        tablet: TabletRef,
        txn_id: u64,
        partial_update_columns: Vec<usize>,
    ) -> Result<Self> {
        Self::open_partial_with_allocator(
            tablet,
            txn_id,
            partial_update_columns,
            Arc::new(default_allocator()),
        )
    }

    /// Open a new DeltaWriter with an explicit allocator for write-path buffers.
    pub fn open_with_allocator(
        tablet: TabletRef,
        txn_id: u64,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        Self::open_internal(tablet, txn_id, allocator, None)
    }

    pub fn open_partial_with_allocator(
        tablet: TabletRef,
        txn_id: u64,
        partial_update_columns: Vec<usize>,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        Self::open_internal(tablet, txn_id, allocator, Some(partial_update_columns))
    }

    fn open_internal(
        tablet: TabletRef,
        txn_id: u64,
        allocator: Arc<dyn Allocator>,
        partial_update_columns: Option<Vec<usize>>,
    ) -> Result<Self> {
        // Tablet must be runnable and have schema.
        let state = tablet.state();
        if state != TabletState::Running {
            return Err(paro_error::invalid_input(format!(
                "Tablet not writable in state {}",
                state
            )));
        }
        tablet.prepare_txn(txn_id)?;
        let schema = tablet
            .schema()
            .ok_or_else(|| paro_error::internal("Tablet schema not set"))?;

        // Use a placeholder version; actual version assigned on rowset_commit.
        let version = Version::singleton(0);

        // Allocate the final rowset id up front so the on-disk namespace is canonical.
        let rowset_id = tablet.next_rowset_id();

        // Flush writes directly into the canonical final rowset namespace.
        let rowset_path = tablet.staged_rowset_path(txn_id, rowset_id);
        std::fs::create_dir_all(&rowset_path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to create rowset path {:?}: {}",
                rowset_path, e
            ))
        })?;

        // Build RowsetWriter.
        let rowset_writer = if let Some(column_indices) = &partial_update_columns {
            let write_column_ids = column_indices
                .iter()
                .map(|&idx| {
                    schema.column(idx).map(|col| col.id).ok_or_else(|| {
                        paro_error::invalid_input(format!(
                            "partial update column {} is out of range",
                            idx
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            RowsetWriterBuilder::new(schema.clone(), tablet.tablet_id(), version, &rowset_path)
                .rowset_id(rowset_id)
                .max_rows_per_segment(u64::MAX)
                .write_column_ids(write_column_ids)
                .build()?
        } else {
            let ctx =
                RowsetWriterContext::new(schema.clone(), tablet.tablet_id(), version, &rowset_path)
                    .with_rowset_id(rowset_id);
            RowsetWriter::create(ctx)?
        };

        let serializer = if schema.keys_type() == KeysType::PrimaryKeys {
            Some(PrimaryKeySerializer::from_schema_ref(&schema)?)
        } else {
            None
        };

        // MemTable with defaults and PRIMARY_KEYS insert-time dedup.
        let memtable = if let Some(serializer) = serializer.as_ref() {
            MemTable::with_primary_keys_with_allocator(
                tablet.tablet_id(),
                schema.clone(),
                serializer.clone(),
                64 * 1024,
                8 * 1024 * 1024,
                allocator.clone(),
            )
        } else {
            MemTable::with_defaults_with_allocator(tablet.tablet_id(), schema.clone(), allocator)
        };

        Ok(Self {
            tablet,
            memtable,
            rowset_writer: Some(rowset_writer),
            txn_id,
            version,
            rowset_path,
            closed: false,
            prepared: true,
            schema,
            serializer,
            written: Vec::new(),
            pending_delete_vectors: HashMap::new(),
            partial_update_columns,
            partial_base_rowids_by_key: HashMap::new(),
            partial_base_rowids: Vec::new(),
        })
    }

    /// Version hint for this delta (actual version assigned on rowset_commit).
    pub fn version(&self) -> Version {
        self.version
    }

    /// Transaction id.
    pub fn txn_id(&self) -> u64 {
        self.txn_id
    }

    /// Write a batch of column data (already encoded column-wise).
    ///
    /// For PRIMARY_KEYS tables prefer `write_chunk` to ensure key encoding.
    pub fn write(&mut self, columns: &[ColumnData]) -> Result<()> {
        if let Some(writer) = self.rowset_writer.as_mut() {
            writer.add_chunk(columns)?;
        }
        Ok(())
    }

    /// Write a Chunk; for PRIMARY_KEYS this performs in-batch dedup using the primary key serializer.
    pub fn write_chunk(&mut self, chunk: &Chunk) -> Result<()> {
        if chunk.size() == 0 {
            return Ok(());
        }

        // Buffer into MemTable; actual write happens on flush.
        match self.memtable.insert(chunk)? {
            MemTableDecision::None => {}
            MemTableDecision::Flush => {
                self.flush_memtable_to_rowset()?;
            }
            MemTableDecision::Backpressure => {
                let start = Instant::now();
                self.flush_memtable_to_rowset()?;
                let elapsed = start.elapsed();
                let metrics = storage_metrics();
                metrics.inc_memtable_backpressure();
                metrics.add_memtable_backpressure_time(elapsed);
            }
        }
        Ok(())
    }

    pub fn write_partial_chunk(&mut self, chunk: &Chunk, base_rowids: &[RowID]) -> Result<()> {
        if chunk.size() != base_rowids.len() {
            return Err(paro_error::invalid_input(format!(
                "partial update chunk/base_rowids length mismatch: {} vs {}",
                chunk.size(),
                base_rowids.len()
            )));
        }
        let serializer = self.serializer.as_ref().ok_or_else(|| {
            paro_error::invalid_input("partial update is only supported for PRIMARY_KEYS tablets")
        })?;
        let encoded_keys = serializer.encode_chunk(chunk)?;
        self.partial_base_rowids_by_key.clear();
        for (key, rowid) in encoded_keys.into_iter().zip(base_rowids.iter().copied()) {
            self.partial_base_rowids_by_key.insert(key, rowid);
        }
        self.write_chunk(chunk)
    }

    pub fn mark_savepoint(&mut self) -> Result<DeltaWriterSavepoint> {
        let memtable_mark = self.memtable.mark_savepoint();
        let rowset_writer_mark = self
            .rowset_writer
            .as_mut()
            .ok_or_else(|| paro_error::internal("rowset_writer missing while marking savepoint"))?
            .mark_savepoint()?;
        Ok(DeltaWriterSavepoint {
            memtable_mark,
            rowset_writer_mark,
            written: self.written.clone(),
            pending_delete_vectors: self.pending_delete_vectors.clone(),
            partial_base_rowids_by_key: self.partial_base_rowids_by_key.clone(),
            partial_base_rowids: self.partial_base_rowids.clone(),
        })
    }

    pub fn rollback_to_savepoint(&mut self, mark: &DeltaWriterSavepoint) -> Result<()> {
        self.memtable.rollback_to_savepoint(&mark.memtable_mark);
        self.rowset_writer
            .as_mut()
            .ok_or_else(|| {
                paro_error::internal("rowset_writer missing while rolling back to savepoint")
            })?
            .rollback_to_savepoint(&mark.rowset_writer_mark)?;
        self.written = mark.written.clone();
        self.pending_delete_vectors = mark.pending_delete_vectors.clone();
        self.partial_base_rowids_by_key = mark.partial_base_rowids_by_key.clone();
        self.partial_base_rowids = mark.partial_base_rowids.clone();

        if self.partial_update_columns.is_some() {
            let base_rowids_path = self.rowset_path.join("0.base_rowids");
            if self.partial_base_rowids.is_empty() {
                if base_rowids_path.exists() {
                    std::fs::remove_file(&base_rowids_path).map_err(|e| {
                        paro_error::io_error(format!(
                            "Failed to remove partial row sidecar {:?}: {}",
                            base_rowids_path, e
                        ))
                    })?;
                }
            } else {
                save_base_rowids(&self.rowset_path, 0, &self.partial_base_rowids)?;
            }
        }

        Ok(())
    }

    /// Flush current memtable/segment.
    pub fn flush_memtable(&mut self) -> Result<()> {
        self.flush_memtable_to_rowset()?;
        if let Some(writer) = self.rowset_writer.as_mut() {
            writer.flush_segment()?;
        }
        Ok(())
    }

    /// Delete by primary key (in-memory index only; does not create delete vectors yet).
    pub fn delete_keys(&self, keys: &Chunk) -> Result<usize> {
        let serializer = self
            .serializer
            .as_ref()
            .ok_or_else(|| paro_error::invalid_input("delete_keys requires PRIMARY_KEYS tablet"))?;
        let encoded_keys = serializer.encode_chunk(keys)?;
        let existing = self.tablet.lookup_primary_keys(&encoded_keys)?;
        let removed_keys: Vec<Vec<u8>> = encoded_keys
            .into_iter()
            .zip(existing)
            .filter_map(|(key, row_id)| row_id.map(|_| key))
            .collect();
        let removed = removed_keys.len();
        if removed == 0 {
            return Ok(0);
        }

        self.tablet.apply_primary_delete(removed_keys)?;
        Ok(removed)
    }

    /// Alias for delete by primary key.
    pub fn delete(&self, keys: &Chunk) -> Result<usize> {
        self.delete_keys(keys)
    }

    /// Close writer: flush pending segment once.
    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.flush_memtable()?;
        self.closed = true;
        Ok(())
    }

    /// Commit: finalize rowset, publish to tablet, return shared rowset.
    pub fn commit(mut self) -> Result<RowsetSharedPtr> {
        let start = Instant::now();
        let mut publish_attempted = false;
        let result = (|| {
            let (rowset, primary_update) = self.finalize_rowset_and_update()?;
            publish_attempted = true;
            if let Some(update) = primary_update {
                self.tablet
                    .publish_rowset_with_index_auto(rowset.clone(), update)?;
            } else {
                self.tablet.rowset_commit_auto(rowset.clone())?;
            }
            Ok(rowset)
        })();

        if !publish_attempted {
            self.cleanup_uncommitted_files();
        }
        self.finish_txn();
        storage_metrics().add_delta_writer_commit_time(start.elapsed());
        result
    }

    /// Commit: finalize rowset and register it with the transaction (deferred publish).
    pub fn commit_in_transaction(self, txn: Arc<Transaction>) -> Result<RowsetSharedPtr> {
        let (tablet, rowset, primary_update) = self.finalize_for_transaction()?;
        if let Err(err) = txn.add_pending_rowset(tablet, rowset.clone(), primary_update) {
            let _ = std::fs::remove_dir_all(rowset.rowset_path());
            return Err(err);
        }
        Ok(rowset)
    }

    /// Cancel the writer and remove partial files.
    pub fn cancel(mut self) -> Result<()> {
        // Best-effort cleanup of on-disk artifacts.
        self.closed = true;
        if Path::new(&self.rowset_path).exists() {
            let _ = std::fs::remove_dir_all(&self.rowset_path);
        }
        self.finish_txn();
        Ok(())
    }

    /// Abort the writer and remove partial files.
    pub fn abort(self) -> Result<()> {
        self.cancel()
    }

    // ----- helpers -----

    /// Flush buffered memtable into rowset writer with PK sort+dedup when applicable.
    fn flush_memtable_to_rowset(&mut self) -> Result<()> {
        let start = Instant::now();
        // Nothing to do without writer.
        if self.rowset_writer.is_none() {
            return Ok(());
        }

        // Drain memtable buffers; for PK returns ordered key→(chunk,row) mapping.
        let (chunks, ordered_rows) = self.memtable.drain_sorted_dedup()?;

        if chunks.is_empty() {
            storage_metrics().add_delta_writer_flush_time(start.elapsed());
            return Ok(());
        }

        storage_metrics().inc_memtable_flush();

        // Non-PK path: just forward each chunk.
        if self.serializer.is_none() {
            for chunk in chunks.iter() {
                let columns = self.chunk_to_column_data(chunk, None)?;
                let Some(writer) = self.rowset_writer.as_mut() else {
                    return Ok(());
                };
                writer.add_chunk(&columns)?;
            }
            storage_metrics().add_delta_writer_flush_time(start.elapsed());
            return Ok(());
        }

        // PRIMARY_KEYS path: build a single ordered batch from deduped rows.
        if ordered_rows.is_empty() {
            return Ok(());
        }

        let all_logical_types = self.schema.logical_types();
        let write_column_indices: Vec<usize> = self
            .partial_update_columns
            .clone()
            .unwrap_or_else(|| (0..all_logical_types.len()).collect());
        let row_count = ordered_rows.len();
        let mut columns: Vec<ColumnData> = Vec::with_capacity(write_column_indices.len());

        for &col_idx in &write_column_indices {
            let ty = &all_logical_types[col_idx];
            let mut nulls = vec![0u8; row_count.div_ceil(8)];
            let mut data: Vec<u8> = Vec::with_capacity(row_count * ty.physical_size());

            for (out_row, (_key, (chunk_idx, row_idx))) in ordered_rows.iter().enumerate() {
                let chunk = chunks
                    .get(*chunk_idx)
                    .ok_or_else(|| paro_error::internal("chunk index out of range"))?;
                let vec = chunk
                    .column(col_idx)
                    .ok_or_else(|| paro_error::invalid_input("column missing in chunk"))?;
                let validity = vec.validity();
                let is_null = !validity.is_valid(*row_idx);
                if is_null {
                    nulls[out_row / 8] |= 1 << (out_row % 8);
                }
                match ty {
                    LogicalType::TinyInt => {
                        let val: i8 = unsafe { vec.get_flat(*row_idx) };
                        data.push(val as u8);
                    }
                    LogicalType::SmallInt => {
                        let val: i16 = unsafe { vec.get_flat(*row_idx) };
                        data.extend_from_slice(&val.to_le_bytes());
                    }
                    LogicalType::Integer => {
                        let val: i32 = unsafe { vec.get_flat(*row_idx) };
                        data.extend_from_slice(&val.to_le_bytes());
                    }
                    LogicalType::BigInt => {
                        let val: i64 = unsafe { vec.get_flat(*row_idx) };
                        data.extend_from_slice(&val.to_le_bytes());
                    }
                    LogicalType::UTinyInt => {
                        let val: u8 = unsafe { vec.get_flat(*row_idx) };
                        data.push(val);
                    }
                    LogicalType::USmallInt => {
                        let val: u16 = unsafe { vec.get_flat(*row_idx) };
                        data.extend_from_slice(&val.to_le_bytes());
                    }
                    LogicalType::UInteger => {
                        let val: u32 = unsafe { vec.get_flat(*row_idx) };
                        data.extend_from_slice(&val.to_le_bytes());
                    }
                    LogicalType::UBigInt => {
                        let val: u64 = unsafe { vec.get_flat(*row_idx) };
                        data.extend_from_slice(&val.to_le_bytes());
                    }
                    LogicalType::Float => {
                        let val: f32 = unsafe { vec.get_flat(*row_idx) };
                        data.extend_from_slice(&val.to_le_bytes());
                    }
                    LogicalType::Double => {
                        let val: f64 = unsafe { vec.get_flat(*row_idx) };
                        data.extend_from_slice(&val.to_le_bytes());
                    }
                    LogicalType::Varchar
                    | LogicalType::VarcharCollation(_)
                    | LogicalType::TsVector
                    | LogicalType::TsQuery
                    | LogicalType::Json
                    | LogicalType::Jsonb => {
                        if is_null {
                            data.extend_from_slice(&0u32.to_le_bytes());
                            continue;
                        }

                        let s = vec.get_string(*row_idx).ok_or_else(|| {
                            paro_error::internal("Failed to read string value in write path")
                        })?;
                        data.extend_from_slice(&(s.len() as u32).to_le_bytes());
                        data.extend_from_slice(s.as_bytes());
                    }
                    LogicalType::Blob => {
                        if is_null {
                            data.extend_from_slice(&0u32.to_le_bytes());
                            continue;
                        }

                        let b = vec.get_blob(*row_idx).ok_or_else(|| {
                            paro_error::internal("Failed to read blob value in write path")
                        })?;
                        data.extend_from_slice(&(b.len() as u32).to_le_bytes());
                        data.extend_from_slice(b);
                    }
                    LogicalType::List(child_type) => {
                        if is_null {
                            data.extend_from_slice(&0u32.to_le_bytes());
                            continue;
                        }

                        let row_value = vec.get_value(*row_idx);
                        let values = match row_value {
                            Value::List(values, _) | Value::Array(values, _, _) => values,
                            Value::Null(_) => {
                                data.extend_from_slice(&0u32.to_le_bytes());
                                continue;
                            }
                            _ => {
                                return Err(paro_error::not_supported(format!(
                                    "Type {:?} not yet supported in write path",
                                    ty
                                )));
                            }
                        };

                        let payload = crate::codec::nested_payload_codec::encode_list_payload(
                            child_type, &values,
                        )?;
                        data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                        data.extend_from_slice(&payload);
                    }
                    LogicalType::Struct(fields) => {
                        if is_null {
                            data.extend_from_slice(&0u32.to_le_bytes());
                            continue;
                        }

                        let row_value = vec.get_value(*row_idx);
                        let values = match row_value {
                            Value::Struct(values, _) => values,
                            Value::Null(_) => {
                                data.extend_from_slice(&0u32.to_le_bytes());
                                continue;
                            }
                            _ => {
                                return Err(paro_error::not_supported(format!(
                                    "Type {:?} not yet supported in write path",
                                    ty
                                )));
                            }
                        };

                        let payload = crate::codec::nested_payload_codec::encode_struct_payload(
                            fields, &values,
                        )?;
                        data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                        data.extend_from_slice(&payload);
                    }
                    LogicalType::Boolean => {
                        let val: bool = unsafe { vec.get_flat(*row_idx) };
                        data.push(val as u8);
                    }
                    LogicalType::Date
                    | LogicalType::Timestamp
                    | LogicalType::TimestampTz
                    | LogicalType::Time => {
                        if matches!(ty, LogicalType::Date) {
                            let val: i32 = unsafe { vec.get_flat(*row_idx) };
                            data.extend_from_slice(&val.to_le_bytes());
                        } else {
                            let val: i64 = unsafe { vec.get_flat(*row_idx) };
                            data.extend_from_slice(&val.to_le_bytes());
                        }
                    }
                    LogicalType::HugeInt | LogicalType::Interval => {
                        let val: i128 = unsafe { vec.get_flat(*row_idx) };
                        data.extend_from_slice(&val.to_le_bytes());
                    }
                    LogicalType::Decimal { precision, .. } => {
                        let width =
                            crate::codec::physical_layout::decimal_storage_width(*precision);
                        if is_null {
                            data.resize(data.len() + width, 0);
                            continue;
                        }

                        if width == std::mem::size_of::<i64>() {
                            let val = vec.get_i64(*row_idx).ok_or_else(|| {
                                paro_error::internal(
                                    "Failed to read Decimal(i64) value in write path",
                                )
                            })?;
                            data.extend_from_slice(&val.to_le_bytes());
                        } else {
                            let val = vec.get_i128(*row_idx).ok_or_else(|| {
                                paro_error::internal(
                                    "Failed to read Decimal(i128) value in write path",
                                )
                            })?;
                            data.extend_from_slice(&val.to_le_bytes());
                        }
                    }
                    LogicalType::UHugeInt | LogicalType::Uuid => {
                        let val: u128 = unsafe { vec.get_flat(*row_idx) };
                        data.extend_from_slice(&val.to_le_bytes());
                    }
                    LogicalType::Array(inner, dim) if matches!(**inner, LogicalType::Float) => {
                        let dim = *dim;
                        if is_null {
                            data.resize(data.len() + dim * std::mem::size_of::<f32>(), 0);
                            continue;
                        }

                        let row_value = vec.get_value(*row_idx);
                        let values = match row_value {
                            Value::Array(values, _, _) | Value::List(values, _) => values,
                            Value::Null(_) => {
                                data.resize(data.len() + dim * std::mem::size_of::<f32>(), 0);
                                continue;
                            }
                            _ => {
                                return Err(paro_error::not_supported(format!(
                                    "Type {:?} not yet supported in write path",
                                    ty
                                )));
                            }
                        };

                        if values.len() != dim {
                            return Err(paro_error::invalid_input(format!(
                                "Vector dimension mismatch in write path: expected {}, got {}",
                                dim,
                                values.len()
                            )));
                        }

                        for value in values {
                            let f = match value {
                                Value::Float(v) => v,
                                Value::Double(v) => v as f32,
                                Value::TinyInt(v) => v as f32,
                                Value::SmallInt(v) => v as f32,
                                Value::Integer(v) => v as f32,
                                Value::BigInt(v) => v as f32,
                                Value::UTinyInt(v) => v as f32,
                                Value::USmallInt(v) => v as f32,
                                Value::UInteger(v) => v as f32,
                                Value::UBigInt(v) => v as f32,
                                _ => {
                                    return Err(paro_error::not_supported(format!(
                                        "Value {:?} for type {:?} not yet supported in write path",
                                        value, ty
                                    )));
                                }
                            };
                            data.extend_from_slice(&f.to_le_bytes());
                        }
                    }
                    _ => {
                        return Err(paro_error::not_supported(format!(
                            "Type {:?} not yet supported in write path",
                            ty
                        )));
                    }
                }
            }

            let col = ColumnData::with_nulls(data, nulls, row_count as u32);
            columns.push(col);
        }

        // Write ordered batch to rowset.
        if let Some(writer) = self.rowset_writer.as_mut() {
            writer.add_chunk(&columns)?;
        }

        let partial_base_rowids = if self.partial_update_columns.is_some() {
            let mut base_rowids = Vec::with_capacity(ordered_rows.len());
            for (key, _) in &ordered_rows {
                let rowid = self
                    .partial_base_rowids_by_key
                    .get(key)
                    .copied()
                    .ok_or_else(|| {
                        paro_error::internal(
                            "partial update base row-id missing for deduplicated key",
                        )
                    })?;
                base_rowids.push(rowid);
            }
            self.partial_base_rowids.extend(base_rowids.iter().copied());
            save_base_rowids(&self.rowset_path, 0, &self.partial_base_rowids)?;
            self.partial_base_rowids_by_key.clear();
            Some(base_rowids)
        } else {
            None
        };

        // Track written keys and prior locations for delete vector + index updates.
        let mut prior_locs: Vec<Option<RowID>> = Vec::with_capacity(ordered_rows.len());

        for (row_idx, (key, _)) in ordered_rows.iter().enumerate() {
            let old = if let Some(base_rowids) = partial_base_rowids.as_ref() {
                Some(base_rowids[row_idx])
            } else {
                self.tablet.lookup_primary_key(key)?
            };
            prior_locs.push(old);
        }

        for row_id in prior_locs.iter().flatten() {
            self.mark_delete(*row_id)?;
        }

        for ((key, _), old) in ordered_rows.into_iter().zip(prior_locs.into_iter()) {
            self.written.push((key, old));
        }

        storage_metrics().add_delta_writer_flush_time(start.elapsed());
        Ok(())
    }

    /// Finalize the rowset for transactional commit (without publishing).
    pub(crate) fn finalize_for_transaction(
        mut self,
    ) -> Result<(TabletRef, RowsetSharedPtr, Option<PrimaryIndexUpdate>)> {
        let start = Instant::now();
        let result = self
            .finalize_rowset_and_update()
            .map(|(rowset, primary_update)| (self.tablet.clone(), rowset, primary_update));

        if result.is_err() {
            self.cleanup_uncommitted_files();
        }
        self.finish_txn();
        storage_metrics().add_delta_writer_commit_time(start.elapsed());
        result
    }

    fn finalize_rowset_and_update(
        &mut self,
    ) -> Result<(RowsetSharedPtr, Option<PrimaryIndexUpdate>)> {
        self.close()?;
        let writer = self
            .rowset_writer
            .take()
            .ok_or_else(|| paro_error::internal("rowset_writer missing"))?;
        let rowset = writer.build_shared()?;

        let primary_update = if self.serializer.is_some() {
            Some(PrimaryIndexUpdate {
                written: std::mem::take(&mut self.written),
                pending_delete_vectors: std::mem::take(&mut self.pending_delete_vectors),
            })
        } else {
            None
        };

        Ok((rowset, primary_update))
    }

    fn finish_txn(&mut self) {
        if self.prepared {
            self.tablet.finish_txn(self.txn_id);
            self.prepared = false;
        }
    }

    fn cleanup_uncommitted_files(&self) {
        if Path::new(&self.rowset_path).exists() {
            let _ = std::fs::remove_dir_all(&self.rowset_path);
        }
    }

    /// Convert a Chunk into ColumnData, optionally with row selection.
    fn chunk_to_column_data(
        &self,
        chunk: &Chunk,
        row_selection: Option<&[usize]>,
    ) -> Result<Vec<ColumnData>> {
        if row_selection.is_some() {
            return Err(paro_error::not_supported(
                "row selection is not supported by shared chunk_to_column_data helper",
            ));
        }
        let logical_types = self.schema.logical_types();
        crate::codec::chunk_encoder::encode_chunk(&logical_types, chunk)
    }

    fn mark_delete(&mut self, row_id: RowID) -> Result<()> {
        let loc = self.tablet.decode_row_id(row_id)?;
        let entry = self
            .pending_delete_vectors
            .entry(loc.segment_key())
            .or_default();
        entry.mark_deleted(loc.row_offset);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::{
        tablet_schema::{KeysType, TabletColumn, TabletSchema},
        Tablet,
    };
    use paro_common::types::LogicalType;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_tablet() -> (TabletRef, TempDir) {
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

    fn sample_chunk(num: i32) -> Chunk {
        let v0 = paro_common::vector::Vector::from_i32(&(0..num).collect::<Vec<i32>>());
        let v1 = paro_common::vector::Vector::from_i32(&(100..100 + num).collect::<Vec<i32>>());
        Chunk::from_vectors(vec![v0, v1])
    }

    #[test]
    fn delta_writer_basic_commit() {
        let (tablet, _tmp) = create_test_tablet();
        let mut writer = DeltaWriter::open(tablet.clone(), 1).unwrap();

        // Write two batches with same keys 0-59; dedup means latest batch wins
        writer.write_chunk(&sample_chunk(60)).unwrap();
        writer.write_chunk(&sample_chunk(60)).unwrap();

        let rowset = writer.commit().unwrap();
        // After dedup, only 60 unique keys remain
        assert_eq!(rowset.num_rows(), 60);
        assert!(rowset.is_visible());
        assert_eq!(tablet.max_version(), rowset.version().end);
        assert_eq!(tablet.num_rowsets(), 1);
        // Primary index should contain 60 unique keys (latest wins across batches)
        assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 60);
    }

    #[test]
    fn delta_writer_cancel_cleans_dir() {
        let (tablet, tmp) = create_test_tablet();
        let writer = DeltaWriter::open(tablet, 2).unwrap();
        let path = writer.rowset_path.clone();
        assert!(path.exists());
        writer.cancel().unwrap();
        assert!(!path.exists());
        drop(tmp);
    }

    #[test]
    fn delta_writer_prepared_txn_lifecycle() {
        let (tablet, _tmp) = create_test_tablet();

        let writer = DeltaWriter::open(tablet.clone(), 22).unwrap();
        let err = DeltaWriter::open(tablet.clone(), 22).unwrap_err();
        assert!(format!("{err}").contains("already prepared"));

        writer.cancel().unwrap();

        let mut writer = DeltaWriter::open(tablet.clone(), 22).unwrap();
        writer.write_chunk(&sample_chunk(1)).unwrap();
        writer.commit().unwrap();

        DeltaWriter::open(tablet, 22).unwrap().cancel().unwrap();
    }

    #[test]
    fn delta_writer_dedup_in_chunk() {
        let (tablet, _tmp) = create_test_tablet();
        let alloc = std::sync::Arc::new(paro_common::allocator::default_allocator());
        let ids = paro_common::vector::Vector::from_i32_with_allocator(&[1, 2, 2], alloc.clone());
        let vals = paro_common::vector::Vector::from_i32_with_allocator(&[10, 20, 30], alloc);
        let chunk = Chunk::from_arc_vectors(vec![Arc::new(ids), Arc::new(vals)]);

        let mut writer = DeltaWriter::open(tablet.clone(), 3).unwrap();
        writer.write_chunk(&chunk).unwrap();
        let rowset = writer.commit().unwrap();
        assert_eq!(rowset.num_rows(), 2);
        assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 2);
    }

    #[test]
    fn delta_writer_delete_keys_persists_delete_vector() {
        let (tablet, tmp) = create_test_tablet();
        // Seed data
        let mut writer = DeltaWriter::open(tablet.clone(), 10).unwrap();
        writer.write_chunk(&sample_chunk(10)).unwrap();
        let rowset = writer.commit().unwrap();
        assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 10);

        // Delete first 4 keys
        let del_writer = DeltaWriter::open(tablet.clone(), 11).unwrap();
        let removed = del_writer.delete_keys(&sample_chunk(4)).unwrap();
        assert_eq!(removed, 4);

        // Primary index shrinks
        assert_eq!(tablet.snapshot_primary_index_entries().unwrap().len(), 6);

        // DeleteVector persisted for segment 0
        let dv = crate::primary_key::DeleteVector::load_from_dir(rowset.rowset_path(), 0)
            .unwrap()
            .unwrap();
        assert_eq!(dv.cardinality(), 4);
        assert!(dv.is_deleted(0));

        drop(tmp);
    }

    #[test]
    fn delta_writer_savepoint_restores_buffered_rows() {
        let (tablet, _tmp) = create_test_tablet();
        let mut writer = DeltaWriter::open(tablet, 12).unwrap();

        writer.write_chunk(&sample_chunk(3)).unwrap();
        let mark = writer.mark_savepoint().unwrap();
        writer.write_chunk(&sample_chunk(5)).unwrap();
        writer.rollback_to_savepoint(&mark).unwrap();

        let rowset = writer.commit().unwrap();
        assert_eq!(rowset.num_rows(), 3);
    }
}
