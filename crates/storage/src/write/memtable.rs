// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! MemTable - in-memory write buffer with PRIMARY_KEYS insert-time dedup.
//!
//! Minimal implementation:
//! - Buffer incoming `Chunk`s for a Tablet
//! - Track row/byte usage
//! - Decide when to flush based on thresholds
//! - Provide flush to hand buffered chunks to a downstream sink

use crate::buffer::{WriteBufferReservation, WriteBufferReserve};
use crate::primary_key::PrimaryKeySerializer;
use crate::tablet::{KeysType, TabletSchemaRef};
use paro_common::allocator::{default_allocator, Allocator};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// Mapping from encoded primary key to (chunk_index, row_index).
pub type OrderedRowMapping = Vec<(Vec<u8>, (usize, usize))>;
/// Result of draining memtable: (chunks, ordered_row_mapping).
pub type MemTableDrainResult = (Vec<Chunk>, OrderedRowMapping);

#[derive(Debug, Clone)]
pub(crate) struct MemTableSavepoint {
    buffered: Vec<Chunk>,
    rows: usize,
    pk_row_index: HashMap<Vec<u8>, (usize, usize)>,
}

/// Simple statistics snapshot
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemTableStats {
    pub rows: usize,
    pub bytes: usize,
    pub chunks: usize,
}

/// Insert decision for MemTable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemTableDecision {
    /// No action needed.
    None,
    /// Soft threshold reached - flush recommended.
    Flush,
    /// Hard threshold reached or memory reservation too low - backpressure required.
    Backpressure,
}

/// MemTable options for thresholds and allocation.
#[derive(Clone)]
pub struct MemTableOptions {
    pub max_rows: usize,
    pub soft_max_bytes: usize,
    pub hard_max_bytes: usize,
    pub allocator: Arc<dyn Allocator>,
    pub write_buffer_reserve: Option<Arc<dyn WriteBufferReserve>>,
    /// Spill is optional and disabled by default.
    pub spill_enabled: bool,
}

impl std::fmt::Debug for MemTableOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTableOptions")
            .field("max_rows", &self.max_rows)
            .field("soft_max_bytes", &self.soft_max_bytes)
            .field("hard_max_bytes", &self.hard_max_bytes)
            .field("allocator", &self.allocator.name())
            .field(
                "has_write_buffer_reserve",
                &self.write_buffer_reserve.is_some(),
            )
            .field("spill_enabled", &self.spill_enabled)
            .finish()
    }
}

impl MemTableOptions {
    pub fn new(max_rows: usize, soft_max_bytes: usize, allocator: Arc<dyn Allocator>) -> Self {
        let hard_max_bytes = soft_max_bytes.saturating_mul(2);
        Self {
            max_rows,
            soft_max_bytes,
            hard_max_bytes,
            allocator,
            write_buffer_reserve: None,
            spill_enabled: false,
        }
    }

    /// Test convenience constructor that keeps legacy behavior.
    /// Production code should pass an explicit allocator via `new`.
    pub fn new_for_test(max_rows: usize, soft_max_bytes: usize) -> Self {
        Self::new(max_rows, soft_max_bytes, Arc::new(default_allocator()))
    }

    pub fn with_hard_max_bytes(mut self, hard_max_bytes: usize) -> Self {
        self.hard_max_bytes = hard_max_bytes.max(self.soft_max_bytes);
        self
    }

    pub fn with_allocator(mut self, allocator: Arc<dyn Allocator>) -> Self {
        self.allocator = allocator;
        self
    }

    pub fn with_write_buffer_reserve(mut self, reserve: Arc<dyn WriteBufferReserve>) -> Self {
        self.write_buffer_reserve = Some(reserve);
        self
    }

    pub fn with_spill_enabled(mut self, enabled: bool) -> Self {
        self.spill_enabled = enabled;
        self
    }
}

impl Default for MemTableOptions {
    fn default() -> Self {
        Self::new_for_test(64 * 1024, 8 * 1024 * 1024)
    }
}

/// In-memory buffer for a single Tablet
pub struct MemTable {
    tablet_id: u64,
    schema: TabletSchemaRef,
    buffered: Vec<Chunk>,
    rows: usize,
    max_rows: usize,
    soft_max_bytes: usize,
    hard_max_bytes: usize,
    row_size: usize,
    allocator: Arc<dyn Allocator>,
    write_buffer_reservation: Option<WriteBufferReservation>,
    #[allow(dead_code)]
    spill_enabled: bool,
    pk_serializer: Option<PrimaryKeySerializer>,
    pk_row_index: HashMap<Vec<u8>, (usize, usize)>,
}

impl std::fmt::Debug for MemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTable")
            .field("tablet_id", &self.tablet_id)
            .field("rows", &self.rows)
            .field("max_rows", &self.max_rows)
            .field("soft_max_bytes", &self.soft_max_bytes)
            .field("hard_max_bytes", &self.hard_max_bytes)
            .field("buffered_chunks", &self.buffered.len())
            .field("allocator", &self.allocator.name())
            .field(
                "has_write_buffer_reservation",
                &self.write_buffer_reservation.is_some(),
            )
            .field("primary_keys", &self.is_primary_keys())
            .finish()
    }
}

impl MemTable {
    /// Create a MemTable with custom thresholds.
    ///
    /// NOTE: This convenience constructor uses `default_allocator()` and is
    /// intended for tests/utility paths. Production code should use
    /// `new_with_allocator`.
    pub fn new(tablet_id: u64, schema: TabletSchemaRef, max_rows: usize, max_bytes: usize) -> Self {
        Self::new_with_options(
            tablet_id,
            schema,
            MemTableOptions::new_for_test(max_rows, max_bytes),
        )
    }

    /// Create a MemTable with custom thresholds and explicit allocator.
    pub fn new_with_allocator(
        tablet_id: u64,
        schema: TabletSchemaRef,
        max_rows: usize,
        max_bytes: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        Self::new_with_options(
            tablet_id,
            schema,
            MemTableOptions::new(max_rows, max_bytes, allocator),
        )
    }

    /// Create with sensible defaults (~64k rows or ~8MB).
    ///
    /// NOTE: This convenience constructor uses `default_allocator()` and is
    /// intended for tests/utility paths. Production code should use
    /// `with_defaults_with_allocator`.
    pub fn with_defaults(tablet_id: u64, schema: TabletSchemaRef) -> Self {
        Self::new_with_options(tablet_id, schema, MemTableOptions::default())
    }

    /// Create with sensible defaults (~64k rows or ~8MB) and explicit allocator.
    pub fn with_defaults_with_allocator(
        tablet_id: u64,
        schema: TabletSchemaRef,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        Self::new_with_options(
            tablet_id,
            schema,
            MemTableOptions::new(64 * 1024, 8 * 1024 * 1024, allocator),
        )
    }

    /// Create a MemTable for PRIMARY_KEYS tablet with serializer attached.
    pub fn with_primary_keys(
        tablet_id: u64,
        schema: TabletSchemaRef,
        serializer: PrimaryKeySerializer,
        max_rows: usize,
        max_bytes: usize,
    ) -> Self {
        let mut mt = Self::new_with_options(
            tablet_id,
            schema,
            MemTableOptions::new_for_test(max_rows, max_bytes),
        );
        mt.pk_serializer = Some(serializer);
        mt
    }

    /// Create a MemTable for PRIMARY_KEYS tablet with serializer and explicit allocator.
    pub fn with_primary_keys_with_allocator(
        tablet_id: u64,
        schema: TabletSchemaRef,
        serializer: PrimaryKeySerializer,
        max_rows: usize,
        max_bytes: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        let mut mt = Self::new_with_options(
            tablet_id,
            schema,
            MemTableOptions::new(max_rows, max_bytes, allocator),
        );
        mt.pk_serializer = Some(serializer);
        mt
    }

    /// Create a MemTable with full options.
    pub fn new_with_options(
        tablet_id: u64,
        schema: TabletSchemaRef,
        options: MemTableOptions,
    ) -> Self {
        let row_size = schema
            .columns()
            .iter()
            .map(|c| c.logical_type.type_size())
            .sum();

        let write_buffer_reservation = options
            .write_buffer_reserve
            .map(WriteBufferReservation::new);

        Self {
            tablet_id,
            schema,
            buffered: Vec::new(),
            rows: 0,
            max_rows: options.max_rows,
            soft_max_bytes: options.soft_max_bytes,
            hard_max_bytes: options.hard_max_bytes.max(options.soft_max_bytes),
            row_size,
            allocator: options.allocator,
            write_buffer_reservation,
            spill_enabled: options.spill_enabled,
            pk_serializer: None,
            pk_row_index: HashMap::new(),
        }
    }

    /// Create a MemTable for PRIMARY_KEYS tablet with serializer and options.
    pub fn with_primary_keys_and_options(
        tablet_id: u64,
        schema: TabletSchemaRef,
        serializer: PrimaryKeySerializer,
        options: MemTableOptions,
    ) -> Self {
        let mut mt = Self::new_with_options(tablet_id, schema, options);
        mt.pk_serializer = Some(serializer);
        mt
    }

    /// Insert a chunk; returns an action decision.
    pub fn insert(&mut self, chunk: &Chunk) -> Result<MemTableDecision> {
        if chunk.size() == 0 {
            return Ok(MemTableDecision::None);
        }
        if chunk.column_count() != self.schema.num_columns() {
            return Err(paro_error::invalid_input("MemTable column count mismatch"));
        }
        // Basic type check
        for (i, ty) in self.schema.logical_types().iter().enumerate() {
            if chunk.data[i].logical_type() != ty {
                return Err(paro_error::invalid_input("MemTable column type mismatch"));
            }
        }

        let stored = if Arc::ptr_eq(chunk.allocator(), &self.allocator) {
            chunk.clone()
        } else {
            chunk.try_deep_copy(self.allocator.clone())?
        };

        if self.is_primary_keys() {
            self.insert_primary_keys_chunk(stored)
        } else {
            self.push_buffered_chunk(stored)
        }
    }

    /// Should flush based on thresholds.
    pub fn should_flush(&self) -> bool {
        self.should_flush_with_bytes(self.estimated_bytes())
    }

    /// Flush buffered chunks to sink callback.
    ///
    /// The sink receives a slice of chunks and total rows; MemTable is cleared afterwards.
    pub fn flush<F>(&mut self, mut sink: F) -> Result<()>
    where
        F: FnMut(&[Chunk], usize) -> Result<()>,
    {
        if self.rows == 0 {
            return Ok(());
        }
        let total = self.rows;
        sink(&self.buffered, total)?;
        self.buffered.clear();
        self.pk_row_index.clear();
        self.rows = 0;
        self.update_write_buffer_reservation(0);
        Ok(())
    }

    /// Current stats.
    pub fn stats(&self) -> MemTableStats {
        MemTableStats {
            rows: self.rows,
            bytes: self.estimated_bytes(),
            chunks: self.buffered.len(),
        }
    }

    /// Tablet id.
    pub fn tablet_id(&self) -> u64 {
        self.tablet_id
    }

    /// Whether this memtable is for a primary key tablet.
    pub fn is_primary_keys(&self) -> bool {
        self.pk_serializer.is_some() && self.schema.keys_type() == KeysType::PrimaryKeys
    }

    /// Drain buffered chunks into a sorted & deduplicated order by primary key.
    ///
    /// Returns ownership of buffered chunks and the ordered key-row mapping:
    /// Vec<(key_bytes, (chunk_idx, row_idx))>. MemTable is emptied.
    pub fn drain_sorted_dedup(&mut self) -> Result<MemTableDrainResult> {
        let buffered = std::mem::take(&mut self.buffered);
        self.rows = 0;
        self.update_write_buffer_reservation(0);

        if !self.is_primary_keys() {
            self.pk_row_index.clear();
            return Ok((buffered, Vec::new()));
        }

        let mut ordered: Vec<_> = std::mem::take(&mut self.pk_row_index).into_iter().collect();
        ordered.sort_by(|a, b| a.0.cmp(&b.0));

        Ok((buffered, ordered))
    }

    /// Schema.
    pub fn schema(&self) -> &TabletSchemaRef {
        &self.schema
    }

    pub(crate) fn mark_savepoint(&self) -> MemTableSavepoint {
        MemTableSavepoint {
            buffered: self.buffered.clone(),
            rows: self.rows,
            pk_row_index: self.pk_row_index.clone(),
        }
    }

    pub(crate) fn rollback_to_savepoint(&mut self, mark: &MemTableSavepoint) {
        self.buffered = mark.buffered.clone();
        self.rows = mark.rows;
        self.pk_row_index = mark.pk_row_index.clone();
        self.update_write_buffer_reservation(self.estimated_bytes());
    }

    fn insert_primary_keys_chunk(&mut self, mut stored: Chunk) -> Result<MemTableDecision> {
        let serializer = self
            .pk_serializer
            .as_ref()
            .ok_or_else(|| paro_error::internal("primary key serializer missing"))?;
        let encoded_keys = serializer.encode_chunk(&stored)?;

        let mut inserted_keys = HashMap::<Vec<u8>, usize>::new();

        for (row_idx, key) in encoded_keys.into_iter().enumerate() {
            if let Some(&(chunk_idx, existing_row_idx)) = self.pk_row_index.get(&key) {
                let target = self.buffered.get_mut(chunk_idx).ok_or_else(|| {
                    paro_error::internal("buffered chunk index missing for primary key update")
                })?;
                Self::copy_row(&stored, row_idx, target, existing_row_idx)?;
                continue;
            }

            if let Some(existing_row_idx) = inserted_keys.get_mut(&key) {
                *existing_row_idx = row_idx;
                continue;
            }

            inserted_keys.insert(key, row_idx);
        }

        if inserted_keys.is_empty() {
            return Ok(MemTableDecision::None);
        }

        let mut keep_rows: Vec<u32> = inserted_keys
            .values()
            .map(|row_idx| *row_idx as u32)
            .collect();
        keep_rows.sort_unstable();

        let remapped_rows = if keep_rows.len() != stored.size() {
            let row_remap: HashMap<usize, usize> = keep_rows
                .iter()
                .enumerate()
                .map(|(new_row_idx, original_row_idx)| (*original_row_idx as usize, new_row_idx))
                .collect();
            stored = Self::materialize_rows_as_flat_chunk(&stored, &keep_rows)?;
            row_remap
        } else {
            inserted_keys
                .values()
                .copied()
                .map(|row_idx| (row_idx, row_idx))
                .collect()
        };

        let chunk_idx = self.buffered.len();
        for (key, original_row_idx) in inserted_keys {
            let row_idx = remapped_rows
                .get(&original_row_idx)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal("primary key row remap missing after insert-time dedup")
                })?;
            self.pk_row_index.insert(key, (chunk_idx, row_idx));
        }

        self.push_buffered_chunk(stored)
    }

    fn push_buffered_chunk(&mut self, stored: Chunk) -> Result<MemTableDecision> {
        self.rows += stored.size();
        self.buffered.push(stored);
        let bytes = self.estimated_bytes();
        let reserve_available = self.update_write_buffer_reservation(bytes);

        if self.is_backpressure(bytes, reserve_available) {
            return Ok(MemTableDecision::Backpressure);
        }

        if self.should_flush_with_bytes(bytes) {
            return Ok(MemTableDecision::Flush);
        }

        Ok(MemTableDecision::None)
    }

    fn estimated_bytes(&self) -> usize {
        self.rows.saturating_mul(self.row_size)
    }

    fn copy_row(
        source: &Chunk,
        source_row_idx: usize,
        target: &mut Chunk,
        target_row_idx: usize,
    ) -> Result<()> {
        for col_idx in 0..source.column_count() {
            let src_vec = source.column(col_idx).ok_or_else(|| {
                paro_error::internal("source column missing during memtable insert-time dedup")
            })?;
            let dest_vec = target.column_mut(col_idx).ok_or_else(|| {
                paro_error::internal("target column missing during memtable insert-time dedup")
            })?;
            dest_vec.try_copy_at(target_row_idx, src_vec, source_row_idx)?;
        }
        Ok(())
    }

    fn materialize_rows_as_flat_chunk(source: &Chunk, row_indices: &[u32]) -> Result<Chunk> {
        let mut chunk = Chunk::try_initialize(
            &source.types(),
            row_indices.len(),
            source.allocator().clone(),
        )?;
        chunk.try_set_cardinality(row_indices.len())?;

        for col_idx in 0..source.column_count() {
            let src_vec = source.column(col_idx).ok_or_else(|| {
                paro_error::internal("source column missing during memtable row materialization")
            })?;
            let dest_vec = chunk.column_mut(col_idx).ok_or_else(|| {
                paro_error::internal("target column missing during memtable row materialization")
            })?;
            for (new_row_idx, source_row_idx) in row_indices.iter().enumerate() {
                dest_vec.try_copy_at(new_row_idx, src_vec, *source_row_idx as usize)?;
            }
        }

        Ok(chunk)
    }

    fn should_flush_with_bytes(&self, bytes: usize) -> bool {
        self.rows >= self.max_rows || bytes >= self.soft_max_bytes
    }

    fn is_backpressure(&self, bytes: usize, reserve_available: bool) -> bool {
        if bytes >= self.hard_max_bytes {
            return true;
        }
        !reserve_available
    }

    fn update_write_buffer_reservation(&self, bytes: usize) -> bool {
        self.write_buffer_reservation
            .as_ref()
            .map(|reservation| reservation.resize(bytes))
            .unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::WriteBufferReserve;
    use crate::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};
    use crate::test_utils::*;
    use paro_common::allocator::default_allocator;
    use paro_common::types::LogicalType;

    use std::collections::HashMap;
    use std::sync::Arc;

    fn test_schema() -> TabletSchemaRef {
        Arc::new(
            crate::tablet::tablet_schema::TabletSchema::from_types(
                1,
                &[LogicalType::Integer, LogicalType::Varchar],
            )
            .unwrap(),
        )
    }

    fn test_primary_key_schema() -> TabletSchemaRef {
        Arc::new(
            TabletSchema::new(
                1,
                vec![
                    TabletColumn::key(0, "id", LogicalType::Integer),
                    TabletColumn::new(1, "name", LogicalType::Varchar),
                ],
                KeysType::PrimaryKeys,
            )
            .unwrap(),
        )
    }

    fn sample_chunk() -> Chunk {
        let alloc = Arc::new(default_allocator());
        let v1 =
            paro_common::test_utils::test_i32_vector_with_allocator(&[1, 2, 3, 4], alloc.clone());
        let v2 = paro_common::test_utils::test_string_vector_with_allocator(
            &["a", "b", "c", "d"],
            alloc,
        );
        test_chunk_from_arc_vectors(vec![Arc::new(v1), Arc::new(v2)])
    }

    #[test]
    fn insert_and_should_flush() {
        let schema = test_schema();
        let mut mt = MemTable::new(1, schema, 4, 1024);
        let decision = mt.insert(&sample_chunk()).unwrap();
        assert_eq!(decision, MemTableDecision::Flush); // reached max_rows
        assert_eq!(mt.stats().rows, 4);
    }

    #[test]
    fn write_buffer_reserve_drives_backpressure_and_releases_on_flush() {
        let schema = test_schema();
        let reserve = Arc::new(crate::buffer::FixedWriteBufferReserve::new(16));
        let options = MemTableOptions::new(1024, 1024 * 1024, Arc::new(default_allocator()))
            .with_write_buffer_reserve(reserve.clone());
        let mut mt = MemTable::new_with_options(1, schema, options);

        let decision = mt.insert(&sample_chunk()).unwrap();
        assert_eq!(decision, MemTableDecision::Backpressure);
        assert_eq!(reserve.reserved_bytes(), 0);

        mt.flush(|_, _| Ok(())).unwrap();
        assert_eq!(reserve.reserved_bytes(), 0);
    }

    #[test]
    fn flush_clears_buffer() {
        let schema = test_schema();
        let mut mt = MemTable::with_defaults(1, schema);
        mt.insert(&sample_chunk()).unwrap();
        let mut flushed_rows = 0;
        mt.flush(|chunks, rows| {
            flushed_rows = rows;
            assert_eq!(chunks.len(), 1);
            Ok(())
        })
        .unwrap();
        assert_eq!(flushed_rows, 4);
        assert_eq!(mt.stats().rows, 0);
        assert_eq!(mt.stats().chunks, 0);
    }

    #[test]
    fn primary_key_insert_dedups_within_and_across_batches() {
        let schema = test_primary_key_schema();
        let serializer = PrimaryKeySerializer::from_schema_ref(&schema).unwrap();
        let mut mt = MemTable::with_primary_keys(1, schema, serializer.clone(), 1024, 1024 * 1024);

        let alloc = Arc::new(default_allocator());
        let ids =
            paro_common::test_utils::test_i32_vector_with_allocator(&[1, 2, 2], alloc.clone());
        let names = paro_common::test_utils::test_string_vector_with_allocator(
            &["old-1", "old-2", "mid-2"],
            alloc,
        );
        let first = test_chunk_from_arc_vectors(vec![Arc::new(ids), Arc::new(names)]);

        let alloc = Arc::new(default_allocator());
        let ids = paro_common::test_utils::test_i32_vector_with_allocator(&[2, 3], alloc.clone());
        let names =
            paro_common::test_utils::test_string_vector_with_allocator(&["new-2", "new-3"], alloc);
        let second = test_chunk_from_arc_vectors(vec![Arc::new(ids), Arc::new(names)]);

        mt.insert(&first).unwrap();
        mt.insert(&second).unwrap();

        assert_eq!(mt.stats().rows, 3);
        let (chunks, ordered) = mt.drain_sorted_dedup().unwrap();
        assert_eq!(ordered.len(), 3);

        let rows: HashMap<Vec<u8>, String> = ordered
            .into_iter()
            .map(|(key, (chunk_idx, row_idx))| {
                let value = chunks[chunk_idx]
                    .column(1)
                    .unwrap()
                    .get_string(row_idx)
                    .unwrap()
                    .to_string();
                (key, value)
            })
            .collect();

        assert_eq!(
            rows.get(
                &serializer
                    .encode_values(&[paro_common::runtime_value::Value::Integer(1)])
                    .unwrap()
            ),
            Some(&"old-1".to_string())
        );
        assert_eq!(
            rows.get(
                &serializer
                    .encode_values(&[paro_common::runtime_value::Value::Integer(2)])
                    .unwrap()
            ),
            Some(&"new-2".to_string())
        );
        assert_eq!(
            rows.get(
                &serializer
                    .encode_values(&[paro_common::runtime_value::Value::Integer(3)])
                    .unwrap()
            ),
            Some(&"new-3".to_string())
        );
    }
}
