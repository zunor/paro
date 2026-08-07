// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::mem::size_of;
use std::ptr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    AccountedVec, GrantBuffer, MemoryAccountingClass, MemoryAccountingContext, MemoryGrant,
    MemoryReleaseHandle,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_storage::buffer::{BufferPool, MemoryTag, DEFAULT_BLOCK_ALLOC_SIZE};
use paro_storage::row::codec::{unsafe_api, PreparedRowScatter, RowHeapUsage, RowHeapWriter};
use paro_storage::row::{RowFormat, RowFormatHandle, RowLayout, RowValidityType};

use crate::operators::join::hash::row_format::HashJoinRowFormat;

use super::ht_entry::{increment_and_wrap, HtEntry};

#[derive(Debug, Clone)]
pub struct BuildRowLayout {
    base: Arc<RowLayout>,
    row_format: HashJoinRowFormat,
    key_count: usize,
    payload_count: usize,
    spill_types: Vec<LogicalType>,
    found_input_col_idx: Option<usize>,
    hash_input_col_idx: usize,
    hash_offset: usize,
    next_offset: usize,
    found_offset: usize,
    build_row_width: usize,
}

impl BuildRowLayout {
    pub fn new(
        equality_types: Vec<LogicalType>,
        build_types: Vec<LogicalType>,
        has_found_flag: bool,
    ) -> Self {
        let row_format = HashJoinRowFormat::build_spill(
            equality_types.clone(),
            build_types.clone(),
            has_found_flag,
        );
        let mut base_types = equality_types.clone();
        base_types.extend(build_types.clone());
        let base = Arc::new(RowLayout::from_types(
            base_types.clone(),
            RowValidityType::CanHaveNullValues,
        ));

        let spill_types = row_format.logical_types().to_vec();
        let found_input_col_idx = if has_found_flag {
            Some(base_types.len())
        } else {
            None
        };
        let hash_input_col_idx = spill_types.len() - 1;

        let hash_offset = base.row_width();
        let next_offset = hash_offset + size_of::<u64>();
        let found_offset = next_offset + size_of::<usize>();
        let build_row_width = found_offset + size_of::<u8>();

        Self {
            base,
            row_format,
            key_count: equality_types.len(),
            payload_count: build_types.len(),
            spill_types,
            found_input_col_idx,
            hash_input_col_idx,
            hash_offset,
            next_offset,
            found_offset,
            build_row_width,
        }
    }

    #[inline]
    pub fn base(&self) -> &Arc<RowLayout> {
        &self.base
    }

    #[inline]
    pub fn row_format(&self) -> &HashJoinRowFormat {
        &self.row_format
    }

    #[inline]
    pub fn key_count(&self) -> usize {
        self.key_count
    }

    #[inline]
    pub fn payload_count(&self) -> usize {
        self.payload_count
    }

    #[inline]
    pub fn spill_types(&self) -> &[LogicalType] {
        &self.spill_types
    }

    #[inline]
    pub fn found_input_col_idx(&self) -> Option<usize> {
        self.found_input_col_idx
    }

    #[inline]
    pub fn hash_input_col_idx(&self) -> usize {
        self.hash_input_col_idx
    }

    #[inline]
    pub fn hash_offset(&self) -> usize {
        self.hash_offset
    }

    #[inline]
    pub fn next_offset(&self) -> usize {
        self.next_offset
    }

    #[inline]
    pub fn found_offset(&self) -> usize {
        self.found_offset
    }

    #[inline]
    pub fn build_row_width(&self) -> usize {
        self.build_row_width
    }

    #[inline]
    pub fn payload_base_col_idx(&self, build_idx: usize) -> usize {
        self.key_count + build_idx
    }

    #[inline]
    pub fn set_hash(&self, row_ptr: *mut u8, hash: u64) {
        unsafe {
            ptr::write_unaligned(row_ptr.add(self.hash_offset) as *mut u64, hash);
        }
    }

    #[inline]
    pub fn hash(&self, row_ptr: *const u8) -> u64 {
        unsafe { ptr::read_unaligned(row_ptr.add(self.hash_offset) as *const u64) }
    }

    #[inline]
    pub fn set_next(&self, row_ptr: *mut u8, next: *const u8) {
        unsafe {
            ptr::write_unaligned(row_ptr.add(self.next_offset) as *mut *const u8, next);
        }
    }

    #[inline]
    pub fn next(&self, row_ptr: *const u8) -> *const u8 {
        unsafe { ptr::read_unaligned(row_ptr.add(self.next_offset) as *const *const u8) }
    }

    #[inline]
    pub fn set_found(&self, row_ptr: *mut u8, found: bool) {
        unsafe {
            (*(row_ptr.add(self.found_offset) as *const AtomicU8))
                .store(u8::from(found), Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn found(&self, row_ptr: *const u8) -> bool {
        unsafe {
            (*(row_ptr.add(self.found_offset) as *const AtomicU8)).load(Ordering::Relaxed) != 0
        }
    }

    pub fn read_value(&self, row_ptr: *const u8, col_idx: usize) -> Value {
        unsafe { unsafe_api::read_row_value(self.base.as_ref(), row_ptr, col_idx) }
    }

    /// Gather one build payload column directly into a flat output vector.
    ///
    /// # Safety
    /// Every non-zero entry in `row_ptrs` must reference a live row owned by
    /// the corresponding hash-build store.
    pub unsafe fn gather_payload_column(
        &self,
        row_ptrs: &[usize],
        build_idx: usize,
        output: &mut Vector,
    ) -> Result<()> {
        if build_idx >= self.payload_count {
            return Err(paro_error::internal(format!(
                "build payload column {build_idx} out of range {}",
                self.payload_count
            )));
        }
        let column_idx = self.payload_base_col_idx(build_idx);
        // SAFETY: upheld by this method's caller; a zero address is deliberately
        // mapped to a null row for unmatched outer/single join results.
        unsafe {
            paro_storage::row::codec::gather_column_from_rows(
                self.base.as_ref(),
                column_idx,
                output,
                row_ptrs.len(),
                |row_idx| row_ptrs[row_idx] as *const u8,
            )
        }
    }
}

/// One contiguous build-row slab.
///
/// Row pointers derived from this block remain valid only while the block itself is alive.
/// Out-of-line values are retained by the owning [`HashBuildStore`], so callers must not
/// retain row pointers after that store drops.
struct BuildBlock {
    data: paro_common::memory::GrantBuffer,
    row_width: usize,
    row_count: usize,
    max_rows: usize,
    used_bytes: usize,
}

/// Owns nested row values without allowing their addresses to move.
#[derive(Default)]
struct StableValueHeap {
    #[allow(clippy::vec_box)] // Each box address is embedded in a serialized row.
    values: Vec<Box<Value>>,
}

impl StableValueHeap {
    fn push(&mut self, value: Box<Value>) {
        self.values.push(value);
    }

    fn append(&mut self, other: &mut Self) {
        self.values.append(&mut other.values);
    }

    fn clear(&mut self) {
        self.values.clear();
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

/// Append-scoped writer over one exactly sized byte allocation.
///
/// Moving the owning [`GrantBuffer`] after the append does not move its
/// allocation, and nested values are boxed before their pointers enter rows.
struct BatchRowHeap<'a> {
    bytes: Option<&'a GrantBuffer>,
    byte_offset: usize,
    values: &'a mut StableValueHeap,
    value_bytes: usize,
}

impl<'a> BatchRowHeap<'a> {
    fn new(bytes: Option<&'a GrantBuffer>, values: &'a mut StableValueHeap) -> Self {
        Self {
            bytes,
            byte_offset: 0,
            values,
            value_bytes: 0,
        }
    }

    fn validate_complete(&self, expected: RowHeapUsage) -> Result<()> {
        if self.byte_offset != expected.varlen_bytes()
            || self.value_bytes != expected.nested_value_bytes()
        {
            return Err(paro_error::internal(format!(
                "prepared row heap measurement changed while scattering: expected_bytes={}, actual_bytes={}, expected_values={}, actual_values={}",
                expected.varlen_bytes(),
                self.byte_offset,
                expected.nested_value_bytes(),
                self.value_bytes
            )));
        }
        Ok(())
    }
}

impl RowHeapWriter for BatchRowHeap<'_> {
    fn store_bytes(&mut self, bytes: &[u8]) -> Result<*const u8> {
        let buffer = self.bytes.ok_or_else(|| {
            paro_error::internal("row scatter wrote bytes without measured byte heap")
        })?;
        let end = self
            .byte_offset
            .checked_add(bytes.len())
            .ok_or_else(|| paro_error::out_of_range("row byte heap cursor overflow"))?;
        if end > buffer.size() {
            return Err(paro_error::internal(format!(
                "row scatter exceeded measured byte heap: end={end}, capacity={}",
                buffer.size()
            )));
        }
        let target = unsafe { buffer.as_ptr().add(self.byte_offset) };
        unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), target, bytes.len()) };
        self.byte_offset = end;
        Ok(target)
    }

    fn store_value(&mut self, value: Value) -> Result<*const Value> {
        self.value_bytes = self
            .value_bytes
            .checked_add(size_of::<Value>())
            .and_then(|bytes| bytes.checked_add(value.allocation_size()))
            .ok_or_else(|| paro_error::out_of_range("row nested-value heap size overflow"))?;
        let retained = Box::new(value);
        let pointer = retained.as_ref() as *const Value;
        self.values.push(retained);
        Ok(pointer)
    }
}

struct RetainedMemory(MemoryReleaseHandle);

impl Drop for RetainedMemory {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl BuildBlock {
    fn new(
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
        max_rows: usize,
        row_width: usize,
    ) -> Result<Self> {
        let allocated_bytes = max_rows.saturating_mul(row_width);
        let data = memory.allocate_zeroed_buffer(allocator, allocated_bytes)?;
        Ok(Self {
            data,
            row_width,
            row_count: 0,
            max_rows,
            used_bytes: 0,
        })
    }

    fn can_accept(&self) -> bool {
        self.row_count < self.max_rows
    }

    fn row_count(&self) -> usize {
        self.row_count
    }

    #[inline]
    fn row_ptr(&self, row_idx: usize) -> *const u8 {
        debug_assert!(row_idx < self.row_count);
        unsafe { self.data.as_ptr().add(row_idx * self.row_width) }
    }

    #[inline]
    fn row_ptr_mut(&mut self, row_idx: usize) -> *mut u8 {
        debug_assert!(row_idx < self.max_rows);
        unsafe { self.data.as_ptr().add(row_idx * self.row_width) }
    }

    fn append_row<H, F>(
        &mut self,
        layout: &BuildRowLayout,
        row_heap_bytes: usize,
        heap: &mut H,
        write_row: F,
    ) -> Result<*mut u8>
    where
        H: RowHeapWriter,
        F: FnOnce(*mut u8, &mut H) -> Result<()>,
    {
        if !self.can_accept() {
            return Err(paro_error::internal(
                "BuildBlock append_row called on a full block".to_string(),
            ));
        }

        let row_idx = self.row_count;
        let row_ptr = self.row_ptr_mut(row_idx);
        unsafe {
            ptr::write_bytes(row_ptr, 0, self.row_width);
        }

        write_row(row_ptr, heap)?;

        if let Some(heap_size_offset) = layout.base().heap_size_offset() {
            let heap_used = u64::try_from(row_heap_bytes)
                .map_err(|_| paro_error::out_of_range("hash build row heap size exceeds u64"))?;
            unsafe {
                ptr::write_unaligned(row_ptr.add(heap_size_offset) as *mut u64, heap_used);
            }
        }

        self.row_count += 1;
        self.used_bytes = self
            .used_bytes
            .saturating_add(self.row_width)
            .saturating_add(row_heap_bytes);
        Ok(row_ptr)
    }
}

impl std::fmt::Debug for BuildBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuildBlock")
            .field("row_count", &self.row_count)
            .field("max_rows", &self.max_rows)
            .field("used_bytes", &self.used_bytes)
            .finish()
    }
}

#[derive(Debug, Clone, Default)]
pub struct BuildStoreScanState {
    block_idx: usize,
    row_idx: usize,
}

pub struct HashBuildStore {
    layout: BuildRowLayout,
    buffer_pool: Arc<BufferPool>,
    allocator: Arc<dyn Allocator>,
    tag: MemoryTag,
    memory: MemoryAccountingContext,
    blocks: AccountedVec<BuildBlock>,
    heap_buffers: Vec<GrantBuffer>,
    owned_values: StableValueHeap,
    value_releases: Vec<RetainedMemory>,
    count: u32,
}

impl std::fmt::Debug for HashBuildStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashBuildStore")
            .field("layout", &self.layout)
            .field(
                "row_format",
                &RowFormatHandle::from_format(self.layout.row_format()),
            )
            .field("buffer_pool", &self.buffer_pool)
            .field("allocator", &self.allocator.name())
            .field("tag", &self.tag)
            .field("memory", &self.memory)
            .field("blocks", &self.blocks)
            .field("heap_buffers", &self.heap_buffers.len())
            .field("owned_values", &self.owned_values.len())
            .field("count", &self.count)
            .finish()
    }
}

impl HashBuildStore {
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        allocator: Arc<dyn Allocator>,
        layout: BuildRowLayout,
        tag: MemoryTag,
    ) -> Self {
        let memory =
            MemoryAccountingContext::detached(tag, MemoryAccountingClass::default_for_tag(tag));
        Self::new_with_memory(buffer_pool, allocator, layout, tag, memory)
    }

    pub fn new_with_memory(
        buffer_pool: Arc<BufferPool>,
        allocator: Arc<dyn Allocator>,
        layout: BuildRowLayout,
        tag: MemoryTag,
        memory: MemoryAccountingContext,
    ) -> Self {
        let block_metadata_memory = memory.with_class(MemoryAccountingClass::Metadata);
        let blocks =
            accounted_vec_for_context(&block_metadata_memory, tag, MemoryAccountingClass::Metadata);
        Self {
            layout,
            buffer_pool,
            allocator,
            tag,
            memory,
            blocks,
            heap_buffers: Vec::new(),
            owned_values: StableValueHeap::default(),
            value_releases: Vec::new(),
            count: 0,
        }
    }

    #[inline]
    pub fn layout(&self) -> &BuildRowLayout {
        &self.layout
    }

    #[inline]
    pub fn count(&self) -> u32 {
        self.count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn size_in_bytes(&self) -> usize {
        self.blocks.iter().map(|block| block.used_bytes).sum()
    }

    pub fn reset(&mut self) {
        self.blocks.clear();
        self.heap_buffers.clear();
        self.owned_values.clear();
        self.value_releases.clear();
        self.count = 0;
    }

    pub fn merge(&mut self, mut other: HashBuildStore) -> Result<()> {
        if self.layout.row_format() != other.layout.row_format() {
            return Err(paro_error::internal(
                "cannot merge HashBuildStore with mismatched layouts".to_string(),
            ));
        }

        self.count = self.count.saturating_add(other.count);
        let mut other_blocks = other.blocks;
        for block in other_blocks.drain() {
            self.blocks.try_push(block)?;
        }
        self.heap_buffers.append(&mut other.heap_buffers);
        self.owned_values.append(&mut other.owned_values);
        self.value_releases.append(&mut other.value_releases);
        Ok(())
    }

    pub fn append_chunk(&mut self, chunk: &Chunk) -> Result<usize> {
        if chunk.size() == 0 {
            return Ok(0);
        }
        if chunk.column_count() != self.layout.row_format().logical_types().len() {
            return Err(paro_error::internal(format!(
                "HashBuildStore append width mismatch: expected {}, got {}",
                self.layout.row_format().logical_types().len(),
                chunk.column_count()
            )));
        }
        if chunk.types() != self.layout.row_format().logical_types() {
            return Err(paro_error::internal(format!(
                "HashBuildStore append type mismatch: expected {:?}, got {:?}",
                self.layout.row_format().logical_types(),
                chunk.types()
            )));
        }

        let base_columns = (0..self.layout.base().column_count())
            .map(|col_idx| {
                chunk.column(col_idx).ok_or_else(|| {
                    paro_error::internal(format!("missing HashBuildStore input column {col_idx}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let hash_vector = chunk
            .column(self.layout.hash_input_col_idx())
            .ok_or_else(|| {
                paro_error::internal("missing HashBuildStore hash column".to_string())
            })?;
        let found_vector = self
            .layout
            .found_input_col_idx()
            .map(|idx| {
                chunk.column(idx).ok_or_else(|| {
                    paro_error::internal("missing HashBuildStore found column".to_string())
                })
            })
            .transpose()?;
        let base_columns = base_columns
            .iter()
            .map(|column| column.as_ref())
            .collect::<Vec<_>>();
        let source =
            PreparedRowScatter::try_new(self.layout.base().as_ref(), &base_columns, chunk.size())?;
        self.append_prepared_rows(
            &source,
            chunk.size(),
            |output_idx| output_idx,
            |output_idx, _| {
                hash_vector.get_u64(output_idx).ok_or_else(|| {
                    paro_error::internal("HashBuildStore hash must not be NULL".to_string())
                })
            },
            |output_idx, _| {
                found_vector
                    .as_ref()
                    .and_then(|vector| vector.get_bool(output_idx))
                    .unwrap_or(false)
            },
        )
    }

    pub fn append_key_payload_chunk(
        &mut self,
        keys: &Chunk,
        payload: &Chunk,
        selection: &SelectionVector,
        selected_count: usize,
        hashes: &[u64],
        found: bool,
    ) -> Result<usize> {
        if selected_count == 0 {
            return Ok(0);
        }
        if hashes.len() < selected_count {
            return Err(paro_error::internal(format!(
                "HashBuildStore append hash count mismatch: selected={selected_count}, hashes={}",
                hashes.len()
            )));
        }
        if keys.column_count() != self.layout.key_count() {
            return Err(paro_error::internal(format!(
                "HashBuildStore key width mismatch: expected {}, got {}",
                self.layout.key_count(),
                keys.column_count()
            )));
        }
        if payload.column_count() != self.layout.payload_count() {
            return Err(paro_error::internal(format!(
                "HashBuildStore payload width mismatch: expected {}, got {}",
                self.layout.payload_count(),
                payload.column_count()
            )));
        }
        let expected_keys = &self.layout.base().types()[..self.layout.key_count()];
        if keys.types() != expected_keys {
            return Err(paro_error::internal(format!(
                "HashBuildStore key type mismatch: expected {expected_keys:?}, got {:?}",
                keys.types()
            )));
        }
        let payload_start = self.layout.key_count();
        let expected_payload =
            &self.layout.base().types()[payload_start..payload_start + self.layout.payload_count()];
        if payload.types() != expected_payload {
            return Err(paro_error::internal(format!(
                "HashBuildStore payload type mismatch: expected {expected_payload:?}, got {:?}",
                payload.types()
            )));
        }

        let mut base_columns = Vec::with_capacity(self.layout.base().column_count());
        for col_idx in 0..self.layout.key_count() {
            base_columns.push(keys.column(col_idx).ok_or_else(|| {
                paro_error::internal(format!("missing hash join key column {col_idx}"))
            })?);
        }
        for payload_idx in 0..self.layout.payload_count() {
            base_columns.push(payload.column(payload_idx).ok_or_else(|| {
                paro_error::internal(format!("missing hash join payload column {payload_idx}"))
            })?);
        }

        let base_columns = base_columns
            .iter()
            .map(|column| column.as_ref())
            .collect::<Vec<_>>();
        let source =
            PreparedRowScatter::try_new(self.layout.base().as_ref(), &base_columns, keys.size())?;
        self.append_prepared_rows(
            &source,
            selected_count,
            |output_idx| selection.get(output_idx),
            |output_idx, _| Ok(hashes[output_idx]),
            |_, _| found,
        )
    }

    fn append_prepared_rows<S, H, F>(
        &mut self,
        source: &PreparedRowScatter<'_>,
        output_count: usize,
        source_row_at: S,
        hash_at: H,
        found_at: F,
    ) -> Result<usize>
    where
        S: Fn(usize) -> usize,
        H: Fn(usize, usize) -> Result<u64>,
        F: Fn(usize, usize) -> bool,
    {
        let mut row_usage = Vec::with_capacity(output_count);
        let mut total_usage = RowHeapUsage::default();
        for output_idx in 0..output_count {
            let usage = source.heap_usage(source_row_at(output_idx))?;
            total_usage = total_usage.checked_add(usage)?;
            row_usage.push(usage);
        }

        let byte_buffer = if total_usage.varlen_bytes() == 0 {
            None
        } else {
            Some(
                self.memory
                    .allocate_buffer(self.allocator.clone(), total_usage.varlen_bytes())?,
            )
        };
        let value_release = if total_usage.nested_value_bytes() == 0 {
            None
        } else {
            match self.memory.retain(total_usage.nested_value_bytes()) {
                Ok(release) => Some(release),
                Err(error) => return Err(error.into()),
            }
        };
        let mut batch_values = StableValueHeap::default();
        let layout = self.layout.clone();

        let write_result = {
            let mut heap = BatchRowHeap::new(byte_buffer.as_ref(), &mut batch_values);
            (|| {
                for output_idx in 0..output_count {
                    let source_row_idx = source_row_at(output_idx);
                    let usage = row_usage[output_idx];
                    let row_heap_bytes = usage
                        .varlen_bytes()
                        .checked_add(usage.nested_value_bytes())
                        .ok_or_else(|| {
                            paro_error::out_of_range("hash build row heap size overflow")
                        })?;
                    let block = self.ensure_current_block()?;
                    block.append_row(&layout, row_heap_bytes, &mut heap, |row_ptr, heap| {
                        unsafe {
                            source.scatter_row(
                                layout.base().as_ref(),
                                row_ptr,
                                source_row_idx,
                                heap,
                            )?;
                        }
                        layout.set_hash(row_ptr, hash_at(output_idx, source_row_idx)?);
                        layout.set_next(row_ptr, ptr::null());
                        layout.set_found(row_ptr, found_at(output_idx, source_row_idx));
                        Ok(())
                    })?;
                }
                heap.validate_complete(total_usage)
            })()
        };
        if let Err(error) = write_result {
            if let Some(release) = value_release {
                release.release();
            }
            return Err(error);
        }

        if let Some(buffer) = byte_buffer {
            self.heap_buffers.push(buffer);
        }
        self.owned_values.append(&mut batch_values);
        if let Some(release) = value_release {
            self.value_releases.push(RetainedMemory(release));
        }
        self.count = self.count.saturating_add(output_count as u32);
        Ok(output_count)
    }

    fn ensure_current_block(&mut self) -> Result<&mut BuildBlock> {
        if self
            .blocks
            .last()
            .map(|block| !block.can_accept())
            .unwrap_or(true)
        {
            let rows_per_block =
                (DEFAULT_BLOCK_ALLOC_SIZE / self.layout.build_row_width().max(1)).max(1);
            let block = BuildBlock::new(
                self.allocator.clone(),
                self.memory.clone(),
                rows_per_block,
                self.layout.build_row_width(),
            )?;
            self.blocks.try_push(block)?;
        }
        Ok(self.blocks.last_mut().expect("build block must exist"))
    }

    pub fn build_pointer_chains(&mut self, entries: &mut [HtEntry], bitmask: usize) -> bool {
        let mut has_long_chains = false;

        for block in self.blocks.iter_mut() {
            for row_idx in 0..block.row_count() {
                let row_ptr = block.row_ptr_mut(row_idx);
                let hash = self.layout.hash(row_ptr);
                let salt = hash & HtEntry::SALT_MASK;
                let mut ht_offset = (hash as usize) & bitmask;

                while entries[ht_offset].is_occupied() && entries[ht_offset].get_salt_bits() != salt
                {
                    increment_and_wrap(&mut ht_offset, bitmask);
                }

                let prev_ptr = if entries[ht_offset].is_occupied() {
                    entries[ht_offset].get_pointer()
                } else {
                    ptr::null()
                };
                self.layout.set_next(row_ptr, prev_ptr);

                if !entries[ht_offset].is_occupied() {
                    entries[ht_offset] = HtEntry::new(salt, row_ptr);
                } else {
                    entries[ht_offset].set_pointer(row_ptr);
                    has_long_chains = true;
                }
            }
        }

        has_long_chains
    }

    pub fn scan_spill_chunk(
        &self,
        state: &mut BuildStoreScanState,
        output: &mut Chunk,
    ) -> Result<usize> {
        ensure_chunk(output, self.layout.spill_types(), VECTOR_SIZE)?;
        output.try_reset(output.allocator().clone())?;

        let mut scanned = 0usize;
        while state.block_idx < self.blocks.len() && scanned < VECTOR_SIZE {
            let block = &self.blocks[state.block_idx];
            while state.row_idx < block.row_count() && scanned < VECTOR_SIZE {
                let row_ptr = block.row_ptr(state.row_idx);
                Self::write_spill_row(&self.layout, row_ptr, output, scanned);
                scanned += 1;
                state.row_idx += 1;
            }

            if state.row_idx >= block.row_count() {
                state.block_idx += 1;
                state.row_idx = 0;
            }
        }

        output.set_cardinality(scanned);
        Ok(scanned)
    }

    pub fn drain_spill_chunks<F>(self, mut visitor: F) -> Result<()>
    where
        F: FnMut(&Chunk) -> Result<()>,
    {
        let HashBuildStore {
            layout,
            allocator,
            mut blocks,
            ..
        } = self;
        let mut chunk = Chunk::try_new(allocator.clone())?;
        for block in blocks.drain() {
            let row_count = block.row_count();
            if row_count == 0 {
                continue;
            }

            ensure_chunk(&mut chunk, layout.spill_types(), row_count)?;
            chunk.try_reset(chunk.allocator().clone())?;
            for scanned in 0..row_count {
                Self::write_spill_row(&layout, block.row_ptr(scanned), &mut chunk, scanned);
            }
            chunk.set_cardinality(row_count);
            visitor(&chunk)?;
        }
        Ok(())
    }

    pub fn scan_payload_rows(
        &self,
        state: &mut BuildStoreScanState,
        emit_found: bool,
        build_types: &[LogicalType],
        output: &mut Chunk,
    ) -> Result<usize> {
        ensure_chunk(output, build_types, VECTOR_SIZE)?;
        output.try_reset(output.allocator().clone())?;

        let mut scanned = 0usize;
        while state.block_idx < self.blocks.len() && scanned < VECTOR_SIZE {
            let block = &self.blocks[state.block_idx];
            while state.row_idx < block.row_count() && scanned < VECTOR_SIZE {
                let row_ptr = block.row_ptr(state.row_idx);
                if self.layout.found(row_ptr) != emit_found {
                    state.row_idx += 1;
                    continue;
                }

                for build_idx in 0..build_types.len() {
                    let value = self
                        .layout
                        .read_value(row_ptr, self.layout.payload_base_col_idx(build_idx));
                    output
                        .column_mut(build_idx)
                        .expect("payload output column must exist")
                        .set_value(scanned, &value);
                }

                scanned += 1;
                state.row_idx += 1;
            }

            if state.row_idx >= block.row_count() {
                state.block_idx += 1;
                state.row_idx = 0;
            }
        }

        output.set_cardinality(scanned);
        Ok(scanned)
    }

    pub fn all_row_ptrs(&self) -> Vec<usize> {
        self.blocks
            .iter()
            .flat_map(|block| (0..block.row_count()).map(|row_idx| block.row_ptr(row_idx) as usize))
            .collect()
    }

    pub fn visit_row_ptrs(&self, mut visitor: impl FnMut(usize) -> Result<()>) -> Result<()> {
        for block in self.blocks.iter() {
            for row_idx in 0..block.row_count() {
                visitor(block.row_ptr(row_idx) as usize)?;
            }
        }
        Ok(())
    }

    fn write_spill_row(
        layout: &BuildRowLayout,
        row_ptr: *const u8,
        output: &mut Chunk,
        output_idx: usize,
    ) {
        for col_idx in 0..layout.base().column_count() {
            let value = layout.read_value(row_ptr, col_idx);
            output
                .column_mut(col_idx)
                .expect("spill output column must exist")
                .set_value(output_idx, &value);
        }
        if let Some(found_col_idx) = layout.found_input_col_idx() {
            output
                .column_mut(found_col_idx)
                .expect("spill found column must exist")
                .set_value(output_idx, &Value::Boolean(layout.found(row_ptr)));
        }
        output
            .column_mut(layout.hash_input_col_idx())
            .expect("spill hash column must exist")
            .set_value(output_idx, &Value::UBigInt(layout.hash(row_ptr)));
    }
}

fn ensure_chunk(output: &mut Chunk, types: &[LogicalType], capacity: usize) -> Result<()> {
    if output.column_count() != types.len()
        || output.types() != types
        || output.capacity() < capacity
    {
        *output = Chunk::try_initialize(types, capacity, output.allocator().clone())?;
    } else {
        output.try_reset(output.allocator().clone())?;
    }
    Ok(())
}

fn accounted_vec_for_context<T>(
    memory: &MemoryAccountingContext,
    tag: MemoryTag,
    class: MemoryAccountingClass,
) -> AccountedVec<T> {
    let grant = if let Some(owner) = memory.owner() {
        MemoryGrant::new(0, memory.domain(), owner)
            .expect("zero-byte metadata grant should not fail")
    } else {
        MemoryGrant::detached(usize::MAX / 4, memory.domain())
    };
    AccountedVec::new_with_accounting(grant, tag, class)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;

    fn create_test_store(
        equality_types: Vec<LogicalType>,
        build_types: Vec<LogicalType>,
        has_found_flag: bool,
    ) -> HashBuildStore {
        HashBuildStore::new(
            Arc::new(BufferPool::new(32 * 1024 * 1024)),
            paro_common::test_utils::test_allocator(),
            BuildRowLayout::new(equality_types, build_types, has_found_flag),
            MemoryTag::HashTable,
        )
    }

    #[test]
    fn stores_rows_contiguously_with_fixed_stride() {
        let mut keys = paro_common::test_utils::test_vector(LogicalType::Integer);
        keys.set_i32(0, 1);
        keys.set_i32(1, 2);
        keys.set_i32(2, 3);
        keys.set_count(3);

        let mut payload = paro_common::test_utils::test_vector(LogicalType::Integer);
        payload.set_i32(0, 10);
        payload.set_i32(1, 20);
        payload.set_i32(2, 30);
        payload.set_count(3);

        let mut hashes = paro_common::test_utils::test_vector(LogicalType::UBigInt);
        hashes.set_u64(0, 101);
        hashes.set_u64(1, 202);
        hashes.set_u64(2, 303);
        hashes.set_count(3);

        let chunk = Chunk::from_arc_vectors(
            vec![Arc::new(keys), Arc::new(payload), Arc::new(hashes)],
            paro_common::test_utils::test_allocator(),
        );
        let mut store = create_test_store(
            vec![LogicalType::Integer],
            vec![LogicalType::Integer],
            false,
        );
        store.append_chunk(&chunk).unwrap();

        let row_ptrs = store.all_row_ptrs();
        assert_eq!(row_ptrs.len(), 3);
        assert_eq!(row_ptrs[1] - row_ptrs[0], store.layout().build_row_width());
        assert_eq!(row_ptrs[2] - row_ptrs[1], store.layout().build_row_width());
    }

    #[test]
    fn varlen_spill_scan_round_trips_without_boxing_values() {
        let mut keys = paro_common::test_utils::test_vector(LogicalType::Integer);
        keys.set_i32(0, 1);
        keys.set_i32(1, 2);
        keys.set_count(2);

        let mut payload = paro_common::test_utils::test_vector(LogicalType::Varchar);
        payload.set_string(0, "this string is long enough to spill");
        payload.set_string(1, "short");
        payload.set_count(2);

        let mut found = paro_common::test_utils::test_vector(LogicalType::Boolean);
        found.set_bool(0, true);
        found.set_bool(1, false);
        found.set_count(2);

        let mut hashes = paro_common::test_utils::test_vector(LogicalType::UBigInt);
        hashes.set_u64(0, 501);
        hashes.set_u64(1, 502);
        hashes.set_count(2);

        let chunk = Chunk::from_arc_vectors(
            vec![
                Arc::new(keys),
                Arc::new(payload),
                Arc::new(found),
                Arc::new(hashes),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut store =
            create_test_store(vec![LogicalType::Integer], vec![LogicalType::Varchar], true);
        store.append_chunk(&chunk).unwrap();

        assert_eq!(store.owned_values.len(), 0);
        assert_eq!(store.heap_buffers.len(), 1);

        let mut scan_state = BuildStoreScanState::default();
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let scanned = store
            .scan_spill_chunk(&mut scan_state, &mut output)
            .unwrap();
        assert_eq!(scanned, 2);
        assert_eq!(output.size(), 2);
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(1)));
        assert_eq!(
            output.get_value(1, 0),
            Some(Value::Varchar(
                "this string is long enough to spill".to_string()
            ))
        );
        assert_eq!(output.get_value(2, 0), Some(Value::Boolean(true)));
        assert_eq!(output.get_value(3, 1), Some(Value::UBigInt(502)));
    }
}
