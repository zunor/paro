// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::mem::size_of;
use std::ptr;
use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    AccountedVec, MemoryAccountingClass, MemoryAccountingContext, MemoryGrant, MemoryReleaseHandle,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_storage::buffer::{BufferPool, MemoryTag, DEFAULT_BLOCK_ALLOC_SIZE};
use paro_storage::row::codec::unsafe_api;
use paro_storage::row::{RowLayout, RowValidityType};

use super::ht_entry::{increment_and_wrap, HtEntry};

#[derive(Debug, Clone)]
pub struct BuildRowLayout {
    base: Arc<RowLayout>,
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
        let mut base_types = equality_types.clone();
        base_types.extend(build_types.clone());
        let base = Arc::new(RowLayout::from_types(
            base_types.clone(),
            RowValidityType::CanHaveNullValues,
        ));

        let mut spill_types = base_types;
        let found_input_col_idx = if has_found_flag {
            spill_types.push(LogicalType::Boolean);
            Some(spill_types.len() - 1)
        } else {
            None
        };
        let hash_input_col_idx = spill_types.len();
        spill_types.push(LogicalType::UBigInt);

        let hash_offset = base.row_width();
        let next_offset = hash_offset + size_of::<u64>();
        let found_offset = next_offset + size_of::<usize>();
        let build_row_width = found_offset + size_of::<u8>();

        Self {
            base,
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
            ptr::write_unaligned(row_ptr.add(self.found_offset), u8::from(found));
        }
    }

    #[inline]
    pub fn found(&self, row_ptr: *const u8) -> bool {
        unsafe { ptr::read_unaligned(row_ptr.add(self.found_offset)) != 0 }
    }

    pub fn read_value(&self, row_ptr: *const u8, col_idx: usize) -> Value {
        unsafe { unsafe_api::read_row_value(self.base.as_ref(), row_ptr, col_idx) }
    }
}

/// One contiguous build-row slab plus the heap ownership that keeps row pointers valid.
///
/// Row pointers derived from this block remain valid only while the block itself is alive.
/// The varlen / nested heap owners stored alongside the slab intentionally share the same
/// lifetime so callers must not retain row pointers after the owning `HashBuildStore` drops.
struct BuildBlock {
    data: paro_common::memory::GrantBuffer,
    memory: MemoryAccountingContext,
    row_width: usize,
    row_count: usize,
    max_rows: usize,
    heap_bytes: usize,
    used_bytes: usize,
    heap_releases: Vec<MemoryReleaseHandle>,
    owned_bytes: Vec<Box<[u8]>>,
    // These boxed values back row pointers written into the slab, so their
    // allocation addresses must remain stable even if the Vec grows.
    #[allow(clippy::vec_box)]
    owned_values: Vec<Box<Value>>,
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
            memory,
            row_width,
            row_count: 0,
            max_rows,
            heap_bytes: 0,
            used_bytes: 0,
            heap_releases: Vec::new(),
            owned_bytes: Vec::new(),
            owned_values: Vec::new(),
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

    fn append_row<F>(
        &mut self,
        layout: &BuildRowLayout,
        estimated_heap_bytes: usize,
        write_row: F,
    ) -> Result<*mut u8>
    where
        F: FnOnce(*mut u8, &mut Vec<Box<[u8]>>, &mut Vec<Box<Value>>, &mut usize) -> Result<()>,
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

        if !layout.base().all_valid() {
            let validity_width = layout.base().validity().flag_width();
            unsafe {
                std::slice::from_raw_parts_mut(row_ptr, validity_width).fill(0xFF);
            }
        }

        let mut row_used_bytes = self.row_width;
        let heap_release = if estimated_heap_bytes == 0 {
            None
        } else {
            Some(self.memory.retain(estimated_heap_bytes)?)
        };

        let write_result = write_row(
            row_ptr,
            &mut self.owned_bytes,
            &mut self.owned_values,
            &mut row_used_bytes,
        );
        if let Err(err) = write_result {
            if let Some(release) = heap_release {
                release.release();
            }
            return Err(err);
        }

        if let Some(heap_size_offset) = layout.base().heap_size_offset() {
            let heap_used = row_used_bytes.saturating_sub(self.row_width) as u64;
            unsafe {
                ptr::write_unaligned(row_ptr.add(heap_size_offset) as *mut u64, heap_used);
            }
        }

        let row_heap_bytes = row_used_bytes.saturating_sub(self.row_width);
        match heap_release {
            Some(release) if row_heap_bytes == estimated_heap_bytes => {
                self.heap_releases.push(release);
            }
            Some(release) if row_heap_bytes == 0 => {
                release.release();
            }
            Some(release) if row_heap_bytes < estimated_heap_bytes => {
                release.release();
                self.heap_releases.push(self.memory.retain(row_heap_bytes)?);
            }
            Some(release) => {
                self.heap_releases.push(release);
                self.heap_releases
                    .push(self.memory.retain(row_heap_bytes - estimated_heap_bytes)?);
            }
            None if row_heap_bytes > 0 => {
                self.heap_releases.push(self.memory.retain(row_heap_bytes)?);
            }
            None => {}
        }
        self.row_count += 1;
        self.heap_bytes = self.heap_bytes.saturating_add(row_heap_bytes);
        self.used_bytes = self.used_bytes.saturating_add(row_used_bytes);
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

impl Drop for BuildBlock {
    fn drop(&mut self) {
        for release in &self.heap_releases {
            release.release();
        }
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
    count: u32,
}

impl std::fmt::Debug for HashBuildStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashBuildStore")
            .field("layout", &self.layout)
            .field("buffer_pool", &self.buffer_pool)
            .field("allocator", &self.allocator.name())
            .field("tag", &self.tag)
            .field("memory", &self.memory)
            .field("blocks", &self.blocks)
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
        self.count = 0;
    }

    pub fn merge(&mut self, other: HashBuildStore) -> Result<()> {
        if self.layout.spill_types() != other.layout.spill_types() {
            return Err(paro_error::internal(
                "cannot merge HashBuildStore with mismatched layouts".to_string(),
            ));
        }

        self.count = self.count.saturating_add(other.count);
        let mut other_blocks = other.blocks;
        for block in other_blocks.drain() {
            self.blocks.try_push(block)?;
        }
        Ok(())
    }

    pub fn append_chunk(&mut self, chunk: &Chunk) -> Result<usize> {
        if chunk.size() == 0 {
            return Ok(0);
        }
        if chunk.column_count() != self.layout.spill_types().len() {
            return Err(paro_error::internal(format!(
                "HashBuildStore append width mismatch: expected {}, got {}",
                self.layout.spill_types().len(),
                chunk.column_count()
            )));
        }

        let mut appended = 0usize;
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

        let layout = self.layout.clone();
        for row_idx in 0..chunk.size() {
            let estimated_heap_bytes = estimate_row_heap_bytes(&layout, &base_columns, row_idx);
            let block = self.ensure_current_block()?;
            block.append_row(
                &layout,
                estimated_heap_bytes,
                |row_ptr, owned_bytes, owned_values, used_bytes| {
                    for (col_idx, column) in base_columns.iter().enumerate() {
                        unsafe {
                            unsafe_api::write_vector_value(
                                layout.base(),
                                row_ptr,
                                col_idx,
                                column.as_ref(),
                                row_idx,
                                owned_bytes,
                                owned_values,
                                used_bytes,
                            )
                        }?;
                    }

                    let hash = hash_vector.get_u64(row_idx).ok_or_else(|| {
                        paro_error::internal("HashBuildStore hash must not be NULL".to_string())
                    })?;
                    layout.set_hash(row_ptr, hash);
                    layout.set_next(row_ptr, ptr::null());

                    let found = found_vector
                        .as_ref()
                        .and_then(|vector| vector.get_bool(row_idx))
                        .unwrap_or(false);
                    layout.set_found(row_ptr, found);
                    Ok(())
                },
            )?;
            appended += 1;
        }
        self.count = self.count.saturating_add(appended as u32);
        Ok(appended)
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

fn estimate_row_heap_bytes(
    layout: &BuildRowLayout,
    columns: &[&Arc<paro_common::vector::Vector>],
    row_idx: usize,
) -> usize {
    layout
        .base()
        .types()
        .iter()
        .enumerate()
        .map(|(col_idx, logical_type)| {
            if columns[col_idx].is_null(row_idx) {
                return 0;
            }
            let value = columns[col_idx].get_value(row_idx);
            estimate_value_heap_bytes(logical_type, &value)
        })
        .sum()
}

fn estimate_value_heap_bytes(logical_type: &LogicalType, value: &Value) -> usize {
    match (logical_type, value) {
        (
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::StringLiteral,
            Value::Varchar(value),
        ) => {
            if value.len() > 12 {
                value.len()
            } else {
                0
            }
        }
        (LogicalType::Blob, Value::Blob(value)) => {
            if value.len() > 12 {
                value.len()
            } else {
                0
            }
        }
        (LogicalType::List(_) | LogicalType::Array(_, _) | LogicalType::Struct(_), value) => {
            std::mem::size_of::<Value>().saturating_add(value.allocation_size())
        }
        _ => 0,
    }
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

        assert!(store
            .blocks
            .iter()
            .all(|block| block.owned_values.is_empty()));
        assert_eq!(
            store
                .blocks
                .iter()
                .map(|block| block.owned_bytes.len())
                .sum::<usize>(),
            1
        );

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
