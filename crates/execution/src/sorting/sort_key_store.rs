// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::sync::Mutex;

use paro_common::allocator::{Allocator, BufferAllocator, BufferManager};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    GrantBuffer, MemoryAccountingClass, MemoryAccountingContext, MemoryReleaseHandle,
};
use paro_common::sort_key::{compare_keys, SortKeyEncoding, MAX_SORT_KEY_SLOT_SIZE};
use paro_storage::buffer::{
    BlockId, BufferHandle, BufferPool, FileBufferType, MemoryTag, SharedBlockHandle,
    DEFAULT_BLOCK_SIZE,
};

const MATERIALIZED_SORT_PREFIX_LEN: usize = 32;
const PREFIX_SORT_MIN_ROWS: usize = 256;

#[derive(Debug, Clone, Copy)]
struct VariableSlotLayout {
    inline_len: usize,
    total_len_offset: usize,
    overflow_len_offset: usize,
    overflow_block_offset: usize,
    overflow_offset_offset: usize,
}

impl VariableSlotLayout {
    #[inline]
    fn new(encoding: &SortKeyEncoding) -> Self {
        debug_assert!(encoding.is_variable());
        let inline_len = encoding.inline_prefix_len();
        let layout = Self {
            inline_len,
            total_len_offset: inline_len,
            overflow_len_offset: inline_len + 4,
            overflow_block_offset: inline_len + 8,
            overflow_offset_offset: inline_len + 12,
        };
        debug_assert_eq!(layout.metadata_end(), encoding.slot_size());
        layout
    }

    #[inline]
    fn metadata_end(self) -> usize {
        self.overflow_offset_offset + std::mem::size_of::<u32>()
    }
}

#[derive(Debug)]
struct SlotBlockMeta {
    handle: Mutex<Option<SharedBlockHandle>>,
    release: MemoryReleaseHandle,
    row_start: u32,
    row_count: u32,
}

#[derive(Debug)]
struct OverflowBlockMeta {
    handle: Mutex<Option<SharedBlockHandle>>,
    release: MemoryReleaseHandle,
    ordinal_end: u32,
    used_bytes: usize,
    size: usize,
}

#[derive(Debug)]
struct KeyReleaseState {
    logical_release_frontier: AtomicU64,
    physical_release_frontier: AtomicU64,
    released_slot_block_prefix: AtomicUsize,
    released_overflow_block_prefix: AtomicUsize,
    outstanding_pins: AtomicUsize,
}

impl Default for KeyReleaseState {
    fn default() -> Self {
        Self {
            logical_release_frontier: AtomicU64::new(0),
            physical_release_frontier: AtomicU64::new(0),
            released_slot_block_prefix: AtomicUsize::new(0),
            released_overflow_block_prefix: AtomicUsize::new(0),
            outstanding_pins: AtomicUsize::new(0),
        }
    }
}

#[derive(Debug)]
struct CursorPin<'a> {
    store: &'a SortKeyStore,
}

impl Drop for CursorPin<'_> {
    fn drop(&mut self) {
        if self
            .store
            .release_state
            .outstanding_pins
            .fetch_sub(1, AtomicOrdering::AcqRel)
            == 1
        {
            self.store.try_release_prefix();
        }
    }
}

#[derive(Debug)]
struct PinnedBlocks {
    slot_handles: Vec<Option<BufferHandle>>,
    overflow_handles: Vec<Option<BufferHandle>>,
}

#[derive(Debug)]
struct OnDemandCachedBlock {
    block_idx: usize,
    handle: BufferHandle,
}

#[derive(Debug)]
struct OnDemandPinnedBlocks {
    window_size: usize,
    slot_handles: Vec<OnDemandCachedBlock>,
    overflow_handles: Vec<OnDemandCachedBlock>,
}

#[derive(Debug)]
enum CursorMode {
    Pinned(PinnedBlocks),
    OnDemand(OnDemandPinnedBlocks),
}

#[derive(Debug, Clone, Copy)]
struct OverflowKeyRef {
    block_idx: usize,
    offset: usize,
    len: usize,
}

#[derive(Debug, Clone, Copy)]
enum SlotComparison {
    Ordered(Ordering),
    Overflow {
        left: OverflowKeyRef,
        right: OverflowKeyRef,
    },
}

/// Immutable block-backed store for encoded sort keys.
#[derive(Debug)]
pub struct SortKeyStore {
    buffer_pool: Arc<BufferPool>,
    encoding: Arc<SortKeyEncoding>,
    memory: MemoryAccountingContext,
    slot_block_capacity: u32,
    slot_size: usize,
    slot_blocks: Vec<SlotBlockMeta>,
    overflow_blocks: Vec<OverflowBlockMeta>,
    total_bytes: usize,
    count: u32,
    current_slot: Option<BufferHandle>,
    current_overflow: Option<BufferHandle>,
    release_state: KeyReleaseState,
}

/// Query-accounted typed storage for sort finalize scratch and retained
/// permutations.
///
/// The raw grant buffer is `Send + Sync`, unlike a mutable `MemoryGrant`, so a
/// sealed run can safely share its immutable permutation across merge workers.
mod sort_buffer_element {
    pub(crate) trait Sealed {}

    impl Sealed for u32 {}
    impl Sealed for super::MaterializedSortPrefix {}
}

pub(crate) trait ZeroValidSortElement:
    sort_buffer_element::Sealed + Copy + Send + Sync + 'static
{
}

impl ZeroValidSortElement for u32 {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
struct MaterializedSortPrefix {
    bytes: [u8; MATERIALIZED_SORT_PREFIX_LEN],
    effective_len: u8,
}

impl ZeroValidSortElement for MaterializedSortPrefix {}

#[derive(Debug)]
pub(crate) struct AccountedSortBuffer<T: ZeroValidSortElement> {
    buffer: GrantBuffer,
    len: usize,
    _value: PhantomData<T>,
}

impl<T: ZeroValidSortElement> AccountedSortBuffer<T> {
    fn try_new(
        memory: &MemoryAccountingContext,
        buffer_pool: Arc<BufferPool>,
        len: usize,
    ) -> Result<Self> {
        let bytes = len
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| paro_error::internal("sort finalize allocation overflow"))?;
        let manager: Arc<dyn BufferManager> = buffer_pool;
        let allocator: Arc<dyn Allocator> =
            Arc::new(BufferAllocator::new(manager, MemoryTag::OrderBy));
        let buffer = memory.allocate_zeroed_buffer(allocator, bytes)?;
        if bytes > 0 && buffer.as_ptr().is_null() {
            return Err(paro_error::internal(
                "sort finalize allocation returned no storage",
            ));
        }
        if !(Self::slice_ptr(&buffer, len) as usize).is_multiple_of(std::mem::align_of::<T>()) {
            return Err(paro_error::internal(
                "sort finalize allocation does not satisfy element alignment",
            ));
        }
        Ok(Self {
            buffer,
            len,
            _value: PhantomData,
        })
    }

    #[inline]
    fn slice_ptr(buffer: &GrantBuffer, len: usize) -> *mut T {
        if len == 0 {
            NonNull::<T>::dangling().as_ptr()
        } else {
            buffer.as_ptr().cast::<T>()
        }
    }

    #[inline]
    pub(crate) fn size_in_bytes(&self) -> usize {
        self.buffer.size()
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[T] {
        self
    }

    #[inline]
    pub(crate) fn as_mut_slice(&mut self) -> &mut [T] {
        self
    }
}

impl<T: ZeroValidSortElement> Deref for AccountedSortBuffer<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        // SAFETY: construction allocates and zero-initializes exactly
        // `len * size_of::<T>()` bytes with `T` alignment. The sealed
        // `ZeroValidSortElement` bound admits only integer types whose all-zero
        // bit pattern is valid. Empty buffers use an aligned, non-null dangling
        // pointer as required by Rust's slice representation.
        unsafe { std::slice::from_raw_parts(Self::slice_ptr(&self.buffer, self.len), self.len) }
    }
}

impl<T: ZeroValidSortElement> DerefMut for AccountedSortBuffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: mutable access requires `&mut self`, so the backing grant
        // buffer is exclusively borrowed for the returned slice. Empty buffers
        // use an aligned, non-null dangling pointer and expose no elements.
        unsafe { std::slice::from_raw_parts_mut(Self::slice_ptr(&self.buffer, self.len), self.len) }
    }
}

pub(crate) type SortPermutation = AccountedSortBuffer<u32>;
type SortPrefixes = AccountedSortBuffer<MaterializedSortPrefix>;

/// Cursor over encoded sort keys.
#[derive(Debug)]
pub struct KeyCursor<'a> {
    store: &'a SortKeyStore,
    mode: CursorMode,
    _pin: CursorPin<'a>,
}

/// Comparator wrapper that delegates to a [`KeyCursor`].
#[derive(Debug)]
pub struct KeyComparator<'cursor, 'store> {
    cursor: &'cursor mut KeyCursor<'store>,
}

impl<'cursor, 'store> KeyComparator<'cursor, 'store> {
    pub fn new(cursor: &'cursor mut KeyCursor<'store>) -> Self {
        Self { cursor }
    }

    pub fn compare(&mut self, left: u32, right: u32) -> Result<Ordering> {
        self.cursor.compare(left, right)
    }
}

impl SortKeyStore {
    pub fn new(buffer_pool: Arc<BufferPool>, encoding: Arc<SortKeyEncoding>) -> Self {
        let memory =
            MemoryAccountingContext::detached(MemoryTag::OrderBy, MemoryAccountingClass::Revocable);
        Self::new_with_memory(buffer_pool, encoding, memory)
    }

    pub fn new_with_memory(
        buffer_pool: Arc<BufferPool>,
        encoding: Arc<SortKeyEncoding>,
        memory: MemoryAccountingContext,
    ) -> Self {
        let slot_size = encoding.slot_size().max(1);
        let slot_block_capacity = (DEFAULT_BLOCK_SIZE / slot_size).max(1) as u32;
        Self {
            buffer_pool,
            encoding,
            memory,
            slot_block_capacity,
            slot_size,
            slot_blocks: Vec::new(),
            overflow_blocks: Vec::new(),
            total_bytes: 0,
            count: 0,
            current_slot: None,
            current_overflow: None,
            release_state: KeyReleaseState::default(),
        }
    }

    #[inline]
    pub fn encoding(&self) -> &Arc<SortKeyEncoding> {
        &self.encoding
    }

    #[inline]
    pub fn count(&self) -> u32 {
        self.count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    #[inline]
    pub fn size_in_bytes(&self) -> usize {
        self.total_bytes
    }

    #[inline]
    pub fn logical_release_frontier(&self) -> u64 {
        self.release_state
            .logical_release_frontier
            .load(AtomicOrdering::Acquire)
    }

    #[inline]
    pub fn physical_release_frontier(&self) -> u64 {
        self.release_state
            .physical_release_frontier
            .load(AtomicOrdering::Acquire)
    }

    #[inline]
    pub fn outstanding_pins(&self) -> usize {
        self.release_state
            .outstanding_pins
            .load(AtomicOrdering::Acquire)
    }

    pub fn encode_batch(&mut self, chunk: &Chunk) -> Result<()> {
        self.ensure_not_released_tail()?;
        let row_count = chunk.size();
        if row_count == 0 {
            return Ok(());
        }
        if chunk.column_count() != self.encoding.field_count() {
            return Err(paro_error::internal(format!(
                "sort key chunk column count mismatch: expected {}, got {}",
                self.encoding.field_count(),
                chunk.column_count()
            )));
        }

        let columns: Vec<usize> = (0..chunk.column_count()).collect();
        self.encoding.validate_columns(chunk, &columns)?;
        let encoding = Arc::clone(&self.encoding);
        if let Some(fixed_len) = self.encoding.fixed_key_len() {
            for row_idx in 0..row_count {
                self.append_encoded_key_from_parts(fixed_len, |inline_prefix, overflow| {
                    encoding.encode_row_into_parts_trusted(
                        chunk,
                        row_idx,
                        &columns,
                        inline_prefix,
                        overflow,
                    )
                })?;
            }
            return Ok(());
        }

        let mut key_lengths = Vec::with_capacity(row_count);
        for row_idx in 0..row_count {
            let key_len = self
                .encoding
                .encoded_len_trusted(chunk, row_idx, &columns)?;
            key_lengths.push(key_len);
        }
        for (row_idx, &key_len) in key_lengths.iter().enumerate() {
            self.append_encoded_key_from_parts(key_len, |inline_prefix, overflow| {
                encoding.encode_row_into_parts_trusted(
                    chunk,
                    row_idx,
                    &columns,
                    inline_prefix,
                    overflow,
                )
            })?;
        }

        Ok(())
    }

    pub fn cursor_pinned(&self) -> Result<KeyCursor<'_>> {
        self.acquire_cursor_pin();
        let slot_handles = self
            .slot_blocks
            .iter()
            .map(|block| match block.handle.lock().unwrap().as_ref() {
                Some(handle) => self.pin_block(handle.block_id()).map(Some),
                None => Ok(None),
            })
            .collect::<Result<Vec<_>>>()?;
        let overflow_handles = self
            .overflow_blocks
            .iter()
            .map(|block| match block.handle.lock().unwrap().as_ref() {
                Some(handle) => self.pin_block(handle.block_id()).map(Some),
                None => Ok(None),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(KeyCursor {
            store: self,
            mode: CursorMode::Pinned(PinnedBlocks {
                slot_handles,
                overflow_handles,
            }),
            _pin: CursorPin { store: self },
        })
    }

    pub fn cursor_on_demand(&self, window_size: usize) -> KeyCursor<'_> {
        self.acquire_cursor_pin();
        KeyCursor {
            store: self,
            mode: CursorMode::OnDemand(OnDemandPinnedBlocks {
                window_size: window_size.max(1),
                slot_handles: Vec::new(),
                overflow_handles: Vec::new(),
            }),
            _pin: CursorPin { store: self },
        }
    }

    pub fn comparator<'cursor, 'store>(
        cursor: &'cursor mut KeyCursor<'store>,
    ) -> KeyComparator<'cursor, 'store> {
        KeyComparator::new(cursor)
    }

    /// Sort row ordinals using the byte-comparable key representation.
    ///
    /// Encoded inline prefixes are materialized once and sorted as integers;
    /// only equal-prefix ranges pay for full key comparisons.
    pub(crate) fn sorted_permutation(&self) -> Result<SortPermutation> {
        let count = self.count as usize;
        let mut permutation = self.accounted_ordinal_buffer(count)?;
        for (ordinal, slot) in permutation.iter_mut().enumerate() {
            *slot = ordinal as u32;
        }
        if count <= 1 {
            return Ok(permutation);
        }

        let mut cursor = self.cursor_pinned()?;
        if count < PREFIX_SORT_MIN_ROWS {
            permutation.sort_unstable_by(|left, right| {
                cursor
                    .compare(*left, *right)
                    .expect("sort key comparison must succeed during finalize")
            });
            return Ok(permutation);
        }

        let mut prefixes =
            SortPrefixes::try_new(&self.memory, Arc::clone(&self.buffer_pool), count)?;
        for (ordinal, prefix) in prefixes.iter_mut().enumerate() {
            *prefix = cursor.materialized_sort_prefix(ordinal as u32)?;
        }
        permutation.sort_unstable_by(|left, right| {
            prefixes[*left as usize].cmp(&prefixes[*right as usize])
        });

        let prefix_is_complete = self
            .encoding
            .fixed_key_len()
            .is_some_and(|width| width <= MATERIALIZED_SORT_PREFIX_LEN);
        if !prefix_is_complete {
            refine_equal_prefix_ranges(&mut cursor, &mut permutation, &prefixes)?;
        }
        Ok(permutation)
    }

    fn accounted_ordinal_buffer(&self, len: usize) -> Result<SortPermutation> {
        SortPermutation::try_new(&self.memory, Arc::clone(&self.buffer_pool), len)
    }

    pub fn advance_release_frontier(&self, frontier: u64) -> Result<()> {
        if frontier > self.count as u64 {
            return Err(paro_error::internal(format!(
                "sort key release frontier {} exceeds row count {}",
                frontier, self.count
            )));
        }
        let current = self.logical_release_frontier();
        if frontier < current {
            return Err(paro_error::internal(format!(
                "sort key release frontier cannot move backwards: current={}, requested={}",
                current, frontier
            )));
        }
        self.release_state
            .logical_release_frontier
            .store(frontier, AtomicOrdering::Release);
        self.try_release_prefix();
        Ok(())
    }

    pub fn reorder_by_permutation(&self, permutation: &[u32]) -> Result<Self> {
        let mut reordered = SortKeyStore::new_with_memory(
            Arc::clone(&self.buffer_pool),
            Arc::clone(&self.encoding),
            self.memory.clone(),
        );
        let mut cursor = self.cursor_on_demand(1);
        for &ordinal in permutation {
            reordered.append_key_from_cursor(&mut cursor, ordinal)?;
        }
        reordered.finish_writing();
        Ok(reordered)
    }

    pub fn read_key(&self, ordinal: u32) -> Result<Vec<u8>> {
        let mut cursor = self.cursor_on_demand(1);
        cursor.read_key(ordinal)
    }

    fn append_encoded_key_from_parts<F>(&mut self, total_len: usize, write_key: F) -> Result<()>
    where
        F: FnOnce(&mut [u8], &mut [u8]) -> Result<()>,
    {
        self.ensure_slot_block()?;
        let ordinal = self.count;
        let slot_block_idx = self.slot_block_index_for_append();
        let row_in_block = self
            .slot_blocks
            .get(slot_block_idx)
            .expect("current slot block must exist")
            .row_count as usize;
        let slot_offset = row_in_block * self.slot_size;
        let inline_len = total_len.min(self.encoding.inline_prefix_len());
        let overflow_len = total_len.saturating_sub(inline_len);
        let variable_layout = self
            .encoding
            .is_variable()
            .then(|| VariableSlotLayout::new(&self.encoding));
        let overflow_location = if overflow_len > 0 {
            Some(self.reserve_overflow_block(overflow_len, ordinal)?)
        } else {
            None
        };

        let slot_handle = self
            .current_slot
            .as_ref()
            .expect("current slot handle must exist");
        let slot_data = unsafe {
            slot_handle
                .data_mut()
                .ok_or_else(|| paro_error::internal("missing slot block data"))?
        };
        let slot = &mut slot_data[slot_offset..slot_offset + self.slot_size];
        slot.fill(0);

        if let Some((overflow_block_idx, overflow_offset)) = overflow_location {
            let overflow_handle = self
                .current_overflow
                .as_ref()
                .expect("current overflow handle must exist");
            let overflow_data = unsafe {
                overflow_handle
                    .data_mut()
                    .ok_or_else(|| paro_error::internal("missing overflow block data"))?
            };
            let overflow =
                &mut overflow_data[overflow_offset..overflow_offset.saturating_add(overflow_len)];
            write_key(&mut slot[..inline_len], overflow)?;
            let layout = variable_layout.expect("overflow keys require variable slot metadata");
            write_u32(slot, layout.total_len_offset, total_len as u32);
            write_u32(slot, layout.overflow_len_offset, overflow_len as u32);
            write_u32(
                slot,
                layout.overflow_block_offset,
                overflow_block_idx as u32,
            );
            write_u32(slot, layout.overflow_offset_offset, overflow_offset as u32);
        } else {
            write_key(&mut slot[..inline_len], &mut [])?;
            if let Some(layout) = variable_layout {
                write_u32(slot, layout.total_len_offset, total_len as u32);
                write_u32(slot, layout.overflow_len_offset, 0);
            }
        }

        self.slot_blocks[slot_block_idx].row_count += 1;
        self.count += 1;
        Ok(())
    }

    fn append_key_from_cursor(&mut self, cursor: &mut KeyCursor<'_>, ordinal: u32) -> Result<()> {
        let mut slot = [0u8; MAX_SORT_KEY_SLOT_SIZE];
        cursor.copy_slot(ordinal, &mut slot)?;

        if let Some(fixed_len) = self.encoding.fixed_key_len() {
            return self.append_encoded_key_from_parts(fixed_len, |inline_prefix, _overflow| {
                inline_prefix.copy_from_slice(&slot[..fixed_len]);
                Ok(())
            });
        }

        let source_layout = VariableSlotLayout::new(&cursor.store.encoding);
        let total_len = read_u32(&slot, source_layout.total_len_offset) as usize;
        let inline_len = total_len.min(source_layout.inline_len);
        let overflow_len = read_u32(&slot, source_layout.overflow_len_offset) as usize;
        let source_overflow = if overflow_len > 0 {
            Some((
                read_u32(&slot, source_layout.overflow_block_offset) as usize,
                read_u32(&slot, source_layout.overflow_offset_offset) as usize,
            ))
        } else {
            None
        };

        self.append_encoded_key_from_parts(total_len, |inline_prefix, overflow| {
            inline_prefix.copy_from_slice(&slot[..inline_len]);
            if let Some((block_idx, overflow_offset)) = source_overflow {
                cursor.with_overflow_slice(block_idx, overflow_offset, overflow_len, |bytes| {
                    overflow.copy_from_slice(bytes);
                })?;
            }
            Ok(())
        })
    }

    pub(crate) fn finish_writing(&mut self) {
        self.current_slot = None;
        self.current_overflow = None;
    }

    fn ensure_not_released_tail(&self) -> Result<()> {
        if self.logical_release_frontier() != 0 || self.physical_release_frontier() != 0 {
            return Err(paro_error::internal(
                "cannot append to a sort key store after release frontier advanced",
            ));
        }
        Ok(())
    }

    fn ensure_slot_block(&mut self) -> Result<()> {
        let needs_new_block = match self.slot_blocks.last() {
            None => true,
            Some(block) => block.row_count >= self.slot_block_capacity,
        };
        if !needs_new_block {
            return Ok(());
        }

        self.current_slot = None;
        let block_size = self.slot_block_capacity as usize * self.slot_size;
        let release = self.memory.retain(block_size)?;
        let handle = match self.buffer_pool.allocate(
            MemoryTag::OrderBy,
            FileBufferType::ManagedBuffer,
            block_size,
        ) {
            Ok(handle) => handle,
            Err(err) => {
                release.release();
                return Err(err);
            }
        };
        let block_handle = handle
            .block_handle()
            .cloned()
            .ok_or_else(|| paro_error::internal("failed to access slot block handle"))?;
        self.total_bytes += block_size;
        self.slot_blocks.push(SlotBlockMeta {
            handle: Mutex::new(Some(block_handle)),
            release,
            row_start: self.count,
            row_count: 0,
        });
        self.current_slot = Some(handle);
        Ok(())
    }

    fn reserve_overflow_block(&mut self, required: usize, ordinal: u32) -> Result<(usize, usize)> {
        let needs_new_block = match self.current_overflow.as_ref() {
            Some(_handle) => {
                let block_idx = self.overflow_blocks.len() - 1;
                let block = &self.overflow_blocks[block_idx];
                required > block.size.saturating_sub(block.used_bytes)
            }
            None => true,
        };

        if needs_new_block {
            self.current_overflow = None;
            let block_size = DEFAULT_BLOCK_SIZE.max(required);
            let release = self.memory.retain(block_size)?;
            let handle = match self.buffer_pool.allocate(
                MemoryTag::OrderBy,
                FileBufferType::ManagedBuffer,
                block_size,
            ) {
                Ok(handle) => handle,
                Err(err) => {
                    release.release();
                    return Err(err);
                }
            };
            let block_handle = handle
                .block_handle()
                .cloned()
                .ok_or_else(|| paro_error::internal("failed to access overflow block handle"))?;
            self.total_bytes += block_size;
            self.overflow_blocks.push(OverflowBlockMeta {
                handle: Mutex::new(Some(block_handle)),
                release,
                ordinal_end: ordinal,
                used_bytes: 0,
                size: block_size,
            });
            self.current_overflow = Some(handle);
        }

        let block_idx = self.overflow_blocks.len() - 1;
        let overflow_offset = self.current_overflow_used_bytes();
        {
            let block = self
                .overflow_blocks
                .get_mut(block_idx)
                .expect("overflow block must exist");
            block.ordinal_end = ordinal + 1;
        }
        let next_used = overflow_offset + required;
        self.set_current_overflow_used_bytes(next_used);
        Ok((block_idx, overflow_offset))
    }

    fn current_overflow_used_bytes(&self) -> usize {
        let Some(_handle) = self.current_overflow.as_ref() else {
            return 0;
        };
        let block_idx = self.overflow_blocks.len() - 1;
        self.overflow_blocks[block_idx].used_bytes
    }

    fn set_current_overflow_used_bytes(&mut self, used_bytes: usize) {
        let block_idx = self.overflow_blocks.len() - 1;
        self.overflow_blocks[block_idx].used_bytes = used_bytes;
    }

    fn slot_block_index_for_append(&self) -> usize {
        self.slot_blocks.len() - 1
    }

    fn slot_block_index(&self, ordinal: u32) -> Result<(usize, usize)> {
        if ordinal < self.physical_release_frontier() as u32 {
            return Err(paro_error::internal(format!(
                "sort key ordinal {} is before physical release frontier {}",
                ordinal,
                self.physical_release_frontier()
            )));
        }
        if ordinal >= self.count {
            return Err(paro_error::internal(format!(
                "sort key ordinal {} out of bounds {}",
                ordinal, self.count
            )));
        }

        let block_idx = (ordinal / self.slot_block_capacity) as usize;
        let within_block = (ordinal % self.slot_block_capacity) as usize;
        let block = self
            .slot_blocks
            .get(block_idx)
            .ok_or_else(|| paro_error::internal("sort key slot block missing"))?;
        if within_block >= block.row_count as usize {
            return Err(paro_error::internal(format!(
                "sort key ordinal {} exceeds slot block row count {}",
                ordinal, block.row_count
            )));
        }
        Ok((block_idx, within_block))
    }

    fn pin_block(&self, block_id: BlockId) -> Result<BufferHandle> {
        self.buffer_pool.pin(block_id)
    }

    fn acquire_cursor_pin(&self) {
        self.release_state
            .outstanding_pins
            .fetch_add(1, AtomicOrdering::AcqRel);
    }

    fn try_release_prefix(&self) {
        if self.outstanding_pins() != 0 {
            return;
        }

        let frontier = self.logical_release_frontier();
        let slot_target = self.slot_block_prefix_for_frontier(frontier);
        let overflow_target = self.overflow_block_prefix_for_frontier(frontier);

        let current_slot = self
            .release_state
            .released_slot_block_prefix
            .load(AtomicOrdering::Acquire);
        for block_idx in current_slot..slot_target {
            self.release_slot_block(block_idx);
        }
        self.release_state
            .released_slot_block_prefix
            .store(slot_target, AtomicOrdering::Release);

        let current_overflow = self
            .release_state
            .released_overflow_block_prefix
            .load(AtomicOrdering::Acquire);
        for block_idx in current_overflow..overflow_target {
            self.release_overflow_block(block_idx);
        }
        self.release_state
            .released_overflow_block_prefix
            .store(overflow_target, AtomicOrdering::Release);

        self.release_state.physical_release_frontier.store(
            self.ordinal_frontier_for_slot_prefix(slot_target),
            AtomicOrdering::Release,
        );
    }

    fn release_slot_block(&self, block_idx: usize) {
        let Some(handle) = self.slot_blocks[block_idx].handle.lock().unwrap().take() else {
            return;
        };
        let block_id = handle.block_id();
        drop(handle);
        if self.buffer_pool.free(block_id).is_ok() {
            self.slot_blocks[block_idx].release.release();
        }
    }

    fn release_overflow_block(&self, block_idx: usize) {
        let Some(handle) = self.overflow_blocks[block_idx]
            .handle
            .lock()
            .unwrap()
            .take()
        else {
            return;
        };
        let block_id = handle.block_id();
        drop(handle);
        if self.buffer_pool.free(block_id).is_ok() {
            self.overflow_blocks[block_idx].release.release();
        }
    }

    fn slot_block_prefix_for_frontier(&self, frontier: u64) -> usize {
        self.slot_blocks
            .iter()
            .take_while(|block| block.row_start as u64 + block.row_count as u64 <= frontier)
            .count()
    }

    fn ordinal_frontier_for_slot_prefix(&self, prefix: usize) -> u64 {
        if prefix == 0 {
            0
        } else {
            let block = &self.slot_blocks[prefix - 1];
            block.row_start as u64 + block.row_count as u64
        }
    }

    fn overflow_block_prefix_for_frontier(&self, frontier: u64) -> usize {
        self.overflow_blocks
            .iter()
            .take_while(|block| block.ordinal_end as u64 <= frontier)
            .count()
    }
}

impl KeyCursor<'_> {
    pub fn compare_with(
        left_cursor: &mut KeyCursor<'_>,
        left: u32,
        right_cursor: &mut KeyCursor<'_>,
        right: u32,
    ) -> Result<Ordering> {
        if std::ptr::eq(left_cursor.store, right_cursor.store) {
            return left_cursor.compare(left, right);
        }

        let left_encoding = &left_cursor.store.encoding;
        let right_encoding = &right_cursor.store.encoding;
        if left_encoding.fixed_key_len() != right_encoding.fixed_key_len()
            || left_encoding.inline_prefix_len() != right_encoding.inline_prefix_len()
            || left_encoding.slot_size() != right_encoding.slot_size()
        {
            return Err(paro_error::internal(
                "cannot compare sort keys built with different storage layouts",
            ));
        }
        let comparison = left_cursor.with_slot(left, |left_slot| {
            right_cursor.with_slot(right, |right_slot| {
                compare_slot_prefixes(left_slot, right_slot, left_encoding)
            })
        })?;
        let comparison = comparison?;
        let comparison = comparison?;
        match comparison {
            SlotComparison::Ordered(ordering) => Ok(ordering),
            SlotComparison::Overflow { left, right } => left_cursor.with_overflow_slice(
                left.block_idx,
                left.offset,
                left.len,
                |left_bytes| {
                    right_cursor.with_overflow_slice(
                        right.block_idx,
                        right.offset,
                        right.len,
                        |right_bytes| compare_keys(left_bytes, right_bytes),
                    )
                },
            )?,
        }
    }

    pub fn compare(&mut self, left: u32, right: u32) -> Result<Ordering> {
        if let CursorMode::Pinned(blocks) = &self.mode {
            return compare_pinned_slots(self.store, blocks, left, right);
        }

        let mut left_slot = [0u8; MAX_SORT_KEY_SLOT_SIZE];
        let mut right_slot = [0u8; MAX_SORT_KEY_SLOT_SIZE];
        let left_len = self.copy_slot(left, &mut left_slot)?;
        let right_len = self.copy_slot(right, &mut right_slot)?;
        match compare_slot_prefixes(
            &left_slot[..left_len],
            &right_slot[..right_len],
            &self.store.encoding,
        )? {
            SlotComparison::Ordered(ordering) => Ok(ordering),
            SlotComparison::Overflow { left, right } => self.with_overflow_slices(
                left.block_idx,
                left.offset,
                left.len,
                right.block_idx,
                right.offset,
                right.len,
                |left_bytes, right_bytes| compare_keys(left_bytes, right_bytes),
            ),
        }
    }

    pub fn read_key(&mut self, ordinal: u32) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.read_key_into(ordinal, &mut out)?;
        Ok(out)
    }

    pub fn read_key_into(&mut self, ordinal: u32, out: &mut Vec<u8>) -> Result<()> {
        let mut slot = [0u8; MAX_SORT_KEY_SLOT_SIZE];
        let _slot_len = self.copy_slot(ordinal, &mut slot)?;
        if let Some(fixed_len) = self.store.encoding.fixed_key_len() {
            out.clear();
            out.extend_from_slice(&slot[..fixed_len]);
            return Ok(());
        }

        let layout = VariableSlotLayout::new(&self.store.encoding);
        let total_len = read_u32(&slot, layout.total_len_offset) as usize;
        let inline_len = total_len.min(layout.inline_len);
        let overflow_len = read_u32(&slot, layout.overflow_len_offset) as usize;
        out.clear();
        out.extend_from_slice(&slot[..inline_len]);

        if overflow_len > 0 {
            let block_idx = read_u32(&slot, layout.overflow_block_offset) as usize;
            let offset = read_u32(&slot, layout.overflow_offset_offset) as usize;
            self.with_overflow_slice(block_idx, offset, overflow_len, |bytes| {
                out.extend_from_slice(bytes);
            })?;
        }

        Ok(())
    }

    fn copy_slot(&mut self, ordinal: u32, out: &mut [u8; MAX_SORT_KEY_SLOT_SIZE]) -> Result<usize> {
        let (block_idx, within_block) = self.store.slot_block_index(ordinal)?;
        let slot_offset = within_block * self.store.slot_size;
        let slot_size = self.store.slot_size;
        match &mut self.mode {
            CursorMode::Pinned(blocks) => {
                let handle = blocks
                    .slot_handles
                    .get(block_idx)
                    .and_then(|handle| handle.as_ref())
                    .ok_or_else(|| paro_error::internal("slot block was already released"))?;
                let data = handle
                    .data()
                    .ok_or_else(|| paro_error::internal("slot block missing data"))?;
                out[..slot_size].copy_from_slice(&data[slot_offset..slot_offset + slot_size]);
            }
            CursorMode::OnDemand(cache) => {
                let handle_idx = Self::cached_slot_handle(self.store, cache, block_idx)?;
                let handle = &cache.slot_handles[handle_idx].handle;
                let data = handle
                    .data()
                    .ok_or_else(|| paro_error::internal("slot block missing data"))?;
                out[..slot_size].copy_from_slice(&data[slot_offset..slot_offset + slot_size]);
            }
        }
        Ok(slot_size)
    }

    fn with_slot<T>(&mut self, ordinal: u32, f: impl FnOnce(&[u8]) -> T) -> Result<T> {
        let (block_idx, within_block) = self.store.slot_block_index(ordinal)?;
        let slot_offset = within_block * self.store.slot_size;
        let slot_size = self.store.slot_size;
        match &mut self.mode {
            CursorMode::Pinned(blocks) => {
                let slot = pinned_slot(blocks, block_idx, slot_offset, slot_size)?;
                Ok(f(slot))
            }
            CursorMode::OnDemand(cache) => {
                let handle_idx = Self::cached_slot_handle(self.store, cache, block_idx)?;
                let handle = &cache.slot_handles[handle_idx].handle;
                let data = handle
                    .data()
                    .ok_or_else(|| paro_error::internal("slot block missing data"))?;
                Ok(f(&data[slot_offset..slot_offset + slot_size]))
            }
        }
    }

    fn materialized_sort_prefix(&mut self, ordinal: u32) -> Result<MaterializedSortPrefix> {
        let mut slot = [0u8; MAX_SORT_KEY_SLOT_SIZE];
        let slot_len = self.copy_slot(ordinal, &mut slot)?;
        if let Some(fixed_len) = self.store.encoding.fixed_key_len() {
            let effective_len = fixed_len.min(MATERIALIZED_SORT_PREFIX_LEN);
            let mut prefix = MaterializedSortPrefix {
                effective_len: effective_len as u8,
                ..MaterializedSortPrefix::default()
            };
            prefix.bytes[..effective_len].copy_from_slice(&slot[..effective_len]);
            return Ok(prefix);
        }
        let layout = VariableSlotLayout::new(&self.store.encoding);
        if slot_len < layout.metadata_end() {
            return Err(paro_error::internal(format!(
                "variable sort-key slot is too short: actual={slot_len}"
            )));
        }

        let total_len = read_u32(&slot, layout.total_len_offset) as usize;
        let effective_len = total_len.min(MATERIALIZED_SORT_PREFIX_LEN);
        let inline_len = total_len.min(layout.inline_len);
        let mut prefix = MaterializedSortPrefix {
            effective_len: effective_len as u8,
            ..MaterializedSortPrefix::default()
        };
        prefix.bytes[..inline_len].copy_from_slice(&slot[..inline_len]);

        let overflow_prefix_len = effective_len.saturating_sub(inline_len);
        if overflow_prefix_len == 0 {
            return Ok(prefix);
        }
        let overflow_len = read_u32(&slot, layout.overflow_len_offset) as usize;
        if overflow_prefix_len > overflow_len {
            return Err(paro_error::internal(format!(
                "sort-key overflow is shorter than its prefix: required={overflow_prefix_len}, actual={overflow_len}"
            )));
        }
        let block_idx = read_u32(&slot, layout.overflow_block_offset) as usize;
        let offset = read_u32(&slot, layout.overflow_offset_offset) as usize;
        self.with_overflow_slice(block_idx, offset, overflow_prefix_len, |bytes| {
            prefix.bytes[inline_len..effective_len].copy_from_slice(bytes);
        })?;
        Ok(prefix)
    }

    fn with_overflow_slice<T, F>(
        &mut self,
        block_idx: usize,
        offset: usize,
        len: usize,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(&[u8]) -> T,
    {
        match &mut self.mode {
            CursorMode::Pinned(blocks) => {
                let handle = blocks
                    .overflow_handles
                    .get(block_idx)
                    .and_then(|handle| handle.as_ref())
                    .ok_or_else(|| paro_error::internal("overflow block was already released"))?;
                let data = handle
                    .data()
                    .ok_or_else(|| paro_error::internal("overflow block missing data"))?;
                Ok(f(&data[offset..offset + len]))
            }
            CursorMode::OnDemand(cache) => {
                let handle_idx = Self::cached_overflow_handle(self.store, cache, block_idx)?;
                let handle = &cache.overflow_handles[handle_idx].handle;
                let data = handle
                    .data()
                    .ok_or_else(|| paro_error::internal("overflow block missing data"))?;
                Ok(f(&data[offset..offset + len]))
            }
        }
    }

    fn with_overflow_slices<T, F>(
        &mut self,
        left_block_idx: usize,
        left_offset: usize,
        left_len: usize,
        right_block_idx: usize,
        right_offset: usize,
        right_len: usize,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(&[u8], &[u8]) -> T,
    {
        match &mut self.mode {
            CursorMode::Pinned(blocks) => {
                let left_handle = blocks
                    .overflow_handles
                    .get(left_block_idx)
                    .and_then(|handle| handle.as_ref())
                    .ok_or_else(|| paro_error::internal("overflow block was already released"))?;
                let left_data = left_handle
                    .data()
                    .ok_or_else(|| paro_error::internal("overflow block missing data"))?;
                let left_bytes = &left_data[left_offset..left_offset + left_len];

                let right_handle = blocks
                    .overflow_handles
                    .get(right_block_idx)
                    .and_then(|handle| handle.as_ref())
                    .ok_or_else(|| paro_error::internal("overflow block was already released"))?;
                let right_data = right_handle
                    .data()
                    .ok_or_else(|| paro_error::internal("overflow block missing data"))?;
                let right_bytes = &right_data[right_offset..right_offset + right_len];

                Ok(f(left_bytes, right_bytes))
            }
            CursorMode::OnDemand(cache) => {
                let left_handle_idx =
                    Self::cached_overflow_handle(self.store, cache, left_block_idx)?;
                let right_handle_idx =
                    Self::cached_overflow_handle(self.store, cache, right_block_idx)?;
                let left_handle = &cache.overflow_handles[left_handle_idx].handle;
                let left_data = left_handle
                    .data()
                    .ok_or_else(|| paro_error::internal("overflow block missing data"))?;
                let left_bytes = &left_data[left_offset..left_offset + left_len];

                let right_handle = &cache.overflow_handles[right_handle_idx].handle;
                let right_data = right_handle
                    .data()
                    .ok_or_else(|| paro_error::internal("overflow block missing data"))?;
                let right_bytes = &right_data[right_offset..right_offset + right_len];

                Ok(f(left_bytes, right_bytes))
            }
        }
    }

    fn cached_slot_handle(
        store: &SortKeyStore,
        cache: &mut OnDemandPinnedBlocks,
        block_idx: usize,
    ) -> Result<usize> {
        Self::ensure_cached_handle(
            &store.slot_blocks,
            &mut cache.slot_handles,
            cache.window_size,
            block_idx,
            |block_id| store.pin_block(block_id),
        )
    }

    fn cached_overflow_handle(
        store: &SortKeyStore,
        cache: &mut OnDemandPinnedBlocks,
        block_idx: usize,
    ) -> Result<usize> {
        Self::ensure_cached_handle(
            &store.overflow_blocks,
            &mut cache.overflow_handles,
            cache.window_size,
            block_idx,
            |block_id| store.pin_block(block_id),
        )
    }

    fn ensure_cached_handle<T, F>(
        blocks: &[T],
        cache: &mut Vec<OnDemandCachedBlock>,
        window_size: usize,
        block_idx: usize,
        mut pin: F,
    ) -> Result<usize>
    where
        T: BlockHandleProvider,
        F: FnMut(BlockId) -> Result<BufferHandle>,
    {
        if let Some(pos) = cache.iter().position(|entry| entry.block_idx == block_idx) {
            let entry = cache.remove(pos);
            cache.push(entry);
        } else {
            let block_id = blocks
                .get(block_idx)
                .and_then(BlockHandleProvider::block_id)
                .ok_or_else(|| paro_error::internal("block was already released"))?;
            let handle = pin(block_id)?;
            if cache.len() >= window_size {
                cache.remove(0);
            }
            cache.push(OnDemandCachedBlock { block_idx, handle });
        }

        cache
            .iter()
            .position(|entry| entry.block_idx == block_idx)
            .ok_or_else(|| paro_error::internal("failed to pin on-demand block"))
    }
}

impl Drop for SortKeyStore {
    fn drop(&mut self) {
        self.current_slot = None;
        self.current_overflow = None;
        for block_idx in 0..self.slot_blocks.len() {
            self.release_slot_block(block_idx);
        }
        for block_idx in 0..self.overflow_blocks.len() {
            self.release_overflow_block(block_idx);
        }
    }
}

trait BlockHandleProvider {
    fn block_id(&self) -> Option<BlockId>;
}

impl BlockHandleProvider for SlotBlockMeta {
    fn block_id(&self) -> Option<BlockId> {
        self.handle
            .lock()
            .unwrap()
            .as_ref()
            .map(|handle| handle.block_id())
    }
}

impl BlockHandleProvider for OverflowBlockMeta {
    fn block_id(&self) -> Option<BlockId> {
        self.handle
            .lock()
            .unwrap()
            .as_ref()
            .map(|handle| handle.block_id())
    }
}

fn refine_equal_prefix_ranges(
    cursor: &mut KeyCursor<'_>,
    permutation: &mut SortPermutation,
    prefixes: &SortPrefixes,
) -> Result<()> {
    if permutation.len() != prefixes.len() {
        return Err(paro_error::internal(format!(
            "sort prefix size mismatch: permutation={}, prefixes={}",
            permutation.len(),
            prefixes.len()
        )));
    }
    let mut start = 0usize;
    while start < permutation.len() {
        let prefix = prefixes[permutation[start] as usize];
        let mut end = start + 1;
        while end < permutation.len() && prefixes[permutation[end] as usize] == prefix {
            end += 1;
        }
        if end - start > 1 {
            permutation.as_mut_slice()[start..end].sort_unstable_by(|left, right| {
                cursor
                    .compare(*left, *right)
                    .expect("sort key comparison must succeed during radix refinement")
            });
        }
        start = end;
    }
    Ok(())
}

fn compare_pinned_slots(
    store: &SortKeyStore,
    blocks: &PinnedBlocks,
    left: u32,
    right: u32,
) -> Result<Ordering> {
    let (left_block, left_row) = store.slot_block_index(left)?;
    let (right_block, right_row) = store.slot_block_index(right)?;
    let left_slot = pinned_slot(
        blocks,
        left_block,
        left_row * store.slot_size,
        store.slot_size,
    )?;
    let right_slot = pinned_slot(
        blocks,
        right_block,
        right_row * store.slot_size,
        store.slot_size,
    )?;
    match compare_slot_prefixes(left_slot, right_slot, &store.encoding)? {
        SlotComparison::Ordered(ordering) => Ok(ordering),
        SlotComparison::Overflow { left, right } => {
            let left_bytes = pinned_overflow_slice(blocks, left)?;
            let right_bytes = pinned_overflow_slice(blocks, right)?;
            Ok(compare_keys(left_bytes, right_bytes))
        }
    }
}

fn compare_slot_prefixes(
    left_slot: &[u8],
    right_slot: &[u8],
    encoding: &SortKeyEncoding,
) -> Result<SlotComparison> {
    if let Some(fixed_len) = encoding.fixed_key_len() {
        let left = left_slot.get(..fixed_len).ok_or_else(|| {
            paro_error::internal(format!(
                "fixed sort-key slot is too short: required={fixed_len}, actual={}",
                left_slot.len()
            ))
        })?;
        let right = right_slot.get(..fixed_len).ok_or_else(|| {
            paro_error::internal(format!(
                "fixed sort-key slot is too short: required={fixed_len}, actual={}",
                right_slot.len()
            ))
        })?;
        return Ok(SlotComparison::Ordered(compare_keys(left, right)));
    }
    let layout = VariableSlotLayout::new(encoding);
    if left_slot.len() < layout.metadata_end() || right_slot.len() < layout.metadata_end() {
        return Err(paro_error::internal(format!(
            "variable sort-key slot is too short: left={}, right={}",
            left_slot.len(),
            right_slot.len()
        )));
    }

    let left_total = read_u32(left_slot, layout.total_len_offset) as usize;
    let right_total = read_u32(right_slot, layout.total_len_offset) as usize;
    let left_inline = left_total.min(layout.inline_len);
    let right_inline = right_total.min(layout.inline_len);
    let shared_inline = left_inline.min(right_inline);
    let inline_order = compare_keys(&left_slot[..shared_inline], &right_slot[..shared_inline]);
    if inline_order != Ordering::Equal {
        return Ok(SlotComparison::Ordered(inline_order));
    }
    if left_total <= layout.inline_len || right_total <= layout.inline_len {
        return Ok(SlotComparison::Ordered(left_total.cmp(&right_total)));
    }

    Ok(SlotComparison::Overflow {
        left: OverflowKeyRef {
            block_idx: read_u32(left_slot, layout.overflow_block_offset) as usize,
            offset: read_u32(left_slot, layout.overflow_offset_offset) as usize,
            len: read_u32(left_slot, layout.overflow_len_offset) as usize,
        },
        right: OverflowKeyRef {
            block_idx: read_u32(right_slot, layout.overflow_block_offset) as usize,
            offset: read_u32(right_slot, layout.overflow_offset_offset) as usize,
            len: read_u32(right_slot, layout.overflow_len_offset) as usize,
        },
    })
}

fn pinned_slot(
    blocks: &PinnedBlocks,
    block_idx: usize,
    offset: usize,
    len: usize,
) -> Result<&[u8]> {
    let handle = blocks
        .slot_handles
        .get(block_idx)
        .and_then(|handle| handle.as_ref())
        .ok_or_else(|| paro_error::internal("slot block was already released"))?;
    let data = handle
        .data()
        .ok_or_else(|| paro_error::internal("slot block missing data"))?;
    data.get(offset..offset.saturating_add(len))
        .ok_or_else(|| paro_error::internal("sort-key slot range is out of bounds"))
}

fn pinned_overflow_slice(blocks: &PinnedBlocks, key: OverflowKeyRef) -> Result<&[u8]> {
    let handle = blocks
        .overflow_handles
        .get(key.block_idx)
        .and_then(|handle| handle.as_ref())
        .ok_or_else(|| paro_error::internal("overflow block was already released"))?;
    let data = handle
        .data()
        .ok_or_else(|| paro_error::internal("overflow block missing data"))?;
    data.get(key.offset..key.offset.saturating_add(key.len))
        .ok_or_else(|| paro_error::internal("sort-key overflow range is out of bounds"))
}

#[inline]
fn write_u32(slot: &mut [u8], offset: usize, value: u32) {
    slot[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

#[inline]
fn read_u32(slot: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(slot[offset..offset + 4].try_into().expect("u32 slot field"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;

    fn build_store(chunk: Chunk, types: Vec<LogicalType>) -> SortKeyStore {
        let pool = Arc::new(BufferPool::new(32 * 1024 * 1024));
        let modifiers = vec![paro_common::sort_key::OrderModifiers::new(true, false); types.len()];
        let encoding = Arc::new(SortKeyEncoding::new(types, modifiers).unwrap());
        let mut store = SortKeyStore::new(pool, encoding);
        store.encode_batch(&chunk).unwrap();
        store.finish_writing();
        store
    }

    #[test]
    fn empty_accounted_sort_buffer_exposes_valid_slices() {
        let pool = Arc::new(BufferPool::new(32 * 1024 * 1024));
        let memory =
            MemoryAccountingContext::detached(MemoryTag::OrderBy, MemoryAccountingClass::Revocable);
        let mut buffer = AccountedSortBuffer::<u32>::try_new(&memory, pool, 0).unwrap();

        assert_eq!(buffer.size_in_bytes(), 0);
        assert!(buffer.as_slice().is_empty());
        assert!(buffer.as_mut_slice().is_empty());
    }

    #[test]
    fn fixed_keys_compare_in_order() {
        let mut values = paro_common::test_utils::test_vector(LogicalType::Integer);
        values.set_i32(0, 10);
        values.set_i32(1, 20);
        values.set_i32(2, 30);
        values.set_count(3);
        let chunk = Chunk::from_arc_vectors(
            vec![Arc::new(values)],
            paro_common::test_utils::test_allocator(),
        );
        let store = build_store(chunk, vec![LogicalType::Integer]);

        let mut cursor = store.cursor_pinned().unwrap();
        assert_eq!(cursor.compare(0, 1).unwrap(), Ordering::Less);
        assert_eq!(cursor.compare(2, 1).unwrap(), Ordering::Greater);
    }

    #[test]
    fn encode_batch_rejects_key_type_mismatch() {
        let mut values = paro_common::test_utils::test_vector(LogicalType::Boolean);
        values.set_bool(0, true);
        values.set_count(1);
        let chunk = Chunk::from_arc_vectors(
            vec![Arc::new(values)],
            paro_common::test_utils::test_allocator(),
        );

        let pool = Arc::new(BufferPool::new(32 * 1024 * 1024));
        let encoding = Arc::new(
            SortKeyEncoding::new(
                vec![LogicalType::Integer],
                vec![paro_common::sort_key::OrderModifiers::new(true, false)],
            )
            .unwrap(),
        );
        let mut store = SortKeyStore::new(pool, encoding);
        let error = store.encode_batch(&chunk).unwrap_err().to_string();
        assert!(error.contains("sort key column 0 type mismatch"));
    }

    #[test]
    fn variable_keys_round_trip_and_compare() {
        let mut values = paro_common::test_utils::test_vector(LogicalType::Varchar);
        values.set_string(0, "alpha");
        values.set_string(1, "alphabet");
        values.set_string(2, "beta");
        values.set_count(3);
        let chunk = Chunk::from_arc_vectors(
            vec![Arc::new(values)],
            paro_common::test_utils::test_allocator(),
        );
        let store = build_store(chunk, vec![LogicalType::Varchar]);

        let mut cursor = store.cursor_on_demand(2);
        assert_eq!(cursor.read_key(0).unwrap(), b"\x01bmqib\0");
        assert_eq!(cursor.compare(0, 1).unwrap(), Ordering::Less);
        assert_eq!(cursor.compare(2, 1).unwrap(), Ordering::Greater);
    }

    #[test]
    fn prefix_permutation_refines_variable_prefix_collisions() {
        let row_count = PREFIX_SORT_MIN_ROWS + 64;
        let mut values = paro_common::test_utils::test_vector(LogicalType::Varchar);
        for row_idx in 0..row_count {
            let value = row_count - row_idx;
            values.set_string(
                row_idx,
                &format!("shared-prefix-0123456789-abcdefghijklmnop-{value:04}"),
            );
        }
        values.set_count(row_count);
        let chunk = Chunk::from_arc_vectors(
            vec![Arc::new(values)],
            paro_common::test_utils::test_allocator(),
        );
        let store = build_store(chunk, vec![LogicalType::Varchar]);
        let permutation = store.sorted_permutation().expect("radix permutation");
        assert_eq!(permutation.len(), row_count);

        let mut cursor = store.cursor_pinned().expect("pinned cursor");
        assert_eq!(
            cursor
                .materialized_sort_prefix(permutation[0])
                .expect("first prefix"),
            cursor
                .materialized_sort_prefix(permutation[row_count - 1])
                .expect("last prefix")
        );
        for adjacent in permutation.windows(2) {
            assert_ne!(
                cursor.compare(adjacent[0], adjacent[1]).expect("compare"),
                Ordering::Greater
            );
        }
    }

    #[test]
    fn variable_overflow_is_row_contiguous() {
        let mut values = paro_common::test_utils::test_vector(LogicalType::Blob);
        values.set_blob(0, &[7; 40]);
        values.set_blob(1, &[9; 40]);
        values.set_count(2);
        let chunk = Chunk::from_arc_vectors(
            vec![Arc::new(values)],
            paro_common::test_utils::test_allocator(),
        );
        let store = build_store(chunk, vec![LogicalType::Blob]);

        assert_eq!(store.overflow_blocks.len(), 1);
        assert_eq!(store.overflow_blocks[0].ordinal_end, 2);
    }

    #[test]
    fn reorder_copies_without_reencoding() {
        let mut values = paro_common::test_utils::test_vector(LogicalType::Varchar);
        values.set_string(0, "c");
        values.set_string(1, "a");
        values.set_string(2, "b");
        values.set_count(3);
        let chunk = Chunk::from_arc_vectors(
            vec![Arc::new(values)],
            paro_common::test_utils::test_allocator(),
        );
        let store = build_store(chunk, vec![LogicalType::Varchar]);
        let reordered = store.reorder_by_permutation(&[1, 2, 0]).unwrap();

        let mut cursor = reordered.cursor_on_demand(1);
        assert_eq!(cursor.read_key(0).unwrap(), b"\x01b\0");
        assert_eq!(cursor.read_key(1).unwrap(), b"\x01c\0");
        assert_eq!(cursor.read_key(2).unwrap(), b"\x01d\0");
    }

    #[test]
    fn release_frontier_waits_for_cursor_drop() {
        let mut values = paro_common::test_utils::test_vector(LogicalType::Integer);
        for idx in 0..8 {
            values.set_i32(idx, idx as i32);
        }
        values.set_count(8);
        let chunk = Chunk::from_arc_vectors(
            vec![Arc::new(values)],
            paro_common::test_utils::test_allocator(),
        );
        let store = build_store(chunk, vec![LogicalType::Integer]);

        let cursor = store.cursor_on_demand(1);
        store.advance_release_frontier(8).unwrap();
        assert_eq!(store.logical_release_frontier(), 8);
        assert_eq!(store.physical_release_frontier(), 0);
        drop(cursor);
        assert_eq!(store.physical_release_frontier(), 8);
    }
}
