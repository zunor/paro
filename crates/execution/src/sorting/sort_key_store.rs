// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::sync::Mutex;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::sort_key::{compare_keys, SortKeyEncoding};
use paro_storage::buffer::{
    BlockId, BufferHandle, BufferPool, FileBufferType, MemoryTag, SharedBlockHandle,
    DEFAULT_BLOCK_SIZE,
};

const VARIABLE_TOTAL_LEN_OFFSET: usize = 16;
const VARIABLE_OVERFLOW_LEN_OFFSET: usize = 20;
const VARIABLE_OVERFLOW_BLOCK_OFFSET: usize = 24;
const VARIABLE_OVERFLOW_OFFSET_OFFSET: usize = 28;
const VARIABLE_INLINE_PREFIX_LEN: usize = 16;

#[derive(Debug)]
struct SlotBlockMeta {
    handle: Mutex<Option<SharedBlockHandle>>,
    row_start: u32,
    row_count: u32,
}

#[derive(Debug)]
struct OverflowBlockMeta {
    handle: Mutex<Option<SharedBlockHandle>>,
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

/// Immutable block-backed store for encoded sort keys.
#[derive(Debug)]
pub struct SortKeyStore {
    buffer_pool: Arc<BufferPool>,
    encoding: Arc<SortKeyEncoding>,
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
        let slot_size = encoding.slot_size().max(1);
        let slot_block_capacity = (DEFAULT_BLOCK_SIZE / slot_size).max(1) as u32;
        Self {
            buffer_pool,
            encoding,
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
        let key_lengths = if let Some(fixed_len) = self.encoding.fixed_key_len() {
            vec![fixed_len; row_count]
        } else {
            let mut key_lengths = Vec::with_capacity(row_count);
            for row_idx in 0..row_count {
                let key_len = self.encoding.encoded_len(chunk, row_idx, &columns)?;
                key_lengths.push(key_len);
            }
            key_lengths
        };

        let encoding = Arc::clone(&self.encoding);
        for (row_idx, &key_len) in key_lengths.iter().enumerate() {
            self.append_encoded_key_from_parts(key_len, |inline_prefix, overflow| {
                encoding.encode_row_into_parts(chunk, row_idx, &columns, inline_prefix, overflow)
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
        let mut reordered =
            SortKeyStore::new(Arc::clone(&self.buffer_pool), Arc::clone(&self.encoding));
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
            write_u32(slot, VARIABLE_TOTAL_LEN_OFFSET, total_len as u32);
            write_u32(slot, VARIABLE_OVERFLOW_LEN_OFFSET, overflow_len as u32);
            write_u32(
                slot,
                VARIABLE_OVERFLOW_BLOCK_OFFSET,
                overflow_block_idx as u32,
            );
            write_u32(
                slot,
                VARIABLE_OVERFLOW_OFFSET_OFFSET,
                overflow_offset as u32,
            );
        } else {
            write_key(&mut slot[..inline_len], &mut [])?;
            if self.encoding.fixed_key_len().is_none() {
                write_u32(slot, VARIABLE_TOTAL_LEN_OFFSET, total_len as u32);
                write_u32(slot, VARIABLE_OVERFLOW_LEN_OFFSET, 0);
            }
        }

        self.slot_blocks[slot_block_idx].row_count += 1;
        self.count += 1;
        Ok(())
    }

    fn append_key_from_cursor(&mut self, cursor: &mut KeyCursor<'_>, ordinal: u32) -> Result<()> {
        let mut slot = [0u8; 32];
        cursor.copy_slot(ordinal, &mut slot)?;

        if let Some(fixed_len) = self.encoding.fixed_key_len() {
            return self.append_encoded_key_from_parts(fixed_len, |inline_prefix, _overflow| {
                inline_prefix.copy_from_slice(&slot[..fixed_len]);
                Ok(())
            });
        }

        let total_len = read_u32(&slot, VARIABLE_TOTAL_LEN_OFFSET) as usize;
        let inline_len = total_len.min(VARIABLE_INLINE_PREFIX_LEN);
        let overflow_len = read_u32(&slot, VARIABLE_OVERFLOW_LEN_OFFSET) as usize;
        let source_overflow = if overflow_len > 0 {
            Some((
                read_u32(&slot, VARIABLE_OVERFLOW_BLOCK_OFFSET) as usize,
                read_u32(&slot, VARIABLE_OVERFLOW_OFFSET_OFFSET) as usize,
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
        let handle = self.buffer_pool.allocate(
            MemoryTag::OrderBy,
            FileBufferType::ManagedBuffer,
            block_size,
        )?;
        let block_handle = handle
            .block_handle()
            .cloned()
            .ok_or_else(|| paro_error::internal("failed to access slot block handle"))?;
        self.total_bytes += block_size;
        self.slot_blocks.push(SlotBlockMeta {
            handle: Mutex::new(Some(block_handle)),
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
            let handle = self.buffer_pool.allocate(
                MemoryTag::OrderBy,
                FileBufferType::ManagedBuffer,
                block_size,
            )?;
            let block_handle = handle
                .block_handle()
                .cloned()
                .ok_or_else(|| paro_error::internal("failed to access overflow block handle"))?;
            self.total_bytes += block_size;
            self.overflow_blocks.push(OverflowBlockMeta {
                handle: Mutex::new(Some(block_handle)),
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
            if let Some(handle) = self.slot_blocks[block_idx].handle.lock().unwrap().take() {
                let _ = self.buffer_pool.free(handle.block_id());
            }
        }
        self.release_state
            .released_slot_block_prefix
            .store(slot_target, AtomicOrdering::Release);

        let current_overflow = self
            .release_state
            .released_overflow_block_prefix
            .load(AtomicOrdering::Acquire);
        for block_idx in current_overflow..overflow_target {
            if let Some(handle) = self.overflow_blocks[block_idx]
                .handle
                .lock()
                .unwrap()
                .take()
            {
                let _ = self.buffer_pool.free(handle.block_id());
            }
        }
        self.release_state
            .released_overflow_block_prefix
            .store(overflow_target, AtomicOrdering::Release);

        self.release_state.physical_release_frontier.store(
            self.ordinal_frontier_for_slot_prefix(slot_target),
            AtomicOrdering::Release,
        );
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

        let mut left_slot = [0u8; 32];
        let mut right_slot = [0u8; 32];
        left_cursor.copy_slot(left, &mut left_slot)?;
        right_cursor.copy_slot(right, &mut right_slot)?;

        let fixed_left = left_cursor.store.encoding.fixed_key_len();
        let fixed_right = right_cursor.store.encoding.fixed_key_len();
        if fixed_left != fixed_right {
            return Err(paro_error::internal(
                "cannot compare sort keys built with different fixed-width contracts",
            ));
        }

        if let Some(fixed_len) = fixed_left {
            return Ok(compare_keys(
                &left_slot[..fixed_len],
                &right_slot[..fixed_len],
            ));
        }

        let left_total = read_u32(&left_slot, VARIABLE_TOTAL_LEN_OFFSET) as usize;
        let right_total = read_u32(&right_slot, VARIABLE_TOTAL_LEN_OFFSET) as usize;
        let left_inline = left_total.min(VARIABLE_INLINE_PREFIX_LEN);
        let right_inline = right_total.min(VARIABLE_INLINE_PREFIX_LEN);
        let shared_inline = left_inline.min(right_inline);
        let inline_order = compare_keys(&left_slot[..shared_inline], &right_slot[..shared_inline]);
        if inline_order != Ordering::Equal {
            return Ok(inline_order);
        }

        if left_total <= VARIABLE_INLINE_PREFIX_LEN || right_total <= VARIABLE_INLINE_PREFIX_LEN {
            return Ok(left_total.cmp(&right_total));
        }

        let left_overflow_len = read_u32(&left_slot, VARIABLE_OVERFLOW_LEN_OFFSET) as usize;
        let right_overflow_len = read_u32(&right_slot, VARIABLE_OVERFLOW_LEN_OFFSET) as usize;
        let left_block_idx = read_u32(&left_slot, VARIABLE_OVERFLOW_BLOCK_OFFSET) as usize;
        let right_block_idx = read_u32(&right_slot, VARIABLE_OVERFLOW_BLOCK_OFFSET) as usize;
        let left_offset = read_u32(&left_slot, VARIABLE_OVERFLOW_OFFSET_OFFSET) as usize;
        let right_offset = read_u32(&right_slot, VARIABLE_OVERFLOW_OFFSET_OFFSET) as usize;

        left_cursor.with_overflow_slice(
            left_block_idx,
            left_offset,
            left_overflow_len,
            |left_bytes| {
                right_cursor.with_overflow_slice(
                    right_block_idx,
                    right_offset,
                    right_overflow_len,
                    |right_bytes| compare_keys(left_bytes, right_bytes),
                )
            },
        )?
    }

    pub fn compare(&mut self, left: u32, right: u32) -> Result<Ordering> {
        let mut left_slot = [0u8; 32];
        let mut right_slot = [0u8; 32];
        self.copy_slot(left, &mut left_slot)?;
        self.copy_slot(right, &mut right_slot)?;

        if let Some(fixed_len) = self.store.encoding.fixed_key_len() {
            return Ok(compare_keys(
                &left_slot[..fixed_len],
                &right_slot[..fixed_len],
            ));
        }

        let left_total = read_u32(&left_slot, VARIABLE_TOTAL_LEN_OFFSET) as usize;
        let right_total = read_u32(&right_slot, VARIABLE_TOTAL_LEN_OFFSET) as usize;
        let left_inline = left_total.min(VARIABLE_INLINE_PREFIX_LEN);
        let right_inline = right_total.min(VARIABLE_INLINE_PREFIX_LEN);
        let shared_inline = left_inline.min(right_inline);
        let inline_order = compare_keys(&left_slot[..shared_inline], &right_slot[..shared_inline]);
        if inline_order != Ordering::Equal {
            return Ok(inline_order);
        }

        if left_total <= VARIABLE_INLINE_PREFIX_LEN || right_total <= VARIABLE_INLINE_PREFIX_LEN {
            return Ok(left_total.cmp(&right_total));
        }

        let left_overflow_len = read_u32(&left_slot, VARIABLE_OVERFLOW_LEN_OFFSET) as usize;
        let right_overflow_len = read_u32(&right_slot, VARIABLE_OVERFLOW_LEN_OFFSET) as usize;
        let left_block_idx = read_u32(&left_slot, VARIABLE_OVERFLOW_BLOCK_OFFSET) as usize;
        let right_block_idx = read_u32(&right_slot, VARIABLE_OVERFLOW_BLOCK_OFFSET) as usize;
        let left_offset = read_u32(&left_slot, VARIABLE_OVERFLOW_OFFSET_OFFSET) as usize;
        let right_offset = read_u32(&right_slot, VARIABLE_OVERFLOW_OFFSET_OFFSET) as usize;

        self.with_overflow_slices(
            left_block_idx,
            left_offset,
            left_overflow_len,
            right_block_idx,
            right_offset,
            right_overflow_len,
            |left_bytes, right_bytes| compare_keys(left_bytes, right_bytes),
        )
    }

    pub fn read_key(&mut self, ordinal: u32) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.read_key_into(ordinal, &mut out)?;
        Ok(out)
    }

    pub fn read_key_into(&mut self, ordinal: u32, out: &mut Vec<u8>) -> Result<()> {
        let mut slot = [0u8; 32];
        let _slot_len = self.copy_slot(ordinal, &mut slot)?;
        if let Some(fixed_len) = self.store.encoding.fixed_key_len() {
            out.clear();
            out.extend_from_slice(&slot[..fixed_len]);
            return Ok(());
        }

        let total_len = read_u32(&slot, VARIABLE_TOTAL_LEN_OFFSET) as usize;
        let inline_len = total_len.min(VARIABLE_INLINE_PREFIX_LEN);
        let overflow_len = read_u32(&slot, VARIABLE_OVERFLOW_LEN_OFFSET) as usize;
        out.clear();
        out.extend_from_slice(&slot[..inline_len]);

        if overflow_len > 0 {
            let block_idx = read_u32(&slot, VARIABLE_OVERFLOW_BLOCK_OFFSET) as usize;
            let offset = read_u32(&slot, VARIABLE_OVERFLOW_OFFSET_OFFSET) as usize;
            self.with_overflow_slice(block_idx, offset, overflow_len, |bytes| {
                out.extend_from_slice(bytes);
            })?;
        }

        Ok(())
    }

    fn copy_slot(&mut self, ordinal: u32, out: &mut [u8; 32]) -> Result<usize> {
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
    use paro_common::vector::Vector;

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
    fn fixed_keys_compare_in_order() {
        let mut values = Vector::new(LogicalType::Integer);
        values.set_i32(0, 10);
        values.set_i32(1, 20);
        values.set_i32(2, 30);
        values.set_count(3);
        let chunk = Chunk::from_arc_vectors(vec![Arc::new(values)]);
        let store = build_store(chunk, vec![LogicalType::Integer]);

        let mut cursor = store.cursor_pinned().unwrap();
        assert_eq!(cursor.compare(0, 1).unwrap(), Ordering::Less);
        assert_eq!(cursor.compare(2, 1).unwrap(), Ordering::Greater);
    }

    #[test]
    fn variable_keys_round_trip_and_compare() {
        let mut values = Vector::new(LogicalType::Varchar);
        values.set_string(0, "alpha");
        values.set_string(1, "alphabet");
        values.set_string(2, "beta");
        values.set_count(3);
        let chunk = Chunk::from_arc_vectors(vec![Arc::new(values)]);
        let store = build_store(chunk, vec![LogicalType::Varchar]);

        let mut cursor = store.cursor_on_demand(2);
        assert_eq!(cursor.read_key(0).unwrap(), b"\x01bmqib\0");
        assert_eq!(cursor.compare(0, 1).unwrap(), Ordering::Less);
        assert_eq!(cursor.compare(2, 1).unwrap(), Ordering::Greater);
    }

    #[test]
    fn variable_overflow_is_row_contiguous() {
        let mut values = Vector::new(LogicalType::Blob);
        values.set_blob(0, &[7; 40]);
        values.set_blob(1, &[9; 40]);
        values.set_count(2);
        let chunk = Chunk::from_arc_vectors(vec![Arc::new(values)]);
        let store = build_store(chunk, vec![LogicalType::Blob]);

        assert_eq!(store.overflow_blocks.len(), 1);
        assert_eq!(store.overflow_blocks[0].ordinal_end, 2);
    }

    #[test]
    fn reorder_copies_without_reencoding() {
        let mut values = Vector::new(LogicalType::Varchar);
        values.set_string(0, "c");
        values.set_string(1, "a");
        values.set_string(2, "b");
        values.set_count(3);
        let chunk = Chunk::from_arc_vectors(vec![Arc::new(values)]);
        let store = build_store(chunk, vec![LogicalType::Varchar]);
        let reordered = store.reorder_by_permutation(&[1, 2, 0]).unwrap();

        let mut cursor = reordered.cursor_on_demand(1);
        assert_eq!(cursor.read_key(0).unwrap(), b"\x01b\0");
        assert_eq!(cursor.read_key(1).unwrap(), b"\x01c\0");
        assert_eq!(cursor.read_key(2).unwrap(), b"\x01d\0");
    }

    #[test]
    fn release_frontier_waits_for_cursor_drop() {
        let mut values = Vector::new(LogicalType::Integer);
        for idx in 0..8 {
            values.set_i32(idx, idx as i32);
        }
        values.set_count(8);
        let chunk = Chunk::from_arc_vectors(vec![Arc::new(values)]);
        let store = build_store(chunk, vec![LogicalType::Integer]);

        let cursor = store.cursor_on_demand(1);
        store.advance_release_frontier(8).unwrap();
        assert_eq!(store.logical_release_frontier(), 8);
        assert_eq!(store.physical_release_frontier(), 0);
        drop(cursor);
        assert_eq!(store.physical_release_frontier(), 8);
    }
}
