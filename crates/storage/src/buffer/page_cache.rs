// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PageCache - caching for storage pages with single-flight loading.
//!
//! This cache maps a PageKey (location + version isolation) to cached page
//! buffers stored in the BufferPool. Physical, block-decompressed, and
//! codec-decoded representations have independent eviction slots.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use bytes::Bytes;
use paro_common::allocator::MemoryTag;
use paro_common::error::{self as paro_error, Result};

use super::{BufferHandle, BufferPool, FileBufferType, SharedBlockHandle};
use crate::metrics::storage_metrics;

/// Page key with version isolation and page pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageKey {
    pub tablet_id: u64,
    pub rowset_id: u64,
    pub rowset_gen: u64,
    pub segment_id: u32,
    pub page_offset: u64,
    pub page_size: u32,
}

impl PageKey {
    pub fn new(
        tablet_id: u64,
        rowset_id: u64,
        rowset_gen: u64,
        segment_id: u32,
        page_offset: u64,
        page_size: u32,
    ) -> Self {
        Self {
            tablet_id,
            rowset_id,
            rowset_gen,
            segment_id,
            page_offset,
            page_size,
        }
    }
}

/// Cached page content kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageContentKind {
    Compressed,
    Decompressed,
    Decoded,
}

impl PageContentKind {
    fn tag(self) -> MemoryTag {
        match self {
            PageContentKind::Compressed | PageContentKind::Decompressed => MemoryTag::PageCache,
            PageContentKind::Decoded => MemoryTag::DecodedPageCache,
        }
    }

    fn buffer_type(self) -> FileBufferType {
        // Every page-cache representation is reconstructible from the source
        // rowset. Cache eviction must discard it, never spill it as temporary
        // operator state.
        FileBufferType::ExternalFile
    }
}

/// RAII handle for a pinned page cache entry.
#[derive(Debug)]
pub struct PageCacheHandle {
    buffer: BufferHandle,
    kind: PageContentKind,
}

impl PageCacheHandle {
    fn new(buffer: BufferHandle, kind: PageContentKind) -> Self {
        Self { buffer, kind }
    }

    #[inline]
    pub fn kind(&self) -> PageContentKind {
        self.kind
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.buffer.size()
    }

    #[inline]
    pub fn data(&self) -> Option<&[u8]> {
        self.buffer.data()
    }

    /// Convert this pin into immutable shared bytes without copying. The
    /// buffer remains pinned until the last clone or slice is dropped.
    pub fn try_into_bytes(self) -> Result<Bytes> {
        let data = self
            .data()
            .ok_or_else(|| paro_error::internal("page cache buffer missing"))?;
        if data.is_empty() {
            return Err(paro_error::data_corrupted("page cache buffer is empty"));
        }
        let owner = PinnedPageBytes {
            address: data.as_ptr() as usize,
            len: data.len(),
            _pin: self,
        };
        Ok(Bytes::from_owner(owner))
    }

    /// Returns mutable slice of the page data if available.
    ///
    /// # Safety
    ///
    /// Caller must ensure no other references to this buffer exist.
    /// Interior mutability via raw pointer is intentional for page cache.
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn data_mut(&self) -> Option<&mut [u8]> {
        self.buffer.data_mut()
    }
}

/// Validated immutable view over a pinned cache buffer. The address and length
/// are captured only after `BufferHandle::data` succeeds; `_pin` keeps that
/// allocation resident for the entire lifetime of every derived `Bytes`.
struct PinnedPageBytes {
    address: usize,
    len: usize,
    _pin: PageCacheHandle,
}

impl AsRef<[u8]> for PinnedPageBytes {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: construction validated this non-empty range while `_pin`
        // owned a buffer pin. BufferPool cannot unload or move pinned blocks.
        unsafe { std::slice::from_raw_parts(self.address as *const u8, self.len) }
    }
}

#[derive(Debug, Clone)]
struct PageSlot {
    handle: SharedBlockHandle,
    decoded_meta: Option<Arc<DecodedEntryMeta>>,
}

#[derive(Debug)]
enum PageSlotState {
    Empty,
    Loading,
    Ready(PageSlot),
    Failed(String),
}

impl PageSlotState {
    fn is_empty(&self) -> bool {
        matches!(self, PageSlotState::Empty)
    }
}

#[derive(Debug)]
struct PageCacheEntryState {
    compressed: PageSlotState,
    decompressed: PageSlotState,
    decoded: PageSlotState,
    removing: bool,
    removed: bool,
}

impl PageCacheEntryState {
    fn new() -> Self {
        Self {
            compressed: PageSlotState::Empty,
            decompressed: PageSlotState::Empty,
            decoded: PageSlotState::Empty,
            removing: false,
            removed: false,
        }
    }

    fn slot(&self, kind: PageContentKind) -> &PageSlotState {
        match kind {
            PageContentKind::Compressed => &self.compressed,
            PageContentKind::Decompressed => &self.decompressed,
            PageContentKind::Decoded => &self.decoded,
        }
    }

    fn slot_mut(&mut self, kind: PageContentKind) -> &mut PageSlotState {
        match kind {
            PageContentKind::Compressed => &mut self.compressed,
            PageContentKind::Decompressed => &mut self.decompressed,
            PageContentKind::Decoded => &mut self.decoded,
        }
    }

    fn is_empty(&self) -> bool {
        !self.removing
            && self.compressed.is_empty()
            && self.decompressed.is_empty()
            && self.decoded.is_empty()
    }

    fn is_loading(&self) -> bool {
        matches!(self.compressed, PageSlotState::Loading)
            || matches!(self.decompressed, PageSlotState::Loading)
            || matches!(self.decoded, PageSlotState::Loading)
    }
}

struct PageCacheEntry {
    state: Mutex<PageCacheEntryState>,
    cvar: Condvar,
    decoded_accesses: AtomicU8,
}

impl PageCacheEntry {
    fn new() -> Self {
        Self {
            state: Mutex::new(PageCacheEntryState::new()),
            cvar: Condvar::new(),
            decoded_accesses: AtomicU8::new(0),
        }
    }

    /// A first sparse access leaves the page on probation. Repeated access is
    /// evidence of reuse and promotes it into the decoded cache. Saturation
    /// preserves that history without adding global admission metadata.
    fn observe_sparse_decoded_access(&self) -> bool {
        self.decoded_accesses
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                Some(count.saturating_add(1))
            })
            .is_ok_and(|previous| previous > 0)
    }
}

const DEFAULT_UNBOUNDED_DECODED_CAPACITY: usize = 256 * 1024 * 1024;
const DECODED_CACHE_MEMORY_FRACTION: usize = 4;
const DECODED_CACHE_MAX_ENTRY_FRACTION: usize = 8;

/// Capacity and admission policy for codec-decoded logical pages.
///
/// Decoded pages are larger than their physical representation and compete
/// with query operators for the instance buffer-pool limit. Their capacity is
/// therefore independent from the physical/decompressed page slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageCacheOptions {
    pub decoded_capacity: usize,
    pub decoded_max_entry_size: usize,
}

impl PageCacheOptions {
    pub fn for_memory_limit(memory_limit: usize) -> Self {
        let decoded_capacity = if memory_limit == 0 {
            DEFAULT_UNBOUNDED_DECODED_CAPACITY
        } else {
            memory_limit / DECODED_CACHE_MEMORY_FRACTION
        };
        Self {
            decoded_capacity,
            decoded_max_entry_size: decoded_capacity / DECODED_CACHE_MAX_ENTRY_FRACTION,
        }
    }

    pub fn with_decoded_capacity(mut self, capacity: usize) -> Self {
        self.decoded_capacity = capacity;
        self.decoded_max_entry_size = capacity / DECODED_CACHE_MAX_ENTRY_FRACTION;
        self
    }

    pub fn with_decoded_max_entry_size(mut self, max_entry_size: usize) -> Self {
        self.decoded_max_entry_size = max_entry_size;
        self
    }
}

#[derive(Debug)]
struct DecodedEntryMeta {
    size: usize,
    generation: u64,
    referenced: AtomicBool,
}

#[derive(Debug)]
struct DecodedCacheState {
    options: PageCacheOptions,
    resident_bytes: usize,
    pending_bytes: usize,
    generation: u64,
    entries: HashMap<PageKey, Arc<DecodedEntryMeta>>,
    clock: VecDeque<(u64, PageKey)>,
    retired_blocks: Vec<i64>,
}

impl DecodedCacheState {
    fn new(options: PageCacheOptions) -> Self {
        Self {
            options,
            resident_bytes: 0,
            pending_bytes: 0,
            generation: 0,
            entries: HashMap::new(),
            clock: VecDeque::new(),
            retired_blocks: Vec::new(),
        }
    }

    fn remove(&mut self, key: &PageKey) -> Option<Arc<DecodedEntryMeta>> {
        let meta = self.entries.remove(key)?;
        self.resident_bytes = self.resident_bytes.saturating_sub(meta.size);
        Some(meta)
    }

    fn commit(&mut self, key: PageKey, size: usize) -> Arc<DecodedEntryMeta> {
        self.pending_bytes = self.pending_bytes.saturating_sub(size);
        self.remove(&key);
        self.generation = self.generation.wrapping_add(1);
        let meta = Arc::new(DecodedEntryMeta {
            size,
            generation: self.generation,
            referenced: AtomicBool::new(false),
        });
        self.entries.insert(key, meta.clone());
        self.clock.push_back((meta.generation, key));
        self.resident_bytes = self.resident_bytes.saturating_add(size);
        self.compact_clock_if_needed();
        meta
    }

    fn release_reservation(&mut self, size: usize) {
        self.pending_bytes = self.pending_bytes.saturating_sub(size);
    }

    /// CLOCK keeps cache hits lock-free: page-local metadata gets a reference
    /// bit, while admission pays the synchronization and grants one second
    /// chance to pages touched since the previous sweep.
    fn next_victim(&mut self) -> Option<PageKey> {
        let mut remaining = self.clock.len().saturating_mul(2);
        while remaining > 0 {
            remaining -= 1;
            let (generation, key) = self.clock.pop_front()?;
            let Some(meta) = self.entries.get(&key) else {
                continue;
            };
            if meta.generation != generation {
                continue;
            }
            if meta.referenced.swap(false, Ordering::Relaxed) {
                self.clock.push_back((generation, key));
                continue;
            }
            return Some(key);
        }
        None
    }

    fn compact_clock_if_needed(&mut self) {
        let live = self.entries.len();
        if self.clock.len() <= live.saturating_mul(4).saturating_add(1024) {
            return;
        }
        self.clock = self
            .entries
            .iter()
            .map(|(key, meta)| (meta.generation, *key))
            .collect();
    }
}

/// Page cache statistics (atomic counters).
#[derive(Debug, Default)]
pub struct PageCacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    entries: AtomicUsize,
    decoded_admission_rejections: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageCacheStatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
    pub decoded_entries: usize,
    pub decoded_bytes: usize,
    pub decoded_physical_bytes: usize,
    pub decoded_capacity: usize,
    pub decoded_admission_rejections: u64,
}

impl PageCacheStats {
    fn snapshot(
        &self,
        decoded: &DecodedCacheState,
        decoded_physical_bytes: usize,
    ) -> PageCacheStatsSnapshot {
        PageCacheStatsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            entries: self.entries.load(Ordering::Relaxed),
            decoded_entries: decoded.entries.len(),
            decoded_bytes: decoded.resident_bytes,
            decoded_physical_bytes,
            decoded_capacity: decoded.options.decoded_capacity,
            decoded_admission_rejections: self.decoded_admission_rejections.load(Ordering::Relaxed),
        }
    }
}

/// Page cache mapping PageKey -> cached page buffers.
pub struct PageCache {
    buffer_pool: Arc<BufferPool>,
    entries: RwLock<HashMap<PageKey, Arc<PageCacheEntry>>>,
    decoded: Mutex<DecodedCacheState>,
    stats: PageCacheStats,
}

impl std::fmt::Debug for PageCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageCache")
            .field("stats", &self.stats())
            .finish()
    }
}

impl PageCache {
    pub fn new(buffer_pool: Arc<BufferPool>) -> Self {
        let options = PageCacheOptions::for_memory_limit(buffer_pool.max_memory());
        Self::with_options(buffer_pool, options)
    }

    pub fn with_options(buffer_pool: Arc<BufferPool>, options: PageCacheOptions) -> Self {
        Self {
            buffer_pool,
            entries: RwLock::new(HashMap::new()),
            decoded: Mutex::new(DecodedCacheState::new(options)),
            stats: PageCacheStats::default(),
        }
    }

    pub fn buffer_pool(&self) -> Arc<BufferPool> {
        self.buffer_pool.clone()
    }

    pub fn stats(&self) -> PageCacheStatsSnapshot {
        let decoded = self.decoded.lock().unwrap();
        let decoded_physical_bytes = self
            .buffer_pool
            .get_tag_usage(MemoryTag::DecodedPageCache)
            .max(0) as usize;
        self.stats.snapshot(&decoded, decoded_physical_bytes)
    }

    /// Return whether a sparse codec access has demonstrated reuse and should
    /// be promoted to the decoded cache. The probation counter shares the
    /// lifetime of the physical page entry, keeping admission metadata bounded
    /// by the page cache itself.
    pub(crate) fn should_promote_sparse_decoded(&self, key: &PageKey) -> bool {
        let entry = self.entries.read().unwrap().get(key).cloned();
        entry.is_some_and(|entry| entry.observe_sparse_decoded_access())
    }

    /// Non-blocking lookup for a cached page.
    pub fn lookup(&self, key: &PageKey, kind: PageContentKind) -> Option<PageCacheHandle> {
        let entry = {
            let guard = self.entries.read().unwrap();
            guard.get(key).cloned()
        };
        let Some(entry) = entry else {
            self.record_miss();
            return None;
        };

        let slot_handle = {
            let state = entry.state.lock().unwrap();
            if state.removing {
                self.record_miss();
                return None;
            }
            match state.slot(kind) {
                PageSlotState::Ready(slot) => {
                    if let Some(meta) = &slot.decoded_meta {
                        meta.referenced.store(true, Ordering::Relaxed);
                    }
                    slot.handle.clone()
                }
                PageSlotState::Empty | PageSlotState::Loading | PageSlotState::Failed(_) => {
                    self.record_miss();
                    return None;
                }
            }
        };

        if !slot_handle.is_loaded() {
            self.handle_unloaded(key, &entry, kind);
            self.record_miss();
            return None;
        }

        let buffer = match self.buffer_pool.pin(slot_handle.block_id()) {
            Ok(buf) => buf,
            Err(_) => {
                self.handle_unloaded(key, &entry, kind);
                self.record_miss();
                return None;
            }
        };

        self.record_hit();
        Some(PageCacheHandle::new(buffer, kind))
    }

    /// Insert data into the cache and return a pinned handle when admitted.
    pub fn insert(
        &self,
        key: PageKey,
        kind: PageContentKind,
        data: Vec<u8>,
    ) -> Result<Option<PageCacheHandle>> {
        if kind == PageContentKind::Decoded {
            return self.get_or_load_decoded_into(key, data.len(), |destination| {
                destination.copy_from_slice(&data);
                Ok(())
            });
        }
        self.get_or_load(key, kind, || Ok(data)).map(Some)
    }

    /// Get a cached page or load it with single-flight semantics.
    pub fn get_or_load<F>(
        &self,
        key: PageKey,
        kind: PageContentKind,
        loader: F,
    ) -> Result<PageCacheHandle>
    where
        F: FnOnce() -> Result<Vec<u8>>,
    {
        if kind == PageContentKind::Decoded {
            return Err(paro_error::invalid_input(
                "decoded pages require a sized cache loader",
            ));
        }
        loop {
            let (entry, _) = self.get_or_insert_entry(&key);

            let mut state = entry.state.lock().unwrap();
            if state.removing {
                while !state.removed {
                    state = entry.cvar.wait(state).unwrap();
                }
                continue;
            }
            match state.slot_mut(kind) {
                PageSlotState::Ready(slot) => {
                    let slot_handle = slot.handle.clone();
                    drop(state);

                    if !slot_handle.is_loaded() {
                        self.handle_unloaded(&key, &entry, kind);
                        continue;
                    }

                    let buffer = self.buffer_pool.pin(slot_handle.block_id())?;
                    self.record_hit();
                    return Ok(PageCacheHandle::new(buffer, kind));
                }
                PageSlotState::Loading => {
                    state = entry.cvar.wait(state).unwrap();
                    continue;
                }
                PageSlotState::Failed(err) => {
                    let err = err.clone();
                    *state.slot_mut(kind) = PageSlotState::Empty;
                    drop(state);
                    self.maybe_remove_entry(&key, &entry);
                    return Err(paro_error::internal(err));
                }
                PageSlotState::Empty => {
                    *state.slot_mut(kind) = PageSlotState::Loading;
                    drop(state);

                    self.record_miss();
                    let data = match loader() {
                        Ok(data) => data,
                        Err(err) => {
                            let err_msg = err.to_string();
                            let mut state = entry.state.lock().unwrap();
                            *state.slot_mut(kind) = PageSlotState::Failed(err_msg);
                            entry.cvar.notify_all();
                            drop(state);
                            return Err(err);
                        }
                    };
                    let (buffer, block_handle) = match self.allocate_and_copy(kind, &data) {
                        Ok(allocation) => allocation,
                        Err(err) => {
                            self.fail_loading(&entry, kind, &err);
                            return Err(err);
                        }
                    };

                    let mut state = entry.state.lock().unwrap();
                    *state.slot_mut(kind) = PageSlotState::Ready(PageSlot {
                        handle: block_handle,
                        decoded_meta: None,
                    });
                    entry.cvar.notify_all();
                    drop(state);

                    return Ok(PageCacheHandle::new(buffer, kind));
                }
            }
        }
    }

    /// Get or initialize a page directly in its buffer-pool allocation.
    ///
    /// `None` is a normal decoded-cache admission rejection. Callers retain
    /// their uncached representation and continue the query in that case.
    pub fn get_or_load_decoded_into<F>(
        &self,
        key: PageKey,
        size: usize,
        initializer: F,
    ) -> Result<Option<PageCacheHandle>>
    where
        F: FnOnce(&mut [u8]) -> Result<()>,
    {
        if size == 0 {
            return Err(paro_error::invalid_input("page data is empty"));
        }

        loop {
            let (entry, _) = self.get_or_insert_entry(&key);
            let mut state = entry.state.lock().unwrap();
            if state.removing {
                while !state.removed {
                    state = entry.cvar.wait(state).unwrap();
                }
                continue;
            }
            match state.slot_mut(PageContentKind::Decoded) {
                PageSlotState::Ready(slot) => {
                    let slot_handle = slot.handle.clone();
                    if let Some(meta) = &slot.decoded_meta {
                        meta.referenced.store(true, Ordering::Relaxed);
                    }
                    drop(state);
                    if !slot_handle.is_loaded() {
                        self.handle_unloaded(&key, &entry, PageContentKind::Decoded);
                        continue;
                    }
                    let buffer = self.buffer_pool.pin(slot_handle.block_id())?;
                    self.record_hit();
                    return Ok(Some(PageCacheHandle::new(buffer, PageContentKind::Decoded)));
                }
                PageSlotState::Loading => {
                    drop(entry.cvar.wait(state).unwrap());
                    continue;
                }
                PageSlotState::Failed(err) => {
                    let err = err.clone();
                    *state.slot_mut(PageContentKind::Decoded) = PageSlotState::Empty;
                    drop(state);
                    self.maybe_remove_entry(&key, &entry);
                    return Err(paro_error::internal(err));
                }
                PageSlotState::Empty => {
                    *state.slot_mut(PageContentKind::Decoded) = PageSlotState::Loading;
                    drop(state);
                }
            }

            self.record_miss();
            let Some(buffer) = self.try_allocate_decoded(key, size) else {
                self.cancel_loading(&key, &entry, PageContentKind::Decoded);
                self.stats
                    .decoded_admission_rejections
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            };

            // SAFETY: this newly allocated buffer is pinned by `buffer` and
            // is not reachable from the cache until initialization succeeds.
            let initialize_result = unsafe {
                buffer
                    .data_mut()
                    .ok_or_else(|| paro_error::internal("page cache buffer missing"))
                    .and_then(initializer)
            };
            if let Err(err) = initialize_result {
                let block_id = buffer.block_handle().map(|block| block.block_id());
                drop(buffer);
                if let Some(block_id) = block_id {
                    let _ = self.buffer_pool.free(block_id);
                }
                self.release_decoded_reservation(size);
                self.fail_loading(&entry, PageContentKind::Decoded, &err);
                return Err(err);
            }

            let Some(block_handle) = buffer.block_handle().cloned() else {
                let err = paro_error::internal("page cache block handle missing");
                self.release_decoded_reservation(size);
                self.fail_loading(&entry, PageContentKind::Decoded, &err);
                return Err(err);
            };
            // Keep decoded admission locked until its ready slot is visible.
            // Eviction acquires the same lock before removing a slot, so it
            // cannot observe committed metadata while the entry is Loading.
            let mut decoded = self.decoded.lock().unwrap();
            let decoded_meta = decoded.commit(key, size);
            let mut state = entry.state.lock().unwrap();
            *state.slot_mut(PageContentKind::Decoded) = PageSlotState::Ready(PageSlot {
                handle: block_handle,
                decoded_meta: Some(decoded_meta),
            });
            entry.cvar.notify_all();
            drop(state);
            drop(decoded);
            return Ok(Some(PageCacheHandle::new(buffer, PageContentKind::Decoded)));
        }
    }

    /// Remove a cache entry by key.
    pub fn remove(&self, key: &PageKey) -> bool {
        let Some(entry) = self.entries.read().unwrap().get(key).cloned() else {
            return false;
        };
        let mut state = entry.state.lock().unwrap();
        if state.removing {
            while !state.removed {
                state = entry.cvar.wait(state).unwrap();
            }
            return false;
        }
        state.removing = true;
        while state.is_loading() {
            state = entry.cvar.wait(state).unwrap();
        }
        let blocks = [
            (PageContentKind::Compressed, &state.compressed),
            (PageContentKind::Decompressed, &state.decompressed),
            (PageContentKind::Decoded, &state.decoded),
        ]
        .into_iter()
        .filter_map(|(kind, slot)| match slot {
            PageSlotState::Ready(slot) => Some((kind, slot.handle.block_id())),
            _ => None,
        })
        .collect::<Vec<_>>();
        drop(state);

        // Admission holds `decoded` before taking an entry-state lock. Do not
        // retain the entry lock while removing its decoded accounting.
        self.forget_decoded(key);
        for (kind, block_id) in blocks {
            if self.buffer_pool.free(block_id).is_err() && kind == PageContentKind::Decoded {
                self.decoded.lock().unwrap().retired_blocks.push(block_id);
            }
        }

        let mut state = entry.state.lock().unwrap();
        state.compressed = PageSlotState::Empty;
        state.decompressed = PageSlotState::Empty;
        state.decoded = PageSlotState::Empty;
        let removed = {
            let mut entries = self.entries.write().unwrap();
            if entries
                .get(key)
                .is_some_and(|existing| Arc::ptr_eq(existing, &entry))
            {
                entries.remove(key);
                true
            } else {
                false
            }
        };
        state.removed = true;
        entry.cvar.notify_all();
        drop(state);

        if removed {
            self.stats.entries.fetch_sub(1, Ordering::Relaxed);
            self.record_eviction();
            self.update_entries_gauge();
        }
        removed
    }

    fn get_or_insert_entry(&self, key: &PageKey) -> (Arc<PageCacheEntry>, bool) {
        let mut guard = self.entries.write().unwrap();
        if let Some(entry) = guard.get(key) {
            return (entry.clone(), false);
        }

        let entry = Arc::new(PageCacheEntry::new());
        guard.insert(*key, entry.clone());
        self.stats.entries.fetch_add(1, Ordering::Relaxed);
        self.update_entries_gauge();
        (entry, true)
    }

    /// Reserve decoded capacity and allocate its buffer as one serialized cold
    /// path. Keeping both operations under the capacity lock prevents parallel
    /// loaders from each observing the same physical headroom.
    fn try_allocate_decoded(&self, key: PageKey, size: usize) -> Option<BufferHandle> {
        let mut decoded = self.decoded.lock().unwrap();
        decoded
            .retired_blocks
            .retain(|block_id| self.buffer_pool.free(*block_id).is_err());
        let capacity = decoded.options.decoded_capacity;
        if capacity == 0 || size > capacity || size > decoded.options.decoded_max_entry_size {
            return None;
        }

        while decoded
            .resident_bytes
            .saturating_add(decoded.pending_bytes)
            .saturating_add(size)
            > capacity
        {
            let victim = decoded.next_victim()?;
            decoded.remove(&victim);
            if let Some(block_id) =
                self.remove_slot_without_index(&victim, PageContentKind::Decoded)
            {
                if self.buffer_pool.free(block_id).is_err() {
                    decoded.retired_blocks.push(block_id);
                }
            }
            self.record_eviction();
        }

        // Logical eviction cannot release a still-pinned retired entry. The
        // dedicated memory tag exposes that physical pressure, so admission
        // never promises capacity which the buffer pool cannot reclaim.
        let physical_bytes = self
            .buffer_pool
            .get_tag_usage(MemoryTag::DecodedPageCache)
            .max(0) as usize;
        if physical_bytes.saturating_add(size) > capacity {
            return None;
        }

        decoded.pending_bytes = decoded.pending_bytes.saturating_add(size);
        // A loading key is not in CLOCK yet, but removing stale metadata
        // makes retries after an externally evicted slot deterministic.
        decoded.remove(&key);
        match self.allocate(PageContentKind::Decoded, size) {
            Ok(buffer) => Some(buffer),
            Err(err) => {
                decoded.release_reservation(size);
                tracing::debug!(error = %err, size, "decoded page cache allocation rejected");
                None
            }
        }
    }

    fn release_decoded_reservation(&self, size: usize) {
        self.decoded.lock().unwrap().release_reservation(size);
    }

    fn forget_decoded(&self, key: &PageKey) {
        self.decoded.lock().unwrap().remove(key);
    }

    fn remove_slot_without_index(&self, key: &PageKey, kind: PageContentKind) -> Option<i64> {
        let entry = self.entries.read().unwrap().get(key).cloned()?;
        let block_id = {
            let mut state = entry.state.lock().unwrap();
            // Explicit removal owns every slot after publishing its tombstone.
            // Leaving the slot intact lets that path retire a pinned block
            // exactly once and prevents replacement from racing old cleanup.
            if state.removing {
                return None;
            }
            match std::mem::replace(state.slot_mut(kind), PageSlotState::Empty) {
                PageSlotState::Ready(slot) => Some(slot.handle.block_id()),
                previous => {
                    *state.slot_mut(kind) = previous;
                    None
                }
            }
        };
        self.maybe_remove_entry(key, &entry);
        block_id
    }

    fn cancel_loading(&self, key: &PageKey, entry: &Arc<PageCacheEntry>, kind: PageContentKind) {
        let mut state = entry.state.lock().unwrap();
        *state.slot_mut(kind) = PageSlotState::Empty;
        entry.cvar.notify_all();
        drop(state);
        self.maybe_remove_entry(key, entry);
    }

    fn fail_loading(
        &self,
        entry: &Arc<PageCacheEntry>,
        kind: PageContentKind,
        err: &paro_common::error::ParoError,
    ) {
        let mut state = entry.state.lock().unwrap();
        *state.slot_mut(kind) = PageSlotState::Failed(err.to_string());
        entry.cvar.notify_all();
    }

    fn handle_unloaded(&self, key: &PageKey, entry: &Arc<PageCacheEntry>, kind: PageContentKind) {
        {
            let mut state = entry.state.lock().unwrap();
            let slot = state.slot_mut(kind);
            if let PageSlotState::Ready(slot_val) = slot {
                if !slot_val.handle.is_loaded() {
                    *slot = PageSlotState::Empty;
                } else {
                    return;
                }
            } else if !matches!(slot, PageSlotState::Empty) {
                *slot = PageSlotState::Empty;
            }
        }

        if kind == PageContentKind::Decoded {
            self.forget_decoded(key);
        }
        self.record_eviction();
        self.maybe_remove_entry(key, entry);
    }

    fn maybe_remove_entry(&self, key: &PageKey, entry: &Arc<PageCacheEntry>) {
        let empty = {
            let state = entry.state.lock().unwrap();
            state.is_empty()
        };
        if !empty {
            return;
        }

        let mut guard = self.entries.write().unwrap();
        if let Some(existing) = guard.get(key) {
            if Arc::ptr_eq(existing, entry) {
                guard.remove(key);
                self.stats.entries.fetch_sub(1, Ordering::Relaxed);
                self.update_entries_gauge();
            }
        }
    }

    #[inline]
    fn record_hit(&self) {
        self.stats.hits.fetch_add(1, Ordering::Relaxed);
        storage_metrics().inc_page_cache_hit();
    }

    #[inline]
    fn record_miss(&self) {
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        storage_metrics().inc_page_cache_miss();
    }

    #[inline]
    fn record_eviction(&self) {
        self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        storage_metrics().inc_page_cache_eviction();
    }

    #[inline]
    fn update_entries_gauge(&self) {
        let entries = self.stats.entries.load(Ordering::Relaxed);
        storage_metrics().set_page_cache_entries(entries);
    }

    fn allocate_and_copy(
        &self,
        kind: PageContentKind,
        data: &[u8],
    ) -> Result<(BufferHandle, SharedBlockHandle)> {
        if data.is_empty() {
            return Err(paro_error::invalid_input("page data is empty"));
        }

        let buffer = self.allocate(kind, data.len())?;

        // SAFETY: BufferHandle guarantees the block is pinned.
        unsafe {
            let dest = buffer
                .data_mut()
                .ok_or_else(|| paro_error::internal("page cache buffer missing"))?;
            dest[..data.len()].copy_from_slice(data);
        }

        let handle = buffer
            .block_handle()
            .cloned()
            .ok_or_else(|| paro_error::internal("page cache block handle missing"))?;

        Ok((buffer, handle))
    }

    fn allocate(&self, kind: PageContentKind, size: usize) -> Result<BufferHandle> {
        if size == 0 {
            return Err(paro_error::invalid_input("page data is empty"));
        }
        self.buffer_pool
            .allocate(kind.tag(), kind.buffer_type(), size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_cache_insert_and_lookup() {
        let pool = BufferPool::new_arc(1024 * 1024);
        let cache = PageCache::new(pool);
        let key = PageKey::new(1, 2, 0, 3, 1024, 256);
        let data = vec![1u8, 2, 3, 4];

        let handle = cache
            .get_or_load(key, PageContentKind::Compressed, || Ok(data.clone()))
            .unwrap();
        assert_eq!(handle.size(), 4);
        assert_eq!(handle.data().unwrap()[0], 1u8);

        drop(handle);

        let handle2 = cache.lookup(&key, PageContentKind::Compressed).unwrap();
        assert_eq!(handle2.size(), 4);
        assert_eq!(handle2.data().unwrap()[3], 4u8);

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.entries, 1);
    }

    #[test]
    fn page_cache_handle_converts_to_zero_copy_bytes() {
        let pool = BufferPool::new_arc(1024 * 1024);
        let cache = PageCache::new(pool);
        let key = PageKey::new(1, 2, 0, 3, 1024, 256);
        let handle = cache
            .insert(key, PageContentKind::Decoded, vec![1, 2, 3, 4])
            .unwrap()
            .unwrap();
        let cached_ptr = handle.data().unwrap().as_ptr();

        let bytes = handle.try_into_bytes().unwrap();
        assert_eq!(bytes.as_ptr(), cached_ptr);
        assert_eq!(bytes.as_ref(), &[1, 2, 3, 4]);
        assert!(cache.remove(&key));
        assert_eq!(bytes.as_ref(), &[1, 2, 3, 4]);
    }

    #[test]
    fn invalid_page_cache_handle_conversion_is_fallible() {
        let handle = PageCacheHandle::new(BufferHandle::invalid(), PageContentKind::Decoded);
        assert!(handle.try_into_bytes().is_err());
    }

    #[test]
    fn decoded_cache_clock_preserves_recently_referenced_pages() {
        let pool = BufferPool::new_arc(1024 * 1024);
        let options = PageCacheOptions::for_memory_limit(pool.max_memory())
            .with_decoded_capacity(8)
            .with_decoded_max_entry_size(4);
        let cache = PageCache::with_options(pool, options);
        let first = PageKey::new(1, 1, 0, 0, 0, 4);
        let second = PageKey::new(1, 1, 0, 0, 4, 4);
        let third = PageKey::new(1, 1, 0, 0, 8, 4);

        drop(
            cache
                .insert(first, PageContentKind::Decoded, vec![1; 4])
                .unwrap(),
        );
        drop(
            cache
                .insert(second, PageContentKind::Decoded, vec![2; 4])
                .unwrap(),
        );
        drop(cache.lookup(&first, PageContentKind::Decoded));
        drop(
            cache
                .insert(third, PageContentKind::Decoded, vec![3; 4])
                .unwrap(),
        );

        assert!(cache.lookup(&first, PageContentKind::Decoded).is_some());
        assert!(cache.lookup(&second, PageContentKind::Decoded).is_none());
        assert!(cache.lookup(&third, PageContentKind::Decoded).is_some());
        let stats = cache.stats();
        assert_eq!(stats.decoded_entries, 2);
        assert_eq!(stats.decoded_bytes, 8);
        assert_eq!(stats.decoded_physical_bytes, 8);
    }

    #[test]
    fn pinned_retired_decoded_pages_block_admission_until_reaped() {
        let pool = BufferPool::new_arc(1024 * 1024);
        let options = PageCacheOptions::for_memory_limit(pool.max_memory())
            .with_decoded_capacity(4)
            .with_decoded_max_entry_size(4);
        let cache = PageCache::with_options(pool, options);
        let first = PageKey::new(1, 1, 0, 0, 0, 4);
        let second = PageKey::new(1, 1, 0, 0, 4, 4);

        let pinned = cache
            .insert(first, PageContentKind::Decoded, vec![1; 4])
            .unwrap()
            .unwrap()
            .try_into_bytes()
            .unwrap();
        assert!(cache
            .insert(second, PageContentKind::Decoded, vec![2; 4])
            .unwrap()
            .is_none());
        assert_eq!(cache.stats().decoded_physical_bytes, 4);

        drop(pinned);
        assert!(cache
            .insert(second, PageContentKind::Decoded, vec![2; 4])
            .unwrap()
            .is_some());
        assert_eq!(cache.stats().decoded_physical_bytes, 4);
    }

    #[test]
    fn in_flight_decoded_loaders_share_physical_capacity() {
        use std::sync::mpsc;

        let pool = BufferPool::new_arc(1024 * 1024);
        let options = PageCacheOptions::for_memory_limit(pool.max_memory())
            .with_decoded_capacity(12)
            .with_decoded_max_entry_size(4);
        let cache = Arc::new(PageCache::with_options(pool, options));
        let retired_key = PageKey::new(1, 1, 0, 0, 0, 4);
        let first_key = PageKey::new(1, 1, 0, 0, 4, 4);
        let second_key = PageKey::new(1, 1, 0, 0, 8, 4);
        let rejected_key = PageKey::new(1, 1, 0, 0, 12, 4);
        let retired_pin = cache
            .insert(retired_key, PageContentKind::Decoded, vec![0; 4])
            .unwrap()
            .unwrap()
            .try_into_bytes()
            .unwrap();

        let spawn_loader = |key| {
            let (entered_tx, entered_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();
            let cache = cache.clone();
            let thread = std::thread::spawn(move || {
                cache
                    .get_or_load_decoded_into(key, 4, |destination| {
                        entered_tx.send(()).unwrap();
                        resume_rx.recv().unwrap();
                        destination.fill(1);
                        Ok(())
                    })
                    .unwrap()
            });
            entered_rx.recv().unwrap();
            (thread, resume_tx)
        };

        let (first, resume_first) = spawn_loader(first_key);
        let (second, resume_second) = spawn_loader(second_key);
        assert!(cache
            .insert(rejected_key, PageContentKind::Decoded, vec![2; 4])
            .unwrap()
            .is_none());
        assert_eq!(cache.stats().decoded_physical_bytes, 12);

        resume_first.send(()).unwrap();
        resume_second.send(()).unwrap();
        drop(first.join().unwrap());
        drop(second.join().unwrap());
        assert_eq!(cache.stats().decoded_entries, 2);
        assert_eq!(cache.stats().decoded_bytes, 8);
        drop(retired_pin);
    }

    #[test]
    fn oversized_decoded_page_is_not_admitted() {
        let pool = BufferPool::new_arc(1024 * 1024);
        let options = PageCacheOptions::for_memory_limit(pool.max_memory())
            .with_decoded_capacity(64)
            .with_decoded_max_entry_size(8);
        let cache = PageCache::with_options(pool, options);
        let key = PageKey::new(1, 1, 0, 0, 0, 16);

        assert!(cache
            .insert(key, PageContentKind::Decoded, vec![1; 16])
            .unwrap()
            .is_none());
        assert_eq!(cache.stats().decoded_admission_rejections, 1);
        assert_eq!(
            cache.buffer_pool.get_tag_usage(MemoryTag::DecodedPageCache),
            0
        );
    }

    #[test]
    fn removal_tombstone_serializes_same_key_replacement() {
        use std::sync::mpsc;

        let pool = BufferPool::new_arc(1024 * 1024);
        let options = PageCacheOptions::for_memory_limit(pool.max_memory())
            .with_decoded_capacity(8)
            .with_decoded_max_entry_size(4);
        let cache = Arc::new(PageCache::with_options(pool, options));
        let key = PageKey::new(1, 1, 0, 0, 0, 4);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();

        let loading_cache = cache.clone();
        let loader = std::thread::spawn(move || {
            let handle = loading_cache
                .get_or_load_decoded_into(key, 4, |destination| {
                    entered_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                    destination.copy_from_slice(&[1; 4]);
                    Ok(())
                })
                .unwrap();
            drop(handle);
        });
        entered_rx.recv().unwrap();

        let removing_cache = cache.clone();
        let remover = std::thread::spawn(move || removing_cache.remove(&key));
        loop {
            let entry = cache.entries.read().unwrap().get(&key).cloned().unwrap();
            if entry.state.lock().unwrap().removing {
                break;
            }
            std::thread::yield_now();
        }

        let (replacement_entered_tx, replacement_entered_rx) = mpsc::channel();
        let replacement_cache = cache.clone();
        let replacement = std::thread::spawn(move || {
            replacement_cache
                .get_or_load_decoded_into(key, 4, |destination| {
                    replacement_entered_tx.send(()).unwrap();
                    destination.copy_from_slice(&[2; 4]);
                    Ok(())
                })
                .unwrap()
        });
        assert!(replacement_entered_rx.try_recv().is_err());

        resume_tx.send(()).unwrap();
        loader.join().unwrap();
        assert!(remover.join().unwrap());
        replacement_entered_rx.recv().unwrap();
        drop(replacement.join().unwrap());

        let replacement = cache.lookup(&key, PageContentKind::Decoded).unwrap();
        assert_eq!(replacement.data().unwrap(), &[2; 4]);
        assert_eq!(cache.stats().decoded_entries, 1);
        assert_eq!(cache.stats().decoded_physical_bytes, 4);
    }
}
