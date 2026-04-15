//! PageCache - caching for storage pages with single-flight loading.
//!
//! This cache maps a PageKey (location + version isolation) to cached page
//! buffers stored in the BufferPool. It supports two kinds of cached pages:
//! compressed and decompressed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

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
}

impl PageContentKind {
    fn tag(self) -> MemoryTag {
        MemoryTag::PageCache
    }

    fn buffer_type(self) -> FileBufferType {
        match self {
            PageContentKind::Compressed => FileBufferType::ExternalFile,
            PageContentKind::Decompressed => FileBufferType::ManagedBuffer,
        }
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

#[derive(Debug, Clone)]
struct PageSlot {
    handle: SharedBlockHandle,
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
}

impl PageCacheEntryState {
    fn new() -> Self {
        Self {
            compressed: PageSlotState::Empty,
            decompressed: PageSlotState::Empty,
        }
    }

    fn slot(&self, kind: PageContentKind) -> &PageSlotState {
        match kind {
            PageContentKind::Compressed => &self.compressed,
            PageContentKind::Decompressed => &self.decompressed,
        }
    }

    fn slot_mut(&mut self, kind: PageContentKind) -> &mut PageSlotState {
        match kind {
            PageContentKind::Compressed => &mut self.compressed,
            PageContentKind::Decompressed => &mut self.decompressed,
        }
    }

    fn is_empty(&self) -> bool {
        self.compressed.is_empty() && self.decompressed.is_empty()
    }
}

struct PageCacheEntry {
    state: Mutex<PageCacheEntryState>,
    cvar: Condvar,
}

impl PageCacheEntry {
    fn new() -> Self {
        Self {
            state: Mutex::new(PageCacheEntryState::new()),
            cvar: Condvar::new(),
        }
    }
}

/// Page cache statistics (atomic counters).
#[derive(Debug, Default)]
pub struct PageCacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    entries: AtomicUsize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageCacheStatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
}

impl PageCacheStats {
    fn snapshot(&self) -> PageCacheStatsSnapshot {
        PageCacheStatsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            entries: self.entries.load(Ordering::Relaxed),
        }
    }
}

/// Page cache mapping PageKey -> cached page buffers.
pub struct PageCache {
    buffer_pool: Arc<BufferPool>,
    entries: RwLock<HashMap<PageKey, Arc<PageCacheEntry>>>,
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
        Self {
            buffer_pool,
            entries: RwLock::new(HashMap::new()),
            stats: PageCacheStats::default(),
        }
    }

    pub fn buffer_pool(&self) -> Arc<BufferPool> {
        self.buffer_pool.clone()
    }

    pub fn stats(&self) -> PageCacheStatsSnapshot {
        self.stats.snapshot()
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
            match state.slot(kind) {
                PageSlotState::Ready(slot) => slot.handle.clone(),
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

    /// Insert data into the cache and return a pinned handle.
    pub fn insert(
        &self,
        key: PageKey,
        kind: PageContentKind,
        data: Vec<u8>,
    ) -> Result<PageCacheHandle> {
        self.get_or_load(key, kind, || Ok(data))
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
        loop {
            let (entry, _) = self.get_or_insert_entry(&key);

            let mut state = entry.state.lock().unwrap();
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
                    let (buffer, block_handle) = self.allocate_and_copy(kind, &data)?;

                    let mut state = entry.state.lock().unwrap();
                    *state.slot_mut(kind) = PageSlotState::Ready(PageSlot {
                        handle: block_handle,
                    });
                    entry.cvar.notify_all();
                    drop(state);

                    return Ok(PageCacheHandle::new(buffer, kind));
                }
            }
        }
    }

    /// Remove a cache entry by key.
    pub fn remove(&self, key: &PageKey) -> bool {
        let removed = self.entries.write().unwrap().remove(key).is_some();
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

        let buffer = self
            .buffer_pool
            .allocate(kind.tag(), kind.buffer_type(), data.len())?;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::storage_metrics;

    #[test]
    fn page_cache_insert_and_lookup() {
        storage_metrics().reset_for_tests();
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

        let snap = storage_metrics().snapshot();
        assert_eq!(snap.page_cache_misses, 1);
        assert_eq!(snap.page_cache_hits, 1);
        assert_eq!(snap.page_cache_entries, 1);
    }
}
