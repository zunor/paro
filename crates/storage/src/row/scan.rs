use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::vector::VECTOR_SIZE;

use crate::row::RowStore;

/// Block-aligned scan unit used by reclaim and sequential scanning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanChunkMeta {
    pub region_idx: usize,
    pub row_block_idx: usize,
    pub ordinal_start: u32,
    pub ordinal_end: u32,
    pub row_count: u32,
    pub local_ordinal_start: u32,
    pub local_ordinal_end: u32,
    pub heap_block_end: u32,
}

/// Owned scan progress that can be persisted independently from a cursor borrow.
#[derive(Debug, Clone, Default)]
pub struct RowScanState {
    scan_chunk_index: u32,
    offset_in_scan_chunk: u32,
}

impl RowScanState {
    pub fn reset(&mut self) {
        self.scan_chunk_index = 0;
        self.offset_in_scan_chunk = 0;
    }

    fn advance_scan_chunk(&mut self) {
        self.scan_chunk_index = self.scan_chunk_index.saturating_add(1);
        self.offset_in_scan_chunk = 0;
    }
}

fn next_chunk_with_state(
    store: &RowStore,
    state: &mut RowScanState,
    output: &mut Chunk,
    mut on_advance: impl FnMut(u32),
) -> Result<usize> {
    loop {
        let Some(meta) = store
            .scan_chunks()
            .get(state.scan_chunk_index as usize)
            .copied()
        else {
            output.set_cardinality(0);
            return Ok(0);
        };

        let remaining = meta.row_count.saturating_sub(state.offset_in_scan_chunk);
        if remaining == 0 {
            state.advance_scan_chunk();
            on_advance(state.scan_chunk_index);
            continue;
        }

        let batch_len = remaining.min(VECTOR_SIZE as u32);
        let start = meta.ordinal_start + state.offset_in_scan_chunk;
        let required_capacity = batch_len as usize;
        let output_types = store.layout().types();
        if output.column_count() != output_types.len()
            || output.types() != output_types
            || output.capacity() < required_capacity
        {
            *output = Chunk::initialize(output_types, VECTOR_SIZE.max(required_capacity));
        } else {
            output.reset();
        }

        let pinned = store.pin_ordinal_range(start, batch_len)?;
        let columns: Vec<usize> = (0..store.layout().column_count()).collect();
        pinned.gather_columns(&columns, output, 0)?;
        drop(pinned);

        state.offset_in_scan_chunk += batch_len;
        if state.offset_in_scan_chunk == meta.row_count {
            state.advance_scan_chunk();
            on_advance(state.scan_chunk_index);
        }

        return Ok(batch_len as usize);
    }
}

#[derive(Debug)]
struct FrontierRegistry {
    slots: Mutex<Vec<Option<Arc<AtomicU32>>>>,
}

impl FrontierRegistry {
    fn new() -> Self {
        Self {
            slots: Mutex::new(Vec::new()),
        }
    }

    fn register(&self, frontier: u32) -> ScannerSlot {
        let mut slots = self.slots.lock().expect("frontier registry poisoned");
        if let Some((index, slot)) = slots
            .iter_mut()
            .enumerate()
            .find(|(_, slot)| slot.is_none())
        {
            let frontier = Arc::new(AtomicU32::new(frontier));
            *slot = Some(Arc::clone(&frontier));
            return ScannerSlot { frontier, index };
        }

        let index = slots.len();
        let frontier = Arc::new(AtomicU32::new(frontier));
        slots.push(Some(Arc::clone(&frontier)));
        ScannerSlot { frontier, index }
    }

    fn unregister(&self, slot: &ScannerSlot) {
        let mut slots = self.slots.lock().expect("frontier registry poisoned");
        if let Some(entry) = slots.get_mut(slot.index) {
            *entry = None;
        }
    }

    fn min_frontier(&self, fallback: u32) -> u32 {
        let slots = self.slots.lock().expect("frontier registry poisoned");
        slots
            .iter()
            .filter_map(|slot| slot.as_ref())
            .map(|frontier| frontier.load(Ordering::Acquire))
            .min()
            .unwrap_or(fallback)
    }
}

#[derive(Debug)]
struct ScannerSlot {
    frontier: Arc<AtomicU32>,
    index: usize,
}

/// Multi-scanner reclaim tracker.
#[derive(Debug)]
pub struct ReclaimTracker {
    frontiers: FrontierRegistry,
    released_scan_chunk_prefix: AtomicU32,
}

impl ReclaimTracker {
    pub(crate) fn new() -> Self {
        Self {
            frontiers: FrontierRegistry::new(),
            released_scan_chunk_prefix: AtomicU32::new(0),
        }
    }

    pub(crate) fn register_scanner<'a>(&'a self, store: &'a RowStore) -> ReclaimToken<'a> {
        ReclaimToken {
            store,
            tracker: self,
            slot: Some(self.frontiers.register(0)),
        }
    }

    #[cfg(test)]
    pub(crate) fn released_scan_chunk_prefix(&self) -> u32 {
        self.released_scan_chunk_prefix.load(Ordering::Acquire)
    }

    fn update_frontier(&self, store: &RowStore, slot: &ScannerSlot, frontier: u32) {
        slot.frontier.store(frontier, Ordering::Release);
        self.try_reclaim(store);
    }

    fn unregister_scanner(&self, store: &RowStore, slot: &ScannerSlot) {
        self.frontiers.unregister(slot);
        self.try_reclaim(store);
    }

    fn try_reclaim(&self, store: &RowStore) {
        let target = self.frontiers.min_frontier(store.scan_chunk_count());
        let current = self.released_scan_chunk_prefix.load(Ordering::Acquire);
        if target <= current {
            return;
        }

        store.release_scan_chunk_prefix(current, target);
        self.released_scan_chunk_prefix
            .store(target, Ordering::Release);
    }
}

/// Reclaim registration for one scanner.
#[derive(Debug)]
pub struct ReclaimToken<'a> {
    store: &'a RowStore,
    tracker: &'a ReclaimTracker,
    slot: Option<ScannerSlot>,
}

impl<'a> ReclaimToken<'a> {
    fn advance(&self, frontier: u32) {
        if let Some(slot) = &self.slot {
            self.tracker.update_frontier(self.store, slot, frontier);
        }
    }
}

impl Drop for ReclaimToken<'_> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            self.tracker.unregister_scanner(self.store, &slot);
        }
    }
}

/// Sequential scan cursor over a sealed row store.
#[derive(Debug)]
pub struct RowScanCursor<'a> {
    store: &'a RowStore,
    state: RowScanState,
    reclaim: Option<ReclaimToken<'a>>,
}

impl<'a> RowScanCursor<'a> {
    pub(crate) fn new(store: &'a RowStore) -> Self {
        Self {
            store,
            state: RowScanState::default(),
            reclaim: None,
        }
    }

    pub(crate) fn with_reclaim(store: &'a RowStore, reclaim: ReclaimToken<'a>) -> Self {
        Self {
            store,
            state: RowScanState::default(),
            reclaim: Some(reclaim),
        }
    }

    /// Read the next scan chunk. Returns `0` at end of stream.
    pub fn next_chunk(&mut self, output: &mut Chunk) -> Result<usize> {
        let reclaim = self.reclaim.as_ref();
        next_chunk_with_state(self.store, &mut self.state, output, |frontier| {
            if let Some(reclaim) = reclaim {
                reclaim.advance(frontier);
            }
        })
    }
}

/// Thread-safe work-sharing scanner for a sealed row store.
#[derive(Debug)]
pub struct RowParallelScanCursor<'a> {
    inner: Mutex<RowScanCursor<'a>>,
}

impl<'a> RowParallelScanCursor<'a> {
    pub(crate) fn new(store: &'a RowStore) -> Self {
        Self {
            inner: Mutex::new(RowScanCursor::new(store)),
        }
    }

    pub fn next_chunk(&self, output: &mut Chunk) -> Result<usize> {
        let mut cursor = self.inner.lock().expect("row parallel scanner poisoned");
        cursor.next_chunk(output)
    }
}

impl RowStore {
    /// Scan using owned progress state instead of a borrowed cursor.
    pub fn scan_with_state(&self, state: &mut RowScanState, output: &mut Chunk) -> Result<usize> {
        next_chunk_with_state(self, state, output, |_| {})
    }
}
