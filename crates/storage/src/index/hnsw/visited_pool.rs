// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Visited Pool
//!
//! Thread-safe pool of visited-point trackers for HNSW search.
//!
//! ## Design Notes
//!
//! During HNSW search, we need to track which points have been visited.
//! Rather than allocating a new HashSet per search, we reuse VisitedList
//! instances from a pool. Each list stores a dense bitset and a fixed-capacity
//! directory of the bitset words touched by the current traversal. Resetting a
//! list clears only that directory, avoiding both O(N) generation wraparound
//! stalls and a byte-or-word counter for every graph point.

use crossbeam::queue::ArrayQueue;
use std::ptr::NonNull;
use std::sync::{Arc, OnceLock};

use super::graph_links::GraphPoint;
use super::types::PointOffset;
use crate::buffer::{BlockHandle, BufferHandle, BufferPool, FileBufferType, MemoryTag};
use paro_common::error::{self as paro_error, Result};

/// A filtered query can hold the ordinary traversal and one bridge traversal
/// workspace at the same time. Retaining exactly that pair keeps the common
/// path allocation-free without turning every opened artifact into a cache of
/// one workspace per possible concurrent query. Concurrent borrowers are
/// admitted by the query memory budget and surplus workspaces are not retained.
const DEFAULT_POOL_KEEP_LIMIT: usize = 2;
const POINTS_PER_VISITED_WORD: usize = u64::BITS as usize;
/// Once random word clears cover this fraction of the workspace, one
/// contiguous zero-fill is cheaper and friendlier to the memory hierarchy.
/// The comparison deliberately avoids timing-derived process-global state:
/// reset behavior is a local property of this traversal's density.
const RANDOM_CLEAR_COST_RATIO: usize = 4;

const fn visited_word_count(num_points: usize) -> usize {
    num_points.div_ceil(POINTS_PER_VISITED_WORD)
}

const fn visited_workspace_bytes(num_points: usize) -> usize {
    let words = visited_word_count(num_points);
    words
        .saturating_mul(std::mem::size_of::<u64>())
        .saturating_add(words.saturating_mul(std::mem::size_of::<u32>()))
}

/// RAII handle for a VisitedList borrowed from a VisitedPool.
///
/// Automatically returns the list to the pool when dropped.
#[derive(Debug)]
pub struct VisitedListHandle<'a> {
    pool: &'a VisitedPool,
    idle_workspace: Option<IdleVisitedWorkspace>,
    /// Present only for managed storage. Owning the pin on the active handle
    /// makes the lifetime relation explicit: typed addresses cannot outlive
    /// the guard that excludes buffer eviction.
    active_pin: Option<BufferHandle>,
    /// Typed regions resolved exactly once after the pooled storage is active.
    /// Keeping these on the active handle makes it impossible for idle,
    /// evictable storage to expose a dereferenceable pointer and removes
    /// representation dispatch from the per-neighbor graph loop.
    words: NonNull<u64>,
    touched_words: NonNull<u32>,
    word_count: usize,
    touched_word_count: usize,
}

/// Internal visited list that tracks which points have been seen.
///
/// Uses one bit per point plus a dense directory of words touched by the
/// current traversal. Clearing visits is proportional to the words this query
/// actually touched, with no generation-counter wraparound and no O(N) query
/// stall. The directory has a fixed worst-case bound of one u32 per 64 points,
/// so the complete workspace is governable before a query starts.
#[derive(Debug)]
struct IdleVisitedWorkspace {
    storage: IdleVisitedStorage,
}

impl IdleVisitedWorkspace {
    fn heap(num_points: usize) -> Result<Self> {
        let word_count = visited_word_count(num_points);
        let mut words = Vec::new();
        words.try_reserve_exact(word_count).map_err(|_| {
            paro_error::out_of_memory(format!(
                "allocate HNSW visited workspace for {num_points} points"
            ))
        })?;
        words.resize(word_count, 0);
        let mut touched_words = Vec::new();
        touched_words.try_reserve_exact(word_count).map_err(|_| {
            paro_error::out_of_memory(format!(
                "allocate HNSW visited reset directory for {num_points} points"
            ))
        })?;
        touched_words.resize(word_count, 0);
        Ok(Self {
            storage: IdleVisitedStorage::Heap {
                words: words.into_boxed_slice(),
                touched_words: touched_words.into_boxed_slice(),
            },
        })
    }

    fn managed(num_points: usize, pool: Arc<BufferPool>) -> Result<Self> {
        Ok(Self {
            storage: IdleVisitedStorage::managed(num_points, pool)?,
        })
    }

    fn is_managed_by(&self, pool: &Arc<BufferPool>) -> bool {
        self.storage.is_managed_by(pool)
    }
}

#[derive(Debug)]
enum IdleVisitedStorage {
    Heap {
        words: Box<[u64]>,
        touched_words: Box<[u32]>,
    },
    Managed(ManagedIdleVisitedStorage),
}

impl IdleVisitedStorage {
    fn managed(num_points: usize, pool: Arc<BufferPool>) -> Result<Self> {
        let word_count = visited_word_count(num_points);
        let bytes = visited_workspace_bytes(num_points);
        if bytes == 0 {
            return Ok(Self::Heap {
                words: Box::new([]),
                touched_words: Box::new([]),
            });
        }
        let pinned = pool.allocate(MemoryTag::VectorIndex, FileBufferType::Scratch, bytes)?;
        let block = pinned
            .block_handle()
            .cloned()
            .ok_or_else(|| paro_error::internal("scratch workspace has no buffer block"))?;
        // The queue stores only the evictable identity. An active borrow pins
        // the block and derives typed addresses on its handle; no pointer or
        // pin guard can leak into the idle cross-thread state.
        drop(pinned);
        let counters = ManagedIdleVisitedStorage {
            pool,
            block,
            word_count,
        };
        Ok(Self::Managed(counters))
    }

    fn is_managed_by(&self, pool: &Arc<BufferPool>) -> bool {
        matches!(self, Self::Managed(counters) if Arc::ptr_eq(&counters.pool, pool))
    }

    fn activate(&mut self) -> Result<ActiveVisitedRegions> {
        match self {
            Self::Heap {
                words,
                touched_words,
            } => {
                debug_assert_eq!(words.len(), touched_words.len());
                Ok(ActiveVisitedRegions {
                    pin: None,
                    words: NonNull::new(words.as_mut_ptr()).unwrap_or_else(NonNull::dangling),
                    touched_words: NonNull::new(touched_words.as_mut_ptr())
                        .unwrap_or_else(NonNull::dangling),
                    word_count: words.len(),
                })
            }
            Self::Managed(storage) => storage.activate(),
        }
    }
}

#[derive(Debug)]
struct ManagedIdleVisitedStorage {
    pool: Arc<BufferPool>,
    block: Arc<BlockHandle>,
    word_count: usize,
}

#[derive(Debug)]
struct ActiveVisitedRegions {
    pin: Option<BufferHandle>,
    words: NonNull<u64>,
    touched_words: NonNull<u32>,
    word_count: usize,
}

impl ManagedIdleVisitedStorage {
    fn activate(&self) -> Result<ActiveVisitedRegions> {
        let pin = self.pool.pin(self.block.block_id())?;
        let base = pin.ptr().ok_or_else(|| {
            paro_error::internal("managed visited workspace has no pinned allocation")
        })?;
        let words = NonNull::new(base.cast::<u64>()).ok_or_else(|| {
            paro_error::internal("managed visited workspace has a null word region")
        })?;
        // SAFETY: the buffer is at least `visited_workspace_bytes` long and
        // the word region has an exact multiple-of-eight byte length.
        let touched_words = unsafe {
            NonNull::new(
                base.add(self.word_count * std::mem::size_of::<u64>())
                    .cast::<u32>(),
            )
        }
        .ok_or_else(|| {
            paro_error::internal("managed visited workspace has a null reset directory")
        })?;
        Ok(ActiveVisitedRegions {
            pin: Some(pin),
            words,
            touched_words,
            word_count: self.word_count,
        })
    }
}

impl Drop for ManagedIdleVisitedStorage {
    fn drop(&mut self) {
        let _ = self.pool.free(self.block.block_id());
    }
}

impl Drop for VisitedListHandle<'_> {
    fn drop(&mut self) {
        // Pooled storage is always returned logically empty. Besides making
        // idle values representation-independent, this ensures no stale bit
        // can become visible after an evict/reconstruct cycle.
        self.clear_touched_words();
        // Invalidate the capability before releasing the eviction pin. The
        // addresses remain fields only because Rust cannot express a slice
        // borrowing a guard owned by the same struct.
        self.words = NonNull::dangling();
        self.touched_words = NonNull::dangling();
        self.word_count = 0;
        self.active_pin.take();
        if let Some(idle_workspace) = self.idle_workspace.take() {
            self.pool.return_back(idle_workspace);
        }
    }
}

impl<'a> VisitedListHandle<'a> {
    fn new(pool: &'a VisitedPool, mut idle_workspace: IdleVisitedWorkspace) -> Result<Self> {
        let active = idle_workspace.storage.activate()?;
        Ok(VisitedListHandle {
            pool,
            idle_workspace: Some(idle_workspace),
            active_pin: active.pin,
            words: active.words,
            touched_words: active.touched_words,
            word_count: active.word_count,
            touched_word_count: 0,
        })
    }

    /// Return `true` if the point was already visited in this iteration.
    #[inline(always)]
    pub fn check(&self, point_id: PointOffset) -> bool {
        let point = point_id as usize;
        let word = point / POINTS_PER_VISITED_WORD;
        let mask = 1_u64 << (point % POINTS_PER_VISITED_WORD);
        assert!(
            word < self.word_count,
            "HNSW point id exceeds the fixed visited workspace"
        );
        // SAFETY: `new` derives this pointer from active pinned/owned storage;
        // the handle owns that storage and the graph validates point ids.
        unsafe { self.words.as_ptr().add(word).read() & mask != 0 }
    }

    /// Mark a point as visited. Returns `true` if it was already visited.
    #[inline(always)]
    pub fn check_and_update_visited(&mut self, point_id: PointOffset) -> bool {
        let point = point_id as usize;
        let word_index = point / POINTS_PER_VISITED_WORD;
        assert!(
            word_index < self.word_count,
            "HNSW point id exceeds the fixed visited workspace"
        );
        self.check_and_update_index(point, word_index)
    }

    /// Mark a point already proven to belong to this search's graph/vector
    /// domain. The graph read view and visited workspace are both bound to the
    /// same cardinality at the search boundary, so repeating an assertion for
    /// every edge would not add a new correctness check.
    #[inline(always)]
    pub(crate) fn check_and_update_graph_point(&mut self, point: GraphPoint) -> bool {
        let point = point.index();
        let word_index = point / POINTS_PER_VISITED_WORD;
        debug_assert!(word_index < self.word_count);
        self.check_and_update_index(point, word_index)
    }

    #[inline(always)]
    fn check_and_update_index(&mut self, point: usize, word_index: usize) -> bool {
        let mask = 1_u64 << (point % POINTS_PER_VISITED_WORD);
        // SAFETY: See `check`. The active handle is the sole mutable owner of
        // both regions. The reset directory has exactly `word_count` slots,
        // and a word is appended only on its zero-to-nonzero transition.
        let word = unsafe { self.words.as_ptr().add(word_index) };
        let value = unsafe { word.read() };
        if value & mask != 0 {
            return true;
        }
        if value == 0 {
            debug_assert!(self.touched_word_count < self.word_count);
            unsafe {
                self.touched_words
                    .as_ptr()
                    .add(self.touched_word_count)
                    .write(word_index as u32);
            }
            self.touched_word_count += 1;
        }
        unsafe { word.write(value | mask) };
        false
    }

    /// Advance to the next iteration by clearing only words touched by the
    /// preceding traversal. This has no generation wraparound and never scans
    /// the complete point domain.
    pub fn next_iteration(&mut self) {
        self.clear_touched_words();
    }

    fn clear_touched_words(&mut self) {
        if self.touched_word_count == 0 {
            return;
        }
        if self
            .touched_word_count
            .saturating_mul(RANDOM_CLEAR_COST_RATIO)
            >= self.word_count
        {
            // SAFETY: `words` denotes exactly `word_count` initialized u64
            // values owned exclusively by this active handle.
            unsafe { self.words.as_ptr().write_bytes(0, self.word_count) };
            self.touched_word_count = 0;
            return;
        }
        // SAFETY: Both regions remain active for the handle lifetime and every
        // directory entry was written by `check_and_update_visited` with a
        // value smaller than `word_count`.
        for index in 0..self.touched_word_count {
            let word_index = unsafe { self.touched_words.as_ptr().add(index).read() } as usize;
            debug_assert!(word_index < self.word_count);
            unsafe { self.words.as_ptr().add(word_index).write(0) };
        }
        self.touched_word_count = 0;
    }
}

/// Thread-safe pool of VisitedList instances.
///
/// Keeps a bounded number of lists for reuse, creating new ones
/// dynamically when the pool is empty.
#[derive(Debug)]
pub struct VisitedPool {
    num_points: usize,
    pool: Option<ArrayQueue<IdleVisitedWorkspace>>,
    buffer_pool: OnceLock<Arc<BufferPool>>,
}

impl VisitedPool {
    pub fn new(num_points: usize) -> Self {
        Self::with_keep_limit(num_points, DEFAULT_POOL_KEEP_LIMIT)
    }

    /// Create a pool sized for the maximum number of concurrent query
    /// borrowers. Construction has a distinct generation-counter workspace;
    /// conflating the two makes either build resets or query memory governance
    /// unnecessarily expensive.
    pub fn with_keep_limit(num_points: usize, keep_limit: usize) -> Self {
        VisitedPool {
            num_points,
            // ArrayQueue requires a non-zero capacity. Keep the disabled
            // state explicit instead of allocating a sentinel queue that is
            // structurally present but semantically unreachable.
            pool: (keep_limit > 0).then(|| ArrayQueue::new(keep_limit)),
            buffer_pool: OnceLock::new(),
        }
    }

    /// Bind query workspaces to the instance buffer pool. Idle buffers become
    /// globally accounted, reconstructible eviction candidates instead of
    /// unbounded heap retained by every opened HNSW artifact.
    pub fn bind_buffer_pool(&self, buffer_pool: Arc<BufferPool>) -> Result<()> {
        if let Some(existing) = self.buffer_pool.get() {
            if Arc::ptr_eq(existing, &buffer_pool) {
                return Ok(());
            }
            return Err(paro_error::internal(
                "HNSW visited workspace cannot move between buffer pools",
            ));
        }
        if let Err(buffer_pool) = self.buffer_pool.set(buffer_pool) {
            if !self
                .buffer_pool
                .get()
                .is_some_and(|existing| Arc::ptr_eq(existing, &buffer_pool))
            {
                return Err(paro_error::internal(
                    "concurrent HNSW visited workspace buffer-pool binding",
                ));
            }
        }
        // Drop heap workspaces retained by an earlier explicit ungoverned
        // reader. Concurrent returns re-check the binding in `return_back`.
        if let Some(pool) = &self.pool {
            while pool.pop().is_some() {}
        }
        Ok(())
    }

    /// Get a VisitedListHandle from the pool (or create a new one).
    ///
    /// The handle is automatically returned when dropped.
    pub fn get(&self) -> Result<VisitedListHandle<'_>> {
        loop {
            let data = self.pool.as_ref().and_then(ArrayQueue::pop);
            let data = match data {
                Some(data) => data,
                None => match self.buffer_pool.get() {
                    Some(pool) => IdleVisitedWorkspace::managed(self.num_points, Arc::clone(pool))?,
                    None => IdleVisitedWorkspace::heap(self.num_points)?,
                },
            };
            if let Some(pool) = self.buffer_pool.get() {
                if !data.is_managed_by(pool) {
                    continue;
                }
            }
            return VisitedListHandle::new(self, data);
        }
    }

    /// Bytes owned while one workspace is borrowed. Query execution reserves
    /// this amount before acquisition, including when the backing allocation
    /// is reused from the artifact cache: the reservation governs concurrent
    /// active working sets rather than allocator events.
    pub const fn workspace_bytes(&self) -> usize {
        visited_workspace_bytes(self.num_points)
    }

    #[cfg(test)]
    fn retained_workspace_count(&self) -> usize {
        self.pool.as_ref().map_or(0, ArrayQueue::len)
    }

    fn return_back(&self, data: IdleVisitedWorkspace) {
        if self
            .buffer_pool
            .get()
            .is_some_and(|pool| !data.is_managed_by(pool))
        {
            return;
        }
        if let Some(pool) = &self.pool {
            // A concurrent return can fill the bounded queue between the
            // capacity observation and this push. Dropping that one surplus
            // workspace is the intended backpressure behavior.
            let _ = pool.push(data);
        }
    }
}

/// Construction/repair visited state uses a generation array instead of the
/// compact query bitset. These workloads reset a workspace millions of times
/// while keeping only a bounded build-pool width alive, so O(1) generation
/// advances are more important than query-side resident size or eliminating a
/// periodic sequential wrap reset.
#[derive(Debug)]
pub(crate) struct BuildVisitedPool {
    num_points: usize,
    pool: Option<ArrayQueue<BuildVisitedList>>,
}

#[derive(Debug)]
struct BuildVisitedList {
    generations: Box<[u8]>,
    current_generation: u8,
}

impl BuildVisitedList {
    fn new(num_points: usize) -> Result<Self> {
        let mut generations = Vec::new();
        generations.try_reserve_exact(num_points).map_err(|_| {
            paro_error::out_of_memory(format!(
                "allocate HNSW build visited workspace for {num_points} points"
            ))
        })?;
        generations.resize(num_points, 0);
        Ok(Self {
            generations: generations.into_boxed_slice(),
            current_generation: 0,
        })
    }

    fn advance_generation(&mut self) {
        if self.current_generation == u8::MAX {
            // Sequential clearing is deliberately a build-only tradeoff. At a
            // bounded cadence it trades streaming writes for half the random
            // generation-array footprint. Query workspaces use a different
            // compact backend and never pay this reset in a foreground query.
            self.generations.fill(0);
            self.current_generation = 1;
        } else {
            self.current_generation += 1;
        }
    }
}

#[derive(Debug)]
pub(crate) struct BuildVisitedListHandle<'a> {
    pool: &'a BuildVisitedPool,
    visited_list: Option<BuildVisitedList>,
}

impl BuildVisitedPool {
    pub(crate) fn new(num_points: usize) -> Self {
        Self::with_keep_limit(num_points, DEFAULT_POOL_KEEP_LIMIT)
    }

    pub(crate) fn with_keep_limit(num_points: usize, keep_limit: usize) -> Self {
        Self {
            num_points,
            pool: (keep_limit > 0).then(|| ArrayQueue::new(keep_limit)),
        }
    }

    pub(crate) fn get(&self) -> Result<BuildVisitedListHandle<'_>> {
        let mut visited_list = match self.pool.as_ref().and_then(ArrayQueue::pop) {
            Some(visited_list) => visited_list,
            None => BuildVisitedList::new(self.num_points)?,
        };
        visited_list.advance_generation();
        Ok(BuildVisitedListHandle {
            pool: self,
            visited_list: Some(visited_list),
        })
    }

    #[cfg(test)]
    const fn workspace_bytes(&self) -> usize {
        self.num_points.saturating_mul(std::mem::size_of::<u8>())
    }

    fn return_back(&self, visited_list: BuildVisitedList) {
        if let Some(pool) = &self.pool {
            let _ = pool.push(visited_list);
        }
    }
}

impl BuildVisitedListHandle<'_> {
    #[inline(always)]
    fn list(&self) -> &BuildVisitedList {
        self.visited_list
            .as_ref()
            .expect("HNSW build visited handle is active")
    }

    #[inline(always)]
    fn list_mut(&mut self) -> &mut BuildVisitedList {
        self.visited_list
            .as_mut()
            .expect("HNSW build visited handle is active")
    }

    #[inline(always)]
    pub(crate) fn check(&self, point_id: PointOffset) -> bool {
        let point = point_id as usize;
        assert!(
            point < self.list().generations.len(),
            "HNSW point id exceeds the fixed build visited workspace"
        );
        self.list().generations[point] == self.list().current_generation
    }

    #[inline(always)]
    pub(crate) fn check_and_update_visited(&mut self, point_id: PointOffset) -> bool {
        let point = point_id as usize;
        let current_generation = self.list().current_generation;
        assert!(
            point < self.list().generations.len(),
            "HNSW point id exceeds the fixed build visited workspace"
        );
        let generation = &mut self.list_mut().generations[point];
        let was_visited = *generation == current_generation;
        *generation = current_generation;
        was_visited
    }

    pub(crate) fn next_iteration(&mut self) {
        self.list_mut().advance_generation();
    }
}

impl Drop for BuildVisitedListHandle<'_> {
    fn drop(&mut self) {
        if let Some(visited_list) = self.visited_list.take() {
            self.pool.return_back(visited_list);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_visited_list_basic() {
        let pool = VisitedPool::new(10);
        let mut visited = pool.get().unwrap();

        // Initially not visited
        assert!(!visited.check(0));
        assert!(!visited.check(5));

        // Mark as visited
        assert!(!visited.check_and_update_visited(0)); // was not visited
        assert!(visited.check(0)); // now visited
        assert!(visited.check_and_update_visited(0)); // was already visited

        assert!(!visited.check_and_update_visited(5));
        assert!(visited.check(5));
    }

    #[test]
    fn test_visited_list_iteration_reset() {
        let pool = VisitedPool::new(10);
        let mut visited = pool.get().unwrap();

        visited.check_and_update_visited(3);
        assert!(visited.check(3));

        // Next iteration clears all
        visited.next_iteration();
        assert!(!visited.check(3));
    }

    #[test]
    fn touched_word_reset_has_no_generation_wraparound() {
        let pool = VisitedPool::new(10);
        let mut visited = pool.get().unwrap();

        visited.check_and_update_visited(0);
        assert!(visited.check(0));

        // Reuse well beyond the old u8/u16 generation boundaries. Clearing is
        // tied only to touched words, so there is no periodic full reset.
        for _ in 0..(u16::MAX as usize + 10) {
            visited.next_iteration();
            assert!(!visited.check(0));
            assert!(!visited.check_and_update_visited(0));
        }
    }

    #[test]
    #[should_panic(expected = "HNSW point id exceeds the fixed visited workspace")]
    fn fixed_cardinality_rejects_out_of_domain_point_ids() {
        let pool = VisitedPool::new(5);
        let mut visited = pool.get().unwrap();
        visited.check_and_update_visited(100);
    }

    #[test]
    fn test_pool_reuse() {
        let pool = VisitedPool::new(10);

        // Get and return a list
        {
            let mut visited = pool.get().unwrap();
            visited.check_and_update_visited(3);
        }
        // The list is returned to the pool

        // Get another — should reuse the returned list
        {
            let visited = pool.get().unwrap();
            // After reuse + next_iteration, should be clean
            assert!(!visited.check(3));
        }
    }

    #[test]
    fn concurrent_borrowers_observe_logically_cleared_workspaces() {
        let pool = VisitedPool::with_keep_limit(4096, 8);
        std::thread::scope(|scope| {
            for worker in 0..8_u32 {
                let pool = &pool;
                scope.spawn(move || {
                    for iteration in 0..256_u32 {
                        let point = (worker * 257 + iteration) % 4096;
                        let mut visited = pool.get().unwrap();
                        assert!(!visited.check(point));
                        assert!(!visited.check_and_update_visited(point));
                        assert!(visited.check(point));
                    }
                });
            }
        });
    }

    #[test]
    fn surplus_concurrent_workspaces_are_not_retained() {
        let pool = VisitedPool::with_keep_limit(128, 2);
        {
            let _first = pool.get().unwrap();
            let _second = pool.get().unwrap();
            let _surplus = pool.get().unwrap();
        }
        assert_eq!(pool.retained_workspace_count(), 2);
        assert_eq!(pool.workspace_bytes(), visited_workspace_bytes(128));
    }

    #[test]
    fn managed_workspace_is_accounted_and_reconstructible_after_eviction() {
        let buffer_pool = BufferPool::new_arc(1024 * 1024);
        let pool = VisitedPool::with_keep_limit(128, 1);
        pool.bind_buffer_pool(buffer_pool.clone()).unwrap();

        {
            let mut visited = pool.get().unwrap();
            assert!(!visited.check_and_update_visited(17));
            assert!(visited.check(17));
        }
        assert_eq!(
            buffer_pool.get_tag_usage(MemoryTag::VectorIndex),
            pool.workspace_bytes() as i64
        );

        let eviction = buffer_pool.evict_blocks(MemoryTag::VectorIndex, 0, 0, None);
        assert!(eviction.success);
        assert_eq!(buffer_pool.get_tag_usage(MemoryTag::VectorIndex), 0);

        let visited = pool.get().unwrap();
        assert!(!visited.check(17));
        assert_eq!(
            buffer_pool.get_tag_usage(MemoryTag::VectorIndex),
            pool.workspace_bytes() as i64
        );
    }

    #[test]
    fn managed_workspaces_survive_concurrent_borrow_and_eviction() {
        const POINTS: usize = 1_000_003;
        const WORKERS: usize = 4;
        const ITERATIONS: usize = 2_000;

        let buffer_pool = BufferPool::new_arc(32 * 1024 * 1024);
        let pool = VisitedPool::with_keep_limit(POINTS, 2);
        pool.bind_buffer_pool(Arc::clone(&buffer_pool)).unwrap();

        std::thread::scope(|scope| {
            let buffer_pool = &buffer_pool;
            let evictor = scope.spawn(move || {
                for _ in 0..(WORKERS * ITERATIONS) {
                    let _ = buffer_pool.evict_blocks(MemoryTag::VectorIndex, 0, 0, None);
                    std::thread::yield_now();
                }
            });

            for worker in 0..WORKERS {
                let pool = &pool;
                scope.spawn(move || {
                    for iteration in 0..ITERATIONS {
                        let mut visited = pool.get().unwrap();
                        let first = (worker * 104_729 + iteration * 65_537) % POINTS;
                        let last = POINTS - 1 - first;
                        assert!(!visited.check_and_update_visited(first as PointOffset));
                        assert!(!visited.check_and_update_visited(last as PointOffset));
                        assert!(visited.check(first as PointOffset));
                        assert!(visited.check(last as PointOffset));
                    }
                });
            }

            evictor.join().unwrap();
        });
    }

    #[test]
    fn dense_query_reset_uses_full_clear_without_leaking_visits() {
        let points = POINTS_PER_VISITED_WORD * RANDOM_CLEAR_COST_RATIO;
        let pool = VisitedPool::new(points);
        let mut visited = pool.get().unwrap();
        for point in (0..points).step_by(POINTS_PER_VISITED_WORD) {
            assert!(!visited.check_and_update_visited(point as PointOffset));
        }
        assert_eq!(visited.touched_word_count, visited.word_count);

        visited.next_iteration();
        assert_eq!(visited.touched_word_count, 0);
        for point in (0..points).step_by(POINTS_PER_VISITED_WORD) {
            assert!(!visited.check(point as PointOffset));
        }
    }

    #[test]
    fn build_generation_workspace_reuses_without_per_iteration_clear() {
        let pool = BuildVisitedPool::with_keep_limit(128, 1);
        assert_eq!(pool.workspace_bytes(), 128 * std::mem::size_of::<u8>());
        {
            let mut visited = pool.get().unwrap();
            assert!(!visited.check_and_update_visited(17));
            assert!(visited.check(17));
            visited.next_iteration();
            assert!(!visited.check(17));
        }

        let visited = pool.get().unwrap();
        assert!(!visited.check(17));
    }

    #[test]
    fn build_generation_wrap_performs_complete_reset() {
        let mut visited = BuildVisitedList {
            generations: vec![u8::MAX, 7, u8::MAX].into_boxed_slice(),
            current_generation: u8::MAX,
        };
        visited.advance_generation();
        assert_eq!(visited.current_generation, 1);
        assert_eq!(&*visited.generations, &[0, 0, 0]);
    }
}
