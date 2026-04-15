//! EvictionQueue - manages eviction of unpinned blocks with dead node purging.
//!
//! - Concurrent lock-free queue for eviction nodes
//! - Automatic dead node detection and purging
//! - Multiple priority queues per FileBufferType
//! - Periodic purge triggered by insertion count
//! - Adaptive purge strategy based on dead/alive ratio

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crossbeam::queue::SegQueue;

use super::file_buffer_type::FileBufferType;
use super::BlockHandle;

/// Node in the eviction queue.
///
/// Stores a weak reference to the block handle along with the eviction
/// sequence number at the time of insertion. This allows detecting "dead" nodes
/// (blocks that were re-pinned and unpinned again) during eviction.
#[derive(Clone)]
pub(crate) struct BufferEvictionNode {
    /// Weak reference to the block handle
    handle: Weak<BlockHandle>,
    /// Eviction sequence number when this node was added.
    handle_sequence_number: u64,
}

impl BufferEvictionNode {
    /// Create a new eviction node.
    pub fn new(handle: Weak<BlockHandle>, eviction_seq_num: u64) -> Self {
        debug_assert!(handle.strong_count() > 0, "Handle must not be expired");
        Self {
            handle,
            handle_sequence_number: eviction_seq_num,
        }
    }

    /// Check if this node can unload the given block handle.
    ///
    /// Returns false if:
    /// - The sequence number doesn't match (block was re-pinned)
    /// - The block cannot be unloaded (still pinned or can_destroy=false)
    pub fn can_unload(&self, handle: &BlockHandle) -> bool {
        // Check if this is a stale entry (block was re-pinned and unpinned again)
        if self.handle_sequence_number != handle.current_eviction_seq_num() {
            return false; // Dead node - block was re-added to queue
        }

        handle.can_unload()
    }

    /// Try to get a strong reference to the block handle.
    ///
    /// Returns None if:
    /// - The block handle has been destroyed
    /// - The block cannot be unloaded (sequence mismatch or still pinned)
    pub fn try_get_block_handle(&self) -> Option<Arc<BlockHandle>> {
        let handle = self.handle.upgrade()?;

        // Check if we can unload this handle
        if !self.can_unload(&handle) {
            return None; // Handle was used in between
        }

        // This is the latest node in the queue with this handle
        Some(handle)
    }
}

/// Eviction queue for a specific set of FileBufferTypes.
///
/// Manages a concurrent queue of eviction nodes with automatic dead node purging.
/// Each queue handles one or more FileBufferTypes (e.g., BLOCK+EXTERNAL_FILE, MANAGED_BUFFER, TINY_BUFFER).
pub(crate) struct EvictionQueue {
    /// The types of buffers in this queue (for verification only)
    file_buffer_types: Vec<FileBufferType>,

    /// The concurrent lock-free queue
    /// crossbeam::SegQueue provides the concurrent queue implementation here.
    q: SegQueue<BufferEvictionNode>,

    /// Total number of insertions into the eviction queue.
    /// This guides the schedule for calling Purge.
    evict_queue_insertions: AtomicUsize,

    /// Total dead nodes in the eviction queue.
    /// There are two scenarios in which a node dies:
    /// (1) we destroy its block handle, or
    /// (2) we insert a newer version into the eviction queue.
    total_dead_nodes: AtomicUsize,

    /// Locked if a queue purge is currently active or we're trying to forcefully evict a node.
    /// Only lets a single thread enter the purge phase.
    purge_lock: Mutex<()>,
}

impl EvictionQueue {
    /// We trigger a purge of the eviction queue every INSERT_INTERVAL insertions.
    const INSERT_INTERVAL: usize = 4096;

    /// We multiply the base purge size by this value.
    const PURGE_SIZE_MULTIPLIER: usize = 2;

    /// We multiply the purge size by this value to determine early-outs.
    /// This is the minimum queue size. We never purge below this point.
    const EARLY_OUT_MULTIPLIER: usize = 4;

    /// We multiply the approximate alive nodes by this value to test whether
    /// our total dead nodes exceed their allowed ratio. Must be greater than 1.
    const ALIVE_NODE_MULTIPLIER: usize = 4;

    /// Create a new eviction queue for the given FileBufferTypes.
    pub fn new(file_buffer_types: Vec<FileBufferType>) -> Self {
        Self {
            file_buffer_types,
            q: SegQueue::new(),
            evict_queue_insertions: AtomicUsize::new(0),
            total_dead_nodes: AtomicUsize::new(0),
            purge_lock: Mutex::new(()),
        }
    }

    /// Add a buffer handle to the eviction queue.
    ///
    /// Returns true if the queue is ready to be purged, and false otherwise.
    pub fn add_to_eviction_queue(&self, node: BufferEvictionNode) -> bool {
        self.q.push(node);
        let insertions = self.evict_queue_insertions.fetch_add(1, Ordering::Relaxed) + 1;
        insertions.is_multiple_of(Self::INSERT_INTERVAL)
    }

    /// Tries to dequeue an element from the eviction queue,
    /// but only after acquiring the purge queue lock.
    pub fn try_dequeue_with_lock(&self) -> Option<BufferEvictionNode> {
        let _lock = self.purge_lock.lock().unwrap();
        self.q.pop()
    }

    /// Increment the dead node counter in the purge queue.
    #[inline]
    pub fn increment_dead_nodes(&self) {
        self.total_dead_nodes.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the dead node counter in the purge queue.
    #[inline]
    pub fn decrement_dead_nodes(&self) {
        self.total_dead_nodes.fetch_sub(1, Ordering::Relaxed);
    }

    /// Check if this queue handles the given FileBufferType.
    pub fn has_file_buffer_type(&self, buffer_type: FileBufferType) -> bool {
        self.file_buffer_types.contains(&buffer_type)
    }

    /// Get approximate queue size (for testing and diagnostics).
    ///
    /// Note: This is an approximation due to concurrent access.
    pub fn size_approx(&self) -> usize {
        self.q.len()
    }

    /// Get total dead nodes count.
    #[allow(dead_code)]
    pub fn dead_nodes_count(&self) -> usize {
        self.total_dead_nodes.load(Ordering::Relaxed)
    }

    /// Garbage collect dead nodes in the eviction queue.
    ///
    /// This method implements an adaptive purge strategy:
    /// 1. Try to acquire purge_lock (early-out if another thread is purging)
    /// 2. Check if queue is large enough to justify purging
    /// 3. Iteratively purge until one of the early-out conditions is met:
    ///    - Queue size drops below threshold
    ///    - Ratio of alive/dead nodes is acceptable
    ///    - Maximum purge iterations reached
    pub fn purge(&self) {
        // Only one thread purges the queue, all other threads early-out
        let guard = self.purge_lock.try_lock();
        if guard.is_err() {
            return; // Another thread is already purging
        }
        let _guard = guard.unwrap();

        // We purge INSERT_INTERVAL * PURGE_SIZE_MULTIPLIER nodes
        let purge_size = Self::INSERT_INTERVAL * Self::PURGE_SIZE_MULTIPLIER;

        // Get an estimate of the queue size as-of now
        let approx_q_size = self.size_approx();

        // Early-out if the queue is not big enough to justify purging
        // - we want to keep the LRU characteristic alive
        if approx_q_size < purge_size * Self::EARLY_OUT_MULTIPLIER {
            return;
        }

        // There are two types of situations:
        //
        // For most scenarios, purging INSERT_INTERVAL * PURGE_SIZE_MULTIPLIER nodes is enough.
        // Purging more nodes than we insert also counters oscillation for scenarios where most nodes are dead.
        // If we always purge slightly more, we trigger a purge less often, as we purge below the trigger.
        //
        // However, if the pressure on the queue becomes too contested, we need to purge more aggressively,
        // i.e., we actively seek a specific number of dead nodes to purge. We use the total number of existing dead nodes.
        // We detect this situation by observing the queue's ratio between alive vs. dead nodes. If the ratio of alive vs.
        // dead nodes grows faster than we can purge, we keep purging until we hit one of the following conditions:
        //
        // 2.1. We're back at an approximate queue size less than purge_size * EARLY_OUT_MULTIPLIER.
        // 2.2. We're back at a ratio of 1*alive_node:ALIVE_NODE_MULTIPLIER*dead_nodes.
        // 2.3. We've purged the entire queue: max_purges is zero. This is a worst-case scenario,
        //      guaranteeing that we always exit the loop.

        let mut max_purges = approx_q_size / purge_size;
        while max_purges > 0 {
            self.purge_iteration(purge_size);

            // Update relevant sizes and potentially early-out
            let approx_q_size = self.size_approx();

            // Early-out according to (2.1)
            if approx_q_size < purge_size * Self::EARLY_OUT_MULTIPLIER {
                break;
            }

            let mut approx_dead_nodes = self.total_dead_nodes.load(Ordering::Relaxed);
            approx_dead_nodes = approx_dead_nodes.min(approx_q_size);
            let approx_alive_nodes = approx_q_size.saturating_sub(approx_dead_nodes);

            // Early-out according to (2.2)
            if approx_alive_nodes * (Self::ALIVE_NODE_MULTIPLIER - 1) > approx_dead_nodes {
                break;
            }

            max_purges -= 1;
        }
    }

    /// Bulk purge dead nodes from the eviction queue.
    /// Then, enqueue those that are still alive.
    fn purge_iteration(&self, purge_size: usize) {
        // Bulk dequeue
        // Note: crossbeam::SegQueue doesn't have try_dequeue_bulk, so we loop
        let mut dequeued_nodes = Vec::with_capacity(purge_size);
        for _ in 0..purge_size {
            if let Some(node) = self.q.pop() {
                dequeued_nodes.push(node);
            } else {
                break;
            }
        }

        let actually_dequeued = dequeued_nodes.len();

        // Retrieve all alive nodes that have been wrongly dequeued
        let mut alive_nodes = Vec::new();
        for node in dequeued_nodes {
            if node.try_get_block_handle().is_some() {
                // Node is still alive, keep it
                alive_nodes.push(node);
            }
        }

        let alive_count = alive_nodes.len();

        // Bulk re-add alive nodes
        // (TODO: order them by timestamp to better retain the LRU behavior)
        for node in alive_nodes {
            self.q.push(node);
        }

        // Update dead nodes counter
        let dead_nodes_purged = actually_dequeued - alive_count;
        if dead_nodes_purged > 0 {
            self.total_dead_nodes
                .fetch_sub(dead_nodes_purged, Ordering::Relaxed);
        }
    }

    /// Iterate over unloadable blocks in the eviction queue.
    ///
    /// Calls the provided closure for each block that can be unloaded.
    /// The closure receives the eviction node and block handle, and should
    /// return true to continue iteration or false to stop.
    ///
    /// This method automatically handles:
    /// - Dequeuing nodes from the queue
    /// - Skipping dead nodes (blocks that were re-pinned)
    /// - Decrementing dead node counter for skipped nodes
    /// - Early-out when closure returns false
    pub fn iterate_unloadable_blocks<F>(&self, mut callback: F)
    where
        F: FnMut(&BufferEvictionNode, &Arc<BlockHandle>) -> bool,
    {
        loop {
            // Get a block to unpin from the queue
            let node = match self.q.pop() {
                Some(node) => node,
                None => {
                    // Try one more time with lock
                    match self.try_dequeue_with_lock() {
                        Some(node) => node,
                        None => return, // Queue is empty
                    }
                }
            };

            // Get a reference to the underlying block pointer
            let handle = match node.try_get_block_handle() {
                Some(handle) => handle,
                None => {
                    // Dead node - block was destroyed or re-pinned
                    self.decrement_dead_nodes();
                    continue;
                }
            };

            // We might be able to free this block.
            // Atomic state on BlockHandle is sufficient for thread safety here.
            if !node.can_unload(&handle) {
                // Something changed in the meantime, bail out
                self.decrement_dead_nodes();
                continue;
            }

            // Call the callback - if it returns false, stop iteration
            if !callback(&node, &handle) {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::{default_allocator, MemoryTag};

    #[test]
    fn test_eviction_queue_creation() {
        let queue = EvictionQueue::new(vec![FileBufferType::ManagedBuffer]);
        assert_eq!(queue.size_approx(), 0);
        assert_eq!(queue.dead_nodes_count(), 0);
        assert!(queue.has_file_buffer_type(FileBufferType::ManagedBuffer));
        assert!(!queue.has_file_buffer_type(FileBufferType::Block));
    }

    #[test]
    fn test_add_to_eviction_queue() {
        let queue = EvictionQueue::new(vec![FileBufferType::ManagedBuffer]);

        let block = Arc::new(
            BlockHandle::allocate(
                1,
                MemoryTag::OrderBy,
                1024,
                true,
                Arc::new(default_allocator().clone()),
                FileBufferType::ManagedBuffer,
            )
            .unwrap(),
        );

        let seq_num = block.next_eviction_seq_num();
        let node = BufferEvictionNode::new(Arc::downgrade(&block), seq_num);

        // First INSERT_INTERVAL-1 insertions should return false
        for i in 0..EvictionQueue::INSERT_INTERVAL - 1 {
            let should_purge = queue.add_to_eviction_queue(node.clone());
            assert!(!should_purge, "Insertion {} should not trigger purge", i);
        }

        // INSERT_INTERVAL-th insertion should return true
        let should_purge = queue.add_to_eviction_queue(node.clone());
        assert!(
            should_purge,
            "Insertion at INSERT_INTERVAL should trigger purge"
        );

        assert_eq!(queue.size_approx(), EvictionQueue::INSERT_INTERVAL);
    }

    #[test]
    fn test_try_dequeue_with_lock() {
        let queue = EvictionQueue::new(vec![FileBufferType::ManagedBuffer]);

        let block = Arc::new(
            BlockHandle::allocate(
                1,
                MemoryTag::OrderBy,
                1024,
                true,
                Arc::new(default_allocator().clone()),
                FileBufferType::ManagedBuffer,
            )
            .unwrap(),
        );

        let seq_num = block.next_eviction_seq_num();
        let node = BufferEvictionNode::new(Arc::downgrade(&block), seq_num);

        queue.add_to_eviction_queue(node);

        let dequeued = queue.try_dequeue_with_lock();
        assert!(dequeued.is_some());
        assert_eq!(queue.size_approx(), 0);
    }

    #[test]
    fn test_dead_nodes_counter() {
        let queue = EvictionQueue::new(vec![FileBufferType::ManagedBuffer]);

        assert_eq!(queue.dead_nodes_count(), 0);

        queue.increment_dead_nodes();
        assert_eq!(queue.dead_nodes_count(), 1);

        queue.increment_dead_nodes();
        assert_eq!(queue.dead_nodes_count(), 2);

        queue.decrement_dead_nodes();
        assert_eq!(queue.dead_nodes_count(), 1);
    }

    #[test]
    fn test_purge_early_out_small_queue() {
        let queue = EvictionQueue::new(vec![FileBufferType::ManagedBuffer]);

        // Add a few nodes (less than EARLY_OUT threshold)
        let block = Arc::new(
            BlockHandle::allocate(
                1,
                MemoryTag::OrderBy,
                1024,
                true,
                Arc::new(default_allocator().clone()),
                FileBufferType::ManagedBuffer,
            )
            .unwrap(),
        );

        for _ in 0..100 {
            let seq_num = block.next_eviction_seq_num();
            let node = BufferEvictionNode::new(Arc::downgrade(&block), seq_num);
            queue.add_to_eviction_queue(node);
        }

        let size_before = queue.size_approx();
        queue.purge();
        let size_after = queue.size_approx();

        // Should early-out without purging (queue too small)
        assert_eq!(size_before, size_after);
    }

    #[test]
    fn test_purge_removes_dead_nodes() {
        let queue = EvictionQueue::new(vec![FileBufferType::ManagedBuffer]);

        // Add enough nodes to trigger purge
        let purge_threshold = EvictionQueue::INSERT_INTERVAL
            * EvictionQueue::PURGE_SIZE_MULTIPLIER
            * EvictionQueue::EARLY_OUT_MULTIPLIER;

        // Add dead nodes (block will be dropped)
        for i in 0..purge_threshold {
            let block = Arc::new(
                BlockHandle::allocate(
                    i as i64,
                    MemoryTag::OrderBy,
                    1024,
                    true,
                    Arc::new(default_allocator().clone()),
                    FileBufferType::ManagedBuffer,
                )
                .unwrap(),
            );
            let seq_num = block.next_eviction_seq_num();
            let node = BufferEvictionNode::new(Arc::downgrade(&block), seq_num);
            queue.add_to_eviction_queue(node);
            // Block is dropped here, making the node dead
        }

        let size_before = queue.size_approx();
        queue.purge();
        let size_after = queue.size_approx();

        // Should have removed dead nodes
        assert!(size_after < size_before, "Purge should remove dead nodes");
    }
}
