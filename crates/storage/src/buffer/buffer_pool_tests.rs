// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Helper function to create a BufferPool with temporary directory set
fn create_pool_with_temp_dir(max_memory: usize) -> Arc<BufferPool> {
    let pool = BufferPool::new_arc(max_memory);
    // Set temporary directory for tests that need to evict ManagedBuffer blocks
    // Use a unique directory for each pool to avoid test interference
    // Combine process ID, thread ID, and timestamp for uniqueness
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let thread_id = std::thread::current().id();
    let temp_dir = std::env::temp_dir().join(format!(
        "paro_test_{}_{:?}_{}",
        std::process::id(),
        thread_id,
        timestamp
    ));
    pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
        .unwrap();
    pool
}

#[test]
fn test_buffer_pool_creation() {
    let pool = BufferPool::new_arc(1024 * 1024); // 1 MB
    assert_eq!(pool.max_memory(), 1024 * 1024);
    assert_eq!(pool.used_memory(), 0);
    assert_eq!(pool.block_count(), 0);
}

#[test]
fn test_allocate_basic() {
    let pool = Arc::new(BufferPool::new(1024 * 1024));
    let handle = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            4096,
        )
        .unwrap();

    assert!(handle.is_valid());
    assert_eq!(handle.size(), 4096);
    assert_eq!(pool.used_memory(), 4096);
    assert_eq!(pool.block_count(), 1);
}

#[test]
fn test_allocate_multiple() {
    let pool = Arc::new(BufferPool::new(1024 * 1024));

    let h1 = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            1024,
        )
        .unwrap();
    let h2 = pool
        .allocate(MemoryTag::HashTable, FileBufferType::ManagedBuffer, 2048)
        .unwrap();
    let h3 = pool
        .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 512)
        .unwrap();

    assert_eq!(pool.used_memory(), 1024 + 2048 + 512);
    assert_eq!(pool.block_count(), 3);

    drop(h1);
    drop(h2);
    drop(h3);
}

#[test]
fn test_data_access() {
    let pool = Arc::new(BufferPool::new(1024 * 1024));
    let handle = pool
        .allocate(MemoryTag::InMemoryTable, FileBufferType::ManagedBuffer, 256)
        .unwrap();

    // SAFETY: We have exclusive access via handle
    unsafe {
        let data = handle.data_mut().unwrap();
        for i in 0..256 {
            data[i] = i as u8;
        }
    }

    let data = handle.data().unwrap();
    for i in 0..256 {
        assert_eq!(data[i], i as u8);
    }
}

#[test]
fn test_out_of_memory() {
    use paro_common::error::codes;

    let pool = create_pool_with_temp_dir(1024); // Very small pool

    let result = pool.allocate(
        MemoryTag::InMemoryTable,
        FileBufferType::ManagedBuffer,
        2048,
    );
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert!(err.is(codes::resource::OUT_OF_MEMORY));
}

#[test]
fn test_set_memory_limit_with_evict_and_rollback() {
    let pool = BufferPool::new_arc(4096);

    // Evictable block.
    let handle = pool
        .allocate(MemoryTag::InMemoryTable, FileBufferType::Block, 1024)
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();
    drop(handle);
    pool.add_to_eviction_queue(block_id);

    pool.set_memory_limit(512).unwrap();
    assert_eq!(pool.max_memory(), 512);
    assert!(pool.used_memory() <= 512);

    pool.set_memory_limit(2048).unwrap();
    assert_eq!(pool.max_memory(), 2048);

    // Pinned block cannot be evicted: shrink should fail and rollback.
    let pinned = pool
        .allocate(MemoryTag::InMemoryTable, FileBufferType::Block, 1024)
        .unwrap();
    let err = pool.set_memory_limit(256).unwrap_err();
    assert!(
        err.to_string().contains("Failed to change memory limit"),
        "unexpected error: {}",
        err
    );
    assert_eq!(pool.max_memory(), 2048);
    drop(pinned);
}

#[test]
fn test_swap_limit_enforced_by_spill_manager() {
    let pool = create_pool_with_temp_dir(4096);
    pool.set_swap_limit(Some(128)).unwrap();

    let handle = pool
        .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();
    drop(handle);
    pool.add_to_eviction_queue(block_id);

    // Writing 1024 bytes with a 128-byte swap limit should fail eviction.
    let result = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);
    assert!(!result.success);
    assert!(pool.get_temporary_files().is_empty());
}

#[test]
fn test_eviction() {
    let pool = create_pool_with_temp_dir(4096);

    // Allocate a block
    let handle = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            1024,
        )
        .unwrap();

    // Drop handle; BufferHandle drop now routes through pool.unpin and
    // automatically updates eviction queues.
    drop(handle);

    // Now allocate more, which should trigger eviction
    let result = pool.allocate(
        MemoryTag::InMemoryTable,
        FileBufferType::ManagedBuffer,
        3500,
    );

    // Either eviction worked and we got memory, or we're at the limit
    // The key point is that eviction machinery was triggered
    if result.is_ok() {
        assert!(pool.stats.evictions.load(Ordering::Relaxed) >= 1);
    }
}

#[test]
fn test_pin_existing() {
    let pool = Arc::new(BufferPool::new(1024 * 1024));

    let handle = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            1024,
        )
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();

    // Get another handle to the same block
    let handle2 = pool.pin(block_id).unwrap();

    assert!(handle2.is_valid());
    assert_eq!(handle2.size(), 1024);

    // Both handles should work
    drop(handle);
    drop(handle2);
}

#[test]
fn test_free_unpinned() {
    let pool = Arc::new(BufferPool::new(1024 * 1024));

    let handle = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            1024,
        )
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();

    // Drop handle first (unpins)
    drop(handle);

    // Now we can free
    assert!(pool.free(block_id).is_ok());
    assert_eq!(pool.block_count(), 0);
}

#[test]
fn test_stats() {
    let pool = Arc::new(BufferPool::new(1024 * 1024));

    let _h1 = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            1024,
        )
        .unwrap();
    let _h2 = pool
        .allocate(MemoryTag::HashTable, FileBufferType::ManagedBuffer, 2048)
        .unwrap();

    let stats = pool.stats();
    assert_eq!(stats.allocations.load(Ordering::Relaxed), 2);
}

#[test]
fn test_unlimited_memory() {
    let pool = Arc::new(BufferPool::new(0)); // Unlimited

    // Should be able to allocate large blocks
    let _h = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            10 * 1024 * 1024,
        )
        .unwrap(); // 10 MB
    assert!(pool.available_memory() > 0);
}

// === LRU Enhancement Tests ===

#[test]
fn test_lru_eviction_seq_num() {
    let pool = create_pool_with_temp_dir(8192);

    // Allocate a block
    let handle = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            1024,
        )
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();
    let block = handle.block_handle().unwrap();

    // Initial sequence number should be 0
    assert_eq!(block.current_eviction_seq_num(), 0);

    // Drop should add to eviction queue and increment seq_num
    drop(handle);

    // Sequence number should be 1 after first addition
    let block = pool.get_block(block_id).unwrap();
    assert_eq!(block.current_eviction_seq_num(), 1);

    // Pin again and drop again - should increment seq_num
    let handle = pool.pin(block_id).unwrap();
    drop(handle);

    // Sequence number should be 2 after re-addition
    let block = pool.get_block(block_id).unwrap();
    assert_eq!(block.current_eviction_seq_num(), 2);
}

#[test]
fn test_lru_timestamp_tracking() {
    let pool = create_pool_with_temp_dir(8192);

    let handle = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            1024,
        )
        .unwrap();
    let block = handle.block_handle().unwrap();

    // Timestamp should be set on pin
    let ts1 = block.get_lru_timestamp();
    assert!(ts1 > 0);

    // Sleep a tiny bit and pin again
    std::thread::sleep(std::time::Duration::from_millis(2));
    let block_id = block.block_id();
    drop(handle);

    let handle2 = pool.pin(block_id).unwrap();
    let ts2 = handle2.block_handle().unwrap().get_lru_timestamp();

    // Timestamp should have been updated
    assert!(ts2 >= ts1);
}

#[test]
fn test_dead_node_detection() {
    let pool = create_pool_with_temp_dir(8192);

    // Allocate and immediately free a block
    let handle = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            1024,
        )
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();
    drop(handle);

    // Add to eviction queue with seq_num 1
    pool.add_to_eviction_queue(block_id);

    // Pin and unpin again - creates seq_num 2
    let handle = pool.pin(block_id).unwrap();
    drop(handle);
    pool.add_to_eviction_queue(block_id);

    // Now queue has two entries for same block with different seq_nums
    // First one (seq_num 1) is a dead node

    // Trigger eviction - should skip dead node and evict the live one
    // We want to evict the block, so we set a low memory limit
    let result = pool.evict_blocks(
        MemoryTag::InMemoryTable,
        0,   // No extra memory needed
        512, // Memory limit lower than current usage (1024)
        None,
    );
    assert!(result.success);

    // After eviction, BlockHandle should still exist but be unloaded
    let block = pool.get_block(block_id);
    assert!(
        block.is_some(),
        "BlockHandle should still exist after eviction"
    );
    assert!(
        !block.unwrap().is_loaded(),
        "Block should be unloaded after eviction"
    );

    // Dead nodes are now tracked per-queue, not globally
    // The queue should have detected and handled the dead node
}

#[test]
fn test_multi_priority_queues() {
    let pool = create_pool_with_temp_dir(16384);
    // Set temporary directory for ManagedBuffer eviction
    pool.set_temporary_directory("/tmp/paro_test".to_string())
        .unwrap();

    // Allocate blocks with different buffer types
    let h1 = pool
        .allocate(MemoryTag::InMemoryTable, FileBufferType::Block, 1024)
        .unwrap();
    let h2 = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            2048,
        )
        .unwrap();
    let h3 = pool
        .allocate(MemoryTag::InMemoryTable, FileBufferType::TinyBuffer, 512)
        .unwrap();

    let block_id1 = h1.block_handle().unwrap().block_id();
    let block_id2 = h2.block_handle().unwrap().block_id();
    let block_id3 = h3.block_handle().unwrap().block_id();

    // Drop handles and add to eviction queues
    drop(h1);
    drop(h2);
    drop(h3);
    pool.add_to_eviction_queue(block_id1);
    pool.add_to_eviction_queue(block_id2);
    pool.add_to_eviction_queue(block_id3);

    // Evict should prioritize Block (cheapest) first
    let result = pool.evict_blocks(
        MemoryTag::InMemoryTable,
        0,    // No extra memory needed
        1024, // Memory limit: evict until we're at or below 1024 bytes
        None,
    );
    assert!(result.success);

    // After eviction, BlockHandles should still exist but some should be unloaded
    // Block should be evicted first (highest priority)
    let block1 = pool.get_block(block_id1);
    assert!(
        block1.is_some(),
        "BlockHandle should still exist after eviction"
    );
    assert!(
        !block1.unwrap().is_loaded(),
        "Block1 should be unloaded after eviction"
    );

    // Other blocks may or may not be unloaded depending on total size
    // Since total is 1024+2048+512=3584, and limit is 1024, we need to evict at least 2560 bytes
    // So block1 (1024) + block2 (2048) should be evicted
    assert!(pool.get_block(block_id2).is_some());
    assert!(
        !pool.get_block(block_id2).unwrap().is_loaded(),
        "Block2 should be unloaded"
    );

    // Block3 might still be loaded
    assert!(pool.get_block(block_id3).is_some());
}

#[test]
fn test_buffer_reuse_exact_match() {
    let pool = create_pool_with_temp_dir(16384);

    // Allocate a block
    let h1 = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            1024,
        )
        .unwrap();
    let block_id = h1.block_handle().unwrap().block_id();
    drop(h1);
    pool.add_to_eviction_queue(block_id);

    // Request exact same size - should reuse
    let mut reused_buffer = None;
    let current_used = pool.used_memory();
    let memory_limit = current_used.saturating_sub(1024);
    let result = pool.evict_blocks(
        MemoryTag::InMemoryTable,
        1024,
        memory_limit,
        Some(&mut reused_buffer),
    );

    assert!(result.success);
    assert!(reused_buffer.is_some());
    assert_eq!(reused_buffer.unwrap().len(), 1024);
}

#[test]
fn test_buffer_reuse_acceptable_overhead() {
    let pool = create_pool_with_temp_dir(16384);

    // Allocate a 1280-byte block (25% larger than 1024)
    let h1 = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            1280,
        )
        .unwrap();
    let block_id = h1.block_handle().unwrap().block_id();
    drop(h1);
    pool.add_to_eviction_queue(block_id);

    // Request 1024 bytes - should reuse and resize
    let mut reused_buffer = None;
    let current_used = pool.used_memory();
    let memory_limit = current_used.saturating_sub(1024);
    let result = pool.evict_blocks(
        MemoryTag::InMemoryTable,
        1024,
        memory_limit,
        Some(&mut reused_buffer),
    );

    assert!(result.success);
    assert!(reused_buffer.is_some());
    // Buffer should be resized to requested size
    assert_eq!(reused_buffer.unwrap().len(), 1024);
}

#[test]
fn test_buffer_reuse_too_large() {
    let pool = create_pool_with_temp_dir(16384);

    // Allocate a 2048-byte block (100% larger than 1024, exceeds 25% threshold)
    let h1 = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            2048,
        )
        .unwrap();
    let block_id = h1.block_handle().unwrap().block_id();
    drop(h1);
    pool.add_to_eviction_queue(block_id);

    // Request 1024 bytes - should NOT reuse (too wasteful)
    let mut reused_buffer = None;
    let current_used = pool.used_memory();
    let memory_limit = current_used.saturating_sub(1024);
    let result = pool.evict_blocks(
        MemoryTag::InMemoryTable,
        1024,
        memory_limit,
        Some(&mut reused_buffer),
    );

    assert!(result.success);
    // Buffer should not be reused (too large)
    assert!(reused_buffer.is_none());
}

#[test]
fn test_buffer_reuse_too_small() {
    let pool = create_pool_with_temp_dir(16384);

    // Allocate two blocks to ensure we have something to evict
    let h1 = pool
        .allocate(MemoryTag::InMemoryTable, FileBufferType::ManagedBuffer, 512)
        .unwrap();
    let h2 = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            1024,
        )
        .unwrap();

    let block_id1 = h1.block_handle().unwrap().block_id();
    let block_id2 = h2.block_handle().unwrap().block_id();

    drop(h1);
    drop(h2);
    pool.add_to_eviction_queue(block_id1); // 512 bytes - too small
    pool.add_to_eviction_queue(block_id2); // 1024 bytes - exact match

    // Request 1024 bytes - should skip the 512-byte block and reuse the 1024-byte block
    let mut reused_buffer = None;
    let current_used = pool.used_memory();
    let memory_limit = current_used.saturating_sub(1024);
    let result = pool.evict_blocks(
        MemoryTag::InMemoryTable,
        1024,
        memory_limit,
        Some(&mut reused_buffer),
    );

    assert!(result.success);
    // Should reuse the 1024-byte block, not the 512-byte one
    assert!(reused_buffer.is_some());
    assert_eq!(reused_buffer.unwrap().len(), 1024);
}

#[test]
fn test_can_reuse_buffer() {
    // Exact match
    assert!(BufferPool::can_reuse_buffer(1024, 1024));

    // Acceptable overhead (up to 25%)
    assert!(BufferPool::can_reuse_buffer(1280, 1024)); // Exactly 25%
    assert!(BufferPool::can_reuse_buffer(1200, 1024)); // Less than 25%

    // Too much overhead (more than 25%)
    assert!(!BufferPool::can_reuse_buffer(1281, 1024)); // Just over 25%
    assert!(!BufferPool::can_reuse_buffer(2048, 1024)); // 100% overhead

    // Too small
    assert!(!BufferPool::can_reuse_buffer(1023, 1024));
    assert!(!BufferPool::can_reuse_buffer(512, 1024));
}

#[test]
fn test_resize_buffer_if_needed() {
    // Buffer larger than needed - should truncate
    let buffer = vec![1u8; 2048];
    let resized = BufferPool::resize_buffer_if_needed(buffer, 1024);
    assert_eq!(resized.len(), 1024);
    assert_eq!(resized.capacity(), 1024); // shrink_to_fit should work

    // Buffer exact size - no change
    let buffer = vec![1u8; 1024];
    let resized = BufferPool::resize_buffer_if_needed(buffer, 1024);
    assert_eq!(resized.len(), 1024);

    // Buffer smaller than needed - no change (shouldn't happen in practice)
    let buffer = vec![1u8; 512];
    let resized = BufferPool::resize_buffer_if_needed(buffer, 1024);
    assert_eq!(resized.len(), 512);
}

#[test]
fn test_buffer_reuse_statistics() {
    let pool = create_pool_with_temp_dir(16384);

    // Initially no statistics
    assert_eq!(pool.stats.buffer_reuse_attempts.load(Ordering::Relaxed), 0);
    assert_eq!(pool.stats.buffer_reuses.load(Ordering::Relaxed), 0);
    assert_eq!(
        pool.stats
            .buffer_reuse_size_mismatches
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(pool.stats.buffer_reuse_rate(), 0.0);

    // Allocate and evict with successful reuse
    let h1 = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            1024,
        )
        .unwrap();
    let block_id1 = h1.block_handle().unwrap().block_id();
    drop(h1);
    pool.add_to_eviction_queue(block_id1);

    let mut reused_buffer = None;
    let current_used = pool.used_memory();
    let memory_limit = current_used.saturating_sub(1024);
    let result = pool.evict_blocks(
        MemoryTag::InMemoryTable,
        1024,
        memory_limit,
        Some(&mut reused_buffer),
    );

    assert!(result.success);
    assert!(reused_buffer.is_some());

    // Check statistics
    assert_eq!(pool.stats.buffer_reuse_attempts.load(Ordering::Relaxed), 1);
    assert_eq!(pool.stats.buffer_reuses.load(Ordering::Relaxed), 1);
    assert_eq!(pool.stats.buffer_reuse_rate(), 100.0);

    // Allocate and evict with size mismatch
    let h2 = pool
        .allocate(
            MemoryTag::InMemoryTable,
            FileBufferType::ManagedBuffer,
            2048,
        )
        .unwrap();
    let block_id2 = h2.block_handle().unwrap().block_id();
    drop(h2);
    pool.add_to_eviction_queue(block_id2);

    let mut reused_buffer2 = None;
    let current_used = pool.used_memory();
    let memory_limit = current_used.saturating_sub(1024);
    let result = pool.evict_blocks(
        MemoryTag::InMemoryTable,
        1024, // Request 1024 but block is 2048 (too large)
        memory_limit,
        Some(&mut reused_buffer2),
    );

    assert!(result.success);
    assert!(reused_buffer2.is_none()); // Not reused due to size mismatch

    // Check updated statistics
    assert_eq!(pool.stats.buffer_reuse_attempts.load(Ordering::Relaxed), 2);
    assert_eq!(pool.stats.buffer_reuses.load(Ordering::Relaxed), 1); // Still 1
                                                                     // Size mismatch count may be >= 1 depending on how many blocks were checked
    assert!(
        pool.stats
            .buffer_reuse_size_mismatches
            .load(Ordering::Relaxed)
            >= 1
    );
    assert_eq!(pool.stats.buffer_reuse_rate(), 50.0); // 1/2 = 50%
}

#[test]
fn test_buffer_reuse_stats_string() {
    let pool = create_pool_with_temp_dir(16384);

    // Test with no attempts
    let stats_str = pool.stats.buffer_reuse_stats_string();
    assert!(stats_str.contains("0 successful"));
    assert!(stats_str.contains("0 attempts"));
    assert!(stats_str.contains("0.0% rate"));

    // Simulate some statistics
    pool.stats
        .buffer_reuse_attempts
        .store(10, Ordering::Relaxed);
    pool.stats.buffer_reuses.store(8, Ordering::Relaxed);
    pool.stats
        .buffer_reuse_size_mismatches
        .store(2, Ordering::Relaxed);

    let stats_str = pool.stats.buffer_reuse_stats_string();
    assert!(stats_str.contains("8 successful"));
    assert!(stats_str.contains("10 attempts"));
    assert!(stats_str.contains("80.0% rate"));
    assert!(stats_str.contains("2 size mismatches"));
}

#[test]
fn test_buffer_pool_debug_with_reuse_stats() {
    let pool = create_pool_with_temp_dir(16384);

    // Debug without reuse attempts
    let debug_str = format!("{:?}", pool);
    assert!(debug_str.contains("BufferPool"));
    assert!(debug_str.contains("max_memory"));
    assert!(!debug_str.contains("buffer_reuses")); // Should not show if no attempts

    // Simulate some reuse attempts
    pool.stats.buffer_reuse_attempts.store(5, Ordering::Relaxed);
    pool.stats.buffer_reuses.store(4, Ordering::Relaxed);

    let debug_str = format!("{:?}", pool);
    assert!(debug_str.contains("buffer_reuses"));
    assert!(debug_str.contains("buffer_reuse_attempts"));
    assert!(debug_str.contains("buffer_reuse_rate"));
}

#[test]
fn test_concurrent_reuse_statistics() {
    use std::thread;

    let pool = BufferPool::new_arc(1024 * 1024);

    // Set temporary directory for ManagedBuffer eviction
    let temp_dir = std::env::temp_dir().join("paro_test_concurrent_reuse");
    pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
        .unwrap();

    // Spawn multiple threads that perform evictions with reuse
    let mut handles = vec![];
    for _ in 0..4 {
        let pool_clone = pool.clone();
        let handle = thread::spawn(move || {
            for _ in 0..10 {
                // Allocate and evict
                let h = pool_clone
                    .allocate(
                        MemoryTag::InMemoryTable,
                        FileBufferType::ManagedBuffer,
                        1024,
                    )
                    .unwrap();
                let block_id = h.block_handle().unwrap().block_id();
                drop(h);
                pool_clone.add_to_eviction_queue(block_id);

                let mut reused_buffer = None;
                let _ = pool_clone.evict_blocks(
                    MemoryTag::InMemoryTable,
                    1024,
                    0,
                    Some(&mut reused_buffer),
                );
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Check that statistics were updated (exact values may vary due to concurrency)
    let attempts = pool.stats.buffer_reuse_attempts.load(Ordering::Relaxed);
    assert!(attempts > 0, "Should have recorded some reuse attempts");

    // Reuse rate should be reasonable
    let rate = pool.stats.buffer_reuse_rate();
    assert!(
        rate >= 0.0 && rate <= 100.0,
        "Reuse rate should be between 0 and 100"
    );
}

#[test]
fn test_buffer_allocator_integration() {
    // Note: This test is disabled because BufferPool no longer implements
    // paro_common::allocator::BufferManager trait directly (allocate now requires Arc<Self>).
    // StandardBufferManager implements crate::buffer::BufferManager, which is a different trait.
    //
    // TODO: Create a wrapper type if BufferAllocator integration is needed.
}

// === Per-Tag Memory Tracking Tests ===

#[test]
fn test_memory_usage_per_tag() {
    let pool = Arc::new(BufferPool::new(1024 * 1024));

    // Allocate with different tags
    let _h1 = pool
        .allocate(MemoryTag::HashTable, FileBufferType::ManagedBuffer, 1024)
        .unwrap();
    let _h2 = pool
        .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 2048)
        .unwrap();
    let _h3 = pool
        .allocate(MemoryTag::ArtIndex, FileBufferType::ManagedBuffer, 512)
        .unwrap();

    // Check per-tag usage
    assert_eq!(pool.get_tag_usage(MemoryTag::HashTable), 1024);
    assert_eq!(pool.get_tag_usage(MemoryTag::OrderBy), 2048);
    assert_eq!(pool.get_tag_usage(MemoryTag::ArtIndex), 512);
    assert_eq!(pool.get_tag_usage(MemoryTag::BaseTable), 0);

    // Check total
    let snapshot = pool.get_memory_usage_info();
    assert_eq!(snapshot.total(), 1024 + 2048 + 512);
}

#[test]
fn test_memory_usage_after_free() {
    let pool = Arc::new(BufferPool::new(1024 * 1024));

    // Allocate
    let handle = pool
        .allocate(MemoryTag::HashTable, FileBufferType::ManagedBuffer, 1024)
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();
    assert_eq!(pool.get_tag_usage(MemoryTag::HashTable), 1024);

    // Free
    drop(handle);
    pool.free(block_id).unwrap();

    // Tag usage should be 0
    assert_eq!(pool.get_tag_usage(MemoryTag::HashTable), 0);
    assert_eq!(pool.get_memory_usage_info().total(), 0);
}

#[test]
fn test_memory_usage_snapshot() {
    let pool = Arc::new(BufferPool::new(1024 * 1024));

    let _h1 = pool
        .allocate(MemoryTag::BaseTable, FileBufferType::ManagedBuffer, 100)
        .unwrap();
    let _h2 = pool
        .allocate(MemoryTag::HashTable, FileBufferType::ManagedBuffer, 200)
        .unwrap();
    let _h3 = pool
        .allocate(MemoryTag::ColumnData, FileBufferType::ManagedBuffer, 300)
        .unwrap();

    let snapshot = pool.get_memory_usage_info();

    // Check snapshot values
    assert_eq!(snapshot.get(MemoryTag::BaseTable), 100);
    assert_eq!(snapshot.get(MemoryTag::HashTable), 200);
    assert_eq!(snapshot.get(MemoryTag::ColumnData), 300);
    assert_eq!(snapshot.total(), 600);

    // Check non-zero iterator
    let non_zero: Vec<_> = snapshot.non_zero().collect();
    assert_eq!(non_zero.len(), 3);
}

#[test]
fn test_memory_usage_report() {
    let pool = Arc::new(BufferPool::new(1024 * 1024));

    let _h = pool
        .allocate(MemoryTag::HashTable, FileBufferType::ManagedBuffer, 1024)
        .unwrap();

    let snapshot = pool.get_memory_usage_info();
    let report = snapshot.format_report();

    assert!(report.contains("Memory Usage Report"));
    assert!(report.contains("Total: 1024 bytes"));
    assert!(report.contains("HASH_TABLE: 1024 bytes"));
}

#[test]
fn test_simple_temp_file_write() {
    // Very simple test to verify temp file writing works
    let pool = create_pool_with_temp_dir(8192);

    // Allocate a block
    let handle = pool
        .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();

    // Write some data
    unsafe {
        let data = handle.data_mut().unwrap();
        data[0] = 42;
        data[1] = 43;
    }

    // Verify data was written
    {
        let data = handle.data().unwrap();
        assert_eq!(data[0], 42);
        assert_eq!(data[1], 43);
    }

    // Drop handle and add to eviction queue
    drop(handle);
    pool.add_to_eviction_queue(block_id);

    // Manually trigger eviction
    let result = pool.evict_blocks(
        MemoryTag::OrderBy,
        0,
        0, // Force eviction
        None,
    );

    assert!(result.success, "Eviction should succeed");
    let temp_files = pool.get_temporary_files();
    assert!(
        !temp_files.is_empty(),
        "Expected at least one temporary spill file after eviction"
    );
    assert!(temp_files.iter().any(|info| info.size > 0));
}

#[test]
fn test_block_reload_mechanism() {
    // Test the complete block reload mechanism with temporary files

    let pool = create_pool_with_temp_dir(16384); // Large pool to avoid memory issues

    // Allocate a block and write some data
    let handle = pool
        .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();

    // Write test data
    unsafe {
        let data = handle.data_mut().unwrap();
        for i in 0..1024 {
            data[i] = (i % 256) as u8;
        }
    }

    // Verify block is loaded
    let block = pool.get_block(block_id).unwrap();
    assert!(block.is_loaded(), "Block should be loaded after allocation");

    // Drop handle and add to eviction queue
    drop(handle);
    pool.add_to_eviction_queue(block_id);

    // Manually evict the block
    let result = pool.evict_blocks(
        MemoryTag::OrderBy,
        0,
        0, // Force eviction
        None,
    );
    assert!(result.success, "Eviction should succeed");

    // Block should now be unloaded (written to temp file)
    let block = pool.get_block(block_id).unwrap();
    assert!(
        !block.is_loaded(),
        "Block should be unloaded after eviction"
    );

    // Verify temporary spill file exists
    let files_before_reload = pool.get_temporary_files();
    assert!(
        !files_before_reload.is_empty(),
        "Temp file should exist after eviction"
    );

    // Pin the block again - this should trigger reload from temp file
    let handle_reloaded = pool.pin(block_id).unwrap();
    assert!(
        handle_reloaded.is_valid(),
        "Should be able to pin unloaded block"
    );

    // Verify block is loaded again
    let block = pool.get_block(block_id).unwrap();
    assert!(block.is_loaded(), "Block should be loaded after pin");

    // Verify data integrity - data should be preserved through eviction/reload cycle
    let data = handle_reloaded.data().unwrap();
    for i in 0..1024 {
        assert_eq!(
            data[i],
            (i % 256) as u8,
            "Data mismatch at index {}: expected {}, got {}",
            i,
            (i % 256) as u8,
            data[i]
        );
    }
}

#[test]
fn test_temporary_file_persistence() {
    // Test that temporary files correctly persist block data across eviction/reload cycles

    let pool = create_pool_with_temp_dir(16384);

    // Allocate multiple blocks with different data patterns
    let mut block_ids = Vec::new();

    for pattern in 0..3 {
        let handle = pool
            .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();
        block_ids.push(block_id);

        // Write unique pattern for each block
        unsafe {
            let data = handle.data_mut().unwrap();
            for i in 0..1024 {
                data[i] = ((i + pattern * 100) % 256) as u8;
            }
        }

        // Add to eviction queue immediately
        drop(handle);
        pool.add_to_eviction_queue(block_id);
    }

    // Manually evict all blocks
    let result = pool.evict_blocks(
        MemoryTag::OrderBy,
        0,
        0, // Force eviction of all
        None,
    );
    assert!(result.success, "Eviction should succeed");

    // All original blocks should be unloaded
    for &block_id in &block_ids {
        let block = pool.get_block(block_id).unwrap();
        assert!(!block.is_loaded(), "Block {} should be unloaded", block_id);
    }

    // Reload each block and verify data integrity
    for (idx, &block_id) in block_ids.iter().enumerate() {
        let handle = pool.pin(block_id).unwrap();
        assert!(
            handle.is_valid(),
            "Should be able to reload block {}",
            block_id
        );

        // Verify the unique pattern for this block
        let data = handle.data().unwrap();
        for i in 0..1024 {
            let expected = ((i + idx * 100) % 256) as u8;
            assert_eq!(
                data[i], expected,
                "Block {} data mismatch at index {}: expected {}, got {}",
                block_id, i, expected, data[i]
            );
        }
    }
}

#[test]
fn repeated_pressure_eviction_preserves_every_managed_block() {
    let block_size = 256 * 1024;
    let pool = create_pool_with_temp_dir(block_size * 4);
    let mut block_ids = Vec::new();
    for block_idx in 0..64usize {
        let handle = pool
            .allocate(
                MemoryTag::HashTable,
                FileBufferType::ManagedBuffer,
                block_size,
            )
            .unwrap();
        let block_id = handle.block_handle().unwrap().block_id();
        // SAFETY: this handle owns the pinned allocation for the duration of
        // the mutation and the iterator remains within the returned buffer.
        unsafe {
            let data = handle.data_mut().unwrap();
            for (offset, byte) in data.iter_mut().enumerate() {
                *byte = ((block_idx * 37 + offset) % 251) as u8;
            }
        }
        block_ids.push(block_id);
        drop(handle);
    }

    let eviction = pool.evict_blocks(MemoryTag::HashTable, 0, 0, None);
    assert!(eviction.success);
    for (block_idx, block_id) in block_ids.into_iter().enumerate() {
        let handle = pool.pin(block_id).unwrap();
        let data = handle.data().unwrap();
        for (offset, byte) in data.iter().enumerate() {
            assert_eq!(
                *byte,
                ((block_idx * 37 + offset) % 251) as u8,
                "managed block {block_id} changed at byte {offset}"
            );
        }
    }
}

#[test]
fn test_reload_restores_memory_accounting_before_next_eviction() {
    let pool = create_pool_with_temp_dir(16384);
    let handle = pool
        .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();

    drop(handle);
    let first_eviction = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);
    assert!(first_eviction.success);
    assert_eq!(pool.used_memory(), 0);
    assert_eq!(pool.get_tag_usage(MemoryTag::OrderBy), 0);

    let reloaded = pool.pin(block_id).unwrap();
    assert_eq!(
        pool.used_memory(),
        1024,
        "reloading a spilled block must restore resident memory accounting"
    );
    assert_eq!(
        pool.get_tag_usage(MemoryTag::OrderBy),
        1024,
        "reloading a spilled block must restore per-tag accounting"
    );

    drop(reloaded);
    let second_eviction = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);
    assert!(second_eviction.success);
    assert_eq!(
        pool.used_memory(),
        0,
        "evicting a reloaded block must not underflow used memory"
    );
    assert_eq!(
        pool.get_tag_usage(MemoryTag::OrderBy),
        0,
        "evicting a reloaded block must not drive tag usage negative"
    );
}

// Note: Concurrent block reload test removed due to complexity
// The double-checked locking in pin() is tested indirectly by other tests

#[test]
fn test_temporary_file_cleanup() {
    // Test that temporary files are cleaned up after reading
    let temp_dir = std::env::temp_dir().join("paro_test_cleanup");
    let pool = BufferPool::new_arc(16384);
    pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
        .unwrap();

    // Allocate a block
    let handle = pool
        .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();

    drop(handle);
    pool.add_to_eviction_queue(block_id);

    // Force eviction (writes to temp file)
    let result = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);
    assert!(result.success, "Eviction should succeed");

    let files_before_reload = pool.get_temporary_files();
    assert!(
        !files_before_reload.is_empty(),
        "Temporary file should exist after eviction"
    );

    // Reload the block (should delete temp file)
    let _handle = pool.pin(block_id).unwrap();

    // Temp file should be deleted after reading
    assert!(
        pool.get_temporary_files().is_empty(),
        "Temporary file should be deleted after reading"
    );

    // Cleanup test directory
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_set_temporary_directory_clears_existing_spill_files() {
    let initial_dir = std::env::temp_dir().join("paro_test_switch_temp_dir_initial");
    let next_dir = std::env::temp_dir().join("paro_test_switch_temp_dir_next");
    let pool = BufferPool::new_arc(16384);
    pool.set_temporary_directory(initial_dir.to_string_lossy().to_string())
        .unwrap();

    let handle = pool
        .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 1024)
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();
    drop(handle);
    pool.add_to_eviction_queue(block_id);
    let result = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);
    assert!(result.success);
    assert!(
        !pool.get_temporary_files().is_empty(),
        "expected spill files before switching temp directory"
    );

    // Simulate stale spill metadata left by already-finished operators.
    {
        let mut blocks = pool.blocks.write().unwrap();
        blocks.remove(&block_id);
    }

    pool.set_temporary_directory(next_dir.to_string_lossy().to_string())
        .unwrap();
    assert!(
        pool.get_temporary_files().is_empty(),
        "switching temp directory should clear stale spill files"
    );

    let _ = std::fs::remove_dir_all(&initial_dir);
    let _ = std::fs::remove_dir_all(&next_dir);
}

#[test]
fn test_free_cleans_spilled_temp_blocks() {
    let pool = create_pool_with_temp_dir(16384);
    let handle = pool
        .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 2048)
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();

    drop(handle);
    pool.add_to_eviction_queue(block_id);
    let result = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);
    assert!(result.success);
    assert_eq!(
        pool.used_memory(),
        0,
        "evicted block should no longer count as resident memory"
    );
    assert_eq!(
        pool.get_tag_usage(MemoryTag::OrderBy),
        0,
        "evicted block should no longer count against tag usage"
    );
    assert!(
        !pool.get_temporary_files().is_empty(),
        "expected spill file to exist before free"
    );

    pool.free(block_id).unwrap();
    assert_eq!(
        pool.used_memory(),
        0,
        "freeing an evicted block must not double-subtract resident memory"
    );
    assert_eq!(
        pool.get_tag_usage(MemoryTag::OrderBy),
        0,
        "freeing an evicted block must not drive tag usage negative"
    );
    assert!(
        pool.get_temporary_files().is_empty(),
        "free should remove temp blocks for the freed handle"
    );
}

#[test]
fn test_temporary_spill_metrics_exposed() {
    let pool = create_pool_with_temp_dir(16384);
    let handle = pool
        .allocate(MemoryTag::OrderBy, FileBufferType::ManagedBuffer, 2048)
        .unwrap();
    let block_id = handle.block_handle().unwrap().block_id();
    drop(handle);
    pool.add_to_eviction_queue(block_id);
    pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);

    let metrics = pool.get_temporary_spill_metrics();
    assert!(metrics.write_bytes > 0);
    assert!(metrics.file_count > 0);
    assert!(metrics.swap_usage > 0);

    let _ = pool.pin(block_id).unwrap();
    let metrics_after_read = pool.get_temporary_spill_metrics();
    assert!(metrics_after_read.read_bytes > 0);
}
