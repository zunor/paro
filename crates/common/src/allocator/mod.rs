// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Allocator module - unified memory allocation interface.
//!
//! Production code should use `BufferAllocator` via the execution or session
//! context. `DefaultAllocator` is intended for tests and simple utility code,
//! while `ArenaAllocator` is useful for temporary batch allocations.

mod allocated_data;
#[allow(clippy::module_inception)]
mod allocator;
mod arena_allocator;
mod buffer_allocator;
#[cfg(debug_assertions)]
mod debug_info;
mod default_allocator;

pub use allocated_data::AllocatedData;
pub use allocator::Allocator;
pub use arena_allocator::ArenaAllocator;
pub use buffer_allocator::{
    allocator_lock_count, reset_allocator_lock_count, BufferAllocator, BufferManager, MemoryTag,
    MemoryUsage, MemoryUsageSnapshot, MEMORY_TAG_COUNT,
};
pub use default_allocator::DefaultAllocator;

/// Create a default allocator.
///
/// # ⚠️ For Tests Only
///
/// This function returns a fresh `DefaultAllocator` which does NOT integrate with
/// the BufferPool. Production code should use `BufferAllocator` instead:
///
/// ```ignore
/// // In execution context:
/// let allocator = ctx.allocator(MemoryTag::Operator);
///
/// // Or via Session:
/// let allocator = session.allocator(MemoryTag::Operator);
/// ```
///
/// Using `DefaultAllocator` in production means:
/// - Memory usage is not tracked by BufferPool
/// - Memory limits are not enforced
/// - No spill-to-disk support
pub fn default_allocator() -> DefaultAllocator {
    DefaultAllocator::new()
}
