// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{AllocationSet, VectorBuffer};
use crate::allocator::Allocator;
use std::sync::Arc;

/// Selection vector - maps logical indices to physical indices.
///
/// Uses `VectorBuffer` for memory management and supports shallow clones (Zero-copy).
#[derive(Debug, Clone)]
pub struct SelectionVector {
    /// Buffer holding u32 indices
    buffer: VectorBuffer,
    /// Number of indices
    count: usize,
}

impl SelectionVector {
    /// Create a deep copy of this selection vector.
    pub fn deep_copy(&self) -> Self {
        Self {
            buffer: self.buffer.deep_copy(),
            count: self.count,
        }
    }

    /// Ensure the selection vector is exclusively owned (Copy-on-Write).
    pub fn make_exclusive(&mut self) {
        self.buffer.make_exclusive();
    }
    /// Create a new selection vector with capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: VectorBuffer::new(std::mem::size_of::<u32>(), capacity),
            count: 0,
        }
    }

    /// Create with custom allocator.
    pub fn with_allocator(capacity: usize, allocator: Arc<dyn Allocator>) -> Self {
        Self {
            buffer: VectorBuffer::with_allocator(std::mem::size_of::<u32>(), capacity, allocator),
            count: 0,
        }
    }

    /// Create incremental selection (0, 1, 2, ...).
    pub fn incremental(count: usize) -> Self {
        let mut sv = Self::with_capacity(count);
        sv.count = count;

        // SAFETY: We allocated space for `count` u32s.
        unsafe {
            let ptr = sv.buffer.data() as *mut u32;
            for i in 0..count {
                *ptr.add(i) = i as u32;
            }
        }
        sv
    }

    /// Create constant selection (all zeros) for constant vectors.
    /// All indices point to position 0.
    pub fn constant(count: usize) -> Self {
        let mut sv = Self::with_capacity(count);
        sv.count = count;

        // SAFETY: We allocated space for `count` u32s.
        // All values are 0, which is the default for zeroed memory,
        // but we explicitly set them for clarity.
        unsafe {
            let ptr = sv.buffer.data() as *mut u32;
            for i in 0..count {
                *ptr.add(i) = 0;
            }
        }
        sv
    }

    /// Create from indices.
    pub fn from_indices(indices: Vec<u32>) -> Self {
        let count = indices.len();
        let mut sv = Self::with_capacity(count);
        sv.count = count;

        if count > 0 {
            // SAFETY: We allocated space for `count` u32s.
            unsafe {
                let dst = sv.buffer.data() as *mut u32;
                std::ptr::copy_nonoverlapping(indices.as_ptr(), dst, count);
            }
        }
        sv
    }

    /// Get index at position.
    #[inline]
    pub fn get(&self, idx: usize) -> usize {
        debug_assert!(idx < self.count, "Index out of bounds");
        // SAFETY: Bounds checked in debug mode.
        unsafe {
            let ptr = self.buffer.data() as *const u32;
            (*ptr.add(idx)) as usize
        }
    }

    /// Set index at position.
    #[inline]
    pub fn set(&mut self, idx: usize, value: usize) {
        debug_assert!(idx < self.count, "Index out of bounds");
        self.make_exclusive();
        // SAFETY: Bounds checked in debug mode.
        unsafe {
            let ptr = self.buffer.data() as *mut u32;
            *ptr.add(idx) = value as u32;
        }
    }

    /// Length of selection vector.
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn allocation_identity(&self) -> Option<usize> {
        self.buffer.allocation_identity()
    }

    pub fn allocation_size(&self) -> usize {
        self.buffer.size()
    }

    pub(crate) fn collect_allocation_size(&self, allocations: &mut AllocationSet) -> usize {
        self.buffer.collect_allocation_size(allocations)
    }

    /// Set the length of the selection vector.
    pub fn set_len(&mut self, count: usize) {
        self.count = count;
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get raw slice of indices.
    pub fn as_slice(&self) -> &[u32] {
        // SAFETY: buffer contains valid u32 data up to self.count
        unsafe { std::slice::from_raw_parts(self.buffer.data() as *const u32, self.count) }
    }

    /// Merge this selection vector with another one.
    /// Resulting selection maps 0..other.count -> physical indices.
    /// new_indices[i] = self.get(other.get(i))
    pub fn slice(&self, other: &SelectionVector, count: usize) -> SelectionVector {
        let mut result = SelectionVector::with_allocator(count, self.buffer.allocator().clone());
        result.count = count;

        unsafe {
            let self_ptr = self.buffer.data() as *const u32;
            let other_ptr = other.buffer.data() as *const u32;
            let res_ptr = result.buffer.data() as *mut u32;

            for i in 0..count {
                let idx_in_self = *other_ptr.add(i) as usize;
                *res_ptr.add(i) = *self_ptr.add(idx_in_self);
            }
        }
        result
    }
}

impl From<Vec<u32>> for SelectionVector {
    fn from(indices: Vec<u32>) -> Self {
        Self::from_indices(indices)
    }
}

impl From<&SelectionVector> for SelectionVector {
    fn from(sel: &SelectionVector) -> Self {
        sel.clone()
    }
}
