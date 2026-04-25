// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Chunk - a collection of vectors representing a batch of rows.
//!
//! Chunk is the unit of data processing in Paro's execution engine.
//! It contains multiple vectors (columns) that share the same cardinality.

use std::sync::Arc;

use crate::allocator::Allocator;
use crate::error::{self as paro_error, Result};
use crate::memory::AllocationId;
use crate::runtime_value::Value;
use crate::types::LogicalType;
use crate::vector::{AllocationSet, Vector, VectorResetState, VECTOR_SIZE};

pub(super) struct ChunkResetState {
    pub(super) allocator: Arc<dyn Allocator>,
    pub(super) columns: Vec<VectorResetState>,
}

impl Clone for ChunkResetState {
    fn clone(&self) -> Self {
        Self {
            allocator: self.allocator.clone(),
            columns: self.columns.clone(),
        }
    }
}

/// A collection of vectors representing a batch of rows.
///
/// Chunk is the intermediate representation used by the execution engine.
/// It holds a set of vectors that all have the same length (cardinality).
#[derive(Clone)]
pub struct Chunk {
    /// Column vectors.
    /// Using Arc<Vector> for handle sharing (Zero-copy)
    pub data: Vec<Arc<Vector>>,
    /// Number of valid rows
    pub(super) count: usize,
    /// Maximum capacity
    pub(super) capacity: usize,
    /// Capacity reserved by initialize-time reset state
    pub(super) initial_capacity: usize,
    /// Optional reusable reset metadata
    pub(super) reset_state: Option<ChunkResetState>,
    /// Allocator for this chunk
    pub(super) allocator: Arc<dyn Allocator>,
}

impl std::fmt::Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Chunk")
            .field("data", &self.data)
            .field("count", &self.count)
            .field("capacity", &self.capacity)
            .field("initial_capacity", &self.initial_capacity)
            .field("has_reset_state", &self.reset_state.is_some())
            .field("allocator", &self.allocator.name())
            .finish()
    }
}

impl Chunk {
    fn try_build_reset_state(
        types: &[LogicalType],
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<ChunkResetState> {
        Ok(ChunkResetState {
            allocator: allocator.clone(),
            columns: types
                .iter()
                .map(|t| VectorResetState::try_new(t.clone(), capacity, allocator.clone()))
                .collect::<Result<Vec<_>>>()?,
        })
    }

    /// Create an empty Chunk with the given allocator.
    pub fn try_new(allocator: Arc<dyn Allocator>) -> Result<Self> {
        Ok(Self {
            data: Vec::new(),
            count: 0,
            capacity: VECTOR_SIZE,
            initial_capacity: VECTOR_SIZE,
            reset_state: None,
            allocator,
        })
    }

    /// Initialize an empty Chunk with the given types and allocator (no data allocation).
    pub fn try_init_empty(types: &[LogicalType], allocator: Arc<dyn Allocator>) -> Result<Self> {
        let data = types
            .iter()
            .map(|t| Ok(Arc::new(Vector::try_new(t.clone(), 0, allocator.clone())?)))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            data,
            count: 0,
            capacity: VECTOR_SIZE,
            initial_capacity: 0,
            reset_state: None,
            allocator,
        })
    }

    /// Initialize a Chunk with the given types, capacity, and allocator.
    pub fn try_initialize(
        types: &[LogicalType],
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let data = types
            .iter()
            .map(|t| {
                Ok(Arc::new(Vector::try_new(
                    t.clone(),
                    capacity,
                    allocator.clone(),
                )?))
            })
            .collect::<Result<Vec<_>>>()?;
        let reset_state = Some(Self::try_build_reset_state(
            types,
            capacity,
            allocator.clone(),
        )?);

        Ok(Self {
            data,
            count: 0,
            capacity,
            initial_capacity: capacity,
            reset_state,
            allocator,
        })
    }

    /// Create a Chunk from existing vectors.
    pub fn from_vectors(vectors: Vec<Vector>, allocator: Arc<dyn Allocator>) -> Self {
        let arc_vectors = vectors.into_iter().map(Arc::new).collect();
        Self::from_arc_vectors(arc_vectors, allocator)
    }

    /// Create a Chunk from existing Arc<Vector>s.
    /// All vectors must have the same length.
    pub fn from_arc_vectors(vectors: Vec<Arc<Vector>>, allocator: Arc<dyn Allocator>) -> Self {
        let count = vectors.first().map(|v| v.len()).unwrap_or(0);
        let capacity = count.max(VECTOR_SIZE);

        debug_assert!(
            vectors.iter().all(|v| v.len() == count),
            "All vectors must have the same length"
        );
        Self {
            data: vectors,
            count,
            capacity,
            initial_capacity: capacity,
            reset_state: None,
            allocator,
        }
    }

    // ========== Size and Capacity ==========

    /// Get the number of rows (cardinality).
    #[inline]
    pub fn size(&self) -> usize {
        self.count
    }

    /// Alias for `size()`.
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Check if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get the number of columns.
    #[inline]
    pub fn column_count(&self) -> usize {
        self.data.len()
    }

    /// Get the capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Set the cardinality (row count).
    /// Panics if count exceeds capacity.
    ///
    /// This also updates each Vector's count and validity mask to ensure
    /// consistency between the Chunk and its vectors.
    /// Shared columns that are already aligned are left untouched to avoid
    /// unnecessary CoW on hot reset/output paths.
    #[inline]
    pub fn set_cardinality(&mut self, count: usize) {
        self.try_set_cardinality(count)
            .expect("chunk cardinality allocation failed");
    }

    /// Set the cardinality (row count).
    ///
    /// This also updates each Vector's count and validity mask to ensure
    /// consistency between the Chunk and its vectors.
    #[inline]
    pub fn try_set_cardinality(&mut self, count: usize) -> Result<()> {
        if count > self.capacity {
            return Err(paro_error::internal(format!(
                "chunk cardinality exceeds capacity: count={count}, capacity={}",
                self.capacity
            )));
        }
        if self.count == count {
            return Ok(());
        }

        // Update each vector's count to match the chunk's cardinality.
        // This ensures validity mask capacity is properly set for all vectors.
        for vec in &mut self.data {
            if vec.count_matches_cardinality(count) {
                continue;
            }

            Vector::try_make_arc_mut(vec)?.try_set_count(count)?;
        }

        self.count = count;
        Ok(())
    }

    /// Set capacity.
    #[inline]
    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
    }

    // ========== Column Access ==========

    /// Get a reference to a column Arc by index.
    #[inline]
    pub fn column(&self, idx: usize) -> Option<&Arc<Vector>> {
        self.data.get(idx)
    }

    /// Get a mutable reference to a column by index.
    /// Triggers CoW on the Arc<Vector> handle if shared.
    #[inline]
    pub fn column_mut(&mut self, idx: usize) -> Option<&mut Vector> {
        self.data.get_mut(idx).map(Arc::make_mut)
    }

    /// Get a value from a column/row pair with bounds checks.
    pub fn get_value(&self, col_idx: usize, row_idx: usize) -> Option<Value> {
        if row_idx >= self.count {
            return None;
        }
        self.column(col_idx).map(|column| column.get_value(row_idx))
    }

    /// Set a value at a column/row pair with bounds checks.
    pub fn set_value(&mut self, col_idx: usize, row_idx: usize, val: &Value) -> Option<()> {
        if row_idx >= self.count {
            return None;
        }
        self.column_mut(col_idx)
            .map(|column| column.set_value(row_idx, val))
    }

    /// Get column types.
    pub fn types(&self) -> Vec<LogicalType> {
        self.data.iter().map(|v| v.logical_type().clone()).collect()
    }

    /// Get the allocator for this chunk.
    #[inline]
    pub fn allocator(&self) -> &Arc<dyn Allocator> {
        &self.allocator
    }

    pub fn collect_allocation_size(&self, allocations: &mut AllocationSet) -> usize {
        self.data
            .iter()
            .map(|column| column.collect_allocation_size(allocations))
            .sum()
    }

    pub fn collect_allocation_entries(&self, entries: &mut Vec<(AllocationId, usize)>) {
        for column in &self.data {
            column.collect_allocation_entries(entries);
        }
    }

    pub fn get_allocation_size(&self) -> usize {
        let mut allocations = AllocationSet::new();
        self.collect_allocation_size(&mut allocations)
    }

    pub fn verify(&self) {
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                self.count <= self.capacity,
                "Chunk count {} exceeds capacity {}",
                self.count,
                self.capacity
            );

            if let Some(reset_state) = &self.reset_state {
                debug_assert!(
                    Arc::ptr_eq(&self.allocator, &reset_state.allocator),
                    "Reset state allocator mismatch"
                );
                debug_assert_eq!(
                    reset_state.columns.len(),
                    self.data.len(),
                    "Reset state column count mismatch"
                );
                for (column, state) in self.data.iter().zip(reset_state.columns.iter()) {
                    debug_assert_eq!(
                        column.logical_type(),
                        state.logical_type(),
                        "Reset state type mismatch"
                    );
                }
            }

            for column in &self.data {
                column.verify(self.count);
            }
        }
    }

    // ========== Modification ==========

    // reset, destroy, flatten, all_constant, reference, move_from, copy_to, append, slice, slice_range, split, fuse
    // are now implemented in chunk_ops.rs
}

// ========== Display ==========

/// Convert to string representation for debugging.
impl std::fmt::Display for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Chunk - [{} columns, {} rows]",
            self.column_count(),
            self.size()
        )?;

        for i in 0..self.column_count() {
            writeln!(f, "- Column {}: {}", i, self.data[i].logical_type())?;
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Default for Chunk {
    fn default() -> Self {
        Self::try_new(Arc::new(crate::allocator::default_allocator()))
            .expect("test chunk allocation failed")
    }
}
