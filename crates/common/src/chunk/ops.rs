// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::Chunk;
use crate::allocator::Allocator;
use crate::error::Result;
use crate::types::LogicalType;
use crate::vector::{SelectionVector, Vector, VectorSelection, VectorType};
use std::sync::Arc;

impl Chunk {
    fn vector_has_writable_capacity(vector: &Vector, required_capacity: usize) -> bool {
        if vector.vector_type() != VectorType::Flat || vector.capacity() < required_capacity {
            return false;
        }

        match vector.logical_type() {
            LogicalType::Array(_, array_size) => {
                let Some(child_capacity) = required_capacity.checked_mul(*array_size) else {
                    return false;
                };
                vector
                    .child()
                    .map(|child| Self::vector_has_writable_capacity(child, child_capacity))
                    .unwrap_or(false)
            }
            LogicalType::Struct(_) => vector
                .children()
                .map(|children| {
                    children
                        .iter()
                        .all(|child| Self::vector_has_writable_capacity(child, required_capacity))
                })
                .unwrap_or(false),
            _ => true,
        }
    }

    fn has_writable_capacity(&self, required_capacity: usize) -> bool {
        self.capacity >= required_capacity
            && self
                .data
                .iter()
                .all(|vector| Self::vector_has_writable_capacity(vector, required_capacity))
    }

    fn try_copy_column_ranges(
        source: &Self,
        target: &mut Self,
        source_offset: usize,
        target_offset: usize,
        count: usize,
    ) -> Result<()> {
        debug_assert_eq!(
            source.column_count(),
            target.column_count(),
            "Column count mismatch"
        );
        if count == 0 {
            return Ok(());
        }

        for (src_vec, dest_vec) in source.data.iter().zip(target.data.iter_mut()) {
            Vector::try_make_arc_mut(dest_vec)?.try_copy_range(
                target_offset,
                src_vec,
                source_offset,
                count,
            )?;
        }

        Ok(())
    }

    fn try_ensure_capacity(&mut self, required_capacity: usize) -> Result<()> {
        if self.has_writable_capacity(required_capacity) {
            return Ok(());
        }

        let mut new_capacity = self.capacity.max(1);
        while new_capacity < required_capacity {
            new_capacity = new_capacity.saturating_mul(2).max(required_capacity);
        }

        let types = self.types();
        let mut resized = Chunk::try_initialize(&types, new_capacity, self.allocator.clone())?;
        Self::try_copy_column_ranges(self, &mut resized, 0, 0, self.count)?;
        resized.try_set_cardinality(self.count)?;

        self.data = resized.data;
        self.capacity = resized.capacity;
        self.initial_capacity = resized.initial_capacity;
        self.reset_state = resized.reset_state;
        Ok(())
    }

    fn try_reserve_for_append(&mut self, new_size: usize) -> Result<()> {
        self.try_ensure_capacity(new_size)
    }

    pub fn try_reset(&mut self, allocator: Arc<dyn Allocator>) -> Result<()> {
        if let Some(reset_state) = &mut self.reset_state {
            debug_assert_eq!(
                reset_state.columns.len(),
                self.data.len(),
                "Reset state column count mismatch"
            );
            self.count = 0;
            self.capacity = self.initial_capacity;
            self.allocator = reset_state.allocator.clone();

            for (col, state) in self.data.iter_mut().zip(reset_state.columns.iter_mut()) {
                if let Some(vector) = Arc::get_mut(col) {
                    state.try_reset_unique(vector)?;
                } else {
                    *col = Arc::new(state.try_reset_shared()?);
                }
            }
            return Ok(());
        }

        self.count = 0;
        self.allocator = allocator.clone();
        for col in &mut self.data {
            let logical_type = col.logical_type().clone();
            *col = Arc::new(Vector::try_new(
                logical_type,
                self.capacity,
                allocator.clone(),
            )?);
        }
        Ok(())
    }

    pub fn destroy(&mut self) {
        self.data.clear();
        self.count = 0;
        self.capacity = 0;
        self.initial_capacity = 0;
        self.reset_state = None;
    }

    pub fn try_flatten(&mut self) -> Result<()> {
        for col in &mut self.data {
            Vector::try_make_arc_mut(col)?.try_flatten()?;
        }
        Ok(())
    }

    pub fn all_constant(&self) -> bool {
        self.data
            .iter()
            .all(|v| v.vector_type() == VectorType::Constant)
    }

    pub fn reference(&mut self, other: &Self) {
        debug_assert!(
            other.column_count() <= self.column_count(),
            "Cannot reference chunk with more columns"
        );
        self.capacity = other.capacity;
        self.count = other.count;
        self.allocator = other.allocator.clone();

        // Zero-copy sharing of columns via Arc
        for (i, col) in other.data.iter().enumerate() {
            if i < self.data.len() {
                self.data[i] = Arc::clone(col);
            }
        }
    }

    pub fn clone_referencing_vectors(&self) -> Self {
        Self {
            data: self.data.iter().map(Arc::clone).collect(),
            count: self.count,
            capacity: self.capacity,
            initial_capacity: self.initial_capacity,
            reset_state: None,
            allocator: self.allocator.clone(),
        }
    }

    pub fn handoff_referencing_vectors(&mut self) -> Self {
        let handed_off = self.clone_referencing_vectors();
        self.clear_rows_preserve_storage();
        handed_off
    }

    pub fn clear_rows_preserve_storage(&mut self) {
        self.count = 0;
    }

    pub fn reference_columns(&mut self, other: &Self, column_ids: &[usize]) {
        debug_assert_eq!(
            self.column_count(),
            column_ids.len(),
            "Column count mismatch in reference_columns"
        );

        self.capacity = other.capacity;
        self.count = other.count;
        self.allocator = other.allocator.clone();

        for (target_idx, &source_idx) in column_ids.iter().enumerate() {
            let source_col = other
                .column(source_idx)
                .expect("Source column out of bounds in reference_columns");
            debug_assert_eq!(
                self.data[target_idx].logical_type(),
                source_col.logical_type(),
                "Type mismatch in reference_columns"
            );
            self.data[target_idx] = Arc::clone(source_col);
        }
    }

    /// Replace this chunk with a zero-copy selected view of another chunk.
    ///
    /// Unlike [`Self::try_slice`], this does not first install the source
    /// columns in `self`. View-producing operators can therefore reuse their
    /// output slot without materializing temporary flat vectors between
    /// batches. An empty `column_ids` slice selects every source column.
    pub fn try_reference_selection(
        &mut self,
        other: &Self,
        column_ids: &[usize],
        selection: VectorSelection,
    ) -> Result<()> {
        let count = selection.len();
        debug_assert!(
            (0..count).all(|idx| selection.physical_index(idx) < other.count),
            "selected chunk view row is out of bounds"
        );
        let output_len = if column_ids.is_empty() {
            other.column_count()
        } else {
            column_ids.len()
        };

        for output_idx in 0..output_len {
            let source_idx = if column_ids.is_empty() {
                output_idx
            } else {
                column_ids[output_idx]
            };
            let source = other.data.get(source_idx).ok_or_else(|| {
                crate::error::internal(format!(
                    "selected chunk view column {source_idx} is out of bounds for {} columns",
                    other.column_count()
                ))
            })?;
            let selected = Arc::new(Vector::try_gather_ref(
                Arc::clone(source),
                selection.clone(),
            )?);
            if output_idx < self.data.len() {
                self.data[output_idx] = selected;
            } else {
                self.data.push(selected);
            }
        }
        self.data.truncate(output_len);
        self.count = count;
        self.capacity = other.capacity.max(count);
        self.initial_capacity = self.capacity;
        self.reset_state = None;
        self.allocator = other.allocator.clone();
        Ok(())
    }

    pub fn move_from(&mut self, other: &mut Self) {
        self.count = other.count;
        self.capacity = other.capacity;
        self.initial_capacity = other.initial_capacity;
        self.allocator = other.allocator.clone();
        self.reset_state = other.reset_state.take();
        self.data = std::mem::take(&mut other.data);
        other.destroy();
    }

    /// Move all owned buffers out of this chunk without allocating a replacement chunk.
    ///
    /// This is for sinks that unconditionally take ownership of their input. Pending paths that
    /// may need to restore the chunk should keep using `ChunkLease::take_from_scratch`.
    pub fn take_owned(&mut self) -> Self {
        let allocator = self.allocator.clone();
        let taken = Self {
            data: std::mem::take(&mut self.data),
            count: self.count,
            capacity: self.capacity,
            initial_capacity: self.initial_capacity,
            reset_state: self.reset_state.take(),
            allocator: allocator.clone(),
        };
        self.count = 0;
        self.capacity = 0;
        self.initial_capacity = 0;
        self.allocator = allocator;
        taken
    }

    pub fn try_copy_to(&self, other: &mut Self, offset: usize) -> Result<()> {
        debug_assert_eq!(
            self.column_count(),
            other.column_count(),
            "Column count mismatch"
        );
        debug_assert!(other.count == 0, "Target chunk must be empty");

        let copy_count = self.count.saturating_sub(offset);
        other.try_ensure_capacity(copy_count)?;

        if copy_count == 0 {
            other.try_set_cardinality(0)?;
            return Ok(());
        }

        Self::try_copy_column_ranges(self, other, offset, 0, copy_count)?;
        other.try_set_cardinality(copy_count)?;
        Ok(())
    }

    pub fn try_append(&mut self, other: &Self) -> Result<()> {
        if other.is_empty() {
            return Ok(());
        }
        debug_assert_eq!(
            self.column_count(),
            other.column_count(),
            "Column count mismatch"
        );

        let old_count = self.count;
        let new_size = self.count.checked_add(other.count).ok_or_else(|| {
            crate::error::internal(format!(
                "chunk append row count overflow: left={}, right={}",
                self.count, other.count
            ))
        })?;
        self.try_reserve_for_append(new_size)?;
        Self::try_copy_column_ranges(other, self, 0, old_count, other.count)?;
        self.try_set_cardinality(new_size)?;
        Ok(())
    }

    pub fn try_slice(&mut self, sel: &SelectionVector, count: usize) -> Result<()> {
        debug_assert!(count <= sel.len(), "Slice count exceeds selection length");

        let base_sel = if count == sel.len() {
            sel.clone()
        } else {
            SelectionVector::try_from_indices(
                sel.as_slice()[..count].to_vec(),
                self.allocator.clone(),
            )?
        };

        for col in &mut self.data {
            *col = Arc::new(Vector::try_gather_ref(Arc::clone(col), base_sel.clone())?);
        }

        self.try_set_cardinality(count)?;
        Ok(())
    }

    pub fn try_slice_range(&mut self, offset: usize, slice_count: usize) -> Result<()> {
        debug_assert!(offset + slice_count <= self.count, "Slice out of bounds");
        for col in &mut self.data {
            *col = Arc::new(Vector::try_gather_ref(
                Arc::clone(col),
                VectorSelection::Range {
                    offset,
                    count: slice_count,
                },
            )?);
        }

        self.try_set_cardinality(slice_count)?;
        Ok(())
    }

    pub fn split(&mut self, other: &mut Self, split_idx: usize) {
        debug_assert!(other.is_empty(), "Target chunk must be empty");
        debug_assert!(split_idx < self.data.len(), "Split index out of bounds");

        let remaining: Vec<_> = self.data.drain(split_idx..).collect();
        other.data = remaining;
        other.count = self.count;
        other.capacity = self.capacity;
        other.initial_capacity = self.initial_capacity;
        other.allocator = self.allocator.clone();
        other.reset_state = self.reset_state.as_mut().map(|reset_state| {
            let columns = reset_state.columns.drain(split_idx..).collect();
            super::chunk::ChunkResetState {
                allocator: reset_state.allocator.clone(),
                columns,
            }
        });
    }

    pub fn fuse(&mut self, other: &mut Self) {
        debug_assert_eq!(self.count, other.count, "Row count mismatch");
        self.data.append(&mut other.data);

        self.reset_state = match (self.reset_state.take(), other.reset_state.take()) {
            (Some(mut left), Some(mut right))
                if self.initial_capacity == other.initial_capacity
                    && Arc::ptr_eq(&left.allocator, &right.allocator) =>
            {
                left.columns.append(&mut right.columns);
                Some(left)
            }
            _ => None,
        };
        if self.reset_state.is_none() {
            self.initial_capacity = 0;
        }

        other.destroy();
    }

    /// Deep copy this chunk into a new chunk using the provided allocator.
    ///
    /// This materializes all vectors and string heaps into the target allocator,
    /// ensuring the new chunk owns its buffers independently.
    pub fn try_deep_copy(&self, allocator: Arc<dyn Allocator>) -> Result<Self> {
        if self.is_empty() {
            return Chunk::try_initialize(&self.types(), 0, allocator);
        }

        let types = self.types();
        let mut result = Chunk::try_initialize(&types, self.count, allocator)?;
        Self::try_copy_column_ranges(self, &mut result, 0, 0, self.count)?;
        result.try_set_cardinality(self.count)?;

        Ok(result)
    }
}
