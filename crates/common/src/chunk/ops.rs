use super::Chunk;
use crate::allocator::Allocator;
use crate::types::LogicalType;
use crate::vector::{SelectionVector, Vector, VectorType};
use std::sync::Arc;

impl Chunk {
    fn vector_has_writable_capacity(vector: &Vector, required_capacity: usize) -> bool {
        if vector.vector_type() != VectorType::Flat || vector.capacity() < required_capacity {
            return false;
        }

        match vector.logical_type() {
            LogicalType::Array(_, array_size) => vector
                .child()
                .map(|child| {
                    Self::vector_has_writable_capacity(child, required_capacity * array_size)
                })
                .unwrap_or(false),
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

    fn copy_rows(
        source: &Self,
        target: &mut Self,
        source_offset: usize,
        target_offset: usize,
        count: usize,
    ) {
        debug_assert_eq!(
            source.column_count(),
            target.column_count(),
            "Column count mismatch"
        );

        for (col_idx, src_vec) in source.data.iter().enumerate() {
            let dest_vec = target
                .column_mut(col_idx)
                .expect("destination column missing");
            for row in 0..count {
                dest_vec.copy_at(target_offset + row, src_vec, source_offset + row);
            }
        }
    }

    fn ensure_capacity(&mut self, required_capacity: usize) {
        if self.has_writable_capacity(required_capacity) {
            return;
        }

        let mut new_capacity = self.capacity.max(1);
        while new_capacity < required_capacity {
            new_capacity = new_capacity.saturating_mul(2).max(required_capacity);
        }

        let types = self.types();
        let mut resized =
            Chunk::initialize_with_allocator(&types, new_capacity, self.allocator.clone());
        resized.set_cardinality(self.count);
        Self::copy_rows(self, &mut resized, 0, 0, self.count);

        self.data = resized.data;
        self.capacity = new_capacity;
    }

    fn reserve_for_append(&mut self, new_size: usize) {
        self.ensure_capacity(new_size);
    }

    pub fn reset(&mut self) {
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
                    state.reset_unique(vector);
                } else {
                    *col = Arc::new(state.reset_shared());
                }
            }
            return;
        }

        self.count = 0;
        for col in &mut self.data {
            let logical_type = col.logical_type().clone();
            *col = Arc::new(Vector::with_capacity_and_allocator(
                logical_type,
                self.capacity,
                self.allocator.clone(),
            ));
        }
    }

    pub fn destroy(&mut self) {
        self.data.clear();
        self.count = 0;
        self.capacity = 0;
        self.initial_capacity = 0;
        self.reset_state = None;
    }

    pub fn flatten(&mut self) {
        for col in &mut self.data {
            Arc::make_mut(col).flatten();
        }
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

    pub fn move_from(&mut self, other: &mut Self) {
        self.count = other.count;
        self.capacity = other.capacity;
        self.initial_capacity = other.initial_capacity;
        self.allocator = other.allocator.clone();
        self.reset_state = other.reset_state.take();
        self.data = std::mem::take(&mut other.data);
        other.destroy();
    }

    pub fn copy_to(&self, other: &mut Self, offset: usize) {
        debug_assert_eq!(
            self.column_count(),
            other.column_count(),
            "Column count mismatch"
        );
        debug_assert!(other.count == 0, "Target chunk must be empty");

        let copy_count = self.count.saturating_sub(offset);
        other.ensure_capacity(copy_count);
        other.set_cardinality(copy_count);

        if copy_count == 0 {
            return;
        }

        Self::copy_rows(self, other, offset, 0, copy_count);
    }

    pub fn append(&mut self, other: &Self) {
        if other.is_empty() {
            return;
        }
        debug_assert_eq!(
            self.column_count(),
            other.column_count(),
            "Column count mismatch"
        );

        let old_count = self.count;
        let new_size = self.count + other.count;
        self.reserve_for_append(new_size);
        self.set_cardinality(new_size);
        Self::copy_rows(other, self, 0, old_count, other.count);
    }

    pub fn slice(&mut self, sel: &SelectionVector, count: usize) {
        debug_assert!(count <= sel.len(), "Slice count exceeds selection length");

        let base_sel = if count == sel.len() {
            sel.clone()
        } else {
            SelectionVector::from_indices(sel.as_slice()[..count].to_vec())
        };

        for col in &mut self.data {
            *col = Arc::new(Vector::dictionary(Arc::clone(col), base_sel.clone()));
        }

        self.set_cardinality(count);
    }

    pub fn slice_range(&mut self, offset: usize, slice_count: usize) {
        debug_assert!(offset + slice_count <= self.count, "Slice out of bounds");
        let sel = SelectionVector::from_indices(
            (offset..(offset + slice_count)).map(|i| i as u32).collect(),
        );
        self.slice(&sel, slice_count);
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
    pub fn deep_copy_with_allocator(&self, allocator: Arc<dyn Allocator>) -> Self {
        if self.is_empty() {
            return Chunk::initialize_with_allocator(&self.types(), 0, allocator);
        }

        let types = self.types();
        let mut result = Chunk::initialize_with_allocator(&types, self.count, allocator);
        result.set_cardinality(self.count);

        for (col_idx, src_vec) in self.data.iter().enumerate() {
            let dest_vec = result
                .column_mut(col_idx)
                .expect("destination column missing in deep copy");
            for row in 0..self.count {
                dest_vec.copy_at(row, src_vec, row);
            }
        }

        result
    }
}
