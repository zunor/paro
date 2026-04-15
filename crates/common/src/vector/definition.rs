use std::sync::Arc;

use crate::allocator::{default_allocator, Allocator};
use crate::types::{InlineString, LogicalType};

use super::{
    AllocationSet, SelectionVector, StringHeap, ValidityMask, VectorBuffer, VectorType, VECTOR_SIZE,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictionarySource {
    Storage,
    GenericSelection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictionaryInfo {
    pub unique_len: usize,
    pub provenance_id: Option<u64>,
    pub source: DictionarySource,
}

/// A columnar vector handle.
///
/// Refactored to be a lightweight handle pointing to shared data (Zero-copy).
/// Cloning a Vector instance creates a new reference to the same underlying data.
/// For mutation, call `make_exclusive()` to ensure Copy-on-Write semantics.
#[derive(Debug, Clone)]
pub struct Vector {
    /// How the data is stored
    pub(super) vector_type: VectorType,
    /// Logical SQL type
    pub(super) logical_type: LogicalType,
    /// Main data buffer:
    /// - FLAT (Primitive): Raw bytes of i32/f64/etc.
    /// - FLAT (Array): Unused (data in child) — fixed-size, no offset needed
    /// - FLAT (List): Offset array [0, offset1, offset2, ..., end] to index child
    ///   (e.g., [[1,2], [3], [4,5,6]] → offsets=[0,2,3,6], child=[1,2,3,4,5,6])
    /// - DICTIONARY: Unused (indices in sel_vector)
    /// - SEQUENCE: [start, increment]
    /// - CONSTANT: Single value
    pub(super) buffer: VectorBuffer,
    /// Validity mask for nulls
    pub(super) validity: ValidityMask,
    /// Number of logical elements
    pub(super) count: usize,
    /// For DICTIONARY: selection indices
    /// For SEQUENCE: unused
    pub(super) sel_vector: Option<SelectionVector>,
    /// Multi-purpose shared child vector:
    /// - DICTIONARY: Shared reference to dictionary values (zero-copy!)
    /// - ARRAY/LIST: Flattened elements of the nested structure
    pub(super) child: Option<Arc<Vector>>,
    /// Struct child vectors (one per field)
    pub(super) children: Vec<Arc<Vector>>,
    /// For strings: shared arena allocator (cache-friendly!)
    pub(super) string_heap: Option<Arc<StringHeap>>,
    /// Optional provenance for dictionary overlays.
    pub(super) dictionary_info: Option<DictionaryInfo>,
}

pub(crate) struct VectorResetState {
    logical_type: LogicalType,
    initial_capacity: usize,
    allocator: Arc<dyn Allocator>,
    cached: Vector,
}

impl std::fmt::Debug for VectorResetState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorResetState")
            .field("logical_type", &self.logical_type)
            .field("initial_capacity", &self.initial_capacity)
            .field("allocator", &self.allocator.name())
            .finish()
    }
}

impl Clone for VectorResetState {
    fn clone(&self) -> Self {
        Self::new(
            self.logical_type.clone(),
            self.initial_capacity,
            self.allocator.clone(),
        )
    }
}

impl Vector {
    /// Create a new flat vector with the given type and capacity.
    pub fn new(logical_type: LogicalType) -> Self {
        Self::with_capacity(logical_type, VECTOR_SIZE)
    }

    /// Create a flat vector with specified capacity.
    ///
    /// NOTE: This convenience constructor uses `default_allocator()` and is mainly
    /// intended for tests or standalone utility code. Production paths should pass
    /// an explicit allocator via `with_capacity_and_allocator`.
    pub fn with_capacity(logical_type: LogicalType, capacity: usize) -> Self {
        let allocator = Arc::new(default_allocator());
        Self::with_capacity_and_allocator(logical_type, capacity, allocator)
    }

    /// Create a flat vector with specified capacity and allocator.
    pub fn with_capacity_and_allocator(
        logical_type: LogicalType,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        let element_size = logical_type.physical_size();
        let mut vec = Self {
            vector_type: VectorType::Flat,
            buffer: VectorBuffer::with_allocator(element_size, capacity, allocator.clone()),
            validity: ValidityMask::with_allocator(capacity, allocator.clone()),
            count: 0,
            logical_type: logical_type.clone(),
            sel_vector: None,
            child: None,
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
        };

        // Initialize child vectors for nested types
        match &logical_type {
            LogicalType::Array(child_type, array_size) => {
                let child_capacity = capacity * array_size;
                let child = Self::with_capacity_and_allocator(
                    child_type.as_ref().clone(),
                    child_capacity,
                    allocator,
                );
                vec.child = Some(Arc::new(child));
            }
            LogicalType::List(child_type) => {
                // List data is stored in a child vector.
                // Capacity of child is initially same as parent (grows as needed)
                let child = Self::with_capacity_and_allocator(
                    child_type.as_ref().clone(),
                    capacity,
                    allocator,
                );
                vec.child = Some(Arc::new(child));
            }
            LogicalType::Struct(_fields) => {
                // Struct has multiple children (one per field)
                if let LogicalType::Struct(fields) = &logical_type {
                    let mut children = Vec::with_capacity(fields.len());
                    for (_, field_type) in fields.iter() {
                        let child = Self::with_capacity_and_allocator(
                            field_type.clone(),
                            capacity,
                            allocator.clone(),
                        );
                        children.push(Arc::new(child));
                    }
                    vec.children = children;
                }
            }
            _ => {}
        }
        vec
    }

    pub fn child(&self) -> Option<&Arc<Vector>> {
        self.child.as_ref()
    }

    /// Get mutable reference to child vector.
    /// Used for Array/List types that need to modify child data.
    pub fn child_mut(&mut self) -> Option<&mut Arc<Vector>> {
        self.child.as_mut()
    }

    /// Set the child vector.
    /// Used for Array/List types.
    pub fn set_child(&mut self, child: Arc<Vector>) {
        self.child = Some(child);
    }

    /// Get struct children (one per field).
    pub fn children(&self) -> Option<&[Arc<Vector>]> {
        if matches!(self.logical_type, LogicalType::Struct(_)) {
            Some(self.children.as_slice())
        } else {
            None
        }
    }

    /// Get mutable struct children (one per field).
    pub fn children_mut(&mut self) -> Option<&mut Vec<Arc<Vector>>> {
        if matches!(self.logical_type, LogicalType::Struct(_)) {
            Some(&mut self.children)
        } else {
            None
        }
    }

    /// Set struct children.
    pub fn set_children(&mut self, children: Vec<Arc<Vector>>) {
        self.children = children;
    }

    /// Get the allocator used by this vector.
    pub fn allocator(&self) -> &Arc<dyn Allocator> {
        self.buffer.allocator()
    }

    /// Set the string heap for this vector.
    pub fn set_string_heap(&mut self, heap: Arc<StringHeap>) {
        self.string_heap = Some(heap);
    }

    /// Get the string heap for this vector.
    pub fn string_heap(&self) -> Option<&Arc<StringHeap>> {
        self.string_heap.as_ref()
    }

    /// Prepare a varlen result vector for direct row-wise writes.
    pub fn begin_varlen_write(
        &mut self,
        count: usize,
    ) -> (*mut InlineString, &mut ValidityMask, &mut StringHeap) {
        debug_assert!(matches!(
            self.logical_type,
            LogicalType::Varchar
                | LogicalType::VarcharCollation(_)
                | LogicalType::TsVector
                | LogicalType::TsQuery
                | LogicalType::Json
                | LogicalType::Jsonb
                | LogicalType::Blob
        ));

        self.make_exclusive();
        self.set_len(count);

        let allocator = self.allocator().clone();
        let heap_arc = self.string_heap.get_or_insert_with(|| {
            Arc::new(StringHeap::with_allocator(count.max(1), allocator.clone()))
        });
        if Arc::get_mut(heap_arc).is_none() {
            *heap_arc = Arc::new(StringHeap::with_allocator(count.max(1), allocator));
        }
        let heap = Arc::get_mut(heap_arc).expect("varlen heap must be uniquely owned");
        heap.clear();

        let entries = self.buffer.data() as *mut InlineString;
        let validity = &mut self.validity;
        (entries, validity, heap)
    }

    /// Create a shallow reference to this vector (Zero-copy).
    pub fn reference(&self) -> Self {
        self.clone()
    }

    /// Create a shallow reference while presenting a different logical type.
    ///
    /// This is valid for casts that keep the physical representation unchanged.
    pub fn reference_as(&self, logical_type: LogicalType) -> Self {
        let mut vector = self.reference();
        vector.logical_type = logical_type;
        vector
    }

    /// Ensure the vector's primary buffer and validity mask are exclusively owned.
    pub fn make_exclusive(&mut self) {
        self.buffer.make_exclusive();
        self.validity.make_exclusive();
        if let Some(sel) = &mut self.sel_vector {
            sel.make_exclusive();
        }
    }

    fn ensure_buffer_capacity(&mut self, capacity: usize, allocator: Arc<dyn Allocator>) {
        if self.buffer.capacity() >= capacity {
            return;
        }
        self.buffer =
            VectorBuffer::with_allocator(self.logical_type.physical_size(), capacity, allocator);
    }

    pub(crate) fn reset_for_reuse(&mut self, capacity: usize, allocator: Arc<dyn Allocator>) {
        self.vector_type = VectorType::Flat;
        self.sel_vector = None;
        self.dictionary_info = None;
        self.count = 0;
        self.validity.reset(capacity);

        let logical_type = self.logical_type.clone();
        match logical_type {
            LogicalType::Array(child_type, array_size) => {
                let child_capacity = capacity.saturating_mul(array_size);
                let child = self.child.get_or_insert_with(|| {
                    Arc::new(Vector::with_capacity_and_allocator(
                        child_type.as_ref().clone(),
                        child_capacity,
                        allocator.clone(),
                    ))
                });
                Arc::make_mut(child).reset_for_reuse(child_capacity, allocator.clone());
                self.children.clear();
                self.string_heap = None;
            }
            LogicalType::List(child_type) => {
                self.ensure_buffer_capacity(capacity, allocator.clone());
                let child = self.child.get_or_insert_with(|| {
                    Arc::new(Vector::with_capacity_and_allocator(
                        child_type.as_ref().clone(),
                        capacity,
                        allocator.clone(),
                    ))
                });
                Arc::make_mut(child).reset_for_reuse(0, allocator.clone());
                self.children.clear();
                self.string_heap = None;
            }
            LogicalType::Struct(fields) => {
                if self.children.len() != fields.len() {
                    self.children = fields
                        .iter()
                        .map(|(_, field_type)| {
                            Arc::new(Vector::with_capacity_and_allocator(
                                field_type.clone(),
                                capacity,
                                allocator.clone(),
                            ))
                        })
                        .collect();
                }
                for (child, (_, field_type)) in self.children.iter_mut().zip(fields.iter()) {
                    let child_mut = Arc::make_mut(child);
                    if child_mut.logical_type() != field_type {
                        *child_mut = Vector::with_capacity_and_allocator(
                            field_type.clone(),
                            capacity,
                            allocator.clone(),
                        );
                    } else {
                        child_mut.reset_for_reuse(capacity, allocator.clone());
                    }
                }
                self.child = None;
                self.string_heap = None;
            }
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob => {
                self.ensure_buffer_capacity(capacity, allocator.clone());
                self.child = None;
                self.children.clear();
                if let Some(heap) = &mut self.string_heap {
                    if let Some(heap_mut) = Arc::get_mut(heap) {
                        heap_mut.clear();
                    } else {
                        *heap = Arc::new(StringHeap::with_allocator(capacity.max(1), allocator));
                    }
                } else {
                    self.string_heap = Some(Arc::new(StringHeap::with_allocator(
                        capacity.max(1),
                        allocator,
                    )));
                }
            }
            _ => {
                self.ensure_buffer_capacity(capacity, allocator);
                self.child = None;
                self.children.clear();
                self.string_heap = None;
            }
        }
    }

    /// Reset this vector for execution-time reuse while preserving the logical type.
    pub fn reset_for_execution(&mut self, capacity: usize, allocator: Arc<dyn Allocator>) {
        self.reset_for_reuse(capacity, allocator);
    }

    // ========== Getters ==========

    /// Get the vector type.
    #[inline]
    pub fn vector_type(&self) -> VectorType {
        self.vector_type
    }

    /// Get the logical type.
    #[inline]
    pub fn logical_type(&self) -> &LogicalType {
        &self.logical_type
    }

    /// Get the number of logical elements.
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Get the underlying buffer capacity (number of elements).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Get the selection vector for DICTIONARY types.
    #[inline]
    pub fn sel_vector(&self) -> Option<&SelectionVector> {
        self.sel_vector.as_ref()
    }

    #[inline]
    pub fn dictionary_info(&self) -> Option<&DictionaryInfo> {
        self.dictionary_info.as_ref()
    }

    #[inline]
    fn target_validity_len_for_count(&self, count: usize) -> usize {
        if self.vector_type == VectorType::Constant {
            count.max(1)
        } else {
            count
        }
    }

    /// Returns true when this vector and any nested children already match the
    /// requested logical cardinality and validity shape.
    pub(crate) fn count_matches_cardinality(&self, count: usize) -> bool {
        if self.count != count || self.validity.len() != self.target_validity_len_for_count(count) {
            return false;
        }

        match &self.logical_type {
            LogicalType::Array(_, array_size) => self
                .child
                .as_ref()
                .map(|child| child.count_matches_cardinality(count.saturating_mul(*array_size)))
                .unwrap_or(false),
            LogicalType::Struct(fields) => {
                self.children.len() == fields.len()
                    && self
                        .children
                        .iter()
                        .all(|child| child.count_matches_cardinality(count))
            }
            _ => true,
        }
    }

    /// Set the number of logical elements.
    ///
    /// For Array types, this also updates the child vector's count to
    /// `count * array_size` to ensure consistency.
    #[inline]
    pub fn set_count(&mut self, count: usize) {
        if self.count_matches_cardinality(count) {
            return;
        }

        self.count = count;
        self.validity
            .resize(self.target_validity_len_for_count(count));

        // For Array types, also update the child vector's count
        if let LogicalType::Array(_, array_size) = &self.logical_type {
            if let Some(child) = &mut self.child {
                let child_count = count * array_size;
                let child_mut = Arc::make_mut(child);
                child_mut.set_count(child_count);
            }
        }

        // For Struct types, keep child vectors in sync with parent count
        if matches!(self.logical_type, LogicalType::Struct(_)) && !self.children.is_empty() {
            for child in &mut self.children {
                let child_mut = Arc::make_mut(child);
                child_mut.set_count(count);
            }
        }
    }

    /// Check if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Set the length (number of elements).
    /// Used when manually populating the vector.
    /// Panics if len exceeds capacity (for Flat vectors).
    #[inline]
    pub fn set_len(&mut self, len: usize) {
        if self.vector_type == VectorType::Flat && self.buffer.element_size() > 0 {
            debug_assert!(len <= self.buffer.capacity(), "Length exceeds capacity");
        }
        self.count = len;
        // Also need to resize validity mask if needed
        if len > self.validity.len() {
            self.validity.resize(len);
        }
    }

    /// Check if value at index is null.
    #[inline]
    pub fn is_null(&self, idx: usize) -> bool {
        match self.vector_type {
            VectorType::Constant => !self.validity.is_valid(0),
            VectorType::Dictionary => {
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
                self.child.as_ref().unwrap().is_null(physical_idx)
            }
            _ => !self.validity.is_valid(idx),
        }
    }

    /// Get validity mask reference.
    pub fn validity(&self) -> &ValidityMask {
        &self.validity
    }

    /// Get mutable validity mask reference.
    pub fn validity_mut(&mut self) -> &mut ValidityMask {
        self.make_exclusive();
        &mut self.validity
    }

    // ========== Flat Vector Access ==========

    /// Get raw data pointer for flat vectors.
    ///
    /// # Safety
    /// Only valid for FLAT vectors. Caller must ensure correct type.
    #[inline]
    pub unsafe fn flat_data<T>(&self) -> *const T {
        debug_assert!(
            self.vector_type == VectorType::Flat || self.vector_type == VectorType::Constant
        );
        self.buffer.data() as *const T
    }

    /// Get mutable raw data pointer for flat vectors.
    ///
    /// # Safety
    /// Only valid for FLAT vectors. Caller must ensure correct type.
    #[inline]
    pub unsafe fn flat_data_mut<T>(&mut self) -> *mut T {
        debug_assert!(
            self.vector_type == VectorType::Flat || self.vector_type == VectorType::Constant
        );
        self.make_exclusive();
        self.buffer.data() as *mut T
    }

    /// Get value at index for flat vector.
    ///
    /// # Safety
    /// Caller must ensure T matches the logical type.
    #[inline]
    pub unsafe fn get_flat<T: Copy>(&self, idx: usize) -> T {
        debug_assert!(
            self.vector_type == VectorType::Flat || self.vector_type == VectorType::Constant
        );
        let ptr = self.buffer.data() as *const T;
        *ptr.add(idx)
    }

    /// Set value at index for flat vector.
    ///
    /// # Safety
    /// Caller must ensure T matches the logical type.
    #[inline]
    pub unsafe fn set_flat<T: Copy>(&mut self, idx: usize, value: T) {
        debug_assert!(self.vector_type == VectorType::Flat);
        self.make_exclusive();
        let ptr = self.buffer.data() as *mut T;
        *ptr.add(idx) = value;
    }

    /// Get data as a slice.
    ///
    /// # Safety
    /// Only valid for FLAT or CONSTANT vectors. Caller must ensure T matches logical type.
    pub fn as_slice<T>(&self) -> &[T] {
        debug_assert!(
            self.vector_type == VectorType::Flat || self.vector_type == VectorType::Constant
        );
        unsafe {
            std::slice::from_raw_parts(self.buffer.data() as *const T, self.buffer.capacity())
        }
    }

    /// Get data as a mutable slice.
    ///
    /// # Safety
    /// Only valid for FLAT or CONSTANT vectors. Caller must ensure T matches logical type.
    pub fn as_mut_slice<T>(&mut self) -> &mut [T] {
        debug_assert!(
            self.vector_type == VectorType::Flat || self.vector_type == VectorType::Constant
        );
        self.make_exclusive();
        unsafe {
            std::slice::from_raw_parts_mut(self.buffer.data() as *mut T, self.buffer.capacity())
        }
    }

    /// Set the vector type.
    pub fn set_vector_type(&mut self, vector_type: VectorType) {
        self.vector_type = vector_type;
    }

    /// Set the null status of a value.
    pub fn set_null(&mut self, idx: usize, is_null: bool) {
        if is_null {
            self.validity_mut().set_null(idx);
        } else {
            self.validity_mut().set_valid(idx);
        }
    }

    /// Copy a value from another vector at the given index.
    ///
    /// This is a single-value copy operation. Array values recurse into the
    /// child vector so nested elements stay consistent.
    pub fn copy_at(&mut self, idx: usize, source: &Vector, source_idx: usize) {
        self.make_exclusive();
        // Check for null - using logical index for both
        if source.is_null(source_idx) {
            self.set_null(idx, true);
            return;
        }

        if matches!(
            self.logical_type,
            LogicalType::Varchar
                | LogicalType::VarcharCollation(_)
                | LogicalType::TsVector
                | LogicalType::TsQuery
                | LogicalType::Json
                | LogicalType::Jsonb
        ) {
            if let Some(s) = source.get_string(source_idx) {
                self.set_string(idx, s);
            } else {
                self.set_null(idx, true);
            }
            return;
        }

        if self.logical_type == LogicalType::Blob {
            if let Some(b) = source.get_blob(source_idx) {
                self.set_blob(idx, b);
            } else {
                self.set_null(idx, true);
            }
            return;
        }

        // Handle Array type - copy child elements
        if let LogicalType::Array(_, array_size) = &self.logical_type {
            let array_size = *array_size;
            fn resolve_array_row(vector: &Vector, idx: usize) -> (&Vector, usize) {
                match vector.vector_type {
                    VectorType::Flat => (vector, idx),
                    VectorType::Constant => (vector, 0),
                    VectorType::Dictionary => {
                        let sel = vector
                            .sel_vector
                            .as_ref()
                            .expect("Dictionary vector missing selection vector");
                        let child = vector
                            .child
                            .as_ref()
                            .expect("Dictionary vector missing child");
                        resolve_array_row(child, sel.get(idx))
                    }
                    VectorType::Sequence => {
                        panic!("Sequence vectors cannot be Array type");
                    }
                }
            }

            let (src_base, src_idx) = resolve_array_row(source, source_idx);
            if let (Some(dest_child), Some(src_child)) = (&mut self.child, src_base.child.as_ref())
            {
                let dest_child = Arc::make_mut(dest_child);
                let dest_offset = idx * array_size;
                let src_offset = src_idx * array_size;
                for i in 0..array_size {
                    dest_child.copy_at(dest_offset + i, src_child, src_offset + i);
                }
            }
            self.set_null(idx, false);
            return;
        }

        // Handle List type - append child elements and write list entry
        if let LogicalType::List(_) = &self.logical_type {
            fn resolve_list_row(vector: &Vector, idx: usize) -> (&Vector, usize) {
                match vector.vector_type {
                    VectorType::Flat => (vector, idx),
                    VectorType::Constant => (vector, 0),
                    VectorType::Dictionary => {
                        let sel = vector
                            .sel_vector
                            .as_ref()
                            .expect("Dictionary vector missing selection vector");
                        let child = vector
                            .child
                            .as_ref()
                            .expect("Dictionary vector missing child");
                        resolve_list_row(child, sel.get(idx))
                    }
                    VectorType::Sequence => {
                        panic!("Sequence vectors cannot be List type");
                    }
                }
            }

            fn read_list_entry(vector: &Vector, idx: usize) -> (usize, usize) {
                let entry_base = unsafe { vector.flat_data::<u8>() };
                let entry_ptr = unsafe { entry_base.add(idx * 8) as *const u32 };
                let offset = unsafe { std::ptr::read_unaligned(entry_ptr) as usize };
                let length = unsafe { std::ptr::read_unaligned(entry_ptr.add(1)) as usize };
                (offset, length)
            }

            fn write_list_entry(vector: &mut Vector, idx: usize, offset: u32, length: u32) {
                let entry_base = unsafe { vector.flat_data_mut::<u8>() };
                let entry_ptr = unsafe { entry_base.add(idx * 8) as *mut u32 };
                unsafe {
                    std::ptr::write_unaligned(entry_ptr, offset);
                    std::ptr::write_unaligned(entry_ptr.add(1), length);
                }
            }

            let (src_base, src_idx) = resolve_list_row(source, source_idx);
            let (src_offset, src_length) = read_list_entry(src_base, src_idx);
            let src_child = src_base.child.as_ref().expect("List vector missing child");

            let (dest_offset, dest_capacity, dest_allocator, old_child) = {
                let child = self.child.as_ref().expect("List vector missing child");
                (
                    child.len(),
                    child.buffer.capacity(),
                    child.allocator().clone(),
                    Arc::clone(child),
                )
            };

            let needed = dest_offset + src_length;
            if needed > dest_capacity {
                let new_capacity = needed.max(dest_capacity.saturating_mul(2)).max(1);
                let mut new_child = Vector::with_capacity_and_allocator(
                    old_child.logical_type.clone(),
                    new_capacity,
                    dest_allocator,
                );
                new_child.set_count(dest_offset);
                for i in 0..dest_offset {
                    new_child.copy_at(i, &old_child, i);
                }
                self.child = Some(Arc::new(new_child));
            }

            let dest_child = Arc::make_mut(self.child.as_mut().expect("List vector missing child"));
            // Ensure validity mask can address the appended range without bumping count.
            dest_child.validity_mut().resize(needed);
            for i in 0..src_length {
                dest_child.copy_at(dest_offset + i, src_child, src_offset + i);
            }
            dest_child.set_count(dest_offset + src_length);

            if dest_offset > u32::MAX as usize || src_length > u32::MAX as usize {
                panic!("List entry exceeds u32 range");
            }
            write_list_entry(self, idx, dest_offset as u32, src_length as u32);
            self.set_null(idx, false);
            return;
        }

        // Handle Struct type - copy each field value
        if let LogicalType::Struct(_fields) = &self.logical_type {
            fn resolve_struct_row(vector: &Vector, idx: usize) -> (&Vector, usize) {
                match vector.vector_type {
                    VectorType::Flat => (vector, idx),
                    VectorType::Constant => (vector, 0),
                    VectorType::Dictionary => {
                        let sel = vector
                            .sel_vector
                            .as_ref()
                            .expect("Dictionary vector missing selection vector");
                        let child = vector
                            .child
                            .as_ref()
                            .expect("Dictionary vector missing child");
                        resolve_struct_row(child, sel.get(idx))
                    }
                    VectorType::Sequence => {
                        panic!("Sequence vectors cannot be Struct type");
                    }
                }
            }

            let (src_base, src_idx) = resolve_struct_row(source, source_idx);
            let src_children = src_base.children().expect("Struct vector missing children");

            if self.children.len() != src_children.len() {
                panic!(
                    "Struct child count mismatch: dest={}, src={}",
                    self.children.len(),
                    src_children.len()
                );
            }

            for (dest_child, src_child) in self.children.iter_mut().zip(src_children.iter()) {
                let dest_child = Arc::make_mut(dest_child);
                dest_child.copy_at(idx, src_child, src_idx);
            }

            self.set_null(idx, false);
            return;
        }

        self.set_null(idx, false);
        let size = self.logical_type.type_size();
        unsafe {
            let dest_ptr = self.buffer.data().add(idx * size);

            // Source could be Flat, Constant, or Dictionary; normalize access here.
            match source.vector_type() {
                VectorType::Flat => {
                    let src_ptr = source.buffer.data().add(source_idx * size);
                    std::ptr::copy_nonoverlapping(src_ptr, dest_ptr, size);
                }
                VectorType::Constant => {
                    let src_ptr = source.buffer.data();
                    std::ptr::copy_nonoverlapping(src_ptr, dest_ptr, size);
                }
                VectorType::Dictionary => {
                    let child = source.child.as_ref().expect("Dictionary missing child");
                    let sel = source
                        .sel_vector
                        .as_ref()
                        .expect("Dictionary missing selection vector");
                    let physical_idx = sel.get(source_idx);
                    let src_ptr = child.buffer.data().add(physical_idx * size);
                    std::ptr::copy_nonoverlapping(src_ptr, dest_ptr, size);
                }
                VectorType::Sequence => {
                    // Sequence vectors only exist for i64.
                    if let Some(val) = source.get_i64(source_idx) {
                        *(dest_ptr as *mut i64) = val;
                    }
                }
            }
        }
    }

    // ========== Associated functions that were forwards ==========

    pub fn constant<T: Copy>(logical_type: LogicalType, value: T, count: usize) -> Self {
        let allocator = Arc::new(default_allocator());
        Self::constant_with_allocator(logical_type, value, count, allocator)
    }

    /// Create a constant null vector.
    pub fn constant_null(logical_type: LogicalType, count: usize) -> Self {
        Self::constant_null_with_allocator(logical_type, count, Arc::new(default_allocator()))
    }

    /// Create a sequence vector: start, start+inc, start+2*inc, ...
    pub fn sequence(start: i64, increment: i64, count: usize) -> Self {
        let allocator = Arc::new(default_allocator());
        Self::sequence_with_allocator(start, increment, count, allocator)
    }

    /// Create an embedding vector (fixed-size float array).
    pub fn from_embeddings(embeddings: &[Vec<f32>], dimensions: usize) -> Self {
        Self::from_embeddings_with_allocator(embeddings, dimensions, Arc::new(default_allocator()))
    }

    /// Create a flat vector from i64 values.
    pub fn from_i64(values: &[i64]) -> Self {
        Self::from_i64_with_allocator(values, Arc::new(default_allocator()))
    }

    /// Create a flat vector from i32 values.
    pub fn from_i32(values: &[i32]) -> Self {
        Self::from_i32_with_allocator(values, Arc::new(default_allocator()))
    }

    /// Create a flat vector from f64 values.
    pub fn from_f64(values: &[f64]) -> Self {
        Self::from_f64_with_allocator(values, Arc::new(default_allocator()))
    }

    /// Create a flat vector from f32 values.
    pub fn from_f32(values: &[f32]) -> Self {
        Self::from_f32_with_allocator(values, Arc::new(default_allocator()))
    }

    /// Create a flat vector from bool values.
    pub fn from_bool(values: &[bool]) -> Self {
        Self::from_bool_with_allocator(values, Arc::new(default_allocator()))
    }

    /// Create a flat vector from strings.
    pub fn from_strings(values: &[&str]) -> Self {
        Self::from_strings_with_allocator(values, Arc::new(default_allocator()))
    }
}

impl VectorResetState {
    #[cfg_attr(not(debug_assertions), allow(dead_code))]
    pub(crate) fn logical_type(&self) -> &LogicalType {
        &self.logical_type
    }

    pub(crate) fn new(
        logical_type: LogicalType,
        initial_capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        let mut cached = Vector::with_capacity_and_allocator(
            logical_type.clone(),
            initial_capacity,
            allocator.clone(),
        );
        cached.reset_for_reuse(initial_capacity, allocator.clone());
        Self {
            logical_type,
            initial_capacity,
            allocator,
            cached,
        }
    }

    fn fresh_vector(&self) -> Vector {
        let mut vector = Vector::with_capacity_and_allocator(
            self.logical_type.clone(),
            self.initial_capacity,
            self.allocator.clone(),
        );
        vector.reset_for_reuse(self.initial_capacity, self.allocator.clone());
        vector
    }

    fn recycle_cached(&mut self) {
        if self.cached.logical_type() != &self.logical_type {
            self.cached = self.fresh_vector();
            return;
        }
        self.cached
            .reset_for_reuse(self.initial_capacity, self.allocator.clone());
    }

    pub(crate) fn reset_unique(&mut self, target: &mut Vector) {
        std::mem::swap(target, &mut self.cached);
        target.reset_for_reuse(self.initial_capacity, self.allocator.clone());
        self.recycle_cached();
    }

    pub(crate) fn reset_shared(&mut self) -> Vector {
        let fresh = self.fresh_vector();
        let mut active = std::mem::replace(&mut self.cached, fresh);
        active.reset_for_reuse(self.initial_capacity, self.allocator.clone());
        active
    }
}

impl Vector {
    pub fn collect_allocation_size(&self, allocations: &mut AllocationSet) -> usize {
        let mut total_size = 0;
        total_size += self.buffer.collect_allocation_size(allocations);
        total_size += self.validity.collect_allocation_size(allocations);
        if let Some(sel) = &self.sel_vector {
            total_size += sel.collect_allocation_size(allocations);
        }
        if let Some(heap) = &self.string_heap {
            total_size += allocations.add(heap.allocation_identity(), heap.allocation_size());
        }
        if let Some(child) = &self.child {
            total_size += child.collect_allocation_size(allocations);
        }
        for child in &self.children {
            total_size += child.collect_allocation_size(allocations);
        }
        total_size
    }

    pub fn verify(&self, count: usize) {
        #[cfg(not(debug_assertions))]
        let _ = count;

        #[cfg(debug_assertions)]
        {
            debug_assert!(
                count <= self.len(),
                "Vector count {} exceeds logical length {} for {:?}",
                count,
                self.len(),
                self.logical_type
            );

            let validity_len = if self.vector_type == VectorType::Constant && count > 0 {
                1
            } else {
                count
            };
            debug_assert!(
                self.validity.len() >= validity_len,
                "Validity length {} smaller than expected {} for {:?}",
                self.validity.len(),
                validity_len,
                self.logical_type
            );

            match self.vector_type {
                VectorType::Dictionary => {
                    let sel = self
                        .sel_vector
                        .as_ref()
                        .expect("Dictionary vector missing selection vector");
                    let child = self
                        .child
                        .as_ref()
                        .expect("Dictionary vector missing child");
                    debug_assert_eq!(
                        sel.len(),
                        self.len(),
                        "Dictionary selection length mismatch for {:?}",
                        self.logical_type
                    );
                    debug_assert_eq!(
                        child.logical_type(),
                        &self.logical_type,
                        "Dictionary child type mismatch"
                    );
                    for &physical_idx in sel.as_slice().iter().take(count) {
                        debug_assert!(
                            (physical_idx as usize) < child.len(),
                            "Dictionary index {} out of bounds for child len {}",
                            physical_idx,
                            child.len()
                        );
                    }
                    child.verify(child.len());
                }
                _ => match &self.logical_type {
                    LogicalType::Array(child_type, array_size) => {
                        let child = self.child.as_ref().expect("Array vector missing child");
                        debug_assert_eq!(child.logical_type(), child_type.as_ref());
                        if self.vector_type != VectorType::Constant {
                            debug_assert_eq!(
                                child.len(),
                                count.saturating_mul(*array_size),
                                "Array child length mismatch"
                            );
                        }
                        child.verify(child.len());
                    }
                    LogicalType::List(child_type) => {
                        let child = self.child.as_ref().expect("List vector missing child");
                        debug_assert_eq!(child.logical_type(), child_type.as_ref());
                        if self.vector_type == VectorType::Flat {
                            for row in 0..count {
                                if self.is_null(row) {
                                    continue;
                                }
                                let base = unsafe { self.flat_data::<u8>() };
                                let entry_ptr = unsafe { base.add(row * 8) as *const u32 };
                                let offset =
                                    unsafe { std::ptr::read_unaligned(entry_ptr) as usize };
                                let len =
                                    unsafe { std::ptr::read_unaligned(entry_ptr.add(1)) as usize };
                                debug_assert!(
                                    offset + len <= child.len(),
                                    "List entry [{offset}, {}) exceeds child len {}",
                                    offset + len,
                                    child.len()
                                );
                            }
                        }
                        child.verify(child.len());
                    }
                    LogicalType::Struct(fields) => {
                        debug_assert_eq!(
                            self.children.len(),
                            fields.len(),
                            "Struct child count mismatch"
                        );
                        for (child, (_, field_type)) in self.children.iter().zip(fields.iter()) {
                            debug_assert_eq!(
                                child.logical_type(),
                                field_type,
                                "Struct child type mismatch"
                            );
                            if self.vector_type != VectorType::Constant {
                                debug_assert_eq!(
                                    child.len(),
                                    count,
                                    "Struct child length mismatch"
                                );
                            }
                            child.verify(child.len());
                        }
                    }
                    _ => {}
                },
            }
        }
    }
}

// ============================================================================
// LogicalType extension
// ============================================================================

impl LogicalType {
    /// Get the physical size in bytes for this type.
    ///
    /// For compound types (Array, List, Struct), returns 0 as data is stored in child vector.
    pub fn physical_size(&self) -> usize {
        match self {
            LogicalType::Boolean => 1,
            LogicalType::TinyInt | LogicalType::UTinyInt => 1,
            LogicalType::SmallInt | LogicalType::USmallInt => 2,
            LogicalType::Integer | LogicalType::UInteger => 4,
            LogicalType::BigInt | LogicalType::UBigInt => 8,
            LogicalType::HugeInt | LogicalType::UHugeInt | LogicalType::Uuid => 16,
            LogicalType::Float => 4,
            LogicalType::Double => 8,
            LogicalType::Date => 4,
            LogicalType::Timestamp => 8,
            LogicalType::TimestampTz => 8,
            LogicalType::Time => 8,
            LogicalType::Interval => 16,
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb => 16, // InlineString: 16 bytes
            LogicalType::Blob => 16, // InlineString: 16 bytes (same as Varchar)
            LogicalType::Decimal { precision, .. } => {
                if *precision <= 18 {
                    8
                } else {
                    16
                }
            }
            LogicalType::Null => 1, // Still needs validity bit, and at least 1 byte if we allocate
            LogicalType::Array(_, _) => 0, // Data in child vector
            LogicalType::List(_) => 8, // Offset (u32) + length (u32)
            LogicalType::Struct(_) => 0, // Data in child vectors
            _ => 8,
        }
    }
}
