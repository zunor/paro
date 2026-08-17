// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use bytes::Bytes;

use crate::allocator::Allocator;
use crate::error::{self as paro_error, Result};
use crate::memory::AllocationId;
use crate::types::{LogicalType, PhysicalType, StringView};

use super::{
    AllocationSet, SelectionVector, StringHeap, ValidityMask, VectorBuffer, VectorSelection,
    VectorType,
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

/// Opaque ownership token whose lifetime follows a shallow vector reference.
///
/// Vectors normally own their allocations through buffers and heaps. Blocking
/// operators may also retain allocations owned by another subsystem (for
/// example a page-cache pin or query accounting lease). Attaching that owner
/// to the vector makes zero-copy handoff explicit: clones and dictionary
/// children keep the token alive until the last data reference disappears.
pub trait VectorLifetimeOwner: std::fmt::Debug + Send + Sync {}

impl<T> VectorLifetimeOwner for T where T: std::fmt::Debug + Send + Sync {}

#[derive(Debug)]
pub(super) struct VectorLifetimeOwners {
    owners: Box<[Arc<dyn VectorLifetimeOwner>]>,
}

impl VectorLifetimeOwners {
    fn with_added(existing: Option<&Arc<Self>>, owner: Arc<dyn VectorLifetimeOwner>) -> Arc<Self> {
        if let Some(existing) = existing {
            if existing
                .owners
                .iter()
                .any(|candidate| Arc::ptr_eq(candidate, &owner))
            {
                return Arc::clone(existing);
            }
        }
        let mut owners = Vec::with_capacity(existing.map_or(1, |set| set.owners.len() + 1));
        if let Some(existing) = existing {
            owners.extend(existing.owners.iter().cloned());
        }
        owners.push(owner);
        Arc::new(Self {
            owners: owners.into_boxed_slice(),
        })
    }
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
    /// - DICTIONARY: Unused (indices in selection)
    /// - SEQUENCE: [start, increment]
    /// - CONSTANT: Single value
    pub(super) buffer: VectorBuffer,
    /// Validity mask for nulls
    pub(super) validity: ValidityMask,
    /// Number of logical elements
    pub(super) count: usize,
    /// For DICTIONARY: logical-to-physical row mapping
    ///
    /// Dictionary construction guarantees every logical index is inside the
    /// canonical base child's cardinality. Producers that already validated
    /// indices may transfer that proof through the explicit unsafe constructor.
    /// For SEQUENCE: unused
    pub(super) selection: VectorSelection,
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
    /// Non-allocation owners required by the referenced vector storage.
    /// Allocated only for the uncommon zero-copy handoff that needs an
    /// ownership token beyond the vector's ordinary buffers and heaps.
    pub(super) lifetime_owners: Option<Arc<VectorLifetimeOwners>>,
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
        Self::try_new(
            self.logical_type.clone(),
            self.initial_capacity,
            self.allocator.clone(),
        )
        .expect("vector reset state clone allocation failed")
    }
}

impl Vector {
    /// Create a flat vector with specified capacity and allocator.
    pub fn try_new(
        logical_type: LogicalType,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let element_size = logical_type.physical_size();
        let mut vec = Self {
            vector_type: VectorType::Flat,
            buffer: VectorBuffer::try_with_allocator(element_size, capacity, allocator.clone())?,
            validity: ValidityMask::with_allocator(capacity, allocator.clone()),
            count: 0,
            logical_type: logical_type.clone(),
            selection: VectorSelection::None,
            child: None,
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
            lifetime_owners: None,
        };

        // Initialize child vectors for nested types
        match &logical_type {
            LogicalType::Array(child_type, array_size) => {
                let child_capacity = capacity.checked_mul(*array_size).ok_or_else(|| {
                    paro_error::out_of_memory(format!(
                        "array vector capacity overflow: capacity={capacity}, array_size={array_size}"
                    ))
                })?;
                let child = Self::try_new(child_type.as_ref().clone(), child_capacity, allocator)?;
                vec.child = Some(Arc::new(child));
            }
            LogicalType::List(child_type) => {
                // List data is stored in a child vector.
                // Capacity of child is initially same as parent (grows as needed)
                let child = Self::try_new(child_type.as_ref().clone(), capacity, allocator)?;
                vec.child = Some(Arc::new(child));
            }
            LogicalType::Struct(_fields) => {
                // Struct has multiple children (one per field)
                if let LogicalType::Struct(fields) = &logical_type {
                    let mut children = Vec::with_capacity(fields.len());
                    for (_, field_type) in fields.iter() {
                        let child = Self::try_new(field_type.clone(), capacity, allocator.clone())?;
                        children.push(Arc::new(child));
                    }
                    vec.children = children;
                }
            }
            _ => {}
        }
        Ok(vec)
    }

    /// Create an all-valid flat vector over immutable fixed-width bytes.
    ///
    /// The byte owner is retained by the vector. Any later mutable access
    /// transparently materializes allocator-owned storage through COW.
    pub fn try_from_fixed_width_bytes(
        logical_type: LogicalType,
        rows: usize,
        bytes: Bytes,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        if matches!(
            logical_type.physical_type(),
            PhysicalType::Varchar | PhysicalType::List | PhysicalType::Struct | PhysicalType::Array
        ) {
            return Err(paro_error::invalid_input(format!(
                "external fixed-width vector does not support {logical_type:?}"
            )));
        }
        let element_size = logical_type.physical_size();
        Ok(Self {
            vector_type: VectorType::Flat,
            buffer: VectorBuffer::try_from_bytes(element_size, rows, bytes, allocator.clone())?,
            validity: ValidityMask::with_allocator(rows, allocator),
            count: rows,
            logical_type,
            selection: VectorSelection::None,
            child: None,
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
            lifetime_owners: None,
        })
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

    #[inline]
    pub fn logical_capacity(&self) -> usize {
        if self.buffer.element_size() == 0 {
            self.validity.len()
        } else {
            self.buffer.capacity()
        }
    }

    pub(crate) fn try_materialize_for_write(source: &Vector) -> Result<Vector> {
        let capacity = source.logical_capacity().max(source.len());
        let mut materialized = Vector::try_new(
            source.logical_type.clone(),
            capacity,
            source.allocator().clone(),
        )?;
        if !source.is_empty() {
            materialized.try_copy_range(0, source, 0, source.len())?;
        }
        materialized.try_set_count(source.len())?;
        Ok(materialized)
    }

    pub fn try_make_arc_mut(vector: &mut Arc<Vector>) -> Result<&mut Vector> {
        if Arc::get_mut(vector).is_none() {
            let materialized = Self::try_materialize_for_write(vector.as_ref())?;
            *vector = Arc::new(materialized);
        }
        Ok(Arc::get_mut(vector).expect("vector must be uniquely owned after materialization"))
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
    ) -> (*mut StringView, &mut ValidityMask, &mut StringHeap) {
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

        let entries = self.buffer.data() as *mut StringView;
        let validity = &mut self.validity;
        (entries, validity, heap)
    }

    /// Create a shallow reference to this vector (Zero-copy).
    pub fn reference(&self) -> Self {
        self.clone()
    }

    /// Create a zero-copy reference that retains an additional opaque owner.
    ///
    /// The owner is attached recursively to nested children so extracting a
    /// list/struct/dictionary child cannot outlive the lease that protects its
    /// shared allocation.
    pub fn reference_with_lifetime_owner(&self, owner: Arc<dyn VectorLifetimeOwner>) -> Self {
        let mut vector = self.reference();
        vector.attach_lifetime_owner(owner);
        vector
    }

    fn attach_lifetime_owner(&mut self, owner: Arc<dyn VectorLifetimeOwner>) {
        self.lifetime_owners = Some(VectorLifetimeOwners::with_added(
            self.lifetime_owners.as_ref(),
            Arc::clone(&owner),
        ));
        if let Some(child) = &mut self.child {
            let mut referenced = child.reference();
            referenced.attach_lifetime_owner(Arc::clone(&owner));
            *child = Arc::new(referenced);
        }
        for child in &mut self.children {
            let mut referenced = child.reference();
            referenced.attach_lifetime_owner(Arc::clone(&owner));
            *child = Arc::new(referenced);
        }
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
    pub fn try_make_exclusive(&mut self) -> Result<()> {
        self.buffer.try_make_exclusive()?;
        self.validity.try_make_exclusive()?;
        self.selection.try_make_exclusive()?;
        Ok(())
    }

    /// Ensure exclusive ownership for call sites that cannot surface errors.
    pub fn make_exclusive(&mut self) {
        self.try_make_exclusive()
            .expect("vector copy-on-write allocation failed");
    }

    fn try_ensure_buffer_capacity(
        &mut self,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<()> {
        if self.buffer.capacity() >= capacity {
            return Ok(());
        }
        self.buffer = VectorBuffer::try_with_allocator(
            self.logical_type.physical_size(),
            capacity,
            allocator,
        )?;
        Ok(())
    }

    pub(crate) fn try_reset_for_reuse(
        &mut self,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<()> {
        self.vector_type = VectorType::Flat;
        self.selection = VectorSelection::None;
        self.dictionary_info = None;
        self.count = 0;
        self.validity.reset(capacity);

        let logical_type = self.logical_type.clone();
        match logical_type {
            LogicalType::Array(child_type, array_size) => {
                let child_capacity = capacity.checked_mul(array_size).ok_or_else(|| {
                    paro_error::out_of_memory(format!(
                        "array vector reset capacity overflow: capacity={capacity}, array_size={array_size}"
                    ))
                })?;
                if self.child.is_none() {
                    self.child = Some(Arc::new(Vector::try_new(
                        child_type.as_ref().clone(),
                        child_capacity,
                        allocator.clone(),
                    )?));
                }
                let child = self.child.as_mut().expect("array child must exist");
                if let Some(child_mut) = Arc::get_mut(child) {
                    child_mut.try_reset_for_reuse(child_capacity, allocator.clone())?;
                } else {
                    *child = Arc::new(Vector::try_new(
                        child_type.as_ref().clone(),
                        child_capacity,
                        allocator.clone(),
                    )?);
                }
                self.children.clear();
                self.string_heap = None;
            }
            LogicalType::List(child_type) => {
                self.try_ensure_buffer_capacity(capacity, allocator.clone())?;
                if self.child.is_none() {
                    self.child = Some(Arc::new(Vector::try_new(
                        child_type.as_ref().clone(),
                        capacity,
                        allocator.clone(),
                    )?));
                }
                let child = self.child.as_mut().expect("list child must exist");
                if let Some(child_mut) = Arc::get_mut(child) {
                    child_mut.try_reset_for_reuse(0, allocator.clone())?;
                } else {
                    *child = Arc::new(Vector::try_new(
                        child_type.as_ref().clone(),
                        0,
                        allocator.clone(),
                    )?);
                }
                self.children.clear();
                self.string_heap = None;
            }
            LogicalType::Struct(fields) => {
                if self.children.len() != fields.len() {
                    self.children = fields
                        .iter()
                        .map(|(_, field_type)| {
                            Ok(Arc::new(Vector::try_new(
                                field_type.clone(),
                                capacity,
                                allocator.clone(),
                            )?))
                        })
                        .collect::<Result<Vec<_>>>()?;
                }
                for (child, (_, field_type)) in self.children.iter_mut().zip(fields.iter()) {
                    let child_mut = Self::try_make_arc_mut(child)?;
                    if child_mut.logical_type() != field_type {
                        *child_mut =
                            Vector::try_new(field_type.clone(), capacity, allocator.clone())?;
                    } else {
                        child_mut.try_reset_for_reuse(capacity, allocator.clone())?;
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
                self.try_ensure_buffer_capacity(capacity, allocator.clone())?;
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
                self.try_ensure_buffer_capacity(capacity, allocator)?;
                self.child = None;
                self.children.clear();
                self.string_heap = None;
            }
        }
        Ok(())
    }

    /// Reset this vector for execution-time reuse while preserving the logical type.
    pub fn try_reset_for_execution(
        &mut self,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<()> {
        self.try_reset_for_reuse(capacity, allocator)
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
        self.selection.as_materialized()
    }

    /// Get the dictionary selection mapping.
    #[inline]
    pub fn selection(&self) -> &VectorSelection {
        &self.selection
    }

    #[inline]
    pub(super) fn physical_index(&self, idx: usize) -> usize {
        self.selection.physical_index(idx)
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
        if self.count != count || self.validity.len() < self.target_validity_len_for_count(count) {
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
        self.try_set_count(count)
            .expect("vector count allocation failed");
    }

    /// Set the number of logical elements.
    ///
    /// For Array types, this also updates the child vector's count to
    /// `count * array_size` to ensure consistency.
    #[inline]
    pub fn try_set_count(&mut self, count: usize) -> Result<()> {
        if self.count_matches_cardinality(count) {
            return Ok(());
        }

        let target_validity_len = self.target_validity_len_for_count(count);
        if self.validity.len() < target_validity_len {
            self.validity.try_resize(target_validity_len)?;
        }

        // For Array types, also update the child vector's count
        if let LogicalType::Array(_, array_size) = &self.logical_type {
            if let Some(child) = &mut self.child {
                let child_count = count.checked_mul(*array_size).ok_or_else(|| {
                    paro_error::internal(format!(
                        "array child count overflow: count={count}, array_size={array_size}"
                    ))
                })?;
                let child_mut = Self::try_make_arc_mut(child)?;
                child_mut.try_set_count(child_count)?;
            }
        }

        // For Struct types, keep child vectors in sync with parent count
        if matches!(self.logical_type, LogicalType::Struct(_)) && !self.children.is_empty() {
            for child in &mut self.children {
                let child_mut = Self::try_make_arc_mut(child)?;
                child_mut.try_set_count(count)?;
            }
        }

        self.count = count;
        Ok(())
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
        self.try_set_len(len)
            .expect("vector length allocation failed");
    }

    /// Set the length (number of elements).
    /// Used when manually populating the vector.
    #[inline]
    pub fn try_set_len(&mut self, len: usize) -> Result<()> {
        if self.vector_type == VectorType::Flat
            && self.buffer.element_size() > 0
            && len > self.buffer.capacity()
        {
            return Err(paro_error::out_of_range(format!(
                "vector length exceeds capacity: length={len}, capacity={}",
                self.buffer.capacity()
            )));
        }
        // Also need to resize validity mask if needed
        if len > self.validity.len() {
            self.validity.try_resize(len)?;
        }
        self.count = len;
        Ok(())
    }

    /// Check if value at index is null.
    #[inline]
    pub fn is_null(&self, idx: usize) -> bool {
        match self.vector_type {
            VectorType::Constant => !self.validity.is_valid(0),
            VectorType::Dictionary => {
                let physical_idx = self.selection.physical_index(idx);
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

    /// Get a fixed-width value through flat/constant/dictionary encodings.
    ///
    /// # Safety
    /// Caller must ensure T matches the vector's physical fixed-width type.
    #[inline]
    pub unsafe fn get_fixed<T: Copy>(&self, idx: usize) -> T {
        match self.vector_type {
            VectorType::Flat => self.get_flat(idx),
            VectorType::Constant => self.get_flat(0),
            VectorType::Dictionary => {
                let physical_idx = self.selection.physical_index(idx);
                self.child
                    .as_ref()
                    .expect("dictionary vector missing child")
                    .get_fixed(physical_idx)
            }
            _ => unreachable!("fixed-width read requires flat, constant, or dictionary vector"),
        }
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
        self.try_set_null(idx, is_null)
            .expect("vector validity allocation failed");
    }

    /// Set the null status of a value.
    pub fn try_set_null(&mut self, idx: usize, is_null: bool) -> Result<()> {
        if is_null {
            self.validity.try_set_null(idx)
        } else {
            self.validity.try_set_valid(idx)
        }
    }
}

impl VectorResetState {
    #[cfg_attr(not(debug_assertions), allow(dead_code))]
    pub(crate) fn logical_type(&self) -> &LogicalType {
        &self.logical_type
    }

    pub(crate) fn try_new(
        logical_type: LogicalType,
        initial_capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let mut cached =
            Vector::try_new(logical_type.clone(), initial_capacity, allocator.clone())?;
        cached.try_reset_for_reuse(initial_capacity, allocator.clone())?;
        Ok(Self {
            logical_type,
            initial_capacity,
            allocator,
            cached,
        })
    }

    fn try_fresh_vector(&self) -> Result<Vector> {
        let mut vector = Vector::try_new(
            self.logical_type.clone(),
            self.initial_capacity,
            self.allocator.clone(),
        )?;
        vector.try_reset_for_reuse(self.initial_capacity, self.allocator.clone())?;
        Ok(vector)
    }

    fn try_recycle_cached(&mut self) -> Result<()> {
        if self.cached.logical_type() != &self.logical_type {
            self.cached = self.try_fresh_vector()?;
            return Ok(());
        }
        self.cached
            .try_reset_for_reuse(self.initial_capacity, self.allocator.clone())?;
        Ok(())
    }

    pub(crate) fn try_reset_unique(&mut self, target: &mut Vector) -> Result<()> {
        std::mem::swap(target, &mut self.cached);
        target.try_reset_for_reuse(self.initial_capacity, self.allocator.clone())?;
        self.try_recycle_cached()
    }

    pub(crate) fn try_reset_shared(&mut self) -> Result<Vector> {
        let fresh = self.try_fresh_vector()?;
        let mut active = std::mem::replace(&mut self.cached, fresh);
        active.try_reset_for_reuse(self.initial_capacity, self.allocator.clone())?;
        Ok(active)
    }
}

impl Vector {
    pub fn collect_allocation_size(&self, allocations: &mut AllocationSet) -> usize {
        let mut total_size = 0;
        total_size += self.buffer.collect_allocation_size(allocations);
        total_size += self.validity.collect_allocation_size(allocations);
        total_size += self.selection.collect_allocation_size(allocations);
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

    pub fn collect_allocation_entries(&self, entries: &mut Vec<(AllocationId, usize)>) {
        let mut raw_entries = Vec::new();
        self.collect_raw_allocation_entries(&mut raw_entries);
        entries.extend(
            raw_entries
                .into_iter()
                .map(|(id, bytes)| (AllocationId(id as u64), bytes)),
        );
    }

    fn collect_raw_allocation_entries(&self, entries: &mut Vec<(usize, usize)>) {
        self.buffer.collect_allocation_entries(entries);
        self.validity.collect_allocation_entries(entries);
        self.selection.collect_allocation_entries(entries);
        if let Some(heap) = &self.string_heap {
            let bytes = heap.allocation_size();
            if bytes > 0 {
                entries.push((heap.allocation_identity(), bytes));
            }
        }
        if let Some(child) = &self.child {
            child.collect_raw_allocation_entries(entries);
        }
        for child in &self.children {
            child.collect_raw_allocation_entries(entries);
        }
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
                    let child = self
                        .child
                        .as_ref()
                        .expect("Dictionary vector missing child");
                    debug_assert_eq!(
                        self.selection.len(),
                        self.len(),
                        "Dictionary selection length mismatch for {:?}",
                        self.logical_type
                    );
                    debug_assert_eq!(
                        child.logical_type(),
                        &self.logical_type,
                        "Dictionary child type mismatch"
                    );
                    for row_idx in 0..count {
                        let physical_idx = self.selection.physical_index(row_idx);
                        debug_assert!(
                            physical_idx < child.len(),
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
            | LogicalType::Jsonb => 16, // StringView: 16 bytes
            LogicalType::Blob => 16, // StringView: 16 bytes (same as Varchar)
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
