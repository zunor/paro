// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::marker::PhantomData;

use crate::error::{self as paro_error, Result};
use crate::types::{LogicalType, PhysicalType, StringView};

use super::{
    ArrayVector, SelectionVector, ValidityMask, Vector, VectorBuffer, VectorSelection, VectorType,
};

#[derive(Debug, Clone)]
pub enum SelectionRef<'a> {
    Borrowed(&'a SelectionVector),
    Owned(SelectionVector),
    Range { offset: usize, count: usize },
    Constant { index: usize, count: usize },
    Incremental { count: usize },
}

impl<'a> SelectionRef<'a> {
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Borrowed(sel) => sel.len(),
            Self::Owned(sel) => sel.len(),
            Self::Range { count, .. } => *count,
            Self::Constant { count, .. } | Self::Incremental { count } => *count,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get(&self, idx: usize) -> usize {
        match self {
            Self::Borrowed(sel) => sel.get(idx),
            Self::Owned(sel) => sel.get(idx),
            Self::Range { offset, .. } => offset + idx,
            Self::Constant { index, .. } => *index,
            Self::Incremental { .. } => idx,
        }
    }

    #[inline]
    pub fn allocation_identity(&self) -> Option<usize> {
        match self {
            Self::Borrowed(sel) => sel.allocation_identity(),
            Self::Owned(sel) => sel.allocation_identity(),
            Self::Range { .. } | Self::Constant { .. } | Self::Incremental { .. } => None,
        }
    }

    #[inline]
    pub fn materialized_indices(&self) -> Option<&[u32]> {
        match self {
            Self::Borrowed(selection) => Some(selection.as_slice()),
            Self::Owned(selection) => Some(selection.as_slice()),
            Self::Range { .. } | Self::Constant { .. } | Self::Incremental { .. } => None,
        }
    }

    fn try_compose(self, selection: &'a VectorSelection, count: usize) -> Result<SelectionRef<'a>> {
        match (self, selection) {
            (base, VectorSelection::Repeated { index, count: len }) => {
                if count > *len {
                    return Err(paro_error::internal(format!(
                        "vector view count exceeds repeated selection: count={count}, selection_count={len}"
                    )));
                }
                Ok(SelectionRef::Constant {
                    index: base.get(*index),
                    count,
                })
            }
            (SelectionRef::Borrowed(child_sel), VectorSelection::Materialized(sel)) => {
                Ok(SelectionRef::Owned(child_sel.try_slice(sel, count)?))
            }
            (SelectionRef::Borrowed(child_sel), VectorSelection::Range { offset, .. }) => Ok(
                SelectionRef::Owned(child_sel.try_slice_range(*offset, count)?),
            ),
            (SelectionRef::Owned(child_sel), VectorSelection::Materialized(sel)) => {
                Ok(SelectionRef::Owned(child_sel.try_slice(sel, count)?))
            }
            (SelectionRef::Owned(child_sel), VectorSelection::Range { offset, .. }) => Ok(
                SelectionRef::Owned(child_sel.try_slice_range(*offset, count)?),
            ),
            (
                SelectionRef::Range {
                    offset: child_offset,
                    ..
                },
                VectorSelection::Range { offset, .. },
            ) => Ok(SelectionRef::Range {
                offset: child_offset + offset,
                count,
            }),
            (SelectionRef::Range { offset, .. }, VectorSelection::Materialized(sel)) => {
                let mut result =
                    SelectionVector::try_with_capacity(count, sel.allocator().clone())?;
                result.set_len(count);
                result.try_fill_offset_from(offset, sel, count)?;
                Ok(SelectionRef::Owned(result))
            }
            (SelectionRef::Constant { index, .. }, _) => {
                Ok(SelectionRef::Constant { index, count })
            }
            (SelectionRef::Incremental { count: _ }, VectorSelection::Materialized(sel))
                if count == sel.len() =>
            {
                Ok(SelectionRef::Borrowed(sel))
            }
            (SelectionRef::Incremental { count: _ }, VectorSelection::Materialized(sel)) => {
                Ok(SelectionRef::Owned(sel.try_slice_range(0, count)?))
            }
            (SelectionRef::Incremental { count: _ }, VectorSelection::Range { offset, .. }) => {
                Ok(SelectionRef::Range {
                    offset: *offset,
                    count,
                })
            }
            (selection, VectorSelection::None) => Ok(selection),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ValidityRef<'a> {
    Borrowed(&'a ValidityMask),
    Owned(ValidityMask),
}

impl<'a> ValidityRef<'a> {
    #[inline]
    pub fn as_mask(&self) -> &ValidityMask {
        match self {
            Self::Borrowed(mask) => mask,
            Self::Owned(mask) => mask,
        }
    }

    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        self.as_mask().is_valid(idx)
    }

    #[inline]
    pub fn all_valid(&self) -> bool {
        self.as_mask().all_valid()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.as_mask().capacity()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DataRef {
    Ptr(*const u8),
    SequenceI64 { start: i64, increment: i64 },
}

#[derive(Debug, Clone)]
pub struct VectorView<'a> {
    logical_type: &'a LogicalType,
    sel: SelectionRef<'a>,
    validity: ValidityRef<'a>,
    data: DataRef,
    physical_count: usize,
    _vector: PhantomData<&'a Vector>,
}

impl<'a> VectorView<'a> {
    #[inline]
    pub fn logical_type(&self) -> &'a LogicalType {
        self.logical_type
    }

    #[inline]
    pub fn sel(&self) -> &SelectionRef<'a> {
        &self.sel
    }

    #[inline]
    pub fn validity(&self) -> &ValidityRef<'a> {
        &self.validity
    }

    #[inline]
    pub fn data(&self) -> DataRef {
        self.data
    }

    #[inline]
    pub fn physical_count(&self) -> usize {
        self.physical_count
    }

    #[inline]
    pub fn physical_index(&self, idx: usize) -> usize {
        self.sel.get(idx)
    }

    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        self.validity.is_valid(self.physical_index(idx))
    }

    #[inline]
    pub fn get_data<T>(&self) -> Option<*const T> {
        match self.data {
            DataRef::Ptr(ptr) => Some(ptr as *const T),
            DataRef::SequenceI64 { .. } => None,
        }
    }

    #[inline]
    pub fn get_i64(&self, idx: usize) -> i64 {
        match self.data {
            DataRef::Ptr(ptr) => unsafe { *(ptr as *const i64).add(self.physical_index(idx)) },
            DataRef::SequenceI64 { start, increment } => {
                start + self.physical_index(idx) as i64 * increment
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct VarlenView<'a> {
    logical_type: &'a LogicalType,
    entries: *const StringView,
    sel: SelectionRef<'a>,
    validity: ValidityRef<'a>,
    _vector: PhantomData<&'a Vector>,
}

impl<'a> VarlenView<'a> {
    #[inline]
    pub fn logical_type(&self) -> &'a LogicalType {
        self.logical_type
    }

    #[inline]
    pub fn sel(&self) -> &SelectionRef<'a> {
        &self.sel
    }

    #[inline]
    pub fn validity(&self) -> &ValidityRef<'a> {
        &self.validity
    }

    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        self.validity.is_valid(self.sel.get(idx))
    }

    #[inline]
    pub fn bytes(&self, idx: usize) -> &[u8] {
        // SAFETY: `entries` belongs to the borrowed vector, and the selected
        // index is valid for this view. The returned slice is tied to `self`.
        unsafe { (&*self.entries.add(self.sel.get(idx))).as_bytes() }
    }

    /// Prove once that this view contains UTF-8 logical values.
    #[inline]
    pub fn try_as_utf8(self) -> Result<Utf8View<'a>> {
        if !self.logical_type.is_utf8_varlen() {
            return Err(paro_error::type_mismatch(format!(
                "UTF-8 view requires a textual varlen type, got {:?}",
                self.logical_type
            )));
        }
        Ok(Utf8View { inner: self })
    }

    /// Copy the physical view value out of the owning vector.
    ///
    /// # Safety
    /// The result must not be used after the vector borrowed by this
    /// `VarlenView` is dropped or its backing string heap is replaced.
    #[inline]
    pub unsafe fn value(&self, idx: usize) -> StringView {
        unsafe { *self.entries.add(self.sel.get(idx)) }
    }
}

/// Varlen vector view whose logical type guarantees valid UTF-8 bytes.
#[derive(Debug, Clone)]
pub struct Utf8View<'a> {
    inner: VarlenView<'a>,
}

impl<'a> Utf8View<'a> {
    #[inline]
    pub fn logical_type(&self) -> &'a LogicalType {
        self.inner.logical_type()
    }

    #[inline]
    pub fn sel(&self) -> &SelectionRef<'a> {
        self.inner.sel()
    }

    #[inline]
    pub fn validity(&self) -> &ValidityRef<'a> {
        self.inner.validity()
    }

    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        self.inner.is_valid(idx)
    }

    /// Read a string without revalidating every row's bytes.
    #[inline]
    pub fn str(&self, idx: usize) -> &str {
        // SAFETY: `try_as_utf8` proves that the vector's logical type carries
        // the UTF-8 invariant, which all safe vector write paths preserve.
        unsafe { std::str::from_utf8_unchecked(self.inner.bytes(idx)) }
    }
}

#[derive(Debug, Clone)]
pub struct ArrayView<'a> {
    parent: VectorView<'a>,
    child: VectorView<'a>,
    array_size: usize,
}

impl<'a> ArrayView<'a> {
    #[inline]
    pub fn parent(&self) -> &VectorView<'a> {
        &self.parent
    }

    #[inline]
    pub fn child(&self) -> &VectorView<'a> {
        &self.child
    }

    #[inline]
    pub fn is_valid(&self, row_idx: usize) -> bool {
        self.parent.is_valid(row_idx)
    }

    #[inline]
    pub fn array_size(&self) -> usize {
        self.array_size
    }

    #[inline]
    pub fn logical_child_index(&self, row_idx: usize, offset: usize) -> usize {
        self.parent.physical_index(row_idx) * self.array_size + offset
    }

    #[inline]
    pub fn physical_child_index(&self, row_idx: usize, offset: usize) -> usize {
        self.child
            .physical_index(self.logical_child_index(row_idx, offset))
    }

    #[inline]
    pub fn child_is_valid(&self, row_idx: usize, offset: usize) -> bool {
        self.child
            .is_valid(self.logical_child_index(row_idx, offset))
    }
}

/// Borrowed decoded vector view for short-lived, zero-allocation readers.
#[derive(Debug, Clone)]
pub struct DecodedVectorRef<'a> {
    sel: SelectionRef<'a>,
    data: DataRef,
    validity: ValidityRef<'a>,
    physical_count: usize,
}

impl<'a> DecodedVectorRef<'a> {
    #[inline]
    pub fn sel(&self) -> &SelectionRef<'a> {
        &self.sel
    }

    #[inline]
    pub fn physical_index(&self, idx: usize) -> usize {
        self.sel.get(idx)
    }

    #[inline]
    pub fn data(&self) -> DataRef {
        self.data
    }

    #[inline]
    pub fn set_data(&mut self, data: DataRef) {
        self.data = data;
    }

    #[inline]
    pub fn get_data<T>(&self) -> *const T {
        match self.data {
            DataRef::Ptr(ptr) => ptr as *const T,
            DataRef::SequenceI64 { .. } => std::ptr::null(),
        }
    }

    #[inline]
    pub fn validity(&self) -> &ValidityRef<'a> {
        &self.validity
    }

    #[inline]
    pub fn physical_count(&self) -> usize {
        self.physical_count
    }

    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        self.validity.is_valid(self.physical_index(idx))
    }

    #[inline]
    /// # Safety
    ///
    /// The caller must ensure pointer-backed decoded data contains initialized
    /// `T` values and that `idx` resolves to an in-bounds physical row. Sequence
    /// data is only valid for 8-byte scalar reads.
    pub unsafe fn get_value<T: Copy>(&self, idx: usize) -> T {
        let physical_idx = self.physical_index(idx);
        match self.data {
            DataRef::Ptr(ptr) => unsafe { *(ptr as *const T).add(physical_idx) },
            DataRef::SequenceI64 { start, increment } => {
                assert_eq!(std::mem::size_of::<T>(), std::mem::size_of::<i64>());
                let value = start + physical_idx as i64 * increment;
                unsafe { std::ptr::read_unaligned((&value as *const i64).cast::<T>()) }
            }
        }
    }
}

/// Owned vector view for callers that need a lifetime-free, self-contained view.
///
/// This mirrors [`VectorView`], but owns or materializes the pieces needed to
/// stay valid after the source vector borrow ends.
pub struct DecodedVectorOwned {
    sel: SelectionVector,
    data: *const u8,
    validity: ValidityMask,
    owned: Option<VectorBuffer>,
}

impl std::fmt::Debug for DecodedVectorOwned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodedVectorOwned")
            .field("sel", &self.sel)
            .field("data", &format!("{:p}", self.data))
            .field("validity", &self.validity)
            .finish()
    }
}

// SAFETY: DecodedVectorOwned only exposes immutable access to data owned by the
// source vector or by `owned`.
unsafe impl Send for DecodedVectorOwned {}
unsafe impl Sync for DecodedVectorOwned {}

impl DecodedVectorOwned {
    #[inline]
    pub fn sel(&self) -> &SelectionVector {
        &self.sel
    }

    #[inline]
    pub fn physical_index(&self, idx: usize) -> usize {
        self.sel.get(idx)
    }

    #[inline]
    pub fn data(&self) -> *const u8 {
        self.data
    }

    #[inline]
    pub fn get_data<T>(&self) -> *const T {
        self.data as *const T
    }

    #[inline]
    pub fn set_data(&mut self, data: *const u8) {
        self.data = data;
    }

    #[inline]
    pub fn validity(&self) -> &ValidityMask {
        &self.validity
    }

    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        self.validity.is_valid(self.physical_index(idx))
    }

    #[inline]
    /// # Safety
    ///
    /// The caller must ensure the decoded buffer points to a contiguous region
    /// of initialized `T` values and that `idx` resolves to an in-bounds row.
    pub unsafe fn get_value<T: Copy>(&self, idx: usize) -> T {
        let physical_idx = self.physical_index(idx);
        let ptr = self.data as *const T;
        unsafe { *ptr.add(physical_idx) }
    }
}

pub struct DecodedVectorTree {
    pub view: DecodedVectorOwned,
    pub children: Vec<DecodedVectorTree>,
    pub logical_type: LogicalType,
}

impl Vector {
    pub fn try_to_view(&self, count: usize) -> Result<VectorView<'_>> {
        match self.vector_type {
            VectorType::Flat => Ok(VectorView {
                logical_type: &self.logical_type,
                sel: SelectionRef::Incremental { count },
                validity: ValidityRef::Borrowed(&self.validity),
                data: DataRef::Ptr(self.buffer.data()),
                physical_count: self.len(),
                _vector: PhantomData,
            }),
            VectorType::Constant => Ok(VectorView {
                logical_type: &self.logical_type,
                sel: SelectionRef::Constant { index: 0, count },
                validity: ValidityRef::Borrowed(&self.validity),
                data: DataRef::Ptr(self.buffer.data()),
                physical_count: 1,
                _vector: PhantomData,
            }),
            VectorType::Dictionary => {
                let child = self.child.as_ref().expect("dictionary child");
                let child_count = child.len().max(self.selection.len());
                let child_view = child.try_to_view(child_count)?;
                Ok(VectorView {
                    logical_type: &self.logical_type,
                    sel: child_view.sel.try_compose(&self.selection, count)?,
                    validity: child_view.validity,
                    data: child_view.data,
                    physical_count: child_view.physical_count,
                    _vector: PhantomData,
                })
            }
            VectorType::Sequence => {
                let (start, increment) = unsafe {
                    let ptr = self.buffer.data() as *const i64;
                    (*ptr, *ptr.add(1))
                };
                Ok(VectorView {
                    logical_type: &self.logical_type,
                    sel: SelectionRef::Incremental { count },
                    validity: ValidityRef::Borrowed(&self.validity),
                    data: DataRef::SequenceI64 { start, increment },
                    physical_count: count,
                    _vector: PhantomData,
                })
            }
        }
    }

    pub fn try_to_varlen_view(&self, count: usize) -> Result<VarlenView<'_>> {
        let view = self.try_to_view(count)?;
        if view.logical_type.physical_type() != PhysicalType::Varchar {
            return Err(paro_error::type_mismatch(format!(
                "varlen view requires VARCHAR physical storage, got {:?}",
                view.logical_type
            )));
        }
        let DataRef::Ptr(entries) = view.data else {
            panic!("to_varlen_view requires pointer-backed data");
        };
        Ok(VarlenView {
            logical_type: view.logical_type,
            entries: entries as *const StringView,
            sel: view.sel,
            validity: view.validity,
            _vector: PhantomData,
        })
    }

    /// Decode a textual varlen vector and prove its UTF-8 invariant once.
    #[inline]
    pub fn try_to_utf8_view(&self, count: usize) -> Result<Utf8View<'_>> {
        self.try_to_varlen_view(count)?.try_as_utf8()
    }

    pub fn try_to_array_view(&self, count: usize) -> Result<ArrayView<'_>> {
        let LogicalType::Array(_, array_size) = &self.logical_type else {
            panic!("to_array_view requires array logical type");
        };
        let child = ArrayVector::get_entry(self);
        Ok(ArrayView {
            parent: self.try_to_view(count)?,
            child: child.try_to_view(child.len())?,
            array_size: *array_size,
        })
    }

    pub fn try_decode_ref(&self, count: usize) -> Result<DecodedVectorRef<'_>> {
        let view = self.try_to_view(count)?;
        Ok(DecodedVectorRef {
            sel: view.sel,
            data: view.data,
            validity: view.validity,
            physical_count: view.physical_count,
        })
    }

    pub fn try_decode_tree(&self, count: usize) -> Result<DecodedVectorTree> {
        let mut data = DecodedVectorTree {
            view: self.try_decode(count)?,
            children: Vec::new(),
            logical_type: self.logical_type.clone(),
        };

        match &self.logical_type {
            LogicalType::Array(_, array_size) => {
                if let Some(child) = &self.child {
                    let child_count = count * array_size;
                    data.children.push(child.try_decode_tree(child_count)?);
                }
            }
            LogicalType::List(_) => {
                if let Some(child) = &self.child {
                    data.children.push(child.try_decode_tree(child.len())?);
                }
            }
            LogicalType::Struct(_) => {
                if let Some(children) = self.children() {
                    for child in children.iter() {
                        data.children.push(child.try_decode_tree(count)?);
                    }
                }
            }
            _ => {}
        }

        Ok(data)
    }

    pub fn try_decode(&self, count: usize) -> Result<DecodedVectorOwned> {
        match self.vector_type {
            VectorType::Flat => Ok(DecodedVectorOwned {
                sel: SelectionVector::try_incremental(count, self.buffer.allocator().clone())?,
                data: self.buffer.data(),
                validity: self.validity.clone(),
                owned: None,
            }),
            VectorType::Constant => Ok(DecodedVectorOwned {
                sel: SelectionVector::try_constant(count, self.buffer.allocator().clone())?,
                data: self.buffer.data(),
                validity: self.validity.clone(),
                owned: None,
            }),
            VectorType::Dictionary => {
                let child = self.child.as_ref().expect("dictionary child");
                let child_count = child.len().max(self.selection.len());
                let child_view = child.try_decode(child_count)?;
                let child_selection = VectorSelection::Materialized(child_view.sel);
                let selection = child_selection
                    .try_compose(self.selection.clone())
                    .and_then(|selection| {
                        selection.try_materialize(self.buffer.allocator().clone())
                    })?;
                Ok(DecodedVectorOwned {
                    sel: if selection.len() == count {
                        selection
                    } else {
                        selection.try_slice_range(0, count)?
                    },
                    data: child_view.data,
                    validity: child_view.validity,
                    owned: child_view.owned,
                })
            }
            VectorType::Sequence => {
                let (start, increment) = unsafe {
                    let ptr = self.buffer.data() as *const i64;
                    (*ptr, *ptr.add(1))
                };

                let owned = VectorBuffer::try_with_allocator(
                    std::mem::size_of::<i64>(),
                    count,
                    self.buffer.allocator().clone(),
                )?;

                unsafe {
                    let dst = owned.data() as *mut i64;
                    for i in 0..count {
                        *dst.add(i) = start + i as i64 * increment;
                    }
                }

                Ok(DecodedVectorOwned {
                    sel: SelectionVector::try_incremental(count, self.buffer.allocator().clone())?,
                    data: owned.data(),
                    validity: self.validity.clone(),
                    owned: Some(owned),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::types::LogicalType;

    #[test]
    fn flat_to_view_borrows_validity_and_avoids_owned_selection() {
        let vector = crate::test_utils::test_i64_vector(&[10, 20, 30]);
        let view = vector.try_to_view(3).unwrap();

        assert!(matches!(view.sel(), SelectionRef::Incremental { count: 3 }));
        assert!(matches!(view.validity(), ValidityRef::Borrowed(_)));
        assert_eq!(view.get_i64(2), 30);
    }

    #[test]
    fn constant_to_view_uses_constant_selection() {
        let vector = crate::test_utils::test_constant(LogicalType::BigInt, 42_i64, 4);
        let view = vector.try_to_view(4).unwrap();

        assert!(matches!(
            view.sel(),
            SelectionRef::Constant { index: 0, count: 4 }
        ));
        assert_eq!(view.get_i64(0), 42);
        assert_eq!(view.get_i64(3), 42);
    }

    #[test]
    fn dictionary_to_view_collapses_nested_selection() {
        let base = Arc::new(crate::test_utils::test_i64_vector(&[10, 20, 30, 40]));
        let first = Arc::new(crate::test_utils::test_dictionary(base, vec![3_u32, 1, 2]));
        let nested = crate::test_utils::test_dictionary(
            first,
            crate::test_utils::test_selection(vec![1, 2]),
        );
        let selection_allocation = nested
            .sel_vector()
            .expect("canonical dictionary selection")
            .allocation_identity();
        let view = nested.try_to_view(2).unwrap();

        assert_eq!(view.get_i64(0), 20);
        assert_eq!(view.get_i64(1), 30);
        assert_eq!(view.sel().allocation_identity(), selection_allocation);
    }

    #[test]
    fn range_dictionary_to_view_stays_borrowed_range() {
        let vector = crate::test_utils::test_i64_vector(&[10, 20, 30, 40]);
        let sliced = vector.slice_ref(1, 2).expect("range slice");
        let view = sliced.try_to_view(2).unwrap();

        assert!(matches!(
            view.sel(),
            SelectionRef::Range {
                offset: 1,
                count: 2
            }
        ));
        assert_eq!(view.get_i64(0), 20);
        assert_eq!(view.get_i64(1), 30);
    }

    #[test]
    fn decode_ref_keeps_range_selection() {
        let vector = crate::test_utils::test_i64_vector(&[10, 20, 30, 40]);
        let sliced = vector.slice_ref(1, 3).expect("range slice");
        let decoded = sliced.try_decode_ref(3).unwrap();

        assert!(matches!(
            decoded.sel(),
            SelectionRef::Range {
                offset: 1,
                count: 3
            }
        ));
        assert!(decoded.is_valid(2));
    }

    #[test]
    fn decode_ref_hot_paths_keep_symbolic_selection() {
        let flat = crate::test_utils::test_i64_vector(&[1, 2, 3]);
        let flat_decoded = flat.try_decode_ref(3).unwrap();
        assert!(matches!(
            flat_decoded.sel(),
            SelectionRef::Incremental { count: 3 }
        ));

        let constant = crate::test_utils::test_constant(LogicalType::BigInt, 9_i64, 4);
        let constant_decoded = constant.try_decode_ref(4).unwrap();
        assert!(matches!(
            constant_decoded.sel(),
            SelectionRef::Constant { index: 0, count: 4 }
        ));

        let range = flat.slice_ref(1, 2).expect("range slice");
        let range_decoded = range.try_decode_ref(2).unwrap();
        assert!(matches!(
            range_decoded.sel(),
            SelectionRef::Range {
                offset: 1,
                count: 2
            }
        ));
    }

    #[test]
    fn sequence_to_view_uses_sequence_data_ref() {
        let vector = crate::test_utils::test_sequence(7, 3, 5);
        let view = vector.try_to_view(5).unwrap();

        assert!(matches!(
            view.data(),
            DataRef::SequenceI64 {
                start: 7,
                increment: 3
            }
        ));
        assert_eq!(view.get_i64(4), 19);
    }

    #[test]
    fn varlen_view_reads_dictionary_entries() {
        let base = Arc::new(crate::test_utils::test_string_vector(&[
            "alpha", "beta", "gamma",
        ]));
        let dictionary = crate::test_utils::test_dictionary(base, vec![2_u32, 0]);
        let view = dictionary.try_to_varlen_view(2).unwrap();

        assert_eq!(std::str::from_utf8(view.bytes(0)).unwrap(), "gamma");
        assert_eq!(std::str::from_utf8(view.bytes(1)).unwrap(), "alpha");
    }

    #[test]
    fn utf8_view_proves_text_type_once() {
        let mut vector =
            Vector::try_new(LogicalType::Varchar, 2, crate::test_utils::test_allocator()).unwrap();
        vector.try_set_count(2).unwrap();
        vector.try_set_string(0, "short").unwrap();
        vector
            .try_set_string(1, "a longer UTF-8 value 你好")
            .unwrap();

        let view = vector.try_to_utf8_view(2).unwrap();
        assert_eq!(view.str(0), "short");
        assert_eq!(view.str(1), "a longer UTF-8 value 你好");
    }

    #[test]
    fn utf8_and_binary_safe_apis_reject_cross_type_access() {
        let mut blob =
            Vector::try_new(LogicalType::Blob, 1, crate::test_utils::test_allocator()).unwrap();
        blob.try_set_blob(0, &[0xff]).unwrap();
        assert!(blob.try_to_utf8_view(1).is_err());
        assert!(blob.try_set_string(0, "text").is_err());

        let mut text =
            Vector::try_new(LogicalType::Varchar, 1, crate::test_utils::test_allocator()).unwrap();
        assert!(text.try_set_blob(0, &[0xff]).is_err());

        let fixed = crate::test_utils::test_i64_vector(&[1]);
        assert!(fixed.try_to_varlen_view(1).is_err());
    }

    #[test]
    fn array_view_uses_parent_selection_stride() {
        let vector =
            crate::test_utils::test_embeddings_vector(&[vec![1.0_f32, 2.0], vec![3.0, 4.0]], 2);
        let dictionary = crate::test_utils::test_dictionary(Arc::new(vector), vec![1_u32]);
        let view = dictionary.try_to_array_view(1).unwrap();

        assert_eq!(view.array_size(), 2);
        assert_eq!(view.logical_child_index(0, 0), 2);
        assert_eq!(view.logical_child_index(0, 1), 3);
        assert_eq!(view.physical_child_index(0, 0), 2);
        assert_eq!(view.physical_child_index(0, 1), 3);
    }

    #[test]
    fn owned_view_materializes_sequence_once() {
        let vector = crate::test_utils::test_sequence(7, 3, 4);
        let view = vector.try_decode(4).unwrap();

        unsafe {
            assert_eq!(view.get_value::<i64>(0), 7);
            assert_eq!(view.get_value::<i64>(3), 16);
        }
    }

    #[test]
    fn owned_view_collapses_nested_dictionary_selection() {
        let base = Arc::new(crate::test_utils::test_i32_vector(&[10, 20, 30, 40]));
        let first = Arc::new(crate::test_utils::test_dictionary(base, vec![3_u32, 1, 2]));
        let nested = crate::test_utils::test_dictionary(
            first,
            crate::test_utils::test_selection(vec![1, 2]),
        );
        let view = nested.try_decode(2).unwrap();

        unsafe {
            assert_eq!(view.get_value::<i32>(0), 20);
            assert_eq!(view.get_value::<i32>(1), 30);
        }
    }
}
