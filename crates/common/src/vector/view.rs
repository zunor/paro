use std::marker::PhantomData;

use crate::types::{InlineString, LogicalType};

use super::{ArrayVector, SelectionVector, ValidityMask, Vector, VectorBuffer, VectorType};

#[derive(Debug, Clone)]
pub enum SelectionRef<'a> {
    Borrowed(&'a SelectionVector),
    Owned(SelectionVector),
    Constant { count: usize },
    Incremental { count: usize },
}

impl<'a> SelectionRef<'a> {
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Borrowed(sel) => sel.len(),
            Self::Owned(sel) => sel.len(),
            Self::Constant { count } | Self::Incremental { count } => *count,
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
            Self::Constant { .. } => 0,
            Self::Incremental { .. } => idx,
        }
    }

    #[inline]
    pub fn allocation_identity(&self) -> Option<usize> {
        match self {
            Self::Borrowed(sel) => sel.allocation_identity(),
            Self::Owned(sel) => sel.allocation_identity(),
            Self::Constant { .. } | Self::Incremental { .. } => None,
        }
    }

    fn compose(self, sel: &'a SelectionVector, count: usize) -> SelectionRef<'a> {
        match self {
            SelectionRef::Borrowed(child_sel) => SelectionRef::Owned(child_sel.slice(sel, count)),
            SelectionRef::Owned(child_sel) => SelectionRef::Owned(child_sel.slice(sel, count)),
            SelectionRef::Constant { count: _ } => SelectionRef::Constant { count },
            SelectionRef::Incremental { count: _ } => SelectionRef::Borrowed(sel),
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
    fn as_mask(&self) -> &ValidityMask {
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
    entries: *const InlineString,
    sel: SelectionRef<'a>,
    validity: ValidityRef<'a>,
    _vector: PhantomData<&'a Vector>,
}

impl<'a> VarlenView<'a> {
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
    pub fn get_inline_string(&self, idx: usize) -> InlineString {
        unsafe { *self.entries.add(self.sel.get(idx)) }
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

/// Owned vector view for callers that need a lifetime-free, self-contained view.
///
/// This mirrors [`VectorView`], but owns or materializes the pieces needed to
/// stay valid after the source vector borrow ends.
pub struct DecodedVector {
    sel: SelectionVector,
    data: *const u8,
    validity: ValidityMask,
    owned: Option<VectorBuffer>,
}

impl std::fmt::Debug for DecodedVector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodedVector")
            .field("sel", &self.sel)
            .field("data", &format!("{:p}", self.data))
            .field("validity", &self.validity)
            .finish()
    }
}

// SAFETY: DecodedVector only exposes immutable access to data owned by the
// source vector or by `owned`.
unsafe impl Send for DecodedVector {}
unsafe impl Sync for DecodedVector {}

impl DecodedVector {
    pub fn empty() -> Self {
        Self {
            sel: SelectionVector::with_capacity(0),
            data: std::ptr::null(),
            validity: ValidityMask::new(0),
            owned: None,
        }
    }

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
    pub view: DecodedVector,
    pub children: Vec<DecodedVectorTree>,
    pub logical_type: LogicalType,
}

impl DecodedVectorTree {
    pub fn empty() -> Self {
        Self {
            view: DecodedVector::empty(),
            children: Vec::new(),
            logical_type: LogicalType::Null,
        }
    }
}

impl Vector {
    pub fn to_view(&self, count: usize) -> VectorView<'_> {
        match self.vector_type {
            VectorType::Flat => VectorView {
                logical_type: &self.logical_type,
                sel: SelectionRef::Incremental { count },
                validity: ValidityRef::Borrowed(&self.validity),
                data: DataRef::Ptr(self.buffer.data()),
                _vector: PhantomData,
            },
            VectorType::Constant => VectorView {
                logical_type: &self.logical_type,
                sel: SelectionRef::Constant { count },
                validity: ValidityRef::Borrowed(&self.validity),
                data: DataRef::Ptr(self.buffer.data()),
                _vector: PhantomData,
            },
            VectorType::Dictionary => {
                let child = self.child.as_ref().expect("dictionary child");
                let sel = self.sel_vector.as_ref().expect("dictionary selection");
                let child_count = child.len().max(sel.len());
                let child_view = child.to_view(child_count);
                VectorView {
                    logical_type: &self.logical_type,
                    sel: child_view.sel.compose(sel, count),
                    validity: child_view.validity,
                    data: child_view.data,
                    _vector: PhantomData,
                }
            }
            VectorType::Sequence => {
                let (start, increment) = unsafe {
                    let ptr = self.buffer.data() as *const i64;
                    (*ptr, *ptr.add(1))
                };
                VectorView {
                    logical_type: &self.logical_type,
                    sel: SelectionRef::Incremental { count },
                    validity: ValidityRef::Borrowed(&self.validity),
                    data: DataRef::SequenceI64 { start, increment },
                    _vector: PhantomData,
                }
            }
        }
    }

    pub fn to_varlen_view(&self, count: usize) -> VarlenView<'_> {
        let view = self.to_view(count);
        let DataRef::Ptr(entries) = view.data else {
            panic!("to_varlen_view requires pointer-backed data");
        };
        VarlenView {
            entries: entries as *const InlineString,
            sel: view.sel,
            validity: view.validity,
            _vector: PhantomData,
        }
    }

    pub fn to_array_view(&self, count: usize) -> ArrayView<'_> {
        let LogicalType::Array(_, array_size) = &self.logical_type else {
            panic!("to_array_view requires array logical type");
        };
        let child = ArrayVector::get_entry(self);
        ArrayView {
            parent: self.to_view(count),
            child: child.to_view(child.len()),
            array_size: *array_size,
        }
    }

    pub fn decode_tree(&self, count: usize) -> DecodedVectorTree {
        let mut data = DecodedVectorTree {
            view: self.decode(count),
            children: Vec::new(),
            logical_type: self.logical_type.clone(),
        };

        match &self.logical_type {
            LogicalType::Array(_, array_size) => {
                if let Some(child) = &self.child {
                    let child_count = count * array_size;
                    data.children.push(child.decode_tree(child_count));
                }
            }
            LogicalType::List(_) => {
                if let Some(child) = &self.child {
                    data.children.push(child.decode_tree(child.len()));
                }
            }
            LogicalType::Struct(_) => {
                if let Some(children) = self.children() {
                    for child in children.iter() {
                        data.children.push(child.decode_tree(count));
                    }
                }
            }
            _ => {}
        }

        data
    }

    pub fn decode(&self, count: usize) -> DecodedVector {
        match self.vector_type {
            VectorType::Flat => DecodedVector {
                sel: SelectionVector::incremental(count),
                data: self.buffer.data(),
                validity: self.validity.clone(),
                owned: None,
            },
            VectorType::Constant => DecodedVector {
                sel: SelectionVector::constant(count),
                data: self.buffer.data(),
                validity: self.validity.clone(),
                owned: None,
            },
            VectorType::Dictionary => {
                let child = self.child.as_ref().expect("dictionary child");
                let sel = self.sel_vector.as_ref().expect("dictionary selection");
                let child_count = child.len().max(sel.len());
                let child_view = child.decode(child_count);
                DecodedVector {
                    sel: child_view.sel.slice(sel, count),
                    data: child_view.data,
                    validity: child_view.validity,
                    owned: child_view.owned,
                }
            }
            VectorType::Sequence => {
                let (start, increment) = unsafe {
                    let ptr = self.buffer.data() as *const i64;
                    (*ptr, *ptr.add(1))
                };

                let owned = VectorBuffer::with_allocator(
                    std::mem::size_of::<i64>(),
                    count,
                    self.buffer.allocator().clone(),
                );

                unsafe {
                    let dst = owned.data() as *mut i64;
                    for i in 0..count {
                        *dst.add(i) = start + i as i64 * increment;
                    }
                }

                DecodedVector {
                    sel: SelectionVector::incremental(count),
                    data: owned.data(),
                    validity: self.validity.clone(),
                    owned: Some(owned),
                }
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
        let vector = Vector::from_i64(&[10, 20, 30]);
        let view = vector.to_view(3);

        assert!(matches!(view.sel(), SelectionRef::Incremental { count: 3 }));
        assert!(matches!(view.validity(), ValidityRef::Borrowed(_)));
        assert_eq!(view.get_i64(2), 30);
    }

    #[test]
    fn constant_to_view_uses_constant_selection() {
        let vector = Vector::constant(LogicalType::BigInt, 42_i64, 4);
        let view = vector.to_view(4);

        assert!(matches!(view.sel(), SelectionRef::Constant { count: 4 }));
        assert_eq!(view.get_i64(0), 42);
        assert_eq!(view.get_i64(3), 42);
    }

    #[test]
    fn dictionary_to_view_collapses_nested_selection() {
        let base = Arc::new(Vector::from_i64(&[10, 20, 30, 40]));
        let first = Arc::new(Vector::dictionary(base, vec![3_u32, 1, 2]));
        let nested = Vector::dictionary(first, SelectionVector::from_indices(vec![1, 2]));
        let selection_allocation = nested
            .sel_vector()
            .expect("canonical dictionary selection")
            .allocation_identity();
        let view = nested.to_view(2);

        assert_eq!(view.get_i64(0), 20);
        assert_eq!(view.get_i64(1), 30);
        assert_eq!(view.sel().allocation_identity(), selection_allocation);
    }

    #[test]
    fn sequence_to_view_uses_sequence_data_ref() {
        let vector = Vector::sequence(7, 3, 5);
        let view = vector.to_view(5);

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
        let base = Arc::new(Vector::from_strings(&["alpha", "beta", "gamma"]));
        let dictionary = Vector::dictionary(base, vec![2_u32, 0]);
        let view = dictionary.to_varlen_view(2);

        assert_eq!(view.get_inline_string(0).as_str(), "gamma");
        assert_eq!(view.get_inline_string(1).as_str(), "alpha");
    }

    #[test]
    fn array_view_uses_parent_selection_stride() {
        let vector = Vector::from_embeddings(&[vec![1.0_f32, 2.0], vec![3.0, 4.0]], 2);
        let dictionary = Vector::dictionary(Arc::new(vector), vec![1_u32]);
        let view = dictionary.to_array_view(1);

        assert_eq!(view.array_size(), 2);
        assert_eq!(view.logical_child_index(0, 0), 2);
        assert_eq!(view.logical_child_index(0, 1), 3);
        assert_eq!(view.physical_child_index(0, 0), 2);
        assert_eq!(view.physical_child_index(0, 1), 3);
    }

    #[test]
    fn owned_view_materializes_sequence_once() {
        let vector = Vector::sequence(7, 3, 4);
        let view = vector.decode(4);

        unsafe {
            assert_eq!(view.get_value::<i64>(0), 7);
            assert_eq!(view.get_value::<i64>(3), 16);
        }
    }

    #[test]
    fn owned_view_collapses_nested_dictionary_selection() {
        let base = Arc::new(Vector::from_i32(&[10, 20, 30, 40]));
        let first = Arc::new(Vector::dictionary(base, vec![3_u32, 1, 2]));
        let nested = Vector::dictionary(first, SelectionVector::from_indices(vec![1, 2]));
        let view = nested.decode(2);

        unsafe {
            assert_eq!(view.get_value::<i32>(0), 20);
            assert_eq!(view.get_value::<i32>(1), 30);
        }
    }
}
