use super::{StringHeap, ValidityMask, Vector, VectorBuffer, VectorType};
use crate::allocator::default_allocator;
use crate::error::{self as paro_error, ParoError};
use crate::types::{InlineString, LogicalType, INLINE_LENGTH};
use std::sync::Arc;

impl Vector {
    /// Merge two vectors based on a boolean mask.
    pub fn merge(
        logical_type: LogicalType,
        count: usize,
        mask: &[bool],
        true_vec: &Vector,
        false_vec: &Vector,
    ) -> Result<Self, ParoError> {
        let allocator = Arc::new(default_allocator());
        let mut result =
            Self::with_capacity_and_allocator(logical_type.clone(), count, allocator.clone());
        result.count = count;
        result.validity = ValidityMask::with_allocator(count, result.buffer.allocator().clone());

        let mut true_idx = 0;
        let mut false_idx = 0;

        macro_rules! merge_loop {
            ($type:ty, $get_fn:ident) => {
                unsafe {
                    let res_ptr = result.buffer.data() as *mut $type;
                    for (i, &m) in mask.iter().enumerate().take(count) {
                        if m {
                            match true_vec.$get_fn(true_idx) {
                                Some(val) => *res_ptr.add(i) = val,
                                None => result.validity.set_null(i),
                            }
                            true_idx += 1;
                        } else {
                            match false_vec.$get_fn(false_idx) {
                                Some(val) => *res_ptr.add(i) = val,
                                None => result.validity.set_null(i),
                            }
                            false_idx += 1;
                        }
                    }
                }
            };
        }

        match logical_type {
            LogicalType::Boolean => merge_loop!(bool, get_bool),
            LogicalType::TinyInt => merge_loop!(i8, get_i8),
            LogicalType::SmallInt => merge_loop!(i16, get_i16),
            LogicalType::Integer => merge_loop!(i32, get_i32),
            LogicalType::BigInt => merge_loop!(i64, get_i64),
            LogicalType::UTinyInt => merge_loop!(u8, get_u8),
            LogicalType::USmallInt => merge_loop!(u16, get_u16),
            LogicalType::UInteger => merge_loop!(u32, get_u32),
            LogicalType::UBigInt => merge_loop!(u64, get_u64),
            LogicalType::Float => merge_loop!(f32, get_f32),
            LogicalType::Double => merge_loop!(f64, get_f64),
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb => {
                let mut heap: Option<StringHeap> = None;
                let buffer = VectorBuffer::with_allocator(
                    std::mem::size_of::<InlineString>(),
                    count,
                    allocator.clone(),
                );
                unsafe {
                    let entries = buffer.data() as *mut InlineString;
                    for (i, &m) in mask.iter().enumerate().take(count) {
                        let s = if m {
                            let s = true_vec.get_string(true_idx);
                            true_idx += 1;
                            s
                        } else {
                            let s = false_vec.get_string(false_idx);
                            false_idx += 1;
                            s
                        };

                        match s {
                            Some(str_val) => {
                                if str_val.len() <= INLINE_LENGTH {
                                    *entries.add(i) = InlineString::new(str_val);
                                } else {
                                    if heap.is_none() {
                                        heap = Some(StringHeap::with_allocator(
                                            1024,
                                            allocator.clone(),
                                        ));
                                    }
                                    let h = heap.as_mut().unwrap();
                                    // add_string returns InlineString with pointer to arena memory
                                    *entries.add(i) = h.add_string(str_val);
                                }
                            }
                            None => {
                                result.validity.set_null(i);
                                *entries.add(i) = InlineString::empty();
                            }
                        }
                    }
                }
                result.buffer = buffer;
                result.string_heap = heap.map(Arc::new);
            }
            _ => {
                return Err(paro_error::not_implemented(format!(
                    "Merge not implemented for type {:?}",
                    logical_type
                )))
            }
        }

        Ok(result)
    }

    /// Merge two vectors of full length based on a boolean mask.
    /// result[i] = mask[i] ? true_vec[i] : false_vec[i]
    pub fn merge_full(
        logical_type: LogicalType,
        count: usize,
        mask: &[bool],
        true_vec: &Vector,
        false_vec: &Vector,
    ) -> Result<Self, ParoError> {
        let allocator = Arc::new(default_allocator());
        let mut result =
            Self::with_capacity_and_allocator(logical_type.clone(), count, allocator.clone());
        result.count = count;
        result.validity = ValidityMask::with_allocator(count, result.buffer.allocator().clone());

        macro_rules! merge_full_loop {
            ($type:ty, $get_fn:ident) => {
                unsafe {
                    let res_ptr = result.buffer.data() as *mut $type;
                    for (i, &m) in mask.iter().enumerate().take(count) {
                        let vec = if m { true_vec } else { false_vec };
                        match vec.$get_fn(i) {
                            Some(val) => *res_ptr.add(i) = val,
                            None => result.validity.set_null(i),
                        }
                    }
                }
            };
        }

        match logical_type {
            LogicalType::Boolean => merge_full_loop!(bool, get_bool),
            LogicalType::TinyInt => merge_full_loop!(i8, get_i8),
            LogicalType::SmallInt => merge_full_loop!(i16, get_i16),
            LogicalType::Integer => merge_full_loop!(i32, get_i32),
            LogicalType::BigInt => merge_full_loop!(i64, get_i64),
            LogicalType::Float => merge_full_loop!(f32, get_f32),
            LogicalType::Double => merge_full_loop!(f64, get_f64),
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb => {
                let mut heap: Option<StringHeap> = None;
                let buffer = VectorBuffer::with_allocator(
                    std::mem::size_of::<InlineString>(),
                    count,
                    allocator.clone(),
                );
                unsafe {
                    let entries = buffer.data() as *mut InlineString;
                    for (i, &m) in mask.iter().enumerate().take(count) {
                        let vec = if m { true_vec } else { false_vec };
                        match vec.get_string(i) {
                            Some(str_val) => {
                                if str_val.len() <= INLINE_LENGTH {
                                    *entries.add(i) = InlineString::new(str_val);
                                } else {
                                    if heap.is_none() {
                                        heap = Some(StringHeap::with_allocator(
                                            1024,
                                            allocator.clone(),
                                        ));
                                    }
                                    let h = heap.as_mut().unwrap();
                                    // add_string returns InlineString with pointer to arena memory
                                    *entries.add(i) = h.add_string(str_val);
                                }
                            }
                            None => {
                                result.validity.set_null(i);
                                *entries.add(i) = InlineString::empty();
                            }
                        }
                    }
                }
                result.buffer = buffer;
                result.string_heap = heap.map(Arc::new);
            }
            _ => {
                return Err(paro_error::not_implemented(format!(
                    "Merge full not implemented for type {:?}",
                    logical_type
                )))
            }
        }

        Ok(result)
    }

    pub fn flatten(&mut self) {
        match self.vector_type {
            VectorType::Flat => {
                // Already flat, but need to flatten nested types
                if let LogicalType::Array(_, array_size) = &self.logical_type {
                    // Flatten the child vector
                    if let Some(child_arc) = &mut self.child {
                        let child = Arc::make_mut(child_arc);
                        let child_count = self.count * array_size;
                        child.flatten();
                        child.set_count(child_count);
                    }
                }
            }
            VectorType::Constant => {
                // Replicate the single value
                let element_size = self.logical_type.physical_size();
                let new_buffer = VectorBuffer::with_allocator(
                    element_size,
                    self.count,
                    self.buffer.allocator().clone(),
                );

                if element_size > 0 && !self.buffer.data().is_null() {
                    // SAFETY: We're copying the constant value to all positions
                    unsafe {
                        let src = self.buffer.data();
                        let dst = new_buffer.data();
                        for i in 0..self.count {
                            std::ptr::copy_nonoverlapping(
                                src,
                                dst.add(i * element_size),
                                element_size,
                            );
                        }
                    }
                }

                // Replicate validity
                let is_null = !self.validity.is_valid(0);
                self.validity =
                    ValidityMask::with_allocator(self.count, self.buffer.allocator().clone());
                if is_null {
                    for i in 0..self.count {
                        self.validity.set_null(i);
                    }
                }

                self.buffer = new_buffer;
                self.vector_type = VectorType::Flat;
                self.dictionary_info = None;
            }
            VectorType::Sequence => {
                // Materialize the sequence
                // SAFETY: Sequence stores [start, increment] as i64
                let (start, increment) = unsafe {
                    let ptr = self.buffer.data() as *const i64;
                    (*ptr, *ptr.add(1))
                };

                let new_buffer = VectorBuffer::with_allocator(
                    std::mem::size_of::<i64>(),
                    self.count,
                    self.buffer.allocator().clone(),
                );

                // SAFETY: We're writing i64 values
                unsafe {
                    let dst = new_buffer.data() as *mut i64;
                    for i in 0..self.count {
                        *dst.add(i) = start + (i as i64) * increment;
                    }
                }

                self.buffer = new_buffer;
                self.vector_type = VectorType::Flat;
                self.dictionary_info = None;
            }
            VectorType::Dictionary => {
                // Materialize through selection vector
                let child = self.child.take().unwrap();
                let sel = self.sel_vector.take().unwrap();
                let element_size = self.logical_type.physical_size();
                let allocator = self.buffer.allocator().clone();

                let new_buffer =
                    VectorBuffer::with_allocator(element_size, self.count, allocator.clone());
                self.validity =
                    ValidityMask::with_allocator(self.count, self.buffer.allocator().clone());

                // SAFETY: We're copying selected elements
                unsafe {
                    let dst = new_buffer.data();
                    let src = child.buffer.data();
                    for i in 0..self.count {
                        let physical_idx = sel.get(i);
                        if !child.validity.is_valid(physical_idx) {
                            self.validity.set_null(i);
                        } else {
                            std::ptr::copy_nonoverlapping(
                                src.add(physical_idx * element_size),
                                dst.add(i * element_size),
                                element_size,
                            );
                        }
                    }
                }

                // Copy string heap if needed (string-like types)
                if matches!(
                    self.logical_type,
                    LogicalType::Varchar
                        | LogicalType::VarcharCollation(_)
                        | LogicalType::TsVector
                        | LogicalType::TsQuery
                        | LogicalType::Json
                        | LogicalType::Jsonb
                ) {
                    let allocator = self.buffer.allocator().clone();
                    let mut new_heap: Option<StringHeap> = None;
                    let string_buffer = VectorBuffer::with_allocator(
                        std::mem::size_of::<InlineString>(),
                        self.count,
                        allocator.clone(),
                    );

                    // SAFETY: We're copying InlineString values and rebuilding heap for long strings
                    unsafe {
                        let src_entries = child.buffer.data() as *const InlineString;
                        let dst_entries = string_buffer.data() as *mut InlineString;

                        for i in 0..self.count {
                            let physical_idx = sel.get(i);
                            if !child.validity.is_valid(physical_idx) {
                                self.validity.set_null(i);
                                *dst_entries.add(i) = InlineString::empty();
                                continue;
                            }

                            let src_str = &*src_entries.add(physical_idx);
                            let str_data = src_str.as_str();

                            if str_data.len() <= INLINE_LENGTH {
                                // Short string: copy inline
                                *dst_entries.add(i) = InlineString::new(str_data);
                            } else {
                                // Long string: copy to new heap
                                if new_heap.is_none() {
                                    new_heap =
                                        Some(StringHeap::with_allocator(1024, allocator.clone()));
                                }
                                let h = new_heap.as_mut().unwrap();
                                // add_string returns InlineString with pointer to arena memory
                                *dst_entries.add(i) = h.add_string(str_data);
                            }
                        }
                    }

                    self.buffer = string_buffer;
                    self.string_heap = new_heap.map(Arc::new);
                } else {
                    self.buffer = new_buffer;
                }

                self.vector_type = VectorType::Flat;
                self.dictionary_info = None;
            }
        }
    }

    /// Convert a FLAT vector with one element to a CONSTANT vector.
    pub fn to_constant(&mut self, count: usize) -> Self {
        debug_assert!(self.count >= 1);
        self.vector_type = VectorType::Constant;
        self.dictionary_info = None;
        self.count = count;
        self.clone()
    }
}
