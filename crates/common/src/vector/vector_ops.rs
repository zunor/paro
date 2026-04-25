// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{StringHeap, ValidityMask, Vector, VectorType};
use crate::allocator::Allocator;
use crate::error::{self as paro_error, ParoError};
use crate::types::{InlineString, LogicalType};
use std::sync::Arc;

impl Vector {
    /// Merge two compact vectors based on a boolean mask.
    ///
    /// When `mask[i]` is true this consumes the next row from `true_vec`,
    /// otherwise it consumes the next row from `false_vec`.
    pub fn try_merge(
        logical_type: LogicalType,
        count: usize,
        mask: &[bool],
        true_vec: &Vector,
        false_vec: &Vector,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self, ParoError> {
        Self::try_merge_internal(
            logical_type,
            count,
            mask,
            true_vec,
            false_vec,
            allocator,
            false,
        )
    }

    /// Merge two vectors of full length based on a boolean mask.
    /// result[i] = mask[i] ? true_vec[i] : false_vec[i]
    pub fn try_merge_full(
        logical_type: LogicalType,
        count: usize,
        mask: &[bool],
        true_vec: &Vector,
        false_vec: &Vector,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self, ParoError> {
        Self::try_merge_internal(
            logical_type,
            count,
            mask,
            true_vec,
            false_vec,
            allocator,
            true,
        )
    }

    fn try_merge_internal(
        logical_type: LogicalType,
        count: usize,
        mask: &[bool],
        true_vec: &Vector,
        false_vec: &Vector,
        allocator: Arc<dyn Allocator>,
        full_length_inputs: bool,
    ) -> Result<Self, ParoError> {
        if mask.len() < count {
            return Err(paro_error::internal(format!(
                "merge mask too short: count={count}, mask_len={}",
                mask.len()
            )));
        }

        let mut result = Self::try_new(logical_type.clone(), count, allocator.clone())?;
        result.count = count;
        result.validity = ValidityMask::with_allocator(count, result.buffer.allocator().clone());

        let mut true_idx = 0;
        let mut false_idx = 0;

        macro_rules! merge_loop {
            ($type:ty, $get_fn:ident) => {
                unsafe {
                    let res_ptr = result.buffer.data() as *mut $type;
                    for (i, &take_true) in mask.iter().take(count).enumerate() {
                        let (vec, source_idx) = Self::merge_source_row(
                            take_true,
                            i,
                            full_length_inputs,
                            &mut true_idx,
                            &mut false_idx,
                            true_vec,
                            false_vec,
                        );
                        Self::check_merge_source_bounds(source_idx, vec, full_length_inputs)?;
                        match vec.$get_fn(source_idx) {
                            Some(val) => *res_ptr.add(i) = val,
                            None => result.validity.try_set_null(i)?,
                        }
                    }
                }
            };
        }

        match &logical_type {
            LogicalType::Boolean => merge_loop!(bool, get_bool),
            LogicalType::TinyInt => merge_loop!(i8, get_i8),
            LogicalType::SmallInt => merge_loop!(i16, get_i16),
            LogicalType::Integer | LogicalType::Date => merge_loop!(i32, get_i32),
            LogicalType::BigInt
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Time => merge_loop!(i64, get_i64),
            LogicalType::HugeInt | LogicalType::Interval => merge_loop!(i128, get_i128),
            LogicalType::UTinyInt => merge_loop!(u8, get_u8),
            LogicalType::USmallInt => merge_loop!(u16, get_u16),
            LogicalType::UInteger => merge_loop!(u32, get_u32),
            LogicalType::UBigInt => merge_loop!(u64, get_u64),
            LogicalType::UHugeInt | LogicalType::Uuid => merge_loop!(u128, get_u128),
            LogicalType::Float => merge_loop!(f32, get_f32),
            LogicalType::Double => merge_loop!(f64, get_f64),
            LogicalType::Decimal { precision, .. } if *precision <= 18 => {
                merge_loop!(i64, get_i64)
            }
            LogicalType::Decimal { .. } => merge_loop!(i128, get_i128),
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob => {
                let mut heap: Option<StringHeap> = None;
                unsafe {
                    let entries = result.buffer.data() as *mut InlineString;
                    for (i, &take_true) in mask.iter().take(count).enumerate() {
                        let (vec, source_idx) = Self::merge_source_row(
                            take_true,
                            i,
                            full_length_inputs,
                            &mut true_idx,
                            &mut false_idx,
                            true_vec,
                            false_vec,
                        );
                        Self::check_merge_source_bounds(source_idx, vec, full_length_inputs)?;
                        match Self::varlen_bytes(&logical_type, vec, source_idx) {
                            Some(bytes) => {
                                *entries.add(i) =
                                    Self::copy_varlen_entry(bytes, &mut heap, allocator.clone())?;
                            }
                            None => {
                                result.validity.try_set_null(i)?;
                                *entries.add(i) = InlineString::empty();
                            }
                        }
                    }
                }
                result.string_heap = heap.map(Arc::new);
            }
            LogicalType::Null => result.validity.try_set_range_invalid(0, count)?,
            _ => {
                return Err(paro_error::not_implemented(format!(
                    "Merge not implemented for type {:?}",
                    logical_type
                )))
            }
        }

        Ok(result)
    }

    #[inline]
    fn merge_source_row<'a>(
        take_true: bool,
        result_idx: usize,
        full_length_inputs: bool,
        true_idx: &mut usize,
        false_idx: &mut usize,
        true_vec: &'a Vector,
        false_vec: &'a Vector,
    ) -> (&'a Vector, usize) {
        if full_length_inputs {
            return (if take_true { true_vec } else { false_vec }, result_idx);
        }

        if take_true {
            let source_idx = *true_idx;
            *true_idx += 1;
            (true_vec, source_idx)
        } else {
            let source_idx = *false_idx;
            *false_idx += 1;
            (false_vec, source_idx)
        }
    }

    #[inline]
    fn check_merge_source_bounds(
        source_idx: usize,
        source: &Vector,
        full_length_inputs: bool,
    ) -> Result<(), ParoError> {
        if source_idx < source.len() {
            return Ok(());
        }

        let mode = if full_length_inputs {
            "full"
        } else {
            "compact"
        };
        Err(paro_error::internal(format!(
            "merge {mode} source index out of bounds: idx={source_idx}, len={}",
            source.len()
        )))
    }

    /// Fallible flatten that materializes dictionary/sequence sources and allocates buffers when needed.
    pub fn try_flatten(&mut self) -> Result<(), ParoError> {
        if self.vector_type == VectorType::Flat {
            self.try_flatten_children()?;
            return Ok(());
        }

        let count = self.count;
        let source = self.clone();
        let allocator = self.buffer.allocator().clone();
        let mut result = Self::try_new(self.logical_type.clone(), count, allocator)?;
        result.try_set_count(count)?;
        result.try_copy_range(0, &source, 0, count)?;
        *self = result;
        Ok(())
    }

    fn try_flatten_children(&mut self) -> Result<(), ParoError> {
        match &self.logical_type {
            LogicalType::Array(_, array_size) => {
                if let Some(child_arc) = &mut self.child {
                    let child_count = self.count.checked_mul(*array_size).ok_or_else(|| {
                        paro_error::internal(format!(
                            "array child count overflow: count={}, array_size={array_size}",
                            self.count
                        ))
                    })?;
                    let child = Self::try_make_arc_mut(child_arc)?;
                    child.try_flatten()?;
                    child.try_set_count(child_count)?;
                }
            }
            LogicalType::List(_) => {
                if let Some(child_arc) = &mut self.child {
                    Self::try_make_arc_mut(child_arc)?.try_flatten()?;
                }
            }
            LogicalType::Struct(_) => {
                for child_arc in &mut self.children {
                    let child = Self::try_make_arc_mut(child_arc)?;
                    child.try_flatten()?;
                    child.try_set_count(self.count)?;
                }
            }
            _ => {}
        }

        Ok(())
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
