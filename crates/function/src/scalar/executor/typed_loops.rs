// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::TypeId;

use paro_common::error::Result;
use paro_common::vector::{
    DataRef, SelectionRef, SelectionVector, ValidityRef, Vector, VectorType, VectorView,
};

use crate::scalar::executor::{BinaryOperator, TernaryOperator, UnaryOperator};

#[derive(Debug, Clone, Copy)]
struct SequenceMeta {
    start: i64,
    increment: i64,
}

#[inline]
pub(crate) fn prepare_result(
    result: &mut Vector,
    vector_type: VectorType,
    count: usize,
) -> Result<()> {
    let allocator = result.allocator().clone();
    result.try_reset_for_execution(count.max(1), allocator)?;
    result.set_vector_type(vector_type);
    result.set_count(count);
    Ok(())
}

#[inline]
fn validity_all_valid(validity: &ValidityRef<'_>) -> bool {
    match validity {
        ValidityRef::Borrowed(mask) => mask.all_valid(),
        ValidityRef::Owned(mask) => mask.all_valid(),
    }
}

#[inline]
fn sequence_meta<T: Copy + 'static>(view: &VectorView<'_>) -> Option<SequenceMeta> {
    if TypeId::of::<T>() != TypeId::of::<i64>() {
        return None;
    }
    match view.data() {
        DataRef::SequenceI64 { start, increment } => Some(SequenceMeta { start, increment }),
        DataRef::Ptr(_) => None,
    }
}

#[inline]
unsafe fn cast_i64<T: Copy + 'static>(value: i64) -> T {
    debug_assert_eq!(TypeId::of::<T>(), TypeId::of::<i64>());
    unsafe { std::mem::transmute_copy(&value) }
}

#[inline]
unsafe fn read_ptr<T: Copy>(ptr: *const T, idx: usize) -> T {
    unsafe { *ptr.add(idx) }
}

#[inline]
fn sequence_value<T: Copy + 'static>(meta: SequenceMeta, sel: &SelectionRef<'_>, row: usize) -> T {
    let physical_idx = sel.get(row) as i64;
    let value = meta.start + physical_idx * meta.increment;
    unsafe { cast_i64::<T>(value) }
}

#[inline]
fn load_view_value<T: Copy + 'static>(view: &VectorView<'_>, row: usize) -> T {
    if let Some(meta) = sequence_meta::<T>(view) {
        return sequence_value::<T>(meta, view.sel(), row);
    }
    let ptr = view
        .get_data::<T>()
        .expect("pointer-backed VectorView expected for this physical type");
    let physical_idx = view.sel().get(row);
    unsafe { read_ptr(ptr, physical_idx) }
}

pub trait CastOperator<INPUT, RESULT> {
    fn cast(value: INPUT) -> Result<RESULT>;
}

pub(crate) fn execute_cast_view<INPUT, RESULT, OP>(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    try_cast: bool,
) -> Result<bool>
where
    INPUT: Copy + 'static,
    RESULT: Copy + 'static,
    OP: CastOperator<INPUT, RESULT>,
{
    prepare_result(result, VectorType::Flat, count)?;
    if count == 0 {
        return Ok(true);
    }

    let view = input.try_to_view(count)?;
    let mut all_success = true;
    let result_data = unsafe { result.flat_data_mut::<RESULT>() };

    match view.sel() {
        SelectionRef::Incremental { .. } if validity_all_valid(view.validity()) => {
            for row in 0..count {
                let input = load_view_value::<INPUT>(&view, row);
                match OP::cast(input) {
                    Ok(value) => unsafe {
                        *result_data.add(row) = value;
                    },
                    Err(error) => {
                        if try_cast {
                            result.validity_mut().set_null(row);
                            all_success = false;
                        } else {
                            return Err(error);
                        }
                    }
                }
            }
        }
        SelectionRef::Constant { .. } => {
            if !view.validity().is_valid(0) {
                result.validity_mut().set_all_invalid(count);
                return Ok(true);
            }

            let input = load_view_value::<INPUT>(&view, 0);
            match OP::cast(input) {
                Ok(value) => {
                    for row in 0..count {
                        unsafe {
                            *result_data.add(row) = value;
                        }
                    }
                }
                Err(error) => {
                    if try_cast {
                        result.validity_mut().set_all_invalid(count);
                        all_success = false;
                    } else {
                        return Err(error);
                    }
                }
            }
        }
        _ => {
            for row in 0..count {
                let physical_idx = view.sel().get(row);
                if !view.validity().is_valid(physical_idx) {
                    result.validity_mut().set_null(row);
                    continue;
                }

                let input = load_view_value::<INPUT>(&view, row);
                match OP::cast(input) {
                    Ok(value) => unsafe {
                        *result_data.add(row) = value;
                    },
                    Err(error) => {
                        if try_cast {
                            result.validity_mut().set_null(row);
                            all_success = false;
                        } else {
                            return Err(error);
                        }
                    }
                }
            }
        }
    }

    Ok(all_success)
}

pub(crate) fn execute_unary_flat<INPUT, RESULT, OP>(
    input: &Vector,
    result: &mut Vector,
    count: usize,
) -> Result<()>
where
    INPUT: Copy + 'static,
    RESULT: Copy,
    OP: UnaryOperator<INPUT, RESULT>,
{
    prepare_result(result, VectorType::Flat, count)?;
    if count == 0 {
        return Ok(());
    }

    let input_data = unsafe { input.flat_data::<INPUT>() };
    let result_data = unsafe { result.flat_data_mut::<RESULT>() };

    if input.validity().all_valid() {
        for row in 0..count {
            unsafe {
                *result_data.add(row) = OP::operation(read_ptr(input_data, row));
            }
        }
        return Ok(());
    }

    for row in 0..count {
        if input.validity().is_valid(row) {
            unsafe {
                *result_data.add(row) = OP::operation(read_ptr(input_data, row));
            }
        } else {
            result.validity_mut().set_null(row);
        }
    }
    Ok(())
}

pub(crate) fn execute_unary_view<INPUT, RESULT, OP>(
    input: &Vector,
    result: &mut Vector,
    count: usize,
) -> Result<()>
where
    INPUT: Copy + 'static,
    RESULT: Copy,
    OP: UnaryOperator<INPUT, RESULT>,
{
    prepare_result(result, VectorType::Flat, count)?;
    if count == 0 {
        return Ok(());
    }

    let view = input.try_to_view(count)?;
    if let Some(meta) = sequence_meta::<INPUT>(&view) {
        unary_sequence_loop::<INPUT, RESULT, OP>(meta, view.sel(), view.validity(), result, count);
        return Ok(());
    }

    let ptr = view
        .get_data::<INPUT>()
        .expect("pointer-backed VectorView expected for unary execution");
    unary_ptr_loop::<INPUT, RESULT, OP>(ptr, view.sel(), view.validity(), result, count);
    Ok(())
}

fn unary_ptr_loop<INPUT, RESULT, OP>(
    ptr: *const INPUT,
    sel: &SelectionRef<'_>,
    validity: &ValidityRef<'_>,
    result: &mut Vector,
    count: usize,
) where
    INPUT: Copy + 'static,
    RESULT: Copy,
    OP: UnaryOperator<INPUT, RESULT>,
{
    let result_data = unsafe { result.flat_data_mut::<RESULT>() };

    match sel {
        SelectionRef::Incremental { .. } if validity_all_valid(validity) => {
            for row in 0..count {
                unsafe {
                    *result_data.add(row) = OP::operation(read_ptr(ptr, row));
                }
            }
        }
        SelectionRef::Constant { .. } => {
            if !validity.is_valid(0) {
                result.validity_mut().set_all_invalid(count);
                return;
            }
            let value = unsafe { OP::operation(read_ptr(ptr, 0)) };
            for row in 0..count {
                unsafe {
                    *result_data.add(row) = value;
                }
            }
        }
        _ => {
            for row in 0..count {
                let physical_idx = sel.get(row);
                if validity.is_valid(physical_idx) {
                    unsafe {
                        *result_data.add(row) = OP::operation(read_ptr(ptr, physical_idx));
                    }
                } else {
                    result.validity_mut().set_null(row);
                }
            }
        }
    }
}

fn unary_sequence_loop<INPUT, RESULT, OP>(
    meta: SequenceMeta,
    sel: &SelectionRef<'_>,
    validity: &ValidityRef<'_>,
    result: &mut Vector,
    count: usize,
) where
    INPUT: Copy + 'static,
    RESULT: Copy,
    OP: UnaryOperator<INPUT, RESULT>,
{
    let result_data = unsafe { result.flat_data_mut::<RESULT>() };

    match sel {
        SelectionRef::Incremental { .. } if validity_all_valid(validity) => {
            for row in 0..count {
                let value = unsafe { cast_i64::<INPUT>(meta.start + row as i64 * meta.increment) };
                unsafe {
                    *result_data.add(row) = OP::operation(value);
                }
            }
        }
        SelectionRef::Constant { .. } => {
            if !validity.is_valid(0) {
                result.validity_mut().set_all_invalid(count);
                return;
            }
            let input = unsafe { cast_i64::<INPUT>(meta.start) };
            let value = OP::operation(input);
            for row in 0..count {
                unsafe {
                    *result_data.add(row) = value;
                }
            }
        }
        _ => {
            for row in 0..count {
                let physical_idx = sel.get(row);
                if validity.is_valid(physical_idx) {
                    let input = unsafe {
                        cast_i64::<INPUT>(meta.start + physical_idx as i64 * meta.increment)
                    };
                    unsafe {
                        *result_data.add(row) = OP::operation(input);
                    }
                } else {
                    result.validity_mut().set_null(row);
                }
            }
        }
    }
}

pub(crate) fn execute_binary_view<LEFT, RIGHT, RESULT, OP>(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
) -> Result<()>
where
    LEFT: Copy + 'static,
    RIGHT: Copy + 'static,
    RESULT: Copy,
    OP: BinaryOperator<LEFT, RIGHT, RESULT>,
{
    prepare_result(result, VectorType::Flat, count)?;
    if count == 0 {
        return Ok(());
    }

    let left_view = left.try_to_view(count)?;
    let right_view = right.try_to_view(count)?;

    match (
        sequence_meta::<LEFT>(&left_view),
        sequence_meta::<RIGHT>(&right_view),
    ) {
        (Some(left_seq), None) => {
            let right_ptr = right_view
                .get_data::<RIGHT>()
                .expect("pointer-backed right VectorView expected for binary execution");
            binary_left_sequence_loop::<LEFT, RIGHT, RESULT, OP>(
                left_seq,
                left_view.sel(),
                left_view.validity(),
                right_ptr,
                right_view.sel(),
                right_view.validity(),
                result,
                count,
            );
        }
        (None, Some(right_seq)) => {
            let left_ptr = left_view
                .get_data::<LEFT>()
                .expect("pointer-backed left VectorView expected for binary execution");
            binary_right_sequence_loop::<LEFT, RIGHT, RESULT, OP>(
                left_ptr,
                left_view.sel(),
                left_view.validity(),
                right_seq,
                right_view.sel(),
                right_view.validity(),
                result,
                count,
            );
        }
        (Some(_), Some(_)) => {
            binary_generic_view_loop::<LEFT, RIGHT, RESULT, OP>(
                &left_view,
                &right_view,
                result,
                count,
            );
        }
        (None, None) => {
            let left_ptr = left_view
                .get_data::<LEFT>()
                .expect("pointer-backed left VectorView expected for binary execution");
            let right_ptr = right_view
                .get_data::<RIGHT>()
                .expect("pointer-backed right VectorView expected for binary execution");
            binary_ptr_loop::<LEFT, RIGHT, RESULT, OP>(
                left_ptr,
                left_view.sel(),
                left_view.validity(),
                right_ptr,
                right_view.sel(),
                right_view.validity(),
                result,
                count,
            );
        }
    }
    Ok(())
}

fn binary_ptr_loop<LEFT, RIGHT, RESULT, OP>(
    left_ptr: *const LEFT,
    left_sel: &SelectionRef<'_>,
    left_validity: &ValidityRef<'_>,
    right_ptr: *const RIGHT,
    right_sel: &SelectionRef<'_>,
    right_validity: &ValidityRef<'_>,
    result: &mut Vector,
    count: usize,
) where
    LEFT: Copy + 'static,
    RIGHT: Copy + 'static,
    RESULT: Copy,
    OP: BinaryOperator<LEFT, RIGHT, RESULT>,
{
    let result_data = unsafe { result.flat_data_mut::<RESULT>() };

    match (left_sel, right_sel) {
        (SelectionRef::Incremental { .. }, SelectionRef::Incremental { .. })
            if validity_all_valid(left_validity) && validity_all_valid(right_validity) =>
        {
            for row in 0..count {
                unsafe {
                    *result_data.add(row) =
                        OP::operation(read_ptr(left_ptr, row), read_ptr(right_ptr, row));
                }
            }
        }
        (SelectionRef::Incremental { .. }, SelectionRef::Constant { .. }) => {
            if !right_validity.is_valid(0) {
                result.validity_mut().set_all_invalid(count);
                return;
            }
            let right_value = unsafe { read_ptr(right_ptr, 0) };
            if validity_all_valid(left_validity) {
                for row in 0..count {
                    unsafe {
                        *result_data.add(row) = OP::operation(read_ptr(left_ptr, row), right_value);
                    }
                }
            } else {
                for row in 0..count {
                    if left_validity.is_valid(row) {
                        unsafe {
                            *result_data.add(row) =
                                OP::operation(read_ptr(left_ptr, row), right_value);
                        }
                    } else {
                        result.validity_mut().set_null(row);
                    }
                }
            }
        }
        (SelectionRef::Constant { .. }, SelectionRef::Incremental { .. }) => {
            if !left_validity.is_valid(0) {
                result.validity_mut().set_all_invalid(count);
                return;
            }
            let left_value = unsafe { read_ptr(left_ptr, 0) };
            if validity_all_valid(right_validity) {
                for row in 0..count {
                    unsafe {
                        *result_data.add(row) = OP::operation(left_value, read_ptr(right_ptr, row));
                    }
                }
            } else {
                for row in 0..count {
                    if right_validity.is_valid(row) {
                        unsafe {
                            *result_data.add(row) =
                                OP::operation(left_value, read_ptr(right_ptr, row));
                        }
                    } else {
                        result.validity_mut().set_null(row);
                    }
                }
            }
        }
        (SelectionRef::Constant { .. }, SelectionRef::Constant { .. }) => {
            if !left_validity.is_valid(0) || !right_validity.is_valid(0) {
                result.validity_mut().set_all_invalid(count);
                return;
            }
            let value = unsafe { OP::operation(read_ptr(left_ptr, 0), read_ptr(right_ptr, 0)) };
            for row in 0..count {
                unsafe {
                    *result_data.add(row) = value;
                }
            }
        }
        _ => {
            for row in 0..count {
                let left_idx = left_sel.get(row);
                let right_idx = right_sel.get(row);
                if left_validity.is_valid(left_idx) && right_validity.is_valid(right_idx) {
                    unsafe {
                        *result_data.add(row) = OP::operation(
                            read_ptr(left_ptr, left_idx),
                            read_ptr(right_ptr, right_idx),
                        );
                    }
                } else {
                    result.validity_mut().set_null(row);
                }
            }
        }
    }
}

fn binary_left_sequence_loop<LEFT, RIGHT, RESULT, OP>(
    left_seq: SequenceMeta,
    left_sel: &SelectionRef<'_>,
    left_validity: &ValidityRef<'_>,
    right_ptr: *const RIGHT,
    right_sel: &SelectionRef<'_>,
    right_validity: &ValidityRef<'_>,
    result: &mut Vector,
    count: usize,
) where
    LEFT: Copy + 'static,
    RIGHT: Copy + 'static,
    RESULT: Copy,
    OP: BinaryOperator<LEFT, RIGHT, RESULT>,
{
    let result_data = unsafe { result.flat_data_mut::<RESULT>() };

    match (left_sel, right_sel) {
        (SelectionRef::Incremental { .. }, SelectionRef::Incremental { .. })
            if validity_all_valid(left_validity) && validity_all_valid(right_validity) =>
        {
            for row in 0..count {
                let left_value =
                    unsafe { cast_i64::<LEFT>(left_seq.start + row as i64 * left_seq.increment) };
                unsafe {
                    *result_data.add(row) = OP::operation(left_value, read_ptr(right_ptr, row));
                }
            }
        }
        (_, SelectionRef::Constant { .. }) => {
            if !right_validity.is_valid(0) {
                result.validity_mut().set_all_invalid(count);
                return;
            }
            let right_value = unsafe { read_ptr(right_ptr, 0) };
            for row in 0..count {
                let left_idx = left_sel.get(row);
                if left_validity.is_valid(left_idx) {
                    let left_value = unsafe {
                        cast_i64::<LEFT>(left_seq.start + left_idx as i64 * left_seq.increment)
                    };
                    unsafe {
                        *result_data.add(row) = OP::operation(left_value, right_value);
                    }
                } else {
                    result.validity_mut().set_null(row);
                }
            }
        }
        _ => {
            for row in 0..count {
                let left_idx = left_sel.get(row);
                let right_idx = right_sel.get(row);
                if left_validity.is_valid(left_idx) && right_validity.is_valid(right_idx) {
                    let left_value = unsafe {
                        cast_i64::<LEFT>(left_seq.start + left_idx as i64 * left_seq.increment)
                    };
                    unsafe {
                        *result_data.add(row) =
                            OP::operation(left_value, read_ptr(right_ptr, right_idx));
                    }
                } else {
                    result.validity_mut().set_null(row);
                }
            }
        }
    }
}

fn binary_right_sequence_loop<LEFT, RIGHT, RESULT, OP>(
    left_ptr: *const LEFT,
    left_sel: &SelectionRef<'_>,
    left_validity: &ValidityRef<'_>,
    right_seq: SequenceMeta,
    right_sel: &SelectionRef<'_>,
    right_validity: &ValidityRef<'_>,
    result: &mut Vector,
    count: usize,
) where
    LEFT: Copy + 'static,
    RIGHT: Copy + 'static,
    RESULT: Copy,
    OP: BinaryOperator<LEFT, RIGHT, RESULT>,
{
    let result_data = unsafe { result.flat_data_mut::<RESULT>() };

    match (left_sel, right_sel) {
        (SelectionRef::Incremental { .. }, SelectionRef::Incremental { .. })
            if validity_all_valid(left_validity) && validity_all_valid(right_validity) =>
        {
            for row in 0..count {
                let right_value = unsafe {
                    cast_i64::<RIGHT>(right_seq.start + row as i64 * right_seq.increment)
                };
                unsafe {
                    *result_data.add(row) = OP::operation(read_ptr(left_ptr, row), right_value);
                }
            }
        }
        (SelectionRef::Constant { .. }, _) => {
            if !left_validity.is_valid(0) {
                result.validity_mut().set_all_invalid(count);
                return;
            }
            let left_value = unsafe { read_ptr(left_ptr, 0) };
            for row in 0..count {
                let right_idx = right_sel.get(row);
                if right_validity.is_valid(right_idx) {
                    let right_value = unsafe {
                        cast_i64::<RIGHT>(right_seq.start + right_idx as i64 * right_seq.increment)
                    };
                    unsafe {
                        *result_data.add(row) = OP::operation(left_value, right_value);
                    }
                } else {
                    result.validity_mut().set_null(row);
                }
            }
        }
        _ => {
            for row in 0..count {
                let left_idx = left_sel.get(row);
                let right_idx = right_sel.get(row);
                if left_validity.is_valid(left_idx) && right_validity.is_valid(right_idx) {
                    let right_value = unsafe {
                        cast_i64::<RIGHT>(right_seq.start + right_idx as i64 * right_seq.increment)
                    };
                    unsafe {
                        *result_data.add(row) =
                            OP::operation(read_ptr(left_ptr, left_idx), right_value);
                    }
                } else {
                    result.validity_mut().set_null(row);
                }
            }
        }
    }
}

fn binary_generic_view_loop<LEFT, RIGHT, RESULT, OP>(
    left: &VectorView<'_>,
    right: &VectorView<'_>,
    result: &mut Vector,
    count: usize,
) where
    LEFT: Copy + 'static,
    RIGHT: Copy + 'static,
    RESULT: Copy,
    OP: BinaryOperator<LEFT, RIGHT, RESULT>,
{
    let result_data = unsafe { result.flat_data_mut::<RESULT>() };
    for row in 0..count {
        if left.is_valid(row) && right.is_valid(row) {
            unsafe {
                *result_data.add(row) = OP::operation(
                    load_view_value::<LEFT>(left, row),
                    load_view_value::<RIGHT>(right, row),
                );
            }
        } else {
            result.validity_mut().set_null(row);
        }
    }
}

pub(crate) fn execute_ternary_view<A, B, C, RESULT, OP>(
    a_vec: &Vector,
    b_vec: &Vector,
    c_vec: &Vector,
    result: &mut Vector,
    count: usize,
) -> Result<()>
where
    A: Copy + 'static,
    B: Copy + 'static,
    C: Copy + 'static,
    RESULT: Copy,
    OP: TernaryOperator<A, B, C, RESULT>,
{
    prepare_result(result, VectorType::Flat, count)?;
    if count == 0 {
        return Ok(());
    }

    let a_view = a_vec.try_to_view(count)?;
    let b_view = b_vec.try_to_view(count)?;
    let c_view = c_vec.try_to_view(count)?;

    if sequence_meta::<A>(&a_view).is_none()
        && sequence_meta::<B>(&b_view).is_none()
        && sequence_meta::<C>(&c_view).is_none()
    {
        let a_ptr = a_view
            .get_data::<A>()
            .expect("pointer-backed first VectorView expected for ternary execution");
        let b_ptr = b_view
            .get_data::<B>()
            .expect("pointer-backed second VectorView expected for ternary execution");
        let c_ptr = c_view
            .get_data::<C>()
            .expect("pointer-backed third VectorView expected for ternary execution");
        ternary_ptr_loop::<A, B, C, RESULT, OP>(
            a_ptr, &a_view, b_ptr, &b_view, c_ptr, &c_view, result, count,
        );
        return Ok(());
    }

    ternary_generic_view_loop::<A, B, C, RESULT, OP>(&a_view, &b_view, &c_view, result, count);
    Ok(())
}

fn ternary_ptr_loop<A, B, C, RESULT, OP>(
    a_ptr: *const A,
    a_view: &VectorView<'_>,
    b_ptr: *const B,
    b_view: &VectorView<'_>,
    c_ptr: *const C,
    c_view: &VectorView<'_>,
    result: &mut Vector,
    count: usize,
) where
    A: Copy + 'static,
    B: Copy + 'static,
    C: Copy + 'static,
    RESULT: Copy,
    OP: TernaryOperator<A, B, C, RESULT>,
{
    let result_data = unsafe { result.flat_data_mut::<RESULT>() };
    if matches!(a_view.sel(), SelectionRef::Incremental { .. })
        && matches!(b_view.sel(), SelectionRef::Incremental { .. })
        && matches!(c_view.sel(), SelectionRef::Incremental { .. })
        && validity_all_valid(a_view.validity())
        && validity_all_valid(b_view.validity())
        && validity_all_valid(c_view.validity())
    {
        for row in 0..count {
            unsafe {
                *result_data.add(row) = OP::operation(
                    read_ptr(a_ptr, row),
                    read_ptr(b_ptr, row),
                    read_ptr(c_ptr, row),
                );
            }
        }
        return;
    }

    for row in 0..count {
        if a_view.is_valid(row) && b_view.is_valid(row) && c_view.is_valid(row) {
            let a_idx = a_view.sel().get(row);
            let b_idx = b_view.sel().get(row);
            let c_idx = c_view.sel().get(row);
            unsafe {
                *result_data.add(row) = OP::operation(
                    read_ptr(a_ptr, a_idx),
                    read_ptr(b_ptr, b_idx),
                    read_ptr(c_ptr, c_idx),
                );
            }
        } else {
            result.validity_mut().set_null(row);
        }
    }
}

fn ternary_generic_view_loop<A, B, C, RESULT, OP>(
    a_view: &VectorView<'_>,
    b_view: &VectorView<'_>,
    c_view: &VectorView<'_>,
    result: &mut Vector,
    count: usize,
) where
    A: Copy + 'static,
    B: Copy + 'static,
    C: Copy + 'static,
    RESULT: Copy,
    OP: TernaryOperator<A, B, C, RESULT>,
{
    let result_data = unsafe { result.flat_data_mut::<RESULT>() };
    for row in 0..count {
        if a_view.is_valid(row) && b_view.is_valid(row) && c_view.is_valid(row) {
            unsafe {
                *result_data.add(row) = OP::operation(
                    load_view_value::<A>(a_view, row),
                    load_view_value::<B>(b_view, row),
                    load_view_value::<C>(c_view, row),
                );
            }
        } else {
            result.validity_mut().set_null(row);
        }
    }
}

pub(crate) fn select_binary_view_into<LEFT, RIGHT, OP>(
    left: &Vector,
    right: &Vector,
    input_sel: Option<&SelectionVector>,
    count: usize,
    selection: &mut SelectionVector,
) -> Result<usize>
where
    LEFT: Copy + 'static,
    RIGHT: Copy + 'static,
    OP: BinaryOperator<LEFT, RIGHT, bool>,
{
    if count == 0 {
        selection.set_len(0);
        return Ok(0);
    }

    let left_view = left.try_to_view(count)?;
    let right_view = right.try_to_view(count)?;
    selection.set_len(count);
    let mut selected = 0;

    match (
        sequence_meta::<LEFT>(&left_view),
        sequence_meta::<RIGHT>(&right_view),
    ) {
        (Some(_), Some(_)) => {
            for row in 0..count {
                if left_view.is_valid(row)
                    && right_view.is_valid(row)
                    && OP::operation(
                        load_view_value::<LEFT>(&left_view, row),
                        load_view_value::<RIGHT>(&right_view, row),
                    )
                {
                    selection.set(selected, input_sel.map_or(row, |sel| sel.get(row)));
                    selected += 1;
                }
            }
        }
        (Some(left_seq), None) => {
            let right_ptr = right_view
                .get_data::<RIGHT>()
                .expect("pointer-backed right VectorView expected for selection");
            for row in 0..count {
                let left_idx = left_view.sel().get(row);
                let right_idx = right_view.sel().get(row);
                if left_view.validity().is_valid(left_idx)
                    && right_view.validity().is_valid(right_idx)
                {
                    let left_value = unsafe {
                        cast_i64::<LEFT>(left_seq.start + left_idx as i64 * left_seq.increment)
                    };
                    let right_value = unsafe { read_ptr(right_ptr, right_idx) };
                    if OP::operation(left_value, right_value) {
                        selection.set(selected, input_sel.map_or(row, |sel| sel.get(row)));
                        selected += 1;
                    }
                }
            }
        }
        (None, Some(right_seq)) => {
            let left_ptr = left_view
                .get_data::<LEFT>()
                .expect("pointer-backed left VectorView expected for selection");
            for row in 0..count {
                let left_idx = left_view.sel().get(row);
                let right_idx = right_view.sel().get(row);
                if left_view.validity().is_valid(left_idx)
                    && right_view.validity().is_valid(right_idx)
                {
                    let left_value = unsafe { read_ptr(left_ptr, left_idx) };
                    let right_value = unsafe {
                        cast_i64::<RIGHT>(right_seq.start + right_idx as i64 * right_seq.increment)
                    };
                    if OP::operation(left_value, right_value) {
                        selection.set(selected, input_sel.map_or(row, |sel| sel.get(row)));
                        selected += 1;
                    }
                }
            }
        }
        (None, None) => {
            let left_ptr = left_view
                .get_data::<LEFT>()
                .expect("pointer-backed left VectorView expected for selection");
            let right_ptr = right_view
                .get_data::<RIGHT>()
                .expect("pointer-backed right VectorView expected for selection");
            for row in 0..count {
                let left_idx = left_view.sel().get(row);
                let right_idx = right_view.sel().get(row);
                if left_view.validity().is_valid(left_idx)
                    && right_view.validity().is_valid(right_idx)
                {
                    let left_value = unsafe { read_ptr(left_ptr, left_idx) };
                    let right_value = unsafe { read_ptr(right_ptr, right_idx) };
                    if OP::operation(left_value, right_value) {
                        selection.set(selected, input_sel.map_or(row, |sel| sel.get(row)));
                        selected += 1;
                    }
                }
            }
        }
    }

    selection.set_len(selected);
    Ok(selected)
}
