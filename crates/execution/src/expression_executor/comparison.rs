// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;

use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::{InlineString, LogicalType, PhysicalType};
use paro_common::vector::{SelectionVector, Vector, VectorOperations};
use paro_function::scalar::executor::binary::BinaryExecutor;
use paro_function::scalar::operators::comparison::{
    EqualsOperator, GreaterThanEqualsOperator, GreaterThanOperator, LessThanEqualsOperator,
    LessThanOperator, NotEqualsOperator,
};
use paro_planner::expression::ComparisonType;

/// Zero-sized context passed through comparison dispatch (reserved for future use).
#[derive(Copy, Clone)]
pub struct ComparisonExecCtx;

/// Shared instance for comparison callbacks.
pub const COMPARISON_EXEC_CTX: ComparisonExecCtx = ComparisonExecCtx;

pub type ComparisonFn = fn(&Vector, &Vector, &mut Vector, usize, &ComparisonExecCtx) -> Result<()>;
pub type ComparisonSelectFn =
    fn(&Vector, &Vector, Option<&SelectionVector>, usize, &mut SelectionVector) -> Result<usize>;

#[derive(Debug, Clone, Copy)]
pub struct ComparisonDispatch {
    pub compare: ComparisonFn,
    pub select: Option<ComparisonSelectFn>,
}

macro_rules! define_fixed_width_comparison {
    ($compare_name:ident, $select_name:ident, $ty:ty, $op:ty) => {
        fn $compare_name(
            left: &Vector,
            right: &Vector,
            result: &mut Vector,
            count: usize,
            _ctx: &ComparisonExecCtx,
        ) -> Result<()> {
            BinaryExecutor::execute::<$ty, $ty, bool, $op>(left, right, result, count)
        }

        fn $select_name(
            left: &Vector,
            right: &Vector,
            input_sel: Option<&SelectionVector>,
            count: usize,
            output: &mut SelectionVector,
        ) -> Result<usize> {
            BinaryExecutor::select_into::<$ty, $ty, $op>(left, right, input_sel, count, output)
        }
    };
}

define_fixed_width_comparison!(compare_i32_eq, select_i32_eq, i32, EqualsOperator);
define_fixed_width_comparison!(compare_i32_ne, select_i32_ne, i32, NotEqualsOperator);
define_fixed_width_comparison!(compare_i32_lt, select_i32_lt, i32, LessThanOperator);
define_fixed_width_comparison!(compare_i32_le, select_i32_le, i32, LessThanEqualsOperator);
define_fixed_width_comparison!(compare_i32_gt, select_i32_gt, i32, GreaterThanOperator);
define_fixed_width_comparison!(
    compare_i32_ge,
    select_i32_ge,
    i32,
    GreaterThanEqualsOperator
);

define_fixed_width_comparison!(compare_i8_eq, select_i8_eq, i8, EqualsOperator);
define_fixed_width_comparison!(compare_i8_ne, select_i8_ne, i8, NotEqualsOperator);
define_fixed_width_comparison!(compare_i8_lt, select_i8_lt, i8, LessThanOperator);
define_fixed_width_comparison!(compare_i8_le, select_i8_le, i8, LessThanEqualsOperator);
define_fixed_width_comparison!(compare_i8_gt, select_i8_gt, i8, GreaterThanOperator);
define_fixed_width_comparison!(compare_i8_ge, select_i8_ge, i8, GreaterThanEqualsOperator);

define_fixed_width_comparison!(compare_i16_eq, select_i16_eq, i16, EqualsOperator);
define_fixed_width_comparison!(compare_i16_ne, select_i16_ne, i16, NotEqualsOperator);
define_fixed_width_comparison!(compare_i16_lt, select_i16_lt, i16, LessThanOperator);
define_fixed_width_comparison!(compare_i16_le, select_i16_le, i16, LessThanEqualsOperator);
define_fixed_width_comparison!(compare_i16_gt, select_i16_gt, i16, GreaterThanOperator);
define_fixed_width_comparison!(
    compare_i16_ge,
    select_i16_ge,
    i16,
    GreaterThanEqualsOperator
);

define_fixed_width_comparison!(compare_i64_eq, select_i64_eq, i64, EqualsOperator);
define_fixed_width_comparison!(compare_i64_ne, select_i64_ne, i64, NotEqualsOperator);
define_fixed_width_comparison!(compare_i64_lt, select_i64_lt, i64, LessThanOperator);
define_fixed_width_comparison!(compare_i64_le, select_i64_le, i64, LessThanEqualsOperator);
define_fixed_width_comparison!(compare_i64_gt, select_i64_gt, i64, GreaterThanOperator);
define_fixed_width_comparison!(
    compare_i64_ge,
    select_i64_ge,
    i64,
    GreaterThanEqualsOperator
);

define_fixed_width_comparison!(compare_u64_eq, select_u64_eq, u64, EqualsOperator);
define_fixed_width_comparison!(compare_u64_ne, select_u64_ne, u64, NotEqualsOperator);
define_fixed_width_comparison!(compare_u64_lt, select_u64_lt, u64, LessThanOperator);
define_fixed_width_comparison!(compare_u64_le, select_u64_le, u64, LessThanEqualsOperator);
define_fixed_width_comparison!(compare_u64_gt, select_u64_gt, u64, GreaterThanOperator);
define_fixed_width_comparison!(
    compare_u64_ge,
    select_u64_ge,
    u64,
    GreaterThanEqualsOperator
);

define_fixed_width_comparison!(compare_u8_eq, select_u8_eq, u8, EqualsOperator);
define_fixed_width_comparison!(compare_u8_ne, select_u8_ne, u8, NotEqualsOperator);
define_fixed_width_comparison!(compare_u8_lt, select_u8_lt, u8, LessThanOperator);
define_fixed_width_comparison!(compare_u8_le, select_u8_le, u8, LessThanEqualsOperator);
define_fixed_width_comparison!(compare_u8_gt, select_u8_gt, u8, GreaterThanOperator);
define_fixed_width_comparison!(compare_u8_ge, select_u8_ge, u8, GreaterThanEqualsOperator);

define_fixed_width_comparison!(compare_u16_eq, select_u16_eq, u16, EqualsOperator);
define_fixed_width_comparison!(compare_u16_ne, select_u16_ne, u16, NotEqualsOperator);
define_fixed_width_comparison!(compare_u16_lt, select_u16_lt, u16, LessThanOperator);
define_fixed_width_comparison!(compare_u16_le, select_u16_le, u16, LessThanEqualsOperator);
define_fixed_width_comparison!(compare_u16_gt, select_u16_gt, u16, GreaterThanOperator);
define_fixed_width_comparison!(
    compare_u16_ge,
    select_u16_ge,
    u16,
    GreaterThanEqualsOperator
);

define_fixed_width_comparison!(compare_u32_eq, select_u32_eq, u32, EqualsOperator);
define_fixed_width_comparison!(compare_u32_ne, select_u32_ne, u32, NotEqualsOperator);
define_fixed_width_comparison!(compare_u32_lt, select_u32_lt, u32, LessThanOperator);
define_fixed_width_comparison!(compare_u32_le, select_u32_le, u32, LessThanEqualsOperator);
define_fixed_width_comparison!(compare_u32_gt, select_u32_gt, u32, GreaterThanOperator);
define_fixed_width_comparison!(
    compare_u32_ge,
    select_u32_ge,
    u32,
    GreaterThanEqualsOperator
);

define_fixed_width_comparison!(compare_f32_eq, select_f32_eq, f32, EqualsOperator);
define_fixed_width_comparison!(compare_f32_ne, select_f32_ne, f32, NotEqualsOperator);
define_fixed_width_comparison!(compare_f32_lt, select_f32_lt, f32, LessThanOperator);
define_fixed_width_comparison!(compare_f32_le, select_f32_le, f32, LessThanEqualsOperator);
define_fixed_width_comparison!(compare_f32_gt, select_f32_gt, f32, GreaterThanOperator);
define_fixed_width_comparison!(
    compare_f32_ge,
    select_f32_ge,
    f32,
    GreaterThanEqualsOperator
);

define_fixed_width_comparison!(compare_f64_eq, select_f64_eq, f64, EqualsOperator);
define_fixed_width_comparison!(compare_f64_ne, select_f64_ne, f64, NotEqualsOperator);
define_fixed_width_comparison!(compare_f64_lt, select_f64_lt, f64, LessThanOperator);
define_fixed_width_comparison!(compare_f64_le, select_f64_le, f64, LessThanEqualsOperator);
define_fixed_width_comparison!(compare_f64_gt, select_f64_gt, f64, GreaterThanOperator);
define_fixed_width_comparison!(
    compare_f64_ge,
    select_f64_ge,
    f64,
    GreaterThanEqualsOperator
);

define_fixed_width_comparison!(compare_bool_eq, select_bool_eq, bool, EqualsOperator);
define_fixed_width_comparison!(compare_bool_ne, select_bool_ne, bool, NotEqualsOperator);

define_fixed_width_comparison!(
    compare_varchar_eq,
    select_varchar_eq,
    InlineString,
    EqualsOperator
);
define_fixed_width_comparison!(
    compare_varchar_ne,
    select_varchar_ne,
    InlineString,
    NotEqualsOperator
);
define_fixed_width_comparison!(
    compare_varchar_lt,
    select_varchar_lt,
    InlineString,
    LessThanOperator
);
define_fixed_width_comparison!(
    compare_varchar_le,
    select_varchar_le,
    InlineString,
    LessThanEqualsOperator
);
define_fixed_width_comparison!(
    compare_varchar_gt,
    select_varchar_gt,
    InlineString,
    GreaterThanOperator
);
define_fixed_width_comparison!(
    compare_varchar_ge,
    select_varchar_ge,
    InlineString,
    GreaterThanEqualsOperator
);

define_fixed_width_comparison!(compare_i128_eq, select_i128_eq, i128, EqualsOperator);
define_fixed_width_comparison!(compare_i128_ne, select_i128_ne, i128, NotEqualsOperator);
define_fixed_width_comparison!(compare_i128_lt, select_i128_lt, i128, LessThanOperator);
define_fixed_width_comparison!(
    compare_i128_le,
    select_i128_le,
    i128,
    LessThanEqualsOperator
);
define_fixed_width_comparison!(compare_i128_gt, select_i128_gt, i128, GreaterThanOperator);
define_fixed_width_comparison!(
    compare_i128_ge,
    select_i128_ge,
    i128,
    GreaterThanEqualsOperator
);

define_fixed_width_comparison!(compare_u128_eq, select_u128_eq, u128, EqualsOperator);
define_fixed_width_comparison!(compare_u128_ne, select_u128_ne, u128, NotEqualsOperator);
define_fixed_width_comparison!(compare_u128_lt, select_u128_lt, u128, LessThanOperator);
define_fixed_width_comparison!(
    compare_u128_le,
    select_u128_le,
    u128,
    LessThanEqualsOperator
);
define_fixed_width_comparison!(compare_u128_gt, select_u128_gt, u128, GreaterThanOperator);
define_fixed_width_comparison!(
    compare_u128_ge,
    select_u128_ge,
    u128,
    GreaterThanEqualsOperator
);

fn compare_array_eq(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &ComparisonExecCtx,
) -> Result<()> {
    VectorOperations::equals(left, right, result, count);
    Ok(())
}

fn compare_array_ne(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &ComparisonExecCtx,
) -> Result<()> {
    VectorOperations::not_equals(left, right, result, count);
    Ok(())
}

fn compare_value_pair(comparison_type: ComparisonType, left: &Value, right: &Value) -> bool {
    match comparison_type {
        ComparisonType::Equal => left == right,
        ComparisonType::NotEqual => left != right,
        ComparisonType::LessThan => left.partial_cmp(right) == Some(Ordering::Less),
        ComparisonType::LessThanOrEqual => {
            matches!(
                left.partial_cmp(right),
                Some(Ordering::Less) | Some(Ordering::Equal)
            )
        }
        ComparisonType::GreaterThan => left.partial_cmp(right) == Some(Ordering::Greater),
        ComparisonType::GreaterThanOrEqual => {
            matches!(
                left.partial_cmp(right),
                Some(Ordering::Greater) | Some(Ordering::Equal)
            )
        }
        ComparisonType::DistinctFrom => left != right,
        ComparisonType::NotDistinctFrom => left == right,
    }
}

fn compare_generic(
    comparison_type: ComparisonType,
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
) {
    for row_idx in 0..count {
        let left_value = left.get_value(row_idx);
        let right_value = right.get_value(row_idx);
        let value = match comparison_type {
            ComparisonType::DistinctFrom => {
                (!left_value.is_null() && right_value.is_null())
                    || (left_value.is_null() && !right_value.is_null())
                    || (!left_value.is_null()
                        && !right_value.is_null()
                        && left_value != right_value)
            }
            ComparisonType::NotDistinctFrom => {
                (left_value.is_null() && right_value.is_null())
                    || (!left_value.is_null()
                        && !right_value.is_null()
                        && left_value == right_value)
            }
            _ => {
                if left_value.is_null() || right_value.is_null() {
                    result.set_null(row_idx, true);
                    continue;
                }
                compare_value_pair(comparison_type, &left_value, &right_value)
            }
        };
        result.set_bool(row_idx, value);
    }
}

fn compare_generic_eq(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &ComparisonExecCtx,
) -> Result<()> {
    compare_generic(ComparisonType::Equal, left, right, result, count);
    Ok(())
}

fn compare_generic_ne(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &ComparisonExecCtx,
) -> Result<()> {
    compare_generic(ComparisonType::NotEqual, left, right, result, count);
    Ok(())
}

fn compare_generic_lt(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &ComparisonExecCtx,
) -> Result<()> {
    compare_generic(ComparisonType::LessThan, left, right, result, count);
    Ok(())
}

fn compare_generic_le(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &ComparisonExecCtx,
) -> Result<()> {
    compare_generic(ComparisonType::LessThanOrEqual, left, right, result, count);
    Ok(())
}

fn compare_generic_gt(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &ComparisonExecCtx,
) -> Result<()> {
    compare_generic(ComparisonType::GreaterThan, left, right, result, count);
    Ok(())
}

fn compare_generic_ge(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &ComparisonExecCtx,
) -> Result<()> {
    compare_generic(
        ComparisonType::GreaterThanOrEqual,
        left,
        right,
        result,
        count,
    );
    Ok(())
}

fn compare_distinct_from(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &ComparisonExecCtx,
) -> Result<()> {
    compare_generic(ComparisonType::DistinctFrom, left, right, result, count);
    Ok(())
}

fn compare_not_distinct_from(
    left: &Vector,
    right: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &ComparisonExecCtx,
) -> Result<()> {
    compare_generic(ComparisonType::NotDistinctFrom, left, right, result, count);
    Ok(())
}

#[derive(Clone, Copy)]
enum ComparisonClass {
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
    Bool,
    Varchar,
    I128,
    Array,
    Generic,
}

fn comparison_class(logical_type: &LogicalType) -> ComparisonClass {
    match logical_type.physical_type() {
        PhysicalType::Int8 => ComparisonClass::I8,
        PhysicalType::Int16 => ComparisonClass::I16,
        PhysicalType::Int32 => ComparisonClass::I32,
        PhysicalType::Int64 => ComparisonClass::I64,
        PhysicalType::Int128 => ComparisonClass::I128,
        PhysicalType::UInt8 => ComparisonClass::U8,
        PhysicalType::UInt16 => ComparisonClass::U16,
        PhysicalType::UInt32 => ComparisonClass::U32,
        PhysicalType::UInt64 => ComparisonClass::U64,
        PhysicalType::UInt128 => ComparisonClass::U128,
        PhysicalType::Float => ComparisonClass::F32,
        PhysicalType::Double => ComparisonClass::F64,
        PhysicalType::Bool => ComparisonClass::Bool,
        PhysicalType::Varchar if matches!(logical_type, LogicalType::Varchar) => {
            ComparisonClass::Varchar
        }
        PhysicalType::Array => ComparisonClass::Array,
        PhysicalType::Varchar | PhysicalType::Bit | PhysicalType::List | PhysicalType::Struct => {
            ComparisonClass::Generic
        }
    }
}

fn generic_dispatch(comparison_type: ComparisonType) -> ComparisonDispatch {
    use ComparisonType::{
        Equal, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, NotEqual,
    };

    match comparison_type {
        Equal => ComparisonDispatch {
            compare: compare_generic_eq,
            select: None,
        },
        NotEqual => ComparisonDispatch {
            compare: compare_generic_ne,
            select: None,
        },
        LessThan => ComparisonDispatch {
            compare: compare_generic_lt,
            select: None,
        },
        LessThanOrEqual => ComparisonDispatch {
            compare: compare_generic_le,
            select: None,
        },
        GreaterThan => ComparisonDispatch {
            compare: compare_generic_gt,
            select: None,
        },
        GreaterThanOrEqual => ComparisonDispatch {
            compare: compare_generic_ge,
            select: None,
        },
        _ => ComparisonDispatch {
            compare: compare_generic_eq,
            select: None,
        },
    }
}

fn ordered_dispatch(
    comparison_type: ComparisonType,
    eq: ComparisonFn,
    eq_select: ComparisonSelectFn,
    ne: ComparisonFn,
    ne_select: ComparisonSelectFn,
    lt: ComparisonFn,
    lt_select: ComparisonSelectFn,
    le: ComparisonFn,
    le_select: ComparisonSelectFn,
    gt: ComparisonFn,
    gt_select: ComparisonSelectFn,
    ge: ComparisonFn,
    ge_select: ComparisonSelectFn,
) -> ComparisonDispatch {
    use ComparisonType::{
        Equal, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, NotEqual,
    };

    match comparison_type {
        Equal => dispatch_with_select(eq, eq_select),
        NotEqual => dispatch_with_select(ne, ne_select),
        LessThan => dispatch_with_select(lt, lt_select),
        LessThanOrEqual => dispatch_with_select(le, le_select),
        GreaterThan => dispatch_with_select(gt, gt_select),
        GreaterThanOrEqual => dispatch_with_select(ge, ge_select),
        _ => generic_dispatch(comparison_type),
    }
}

pub fn compile_comparison_dispatch(
    logical_type: &LogicalType,
    comparison_type: ComparisonType,
) -> ComparisonDispatch {
    use ComparisonType::{DistinctFrom, Equal, NotDistinctFrom, NotEqual};

    match comparison_type {
        DistinctFrom => ComparisonDispatch {
            compare: compare_distinct_from,
            select: None,
        },
        NotDistinctFrom => ComparisonDispatch {
            compare: compare_not_distinct_from,
            select: None,
        },
        _ => match comparison_class(logical_type) {
            ComparisonClass::I8 => ordered_dispatch(
                comparison_type,
                compare_i8_eq,
                select_i8_eq,
                compare_i8_ne,
                select_i8_ne,
                compare_i8_lt,
                select_i8_lt,
                compare_i8_le,
                select_i8_le,
                compare_i8_gt,
                select_i8_gt,
                compare_i8_ge,
                select_i8_ge,
            ),
            ComparisonClass::I16 => ordered_dispatch(
                comparison_type,
                compare_i16_eq,
                select_i16_eq,
                compare_i16_ne,
                select_i16_ne,
                compare_i16_lt,
                select_i16_lt,
                compare_i16_le,
                select_i16_le,
                compare_i16_gt,
                select_i16_gt,
                compare_i16_ge,
                select_i16_ge,
            ),
            ComparisonClass::I32 => ordered_dispatch(
                comparison_type,
                compare_i32_eq,
                select_i32_eq,
                compare_i32_ne,
                select_i32_ne,
                compare_i32_lt,
                select_i32_lt,
                compare_i32_le,
                select_i32_le,
                compare_i32_gt,
                select_i32_gt,
                compare_i32_ge,
                select_i32_ge,
            ),
            ComparisonClass::I64 => ordered_dispatch(
                comparison_type,
                compare_i64_eq,
                select_i64_eq,
                compare_i64_ne,
                select_i64_ne,
                compare_i64_lt,
                select_i64_lt,
                compare_i64_le,
                select_i64_le,
                compare_i64_gt,
                select_i64_gt,
                compare_i64_ge,
                select_i64_ge,
            ),
            ComparisonClass::U64 => ordered_dispatch(
                comparison_type,
                compare_u64_eq,
                select_u64_eq,
                compare_u64_ne,
                select_u64_ne,
                compare_u64_lt,
                select_u64_lt,
                compare_u64_le,
                select_u64_le,
                compare_u64_gt,
                select_u64_gt,
                compare_u64_ge,
                select_u64_ge,
            ),
            ComparisonClass::U8 => ordered_dispatch(
                comparison_type,
                compare_u8_eq,
                select_u8_eq,
                compare_u8_ne,
                select_u8_ne,
                compare_u8_lt,
                select_u8_lt,
                compare_u8_le,
                select_u8_le,
                compare_u8_gt,
                select_u8_gt,
                compare_u8_ge,
                select_u8_ge,
            ),
            ComparisonClass::U16 => ordered_dispatch(
                comparison_type,
                compare_u16_eq,
                select_u16_eq,
                compare_u16_ne,
                select_u16_ne,
                compare_u16_lt,
                select_u16_lt,
                compare_u16_le,
                select_u16_le,
                compare_u16_gt,
                select_u16_gt,
                compare_u16_ge,
                select_u16_ge,
            ),
            ComparisonClass::U32 => ordered_dispatch(
                comparison_type,
                compare_u32_eq,
                select_u32_eq,
                compare_u32_ne,
                select_u32_ne,
                compare_u32_lt,
                select_u32_lt,
                compare_u32_le,
                select_u32_le,
                compare_u32_gt,
                select_u32_gt,
                compare_u32_ge,
                select_u32_ge,
            ),
            ComparisonClass::U128 => ordered_dispatch(
                comparison_type,
                compare_u128_eq,
                select_u128_eq,
                compare_u128_ne,
                select_u128_ne,
                compare_u128_lt,
                select_u128_lt,
                compare_u128_le,
                select_u128_le,
                compare_u128_gt,
                select_u128_gt,
                compare_u128_ge,
                select_u128_ge,
            ),
            ComparisonClass::F32 => ordered_dispatch(
                comparison_type,
                compare_f32_eq,
                select_f32_eq,
                compare_f32_ne,
                select_f32_ne,
                compare_f32_lt,
                select_f32_lt,
                compare_f32_le,
                select_f32_le,
                compare_f32_gt,
                select_f32_gt,
                compare_f32_ge,
                select_f32_ge,
            ),
            ComparisonClass::F64 => ordered_dispatch(
                comparison_type,
                compare_f64_eq,
                select_f64_eq,
                compare_f64_ne,
                select_f64_ne,
                compare_f64_lt,
                select_f64_lt,
                compare_f64_le,
                select_f64_le,
                compare_f64_gt,
                select_f64_gt,
                compare_f64_ge,
                select_f64_ge,
            ),
            ComparisonClass::Bool => match comparison_type {
                Equal => dispatch_with_select(compare_bool_eq, select_bool_eq),
                NotEqual => dispatch_with_select(compare_bool_ne, select_bool_ne),
                _ => generic_dispatch(comparison_type),
            },
            ComparisonClass::Varchar => ordered_dispatch(
                comparison_type,
                compare_varchar_eq,
                select_varchar_eq,
                compare_varchar_ne,
                select_varchar_ne,
                compare_varchar_lt,
                select_varchar_lt,
                compare_varchar_le,
                select_varchar_le,
                compare_varchar_gt,
                select_varchar_gt,
                compare_varchar_ge,
                select_varchar_ge,
            ),
            ComparisonClass::I128 => ordered_dispatch(
                comparison_type,
                compare_i128_eq,
                select_i128_eq,
                compare_i128_ne,
                select_i128_ne,
                compare_i128_lt,
                select_i128_lt,
                compare_i128_le,
                select_i128_le,
                compare_i128_gt,
                select_i128_gt,
                compare_i128_ge,
                select_i128_ge,
            ),
            ComparisonClass::Array => match comparison_type {
                Equal => ComparisonDispatch {
                    compare: compare_array_eq,
                    select: None,
                },
                NotEqual => ComparisonDispatch {
                    compare: compare_array_ne,
                    select: None,
                },
                _ => generic_dispatch(comparison_type),
            },
            ComparisonClass::Generic => generic_dispatch(comparison_type),
        },
    }
}

fn dispatch_with_select(compare: ComparisonFn, select: ComparisonSelectFn) -> ComparisonDispatch {
    ComparisonDispatch {
        compare,
        select: Some(select),
    }
}

#[cfg(test)]
mod tests {
    use paro_common::runtime_value::Value;
    use paro_common::vector::{SelectionVector, Vector};

    use super::*;

    fn assert_decimal_greater_than(
        precision: u8,
        values: &[Option<i128>],
        threshold: i128,
        expected: &[u32],
    ) {
        let allocator = paro_common::test_utils::test_allocator();
        let logical_type = LogicalType::Decimal {
            precision,
            scale: 2,
        };
        let mut left =
            Vector::try_new(logical_type.clone(), values.len(), allocator.clone()).unwrap();
        left.set_count(values.len());
        for (row, value) in values.iter().enumerate() {
            match value {
                Some(value) => left.set_value(row, &Value::Decimal(*value, precision, 2)),
                None => left.set_value(row, &Value::Null(logical_type.clone())),
            }
        }
        let right = Vector::try_constant_from_value(
            logical_type.clone(),
            Value::Decimal(threshold, precision, 2),
            values.len(),
            allocator.clone(),
        )
        .unwrap();
        let mut selection = SelectionVector::try_with_capacity(values.len(), allocator).unwrap();
        let dispatch = compile_comparison_dispatch(&logical_type, ComparisonType::GreaterThan);

        let selected =
            dispatch.select.unwrap()(&left, &right, None, values.len(), &mut selection).unwrap();

        assert_eq!(selected, expected.len());
        assert_eq!(selection.as_slice(), expected);
    }

    #[test]
    fn decimal_comparison_selection_uses_its_physical_width() {
        assert_decimal_greater_than(15, &[Some(100), Some(250), None, Some(200)], 200, &[1]);
        assert_decimal_greater_than(
            38,
            &[
                Some(10_000_000_000_000_000_000),
                Some(30_000_000_000_000_000_000),
                None,
            ],
            20_000_000_000_000_000_000,
            &[1],
        );
    }
}
