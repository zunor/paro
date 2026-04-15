// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::scalar::cast::CastExecCtx;
use crate::scalar::executor::typed_loops::{self, CastOperator};
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::Vector;

macro_rules! generate_integer_cast {
    ($name:ident, $source:ty, $target:ty) => {
        pub fn $name(
            input: &Vector,
            result: &mut Vector,
            count: usize,
            ctx: &CastExecCtx<'_>,
        ) -> Result<bool> {
            struct Op;

            impl CastOperator<$source, $target> for Op {
                fn cast(value: $source) -> Result<$target> {
                    <$target>::try_from(value).map_err(|_| {
                        paro_error::out_of_range(format!(
                            "Value {} out of range for target type {}",
                            value,
                            stringify!($target)
                        ))
                    })
                }
            }

            typed_loops::execute_cast_view::<$source, $target, Op>(
                input,
                result,
                count,
                ctx.try_cast,
            )
        }
    };
}

macro_rules! generate_float_to_int_cast {
    ($name:ident, $source:ty, $target:ty) => {
        pub fn $name(
            input: &Vector,
            result: &mut Vector,
            count: usize,
            ctx: &CastExecCtx<'_>,
        ) -> Result<bool> {
            struct Op;

            impl CastOperator<$source, $target> for Op {
                fn cast(value: $source) -> Result<$target> {
                    if value.is_nan()
                        || value.is_infinite()
                        || value < <$target>::MIN as $source
                        || value > <$target>::MAX as $source
                    {
                        Err(paro_error::out_of_range(format!(
                            "Float value {} out of range for target type {}",
                            value,
                            stringify!($target)
                        )))
                    } else {
                        Ok(value as $target)
                    }
                }
            }

            typed_loops::execute_cast_view::<$source, $target, Op>(
                input,
                result,
                count,
                ctx.try_cast,
            )
        }
    };
}

macro_rules! generate_infallible_cast {
    ($name:ident, $source:ty, $target:ty) => {
        pub fn $name(
            input: &Vector,
            result: &mut Vector,
            count: usize,
            ctx: &CastExecCtx<'_>,
        ) -> Result<bool> {
            struct Op;

            impl CastOperator<$source, $target> for Op {
                fn cast(value: $source) -> Result<$target> {
                    Ok(value as $target)
                }
            }

            typed_loops::execute_cast_view::<$source, $target, Op>(
                input,
                result,
                count,
                ctx.try_cast,
            )
        }
    };
}

generate_integer_cast!(int8_to_int16, i8, i16);
generate_integer_cast!(int8_to_int32, i8, i32);
generate_integer_cast!(int8_to_int64, i8, i64);
generate_integer_cast!(int8_to_int128, i8, i128);

generate_integer_cast!(int16_to_int8, i16, i8);
generate_integer_cast!(int16_to_int32, i16, i32);
generate_integer_cast!(int16_to_int64, i16, i64);
generate_integer_cast!(int16_to_int128, i16, i128);

generate_integer_cast!(int32_to_int8, i32, i8);
generate_integer_cast!(int32_to_int16, i32, i16);
generate_integer_cast!(int32_to_int64, i32, i64);
generate_integer_cast!(int32_to_int128, i32, i128);

generate_integer_cast!(int64_to_int8, i64, i8);
generate_integer_cast!(int64_to_int16, i64, i16);
generate_integer_cast!(int64_to_int32, i64, i32);
generate_integer_cast!(int64_to_int128, i64, i128);

generate_integer_cast!(int128_to_int8, i128, i8);
generate_integer_cast!(int128_to_int16, i128, i16);
generate_integer_cast!(int128_to_int32, i128, i32);
generate_integer_cast!(int128_to_int64, i128, i64);

generate_integer_cast!(uint8_to_uint16, u8, u16);
generate_integer_cast!(uint8_to_uint32, u8, u32);
generate_integer_cast!(uint8_to_uint64, u8, u64);

generate_integer_cast!(uint16_to_uint8, u16, u8);
generate_integer_cast!(uint16_to_uint32, u16, u32);
generate_integer_cast!(uint16_to_uint64, u16, u64);

generate_integer_cast!(uint32_to_uint8, u32, u8);
generate_integer_cast!(uint32_to_uint16, u32, u16);
generate_integer_cast!(uint32_to_uint64, u32, u64);

generate_integer_cast!(uint64_to_uint8, u64, u8);
generate_integer_cast!(uint64_to_uint16, u64, u16);
generate_integer_cast!(uint64_to_uint32, u64, u32);

generate_integer_cast!(int8_to_uint8, i8, u8);
generate_integer_cast!(uint8_to_int8, u8, i8);
generate_integer_cast!(int16_to_uint16, i16, u16);
generate_integer_cast!(uint16_to_int16, u16, i16);
generate_integer_cast!(int32_to_uint8, i32, u8);
generate_integer_cast!(int32_to_uint16, i32, u16);
generate_integer_cast!(int32_to_uint32, i32, u32);
generate_integer_cast!(uint32_to_int32, u32, i32);
generate_integer_cast!(int32_to_uint64, i32, u64);
generate_integer_cast!(int32_to_uint128, i32, u128);
generate_integer_cast!(int64_to_uint8, i64, u8);
generate_integer_cast!(int64_to_uint16, i64, u16);
generate_integer_cast!(int64_to_uint32, i64, u32);
generate_integer_cast!(int64_to_uint64, i64, u64);
generate_integer_cast!(uint64_to_int64, u64, i64);
generate_integer_cast!(int64_to_uint128, i64, u128);
generate_integer_cast!(int128_to_uint8, i128, u8);
generate_integer_cast!(int128_to_uint16, i128, u16);
generate_integer_cast!(int128_to_uint32, i128, u32);
generate_integer_cast!(int128_to_uint64, i128, u64);
generate_integer_cast!(int128_to_uint128, i128, u128);
generate_integer_cast!(uint128_to_int128, u128, i128);

generate_infallible_cast!(int32_to_float, i32, f32);
generate_infallible_cast!(int32_to_double, i32, f64);
generate_infallible_cast!(int64_to_float, i64, f32);
generate_infallible_cast!(int64_to_double, i64, f64);

generate_float_to_int_cast!(float_to_int32, f32, i32);
generate_float_to_int_cast!(float_to_int64, f32, i64);
generate_float_to_int_cast!(double_to_int32, f64, i32);
generate_float_to_int_cast!(double_to_int64, f64, i64);

generate_infallible_cast!(float_to_double, f32, f64);
generate_infallible_cast!(double_to_float, f64, f32);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::scalar::FunctionExecContext;
    use paro_common::types::LogicalType;

    #[derive(Debug)]
    struct NoopRuntime;

    impl FunctionExecContext for NoopRuntime {
        fn current_database(&self) -> Option<&str> {
            None
        }

        fn current_schema(&self) -> Option<&str> {
            None
        }

        fn current_user(&self) -> Option<&str> {
            None
        }
    }

    static NOOP_RUNTIME: NoopRuntime = NoopRuntime;

    fn ctx(try_cast: bool) -> CastExecCtx<'static> {
        CastExecCtx {
            runtime: &NOOP_RUNTIME,
            try_cast,
            cast_data: None,
        }
    }

    #[test]
    fn sequence_casts_without_materializing() {
        let input = Vector::sequence(10, 3, 4);
        let mut result = Vector::with_capacity(LogicalType::Double, 4);

        let success = int64_to_double(&input, &mut result, 4, &ctx(false)).unwrap();

        assert!(success);
        assert_eq!(result.get_f64(0), Some(10.0));
        assert_eq!(result.get_f64(1), Some(13.0));
        assert_eq!(result.get_f64(2), Some(16.0));
        assert_eq!(result.get_f64(3), Some(19.0));
    }

    #[test]
    fn try_cast_nullifies_out_of_range_dictionary_rows() {
        let input =
            Vector::dictionary(Arc::new(Vector::from_i64(&[127, 128, -129])), vec![2, 0, 1]);
        let mut result = Vector::with_capacity(LogicalType::TinyInt, 3);

        let success = int64_to_int8(&input, &mut result, 3, &ctx(true)).unwrap();

        assert!(!success);
        assert!(result.is_null(0));
        assert_eq!(result.get_i8(1), Some(127));
        assert!(result.is_null(2));
    }

    #[test]
    fn reusing_result_vector_clears_stale_try_cast_nulls() {
        let input = Vector::from_i64(&[127, 128]);
        let mut result = Vector::with_capacity(LogicalType::TinyInt, 2);

        let success = int64_to_int8(&input, &mut result, 2, &ctx(true)).unwrap();
        assert!(!success);
        assert_eq!(result.get_i8(0), Some(127));
        assert!(result.is_null(1));

        let clean_input = Vector::from_i64(&[12, 13]);
        let success = int64_to_int8(&clean_input, &mut result, 2, &ctx(false)).unwrap();
        assert!(success);
        assert_eq!(result.get_i8(0), Some(12));
        assert_eq!(result.get_i8(1), Some(13));
        assert!(!result.is_null(0));
        assert!(!result.is_null(1));
    }
}
