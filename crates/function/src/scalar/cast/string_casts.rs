use crate::scalar::cast::CastExecCtx;
use crate::scalar::executor::varlen::VarcharResultWriter;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::{format_uuid, parse_uuid_str};
use paro_common::vector::Vector;
use serde_json::Value as JsonValue;

/// Optimized numeric to varchar cast.
///
/// Uses a stack-allocated buffer and Cursor to avoid per-row allocations.
pub fn numeric_to_varchar_cast<T: std::fmt::Display + Copy>(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let view = input.to_view(count);
    let data = view.get_data::<T>();
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if view.is_valid(row) {
            if let Some(data) = data {
                let value = unsafe { *data.add(view.physical_index(row)) };
                writer.write_str(row, &format!("{value}"));
            } else {
                writer.write_str(row, &format!("{}", input.get_value(row)));
            }
        } else {
            writer.set_null(row);
        }
    }

    Ok(true)
}

/// Vectorized varchar to numeric cast.
pub fn varchar_to_numeric_cast<T>(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool>
where
    T: std::str::FromStr + Copy + Default + 'static,
{
    let mut all_success = true;
    let view = input.to_varlen_view(count);
    result.set_count(count);

    for row in 0..count {
        if view.is_valid(row) {
            let source_value = view.get_inline_string(row);
            let source = source_value.as_str();
            match source.trim().parse::<T>() {
                Ok(value) => {
                    unsafe { result.set_flat::<T>(row, value) };
                    result.set_null(row, false);
                }
                Err(_) => {
                    if ctx.try_cast {
                        result.set_null(row, true);
                        all_success = false;
                    } else {
                        return Err(paro_error::invalid_value("NUMERIC", source));
                    }
                }
            }
        } else {
            result.set_null(row, true);
        }
    }

    Ok(all_success)
}

/// Vectorized varchar to UUID cast.
pub fn varchar_to_uuid_cast(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let mut all_success = true;
    let view = input.to_varlen_view(count);
    result.set_count(count);

    for row in 0..count {
        if view.is_valid(row) {
            let source_value = view.get_inline_string(row);
            let source = source_value.as_str();
            match parse_uuid_str(source) {
                Ok(value) => {
                    unsafe { result.set_flat::<u128>(row, value) };
                    result.set_null(row, false);
                }
                Err(_) => {
                    if ctx.try_cast {
                        result.set_null(row, true);
                        all_success = false;
                    } else {
                        return Err(paro_error::invalid_value("UUID", source));
                    }
                }
            }
        } else {
            result.set_null(row, true);
        }
    }

    Ok(all_success)
}

/// Vectorized UUID to varchar cast.
pub fn uuid_to_varchar_cast(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let view = input.to_view(count);
    let data = view
        .get_data::<u128>()
        .expect("uuid cast requires pointer data");
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if view.is_valid(row) {
            let value = unsafe { *data.add(view.physical_index(row)) };
            writer.write_str(row, &format_uuid(value));
        } else {
            writer.set_null(row);
        }
    }

    Ok(true)
}

fn varchar_to_json_like_cast(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
    type_name: &str,
) -> Result<bool> {
    let mut all_success = true;
    let view = input.to_varlen_view(count);
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if view.is_valid(row) {
            let source_value = view.get_inline_string(row);
            let source = source_value.as_str();
            if serde_json::from_str::<JsonValue>(source).is_ok() {
                writer.write_str(row, source);
            } else if ctx.try_cast {
                writer.set_null(row);
                all_success = false;
            } else {
                return Err(paro_error::invalid_value(type_name, source));
            }
        } else {
            writer.set_null(row);
        }
    }

    Ok(all_success)
}

/// Vectorized varchar to JSON cast with validation.
pub fn varchar_to_json_cast(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    varchar_to_json_like_cast(input, result, count, ctx, "JSON")
}

/// Vectorized varchar to JSONB cast with validation.
pub fn varchar_to_jsonb_cast(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    varchar_to_json_like_cast(input, result, count, ctx, "JSONB")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::types::LogicalType;

    use super::*;

    #[derive(Debug)]
    struct TestContext;

    impl crate::scalar::FunctionExecContext for TestContext {
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

    static TEST_CONTEXT: TestContext = TestContext;

    fn ctx(try_cast: bool) -> CastExecCtx<'static> {
        CastExecCtx {
            runtime: &TEST_CONTEXT,
            try_cast,
            cast_data: None,
        }
    }

    #[test]
    fn numeric_to_varchar_reads_dictionary_rows() {
        let base = Arc::new(Vector::from_i32(&[10, 20, 30]));
        let input = Vector::dictionary(base, vec![2_u32, 0]);
        let mut result = Vector::new(LogicalType::Varchar);

        numeric_to_varchar_cast::<i32>(&input, &mut result, 2, &ctx(false))
            .expect("numeric to varchar should succeed");

        assert_eq!(result.get_string(0), Some("30"));
        assert_eq!(result.get_string(1), Some("10"));
    }

    #[test]
    fn varchar_to_numeric_try_cast_nullifies_invalid_rows() {
        let input = Vector::from_strings(&["42", "oops", "-7"]);
        let mut result = Vector::new(LogicalType::BigInt);

        let all_success = varchar_to_numeric_cast::<i64>(&input, &mut result, 3, &ctx(true))
            .expect("varchar try cast should succeed");

        assert!(!all_success);
        assert_eq!(result.get_i64(0), Some(42));
        assert!(result.is_null(1));
        assert_eq!(result.get_i64(2), Some(-7));
    }

    #[test]
    fn uuid_to_varchar_reads_dictionary_rows() {
        let mut base = Vector::with_capacity(LogicalType::Uuid, 2);
        base.set_len(2);
        unsafe {
            base.set_flat::<u128>(0, 0x1234567890abcdef1234567890abcdef_u128);
            base.set_flat::<u128>(1, 0xffffffffffffffffffffffffffffffff_u128);
        }
        base.set_null(0, false);
        base.set_null(1, false);
        let base = Arc::new(base);
        let input = Vector::dictionary(base, vec![1_u32, 0]);
        let mut result = Vector::new(LogicalType::Varchar);

        uuid_to_varchar_cast(&input, &mut result, 2, &ctx(false))
            .expect("uuid to varchar should succeed");

        assert_eq!(
            result.get_string(0),
            Some("ffffffff-ffff-ffff-ffff-ffffffffffff")
        );
        assert_eq!(
            result.get_string(1),
            Some("12345678-90ab-cdef-1234-567890abcdef")
        );
    }

    #[test]
    fn varchar_to_json_try_cast_nullifies_invalid_rows() {
        let input = Vector::from_strings(&[r#"{"a":1}"#, "not json"]);
        let mut result = Vector::new(LogicalType::Json);

        let all_success =
            varchar_to_json_cast(&input, &mut result, 2, &ctx(true)).expect("json try cast");

        assert!(!all_success);
        assert_eq!(result.get_string(0), Some(r#"{"a":1}"#));
        assert!(result.is_null(1));
    }
}
