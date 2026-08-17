// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::scalar::cast::CastExecCtx;
use crate::scalar::executor::typed_loops::{self, CastOperator};
use crate::scalar::executor::varlen::VarcharResultWriter;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::Vector;

macro_rules! generate_bool_to_numeric_cast {
    ($name:ident, $target:ty) => {
        pub fn $name(
            input: &Vector,
            result: &mut Vector,
            count: usize,
            ctx: &CastExecCtx<'_>,
        ) -> Result<bool> {
            struct Op;

            impl CastOperator<bool, $target> for Op {
                fn cast(value: bool) -> Result<$target> {
                    Ok(if value { 1 } else { 0 })
                }
            }

            typed_loops::execute_cast_view::<bool, $target, Op>(input, result, count, ctx.try_cast)
        }
    };
}

macro_rules! generate_numeric_to_bool_cast {
    ($name:ident, $source:ty) => {
        pub fn $name(
            input: &Vector,
            result: &mut Vector,
            count: usize,
            ctx: &CastExecCtx<'_>,
        ) -> Result<bool> {
            struct Op;

            impl CastOperator<$source, bool> for Op {
                fn cast(value: $source) -> Result<bool> {
                    Ok(value != 0 as $source)
                }
            }

            typed_loops::execute_cast_view::<$source, bool, Op>(input, result, count, ctx.try_cast)
        }
    };
}

generate_bool_to_numeric_cast!(bool_to_int8, i8);
generate_bool_to_numeric_cast!(bool_to_int32, i32);
generate_bool_to_numeric_cast!(bool_to_int64, i64);

generate_numeric_to_bool_cast!(int8_to_bool, i8);
generate_numeric_to_bool_cast!(int32_to_bool, i32);
generate_numeric_to_bool_cast!(int64_to_bool, i64);
generate_numeric_to_bool_cast!(float_to_bool, f32);
generate_numeric_to_bool_cast!(double_to_bool, f64);

fn parse_bool_literal(source: &str) -> Option<bool> {
    let trimmed = source.trim();
    if matches!(trimmed, "1")
        || trimmed.eq_ignore_ascii_case("true")
        || trimmed.eq_ignore_ascii_case("t")
        || trimmed.eq_ignore_ascii_case("yes")
        || trimmed.eq_ignore_ascii_case("y")
    {
        return Some(true);
    }
    if matches!(trimmed, "0")
        || trimmed.eq_ignore_ascii_case("false")
        || trimmed.eq_ignore_ascii_case("f")
        || trimmed.eq_ignore_ascii_case("no")
        || trimmed.eq_ignore_ascii_case("n")
    {
        return Some(false);
    }
    None
}

pub fn bool_to_varchar(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let view = input.try_to_view(count)?;
    let data = view
        .get_data::<bool>()
        .expect("bool cast requires pointer data");
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if view.is_valid(row) {
            let value = unsafe { *data.add(view.physical_index(row)) };
            writer.write_str(row, if value { "true" } else { "false" })?;
        } else {
            writer.set_null(row);
        }
    }

    Ok(true)
}

pub fn varchar_to_bool(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let mut all_success = true;
    let view = input.try_to_varlen_view(count)?;
    result.set_count(count);

    for row in 0..count {
        if view.is_valid(row) {
            let source_value = view.get_string_view(row);
            let source = source_value.as_str();
            if let Some(value) = parse_bool_literal(source) {
                result.set_bool(row, value);
            } else if ctx.try_cast {
                result.set_null(row, true);
                all_success = false;
            } else {
                return Err(paro_error::invalid_value("BOOLEAN", source));
            }
        } else {
            result.set_null(row, true);
        }
    }

    Ok(all_success)
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
    fn bool_to_varchar_reads_dictionary_rows() {
        let base = Arc::new(paro_common::test_utils::test_bool_vector_with_allocator(
            &[true, false, true],
            paro_common::test_utils::test_allocator(),
        ));
        let input = paro_common::test_utils::test_dictionary(base, vec![2_u32, 0, 1]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        bool_to_varchar(&input, &mut result, 3, &ctx(false)).expect("bool cast should succeed");

        assert_eq!(result.get_string(0), Some("true"));
        assert_eq!(result.get_string(1), Some("true"));
        assert_eq!(result.get_string(2), Some("false"));
    }

    #[test]
    fn varchar_to_bool_try_cast_nullifies_invalid_rows() {
        let input = paro_common::test_utils::test_string_vector_with_allocator(
            &["yes", "maybe", "0"],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);

        let all_success =
            varchar_to_bool(&input, &mut result, 3, &ctx(true)).expect("try cast should succeed");

        assert!(!all_success);
        assert_eq!(result.get_bool(0), Some(true));
        assert!(result.is_null(1));
        assert_eq!(result.get_bool(2), Some(false));
    }
}
