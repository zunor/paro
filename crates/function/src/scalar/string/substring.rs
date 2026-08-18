// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::scalar::function_data_fingerprint;
use crate::{
    scalar::executor::varlen::VarcharResultWriter, BoundScalarFunction, ExpressionState,
    FunctionData, FunctionErrorMode, ScalarBindInput, ScalarFunction, ScalarFunctionSet,
    ScalarPredicateProjection,
};

#[derive(Debug, Clone, PartialEq, Hash)]
struct SubstringBindData {
    start: Option<i64>,
    length: Option<i64>,
}

impl FunctionData for SubstringBindData {
    fn clone_box(&self) -> Box<dyn FunctionData> {
        Box::new(self.clone())
    }

    fn equals(&self, other: &dyn FunctionData) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|other| other == self)
    }

    fn fingerprint(&self) -> u64 {
        function_data_fingerprint(self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
struct CountBindData {
    count: i64,
}

impl FunctionData for CountBindData {
    fn clone_box(&self) -> Box<dyn FunctionData> {
        Box::new(self.clone())
    }

    fn equals(&self, other: &dyn FunctionData) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|other| other == self)
    }

    fn fingerprint(&self) -> u64 {
        function_data_fingerprint(self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn bind_substring_2(
    function: &ScalarFunction,
    input: &ScalarBindInput,
) -> Result<BoundScalarFunction> {
    let mut bound =
        BoundScalarFunction::from(function.clone()).with_error_mode(FunctionErrorMode::Infallible);
    if let Some(start) = input.constant_value(1).and_then(Value::as_i64) {
        bound = bound.with_bind_data(SubstringBindData {
            start: Some(start),
            length: None,
        });
        bound = bound.with_predicate_projection(ScalarPredicateProjection::Utf8Substring {
            source_argument: 0,
            start,
            length: None,
        });
    }
    Ok(bound)
}

fn bind_substring_3(
    function: &ScalarFunction,
    input: &ScalarBindInput,
) -> Result<BoundScalarFunction> {
    let mut bound =
        BoundScalarFunction::from(function.clone()).with_error_mode(FunctionErrorMode::Infallible);
    let start = input.constant_value(1).and_then(Value::as_i64);
    let length = input.constant_value(2).and_then(Value::as_i64);
    if start.is_some() || length.is_some() {
        bound = bound.with_bind_data(SubstringBindData { start, length });
    }
    if let (Some(start), Some(length)) = (start, length) {
        bound = bound.with_predicate_projection(ScalarPredicateProjection::Utf8Substring {
            source_argument: 0,
            start,
            length: Some(length),
        });
    }
    Ok(bound)
}

fn bind_count_argument(
    function: &ScalarFunction,
    input: &ScalarBindInput,
) -> Result<BoundScalarFunction> {
    let mut bound =
        BoundScalarFunction::from(function.clone()).with_error_mode(FunctionErrorMode::Infallible);
    if let Some(count) = input.constant_value(1).and_then(Value::as_i64) {
        bound = bound.with_bind_data(CountBindData { count });
    }
    Ok(bound)
}

fn substring_bind_data(state: &dyn ExpressionState) -> Option<&SubstringBindData> {
    state
        .bind_data()
        .and_then(|data| data.as_any().downcast_ref::<SubstringBindData>())
}

fn count_bind_data(state: &dyn ExpressionState) -> Option<&CountBindData> {
    state
        .bind_data()
        .and_then(|data| data.as_any().downcast_ref::<CountBindData>())
}

/// Extract a borrowed UTF-8 substring using SQL's codepoint positions.
///
/// A substring is always contiguous in the source, so materializing `Vec<char>`
/// and a second `String` only adds per-row allocations. ASCII values use direct
/// byte offsets; non-ASCII values scan only to the requested boundaries.
fn substring_unicode(s: &str, start: i64, length: Option<i64>) -> &str {
    if s.is_empty() {
        return s;
    }

    let character_count = if start < 0 {
        Some(if s.is_ascii() {
            s.len()
        } else {
            s.chars().count()
        })
    } else {
        None
    };
    let start_index = if start > 0 {
        usize::try_from(start - 1).unwrap_or(usize::MAX)
    } else if start < 0 {
        character_count
            .expect("negative substring start computes the character count")
            .saturating_sub(start.unsigned_abs().min(usize::MAX as u64) as usize)
    } else {
        0
    };
    let requested_length = match length {
        Some(value) if value <= 0 => 0,
        Some(value) => {
            let value = usize::try_from(value).unwrap_or(usize::MAX);
            if start == 0 {
                value.saturating_sub(1)
            } else {
                value
            }
        }
        None => usize::MAX,
    };
    if requested_length == 0 {
        return &s[0..0];
    }

    let start_byte = codepoint_offset(s, start_index);
    let end_byte = if requested_length == usize::MAX {
        s.len()
    } else {
        codepoint_offset(s, start_index.saturating_add(requested_length))
    };
    &s[start_byte..end_byte]
}

#[inline]
fn codepoint_offset(value: &str, index: usize) -> usize {
    if value.is_ascii() {
        return index.min(value.len());
    }
    value
        .char_indices()
        .nth(index)
        .map_or(value.len(), |(offset, _)| offset)
}

/// Implementation of `substring(VARCHAR, BIGINT) -> VARCHAR`.
fn substring_2_varchar(
    input: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let str_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing string column".to_string()))?;
    let start_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing start column".to_string()))?;
    let str_view = str_vec.try_to_utf8_view(count)?;
    let start_view = start_vec.try_to_view(count)?;
    let bound_start = substring_bind_data(state).and_then(|data| data.start);
    let mut writer = VarcharResultWriter::try_new(result, count)?;

    for row in 0..count {
        if !str_view.is_valid(row) || !start_view.is_valid(row) {
            writer.set_null(row);
            continue;
        }
        let start = bound_start.unwrap_or_else(|| start_view.get_i64(row));
        let value = str_view.str(row);
        let sub = substring_unicode(value, start, None);
        writer.write_str(row, sub)?;
    }

    Ok(())
}

/// Implementation of `substring(VARCHAR, BIGINT, BIGINT) -> VARCHAR`.
fn substring_3_varchar(
    input: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let str_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing string column".to_string()))?;
    let start_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing start column".to_string()))?;
    let len_vec = input
        .column(2)
        .ok_or_else(|| paro_common::error::internal("Missing length column".to_string()))?;
    let str_view = str_vec.try_to_utf8_view(count)?;
    let start_view = start_vec.try_to_view(count)?;
    let len_view = len_vec.try_to_view(count)?;
    let bind_data = substring_bind_data(state);
    let bound_start = bind_data.and_then(|data| data.start);
    let bound_length = bind_data.and_then(|data| data.length);
    let mut writer = VarcharResultWriter::try_new(result, count)?;

    for row in 0..count {
        if !str_view.is_valid(row) || !start_view.is_valid(row) || !len_view.is_valid(row) {
            writer.set_null(row);
            continue;
        }
        let start = bound_start.unwrap_or_else(|| start_view.get_i64(row));
        let length = bound_length.unwrap_or_else(|| len_view.get_i64(row));
        let value = str_view.str(row);
        let sub = substring_unicode(value, start, Some(length));
        writer.write_str(row, sub)?;
    }

    Ok(())
}

/// Implementation of `left(VARCHAR, BIGINT) -> VARCHAR`.
fn left_varchar(input: &Chunk, state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let count = input.size();
    let str_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing string column".to_string()))?;
    let n_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing n column".to_string()))?;
    let str_view = str_vec.try_to_utf8_view(count)?;
    let n_view = n_vec.try_to_view(count)?;
    let bound_count = count_bind_data(state).map(|data| data.count);
    let mut writer = VarcharResultWriter::try_new(result, count)?;

    for row in 0..count {
        if !str_view.is_valid(row) || !n_view.is_valid(row) {
            writer.set_null(row);
            continue;
        }
        let value = str_view.str(row);
        let n = bound_count.unwrap_or_else(|| n_view.get_i64(row));
        let sub = if n >= 0 {
            value.chars().take(n as usize).collect::<String>()
        } else {
            let chars: Vec<char> = value.chars().collect();
            let take_count = (chars.len() as i64 + n).max(0) as usize;
            chars[..take_count].iter().collect()
        };
        writer.write_str(row, &sub)?;
    }

    Ok(())
}

/// Implementation of `right(VARCHAR, BIGINT) -> VARCHAR`.
fn right_varchar(input: &Chunk, state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let count = input.size();
    let str_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing string column".to_string()))?;
    let n_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing n column".to_string()))?;
    let str_view = str_vec.try_to_utf8_view(count)?;
    let n_view = n_vec.try_to_view(count)?;
    let bound_count = count_bind_data(state).map(|data| data.count);
    let mut writer = VarcharResultWriter::try_new(result, count)?;

    for row in 0..count {
        if !str_view.is_valid(row) || !n_view.is_valid(row) {
            writer.set_null(row);
            continue;
        }
        let value = str_view.str(row);
        let n = bound_count.unwrap_or_else(|| n_view.get_i64(row));

        let chars: Vec<char> = value.chars().collect();
        let char_count = chars.len();

        let sub: String = if n >= 0 {
            let skip = char_count.saturating_sub(n as usize);
            chars[skip..].iter().collect()
        } else {
            let skip = (-n as usize).min(char_count);
            chars[skip..].iter().collect()
        };

        writer.write_str(row, &sub)?;
    }

    Ok(())
}

/// Get `substring` function set.
pub fn get_substring_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("substring".to_string());

    // substring(VARCHAR, BIGINT) -> VARCHAR
    set.add_function(
        ScalarFunction::new(
            "substring".to_string(),
            vec![LogicalType::Varchar, LogicalType::BigInt],
            LogicalType::Varchar,
            substring_2_varchar,
        )
        .with_bind(bind_substring_2),
    );

    // substring(VARCHAR, BIGINT, BIGINT) -> VARCHAR
    set.add_function(
        ScalarFunction::new(
            "substring".to_string(),
            vec![
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::BigInt,
            ],
            LogicalType::Varchar,
            substring_3_varchar,
        )
        .with_bind(bind_substring_3),
    );

    set
}

/// Get `left` function set.
pub fn get_left_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("left".to_string());
    set.add_function(
        ScalarFunction::new(
            "left".to_string(),
            vec![LogicalType::Varchar, LogicalType::BigInt],
            LogicalType::Varchar,
            left_varchar,
        )
        .with_bind(bind_count_argument),
    );
    set
}

/// Get `right` function set.
pub fn get_right_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("right".to_string());
    set.add_function(
        ScalarFunction::new(
            "right".to_string(),
            vec![LogicalType::Varchar, LogicalType::BigInt],
            LogicalType::Varchar,
            right_varchar,
        )
        .with_bind(bind_count_argument),
    );
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use std::sync::Arc;

    struct MockState;
    impl ExpressionState for MockState {
        fn current_database(&self) -> Option<&str> {
            None
        }
        fn current_schema(&self) -> Option<&str> {
            None
        }
        fn current_user(&self) -> Option<&str> {
            None
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct BindState {
        bind_data: Arc<dyn FunctionData>,
    }

    impl ExpressionState for BindState {
        fn current_database(&self) -> Option<&str> {
            None
        }

        fn current_schema(&self) -> Option<&str> {
            None
        }

        fn current_user(&self) -> Option<&str> {
            None
        }

        fn bind_data(&self) -> Option<&dyn FunctionData> {
            Some(self.bind_data.as_ref())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_substring_unicode_basic() {
        assert_eq!(substring_unicode("hello", 1, Some(3)), "hel");
        assert_eq!(substring_unicode("hello", 2, Some(3)), "ell");
        assert_eq!(substring_unicode("hello", 1, None), "hello");
        assert_eq!(substring_unicode("hello", 3, None), "llo");
    }

    #[test]
    fn test_substring_unicode_negative_start() {
        assert_eq!(substring_unicode("hello", -2, Some(2)), "lo");
        assert_eq!(substring_unicode("hello", -3, None), "llo");
    }

    #[test]
    fn test_substring_unicode_multibyte() {
        assert_eq!(substring_unicode("你好世界", 1, Some(2)), "你好");
        assert_eq!(substring_unicode("你好世界", 3, Some(2)), "世界");
        assert_eq!(substring_unicode("hello世界", 6, Some(2)), "世界");
    }

    #[test]
    fn test_substring_function() {
        let str_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello", "world"],
            paro_common::test_utils::test_allocator(),
        );
        let start_vec = paro_common::test_utils::test_i64_vector_with_allocator(
            &[2, 1],
            paro_common::test_utils::test_allocator(),
        );
        let len_vec = paro_common::test_utils::test_i64_vector_with_allocator(
            &[3, 2],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![str_vec, start_vec, len_vec],
            paro_common::test_utils::test_allocator(),
        );
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        substring_3_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("ell"));
        assert_eq!(result.get_string(1), Some("wo"));
    }

    #[test]
    fn test_left_function() {
        let str_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello", "世界你好"],
            paro_common::test_utils::test_allocator(),
        );
        let n_vec = paro_common::test_utils::test_i64_vector_with_allocator(
            &[3, 2],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![str_vec, n_vec],
            paro_common::test_utils::test_allocator(),
        );
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        left_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hel"));
        assert_eq!(result.get_string(1), Some("世界"));
    }

    #[test]
    fn test_left_negative() {
        let str_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello"],
            paro_common::test_utils::test_allocator(),
        );
        let n_vec = paro_common::test_utils::test_i64_vector_with_allocator(
            &[-2],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![str_vec, n_vec],
            paro_common::test_utils::test_allocator(),
        );
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        left_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hel"));
    }

    #[test]
    fn test_right_function() {
        let str_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello", "世界你好"],
            paro_common::test_utils::test_allocator(),
        );
        let n_vec = paro_common::test_utils::test_i64_vector_with_allocator(
            &[3, 2],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![str_vec, n_vec],
            paro_common::test_utils::test_allocator(),
        );
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        right_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("llo"));
        assert_eq!(result.get_string(1), Some("你好"));
    }

    #[test]
    fn test_right_negative() {
        let str_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello"],
            paro_common::test_utils::test_allocator(),
        );
        let n_vec = paro_common::test_utils::test_i64_vector_with_allocator(
            &[-2],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![str_vec, n_vec],
            paro_common::test_utils::test_allocator(),
        );
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        right_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("llo"));
    }

    #[test]
    fn test_substring_with_null() {
        let mut str_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello", "world"],
            paro_common::test_utils::test_allocator(),
        );
        str_vec.validity_mut().set_null(1);
        let start_vec = paro_common::test_utils::test_i64_vector_with_allocator(
            &[1, 1],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![str_vec, start_vec],
            paro_common::test_utils::test_allocator(),
        );
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        substring_2_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
        assert!(result.is_null(1));
    }

    #[test]
    fn test_substring_bind_data_reuses_constant_arguments() {
        let set = get_substring_functions();
        let (function, _) = set
            .bind(&[
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::BigInt,
            ])
            .unwrap();
        let bound = function
            .bind(&ScalarBindInput::new(
                vec![
                    LogicalType::Varchar,
                    LogicalType::BigInt,
                    LogicalType::BigInt,
                ],
                vec![None, Some(Value::BigInt(2)), Some(Value::BigInt(3))],
            ))
            .unwrap();
        let state = BindState {
            bind_data: bound.bind_data.clone().unwrap(),
        };

        let chunk = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["hello"],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_i64_vector_with_allocator(
                    &[99],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_i64_vector_with_allocator(
                    &[99],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        substring_3_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("ell"));
    }
}
