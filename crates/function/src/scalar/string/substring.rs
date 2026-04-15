use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::{
    scalar::executor::varlen::VarcharResultWriter, BoundScalarFunction, ExpressionState,
    FunctionData, FunctionErrorMode, ScalarBindInput, ScalarFunction, ScalarFunctionSet,
};

#[derive(Debug, Clone, PartialEq)]
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
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

/// Extract substring using Unicode codepoints.
/// start is 1-indexed (SQL standard).
/// Negative start counts from end.
fn substring_unicode(s: &str, start: i64, length: Option<i64>) -> String {
    let chars: Vec<char> = s.chars().collect();
    let char_count = chars.len() as i64;

    if char_count == 0 {
        return String::new();
    }

    // Handle start position (1-indexed)
    let start_idx = if start > 0 {
        (start - 1).min(char_count) as usize
    } else if start < 0 {
        // Negative: count from end
        (char_count + start).max(0) as usize
    } else {
        // start = 0: special case, treat as 1 but reduce length by 1
        0
    };

    // Handle length
    let end_idx = match length {
        Some(len) if len <= 0 => start_idx, // Zero or negative length = empty
        Some(len) => {
            let adjusted_len = if start == 0 { len - 1 } else { len };
            (start_idx as i64 + adjusted_len).min(char_count) as usize
        }
        None => char_count as usize, // No length = to end
    };

    if start_idx >= end_idx {
        return String::new();
    }

    chars[start_idx..end_idx].iter().collect()
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
    let str_view = str_vec.to_varlen_view(count);
    let start_view = start_vec.to_view(count);
    let bound_start = substring_bind_data(state).and_then(|data| data.start);
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if !str_view.is_valid(row) || !start_view.is_valid(row) {
            writer.set_null(row);
            continue;
        }
        let start = bound_start.unwrap_or_else(|| start_view.get_i64(row));
        let sub = substring_unicode(str_view.get_inline_string(row).as_str(), start, None);
        writer.write_str(row, &sub);
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
    let str_view = str_vec.to_varlen_view(count);
    let start_view = start_vec.to_view(count);
    let len_view = len_vec.to_view(count);
    let bind_data = substring_bind_data(state);
    let bound_start = bind_data.and_then(|data| data.start);
    let bound_length = bind_data.and_then(|data| data.length);
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if !str_view.is_valid(row) || !start_view.is_valid(row) || !len_view.is_valid(row) {
            writer.set_null(row);
            continue;
        }
        let start = bound_start.unwrap_or_else(|| start_view.get_i64(row));
        let length = bound_length.unwrap_or_else(|| len_view.get_i64(row));
        let sub = substring_unicode(
            str_view.get_inline_string(row).as_str(),
            start,
            Some(length),
        );
        writer.write_str(row, &sub);
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
    let str_view = str_vec.to_varlen_view(count);
    let n_view = n_vec.to_view(count);
    let bound_count = count_bind_data(state).map(|data| data.count);
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if !str_view.is_valid(row) || !n_view.is_valid(row) {
            writer.set_null(row);
            continue;
        }
        let value_inline = str_view.get_inline_string(row);
        let value = value_inline.as_str();
        let n = bound_count.unwrap_or_else(|| n_view.get_i64(row));
        let sub = if n >= 0 {
            value.chars().take(n as usize).collect::<String>()
        } else {
            let chars: Vec<char> = value.chars().collect();
            let take_count = (chars.len() as i64 + n).max(0) as usize;
            chars[..take_count].iter().collect()
        };
        writer.write_str(row, &sub);
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
    let str_view = str_vec.to_varlen_view(count);
    let n_view = n_vec.to_view(count);
    let bound_count = count_bind_data(state).map(|data| data.count);
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if !str_view.is_valid(row) || !n_view.is_valid(row) {
            writer.set_null(row);
            continue;
        }
        let value_inline = str_view.get_inline_string(row);
        let value = value_inline.as_str();
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

        writer.write_str(row, &sub);
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
        let str_vec = Vector::from_strings(&["hello", "world"]);
        let start_vec = Vector::from_i64(&[2, 1]);
        let len_vec = Vector::from_i64(&[3, 2]);
        let chunk = Chunk::from_vectors(vec![str_vec, start_vec, len_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        substring_3_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("ell"));
        assert_eq!(result.get_string(1), Some("wo"));
    }

    #[test]
    fn test_left_function() {
        let str_vec = Vector::from_strings(&["hello", "世界你好"]);
        let n_vec = Vector::from_i64(&[3, 2]);
        let chunk = Chunk::from_vectors(vec![str_vec, n_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        left_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hel"));
        assert_eq!(result.get_string(1), Some("世界"));
    }

    #[test]
    fn test_left_negative() {
        let str_vec = Vector::from_strings(&["hello"]);
        let n_vec = Vector::from_i64(&[-2]);
        let chunk = Chunk::from_vectors(vec![str_vec, n_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        left_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hel"));
    }

    #[test]
    fn test_right_function() {
        let str_vec = Vector::from_strings(&["hello", "世界你好"]);
        let n_vec = Vector::from_i64(&[3, 2]);
        let chunk = Chunk::from_vectors(vec![str_vec, n_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        right_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("llo"));
        assert_eq!(result.get_string(1), Some("你好"));
    }

    #[test]
    fn test_right_negative() {
        let str_vec = Vector::from_strings(&["hello"]);
        let n_vec = Vector::from_i64(&[-2]);
        let chunk = Chunk::from_vectors(vec![str_vec, n_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        right_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("llo"));
    }

    #[test]
    fn test_substring_with_null() {
        let mut str_vec = Vector::from_strings(&["hello", "world"]);
        str_vec.validity_mut().set_null(1);
        let start_vec = Vector::from_i64(&[1, 1]);
        let chunk = Chunk::from_vectors(vec![str_vec, start_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

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

        let chunk = Chunk::from_vectors(vec![
            Vector::from_strings(&["hello"]),
            Vector::from_i64(&[99]),
            Vector::from_i64(&[99]),
        ]);
        let mut result = Vector::new(LogicalType::Varchar);

        substring_3_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("ell"));
    }
}
