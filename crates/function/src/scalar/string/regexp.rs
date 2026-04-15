// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use regex::Regex;

use crate::{
    BoundScalarFunction, ExpressionState, FunctionData, FunctionErrorMode, FunctionLocalState,
    ScalarBindInput, ScalarFunction, ScalarFunctionSet,
};

#[derive(Debug, Clone)]
enum BoundRegexp {
    Compiled(Regex),
    Invalid,
}

#[derive(Debug, Clone)]
struct RegexpBindData {
    pattern: String,
    case_insensitive: bool,
}

impl FunctionData for RegexpBindData {
    fn clone_box(&self) -> Box<dyn FunctionData> {
        Box::new(self.clone())
    }

    fn equals(&self, other: &dyn FunctionData) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };

        self.pattern == other.pattern && self.case_insensitive == other.case_insensitive
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
struct RegexpLocalState {
    pattern: BoundRegexp,
}

impl FunctionLocalState for RegexpLocalState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn bind_regexp_sensitive(
    function: &ScalarFunction,
    input: &ScalarBindInput,
) -> Result<BoundScalarFunction> {
    bind_regexp(function, input, false)
}

fn bind_regexp_insensitive(
    function: &ScalarFunction,
    input: &ScalarBindInput,
) -> Result<BoundScalarFunction> {
    bind_regexp(function, input, true)
}

fn bind_regexp(
    function: &ScalarFunction,
    input: &ScalarBindInput,
    case_insensitive: bool,
) -> Result<BoundScalarFunction> {
    let mut bound =
        BoundScalarFunction::from(function.clone()).with_error_mode(FunctionErrorMode::Infallible);
    if let Some(pattern) = input.constant_value(1).and_then(value_as_str) {
        bound = bound
            .with_bind_data(RegexpBindData {
                pattern: pattern.to_string(),
                case_insensitive,
            })
            .with_init_local_state(init_regexp_local_state);
    }
    Ok(bound)
}

fn value_as_str(value: &Value) -> Option<&str> {
    match value {
        Value::Varchar(value) => Some(value.as_str()),
        _ => None,
    }
}

fn regexp_bind_data(state: &dyn ExpressionState) -> Option<&RegexpBindData> {
    state
        .bind_data()
        .and_then(|data| data.as_any().downcast_ref::<RegexpBindData>())
}

fn regexp_local_state(state: &dyn ExpressionState) -> Option<&RegexpLocalState> {
    state
        .local_state()
        .and_then(|state| state.as_any().downcast_ref::<RegexpLocalState>())
}

fn init_regexp_local_state(
    _state: &dyn ExpressionState,
    bind_data: Option<&dyn FunctionData>,
) -> Result<Box<dyn FunctionLocalState>> {
    let bind_data = bind_data
        .and_then(|data| data.as_any().downcast_ref::<RegexpBindData>())
        .expect("regexp local state requires bind data");
    let pattern = match compile_regex(&bind_data.pattern, bind_data.case_insensitive) {
        Ok(regex) => BoundRegexp::Compiled(regex),
        Err(_) => BoundRegexp::Invalid,
    };
    Ok(Box::new(RegexpLocalState { pattern }))
}

/// Implementation of `regexp_match(VARCHAR, VARCHAR) -> BOOLEAN` (case-sensitive).
/// The `~` operator.
fn regexp_match_varchar(
    input: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_regexp(input, state, result, false, false)
}

/// Implementation of `regexp_match_insensitive(VARCHAR, VARCHAR) -> BOOLEAN`.
/// The `~*` operator.
fn regexp_match_insensitive_varchar(
    input: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_regexp(input, state, result, true, false)
}

/// Implementation of `regexp_not_match(VARCHAR, VARCHAR) -> BOOLEAN` (case-sensitive).
/// The `!~` operator.
fn regexp_not_match_varchar(
    input: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_regexp(input, state, result, false, true)
}

/// Implementation of `regexp_not_match_insensitive(VARCHAR, VARCHAR) -> BOOLEAN`.
/// The `!~*` operator.
fn regexp_not_match_insensitive_varchar(
    input: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_regexp(input, state, result, true, true)
}

fn execute_regexp(
    input: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
    case_insensitive: bool,
    negate: bool,
) -> Result<()> {
    let count = input.size();
    let text_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing text column".to_string()))?;
    let pattern_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing pattern column".to_string()))?;
    let text_view = text_vec.to_varlen_view(count);
    let pattern_view = pattern_vec.to_varlen_view(count);

    result.set_count(count);
    let local_pattern = regexp_local_state(state).map(|data| &data.pattern);
    let bound_pattern = regexp_bind_data(state);

    for row in 0..count {
        if !text_view.is_valid(row) || !pattern_view.is_valid(row) {
            result.set_null(row, true);
        } else {
            let text_value = text_view.get_inline_string(row);
            let text = text_value.as_str();
            let matched = match local_pattern {
                Some(BoundRegexp::Compiled(regex)) => regex.is_match(text),
                Some(BoundRegexp::Invalid) => false,
                None => {
                    let pattern_value = pattern_view.get_inline_string(row);
                    let pattern = match bound_pattern {
                        Some(bind) => bind.pattern.as_str(),
                        None => pattern_value.as_str(),
                    };
                    match compile_regex(pattern, case_insensitive) {
                        Ok(regex) => regex.is_match(text),
                        Err(_) => false,
                    }
                }
            };
            result.set_bool(row, if negate { !matched } else { matched });
        }
    }

    Ok(())
}

/// Compile a regex pattern with optional case insensitivity.
fn compile_regex(
    pattern: &str,
    case_insensitive: bool,
) -> std::result::Result<Regex, regex::Error> {
    if case_insensitive {
        Regex::new(&format!("(?i){}", pattern))
    } else {
        Regex::new(pattern)
    }
}

/// Get `regexp` function set (case-sensitive `~` operator).
pub fn get_regexp_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("regexp".to_string());
    set.add_function(
        ScalarFunction::new(
            "regexp".to_string(),
            vec![LogicalType::Varchar, LogicalType::Varchar],
            LogicalType::Boolean,
            regexp_match_varchar,
        )
        .with_bind(bind_regexp_sensitive),
    );
    set
}

/// Get `regexp_insensitive` function set (case-insensitive `~*` operator).
pub fn get_regexp_insensitive_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("regexp_insensitive".to_string());
    set.add_function(
        ScalarFunction::new(
            "regexp_insensitive".to_string(),
            vec![LogicalType::Varchar, LogicalType::Varchar],
            LogicalType::Boolean,
            regexp_match_insensitive_varchar,
        )
        .with_bind(bind_regexp_insensitive),
    );
    set
}

/// Get `not_regexp` function set (case-sensitive `!~` operator).
pub fn get_not_regexp_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("not_regexp".to_string());
    set.add_function(
        ScalarFunction::new(
            "not_regexp".to_string(),
            vec![LogicalType::Varchar, LogicalType::Varchar],
            LogicalType::Boolean,
            regexp_not_match_varchar,
        )
        .with_bind(bind_regexp_sensitive),
    );
    set
}

/// Get `not_regexp_insensitive` function set (case-insensitive `!~*` operator).
pub fn get_not_regexp_insensitive_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("not_regexp_insensitive".to_string());
    set.add_function(
        ScalarFunction::new(
            "not_regexp_insensitive".to_string(),
            vec![LogicalType::Varchar, LogicalType::Varchar],
            LogicalType::Boolean,
            regexp_not_match_insensitive_varchar,
        )
        .with_bind(bind_regexp_insensitive),
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
        local_state: Option<Box<dyn FunctionLocalState>>,
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

        fn local_state(&self) -> Option<&dyn FunctionLocalState> {
            self.local_state.as_deref()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_regexp_match_basic() {
        let text = Vector::from_strings(&["hello world", "foo bar", "test123"]);
        let pattern = Vector::from_strings(&["world$", "^baz", r"\d+"]);
        let chunk = Chunk::from_vectors(vec![text, pattern]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        regexp_match_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true)); // "hello world" ~ "world$"
        assert_eq!(result.get_bool(1), Some(false)); // "foo bar" ~ "^baz"
        assert_eq!(result.get_bool(2), Some(true)); // "test123" ~ "\d+"
    }

    #[test]
    fn test_regexp_match_case_sensitive() {
        let text = Vector::from_strings(&["Hello", "hello"]);
        let pattern = Vector::from_strings(&["hello", "hello"]);
        let chunk = Chunk::from_vectors(vec![text, pattern]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        regexp_match_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(false)); // "Hello" ~ "hello" (case-sensitive)
        assert_eq!(result.get_bool(1), Some(true)); // "hello" ~ "hello"
    }

    #[test]
    fn test_regexp_match_insensitive() {
        let text = Vector::from_strings(&["Hello", "HELLO"]);
        let pattern = Vector::from_strings(&["hello", "hello"]);
        let chunk = Chunk::from_vectors(vec![text, pattern]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        regexp_match_insensitive_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true)); // "Hello" ~* "hello"
        assert_eq!(result.get_bool(1), Some(true)); // "HELLO" ~* "hello"
    }

    #[test]
    fn test_regexp_not_match() {
        let text = Vector::from_strings(&["hello world", "foo bar"]);
        let pattern = Vector::from_strings(&["^pg_toast", "^pg_toast"]);
        let chunk = Chunk::from_vectors(vec![text, pattern]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        regexp_not_match_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true)); // "hello world" !~ "^pg_toast"
        assert_eq!(result.get_bool(1), Some(true)); // "foo bar" !~ "^pg_toast"
    }

    #[test]
    fn test_regexp_with_null() {
        let mut text = Vector::from_strings(&["hello", "world"]);
        text.validity_mut().set_null(1);
        let pattern = Vector::from_strings(&["ell", "orl"]);
        let chunk = Chunk::from_vectors(vec![text, pattern]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        regexp_match_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert!(result.is_null(1));
    }

    #[test]
    fn test_regexp_bind_data_reuses_compiled_pattern() {
        let set = get_regexp_functions();
        let (function, _) = set
            .bind(&[LogicalType::Varchar, LogicalType::Varchar])
            .unwrap();
        let bound = function
            .bind(&ScalarBindInput::new(
                vec![LogicalType::Varchar, LogicalType::Varchar],
                vec![None, Some(Value::Varchar("^hello$".to_string()))],
            ))
            .unwrap();
        let local_state = bound
            .init_local_state
            .map(|init| init(&MockState, bound.bind_data.as_deref()).unwrap());
        let state = BindState {
            bind_data: bound.bind_data.clone().unwrap(),
            local_state,
        };

        let chunk = Chunk::from_vectors(vec![
            Vector::from_strings(&["hello", "world"]),
            Vector::from_strings(&["nomatch", "nomatch"]),
        ]);
        let mut result = Vector::new(LogicalType::Boolean);

        regexp_match_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(false));
    }

    #[test]
    fn test_invalid_bound_regexp_short_circuits_to_false() {
        let set = get_regexp_functions();
        let (function, _) = set
            .bind(&[LogicalType::Varchar, LogicalType::Varchar])
            .unwrap();
        let bound = function
            .bind(&ScalarBindInput::new(
                vec![LogicalType::Varchar, LogicalType::Varchar],
                vec![None, Some(Value::Varchar("[".to_string()))],
            ))
            .unwrap();
        let local_state = bound
            .init_local_state
            .map(|init| init(&MockState, bound.bind_data.as_deref()).unwrap());
        let state = BindState {
            bind_data: bound.bind_data.clone().unwrap(),
            local_state,
        };

        let chunk = Chunk::from_vectors(vec![
            Vector::from_strings(&["hello"]),
            Vector::from_strings(&["ignored"]),
        ]);
        let mut result = Vector::new(LogicalType::Boolean);

        regexp_match_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(false));
    }
}
