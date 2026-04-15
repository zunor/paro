// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::scalar::ExpressionState;
use crate::{ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

/// The `bm25` function returns a score for a text column against a query string.
/// In most cases, this is pushed down to the full-text index.
/// For sequential scan fallback, it returns a simple term frequency count.
fn bm25_fn(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    evaluate_legacy_bm25_fallback(input, result)
}

/// Internal score function used by `ts_rank`-style planning path.
pub(super) fn bm25_score_internal_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    evaluate_internal_bm25_fallback(input, result)
}

fn evaluate_legacy_bm25_fallback(input: &Chunk, result: &mut Vector) -> Result<()> {
    let count = input.size();
    result.set_count(count);

    let text_vec = &input.data[0];
    let query_vec = &input.data[1];

    for i in 0..count {
        if text_vec.is_null(i) || query_vec.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let text = text_vec.get_string(i).unwrap_or_default().to_lowercase();
        let query = query_vec.get_string(i).unwrap_or_default().to_lowercase();
        let parsed = super::eval::parse_legacy_query(&query)?;
        result.set_f32(i, super::eval::score_query_text(&parsed, &text));
    }

    Ok(())
}

fn evaluate_internal_bm25_fallback(input: &Chunk, result: &mut Vector) -> Result<()> {
    let count = input.size();
    result.set_count(count);

    let text_vec = &input.data[0];
    let query_vec = &input.data[1];

    for i in 0..count {
        if text_vec.is_null(i) || query_vec.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let text = text_vec.get_string(i).unwrap_or_default().to_lowercase();
        let query = query_vec.get_string(i).unwrap_or_default().to_lowercase();
        let parsed = super::eval::parse_internal_query(&query)?;
        result.set_f32(i, super::eval::score_query_text(&parsed, &text));
    }

    Ok(())
}

pub fn get_bm25_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("bm25".to_string());
    set.add_function(ScalarFunction::new(
        "bm25".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::Float,
        bm25_fn,
    ));
    set
}

pub fn get_bm25_score_internal_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("bm25_score_internal".to_string());
    set.add_function(ScalarFunction::new(
        "bm25_score_internal".to_string(),
        vec![LogicalType::TsVector, LogicalType::TsQuery],
        LogicalType::Float,
        bm25_score_internal_fn,
    ));
    set
}

/// `fulltext_match(text, query)` returns true if any terms match.
fn fulltext_match_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    evaluate_legacy_fulltext_match_fallback(input, result)
}

/// Internal boolean match function used by `@@` planning path.
fn fulltext_match_internal_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    evaluate_internal_fulltext_match_fallback(input, result)
}

fn evaluate_legacy_fulltext_match_fallback(input: &Chunk, result: &mut Vector) -> Result<()> {
    let count = input.size();
    result.set_count(count);

    let text_vec = &input.data[0];
    let query_vec = &input.data[1];

    for i in 0..count {
        if text_vec.is_null(i) || query_vec.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let text = text_vec.get_string(i).unwrap_or_default().to_lowercase();
        let query = query_vec.get_string(i).unwrap_or_default().to_lowercase();
        let parsed = super::eval::parse_legacy_query(&query)?;
        result.set_bool(i, super::eval::query_matches_text(&parsed, &text));
    }

    Ok(())
}

fn evaluate_internal_fulltext_match_fallback(input: &Chunk, result: &mut Vector) -> Result<()> {
    let count = input.size();
    result.set_count(count);

    let text_vec = &input.data[0];
    let query_vec = &input.data[1];

    for i in 0..count {
        if text_vec.is_null(i) || query_vec.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let text = text_vec.get_string(i).unwrap_or_default().to_lowercase();
        let query = query_vec.get_string(i).unwrap_or_default().to_lowercase();
        let parsed = super::eval::parse_internal_query(&query)?;
        result.set_bool(i, super::eval::query_matches_text(&parsed, &text));
    }

    Ok(())
}

pub fn get_fulltext_match_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("fulltext_match".to_string());
    set.add_function(ScalarFunction::new(
        "fulltext_match".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::Boolean,
        fulltext_match_fn,
    ));
    set
}

pub fn get_fulltext_match_internal_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("fulltext_match_internal".to_string());
    set.add_function(ScalarFunction::new(
        "fulltext_match_internal".to_string(),
        vec![LogicalType::TsVector, LogicalType::TsQuery],
        LogicalType::Boolean,
        fulltext_match_internal_fn,
    ));
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;

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

    #[test]
    fn test_internal_function_signatures() {
        let match_set = get_fulltext_match_internal_functions();
        assert_eq!(match_set.name, "fulltext_match_internal");
        assert_eq!(match_set.functions.len(), 1);
        assert_eq!(
            match_set.functions[0].arguments,
            vec![LogicalType::TsVector, LogicalType::TsQuery]
        );
        assert_eq!(match_set.functions[0].return_type, LogicalType::Boolean);

        let score_set = get_bm25_score_internal_functions();
        assert_eq!(score_set.name, "bm25_score_internal");
        assert_eq!(score_set.functions.len(), 1);
        assert_eq!(
            score_set.functions[0].arguments,
            vec![LogicalType::TsVector, LogicalType::TsQuery]
        );
        assert_eq!(score_set.functions[0].return_type, LogicalType::Float);
    }

    #[test]
    fn test_internal_fallback_behavior_matches_legacy() {
        let text_vec = Vector::from_strings(&["vector database systems", "hello world"]);
        let legacy_query_vec = Vector::from_strings(&["vector database", "database"]);
        let internal_query_vec = Vector::from_strings(&["vector & database", "database"]);
        let legacy_input = Chunk::from_vectors(vec![text_vec.clone(), legacy_query_vec]);
        let internal_input = Chunk::from_vectors(vec![text_vec, internal_query_vec]);
        let state = MockState;

        let mut legacy_match = Vector::new(LogicalType::Boolean);
        let mut internal_match = Vector::new(LogicalType::Boolean);
        fulltext_match_fn(&legacy_input, &state, &mut legacy_match).unwrap();
        fulltext_match_internal_fn(&internal_input, &state, &mut internal_match).unwrap();
        assert_eq!(legacy_match.get_bool(0), internal_match.get_bool(0));
        assert_eq!(legacy_match.get_bool(1), internal_match.get_bool(1));

        let mut legacy_score = Vector::new(LogicalType::Float);
        let mut internal_score = Vector::new(LogicalType::Float);
        bm25_fn(&legacy_input, &state, &mut legacy_score).unwrap();
        bm25_score_internal_fn(&internal_input, &state, &mut internal_score).unwrap();
        assert_eq!(legacy_score.get_f32(0), internal_score.get_f32(0));
        assert_eq!(legacy_score.get_f32(1), internal_score.get_f32(1));
    }
}
