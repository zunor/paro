// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VectorType};
use paro_storage::index::fulltext::tokenizer::{Token, Tokenizer};

use crate::scalar::ExpressionState;
use crate::{ScalarFunction, ScalarFunctionSet};

use super::fallback::{
    default_tokenizer, parse_legacy_query, parse_serialized_tsquery, query_matches_text,
    tokenize_serialized_tsvector,
};

fn fulltext_match_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    evaluate_legacy_fulltext_match_fallback(input, result)
}

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
    let tokenizer = default_tokenizer();
    let query_is_const = query_vec.vector_type() == VectorType::Constant;
    let cached_query = if count > 0 && query_is_const && !query_vec.is_null(0) {
        Some(parse_legacy_query(
            query_vec.get_string(0).unwrap_or_default(),
        )?)
    } else {
        None
    };

    for i in 0..count {
        if text_vec.is_null(i) || query_vec.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let parsed_query;
        let query = if let Some(query) = cached_query.as_ref() {
            query
        } else {
            parsed_query = parse_legacy_query(query_vec.get_string(i).unwrap_or_default())?;
            &parsed_query
        };

        let text = text_vec.get_string(i).unwrap_or_default();
        let mut tokens = Vec::<Token>::new();
        tokenizer.tokenize(text, &mut tokens);
        result.set_bool(i, query_matches_text(query, &tokens));
    }

    Ok(())
}

fn evaluate_internal_fulltext_match_fallback(input: &Chunk, result: &mut Vector) -> Result<()> {
    let count = input.size();
    result.set_count(count);

    let text_vec = &input.data[0];
    let query_vec = &input.data[1];
    let query_is_const = query_vec.vector_type() == VectorType::Constant;
    let cached_query = if count > 0 && query_is_const && !query_vec.is_null(0) {
        Some(parse_serialized_tsquery(
            query_vec.get_string(0).unwrap_or_default(),
        )?)
    } else {
        None
    };

    for i in 0..count {
        if text_vec.is_null(i) || query_vec.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let parsed_query;
        let query = if let Some(query) = cached_query.as_ref() {
            query
        } else {
            parsed_query = parse_serialized_tsquery(query_vec.get_string(i).unwrap_or_default())?;
            &parsed_query
        };

        let text = text_vec.get_string(i).unwrap_or_default();
        let tokens: Vec<_> = tokenize_serialized_tsvector(text).collect();
        result.set_bool(i, query_matches_text(query, &tokens));
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
    }

    #[test]
    fn test_internal_fallback_behavior_matches_legacy() {
        let text_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["vector database systems", "hello world"],
            paro_common::test_utils::test_allocator(),
        );
        let legacy_query_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["vector database", "database"],
            paro_common::test_utils::test_allocator(),
        );
        let internal_query_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["vector & database", "database"],
            paro_common::test_utils::test_allocator(),
        );
        let legacy_input = Chunk::from_vectors(
            vec![text_vec.clone(), legacy_query_vec],
            paro_common::test_utils::test_allocator(),
        );
        let internal_input = Chunk::from_vectors(
            vec![text_vec, internal_query_vec],
            paro_common::test_utils::test_allocator(),
        );
        let state = MockState;

        let mut legacy_match = paro_common::test_utils::test_vector(LogicalType::Boolean);
        let mut internal_match = paro_common::test_utils::test_vector(LogicalType::Boolean);
        fulltext_match_fn(&legacy_input, &state, &mut legacy_match).unwrap();
        fulltext_match_internal_fn(&internal_input, &state, &mut internal_match).unwrap();
        assert_eq!(legacy_match.get_bool(0), internal_match.get_bool(0));
        assert_eq!(legacy_match.get_bool(1), internal_match.get_bool(1));
    }

    #[test]
    fn test_legacy_fulltext_match_uses_tokenizer_not_whitespace_split() {
        let state = MockState;
        let input = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["Vector-database systems"],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["DATABASE"],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);
        fulltext_match_fn(&input, &state, &mut result).unwrap();
        assert_eq!(result.get_bool(0), Some(true));
    }

    #[test]
    fn test_non_constant_query_rows_do_not_reuse_previous_parse() {
        let state = MockState;
        let input = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["vector database", "graph storage"],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["vector", "graph"],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);
        fulltext_match_fn(&input, &state, &mut result).unwrap();
        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(true));
    }
}
