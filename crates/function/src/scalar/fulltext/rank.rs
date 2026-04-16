// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VectorType};
use paro_storage::index::fulltext::tokenizer::Tokenizer;

use crate::scalar::ExpressionState;
use crate::{ScalarFunction, ScalarFunctionSet};

use super::fallback::{
    default_tokenizer, parse_legacy_query, parse_serialized_tsquery, score_query,
    tokenize_serialized_tsvector, FullTextScoreMode,
};

fn bm25_fn(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    evaluate_legacy_bm25_fallback(input, result)
}

fn bm25_score_internal_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    evaluate_internal_score_fallback(input, result, FullTextScoreMode::Bm25)
}

fn ts_rank_fn(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    evaluate_internal_score_fallback(input, result, FullTextScoreMode::Bm25)
}

fn ts_rank_cd_fn(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    evaluate_internal_score_fallback(input, result, FullTextScoreMode::CoverDensity)
}

fn evaluate_legacy_bm25_fallback(input: &Chunk, result: &mut Vector) -> Result<()> {
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
        let tokens = tokenizer.tokenize_to_vec(text);
        let score = score_query(&tokens, query, FullTextScoreMode::Bm25);
        result.set_f32(i, score);
    }

    Ok(())
}

fn evaluate_internal_score_fallback(
    input: &Chunk,
    result: &mut Vector,
    mode: FullTextScoreMode,
) -> Result<()> {
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
        let score = score_query(&tokens, query, mode);
        result.set_f32(i, score);
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

pub fn get_ts_rank_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("ts_rank".to_string());
    set.add_function(ScalarFunction::new(
        "ts_rank".to_string(),
        vec![LogicalType::TsVector, LogicalType::TsQuery],
        LogicalType::Float,
        ts_rank_fn,
    ));
    set
}

pub fn get_ts_rank_cd_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("ts_rank_cd".to_string());
    set.add_function(ScalarFunction::new(
        "ts_rank_cd".to_string(),
        vec![LogicalType::TsVector, LogicalType::TsQuery],
        LogicalType::Float,
        ts_rank_cd_fn,
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
    fn test_registration_signatures() {
        let rank_set = get_ts_rank_functions();
        assert_eq!(rank_set.name, "ts_rank");
        assert_eq!(
            rank_set.functions[0].arguments,
            vec![LogicalType::TsVector, LogicalType::TsQuery]
        );
    }

    #[test]
    fn test_bm25_ranking_prefers_repeated_terms() {
        let state = MockState;
        let input = Chunk::from_vectors(vec![
            Vector::from_strings(&["hello world", "hello hello world"]),
            Vector::from_strings(&["hello", "hello"]),
        ]);
        let mut result = Vector::new(LogicalType::Float);
        bm25_fn(&input, &state, &mut result).unwrap();
        assert!(result.get_f32(1) >= result.get_f32(0));
    }

    #[test]
    fn test_ts_rank_cd_differs_from_ts_rank() {
        let state = MockState;
        let input = Chunk::from_vectors(vec![
            Vector::from_strings(&["alpha beta", "alpha x beta"]),
            Vector::from_strings(&["alpha & beta", "alpha & beta"]),
        ]);

        let mut rank = Vector::new(LogicalType::Float);
        let mut rank_cd = Vector::new(LogicalType::Float);
        ts_rank_fn(&input, &state, &mut rank).unwrap();
        ts_rank_cd_fn(&input, &state, &mut rank_cd).unwrap();
        let rank0 = rank.get_f32(0).unwrap_or_default();
        let rank1 = rank.get_f32(1).unwrap_or_default();
        let rank_cd0 = rank_cd.get_f32(0).unwrap_or_default();
        let rank_cd1 = rank_cd.get_f32(1).unwrap_or_default();
        assert!((rank0 - rank1).abs() < f32::EPSILON);
        assert!(rank_cd0 > rank_cd1);
    }

    #[test]
    fn test_non_constant_query_rows_keep_distinct_scores() {
        let state = MockState;
        let input = Chunk::from_vectors(vec![
            Vector::from_strings(&["vector database", "graph graph storage"]),
            Vector::from_strings(&["vector", "graph"]),
        ]);
        let mut result = Vector::new(LogicalType::Float);
        bm25_fn(&input, &state, &mut result).unwrap();
        assert!(result.get_f32(0).unwrap_or_default() > 0.0);
        assert!(result.get_f32(1).unwrap_or_default() > result.get_f32(0).unwrap_or_default());
    }
}
