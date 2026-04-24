// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VectorType};
use paro_storage::index::fulltext::tokenizer::{tokenizer_from_config, Tokenizer};

use crate::scalar::ExpressionState;
use crate::{ScalarFunction, ScalarFunctionSet};

const SIMPLE_CONFIG: &str = "simple";

fn resolve_tokenizer(config: &str) -> Result<Box<dyn Tokenizer>> {
    let (_kind, tokenizer) = tokenizer_from_config(config)?;
    Ok(tokenizer)
}

fn tokenize_text(tokenizer: &dyn Tokenizer, text: &str) -> String {
    let mut tokens = Vec::new();
    tokenizer.tokenize(text, &mut tokens);
    let mut out = String::new();
    for (idx, token) in tokens.iter().enumerate() {
        if idx > 0 {
            out.push(' ');
        }
        out.push_str(&token.term);
    }
    out
}

fn execute_to_tsvector_with_config(
    input: &Chunk,
    result: &mut Vector,
    default_config: Option<&str>,
) -> Result<()> {
    let count = input.size();
    result.set_count(count);

    let text_col = if default_config.is_some() { 0 } else { 1 };

    let cached_tokenizer = if let Some(cfg) = default_config {
        Some(resolve_tokenizer(cfg)?)
    } else if count > 0 {
        let vec = &input.data[0];
        if vec.vector_type() == VectorType::Constant && !vec.is_null(0) {
            let config = vec.get_string(0).unwrap_or_default();
            Some(resolve_tokenizer(config)?)
        } else {
            None
        }
    } else {
        None
    };

    for i in 0..count {
        let config = if let Some(cfg) = default_config {
            cfg
        } else {
            let config_vec = &input.data[0];
            if config_vec.is_null(i) {
                result.set_null(i, true);
                continue;
            }
            config_vec.get_string(i).unwrap_or_default()
        };

        let text_vec = &input.data[text_col];
        if text_vec.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let tokenizer_box = if cached_tokenizer.is_some() {
            None
        } else {
            Some(resolve_tokenizer(config)?)
        };
        let tokenizer: &dyn Tokenizer = match cached_tokenizer.as_deref() {
            Some(tokenizer) => tokenizer,
            None => tokenizer_box.as_deref().expect("tokenizer just resolved"),
        };

        let text = text_vec.get_string(i).unwrap_or_default();
        let tsvector = tokenize_text(tokenizer, text);
        result.set_string(i, &tsvector);
    }

    Ok(())
}

fn to_tsvector_with_config_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_to_tsvector_with_config(input, result, None)
}

fn to_tsvector_default_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_to_tsvector_with_config(input, result, Some(SIMPLE_CONFIG))
}

pub fn get_to_tsvector_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("to_tsvector".to_string());
    set.add_function(ScalarFunction::new(
        "to_tsvector".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::TsVector,
        to_tsvector_with_config_fn,
    ));
    set.add_function(ScalarFunction::new(
        "to_tsvector".to_string(),
        vec![LogicalType::Varchar],
        LogicalType::TsVector,
        to_tsvector_default_fn,
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
        let set = get_to_tsvector_functions();
        assert_eq!(set.name, "to_tsvector");
        assert_eq!(set.functions.len(), 2);
        assert_eq!(set.functions[0].return_type, LogicalType::TsVector);
        assert_eq!(set.functions[1].return_type, LogicalType::TsVector);
    }

    #[test]
    fn test_to_tsvector_normalizes_text() {
        let state = MockState;
        let input = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["simple"],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["Hello, Vector DB!"],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::TsVector);
        to_tsvector_with_config_fn(&input, &state, &mut result).unwrap();
        assert_eq!(result.get_string(0), Some("hello vector db"));

        let input_default = Chunk::from_vectors(
            vec![paro_common::test_utils::test_string_vector_with_allocator(
                &["Hello, Vector DB!"],
                paro_common::test_utils::test_allocator(),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let mut result_default = paro_common::test_utils::test_vector(LogicalType::TsVector);
        to_tsvector_default_fn(&input_default, &state, &mut result_default).unwrap();
        assert_eq!(result_default.get_string(0), Some("hello vector db"));
    }

    #[test]
    fn test_to_tsvector_rejects_unsupported_config() {
        let state = MockState;
        let input = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["unsupported_lang"],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["hello world"],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::TsVector);
        let err = to_tsvector_with_config_fn(&input, &state, &mut result).unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn test_to_tsvector_supports_chinese_and_japanese_config() {
        let state = MockState;

        let chinese_input = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["chinese"],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["向量数据库"],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut chinese_result = paro_common::test_utils::test_vector(LogicalType::TsVector);
        to_tsvector_with_config_fn(&chinese_input, &state, &mut chinese_result).unwrap();
        assert_eq!(chinese_result.get_string(0), Some("向 量 数 据 库"));

        let japanese_input = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["japanese"],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["東京ベクトルDB"],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut japanese_result = paro_common::test_utils::test_vector(LogicalType::TsVector);
        to_tsvector_with_config_fn(&japanese_input, &state, &mut japanese_result).unwrap();
        assert_eq!(japanese_result.get_string(0), Some("東 京 ベ ク ト ル db"));

        let english_input = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["english"],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["The databases are running quickly"],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut english_result = paro_common::test_utils::test_vector(LogicalType::TsVector);
        to_tsvector_with_config_fn(&english_input, &state, &mut english_result).unwrap();
        assert_eq!(english_result.get_string(0), Some("databas run quick"));
    }
}
