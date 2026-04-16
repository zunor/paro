// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VectorType};
use paro_storage::index::fulltext::tokenizer::{tokenizer_from_config, SpannedToken, Tokenizer};

use crate::scalar::ExpressionState;
use crate::{ScalarFunction, ScalarFunctionSet};

use super::fallback::{collect_match_extents, parse_serialized_tsquery, MatchExtent};

const SIMPLE_CONFIG: &str = "simple";
const WINDOW_TOKENS: usize = 24;

fn resolve_tokenizer(config: &str) -> Result<Box<dyn Tokenizer>> {
    let (_kind, tokenizer) = tokenizer_from_config(config)?;
    Ok(tokenizer)
}

fn choose_best_window(weights: &[u32]) -> (usize, usize) {
    if weights.is_empty() {
        return (0, 0);
    }
    if weights.len() <= WINDOW_TOKENS {
        return (0, weights.len());
    }

    let mut prefix = vec![0u32; weights.len() + 1];
    for (idx, &weight) in weights.iter().enumerate() {
        prefix[idx + 1] = prefix[idx] + weight;
    }

    let mut best_start = 0usize;
    let mut best_score = 0u32;
    for start in 0..=(weights.len() - WINDOW_TOKENS) {
        let end = start + WINDOW_TOKENS;
        let score = prefix[end] - prefix[start];
        if score > best_score {
            best_score = score;
            best_start = start;
        }
    }

    (best_start, best_start + WINDOW_TOKENS)
}

fn apply_extents(weights: &mut [u32], extents: &[MatchExtent]) {
    if weights.is_empty() {
        return;
    }
    for extent in extents {
        let start = extent.start_pos as usize;
        let end = (extent.end_pos as usize).min(weights.len() - 1);
        let weight = extent.kind.weight();
        for idx in start..=end {
            weights[idx] = weights[idx].saturating_add(weight);
        }
    }
}

fn render_highlighted_document(
    document: &str,
    spans: &[SpannedToken],
    extents: &[MatchExtent],
) -> String {
    if document.is_empty() || spans.is_empty() || extents.is_empty() {
        return document.to_string();
    }

    let mut weights = vec![0u32; spans.len()];
    apply_extents(&mut weights, extents);
    if weights.iter().all(|weight| *weight == 0) {
        return document.to_string();
    }

    let (window_start, window_end) = choose_best_window(&weights);
    let snippet_start = spans[window_start].byte_start;
    let snippet_end = spans[window_end - 1].byte_end;

    let mut out = String::new();
    if snippet_start > 0 {
        out.push_str("... ");
    }

    let mut cursor = snippet_start;
    for token_idx in window_start..window_end {
        let token = &spans[token_idx];
        out.push_str(&document[cursor..token.byte_start]);
        let raw = &document[token.byte_start..token.byte_end];
        if weights[token_idx] > 0 {
            out.push_str("<b>");
            out.push_str(raw);
            out.push_str("</b>");
        } else {
            out.push_str(raw);
        }
        cursor = token.byte_end;
    }
    out.push_str(&document[cursor..snippet_end]);

    if snippet_end < document.len() {
        out.push_str("...");
    }
    out
}

fn execute_ts_headline(
    input: &Chunk,
    result: &mut Vector,
    default_config: Option<&str>,
) -> Result<()> {
    let count = input.size();
    result.set_count(count);

    let (doc_col, query_col) = if default_config.is_some() {
        (0usize, 1usize)
    } else {
        (1usize, 2usize)
    };
    let cached_tokenizer = if let Some(cfg) = default_config {
        Some(resolve_tokenizer(cfg)?)
    } else if count > 0
        && input.data[0].vector_type() == VectorType::Constant
        && !input.data[0].is_null(0)
    {
        let config = input.data[0].get_string(0).unwrap_or_default();
        Some(resolve_tokenizer(config)?)
    } else {
        None
    };
    let query_vec = &input.data[query_col];
    let query_is_const = query_vec.vector_type() == VectorType::Constant;
    let cached_query = if count > 0 && query_is_const && !query_vec.is_null(0) {
        Some(parse_serialized_tsquery(
            query_vec.get_string(0).unwrap_or_default(),
        )?)
    } else {
        None
    };

    for row_idx in 0..count {
        let config = if let Some(cfg) = default_config {
            cfg
        } else {
            let config_vec = &input.data[0];
            if config_vec.is_null(row_idx) {
                result.set_null(row_idx, true);
                continue;
            }
            config_vec.get_string(row_idx).unwrap_or_default()
        };

        let doc_vec = &input.data[doc_col];
        if query_vec.is_null(row_idx) {
            result.set_null(row_idx, true);
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

        let parsed_query;
        let query = if let Some(query) = cached_query.as_ref() {
            query
        } else {
            parsed_query =
                parse_serialized_tsquery(query_vec.get_string(row_idx).unwrap_or_default())?;
            &parsed_query
        };

        let document = if doc_vec.is_null(row_idx) {
            ""
        } else {
            doc_vec.get_string(row_idx).unwrap_or_default()
        };
        let spans = tokenizer.tokenize_spanned_to_vec(document);
        let extents = collect_match_extents(&spans, query);
        let highlighted = render_highlighted_document(document, &spans, &extents);
        result.set_string(row_idx, &highlighted);
    }

    Ok(())
}

fn ts_headline_with_config_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_ts_headline(input, result, None)
}

fn ts_headline_default_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_ts_headline(input, result, Some(SIMPLE_CONFIG))
}

pub fn get_ts_headline_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("ts_headline".to_string());
    set.add_function(
        ScalarFunction::new(
            "ts_headline".to_string(),
            vec![
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::TsQuery,
            ],
            LogicalType::Varchar,
            ts_headline_with_config_fn,
        )
        .with_null_handling(crate::FunctionNullHandling::SpecialHandling),
    );
    set.add_function(
        ScalarFunction::new(
            "ts_headline".to_string(),
            vec![LogicalType::Varchar, LogicalType::TsQuery],
            LogicalType::Varchar,
            ts_headline_default_fn,
        )
        .with_null_handling(crate::FunctionNullHandling::SpecialHandling),
    );
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_storage::index::fulltext::query_parser::{parse_plainto_tsquery, serialize_query};
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
    fn test_ts_headline_registration() {
        let set = get_ts_headline_functions();
        assert_eq!(set.name, "ts_headline");
        assert_eq!(set.functions.len(), 2);
        assert_eq!(
            set.functions[0].arguments,
            vec![
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::TsQuery
            ]
        );
        assert_eq!(
            set.functions[1].arguments,
            vec![LogicalType::Varchar, LogicalType::TsQuery]
        );
    }

    #[test]
    fn test_ts_headline_highlights_matching_terms() {
        let state = MockState;
        let input = Chunk::from_vectors(vec![
            Vector::from_strings(&["simple"]),
            Vector::from_strings(&["Vector database systems"]),
            Vector::from_strings(&["vector & database"]),
        ]);
        let mut result = Vector::new(LogicalType::Varchar);
        ts_headline_with_config_fn(&input, &state, &mut result).unwrap();
        assert_eq!(
            result.get_string(0),
            Some("<b>Vector</b> <b>database</b> systems")
        );
    }

    #[test]
    fn test_ts_headline_default_signature_and_nulls() {
        let state = MockState;
        let mut doc = Vector::from_strings(&["Vector database", "plain text"]);
        let mut query = Vector::from_strings(&["database", "vector"]);
        doc.set_null(1, true);
        query.set_null(1, true);
        let input = Chunk::from_vectors(vec![doc, query]);
        let mut result = Vector::new(LogicalType::Varchar);
        ts_headline_default_fn(&input, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("Vector <b>database</b>"));
        assert!(result.is_null(1));
    }

    #[test]
    fn test_ts_headline_rejects_unsupported_config() {
        let state = MockState;
        let input = Chunk::from_vectors(vec![
            Vector::from_strings(&["unsupported_lang"]),
            Vector::from_strings(&["Vector database systems"]),
            Vector::from_strings(&["vector"]),
        ]);
        let mut result = Vector::new(LogicalType::Varchar);
        let err = ts_headline_with_config_fn(&input, &state, &mut result).unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn test_ts_headline_phrase_and_not_semantics() {
        let state = MockState;
        let input = Chunk::from_vectors(vec![
            Vector::from_strings(&["simple"]),
            Vector::from_strings(&["vector database search spam"]),
            Vector::from_strings(&["vector <-> database"]),
        ]);
        let mut result = Vector::new(LogicalType::Varchar);
        ts_headline_with_config_fn(&input, &state, &mut result).unwrap();
        assert_eq!(
            result.get_string(0),
            Some("<b>vector</b> <b>database</b> search spam")
        );

        let prefix_input = Chunk::from_vectors(vec![
            Vector::from_strings(&["simple"]),
            Vector::from_strings(&["vectors everywhere"]),
            Vector::from_strings(&["vec:*"]),
        ]);
        let mut prefix_result = Vector::new(LogicalType::Varchar);
        ts_headline_with_config_fn(&prefix_input, &state, &mut prefix_result).unwrap();
        assert_eq!(
            prefix_result.get_string(0),
            Some("<b>vectors</b> everywhere")
        );

        let not_input = Chunk::from_vectors(vec![
            Vector::from_strings(&["simple"]),
            Vector::from_strings(&["vector database spam"]),
            Vector::from_strings(&["!spam"]),
        ]);
        let mut not_result = Vector::new(LogicalType::Varchar);
        ts_headline_with_config_fn(&not_input, &state, &mut not_result).unwrap();
        assert_eq!(not_result.get_string(0), Some("vector database spam"));
    }

    #[test]
    fn test_ts_headline_handles_english_stemming_and_cjk_spans() {
        let state = MockState;

        let english_tokenizer = resolve_tokenizer("english").unwrap();
        let english_query = serialize_query(
            &parse_plainto_tsquery("database", english_tokenizer.as_ref(), 1, None).unwrap(),
        );
        let english_input = Chunk::from_vectors(vec![
            Vector::from_strings(&["english"]),
            Vector::from_strings(&["Databases are practical"]),
            Vector::from_strings(&[english_query.as_str()]),
        ]);
        let mut english_result = Vector::new(LogicalType::Varchar);
        ts_headline_with_config_fn(&english_input, &state, &mut english_result).unwrap();
        assert_eq!(
            english_result.get_string(0),
            Some("<b>Databases</b> are practical")
        );

        let chinese_tokenizer = resolve_tokenizer("chinese").unwrap();
        let chinese_query = serialize_query(
            &parse_plainto_tsquery("向", chinese_tokenizer.as_ref(), 1, None).unwrap(),
        );
        let chinese_input = Chunk::from_vectors(vec![
            Vector::from_strings(&["chinese"]),
            Vector::from_strings(&["向量"]),
            Vector::from_strings(&[chinese_query.as_str()]),
        ]);
        let mut chinese_result = Vector::new(LogicalType::Varchar);
        ts_headline_with_config_fn(&chinese_input, &state, &mut chinese_result).unwrap();
        assert_eq!(chinese_result.get_string(0), Some("<b>向</b>量"));
    }

    #[test]
    fn test_ts_headline_non_constant_queries_do_not_reuse_previous_ast() {
        let state = MockState;
        let tokenizer = resolve_tokenizer(SIMPLE_CONFIG).unwrap();
        let vector_query =
            serialize_query(&parse_plainto_tsquery("vector", tokenizer.as_ref(), 1, None).unwrap());
        let graph_query =
            serialize_query(&parse_plainto_tsquery("graph", tokenizer.as_ref(), 1, None).unwrap());

        let input = Chunk::from_vectors(vec![
            Vector::from_strings(&["vector database", "graph storage"]),
            Vector::from_strings(&[vector_query.as_str(), graph_query.as_str()]),
        ]);
        let mut result = Vector::new(LogicalType::Varchar);
        ts_headline_default_fn(&input, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("<b>vector</b> database"));
        assert_eq!(result.get_string(1), Some("<b>graph</b> storage"));
    }
}
