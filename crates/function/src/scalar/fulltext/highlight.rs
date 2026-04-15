use std::collections::HashSet;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_storage::index::fulltext::tokenizer::{DefaultTokenizer, Tokenizer};

use crate::scalar::ExpressionState;
use crate::{ScalarFunction, ScalarFunctionSet};

const SIMPLE_CONFIG: &str = "simple";
const WINDOW_TOKENS: usize = 24;

#[derive(Debug, Clone)]
struct TokenSpan {
    term: String,
    start: usize,
    end: usize,
}

fn ensure_simple_config(config: &str) -> Result<()> {
    if config.trim().eq_ignore_ascii_case(SIMPLE_CONFIG) {
        Ok(())
    } else {
        Err(paro_error::not_supported(format!(
            "full-text config '{}' (only 'simple' is currently supported)",
            config
        )))
    }
}

fn collect_query_terms(tokenizer: &dyn Tokenizer, tsquery_text: &str) -> HashSet<String> {
    let mut tokens = Vec::new();
    tokenizer.tokenize(tsquery_text, &mut tokens);
    tokens.into_iter().map(|t| t.term).collect()
}

fn collect_document_token_spans(text: &str) -> Vec<TokenSpan> {
    let mut spans = Vec::new();
    let mut token_start: Option<usize> = None;

    for (idx, ch) in text.char_indices() {
        if ch.is_alphanumeric() {
            if token_start.is_none() {
                token_start = Some(idx);
            }
            continue;
        }

        if let Some(start) = token_start.take() {
            let raw = &text[start..idx];
            spans.push(TokenSpan {
                term: raw.to_lowercase(),
                start,
                end: idx,
            });
        }
    }

    if let Some(start) = token_start {
        let raw = &text[start..];
        spans.push(TokenSpan {
            term: raw.to_lowercase(),
            start,
            end: text.len(),
        });
    }

    spans
}

fn choose_best_window(matched: &[bool]) -> (usize, usize) {
    if matched.is_empty() {
        return (0, 0);
    }
    if matched.len() <= WINDOW_TOKENS {
        return (0, matched.len());
    }

    let mut prefix = vec![0usize; matched.len() + 1];
    for (idx, &m) in matched.iter().enumerate() {
        prefix[idx + 1] = prefix[idx] + usize::from(m);
    }

    let mut best_start = 0usize;
    let mut best_score = 0usize;
    for start in 0..=(matched.len() - WINDOW_TOKENS) {
        let end = start + WINDOW_TOKENS;
        let score = prefix[end] - prefix[start];
        if score > best_score {
            best_score = score;
            best_start = start;
        }
    }

    (best_start, best_start + WINDOW_TOKENS)
}

fn render_highlighted_document(document: &str, query_terms: &HashSet<String>) -> String {
    if document.is_empty() || query_terms.is_empty() {
        return document.to_string();
    }

    let spans = collect_document_token_spans(document);
    if spans.is_empty() {
        return document.to_string();
    }

    let matched: Vec<bool> = spans
        .iter()
        .map(|token| query_terms.contains(token.term.as_str()))
        .collect();
    if !matched.iter().any(|m| *m) {
        return document.to_string();
    }

    let (window_start, window_end) = choose_best_window(&matched);
    let snippet_start = spans[window_start].start;
    let snippet_end = spans[window_end - 1].end;

    let mut out = String::new();
    if snippet_start > 0 {
        out.push_str("... ");
    }

    let mut cursor = snippet_start;
    for token_idx in window_start..window_end {
        let token = &spans[token_idx];
        out.push_str(&document[cursor..token.start]);
        let raw = &document[token.start..token.end];
        if query_terms.contains(token.term.as_str()) {
            out.push_str("<b>");
            out.push_str(raw);
            out.push_str("</b>");
        } else {
            out.push_str(raw);
        }
        cursor = token.end;
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

    let tokenizer = DefaultTokenizer::new();
    let (doc_col, query_col) = if default_config.is_some() {
        (0usize, 1usize)
    } else {
        (1usize, 2usize)
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
        let query_vec = &input.data[query_col];
        if query_vec.is_null(row_idx) {
            result.set_null(row_idx, true);
            continue;
        }

        ensure_simple_config(config)?;

        let document = if doc_vec.is_null(row_idx) {
            ""
        } else {
            doc_vec.get_string(row_idx).unwrap_or_default()
        };
        let query = query_vec.get_string(row_idx).unwrap_or_default();
        let query_terms = collect_query_terms(&tokenizer, query);
        let highlighted = render_highlighted_document(document, &query_terms);
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
    use std::any::Any;

    use super::*;

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
            Vector::from_strings(&["vector database"]),
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
            Vector::from_strings(&["english"]),
            Vector::from_strings(&["Vector database systems"]),
            Vector::from_strings(&["vector"]),
        ]);
        let mut result = Vector::new(LogicalType::Varchar);
        let err = ts_headline_with_config_fn(&input, &state, &mut result).unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn test_render_highlighted_document_prefers_best_window() {
        let mut terms = HashSet::new();
        terms.insert("needle".to_string());
        let doc = "a b c d e f g h i j k l m n o p q r s t u v w x y needle z needle";
        let rendered = render_highlighted_document(doc, &terms);
        assert!(rendered.contains("<b>needle</b>"));
    }
}
