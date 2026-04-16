// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VectorType};
use paro_storage::index::fulltext::query_parser::{
    parse_phraseto_tsquery, parse_plainto_tsquery, parse_to_tsquery, parse_websearch_to_tsquery,
    serialize_query, ParsedQuery,
};
use paro_storage::index::fulltext::tokenizer::{tokenizer_from_config, Tokenizer};

use crate::scalar::ExpressionState;
use crate::{ScalarFunction, ScalarFunctionSet};

const SIMPLE_CONFIG: &str = "simple";
const MIN_TOKEN_LEN: usize = 1;
const MAX_TOKEN_LEN: Option<usize> = None;

type QueryParseFn = fn(&str, &dyn Tokenizer, usize, Option<usize>) -> Result<ParsedQuery>;

fn resolve_tokenizer(config: &str) -> Result<Box<dyn Tokenizer>> {
    let (_kind, tokenizer) = tokenizer_from_config(config)?;
    Ok(tokenizer)
}

fn execute_to_tsquery_with_config(
    input: &Chunk,
    result: &mut Vector,
    parse_fn: QueryParseFn,
    default_config: Option<&str>,
) -> Result<()> {
    let count = input.size();
    result.set_count(count);

    let query_col = if default_config.is_some() { 0 } else { 1 };
    let query_vec = &input.data[query_col];
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
    let mut cached_serialized_query = None;
    if let Some(tokenizer) = cached_tokenizer.as_deref() {
        if count > 0 && query_vec.vector_type() == VectorType::Constant && !query_vec.is_null(0) {
            let query_text = query_vec.get_string(0).unwrap_or_default();
            let parsed = parse_fn(query_text, tokenizer, MIN_TOKEN_LEN, MAX_TOKEN_LEN)?;
            cached_serialized_query = Some(serialize_query(&parsed));
        }
    }

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

        let query_vec = &input.data[query_col];
        if query_vec.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        if let Some(serialized) = cached_serialized_query.as_ref() {
            result.set_string(i, serialized);
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

        let query_text = query_vec.get_string(i).unwrap_or_default();
        let parsed = parse_fn(query_text, tokenizer, MIN_TOKEN_LEN, MAX_TOKEN_LEN)?;
        let tsquery = serialize_query(&parsed);
        result.set_string(i, &tsquery);
    }

    Ok(())
}

fn to_tsquery_with_config_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_to_tsquery_with_config(input, result, parse_to_tsquery, None)
}

fn plainto_tsquery_with_config_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_to_tsquery_with_config(input, result, parse_plainto_tsquery, None)
}

fn plainto_tsquery_default_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_to_tsquery_with_config(input, result, parse_plainto_tsquery, Some(SIMPLE_CONFIG))
}

fn phraseto_tsquery_with_config_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_to_tsquery_with_config(input, result, parse_phraseto_tsquery, None)
}

fn websearch_to_tsquery_with_config_fn(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_to_tsquery_with_config(input, result, parse_websearch_to_tsquery, None)
}

pub fn get_to_tsquery_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("to_tsquery".to_string());
    set.add_function(ScalarFunction::new(
        "to_tsquery".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::TsQuery,
        to_tsquery_with_config_fn,
    ));
    set
}

pub fn get_plainto_tsquery_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("plainto_tsquery".to_string());
    set.add_function(ScalarFunction::new(
        "plainto_tsquery".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::TsQuery,
        plainto_tsquery_with_config_fn,
    ));
    set.add_function(ScalarFunction::new(
        "plainto_tsquery".to_string(),
        vec![LogicalType::Varchar],
        LogicalType::TsQuery,
        plainto_tsquery_default_fn,
    ));
    set
}

pub fn get_phraseto_tsquery_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("phraseto_tsquery".to_string());
    set.add_function(ScalarFunction::new(
        "phraseto_tsquery".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::TsQuery,
        phraseto_tsquery_with_config_fn,
    ));
    set
}

pub fn get_websearch_to_tsquery_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("websearch_to_tsquery".to_string());
    set.add_function(ScalarFunction::new(
        "websearch_to_tsquery".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::TsQuery,
        websearch_to_tsquery_with_config_fn,
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
        let plainto_set = get_plainto_tsquery_functions();
        assert_eq!(plainto_set.name, "plainto_tsquery");
        assert_eq!(plainto_set.functions.len(), 2);
        assert_eq!(plainto_set.functions[0].return_type, LogicalType::TsQuery);
        assert_eq!(plainto_set.functions[1].return_type, LogicalType::TsQuery);
    }

    #[test]
    fn test_to_tsquery_family_validation_and_output() {
        let state = MockState;

        let to_tsquery_input = Chunk::from_vectors(vec![
            Vector::from_strings(&["simple"]),
            Vector::from_strings(&["vector & !spam"]),
        ]);
        let mut to_tsquery_out = Vector::new(LogicalType::TsQuery);
        to_tsquery_with_config_fn(&to_tsquery_input, &state, &mut to_tsquery_out).unwrap();
        assert_eq!(to_tsquery_out.get_string(0), Some("vector & !spam"));

        let plainto_input = Chunk::from_vectors(vec![Vector::from_strings(&["vector database"])]);
        let mut plainto_out = Vector::new(LogicalType::TsQuery);
        plainto_tsquery_default_fn(&plainto_input, &state, &mut plainto_out).unwrap();
        assert_eq!(plainto_out.get_string(0), Some("vector & database"));

        let phrase_input = Chunk::from_vectors(vec![
            Vector::from_strings(&["simple"]),
            Vector::from_strings(&["exact match"]),
        ]);
        let mut phrase_out = Vector::new(LogicalType::TsQuery);
        phraseto_tsquery_with_config_fn(&phrase_input, &state, &mut phrase_out).unwrap();
        assert_eq!(phrase_out.get_string(0), Some("exact <-> match"));

        let web_input = Chunk::from_vectors(vec![
            Vector::from_strings(&["simple"]),
            Vector::from_strings(&["vector database -spam \"exact match\" OR graph"]),
        ]);
        let mut web_out = Vector::new(LogicalType::TsQuery);
        websearch_to_tsquery_with_config_fn(&web_input, &state, &mut web_out).unwrap();
        let normalized = web_out.get_string(0).unwrap_or_default();
        assert!(normalized.contains("vector"));
        assert!(normalized.contains("database"));
        assert!(normalized.contains("!spam"));
        assert!(normalized.contains("exact <-> match"));
        assert!(normalized.contains("graph"));
    }

    #[test]
    fn test_to_tsquery_rejects_invalid_syntax() {
        let state = MockState;
        let input = Chunk::from_vectors(vec![
            Vector::from_strings(&["simple"]),
            Vector::from_strings(&["vector &"]),
        ]);
        let mut result = Vector::new(LogicalType::TsQuery);
        assert!(to_tsquery_with_config_fn(&input, &state, &mut result).is_err());
    }
}
