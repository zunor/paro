use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
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

        let tokenizer = resolve_tokenizer(config)?;
        let text = text_vec.get_string(i).unwrap_or_default();
        let tsvector = tokenize_text(tokenizer.as_ref(), text);
        result.set_string(i, &tsvector);
    }

    Ok(())
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

        let tokenizer = resolve_tokenizer(config)?;
        let query_text = query_vec.get_string(i).unwrap_or_default();
        let parsed = parse_fn(query_text, tokenizer.as_ref(), MIN_TOKEN_LEN, MAX_TOKEN_LEN)?;
        let tsquery = serialize_query(&parsed);
        result.set_string(i, &tsquery);
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

fn ts_rank_fn(input: &Chunk, state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    super::score::bm25_score_internal_fn(input, state, result)
}

fn ts_rank_cd_fn(input: &Chunk, state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    super::score::bm25_score_internal_fn(input, state, result)
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
        let tsvector_set = get_to_tsvector_functions();
        assert_eq!(tsvector_set.name, "to_tsvector");
        assert_eq!(tsvector_set.functions.len(), 2);
        assert_eq!(tsvector_set.functions[0].return_type, LogicalType::TsVector);
        assert_eq!(tsvector_set.functions[1].return_type, LogicalType::TsVector);

        let plainto_set = get_plainto_tsquery_functions();
        assert_eq!(plainto_set.name, "plainto_tsquery");
        assert_eq!(plainto_set.functions.len(), 2);
        assert_eq!(plainto_set.functions[0].return_type, LogicalType::TsQuery);
        assert_eq!(plainto_set.functions[1].return_type, LogicalType::TsQuery);

        let rank_set = get_ts_rank_functions();
        assert_eq!(rank_set.name, "ts_rank");
        assert_eq!(
            rank_set.functions[0].arguments,
            vec![LogicalType::TsVector, LogicalType::TsQuery]
        );
    }

    #[test]
    fn test_to_tsvector_normalizes_text() {
        let state = MockState;
        let input = Chunk::from_vectors(vec![
            Vector::from_strings(&["simple"]),
            Vector::from_strings(&["Hello, Vector DB!"]),
        ]);
        let mut result = Vector::new(LogicalType::TsVector);
        to_tsvector_with_config_fn(&input, &state, &mut result).unwrap();
        assert_eq!(result.get_string(0), Some("hello vector db"));

        let input_default = Chunk::from_vectors(vec![Vector::from_strings(&["Hello, Vector DB!"])]);
        let mut result_default = Vector::new(LogicalType::TsVector);
        to_tsvector_default_fn(&input_default, &state, &mut result_default).unwrap();
        assert_eq!(result_default.get_string(0), Some("hello vector db"));
    }

    #[test]
    fn test_to_tsvector_rejects_unsupported_config() {
        let state = MockState;
        let input = Chunk::from_vectors(vec![
            Vector::from_strings(&["unsupported_lang"]),
            Vector::from_strings(&["hello world"]),
        ]);
        let mut result = Vector::new(LogicalType::TsVector);
        let err = to_tsvector_with_config_fn(&input, &state, &mut result).unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn test_to_tsvector_supports_chinese_and_japanese_config() {
        let state = MockState;

        let chinese_input = Chunk::from_vectors(vec![
            Vector::from_strings(&["chinese"]),
            Vector::from_strings(&["向量数据库"]),
        ]);
        let mut chinese_result = Vector::new(LogicalType::TsVector);
        to_tsvector_with_config_fn(&chinese_input, &state, &mut chinese_result).unwrap();
        assert_eq!(chinese_result.get_string(0), Some("向 量 数 据 库"));

        let japanese_input = Chunk::from_vectors(vec![
            Vector::from_strings(&["japanese"]),
            Vector::from_strings(&["東京ベクトルDB"]),
        ]);
        let mut japanese_result = Vector::new(LogicalType::TsVector);
        to_tsvector_with_config_fn(&japanese_input, &state, &mut japanese_result).unwrap();
        assert_eq!(japanese_result.get_string(0), Some("東 京 ベ ク ト ル db"));

        let english_input = Chunk::from_vectors(vec![
            Vector::from_strings(&["english"]),
            Vector::from_strings(&["The databases are running quickly"]),
        ]);
        let mut english_result = Vector::new(LogicalType::TsVector);
        to_tsvector_with_config_fn(&english_input, &state, &mut english_result).unwrap();
        assert_eq!(english_result.get_string(0), Some("databas run quick"));
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

    #[test]
    fn test_ts_rank_and_ts_rank_cd_delegate_internal() {
        let state = MockState;
        let input = Chunk::from_vectors(vec![
            Vector::from_strings(&["vector database systems", "hello world"]),
            Vector::from_strings(&["vector & database", "database"]),
        ]);

        let mut rank = Vector::new(LogicalType::Float);
        let mut rank_cd = Vector::new(LogicalType::Float);
        let mut internal = Vector::new(LogicalType::Float);

        ts_rank_fn(&input, &state, &mut rank).unwrap();
        ts_rank_cd_fn(&input, &state, &mut rank_cd).unwrap();
        super::super::score::bm25_score_internal_fn(&input, &state, &mut internal).unwrap();

        assert_eq!(rank.get_f32(0), internal.get_f32(0));
        assert_eq!(rank.get_f32(1), internal.get_f32(1));
        assert_eq!(rank_cd.get_f32(0), internal.get_f32(0));
        assert_eq!(rank_cd.get_f32(1), internal.get_f32(1));
    }
}
