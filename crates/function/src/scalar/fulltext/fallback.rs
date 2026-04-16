// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::Result;
use paro_storage::index::fulltext::query_eval::{self, TokenLike};
use paro_storage::index::fulltext::query_parser::{parse_query, ParsedQuery};
use paro_storage::index::fulltext::scoring::score_document_from_tokens;
use paro_storage::index::fulltext::tokenizer::DefaultTokenizer;
use paro_storage::index::fulltext::ts_serde::{iter_serialized_tsvector, SerializedTerm};

pub(crate) use paro_storage::index::fulltext::query_eval::{collect_match_extents, MatchExtent};
pub(crate) use paro_storage::index::fulltext::scoring::FullTextScoreMode;
pub(crate) use paro_storage::index::fulltext::ts_serde::parse_serialized_tsquery;

const MIN_TOKEN_LEN: usize = 1;
const MAX_TOKEN_LEN: Option<usize> = None;

pub(crate) fn default_tokenizer() -> DefaultTokenizer {
    DefaultTokenizer::new()
}

pub(crate) fn parse_legacy_query(query: &str) -> Result<ParsedQuery> {
    let tokenizer = DefaultTokenizer::new();
    parse_query(query, &tokenizer, MIN_TOKEN_LEN, MAX_TOKEN_LEN)
}

pub(crate) fn query_matches_text<T: TokenLike>(query: &ParsedQuery, tokens: &[T]) -> bool {
    query_eval::matches_query(tokens, query)
}

pub(crate) fn score_query<T: TokenLike>(
    tokens: &[T],
    query: &ParsedQuery,
    mode: FullTextScoreMode,
) -> f32 {
    score_document_from_tokens(mode, tokens, query)
}

pub(crate) fn tokenize_serialized_tsvector<'a>(
    text: &'a str,
) -> impl Iterator<Item = SerializedTerm<'a>> {
    iter_serialized_tsvector(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_storage::index::fulltext::query_parser::{parse_query, serialize_query};
    use paro_storage::index::fulltext::tokenizer::Token;

    #[test]
    fn serialized_query_roundtrip_uses_identity_normalizer() {
        let tokenizer = DefaultTokenizer::new();
        let parsed =
            parse_query("vector database", &tokenizer, MIN_TOKEN_LEN, MAX_TOKEN_LEN).unwrap();
        let serialized = serialize_query(&parsed);
        let reparsed = parse_serialized_tsquery(&serialized).unwrap();
        assert_eq!(parsed, reparsed);
    }

    #[test]
    fn token_scoring_routes_to_shared_storage_scorer() {
        let tokens = vec![
            Token::new("alpha".to_string(), 0),
            Token::new("beta".to_string(), 1),
        ];
        let query = ParsedQuery::Phrase(vec!["alpha".to_string(), "beta".to_string()]);

        let bm25 = score_query(&tokens, &query, FullTextScoreMode::Bm25);
        let cover_density = score_query(&tokens, &query, FullTextScoreMode::CoverDensity);
        assert!(bm25 > 0.0);
        assert!(cover_density > 0.0);
    }
}
