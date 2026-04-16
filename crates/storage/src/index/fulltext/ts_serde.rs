// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Full-Text Serialized Views
//!
//! Helpers for serialized `TsVector` / `TsQuery` text representations.

use paro_common::error::Result;

use super::query_eval::TokenLike;
use super::query_parser::{parse_to_tsquery, ParsedQuery};
use super::tokenizer::{SpannedToken, Token, TokenPosition, Tokenizer, TokenizerKind};

/// Borrowed token view for serialized `TsVector` text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SerializedTerm<'a> {
    pub term: &'a str,
    pub position: TokenPosition,
}

impl<'a> TokenLike for SerializedTerm<'a> {
    fn term(&self) -> &str {
        self.term
    }

    fn position(&self) -> TokenPosition {
        self.position
    }
}

/// Identity normalizer used for serialized `TsQuery` parsing.
#[derive(Debug, Default, Clone, Copy)]
pub struct IdentityNormalizer;

impl Tokenizer for IdentityNormalizer {
    fn tokenize(&self, text: &str, out: &mut Vec<Token>) {
        if text.is_empty() {
            return;
        }
        out.push(Token::new(text.to_string(), 0));
    }

    fn tokenize_spanned(&self, text: &str, out: &mut Vec<SpannedToken>) {
        if text.is_empty() {
            return;
        }
        out.push(SpannedToken::new(text.to_string(), 0, 0, text.len()));
    }

    fn kind(&self) -> TokenizerKind {
        TokenizerKind::Default
    }
}

/// Parse a serialized tsquery string without reapplying locale-specific normalization.
///
/// The caller must ensure `text` is already normalized into the engine's serialized
/// tsquery form. This is intended for internal fallback paths that consume the output
/// of `to_tsquery()` / `plainto_tsquery()` / related SQL surfaces.
pub fn parse_serialized_tsquery(text: &str) -> Result<ParsedQuery> {
    let tokenizer = IdentityNormalizer;
    parse_to_tsquery(text, &tokenizer, 0, None)
}

/// Iterate over a serialized `TsVector` string as borrowed terms with dense positions.
pub fn iter_serialized_tsvector(text: &str) -> impl Iterator<Item = SerializedTerm<'_>> {
    text.split_whitespace()
        .enumerate()
        .map(|(idx, term)| SerializedTerm {
            term,
            position: idx as TokenPosition,
        })
}

/// Serialize a query through the shared query serializer.
pub fn serialize_query(query: &ParsedQuery) -> String {
    super::query_parser::serialize_query(query)
}

#[cfg(test)]
mod tests {
    use super::super::query_parser::{parse_to_tsquery, serialize_query as serialize_query_ast};
    use super::super::tokenizer::DefaultTokenizer;
    use super::*;

    #[test]
    fn parse_serialized_tsquery_uses_identity_normalizer() {
        let query = parse_serialized_tsquery("alpha <-> beta & !spam").unwrap();
        let tokenizer = DefaultTokenizer::new();
        let expected = parse_to_tsquery("alpha <-> beta & !spam", &tokenizer, 0, None).unwrap();
        assert_eq!(serialize_query(&query), serialize_query_ast(&expected));
    }

    #[test]
    fn iter_serialized_tsvector_produces_dense_positions() {
        let terms: Vec<SerializedTerm<'_>> = iter_serialized_tsvector("alpha beta gamma").collect();
        assert_eq!(terms.len(), 3);
        assert_eq!(terms[0].term, "alpha");
        assert_eq!(terms[1].position, 1);
        assert_eq!(terms[2].position, 2);
    }
}
