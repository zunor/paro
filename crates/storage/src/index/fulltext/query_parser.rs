// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Full-Text Query Parser
//!
//! Supports legacy MATCH syntax (`AND`/`OR` + quoted phrase) and PostgreSQL-style
//! tsquery parsers used by `to_tsquery`/`plainto_tsquery`/`phraseto_tsquery`/
//! `websearch_to_tsquery`.

use paro_common::error::{self as paro_error, Result};

use super::tokenizer::Tokenizer;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedQuery {
    Term(String),
    Phrase(Vec<String>),
    And(Vec<ParsedQuery>),
    Or(Vec<ParsedQuery>),
    Not(Box<ParsedQuery>),
    FollowedBy(Vec<ParsedQuery>, u32),
    Prefix(String),
}

impl ParsedQuery {
    fn and(items: Vec<ParsedQuery>) -> ParsedQuery {
        let mut out = Vec::new();
        for item in items {
            match item {
                ParsedQuery::And(mut inner) => out.append(&mut inner),
                other => out.push(other),
            }
        }
        if out.len() == 1 {
            out.pop().unwrap()
        } else {
            ParsedQuery::And(out)
        }
    }

    fn or(items: Vec<ParsedQuery>) -> ParsedQuery {
        let mut out = Vec::new();
        for item in items {
            match item {
                ParsedQuery::Or(mut inner) => out.append(&mut inner),
                other => out.push(other),
            }
        }
        if out.len() == 1 {
            out.pop().unwrap()
        } else {
            ParsedQuery::Or(out)
        }
    }

    fn followed_by(left: ParsedQuery, right: ParsedQuery, distance: u32) -> ParsedQuery {
        let mut out = Vec::new();
        match left {
            ParsedQuery::FollowedBy(mut inner, d) if d == distance => out.append(&mut inner),
            other => out.push(other),
        }
        match right {
            ParsedQuery::FollowedBy(mut inner, d) if d == distance => out.append(&mut inner),
            other => out.push(other),
        }
        if out.len() == 1 {
            out.pop().unwrap()
        } else {
            ParsedQuery::FollowedBy(out, distance)
        }
    }
}

/// Legacy parser used by existing `fulltext_match`/`bm25` query text.
pub fn parse_query(
    text: &str,
    tokenizer: &dyn Tokenizer,
    min_token_len: usize,
    max_token_len: Option<usize>,
) -> Result<ParsedQuery> {
    let tokens = lex_legacy(text)?;
    if tokens.is_empty() {
        return Err(paro_error::invalid_input("FullTextQuery: empty query"));
    }

    let mut parser = LegacyParser {
        tokens,
        pos: 0,
        tokenizer,
        min_token_len,
        max_token_len,
    };
    let parsed = parser.parse_or()?;
    if parser.pos < parser.tokens.len() {
        return Err(paro_error::invalid_input(
            "FullTextQuery: unexpected tokens",
        ));
    }
    Ok(parsed)
}

/// Parse PostgreSQL tsquery syntax.
pub fn parse_to_tsquery(
    text: &str,
    tokenizer: &dyn Tokenizer,
    min_token_len: usize,
    max_token_len: Option<usize>,
) -> Result<ParsedQuery> {
    let tokens = lex_tsquery(text)?;
    if tokens.is_empty() {
        return Err(paro_error::invalid_input("TsQuery: empty query"));
    }

    let mut parser = TsQueryParser {
        tokens,
        pos: 0,
        tokenizer,
        min_token_len,
        max_token_len,
    };
    let parsed = parser.parse_or()?;
    if parser.pos < parser.tokens.len() {
        return Err(paro_error::invalid_input("TsQuery: unexpected token"));
    }
    Ok(parsed)
}

/// Parse PostgreSQL plainto_tsquery semantics (space => implicit AND).
pub fn parse_plainto_tsquery(
    text: &str,
    tokenizer: &dyn Tokenizer,
    min_token_len: usize,
    max_token_len: Option<usize>,
) -> Result<ParsedQuery> {
    let terms = tokenize_terms(tokenizer, text, min_token_len, max_token_len);
    terms_to_and_query(terms, "TsQuery: empty query")
}

/// Parse PostgreSQL phraseto_tsquery semantics (space => implicit followed-by).
pub fn parse_phraseto_tsquery(
    text: &str,
    tokenizer: &dyn Tokenizer,
    min_token_len: usize,
    max_token_len: Option<usize>,
) -> Result<ParsedQuery> {
    let terms = tokenize_terms(tokenizer, text, min_token_len, max_token_len);
    terms_to_followed_by_query(terms, "TsQuery: empty query")
}

/// Parse PostgreSQL websearch_to_tsquery semantics.
///
/// Supported semantics:
/// - space => AND
/// - `"..."` => followed-by phrase
/// - `-` => NOT
/// - `OR` => OR
pub fn parse_websearch_to_tsquery(
    text: &str,
    tokenizer: &dyn Tokenizer,
    min_token_len: usize,
    max_token_len: Option<usize>,
) -> Result<ParsedQuery> {
    let tokens = lex_websearch(text)?;
    if tokens.is_empty() {
        return Err(paro_error::invalid_input("TsQuery: empty query"));
    }

    let mut parser = WebsearchParser {
        tokens,
        pos: 0,
        tokenizer,
        min_token_len,
        max_token_len,
    };
    let parsed = parser.parse_or()?;
    if parser.pos < parser.tokens.len() {
        return Err(paro_error::invalid_input("TsQuery: unexpected token"));
    }
    Ok(parsed)
}

/// Serialize a parsed query back to tsquery syntax.
///
/// This preserves boolean / phrase / proximity structure so callers can round-trip
/// the query text without flattening it to whitespace-separated terms.
pub fn serialize_query(query: &ParsedQuery) -> String {
    serialize_query_prec(query, 0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LegacyLexToken {
    Term(String),
    Phrase(String),
    And,
    Or,
}

struct LegacyParser<'a> {
    tokens: Vec<LegacyLexToken>,
    pos: usize,
    tokenizer: &'a dyn Tokenizer,
    min_token_len: usize,
    max_token_len: Option<usize>,
}

impl<'a> LegacyParser<'a> {
    fn parse_or(&mut self) -> Result<ParsedQuery> {
        let mut items = vec![self.parse_and()?];
        while self.peek_is_or() {
            self.pos += 1;
            items.push(self.parse_and()?);
        }
        Ok(ParsedQuery::or(items))
    }

    fn parse_and(&mut self) -> Result<ParsedQuery> {
        let mut items = vec![self.parse_atom()?];
        loop {
            if self.is_end() || self.peek_is_or() {
                break;
            }
            if self.peek_is_and() {
                self.pos += 1;
                if self.is_end() {
                    return Err(paro_error::invalid_input("FullTextQuery: dangling AND"));
                }
            }
            if self.peek_is_term_or_phrase() {
                items.push(self.parse_atom()?);
            } else {
                return Err(paro_error::invalid_input(
                    "FullTextQuery: expected term or phrase",
                ));
            }
        }
        Ok(ParsedQuery::and(items))
    }

    fn parse_atom(&mut self) -> Result<ParsedQuery> {
        let token = self
            .tokens
            .get(self.pos)
            .cloned()
            .ok_or_else(|| paro_error::invalid_input("FullTextQuery: unexpected end"))?;
        self.pos += 1;
        match token {
            LegacyLexToken::Term(term) => parse_term_like(
                self.tokenizer,
                &term,
                self.min_token_len,
                self.max_token_len,
                false,
                "FullTextQuery: empty term after tokenization",
            ),
            LegacyLexToken::Phrase(phrase) => parse_phrase(
                self.tokenizer,
                &phrase,
                self.min_token_len,
                self.max_token_len,
                true,
            ),
            LegacyLexToken::And | LegacyLexToken::Or => Err(paro_error::invalid_input(
                "FullTextQuery: unexpected operator",
            )),
        }
    }

    fn peek_is_or(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(LegacyLexToken::Or))
    }

    fn peek_is_and(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(LegacyLexToken::And))
    }

    fn peek_is_term_or_phrase(&self) -> bool {
        matches!(
            self.tokens.get(self.pos),
            Some(LegacyLexToken::Term(_)) | Some(LegacyLexToken::Phrase(_))
        )
    }

    fn is_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TsLexeme {
    text: String,
    prefix: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TsToken {
    Lexeme(TsLexeme),
    And,
    Or,
    Not,
    FollowedBy(u32),
    LParen,
    RParen,
}

struct TsQueryParser<'a> {
    tokens: Vec<TsToken>,
    pos: usize,
    tokenizer: &'a dyn Tokenizer,
    min_token_len: usize,
    max_token_len: Option<usize>,
}

impl<'a> TsQueryParser<'a> {
    fn parse_or(&mut self) -> Result<ParsedQuery> {
        let mut items = vec![self.parse_and()?];
        while self.peek_is_or() {
            self.pos += 1;
            if self.is_end() {
                return Err(paro_error::invalid_input("TsQuery: dangling '|'"));
            }
            items.push(self.parse_and()?);
        }
        Ok(ParsedQuery::or(items))
    }

    fn parse_and(&mut self) -> Result<ParsedQuery> {
        let mut items = vec![self.parse_follow()?];
        while self.peek_is_and() {
            self.pos += 1;
            if self.is_end() {
                return Err(paro_error::invalid_input("TsQuery: dangling '&'"));
            }
            items.push(self.parse_follow()?);
        }
        Ok(ParsedQuery::and(items))
    }

    fn parse_follow(&mut self) -> Result<ParsedQuery> {
        let mut expr = self.parse_not()?;
        while let Some(distance) = self.peek_follow_distance() {
            self.pos += 1;
            if self.is_end() {
                return Err(paro_error::invalid_input(
                    "TsQuery: dangling followed-by operator",
                ));
            }
            let rhs = self.parse_not()?;
            expr = ParsedQuery::followed_by(expr, rhs, distance);
        }
        Ok(expr)
    }

    fn parse_not(&mut self) -> Result<ParsedQuery> {
        if self.peek_is_not() {
            self.pos += 1;
            if self.is_end() {
                return Err(paro_error::invalid_input("TsQuery: dangling '!'"));
            }
            let child = self.parse_not()?;
            Ok(ParsedQuery::Not(Box::new(child)))
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<ParsedQuery> {
        let token = self
            .tokens
            .get(self.pos)
            .cloned()
            .ok_or_else(|| paro_error::invalid_input("TsQuery: unexpected end"))?;
        match token {
            TsToken::Lexeme(lexeme) => {
                self.pos += 1;
                parse_term_like(
                    self.tokenizer,
                    &lexeme.text,
                    self.min_token_len,
                    self.max_token_len,
                    lexeme.prefix,
                    "TsQuery: empty term after tokenization",
                )
            }
            TsToken::LParen => {
                self.pos += 1;
                if self.peek_is_rparen() {
                    return Err(paro_error::invalid_input("TsQuery: empty parentheses"));
                }
                let expr = self.parse_or()?;
                if !self.peek_is_rparen() {
                    return Err(paro_error::invalid_input("TsQuery: missing ')'"));
                }
                self.pos += 1;
                Ok(expr)
            }
            TsToken::RParen => Err(paro_error::invalid_input("TsQuery: unexpected ')'")),
            TsToken::And => Err(paro_error::invalid_input("TsQuery: unexpected '&'")),
            TsToken::Or => Err(paro_error::invalid_input("TsQuery: unexpected '|'")),
            TsToken::Not => Err(paro_error::invalid_input("TsQuery: unexpected '!'")),
            TsToken::FollowedBy(_) => Err(paro_error::invalid_input(
                "TsQuery: unexpected followed-by operator",
            )),
        }
    }

    fn peek_is_or(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(TsToken::Or))
    }

    fn peek_is_and(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(TsToken::And))
    }

    fn peek_is_not(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(TsToken::Not))
    }

    fn peek_is_rparen(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(TsToken::RParen))
    }

    fn peek_follow_distance(&self) -> Option<u32> {
        match self.tokens.get(self.pos) {
            Some(TsToken::FollowedBy(distance)) => Some(*distance),
            _ => None,
        }
    }

    fn is_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WebToken {
    Term(String),
    Phrase(String),
    Or,
    Not,
    LParen,
    RParen,
}

struct WebsearchParser<'a> {
    tokens: Vec<WebToken>,
    pos: usize,
    tokenizer: &'a dyn Tokenizer,
    min_token_len: usize,
    max_token_len: Option<usize>,
}

impl<'a> WebsearchParser<'a> {
    fn parse_or(&mut self) -> Result<ParsedQuery> {
        let mut items = vec![self.parse_and()?];
        while self.peek_is_or() {
            self.pos += 1;
            if self.is_end() {
                return Err(paro_error::invalid_input("TsQuery: dangling OR"));
            }
            items.push(self.parse_and()?);
        }
        Ok(ParsedQuery::or(items))
    }

    fn parse_and(&mut self) -> Result<ParsedQuery> {
        let mut items = vec![self.parse_unary()?];
        loop {
            if self.is_end() || self.peek_is_or() || self.peek_is_rparen() {
                break;
            }
            if self.peek_starts_unary() {
                items.push(self.parse_unary()?);
            } else {
                return Err(paro_error::invalid_input(
                    "TsQuery: invalid websearch syntax",
                ));
            }
        }
        Ok(ParsedQuery::and(items))
    }

    fn parse_unary(&mut self) -> Result<ParsedQuery> {
        if self.peek_is_not() {
            self.pos += 1;
            if self.is_end() {
                return Err(paro_error::invalid_input("TsQuery: dangling '-'"));
            }
            let child = self.parse_unary()?;
            Ok(ParsedQuery::Not(Box::new(child)))
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<ParsedQuery> {
        let token = self
            .tokens
            .get(self.pos)
            .cloned()
            .ok_or_else(|| paro_error::invalid_input("TsQuery: unexpected end"))?;

        match token {
            WebToken::Term(term) => {
                self.pos += 1;
                parse_term_like(
                    self.tokenizer,
                    &term,
                    self.min_token_len,
                    self.max_token_len,
                    false,
                    "TsQuery: empty term after tokenization",
                )
            }
            WebToken::Phrase(phrase) => {
                self.pos += 1;
                parse_phrase(
                    self.tokenizer,
                    &phrase,
                    self.min_token_len,
                    self.max_token_len,
                    false,
                )
            }
            WebToken::LParen => {
                self.pos += 1;
                if self.peek_is_rparen() {
                    return Err(paro_error::invalid_input("TsQuery: empty parentheses"));
                }
                let expr = self.parse_or()?;
                if !self.peek_is_rparen() {
                    return Err(paro_error::invalid_input("TsQuery: missing ')'"));
                }
                self.pos += 1;
                Ok(expr)
            }
            WebToken::RParen => Err(paro_error::invalid_input("TsQuery: unexpected ')'")),
            WebToken::Or => Err(paro_error::invalid_input("TsQuery: unexpected OR")),
            WebToken::Not => Err(paro_error::invalid_input("TsQuery: unexpected '-'")),
        }
    }

    fn peek_is_or(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(WebToken::Or))
    }

    fn peek_is_not(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(WebToken::Not))
    }

    fn peek_is_rparen(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(WebToken::RParen))
    }

    fn peek_starts_unary(&self) -> bool {
        matches!(
            self.tokens.get(self.pos),
            Some(WebToken::Term(_))
                | Some(WebToken::Phrase(_))
                | Some(WebToken::Not)
                | Some(WebToken::LParen)
        )
    }

    fn is_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }
}

fn tokenize_terms(
    tokenizer: &dyn Tokenizer,
    text: &str,
    min_token_len: usize,
    max_token_len: Option<usize>,
) -> Vec<String> {
    let mut out = Vec::new();
    let mut tokens = Vec::new();
    tokenizer.tokenize(text, &mut tokens);
    for token in tokens {
        let len = token.term.chars().count();
        if len < min_token_len {
            continue;
        }
        if max_token_len.is_some_and(|max_len| len > max_len) {
            continue;
        }
        out.push(token.term);
    }
    out
}

fn terms_to_and_query(terms: Vec<String>, empty_error: &str) -> Result<ParsedQuery> {
    if terms.is_empty() {
        return Err(paro_error::invalid_input(empty_error.to_string()));
    }
    if terms.len() == 1 {
        Ok(ParsedQuery::Term(terms.into_iter().next().unwrap()))
    } else {
        Ok(ParsedQuery::and(
            terms.into_iter().map(ParsedQuery::Term).collect(),
        ))
    }
}

fn terms_to_followed_by_query(terms: Vec<String>, empty_error: &str) -> Result<ParsedQuery> {
    if terms.is_empty() {
        return Err(paro_error::invalid_input(empty_error.to_string()));
    }
    if terms.len() == 1 {
        Ok(ParsedQuery::Term(terms.into_iter().next().unwrap()))
    } else {
        Ok(ParsedQuery::FollowedBy(
            terms.into_iter().map(ParsedQuery::Term).collect(),
            1,
        ))
    }
}

fn parse_term_like(
    tokenizer: &dyn Tokenizer,
    term: &str,
    min_token_len: usize,
    max_token_len: Option<usize>,
    prefix: bool,
    empty_error: &str,
) -> Result<ParsedQuery> {
    let terms = tokenize_terms(tokenizer, term, min_token_len, max_token_len);
    if terms.is_empty() {
        return Err(paro_error::invalid_input(empty_error.to_string()));
    }

    if prefix {
        if terms.len() != 1 {
            return Err(paro_error::invalid_input(
                "TsQuery: prefix query must resolve to one term",
            ));
        }
        return Ok(ParsedQuery::Prefix(terms.into_iter().next().unwrap()));
    }

    terms_to_and_query(terms, empty_error)
}

fn parse_phrase(
    tokenizer: &dyn Tokenizer,
    phrase: &str,
    min_token_len: usize,
    max_token_len: Option<usize>,
    legacy_phrase_node: bool,
) -> Result<ParsedQuery> {
    let terms = tokenize_terms(tokenizer, phrase, min_token_len, max_token_len);
    let empty_error = if legacy_phrase_node {
        "FullTextQuery: empty phrase after tokenization"
    } else {
        "TsQuery: empty phrase after tokenization"
    };
    if terms.is_empty() {
        return Err(paro_error::invalid_input(empty_error));
    }

    if legacy_phrase_node {
        Ok(ParsedQuery::Phrase(terms))
    } else {
        terms_to_followed_by_query(terms, empty_error)
    }
}

fn serialize_query_prec(query: &ParsedQuery, parent_prec: u8) -> String {
    let (text, prec) = match query {
        ParsedQuery::Term(term) => (term.clone(), 5),
        ParsedQuery::Prefix(prefix) => (format!("{prefix}:*"), 5),
        ParsedQuery::Phrase(terms) => (serialize_followed_by_terms(terms, 1), 3),
        ParsedQuery::FollowedBy(items, distance) => {
            (serialize_followed_by_items(items, *distance), 3)
        }
        ParsedQuery::Not(child) => {
            let child_text = serialize_query_prec(child, 4);
            let wrapped = if query_prec(child) < 4 {
                format!("({child_text})")
            } else {
                child_text
            };
            (format!("!{wrapped}"), 4)
        }
        ParsedQuery::And(items) => (
            items
                .iter()
                .map(|item| {
                    let text = serialize_query_prec(item, 2);
                    if query_prec(item) < 2 {
                        format!("({text})")
                    } else {
                        text
                    }
                })
                .collect::<Vec<_>>()
                .join(" & "),
            2,
        ),
        ParsedQuery::Or(items) => (
            items
                .iter()
                .map(|item| {
                    let text = serialize_query_prec(item, 1);
                    if query_prec(item) < 1 {
                        format!("({text})")
                    } else {
                        text
                    }
                })
                .collect::<Vec<_>>()
                .join(" | "),
            1,
        ),
    };

    if prec < parent_prec {
        format!("({text})")
    } else {
        text
    }
}

fn query_prec(query: &ParsedQuery) -> u8 {
    match query {
        ParsedQuery::Term(_) | ParsedQuery::Prefix(_) => 5,
        ParsedQuery::Phrase(_) | ParsedQuery::FollowedBy(_, _) => 3,
        ParsedQuery::Not(_) => 4,
        ParsedQuery::And(_) => 2,
        ParsedQuery::Or(_) => 1,
    }
}

fn serialize_followed_by_items(items: &[ParsedQuery], distance: u32) -> String {
    let sep = if distance == 1 {
        " <-> ".to_string()
    } else {
        format!(" <{}> ", distance)
    };
    items
        .iter()
        .map(|item| {
            let text = serialize_query_prec(item, 3);
            if query_prec(item) < 3 {
                format!("({text})")
            } else {
                text
            }
        })
        .collect::<Vec<_>>()
        .join(&sep)
}

fn serialize_followed_by_terms(terms: &[String], distance: u32) -> String {
    let items: Vec<ParsedQuery> = terms.iter().cloned().map(ParsedQuery::Term).collect();
    serialize_followed_by_items(&items, distance)
}

fn lex_legacy(text: &str) -> Result<Vec<LegacyLexToken>> {
    let mut tokens = Vec::new();
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        if ch == '"' {
            chars.next();
            let mut phrase = String::new();
            let mut closed = false;
            for c in chars.by_ref() {
                if c == '"' {
                    closed = true;
                    break;
                }
                phrase.push(c);
            }
            if !closed {
                return Err(paro_error::invalid_input(
                    "FullTextQuery: unterminated phrase",
                ));
            }
            if !phrase.trim().is_empty() {
                tokens.push(LegacyLexToken::Phrase(phrase));
            }
            continue;
        }

        let mut word = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_whitespace() || c == '"' {
                break;
            }
            word.push(c);
            chars.next();
        }
        if word.is_empty() {
            continue;
        }
        let upper = word.to_ascii_uppercase();
        if upper == "AND" {
            tokens.push(LegacyLexToken::And);
        } else if upper == "OR" {
            tokens.push(LegacyLexToken::Or);
        } else {
            tokens.push(LegacyLexToken::Term(word));
        }
    }
    Ok(tokens)
}

fn lex_tsquery(text: &str) -> Result<Vec<TsToken>> {
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0usize;
    let mut tokens = Vec::new();

    while i < chars.len() {
        let ch = chars[i];
        if ch.is_whitespace() {
            i += 1;
            continue;
        }

        match ch {
            '&' => {
                tokens.push(TsToken::And);
                i += 1;
            }
            '|' => {
                tokens.push(TsToken::Or);
                i += 1;
            }
            '!' => {
                tokens.push(TsToken::Not);
                i += 1;
            }
            '(' => {
                tokens.push(TsToken::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(TsToken::RParen);
                i += 1;
            }
            '<' => {
                let (distance, next_i) = parse_follow_operator(&chars, i)?;
                tokens.push(TsToken::FollowedBy(distance));
                i = next_i;
            }
            _ => {
                let start = i;
                while i < chars.len() {
                    let c = chars[i];
                    if c.is_whitespace() || matches!(c, '&' | '|' | '!' | '(' | ')' | '<') {
                        break;
                    }
                    i += 1;
                }
                let raw: String = chars[start..i].iter().collect();
                if raw.is_empty() {
                    return Err(paro_error::invalid_input("TsQuery: unexpected token"));
                }
                let lexeme = parse_ts_lexeme(&raw)?;
                tokens.push(TsToken::Lexeme(lexeme));
            }
        }
    }

    Ok(tokens)
}

fn parse_follow_operator(chars: &[char], start: usize) -> Result<(u32, usize)> {
    // <-> (distance=1) or <N>
    let mut i = start + 1;
    if i >= chars.len() {
        return Err(paro_error::invalid_input(
            "TsQuery: invalid followed-by operator",
        ));
    }

    if chars[i] == '-' {
        i += 1;
        if i < chars.len() && chars[i] == '>' {
            return Ok((1, i + 1));
        }
        return Err(paro_error::invalid_input(
            "TsQuery: invalid followed-by operator",
        ));
    }

    let num_start = i;
    while i < chars.len() && chars[i].is_ascii_digit() {
        i += 1;
    }
    if num_start == i {
        return Err(paro_error::invalid_input(
            "TsQuery: invalid followed-by operator",
        ));
    }
    if i >= chars.len() || chars[i] != '>' {
        return Err(paro_error::invalid_input(
            "TsQuery: invalid followed-by operator",
        ));
    }

    let num_text: String = chars[num_start..i].iter().collect();
    let distance = num_text
        .parse::<u32>()
        .map_err(|_| paro_error::invalid_input("TsQuery: invalid proximity distance"))?;
    if distance == 0 {
        return Err(paro_error::invalid_input(
            "TsQuery: proximity distance must be greater than 0",
        ));
    }

    Ok((distance, i + 1))
}

fn parse_ts_lexeme(raw: &str) -> Result<TsLexeme> {
    let mut parts = raw.split(':');
    let base = parts.next().unwrap_or_default();
    if base.is_empty() {
        return Err(paro_error::invalid_input("TsQuery: empty lexeme"));
    }

    let mut prefix = false;
    for modifier in parts {
        if modifier.is_empty() {
            return Err(paro_error::invalid_input(
                "TsQuery: invalid lexeme modifier",
            ));
        }
        if modifier == "*" {
            if prefix {
                return Err(paro_error::invalid_input(
                    "TsQuery: duplicate prefix modifier",
                ));
            }
            prefix = true;
            continue;
        }
        if modifier
            .chars()
            .all(|c| matches!(c, 'A' | 'B' | 'C' | 'D' | 'a' | 'b' | 'c' | 'd'))
        {
            continue;
        }
        return Err(paro_error::invalid_input(
            "TsQuery: invalid lexeme modifier",
        ));
    }

    Ok(TsLexeme {
        text: base.to_string(),
        prefix,
    })
}

fn lex_websearch(text: &str) -> Result<Vec<WebToken>> {
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0usize;
    let mut tokens = Vec::new();

    while i < chars.len() {
        let ch = chars[i];
        if ch.is_whitespace() {
            i += 1;
            continue;
        }

        match ch {
            '"' => {
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '"' {
                    i += 1;
                }
                if i >= chars.len() {
                    return Err(paro_error::invalid_input(
                        "TsQuery: unterminated quoted phrase",
                    ));
                }
                let phrase: String = chars[start..i].iter().collect();
                if !phrase.trim().is_empty() {
                    tokens.push(WebToken::Phrase(phrase));
                }
                i += 1;
            }
            '-' => {
                tokens.push(WebToken::Not);
                i += 1;
            }
            '(' => {
                tokens.push(WebToken::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(WebToken::RParen);
                i += 1;
            }
            _ => {
                let start = i;
                while i < chars.len() {
                    let c = chars[i];
                    if c.is_whitespace() || matches!(c, '"' | '(' | ')') {
                        break;
                    }
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                if word.is_empty() {
                    continue;
                }
                if word.eq_ignore_ascii_case("OR") {
                    tokens.push(WebToken::Or);
                } else {
                    tokens.push(WebToken::Term(word));
                }
            }
        }
    }

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::fulltext::tokenizer::DefaultTokenizer;

    #[test]
    fn parse_and_or_phrase() {
        let tokenizer = DefaultTokenizer::new();
        let query = parse_query("vector AND database", &tokenizer, 1, None).unwrap();
        assert_eq!(
            query,
            ParsedQuery::And(vec![
                ParsedQuery::Term("vector".to_string()),
                ParsedQuery::Term("database".to_string())
            ])
        );

        let query = parse_query("vector OR graph", &tokenizer, 1, None).unwrap();
        assert_eq!(
            query,
            ParsedQuery::Or(vec![
                ParsedQuery::Term("vector".to_string()),
                ParsedQuery::Term("graph".to_string())
            ])
        );

        let query = parse_query("\"vector database\"", &tokenizer, 1, None).unwrap();
        assert_eq!(
            query,
            ParsedQuery::Phrase(vec!["vector".to_string(), "database".to_string()])
        );
    }

    #[test]
    fn parse_precedence_and_implicit_and() {
        let tokenizer = DefaultTokenizer::new();
        let query = parse_query("a OR b c", &tokenizer, 1, None).unwrap();
        assert_eq!(
            query,
            ParsedQuery::Or(vec![
                ParsedQuery::Term("a".to_string()),
                ParsedQuery::And(vec![
                    ParsedQuery::Term("b".to_string()),
                    ParsedQuery::Term("c".to_string())
                ])
            ])
        );
    }

    #[test]
    fn test_to_tsquery_and_or_not() {
        let tokenizer = DefaultTokenizer::new();
        let query = parse_to_tsquery("vector & !spam | graph", &tokenizer, 1, None).unwrap();
        assert_eq!(
            query,
            ParsedQuery::Or(vec![
                ParsedQuery::And(vec![
                    ParsedQuery::Term("vector".to_string()),
                    ParsedQuery::Not(Box::new(ParsedQuery::Term("spam".to_string())))
                ]),
                ParsedQuery::Term("graph".to_string())
            ])
        );
    }

    #[test]
    fn test_to_tsquery_followed_by() {
        let tokenizer = DefaultTokenizer::new();

        let query = parse_to_tsquery("vector <-> database", &tokenizer, 1, None).unwrap();
        assert_eq!(
            query,
            ParsedQuery::FollowedBy(
                vec![
                    ParsedQuery::Term("vector".to_string()),
                    ParsedQuery::Term("database".to_string())
                ],
                1
            )
        );

        let query = parse_to_tsquery("vector <2> database <2> graph", &tokenizer, 1, None).unwrap();
        assert_eq!(
            query,
            ParsedQuery::FollowedBy(
                vec![
                    ParsedQuery::Term("vector".to_string()),
                    ParsedQuery::Term("database".to_string()),
                    ParsedQuery::Term("graph".to_string())
                ],
                2
            )
        );
    }

    #[test]
    fn test_to_tsquery_prefix() {
        let tokenizer = DefaultTokenizer::new();
        let query = parse_to_tsquery("vec:* & graph:A", &tokenizer, 1, None).unwrap();
        assert_eq!(
            query,
            ParsedQuery::And(vec![
                ParsedQuery::Prefix("vec".to_string()),
                ParsedQuery::Term("graph".to_string())
            ])
        );
    }

    #[test]
    fn test_to_tsquery_precedence() {
        let tokenizer = DefaultTokenizer::new();
        let query = parse_to_tsquery("!a & b | c <-> d", &tokenizer, 1, None).unwrap();
        assert_eq!(
            query,
            ParsedQuery::Or(vec![
                ParsedQuery::And(vec![
                    ParsedQuery::Not(Box::new(ParsedQuery::Term("a".to_string()))),
                    ParsedQuery::Term("b".to_string())
                ]),
                ParsedQuery::FollowedBy(
                    vec![
                        ParsedQuery::Term("c".to_string()),
                        ParsedQuery::Term("d".to_string())
                    ],
                    1
                )
            ])
        );

        let query = parse_to_tsquery("!(a | b) & c", &tokenizer, 1, None).unwrap();
        assert_eq!(
            query,
            ParsedQuery::And(vec![
                ParsedQuery::Not(Box::new(ParsedQuery::Or(vec![
                    ParsedQuery::Term("a".to_string()),
                    ParsedQuery::Term("b".to_string())
                ]))),
                ParsedQuery::Term("c".to_string())
            ])
        );
    }

    #[test]
    fn test_plainto_tsquery_implicit_and() {
        let tokenizer = DefaultTokenizer::new();
        let query = parse_plainto_tsquery("vector database", &tokenizer, 1, None).unwrap();
        assert_eq!(
            query,
            ParsedQuery::And(vec![
                ParsedQuery::Term("vector".to_string()),
                ParsedQuery::Term("database".to_string())
            ])
        );
    }

    #[test]
    fn test_phraseto_tsquery_implicit_followed_by() {
        let tokenizer = DefaultTokenizer::new();
        let query = parse_phraseto_tsquery("vector database", &tokenizer, 1, None).unwrap();
        assert_eq!(
            query,
            ParsedQuery::FollowedBy(
                vec![
                    ParsedQuery::Term("vector".to_string()),
                    ParsedQuery::Term("database".to_string())
                ],
                1
            )
        );
    }

    #[test]
    fn test_websearch_to_tsquery_pg_behavior() {
        let tokenizer = DefaultTokenizer::new();
        let query = parse_websearch_to_tsquery(
            "vector database -spam \"exact match\" OR graph",
            &tokenizer,
            1,
            None,
        )
        .unwrap();

        assert_eq!(
            query,
            ParsedQuery::Or(vec![
                ParsedQuery::And(vec![
                    ParsedQuery::Term("vector".to_string()),
                    ParsedQuery::Term("database".to_string()),
                    ParsedQuery::Not(Box::new(ParsedQuery::Term("spam".to_string()))),
                    ParsedQuery::FollowedBy(
                        vec![
                            ParsedQuery::Term("exact".to_string()),
                            ParsedQuery::Term("match".to_string())
                        ],
                        1
                    )
                ]),
                ParsedQuery::Term("graph".to_string())
            ])
        );
    }
}
