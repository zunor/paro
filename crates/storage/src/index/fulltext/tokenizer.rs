// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Tokenizer
//!
//! Basic tokenization for full-text indexing.

use paro_common::error::{self as paro_error, Result};

/// Token position within a document (0-based).
pub type TokenPosition = u32;

/// Token emitted by a tokenizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub term: String,
    pub position: TokenPosition,
}

impl Token {
    pub fn new(term: String, position: TokenPosition) -> Self {
        Self { term, position }
    }
}

/// Token emitted by a tokenizer together with byte offsets into the source string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpannedToken {
    pub term: String,
    pub position: TokenPosition,
    pub byte_start: usize,
    pub byte_end: usize,
}

impl SpannedToken {
    pub fn new(term: String, position: TokenPosition, byte_start: usize, byte_end: usize) -> Self {
        Self {
            term,
            position,
            byte_start,
            byte_end,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SurfaceToken {
    term: String,
    byte_start: usize,
    byte_end: usize,
}

/// Tokenizer kind for persistence/compatibility.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenizerKind {
    Default = 0,
    Chinese = 1,
    Japanese = 2,
    English = 3,
}

impl TokenizerKind {
    pub fn from_id(id: u8) -> Result<Self> {
        match id {
            0 => Ok(Self::Default),
            1 => Ok(Self::Chinese),
            2 => Ok(Self::Japanese),
            3 => Ok(Self::English),
            _ => Err(paro_error::not_supported(format!(
                "unsupported tokenizer id '{}'",
                id
            ))),
        }
    }

    pub fn from_config(config: &str) -> Result<Self> {
        let trimmed = config.trim();
        if trimmed.eq_ignore_ascii_case("simple") {
            return Ok(Self::Default);
        }
        if trimmed.eq_ignore_ascii_case("chinese") {
            return Ok(Self::Chinese);
        }
        if trimmed.eq_ignore_ascii_case("japanese") {
            return Ok(Self::Japanese);
        }
        if trimmed.eq_ignore_ascii_case("english") {
            return Ok(Self::English);
        }
        Err(paro_error::not_supported(format!(
            "full-text config '{}' (supported: 'simple', 'chinese', 'japanese', 'english')",
            config
        )))
    }

    pub fn config_name(self) -> &'static str {
        match self {
            Self::Default => "simple",
            Self::Chinese => "chinese",
            Self::Japanese => "japanese",
            Self::English => "english",
        }
    }
}

pub fn tokenizer_from_kind(kind: TokenizerKind) -> Box<dyn Tokenizer> {
    match kind {
        TokenizerKind::Default => Box::new(DefaultTokenizer::new()),
        TokenizerKind::Chinese => Box::new(ChineseTokenizer::new()),
        TokenizerKind::Japanese => Box::new(JapaneseTokenizer::new()),
        TokenizerKind::English => Box::new(EnglishTokenizer::new()),
    }
}

pub fn tokenizer_from_config(config: &str) -> Result<(TokenizerKind, Box<dyn Tokenizer>)> {
    let kind = TokenizerKind::from_config(config)?;
    Ok((kind, tokenizer_from_kind(kind)))
}

/// Tokenizer trait for full-text indexing.
pub trait Tokenizer: Send + Sync {
    /// Tokenize input text and append tokens to `out`.
    fn tokenize(&self, text: &str, out: &mut Vec<Token>);

    /// Tokenize input text and append tokens with byte offsets to `out`.
    fn tokenize_spanned(&self, text: &str, out: &mut Vec<SpannedToken>);

    /// Convenience helper to return tokens as a vector.
    fn tokenize_to_vec(&self, text: &str) -> Vec<Token> {
        let mut out = Vec::new();
        self.tokenize(text, &mut out);
        out
    }

    /// Convenience helper to return spanned tokens as a vector.
    fn tokenize_spanned_to_vec(&self, text: &str) -> Vec<SpannedToken> {
        let mut out = Vec::new();
        self.tokenize_spanned(text, &mut out);
        out
    }

    /// Tokenizer kind for persistence.
    fn kind(&self) -> TokenizerKind {
        TokenizerKind::Default
    }
}

fn flush_alnum_surface_token(
    buffer: &mut String,
    token_start: &mut Option<usize>,
    token_end: usize,
    out: &mut Vec<SurfaceToken>,
) {
    if buffer.is_empty() {
        return;
    }
    let start = token_start.take().expect("token start for buffered token");
    out.push(SurfaceToken {
        term: buffer.to_lowercase(),
        byte_start: start,
        byte_end: token_end,
    });
    buffer.clear();
}

fn collect_basic_word_surfaces(text: &str, out: &mut Vec<SurfaceToken>) {
    let mut buffer = String::new();
    let mut token_start = None;

    for (idx, ch) in text.char_indices() {
        if ch.is_alphanumeric() {
            if token_start.is_none() {
                token_start = Some(idx);
            }
            buffer.push(ch);
        } else {
            flush_alnum_surface_token(&mut buffer, &mut token_start, idx, out);
        }
    }

    flush_alnum_surface_token(&mut buffer, &mut token_start, text.len(), out);
}

fn collect_script_boundary_surfaces<F>(text: &str, out: &mut Vec<SurfaceToken>, is_script_char: F)
where
    F: Fn(char) -> bool,
{
    let mut buffer = String::new();
    let mut token_start = None;

    for (idx, ch) in text.char_indices() {
        if is_script_char(ch) {
            flush_alnum_surface_token(&mut buffer, &mut token_start, idx, out);
            out.push(SurfaceToken {
                term: ch.to_string(),
                byte_start: idx,
                byte_end: idx + ch.len_utf8(),
            });
            continue;
        }

        if ch.is_alphanumeric() {
            if token_start.is_none() {
                token_start = Some(idx);
            }
            buffer.push(ch);
        } else {
            flush_alnum_surface_token(&mut buffer, &mut token_start, idx, out);
        }
    }

    flush_alnum_surface_token(&mut buffer, &mut token_start, text.len(), out);
}

fn append_surface_tokens(surface_tokens: &[SurfaceToken], out: &mut Vec<Token>) {
    out.extend(
        surface_tokens
            .iter()
            .enumerate()
            .map(|(idx, token)| Token::new(token.term.clone(), idx as TokenPosition)),
    );
}

fn append_surface_spanned_tokens(surface_tokens: &[SurfaceToken], out: &mut Vec<SpannedToken>) {
    out.extend(surface_tokens.iter().enumerate().map(|(idx, token)| {
        SpannedToken::new(
            token.term.clone(),
            idx as TokenPosition,
            token.byte_start,
            token.byte_end,
        )
    }));
}

fn retain_and_reposition_tokens<F>(out: &mut Vec<Token>, mut keep: F)
where
    F: FnMut(&str) -> bool,
{
    out.retain(|token| keep(token.term.as_str()));
    for (idx, token) in out.iter_mut().enumerate() {
        token.position = idx as TokenPosition;
    }
}

fn retain_and_reposition_spanned_tokens<F>(out: &mut Vec<SpannedToken>, mut keep: F)
where
    F: FnMut(&str) -> bool,
{
    out.retain(|token| keep(token.term.as_str()));
    for (idx, token) in out.iter_mut().enumerate() {
        token.position = idx as TokenPosition;
    }
}

fn is_english_stopword(term: &str) -> bool {
    matches!(
        term,
        "a" | "an"
            | "and"
            | "are"
            | "as"
            | "at"
            | "be"
            | "by"
            | "for"
            | "from"
            | "in"
            | "is"
            | "it"
            | "of"
            | "on"
            | "or"
            | "that"
            | "the"
            | "this"
            | "these"
            | "those"
            | "to"
            | "was"
            | "were"
            | "with"
    )
}

fn is_chinese_stopword(term: &str) -> bool {
    matches!(
        term,
        "的" | "了" | "和" | "是" | "在" | "有" | "就" | "都" | "而" | "及" | "与"
    )
}

fn is_japanese_stopword(term: &str) -> bool {
    matches!(term, "の" | "に" | "は" | "を" | "が" | "と" | "で" | "も")
}

fn trim_double_tail(stem: &mut String) {
    if stem.len() < 2 {
        return;
    }
    let mut chars = stem.chars().rev();
    let Some(last) = chars.next() else {
        return;
    };
    let Some(prev) = chars.next() else {
        return;
    };
    if last == prev {
        stem.pop();
    }
}

fn snowball_like_english_stem(term: &str) -> String {
    if term.len() <= 2 {
        return term.to_string();
    }
    let mut stem = term.to_string();

    if stem.len() > 4 && stem.ends_with("ies") {
        stem.truncate(stem.len() - 3);
        stem.push('y');
    } else if stem.len() > 5 && stem.ends_with("ing") {
        stem.truncate(stem.len() - 3);
        trim_double_tail(&mut stem);
    } else if stem.len() > 4 && stem.ends_with("ed") {
        stem.truncate(stem.len() - 2);
        trim_double_tail(&mut stem);
    } else if stem.len() > 4 && stem.ends_with("es") {
        stem.truncate(stem.len() - 2);
    } else if stem.len() > 3 && stem.ends_with('s') && !stem.ends_with("ss") {
        stem.truncate(stem.len() - 1);
    }

    if stem.len() > 5 && stem.ends_with("ment") {
        stem.truncate(stem.len() - 4);
    } else if stem.len() > 4 && stem.ends_with("ly") {
        stem.truncate(stem.len() - 2);
    }

    if stem.len() > 4 && stem.ends_with('e') && !stem.ends_with("ee") {
        stem.truncate(stem.len() - 1);
    }

    if stem.is_empty() {
        term.to_string()
    } else {
        stem
    }
}

fn is_han(ch: char) -> bool {
    let cp = ch as u32;
    matches!(
        cp,
        0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0xF900..=0xFAFF
            | 0x20000..=0x2A6DF
            | 0x2A700..=0x2B73F
            | 0x2B740..=0x2B81F
            | 0x2B820..=0x2CEAF
            | 0x2F800..=0x2FA1F
    )
}

fn is_hiragana(ch: char) -> bool {
    let cp = ch as u32;
    (0x3040..=0x309F).contains(&cp)
}

fn is_katakana(ch: char) -> bool {
    let cp = ch as u32;
    matches!(cp, 0x30A0..=0x30FF | 0x31F0..=0x31FF | 0xFF66..=0xFF9D)
}

/// Default tokenizer: splits on non-alphanumeric characters and lowercases tokens.
#[derive(Debug, Default, Clone)]
pub struct DefaultTokenizer;

impl DefaultTokenizer {
    pub fn new() -> Self {
        Self
    }
}

impl Tokenizer for DefaultTokenizer {
    fn tokenize(&self, text: &str, out: &mut Vec<Token>) {
        let mut surface_tokens = Vec::new();
        collect_basic_word_surfaces(text, &mut surface_tokens);
        append_surface_tokens(&surface_tokens, out);
    }

    fn tokenize_spanned(&self, text: &str, out: &mut Vec<SpannedToken>) {
        let mut surface_tokens = Vec::new();
        collect_basic_word_surfaces(text, &mut surface_tokens);
        append_surface_spanned_tokens(&surface_tokens, out);
    }
}

/// English tokenizer: applies stop-word filtering and Snowball-like stemming.
#[derive(Debug, Default, Clone)]
pub struct EnglishTokenizer;

impl EnglishTokenizer {
    pub fn new() -> Self {
        Self
    }
}

impl Tokenizer for EnglishTokenizer {
    fn tokenize(&self, text: &str, out: &mut Vec<Token>) {
        let mut surface_tokens = Vec::new();
        collect_basic_word_surfaces(text, &mut surface_tokens);
        append_surface_tokens(&surface_tokens, out);
        retain_and_reposition_tokens(out, |term| !is_english_stopword(term));
        for token in out.iter_mut() {
            token.term = snowball_like_english_stem(token.term.as_str());
        }
        retain_and_reposition_tokens(out, |term| !term.is_empty());
    }

    fn tokenize_spanned(&self, text: &str, out: &mut Vec<SpannedToken>) {
        let mut surface_tokens = Vec::new();
        collect_basic_word_surfaces(text, &mut surface_tokens);
        append_surface_spanned_tokens(&surface_tokens, out);
        retain_and_reposition_spanned_tokens(out, |term| !is_english_stopword(term));
        for token in out.iter_mut() {
            token.term = snowball_like_english_stem(token.term.as_str());
        }
        retain_and_reposition_spanned_tokens(out, |term| !term.is_empty());
    }

    fn kind(&self) -> TokenizerKind {
        TokenizerKind::English
    }
}

/// Chinese tokenizer: emits one token per Han character and word tokens for alphanumeric runs.
#[derive(Debug, Default, Clone)]
pub struct ChineseTokenizer;

impl ChineseTokenizer {
    pub fn new() -> Self {
        Self
    }
}

impl Tokenizer for ChineseTokenizer {
    fn tokenize(&self, text: &str, out: &mut Vec<Token>) {
        let mut surface_tokens = Vec::new();
        collect_script_boundary_surfaces(text, &mut surface_tokens, is_han);
        append_surface_tokens(&surface_tokens, out);
        retain_and_reposition_tokens(out, |term| !is_chinese_stopword(term));
    }

    fn tokenize_spanned(&self, text: &str, out: &mut Vec<SpannedToken>) {
        let mut surface_tokens = Vec::new();
        collect_script_boundary_surfaces(text, &mut surface_tokens, is_han);
        append_surface_spanned_tokens(&surface_tokens, out);
        retain_and_reposition_spanned_tokens(out, |term| !is_chinese_stopword(term));
    }

    fn kind(&self) -> TokenizerKind {
        TokenizerKind::Chinese
    }
}

/// Japanese tokenizer: emits one token per Japanese script character.
#[derive(Debug, Default, Clone)]
pub struct JapaneseTokenizer;

impl JapaneseTokenizer {
    pub fn new() -> Self {
        Self
    }
}

impl Tokenizer for JapaneseTokenizer {
    fn tokenize(&self, text: &str, out: &mut Vec<Token>) {
        let mut surface_tokens = Vec::new();
        collect_script_boundary_surfaces(text, &mut surface_tokens, |ch| {
            is_han(ch) || is_hiragana(ch) || is_katakana(ch)
        });
        append_surface_tokens(&surface_tokens, out);
        retain_and_reposition_tokens(out, |term| !is_japanese_stopword(term));
    }

    fn tokenize_spanned(&self, text: &str, out: &mut Vec<SpannedToken>) {
        let mut surface_tokens = Vec::new();
        collect_script_boundary_surfaces(text, &mut surface_tokens, |ch| {
            is_han(ch) || is_hiragana(ch) || is_katakana(ch)
        });
        append_surface_spanned_tokens(&surface_tokens, out);
        retain_and_reposition_spanned_tokens(out, |term| !is_japanese_stopword(term));
    }

    fn kind(&self) -> TokenizerKind {
        TokenizerKind::Japanese
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_terms_and_positions_match(spanned: &[SpannedToken], plain: &[Token]) {
        let plain_view: Vec<(&str, TokenPosition)> = plain
            .iter()
            .map(|token| (token.term.as_str(), token.position))
            .collect();
        let spanned_view: Vec<(&str, TokenPosition)> = spanned
            .iter()
            .map(|token| (token.term.as_str(), token.position))
            .collect();
        assert_eq!(spanned_view, plain_view);
    }

    #[test]
    fn default_tokenizer_splits_and_lowercases() {
        let tokenizer = DefaultTokenizer::new();
        let tokens = tokenizer.tokenize_to_vec("Hello, world! This is 2024.");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["hello", "world", "this", "is", "2024"]);
    }

    #[test]
    fn default_tokenizer_tracks_positions() {
        let tokenizer = DefaultTokenizer::new();
        let tokens = tokenizer.tokenize_to_vec("One two\tthree");
        let positions: Vec<TokenPosition> = tokens.iter().map(|t| t.position).collect();
        assert_eq!(positions, vec![0, 1, 2]);
    }

    #[test]
    fn tokenizer_kind_resolves_config() {
        assert_eq!(
            TokenizerKind::from_config("simple").unwrap(),
            TokenizerKind::Default
        );
        assert_eq!(
            TokenizerKind::from_config("CHINESE").unwrap(),
            TokenizerKind::Chinese
        );
        assert_eq!(
            TokenizerKind::from_config("japanese").unwrap(),
            TokenizerKind::Japanese
        );
        assert_eq!(
            TokenizerKind::from_config("english").unwrap(),
            TokenizerKind::English
        );
        assert!(TokenizerKind::from_config("unsupported_lang").is_err());
    }

    #[test]
    fn chinese_tokenizer_splits_han_characters() {
        let tokenizer = ChineseTokenizer::new();
        let tokens = tokenizer.tokenize_to_vec("向量数据库 vectorDB");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["向", "量", "数", "据", "库", "vectordb"]);
    }

    #[test]
    fn japanese_tokenizer_splits_japanese_scripts() {
        let tokenizer = JapaneseTokenizer::new();
        let tokens = tokenizer.tokenize_to_vec("東京ベクトルDB");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["東", "京", "ベ", "ク", "ト", "ル", "db"]);
    }

    #[test]
    fn english_tokenizer_applies_stopwords_and_stemming() {
        let tokenizer = EnglishTokenizer::new();
        let tokens = tokenizer.tokenize_to_vec("The databases are running quickly");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["databas", "run", "quick"]);
    }

    #[test]
    fn chinese_tokenizer_filters_stopwords() {
        let tokenizer = ChineseTokenizer::new();
        let tokens = tokenizer.tokenize_to_vec("数据库的系统");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["数", "据", "库", "系", "统"]);
    }

    #[test]
    fn japanese_tokenizer_filters_stopwords() {
        let tokenizer = JapaneseTokenizer::new();
        let tokens = tokenizer.tokenize_to_vec("東京のデータベース");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["東", "京", "デ", "ー", "タ", "ベ", "ー", "ス"]);
    }

    #[test]
    fn tokenize_spanned_matches_default_token_sequence() {
        let tokenizer = DefaultTokenizer::new();
        let plain = tokenizer.tokenize_to_vec("Hello, 向量 database!");
        let spanned = tokenizer.tokenize_spanned_to_vec("Hello, 向量 database!");
        assert_terms_and_positions_match(&spanned, &plain);
        let spans: Vec<(usize, usize)> = spanned
            .iter()
            .map(|token| (token.byte_start, token.byte_end))
            .collect();
        assert_eq!(spans, vec![(0, 5), (7, 13), (14, 22)]);
    }

    #[test]
    fn english_tokenize_spanned_preserves_offsets_after_filter_and_stem() {
        let tokenizer = EnglishTokenizer::new();
        let text = "The databases are running quickly";
        let plain = tokenizer.tokenize_to_vec(text);
        let spanned = tokenizer.tokenize_spanned_to_vec(text);
        assert_terms_and_positions_match(&spanned, &plain);

        let raw_terms: Vec<&str> = spanned
            .iter()
            .map(|token| &text[token.byte_start..token.byte_end])
            .collect();
        assert_eq!(raw_terms, vec!["databases", "running", "quickly"]);
    }

    #[test]
    fn chinese_tokenize_spanned_matches_plain_tokens() {
        let tokenizer = ChineseTokenizer::new();
        let text = "数据库的系统";
        let plain = tokenizer.tokenize_to_vec(text);
        let spanned = tokenizer.tokenize_spanned_to_vec(text);
        assert_terms_and_positions_match(&spanned, &plain);

        let raw_terms: Vec<&str> = spanned
            .iter()
            .map(|token| &text[token.byte_start..token.byte_end])
            .collect();
        assert_eq!(raw_terms, vec!["数", "据", "库", "系", "统"]);
    }

    #[test]
    fn japanese_tokenize_spanned_matches_plain_tokens() {
        let tokenizer = JapaneseTokenizer::new();
        let text = "東京のデータベース";
        let plain = tokenizer.tokenize_to_vec(text);
        let spanned = tokenizer.tokenize_spanned_to_vec(text);
        assert_terms_and_positions_match(&spanned, &plain);

        let raw_terms: Vec<&str> = spanned
            .iter()
            .map(|token| &text[token.byte_start..token.byte_end])
            .collect();
        assert_eq!(
            raw_terms,
            vec!["東", "京", "デ", "ー", "タ", "ベ", "ー", "ス"]
        );
    }
}
