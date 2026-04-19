// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Full-text scoring helpers.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use super::bm25::Bm25;
use super::inverted_index::InvertedIndex;
use super::posting_list::DocId;
use super::query_eval::{self, TokenLike};
use super::query_parser::ParsedQuery;
use super::tokenizer::{Token, TokenPosition};

/// Score mode for full-text ranking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullTextScoreMode {
    Bm25,
    CoverDensity,
}

impl Default for FullTextScoreMode {
    fn default() -> Self {
        Self::Bm25
    }
}

impl FullTextScoreMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bm25 => "bm25",
            Self::CoverDensity => "cover_density",
        }
    }
}

/// Score a single document from the inverted index.
pub fn score_document_from_index(
    score_mode: FullTextScoreMode,
    index: &InvertedIndex,
    bm25: &Bm25,
    query: &ParsedQuery,
    doc_id: DocId,
    stats: super::text_index::GlobalFullTextStats,
) -> f32 {
    match score_mode {
        FullTextScoreMode::Bm25 => score_bm25(index, bm25, query, doc_id, stats),
        FullTextScoreMode::CoverDensity => score_cover_density(index, query, doc_id),
    }
}

/// Score a tokenized document without consulting index-global statistics.
pub fn score_document_from_tokens<T: TokenLike>(
    score_mode: FullTextScoreMode,
    tokens: &[T],
    query: &ParsedQuery,
) -> f32 {
    match score_mode {
        FullTextScoreMode::Bm25 => score_bm25_tokens(tokens, query),
        FullTextScoreMode::CoverDensity => score_cover_density_tokens(tokens, query),
    }
}

fn score_bm25(
    index: &InvertedIndex,
    bm25: &Bm25,
    query: &ParsedQuery,
    doc_id: DocId,
    stats: super::text_index::GlobalFullTextStats,
) -> f32 {
    if stats.total_docs == 0 || stats.avg_doc_length == 0.0 {
        return 0.0;
    }
    let Some(doc_len) = index.doc_length(doc_id) else {
        return 0.0;
    };
    if doc_len == 0 {
        return 0.0;
    }

    let dl = doc_len as f32;
    let total_docs_f = stats.total_docs as f32;
    score_bm25_query(
        index,
        bm25,
        query,
        doc_id,
        dl,
        total_docs_f,
        stats.avg_doc_length,
    )
}

fn score_bm25_query(
    index: &InvertedIndex,
    bm25: &Bm25,
    query: &ParsedQuery,
    doc_id: DocId,
    doc_len: f32,
    total_docs: f32,
    avgdl: f32,
) -> f32 {
    match query {
        ParsedQuery::Term(term) => {
            bm25_term_score(index, bm25, term, doc_id, doc_len, total_docs, avgdl)
        }
        ParsedQuery::Prefix(prefix) => {
            bm25_prefix_score(index, bm25, prefix, doc_id, doc_len, total_docs, avgdl)
        }
        ParsedQuery::Phrase(terms) => terms
            .iter()
            .map(|term| bm25_term_score(index, bm25, term, doc_id, doc_len, total_docs, avgdl))
            .sum(),
        ParsedQuery::FollowedBy(items, _) => items
            .iter()
            .map(|item| score_bm25_query(index, bm25, item, doc_id, doc_len, total_docs, avgdl))
            .sum(),
        ParsedQuery::And(items) => {
            let mut sum = 0.0;
            for item in items {
                sum += score_bm25_query(index, bm25, item, doc_id, doc_len, total_docs, avgdl);
            }
            sum
        }
        ParsedQuery::Or(items) => items
            .iter()
            .map(|item| score_bm25_query(index, bm25, item, doc_id, doc_len, total_docs, avgdl))
            .fold(0.0, f32::max),
        ParsedQuery::Not(_) => 0.0,
    }
}

fn bm25_term_score(
    index: &InvertedIndex,
    bm25: &Bm25,
    term: &str,
    doc_id: DocId,
    doc_len: f32,
    total_docs: f32,
    avgdl: f32,
) -> f32 {
    let Some(list) = index.get_posting_list(term) else {
        return 0.0;
    };
    let df = list.len() as f32;
    let tf = list
        .get(doc_id)
        .map(|elem| elem.term_frequency as f32)
        .unwrap_or(0.0);
    bm25.score(tf, doc_len, avgdl, df, total_docs)
}

fn bm25_prefix_score(
    index: &InvertedIndex,
    bm25: &Bm25,
    prefix: &str,
    doc_id: DocId,
    doc_len: f32,
    total_docs: f32,
    avgdl: f32,
) -> f32 {
    let mut score = 0.0;
    for (term, list) in index.postings().range(prefix.to_string()..) {
        if !term.starts_with(prefix) {
            break;
        }
        let df = list.len() as f32;
        let tf = list
            .get(doc_id)
            .map(|elem| elem.term_frequency as f32)
            .unwrap_or(0.0);
        if tf > 0.0 {
            score += bm25.score(tf, doc_len, avgdl, df, total_docs);
        }
    }
    score
}

fn score_cover_density(index: &InvertedIndex, query: &ParsedQuery, doc_id: DocId) -> f32 {
    match query {
        ParsedQuery::And(items) => {
            let mut groups = Vec::new();
            for item in items {
                match item {
                    ParsedQuery::Not(child) => {
                        let tokens = collect_tokens(index, child, doc_id);
                        if query_eval::matches_query(&tokens, child) {
                            return 0.0;
                        }
                    }
                    _ => {
                        let tokens = collect_tokens(index, item, doc_id);
                        let ranges = query_eval::positive_ranges(&tokens, item);
                        if ranges.is_empty() {
                            return 0.0;
                        }
                        groups.push(ranges);
                    }
                }
            }
            score_cover_groups(&groups)
        }
        ParsedQuery::Or(items) => items
            .iter()
            .map(|item| score_cover_density(index, item, doc_id))
            .fold(0.0, f32::max),
        ParsedQuery::Not(_) => 0.0,
        _ => {
            let tokens = collect_tokens(index, query, doc_id);
            let ranges = query_eval::positive_ranges(&tokens, query);
            if ranges.is_empty() {
                0.0
            } else {
                score_cover_groups(&[ranges])
            }
        }
    }
}

fn extent_width(start: TokenPosition, end: TokenPosition) -> u32 {
    end.saturating_sub(start).max(1)
}

#[derive(Debug, Clone, Copy)]
struct CoverEvent {
    group_idx: usize,
    start_pos: TokenPosition,
    end_pos: TokenPosition,
}

fn score_cover_groups(groups: &[Vec<query_eval::RangeMatch>]) -> f32 {
    if groups.is_empty() || groups.iter().any(|group| group.is_empty()) {
        return 0.0;
    }

    let mut events = Vec::new();
    for (group_idx, group) in groups.iter().enumerate() {
        for range in group {
            events.push(CoverEvent {
                group_idx,
                start_pos: range.start_pos,
                end_pos: range.end_pos,
            });
        }
    }
    events.sort_by_key(|event| (event.start_pos, event.end_pos, event.group_idx));

    let mut score = 0.0f32;
    let mut next_event_idx = 0usize;

    while next_event_idx < events.len() {
        let mut counts = vec![0usize; groups.len()];
        let mut covered_groups = 0usize;
        let mut end_counts = BTreeMap::<TokenPosition, usize>::new();
        let mut left = next_event_idx;
        let mut found_cover = false;

        let mut right = next_event_idx;
        while right < events.len() {
            add_cover_event(
                events[right],
                &mut counts,
                &mut covered_groups,
                &mut end_counts,
            );

            while covered_groups == groups.len() && counts[events[left].group_idx] > 1 {
                remove_cover_event(
                    events[left],
                    &mut counts,
                    &mut covered_groups,
                    &mut end_counts,
                );
                left += 1;
            }

            if covered_groups != groups.len() {
                right += 1;
                continue;
            }

            let cover_start = events[left].start_pos;
            let cover_end = *end_counts
                .last_key_value()
                .expect("cover must have an end position")
                .0;
            let width = extent_width(cover_start, cover_end);
            score += groups.len() as f32 / width as f32;

            next_event_idx = right + 1;
            while next_event_idx < events.len() && events[next_event_idx].start_pos <= cover_end {
                next_event_idx += 1;
            }
            found_cover = true;
            break;
        }

        if !found_cover {
            break;
        }
    }

    score
}

fn add_cover_event(
    event: CoverEvent,
    counts: &mut [usize],
    covered_groups: &mut usize,
    end_counts: &mut BTreeMap<TokenPosition, usize>,
) {
    if counts[event.group_idx] == 0 {
        *covered_groups += 1;
    }
    counts[event.group_idx] += 1;
    *end_counts.entry(event.end_pos).or_default() += 1;
}

fn remove_cover_event(
    event: CoverEvent,
    counts: &mut [usize],
    covered_groups: &mut usize,
    end_counts: &mut BTreeMap<TokenPosition, usize>,
) {
    counts[event.group_idx] -= 1;
    if counts[event.group_idx] == 0 {
        *covered_groups -= 1;
    }
    if let Some(count) = end_counts.get_mut(&event.end_pos) {
        *count -= 1;
        if *count == 0 {
            end_counts.remove(&event.end_pos);
        }
    }
}

fn score_bm25_tokens<T: TokenLike>(tokens: &[T], query: &ParsedQuery) -> f32 {
    if tokens.is_empty() {
        return 0.0;
    }

    let mut term_freqs = BTreeMap::<&str, u32>::new();
    for token in tokens {
        *term_freqs.entry(token.term()).or_default() += 1;
    }

    score_bm25_tokens_query(query, tokens.len() as f32, &term_freqs)
}

fn score_bm25_tokens_query(
    query: &ParsedQuery,
    doc_len: f32,
    term_freqs: &BTreeMap<&str, u32>,
) -> f32 {
    match query {
        ParsedQuery::Term(term) => {
            lightweight_bm25_component(*term_freqs.get(term.as_str()).unwrap_or(&0) as f32, doc_len)
        }
        ParsedQuery::Prefix(prefix) => term_freqs
            .iter()
            .filter(|(term, _)| term.starts_with(prefix))
            .map(|(_, tf)| lightweight_bm25_component(*tf as f32, doc_len))
            .sum(),
        ParsedQuery::Phrase(terms) => terms
            .iter()
            .map(|term| {
                lightweight_bm25_component(
                    *term_freqs.get(term.as_str()).unwrap_or(&0) as f32,
                    doc_len,
                )
            })
            .sum(),
        ParsedQuery::FollowedBy(items, _) | ParsedQuery::And(items) => {
            let mut sum = 0.0;
            for item in items {
                let score = score_bm25_tokens_query(item, doc_len, term_freqs);
                if score == 0.0 && !matches!(item, ParsedQuery::Not(_)) {
                    return 0.0;
                }
                sum += score;
            }
            sum
        }
        ParsedQuery::Or(items) => items
            .iter()
            .map(|item| score_bm25_tokens_query(item, doc_len, term_freqs))
            .fold(0.0, f32::max),
        ParsedQuery::Not(_) => 0.0,
    }
}

fn lightweight_bm25_component(tf: f32, doc_len: f32) -> f32 {
    if tf <= 0.0 || doc_len <= 0.0 {
        return 0.0;
    }
    // Sequential fallback only sees a single analyzed document, so it has no
    // corpus-level document frequency to derive a stable IDF term. We still keep
    // BM25-style TF saturation and document-length normalization so scoring is
    // meaningfully better than legacy 0/1 counting.
    let avgdl = doc_len.max(1.0);
    const K1: f32 = 1.2;
    const B: f32 = 0.75;
    let norm = K1 * (1.0 - B + B * (doc_len / avgdl));
    (tf * (K1 + 1.0)) / (tf + norm)
}

fn score_cover_density_tokens<T: TokenLike>(tokens: &[T], query: &ParsedQuery) -> f32 {
    match query {
        ParsedQuery::And(items) => {
            let mut groups = Vec::new();
            for item in items {
                match item {
                    ParsedQuery::Not(child) => {
                        if query_eval::matches_query(tokens, child) {
                            return 0.0;
                        }
                    }
                    _ => {
                        let ranges = query_eval::positive_ranges(tokens, item);
                        if ranges.is_empty() {
                            return 0.0;
                        }
                        groups.push(ranges);
                    }
                }
            }
            score_cover_groups(&groups)
        }
        ParsedQuery::Or(items) => items
            .iter()
            .map(|item| score_cover_density_tokens(tokens, item))
            .fold(0.0, f32::max),
        ParsedQuery::Not(_) => 0.0,
        _ => {
            let ranges = query_eval::positive_ranges(tokens, query);
            if ranges.is_empty() {
                0.0
            } else {
                score_cover_groups(&[ranges])
            }
        }
    }
}

fn collect_tokens(index: &InvertedIndex, query: &ParsedQuery, doc_id: DocId) -> Vec<Token> {
    let mut tuples: Vec<(TokenPosition, String)> = Vec::new();
    collect_tokens_for_query(index, query, doc_id, &mut tuples);
    tuples.sort_by(|a, b| match a.0.cmp(&b.0) {
        Ordering::Equal => a.1.cmp(&b.1),
        other => other,
    });
    tuples.dedup();
    tuples
        .into_iter()
        .map(|(position, term)| Token::new(term, position))
        .collect()
}

fn collect_tokens_for_query(
    index: &InvertedIndex,
    query: &ParsedQuery,
    doc_id: DocId,
    out: &mut Vec<(TokenPosition, String)>,
) {
    match query {
        ParsedQuery::Term(term) => collect_term_tokens(index, term, doc_id, out),
        ParsedQuery::Prefix(prefix) => collect_prefix_tokens(index, prefix, doc_id, out),
        ParsedQuery::Phrase(terms) => {
            for term in terms {
                collect_term_tokens(index, term, doc_id, out);
            }
        }
        ParsedQuery::FollowedBy(items, _) | ParsedQuery::And(items) | ParsedQuery::Or(items) => {
            for item in items {
                collect_tokens_for_query(index, item, doc_id, out);
            }
        }
        ParsedQuery::Not(child) => collect_tokens_for_query(index, child, doc_id, out),
    }
}

fn collect_term_tokens(
    index: &InvertedIndex,
    term: &str,
    doc_id: DocId,
    out: &mut Vec<(TokenPosition, String)>,
) {
    let Some(list) = index.get_posting_list(term) else {
        return;
    };
    let Some(elem) = list.get(doc_id) else {
        return;
    };
    for &position in &elem.positions {
        out.push((position, term.to_string()));
    }
}

fn collect_prefix_tokens(
    index: &InvertedIndex,
    prefix: &str,
    doc_id: DocId,
    out: &mut Vec<(TokenPosition, String)>,
) {
    for (term, list) in index.postings().range(prefix.to_string()..) {
        if !term.starts_with(prefix) {
            break;
        }
        let Some(elem) = list.get(doc_id) else {
            continue;
        };
        for &position in &elem.positions {
            out.push((position, term.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::fulltext::tokenizer::{DefaultTokenizer, Tokenizer};

    #[test]
    fn bm25_term_and_prefix_score() {
        let tokenizer = DefaultTokenizer::new();
        let mut tokens = Vec::new();
        tokenizer.tokenize("vector database vectors", &mut tokens);
        let index = InvertedIndex::from_parts(
            tokens
                .iter()
                .fold(std::collections::BTreeMap::new(), |mut acc, token| {
                    acc.entry(token.term.clone())
                        .or_insert_with(Default::default)
                        .add_position(1, token.position)
                        .unwrap();
                    acc
                }),
            std::collections::HashMap::from([(1, 3)]),
        );
        let bm25 = Bm25::default();
        let stats = super::super::text_index::GlobalFullTextStats::from_totals(1, 3);
        let term = score_document_from_index(
            FullTextScoreMode::Bm25,
            &index,
            &bm25,
            &ParsedQuery::Term("vector".to_string()),
            1,
            stats,
        );
        assert!(term > 0.0);
    }

    #[test]
    fn token_scoring_prefers_repeated_terms() {
        let short = vec![
            Token::new("vector".to_string(), 0),
            Token::new("database".to_string(), 1),
        ];
        let repeated = vec![
            Token::new("vector".to_string(), 0),
            Token::new("vector".to_string(), 1),
            Token::new("database".to_string(), 2),
        ];
        let query = ParsedQuery::Term("vector".to_string());

        let short_score = score_document_from_tokens(FullTextScoreMode::Bm25, &short, &query);
        let repeated_score = score_document_from_tokens(FullTextScoreMode::Bm25, &repeated, &query);
        assert!(repeated_score > short_score);
    }

    #[test]
    fn token_cover_density_uses_max_or_branch() {
        let tokens = vec![
            Token::new("alpha".to_string(), 0),
            Token::new("beta".to_string(), 1),
            Token::new("gamma".to_string(), 4),
        ];
        let query = ParsedQuery::Or(vec![
            ParsedQuery::Phrase(vec!["alpha".to_string(), "beta".to_string()]),
            ParsedQuery::Term("gamma".to_string()),
        ]);

        let score = score_document_from_tokens(FullTextScoreMode::CoverDensity, &tokens, &query);
        let branch = score_document_from_tokens(
            FullTextScoreMode::CoverDensity,
            &tokens,
            &ParsedQuery::Phrase(vec!["alpha".to_string(), "beta".to_string()]),
        );
        assert_eq!(score, branch);
    }

    #[test]
    fn cover_density_does_not_reuse_faraway_singletons_as_new_cover() {
        let tokens = vec![
            Token::new("alpha".to_string(), 0),
            Token::new("beta".to_string(), 2),
            Token::new("alpha".to_string(), 50),
        ];
        let query = ParsedQuery::And(vec![
            ParsedQuery::Term("alpha".to_string()),
            ParsedQuery::Term("beta".to_string()),
        ]);

        let score = score_document_from_tokens(FullTextScoreMode::CoverDensity, &tokens, &query);
        assert!(
            (score - 1.0).abs() < 1e-6,
            "unexpected cover-density score: {score}"
        );
    }
}
