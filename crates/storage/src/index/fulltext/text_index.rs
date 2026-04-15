// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Full-Text Index
//!
//! Combines tokenizer and inverted index with basic configuration.

use std::collections::HashSet;
use std::sync::Mutex;
use std::time::Instant;

use paro_common::error::Result;
use roaring::RoaringBitmap;

use super::bm25::Bm25;
use super::inverted_index::InvertedIndex;
use super::posting_list::{DocId, PostingList};
use super::query_parser::{parse_query, ParsedQuery};
use super::tokenizer::{tokenizer_from_kind, Token, Tokenizer, TokenizerKind};
use crate::index::hnsw::{FixedLengthPriorityQueue, PointOffset, ScoredPoint};
use crate::statistics::FullTextSearchTelemetry;

/// Configuration for full-text indexing.
#[derive(Debug, Clone)]
pub struct FullTextIndexConfig {
    pub min_token_len: usize,
    pub max_token_len: Option<usize>,
    pub bm25_k1: f32,
    pub bm25_b: f32,
}

impl Default for FullTextIndexConfig {
    fn default() -> Self {
        Self {
            min_token_len: 1,
            max_token_len: None,
            bm25_k1: 1.2,
            bm25_b: 0.75,
        }
    }
}

/// BM25 statistics aggregated across all queried segments.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GlobalFullTextStats {
    pub total_docs: u32,
    pub total_terms: u64,
    pub avg_doc_length: f32,
}

impl GlobalFullTextStats {
    pub fn from_totals(total_docs: u32, total_terms: u64) -> Self {
        let avg_doc_length = if total_docs == 0 {
            0.0
        } else {
            total_terms as f32 / total_docs as f32
        };
        Self {
            total_docs,
            total_terms,
            avg_doc_length,
        }
    }
}

/// Full-text index with tokenizer and inverted index.
pub struct FullTextIndex {
    tokenizer: Box<dyn Tokenizer>,
    inverted_index: InvertedIndex,
    config: FullTextIndexConfig,
    bm25: Bm25,
    telemetry: Mutex<FullTextSearchTelemetry>,
}

impl FullTextIndex {
    pub fn new(tokenizer: Box<dyn Tokenizer>, config: FullTextIndexConfig) -> Self {
        let bm25 = Bm25::new(config.bm25_k1, config.bm25_b);
        Self {
            tokenizer,
            inverted_index: InvertedIndex::new(),
            config,
            bm25,
            telemetry: Mutex::new(FullTextSearchTelemetry::default()),
        }
    }

    pub fn new_with_tokenizer_kind(kind: TokenizerKind, config: FullTextIndexConfig) -> Self {
        Self::new(tokenizer_from_kind(kind), config)
    }

    pub fn new_default() -> Self {
        Self::new_with_tokenizer_kind(TokenizerKind::Default, FullTextIndexConfig::default())
    }

    pub fn config(&self) -> &FullTextIndexConfig {
        &self.config
    }

    pub fn tokenizer(&self) -> &dyn Tokenizer {
        self.tokenizer.as_ref()
    }

    pub fn inverted_index(&self) -> &InvertedIndex {
        &self.inverted_index
    }

    pub fn inverted_index_mut(&mut self) -> &mut InvertedIndex {
        &mut self.inverted_index
    }

    /// Snapshot search telemetry.
    pub fn search_telemetry(&self) -> FullTextSearchTelemetry {
        self.telemetry.lock().unwrap().clone()
    }

    pub(crate) fn from_parts(
        tokenizer: Box<dyn Tokenizer>,
        config: FullTextIndexConfig,
        inverted_index: InvertedIndex,
    ) -> Self {
        let bm25 = Bm25::new(config.bm25_k1, config.bm25_b);
        Self {
            tokenizer,
            inverted_index,
            config,
            bm25,
            telemetry: Mutex::new(FullTextSearchTelemetry::default()),
        }
    }

    /// Tokenize and add a document into the index.
    pub fn add_document(&mut self, doc_id: DocId, text: &str) -> Result<()> {
        let tokens = self.tokenize_filtered(text);
        self.inverted_index.add_document(doc_id, &tokens)
    }

    /// Parse MATCH query text into a parsed query.
    pub fn parse_query(&self, text: &str) -> Result<ParsedQuery> {
        parse_query(
            text,
            self.tokenizer.as_ref(),
            self.config.min_token_len,
            self.config.max_token_len,
        )
    }

    /// Filter mode: returns matching doc_ids as a bitmap, optionally intersected with `filter_bitmap`.
    pub fn filter(
        &self,
        query: &ParsedQuery,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> RoaringBitmap {
        let start = Instant::now();
        let mut match_bitmap = self.match_bitmap(query);
        let pre_filter_count = match_bitmap.len();
        if let Some(bm) = filter_bitmap {
            match_bitmap &= bm;
        }
        let post_filter_count = match_bitmap.len();

        let elapsed_us = start.elapsed().as_micros() as u64;
        self.telemetry.lock().unwrap().record_filter(
            elapsed_us,
            pre_filter_count,
            post_filter_count,
            post_filter_count,
        );
        match_bitmap
    }

    /// Search mode: compute BM25 scores for matching documents.
    pub fn search(
        &self,
        query: &ParsedQuery,
        top_k: usize,
        filter_bitmap: Option<&RoaringBitmap>,
        global_stats: Option<&GlobalFullTextStats>,
    ) -> Vec<ScoredPoint> {
        if top_k == 0 {
            return Vec::new();
        }

        let start = Instant::now();
        let mut match_bitmap = self.match_bitmap(query);
        let pre_filter_count = match_bitmap.len();
        if let Some(bm) = filter_bitmap {
            match_bitmap &= bm;
        }
        let post_filter_count = match_bitmap.len();
        if match_bitmap.is_empty() {
            let elapsed_us = start.elapsed().as_micros() as u64;
            self.telemetry.lock().unwrap().record_search(
                elapsed_us,
                pre_filter_count,
                post_filter_count,
                post_filter_count,
                0,
            );
            return Vec::new();
        }

        let terms = self.collect_unique_terms(query);
        let stats = global_stats.copied().unwrap_or_else(|| {
            GlobalFullTextStats::from_totals(
                self.inverted_index.total_docs(),
                self.inverted_index.total_terms(),
            )
        });
        if stats.total_docs == 0 || stats.avg_doc_length == 0.0 {
            let elapsed_us = start.elapsed().as_micros() as u64;
            self.telemetry.lock().unwrap().record_search(
                elapsed_us,
                pre_filter_count,
                post_filter_count,
                post_filter_count,
                0,
            );
            return Vec::new();
        }

        let candidate_count = usize::try_from(match_bitmap.len()).unwrap_or(usize::MAX);
        let effective_top_k = top_k.min(candidate_count);
        if effective_top_k == 0 {
            let elapsed_us = start.elapsed().as_micros() as u64;
            self.telemetry.lock().unwrap().record_search(
                elapsed_us,
                pre_filter_count,
                post_filter_count,
                post_filter_count,
                0,
            );
            return Vec::new();
        }

        let mut topk = FixedLengthPriorityQueue::new(effective_top_k);
        for doc_id in match_bitmap.iter() {
            let score = self.bm25_score(
                doc_id as DocId,
                &terms,
                stats.total_docs,
                stats.avg_doc_length,
            );
            if score >= 0.0 {
                topk.push(ScoredPoint {
                    idx: doc_id as PointOffset,
                    score,
                });
            }
        }
        let elapsed_us = start.elapsed().as_micros() as u64;
        self.telemetry.lock().unwrap().record_search(
            elapsed_us,
            pre_filter_count,
            post_filter_count,
            post_filter_count,
            post_filter_count,
        );
        topk.into_sorted_vec()
    }

    fn tokenize_filtered(&self, text: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        self.tokenizer.tokenize(text, &mut tokens);
        tokens
            .into_iter()
            .filter(|token| self.is_token_len_valid(&token.term))
            .collect()
    }

    fn match_bitmap(&self, query: &ParsedQuery) -> RoaringBitmap {
        match query {
            ParsedQuery::Term(term) => self.term_bitmap(term),
            ParsedQuery::Phrase(terms) => self.phrase_bitmap(terms),
            ParsedQuery::And(items) => {
                let mut iter = items.iter();
                let Some(first) = iter.next() else {
                    return RoaringBitmap::new();
                };
                let mut bitmap = self.match_bitmap(first);
                for item in iter {
                    let right = self.match_bitmap(item);
                    bitmap &= &right;
                    if bitmap.is_empty() {
                        break;
                    }
                }
                bitmap
            }
            ParsedQuery::Or(items) => {
                let mut bitmap = RoaringBitmap::new();
                for item in items {
                    let right = self.match_bitmap(item);
                    bitmap |= &right;
                }
                bitmap
            }
            ParsedQuery::Not(child) => {
                let mut bitmap = self.inverted_index.all_doc_ids();
                let child_bitmap = self.match_bitmap(child);
                bitmap -= &child_bitmap;
                bitmap
            }
            ParsedQuery::FollowedBy(items, distance) => self.followed_by_bitmap(items, *distance),
            ParsedQuery::Prefix(prefix) => self.prefix_bitmap(prefix),
        }
    }

    fn prefix_bitmap(&self, prefix: &str) -> RoaringBitmap {
        self.inverted_index.prefix_doc_ids(prefix)
    }

    fn followed_by_bitmap(&self, items: &[ParsedQuery], distance: u32) -> RoaringBitmap {
        if distance == 0 {
            return RoaringBitmap::new();
        }
        let Some(terms) = extract_followed_by_terms(items, distance) else {
            return RoaringBitmap::new();
        };
        if terms.is_empty() {
            return RoaringBitmap::new();
        }
        let Some(candidates) = self.intersect_term_postings_bitmap(&terms) else {
            return RoaringBitmap::new();
        };

        let mut out = RoaringBitmap::new();
        for doc_id in candidates.iter() {
            if self.doc_has_followed_by(doc_id as DocId, &terms, distance) {
                out.insert(doc_id);
            }
        }
        out
    }

    fn term_bitmap(&self, term: &str) -> RoaringBitmap {
        let Some(list) = self.inverted_index.get_posting_list(term) else {
            return RoaringBitmap::new();
        };
        RoaringBitmap::from_iter(list.iter().map(|e| e.doc_id))
    }

    fn phrase_bitmap(&self, terms: &[String]) -> RoaringBitmap {
        if terms.is_empty() {
            return RoaringBitmap::new();
        }

        let Some(candidates) = self.intersect_term_postings_bitmap(terms) else {
            return RoaringBitmap::new();
        };

        let mut out = RoaringBitmap::new();
        for doc_id in candidates.iter() {
            if self.doc_has_phrase(doc_id as DocId, terms) {
                out.insert(doc_id);
            }
        }
        out
    }

    fn doc_has_phrase(&self, doc_id: DocId, terms: &[String]) -> bool {
        self.doc_has_followed_by(doc_id, terms, 1)
    }

    fn doc_has_followed_by(&self, doc_id: DocId, terms: &[String], distance: u32) -> bool {
        if terms.is_empty() {
            return false;
        }
        if distance == 0 {
            return false;
        }

        let mut positions_per_term = Vec::with_capacity(terms.len());
        for term in terms {
            let Some(list) = self.inverted_index.get_posting_list(term) else {
                return false;
            };
            let Some(elem) = list.get(doc_id) else {
                return false;
            };
            positions_per_term.push(elem.positions.as_slice());
        }

        let mut reachable = positions_per_term[0].to_vec();
        for positions in positions_per_term.iter().skip(1) {
            reachable = positions_following_by_distance(&reachable, positions, distance);
            if reachable.is_empty() {
                return false;
            }
        }
        !reachable.is_empty()
    }

    fn intersect_term_postings_bitmap(&self, terms: &[String]) -> Option<RoaringBitmap> {
        let mut posting_lists: Vec<&PostingList> = Vec::with_capacity(terms.len());
        for term in terms {
            posting_lists.push(self.inverted_index.get_posting_list(term)?);
        }
        posting_lists.sort_by_key(|list| list.len());

        let mut iter = posting_lists.into_iter();
        let first = iter.next()?;
        let mut candidates = RoaringBitmap::from_iter(first.iter().map(|e| e.doc_id));
        for list in iter {
            let right = RoaringBitmap::from_iter(list.iter().map(|e| e.doc_id));
            candidates &= &right;
            if candidates.is_empty() {
                return Some(candidates);
            }
        }
        Some(candidates)
    }

    fn collect_unique_terms(&self, query: &ParsedQuery) -> Vec<String> {
        let mut terms = Vec::new();
        collect_terms(query, &mut terms);
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for term in terms {
            if seen.insert(term.clone()) {
                out.push(term);
            }
        }
        out
    }

    fn bm25_score(&self, doc_id: DocId, terms: &[String], total_docs: u32, avgdl: f32) -> f32 {
        if total_docs == 0 || avgdl == 0.0 {
            return 0.0;
        }
        let Some(doc_len) = self.inverted_index.doc_length(doc_id) else {
            return 0.0;
        };
        if doc_len == 0 {
            return 0.0;
        }

        let dl = doc_len as f32;
        let total_docs_f = total_docs as f32;

        let mut score = 0.0f32;
        for term in terms {
            let Some(list) = self.inverted_index.get_posting_list(term) else {
                continue;
            };
            let df = list.len() as f32;
            if df == 0.0 {
                continue;
            }
            let tf = list
                .get(doc_id)
                .map(|elem| elem.term_frequency as f32)
                .unwrap_or(0.0);
            if tf == 0.0 {
                continue;
            }
            score += self.bm25.score(tf, dl, avgdl, df, total_docs_f);
        }
        score
    }

    fn is_token_len_valid(&self, term: &str) -> bool {
        let len = term.chars().count();
        if len < self.config.min_token_len {
            return false;
        }
        if let Some(max_len) = self.config.max_token_len {
            if len > max_len {
                return false;
            }
        }
        true
    }
}

impl Default for FullTextIndex {
    fn default() -> Self {
        Self::new_default()
    }
}

fn extract_followed_by_terms(items: &[ParsedQuery], distance: u32) -> Option<Vec<String>> {
    let mut out = Vec::new();
    for item in items {
        match item {
            ParsedQuery::Term(term) => out.push(term.clone()),
            ParsedQuery::FollowedBy(inner, inner_distance) if *inner_distance == distance => {
                out.extend(extract_followed_by_terms(inner, distance)?);
            }
            _ => return None,
        }
    }
    Some(out)
}

fn positions_following_by_distance(
    left_positions: &[u32],
    right_positions: &[u32],
    distance: u32,
) -> Vec<u32> {
    let mut i = 0usize;
    let mut j = 0usize;
    let mut out = Vec::new();

    while i < left_positions.len() && j < right_positions.len() {
        let Some(target) = left_positions[i].checked_add(distance) else {
            break;
        };
        let right = right_positions[j];
        if right < target {
            j += 1;
        } else if right > target {
            i += 1;
        } else {
            out.push(right);
            i += 1;
            j += 1;
        }
    }
    out
}

fn collect_terms(query: &ParsedQuery, out: &mut Vec<String>) {
    match query {
        ParsedQuery::Term(term) => out.push(term.clone()),
        ParsedQuery::Phrase(terms) => out.extend(terms.iter().cloned()),
        ParsedQuery::Not(item) => collect_terms(item, out),
        ParsedQuery::FollowedBy(items, _) => {
            for item in items {
                collect_terms(item, out);
            }
        }
        ParsedQuery::Prefix(prefix) => out.push(prefix.clone()),
        ParsedQuery::And(items) | ParsedQuery::Or(items) => {
            for item in items {
                collect_terms(item, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::fulltext::tokenizer::DefaultTokenizer;
    use roaring::RoaringBitmap;

    #[test]
    fn fulltext_index_add_and_parse() {
        let mut index = FullTextIndex::new_default();
        index.add_document(1, "Hello world").unwrap();
        index.add_document(2, "World of vector search").unwrap();

        let query = index.parse_query("world AND vector").unwrap();
        assert_eq!(
            query,
            ParsedQuery::And(vec![
                ParsedQuery::Term("world".to_string()),
                ParsedQuery::Term("vector".to_string())
            ])
        );
    }

    #[test]
    fn fulltext_filter_and_phrase() {
        let mut index = FullTextIndex::new_default();
        index.add_document(1, "vector database").unwrap();
        index.add_document(2, "database for vectors").unwrap();

        let query = index.parse_query("\"vector database\"").unwrap();
        let bitmap = index.filter(&query, None);
        let ids: Vec<u32> = bitmap.iter().collect();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn fulltext_search_bm25() {
        let mut index = FullTextIndex::new_default();
        index.add_document(1, "hello world").unwrap();
        index.add_document(2, "hello hello world").unwrap();

        let query = index.parse_query("hello").unwrap();
        let results = index.search(&query, 2, None, None);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].idx, 2);
    }

    #[test]
    fn test_tokenizer() {
        let tokenizer = DefaultTokenizer::new();
        let tokens = tokenizer.tokenize_to_vec("Hello, world!");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["hello", "world"]);
    }

    #[test]
    fn test_fulltext_filter() {
        let mut index = FullTextIndex::new_default();
        index.add_document(1, "hello world").unwrap();
        index.add_document(2, "hello paro").unwrap();
        index.add_document(3, "vector search").unwrap();

        let query = index.parse_query("hello").unwrap();
        let bitmap = index.filter(&query, None);
        let ids: Vec<u32> = bitmap.iter().collect();
        assert_eq!(ids, vec![1, 2]);

        let query = index.parse_query("hello AND world").unwrap();
        let bitmap = index.filter(&query, None);
        let ids: Vec<u32> = bitmap.iter().collect();
        assert_eq!(ids, vec![1]);

        let query = index.parse_query("paro OR vector").unwrap();
        let bitmap = index.filter(&query, None);
        let ids: Vec<u32> = bitmap.iter().collect();
        assert_eq!(ids, vec![2, 3]);
    }

    #[test]
    fn test_bm25_ranking() {
        let mut index = FullTextIndex::new_default();
        index.add_document(1, "hello world").unwrap();
        index.add_document(2, "hello hello world").unwrap();

        let query = index.parse_query("hello").unwrap();
        let results = index.search(&query, 2, None, None);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].idx, 2);
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn test_fulltext_with_filter_bitmap() {
        let mut index = FullTextIndex::new_default();
        index.add_document(1, "hello world").unwrap();
        index.add_document(2, "hello paro").unwrap();

        let query = index.parse_query("hello").unwrap();
        let mut filter_bitmap = RoaringBitmap::new();
        filter_bitmap.insert(2);

        let bitmap = index.filter(&query, Some(&filter_bitmap));
        let ids: Vec<u32> = bitmap.iter().collect();
        assert_eq!(ids, vec![2]);
    }

    #[test]
    fn test_search_uses_global_stats_for_cross_segment_consistency() {
        let mut seg_small = FullTextIndex::new_default();
        seg_small.add_document(1, "vector").unwrap();

        let mut seg_large = FullTextIndex::new_default();
        seg_large.add_document(1, "vector").unwrap();
        for i in 2..=200 {
            seg_large.add_document(i, "other").unwrap();
        }

        let query_small = seg_small.parse_query("vector").unwrap();
        let query_large = seg_large.parse_query("vector").unwrap();

        let local_small = seg_small.search(&query_small, 1, None, None);
        let local_large = seg_large.search(&query_large, 1, None, None);
        assert_eq!(local_small.len(), 1);
        assert_eq!(local_large.len(), 1);
        assert_ne!(
            local_small[0].score, local_large[0].score,
            "Local per-segment stats should differ for asymmetric segments"
        );

        let global = GlobalFullTextStats::from_totals(201, 201);
        let global_small = seg_small.search(&query_small, 1, None, Some(&global));
        let global_large = seg_large.search(&query_large, 1, None, Some(&global));
        assert_eq!(global_small.len(), 1);
        assert_eq!(global_large.len(), 1);

        let delta = (global_small[0].score - global_large[0].score).abs();
        assert!(
            delta < 1e-6,
            "Expected equal scores with global stats, delta={delta}"
        );
    }

    #[test]
    fn test_not_query_bitmap() {
        let mut index = FullTextIndex::new_default();
        index.add_document(1, "hello world").unwrap();
        index.add_document(2, "hello paro").unwrap();
        index.add_document(3, "vector search").unwrap();

        let query = ParsedQuery::Not(Box::new(ParsedQuery::Term("hello".to_string())));
        let bitmap = index.filter(&query, None);
        let ids: Vec<u32> = bitmap.iter().collect();
        assert_eq!(ids, vec![3]);
    }

    #[test]
    fn test_prefix_query_bitmap() {
        let mut index = FullTextIndex::new_default();
        index.add_document(1, "vector database").unwrap();
        index.add_document(2, "vectors everywhere").unwrap();
        index.add_document(3, "graph storage").unwrap();

        let query = ParsedQuery::Prefix("vec".to_string());
        let bitmap = index.filter(&query, None);
        let ids: Vec<u32> = bitmap.iter().collect();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn test_followed_by_query_distance_n() {
        let mut index = FullTextIndex::new_default();
        index.add_document(1, "alpha beta gamma").unwrap();
        index.add_document(2, "alpha x y gamma").unwrap();
        index.add_document(3, "gamma alpha beta").unwrap();

        let query = ParsedQuery::FollowedBy(
            vec![
                ParsedQuery::Term("alpha".to_string()),
                ParsedQuery::Term("gamma".to_string()),
            ],
            2,
        );
        let bitmap = index.filter(&query, None);
        let ids: Vec<u32> = bitmap.iter().collect();
        assert_eq!(ids, vec![1]);

        let query = ParsedQuery::FollowedBy(
            vec![
                ParsedQuery::Term("alpha".to_string()),
                ParsedQuery::Term("beta".to_string()),
                ParsedQuery::Term("gamma".to_string()),
            ],
            1,
        );
        let bitmap = index.filter(&query, None);
        let ids: Vec<u32> = bitmap.iter().collect();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn test_followed_by_query_uses_position_chain_across_multiple_hits() {
        let mut index = FullTextIndex::new_default();
        index
            .add_document(1, "alpha x alpha y gamma alpha y gamma")
            .unwrap();
        index.add_document(2, "alpha y y gamma").unwrap();

        let query = ParsedQuery::FollowedBy(
            vec![
                ParsedQuery::Term("alpha".to_string()),
                ParsedQuery::Term("gamma".to_string()),
            ],
            2,
        );
        let bitmap = index.filter(&query, None);
        let ids: Vec<u32> = bitmap.iter().collect();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn test_positions_following_by_distance_linear_match() {
        let left = vec![0u32, 2, 10];
        let right = vec![1u32, 3, 11, 12];
        assert_eq!(
            positions_following_by_distance(&left, &right, 1),
            vec![1, 3, 11]
        );
        assert!(positions_following_by_distance(&left, &right, 5).is_empty());
    }
}
