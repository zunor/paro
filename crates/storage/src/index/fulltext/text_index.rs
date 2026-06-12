// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Full-Text Index
//!
//! Combines tokenizer and inverted index with basic configuration.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Instant;

use paro_common::error::Result;
use roaring::RoaringBitmap;

use super::bm25::Bm25;
use super::inverted_index::InvertedIndex;
use super::posting_list::{DocId, PostingList};
use super::query_eval::positions_following_by_distance;
use super::query_parser::{parse_query, ParsedQuery};
use super::scoring::{score_document_from_index, FullTextScoreMode};
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
    pub bm25_k1: f32,
    pub bm25_b: f32,
}

impl GlobalFullTextStats {
    pub fn from_totals(total_docs: u32, total_terms: u64) -> Self {
        Self::from_totals_with_bm25(total_docs, total_terms, 1.2, 0.75)
    }

    pub fn from_totals_with_bm25(
        total_docs: u32,
        total_terms: u64,
        bm25_k1: f32,
        bm25_b: f32,
    ) -> Self {
        let avg_doc_length = if total_docs == 0 {
            0.0
        } else {
            total_terms as f32 / total_docs as f32
        };
        Self {
            total_docs,
            total_terms,
            avg_doc_length,
            bm25_k1,
            bm25_b,
        }
    }

    pub fn with_added_totals(self, docs: u32, terms: u64) -> Self {
        Self::from_totals_with_bm25(
            self.total_docs.saturating_add(docs),
            self.total_terms.saturating_add(terms),
            self.bm25_k1,
            self.bm25_b,
        )
    }
}

/// Query-time BM25 snapshot used to score every candidate in one corpus frame.
#[derive(Debug, Clone, PartialEq)]
pub struct FullTextScoringStats {
    pub global: GlobalFullTextStats,
    term_doc_freqs: BTreeMap<String, u32>,
}

impl FullTextScoringStats {
    pub fn from_global_stats(global: GlobalFullTextStats) -> Self {
        Self {
            global,
            term_doc_freqs: BTreeMap::new(),
        }
    }

    pub fn with_term_doc_freqs(
        global: GlobalFullTextStats,
        term_doc_freqs: BTreeMap<String, u32>,
    ) -> Self {
        Self {
            global,
            term_doc_freqs,
        }
    }

    pub fn local_index(index: &InvertedIndex, config: &FullTextIndexConfig) -> Self {
        Self::from_global_stats(GlobalFullTextStats::from_totals_with_bm25(
            index.total_docs(),
            index.total_terms(),
            config.bm25_k1,
            config.bm25_b,
        ))
    }

    pub fn bm25(&self) -> Bm25 {
        Bm25::new(self.global.bm25_k1, self.global.bm25_b)
    }

    pub fn doc_freq(&self, term: &str, fallback: u32) -> u32 {
        self.term_doc_freqs
            .get(term)
            .copied()
            .filter(|doc_freq| *doc_freq > 0)
            .unwrap_or(fallback)
    }
}

/// Full-text index with tokenizer and inverted index.
pub struct FullTextIndex {
    tokenizer: Box<dyn Tokenizer>,
    inverted_index: InvertedIndex,
    config: FullTextIndexConfig,
    telemetry: Mutex<FullTextSearchTelemetry>,
}

impl FullTextIndex {
    pub fn new(tokenizer: Box<dyn Tokenizer>, config: FullTextIndexConfig) -> Self {
        Self {
            tokenizer,
            inverted_index: InvertedIndex::new(),
            config,
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
        Self {
            tokenizer,
            inverted_index,
            config,
            telemetry: Mutex::new(FullTextSearchTelemetry::default()),
        }
    }

    /// Tokenize and add a document into the index.
    pub fn add_document(&mut self, doc_id: DocId, text: &str) -> Result<()> {
        let mut tokens = Vec::new();
        self.add_document_with_token_buffer(doc_id, text, &mut tokens)
    }

    /// Tokenize and add a document using caller-owned scratch storage.
    pub fn add_document_with_token_buffer(
        &mut self,
        doc_id: DocId,
        text: &str,
        tokens: &mut Vec<Token>,
    ) -> Result<()> {
        tokens.clear();
        self.tokenize_filtered_into(text, tokens);
        self.inverted_index.add_document(doc_id, tokens)
    }

    pub(crate) fn add_document_with_token_buffer_deferred_prefix(
        &mut self,
        doc_id: DocId,
        text: &str,
        tokens: &mut Vec<Token>,
    ) -> Result<()> {
        tokens.clear();
        self.tokenize_filtered_into(text, tokens);
        self.inverted_index
            .add_document_deferred_prefix(doc_id, tokens)
    }

    /// Remove a document from the index.
    pub fn remove_document(&mut self, doc_id: DocId) -> bool {
        self.inverted_index.remove_document(doc_id)
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

    /// Search mode: compute ranked scores for matching documents.
    pub fn search(
        &self,
        query: &ParsedQuery,
        top_k: usize,
        filter_bitmap: Option<&RoaringBitmap>,
        scoring_stats: Option<&FullTextScoringStats>,
        score_mode: FullTextScoreMode,
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

        let local_stats;
        let stats = match scoring_stats {
            Some(stats) => stats,
            None => {
                local_stats = FullTextScoringStats::local_index(&self.inverted_index, &self.config);
                &local_stats
            }
        };
        if stats.global.total_docs == 0 || stats.global.avg_doc_length == 0.0 {
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
        let bm25 = stats.bm25();
        for doc_id in match_bitmap.iter() {
            let score = score_document_from_index(
                score_mode,
                &self.inverted_index,
                &bm25,
                query,
                doc_id as DocId,
                stats,
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

    fn tokenize_filtered_into(&self, text: &str, tokens: &mut Vec<Token>) {
        self.tokenizer.tokenize(text, tokens);
        if self.config.min_token_len != 1 || self.config.max_token_len.is_some() {
            tokens.retain(|token| self.is_token_len_valid(&token.term));
        }
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
        let results = index.search(&query, 2, None, None, FullTextScoreMode::Bm25);
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
        let results = index.search(&query, 2, None, None, FullTextScoreMode::Bm25);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].idx, 2);
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn test_cover_density_prefers_tighter_window() {
        let mut index = FullTextIndex::new_default();
        index.add_document(1, "alpha beta x x").unwrap();
        index.add_document(2, "alpha x beta x").unwrap();

        let query = index.parse_query("alpha beta").unwrap();
        let results = index.search(&query, 2, None, None, FullTextScoreMode::CoverDensity);
        assert_eq!(results.len(), 2);

        let mut scores = std::collections::HashMap::new();
        for point in results {
            scores.insert(point.idx, point.score);
        }
        assert!(scores[&1] > scores[&2]);
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

        let local_small = seg_small.search(&query_small, 1, None, None, FullTextScoreMode::Bm25);
        let local_large = seg_large.search(&query_large, 1, None, None, FullTextScoreMode::Bm25);
        assert_eq!(local_small.len(), 1);
        assert_eq!(local_large.len(), 1);
        assert_ne!(
            local_small[0].score, local_large[0].score,
            "Local per-segment stats should differ for asymmetric segments"
        );

        let mut term_doc_freqs = BTreeMap::new();
        term_doc_freqs.insert("vector".to_string(), 2);
        let global = FullTextScoringStats::with_term_doc_freqs(
            GlobalFullTextStats::from_totals(201, 201),
            term_doc_freqs,
        );
        let global_small = seg_small.search(
            &query_small,
            1,
            None,
            Some(&global),
            FullTextScoreMode::Bm25,
        );
        let global_large = seg_large.search(
            &query_large,
            1,
            None,
            Some(&global),
            FullTextScoreMode::Bm25,
        );
        assert_eq!(global_small.len(), 1);
        assert_eq!(global_large.len(), 1);

        let delta = (global_small[0].score - global_large[0].score).abs();
        assert!(
            delta < 1e-6,
            "Expected equal scores with global stats, delta={delta}"
        );
    }

    #[test]
    fn test_search_uses_generation_doc_freqs_instead_of_segment_local_df() {
        let mut rare_segment = FullTextIndex::new_default();
        rare_segment.add_document(1, "vector").unwrap();

        let mut common_segment = FullTextIndex::new_default();
        common_segment.add_document(1, "vector").unwrap();
        for i in 2..=50 {
            common_segment.add_document(i, "vector").unwrap();
        }

        let query = rare_segment.parse_query("vector").unwrap();
        let global_without_df =
            FullTextScoringStats::from_global_stats(GlobalFullTextStats::from_totals(51, 51));
        let rare_local_df = rare_segment.search(
            &query,
            1,
            None,
            Some(&global_without_df),
            FullTextScoreMode::Bm25,
        );
        let common_local_df = common_segment.search(
            &query,
            1,
            None,
            Some(&global_without_df),
            FullTextScoreMode::Bm25,
        );
        assert!(
            (rare_local_df[0].score - common_local_df[0].score).abs() > 1e-3,
            "local df fallback should expose asymmetric segment scoring"
        );

        let mut term_doc_freqs = BTreeMap::new();
        term_doc_freqs.insert("vector".to_string(), 51);
        let generation_stats = FullTextScoringStats::with_term_doc_freqs(
            GlobalFullTextStats::from_totals(51, 51),
            term_doc_freqs,
        );
        let rare_global_df = rare_segment.search(
            &query,
            1,
            None,
            Some(&generation_stats),
            FullTextScoreMode::Bm25,
        );
        let common_global_df = common_segment.search(
            &query,
            1,
            None,
            Some(&generation_stats),
            FullTextScoreMode::Bm25,
        );

        let delta = (rare_global_df[0].score - common_global_df[0].score).abs();
        assert!(
            delta < 1e-6,
            "expected unified generation df, delta={delta}"
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
