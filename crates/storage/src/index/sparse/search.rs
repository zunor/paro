// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Sparse Search
//!
//! Search context for sparse vector inverted index.

use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::index::hnsw::{FixedLengthPriorityQueue, PointOffset, ScoredPoint};
use crate::rowset::sparse_vector::SparseVector;

use super::inverted_index::InvertedIndex;
use super::posting_list::PostingList;

/// Sparse search configuration.
#[derive(Debug, Clone, Copy)]
pub struct SparseSearchConfig {
    /// Selectivity threshold (0..=1). Below this, prefer pre-filter (plain) search.
    pub full_scan_threshold: f64,
}

impl Default for SparseSearchConfig {
    fn default() -> Self {
        // 10% selectivity is a common cutoff for pre-filtering.
        SparseSearchConfig {
            full_scan_threshold: 0.1,
        }
    }
}

impl SparseSearchConfig {
    pub fn new(full_scan_threshold: f64) -> Self {
        SparseSearchConfig {
            full_scan_threshold,
        }
    }
}

/// Search context for sparse vectors.
pub struct SparseSearchContext<'a> {
    query: SparseVector,
    top_k: usize,
    inverted_index: &'a InvertedIndex,
}

impl<'a> SparseSearchContext<'a> {
    pub fn new(query: SparseVector, top_k: usize, inverted_index: &'a InvertedIndex) -> Self {
        Self {
            query,
            top_k,
            inverted_index,
        }
    }

    /// Post-filtering search: traverse posting lists and apply filter on-the-fly.
    pub fn search<F>(&self, filter_fn: F) -> Vec<ScoredPoint>
    where
        F: Fn(PointOffset) -> bool,
    {
        if self.top_k == 0 || self.query.is_empty() {
            return Vec::new();
        }

        let mut scores: HashMap<PointOffset, f32> = HashMap::new();

        for (i, dim) in self.query.dims.iter().copied().enumerate() {
            let Some(list) = self.inverted_index.get_posting_list(dim) else {
                continue;
            };
            let q_weight = self.query.weights[i];
            for elem in list.elements() {
                let doc_id = elem.doc_id as PointOffset;
                if !filter_fn(doc_id) {
                    continue;
                }
                *scores.entry(doc_id).or_insert(0.0) += elem.weight * q_weight;
            }
        }

        let mut topk = FixedLengthPriorityQueue::new(self.top_k);
        for (doc_id, score) in scores {
            if score != 0.0 {
                topk.push(ScoredPoint { idx: doc_id, score });
            }
        }
        topk.into_sorted_vec()
    }

    /// Pre-filtering search: compute scores only for candidate IDs.
    pub fn plain_search(&self, ids: &[PointOffset]) -> Vec<ScoredPoint> {
        if self.top_k == 0 || self.query.is_empty() || ids.is_empty() {
            return Vec::new();
        }

        let mut topk = FixedLengthPriorityQueue::new(self.top_k);

        for &doc_id in ids {
            let mut score = 0.0f32;
            let mut matched = false;
            for (i, dim) in self.query.dims.iter().copied().enumerate() {
                let Some(list) = self.inverted_index.get_posting_list(dim) else {
                    continue;
                };
                if let Some(weight) = weight_for_doc(list, doc_id) {
                    matched = true;
                    score += weight * self.query.weights[i];
                }
            }
            if matched {
                topk.push(ScoredPoint { idx: doc_id, score });
            }
        }

        topk.into_sorted_vec()
    }
}

/// Helper to extract weight for a given doc_id from a posting list.
fn weight_for_doc(list: &PostingList, doc_id: PointOffset) -> Option<f32> {
    let elements = list.elements();
    match elements.binary_search_by_key(&doc_id, |e| e.doc_id) {
        Ok(idx) => Some(elements[idx].weight),
        Err(_) => None,
    }
}

/// Decide whether to use pre-filter (plain) search based on selectivity.
pub fn should_plain_search(
    config: SparseSearchConfig,
    num_vectors: usize,
    filter_bitmap: &RoaringBitmap,
) -> bool {
    if num_vectors == 0 {
        return true;
    }
    let selectivity = filter_bitmap.len() as f64 / num_vectors as f64;
    selectivity < config.full_scan_threshold
}
