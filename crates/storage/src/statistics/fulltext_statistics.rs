// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Full-Text Index Statistics
//!
//! Statistics derived from a full-text inverted index.

use paro_common::error::{self as paro_error, Result};

use crate::index::fulltext::text_index::FullTextIndex;
use crate::index::fulltext::tokenizer::TokenizerKind;

/// Statistics for a full-text index.
#[derive(Debug, Clone)]
pub struct FullTextIndexStatistics {
    pub total_docs: u32,
    pub total_terms: u64,
    pub avg_doc_length: f32,
    pub unique_terms: u32,
    pub total_postings: u64,
    pub max_posting_list_len: u32,
    pub min_posting_list_len: u32,
    pub bm25_k1: f32,
    pub bm25_b: f32,
    pub tokenizer_kind: TokenizerKind,
}

impl FullTextIndexStatistics {
    pub const BYTE_LEN: usize = 4 + 8 + 4 + 4 + 8 + 4 + 4 + 4 + 4 + 1;

    pub fn collect(index: &FullTextIndex) -> Self {
        let inv = index.inverted_index();
        let postings = inv.postings();
        let mut total_postings = 0u64;
        let mut max_pl = 0u32;
        let mut min_pl = u32::MAX;
        for list in postings.values() {
            let len = list.len() as u32;
            total_postings += len as u64;
            max_pl = max_pl.max(len);
            min_pl = min_pl.min(len);
        }

        let config = index.config();
        Self {
            total_docs: inv.total_docs(),
            total_terms: inv.total_terms(),
            avg_doc_length: inv.avg_doc_length(),
            unique_terms: postings.len() as u32,
            total_postings,
            max_posting_list_len: max_pl,
            min_posting_list_len: if postings.is_empty() { 0 } else { min_pl },
            bm25_k1: config.bm25_k1,
            bm25_b: config.bm25_b,
            tokenizer_kind: index.tokenizer().kind(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + 8 + 4 + 4 + 8 + 4 + 4 + 4 + 4 + 1);
        buf.extend_from_slice(&self.total_docs.to_le_bytes());
        buf.extend_from_slice(&self.total_terms.to_le_bytes());
        buf.extend_from_slice(&self.avg_doc_length.to_le_bytes());
        buf.extend_from_slice(&self.unique_terms.to_le_bytes());
        buf.extend_from_slice(&self.total_postings.to_le_bytes());
        buf.extend_from_slice(&self.max_posting_list_len.to_le_bytes());
        buf.extend_from_slice(&self.min_posting_list_len.to_le_bytes());
        buf.extend_from_slice(&self.bm25_k1.to_le_bytes());
        buf.extend_from_slice(&self.bm25_b.to_le_bytes());
        buf.push(self.tokenizer_kind as u8);
        buf
    }

    /// Merge statistics using a merged full-text index as source of term/posting details.
    ///
    /// This follows compaction semantics:
    /// - total_docs/total_terms are summed from inputs
    /// - avg_doc_length is recomputed from summed totals
    /// - unique_terms/posting stats are re-collected from the merged index
    pub fn merge(left: &Self, right: &Self, merged_index: &FullTextIndex) -> Self {
        let mut merged = Self::collect(merged_index);
        let total_docs = left.total_docs.saturating_add(right.total_docs);
        let total_terms = left.total_terms.saturating_add(right.total_terms);
        merged.total_docs = total_docs;
        merged.total_terms = total_terms;
        merged.avg_doc_length = if total_docs == 0 {
            0.0
        } else {
            total_terms as f32 / total_docs as f32
        };
        merged
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::BYTE_LEN {
            return Err(paro_error::data_corrupted(
                "FullTextIndexStatistics: truncated",
            ));
        }
        let mut offset = 0;
        let read_u32 = |bytes: &[u8], offset: &mut usize| {
            let v = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
            *offset += 4;
            v
        };
        let read_u64 = |bytes: &[u8], offset: &mut usize| {
            let v = u64::from_le_bytes(bytes[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            v
        };

        let total_docs = read_u32(bytes, &mut offset);
        let total_terms = read_u64(bytes, &mut offset);
        if offset + 4 > bytes.len() {
            return Err(paro_error::data_corrupted(
                "FullTextIndexStatistics: truncated avg",
            ));
        }
        let avg_doc_length = f32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let unique_terms = read_u32(bytes, &mut offset);
        let total_postings = read_u64(bytes, &mut offset);
        let max_posting_list_len = read_u32(bytes, &mut offset);
        let min_posting_list_len = read_u32(bytes, &mut offset);
        if offset + 4 > bytes.len() {
            return Err(paro_error::data_corrupted(
                "FullTextIndexStatistics: truncated bm25_k1",
            ));
        }
        let bm25_k1 = f32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if offset + 4 > bytes.len() {
            return Err(paro_error::data_corrupted(
                "FullTextIndexStatistics: truncated bm25_b",
            ));
        }
        let bm25_b = f32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if offset >= bytes.len() {
            return Err(paro_error::data_corrupted(
                "FullTextIndexStatistics: truncated tokenizer",
            ));
        }
        let tokenizer_kind = TokenizerKind::from_id(bytes[offset]).map_err(|_| {
            paro_error::data_corrupted("FullTextIndexStatistics: invalid tokenizer kind")
        })?;

        Ok(Self {
            total_docs,
            total_terms,
            avg_doc_length,
            unique_terms,
            total_postings,
            max_posting_list_len,
            min_posting_list_len,
            bm25_k1,
            bm25_b,
            tokenizer_kind,
        })
    }
}
