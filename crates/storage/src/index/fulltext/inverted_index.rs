// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Full-Text Inverted Index
//!
//! Maps terms to posting lists and tracks document statistics.
//!
//! Prefix lookup is accelerated by a trie-backed aggregated bitmap, avoiding
//! repeated BTree range scans on hot `:*` queries.

use std::collections::{BTreeMap, HashMap};

use paro_common::error::{self as paro_error, Result};
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use super::posting_list::{DocId, PostingList};
use super::tokenizer::Token;

/// Term type for full-text index.
pub type Term = String;

#[derive(Debug, Default, Clone)]
struct PrefixTrieNode {
    children: HashMap<char, PrefixTrieNode>,
    doc_ids: RoaringBitmap,
}

#[derive(Debug, Default, Clone)]
struct PrefixTrie {
    root: PrefixTrieNode,
}

impl PrefixTrie {
    fn insert(&mut self, term: &str, doc_id: DocId) {
        self.root.doc_ids.insert(doc_id);
        let mut node = &mut self.root;
        for ch in term.chars() {
            node = node.children.entry(ch).or_default();
            node.doc_ids.insert(doc_id);
        }
    }

    fn docs_for_prefix(&self, prefix: &str) -> RoaringBitmap {
        let mut node = &self.root;
        for ch in prefix.chars() {
            let Some(child) = node.children.get(&ch) else {
                return RoaringBitmap::new();
            };
            node = child;
        }
        node.doc_ids.clone()
    }
}

/// Inverted index for full-text search.
#[derive(Debug, Default, Clone)]
pub struct InvertedIndex {
    postings: BTreeMap<Term, PostingList>,
    prefix_trie: PrefixTrie,
    doc_lengths: HashMap<DocId, u32>,
    total_docs: u32,
    total_terms: u64,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a document to the index.
    pub fn add_document(&mut self, doc_id: DocId, tokens: &[Token]) -> Result<()> {
        self.add_document_internal(doc_id, tokens, true)
    }

    pub(crate) fn add_document_deferred_prefix(
        &mut self,
        doc_id: DocId,
        tokens: &[Token],
    ) -> Result<()> {
        self.add_document_internal(doc_id, tokens, false)
    }

    fn add_document_internal(
        &mut self,
        doc_id: DocId,
        tokens: &[Token],
        update_prefix_trie: bool,
    ) -> Result<()> {
        if self.doc_lengths.contains_key(&doc_id) {
            return Err(paro_error::invalid_input(
                "FullTextInvertedIndex: duplicate doc_id",
            ));
        }

        let mut doc_len = 0u32;
        let mut unique_terms = SmallVec::<[&str; 16]>::new();
        for token in tokens {
            let list = self.postings.entry(token.term.clone()).or_default();
            list.add_position(doc_id, token.position)?;
            doc_len += 1;
            let term = token.term.as_str();
            if !unique_terms.contains(&term) {
                unique_terms.push(term);
            }
        }
        if update_prefix_trie {
            for term in unique_terms {
                self.prefix_trie.insert(term, doc_id);
            }
        }

        self.doc_lengths.insert(doc_id, doc_len);
        self.total_docs += 1;
        self.total_terms += doc_len as u64;
        Ok(())
    }

    /// Remove a document from the index.
    pub fn remove_document(&mut self, doc_id: DocId) -> bool {
        let Some(doc_len) = self.doc_lengths.remove(&doc_id) else {
            return false;
        };

        self.total_docs = self.total_docs.saturating_sub(1);
        self.total_terms = self.total_terms.saturating_sub(doc_len as u64);

        let mut empty_terms = Vec::new();
        for (term, list) in self.postings.iter_mut() {
            list.remove(doc_id);
            if list.is_empty() {
                empty_terms.push(term.clone());
            }
        }
        for term in empty_terms {
            self.postings.remove(&term);
        }
        self.rebuild_prefix_trie();
        true
    }

    pub fn total_docs(&self) -> u32 {
        self.total_docs
    }

    pub fn total_terms(&self) -> u64 {
        self.total_terms
    }

    pub fn avg_doc_length(&self) -> f32 {
        if self.total_docs == 0 {
            0.0
        } else {
            self.total_terms as f32 / self.total_docs as f32
        }
    }

    pub fn doc_length(&self, doc_id: DocId) -> Option<u32> {
        self.doc_lengths.get(&doc_id).copied()
    }

    pub fn get_posting_list(&self, term: &str) -> Option<&PostingList> {
        self.postings.get(term)
    }

    pub fn postings(&self) -> &BTreeMap<Term, PostingList> {
        &self.postings
    }

    pub fn doc_lengths(&self) -> &HashMap<DocId, u32> {
        &self.doc_lengths
    }

    pub fn all_doc_ids(&self) -> RoaringBitmap {
        RoaringBitmap::from_iter(self.doc_lengths.keys().copied())
    }

    pub fn prefix_doc_ids(&self, prefix: &str) -> RoaringBitmap {
        self.prefix_trie.docs_for_prefix(prefix)
    }

    pub fn from_parts(
        postings: BTreeMap<Term, PostingList>,
        doc_lengths: HashMap<DocId, u32>,
    ) -> Self {
        let total_docs = doc_lengths.len() as u32;
        let total_terms = doc_lengths.values().map(|&v| v as u64).sum();
        let mut index = Self {
            postings,
            prefix_trie: PrefixTrie::default(),
            doc_lengths,
            total_docs,
            total_terms,
        };
        index.rebuild_prefix_trie();
        index
    }

    pub(crate) fn rebuild_prefix_trie(&mut self) {
        let mut trie = PrefixTrie::default();
        for (term, list) in &self.postings {
            for elem in list.iter() {
                trie.insert(term, elem.doc_id);
            }
        }
        self.prefix_trie = trie;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::fulltext::tokenizer::{DefaultTokenizer, Tokenizer};
    use roaring::RoaringBitmap;

    #[test]
    fn inverted_index_add_and_stats() {
        let tokenizer = DefaultTokenizer::new();
        let mut tokens = Vec::new();
        tokenizer.tokenize("hello world hello", &mut tokens);

        let mut index = InvertedIndex::new();
        index.add_document(1, &tokens).unwrap();

        assert_eq!(index.total_docs(), 1);
        assert_eq!(index.total_terms(), 3);
        assert_eq!(index.avg_doc_length(), 3.0);

        let list = index.get_posting_list("hello").unwrap();
        assert_eq!(list.len(), 1);
        let elem = list.get(1).unwrap();
        assert_eq!(elem.term_frequency, 2);
    }

    #[test]
    fn inverted_index_remove_document() {
        let tokenizer = DefaultTokenizer::new();
        let mut tokens = Vec::new();
        tokenizer.tokenize("a b", &mut tokens);

        let mut index = InvertedIndex::new();
        index.add_document(1, &tokens).unwrap();
        assert!(index.remove_document(1));
        assert_eq!(index.total_docs(), 0);
        assert!(index.get_posting_list("a").is_none());
    }

    #[test]
    fn inverted_index_all_doc_ids_and_prefix_trie_lookup() {
        let tokenizer = DefaultTokenizer::new();
        let mut index = InvertedIndex::new();

        let mut tokens = Vec::new();
        tokenizer.tokenize("apple banana", &mut tokens);
        index.add_document(1, &tokens).unwrap();

        tokens.clear();
        tokenizer.tokenize("apply carrot", &mut tokens);
        index.add_document(3, &tokens).unwrap();

        tokens.clear();
        tokenizer.tokenize("app delta", &mut tokens);
        index.add_document(7, &tokens).unwrap();

        let all_doc_ids = index.all_doc_ids();
        assert_eq!(all_doc_ids, RoaringBitmap::from_iter([1u32, 3, 7]));

        let prefix_docs = index.prefix_doc_ids("app");
        assert_eq!(prefix_docs, RoaringBitmap::from_iter([1u32, 3, 7]));

        assert!(index.remove_document(3));
        let prefix_docs_after_remove = index.prefix_doc_ids("app");
        assert_eq!(
            prefix_docs_after_remove,
            RoaringBitmap::from_iter([1u32, 7])
        );
    }
}
