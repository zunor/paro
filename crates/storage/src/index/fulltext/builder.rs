//! # Full-Text Index Builder
//!
//! Builds a full-text index from documents.

use paro_common::error::{self as paro_error, Result};

use super::posting_list::DocId;
use super::text_index::{FullTextIndex, FullTextIndexConfig};
use super::tokenizer::{DefaultTokenizer, Tokenizer};

/// Builder for FullTextIndex.
pub struct FullTextIndexBuilder {
    index: FullTextIndex,
    next_doc_id: DocId,
}

impl FullTextIndexBuilder {
    pub fn new() -> Self {
        Self {
            index: FullTextIndex::new_default(),
            next_doc_id: 0,
        }
    }

    pub fn with_config(config: FullTextIndexConfig) -> Self {
        Self {
            index: FullTextIndex::new(Box::new(DefaultTokenizer::new()), config),
            next_doc_id: 0,
        }
    }

    pub fn with_tokenizer(tokenizer: Box<dyn Tokenizer>, config: FullTextIndexConfig) -> Self {
        Self {
            index: FullTextIndex::new(tokenizer, config),
            next_doc_id: 0,
        }
    }

    /// Add a document with an explicit ID.
    pub fn add(&mut self, doc_id: DocId, text: &str) -> Result<()> {
        self.index.add_document(doc_id, text)?;
        let next = doc_id
            .checked_add(1)
            .ok_or_else(|| paro_error::out_of_range("doc_id exceeds u32 range"))?;
        if next > self.next_doc_id {
            self.next_doc_id = next;
        }
        Ok(())
    }

    /// Add a document with the next sequential ID.
    pub fn push(&mut self, text: &str) -> Result<DocId> {
        let doc_id = self.next_doc_id;
        self.add(doc_id, text)?;
        Ok(doc_id)
    }

    /// Add a batch of documents starting at a given ID.
    pub fn add_batch<T: AsRef<str>>(&mut self, start_doc_id: DocId, texts: &[T]) -> Result<()> {
        let mut doc_id = start_doc_id;
        for text in texts {
            self.index.add_document(doc_id, text.as_ref())?;
            doc_id = doc_id
                .checked_add(1)
                .ok_or_else(|| paro_error::out_of_range("doc_id exceeds u32 range"))?;
        }
        if doc_id > self.next_doc_id {
            self.next_doc_id = doc_id;
        }
        Ok(())
    }

    /// Finish building and return the index.
    pub fn build(self) -> FullTextIndex {
        self.index
    }
}

impl Default for FullTextIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}
