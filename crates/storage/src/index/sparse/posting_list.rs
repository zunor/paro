// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Sparse Posting List
//!
//! Posting list for sparse vector inverted index.
//! Stores (doc_id, weight) pairs sorted by doc_id and supports (de)serialization.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use paro_common::error::{self as paro_error, Result};

/// Document ID type for posting lists.
pub type DocId = u32;

/// Weight type for posting lists.
pub type Weight = f32;

const MAGIC: &[u8; 4] = b"SPL1";

/// Single posting element.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PostingElement {
    pub doc_id: DocId,
    pub weight: Weight,
}

impl PostingElement {
    pub fn new(doc_id: DocId, weight: Weight) -> Self {
        Self { doc_id, weight }
    }
}

/// Posting list ordered by doc_id.
#[derive(Debug, Clone, Default)]
pub struct PostingList {
    elements: Vec<PostingElement>,
}

impl PostingList {
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            elements: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    pub fn elements(&self) -> &[PostingElement] {
        &self.elements
    }

    pub fn last_doc_id(&self) -> Option<DocId> {
        self.elements.last().map(|element| element.doc_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &PostingElement> {
        self.elements.iter()
    }

    /// Returns true if doc_ids are strictly increasing.
    pub fn is_sorted(&self) -> bool {
        self.elements.windows(2).all(|w| w[0].doc_id < w[1].doc_id)
    }

    /// Sort by doc_id and validate uniqueness.
    pub fn sort_by_doc_id(&mut self) -> Result<()> {
        if self.elements.len() <= 1 {
            return Ok(());
        }
        self.elements.sort_unstable_by_key(|e| e.doc_id);
        if self.elements.windows(2).any(|w| w[0].doc_id == w[1].doc_id) {
            return Err(paro_error::invalid_input("PostingList: duplicate doc_id"));
        }
        Ok(())
    }

    /// Append an element when doc_id is strictly increasing.
    pub fn push_sorted(&mut self, element: PostingElement) -> Result<()> {
        if let Some(last) = self.elements.last() {
            if element.doc_id <= last.doc_id {
                return Err(paro_error::invalid_input(
                    "PostingList: doc_id must be strictly increasing",
                ));
            }
        }
        self.elements.push(element);
        Ok(())
    }

    /// Upsert an element (keeps list sorted).
    pub fn upsert(&mut self, doc_id: DocId, weight: Weight) {
        debug_assert!(self.is_sorted());
        match self.elements.binary_search_by_key(&doc_id, |e| e.doc_id) {
            Ok(idx) => {
                self.elements[idx].weight = weight;
            }
            Err(insert_idx) => {
                self.elements
                    .insert(insert_idx, PostingElement { doc_id, weight });
            }
        }
    }

    /// Delete an element by doc_id. Returns true if found.
    pub fn delete(&mut self, doc_id: DocId) -> bool {
        debug_assert!(self.is_sorted());
        match self.elements.binary_search_by_key(&doc_id, |e| e.doc_id) {
            Ok(idx) => {
                self.elements.remove(idx);
                true
            }
            Err(_) => false,
        }
    }

    /// Serialize to bytes.
    ///
    /// Format:
    /// ```text
    /// magic(4) | num_elements(4) | [doc_id(4) | weight(4)] * num_elements
    /// ```
    pub fn to_bytes(&self) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(self.serialized_len());
        self.write_to(&mut buf)?;
        Ok(buf.freeze())
    }

    pub(crate) fn serialized_len(&self) -> usize {
        8 + self.elements.len() * 8
    }

    pub(crate) fn write_to(&self, buf: &mut BytesMut) -> Result<()> {
        if !self.is_sorted() {
            return Err(paro_error::invalid_input(
                "PostingList: doc_ids must be sorted before serialization",
            ));
        }

        buf.extend_from_slice(MAGIC);
        buf.put_u32_le(self.elements.len() as u32);
        for elem in &self.elements {
            buf.put_u32_le(elem.doc_id);
            buf.put_f32_le(elem.weight);
        }
        Ok(())
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut buf = data;
        if buf.remaining() < 8 {
            return Err(paro_error::data_corrupted("PostingList: data too small"));
        }

        let mut magic = [0u8; 4];
        buf.copy_to_slice(&mut magic);
        if &magic != MAGIC {
            return Err(paro_error::data_corrupted("PostingList: invalid magic"));
        }

        let num_elements = buf.get_u32_le() as usize;
        if buf.remaining() < num_elements * 8 {
            return Err(paro_error::data_corrupted(
                "PostingList: truncated elements",
            ));
        }

        let mut elements = Vec::with_capacity(num_elements);
        for _ in 0..num_elements {
            let doc_id = buf.get_u32_le();
            let weight = buf.get_f32_le();
            elements.push(PostingElement { doc_id, weight });
        }

        if elements.windows(2).any(|w| w[0].doc_id >= w[1].doc_id) {
            return Err(paro_error::data_corrupted(
                "PostingList: doc_ids not strictly increasing",
            ));
        }

        Ok(Self { elements })
    }
}
