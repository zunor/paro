// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Full-Text Posting List
//!
//! Stores per-term postings with document IDs, term frequencies, and positions.
//! Uses delta + varint encoding for compact serialization.

use bytes::Bytes;
use paro_common::error::{self as paro_error, Result};

use super::tokenizer::TokenPosition;

/// Document ID type for full-text posting lists.
pub type DocId = u32;

const MAGIC: &[u8; 4] = b"FTPL";
const VERSION: u8 = 1;

/// Posting element for a single document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostingElement {
    pub doc_id: DocId,
    pub term_frequency: u32,
    pub positions: Vec<TokenPosition>,
}

impl PostingElement {
    pub fn with_position(doc_id: DocId, position: TokenPosition) -> Self {
        Self {
            doc_id,
            term_frequency: 1,
            positions: vec![position],
        }
    }

    pub fn add_position(&mut self, position: TokenPosition) -> Result<()> {
        if let Some(last) = self.positions.last() {
            if position <= *last {
                return Err(paro_error::invalid_input(
                    "FullTextPostingList: positions must be strictly increasing",
                ));
            }
        }
        self.positions.push(position);
        self.term_frequency += 1;
        Ok(())
    }

    pub fn is_positions_sorted(&self) -> bool {
        self.positions.windows(2).all(|w| w[0] < w[1])
    }
}

/// Posting list ordered by doc_id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PostingList {
    elements: Vec<PostingElement>,
}

impl PostingList {
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
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

    pub fn iter(&self) -> impl Iterator<Item = &PostingElement> {
        self.elements.iter()
    }

    pub fn get(&self, doc_id: DocId) -> Option<&PostingElement> {
        self.elements
            .binary_search_by_key(&doc_id, |e| e.doc_id)
            .ok()
            .map(|idx| &self.elements[idx])
    }

    /// Add a position for a document, keeping doc_id order.
    pub fn add_position(&mut self, doc_id: DocId, position: TokenPosition) -> Result<()> {
        match self.elements.binary_search_by_key(&doc_id, |e| e.doc_id) {
            Ok(idx) => self.elements[idx].add_position(position),
            Err(insert_idx) => {
                self.elements
                    .insert(insert_idx, PostingElement::with_position(doc_id, position));
                Ok(())
            }
        }
    }

    /// Remove a document from the list. Returns true if removed.
    pub fn remove(&mut self, doc_id: DocId) -> bool {
        match self.elements.binary_search_by_key(&doc_id, |e| e.doc_id) {
            Ok(idx) => {
                self.elements.remove(idx);
                true
            }
            Err(_) => false,
        }
    }

    pub fn is_sorted(&self) -> bool {
        self.elements.windows(2).all(|w| w[0].doc_id < w[1].doc_id)
            && self.elements.iter().all(|e| e.is_positions_sorted())
    }

    /// Serialize to bytes with delta + varint encoding.
    ///
    /// Format:
    /// ```text
    /// magic(4) | version(1) | num_elements(varint)
    ///   [doc_id_delta(varint) | tf(varint) | positions_delta(varint)*] * num_elements
    /// ```
    pub fn to_bytes(&self) -> Result<Bytes> {
        if !self.is_sorted() {
            return Err(paro_error::invalid_input(
                "FullTextPostingList: postings must be sorted before serialization",
            ));
        }

        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.push(VERSION);
        encode_varint(self.elements.len() as u32, &mut buf);

        let mut prev_doc = 0u32;
        for (i, elem) in self.elements.iter().enumerate() {
            if elem.term_frequency as usize != elem.positions.len() {
                return Err(paro_error::invalid_input(
                    "FullTextPostingList: term_frequency mismatch",
                ));
            }
            if i > 0 && elem.doc_id <= prev_doc {
                return Err(paro_error::invalid_input(
                    "FullTextPostingList: doc_ids must be strictly increasing",
                ));
            }
            let doc_delta = if i == 0 {
                elem.doc_id
            } else {
                elem.doc_id - prev_doc
            };
            encode_varint(doc_delta, &mut buf);
            encode_varint(elem.term_frequency, &mut buf);

            let mut prev_pos = 0u32;
            for (j, &pos) in elem.positions.iter().enumerate() {
                if j > 0 && pos <= prev_pos {
                    return Err(paro_error::invalid_input(
                        "FullTextPostingList: positions must be strictly increasing",
                    ));
                }
                let delta = if j == 0 { pos } else { pos - prev_pos };
                encode_varint(delta, &mut buf);
                prev_pos = pos;
            }
            prev_doc = elem.doc_id;
        }

        Ok(Bytes::from(buf))
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 5 {
            return Err(paro_error::data_corrupted(
                "FullTextPostingList: data too small",
            ));
        }
        if &data[..4] != MAGIC {
            return Err(paro_error::data_corrupted(
                "FullTextPostingList: invalid magic",
            ));
        }
        if data[4] != VERSION {
            return Err(paro_error::data_corrupted(
                "FullTextPostingList: unsupported version",
            ));
        }

        let mut offset = 5usize;
        let num_elements = decode_varint(data, &mut offset)? as usize;
        let mut elements = Vec::with_capacity(num_elements);

        let mut prev_doc = 0u32;
        for i in 0..num_elements {
            let doc_delta = decode_varint(data, &mut offset)?;
            let doc_id = if i == 0 {
                doc_delta
            } else {
                prev_doc + doc_delta
            };
            if i > 0 && doc_id <= prev_doc {
                return Err(paro_error::data_corrupted(
                    "FullTextPostingList: non-increasing doc_id",
                ));
            }
            let tf = decode_varint(data, &mut offset)?;
            let mut positions = Vec::with_capacity(tf as usize);
            let mut prev_pos = 0u32;
            for j in 0..tf {
                let delta = decode_varint(data, &mut offset)?;
                let pos = if j == 0 { delta } else { prev_pos + delta };
                if j > 0 && pos <= prev_pos {
                    return Err(paro_error::data_corrupted(
                        "FullTextPostingList: non-increasing positions",
                    ));
                }
                positions.push(pos);
                prev_pos = pos;
            }
            elements.push(PostingElement {
                doc_id,
                term_frequency: tf,
                positions,
            });
            prev_doc = doc_id;
        }

        Ok(Self { elements })
    }
}

fn encode_varint(mut value: u32, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_varint(data: &[u8], offset: &mut usize) -> Result<u32> {
    let mut shift = 0u32;
    let mut result = 0u32;
    loop {
        if *offset >= data.len() {
            return Err(paro_error::data_corrupted(
                "FullTextPostingList: unexpected end of varint",
            ));
        }
        let byte = data[*offset];
        *offset += 1;
        result |= ((byte & 0x7F) as u32) << shift;
        if (byte & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift > 28 {
            return Err(paro_error::data_corrupted(
                "FullTextPostingList: varint overflow",
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn posting_list_add_and_get() {
        let mut list = PostingList::new();
        list.add_position(2, 1).unwrap();
        list.add_position(1, 0).unwrap();
        list.add_position(2, 3).unwrap();

        let e1 = list.get(1).unwrap();
        assert_eq!(e1.term_frequency, 1);
        assert_eq!(e1.positions, vec![0]);

        let e2 = list.get(2).unwrap();
        assert_eq!(e2.term_frequency, 2);
        assert_eq!(e2.positions, vec![1, 3]);
    }

    #[test]
    fn posting_list_roundtrip() {
        let mut list = PostingList::new();
        list.add_position(1, 0).unwrap();
        list.add_position(1, 2).unwrap();
        list.add_position(3, 1).unwrap();

        let bytes = list.to_bytes().unwrap();
        let restored = PostingList::from_bytes(&bytes).unwrap();
        assert_eq!(list, restored);
    }
}
