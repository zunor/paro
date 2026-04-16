// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Full-Text Index Persistence
//!
//! Serialization and deserialization for full-text index.

use bytes::{Buf, BufMut, BytesMut};
use paro_common::error::{self as paro_error, Result};
use std::collections::BTreeMap;

use crate::statistics::{append_stats_trailer, FullTextIndexStatistics};

use super::inverted_index::InvertedIndex;
use super::posting_list::{DocId, PostingList};
use super::text_index::{FullTextIndex, FullTextIndexConfig};
use super::tokenizer::{tokenizer_from_kind, TokenizerKind};

const MAGIC: &[u8; 4] = b"FTX1";
const VERSION: u32 = 1;

impl FullTextIndex {
    /// Serialize the full-text index into bytes.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(MAGIC);
        buf.put_u32_le(VERSION);
        buf.put_u8(self.tokenizer().kind() as u8);

        let config = self.config();
        buf.put_u32_le(
            u32::try_from(config.min_token_len)
                .map_err(|_| paro_error::out_of_range("min_token_len exceeds u32 range"))?,
        );
        buf.put_u32_le(
            config
                .max_token_len
                .map(|v| {
                    u32::try_from(v)
                        .map_err(|_| paro_error::out_of_range("max_token_len exceeds u32 range"))
                })
                .transpose()? // Option<Result<u32>> -> Result<Option<u32>>
                .unwrap_or(0),
        );
        buf.put_f32_le(config.bm25_k1);
        buf.put_f32_le(config.bm25_b);

        let doc_lengths = self.inverted_index().doc_lengths();
        let doc_len_count = u32::try_from(doc_lengths.len())
            .map_err(|_| paro_error::out_of_range("doc_lengths too large"))?;
        buf.put_u32_le(doc_len_count);
        let mut doc_entries: Vec<(DocId, u32)> =
            doc_lengths.iter().map(|(&k, &v)| (k, v)).collect();
        doc_entries.sort_unstable_by_key(|(k, _)| *k);
        for (doc_id, len) in doc_entries {
            buf.put_u32_le(doc_id);
            buf.put_u32_le(len);
        }

        let postings = self.inverted_index().postings();
        let postings_count = u32::try_from(postings.len())
            .map_err(|_| paro_error::out_of_range("postings too large"))?;
        buf.put_u32_le(postings_count);
        for (term, list) in postings {
            let term_bytes = term.as_bytes();
            let term_len = u32::try_from(term_bytes.len())
                .map_err(|_| paro_error::out_of_range("term too large"))?;
            buf.put_u32_le(term_len);
            buf.extend_from_slice(term_bytes);

            let list_bytes = list.to_bytes()?;
            let list_len = u32::try_from(list_bytes.len())
                .map_err(|_| paro_error::out_of_range("posting list too large"))?;
            buf.put_u32_le(list_len);
            buf.extend_from_slice(&list_bytes);
        }

        let stats = FullTextIndexStatistics::collect(self);
        let mut out = buf.to_vec();
        append_stats_trailer(&mut out, &stats.to_bytes())?;
        Ok(out)
    }

    /// Deserialize a full-text index from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let mut buf = data;
        if buf.remaining() < 9 {
            return Err(paro_error::data_corrupted("FullTextIndex: data too small"));
        }

        let mut magic = [0u8; 4];
        buf.copy_to_slice(&mut magic);
        if &magic != MAGIC {
            return Err(paro_error::data_corrupted("FullTextIndex: invalid magic"));
        }

        let version = buf.get_u32_le();
        if version != VERSION {
            return Err(paro_error::not_supported(format!(
                "FullTextIndex: unsupported version {}",
                version
            )));
        }

        let tokenizer_id = buf.get_u8();
        let tokenizer_kind = TokenizerKind::from_id(tokenizer_id)
            .map_err(|_| paro_error::not_supported("FullTextIndex: unsupported tokenizer"))?;

        if buf.remaining() < 16 {
            return Err(paro_error::data_corrupted(
                "FullTextIndex: truncated config",
            ));
        }
        let min_token_len = buf.get_u32_le() as usize;
        let max_token_len_raw = buf.get_u32_le();
        let max_token_len = if max_token_len_raw == 0 {
            None
        } else {
            Some(max_token_len_raw as usize)
        };
        let bm25_k1 = buf.get_f32_le();
        let bm25_b = buf.get_f32_le();

        if buf.remaining() < 4 {
            return Err(paro_error::data_corrupted(
                "FullTextIndex: truncated doc lengths header",
            ));
        }
        let doc_len_count = buf.get_u32_le() as usize;
        if buf.remaining() < doc_len_count * 8 {
            return Err(paro_error::data_corrupted(
                "FullTextIndex: truncated doc lengths",
            ));
        }
        let mut doc_lengths = std::collections::HashMap::with_capacity(doc_len_count);
        for _ in 0..doc_len_count {
            let doc_id = buf.get_u32_le();
            let len = buf.get_u32_le();
            doc_lengths.insert(doc_id, len);
        }

        if buf.remaining() < 4 {
            return Err(paro_error::data_corrupted(
                "FullTextIndex: truncated postings header",
            ));
        }
        let postings_len = buf.get_u32_le() as usize;
        let mut postings = BTreeMap::new();
        for _ in 0..postings_len {
            if buf.remaining() < 4 {
                return Err(paro_error::data_corrupted(
                    "FullTextIndex: truncated term length",
                ));
            }
            let term_len = buf.get_u32_le() as usize;
            if buf.remaining() < term_len {
                return Err(paro_error::data_corrupted(
                    "FullTextIndex: truncated term bytes",
                ));
            }
            let term = std::str::from_utf8(&buf[..term_len])
                .map_err(|_| paro_error::data_corrupted("FullTextIndex: invalid term utf8"))?
                .to_string();
            buf.advance(term_len);

            if buf.remaining() < 4 {
                return Err(paro_error::data_corrupted(
                    "FullTextIndex: truncated posting list length",
                ));
            }
            let list_len = buf.get_u32_le() as usize;
            if buf.remaining() < list_len {
                return Err(paro_error::data_corrupted(
                    "FullTextIndex: truncated posting list body",
                ));
            }
            let list_bytes = &buf[..list_len];
            let list = PostingList::from_bytes(list_bytes)?;
            postings.insert(term, list);
            buf.advance(list_len);
        }

        let inverted_index = InvertedIndex::from_parts(postings, doc_lengths);
        let config = FullTextIndexConfig {
            min_token_len,
            max_token_len,
            bm25_k1,
            bm25_b,
        };
        Ok(FullTextIndex::from_parts(
            tokenizer_from_kind(tokenizer_kind),
            config,
            inverted_index,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::fulltext::scoring::FullTextScoreMode;

    #[test]
    fn fulltext_index_roundtrip() {
        let mut index = FullTextIndex::new_default();
        index.add_document(0, "hello world").unwrap();
        index.add_document(1, "hello vector world").unwrap();

        let bytes = index.serialize().unwrap();
        let restored = FullTextIndex::deserialize(&bytes).unwrap();

        let query = restored.parse_query("hello").unwrap();
        let results = restored.search(&query, 10, None, None, FullTextScoreMode::Bm25);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn fulltext_index_roundtrip_preserves_tokenizer_kind() {
        let mut index = FullTextIndex::new_with_tokenizer_kind(
            TokenizerKind::Chinese,
            FullTextIndexConfig::default(),
        );
        index.add_document(0, "向量数据库").unwrap();

        let bytes = index.serialize().unwrap();
        let restored = FullTextIndex::deserialize(&bytes).unwrap();

        assert_eq!(restored.tokenizer().kind(), TokenizerKind::Chinese);
        let query = restored.parse_query("数据库").unwrap();
        let results = restored.search(&query, 10, None, None, FullTextScoreMode::Bm25);
        assert_eq!(results.len(), 1);
    }
}
