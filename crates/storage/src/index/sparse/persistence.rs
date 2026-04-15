// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Sparse Index Persistence
//!
//! Serialization and deserialization for sparse vector index.

use bytes::{Buf, BufMut, BytesMut};
use paro_common::error::{self as paro_error, Result};

use crate::statistics::{append_stats_trailer, SparseIndexStatistics};

use super::inverted_index::InvertedIndex;
use super::posting_list::PostingList;
use super::sparse_index::{IndicesTracker, SparseVectorIndex};
use super::SparseSearchConfig;
use crate::rowset::sparse_vector::DimensionId;

const MAGIC: &[u8; 4] = b"SPX1";
const VERSION: u32 = 1;

impl SparseVectorIndex {
    /// Serialize the sparse index into bytes.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(MAGIC);
        buf.put_u32_le(VERSION);
        buf.put_f64_le(self.config().full_scan_threshold);

        let num_vectors = u32::try_from(self.num_vectors())
            .map_err(|_| paro_error::out_of_range("num_vectors exceeds u32 range"))?;
        buf.put_u32_le(num_vectors);

        let map = self.indices().map();
        buf.put_u32_le(map.len() as u32);
        let mut map_entries: Vec<(DimensionId, DimensionId)> =
            map.iter().map(|(k, v)| (*k, *v)).collect();
        map_entries.sort_unstable_by_key(|(k, _)| *k);
        for (external, internal) in map_entries {
            buf.put_u32_le(external);
            buf.put_u32_le(internal);
        }

        let postings = self.inverted_index().postings();
        buf.put_u32_le(postings.len() as u32);
        let mut posting_entries: Vec<(DimensionId, &PostingList)> =
            postings.iter().map(|(k, v)| (*k, v)).collect();
        posting_entries.sort_unstable_by_key(|(k, _)| *k);
        for (dim_id, list) in posting_entries {
            let list_bytes = list.to_bytes()?;
            let list_len = u32::try_from(list_bytes.len())
                .map_err(|_| paro_error::out_of_range("posting list too large"))?;
            buf.put_u32_le(dim_id);
            buf.put_u32_le(list_len);
            buf.extend_from_slice(&list_bytes);
        }

        let stats = SparseIndexStatistics::collect(self);
        let mut out = buf.to_vec();
        append_stats_trailer(&mut out, &stats.to_bytes())?;
        Ok(out)
    }

    /// Deserialize a sparse index from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let mut buf = data;
        if buf.remaining() < 8 {
            return Err(paro_error::data_corrupted(
                "SparseVectorIndex: data too small",
            ));
        }

        let mut magic = [0u8; 4];
        buf.copy_to_slice(&mut magic);
        if &magic != MAGIC {
            return Err(paro_error::data_corrupted(
                "SparseVectorIndex: invalid magic",
            ));
        }

        let version = buf.get_u32_le();
        if version != VERSION {
            return Err(paro_error::not_supported(format!(
                "SparseVectorIndex: unsupported version {}",
                version
            )));
        }

        if buf.remaining() < 12 {
            return Err(paro_error::data_corrupted(
                "SparseVectorIndex: truncated header",
            ));
        }

        let full_scan_threshold = buf.get_f64_le();
        let num_vectors = buf.get_u32_le() as usize;

        if buf.remaining() < 4 {
            return Err(paro_error::data_corrupted(
                "SparseVectorIndex: truncated indices map length",
            ));
        }
        let map_len = buf.get_u32_le() as usize;
        if buf.remaining() < map_len * 8 {
            return Err(paro_error::data_corrupted(
                "SparseVectorIndex: truncated indices map",
            ));
        }
        let mut map = std::collections::HashMap::with_capacity(map_len);
        for _ in 0..map_len {
            let external = buf.get_u32_le();
            let internal = buf.get_u32_le();
            map.insert(external, internal);
        }

        if buf.remaining() < 4 {
            return Err(paro_error::data_corrupted(
                "SparseVectorIndex: truncated postings length",
            ));
        }
        let postings_len = buf.get_u32_le() as usize;
        let mut postings = std::collections::HashMap::with_capacity(postings_len);
        for _ in 0..postings_len {
            if buf.remaining() < 8 {
                return Err(paro_error::data_corrupted(
                    "SparseVectorIndex: truncated posting list header",
                ));
            }
            let dim_id = buf.get_u32_le();
            let list_len = buf.get_u32_le() as usize;
            if buf.remaining() < list_len {
                return Err(paro_error::data_corrupted(
                    "SparseVectorIndex: truncated posting list body",
                ));
            }
            let list_bytes = &buf[..list_len];
            let list = PostingList::from_bytes(list_bytes)?;
            postings.insert(dim_id, list);
            buf.advance(list_len);
        }

        let indices = IndicesTracker::from_map(map);
        let inverted_index = InvertedIndex::from_postings(postings);
        let config = SparseSearchConfig::new(full_scan_threshold);

        Ok(SparseVectorIndex::from_parts(
            indices,
            inverted_index,
            num_vectors,
            config,
        ))
    }
}
