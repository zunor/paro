//! # Vector Index Statistics
//!
//! Statistics for HNSW and Sparse vector indexes.

use paro_common::error::{self as paro_error, Result};

use crate::index::hnsw::HnswIndex;
use crate::index::sparse::SparseVectorIndex;

/// Statistics for a HNSW vector index.
#[derive(Debug, Clone)]
pub struct HnswIndexStatistics {
    pub num_indexed_vectors: usize,
    pub dimension: usize,
    pub max_level: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub graph_size_bytes: u64,
    pub storage_size_bytes: u64,
}

impl HnswIndexStatistics {
    pub const BYTE_LEN: usize = 8 * 7;

    pub fn collect(index: &HnswIndex) -> Self {
        let num_vectors = index.vector_storage.num_vectors();
        let dim = index.vector_storage.vector_dim();
        let graph_links_size = index.graph.links.serialized_size_bytes();
        let entry_points_size = (index.graph.entry_points.entry_points.len()
            + index.graph.entry_points.extra_entry_points.len())
            as u64
            * std::mem::size_of::<crate::index::hnsw::EntryPoint>() as u64;
        let graph_size_bytes = graph_links_size + entry_points_size;
        let storage_size_bytes =
            num_vectors as u64 * dim as u64 * std::mem::size_of::<f32>() as u64;

        Self {
            num_indexed_vectors: num_vectors,
            dimension: dim,
            max_level: index.graph.entry_points.max_level(),
            m: index.config.m,
            ef_construction: index.config.ef_construct,
            graph_size_bytes,
            storage_size_bytes,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 * 7);
        buf.extend_from_slice(&(self.num_indexed_vectors as u64).to_le_bytes());
        buf.extend_from_slice(&(self.dimension as u64).to_le_bytes());
        buf.extend_from_slice(&(self.max_level as u64).to_le_bytes());
        buf.extend_from_slice(&(self.m as u64).to_le_bytes());
        buf.extend_from_slice(&(self.ef_construction as u64).to_le_bytes());
        buf.extend_from_slice(&self.graph_size_bytes.to_le_bytes());
        buf.extend_from_slice(&self.storage_size_bytes.to_le_bytes());
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::BYTE_LEN {
            return Err(paro_error::data_corrupted("HnswIndexStatistics: truncated"));
        }
        let mut offset = 0;
        let read_u64 = |bytes: &[u8], offset: &mut usize| {
            let v = u64::from_le_bytes(bytes[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            v
        };

        let num_indexed_vectors = read_u64(bytes, &mut offset) as usize;
        let dimension = read_u64(bytes, &mut offset) as usize;
        let max_level = read_u64(bytes, &mut offset) as usize;
        let m = read_u64(bytes, &mut offset) as usize;
        let ef_construction = read_u64(bytes, &mut offset) as usize;
        let graph_size_bytes = read_u64(bytes, &mut offset);
        let storage_size_bytes = read_u64(bytes, &mut offset);

        Ok(Self {
            num_indexed_vectors,
            dimension,
            max_level,
            m,
            ef_construction,
            graph_size_bytes,
            storage_size_bytes,
        })
    }
}

/// Statistics for a sparse vector index.
#[derive(Debug, Clone)]
pub struct SparseIndexStatistics {
    pub num_indexed_vectors: usize,
    pub num_unique_dimensions: usize,
    pub num_posting_lists: usize,
    pub total_postings: usize,
    pub avg_vector_nnz: f32,
}

impl SparseIndexStatistics {
    pub const BYTE_LEN: usize = 8 * 4 + 4;

    pub fn collect(index: &SparseVectorIndex) -> Self {
        let num_vectors = index.num_vectors();
        let postings = index.inverted_index().postings();
        let mut total_postings = 0usize;
        for list in postings.values() {
            total_postings += list.len();
        }
        let avg_vector_nnz = if num_vectors == 0 {
            0.0
        } else {
            total_postings as f32 / num_vectors as f32
        };

        Self {
            num_indexed_vectors: num_vectors,
            num_unique_dimensions: index.indices().len(),
            num_posting_lists: postings.len(),
            total_postings,
            avg_vector_nnz,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 * 4 + 4);
        buf.extend_from_slice(&(self.num_indexed_vectors as u64).to_le_bytes());
        buf.extend_from_slice(&(self.num_unique_dimensions as u64).to_le_bytes());
        buf.extend_from_slice(&(self.num_posting_lists as u64).to_le_bytes());
        buf.extend_from_slice(&(self.total_postings as u64).to_le_bytes());
        buf.extend_from_slice(&self.avg_vector_nnz.to_le_bytes());
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::BYTE_LEN {
            return Err(paro_error::data_corrupted(
                "SparseIndexStatistics: truncated",
            ));
        }
        let mut offset = 0;
        let read_u64 = |bytes: &[u8], offset: &mut usize| {
            let v = u64::from_le_bytes(bytes[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            v
        };

        let num_indexed_vectors = read_u64(bytes, &mut offset) as usize;
        let num_unique_dimensions = read_u64(bytes, &mut offset) as usize;
        let num_posting_lists = read_u64(bytes, &mut offset) as usize;
        let total_postings = read_u64(bytes, &mut offset) as usize;
        if offset + 4 > bytes.len() {
            return Err(paro_error::data_corrupted(
                "SparseIndexStatistics: truncated avg",
            ));
        }
        let avg_vector_nnz = f32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());

        Ok(Self {
            num_indexed_vectors,
            num_unique_dimensions,
            num_posting_lists,
            total_postings,
            avg_vector_nnz,
        })
    }
}
