// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Vector Index Statistics
//!
//! Statistics for HNSW and Sparse vector indexes.

use std::collections::HashMap;

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
    pub total_graph_links: u64,
    pub level0_graph_links: u64,
    pub max_level0_degree: u32,
    pub avg_level0_degree: f32,
}

impl HnswIndexStatistics {
    pub const BYTE_LEN: usize = 8 * 9 + 4 * 2;

    pub fn collect(index: &HnswIndex) -> Result<Self> {
        let num_vectors = index.vector_storage.num_vectors();
        let dim = index.vector_storage.vector_dim();
        let degree_summary = index.graph.links.degree_summary()?;
        let graph_links_size = index.graph.predicate_links.as_ref().map_or(
            index.graph.links.serialized_size_bytes(),
            |predicate| {
                index
                    .graph
                    .links
                    .serialized_size_bytes()
                    .saturating_add(predicate.serialized_size_bytes())
            },
        );
        let entry_points_size = (index.graph.entry_points.entry_points.len()
            + index.graph.entry_points.extra_entry_points.len())
            as u64
            * std::mem::size_of::<crate::index::hnsw::EntryPoint>() as u64
            + index.graph.predicate_entry_points.len() as u64
                * std::mem::size_of::<crate::index::hnsw::PredicateEntryPoint>() as u64;
        let metric_preprocessing_size = index
            .vector_storage
            .cosine_inverse_norms()
            .map(|norms| norms.len() as u64 * std::mem::size_of::<f32>() as u64)
            .unwrap_or(0);
        let predicate_scan_size = index
            .predicate_scan
            .as_ref()
            .map_or(0, |layout| layout.serialized_size_bytes() as u64);
        let graph_size_bytes =
            graph_links_size + entry_points_size + metric_preprocessing_size + predicate_scan_size;
        let storage_size_bytes =
            num_vectors as u64 * dim as u64 * std::mem::size_of::<f32>() as u64;

        Ok(Self {
            num_indexed_vectors: num_vectors,
            dimension: dim,
            max_level: index.graph.entry_points.max_level(),
            m: index.build_contract.m as usize,
            ef_construction: index.build_contract.ef_construct as usize,
            graph_size_bytes,
            storage_size_bytes,
            total_graph_links: degree_summary.total_links,
            level0_graph_links: degree_summary.level0_links,
            max_level0_degree: degree_summary.max_level0_degree,
            avg_level0_degree: degree_summary.avg_level0_degree,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::BYTE_LEN);
        buf.extend_from_slice(&(self.num_indexed_vectors as u64).to_le_bytes());
        buf.extend_from_slice(&(self.dimension as u64).to_le_bytes());
        buf.extend_from_slice(&(self.max_level as u64).to_le_bytes());
        buf.extend_from_slice(&(self.m as u64).to_le_bytes());
        buf.extend_from_slice(&(self.ef_construction as u64).to_le_bytes());
        buf.extend_from_slice(&self.graph_size_bytes.to_le_bytes());
        buf.extend_from_slice(&self.storage_size_bytes.to_le_bytes());
        buf.extend_from_slice(&self.total_graph_links.to_le_bytes());
        buf.extend_from_slice(&self.level0_graph_links.to_le_bytes());
        buf.extend_from_slice(&self.max_level0_degree.to_le_bytes());
        buf.extend_from_slice(&self.avg_level0_degree.to_le_bytes());
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
        let total_graph_links = read_u64(bytes, &mut offset);
        let level0_graph_links = read_u64(bytes, &mut offset);
        if offset + 4 > bytes.len() {
            return Err(paro_error::data_corrupted(
                "HnswIndexStatistics: truncated max level0 degree",
            ));
        }
        let max_level0_degree = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if offset + 4 > bytes.len() {
            return Err(paro_error::data_corrupted(
                "HnswIndexStatistics: truncated avg level0 degree",
            ));
        }
        let avg_level0_degree = f32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());

        Ok(Self {
            num_indexed_vectors,
            dimension,
            max_level,
            m,
            ef_construction,
            graph_size_bytes,
            storage_size_bytes,
            total_graph_links,
            level0_graph_links,
            max_level0_degree,
            avg_level0_degree,
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
    pub l2_norm_sum: f64,
    pub max_l2_norm: f32,
}

impl SparseIndexStatistics {
    pub const BYTE_LEN: usize = 8 * 5 + 4 * 2;

    pub fn collect(index: &SparseVectorIndex) -> Self {
        let num_vectors = index.num_vectors();
        let postings = index.inverted_index().postings();
        let mut total_postings = 0usize;
        let mut norm_sq_by_doc = HashMap::<u32, f64>::new();
        for list in postings.values() {
            total_postings += list.len();
            for posting in list.iter() {
                *norm_sq_by_doc.entry(posting.doc_id).or_default() +=
                    f64::from(posting.weight) * f64::from(posting.weight);
            }
        }
        let avg_vector_nnz = if num_vectors == 0 {
            0.0
        } else {
            total_postings as f32 / num_vectors as f32
        };
        let mut l2_norm_sum = 0.0;
        let mut max_l2_norm = 0.0f32;
        for norm_sq in norm_sq_by_doc.values() {
            let norm = norm_sq.sqrt() as f32;
            l2_norm_sum += f64::from(norm);
            max_l2_norm = max_l2_norm.max(norm);
        }

        Self {
            num_indexed_vectors: num_vectors,
            num_unique_dimensions: index.indices().len(),
            num_posting_lists: postings.len(),
            total_postings,
            avg_vector_nnz,
            l2_norm_sum,
            max_l2_norm,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::BYTE_LEN);
        buf.extend_from_slice(&(self.num_indexed_vectors as u64).to_le_bytes());
        buf.extend_from_slice(&(self.num_unique_dimensions as u64).to_le_bytes());
        buf.extend_from_slice(&(self.num_posting_lists as u64).to_le_bytes());
        buf.extend_from_slice(&(self.total_postings as u64).to_le_bytes());
        buf.extend_from_slice(&self.avg_vector_nnz.to_le_bytes());
        buf.extend_from_slice(&self.l2_norm_sum.to_le_bytes());
        buf.extend_from_slice(&self.max_l2_norm.to_le_bytes());
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
        offset += 4;
        if offset + 8 > bytes.len() {
            return Err(paro_error::data_corrupted(
                "SparseIndexStatistics: truncated l2 norm sum",
            ));
        }
        let l2_norm_sum = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        if offset + 4 > bytes.len() {
            return Err(paro_error::data_corrupted(
                "SparseIndexStatistics: truncated max l2 norm",
            ));
        }
        let max_l2_norm = f32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());

        Ok(Self {
            num_indexed_vectors,
            num_unique_dimensions,
            num_posting_lists,
            total_postings,
            avg_vector_nnz,
            l2_norm_sum,
            max_l2_norm,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{HnswIndexStatistics, SparseIndexStatistics};
    use crate::index::hnsw::{DistanceMetric, HnswConfig, HnswIndex, InMemoryVectorStorage};
    use crate::index::sparse::SparseIndexBuilder;
    use crate::rowset::SparseVector;
    use std::sync::Arc;

    #[test]
    fn sparse_index_statistics_collects_l2_norm_summary() {
        let mut builder = SparseIndexBuilder::new();
        builder
            .add(0, &SparseVector::new(vec![1, 2], vec![3.0, 4.0]).unwrap())
            .unwrap();
        builder
            .add(1, &SparseVector::new(vec![2], vec![2.0]).unwrap())
            .unwrap();
        let index = builder.build();

        let stats = SparseIndexStatistics::collect(&index);
        assert_eq!(stats.num_indexed_vectors, 2);
        assert_eq!(stats.total_postings, 3);
        assert!((stats.l2_norm_sum - 7.0).abs() < 1e-6);
        assert_eq!(stats.max_l2_norm, 5.0);

        let restored = SparseIndexStatistics::from_bytes(&stats.to_bytes()).unwrap();
        assert!((restored.l2_norm_sum - 7.0).abs() < 1e-6);
        assert_eq!(restored.max_l2_norm, 5.0);
    }

    #[test]
    fn hnsw_index_statistics_collects_graph_degree_summary() {
        let storage = Arc::new(InMemoryVectorStorage::new(
            vec![
                0.0, 0.0, //
                1.0, 0.0, //
                0.0, 1.0, //
                1.0, 1.0, //
                2.0, 2.0, //
            ],
            2,
        ));
        let index = HnswIndex::build(storage, HnswConfig::new(4, 16), DistanceMetric::Euclidean);

        let stats = HnswIndexStatistics::collect(&index).unwrap();
        assert_eq!(stats.num_indexed_vectors, 5);
        assert_eq!(stats.dimension, 2);
        assert!(stats.graph_size_bytes > 0);
        assert_eq!(
            stats.storage_size_bytes,
            5 * 2 * std::mem::size_of::<f32>() as u64
        );
        assert!(stats.total_graph_links >= stats.level0_graph_links);
        assert_eq!(
            stats.avg_level0_degree,
            stats.level0_graph_links as f32 / stats.num_indexed_vectors as f32
        );

        let restored = HnswIndexStatistics::from_bytes(&stats.to_bytes()).unwrap();
        assert_eq!(restored.num_indexed_vectors, stats.num_indexed_vectors);
        assert_eq!(restored.dimension, stats.dimension);
        assert_eq!(restored.total_graph_links, stats.total_graph_links);
        assert_eq!(restored.level0_graph_links, stats.level0_graph_links);
        assert_eq!(restored.max_level0_degree, stats.max_level0_degree);
        assert_eq!(restored.avg_level0_degree, stats.avg_level0_degree);
    }
}
