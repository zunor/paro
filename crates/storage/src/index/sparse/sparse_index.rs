//! # Sparse Vector Index
//!
//! Integrates inverted index with dynamic dimension ID mapping.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use paro_common::error::{self as paro_error, Result};

use crate::index::hnsw::{PointOffset, ScoredPoint};
use crate::rowset::sparse_vector::{DimensionId, SparseVector};
use crate::statistics::SearchTelemetry;
use roaring::RoaringBitmap;

use super::inverted_index::InvertedIndex;
use super::posting_list::PostingList;
use super::search::{should_plain_search, SparseSearchConfig, SparseSearchContext};

/// Tracks dynamic dimension ID mapping (external -> internal).
#[derive(Debug, Clone, Default)]
pub struct IndicesTracker {
    map: HashMap<DimensionId, DimensionId>,
}

impl IndicesTracker {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub(crate) fn from_map(map: HashMap<DimensionId, DimensionId>) -> Self {
        Self { map }
    }

    pub(crate) fn map(&self) -> &HashMap<DimensionId, DimensionId> {
        &self.map
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Register a dimension ID if it does not exist.
    pub fn register_dimension(&mut self, dim_id: DimensionId) -> DimensionId {
        if let Some(existing) = self.map.get(&dim_id) {
            *existing
        } else {
            let next = self.map.len() as DimensionId;
            self.map.insert(dim_id, next);
            next
        }
    }

    /// Register all dimensions in the given vector.
    pub fn register_vector(&mut self, vector: &SparseVector) {
        for dim in &vector.dims {
            self.register_dimension(*dim);
        }
    }

    /// Remap a single external dimension ID to internal ID.
    pub fn remap_index(&self, dim_id: DimensionId) -> Option<DimensionId> {
        self.map.get(&dim_id).copied()
    }

    /// Remap a vector using current mapping.
    ///
    /// Unknown dimensions are mapped to placeholder IDs beyond the current range.
    pub fn remap_vector(&self, vector: &SparseVector) -> Result<SparseVector> {
        if vector.dims.len() != vector.weights.len() {
            return Err(paro_error::invalid_input(
                "IndicesTracker: dims/weights length mismatch",
            ));
        }

        let mut placeholder = self.map.len() as DimensionId;
        let mut dims = Vec::with_capacity(vector.dims.len());
        let mut weights = Vec::with_capacity(vector.weights.len());

        for (dim, weight) in vector
            .dims
            .iter()
            .copied()
            .zip(vector.weights.iter().copied())
        {
            let remapped = match self.map.get(&dim) {
                Some(id) => *id,
                None => {
                    placeholder = placeholder.saturating_add(1);
                    placeholder
                }
            };
            dims.push(remapped);
            weights.push(weight);
        }

        let mut remapped = SparseVector { dims, weights };
        remapped.sort_by_dim()?;
        Ok(remapped)
    }

    /// Register dimensions in the vector, then remap using updated mapping.
    pub fn register_and_remap(&mut self, vector: &SparseVector) -> Result<SparseVector> {
        self.register_vector(vector);
        self.remap_vector(vector)
    }
}

/// Sparse vector index based on inverted index.
#[derive(Debug, Default)]
pub struct SparseVectorIndex {
    indices: IndicesTracker,
    inverted_index: InvertedIndex,
    num_vectors: usize,
    config: SparseSearchConfig,
    telemetry: Mutex<SearchTelemetry>,
}

impl SparseVectorIndex {
    pub fn new() -> Self {
        Self {
            indices: IndicesTracker::new(),
            inverted_index: InvertedIndex::new(),
            num_vectors: 0,
            config: SparseSearchConfig::default(),
            telemetry: Mutex::new(SearchTelemetry::default()),
        }
    }

    pub(crate) fn from_parts(
        indices: IndicesTracker,
        inverted_index: InvertedIndex,
        num_vectors: usize,
        config: SparseSearchConfig,
    ) -> Self {
        Self {
            indices,
            inverted_index,
            num_vectors,
            config,
            telemetry: Mutex::new(SearchTelemetry::default()),
        }
    }

    pub fn with_config(mut self, config: SparseSearchConfig) -> Self {
        self.config = config;
        self
    }

    pub fn num_vectors(&self) -> usize {
        self.num_vectors
    }

    pub fn inverted_index(&self) -> &InvertedIndex {
        &self.inverted_index
    }

    pub fn indices(&self) -> &IndicesTracker {
        &self.indices
    }

    pub fn config(&self) -> SparseSearchConfig {
        self.config
    }

    /// Snapshot search telemetry.
    pub fn search_telemetry(&self) -> SearchTelemetry {
        self.telemetry.lock().unwrap().clone()
    }

    /// Get posting list for an external dimension ID.
    pub fn get_posting_list(&self, dim_id: DimensionId) -> Option<&PostingList> {
        let internal = self.indices.remap_index(dim_id)?;
        self.inverted_index.get_posting_list(internal)
    }

    /// Upsert a vector (registering new dimensions).
    pub fn upsert(&mut self, doc_id: PointOffset, vector: &SparseVector) -> Result<()> {
        let remapped = self.indices.register_and_remap(vector)?;
        self.inverted_index.upsert(doc_id, &remapped)?;
        self.num_vectors = self.num_vectors.max(doc_id as usize + 1);
        Ok(())
    }

    /// Upsert a vector with an optional old vector for cleanup.
    pub fn upsert_with_old(
        &mut self,
        doc_id: PointOffset,
        vector: &SparseVector,
        old_vector: Option<&SparseVector>,
    ) -> Result<()> {
        if let Some(old) = old_vector {
            let old_remapped = self.indices.remap_vector(old)?;
            self.inverted_index.remove(doc_id, &old_remapped)?;
        }
        self.upsert(doc_id, vector)
    }

    /// Remap a query vector without registering new dimensions.
    pub fn remap_query(&self, vector: &SparseVector) -> Result<SparseVector> {
        self.indices.remap_vector(vector)
    }

    /// Search sparse vectors using inverted index and optional filter bitmap.
    pub fn search(
        &self,
        query: &SparseVector,
        top_k: usize,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> Result<Vec<ScoredPoint>> {
        if top_k == 0 || self.num_vectors == 0 {
            return Ok(Vec::new());
        }

        let start = Instant::now();
        let pre_filter_count = self.num_vectors as u64;
        let mut post_filter_count = pre_filter_count;

        let remapped = self.remap_query(query)?;
        let ctx = SparseSearchContext::new(remapped, top_k, &self.inverted_index);

        let results = if let Some(bitmap) = filter_bitmap {
            post_filter_count = bitmap.len();
            if should_plain_search(self.config, self.num_vectors, bitmap) {
                let ids: Vec<PointOffset> = bitmap.iter().map(|id| id as PointOffset).collect();
                ctx.plain_search(&ids)
            } else {
                ctx.search(|id| bitmap.contains(id))
            }
        } else {
            ctx.search(|_| true)
        };

        let elapsed_us = start.elapsed().as_micros() as u64;
        self.telemetry
            .lock()
            .unwrap()
            .record(elapsed_us, pre_filter_count, post_filter_count);

        Ok(results)
    }
}
