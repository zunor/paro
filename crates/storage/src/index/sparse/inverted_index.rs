// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Sparse Inverted Index
//!
//! Maps dimension IDs to posting lists of (doc_id, weight).

use std::collections::HashMap;

use paro_common::error::{self as paro_error, Result};

use crate::index::hnsw::PointOffset;
use crate::rowset::sparse_vector::{DimensionId, SparseVector};

use super::posting_list::{PostingElement, PostingList};

/// Inverted index for sparse vectors.
#[derive(Debug, Default)]
pub struct InvertedIndex {
    postings: HashMap<DimensionId, PostingList>,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
        }
    }

    /// Number of posting lists.
    pub fn len(&self) -> usize {
        self.postings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.postings.is_empty()
    }

    pub(crate) fn postings(&self) -> &HashMap<DimensionId, PostingList> {
        &self.postings
    }

    pub(crate) fn from_postings(postings: HashMap<DimensionId, PostingList>) -> Self {
        Self { postings }
    }

    /// Get posting list for a dimension id.
    pub fn get_posting_list(&self, dim_id: DimensionId) -> Option<&PostingList> {
        self.postings.get(&dim_id)
    }

    /// Get mutable posting list for a dimension id.
    pub fn get_posting_list_mut(&mut self, dim_id: DimensionId) -> Option<&mut PostingList> {
        self.postings.get_mut(&dim_id)
    }

    /// Upsert a sparse vector for the given doc_id.
    ///
    /// The vector must be sorted by dimension IDs.
    pub fn upsert(&mut self, doc_id: PointOffset, vector: &SparseVector) -> Result<()> {
        vector.ensure_sorted()?;
        if vector.dims.len() != vector.weights.len() {
            return Err(paro_error::invalid_input(
                "InvertedIndex: dims/weights length mismatch",
            ));
        }

        for (dim, weight) in vector
            .dims
            .iter()
            .copied()
            .zip(vector.weights.iter().copied())
        {
            let list = self.postings.entry(dim).or_default();
            // PostingList::upsert keeps ordering by doc_id.
            list.upsert(doc_id, weight);
        }
        Ok(())
    }

    /// Remove a sparse vector for the given doc_id.
    ///
    /// The vector must be sorted by dimension IDs.
    pub fn remove(&mut self, doc_id: PointOffset, vector: &SparseVector) -> Result<()> {
        vector.ensure_sorted()?;
        if vector.dims.len() != vector.weights.len() {
            return Err(paro_error::invalid_input(
                "InvertedIndex: dims/weights length mismatch",
            ));
        }

        for dim in vector.dims.iter() {
            if let Some(list) = self.postings.get_mut(dim) {
                list.delete(doc_id);
                if list.is_empty() {
                    self.postings.remove(dim);
                }
            }
        }
        Ok(())
    }

    /// Insert a single posting element (doc_id, weight) into a posting list.
    pub fn upsert_element(&mut self, dim_id: DimensionId, element: PostingElement) {
        let list = self.postings.entry(dim_id).or_default();
        list.upsert(element.doc_id, element.weight);
    }
}
