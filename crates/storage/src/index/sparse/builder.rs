// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Sparse Index Builder
//!
//! Builds a sparse vector index from sparse vectors or column files.

use paro_common::error::{self as paro_error, Result};

use crate::index::hnsw::PointOffset;
use crate::rowset::sparse_vector::{SparseVector, SparseVectorColumnFile};

use super::{SparseSearchConfig, SparseVectorIndex};

/// Builder for SparseVectorIndex.
pub struct SparseIndexBuilder {
    index: SparseVectorIndex,
    next_doc_id: PointOffset,
}

impl SparseIndexBuilder {
    pub fn new() -> Self {
        Self {
            index: SparseVectorIndex::new(),
            next_doc_id: 0,
        }
    }

    pub fn with_config(config: SparseSearchConfig) -> Self {
        Self {
            index: SparseVectorIndex::new().with_config(config),
            next_doc_id: 0,
        }
    }

    /// Add a vector with an explicit document ID.
    pub fn add(&mut self, doc_id: PointOffset, vector: &SparseVector) -> Result<()> {
        self.index.upsert(doc_id, vector)?;
        let next = doc_id
            .checked_add(1)
            .ok_or_else(|| paro_error::out_of_range("doc_id exceeds u32 range"))?;
        if next > self.next_doc_id {
            self.next_doc_id = next;
        }
        Ok(())
    }

    /// Add a vector with the next sequential document ID.
    pub fn push(&mut self, vector: &SparseVector) -> Result<PointOffset> {
        let doc_id = self.next_doc_id;
        self.add(doc_id, vector)?;
        Ok(doc_id)
    }

    /// Add a batch of vectors starting at a given document ID.
    pub fn add_batch(&mut self, start_doc_id: PointOffset, vectors: &[SparseVector]) -> Result<()> {
        let mut doc_id = start_doc_id;
        for vector in vectors {
            self.index.upsert(doc_id, vector)?;
            doc_id = doc_id
                .checked_add(1)
                .ok_or_else(|| paro_error::out_of_range("doc_id exceeds u32 range"))?;
        }
        if doc_id > self.next_doc_id {
            self.next_doc_id = doc_id;
        }
        Ok(())
    }

    /// Build index from a sparse vector column file (doc_id = row ordinal).
    pub fn build_from_column_file(file: &SparseVectorColumnFile) -> Result<SparseVectorIndex> {
        let mut builder = SparseIndexBuilder::new();
        for (i, vec_result) in file.iter().enumerate() {
            let doc_id = PointOffset::try_from(i)
                .map_err(|_| paro_error::out_of_range("doc_id exceeds u32 range"))?;
            let vector = vec_result?;
            builder.add(doc_id, &vector)?;
        }
        Ok(builder.build())
    }

    /// Finish building and return the index.
    pub fn build(self) -> SparseVectorIndex {
        self.index
    }
}
