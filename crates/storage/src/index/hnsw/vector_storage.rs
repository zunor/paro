// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Vector Storage
//!
//! Abstractions for storing and accessing vectors used by HNSW.

use super::types::PointOffset;
use super::DistanceMetric;
use memmap2::Mmap;
use paro_common::error::Result;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Trait for vector storage used by HNSW.
pub trait VectorStorage: Send + Sync {
    /// Get vector at given offset.
    fn get_vector(&self, idx: PointOffset) -> &[f32];
    /// Get number of vectors.
    fn num_vectors(&self) -> usize;
    /// Get vector dimension.
    fn vector_dim(&self) -> usize;
    /// Per-point cosine preprocessing owned by the HNSW artifact. Base table
    /// storage returns `None`; indexed storage returns an immutable array.
    fn cosine_inverse_norms(&self) -> Option<&[f32]> {
        None
    }

    #[inline]
    fn cosine_inverse_norm(&self, idx: PointOffset) -> Option<f32> {
        self.cosine_inverse_norms()
            .and_then(|norms| norms.get(idx as usize))
            .copied()
    }
}

/// Raw table vectors plus HNSW-private metric preprocessing. The wrapper never
/// changes the bytes returned by `get_vector`.
pub struct IndexedVectorStorage {
    base: Arc<dyn VectorStorage>,
    cosine_inverse_norms: Option<Arc<[f32]>>,
}

impl IndexedVectorStorage {
    pub fn prepare(
        base: Arc<dyn VectorStorage>,
        distance: DistanceMetric,
    ) -> Arc<dyn VectorStorage> {
        if distance != DistanceMetric::Cosine {
            if base.cosine_inverse_norms().is_none() {
                return base;
            }
            // Metric preprocessing belongs to one artifact contract. Hide
            // cosine metadata when a caller deliberately reuses the same raw
            // vectors to build a non-cosine graph.
            return Arc::new(Self {
                base,
                cosine_inverse_norms: None,
            });
        }
        if base
            .cosine_inverse_norms()
            .is_some_and(|norms| norms.len() == base.num_vectors())
        {
            return base;
        }
        let inverse_norms = (0..base.num_vectors())
            .map(|idx| {
                let vector = base.get_vector(idx as PointOffset);
                paro_common::distance::inverse_norm(vector)
            })
            .collect::<Vec<_>>()
            .into();
        Arc::new(Self {
            base,
            cosine_inverse_norms: Some(inverse_norms),
        })
    }

    pub fn from_persisted_cosine_norms(
        base: Arc<dyn VectorStorage>,
        inverse_norms: Arc<[f32]>,
    ) -> Result<Arc<dyn VectorStorage>> {
        if inverse_norms.len() != base.num_vectors() {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW cosine inverse norm count mismatch: expected {}, got {}",
                base.num_vectors(),
                inverse_norms.len()
            )));
        }
        Ok(Arc::new(Self {
            base,
            cosine_inverse_norms: Some(inverse_norms),
        }))
    }
}

impl VectorStorage for IndexedVectorStorage {
    fn get_vector(&self, idx: PointOffset) -> &[f32] {
        self.base.get_vector(idx)
    }

    fn num_vectors(&self) -> usize {
        self.base.num_vectors()
    }

    fn vector_dim(&self) -> usize {
        self.base.vector_dim()
    }

    fn cosine_inverse_norms(&self) -> Option<&[f32]> {
        self.cosine_inverse_norms.as_deref()
    }
}

/// In-memory vector storage, primarily for testing and small datasets.
pub struct InMemoryVectorStorage {
    vectors: Vec<f32>,
    dim: usize,
    count: usize,
}

impl InMemoryVectorStorage {
    /// Create new in-memory storage.
    pub fn new(vectors: Vec<f32>, dim: usize) -> Self {
        debug_assert_eq!(
            vectors.len() % dim,
            0,
            "Vectors length must be multiple of dimension"
        );
        let count = vectors.len() / dim;
        Self {
            vectors,
            dim,
            count,
        }
    }

    /// Create an empty in-memory storage with given dimension.
    pub fn empty(dim: usize) -> Self {
        Self {
            vectors: Vec::new(),
            dim,
            count: 0,
        }
    }

    /// Append a vector to the storage.
    pub fn append(&mut self, vector: &[f32]) {
        debug_assert_eq!(vector.len(), self.dim);
        self.vectors.extend_from_slice(vector);
        self.count += 1;
    }
}

impl VectorStorage for InMemoryVectorStorage {
    fn get_vector(&self, idx: PointOffset) -> &[f32] {
        let start = idx as usize * self.dim;
        let end = start + self.dim;
        &self.vectors[start..end]
    }

    fn num_vectors(&self) -> usize {
        self.count
    }

    fn vector_dim(&self) -> usize {
        self.dim
    }
}

/// Mmap-based vector storage for production use.
pub struct MmapVectorStorage {
    mmap: Mmap,
    dim: usize,
    count: usize,
}

impl MmapVectorStorage {
    /// Create new mmap-based storage from a file range.
    pub fn open_range(path: impl AsRef<Path>, offset: u64, size: u64, dim: usize) -> Result<Self> {
        let file = File::open(path)?;
        // We mmap the whole file but only access the range.
        // Alternatively, we could use MapOptions to map a range if supported by the OS.
        // memmap2::MmapOptions::new().offset(offset).len(size).map(&file)
        let mmap = unsafe {
            memmap2::MmapOptions::new()
                .offset(offset)
                .len(size as usize)
                .map(&file)?
        };

        let vector_bytes = dim * std::mem::size_of::<f32>();
        debug_assert_eq!(
            size % vector_bytes as u64,
            0,
            "Range size must be multiple of vector size"
        );
        let count = size as usize / vector_bytes;

        Ok(Self { mmap, dim, count })
    }
}

impl VectorStorage for MmapVectorStorage {
    fn get_vector(&self, idx: PointOffset) -> &[f32] {
        let start_byte = idx as usize * self.dim * std::mem::size_of::<f32>();
        let end_byte = start_byte + self.dim * std::mem::size_of::<f32>();
        let byte_slice = &self.mmap[start_byte..end_byte];

        unsafe { std::slice::from_raw_parts(byte_slice.as_ptr() as *const f32, self.dim) }
    }

    fn num_vectors(&self) -> usize {
        self.count
    }

    fn vector_dim(&self) -> usize {
        self.dim
    }
}

/// Shared pointer to a VectorStorage.
pub type SharedVectorStorage = Arc<dyn VectorStorage>;
