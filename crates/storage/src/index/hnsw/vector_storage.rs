// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Vector Storage
//!
//! Abstractions for storing and accessing vectors used by HNSW.

use super::types::PointOffset;
use super::DistanceMetric;
use bytes::Bytes;
use memmap2::Mmap;
use paro_common::error::Result;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Immutable per-point cosine preprocessing owned by an HNSW artifact.
///
/// Persisted norms deliberately retain their byte backing instead of being
/// decoded into an O(N) heap allocation when an index is opened. Values are
/// little-endian on disk, so byte-backed access is alignment-independent.
#[derive(Debug, Clone)]
pub enum CosineInverseNorms {
    Owned(Arc<[f32]>),
    Bytes(Bytes),
    Mmap {
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
    },
}

impl CosineInverseNorms {
    pub fn from_bytes(bytes: Bytes) -> Result<Self> {
        Self::validate_byte_len(bytes.len())?;
        Ok(Self::Bytes(bytes))
    }

    pub fn from_mmap(mmap: Arc<Mmap>) -> Result<Self> {
        let len = mmap.len();
        Self::from_mmap_range(mmap, 0, len)
    }

    pub fn from_mmap_range(mmap: Arc<Mmap>, offset: usize, len: usize) -> Result<Self> {
        Self::validate_byte_len(len)?;
        let end = offset.checked_add(len).ok_or_else(|| {
            paro_common::error::data_corrupted("HNSW cosine norm mmap range overflow")
        })?;
        if end > mmap.len() {
            return Err(paro_common::error::data_corrupted(
                "HNSW cosine norm mmap range exceeds package length",
            ));
        }
        #[cfg(unix)]
        {
            // Norms follow graph point ids and are read in the same random
            // order. This is an access hint only; unsupported kernels must
            // never make a valid index unavailable.
            let _ = mmap.advise_range(memmap2::Advice::Random, offset, len);
        }
        Ok(Self::Mmap { mmap, offset, len })
    }

    fn validate_byte_len(len: usize) -> Result<()> {
        if len % std::mem::size_of::<f32>() != 0 {
            return Err(paro_common::error::data_corrupted(
                "HNSW cosine inverse norm artifact is truncated",
            ));
        }
        Ok(())
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(values) => values.len(),
            Self::Bytes(bytes) => bytes.len() / std::mem::size_of::<f32>(),
            Self::Mmap { len, .. } => *len / std::mem::size_of::<f32>(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_bytes_backed(&self) -> bool {
        matches!(self, Self::Bytes(_))
    }

    pub fn is_mmap_backed(&self) -> bool {
        matches!(self, Self::Mmap { .. })
    }

    #[inline]
    pub fn get(&self, idx: PointOffset) -> Option<f32> {
        let idx = idx as usize;
        match self {
            Self::Owned(values) => values.get(idx).copied(),
            Self::Bytes(bytes) => Self::read_le(bytes, idx),
            Self::Mmap { mmap, offset, len } => Self::read_le(&mmap[*offset..*offset + *len], idx),
        }
    }

    /// Read a value after the artifact/open boundary has established that the
    /// norm cardinality matches the vector cardinality.
    #[inline]
    pub fn value(&self, idx: PointOffset) -> f32 {
        let idx = idx as usize;
        match self {
            Self::Owned(values) => values[idx],
            Self::Bytes(bytes) => Self::read_validated(bytes, idx),
            Self::Mmap { mmap, offset, len } => {
                Self::read_validated(&mmap[*offset..*offset + *len], idx)
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = f32> + '_ {
        (0..self.len()).map(|idx| self.value(idx as PointOffset))
    }

    #[inline]
    fn read_validated(bytes: &[u8], idx: usize) -> f32 {
        let start = idx * std::mem::size_of::<f32>();
        f32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ])
    }

    fn read_le(bytes: &[u8], idx: usize) -> Option<f32> {
        let start = idx.checked_mul(std::mem::size_of::<f32>())?;
        let raw = bytes.get(start..start + std::mem::size_of::<f32>())?;
        Some(f32::from_le_bytes(raw.try_into().ok()?))
    }
}

/// Trait for vector storage used by HNSW.
pub trait VectorStorage: Send + Sync {
    /// Whole dense row-major vector artifact. Exposing the immutable physical
    /// layout lets a query scorer resolve dynamic storage once, then perform
    /// point lookups without one virtual call per distance calculation.
    fn flat_vectors(&self) -> &[f32];

    /// Get vector at given offset.
    fn get_vector(&self, idx: PointOffset) -> &[f32] {
        let dim = self.vector_dim();
        let start = idx as usize * dim;
        &self.flat_vectors()[start..start + dim]
    }
    /// Get number of vectors.
    fn num_vectors(&self) -> usize;
    /// Get vector dimension.
    fn vector_dim(&self) -> usize;
    /// Per-point cosine preprocessing owned by the HNSW artifact. Base table
    /// storage returns `None`; indexed storage returns an immutable array.
    fn cosine_inverse_norms(&self) -> Option<&CosineInverseNorms> {
        None
    }
}

/// Raw table vectors plus HNSW-private metric preprocessing. The wrapper never
/// changes the bytes returned by `get_vector`.
pub struct IndexedVectorStorage {
    base: Arc<dyn VectorStorage>,
    cosine_inverse_norms: Option<CosineInverseNorms>,
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
        let inverse_norms: Arc<[f32]> = (0..base.num_vectors())
            .map(|idx| {
                let vector = base.get_vector(idx as PointOffset);
                paro_common::distance::inverse_norm(vector)
            })
            .collect::<Vec<_>>()
            .into();
        Arc::new(Self {
            base,
            cosine_inverse_norms: Some(CosineInverseNorms::Owned(inverse_norms)),
        })
    }

    pub fn from_persisted_cosine_norms(
        base: Arc<dyn VectorStorage>,
        inverse_norms: CosineInverseNorms,
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
    fn flat_vectors(&self) -> &[f32] {
        self.base.flat_vectors()
    }

    fn num_vectors(&self) -> usize {
        self.base.num_vectors()
    }

    fn vector_dim(&self) -> usize {
        self.base.vector_dim()
    }

    fn cosine_inverse_norms(&self) -> Option<&CosineInverseNorms> {
        self.cosine_inverse_norms.as_ref()
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
    fn flat_vectors(&self) -> &[f32] {
        &self.vectors
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
        #[cfg(unix)]
        {
            // HNSW point lookups are intentionally non-sequential. Prevent
            // the kernel from turning a small beam into large speculative
            // readahead over the base vector artifact.
            let _ = mmap.advise(memmap2::Advice::Random);
        }

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
    fn flat_vectors(&self) -> &[f32] {
        // SAFETY: mmap offsets are page aligned, `open_range` validates that
        // the byte length is an exact multiple of `dim * size_of::<f32>()`,
        // and the mapping is immutable for the lifetime of this storage.
        unsafe {
            std::slice::from_raw_parts(self.mmap.as_ptr().cast::<f32>(), self.count * self.dim)
        }
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
