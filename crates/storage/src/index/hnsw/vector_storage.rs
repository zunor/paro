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
    /// Get vector at given offset.
    fn get_vector(&self, idx: PointOffset) -> &[f32];

    /// Whole dense row-major vector artifact when the physical backing is
    /// contiguous. Query artifacts require this shape so their scorer can
    /// resolve dynamic storage once; construction inputs may be partitioned
    /// over several immutable base-segment mappings and return `None`.
    fn contiguous_vectors(&self) -> Option<&[f32]> {
        None
    }

    /// Visit the largest physically contiguous row-major regions available.
    /// Construction-time partition views forward one region per base segment;
    /// artifact and in-memory storage forward one region for the whole index.
    fn try_for_each_contiguous_chunk(
        &self,
        visitor: &mut dyn FnMut(&[f32]) -> Result<()>,
    ) -> Result<()> {
        if let Some(vectors) = self.contiguous_vectors() {
            return visitor(vectors);
        }
        for point_id in 0..self.num_vectors() {
            visitor(self.get_vector(point_id as PointOffset))?;
        }
        Ok(())
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

    fn is_mmap_backed(&self) -> bool {
        false
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
    fn get_vector(&self, idx: PointOffset) -> &[f32] {
        self.base.get_vector(idx)
    }

    fn contiguous_vectors(&self) -> Option<&[f32]> {
        self.base.contiguous_vectors()
    }

    fn try_for_each_contiguous_chunk(
        &self,
        visitor: &mut dyn FnMut(&[f32]) -> Result<()>,
    ) -> Result<()> {
        self.base.try_for_each_contiguous_chunk(visitor)
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

    fn is_mmap_backed(&self) -> bool {
        self.base.is_mmap_backed()
    }
}

/// In-memory vector storage, primarily for testing and small datasets.
pub struct InMemoryVectorStorage {
    vectors: Vec<f32>,
    dim: usize,
    count: usize,
}

/// Row-major vectors physically owned by an HNSW artifact.
///
/// Sidecar opens retain an mmap slice and perform no O(N) allocation. Owned
/// byte envelopes are decoded once because `Bytes` does not promise `f32`
/// alignment. Keeping this storage inside the artifact makes graph point ids
/// independent of any one base-table segment and is the foundation for
/// generation-owned multi-segment search partitions.
pub(crate) struct ArtifactVectorStorage {
    backing: ArtifactVectorBacking,
    dim: usize,
    count: usize,
}

enum ArtifactVectorBacking {
    Owned(Arc<[f32]>),
    Mmap {
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
    },
}

impl ArtifactVectorStorage {
    pub(crate) fn from_bytes(
        bytes: &[u8],
        dim: usize,
        count: usize,
    ) -> Result<Arc<dyn VectorStorage>> {
        let expected_bytes = Self::validate_layout(bytes.len(), dim, count)?;
        debug_assert_eq!(expected_bytes, bytes.len());
        let values = bytes
            .chunks_exact(std::mem::size_of::<f32>())
            .map(|raw| f32::from_le_bytes(raw.try_into().expect("f32 width")))
            .collect::<Vec<_>>();
        Ok(Arc::new(Self {
            backing: ArtifactVectorBacking::Owned(values.into()),
            dim,
            count,
        }))
    }

    pub(crate) fn from_mmap_range(
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
        dim: usize,
        count: usize,
    ) -> Result<Arc<dyn VectorStorage>> {
        Self::validate_layout(len, dim, count)?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| paro_common::error::data_corrupted("HNSW vector mmap range overflow"))?;
        if end > mmap.len() {
            return Err(paro_common::error::data_corrupted(
                "HNSW vector mmap range exceeds package length",
            ));
        }
        if offset % std::mem::align_of::<f32>() != 0 {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW vector mmap offset {offset} is not f32-aligned"
            )));
        }
        if cfg!(target_endian = "big") {
            return Err(paro_common::error::not_supported(
                "mmap-backed HNSW vectors require a little-endian target",
            ));
        }
        #[cfg(unix)]
        {
            let _ = mmap.advise_range(memmap2::Advice::Random, offset, len);
        }
        Ok(Arc::new(Self {
            backing: ArtifactVectorBacking::Mmap { mmap, offset, len },
            dim,
            count,
        }))
    }

    fn validate_layout(len: usize, dim: usize, count: usize) -> Result<usize> {
        if dim == 0 {
            return Err(paro_common::error::data_corrupted(
                "HNSW artifact vector dimension must be non-zero",
            ));
        }
        let expected = count
            .checked_mul(dim)
            .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
            .ok_or_else(|| {
                paro_common::error::data_corrupted("HNSW artifact vector byte length overflow")
            })?;
        if len != expected {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW artifact vector byte length mismatch: expected {expected}, got {len}"
            )));
        }
        Ok(expected)
    }
}

impl VectorStorage for ArtifactVectorStorage {
    fn get_vector(&self, idx: PointOffset) -> &[f32] {
        let vectors = self
            .contiguous_vectors()
            .expect("artifact vector storage is contiguous");
        let start = idx as usize * self.dim;
        &vectors[start..start + self.dim]
    }

    fn contiguous_vectors(&self) -> Option<&[f32]> {
        Some(match &self.backing {
            ArtifactVectorBacking::Owned(values) => values,
            ArtifactVectorBacking::Mmap { mmap, offset, len } => {
                // SAFETY: `from_mmap_range` proves f32 alignment, exact
                // row-major length, immutable backing, and little-endian
                // representation for the complete lifetime of this storage.
                unsafe {
                    std::slice::from_raw_parts(
                        mmap.as_ptr().add(*offset).cast::<f32>(),
                        *len / std::mem::size_of::<f32>(),
                    )
                }
            }
        })
    }

    fn num_vectors(&self) -> usize {
        self.count
    }

    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn is_mmap_backed(&self) -> bool {
        matches!(self.backing, ArtifactVectorBacking::Mmap { .. })
    }
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
        &self.vectors[start..start + self.dim]
    }

    fn contiguous_vectors(&self) -> Option<&[f32]> {
        Some(&self.vectors)
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
    fn get_vector(&self, idx: PointOffset) -> &[f32] {
        let vectors = self
            .contiguous_vectors()
            .expect("mmap vector storage is contiguous");
        let start = idx as usize * self.dim;
        &vectors[start..start + self.dim]
    }

    fn contiguous_vectors(&self) -> Option<&[f32]> {
        // SAFETY: mmap offsets are page aligned, `open_range` validates that
        // the byte length is an exact multiple of `dim * size_of::<f32>()`,
        // and the mapping is immutable for the lifetime of this storage.
        Some(unsafe {
            std::slice::from_raw_parts(self.mmap.as_ptr().cast::<f32>(), self.count * self.dim)
        })
    }

    fn num_vectors(&self) -> usize {
        self.count
    }

    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn is_mmap_backed(&self) -> bool {
        true
    }
}

struct PartitionedVectorStoragePart {
    range: std::ops::Range<u32>,
    storage: Arc<dyn VectorStorage>,
}

/// Immutable construction-time view over canonical base-segment mappings.
///
/// Generation-owned HNSW partitions must not first concatenate every input
/// vector into heap memory. The builder addresses this view with one global
/// point-id domain, while each lookup resolves to the owning segment mapping.
/// Published artifacts still serialize one contiguous vector region and open
/// through [`ArtifactVectorStorage`], so query scoring keeps its hot flat
/// layout.
pub(crate) struct PartitionedVectorStorage {
    parts: Box<[PartitionedVectorStoragePart]>,
    dim: usize,
    count: usize,
}

impl PartitionedVectorStorage {
    pub(crate) fn try_new(storages: Vec<Arc<dyn VectorStorage>>, dim: usize) -> Result<Self> {
        if storages.is_empty() {
            return Err(paro_common::error::invalid_input(
                "partitioned vector storage requires at least one input",
            ));
        }
        if dim == 0 {
            return Err(paro_common::error::invalid_input(
                "partitioned vector dimension must be non-zero",
            ));
        }
        let mut parts = Vec::with_capacity(storages.len());
        let mut point_base = 0u32;
        for storage in storages {
            if storage.vector_dim() != dim {
                return Err(paro_common::error::data_corrupted(format!(
                    "partitioned vector dimension mismatch: expected {dim}, got {}",
                    storage.vector_dim()
                )));
            }
            let rows = u32::try_from(storage.num_vectors()).map_err(|_| {
                paro_common::error::configuration_limit_exceeded(
                    "partitioned vector input exceeds the u32 point-id domain",
                )
            })?;
            if rows == 0 {
                return Err(paro_common::error::invalid_input(
                    "partitioned vector storage cannot contain an empty input",
                ));
            }
            let point_end = point_base.checked_add(rows).ok_or_else(|| {
                paro_common::error::configuration_limit_exceeded(
                    "partitioned vector storage exceeds the u32 point-id domain",
                )
            })?;
            parts.push(PartitionedVectorStoragePart {
                range: point_base..point_end,
                storage,
            });
            point_base = point_end;
        }
        Ok(Self {
            parts: parts.into_boxed_slice(),
            dim,
            count: point_base as usize,
        })
    }

    fn part_for(&self, point_id: u32) -> &PartitionedVectorStoragePart {
        let position = self
            .parts
            .partition_point(|part| part.range.end <= point_id);
        self.parts
            .get(position)
            .filter(|part| part.range.contains(&point_id))
            .expect("HNSW construction point id exceeds partitioned vector storage")
    }
}

impl VectorStorage for PartitionedVectorStorage {
    fn get_vector(&self, idx: PointOffset) -> &[f32] {
        let part = self.part_for(idx);
        part.storage.get_vector(idx - part.range.start)
    }

    fn num_vectors(&self) -> usize {
        self.count
    }

    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn try_for_each_contiguous_chunk(
        &self,
        visitor: &mut dyn FnMut(&[f32]) -> Result<()>,
    ) -> Result<()> {
        for part in &self.parts {
            part.storage.try_for_each_contiguous_chunk(visitor)?;
        }
        Ok(())
    }

    fn is_mmap_backed(&self) -> bool {
        self.parts.iter().all(|part| part.storage.is_mmap_backed())
    }
}

/// Shared pointer to a VectorStorage.
pub type SharedVectorStorage = Arc<dyn VectorStorage>;
