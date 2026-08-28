// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Vector Storage
//!
//! Abstractions for storing and accessing vectors used by HNSW.

use super::types::PointOffset;
use super::DistanceMetric;
use bytes::Bytes;
use memmap2::{Mmap, MmapMut};
use paro_common::error::Result;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use crate::index::partition_directory::PartitionDirectory;
use crate::rowset::column::OrdinalIndexReader;
use crate::rowset::encoding::{FieldType, PLAIN_PAGE_HEADER_SIZE};
use crate::rowset::page::{CompressionType, EncodingType, PageFooter, PageIO, PageReadOptions};
use crate::rowset::segment::ColumnMeta;

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
    /// contiguous. Scorers use this as a SIMD fast path; generation-owned
    /// artifacts may instead remain partitioned over immutable base pages.
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

    /// Compact immutable routing image used only by graph navigation. Exact
    /// scans and SQL-visible scores continue to use the canonical f32 matrix.
    fn i16_routing_view(&self) -> Option<I16RoutingView<'_>> {
        None
    }

    /// Hint that a construction-time point will be scored shortly.
    ///
    /// Build wrappers own the physical routing representation, so the
    /// prefetch boundary belongs here rather than in the graph builder. Exact
    /// builds fetch the canonical f32 row; compact builds override this to
    /// fetch their encoded routing row. The hint has no semantic effect and
    /// callers still score points in durable link order.
    #[inline]
    fn prefetch_construction_point(&self, idx: PointOffset) {
        paro_common::distance::prefetch_vector_read(self.get_vector(idx));
    }

    /// Score two stored points for durable graph construction.
    ///
    /// The default is the canonical f32 metric. Build-only wrappers may
    /// override this with a compact deterministic representation; query
    /// scoring remains owned by [`crate::index::hnsw::scorer::VectorScorer`]
    /// and therefore cannot accidentally emit encoded scores to SQL.
    fn construction_similarity(
        &self,
        distance: DistanceMetric,
        left: PointOffset,
        right: PointOffset,
    ) -> f32 {
        if distance == DistanceMetric::Cosine {
            let norms = self
                .cosine_inverse_norms()
                .unwrap_or_else(|| unreachable!("cosine construction storage is prepared once"));
            distance.similarity_indexed(
                self.get_vector(left),
                self.get_vector(right),
                norms.value(left),
                norms.value(right),
            )
        } else {
            distance.similarity(self.get_vector(left), self.get_vector(right))
        }
    }

    /// Score one construction point against a bounded neighbor row.
    ///
    /// This is a storage capability rather than a builder-side representation
    /// check: exact f32 and compact routing artifacts own different physical
    /// arithmetic, and only the storage that defines that arithmetic may batch
    /// it. The default preserves the scalar contract for representations that
    /// do not yet expose a batch kernel.
    fn construction_similarities(
        &self,
        distance: DistanceMetric,
        left: PointOffset,
        rights: &[PointOffset],
        scores: &mut [f32],
    ) {
        assert!(
            scores.len() >= rights.len(),
            "construction score buffer is shorter than point batch"
        );
        for (&right, score) in rights.iter().zip(scores.iter_mut()) {
            *score = self.construction_similarity(distance, left, right);
        }
    }
}

const I16_BUILD_ALIGNMENT: usize = 16;
const MAX_OWNED_I16_BUILD_BYTES: usize = 64 * 1024 * 1024;

/// Build-only contiguous image for exact f32 construction.
///
/// Generation builds commonly read many immutable segment mappings through a
/// [`PartitionedVectorStorage`]. Resolving that partition on every graph score
/// puts storage indirection in the multi-billion-call distance loop and also
/// prevents indexed batch kernels from seeing one row-major matrix. Materialize
/// the canonical values once into a private, file-backed staging mapping. The
/// base storage remains retained for metric metadata, while graph construction
/// and later artifact serialization consume this exact bitwise image.
struct ExactF32BuildVectorStorage {
    /// Retained only when the source was already one contiguous image. A
    /// materialized generation workspace deliberately drops all source page
    /// mappings after the bitwise copy so the OS cannot keep both complete
    /// f32 images resident during the graph build.
    base: Option<Arc<dyn VectorStorage>>,
    materialized_vectors: Option<Mmap>,
    cosine_inverse_norms: Option<CosineInverseNorms>,
    dimension: usize,
    count: usize,
}

impl ExactF32BuildVectorStorage {
    fn prepare(
        base: Arc<dyn VectorStorage>,
        workspace_dir: Option<&Path>,
    ) -> Result<Arc<dyn VectorStorage>> {
        if base.contiguous_vectors().is_some() {
            let dimension = base.vector_dim();
            let count = base.num_vectors();
            let cosine_inverse_norms = base.cosine_inverse_norms().cloned();
            return Ok(Arc::new(Self {
                base: Some(base),
                materialized_vectors: None,
                cosine_inverse_norms,
                dimension,
                count,
            }));
        }
        let Some(workspace_dir) = workspace_dir else {
            // Low-level and bounded inline callers may deliberately operate
            // without a staging directory. They retain the correct partitioned
            // path; production sidecar generations always provide one.
            return Ok(base);
        };
        let dimension = base.vector_dim();
        let count = base.num_vectors();
        let value_count = count.checked_mul(dimension).ok_or_else(|| {
            paro_common::error::out_of_range("HNSW exact-f32 build workspace shape overflow")
        })?;
        let byte_len = value_count
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(|| {
                paro_common::error::out_of_range("HNSW exact-f32 build workspace byte overflow")
            })?;
        if byte_len == 0 {
            return Err(paro_common::error::invalid_input(
                "HNSW exact-f32 build workspace must not be empty",
            ));
        }

        std::fs::create_dir_all(workspace_dir)?;
        let file = tempfile::tempfile_in(workspace_dir)?;
        file.set_len(u64::try_from(byte_len).map_err(|_| {
            paro_common::error::out_of_range("HNSW exact-f32 build workspace exceeds u64")
        })?)?;
        // SAFETY: the private temporary file has the exact validated length
        // and is never concurrently resized or mapped by another writer.
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        if (mmap.as_ptr() as usize) % std::mem::align_of::<f32>() != 0 {
            return Err(paro_common::error::internal(
                "HNSW exact-f32 build workspace is not f32-aligned",
            ));
        }
        // SAFETY: mmap alignment was checked above, its length is exactly
        // `value_count * size_of::<f32>()`, and this mutable slice is confined
        // to the preparation phase before the mapping becomes read-only.
        let output =
            unsafe { std::slice::from_raw_parts_mut(mmap.as_mut_ptr().cast::<f32>(), value_count) };
        let mut written = 0usize;
        base.try_for_each_contiguous_chunk(&mut |values| {
            let end = written.checked_add(values.len()).ok_or_else(|| {
                paro_common::error::out_of_range("HNSW exact-f32 build copy overflow")
            })?;
            let destination = output.get_mut(written..end).ok_or_else(|| {
                paro_common::error::data_corrupted(
                    "HNSW exact-f32 build input exceeds declared cardinality",
                )
            })?;
            destination.copy_from_slice(values);
            written = end;
            Ok(())
        })?;
        if written != value_count {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW exact-f32 build copied {written} values, expected {value_count}"
            )));
        }
        let cosine_inverse_norms = base.cosine_inverse_norms().cloned();

        Ok(Arc::new(Self {
            base: None,
            materialized_vectors: Some(mmap.make_read_only()?),
            cosine_inverse_norms,
            dimension,
            count,
        }))
    }
}

impl VectorStorage for ExactF32BuildVectorStorage {
    #[inline]
    fn get_vector(&self, idx: PointOffset) -> &[f32] {
        let start = idx as usize * self.dimension;
        &self
            .contiguous_vectors()
            .expect("exact-f32 build storage is contiguous")[start..start + self.dimension]
    }

    fn contiguous_vectors(&self) -> Option<&[f32]> {
        let Some(vectors) = self.materialized_vectors.as_ref() else {
            return self
                .base
                .as_ref()
                .and_then(|base| base.contiguous_vectors());
        };
        // SAFETY: `prepare` proves page alignment and exact f32 row-major
        // length before converting the private mapping to read-only.
        Some(unsafe {
            std::slice::from_raw_parts(vectors.as_ptr().cast::<f32>(), self.count * self.dimension)
        })
    }

    fn num_vectors(&self) -> usize {
        self.count
    }

    fn vector_dim(&self) -> usize {
        self.dimension
    }

    fn cosine_inverse_norms(&self) -> Option<&CosineInverseNorms> {
        self.cosine_inverse_norms.as_ref()
    }

    fn is_mmap_backed(&self) -> bool {
        self.materialized_vectors.is_some()
            || self.base.as_ref().is_some_and(|base| base.is_mmap_backed())
    }

    fn construction_similarities(
        &self,
        distance: DistanceMetric,
        left: PointOffset,
        rights: &[PointOffset],
        scores: &mut [f32],
    ) {
        assert!(
            scores.len() >= rights.len(),
            "construction score buffer is shorter than point batch"
        );
        if distance != DistanceMetric::Euclidean {
            for (&right, score) in rights.iter().zip(scores.iter_mut()) {
                *score = self.construction_similarity(distance, left, right);
            }
            return;
        }

        let vectors = self
            .contiguous_vectors()
            .unwrap_or_else(|| unreachable!("exact-f32 build storage is contiguous"));
        paro_common::distance::l2_squared_batch_indexed(
            self.get_vector(left),
            vectors,
            self.dimension,
            rights,
            scores,
        );
        for score in scores.iter_mut().take(rights.len()) {
            *score = -*score;
        }
    }
}

enum WritableI16BuildBacking {
    Owned(Vec<u8>),
    Mmap(MmapMut),
}

impl WritableI16BuildBacking {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Mmap(mmap) => mmap,
        }
    }

    fn freeze(self) -> Result<I16BuildBacking> {
        match self {
            Self::Owned(bytes) => Ok(I16BuildBacking::Owned(bytes.into_boxed_slice())),
            Self::Mmap(mmap) => Ok(I16BuildBacking::Mmap(mmap.make_read_only()?)),
        }
    }
}

pub(crate) enum I16BuildBacking {
    Owned(Box<[u8]>),
    Bytes(Bytes),
    Mmap(Mmap),
    MmapRange {
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
    },
}

impl I16BuildBacking {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Bytes(bytes) => bytes,
            Self::Mmap(mmap) => mmap,
            Self::MmapRange { mmap, offset, len } => &mmap[*offset..*offset + *len],
        }
    }
}

#[derive(Clone, Copy)]
pub struct I16RoutingView<'a> {
    pub codes: &'a [u8],
    pub source_dimension: usize,
    pub row_stride_bytes: usize,
    pub scales: &'a [f32],
    pub selected_dimensions: &'a [usize],
    pub inverse_norms: Option<&'a CosineInverseNorms>,
}

/// Build-only symmetric scalar encoding over canonical base vectors.
///
/// A single scale keeps every supported metric symmetric and makes pair
/// scoring independent of operand order. Large code matrices are file-backed
/// in the caller's sidecar staging directory; only explicitly bounded inline
/// builds may retain codes on the heap.
pub(crate) struct SymmetricI16BuildVectorStorage {
    base: Arc<dyn VectorStorage>,
    codes: I16BuildBacking,
    dimension: usize,
    row_stride_bytes: usize,
    selected_dimensions: Box<[usize]>,
    scales: Box<[f32]>,
    scale_squares: Box<[f32]>,
    routing_inverse_norms: Option<CosineInverseNorms>,
}

impl SymmetricI16BuildVectorStorage {
    fn prepare(
        base: Arc<dyn VectorStorage>,
        routing_dimensions: usize,
        build_seed: u64,
        workspace_dir: Option<&Path>,
    ) -> Result<Arc<dyn VectorStorage>> {
        let dimension = base.vector_dim();
        if dimension == 0 {
            return Err(paro_common::error::invalid_input(
                "HNSW symmetric-i16 build encoding requires a non-zero dimension",
            ));
        }
        if routing_dimensions == 0 || routing_dimensions > dimension {
            return Err(paro_common::error::invalid_input(format!(
                "HNSW symmetric-i16 routing dimension {routing_dimensions} is invalid for base dimension {dimension}"
            )));
        }
        let mut minima = vec![f32::INFINITY; dimension];
        let mut maxima = vec![f32::NEG_INFINITY; dimension];
        base.try_for_each_contiguous_chunk(&mut |values| {
            if values.len() % dimension != 0 {
                return Err(paro_common::error::data_corrupted(
                    "HNSW build vector chunk contains a partial row",
                ));
            }
            for vector in values.chunks_exact(dimension) {
                for (source_dimension, &value) in vector.iter().enumerate() {
                    if !value.is_finite() {
                        return Err(paro_common::error::invalid_input(
                            "symmetric-i16 HNSW construction requires finite vector values",
                        ));
                    }
                    minima[source_dimension] = minima[source_dimension].min(value);
                    maxima[source_dimension] = maxima[source_dimension].max(value);
                }
            }
            Ok(())
        })?;
        let selected_dimensions =
            multiscale_routing_dimensions(&minima, &maxima, routing_dimensions, build_seed);
        let scales = selected_dimensions
            .iter()
            .map(|&source_dimension| {
                let max_abs = minima[source_dimension]
                    .abs()
                    .max(maxima[source_dimension].abs());
                if !max_abs.is_finite() || max_abs == 0.0 {
                    1.0
                } else {
                    max_abs / 32_767.0
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        if scales.iter().any(|scale| {
            let square = *scale * *scale;
            !square.is_finite() || square == 0.0
        }) {
            return Err(paro_common::error::invalid_input(
                "symmetric-i16 HNSW routing scale cannot be squared stably",
            ));
        }
        let scale_squares = scales
            .iter()
            .map(|scale| scale * scale)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let encoded_dimension = routing_dimensions
            .checked_add(I16_BUILD_ALIGNMENT - 1)
            .map(|value| value / I16_BUILD_ALIGNMENT * I16_BUILD_ALIGNMENT)
            .ok_or_else(|| {
                paro_common::error::out_of_range("HNSW i16 encoded dimension overflow")
            })?;
        let row_stride_bytes = encoded_dimension
            .checked_mul(std::mem::size_of::<i16>())
            .ok_or_else(|| paro_common::error::out_of_range("HNSW i16 row stride overflow"))?;
        let encoded_len = base
            .num_vectors()
            .checked_mul(row_stride_bytes)
            .ok_or_else(|| paro_common::error::out_of_range("HNSW i16 build workspace overflow"))?;

        let mut backing = if encoded_len <= MAX_OWNED_I16_BUILD_BYTES {
            WritableI16BuildBacking::Owned(vec![0; encoded_len])
        } else {
            let workspace_dir = workspace_dir.ok_or_else(|| {
                paro_common::error::configuration_limit_exceeded(format!(
                    "HNSW symmetric-i16 build requires a governed staging directory for its {}-byte routing workspace",
                    encoded_len
                ))
            })?;
            std::fs::create_dir_all(workspace_dir)?;
            let file = tempfile::tempfile_in(workspace_dir)?;
            file.set_len(u64::try_from(encoded_len).map_err(|_| {
                paro_common::error::out_of_range("HNSW i16 build workspace exceeds u64")
            })?)?;
            // SAFETY: the private temporary file has the exact validated
            // length and is never concurrently resized or mutated elsewhere.
            let mmap = unsafe { MmapMut::map_mut(&file)? };
            WritableI16BuildBacking::Mmap(mmap)
        };

        let output = backing.as_mut_slice();
        let mut routing_inverse_norms = base
            .cosine_inverse_norms()
            .map(|_| vec![0.0; base.num_vectors()]);
        let mut encoded_row = 0usize;
        base.try_for_each_contiguous_chunk(&mut |values| {
            if values.len() % dimension != 0 {
                return Err(paro_common::error::data_corrupted(
                    "HNSW build vector chunk contains a partial row",
                ));
            }
            for vector in values.chunks_exact(dimension) {
                let start = encoded_row.checked_mul(row_stride_bytes).ok_or_else(|| {
                    paro_common::error::out_of_range("HNSW i16 row offset overflow")
                })?;
                let row = &mut output[start..start + row_stride_bytes];
                if let Some(norms) = routing_inverse_norms.as_mut() {
                    let mut squared_norm = 0.0f32;
                    for ((encoded, &source_dimension), &scale) in row
                        .chunks_exact_mut(std::mem::size_of::<i16>())
                        .take(routing_dimensions)
                        .zip(selected_dimensions.iter())
                        .zip(scales.iter())
                    {
                        let value = vector[source_dimension];
                        let quantized = (value / scale).round().clamp(-32_767.0, 32_767.0) as i16;
                        encoded.copy_from_slice(&quantized.to_le_bytes());
                        let reconstructed = f32::from(quantized) * scale;
                        squared_norm += reconstructed * reconstructed;
                    }
                    norms[encoded_row] = if squared_norm < f32::EPSILON {
                        0.0
                    } else {
                        squared_norm.sqrt().recip()
                    };
                } else {
                    for ((encoded, &source_dimension), &scale) in row
                        .chunks_exact_mut(std::mem::size_of::<i16>())
                        .take(routing_dimensions)
                        .zip(selected_dimensions.iter())
                        .zip(scales.iter())
                    {
                        let value = vector[source_dimension];
                        let quantized = (value / scale).round().clamp(-32_767.0, 32_767.0) as i16;
                        encoded.copy_from_slice(&quantized.to_le_bytes());
                    }
                }
                row[routing_dimensions * std::mem::size_of::<i16>()..].fill(0);
                encoded_row += 1;
            }
            Ok(())
        })?;
        if encoded_row != base.num_vectors() {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW i16 build encoded {encoded_row} rows, expected {}",
                base.num_vectors()
            )));
        }

        Ok(Arc::new(Self {
            base,
            codes: backing.freeze()?,
            dimension,
            row_stride_bytes,
            selected_dimensions,
            scales,
            scale_squares,
            routing_inverse_norms: routing_inverse_norms
                .map(|values| CosineInverseNorms::Owned(Arc::from(values))),
        }))
    }

    pub(crate) fn from_persisted(
        base: Arc<dyn VectorStorage>,
        codes: I16BuildBacking,
        selected_dimensions: Box<[usize]>,
        scales: Box<[f32]>,
        routing_inverse_norms: Option<CosineInverseNorms>,
    ) -> Result<Arc<dyn VectorStorage>> {
        let dimension = base.vector_dim();
        let routing_dimensions = selected_dimensions.len();
        if routing_dimensions == 0 || routing_dimensions > dimension {
            return Err(paro_common::error::data_corrupted(format!(
                "persisted HNSW routing dimension {routing_dimensions} is invalid for base dimension {dimension}"
            )));
        }
        if scales.len() != routing_dimensions
            || scales.iter().any(|scale| {
                let square = *scale * *scale;
                !scale.is_finite() || *scale <= 0.0 || !square.is_finite() || square == 0.0
            })
        {
            return Err(paro_common::error::data_corrupted(
                "persisted HNSW routing scales must be finite, positive, square-stable, and cardinality-aligned",
            ));
        }
        if selected_dimensions
            .iter()
            .any(|source| *source >= dimension)
            || selected_dimensions
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(paro_common::error::data_corrupted(
                "persisted HNSW routing dimensions must be unique, source-ordered, and in range",
            ));
        }
        let encoded_dimension = routing_dimensions
            .checked_add(I16_BUILD_ALIGNMENT - 1)
            .map(|value| value / I16_BUILD_ALIGNMENT * I16_BUILD_ALIGNMENT)
            .ok_or_else(|| paro_common::error::data_corrupted("HNSW routing dimension overflow"))?;
        let row_stride_bytes = encoded_dimension
            .checked_mul(std::mem::size_of::<i16>())
            .ok_or_else(|| paro_common::error::data_corrupted("HNSW routing row overflow"))?;
        let expected_len = base
            .num_vectors()
            .checked_mul(row_stride_bytes)
            .ok_or_else(|| {
                paro_common::error::data_corrupted("HNSW routing code length overflow")
            })?;
        if codes.as_slice().len() != expected_len {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW routing code length mismatch: expected {expected_len}, got {}",
                codes.as_slice().len()
            )));
        }
        match (&routing_inverse_norms, base.cosine_inverse_norms()) {
            (Some(norms), Some(_)) if norms.len() == base.num_vectors() => {}
            (None, None) => {}
            (Some(norms), Some(_)) => {
                return Err(paro_common::error::data_corrupted(format!(
                    "HNSW routing inverse norm count mismatch: expected {}, got {}",
                    base.num_vectors(),
                    norms.len()
                )));
            }
            _ => {
                return Err(paro_common::error::data_corrupted(
                    "HNSW routing inverse norm presence disagrees with the metric",
                ));
            }
        }
        Ok(Arc::new(Self {
            base,
            codes,
            dimension,
            row_stride_bytes,
            selected_dimensions,
            scale_squares: scales
                .iter()
                .map(|scale| scale * scale)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            scales,
            routing_inverse_norms,
        }))
    }

    #[inline]
    fn code(&self, point: PointOffset) -> &[u8] {
        let start = point as usize * self.row_stride_bytes;
        &self.codes.as_slice()[start..start + self.row_stride_bytes]
    }
}

impl VectorStorage for SymmetricI16BuildVectorStorage {
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
        self.dimension
    }

    fn cosine_inverse_norms(&self) -> Option<&CosineInverseNorms> {
        self.base.cosine_inverse_norms()
    }

    fn is_mmap_backed(&self) -> bool {
        self.base.is_mmap_backed()
    }

    fn i16_routing_view(&self) -> Option<I16RoutingView<'_>> {
        Some(I16RoutingView {
            codes: self.codes.as_slice(),
            source_dimension: self.dimension,
            row_stride_bytes: self.row_stride_bytes,
            scales: &self.scales,
            selected_dimensions: &self.selected_dimensions,
            inverse_norms: self.routing_inverse_norms.as_ref(),
        })
    }

    #[inline]
    fn prefetch_construction_point(&self, idx: PointOffset) {
        paro_common::distance::prefetch_bytes_read(self.code(idx));
    }

    #[inline]
    fn construction_similarity(
        &self,
        distance: DistanceMetric,
        left: PointOffset,
        right: PointOffset,
    ) -> f32 {
        let left_code = self.code(left);
        let right_code = self.code(right);
        match distance {
            DistanceMetric::Euclidean => {
                -weighted_i16_l2_squared(left_code, right_code, &self.scale_squares)
            }
            DistanceMetric::DotProduct => {
                weighted_i16_dot_product(left_code, right_code, &self.scale_squares)
            }
            DistanceMetric::Cosine => {
                let norms = self.routing_inverse_norms.as_ref().unwrap_or_else(|| {
                    unreachable!("symmetric-i16 routing norms are prepared once")
                });
                weighted_i16_dot_product(left_code, right_code, &self.scale_squares)
                    * (norms.value(left) * norms.value(right))
            }
            DistanceMetric::Manhattan => {
                -weighted_i16_l1_distance(left_code, right_code, &self.scales)
            }
        }
    }
}

pub(crate) fn prepare_build_vector_storage(
    base: Arc<dyn VectorStorage>,
    encoding: super::HnswBuildVectorEncoding,
    build_seed: u64,
    workspace_dir: Option<&Path>,
) -> Result<Arc<dyn VectorStorage>> {
    match encoding {
        super::HnswBuildVectorEncoding::ExactF32 => {
            ExactF32BuildVectorStorage::prepare(base, workspace_dir)
        }
        super::HnswBuildVectorEncoding::SymmetricI16 { routing_dimensions } => {
            SymmetricI16BuildVectorStorage::prepare(
                base,
                usize::from(routing_dimensions.get()),
                build_seed,
                workspace_dir,
            )
        }
    }
}

/// Dense local point ids projected onto an immutable parent construction
/// space. Predicate-local HNSW graphs need their own contiguous ids, but they
/// must use the exact same routing metric as the generation graph. Keeping an
/// id map avoids copying raw f32 rows and, more importantly, prevents local
/// topology from silently being built under a different metric.
pub(crate) struct PointRemappedBuildVectorStorage {
    base: Arc<dyn VectorStorage>,
    global_points: Arc<[PointOffset]>,
    local_cosine_inverse_norms: Option<CosineInverseNorms>,
}

impl PointRemappedBuildVectorStorage {
    pub(crate) fn try_new(
        base: Arc<dyn VectorStorage>,
        global_points: Arc<[PointOffset]>,
    ) -> Result<Self> {
        if global_points
            .iter()
            .any(|&point| point as usize >= base.num_vectors())
        {
            return Err(paro_common::error::data_corrupted(
                "HNSW remapped construction point exceeds the parent vector domain",
            ));
        }
        let local_cosine_inverse_norms = base.cosine_inverse_norms().map(|norms| {
            CosineInverseNorms::Owned(Arc::from(
                global_points
                    .iter()
                    .map(|&point| norms.value(point))
                    .collect::<Vec<_>>(),
            ))
        });
        Ok(Self {
            base,
            global_points,
            local_cosine_inverse_norms,
        })
    }

    #[inline]
    fn global_point(&self, local_point: PointOffset) -> PointOffset {
        self.global_points[local_point as usize]
    }
}

impl VectorStorage for PointRemappedBuildVectorStorage {
    fn get_vector(&self, idx: PointOffset) -> &[f32] {
        self.base.get_vector(self.global_point(idx))
    }

    fn try_for_each_contiguous_chunk(
        &self,
        visitor: &mut dyn FnMut(&[f32]) -> Result<()>,
    ) -> Result<()> {
        for &point in self.global_points.iter() {
            visitor(self.base.get_vector(point))?;
        }
        Ok(())
    }

    fn num_vectors(&self) -> usize {
        self.global_points.len()
    }

    fn vector_dim(&self) -> usize {
        self.base.vector_dim()
    }

    fn cosine_inverse_norms(&self) -> Option<&CosineInverseNorms> {
        self.local_cosine_inverse_norms.as_ref()
    }

    fn is_mmap_backed(&self) -> bool {
        self.base.is_mmap_backed()
    }

    #[inline]
    fn construction_similarity(
        &self,
        distance: DistanceMetric,
        left: PointOffset,
        right: PointOffset,
    ) -> f32 {
        self.base.construction_similarity(
            distance,
            self.global_point(left),
            self.global_point(right),
        )
    }
}

fn multiscale_routing_dimensions(
    minima: &[f32],
    maxima: &[f32],
    routing_dimensions: usize,
    build_seed: u64,
) -> Box<[usize]> {
    let dimension = minima.len();
    debug_assert_eq!(dimension, maxima.len());
    if routing_dimensions == dimension {
        return (0..dimension).collect::<Vec<_>>().into_boxed_slice();
    }

    let mut by_range = (0..dimension)
        .map(|source_dimension| {
            let range = maxima[source_dimension] - minima[source_dimension];
            let range = if range.is_finite() && range > 0.0 {
                range
            } else {
                0.0
            };
            (range, source_dimension)
        })
        .collect::<Vec<_>>();
    by_range.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });

    let tail = (routing_dimensions / 4).max(1);
    let mut selected = vec![false; dimension];
    let mut selected_count = 0usize;
    for &(_, source_dimension) in by_range.iter().filter(|(range, _)| *range > 0.0).take(tail) {
        selected[source_dimension] = true;
        selected_count += 1;
    }
    for &(_, source_dimension) in by_range.iter().rev().take(tail) {
        if !selected[source_dimension] {
            selected[source_dimension] = true;
            selected_count += 1;
        }
    }
    let mut seeded = (0..dimension)
        .map(|source_dimension| {
            (
                splitmix64(
                    build_seed
                        ^ (source_dimension as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93)
                        ^ 0xA076_1D64_78BD_642F,
                ),
                source_dimension,
            )
        })
        .collect::<Vec<_>>();
    seeded.sort_unstable();
    for (_, source_dimension) in seeded {
        if selected_count == routing_dimensions {
            break;
        }
        if !selected[source_dimension] {
            selected[source_dimension] = true;
            selected_count += 1;
        }
    }

    selected
        .into_iter()
        .enumerate()
        .filter_map(|(source_dimension, selected)| selected.then_some(source_dimension))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

#[inline]
fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

#[inline]
pub(crate) fn weighted_i16_dot_product(left: &[u8], right: &[u8], scale_squares: &[f32]) -> f32 {
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        i16_neon::weighted_dot(left, right, scale_squares)
    }
    #[cfg(not(all(target_arch = "aarch64", target_endian = "little")))]
    weighted_i16_dot_product_scalar(left, right, scale_squares)
}

#[inline]
#[cfg_attr(
    all(target_arch = "aarch64", target_endian = "little"),
    allow(dead_code)
)]
fn weighted_i16_dot_product_scalar(left: &[u8], right: &[u8], scale_squares: &[f32]) -> f32 {
    let len = scale_squares.len().min(left.len() / 2).min(right.len() / 2);
    deterministic_four_lane_reduce(len, |index, accumulator| {
        let product = f32::from(read_i16_le(left, index)) * f32::from(read_i16_le(right, index));
        product.mul_add(scale_squares[index], accumulator)
    })
}

#[inline]
pub(crate) fn weighted_i16_l2_squared(left: &[u8], right: &[u8], scale_squares: &[f32]) -> f32 {
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        i16_neon::weighted_l2(left, right, scale_squares)
    }
    #[cfg(not(all(target_arch = "aarch64", target_endian = "little")))]
    weighted_i16_l2_squared_scalar(left, right, scale_squares)
}

#[inline]
#[cfg_attr(
    all(target_arch = "aarch64", target_endian = "little"),
    allow(dead_code)
)]
fn weighted_i16_l2_squared_scalar(left: &[u8], right: &[u8], scale_squares: &[f32]) -> f32 {
    let len = scale_squares.len().min(left.len() / 2).min(right.len() / 2);
    deterministic_four_lane_reduce(len, |index, accumulator| {
        let delta = f32::from(read_i16_le(left, index)) - f32::from(read_i16_le(right, index));
        (delta * delta).mul_add(scale_squares[index], accumulator)
    })
}

#[inline]
pub(crate) fn weighted_i16_l1_distance(left: &[u8], right: &[u8], scales: &[f32]) -> f32 {
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        i16_neon::weighted_l1(left, right, scales)
    }
    #[cfg(not(all(target_arch = "aarch64", target_endian = "little")))]
    weighted_i16_l1_distance_scalar(left, right, scales)
}

#[inline]
#[cfg_attr(
    all(target_arch = "aarch64", target_endian = "little"),
    allow(dead_code)
)]
fn weighted_i16_l1_distance_scalar(left: &[u8], right: &[u8], scales: &[f32]) -> f32 {
    let len = scales.len().min(left.len() / 2).min(right.len() / 2);
    deterministic_four_lane_reduce(len, |index, accumulator| {
        let delta = (i32::from(read_i16_le(left, index)) - i32::from(read_i16_le(right, index)))
            .unsigned_abs() as f32;
        delta.mul_add(scales[index], accumulator)
    })
}

/// Score an unquantized query against one persisted symmetric-i16 routing row.
///
/// Query values deliberately remain in the original f32 domain. Quantizing a
/// query with the artifact's build-time range would clamp out-of-distribution
/// inputs and change their geometry; only the immutable point image is lossy.
#[inline]
pub(crate) fn f32_query_i16_dot_product(query: &[f32], code: &[u8], scales: &[f32]) -> f32 {
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        i16_neon::query_dot(query, code, scales)
    }
    #[cfg(not(all(target_arch = "aarch64", target_endian = "little")))]
    f32_query_i16_dot_product_scalar(query, code, scales)
}

#[inline]
#[cfg_attr(
    all(target_arch = "aarch64", target_endian = "little"),
    allow(dead_code)
)]
fn f32_query_i16_dot_product_scalar(query: &[f32], code: &[u8], scales: &[f32]) -> f32 {
    let len = query.len().min(scales.len()).min(code.len() / 2);
    deterministic_four_lane_reduce(len, |index, accumulator| {
        let value = f32::from(read_i16_le(code, index)) * scales[index];
        query[index].mul_add(value, accumulator)
    })
}

#[inline]
pub(crate) fn f32_query_i16_l2_squared(query: &[f32], code: &[u8], scales: &[f32]) -> f32 {
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        i16_neon::query_l2(query, code, scales)
    }
    #[cfg(not(all(target_arch = "aarch64", target_endian = "little")))]
    f32_query_i16_l2_squared_scalar(query, code, scales)
}

#[inline]
#[cfg_attr(
    all(target_arch = "aarch64", target_endian = "little"),
    allow(dead_code)
)]
fn f32_query_i16_l2_squared_scalar(query: &[f32], code: &[u8], scales: &[f32]) -> f32 {
    let len = query.len().min(scales.len()).min(code.len() / 2);
    deterministic_four_lane_reduce(len, |index, accumulator| {
        let value = f32::from(read_i16_le(code, index)) * scales[index];
        let delta = query[index] - value;
        delta.mul_add(delta, accumulator)
    })
}

#[inline]
pub(crate) fn f32_query_i16_l1_distance(query: &[f32], code: &[u8], scales: &[f32]) -> f32 {
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        i16_neon::query_l1(query, code, scales)
    }
    #[cfg(not(all(target_arch = "aarch64", target_endian = "little")))]
    f32_query_i16_l1_distance_scalar(query, code, scales)
}

#[inline]
#[cfg_attr(
    all(target_arch = "aarch64", target_endian = "little"),
    allow(dead_code)
)]
fn f32_query_i16_l1_distance_scalar(query: &[f32], code: &[u8], scales: &[f32]) -> f32 {
    let len = query.len().min(scales.len()).min(code.len() / 2);
    deterministic_four_lane_reduce(len, |index, accumulator| {
        let value = f32::from(read_i16_le(code, index)) * scales[index];
        accumulator + (query[index] - value).abs()
    })
}

/// Portable arithmetic contract shared by scalar and SIMD compact scoring.
///
/// Four independent accumulators consume two four-lane groups per iteration,
/// followed by a fixed pairwise reduction. Keeping this order independent of
/// the host ISA makes build-contract version 15 describe one topology rather
/// than an architecture-dependent family of graphs.
#[inline]
fn deterministic_four_lane_reduce(
    len: usize,
    mut accumulate: impl FnMut(usize, f32) -> f32,
) -> f32 {
    let mut lanes = [0.0_f32; 4];
    let mut offset = 0usize;
    while offset + 8 <= len {
        for lane in 0..4 {
            lanes[lane] = accumulate(offset + lane, lanes[lane]);
        }
        for lane in 0..4 {
            lanes[lane] = accumulate(offset + 4 + lane, lanes[lane]);
        }
        offset += 8;
    }
    let mut scalar = (lanes[0] + lanes[1]) + (lanes[2] + lanes[3]);
    while offset < len {
        scalar = accumulate(offset, scalar);
        offset += 1;
    }
    scalar
}

#[inline]
fn read_i16_le(bytes: &[u8], index: usize) -> i16 {
    let start = index * std::mem::size_of::<i16>();
    i16::from_le_bytes([bytes[start], bytes[start + 1]])
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
mod i16_neon {
    use std::arch::aarch64::*;

    #[inline]
    pub(super) fn weighted_dot(left: &[u8], right: &[u8], scales: &[f32]) -> f32 {
        // SAFETY: AArch64 guarantees NEON, byte rows are little-endian on this
        // target, and the kernel bounds every unaligned load by the shortest
        // logical input.
        unsafe { weighted_dot_inner(left, right, scales) }
    }

    #[target_feature(enable = "neon")]
    unsafe fn weighted_dot_inner(left: &[u8], right: &[u8], scales: &[f32]) -> f32 {
        let len = scales.len().min(left.len() / 2).min(right.len() / 2);
        let mut offset = 0usize;
        let mut sum = vdupq_n_f32(0.0);
        while offset + 8 <= len {
            // SAFETY: the loop condition proves eight i16 values and the
            // corresponding eight scales remain in every input.
            let (left_codes, right_codes, low_scales, high_scales) = unsafe {
                (
                    vld1q_s16(left.as_ptr().add(offset * 2).cast()),
                    vld1q_s16(right.as_ptr().add(offset * 2).cast()),
                    vld1q_f32(scales.as_ptr().add(offset)),
                    vld1q_f32(scales.as_ptr().add(offset + 4)),
                )
            };
            let left_low = vcvtq_f32_s32(vmovl_s16(vget_low_s16(left_codes)));
            let left_high = vcvtq_f32_s32(vmovl_high_s16(left_codes));
            let right_low = vcvtq_f32_s32(vmovl_s16(vget_low_s16(right_codes)));
            let right_high = vcvtq_f32_s32(vmovl_high_s16(right_codes));
            sum = vfmaq_f32(sum, vmulq_f32(left_low, right_low), low_scales);
            sum = vfmaq_f32(sum, vmulq_f32(left_high, right_high), high_scales);
            offset += 8;
        }
        // SAFETY: `sum` is a fully initialized four-lane accumulator.
        let mut scalar = unsafe { reduce_sum(sum) };
        while offset < len {
            let left = i16::from_le_bytes([left[offset * 2], left[offset * 2 + 1]]);
            let right = i16::from_le_bytes([right[offset * 2], right[offset * 2 + 1]]);
            let product = f32::from(left) * f32::from(right);
            scalar = product.mul_add(scales[offset], scalar);
            offset += 1;
        }
        scalar
    }

    #[inline]
    pub(super) fn weighted_l2(left: &[u8], right: &[u8], scales: &[f32]) -> f32 {
        // SAFETY: see `weighted_dot`; this kernel uses the same bounded rows.
        unsafe { weighted_l2_inner(left, right, scales) }
    }

    #[target_feature(enable = "neon")]
    unsafe fn weighted_l2_inner(left: &[u8], right: &[u8], scales: &[f32]) -> f32 {
        let len = scales.len().min(left.len() / 2).min(right.len() / 2);
        let mut offset = 0usize;
        let mut sum = vdupq_n_f32(0.0);
        while offset + 8 <= len {
            // SAFETY: the loop condition proves every unaligned load range.
            let (left_codes, right_codes, low_scales, high_scales) = unsafe {
                (
                    vld1q_s16(left.as_ptr().add(offset * 2).cast()),
                    vld1q_s16(right.as_ptr().add(offset * 2).cast()),
                    vld1q_f32(scales.as_ptr().add(offset)),
                    vld1q_f32(scales.as_ptr().add(offset + 4)),
                )
            };
            let low_delta = vsubq_f32(
                vcvtq_f32_s32(vmovl_s16(vget_low_s16(left_codes))),
                vcvtq_f32_s32(vmovl_s16(vget_low_s16(right_codes))),
            );
            let high_delta = vsubq_f32(
                vcvtq_f32_s32(vmovl_high_s16(left_codes)),
                vcvtq_f32_s32(vmovl_high_s16(right_codes)),
            );
            sum = vfmaq_f32(sum, vmulq_f32(low_delta, low_delta), low_scales);
            sum = vfmaq_f32(sum, vmulq_f32(high_delta, high_delta), high_scales);
            offset += 8;
        }
        // SAFETY: `sum` is a fully initialized four-lane accumulator.
        let mut scalar = unsafe { reduce_sum(sum) };
        while offset < len {
            let left = i16::from_le_bytes([left[offset * 2], left[offset * 2 + 1]]);
            let right = i16::from_le_bytes([right[offset * 2], right[offset * 2 + 1]]);
            let delta = f32::from(left) - f32::from(right);
            scalar = (delta * delta).mul_add(scales[offset], scalar);
            offset += 1;
        }
        scalar
    }

    #[inline]
    pub(super) fn weighted_l1(left: &[u8], right: &[u8], scales: &[f32]) -> f32 {
        // SAFETY: see `weighted_dot`; this kernel uses the same bounded rows.
        unsafe { weighted_l1_inner(left, right, scales) }
    }

    #[target_feature(enable = "neon")]
    unsafe fn weighted_l1_inner(left: &[u8], right: &[u8], scales: &[f32]) -> f32 {
        let len = scales.len().min(left.len() / 2).min(right.len() / 2);
        let mut offset = 0usize;
        let mut sum = vdupq_n_f32(0.0);
        while offset + 8 <= len {
            // SAFETY: the loop condition proves every unaligned load range.
            let (left_codes, right_codes, low_scales, high_scales) = unsafe {
                (
                    vld1q_s16(left.as_ptr().add(offset * 2).cast()),
                    vld1q_s16(right.as_ptr().add(offset * 2).cast()),
                    vld1q_f32(scales.as_ptr().add(offset)),
                    vld1q_f32(scales.as_ptr().add(offset + 4)),
                )
            };
            let low_delta = vabsq_f32(vsubq_f32(
                vcvtq_f32_s32(vmovl_s16(vget_low_s16(left_codes))),
                vcvtq_f32_s32(vmovl_s16(vget_low_s16(right_codes))),
            ));
            let high_delta = vabsq_f32(vsubq_f32(
                vcvtq_f32_s32(vmovl_high_s16(left_codes)),
                vcvtq_f32_s32(vmovl_high_s16(right_codes)),
            ));
            sum = vfmaq_f32(sum, low_delta, low_scales);
            sum = vfmaq_f32(sum, high_delta, high_scales);
            offset += 8;
        }
        // SAFETY: `sum` is a fully initialized four-lane accumulator.
        let mut scalar = unsafe { reduce_sum(sum) };
        while offset < len {
            let left = i16::from_le_bytes([left[offset * 2], left[offset * 2 + 1]]);
            let right = i16::from_le_bytes([right[offset * 2], right[offset * 2 + 1]]);
            let delta = (i32::from(left) - i32::from(right)).unsigned_abs() as f32;
            scalar = delta.mul_add(scales[offset], scalar);
            offset += 1;
        }
        scalar
    }

    #[inline]
    pub(super) fn query_dot(query: &[f32], code: &[u8], scales: &[f32]) -> f32 {
        // SAFETY: AArch64 guarantees NEON and the inner kernel bounds every
        // load by the shortest logical input.
        unsafe { query_dot_inner(query, code, scales) }
    }

    #[target_feature(enable = "neon")]
    unsafe fn query_dot_inner(query: &[f32], code: &[u8], scales: &[f32]) -> f32 {
        // SAFETY: this function carries the same target feature and forwards
        // the bounded slices unchanged.
        unsafe { query_reduce(query, code, scales, QueryMetric::Dot) }
    }

    #[inline]
    pub(super) fn query_l2(query: &[f32], code: &[u8], scales: &[f32]) -> f32 {
        // SAFETY: see `query_dot`; this kernel uses the same bounded rows.
        unsafe { query_l2_inner(query, code, scales) }
    }

    #[target_feature(enable = "neon")]
    unsafe fn query_l2_inner(query: &[f32], code: &[u8], scales: &[f32]) -> f32 {
        // SAFETY: see `query_dot_inner`.
        unsafe { query_reduce(query, code, scales, QueryMetric::L2) }
    }

    #[inline]
    pub(super) fn query_l1(query: &[f32], code: &[u8], scales: &[f32]) -> f32 {
        // SAFETY: see `query_dot`; this kernel uses the same bounded rows.
        unsafe { query_l1_inner(query, code, scales) }
    }

    #[target_feature(enable = "neon")]
    unsafe fn query_l1_inner(query: &[f32], code: &[u8], scales: &[f32]) -> f32 {
        // SAFETY: see `query_dot_inner`.
        unsafe { query_reduce(query, code, scales, QueryMetric::L1) }
    }

    #[derive(Clone, Copy)]
    enum QueryMetric {
        Dot,
        L2,
        L1,
    }

    #[target_feature(enable = "neon")]
    unsafe fn query_reduce(query: &[f32], code: &[u8], scales: &[f32], metric: QueryMetric) -> f32 {
        let len = query.len().min(scales.len()).min(code.len() / 2);
        let mut offset = 0usize;
        let mut sum = vdupq_n_f32(0.0);
        while offset + 8 <= len {
            // SAFETY: the loop condition proves eight codes, query values,
            // and scales remain. Durable code rows are little-endian and this
            // module only compiles for little-endian AArch64.
            let (codes, query_low, query_high, scale_low, scale_high) = unsafe {
                (
                    vld1q_s16(code.as_ptr().add(offset * 2).cast()),
                    vld1q_f32(query.as_ptr().add(offset)),
                    vld1q_f32(query.as_ptr().add(offset + 4)),
                    vld1q_f32(scales.as_ptr().add(offset)),
                    vld1q_f32(scales.as_ptr().add(offset + 4)),
                )
            };
            let code_low = vcvtq_f32_s32(vmovl_s16(vget_low_s16(codes)));
            let code_high = vcvtq_f32_s32(vmovl_high_s16(codes));
            let value_low = vmulq_f32(code_low, scale_low);
            let value_high = vmulq_f32(code_high, scale_high);
            match metric {
                QueryMetric::Dot => {
                    sum = vfmaq_f32(sum, query_low, value_low);
                    sum = vfmaq_f32(sum, query_high, value_high);
                }
                QueryMetric::L2 => {
                    let low_delta = vsubq_f32(query_low, value_low);
                    let high_delta = vsubq_f32(query_high, value_high);
                    sum = vfmaq_f32(sum, low_delta, low_delta);
                    sum = vfmaq_f32(sum, high_delta, high_delta);
                }
                QueryMetric::L1 => {
                    sum = vaddq_f32(sum, vabsq_f32(vsubq_f32(query_low, value_low)));
                    sum = vaddq_f32(sum, vabsq_f32(vsubq_f32(query_high, value_high)));
                }
            }
            offset += 8;
        }
        // SAFETY: `sum` is a fully initialized four-lane accumulator.
        let mut scalar = unsafe { reduce_sum(sum) };
        while offset < len {
            let code = i16::from_le_bytes([code[offset * 2], code[offset * 2 + 1]]);
            let value = f32::from(code) * scales[offset];
            scalar = match metric {
                QueryMetric::Dot => query[offset].mul_add(value, scalar),
                QueryMetric::L2 => {
                    let delta = query[offset] - value;
                    delta.mul_add(delta, scalar)
                }
                QueryMetric::L1 => scalar + (query[offset] - value).abs(),
            };
            offset += 1;
        }
        scalar
    }

    /// Match the portable build-contract reduction order exactly instead of
    /// delegating horizontal addition order to a target intrinsic.
    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn reduce_sum(sum: float32x4_t) -> f32 {
        let mut lanes = [0.0_f32; 4];
        // SAFETY: `lanes` owns space for exactly four f32 values.
        unsafe { vst1q_f32(lanes.as_mut_ptr(), sum) };
        (lanes[0] + lanes[1]) + (lanes[2] + lanes[3])
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
        if dim == 0 {
            return Err(paro_common::error::invalid_input(
                "mmap vector dimension must be non-zero",
            ));
        }
        let size = usize::try_from(size).map_err(|_| {
            paro_common::error::configuration_limit_exceeded(
                "mmap vector range exceeds the addressable process domain",
            )
        })?;
        let vector_bytes = dim
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(|| paro_common::error::out_of_range("mmap vector row width overflow"))?;
        if size == 0 || size % vector_bytes != 0 {
            return Err(paro_common::error::data_corrupted(format!(
                "mmap vector range length {size} is not a non-zero multiple of row width {vector_bytes}"
            )));
        }
        let file = File::open(path)?;
        // We mmap the whole file but only access the range.
        // Alternatively, we could use MapOptions to map a range if supported by the OS.
        // memmap2::MmapOptions::new().offset(offset).len(size).map(&file)
        let mmap = unsafe {
            memmap2::MmapOptions::new()
                .offset(offset)
                .len(size)
                .map(&file)?
        };
        if (mmap.as_ptr() as usize) % std::mem::align_of::<f32>() != 0 {
            return Err(paro_common::error::data_corrupted(format!(
                "mmap vector range offset {offset} is not f32-aligned"
            )));
        }
        #[cfg(unix)]
        {
            // HNSW point lookups are intentionally non-sequential. Prevent
            // the kernel from turning a small beam into large speculative
            // readahead over the base vector artifact.
            let _ = mmap.advise(memmap2::Advice::Random);
        }

        let count = size / vector_bytes;

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
/// Generation sidecars bind this same ordered partition image and never
/// serialize another f32 matrix. Compact artifacts additionally persist graph
/// routing codes; inline artifacts remain self-contained.
pub(crate) struct PartitionedVectorStorage {
    parts: Box<[PartitionedVectorStoragePart]>,
    part_directory: PartitionDirectory,
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
        let part_directory = PartitionDirectory::try_new(parts.iter().map(|part| part.range.end))?;
        Ok(Self {
            parts: parts.into_boxed_slice(),
            part_directory,
            dim,
            count: point_base as usize,
        })
    }

    fn part_for(&self, point_id: u32) -> &PartitionedVectorStoragePart {
        let position = self
            .part_directory
            .part_for(point_id)
            .expect("HNSW construction point id exceeds partitioned vector storage");
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

/// Open every physical data page that belongs to one immutable plain vector
/// column. A [`ColumnMeta::data_page_pointer`] names only the first page; the
/// ordinal index is the durable source of truth for the complete page set.
///
/// Keeping one mmap view per data page avoids copying large base columns while
/// preserving page envelopes and checksums. Callers may concatenate these
/// views through [`PartitionedVectorStorage`] without assuming that a logical
/// column occupies one physical page.
pub(crate) fn open_plain_vector_column_pages(
    path: impl AsRef<Path>,
    column: &ColumnMeta,
    dim: usize,
) -> Result<Vec<Arc<dyn VectorStorage>>> {
    let path = path.as_ref();
    if column.field_type != FieldType::Vector {
        return Err(paro_common::error::data_corrupted(format!(
            "HNSW column {} is not stored as a vector",
            column.column_id
        )));
    }
    if column.encoding != EncodingType::Plain || column.compression != CompressionType::None {
        return Err(paro_common::error::data_corrupted(format!(
            "HNSW column {} requires plain, uncompressed base pages",
            column.column_id
        )));
    }
    if dim == 0 {
        return Err(paro_common::error::invalid_input(
            "HNSW vector dimension must be non-zero",
        ));
    }
    if column.num_rows == 0 {
        return Ok(Vec::new());
    }

    let row_width = dim
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| paro_common::error::out_of_range("HNSW vector row width overflow"))?;
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let ordinal_page = PageIO::read_page(
        &mut file,
        &PageReadOptions::new(column.ordinal_index_pointer).with_codec(column.compression),
    )?;
    if !matches!(ordinal_page.footer, PageFooter::Index(_)) {
        return Err(paro_common::error::data_corrupted(format!(
            "HNSW column {} ordinal pointer does not reference an index page",
            column.column_id
        )));
    }
    let mut ordinal = OrdinalIndexReader::from_bytes(&ordinal_page.body)?;
    ordinal.set_num_rows(column.num_rows);
    let entries = ordinal.entries();
    if entries.is_empty() || entries[0].first_ordinal != 0 {
        return Err(paro_common::error::data_corrupted(format!(
            "HNSW column {} ordinal index does not start at row zero",
            column.column_id
        )));
    }
    if entries[0].page_pointer != column.data_page_pointer {
        return Err(paro_common::error::data_corrupted(format!(
            "HNSW column {} first data page disagrees with its ordinal index",
            column.column_id
        )));
    }

    let mut storages = Vec::<Arc<dyn VectorStorage>>::with_capacity(entries.len());
    let mut expected_first = 0_u64;
    for (position, entry) in entries.iter().enumerate() {
        if entry.first_ordinal != expected_first {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW column {} has non-contiguous data pages: expected row {}, got {}",
                column.column_id, expected_first, entry.first_ordinal
            )));
        }
        let next_first = entries
            .get(position + 1)
            .map_or(column.num_rows, |next| next.first_ordinal);
        let page_rows = next_first.checked_sub(entry.first_ordinal).ok_or_else(|| {
            paro_common::error::data_corrupted(format!(
                "HNSW column {} ordinal index is not monotonic",
                column.column_id
            ))
        })?;
        if page_rows == 0 || page_rows > u64::from(u32::MAX) {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW column {} has invalid data-page row count {page_rows}",
                column.column_id
            )));
        }
        let page_end = entry
            .page_pointer
            .offset
            .checked_add(u64::from(entry.page_pointer.size))
            .ok_or_else(|| paro_common::error::data_corrupted("vector page range overflow"))?;
        if page_end > file_len {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW column {} data page exceeds segment length",
                column.column_id
            )));
        }

        let layout = PageIO::read_page_layout(&mut file, entry.page_pointer)?;
        let data_footer = layout.footer.as_data().ok_or_else(|| {
            paro_common::error::data_corrupted(format!(
                "HNSW column {} ordinal entry does not reference a data page",
                column.column_id
            ))
        })?;
        if data_footer.first_ordinal != entry.first_ordinal || data_footer.num_values != page_rows {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW column {} data-page footer disagrees with its ordinal index",
                column.column_id
            )));
        }
        let vector_bytes = page_rows
            .checked_mul(row_width as u64)
            .ok_or_else(|| paro_common::error::out_of_range("vector page byte length overflow"))?;
        let expected_body = u64::try_from(PLAIN_PAGE_HEADER_SIZE)
            .expect("plain header width fits u64")
            .checked_add(vector_bytes)
            .and_then(|bytes| bytes.checked_add(u64::from(data_footer.nullmap_size)))
            .ok_or_else(|| paro_common::error::out_of_range("vector page body length overflow"))?;
        if layout.body_size as u64 != expected_body
            || u64::from(layout.uncompressed_size) != expected_body
        {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW column {} vector page body length mismatch: expected {expected_body}, got {}",
                column.column_id, layout.body_size
            )));
        }

        file.seek(SeekFrom::Start(entry.page_pointer.offset))?;
        let mut header = [0_u8; PLAIN_PAGE_HEADER_SIZE];
        file.read_exact(&mut header)?;
        if u64::from(u32::from_le_bytes(header)) != page_rows {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW column {} plain-page header row count mismatch",
                column.column_id
            )));
        }
        PageIO::verify_page_checksum_streaming(&mut file, entry.page_pointer)?;

        let values_offset = entry
            .page_pointer
            .offset
            .checked_add(PLAIN_PAGE_HEADER_SIZE as u64)
            .ok_or_else(|| paro_common::error::data_corrupted("vector body offset overflow"))?;
        if values_offset % crate::index::hnsw::HNSW_ARTIFACT_ALIGNMENT as u64 != 0 {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW column {} vector payload offset {values_offset} is not {}-byte aligned",
                column.column_id,
                crate::index::hnsw::HNSW_ARTIFACT_ALIGNMENT,
            )));
        }
        storages.push(Arc::new(MmapVectorStorage::open_range(
            path,
            values_offset,
            vector_bytes,
            dim,
        )?));
        expected_first = next_first;
    }
    if expected_first != column.num_rows {
        return Err(paro_common::error::data_corrupted(format!(
            "HNSW column {} data pages cover {expected_first} rows, expected {}",
            column.column_id, column.num_rows
        )));
    }
    Ok(storages)
}

pub(crate) fn open_plain_vector_column(
    path: impl AsRef<Path>,
    column: &ColumnMeta,
    dim: usize,
) -> Result<Arc<dyn VectorStorage>> {
    let mut pages = open_plain_vector_column_pages(path, column, dim)?;
    match pages.len() {
        0 => Err(paro_common::error::invalid_input(
            "cannot open an empty vector column",
        )),
        1 => Ok(pages.pop().expect("single vector page")),
        _ => Ok(Arc::new(PartitionedVectorStorage::try_new(pages, dim)?)),
    }
}

/// Shared pointer to a VectorStorage.
pub type SharedVectorStorage = Arc<dyn VectorStorage>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowset::column::{ColumnWriter, ColumnWriterOptions, ScalarColumnWriter};
    use paro_common::types::LogicalType;
    use std::io::Write;

    #[test]
    fn partitioned_storage_resolves_vectors_across_physical_boundaries() {
        let first: Arc<dyn VectorStorage> =
            Arc::new(InMemoryVectorStorage::new(vec![1.0, 2.0, 3.0, 4.0], 2));
        let second: Arc<dyn VectorStorage> = Arc::new(InMemoryVectorStorage::new(
            vec![5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            2,
        ));
        let storage = PartitionedVectorStorage::try_new(vec![first, second], 2).unwrap();

        assert_eq!(storage.num_vectors(), 5);
        assert_eq!(storage.get_vector(0), &[1.0, 2.0]);
        assert_eq!(storage.get_vector(1), &[3.0, 4.0]);
        assert_eq!(storage.get_vector(2), &[5.0, 6.0]);
        assert_eq!(storage.get_vector(4), &[9.0, 10.0]);
    }

    #[test]
    fn exact_f32_build_stages_partitioned_input_as_one_bitwise_image() {
        let first: Arc<dyn VectorStorage> = Arc::new(InMemoryVectorStorage::new(
            vec![1.0, -0.0, f32::from_bits(0x7fc0_1234), 4.0],
            2,
        ));
        let second: Arc<dyn VectorStorage> =
            Arc::new(InMemoryVectorStorage::new(vec![5.0, 6.0, -7.0, 8.0], 2));
        let partitioned: Arc<dyn VectorStorage> =
            Arc::new(PartitionedVectorStorage::try_new(vec![first, second], 2).unwrap());
        assert!(partitioned.contiguous_vectors().is_none());
        let workspace = tempfile::tempdir().unwrap();
        let expected = (0..partitioned.num_vectors() as PointOffset)
            .flat_map(|point| {
                partitioned
                    .get_vector(point)
                    .iter()
                    .map(|value| value.to_bits())
            })
            .collect::<Vec<_>>();
        let expected_point_3 = partitioned.get_vector(3).to_vec();
        let source = Arc::downgrade(&partitioned);

        let prepared = prepare_build_vector_storage(
            Arc::clone(&partitioned),
            super::super::HnswBuildVectorEncoding::ExactF32,
            17,
            Some(workspace.path()),
        )
        .unwrap();
        drop(partitioned);

        assert!(prepared.is_mmap_backed());
        assert!(
            source.upgrade().is_none(),
            "materialized exact-f32 builds must release source page mappings"
        );
        let actual = prepared
            .contiguous_vectors()
            .unwrap()
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(prepared.get_vector(3), expected_point_3);
    }

    #[test]
    fn exact_f32_build_batches_euclidean_construction_scores() {
        let vectors = vec![
            1.0_f32, 2.0, 3.0, 4.0, // point 0
            2.0, 4.0, 6.0, 8.0, // point 1
            -1.0, -2.0, -3.0, -4.0, // point 2
            0.5, 1.5, 2.5, 3.5, // point 3
        ];
        let raw: Arc<dyn VectorStorage> = Arc::new(InMemoryVectorStorage::new(vectors, 4));
        let prepared = prepare_build_vector_storage(
            raw,
            super::super::HnswBuildVectorEncoding::ExactF32,
            17,
            None,
        )
        .unwrap();
        let rights = [1, 2, 3];
        let mut scores = [0.0; 3];

        prepared.construction_similarities(DistanceMetric::Euclidean, 0, &rights, &mut scores);

        for (&right, &score) in rights.iter().zip(scores.iter()) {
            let expected = prepared.construction_similarity(DistanceMetric::Euclidean, 0, right);
            assert!((score - expected).abs() <= f32::EPSILON * expected.abs().max(1.0));
        }
    }

    #[test]
    fn symmetric_i16_build_storage_preserves_raw_values_and_metric_order() {
        let vectors = vec![
            1.0_f32, -2.0, 0.5, 3.0, // point 0
            1.1, -1.9, 0.4, 2.8, // point 1 (near)
            -3.0, 2.0, -1.0, -2.5, // point 2 (far)
        ];
        for metric in [
            DistanceMetric::Euclidean,
            DistanceMetric::DotProduct,
            DistanceMetric::Cosine,
            DistanceMetric::Manhattan,
        ] {
            let raw: Arc<dyn VectorStorage> =
                Arc::new(InMemoryVectorStorage::new(vectors.clone(), 4));
            let raw = IndexedVectorStorage::prepare(raw, metric);
            let encoded = prepare_build_vector_storage(
                Arc::clone(&raw),
                super::super::HnswBuildVectorEncoding::symmetric_i16(3).unwrap(),
                17,
                None,
            )
            .unwrap();

            assert_eq!(encoded.get_vector(1), raw.get_vector(1));
            let near = encoded.construction_similarity(metric, 0, 1);
            let far = encoded.construction_similarity(metric, 0, 2);
            assert!(
                near > far,
                "{metric:?} encoded construction score must preserve this neighbor order"
            );
            assert_eq!(
                encoded.construction_similarity(metric, 0, 1).to_bits(),
                encoded.construction_similarity(metric, 1, 0).to_bits(),
                "{metric:?} encoded pair scoring must be bitwise symmetric"
            );
        }
    }

    #[test]
    fn symmetric_i16_rejects_scales_that_cannot_be_squared_stably() {
        for value in [f32::from_bits(1), f32::MAX] {
            let raw: Arc<dyn VectorStorage> =
                Arc::new(InMemoryVectorStorage::new(vec![value, -value], 1));
            let error = prepare_build_vector_storage(
                raw,
                super::super::HnswBuildVectorEncoding::symmetric_i16(1).unwrap(),
                17,
                None,
            )
            .err()
            .expect("unstable scale must be rejected");
            assert!(error.to_string().contains("cannot be squared stably"));
        }
    }

    #[test]
    fn remapped_build_storage_preserves_parent_routing_metric() {
        let raw: Arc<dyn VectorStorage> = Arc::new(InMemoryVectorStorage::new(
            vec![0.0, 0.0, 1.0, 2.0, -3.0, 4.0],
            2,
        ));
        let routing = prepare_build_vector_storage(
            raw,
            super::super::HnswBuildVectorEncoding::symmetric_i16(2).unwrap(),
            19,
            None,
        )
        .unwrap();
        let expected = routing.construction_similarity(DistanceMetric::Euclidean, 2, 0);
        let remapped = PointRemappedBuildVectorStorage::try_new(
            Arc::clone(&routing),
            Arc::from([2_u32, 0_u32]),
        )
        .unwrap();

        assert_eq!(remapped.get_vector(0), routing.get_vector(2));
        assert_eq!(
            remapped
                .construction_similarity(DistanceMetric::Euclidean, 0, 1)
                .to_bits(),
            expected.to_bits()
        );
    }

    #[test]
    fn compact_routing_dimensions_are_seeded_unique_and_source_ordered() {
        let minima = vec![-0.01; 768];
        let mut maxima = vec![0.01; 768];
        maxima[0] = 0.52;
        maxima[1] = -0.009_998;
        let first = multiscale_routing_dimensions(&minima, &maxima, 128, 42);
        let repeated = multiscale_routing_dimensions(&minima, &maxima, 128, 42);
        let other_seed = multiscale_routing_dimensions(&minima, &maxima, 128, 43);

        assert_eq!(first, repeated);
        assert_ne!(first, other_seed);
        assert_eq!(first.len(), 128);
        assert!(first.contains(&0), "the high-range tail must be retained");
        assert!(
            first.contains(&1),
            "the low non-zero-range tail must be retained"
        );
        assert!(first.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            multiscale_routing_dimensions(&minima[..4], &maxima[..4], 4, 42).as_ref(),
            &[0, 1, 2, 3]
        );
    }

    #[test]
    fn symmetric_i16_vector_kernels_match_the_scalar_metric() {
        let left_values = (0..131)
            .map(|idx| ((idx * 977) % 65_535) as i32 - 32_767)
            .map(|value| value as i16)
            .collect::<Vec<_>>();
        let right_values = (0..131)
            .map(|idx| ((idx * 313 + 17) % 65_535) as i32 - 32_767)
            .map(|value| value as i16)
            .collect::<Vec<_>>();
        let left = left_values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        let right = right_values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        let scales = (0..131)
            .map(|idx| 0.000_1 + idx as f32 * 0.000_003)
            .collect::<Vec<_>>();
        let scale_squares = scales.iter().map(|scale| scale * scale).collect::<Vec<_>>();
        let query = (0..131)
            .map(|idx| (idx as f32 * 0.17).sin() * 3.0)
            .collect::<Vec<_>>();

        let same_contract_value = |actual: f32, expected: f32| {
            assert_eq!(
                actual.to_bits(),
                expected.to_bits(),
                "SIMD and portable compact-score contracts diverged: actual={actual}, expected={expected}"
            );
        };
        same_contract_value(
            weighted_i16_dot_product(&left, &right, &scale_squares),
            weighted_i16_dot_product_scalar(&left, &right, &scale_squares),
        );
        same_contract_value(
            weighted_i16_l2_squared(&left, &right, &scale_squares),
            weighted_i16_l2_squared_scalar(&left, &right, &scale_squares),
        );
        same_contract_value(
            weighted_i16_l1_distance(&left, &right, &scales),
            weighted_i16_l1_distance_scalar(&left, &right, &scales),
        );
        same_contract_value(
            f32_query_i16_dot_product(&query, &left, &scales),
            f32_query_i16_dot_product_scalar(&query, &left, &scales),
        );
        same_contract_value(
            f32_query_i16_l2_squared(&query, &left, &scales),
            f32_query_i16_l2_squared_scalar(&query, &left, &scales),
        );
        same_contract_value(
            f32_query_i16_l1_distance(&query, &left, &scales),
            f32_query_i16_l1_distance_scalar(&query, &left, &scales),
        );
    }

    #[test]
    fn plain_vector_column_opens_every_physical_page() {
        let opts = ColumnWriterOptions::new(FieldType::Vector, 7)
            .with_logical_type(LogicalType::Array(Box::new(LogicalType::Float), 2))
            .with_nullable(false)
            .with_fixed_len(2 * std::mem::size_of::<f32>())
            .with_page_size(2 * 2 * std::mem::size_of::<f32>())
            .with_compression(CompressionType::None);
        let mut writer = ScalarColumnWriter::create_in_memory(opts).unwrap();
        let values = [
            [1.0_f32, 2.0],
            [3.0, 4.0],
            [5.0, 6.0],
            [7.0, 8.0],
            [9.0, 10.0],
        ];
        let bytes = values
            .iter()
            .flatten()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        writer.append(&bytes, None, values.len() as u32).unwrap();
        let meta = writer.finish().unwrap();

        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&writer.get_data()).unwrap();
        file.flush().unwrap();

        let mut column = ColumnMeta::new(7, FieldType::Vector);
        column.num_rows = meta.num_rows;
        column.encoding = meta.encoding;
        column.compression = meta.compression;
        column.data_page_pointer = meta.data_page_pointer;
        column.ordinal_index_pointer = meta.ordinal_index_pointer;
        column.null_count = Some(meta.null_count);

        let pages = open_plain_vector_column_pages(file.path(), &column, 2).unwrap();
        assert_eq!(pages.len(), 3);
        for page in &pages {
            let values = page.contiguous_vectors().expect("plain mmap vectors");
            assert_eq!(
                values.as_ptr() as usize % crate::index::hnsw::HNSW_ARTIFACT_ALIGNMENT,
                0,
                "the typed f32 region, not only the page envelope, must be cache-line aligned"
            );
        }
        let storage = PartitionedVectorStorage::try_new(pages, 2).unwrap();
        assert_eq!(storage.num_vectors(), values.len());
        for (point, expected) in values.iter().enumerate() {
            assert_eq!(storage.get_vector(point as PointOffset), expected);
        }
    }
}
