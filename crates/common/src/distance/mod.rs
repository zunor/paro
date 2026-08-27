// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Vector Distance Computation Module
//!
//! This module provides efficient distance computation between vectors,
//! with SIMD optimizations for different CPU architectures.
//!
//! # Public Functions
//! - `l2_squared(v1, v2)` - L2 squared distance (sum of squared differences)
//! - `l2_distance(v1, v2)` - L2 distance (Euclidean distance)
//! - `l1_distance(v1, v2)` - L1 distance (Manhattan distance)
//! - `dot_product(v1, v2)` - Dot product (inner product)
//! - `cosine_distance(v1, v2)` - Cosine distance
//! - `normalize(v)` - Vector normalization
//!
//! # SIMD Optimization
//! Functions automatically select the best available SIMD implementation at runtime:
//! - AVX+FMA on x86_64 (for vectors >= 32 elements)
//! - SSE on x86/x86_64 (for vectors >= 16 elements)
//! - NEON on ARM64 (for vectors >= 16 elements)
//! - Scalar fallback for smaller vectors or unsupported platforms
//!
mod scalar;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod simd_sse;

#[cfg(target_arch = "x86_64")]
mod simd_avx;

#[cfg(target_arch = "aarch64")]
mod simd_neon;

/// Minimum vector dimension to use AVX (256-bit, processes 32 f32 per iteration)
#[cfg(target_arch = "x86_64")]
const MIN_DIM_AVX: usize = 32;

/// Minimum vector dimension to use SSE/NEON (128-bit, processes 16 f32 per iteration)
const MIN_DIM_SIMD: usize = 16;

/// Hint that the leading cache lines of a vector will be read soon.
///
/// HNSW discovers point ids while traversing a separate adjacency region.
/// Issuing these hints at discovery time overlaps the random vector fetch with
/// visited-set and link processing before the distance kernel consumes it.
/// At most 256 bytes are requested: enough to cover common 32/64-dimensional
/// vectors without flooding caches for high-dimensional batches.
#[inline]
pub fn prefetch_vector_read(vector: &[f32]) {
    const FLOATS_PER_CACHE_LINE: usize = 16;
    const MAX_PREFETCH_FLOATS: usize = 64;

    for offset in (0..vector.len().min(MAX_PREFETCH_FLOATS)).step_by(FLOATS_PER_CACHE_LINE) {
        let address = unsafe { vector.as_ptr().add(offset) };
        #[cfg(target_arch = "aarch64")]
        unsafe {
            simd_neon::prefetch_l1(address);
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch(address.cast::<i8>(), std::arch::x86_64::_MM_HINT_T0);
        }
        #[cfg(target_arch = "x86")]
        unsafe {
            std::arch::x86::_mm_prefetch(address.cast::<i8>(), std::arch::x86::_MM_HINT_T0);
        }
        #[cfg(not(any(target_arch = "aarch64", target_arch = "x86", target_arch = "x86_64")))]
        {
            let _ = address;
        }
    }
}

/// Hint that the leading cache lines of an encoded routing row will be read
/// soon. Unlike `prefetch_vector_read`, this accepts byte-aligned persistent
/// regions and therefore does not manufacture an aligned typed slice.
#[inline]
pub fn prefetch_bytes_read(bytes: &[u8]) {
    const CACHE_LINE_BYTES: usize = 64;
    const MAX_PREFETCH_BYTES: usize = 256;

    for offset in (0..bytes.len().min(MAX_PREFETCH_BYTES)).step_by(CACHE_LINE_BYTES) {
        let address = unsafe { bytes.as_ptr().add(offset) };
        #[cfg(target_arch = "aarch64")]
        unsafe {
            simd_neon::prefetch_l1(address.cast::<f32>());
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch(address.cast::<i8>(), std::arch::x86_64::_MM_HINT_T0);
        }
        #[cfg(target_arch = "x86")]
        unsafe {
            std::arch::x86::_mm_prefetch(address.cast::<i8>(), std::arch::x86::_MM_HINT_T0);
        }
        #[cfg(not(any(target_arch = "aarch64", target_arch = "x86", target_arch = "x86_64")))]
        {
            let _ = address;
        }
    }
}

/// Compute L2 squared distance (sum of squared differences).
///
/// Automatically selects the best SIMD implementation available.
#[inline]
pub fn l2_squared(v1: &[f32], v2: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("fma")
            && v1.len() >= MIN_DIM_AVX
        {
            return unsafe { simd_avx::l2_squared_avx(v1, v2) };
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIMD {
            return unsafe { simd_sse::l2_squared_sse(v1, v2) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIMD {
            return unsafe { simd_neon::l2_squared_neon(v1, v2) };
        }
    }

    scalar::l2_squared(v1, v2)
}

/// Compute squared L2 distances for vectors selected from one flat row-major
/// matrix. The query and CPU-feature dispatch are shared by the whole batch;
/// architecture kernels may score several rows in parallel to avoid reloading
/// the query for every candidate.
///
/// # Panics
///
/// Panics when `query.len() != dimension`, `scores` is shorter than
/// `point_ids`, or a point id falls outside `vectors`.
#[inline]
pub fn l2_squared_batch_indexed(
    query: &[f32],
    vectors: &[f32],
    dimension: usize,
    point_ids: &[u32],
    scores: &mut [f32],
) {
    assert_eq!(query.len(), dimension, "query dimension mismatch");
    assert!(
        scores.len() >= point_ids.len(),
        "score buffer is shorter than point batch"
    );
    if point_ids.is_empty() {
        return;
    }
    let max_point = point_ids.iter().copied().max().unwrap_or(0) as usize;
    let required = max_point
        .checked_add(1)
        .and_then(|rows| rows.checked_mul(dimension))
        .expect("indexed distance matrix shape overflow");
    assert!(
        required <= vectors.len(),
        "indexed distance point exceeds vector matrix"
    );

    #[cfg(target_arch = "aarch64")]
    if dimension >= MIN_DIM_SIMD {
        // AArch64 requires NEON as part of the base architecture. All slice
        // bounds and row offsets were validated above.
        return unsafe {
            simd_neon::l2_squared_batch_indexed_neon(query, vectors, dimension, point_ids, scores)
        };
    }

    for (&point_id, score) in point_ids.iter().zip(scores.iter_mut()) {
        let start = point_id as usize * dimension;
        *score = l2_squared(query, &vectors[start..start + dimension]);
    }
}

/// Compute squared L2 distances for one contiguous row-major matrix.
///
/// Unlike [`l2_squared_batch_indexed`], this contract has no point-id gather:
/// row `i` begins at `i * dimension`. Exact covering scans use this shape to
/// avoid manufacturing an identity index and to expose sequential memory
/// access directly to architecture kernels.
///
/// # Panics
///
/// Panics when `query.len() != dimension`, `vectors` is not an exact row-major
/// matrix, or `scores` is shorter than the matrix row count.
#[inline]
pub fn l2_squared_batch_contiguous(
    query: &[f32],
    vectors: &[f32],
    dimension: usize,
    scores: &mut [f32],
) {
    assert_eq!(query.len(), dimension, "query dimension mismatch");
    assert!(dimension != 0, "distance matrix dimension must be non-zero");
    assert_eq!(
        vectors.len() % dimension,
        0,
        "distance matrix has a partial row"
    );
    let rows = vectors.len() / dimension;
    assert!(scores.len() >= rows, "score buffer is shorter than matrix");
    if rows == 0 {
        return;
    }

    #[cfg(target_arch = "aarch64")]
    if dimension >= MIN_DIM_SIMD {
        // AArch64 requires NEON as part of the base architecture. Matrix shape
        // and output bounds were validated above.
        return unsafe {
            simd_neon::l2_squared_batch_contiguous_neon(query, vectors, dimension, scores)
        };
    }

    for (vector, score) in vectors
        .chunks_exact(dimension)
        .zip(scores.iter_mut().take(rows))
    {
        *score = l2_squared(query, vector);
    }
}

/// Compute L2 distance (Euclidean distance).
///
/// This is the square root of `l2_squared`.
#[inline]
pub fn l2_distance(v1: &[f32], v2: &[f32]) -> f32 {
    l2_squared(v1, v2).sqrt()
}

/// Compute L1 distance (Manhattan distance).
///
/// Automatically selects the best SIMD implementation available.
#[inline]
pub fn l1_distance(v1: &[f32], v2: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("fma")
            && v1.len() >= MIN_DIM_AVX
        {
            return unsafe { simd_avx::l1_distance_avx(v1, v2) };
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIMD {
            return unsafe { simd_sse::l1_distance_sse(v1, v2) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIMD {
            return unsafe { simd_neon::l1_distance_neon(v1, v2) };
        }
    }

    scalar::l1_distance(v1, v2)
}

/// Compute dot product (inner product).
///
/// Automatically selects the best SIMD implementation available.
#[inline]
pub fn dot_product(v1: &[f32], v2: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("fma")
            && v1.len() >= MIN_DIM_AVX
        {
            return unsafe { simd_avx::dot_product_avx(v1, v2) };
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIMD {
            return unsafe { simd_sse::dot_product_sse(v1, v2) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIMD {
            return unsafe { simd_neon::dot_product_neon(v1, v2) };
        }
    }

    scalar::dot_product(v1, v2)
}

/// Return the multiplicative inverse of a vector norm, or zero for the
/// engine's canonical near-zero vector domain.
#[inline]
pub fn inverse_norm(vector: &[f32]) -> f32 {
    let squared = dot_product(vector, vector);
    if squared < f32::EPSILON {
        0.0
    } else {
        squared.sqrt().recip()
    }
}

/// Normalize a vector to unit length.
///
/// Automatically selects the best SIMD implementation available.
#[inline]
pub fn normalize(vector: Vec<f32>) -> Vec<f32> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("fma")
            && vector.len() >= MIN_DIM_AVX
        {
            return unsafe { simd_avx::normalize_avx(vector) };
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("sse") && vector.len() >= MIN_DIM_SIMD {
            return unsafe { simd_sse::normalize_sse(vector) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") && vector.len() >= MIN_DIM_SIMD {
            return unsafe { simd_neon::normalize_neon(vector) };
        }
    }

    scalar::normalize(vector)
}

/// Compute cosine distance between two vectors.
///
/// Cosine distance = 1 - cosine_similarity
/// where cosine_similarity = dot(v1, v2) / (|v1| * |v2|)
///
/// Note: For better performance with pre-normalized vectors, use `dot_product` directly
/// and compute `1.0 - dot_product(v1, v2)`.
#[inline]
pub fn cosine_distance(v1: &[f32], v2: &[f32]) -> f32 {
    let dot = dot_product(v1, v2);
    let inverse_norm_1 = inverse_norm(v1);
    let inverse_norm_2 = inverse_norm(v2);
    if inverse_norm_1 == 0.0 || inverse_norm_2 == 0.0 {
        return 1.0; // If either vector is zero, distance is maximum
    }
    1.0 - dot * inverse_norm_1 * inverse_norm_2
}

#[cfg(test)]
mod tests;
