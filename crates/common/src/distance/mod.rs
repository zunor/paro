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

    // Compute norms using dot_product with self (benefits from SIMD)
    let norm1_sq = dot_product(v1, v1);
    let norm2_sq = dot_product(v2, v2);
    let norm_product = (norm1_sq * norm2_sq).sqrt();

    if norm_product < f32::EPSILON {
        return 1.0; // If either vector is zero, distance is maximum
    }

    1.0 - (dot / norm_product)
}

#[cfg(test)]
mod tests;
