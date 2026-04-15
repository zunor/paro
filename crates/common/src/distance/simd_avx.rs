// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! AVX SIMD optimized implementations of vector distance functions.
//!
//! These implementations use AVX and FMA instructions available on x86_64 processors.
//! AVX processes 8 f32 values at once (256-bit registers), providing better throughput
//! than SSE for larger vectors.
//!
//! # Safety
//! All functions in this module are unsafe and require AVX and FMA support.
//! Callers must verify feature availability before calling these functions.

use std::arch::x86_64::*;

use super::scalar::is_length_zero_or_normalized;

/// Horizontal sum of a 256-bit AVX register containing 8 f32 values.
#[target_feature(enable = "avx")]
#[inline]
unsafe fn hsum256_ps_avx(x: __m256) -> f32 {
    let lr_sum: __m128 = _mm_add_ps(_mm256_extractf128_ps(x, 1), _mm256_castps256_ps128(x));
    let hsum = _mm_hadd_ps(lr_sum, lr_sum);
    let p1 = _mm_extract_ps(hsum, 0);
    let p2 = _mm_extract_ps(hsum, 1);
    f32::from_bits(p1 as u32) + f32::from_bits(p2 as u32)
}

/// Horizontal sum of four 256-bit AVX registers.
#[target_feature(enable = "avx")]
#[inline]
unsafe fn four_way_hsum(a: __m256, b: __m256, c: __m256, d: __m256) -> f32 {
    let sum1 = _mm256_add_ps(a, b);
    let sum2 = _mm256_add_ps(c, d);
    let total = _mm256_add_ps(sum1, sum2);
    hsum256_ps_avx(total)
}

/// Compute L2 squared distance using AVX+FMA instructions.
///
/// # Safety
/// Caller must ensure AVX and FMA are available on the current CPU.
#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub unsafe fn l2_squared_avx(v1: &[f32], v2: &[f32]) -> f32 {
    let n = v1.len();
    let m = n - (n % 32);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();

    let mut sum256_1: __m256 = _mm256_setzero_ps();
    let mut sum256_2: __m256 = _mm256_setzero_ps();
    let mut sum256_3: __m256 = _mm256_setzero_ps();
    let mut sum256_4: __m256 = _mm256_setzero_ps();

    let mut i: usize = 0;
    while i < m {
        let sub256_1 = _mm256_sub_ps(_mm256_loadu_ps(ptr1), _mm256_loadu_ps(ptr2));
        sum256_1 = _mm256_fmadd_ps(sub256_1, sub256_1, sum256_1);

        let sub256_2 = _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(8)), _mm256_loadu_ps(ptr2.add(8)));
        sum256_2 = _mm256_fmadd_ps(sub256_2, sub256_2, sum256_2);

        let sub256_3 = _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(16)), _mm256_loadu_ps(ptr2.add(16)));
        sum256_3 = _mm256_fmadd_ps(sub256_3, sub256_3, sum256_3);

        let sub256_4 = _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(24)), _mm256_loadu_ps(ptr2.add(24)));
        sum256_4 = _mm256_fmadd_ps(sub256_4, sub256_4, sum256_4);

        ptr1 = ptr1.add(32);
        ptr2 = ptr2.add(32);
        i += 32;
    }

    let mut result = four_way_hsum(sum256_1, sum256_2, sum256_3, sum256_4);

    // Handle remaining elements
    for i in 0..n - m {
        let diff = *ptr1.add(i) - *ptr2.add(i);
        result += diff * diff;
    }

    result
}

/// Compute L1 distance (Manhattan distance) using AVX instructions.
///
/// # Safety
/// Caller must ensure AVX and FMA are available on the current CPU.
#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub unsafe fn l1_distance_avx(v1: &[f32], v2: &[f32]) -> f32 {
    // Mask to clear sign bit (implements abs)
    let mask: __m256 = _mm256_set1_ps(-0.0f32);

    let n = v1.len();
    let m = n - (n % 32);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();

    let mut sum256_1: __m256 = _mm256_setzero_ps();
    let mut sum256_2: __m256 = _mm256_setzero_ps();
    let mut sum256_3: __m256 = _mm256_setzero_ps();
    let mut sum256_4: __m256 = _mm256_setzero_ps();

    let mut i: usize = 0;
    while i < m {
        let sub256_1 = _mm256_sub_ps(_mm256_loadu_ps(ptr1), _mm256_loadu_ps(ptr2));
        sum256_1 = _mm256_add_ps(_mm256_andnot_ps(mask, sub256_1), sum256_1);

        let sub256_2 = _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(8)), _mm256_loadu_ps(ptr2.add(8)));
        sum256_2 = _mm256_add_ps(_mm256_andnot_ps(mask, sub256_2), sum256_2);

        let sub256_3 = _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(16)), _mm256_loadu_ps(ptr2.add(16)));
        sum256_3 = _mm256_add_ps(_mm256_andnot_ps(mask, sub256_3), sum256_3);

        let sub256_4 = _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(24)), _mm256_loadu_ps(ptr2.add(24)));
        sum256_4 = _mm256_add_ps(_mm256_andnot_ps(mask, sub256_4), sum256_4);

        ptr1 = ptr1.add(32);
        ptr2 = ptr2.add(32);
        i += 32;
    }

    let mut result = four_way_hsum(sum256_1, sum256_2, sum256_3, sum256_4);

    // Handle remaining elements
    for i in 0..n - m {
        result += (*ptr1.add(i) - *ptr2.add(i)).abs();
    }

    result
}

/// Compute dot product using AVX+FMA instructions.
///
/// # Safety
/// Caller must ensure AVX and FMA are available on the current CPU.
#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub unsafe fn dot_product_avx(v1: &[f32], v2: &[f32]) -> f32 {
    let n = v1.len();
    let m = n - (n % 32);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();

    let mut sum256_1: __m256 = _mm256_setzero_ps();
    let mut sum256_2: __m256 = _mm256_setzero_ps();
    let mut sum256_3: __m256 = _mm256_setzero_ps();
    let mut sum256_4: __m256 = _mm256_setzero_ps();

    let mut i: usize = 0;
    while i < m {
        sum256_1 = _mm256_fmadd_ps(_mm256_loadu_ps(ptr1), _mm256_loadu_ps(ptr2), sum256_1);
        sum256_2 = _mm256_fmadd_ps(
            _mm256_loadu_ps(ptr1.add(8)),
            _mm256_loadu_ps(ptr2.add(8)),
            sum256_2,
        );
        sum256_3 = _mm256_fmadd_ps(
            _mm256_loadu_ps(ptr1.add(16)),
            _mm256_loadu_ps(ptr2.add(16)),
            sum256_3,
        );
        sum256_4 = _mm256_fmadd_ps(
            _mm256_loadu_ps(ptr1.add(24)),
            _mm256_loadu_ps(ptr2.add(24)),
            sum256_4,
        );

        ptr1 = ptr1.add(32);
        ptr2 = ptr2.add(32);
        i += 32;
    }

    let mut result = four_way_hsum(sum256_1, sum256_2, sum256_3, sum256_4);

    // Handle remaining elements
    for i in 0..n - m {
        result += (*ptr1.add(i)) * (*ptr2.add(i));
    }

    result
}

/// Normalize a vector to unit length using AVX+FMA instructions.
///
/// # Safety
/// Caller must ensure AVX and FMA are available on the current CPU.
#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub unsafe fn normalize_avx(vector: Vec<f32>) -> Vec<f32> {
    let n = vector.len();
    let m = n - (n % 32);
    let mut ptr: *const f32 = vector.as_ptr();

    let mut sum256_1: __m256 = _mm256_setzero_ps();
    let mut sum256_2: __m256 = _mm256_setzero_ps();
    let mut sum256_3: __m256 = _mm256_setzero_ps();
    let mut sum256_4: __m256 = _mm256_setzero_ps();

    let mut i: usize = 0;
    while i < m {
        let m256_1 = _mm256_loadu_ps(ptr);
        sum256_1 = _mm256_fmadd_ps(m256_1, m256_1, sum256_1);

        let m256_2 = _mm256_loadu_ps(ptr.add(8));
        sum256_2 = _mm256_fmadd_ps(m256_2, m256_2, sum256_2);

        let m256_3 = _mm256_loadu_ps(ptr.add(16));
        sum256_3 = _mm256_fmadd_ps(m256_3, m256_3, sum256_3);

        let m256_4 = _mm256_loadu_ps(ptr.add(24));
        sum256_4 = _mm256_fmadd_ps(m256_4, m256_4, sum256_4);

        ptr = ptr.add(32);
        i += 32;
    }

    let mut length_squared = four_way_hsum(sum256_1, sum256_2, sum256_3, sum256_4);

    // Handle remaining elements
    for i in 0..n - m {
        let val = *ptr.add(i);
        length_squared += val * val;
    }

    if is_length_zero_or_normalized(length_squared) {
        return vector;
    }

    let length = length_squared.sqrt();
    vector.into_iter().map(|x| x / length).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::scalar;

    fn avx_fma_available() -> bool {
        is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma")
    }

    fn avx_test_vectors() -> (Vec<f32>, Vec<f32>) {
        (
            vec![
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                26., 27., 28., 29., 30., 31.,
            ],
            vec![
                40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                56., 57., 58., 59., 60., 61.,
            ],
        )
    }

    #[test]
    fn test_avx_l2_squared() {
        if !avx_fma_available() {
            return;
        }

        let (v1, v2) = avx_test_vectors();

        let avx_result = unsafe { l2_squared_avx(&v1, &v2) };
        let scalar_result = scalar::l2_squared(&v1, &v2);
        assert!(
            (avx_result - scalar_result).abs() < 1e-4,
            "AVX: {}, Scalar: {}",
            avx_result,
            scalar_result
        );
    }

    #[test]
    fn test_avx_l1_distance() {
        if !avx_fma_available() {
            return;
        }

        let (v1, v2) = avx_test_vectors();

        let avx_result = unsafe { l1_distance_avx(&v1, &v2) };
        let scalar_result = scalar::l1_distance(&v1, &v2);
        assert!(
            (avx_result - scalar_result).abs() < 1e-4,
            "AVX: {}, Scalar: {}",
            avx_result,
            scalar_result
        );
    }

    #[test]
    fn test_avx_dot_product() {
        if !avx_fma_available() {
            return;
        }

        let (v1, v2) = avx_test_vectors();

        let avx_result = unsafe { dot_product_avx(&v1, &v2) };
        let scalar_result = scalar::dot_product(&v1, &v2);
        assert!(
            (avx_result - scalar_result).abs() < 1e-4,
            "AVX: {}, Scalar: {}",
            avx_result,
            scalar_result
        );
    }

    #[test]
    fn test_avx_normalize() {
        if !avx_fma_available() {
            return;
        }

        let (v, _) = avx_test_vectors();

        let avx_result = unsafe { normalize_avx(v.clone()) };
        let scalar_result = scalar::normalize(v);
        assert_eq!(avx_result.len(), scalar_result.len());
        for (a, b) in avx_result.iter().zip(scalar_result.iter()) {
            assert!((a - b).abs() < 1e-6, "AVX: {}, Scalar: {}", a, b);
        }
    }
}
