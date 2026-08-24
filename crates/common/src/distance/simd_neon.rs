// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! NEON SIMD optimized implementations of vector distance functions.
//!
//! These implementations use NEON instructions available on ARM64 (aarch64) processors.
//! NEON processes 4 f32 values at once (128-bit registers).
//!
//! # Safety
//! All functions in this module are unsafe and require NEON support.
//! On aarch64, NEON is always available, but the functions are still marked unsafe
//! due to raw pointer operations.

use std::arch::aarch64::*;

use super::scalar::is_length_zero_or_normalized;

/// Compute L2 squared distance using NEON instructions.
///
/// # Safety
/// Caller must ensure this is running on an aarch64 platform with NEON support.
#[target_feature(enable = "neon")]
pub unsafe fn l2_squared_neon(v1: &[f32], v2: &[f32]) -> f32 {
    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();

    let mut sum1 = vdupq_n_f32(0.);
    let mut sum2 = vdupq_n_f32(0.);
    let mut sum3 = vdupq_n_f32(0.);
    let mut sum4 = vdupq_n_f32(0.);

    let mut i: usize = 0;
    while i < m {
        let sub1 = vsubq_f32(vld1q_f32(ptr1), vld1q_f32(ptr2));
        sum1 = vfmaq_f32(sum1, sub1, sub1);

        let sub2 = vsubq_f32(vld1q_f32(ptr1.add(4)), vld1q_f32(ptr2.add(4)));
        sum2 = vfmaq_f32(sum2, sub2, sub2);

        let sub3 = vsubq_f32(vld1q_f32(ptr1.add(8)), vld1q_f32(ptr2.add(8)));
        sum3 = vfmaq_f32(sum3, sub3, sub3);

        let sub4 = vsubq_f32(vld1q_f32(ptr1.add(12)), vld1q_f32(ptr2.add(12)));
        sum4 = vfmaq_f32(sum4, sub4, sub4);

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }

    let mut result = vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);

    // Handle remaining elements
    for i in 0..n - m {
        let diff = *ptr1.add(i) - *ptr2.add(i);
        result += diff * diff;
    }

    result
}

/// Score eight indexed matrix rows at a time while sharing query loads. The
/// independent accumulators also expose enough memory-level parallelism for
/// sparse posting lists whose vector rows are not contiguous.
///
/// # Safety
///
/// The caller must ensure NEON support, `query.len() == dimension`, every
/// `point_id * dimension + dimension` range is inside `vectors`, and `scores`
/// has at least `point_ids.len()` elements.
#[target_feature(enable = "neon")]
pub unsafe fn l2_squared_batch_indexed_neon(
    query: &[f32],
    vectors: &[f32],
    dimension: usize,
    point_ids: &[u32],
    scores: &mut [f32],
) {
    const ROWS_PER_GROUP: usize = 8;
    let simd_dimension = dimension - dimension % 4;
    let mut row = 0usize;

    while row + ROWS_PER_GROUP <= point_ids.len() {
        let vector0 = vectors.as_ptr().add(point_ids[row] as usize * dimension);
        let vector1 = vectors
            .as_ptr()
            .add(point_ids[row + 1] as usize * dimension);
        let vector2 = vectors
            .as_ptr()
            .add(point_ids[row + 2] as usize * dimension);
        let vector3 = vectors
            .as_ptr()
            .add(point_ids[row + 3] as usize * dimension);
        let vector4 = vectors
            .as_ptr()
            .add(point_ids[row + 4] as usize * dimension);
        let vector5 = vectors
            .as_ptr()
            .add(point_ids[row + 5] as usize * dimension);
        let vector6 = vectors
            .as_ptr()
            .add(point_ids[row + 6] as usize * dimension);
        let vector7 = vectors
            .as_ptr()
            .add(point_ids[row + 7] as usize * dimension);
        let mut sum0 = vdupq_n_f32(0.0);
        let mut sum1 = vdupq_n_f32(0.0);
        let mut sum2 = vdupq_n_f32(0.0);
        let mut sum3 = vdupq_n_f32(0.0);
        let mut sum4 = vdupq_n_f32(0.0);
        let mut sum5 = vdupq_n_f32(0.0);
        let mut sum6 = vdupq_n_f32(0.0);
        let mut sum7 = vdupq_n_f32(0.0);

        let mut lane = 0usize;
        while lane < simd_dimension {
            let query_values = vld1q_f32(query.as_ptr().add(lane));
            let diff0 = vsubq_f32(vld1q_f32(vector0.add(lane)), query_values);
            let diff1 = vsubq_f32(vld1q_f32(vector1.add(lane)), query_values);
            let diff2 = vsubq_f32(vld1q_f32(vector2.add(lane)), query_values);
            let diff3 = vsubq_f32(vld1q_f32(vector3.add(lane)), query_values);
            let diff4 = vsubq_f32(vld1q_f32(vector4.add(lane)), query_values);
            let diff5 = vsubq_f32(vld1q_f32(vector5.add(lane)), query_values);
            let diff6 = vsubq_f32(vld1q_f32(vector6.add(lane)), query_values);
            let diff7 = vsubq_f32(vld1q_f32(vector7.add(lane)), query_values);
            sum0 = vfmaq_f32(sum0, diff0, diff0);
            sum1 = vfmaq_f32(sum1, diff1, diff1);
            sum2 = vfmaq_f32(sum2, diff2, diff2);
            sum3 = vfmaq_f32(sum3, diff3, diff3);
            sum4 = vfmaq_f32(sum4, diff4, diff4);
            sum5 = vfmaq_f32(sum5, diff5, diff5);
            sum6 = vfmaq_f32(sum6, diff6, diff6);
            sum7 = vfmaq_f32(sum7, diff7, diff7);
            lane += 4;
        }

        let mut result0 = vaddvq_f32(sum0);
        let mut result1 = vaddvq_f32(sum1);
        let mut result2 = vaddvq_f32(sum2);
        let mut result3 = vaddvq_f32(sum3);
        let mut result4 = vaddvq_f32(sum4);
        let mut result5 = vaddvq_f32(sum5);
        let mut result6 = vaddvq_f32(sum6);
        let mut result7 = vaddvq_f32(sum7);
        for lane in simd_dimension..dimension {
            let query_value = *query.get_unchecked(lane);
            let diff0 = *vector0.add(lane) - query_value;
            let diff1 = *vector1.add(lane) - query_value;
            let diff2 = *vector2.add(lane) - query_value;
            let diff3 = *vector3.add(lane) - query_value;
            let diff4 = *vector4.add(lane) - query_value;
            let diff5 = *vector5.add(lane) - query_value;
            let diff6 = *vector6.add(lane) - query_value;
            let diff7 = *vector7.add(lane) - query_value;
            result0 += diff0 * diff0;
            result1 += diff1 * diff1;
            result2 += diff2 * diff2;
            result3 += diff3 * diff3;
            result4 += diff4 * diff4;
            result5 += diff5 * diff5;
            result6 += diff6 * diff6;
            result7 += diff7 * diff7;
        }
        *scores.get_unchecked_mut(row) = result0;
        *scores.get_unchecked_mut(row + 1) = result1;
        *scores.get_unchecked_mut(row + 2) = result2;
        *scores.get_unchecked_mut(row + 3) = result3;
        *scores.get_unchecked_mut(row + 4) = result4;
        *scores.get_unchecked_mut(row + 5) = result5;
        *scores.get_unchecked_mut(row + 6) = result6;
        *scores.get_unchecked_mut(row + 7) = result7;
        row += ROWS_PER_GROUP;
    }

    while row < point_ids.len() {
        let start = point_ids[row] as usize * dimension;
        *scores.get_unchecked_mut(row) = l2_squared_neon(query, &vectors[start..start + dimension]);
        row += 1;
    }
}

/// Compute L1 distance (Manhattan distance) using NEON instructions.
///
/// # Safety
/// Caller must ensure this is running on an aarch64 platform with NEON support.
#[target_feature(enable = "neon")]
pub unsafe fn l1_distance_neon(v1: &[f32], v2: &[f32]) -> f32 {
    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();

    let mut sum1 = vdupq_n_f32(0.);
    let mut sum2 = vdupq_n_f32(0.);
    let mut sum3 = vdupq_n_f32(0.);
    let mut sum4 = vdupq_n_f32(0.);

    let mut i: usize = 0;
    while i < m {
        let sub1 = vsubq_f32(vld1q_f32(ptr1), vld1q_f32(ptr2));
        sum1 = vaddq_f32(sum1, vabsq_f32(sub1));

        let sub2 = vsubq_f32(vld1q_f32(ptr1.add(4)), vld1q_f32(ptr2.add(4)));
        sum2 = vaddq_f32(sum2, vabsq_f32(sub2));

        let sub3 = vsubq_f32(vld1q_f32(ptr1.add(8)), vld1q_f32(ptr2.add(8)));
        sum3 = vaddq_f32(sum3, vabsq_f32(sub3));

        let sub4 = vsubq_f32(vld1q_f32(ptr1.add(12)), vld1q_f32(ptr2.add(12)));
        sum4 = vaddq_f32(sum4, vabsq_f32(sub4));

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }

    let mut result = vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);

    // Handle remaining elements
    for i in 0..n - m {
        result += (*ptr1.add(i) - *ptr2.add(i)).abs();
    }

    result
}

/// Compute dot product using NEON instructions.
///
/// # Safety
/// Caller must ensure this is running on an aarch64 platform with NEON support.
#[target_feature(enable = "neon")]
pub unsafe fn dot_product_neon(v1: &[f32], v2: &[f32]) -> f32 {
    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();

    let mut sum1 = vdupq_n_f32(0.);
    let mut sum2 = vdupq_n_f32(0.);
    let mut sum3 = vdupq_n_f32(0.);
    let mut sum4 = vdupq_n_f32(0.);

    let mut i: usize = 0;
    while i < m {
        sum1 = vfmaq_f32(sum1, vld1q_f32(ptr1), vld1q_f32(ptr2));
        sum2 = vfmaq_f32(sum2, vld1q_f32(ptr1.add(4)), vld1q_f32(ptr2.add(4)));
        sum3 = vfmaq_f32(sum3, vld1q_f32(ptr1.add(8)), vld1q_f32(ptr2.add(8)));
        sum4 = vfmaq_f32(sum4, vld1q_f32(ptr1.add(12)), vld1q_f32(ptr2.add(12)));

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }

    let mut result = vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);

    // Handle remaining elements
    for i in 0..n - m {
        result += (*ptr1.add(i)) * (*ptr2.add(i));
    }

    result
}

/// Normalize a vector to unit length using NEON instructions.
///
/// # Safety
/// Caller must ensure this is running on an aarch64 platform with NEON support.
#[target_feature(enable = "neon")]
pub unsafe fn normalize_neon(vector: Vec<f32>) -> Vec<f32> {
    let n = vector.len();
    let m = n - (n % 16);
    let mut ptr: *const f32 = vector.as_ptr();

    let mut sum1 = vdupq_n_f32(0.);
    let mut sum2 = vdupq_n_f32(0.);
    let mut sum3 = vdupq_n_f32(0.);
    let mut sum4 = vdupq_n_f32(0.);

    let mut i: usize = 0;
    while i < m {
        let d1 = vld1q_f32(ptr);
        sum1 = vfmaq_f32(sum1, d1, d1);

        let d2 = vld1q_f32(ptr.add(4));
        sum2 = vfmaq_f32(sum2, d2, d2);

        let d3 = vld1q_f32(ptr.add(8));
        sum3 = vfmaq_f32(sum3, d3, d3);

        let d4 = vld1q_f32(ptr.add(12));
        sum4 = vfmaq_f32(sum4, d4, d4);

        ptr = ptr.add(16);
        i += 16;
    }

    let mut length_squared =
        vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);

    // Handle remaining elements
    for v in vector.iter().take(n).skip(m) {
        length_squared += v * v;
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

    fn neon_available() -> bool {
        std::arch::is_aarch64_feature_detected!("neon")
    }

    fn neon_test_vectors() -> (Vec<f32>, Vec<f32>) {
        (
            vec![
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                26., 27., 28., 29., 30., 31.,
            ],
            vec![
                40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55.,
                56., 57., 58., 59., 60., 61.,
            ],
        )
    }

    #[test]
    fn test_neon_l2_squared() {
        if !neon_available() {
            return;
        }

        let (v1, v2) = neon_test_vectors();

        let neon_result = unsafe { l2_squared_neon(&v1, &v2) };
        let scalar_result = scalar::l2_squared(&v1, &v2);
        assert!(
            (neon_result - scalar_result).abs() < 1e-5,
            "NEON: {}, Scalar: {}",
            neon_result,
            scalar_result
        );
    }

    #[test]
    fn test_neon_l1_distance() {
        if !neon_available() {
            return;
        }

        let (v1, v2) = neon_test_vectors();

        let neon_result = unsafe { l1_distance_neon(&v1, &v2) };
        let scalar_result = scalar::l1_distance(&v1, &v2);
        assert!(
            (neon_result - scalar_result).abs() < 1e-5,
            "NEON: {}, Scalar: {}",
            neon_result,
            scalar_result
        );
    }

    #[test]
    fn test_neon_dot_product() {
        if !neon_available() {
            return;
        }

        let (v1, v2) = neon_test_vectors();

        let neon_result = unsafe { dot_product_neon(&v1, &v2) };
        let scalar_result = scalar::dot_product(&v1, &v2);
        assert!(
            (neon_result - scalar_result).abs() < 1e-5,
            "NEON: {}, Scalar: {}",
            neon_result,
            scalar_result
        );
    }

    #[test]
    fn test_neon_normalize() {
        if !neon_available() {
            return;
        }

        let (v, _) = neon_test_vectors();

        let neon_result = unsafe { normalize_neon(v.clone()) };
        let scalar_result = scalar::normalize(v);
        assert_eq!(neon_result.len(), scalar_result.len());
        for (a, b) in neon_result.iter().zip(scalar_result.iter()) {
            assert!((a - b).abs() < 1e-6, "NEON: {}, Scalar: {}", a, b);
        }
    }
}
