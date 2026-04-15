//! SSE SIMD optimized implementations of vector distance functions.
//!
//! These implementations use SSE instructions available on x86/x86_64 processors.
//!
//! # Safety
//! All functions in this module are unsafe and require SSE support.
//! Callers must verify SSE availability before calling these functions.

#[cfg(target_arch = "x86")]
use std::arch::x86::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

use super::scalar::is_length_zero_or_normalized;

/// Horizontal sum of a 128-bit SSE register containing 4 f32 values.
#[target_feature(enable = "sse")]
#[inline]
unsafe fn hsum128_ps_sse(x: __m128) -> f32 {
    let x64: __m128 = _mm_add_ps(x, _mm_movehl_ps(x, x));
    let x32: __m128 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    _mm_cvtss_f32(x32)
}

/// Compute L2 squared distance using SSE instructions.
///
/// # Safety
/// Caller must ensure SSE is available on the current CPU.
#[target_feature(enable = "sse")]
pub unsafe fn l2_squared_sse(v1: &[f32], v2: &[f32]) -> f32 {
    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();

    let mut sum128_1: __m128 = _mm_setzero_ps();
    let mut sum128_2: __m128 = _mm_setzero_ps();
    let mut sum128_3: __m128 = _mm_setzero_ps();
    let mut sum128_4: __m128 = _mm_setzero_ps();

    let mut i: usize = 0;
    while i < m {
        let sub128_1 = _mm_sub_ps(_mm_loadu_ps(ptr1), _mm_loadu_ps(ptr2));
        sum128_1 = _mm_add_ps(_mm_mul_ps(sub128_1, sub128_1), sum128_1);

        let sub128_2 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(4)), _mm_loadu_ps(ptr2.add(4)));
        sum128_2 = _mm_add_ps(_mm_mul_ps(sub128_2, sub128_2), sum128_2);

        let sub128_3 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(8)), _mm_loadu_ps(ptr2.add(8)));
        sum128_3 = _mm_add_ps(_mm_mul_ps(sub128_3, sub128_3), sum128_3);

        let sub128_4 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(12)), _mm_loadu_ps(ptr2.add(12)));
        sum128_4 = _mm_add_ps(_mm_mul_ps(sub128_4, sub128_4), sum128_4);

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }

    let mut result = hsum128_ps_sse(sum128_1)
        + hsum128_ps_sse(sum128_2)
        + hsum128_ps_sse(sum128_3)
        + hsum128_ps_sse(sum128_4);

    // Handle remaining elements
    for i in 0..n - m {
        let diff = *ptr1.add(i) - *ptr2.add(i);
        result += diff * diff;
    }

    result
}

/// Compute L1 distance (Manhattan distance) using SSE instructions.
///
/// # Safety
/// Caller must ensure SSE is available on the current CPU.
#[target_feature(enable = "sse")]
pub unsafe fn l1_distance_sse(v1: &[f32], v2: &[f32]) -> f32 {
    // Mask to clear sign bit (implements abs)
    let mask: __m128 = _mm_set1_ps(-0.0f32);

    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();

    let mut sum128_1: __m128 = _mm_setzero_ps();
    let mut sum128_2: __m128 = _mm_setzero_ps();
    let mut sum128_3: __m128 = _mm_setzero_ps();
    let mut sum128_4: __m128 = _mm_setzero_ps();

    let mut i: usize = 0;
    while i < m {
        let sub128_1 = _mm_sub_ps(_mm_loadu_ps(ptr1), _mm_loadu_ps(ptr2));
        sum128_1 = _mm_add_ps(_mm_andnot_ps(mask, sub128_1), sum128_1);

        let sub128_2 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(4)), _mm_loadu_ps(ptr2.add(4)));
        sum128_2 = _mm_add_ps(_mm_andnot_ps(mask, sub128_2), sum128_2);

        let sub128_3 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(8)), _mm_loadu_ps(ptr2.add(8)));
        sum128_3 = _mm_add_ps(_mm_andnot_ps(mask, sub128_3), sum128_3);

        let sub128_4 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(12)), _mm_loadu_ps(ptr2.add(12)));
        sum128_4 = _mm_add_ps(_mm_andnot_ps(mask, sub128_4), sum128_4);

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }

    let mut result = hsum128_ps_sse(sum128_1)
        + hsum128_ps_sse(sum128_2)
        + hsum128_ps_sse(sum128_3)
        + hsum128_ps_sse(sum128_4);

    // Handle remaining elements
    for i in 0..n - m {
        result += (*ptr1.add(i) - *ptr2.add(i)).abs();
    }

    result
}

/// Compute dot product using SSE instructions.
///
/// # Safety
/// Caller must ensure SSE is available on the current CPU.
#[target_feature(enable = "sse")]
pub unsafe fn dot_product_sse(v1: &[f32], v2: &[f32]) -> f32 {
    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();

    let mut sum128_1: __m128 = _mm_setzero_ps();
    let mut sum128_2: __m128 = _mm_setzero_ps();
    let mut sum128_3: __m128 = _mm_setzero_ps();
    let mut sum128_4: __m128 = _mm_setzero_ps();

    let mut i: usize = 0;
    while i < m {
        sum128_1 = _mm_add_ps(_mm_mul_ps(_mm_loadu_ps(ptr1), _mm_loadu_ps(ptr2)), sum128_1);
        sum128_2 = _mm_add_ps(
            _mm_mul_ps(_mm_loadu_ps(ptr1.add(4)), _mm_loadu_ps(ptr2.add(4))),
            sum128_2,
        );
        sum128_3 = _mm_add_ps(
            _mm_mul_ps(_mm_loadu_ps(ptr1.add(8)), _mm_loadu_ps(ptr2.add(8))),
            sum128_3,
        );
        sum128_4 = _mm_add_ps(
            _mm_mul_ps(_mm_loadu_ps(ptr1.add(12)), _mm_loadu_ps(ptr2.add(12))),
            sum128_4,
        );

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }

    let mut result = hsum128_ps_sse(sum128_1)
        + hsum128_ps_sse(sum128_2)
        + hsum128_ps_sse(sum128_3)
        + hsum128_ps_sse(sum128_4);

    // Handle remaining elements
    for i in 0..n - m {
        result += (*ptr1.add(i)) * (*ptr2.add(i));
    }

    result
}

/// Normalize a vector to unit length using SSE instructions.
///
/// # Safety
/// Caller must ensure SSE is available on the current CPU.
#[target_feature(enable = "sse")]
pub unsafe fn normalize_sse(vector: Vec<f32>) -> Vec<f32> {
    let n = vector.len();
    let m = n - (n % 16);
    let mut ptr: *const f32 = vector.as_ptr();

    let mut sum128_1: __m128 = _mm_setzero_ps();
    let mut sum128_2: __m128 = _mm_setzero_ps();
    let mut sum128_3: __m128 = _mm_setzero_ps();
    let mut sum128_4: __m128 = _mm_setzero_ps();

    let mut i: usize = 0;
    while i < m {
        let m128_1 = _mm_loadu_ps(ptr);
        sum128_1 = _mm_add_ps(_mm_mul_ps(m128_1, m128_1), sum128_1);

        let m128_2 = _mm_loadu_ps(ptr.add(4));
        sum128_2 = _mm_add_ps(_mm_mul_ps(m128_2, m128_2), sum128_2);

        let m128_3 = _mm_loadu_ps(ptr.add(8));
        sum128_3 = _mm_add_ps(_mm_mul_ps(m128_3, m128_3), sum128_3);

        let m128_4 = _mm_loadu_ps(ptr.add(12));
        sum128_4 = _mm_add_ps(_mm_mul_ps(m128_4, m128_4), sum128_4);

        ptr = ptr.add(16);
        i += 16;
    }

    let mut length_squared = hsum128_ps_sse(sum128_1)
        + hsum128_ps_sse(sum128_2)
        + hsum128_ps_sse(sum128_3)
        + hsum128_ps_sse(sum128_4);

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

    fn sse_available() -> bool {
        is_x86_feature_detected!("sse")
    }

    fn sse_test_vectors() -> (Vec<f32>, Vec<f32>) {
        (
            vec![
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                26., 27., 28., 29., 30., 31.,
            ],
            vec![
                40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                56., 57., 58., 59., 60., 61.,
            ],
        )
    }

    #[test]
    fn test_sse_l2_squared() {
        if !sse_available() {
            return;
        }

        let (v1, v2) = sse_test_vectors();

        let sse_result = unsafe { l2_squared_sse(&v1, &v2) };
        let scalar_result = scalar::l2_squared(&v1, &v2);
        assert!(
            (sse_result - scalar_result).abs() < 1e-5,
            "SSE: {}, Scalar: {}",
            sse_result,
            scalar_result
        );
    }

    #[test]
    fn test_sse_l1_distance() {
        if !sse_available() {
            return;
        }

        let (v1, v2) = sse_test_vectors();

        let sse_result = unsafe { l1_distance_sse(&v1, &v2) };
        let scalar_result = scalar::l1_distance(&v1, &v2);
        assert!(
            (sse_result - scalar_result).abs() < 1e-5,
            "SSE: {}, Scalar: {}",
            sse_result,
            scalar_result
        );
    }

    #[test]
    fn test_sse_dot_product() {
        if !sse_available() {
            return;
        }

        let (v1, v2) = sse_test_vectors();

        let sse_result = unsafe { dot_product_sse(&v1, &v2) };
        let scalar_result = scalar::dot_product(&v1, &v2);
        assert!(
            (sse_result - scalar_result).abs() < 1e-5,
            "SSE: {}, Scalar: {}",
            sse_result,
            scalar_result
        );
    }

    #[test]
    fn test_sse_normalize() {
        if !sse_available() {
            return;
        }

        let (v, _) = sse_test_vectors();

        let sse_result = unsafe { normalize_sse(v.clone()) };
        let scalar_result = scalar::normalize(v);
        assert_eq!(sse_result.len(), scalar_result.len());
        for (a, b) in sse_result.iter().zip(scalar_result.iter()) {
            assert!((a - b).abs() < 1e-6, "SSE: {}, Scalar: {}", a, b);
        }
    }
}
