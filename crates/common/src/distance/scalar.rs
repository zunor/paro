// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Scalar (non-SIMD) implementations of vector distance functions.
//!
//! These are the baseline implementations used when SIMD is not available
//! or when vector dimensions are too small to benefit from SIMD.

/// Compute L2 squared distance (sum of squared differences).
///
/// Returns the squared Euclidean distance without taking the square root.
/// This is useful for comparisons where the actual distance value is not needed.
#[inline]
pub fn l2_squared(v1: &[f32], v2: &[f32]) -> f32 {
    v1.iter()
        .zip(v2)
        .map(|(a, b)| {
            let diff = a - b;
            diff * diff
        })
        .sum()
}

/// Compute L1 distance (Manhattan distance, sum of absolute differences).
#[inline]
pub fn l1_distance(v1: &[f32], v2: &[f32]) -> f32 {
    v1.iter().zip(v2).map(|(a, b)| (a - b).abs()).sum()
}

/// Compute dot product (inner product) of two vectors.
#[inline]
pub fn dot_product(v1: &[f32], v2: &[f32]) -> f32 {
    v1.iter().zip(v2).map(|(a, b)| a * b).sum()
}

/// Normalize a vector to unit length.
///
/// If the vector is already normalized or is a zero vector, returns it unchanged.
/// This prevents instability from repeated normalization due to floating point errors.
#[inline]
pub fn normalize(vector: Vec<f32>) -> Vec<f32> {
    let length_squared: f32 = vector.iter().map(|x| x * x).sum();
    if is_length_zero_or_normalized(length_squared) {
        return vector;
    }
    let length = length_squared.sqrt();
    vector.iter().map(|x| x / length).collect()
}

/// Check if the length squared indicates a zero vector or already normalized vector.
///
/// When checking if normalized, we use a threshold of 1.0e-6 to account for
/// floating point errors. This prevents multiple normalization iterations
/// from being unstable.
#[inline]
pub(crate) fn is_length_zero_or_normalized(length_squared: f32) -> bool {
    // Zero vector check
    length_squared < f32::EPSILON
        // Already normalized check (length ≈ 1.0, so length² ≈ 1.0)
        || (length_squared - 1.0).abs() <= 1.0e-6
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_squared() {
        let v1 = vec![1.0, 2.0, 3.0];
        let v2 = vec![4.0, 5.0, 6.0];
        // (4-1)² + (5-2)² + (6-3)² = 9 + 9 + 9 = 27
        assert!((l2_squared(&v1, &v2) - 27.0).abs() < 1e-6);
    }

    #[test]
    fn test_l1_distance() {
        let v1 = vec![1.0, 2.0, 3.0];
        let v2 = vec![4.0, 5.0, 6.0];
        // |4-1| + |5-2| + |6-3| = 3 + 3 + 3 = 9
        assert!((l1_distance(&v1, &v2) - 9.0).abs() < 1e-6);
    }

    #[test]
    fn test_dot_product() {
        let v1 = vec![1.0, 2.0, 3.0];
        let v2 = vec![4.0, 5.0, 6.0];
        // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
        assert!((dot_product(&v1, &v2) - 32.0).abs() < 1e-6);
    }

    #[test]
    fn test_normalize() {
        let v = vec![3.0, 4.0];
        let normalized = normalize(v);
        // length = 5, so normalized = [0.6, 0.8]
        assert!((normalized[0] - 0.6).abs() < 1e-6);
        assert!((normalized[1] - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_normalize_zero_vector() {
        let v = vec![0.0, 0.0, 0.0];
        let normalized = normalize(v.clone());
        assert_eq!(normalized, v);
    }

    #[test]
    fn test_normalize_already_normalized() {
        let v = vec![0.6, 0.8]; // already unit length
        let normalized = normalize(v.clone());
        assert_eq!(normalized, v);
    }

    #[test]
    fn test_normalize_stability() {
        // Normalizing twice should give the same result
        let v = vec![1.0, 2.0, 3.0, 4.0];
        let n1 = normalize(v);
        let n2 = normalize(n1.clone());
        assert_eq!(n1, n2);
    }
}
