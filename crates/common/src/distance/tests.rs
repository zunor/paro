// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Comprehensive tests for vector distance functions.
//!
//! This module contains tests for:
//! 1. Basic correctness of all distance functions
//! 2. SIMD vs scalar implementation consistency
//! 3. Edge cases: empty vectors, single element, unaligned lengths
//! 4. Normalize stability: multiple normalizations produce same result
//! 5. Zero vector handling

use super::*;

// ============================================================================
// Basic Correctness Tests
// ============================================================================

#[test]
fn test_l2_squared_basic() {
    let v1 = vec![1.0, 2.0, 3.0];
    let v2 = vec![4.0, 5.0, 6.0];
    // (4-1)² + (5-2)² + (6-3)² = 9 + 9 + 9 = 27
    assert!((l2_squared(&v1, &v2) - 27.0).abs() < 1e-5);
}

#[test]
fn test_l2_distance_basic() {
    let v1 = vec![0.0, 0.0];
    let v2 = vec![3.0, 4.0];
    // sqrt(9 + 16) = 5
    assert!((l2_distance(&v1, &v2) - 5.0).abs() < 1e-5);
}

#[test]
fn test_l1_distance_basic() {
    let v1 = vec![1.0, 2.0, 3.0];
    let v2 = vec![4.0, 5.0, 6.0];
    // |4-1| + |5-2| + |6-3| = 9
    assert!((l1_distance(&v1, &v2) - 9.0).abs() < 1e-5);
}

#[test]
fn test_dot_product_basic() {
    let v1 = vec![1.0, 2.0, 3.0];
    let v2 = vec![4.0, 5.0, 6.0];
    // 1*4 + 2*5 + 3*6 = 32
    assert!((dot_product(&v1, &v2) - 32.0).abs() < 1e-5);
}

#[test]
fn test_cosine_distance_same_direction() {
    let v1 = vec![1.0, 2.0, 3.0];
    let v2 = vec![2.0, 4.0, 6.0];
    // Same direction, cosine similarity = 1, distance = 0
    assert!(cosine_distance(&v1, &v2).abs() < 1e-5);
}

#[test]
fn test_cosine_distance_orthogonal() {
    let v1 = vec![1.0, 0.0];
    let v2 = vec![0.0, 1.0];
    // Orthogonal, cosine similarity = 0, distance = 1
    assert!((cosine_distance(&v1, &v2) - 1.0).abs() < 1e-5);
}

#[test]
fn test_cosine_distance_opposite() {
    let v1 = vec![1.0, 0.0];
    let v2 = vec![-1.0, 0.0];
    // Opposite direction, cosine similarity = -1, distance = 2
    assert!((cosine_distance(&v1, &v2) - 2.0).abs() < 1e-5);
}

#[test]
fn test_normalize_basic() {
    let v = vec![3.0, 4.0];
    let normalized = normalize(v);
    // length = 5, so normalized = [0.6, 0.8]
    assert!((normalized[0] - 0.6).abs() < 1e-5);
    assert!((normalized[1] - 0.8).abs() < 1e-5);

    // Verify unit length
    let length_sq: f32 = normalized.iter().map(|x| x * x).sum();
    assert!((length_sq - 1.0).abs() < 1e-5);
}

// ============================================================================
// Edge Cases: Empty and Single Element Vectors
// ============================================================================

#[test]
fn test_empty_vectors() {
    let v1: Vec<f32> = vec![];
    let v2: Vec<f32> = vec![];

    assert_eq!(l2_squared(&v1, &v2), 0.0);
    assert_eq!(l2_distance(&v1, &v2), 0.0);
    assert_eq!(l1_distance(&v1, &v2), 0.0);
    assert_eq!(dot_product(&v1, &v2), 0.0);
}

#[test]
fn test_single_element_vectors() {
    let v1 = vec![3.0];
    let v2 = vec![7.0];

    // L2 squared: (7-3)² = 16
    assert!((l2_squared(&v1, &v2) - 16.0).abs() < 1e-5);

    // L2 distance: 4
    assert!((l2_distance(&v1, &v2) - 4.0).abs() < 1e-5);

    // L1 distance: |7-3| = 4
    assert!((l1_distance(&v1, &v2) - 4.0).abs() < 1e-5);

    // Dot product: 3*7 = 21
    assert!((dot_product(&v1, &v2) - 21.0).abs() < 1e-5);
}

#[test]
fn test_normalize_single_element() {
    let v = vec![5.0];
    let normalized = normalize(v);
    // Single element normalized to 1.0
    assert!((normalized[0] - 1.0).abs() < 1e-5);
}

// ============================================================================
// Edge Cases: Unaligned Lengths (not multiples of SIMD width)
// ============================================================================

#[test]
fn test_unaligned_length_17() {
    // 17 elements: not aligned to 16 (SSE/NEON) or 32 (AVX)
    let v1: Vec<f32> = (0..17).map(|i| i as f32).collect();
    let v2: Vec<f32> = (17..34).map(|i| i as f32).collect();

    let l2_result = l2_squared(&v1, &v2);
    let expected_l2: f32 = v1
        .iter()
        .zip(v2.iter())
        .map(|(a, b)| (a - b) * (a - b))
        .sum();
    assert!((l2_result - expected_l2).abs() < 1e-3);

    let dot_result = dot_product(&v1, &v2);
    let expected_dot: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
    assert!((dot_result - expected_dot).abs() < 1e-3);
}

#[test]
fn test_unaligned_length_33() {
    // 33 elements: triggers AVX path but has remainder
    let v1: Vec<f32> = (0..33).map(|i| i as f32).collect();
    let v2: Vec<f32> = (33..66).map(|i| i as f32).collect();

    let l2_result = l2_squared(&v1, &v2);
    let expected_l2: f32 = v1
        .iter()
        .zip(v2.iter())
        .map(|(a, b)| (a - b) * (a - b))
        .sum();
    assert!((l2_result - expected_l2).abs() < 1e-2);
}

#[test]
fn test_unaligned_length_50() {
    // 50 elements: triggers AVX but not aligned to 32
    let v1: Vec<f32> = (0..50).map(|i| (i as f32) * 0.1).collect();
    let v2: Vec<f32> = (50..100).map(|i| (i as f32) * 0.1).collect();

    let l1_result = l1_distance(&v1, &v2);
    let expected_l1: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| (a - b).abs()).sum();
    assert!((l1_result - expected_l1).abs() < 1e-3);
}

// ============================================================================
// Zero Vector Handling
// ============================================================================

#[test]
fn test_normalize_zero_vector() {
    let v = vec![0.0, 0.0, 0.0];
    let normalized = normalize(v.clone());
    // Zero vector should remain unchanged
    assert_eq!(normalized, v);
}

#[test]
fn test_cosine_distance_with_zero_vector() {
    let v1 = vec![0.0, 0.0, 0.0];
    let v2 = vec![1.0, 2.0, 3.0];
    // Zero vector should return maximum distance (1.0)
    assert!((cosine_distance(&v1, &v2) - 1.0).abs() < 1e-5);
}

#[test]
fn test_cosine_distance_both_zero() {
    let v1 = vec![0.0, 0.0];
    let v2 = vec![0.0, 0.0];
    // Both zero vectors should return 1.0 (maximum distance)
    assert!((cosine_distance(&v1, &v2) - 1.0).abs() < 1e-5);
}

// ============================================================================
// Normalize Stability Tests
// ============================================================================

#[test]
fn test_normalize_stability_basic() {
    let v = vec![1.0, 2.0, 3.0, 4.0];
    let n1 = normalize(v);
    let n2 = normalize(n1.clone());
    // Normalizing twice should give the same result
    assert_eq!(n1, n2);
}

#[test]
fn test_normalize_stability_large_vector() {
    // Large vector to trigger SIMD path
    let v: Vec<f32> = (0..100).map(|i| (i as f32) * 0.1 + 0.5).collect();
    let n1 = normalize(v);
    let n2 = normalize(n1.clone());
    let n3 = normalize(n2.clone());

    // All normalizations should be identical
    assert_eq!(n1, n2);
    assert_eq!(n2, n3);
}

#[test]
fn test_normalize_already_normalized() {
    let v = vec![0.6, 0.8]; // Already unit length
    let normalized = normalize(v.clone());
    // Should remain unchanged
    assert_eq!(normalized, v);
}

#[test]
fn test_normalize_near_unit_length() {
    // Vector very close to unit length (within threshold)
    let v = vec![0.6000001, 0.7999999];
    let length_sq: f32 = v.iter().map(|x| x * x).sum();
    // Should be close to 1.0
    assert!((length_sq - 1.0).abs() < 1e-5);

    let normalized = normalize(v.clone());
    // Should remain unchanged due to threshold
    assert_eq!(normalized, v);
}

// ============================================================================
// SIMD vs Scalar Consistency Tests (Large Vectors)
// ============================================================================

#[test]
fn test_simd_consistency_l2_squared() {
    // Large enough to trigger SIMD (>= 32 for AVX, >= 16 for SSE/NEON)
    let v1: Vec<f32> = (0..128).map(|i| (i as f32) * 0.5).collect();
    let v2: Vec<f32> = (128..256).map(|i| (i as f32) * 0.5).collect();

    let result = l2_squared(&v1, &v2);
    let expected: f32 = v1
        .iter()
        .zip(v2.iter())
        .map(|(a, b)| (a - b) * (a - b))
        .sum();

    // Allow small floating point difference
    assert!(
        (result - expected).abs() < 1e-2,
        "SIMD result {} differs from scalar {}",
        result,
        expected
    );
}

#[test]
fn test_simd_consistency_l1_distance() {
    let v1: Vec<f32> = (0..128).map(|i| (i as f32) * 0.5).collect();
    let v2: Vec<f32> = (128..256).map(|i| (i as f32) * 0.5).collect();

    let result = l1_distance(&v1, &v2);
    let expected: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| (a - b).abs()).sum();

    assert!(
        (result - expected).abs() < 1e-2,
        "SIMD result {} differs from scalar {}",
        result,
        expected
    );
}

#[test]
fn test_simd_consistency_dot_product() {
    let v1: Vec<f32> = (0..128).map(|i| (i as f32) * 0.1).collect();
    let v2: Vec<f32> = (128..256).map(|i| (i as f32) * 0.1).collect();

    let result = dot_product(&v1, &v2);
    let expected: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();

    assert!(
        (result - expected).abs() < 1e-1,
        "SIMD result {} differs from scalar {}",
        result,
        expected
    );
}

#[test]
fn test_simd_consistency_normalize() {
    let v: Vec<f32> = (1..129).map(|i| i as f32).collect();

    let normalized = normalize(v.clone());

    // Verify unit length
    let length_sq: f32 = normalized.iter().map(|x| x * x).sum();
    assert!(
        (length_sq - 1.0).abs() < 1e-5,
        "Normalized vector length squared {} is not 1.0",
        length_sq
    );

    // Verify direction preserved (proportional)
    let ratio = v[0] / normalized[0];
    for (orig, norm) in v.iter().zip(normalized.iter()) {
        let expected = orig / ratio;
        assert!(
            (norm - expected).abs() < 1e-4,
            "Direction not preserved: {} vs {}",
            norm,
            expected
        );
    }
}

#[test]
fn test_simd_consistency_cosine_distance() {
    let v1: Vec<f32> = (1..65).map(|i| i as f32).collect();
    let v2: Vec<f32> = (65..129).map(|i| i as f32).collect();

    let result = cosine_distance(&v1, &v2);

    // Compute expected using scalar operations
    let dot: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
    let norm1: f32 = v1.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm2: f32 = v2.iter().map(|x| x * x).sum::<f32>().sqrt();
    let expected = 1.0 - dot / (norm1 * norm2);

    assert!(
        (result - expected).abs() < 1e-5,
        "Cosine distance {} differs from expected {}",
        result,
        expected
    );
}

// ============================================================================
// Negative Value Tests
// ============================================================================

#[test]
fn test_negative_values() {
    let v1 = vec![-1.0, -2.0, -3.0];
    let v2 = vec![1.0, 2.0, 3.0];

    // L2 squared: (-1-1)² + (-2-2)² + (-3-3)² = 4 + 16 + 36 = 56
    assert!((l2_squared(&v1, &v2) - 56.0).abs() < 1e-5);

    // L1 distance: |-1-1| + |-2-2| + |-3-3| = 2 + 4 + 6 = 12
    assert!((l1_distance(&v1, &v2) - 12.0).abs() < 1e-5);

    // Dot product: -1*1 + -2*2 + -3*3 = -1 - 4 - 9 = -14
    assert!((dot_product(&v1, &v2) - (-14.0)).abs() < 1e-5);
}

#[test]
fn test_mixed_sign_values() {
    let v1 = vec![-1.0, 2.0, -3.0, 4.0];
    let v2 = vec![1.0, -2.0, 3.0, -4.0];

    // Opposite signs, cosine distance should be 2.0
    assert!((cosine_distance(&v1, &v2) - 2.0).abs() < 1e-5);
}

// ============================================================================
// Large Value Tests
// ============================================================================

#[test]
fn test_large_values() {
    let v1 = vec![1e6, 2e6, 3e6];
    let v2 = vec![4e6, 5e6, 6e6];

    // Should handle large values without overflow
    let l2 = l2_squared(&v1, &v2);
    assert!(l2.is_finite());
    assert!(l2 > 0.0);

    let dot = dot_product(&v1, &v2);
    assert!(dot.is_finite());
    assert!(dot > 0.0);
}

#[test]
fn test_small_values() {
    let v1 = vec![1e-6, 2e-6, 3e-6];
    let v2 = vec![4e-6, 5e-6, 6e-6];

    // Should handle small values without underflow issues
    let l2 = l2_squared(&v1, &v2);
    assert!(l2.is_finite());
    assert!(l2 > 0.0);

    let dot = dot_product(&v1, &v2);
    assert!(dot.is_finite());
    assert!(dot > 0.0);
}

// ============================================================================
// Identical Vectors Tests
// ============================================================================

#[test]
fn test_identical_vectors() {
    let v = vec![1.0, 2.0, 3.0, 4.0, 5.0];

    // L2 distance to self should be 0
    assert_eq!(l2_squared(&v, &v), 0.0);
    assert_eq!(l2_distance(&v, &v), 0.0);

    // L1 distance to self should be 0
    assert_eq!(l1_distance(&v, &v), 0.0);

    // Cosine distance to self should be 0
    assert!(cosine_distance(&v, &v).abs() < 1e-5);
}

#[test]
fn test_identical_vectors_large() {
    // Large vector to trigger SIMD
    let v: Vec<f32> = (0..100).map(|i| i as f32).collect();

    assert!(l2_squared(&v, &v).abs() < 1e-5);
    assert!(l1_distance(&v, &v).abs() < 1e-5);
    assert!(cosine_distance(&v, &v).abs() < 1e-5);
}
