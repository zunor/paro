// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Distance Metrics
//!
//! Distance/similarity functions for dense vector comparison.
//!
//! ## Design Notes
//!
//! This module provides the HNSW-specific `DistanceMetric` enum that wraps
//! Paro's shared distance primitives (`paro_common::distance`)
//! with the following semantics:
//!
//! - **Similarity direction**: All `similarity()` scores follow the convention
//!   where **higher = more similar**. Distance-based metrics (Euclidean, Manhattan)
//!   are negated to achieve this.
//! - **SIMD acceleration**: Automatically inherited from `paro-common`'s
//!   implementations (AVX/SSE/NEON), no duplicate SIMD code here.
//! - **Preprocessing**: Cosine queries are normalized once; table vectors stay
//!   raw and the HNSW artifact stores per-point inverse norms.
//! - **Postprocessing**: Converts internal scores back to user-facing distances.
//!
//! ## Two-Layer Architecture
//!
//! ```text
//! Layer 1 (paro-common):  l2_squared, l1_distance, dot_product, normalize
//!                         ↑ SIMD auto-dispatched (AVX/SSE/NEON/scalar)
//!
//! Layer 2 (this module):  DistanceMetric { Cosine, Euclidean, DotProduct, Manhattan }
//!                         ↑ semantic wrapper: negation, pre/postprocess
//! ```

use paro_common::distance;
use serde::{Deserialize, Serialize};

use super::types::{PreparedQuery, ScoreType};

/// Distance metric type for vector comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetric {
    /// Cosine similarity (dot product with index-private inverse norms).
    /// Range: [-1, 1], higher is more similar.
    Cosine,
    /// Euclidean distance (L2 norm).
    /// Internal score: -l2_squared(v1, v2), postprocessed: sqrt(abs(score)).
    Euclidean,
    /// Dot product similarity.
    /// Range: (-inf, +inf), higher is more similar.
    DotProduct,
    /// Manhattan distance (L1 norm).
    /// Internal score: -l1_distance(v1, v2), postprocessed: abs(score).
    Manhattan,
}

impl DistanceMetric {
    /// Decode the stable tablet-schema distance tag.
    pub const fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(DistanceMetric::Euclidean),
            1 => Some(DistanceMetric::Cosine),
            2 => Some(DistanceMetric::DotProduct),
            3 => Some(DistanceMetric::Manhattan),
            _ => None,
        }
    }

    /// Parse a SQL index option and normalize aliases to the durable name.
    pub fn parse_sql_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "l2" | "euclidean" => Some(Self::Euclidean),
            "cos" | "cosine" => Some(Self::Cosine),
            "dot" | "dot_product" | "inner_product" | "ip" => Some(Self::DotProduct),
            "l1" | "manhattan" => Some(Self::Manhattan),
            _ => None,
        }
    }

    pub const fn durable_name(self) -> &'static str {
        match self {
            Self::Euclidean => "euclidean",
            Self::Cosine => "cosine",
            Self::DotProduct => "dot_product",
            Self::Manhattan => "manhattan",
        }
    }

    /// Compute similarity between two vectors.
    ///
    /// Returns a score where higher values indicate more similar vectors.
    /// For distance-based metrics (Euclidean, Manhattan), the score is negated.
    ///
    /// Delegates to `paro_common::distance` for SIMD-accelerated
    /// computation.
    pub fn similarity(&self, v1: &[f32], v2: &[f32]) -> ScoreType {
        debug_assert_eq!(v1.len(), v2.len(), "Vector dimensions must match");
        match self {
            DistanceMetric::Cosine => 1.0 - distance::cosine_distance(v1, v2),
            DistanceMetric::DotProduct => distance::dot_product(v1, v2),
            DistanceMetric::Euclidean => -distance::l2_squared(v1, v2),
            DistanceMetric::Manhattan => -distance::l1_distance(v1, v2),
        }
    }

    /// Preprocess a query before graph scoring.
    ///
    /// For Cosine, this normalizes the vector to unit length.
    /// For other metrics, vectors are returned as-is.
    pub fn preprocess_query(&self, vector: Vec<f32>) -> Vec<f32> {
        match self {
            DistanceMetric::Cosine => distance::normalize(vector),
            _ => vector,
        }
    }

    /// Preprocess a raw query and record which metric produced it.
    pub fn prepare(&self, raw_query: &[f32]) -> PreparedQuery {
        PreparedQuery::new(self.preprocess_query(raw_query.to_vec()), *self)
    }

    /// Score a vector against a query produced by [`Self::prepare`]. Cosine
    /// queries are already unit length, so only the stored vector norm remains
    /// in the hot loop. Stored table values themselves stay byte-for-byte
    /// unchanged by index construction.
    pub fn similarity_unindexed(&self, prepared_query: &[f32], vector: &[f32]) -> ScoreType {
        debug_assert_eq!(
            prepared_query.len(),
            vector.len(),
            "Vector dimensions must match"
        );
        if !matches!(self, Self::Cosine) {
            return self.similarity(prepared_query, vector);
        }
        distance::dot_product(prepared_query, vector) * distance::inverse_norm(vector)
    }

    /// Score a prepared query against an indexed point. Cosine indexes persist
    /// `inverse_norm` once per point, eliminating a second vector pass and
    /// square root from every graph visit.
    #[inline]
    pub fn similarity_prepared_with_norm(
        &self,
        prepared_query: &[f32],
        vector: &[f32],
        cosine_inverse_norm: f32,
    ) -> ScoreType {
        match self {
            Self::Cosine => distance::dot_product(prepared_query, vector) * cosine_inverse_norm,
            _ => self.similarity(prepared_query, vector),
        }
    }

    /// Score two points whose cosine inverse norms are part of the index
    /// artifact. Other metrics ignore the preprocessing values.
    #[inline]
    pub fn similarity_indexed(
        &self,
        v1: &[f32],
        v2: &[f32],
        cosine_inverse_norm_1: f32,
        cosine_inverse_norm_2: f32,
    ) -> ScoreType {
        match self {
            Self::Cosine => {
                distance::dot_product(v1, v2) * cosine_inverse_norm_1 * cosine_inverse_norm_2
            }
            _ => self.similarity(v1, v2),
        }
    }

    /// Convert the internal larger-is-better score to the corresponding SQL
    /// distance function result. All currently optimized SQL vector operators
    /// order this value ascending.
    pub fn postprocess(&self, score: ScoreType) -> ScoreType {
        match self {
            DistanceMetric::Cosine => 1.0 - score,
            DistanceMetric::DotProduct => -score,
            DistanceMetric::Euclidean => score.abs().sqrt(),
            DistanceMetric::Manhattan => score.abs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPSILON: f32 = 1e-5;

    fn approx_eq(a: f32, b: f32) -> bool {
        (a - b).abs() < EPSILON
    }

    #[test]
    fn test_dot_similarity() {
        let v1 = vec![1.0, 2.0, 3.0];
        let v2 = vec![4.0, 5.0, 6.0];
        // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
        let score = DistanceMetric::DotProduct.similarity(&v1, &v2);
        assert!(approx_eq(score, 32.0));
    }

    #[test]
    fn test_euclid_similarity() {
        let v1 = vec![1.0, 0.0, 0.0];
        let v2 = vec![0.0, 1.0, 0.0];
        // -((1-0)^2 + (0-1)^2 + (0-0)^2) = -(1 + 1 + 0) = -2
        let score = DistanceMetric::Euclidean.similarity(&v1, &v2);
        assert!(approx_eq(score, -2.0));
    }

    #[test]
    fn test_manhattan_similarity() {
        let v1 = vec![1.0, 2.0, 3.0];
        let v2 = vec![4.0, 6.0, 3.0];
        // -(|1-4| + |2-6| + |3-3|) = -(3 + 4 + 0) = -7
        let score = DistanceMetric::Manhattan.similarity(&v1, &v2);
        assert!(approx_eq(score, -7.0));
    }

    #[test]
    fn test_cosine_preprocess() {
        let v = vec![3.0, 4.0];
        let normalized = DistanceMetric::Cosine.preprocess_query(v);
        // length = 5, normalized = [0.6, 0.8]
        assert!(approx_eq(normalized[0], 0.6));
        assert!(approx_eq(normalized[1], 0.8));

        // Check that the normalized vector has unit length
        let len_sq: f32 = normalized.iter().map(|x| x * x).sum();
        assert!(approx_eq(len_sq, 1.0));
    }

    #[test]
    fn test_cosine_preprocess_zero_vector() {
        let v = vec![0.0, 0.0, 0.0];
        let result = DistanceMetric::Cosine.preprocess_query(v.clone());
        assert_eq!(result, v);
    }

    #[test]
    fn test_cosine_preprocess_already_normalized() {
        let v = vec![1.0, 0.0, 0.0];
        let result = DistanceMetric::Cosine.preprocess_query(v.clone());
        assert_eq!(result, v);
    }

    #[test]
    fn test_cosine_preprocess_stable() {
        // Renormalization should produce the same result
        let v = vec![1.5, 2.5, -0.5, 3.0];
        let first = DistanceMetric::Cosine.preprocess_query(v);
        let second = DistanceMetric::Cosine.preprocess_query(first.clone());
        assert_eq!(first, second);
    }

    #[test]
    fn test_prepare_records_metric_provenance() {
        let prepared = DistanceMetric::Cosine.prepare(&[3.0, 4.0]);
        assert_eq!(prepared.metric(), DistanceMetric::Cosine);
        assert!(approx_eq(prepared.as_slice()[0], 0.6));
        assert!(approx_eq(prepared.as_slice()[1], 0.8));
    }

    #[test]
    fn test_distance_metric_similarity() {
        let v1 = vec![1.0, 0.0];
        let v2 = vec![0.0, 1.0];

        // Euclidean
        let score = DistanceMetric::Euclidean.similarity(&v1, &v2);
        assert!(approx_eq(score, -2.0));
        let dist = DistanceMetric::Euclidean.postprocess(score);
        assert!(approx_eq(dist, std::f32::consts::SQRT_2));

        // Manhattan
        let score = DistanceMetric::Manhattan.similarity(&v1, &v2);
        assert!(approx_eq(score, -2.0));
        let dist = DistanceMetric::Manhattan.postprocess(score);
        assert!(approx_eq(dist, 2.0));

        // DotProduct
        let score = DistanceMetric::DotProduct.similarity(&v1, &v2);
        assert!(approx_eq(score, 0.0));
        assert!(approx_eq(DistanceMetric::DotProduct.postprocess(3.5), -3.5));
        assert!(approx_eq(DistanceMetric::Cosine.postprocess(0.75), 0.25));
    }

    #[test]
    fn test_cosine_similarity() {
        let metric = DistanceMetric::Cosine;
        let v1 = vec![3.0, 4.0];
        let v2 = vec![4.0, 3.0];
        let score = metric.similarity(&v1, &v2);
        // cos(angle) = (3*4 + 4*3) / (5 * 5) = 24/25 = 0.96
        assert!(approx_eq(score, 0.96));

        // Query preparation must preserve the same score against an unmodified
        // persisted vector. Index construction must not normalize SQL values.
        let prepared = metric.prepare(&v1);
        assert!(approx_eq(
            metric.similarity_unindexed(prepared.as_slice(), &v2),
            0.96
        ));
        assert!(approx_eq(
            metric.similarity_unindexed(prepared.as_slice(), &[0.0, 0.0]),
            0.0
        ));
    }

    // ========================================================================
    // SIMD consistency tests (large vectors to trigger SIMD paths)
    // ========================================================================

    #[test]
    fn test_simd_euclid_similarity_large() {
        // 128 dims → triggers AVX/SSE/NEON path
        let v1: Vec<f32> = (0..128).map(|i| (i as f32) * 0.5).collect();
        let v2: Vec<f32> = (128..256).map(|i| (i as f32) * 0.5).collect();

        let score = DistanceMetric::Euclidean.similarity(&v1, &v2);
        // Should be negative (negated L2 squared)
        assert!(score < 0.0);

        // Verify against manual scalar computation
        let expected: f32 = -v1
            .iter()
            .zip(v2.iter())
            .map(|(a, b)| (a - b) * (a - b))
            .sum::<f32>();
        assert!(
            (score - expected).abs() < 1.0,
            "SIMD score {} differs from expected {}",
            score,
            expected
        );
    }

    #[test]
    fn test_simd_dot_similarity_large() {
        let v1: Vec<f32> = (0..128).map(|i| (i as f32) * 0.1).collect();
        let v2: Vec<f32> = (128..256).map(|i| (i as f32) * 0.1).collect();

        let score = DistanceMetric::DotProduct.similarity(&v1, &v2);
        let expected: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
        assert!(
            (score - expected).abs() < 1.0,
            "SIMD score {} differs from expected {}",
            score,
            expected
        );
    }

    #[test]
    fn test_simd_manhattan_similarity_large() {
        let v1: Vec<f32> = (0..128).map(|i| (i as f32) * 0.5).collect();
        let v2: Vec<f32> = (128..256).map(|i| (i as f32) * 0.5).collect();

        let score = DistanceMetric::Manhattan.similarity(&v1, &v2);
        assert!(score < 0.0);

        let expected: f32 = -v1
            .iter()
            .zip(v2.iter())
            .map(|(a, b)| (a - b).abs())
            .sum::<f32>();
        assert!(
            (score - expected).abs() < 1.0,
            "SIMD score {} differs from expected {}",
            score,
            expected
        );
    }

    #[test]
    fn test_simd_cosine_preprocess_large() {
        let v: Vec<f32> = (1..129).map(|i| i as f32).collect();
        let normalized = DistanceMetric::Cosine.preprocess_query(v);

        // Verify unit length
        let len_sq: f32 = normalized.iter().map(|x| x * x).sum();
        assert!(
            (len_sq - 1.0).abs() < 1e-5,
            "Normalized vector length squared {} is not 1.0",
            len_sq
        );

        // Verify stability (renormalization produces same result)
        let renormalized = DistanceMetric::Cosine.preprocess_query(normalized.clone());
        assert_eq!(normalized, renormalized);
    }
}
