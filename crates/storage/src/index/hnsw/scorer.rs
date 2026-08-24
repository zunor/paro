// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Vector Scorer
//!
//! Distance scoring helpers shared by graph search and plain/full-scan paths.

use std::cell::Cell;

use paro_common::distance;

use super::types::{PointOffset, ScoreType, ScoredPoint};
use super::vector_storage::CosineInverseNorms;
use super::{DistanceMetric, PreparedQuery, VectorStorage};

enum ScoringKernel<'a> {
    Cosine(&'a CosineInverseNorms),
    Euclidean,
    DotProduct,
    Manhattan,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::InMemoryVectorStorage;

    #[test]
    fn cosine_kernel_requires_norms_at_construction_boundary() {
        let storage = InMemoryVectorStorage::new(vec![1.0, 0.0], 2);
        let query = DistanceMetric::Cosine.prepare(&[1.0, 0.0]);
        assert!(VectorScorer::new(&query, &storage)
            .err()
            .expect("cosine scorer without norms must fail")
            .to_string()
            .contains("missing per-point inverse norms"));
    }
}

/// Vector scorer responsible for calculating distances during search and build.
pub struct VectorScorer<'a> {
    query: &'a PreparedQuery,
    vectors: &'a [f32],
    dimension: usize,
    kernel: ScoringKernel<'a>,
    scores_buffer: Vec<ScoreType>,
    scored_points: Cell<u64>,
}

impl<'a> VectorScorer<'a> {
    pub fn new(
        query: &'a PreparedQuery,
        vector_storage: &'a dyn VectorStorage,
    ) -> paro_common::error::Result<Self> {
        let kernel = match query.metric() {
            DistanceMetric::Cosine => {
                let norms = vector_storage.cosine_inverse_norms().ok_or_else(|| {
                    paro_common::error::data_corrupted(
                        "cosine HNSW artifact is missing per-point inverse norms",
                    )
                })?;
                if norms.len() != vector_storage.num_vectors() {
                    return Err(paro_common::error::data_corrupted(format!(
                        "HNSW cosine inverse norm count mismatch: expected {}, got {}",
                        vector_storage.num_vectors(),
                        norms.len()
                    )));
                }
                ScoringKernel::Cosine(norms)
            }
            DistanceMetric::Euclidean => ScoringKernel::Euclidean,
            DistanceMetric::DotProduct => ScoringKernel::DotProduct,
            DistanceMetric::Manhattan => ScoringKernel::Manhattan,
        };
        let dimension = vector_storage.vector_dim();
        let vectors = vector_storage.flat_vectors();
        let expected_values = vector_storage
            .num_vectors()
            .checked_mul(dimension)
            .ok_or_else(|| {
                paro_common::error::data_corrupted("HNSW vector artifact cardinality overflow")
            })?;
        if vectors.len() != expected_values {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW vector artifact length mismatch: expected {expected_values} values, got {}",
                vectors.len()
            )));
        }
        Ok(Self {
            query,
            vectors,
            dimension,
            kernel,
            scores_buffer: Vec::new(),
            scored_points: Cell::new(0),
        })
    }

    /// Score a single point.
    pub fn score_point(&self, point_id: PointOffset) -> ScoreType {
        self.scored_points
            .set(self.scored_points.get().saturating_add(1));
        self.score_cached_point(point_id, self.vector(point_id))
    }

    #[inline]
    pub(crate) fn vector(&self, point_id: PointOffset) -> &[f32] {
        let start = point_id as usize * self.dimension;
        &self.vectors[start..start + self.dimension]
    }

    #[inline]
    pub(crate) fn vector_layout(&self) -> (&'a [f32], usize) {
        (self.vectors, self.dimension)
    }

    pub fn scored_point_count(&self) -> u64 {
        self.scored_points.get()
    }

    /// Score an indexed point whose vector has already been fetched.
    pub fn score_cached_point(&self, point_id: PointOffset, vector: &[f32]) -> ScoreType {
        match self.kernel {
            ScoringKernel::Cosine(norms) => self.query.metric().similarity_prepared_with_norm(
                self.query.as_slice(),
                vector,
                norms.value(point_id),
            ),
            ScoringKernel::Euclidean => -distance::l2_squared(self.query.as_slice(), vector),
            ScoringKernel::DotProduct => distance::dot_product(self.query.as_slice(), vector),
            ScoringKernel::Manhattan => -distance::l1_distance(self.query.as_slice(), vector),
        }
    }

    /// Score a batch of points into a caller-provided buffer.
    pub fn score_into(&self, points: &[PointOffset], scores: &mut [ScoreType]) {
        assert!(
            scores.len() >= points.len(),
            "scores buffer is shorter than point batch",
        );
        for (i, &point_id) in points.iter().enumerate() {
            scores[i] = self.score_point(point_id);
        }
    }

    /// Score points without any filtering.
    pub fn score_points_unfiltered<'b>(
        &'b mut self,
        points: &'b [PointOffset],
    ) -> impl Iterator<Item = ScoredPoint> + 'b {
        self.scores_buffer.resize(points.len(), 0.0);
        self.scored_points
            .set(self.scored_points.get().saturating_add(points.len() as u64));
        let query = self.query.as_slice();
        match self.kernel {
            ScoringKernel::Cosine(norms) => {
                for (i, &point_id) in points.iter().enumerate() {
                    self.scores_buffer[i] =
                        distance::dot_product(query, self.vector(point_id)) * norms.value(point_id);
                }
            }
            ScoringKernel::Euclidean => {
                distance::l2_squared_batch_indexed(
                    query,
                    self.vectors,
                    self.dimension,
                    points,
                    &mut self.scores_buffer,
                );
                for score in &mut self.scores_buffer {
                    *score = -*score;
                }
            }
            ScoringKernel::DotProduct => {
                for (i, &point_id) in points.iter().enumerate() {
                    self.scores_buffer[i] = distance::dot_product(query, self.vector(point_id));
                }
            }
            ScoringKernel::Manhattan => {
                for (i, &point_id) in points.iter().enumerate() {
                    self.scores_buffer[i] = -distance::l1_distance(query, self.vector(point_id));
                }
            }
        }
        let scores = &self.scores_buffer;
        points
            .iter()
            .zip(scores.iter())
            .map(|(&idx, &score)| ScoredPoint { idx, score })
    }

    /// Score points with optional filtering and optional limit.
    pub fn score_points<'b>(
        &'b mut self,
        points: &'b mut Vec<PointOffset>,
        filter_bitmap: Option<&roaring::RoaringBitmap>,
        limit: usize,
    ) -> impl Iterator<Item = ScoredPoint> + 'b {
        if let Some(bitmap) = filter_bitmap {
            points.retain(|id| bitmap.contains(*id));
        }
        if limit != 0 {
            points.truncate(limit);
        }
        self.score_points_unfiltered(points.as_slice())
    }
}
