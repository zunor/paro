// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Vector Scorer
//!
//! Distance scoring helpers shared by graph search and plain/full-scan paths.

use std::cell::Cell;

use paro_common::distance;

use super::graph_links::GraphPoint;
use super::types::{PointOffset, ScoreType, ScoredPoint};
use super::vector_storage::{
    f32_query_i16_dot_product, f32_query_i16_l1_distance, f32_query_i16_l2_squared,
    CosineInverseNorms, I16RoutingView,
};
use super::{DistanceMetric, PreparedQuery, VectorStorage};

#[derive(Clone, Copy)]
enum ScoringKernel<'a> {
    Cosine(&'a CosineInverseNorms),
    Euclidean,
    DotProduct,
    Manhattan,
}

/// Immutable, shareable scoring plan for independent exact-scan partitions.
///
/// `VectorScorer` owns mutable scratch and telemetry and is intentionally not
/// `Sync`. Resolving the metric and artifact invariants once into this value
/// lets parallel workers create isolated scorers without repeating validation
/// or sharing interior-mutable state.
#[derive(Clone, Copy)]
pub(crate) struct PreparedVectorScoring<'a> {
    query: &'a PreparedQuery,
    storage: &'a dyn VectorStorage,
    vectors: Option<&'a [f32]>,
    dimension: usize,
    point_count: usize,
    kernel: ScoringKernel<'a>,
}

impl<'a> PreparedVectorScoring<'a> {
    pub(crate) fn scorer(self) -> VectorScorer<'a> {
        VectorScorer {
            query: self.query,
            storage: self.storage,
            vectors: self.vectors,
            dimension: self.dimension,
            point_count: self.point_count,
            kernel: self.kernel,
            scores_buffer: Vec::new(),
            scored_points: Cell::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::vector_storage::prepare_build_vector_storage;
    use super::*;
    use crate::index::hnsw::{
        HnswBuildVectorEncoding, InMemoryVectorStorage, PartitionedVectorStorage,
    };
    use std::sync::Arc;

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

    #[test]
    fn compact_routing_does_not_clamp_queries_to_the_build_range() {
        let storage: Arc<dyn VectorStorage> =
            Arc::new(InMemoryVectorStorage::new(vec![0.0, 1.0], 1));
        let storage = prepare_build_vector_storage(
            storage,
            HnswBuildVectorEncoding::symmetric_i16(1).unwrap(),
            7,
            None,
        )
        .unwrap();
        let query = DistanceMetric::Euclidean.prepare(&[-100.0]);
        let scorer = GraphVectorScorer::new(&query, storage.as_ref()).unwrap();

        assert_eq!(scorer.score_point(0), -10_000.0);
        assert_eq!(scorer.score_point(1), -10_201.0);
    }

    #[test]
    fn exact_query_scorers_accept_partitioned_base_page_storage() {
        let first: Arc<dyn VectorStorage> =
            Arc::new(InMemoryVectorStorage::new(vec![0.0, 0.0, 1.0, 1.0], 2));
        let second: Arc<dyn VectorStorage> =
            Arc::new(InMemoryVectorStorage::new(vec![2.0, 2.0, 3.0, 3.0], 2));
        let storage = PartitionedVectorStorage::try_new(vec![first, second], 2).unwrap();
        assert!(storage.contiguous_vectors().is_none());
        let query = DistanceMetric::Euclidean.prepare(&[3.0, 3.0]);

        let scorer = VectorScorer::new(&query, &storage).unwrap();
        assert_eq!(scorer.score_point(3), 0.0);
        assert_eq!(scorer.score_point(2), -2.0);
        assert_eq!(scorer.prepared_scoring().scorer().score_point(1), -8.0);

        let mut graph_scorer = GraphVectorScorer::new(&query, &storage).unwrap();
        let scored = graph_scorer
            .score_points_unfiltered(&[0, 3, 2])
            .collect::<Vec<_>>();
        assert_eq!(scored[0].score, -18.0);
        assert_eq!(scored[1].score, 0.0);
        assert_eq!(scored[2].score, -2.0);
    }
}

/// Vector scorer responsible for calculating distances during search and build.
pub struct VectorScorer<'a> {
    query: &'a PreparedQuery,
    storage: &'a dyn VectorStorage,
    vectors: Option<&'a [f32]>,
    dimension: usize,
    point_count: usize,
    kernel: ScoringKernel<'a>,
    scores_buffer: Vec<ScoreType>,
    scored_points: Cell<u64>,
}

struct I16RoutingScoring<'a> {
    view: I16RoutingView<'a>,
    query: Box<[f32]>,
    query_inverse_norm: f32,
}

impl I16RoutingScoring<'_> {
    #[inline]
    fn score_point(&self, metric: DistanceMetric, point_id: PointOffset) -> ScoreType {
        let start = point_id as usize * self.view.row_stride_bytes;
        let code = &self.view.codes[start..start + self.view.row_stride_bytes];
        match metric {
            DistanceMetric::Euclidean => {
                -f32_query_i16_l2_squared(&self.query, code, self.view.scales)
            }
            DistanceMetric::DotProduct => {
                f32_query_i16_dot_product(&self.query, code, self.view.scales)
            }
            DistanceMetric::Cosine => {
                let inverse_norms = self.view.inverse_norms.unwrap_or_else(|| {
                    unreachable!("cosine routing image is validated at artifact open")
                });
                f32_query_i16_dot_product(&self.query, code, self.view.scales)
                    * (self.query_inverse_norm * inverse_norms.value(point_id))
            }
            DistanceMetric::Manhattan => {
                -f32_query_i16_l1_distance(&self.query, code, self.view.scales)
            }
        }
    }

    #[inline(always)]
    fn prefetch_point(&self, point_id: usize) {
        let start = point_id * self.view.row_stride_bytes;
        distance::prefetch_bytes_read(&self.view.codes[start..start + self.view.row_stride_bytes]);
    }
}

/// Graph-navigation scorer. It may use a lossy compact routing image, but it
/// deliberately exposes no cached-vector or exact-scan API. Exact SQL scores
/// are owned by [`VectorScorer`], so an already fetched f32 row can no longer
/// be silently ignored by a compact scorer.
pub(crate) struct GraphVectorScorer<'a> {
    exact: VectorScorer<'a>,
    routing: Option<I16RoutingScoring<'a>>,
    scores_buffer: Vec<ScoreType>,
    scored_points: Cell<u64>,
}

impl<'a> GraphVectorScorer<'a> {
    pub(crate) fn new(
        query: &'a PreparedQuery,
        vector_storage: &'a dyn VectorStorage,
    ) -> paro_common::error::Result<Self> {
        let exact = VectorScorer::new(query, vector_storage)?;
        let routing = vector_storage
            .i16_routing_view()
            .map(|view| {
                if view.source_dimension != query.as_slice().len()
                    || view
                        .selected_dimensions
                        .len()
                        .saturating_mul(std::mem::size_of::<i16>())
                        > view.row_stride_bytes
                    || view.scales.len() != view.selected_dimensions.len()
                {
                    return Err(paro_common::error::data_corrupted(
                        "HNSW routing image disagrees with the query dimension",
                    ));
                }
                let mut routing_query = Vec::with_capacity(view.selected_dimensions.len());
                let mut squared_norm = 0.0f32;
                for &source_dimension in view.selected_dimensions {
                    let value = query.as_slice()[source_dimension];
                    routing_query.push(value);
                    squared_norm += value * value;
                }
                let query_inverse_norm = if squared_norm < f32::EPSILON {
                    0.0
                } else {
                    squared_norm.sqrt().recip()
                };
                Ok(I16RoutingScoring {
                    view,
                    query: routing_query.into_boxed_slice(),
                    query_inverse_norm,
                })
            })
            .transpose()?;
        Ok(Self {
            exact,
            routing,
            scores_buffer: Vec::new(),
            scored_points: Cell::new(0),
        })
    }

    pub(crate) fn uses_compact_routing(&self) -> bool {
        self.routing.is_some()
    }

    pub(crate) fn point_count(&self) -> usize {
        self.exact.point_count()
    }

    pub(crate) fn scored_point_count(&self) -> u64 {
        self.scored_points.get()
    }

    pub(crate) fn score_point(&self, point_id: PointOffset) -> ScoreType {
        self.scored_points
            .set(self.scored_points.get().saturating_add(1));
        self.routing.as_ref().map_or_else(
            || {
                self.exact
                    .score_cached_point(point_id, self.exact.vector(point_id))
            },
            |routing| routing.score_point(self.exact.query.metric(), point_id),
        )
    }

    #[inline(always)]
    pub(crate) fn prefetch_graph_point(&self, point: GraphPoint) {
        if let Some(routing) = &self.routing {
            routing.prefetch_point(point.index());
        } else {
            self.exact.prefetch_graph_point(point);
        }
    }

    pub(crate) fn score_points_unfiltered<'b>(
        &'b mut self,
        points: &'b [PointOffset],
    ) -> impl Iterator<Item = ScoredPoint> + 'b {
        self.scores_buffer.resize(points.len(), 0.0);
        self.scored_points
            .set(self.scored_points.get().saturating_add(points.len() as u64));
        if let Some(routing) = &self.routing {
            for (score, &point_id) in self.scores_buffer.iter_mut().zip(points) {
                *score = routing.score_point(self.exact.query.metric(), point_id);
            }
        } else {
            if let Some(vectors) = self.exact.vectors {
                VectorScorer::fill_scores_untracked(
                    self.exact.query,
                    self.exact.kernel,
                    vectors,
                    self.exact.dimension,
                    points,
                    &mut self.scores_buffer,
                );
            } else {
                for (score, &point_id) in self.scores_buffer.iter_mut().zip(points) {
                    *score = VectorScorer::score_cached_untracked(
                        self.exact.query,
                        self.exact.kernel,
                        point_id,
                        self.exact.storage.get_vector(point_id),
                    );
                }
            }
        }
        points
            .iter()
            .zip(self.scores_buffer.iter())
            .map(|(&idx, &score)| ScoredPoint { idx, score })
    }

    pub(crate) fn score_points<'b>(
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
        let vectors = vector_storage.contiguous_vectors();
        let expected_values = vector_storage
            .num_vectors()
            .checked_mul(dimension)
            .ok_or_else(|| {
                paro_common::error::data_corrupted("HNSW vector artifact cardinality overflow")
            })?;
        if vectors.is_some_and(|vectors| vectors.len() != expected_values) {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW vector artifact length mismatch: expected {expected_values} values, got {}",
                vectors.map_or(0, <[f32]>::len)
            )));
        }
        Ok(Self {
            query,
            storage: vector_storage,
            vectors,
            dimension,
            point_count: vector_storage.num_vectors(),
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
        self.vectors.map_or_else(
            || self.storage.get_vector(point_id),
            |vectors| {
                let start = point_id as usize * self.dimension;
                &vectors[start..start + self.dimension]
            },
        )
    }

    #[inline]
    pub(crate) fn vector_storage(&self) -> &'a dyn VectorStorage {
        self.storage
    }

    pub(crate) fn point_count(&self) -> usize {
        self.point_count
    }

    /// Schedule the leading vector cache lines while graph-link and visited
    /// processing still has useful independent work to perform.
    ///
    /// `GraphPoint` is yielded only by a `GraphLinksReadView` bound to this
    /// scorer's cardinality. Reusing that capability avoids manufacturing a
    /// checked vector slice solely to issue non-dereferencing prefetch hints.
    #[inline(always)]
    pub(crate) fn prefetch_graph_point(&self, point: GraphPoint) {
        distance::prefetch_vector_read(self.vector(point.offset()));
    }

    pub fn scored_point_count(&self) -> u64 {
        self.scored_points.get()
    }

    pub(crate) fn prepared_scoring(&self) -> PreparedVectorScoring<'a> {
        PreparedVectorScoring {
            query: self.query,
            storage: self.storage,
            vectors: self.vectors,
            dimension: self.dimension,
            point_count: self.point_count,
            kernel: self.kernel,
        }
    }

    /// Merge work performed by isolated exact-scan scorers into the query's
    /// authoritative telemetry counter.
    pub(crate) fn add_scored_point_count(&self, count: u64) {
        self.scored_points
            .set(self.scored_points.get().saturating_add(count));
    }

    /// Score an indexed point whose vector has already been fetched.
    pub fn score_cached_point(&self, point_id: PointOffset, vector: &[f32]) -> ScoreType {
        Self::score_cached_untracked(self.query, self.kernel, point_id, vector)
    }

    fn score_cached_untracked(
        query: &PreparedQuery,
        kernel: ScoringKernel<'_>,
        point_id: PointOffset,
        vector: &[f32],
    ) -> ScoreType {
        match kernel {
            ScoringKernel::Cosine(norms) => query.metric().similarity_prepared_with_norm(
                query.as_slice(),
                vector,
                norms.value(point_id),
            ),
            ScoringKernel::Euclidean => -distance::l2_squared(query.as_slice(), vector),
            ScoringKernel::DotProduct => distance::dot_product(query.as_slice(), vector),
            ScoringKernel::Manhattan => -distance::l1_distance(query.as_slice(), vector),
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
        if let Some(vectors) = self.vectors {
            Self::fill_scores_untracked(
                self.query,
                self.kernel,
                vectors,
                self.dimension,
                points,
                &mut self.scores_buffer,
            );
        } else {
            for (score, &point_id) in self.scores_buffer.iter_mut().zip(points) {
                *score = Self::score_cached_untracked(
                    self.query,
                    self.kernel,
                    point_id,
                    self.storage.get_vector(point_id),
                );
            }
        }
        let scores = &self.scores_buffer;
        points
            .iter()
            .zip(scores.iter())
            .map(|(&idx, &score)| ScoredPoint { idx, score })
    }

    fn fill_scores_untracked(
        prepared_query: &PreparedQuery,
        kernel: ScoringKernel<'_>,
        vectors: &[f32],
        dimension: usize,
        points: &[PointOffset],
        scores: &mut [ScoreType],
    ) {
        let query = prepared_query.as_slice();
        let vector = |point_id: PointOffset| {
            let start = point_id as usize * dimension;
            &vectors[start..start + dimension]
        };
        match kernel {
            ScoringKernel::Cosine(norms) => {
                for (i, &point_id) in points.iter().enumerate() {
                    scores[i] =
                        distance::dot_product(query, vector(point_id)) * norms.value(point_id);
                }
            }
            ScoringKernel::Euclidean => {
                distance::l2_squared_batch_indexed(query, vectors, dimension, points, scores);
                for score in scores {
                    *score = -*score;
                }
            }
            ScoringKernel::DotProduct => {
                for (i, &point_id) in points.iter().enumerate() {
                    scores[i] = distance::dot_product(query, vector(point_id));
                }
            }
            ScoringKernel::Manhattan => {
                for (i, &point_id) in points.iter().enumerate() {
                    scores[i] = -distance::l1_distance(query, vector(point_id));
                }
            }
        }
    }

    /// Score global point identities from a contiguous alternate row-major
    /// covering layout. `global_points` preserve table/HNSW identity for
    /// metric metadata and returned Top-K; row `i` is stored at vector row `i`.
    pub(crate) fn score_covering_contiguous<'b>(
        &'b mut self,
        global_points: &'b [PointOffset],
        vectors: &'b [f32],
    ) -> impl Iterator<Item = ScoredPoint> + 'b {
        assert_eq!(
            vectors.len(),
            global_points.len().saturating_mul(self.dimension)
        );
        self.scores_buffer.resize(global_points.len(), 0.0);
        self.scored_points.set(
            self.scored_points
                .get()
                .saturating_add(global_points.len() as u64),
        );
        let query = self.query.as_slice();
        match self.kernel {
            ScoringKernel::Cosine(norms) => {
                for ((score, &global_point), vector) in self
                    .scores_buffer
                    .iter_mut()
                    .zip(global_points.iter())
                    .zip(vectors.chunks_exact(self.dimension))
                {
                    *score = distance::dot_product(query, vector) * norms.value(global_point);
                }
            }
            ScoringKernel::Euclidean => {
                distance::l2_squared_batch_contiguous(
                    query,
                    vectors,
                    self.dimension,
                    &mut self.scores_buffer,
                );
                for score in &mut self.scores_buffer {
                    *score = -*score;
                }
            }
            ScoringKernel::DotProduct => {
                for (score, vector) in self
                    .scores_buffer
                    .iter_mut()
                    .zip(vectors.chunks_exact(self.dimension))
                {
                    *score = distance::dot_product(query, vector);
                }
            }
            ScoringKernel::Manhattan => {
                for (score, vector) in self
                    .scores_buffer
                    .iter_mut()
                    .zip(vectors.chunks_exact(self.dimension))
                {
                    *score = -distance::l1_distance(query, vector);
                }
            }
        }
        let scores = &self.scores_buffer;
        global_points
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
