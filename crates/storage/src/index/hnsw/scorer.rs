//! # HNSW Vector Scorer
//!
//! Distance scoring helpers shared by graph search and plain/full-scan paths.

use super::types::{PointOffset, ScoreType, ScoredPoint};
use super::{DistanceMetric, VectorStorage};

/// Vector scorer responsible for calculating distances during search and build.
pub struct VectorScorer<'a> {
    pub query: &'a [f32],
    pub vector_storage: &'a dyn VectorStorage,
    pub distance: DistanceMetric,
    scores_buffer: Vec<ScoreType>,
}

impl<'a> VectorScorer<'a> {
    pub fn new(
        query: &'a [f32],
        vector_storage: &'a dyn VectorStorage,
        distance: DistanceMetric,
    ) -> Self {
        Self {
            query,
            vector_storage,
            distance,
            scores_buffer: Vec::new(),
        }
    }

    /// Score a single point.
    pub fn score_point(&self, point_id: PointOffset) -> ScoreType {
        self.score_cached_vector(self.vector_storage.get_vector(point_id))
    }

    /// Score a vector that has already been fetched from storage.
    pub fn score_cached_vector(&self, vector: &[f32]) -> ScoreType {
        self.distance.similarity(self.query, vector)
    }

    /// Score a batch of points into a caller-provided buffer.
    pub fn score_into(&self, points: &[PointOffset], scores: &mut [ScoreType]) {
        assert!(
            scores.len() >= points.len(),
            "scores buffer is shorter than point batch",
        );
        for (i, &point_id) in points.iter().enumerate() {
            scores[i] = self.score_cached_vector(self.vector_storage.get_vector(point_id));
        }
    }

    /// Score points without any filtering.
    pub fn score_points_unfiltered<'b>(
        &'b mut self,
        points: &'b [PointOffset],
    ) -> impl Iterator<Item = ScoredPoint> + 'b {
        self.scores_buffer.resize(points.len(), 0.0);
        for (i, &point_id) in points.iter().enumerate() {
            self.scores_buffer[i] =
                self.score_cached_vector(self.vector_storage.get_vector(point_id));
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
