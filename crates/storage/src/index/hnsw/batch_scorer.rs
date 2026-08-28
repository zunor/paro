// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Batch Scorer
//!
//! Chunked multi-query scorer used by the plain/full-scan path.

use super::search_context::FixedLengthPriorityQueue;
use super::types::{PointOffset, ScoredPoint};
use super::VectorScorer;
use crate::search::SearchWorkBudget;
use paro_common::error::Result;
use smallvec::SmallVec;

/// Large enough to amortize row-set dispatch and work-budget checks while the
/// gathered ids, scores, and one 32-dimensional NEON vector group remain an
/// L1-sized working set. Vector payloads are still consumed in eight-row SIMD
/// groups; this only widens the outer scheduling batch.
pub const BATCH_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq)]
pub struct BatchScoredResult {
    pub points: Vec<ScoredPoint>,
    pub scored_points: u64,
}

/// Batch scorer for full-scan style search.
pub struct BatchScorer<'a> {
    // Keep inline capacity at 1 so batch=1 stays stack-only at the SmallVec layer.
    scorers: SmallVec<[ScorerWithHeap<'a>; 1]>,
}

struct ScorerWithHeap<'a> {
    scorer: VectorScorer<'a>,
    top_k: FixedLengthPriorityQueue<ScoredPoint>,
}

impl<'a> BatchScorer<'a> {
    pub fn new(scorers: Vec<VectorScorer<'a>>, top_k: usize) -> Self {
        let scorers = scorers
            .into_iter()
            .map(|scorer| ScorerWithHeap {
                scorer,
                top_k: FixedLengthPriorityQueue::new(top_k),
            })
            .collect();
        Self { scorers }
    }

    #[cfg(test)]
    pub fn scan<I>(self, point_ids: I) -> Vec<Vec<ScoredPoint>>
    where
        I: IntoIterator<Item = PointOffset>,
    {
        let budget = crate::search::ResourceBudget::default();
        self.scan_with_work(point_ids, budget.work.as_ref())
            .expect("unlimited test work budget")
            .into_iter()
            .map(|result| result.points)
            .collect()
    }

    pub fn scan_with_work<I>(
        mut self,
        point_ids: I,
        work: &SearchWorkBudget,
    ) -> Result<Vec<BatchScoredResult>>
    where
        I: IntoIterator<Item = PointOffset>,
    {
        if self.scorers.is_empty() {
            return Ok(Vec::new());
        }
        if self.scorers[0].top_k.capacity == 0 {
            return Ok((0..self.scorers.len())
                .map(|_| BatchScoredResult {
                    points: Vec::new(),
                    scored_points: 0,
                })
                .collect());
        }

        let mut point_ids = point_ids.into_iter();
        let mut chunk = [0; BATCH_SIZE];
        let mut scored_points = 0u64;
        let vector_storage = self.scorers[0].scorer.vector_storage();

        loop {
            let mut chunk_len = 0usize;
            while chunk_len < BATCH_SIZE {
                let Some(point_id) = point_ids.next() else {
                    break;
                };
                chunk[chunk_len] = point_id;
                chunk_len += 1;
            }

            if chunk_len == 0 {
                break;
            }

            work.check_and_consume(chunk_len.saturating_mul(self.scorers.len()))?;
            let points = &chunk[..chunk_len];
            scored_points = scored_points.saturating_add(chunk_len as u64);
            for &idx in points {
                let vector = vector_storage.get_vector(idx);
                for scorer in &mut self.scorers {
                    let score = scorer.scorer.score_cached_point(idx, vector);
                    scorer.top_k.push(ScoredPoint { idx, score });
                }
            }
        }

        Ok(self
            .scorers
            .into_iter()
            .map(|scorer| BatchScoredResult {
                points: scorer.top_k.into_sorted_vec(),
                scored_points,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::{DistanceMetric, InMemoryVectorStorage, VectorStorage};
    use std::sync::Arc;

    fn brute_force_results(
        query: &[f32],
        storage: &dyn VectorStorage,
        distance: DistanceMetric,
        top_k: usize,
        point_ids: impl IntoIterator<Item = PointOffset>,
    ) -> Vec<ScoredPoint> {
        let mut best = FixedLengthPriorityQueue::new(top_k);
        for idx in point_ids {
            let score = distance.similarity(query, storage.get_vector(idx));
            best.push(ScoredPoint { idx, score });
        }
        best.into_sorted_vec()
    }

    #[test]
    fn batch_scorer_batch_size_one_matches_direct_full_scan() {
        let storage = Arc::new(InMemoryVectorStorage::new(
            vec![0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0, 5.0, 5.0],
            2,
        ));
        let query = vec![4.0, 4.0];
        let top_k = 3;

        let prepared = DistanceMetric::DotProduct.prepare(&query);
        let scorer = VectorScorer::new(&prepared, storage.as_ref()).unwrap();
        let actual = BatchScorer::new(vec![scorer], top_k).scan(0..storage.num_vectors() as u32);
        let expected = brute_force_results(
            &query,
            storage.as_ref(),
            DistanceMetric::DotProduct,
            top_k,
            0..storage.num_vectors() as u32,
        );

        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0], expected);
    }

    #[test]
    fn batch_scorer_matches_repeated_full_scan_for_multiple_queries() {
        let storage = Arc::new(InMemoryVectorStorage::new(
            vec![
                0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0, 5.0, 5.0, 6.0, 6.0, 7.0, 7.0,
            ],
            2,
        ));
        let queries = [vec![1.0, 1.0], vec![3.5, 3.5], vec![7.0, 7.0]];
        let top_k = 4;
        let prepared_queries = queries
            .iter()
            .map(|query| DistanceMetric::Euclidean.prepare(query))
            .collect::<Vec<_>>();
        let scorers = prepared_queries
            .iter()
            .map(|query| VectorScorer::new(query, storage.as_ref()).unwrap())
            .collect::<Vec<_>>();

        let actual =
            BatchScorer::new(scorers, top_k).scan((1..storage.num_vectors() as u32).step_by(2));
        let expected = queries
            .iter()
            .map(|query| {
                brute_force_results(
                    query,
                    storage.as_ref(),
                    DistanceMetric::Euclidean,
                    top_k,
                    (1..storage.num_vectors() as u32).step_by(2),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn exact_batch_scan_obeys_the_shared_query_work_budget() {
        let storage = Arc::new(InMemoryVectorStorage::new(vec![0.0; 32], 2));
        let prepared = DistanceMetric::Euclidean.prepare(&[0.0, 0.0]);
        let scorer = VectorScorer::new(&prepared, storage.as_ref()).unwrap();
        let budget = crate::search::ResourceBudget::default().with_work_controls(
            Some(8),
            Arc::new(crate::search::budget::NoopSearchCancellation),
        );

        let error = BatchScorer::new(vec![scorer], 1)
            .scan_with_work(0..storage.num_vectors() as u32, budget.work.as_ref())
            .unwrap_err();
        assert!(error.to_string().contains("CPU step budget"));
    }
}
