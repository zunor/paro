// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Graph Layers
//!
//! Read-only HNSW graph structure for efficient searching. Supporting both
//! standard HNSW and ACORN-1 filtered search algorithms.

use super::entry_points::{EntryPoint, EntryPoints};
use super::graph_links::GraphLinks;
use super::search_context::SearchContext;
use super::types::{HnswM, PointOffset, ScoredPoint, SearchAlgorithm};
use super::visited_pool::VisitedPool;
use super::VectorScorer;
use rand::{thread_rng, Rng};
use roaring::RoaringBitmap;

/// Read-only HNSW graph layers.
pub struct GraphLayers {
    pub links: GraphLinks,
    pub entry_points: EntryPoints,
    pub visited_pool: VisitedPool,
    pub hnsw_m: HnswM,
}

impl GraphLayers {
    pub fn new(
        links: GraphLinks,
        entry_points: EntryPoints,
        visited_pool: VisitedPool,
        hnsw_m: HnswM,
    ) -> Self {
        Self {
            links,
            entry_points,
            visited_pool,
            hnsw_m,
        }
    }

    /// Primary search entry point for a single query.
    pub fn search_one(
        &self,
        top: usize,
        ef: usize,
        algorithm: SearchAlgorithm,
        scorer: &mut VectorScorer<'_>,
        filter_bitmap: Option<&RoaringBitmap>,
        random_entry_point: bool,
    ) -> Vec<ScoredPoint> {
        if top == 0 {
            return Vec::new();
        }

        let Some(entry_point) = self.select_entry_point(filter_bitmap, random_entry_point) else {
            return Vec::new();
        };

        self.search_from_entry_point(entry_point, top, ef, algorithm, scorer, filter_bitmap)
    }

    /// Batched search entry point for multiple queries sharing one filter bitmap.
    pub fn search_many(
        &self,
        top: usize,
        ef: usize,
        algorithm: SearchAlgorithm,
        scorers: &mut [VectorScorer<'_>],
        filter_bitmap: Option<&RoaringBitmap>,
        random_entry_point: bool,
    ) -> Vec<Vec<ScoredPoint>> {
        if scorers.is_empty() {
            return Vec::new();
        }
        if top == 0 {
            return vec![Vec::new(); scorers.len()];
        }

        let mut rng = thread_rng();
        let entry_points = self.select_entry_points_for_many(
            scorers.len(),
            filter_bitmap,
            random_entry_point,
            &mut rng,
        );

        let mut results = Vec::with_capacity(scorers.len());
        for (entry_point, scorer) in entry_points.into_iter().zip(scorers.iter_mut()) {
            match entry_point {
                Some(entry_point) => results.push(self.search_from_entry_point(
                    entry_point,
                    top,
                    ef,
                    algorithm,
                    scorer,
                    filter_bitmap,
                )),
                None => results.push(Vec::new()),
            }
        }

        results
    }

    fn select_entry_point(
        &self,
        filter_bitmap: Option<&RoaringBitmap>,
        random_entry_point: bool,
    ) -> Option<EntryPoint> {
        if random_entry_point {
            let mut rng = thread_rng();
            self.entry_points
                .get_random_entry_point(&mut rng, |idx| Self::matches_filter(filter_bitmap, idx))
        } else {
            self.entry_points
                .get_entry_point(|idx| Self::matches_filter(filter_bitmap, idx))
        }
    }

    fn select_entry_points_for_many<R: Rng + ?Sized>(
        &self,
        num_queries: usize,
        filter_bitmap: Option<&RoaringBitmap>,
        random_entry_point: bool,
        rng: &mut R,
    ) -> Vec<Option<EntryPoint>> {
        if random_entry_point {
            (0..num_queries)
                .map(|_| {
                    self.entry_points
                        .get_random_entry_point(rng, |idx| Self::matches_filter(filter_bitmap, idx))
                })
                .collect()
        } else {
            let entry_point = self
                .entry_points
                .get_entry_point(|idx| Self::matches_filter(filter_bitmap, idx));
            vec![entry_point; num_queries]
        }
    }

    fn matches_filter(filter_bitmap: Option<&RoaringBitmap>, idx: PointOffset) -> bool {
        filter_bitmap.is_none_or(|bm| bm.contains(idx))
    }

    fn search_from_entry_point(
        &self,
        entry_point: EntryPoint,
        top: usize,
        ef: usize,
        algorithm: SearchAlgorithm,
        scorer: &mut VectorScorer<'_>,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> Vec<ScoredPoint> {
        let zero_level_entry = self.descend_to_zero_level(entry_point, scorer, filter_bitmap);
        let mut results = match algorithm {
            SearchAlgorithm::Hnsw => {
                self.search_on_level(zero_level_entry, ef, scorer, filter_bitmap)
            }
            SearchAlgorithm::Acorn => {
                self.search_on_level_acorn(zero_level_entry, ef, scorer, filter_bitmap)
            }
        };
        results.truncate(top);
        results
    }

    /// Greedy descent from the selected entry point to level 0 for one query.
    fn descend_to_zero_level(
        &self,
        entry_point: EntryPoint,
        scorer: &mut VectorScorer<'_>,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> ScoredPoint {
        let mut current_point = entry_point.point_id;
        let mut current_score = scorer.score_point(current_point);
        let mut current_level = entry_point.level;
        let mut links: Vec<PointOffset> = Vec::new();

        while current_level > 0 {
            let mut changed = true;
            while changed {
                changed = false;
                links.clear();
                self.links
                    .for_each_link(current_point, current_level, |neighbor| {
                        links.push(neighbor);
                    });

                for scored in
                    scorer.score_points(&mut links, filter_bitmap, self.hnsw_m.get_m(current_level))
                {
                    if scored.score > current_score {
                        current_score = scored.score;
                        current_point = scored.idx;
                        changed = true;
                    }
                }
            }
            current_level -= 1;
        }

        ScoredPoint {
            idx: current_point,
            score: current_score,
        }
    }

    /// Standard HNSW search on a level with optional filtering.
    fn search_on_level(
        &self,
        entry_point: ScoredPoint,
        ef: usize,
        scorer: &mut VectorScorer,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> Vec<ScoredPoint> {
        let mut visited = self.visited_pool.get(self.links.num_points());
        let mut context = SearchContext::new(entry_point, ef);
        let mut neighbors: Vec<PointOffset> = Vec::new();

        visited.check_and_update_visited(entry_point.idx);

        while let Some(candidate) = context.candidates.pop() {
            if candidate.score < context.lower_bound() && context.nearest.len() >= ef {
                break;
            }

            neighbors.clear();
            self.links.for_each_link(candidate.idx, 0, |neighbor| {
                if !visited.check_and_update_visited(neighbor) {
                    neighbors.push(neighbor);
                }
            });

            for sp in scorer.score_points(&mut neighbors, filter_bitmap, 0) {
                context.process_candidate(sp);
            }
        }

        context.nearest.into_sorted_vec()
    }

    /// ACORN-1 search on level 0.
    ///
    /// Improved recall for filtered search by exploring through non-matching points
    /// as "stepping stones".
    fn search_on_level_acorn(
        &self,
        entry_point: ScoredPoint,
        ef: usize,
        scorer: &mut VectorScorer,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> Vec<ScoredPoint> {
        let num_points = self.links.num_points();
        // Use two visited sets:
        // 1-hop for direct neighbors,
        // 2-hop for neighbors of neighbors (stepping stones)
        let mut hop1_visited = self.visited_pool.get(num_points);
        let mut hop2_visited = self.visited_pool.get(num_points);

        let mut context = SearchContext::new(entry_point, ef);
        hop1_visited.check_and_update_visited(entry_point.idx);

        let hop1_limit = self.hnsw_m.get_m(0);
        let hop2_limit = self.hnsw_m.get_m(0);
        let mut to_score: Vec<PointOffset> = Vec::with_capacity(hop1_limit * hop2_limit.min(16));
        let mut to_explore: Vec<PointOffset> = Vec::with_capacity(hop1_limit * hop2_limit.min(16));

        while let Some(candidate) = context.candidates.pop() {
            if candidate.score < context.lower_bound() && context.nearest.len() >= ef {
                break;
            }

            to_score.clear();
            to_explore.clear();

            // ===== 1-hop neighbors =====
            self.links.for_each_link(candidate.idx, 0, |hop1| {
                if hop1_visited.check_and_update_visited(hop1) {
                    return;
                }

                let is_match = filter_bitmap.is_none_or(|bm| bm.contains(hop1));
                if is_match {
                    to_score.push(hop1);
                } else {
                    to_explore.push(hop1);
                }
            });

            to_score.truncate(hop1_limit);

            // ===== 2-hop neighbors (stepping stones) =====
            for &hop1 in to_explore.iter() {
                let total_limit = to_score.len() + hop2_limit;
                self.links.for_each_link(hop1, 0, |hop2| {
                    if hop1_visited.check(hop2) || hop2_visited.check_and_update_visited(hop2) {
                        return;
                    }

                    if filter_bitmap.is_none_or(|bm| bm.contains(hop2)) {
                        hop1_visited.check_and_update_visited(hop2);
                        to_score.push(hop2);
                    }
                });

                if to_score.len() >= total_limit {
                    break;
                }
            }

            // ===== batch scoring =====
            for sp in scorer.score_points_unfiltered(to_score.as_slice()) {
                context.process_candidate(sp);
            }
        }

        context.nearest.into_sorted_vec()
    }

    pub fn num_points(&self) -> usize {
        self.links.num_points()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};

    fn make_graph(entry_points: EntryPoints) -> GraphLayers {
        GraphLayers::new(
            GraphLinks::new_from_edges(vec![
                vec![vec![]],
                vec![vec![]],
                vec![vec![]],
                vec![vec![]],
            ]),
            entry_points,
            VisitedPool::new(),
            HnswM::new(8),
        )
    }

    #[test]
    fn search_many_reuses_deterministic_entry_point_when_random_is_disabled() {
        let graph = make_graph(EntryPoints {
            entry_points: vec![EntryPoint {
                point_id: 1,
                level: 4,
            }],
            extra_entry_points: vec![
                EntryPoint {
                    point_id: 2,
                    level: 3,
                },
                EntryPoint {
                    point_id: 3,
                    level: 2,
                },
            ],
        });

        let mut rng = StdRng::seed_from_u64(7);
        let selections = graph.select_entry_points_for_many(5, None, false, &mut rng);
        assert_eq!(
            selections,
            vec![
                Some(EntryPoint {
                    point_id: 1,
                    level: 4
                });
                5
            ]
        );
    }

    #[test]
    fn search_many_random_mode_selects_entry_points_per_query() {
        let graph = make_graph(EntryPoints {
            entry_points: vec![
                EntryPoint {
                    point_id: 1,
                    level: 4,
                },
                EntryPoint {
                    point_id: 2,
                    level: 3,
                },
            ],
            extra_entry_points: vec![
                EntryPoint {
                    point_id: 3,
                    level: 2,
                },
                EntryPoint {
                    point_id: 0,
                    level: 1,
                },
            ],
        });

        let mut expected_rng = StdRng::seed_from_u64(11);
        let expected = (0..8)
            .map(|_| {
                graph
                    .entry_points
                    .get_random_entry_point(&mut expected_rng, |_| true)
            })
            .collect::<Vec<_>>();

        let mut actual_rng = StdRng::seed_from_u64(11);
        let actual = graph.select_entry_points_for_many(8, None, true, &mut actual_rng);

        assert_eq!(actual, expected);
        assert!(actual.windows(2).any(|pair| pair[0] != pair[1]));
    }
}
