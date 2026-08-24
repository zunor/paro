// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Graph Layers
//!
//! Read-only HNSW graph structure for efficient searching. Supporting both
//! standard HNSW and exact-bitmap filtered Top-K search.

use super::entry_points::{EntryPoint, EntryPoints};
use super::graph_links::GraphLinks;
use super::search_context::{FixedLengthPriorityQueue, SearchContext};
use super::types::{
    required_filtered_admissions, HnswM, PointOffset, ScoredPoint, SearchAlgorithm,
};
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

/// Result of one graph traversal together with the adaptive work it performed.
/// Keeping this trace beside the result lets callers distinguish a cheap
/// masked hit from predicate-aware refinement without inferring it from
/// cardinality or latency.
pub(crate) struct GraphSearchResult {
    pub(crate) points: Vec<ScoredPoint>,
    pub(crate) predicate_refined: bool,
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
    pub(crate) fn search_one(
        &self,
        top: usize,
        ef: usize,
        algorithm: SearchAlgorithm,
        scorer: &mut VectorScorer<'_>,
        filter_bitmap: Option<&RoaringBitmap>,
        random_entry_point: bool,
    ) -> GraphSearchResult {
        if top == 0 {
            return GraphSearchResult {
                points: Vec::new(),
                predicate_refined: false,
            };
        }

        let Some(entry_point) = self.select_entry_point(random_entry_point) else {
            return GraphSearchResult {
                points: Vec::new(),
                predicate_refined: false,
            };
        };

        self.search_from_entry_point(entry_point, top, ef, algorithm, scorer, filter_bitmap)
    }

    /// Batched search entry point for multiple queries sharing one filter bitmap.
    pub(crate) fn search_many(
        &self,
        top: usize,
        ef: usize,
        algorithm: SearchAlgorithm,
        scorers: &mut [VectorScorer<'_>],
        filter_bitmap: Option<&RoaringBitmap>,
        random_entry_point: bool,
    ) -> Vec<GraphSearchResult> {
        if scorers.is_empty() {
            return Vec::new();
        }
        if top == 0 {
            return (0..scorers.len())
                .map(|_| GraphSearchResult {
                    points: Vec::new(),
                    predicate_refined: false,
                })
                .collect();
        }

        let mut rng = thread_rng();
        let entry_points =
            self.select_entry_points_for_many(scorers.len(), random_entry_point, &mut rng);

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
                None => results.push(GraphSearchResult {
                    points: Vec::new(),
                    predicate_refined: false,
                }),
            }
        }

        results
    }

    fn select_entry_point(&self, random_entry_point: bool) -> Option<EntryPoint> {
        if random_entry_point {
            let mut rng = thread_rng();
            self.entry_points.get_random_entry_point(&mut rng, |_| true)
        } else {
            self.entry_points.get_entry_point(|_| true)
        }
    }

    fn select_entry_points_for_many<R: Rng + ?Sized>(
        &self,
        num_queries: usize,
        random_entry_point: bool,
        rng: &mut R,
    ) -> Vec<Option<EntryPoint>> {
        if random_entry_point {
            (0..num_queries)
                .map(|_| self.entry_points.get_random_entry_point(rng, |_| true))
                .collect()
        } else {
            let entry_point = self.entry_points.get_entry_point(|_| true);
            vec![entry_point; num_queries]
        }
    }

    fn search_from_entry_point(
        &self,
        entry_point: EntryPoint,
        top: usize,
        ef: usize,
        algorithm: SearchAlgorithm,
        scorer: &mut VectorScorer<'_>,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> GraphSearchResult {
        let zero_level_entry = self.descend_to_zero_level(entry_point, scorer);
        let (mut points, predicate_refined) = match algorithm {
            SearchAlgorithm::Hnsw => (self.search_on_level(zero_level_entry, ef, scorer), false),
            SearchAlgorithm::MaskedTopK | SearchAlgorithm::AdaptiveFilteredTopK => self
                .search_masked_topk(
                    zero_level_entry,
                    top,
                    ef,
                    scorer,
                    filter_bitmap,
                    algorithm == SearchAlgorithm::AdaptiveFilteredTopK,
                ),
        };
        points.truncate(top);
        GraphSearchResult {
            points,
            predicate_refined,
        }
    }

    /// Greedy descent from the selected entry point to level 0 for one query.
    fn descend_to_zero_level(
        &self,
        entry_point: EntryPoint,
        scorer: &mut VectorScorer<'_>,
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
                    scorer.score_points(&mut links, None, self.hnsw_m.get_m(current_level))
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

    /// Standard connected HNSW search on one level. Admission filters are
    /// deliberately absent from navigation.
    fn search_on_level(
        &self,
        entry_point: ScoredPoint,
        ef: usize,
        scorer: &mut VectorScorer,
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

            for sp in scorer.score_points_unfiltered(&neighbors) {
                context.process_candidate(sp);
            }
        }

        context.nearest.into_sorted_vec()
    }

    /// Navigate the ordinary graph exactly as an unfiltered query while
    /// retaining every scored candidate admitted by the exact filter bitmap.
    /// This avoids disconnecting the graph and avoids estimating an
    /// inverse-selectivity oversampling factor.
    fn search_masked_topk(
        &self,
        entry_point: ScoredPoint,
        top: usize,
        ef: usize,
        scorer: &mut VectorScorer,
        filter_bitmap: Option<&RoaringBitmap>,
        adaptive_predicate_refinement: bool,
    ) -> (Vec<ScoredPoint>, bool) {
        let mut visited = self.visited_pool.get(self.links.num_points());
        let mut context = SearchContext::new(entry_point, ef);
        // Preserve up to `ef` already-scored matching candidates as diverse
        // refinement seeds; only the final public result is truncated to K.
        let mut filtered = FixedLengthPriorityQueue::new(ef.max(top));
        let mut neighbors = Vec::new();
        visited.check_and_update_visited(entry_point.idx);
        if filter_bitmap.is_none_or(|bitmap| bitmap.contains(entry_point.idx)) {
            filtered.push(entry_point);
        }

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

            for point in scorer.score_points_unfiltered(&neighbors) {
                if filter_bitmap.is_none_or(|bitmap| bitmap.contains(point.idx)) {
                    filtered.push(point);
                }
                context.process_candidate(point);
            }
        }

        let seeds = filtered.into_sorted_vec();
        let required = usize::try_from(required_filtered_admissions(top)).unwrap_or(usize::MAX);
        if !adaptive_predicate_refinement || seeds.len() >= required {
            return (seeds, false);
        }

        let Some(filter_bitmap) = filter_bitmap else {
            return (seeds, false);
        };
        let Some((&first, remaining)) = seeds.split_first() else {
            return (Vec::new(), true);
        };

        // Phase one already scored every point in `visited`. A matching point
        // omitted from the `ef` seeds cannot improve their Top-K, so retaining
        // those marks avoids duplicate distance work without reducing the
        // reachable result frontier. Non-matching bridge nodes use a separate
        // sparse set: they must remain eligible for two-hop expansion even if
        // ordinary HNSW navigation scored them in phase one.
        let mut context = SearchContext::new(first, ef);
        for &seed in remaining {
            context.process_candidate(seed);
        }
        let mut bridge_visited = RoaringBitmap::new();
        let mut matching_neighbors = Vec::new();
        let mut bridge_neighbors = Vec::new();

        while let Some(candidate) = context.candidates.pop() {
            if candidate.score < context.lower_bound() && context.nearest.len() >= ef {
                break;
            }
            matching_neighbors.clear();
            bridge_neighbors.clear();
            self.links.for_each_link(candidate.idx, 0, |neighbor| {
                if filter_bitmap.contains(neighbor) {
                    if !visited.check_and_update_visited(neighbor) {
                        matching_neighbors.push(neighbor);
                    }
                } else if bridge_visited.insert(neighbor) {
                    bridge_neighbors.push(neighbor);
                }
            });

            // ACORN-1-style bridge expansion: non-matching direct neighbors
            // are never scored or admitted, but their matching neighbors can
            // reconnect a predicate-induced graph whose expected direct degree
            // is below one. Each bridge contributes at most M0 new candidates.
            let hop_limit = self.hnsw_m.get_m(0);
            for bridge in bridge_neighbors.iter().copied() {
                let limit = matching_neighbors.len().saturating_add(hop_limit);
                self.links.for_each_link(bridge, 0, |neighbor| {
                    if matching_neighbors.len() < limit
                        && filter_bitmap.contains(neighbor)
                        && !visited.check_and_update_visited(neighbor)
                    {
                        matching_neighbors.push(neighbor);
                    }
                });
            }

            for point in scorer.score_points_unfiltered(&matching_neighbors) {
                context.process_candidate(point);
            }
        }

        (context.nearest.into_sorted_vec(), true)
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
        let selections = graph.select_entry_points_for_many(5, false, &mut rng);
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
        let actual = graph.select_entry_points_for_many(8, true, &mut actual_rng);

        assert_eq!(actual, expected);
        assert!(actual.windows(2).any(|pair| pair[0] != pair[1]));
    }
}
