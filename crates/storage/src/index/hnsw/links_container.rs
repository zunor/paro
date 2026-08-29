// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Links Container
//!
//! Neighbor container used during graph building.

use std::cell::Cell;

use crate::index::hnsw::types::{PointOffset, ScoreType, ScoredPoint};

/// Container for links of a single point on a single level.
#[derive(Debug, Clone, Default)]
pub struct LinksContainer {
    links: Vec<PointOffset>,
    /// Number of links that have been processed by the heuristic.
    processed_by_heuristic: u32,
}

impl LinksContainer {
    pub fn with_capacity(m: usize) -> Self {
        Self {
            links: Vec::with_capacity(m),
            processed_by_heuristic: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.links.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = PointOffset> + '_ {
        self.links.iter().copied()
    }

    pub fn push(&mut self, link: PointOffset) {
        self.processed_by_heuristic = 0;
        self.links.push(link);
    }

    pub fn as_slice(&self) -> &[PointOffset] {
        &self.links
    }

    /// Replace links with provided points.
    pub fn fill_from(&mut self, points: impl Iterator<Item = PointOffset>) {
        self.links.clear();
        self.links.extend(points);
        self.processed_by_heuristic = 0;
    }

    pub fn into_vec(self) -> Vec<PointOffset> {
        self.links
    }

    /// Fill links with a diversity-selected prefix followed by the nearest
    /// remaining candidates up to `level_m`.
    ///
    /// Candidates must be sorted by score in descending order (higher is better).
    /// For each candidate, if it is closer to any already-selected point than
    /// to the target, it is excluded from the diversity prefix. Unused durable
    /// degree capacity is then filled in original distance order. The suffix
    /// is deliberately not marked as heuristic-processed: a later reciprocal
    /// insertion must reconsider those edges rather than treating them as a
    /// proven mutually diverse set.
    pub fn fill_from_sorted_with_heuristic(
        &mut self,
        candidates: impl Iterator<Item = ScoredPoint> + Clone,
        level_m: usize,
        mut score: impl FnMut(PointOffset, PointOffset) -> ScoreType,
    ) {
        self.links.clear();
        if level_m == 0 {
            self.processed_by_heuristic = 0;
            return;
        }

        let capacity_fill = candidates.clone();
        'outer: for candidate in candidates {
            for &existing in &self.links {
                if score(candidate.idx, existing) > candidate.score {
                    continue 'outer;
                }
            }
            self.links.push(candidate.idx);
            if self.links.len() >= level_m {
                break;
            }
        }
        let diverse_len = self.links.len();
        if diverse_len < level_m {
            // The diversity prefix is a subsequence of the original sorted
            // candidate stream. Walk both sequences once instead of probing
            // the prefix for every candidate: at ef_construct=100 and M0=32,
            // repeated `contains` checks add billions of point-id comparisons
            // to a multi-million-row build without changing topology.
            let mut next_diverse = 0usize;
            for candidate in capacity_fill {
                if next_diverse < diverse_len && self.links[next_diverse] == candidate.idx {
                    next_diverse += 1;
                    continue;
                }
                self.links.push(candidate.idx);
                if self.links.len() >= level_m {
                    break;
                }
            }
        }
        self.processed_by_heuristic = diverse_len as u32;
    }

    /// Connect a new point and keep at most `level_m` closest points to `target_point_id`.
    ///
    /// This is a simple distance-ordered insertion without heuristic pruning.
    pub fn connect(
        &mut self,
        new_point_id: PointOffset,
        target_point_id: PointOffset,
        level_m: usize,
        mut score: impl FnMut(PointOffset, PointOffset) -> ScoreType,
    ) {
        if level_m == 0 {
            return;
        }

        // Incremental insertion invalidates heuristic ordering assumptions.
        self.processed_by_heuristic = 0;

        let new_to_target = score(target_point_id, new_point_id);
        let mut id_to_insert = self.links.len();

        for (i, &item) in self.links.iter().enumerate() {
            let target_to_link = score(target_point_id, item);
            if target_to_link < new_to_target {
                id_to_insert = i;
                break;
            }
        }

        if self.links.len() < level_m {
            self.links.insert(id_to_insert, new_point_id);
        } else if id_to_insert != self.links.len() {
            self.links.pop();
            self.links.insert(id_to_insert, new_point_id);
        }
    }

    /// Append one point and keep up to `level_m` links using heuristic pruning.
    ///
    /// This method reuses `ItemsBuffer` to avoid temporary allocations.
    pub fn connect_with_heuristic(
        &mut self,
        new_point_id: PointOffset,
        target_point_id: PointOffset,
        level_m: usize,
        mut score: impl FnMut(PointOffset, PointOffset) -> ScoreType,
        items: &mut ItemsBuffer,
    ) {
        if level_m == 0 {
            return;
        }

        if self.links.len() < level_m {
            self.links.push(new_point_id);
            return;
        }

        items.candidates.clear();
        items.pruned.clear();
        items.reserve(level_m + 1);
        for (order, &link) in self.links.iter().enumerate() {
            items.candidates.push(Item {
                idx: link,
                score: Cell::new(None),
                order: if order < self.processed_by_heuristic as usize {
                    Some(order as u32)
                } else {
                    None
                },
            });
        }
        items.candidates.push(Item {
            idx: new_point_id,
            score: Cell::new(None),
            order: None,
        });

        items.candidates.sort_unstable_by(|a, b| {
            if let (Some(a_order), Some(b_order)) = (a.order, b.order) {
                return a_order.cmp(&b_order);
            }
            b.cached_score(target_point_id, &mut score)
                .total_cmp(&a.cached_score(target_point_id, &mut score))
        });

        self.links.clear();

        // In-place read/write on one buffer:
        // - items[read] is next candidate
        // - items[0..write] are selected neighbors
        let mut write = 0;
        'outer: for read in 0..items.candidates.len() {
            let candidate = items.candidates[read].clone();
            for existing in &items.candidates[0..write] {
                if candidate.order.is_some() && existing.order.is_some() {
                    continue;
                }
                if score(candidate.idx, existing.idx)
                    > candidate.cached_score(target_point_id, &mut score)
                {
                    items.pruned.push(candidate.idx);
                    continue 'outer;
                }
            }

            self.links.push(candidate.idx);
            items.candidates[write] = candidate;
            write += 1;
            if write >= level_m {
                break;
            }
        }
        let diverse_len = self.links.len();
        for candidate in items.pruned.iter().copied() {
            if self.links.len() >= level_m {
                break;
            }
            self.links.push(candidate);
        }
        self.processed_by_heuristic = diverse_len as u32;
    }

    #[cfg(test)]
    fn connect_with_heuristic_simple(
        &mut self,
        new_point_id: PointOffset,
        target_point_id: PointOffset,
        level_m: usize,
        mut score: impl FnMut(PointOffset, PointOffset) -> ScoreType,
    ) {
        if self.links.len() < level_m {
            self.links.push(new_point_id);
        } else {
            let mut candidates = Vec::with_capacity(level_m + 1);
            for &idx in &self.links {
                candidates.push(ScoredPoint {
                    idx,
                    score: score(target_point_id, idx),
                });
            }
            candidates.push(ScoredPoint {
                idx: new_point_id,
                score: score(target_point_id, new_point_id),
            });
            candidates.sort_unstable_by(|a, b| b.score.total_cmp(&a.score));
            self.fill_from_sorted_with_heuristic(candidates.into_iter(), level_m, score);
        }
    }
}

/// Internal reusable buffer for heuristic selection.
#[derive(Debug, Default)]
pub struct ItemsBuffer {
    candidates: Vec<Item>,
    pruned: Vec<PointOffset>,
}

impl ItemsBuffer {
    fn reserve(&mut self, capacity: usize) {
        if self.candidates.capacity() < capacity {
            self.candidates
                .reserve_exact(capacity - self.candidates.capacity());
        }
        if self.pruned.capacity() < capacity {
            self.pruned.reserve_exact(capacity - self.pruned.capacity());
        }
    }
}

#[derive(Debug, Clone)]
struct Item {
    idx: PointOffset,
    score: Cell<Option<ScoreType>>,
    /// If set, the item has already been processed by heuristic and can preserve order.
    order: Option<u32>,
}

impl Item {
    fn cached_score<F>(&self, query: PointOffset, score: F) -> ScoreType
    where
        F: FnOnce(PointOffset, PointOffset) -> ScoreType,
    {
        if let Some(score) = self.score.get() {
            score
        } else {
            let score = score(query, self.idx);
            self.score.set(Some(score));
            score
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};

    use super::*;

    fn score(_: PointOffset, point_id: PointOffset) -> ScoreType {
        // Higher score means closer/better.
        const SCORES: [ScoreType; 8] = [0.0, 0.95, 0.60, 0.80, 0.40, 0.70, 0.50, 0.85];
        SCORES[point_id as usize]
    }

    #[test]
    fn test_basic_methods() {
        let mut links = LinksContainer::with_capacity(4);
        assert_eq!(links.len(), 0);

        links.push(3);
        links.push(1);

        assert_eq!(links.len(), 2);
        assert_eq!(links.as_slice(), &[3, 1]);
        assert_eq!(links.iter().collect::<Vec<_>>(), vec![3, 1]);
    }

    #[test]
    fn test_connect_sorted_insert_and_overflow() {
        let mut links = LinksContainer::with_capacity(3);

        // Build initial sorted state: 1 (0.95), 3 (0.80), 5 (0.70)
        links.connect(3, 0, 3, score);
        links.connect(1, 0, 3, score);
        links.connect(5, 0, 3, score);
        assert_eq!(links.as_slice(), &[1, 3, 5]);

        // Worse point when full -> ignored.
        links.connect(2, 0, 3, score); // 0.60
        assert_eq!(links.as_slice(), &[1, 3, 5]);

        // Better point when full -> inserted in order, tail truncated.
        links.connect(7, 0, 3, score); // 0.85
        assert_eq!(links.as_slice(), &[1, 7, 3]);
    }

    #[test]
    fn test_connect_boundary_conditions() {
        let mut links = LinksContainer::with_capacity(2);

        // level_m == 0 should keep container unchanged.
        links.connect(1, 0, 0, score);
        assert_eq!(links.len(), 0);

        // Empty container insertion.
        links.connect(5, 0, 2, score);
        assert_eq!(links.as_slice(), &[5]);

        // Insert at front when better than existing.
        links.connect(1, 0, 2, score);
        assert_eq!(links.as_slice(), &[1, 5]);
    }

    #[test]
    fn test_fill_from_sorted_with_heuristic_prefers_diversity() {
        let points: [[f32; 2]; 11] = [
            [21.79, 7.18],  // Target
            [20.58, 5.46],  // 1
            [21.19, 4.51],  // 2
            [24.73, 8.24],  // 3
            [24.55, 9.98],  // 4
            [26.11, 6.85],  // 5
            [17.64, 11.14], // 6
            [14.97, 11.52], // 7
            [14.97, 9.60],  // 8
            [16.23, 14.32], // 9
            [12.69, 19.13], // 10
        ];

        let scorer = |a: PointOffset, b: PointOffset| {
            let dx = points[a as usize][0] - points[b as usize][0];
            let dy = points[a as usize][1] - points[b as usize][1];
            -((dx * dx + dy * dy).sqrt())
        };

        let mut candidates = (1..points.len() as PointOffset)
            .map(|idx| ScoredPoint {
                idx,
                score: scorer(0, idx),
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable_by(|a, b| b.score.total_cmp(&a.score));

        let brute_force = candidates.iter().take(6).map(|p| p.idx).collect::<Vec<_>>();
        assert_eq!(brute_force, vec![1, 2, 3, 4, 5, 6]);

        let mut links = LinksContainer::with_capacity(6);
        links.fill_from_sorted_with_heuristic(candidates.into_iter(), 6, scorer);
        assert_eq!(links.as_slice(), &[1, 3, 6, 2, 4, 5]);
    }

    #[test]
    fn test_heuristic_preserves_diverse_prefix_before_capacity_fill() {
        let points: [[f32; 2]; 11] = [
            [21.79, 7.18],  // Target
            [20.58, 5.46],  // 1
            [21.19, 4.51],  // 2
            [24.73, 8.24],  // 3
            [24.55, 9.98],  // 4
            [26.11, 6.85],  // 5
            [17.64, 11.14], // 6
            [14.97, 11.52], // 7
            [14.97, 9.60],  // 8
            [16.23, 14.32], // 9
            [12.69, 19.13], // 10
        ];

        let scorer = |a: PointOffset, b: PointOffset| {
            let dx = points[a as usize][0] - points[b as usize][0];
            let dy = points[a as usize][1] - points[b as usize][1];
            -((dx * dx + dy * dy).sqrt())
        };

        let mut candidates = (1..points.len() as PointOffset)
            .map(|idx| ScoredPoint {
                idx,
                score: scorer(0, idx),
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable_by(|a, b| b.score.total_cmp(&a.score));

        let brute_force = candidates.iter().take(6).map(|p| p.idx).collect::<Vec<_>>();

        let mut heuristic = LinksContainer::with_capacity(6);
        heuristic.fill_from_sorted_with_heuristic(candidates.into_iter(), 6, scorer);

        assert_eq!(brute_force, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(heuristic.as_slice(), &[1, 3, 6, 2, 4, 5]);
        assert_eq!(&heuristic.as_slice()[..3], &[1, 3, 6]);
    }

    #[test]
    fn test_connect_with_heuristic_matches_reference() {
        const NUM_VECTORS: usize = 20;
        const DIM: usize = 8;
        const M: usize = 5;

        let mut rng = StdRng::seed_from_u64(42);

        for _ in 0..200 {
            let mut points = vec![vec![0.0; DIM]; NUM_VECTORS];
            for point in &mut points {
                for x in point {
                    *x = rng.gen_range(-1.0..1.0);
                }
            }

            let mut candidate_indices: Vec<PointOffset> = (0..NUM_VECTORS as u32).collect();
            candidate_indices.shuffle(&mut rng);

            let query_idx = candidate_indices.pop().unwrap();
            let scorer = |a: PointOffset, b: PointOffset| -> ScoreType {
                let pa = &points[a as usize];
                let pb = &points[b as usize];
                let mut sq = 0.0f32;
                for i in 0..DIM {
                    let d = pa[i] - pb[i];
                    sq += d * d;
                }
                -sq.sqrt()
            };

            let mut init_candidates = candidate_indices
                .iter()
                .copied()
                .take(M)
                .map(|idx| ScoredPoint {
                    idx,
                    score: scorer(query_idx, idx),
                })
                .collect::<Vec<_>>();
            init_candidates.sort_unstable_by(|a, b| b.score.total_cmp(&a.score));

            let mut links = LinksContainer::with_capacity(M);
            links.fill_from_sorted_with_heuristic(init_candidates.iter().copied(), M, scorer);

            let mut reference = LinksContainer::with_capacity(M);
            reference.fill_from_sorted_with_heuristic(init_candidates.into_iter(), M, scorer);

            let mut items = ItemsBuffer::default();
            for &candidate_idx in candidate_indices.iter().skip(M) {
                links.connect_with_heuristic(candidate_idx, query_idx, M, scorer, &mut items);
                reference.connect_with_heuristic_simple(candidate_idx, query_idx, M, scorer);
                assert_eq!(links.as_slice(), reference.as_slice());
            }
        }
    }

    #[test]
    fn test_lazy_score_cache_hit() {
        let item = Item {
            idx: 7,
            score: Cell::new(None),
            order: None,
        };

        let calls = Cell::new(0usize);
        let first = item.cached_score(0, |_, _| {
            calls.set(calls.get() + 1);
            0.42
        });
        let second = item.cached_score(0, |_, _| {
            calls.set(calls.get() + 1);
            1.0
        });

        assert_eq!(first, 0.42);
        assert_eq!(second, 0.42);
        assert_eq!(calls.get(), 1);
    }
}
