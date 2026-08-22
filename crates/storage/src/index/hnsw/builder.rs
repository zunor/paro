// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Graph Builder
//!
//! Logic for building the HNSW graph structure.

use crate::index::hnsw::build_cache::DistanceCache;
use crate::index::hnsw::distance::DistanceMetric;
use crate::index::hnsw::entry_points::EntryPoints;
use crate::index::hnsw::graph_links::GraphLinks;
use crate::index::hnsw::links_container::{ItemsBuffer, LinksContainer};
use crate::index::hnsw::search_context::SearchContext;
use crate::index::hnsw::types::{HnswConfig, HnswM, PointOffset, ScoreType, ScoredPoint};
use crate::index::hnsw::vector_storage::VectorStorage;
use crate::index::hnsw::visited_pool::VisitedPool;
use bitvec::prelude::BitVec;
use parking_lot::{Mutex, RwLock};
use std::cmp::max;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Clone, Copy, Default)]
pub struct DistanceProfileSnapshot {
    pub score_calls: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

impl DistanceProfileSnapshot {
    pub fn duplicate_ratio(&self) -> f64 {
        if self.score_calls == 0 {
            0.0
        } else {
            self.cache_hits as f64 / self.score_calls as f64
        }
    }
}

#[derive(Debug, Default)]
struct DistanceProfile {
    score_calls: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
}

impl DistanceProfile {
    fn reset(&self) {
        self.score_calls.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
    }

    fn record_hit(&self) {
        self.score_calls.fetch_add(1, Ordering::Relaxed);
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_miss(&self) {
        self.score_calls.fetch_add(1, Ordering::Relaxed);
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    fn record_direct(&self) {
        self.score_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> DistanceProfileSnapshot {
        DistanceProfileSnapshot {
            score_calls: self.score_calls.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
        }
    }
}

/// Builder for HNSW graph layers.
pub struct GraphLayersBuilder {
    /// Links for each point at each level: links_layers[point_idx][level] = neighbors
    links_layers: Vec<Vec<RwLock<LinksContainer>>>,
    /// Entry points for search
    entry_points: Mutex<EntryPoints>,
    /// Pool of visited lists for search
    visited_pool: VisitedPool,
    /// HNSW M parameters (m and m0)
    hnsw_m: HnswM,
    /// Number of neighbors to consider during construction
    ef_construct: usize,
    /// Level generation factor
    level_factor: f64,
    /// List of flags indicating whether a point is fully linked and ready for traversal.
    ready_list: BitVec<AtomicUsize>,
    /// Current max level in the graph.
    max_level: AtomicUsize,
    /// Whether to use heuristic link selection.
    use_heuristic: bool,
    /// Distance cache slots for heuristic scoring during build.
    /// Zero means disabled.
    distance_cache_slots: usize,
    /// Whether to collect per-build distance profile metrics.
    distance_profile_enabled: bool,
    distance_profile: DistanceProfile,
    /// Stable construction RNG. The algorithm is owned by Paro so identical
    /// input/configuration remains reproducible across rand crate upgrades.
    build_rng: Mutex<DeterministicBuildRng>,
}

/// SplitMix64 construction RNG, version 1.
///
/// Do not change this algorithm in place. A future algorithm requires a new
/// provider-config version so persisted seeds retain their meaning.
#[derive(Debug)]
struct DeterministicBuildRng {
    state: u64,
}

impl DeterministicBuildRng {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    /// Stable uniform sample in the open interval (0, 1).
    fn next_open_unit_f64(&mut self) -> f64 {
        const MANTISSA_VALUES: u64 = 1_u64 << 53;
        let mantissa = self.next_u64() >> 11;
        (mantissa as f64 + 1.0) / (MANTISSA_VALUES as f64 + 1.0)
    }
}

impl GraphLayersBuilder {
    pub fn new(num_points: usize, config: &HnswConfig) -> Self {
        Self::new_with_heuristic(num_points, config, false)
    }

    pub fn new_with_heuristic(num_points: usize, config: &HnswConfig, use_heuristic: bool) -> Self {
        let hnsw_m = HnswM::from(config);
        let level_factor = 1.0 / (max(hnsw_m.m, 2) as f64).ln();

        let mut links_layers = Vec::with_capacity(num_points);
        for _ in 0..num_points {
            links_layers.push(Vec::new());
        }

        Self {
            links_layers,
            entry_points: Mutex::new(EntryPoints::new()),
            visited_pool: VisitedPool::new(),
            hnsw_m,
            ef_construct: config.ef_construct,
            level_factor,
            ready_list: BitVec::repeat(false, num_points),
            max_level: AtomicUsize::new(0),
            use_heuristic,
            distance_cache_slots: 0,
            distance_profile_enabled: false,
            distance_profile: DistanceProfile::default(),
            build_rng: Mutex::new(DeterministicBuildRng::new(config.build_seed)),
        }
    }

    /// Benchmark-only knob to evaluate DistanceCache ROI.
    pub fn set_distance_cache_slots_for_benchmark(&mut self, slots: usize) {
        self.distance_cache_slots = slots;
    }

    /// Benchmark-only knob to profile duplicate distance calculations.
    pub fn set_distance_profile_enabled_for_benchmark(&mut self, enabled: bool) {
        self.distance_profile_enabled = enabled;
        if enabled {
            self.distance_profile.reset();
        }
    }

    pub fn distance_profile_snapshot(&self) -> DistanceProfileSnapshot {
        self.distance_profile.snapshot()
    }

    /// Generate a random level for a new point using geometric distribution.
    pub fn get_random_layer(&self) -> usize {
        let mut rng = self.build_rng.lock();
        let r = rng.next_open_unit_f64();
        let level = (-r.ln() * self.level_factor) as usize;
        level.min(31) // Cap at reasonable max level
    }

    /// Pre-allocate per-point levels and link containers for parallel construction.
    pub fn set_levels(&mut self, point_id: PointOffset, level: usize) {
        let point_idx = point_id as usize;
        if self.links_layers.len() <= point_idx {
            self.links_layers.resize_with(point_idx + 1, Vec::new);
        }
        if self.ready_list.len() <= point_idx {
            self.ready_list.resize(point_idx + 1, false);
        }

        let point_layers = &mut self.links_layers[point_idx];
        while point_layers.len() <= level {
            let level_idx = point_layers.len();
            point_layers.push(RwLock::new(LinksContainer::with_capacity(
                self.hnsw_m.get_m(level_idx),
            )));
        }

        self.max_level.fetch_max(level, Ordering::Relaxed);
    }

    /// Highest level index for a point.
    pub fn get_point_level(&self, point_id: PointOffset) -> usize {
        self.links_layers
            .get(point_id as usize)
            .map(|levels| levels.len().saturating_sub(1))
            .unwrap_or(0)
    }

    /// Add a point using pre-built links.
    ///
    /// The point levels must be pre-allocated with [`Self::set_levels`].
    /// This method is mutually exclusive with [`Self::link_new_point`].
    pub fn add_new_point(&self, point_id: PointOffset, links_by_level: Vec<Vec<PointOffset>>) {
        let point_idx = point_id as usize;
        assert!(
            point_idx < self.links_layers.len(),
            "point {point_id} levels are not preallocated"
        );
        assert!(
            !self.links_layers[point_idx].is_empty(),
            "point {point_id} has no preallocated levels"
        );

        let point_level = self.get_point_level(point_id);
        assert_eq!(
            links_by_level.len(),
            point_level + 1,
            "point {point_id} expected {} levels, got {}",
            point_level + 1,
            links_by_level.len()
        );

        for (level, neighbours) in links_by_level.into_iter().enumerate() {
            self.links_layers[point_idx][level]
                .write()
                .fill_from(neighbours.into_iter());
        }

        assert!(
            !self.ready_list[point_idx],
            "point {point_id} was already marked as ready"
        );
        self.ready_list.set_aliased(point_idx, true);
        self.entry_points
            .lock()
            .new_point(point_id, point_level, |_| true);
    }

    /// Link a new point into the HNSW graph.
    pub fn link_new_point(
        &self,
        point_id: PointOffset,
        storage: &dyn VectorStorage,
        distance: DistanceMetric,
    ) {
        let point_idx = point_id as usize;
        let target_level = self
            .links_layers
            .get(point_idx)
            .and_then(|levels| levels.len().checked_sub(1))
            .expect("point levels must be preallocated via set_levels before link_new_point");

        let entry_points = {
            let mut entry_points = self.entry_points.lock();
            if entry_points.entry_points.is_empty() {
                // First point in the graph
                entry_points.new_point(point_id, target_level, |_| true);
                self.ready_list.set_aliased(point_idx, true);
                return;
            }
            entry_points.clone()
        };

        let entry_point = entry_points
            .get_entry_point(|_| true)
            .expect("entry points must be non-empty");

        let mut current_point = entry_point.point_id;
        let mut current_score = Self::score_indexed(storage, distance, point_id, current_point);
        let mut current_level = entry_point
            .level
            .min(self.max_level.load(Ordering::Relaxed));

        // 1. Greedy search from top level down to target_level + 1
        while current_level > target_level {
            let mut changed = true;
            while changed {
                changed = false;
                self.for_each_link(current_point, current_level, |neighbor| {
                    let neighbor_score = Self::score_indexed(storage, distance, point_id, neighbor);
                    if neighbor_score > current_score {
                        current_score = neighbor_score;
                        current_point = neighbor;
                        changed = true;
                    }
                });
            }
            current_level -= 1;
        }

        // 2. Search and link at each level from target_level down to 0
        let mut candidates = vec![ScoredPoint {
            idx: current_point,
            score: current_score,
        }];
        let mut heuristic_distance_cache = if self.distance_cache_slots > 0 {
            Some(DistanceCache::new(self.distance_cache_slots))
        } else {
            None
        };

        for level in (0..=target_level.min(current_level)).rev() {
            let ef = if level == 0 {
                self.ef_construct
            } else {
                self.hnsw_m.m
            };

            // Search on this level to find best neighbors
            let search_results =
                self.search_on_level(point_id, candidates.clone(), level, ef, storage, distance);

            let m_limit = self.hnsw_m.get_m(level);
            if self.use_heuristic {
                let selected_nearest = {
                    let mut links = self.links_layers[point_idx][level].write();
                    links.fill_from_sorted_with_heuristic(
                        search_results.iter().copied(),
                        m_limit,
                        |target, candidate| {
                            self.score_with_optional_cache(
                                storage,
                                distance,
                                target,
                                candidate,
                                &mut heuristic_distance_cache,
                            )
                        },
                    );
                    links.as_slice().to_vec()
                };

                let mut items = ItemsBuffer::default();
                for &neighbor_id in &selected_nearest {
                    self.links_layers[neighbor_id as usize][level]
                        .write()
                        .connect_with_heuristic(
                            point_id,
                            neighbor_id,
                            m_limit,
                            |target, candidate| {
                                self.score_with_optional_cache(
                                    storage,
                                    distance,
                                    target,
                                    candidate,
                                    &mut heuristic_distance_cache,
                                )
                            },
                            &mut items,
                        );
                }
            } else {
                let neighbors: Vec<PointOffset> =
                    search_results.iter().take(m_limit).map(|p| p.idx).collect();
                for &neighbor_id in &neighbors {
                    // Link new point -> neighbor
                    self.links_layers[point_idx][level].write().connect(
                        neighbor_id,
                        point_id,
                        m_limit,
                        |target, candidate| {
                            Self::score_indexed(storage, distance, target, candidate)
                        },
                    );

                    // Link neighbor -> new point
                    self.add_link_bidirectional(
                        neighbor_id,
                        point_id,
                        level,
                        m_limit,
                        storage,
                        distance,
                    );
                }
            }

            candidates = search_results;
        }

        // Update overall entry points if the new point is at a higher level
        self.entry_points
            .lock()
            .new_point(point_id, target_level, |_| true);
        self.ready_list.set_aliased(point_idx, true);
    }

    fn score_with_optional_cache(
        &self,
        storage: &dyn VectorStorage,
        distance: DistanceMetric,
        target: PointOffset,
        candidate: PointOffset,
        cache: &mut Option<DistanceCache>,
    ) -> ScoreType {
        if let Some(cache) = cache.as_mut() {
            if let Some(score) = cache.get(target, candidate) {
                if self.distance_profile_enabled {
                    self.distance_profile.record_hit();
                }
                return score;
            }

            let score = Self::score_indexed(storage, distance, target, candidate);
            cache.put(target, candidate, score);
            if self.distance_profile_enabled {
                self.distance_profile.record_miss();
            }
            return score;
        }

        if self.distance_profile_enabled {
            self.distance_profile.record_direct();
        }
        Self::score_indexed(storage, distance, target, candidate)
    }

    /// Internal search on a specific level starting from a set of entry points.
    fn search_on_level(
        &self,
        query_point: PointOffset,
        entry_points: Vec<ScoredPoint>,
        level: usize,
        ef: usize,
        storage: &dyn VectorStorage,
        distance: DistanceMetric,
    ) -> Vec<ScoredPoint> {
        let mut visited = self.visited_pool.get(storage.num_vectors());
        let mut search_context = SearchContext::new(entry_points[0], ef);

        visited.check_and_update_visited(entry_points[0].idx);
        for ep in entry_points.iter().skip(1) {
            visited.check_and_update_visited(ep.idx);
            search_context.process_candidate(*ep);
        }

        while let Some(candidate) = search_context.candidates.pop() {
            if candidate.score < search_context.lower_bound() && search_context.nearest.len() >= ef
            {
                break;
            }

            self.for_each_link(candidate.idx, level, |neighbor| {
                if !visited.check_and_update_visited(neighbor) {
                    let score = Self::score_indexed(storage, distance, query_point, neighbor);
                    search_context.process_candidate(ScoredPoint {
                        idx: neighbor,
                        score,
                    });
                }
            });
        }

        search_context.nearest.into_sorted_vec()
    }

    /// Add a link from `from` to `to` at `level`, ensuring `m_limit` is respected.
    fn add_link_bidirectional(
        &self,
        from: PointOffset,
        to: PointOffset,
        level: usize,
        m_limit: usize,
        storage: &dyn VectorStorage,
        distance: DistanceMetric,
    ) {
        let links_row = &self.links_layers[from as usize];
        if level < links_row.len() {
            let mut links = links_row[level].write();
            links.connect(to, from, m_limit, |target, candidate| {
                Self::score_indexed(storage, distance, target, candidate)
            });
        }
    }

    #[inline]
    fn score_indexed(
        storage: &dyn VectorStorage,
        distance: DistanceMetric,
        left: PointOffset,
        right: PointOffset,
    ) -> ScoreType {
        distance.similarity_indexed(
            storage.get_vector(left),
            storage.get_vector(right),
            storage.cosine_inverse_norm(left),
            storage.cosine_inverse_norm(right),
        )
    }

    /// Helper to iterate over links of a point at a specific level during build.
    fn for_each_link<F>(&self, point_id: PointOffset, level: usize, mut f: F)
    where
        F: FnMut(PointOffset),
    {
        let point_idx = point_id as usize;
        if point_idx < self.links_layers.len() {
            let levels = &self.links_layers[point_idx];
            if level < levels.len() {
                let links = levels[level].read();
                for neighbor in links.iter() {
                    if self.ready_list[neighbor as usize] {
                        f(neighbor);
                    }
                }
            }
        }
    }

    /// Convert the builder into a flattened `GraphLinks` and `EntryPoints`.
    pub fn into_graph_data(self) -> (GraphLinks, EntryPoints) {
        let mut flattened_edges = Vec::with_capacity(self.links_layers.len());
        for point_levels in self.links_layers {
            let mut levels = Vec::with_capacity(point_levels.len());
            for level_lock in point_levels {
                levels.push(level_lock.into_inner().into_vec());
            }
            flattened_edges.push(levels);
        }

        let graph_links = GraphLinks::new_from_edges(flattened_edges);
        let entry_points = self.entry_points.into_inner();

        (graph_links, entry_points)
    }
}
