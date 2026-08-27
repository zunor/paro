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
#[cfg(test)]
use crate::index::hnsw::types::HnswConfig;
use crate::index::hnsw::types::{HnswBuildContract, HnswM, PointOffset, ScoreType, ScoredPoint};
use crate::index::hnsw::vector_storage::VectorStorage;
use crate::index::hnsw::visited_pool::{BuildVisitedListHandle, BuildVisitedPool};
use bitvec::prelude::BitVec;
use parking_lot::{Mutex, RwLock};
use paro_common::error::Result;
use rayon::prelude::*;
use std::cmp::max;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug)]
pub(crate) struct FrozenPointProposal {
    point_id: PointOffset,
    links_by_level: Vec<Vec<PointOffset>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ReciprocalLink {
    target: PointOffset,
    level: usize,
    source: PointOffset,
}

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
    visited_pool: BuildVisitedPool,
    /// HNSW M parameters (m and m0)
    hnsw_m: HnswM,
    /// Number of neighbors to consider during construction
    ef_construct: usize,
    /// Level generation factor
    level_factor: f64,
    /// List of flags indicating whether a point is fully linked and ready for traversal.
    ready_list: BitVec<AtomicUsize>,
    /// Whether to use heuristic link selection.
    use_heuristic: bool,
    /// Distance cache slots for heuristic scoring during build.
    /// Zero means disabled.
    distance_cache_slots: usize,
    /// Whether to collect per-build distance profile metrics.
    distance_profile_enabled: bool,
    distance_profile: DistanceProfile,
    /// Stable construction seed. Levels are derived from `(seed, point_id)`
    /// without shared mutable state, so future parallel proposal generation
    /// cannot make topology depend on scheduling order.
    build_seed: u64,
}

/// SplitMix64 construction RNG, version 1.
///
/// Do not change this algorithm in place. A future algorithm requires a new
/// provider-config version so persisted seeds retain their meaning.
struct DeterministicBuildRng;

impl DeterministicBuildRng {
    fn point_u64(seed: u64, point_id: PointOffset) -> u64 {
        // This is exactly the point-id-indexed form of the previous serial
        // SplitMix64 stream: point 0 observes the first draw, point 1 the
        // second, and so on. It preserves topology while removing draw order.
        let stream_position = u64::from(point_id).wrapping_add(1);
        let mut value = seed.wrapping_add(0x9e37_79b9_7f4a_7c15_u64.wrapping_mul(stream_position));
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    /// Stable uniform sample in the open interval (0, 1).
    fn point_open_unit_f64(seed: u64, point_id: PointOffset) -> f64 {
        const MANTISSA_VALUES: u64 = 1_u64 << 53;
        let mantissa = Self::point_u64(seed, point_id) >> 11;
        (mantissa as f64 + 1.0) / (MANTISSA_VALUES as f64 + 1.0)
    }
}

impl GraphLayersBuilder {
    #[cfg(test)]
    pub fn new(num_points: usize, config: &HnswConfig) -> Self {
        Self::new_with_heuristic(num_points, config, false)
    }

    #[cfg(test)]
    pub fn new_with_heuristic(num_points: usize, config: &HnswConfig, use_heuristic: bool) -> Self {
        let contract = config
            .try_build_contract(DistanceMetric::Euclidean)
            .expect("test HNSW configuration is valid");
        Self::new_from_contract(num_points, &contract, use_heuristic)
    }

    pub fn new_from_contract(
        num_points: usize,
        contract: &HnswBuildContract,
        use_heuristic: bool,
    ) -> Self {
        Self::new_from_contract_with_visited_capacity(num_points, contract, use_heuristic, 16)
    }

    pub(crate) fn new_from_contract_with_visited_capacity(
        num_points: usize,
        contract: &HnswBuildContract,
        use_heuristic: bool,
        visited_capacity: usize,
    ) -> Self {
        let hnsw_m = HnswM::from(contract);
        let level_factor = 1.0 / (max(hnsw_m.m, 2) as f64).ln();

        let mut links_layers = Vec::with_capacity(num_points);
        for _ in 0..num_points {
            links_layers.push(Vec::new());
        }

        Self {
            links_layers,
            entry_points: Mutex::new(EntryPoints::new()),
            visited_pool: BuildVisitedPool::with_keep_limit(
                num_points,
                visited_capacity,
                (contract.ef_construct as usize).saturating_mul(hnsw_m.m0),
            ),
            hnsw_m,
            ef_construct: contract.ef_construct as usize,
            level_factor,
            ready_list: BitVec::repeat(false, num_points),
            use_heuristic,
            distance_cache_slots: 0,
            distance_profile_enabled: false,
            distance_profile: DistanceProfile::default(),
            build_seed: contract.build_seed,
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

    /// Construction breadth is a graph-wide contract, not a level-specific
    /// shortcut. Upper layers are sparse routing indexes and need the same
    /// candidate beam to remain representative as level 0.
    const fn construction_beam_for_level(&self, _level: usize) -> usize {
        self.ef_construct
    }

    /// Generate a random level for a new point using geometric distribution.
    pub fn random_layer_for_point(&self, point_id: PointOffset) -> usize {
        let r = DeterministicBuildRng::point_open_unit_f64(self.build_seed, point_id);
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
    /// This is used only when importing or repairing already-computed links.
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

    /// Snapshot the entry points published before the current frozen wave.
    pub(crate) fn snapshot_entry_points(&self) -> EntryPoints {
        self.entry_points.lock().clone()
    }

    /// Insert one point through the same proposal/publication algorithm used by
    /// parallel waves. A serial warm-up is therefore a wave of size one rather
    /// than a second construction algorithm that can drift over time.
    pub(crate) fn insert_single_point(
        &self,
        point_id: PointOffset,
        storage: &dyn VectorStorage,
        distance: DistanceMetric,
    ) -> Result<()> {
        let entry_points = self.snapshot_entry_points();
        let proposal = self.propose_new_point(point_id, &entry_points, storage, distance)?;
        self.publish_frozen_wave(vec![proposal], storage, distance, 1);
        Ok(())
    }

    /// Compute one point's outgoing links against the currently published graph.
    ///
    /// The method does not mutate graph topology or readiness. A complete wave can
    /// therefore run in parallel while every proposal observes the same immutable
    /// topology. Publication is handled separately by [`Self::publish_frozen_wave`].
    pub(crate) fn propose_new_point(
        &self,
        point_id: PointOffset,
        entry_points: &EntryPoints,
        storage: &dyn VectorStorage,
        distance: DistanceMetric,
    ) -> Result<FrozenPointProposal> {
        let target_level = self.get_point_level(point_id);
        let Some(entry_point) = entry_points.get_entry_point(|_| true) else {
            return Ok(FrozenPointProposal {
                point_id,
                links_by_level: vec![Vec::new(); target_level + 1],
            });
        };

        let mut current_point = entry_point.point_id;
        let mut current_score = Self::score_indexed(storage, distance, point_id, current_point);
        let mut current_level = entry_point.level;

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

        let mut links_by_level = vec![Vec::new(); target_level + 1];
        let mut candidates = vec![ScoredPoint {
            idx: current_point,
            score: current_score,
        }];
        let mut heuristic_distance_cache = if self.distance_cache_slots > 0 {
            Some(DistanceCache::new(self.distance_cache_slots))
        } else {
            None
        };
        // One proposal owns one visited workspace for all hierarchy levels.
        // The generation counter provides O(1) logical clears between levels;
        // borrowing from the shared pool per level only adds synchronization
        // and cannot change the search result.
        debug_assert_eq!(storage.num_vectors(), self.links_layers.len());
        let mut visited = self.visited_pool.get()?;
        let mut first_search_level = true;

        for level in (0..=target_level.min(current_level)).rev() {
            if first_search_level {
                first_search_level = false;
            } else {
                visited.next_iteration();
            }
            // `ef_construct` is the construction beam for every HNSW layer.
            // Narrowing upper layers to M produces cheap but poorly routed
            // large graphs: their sparse hierarchy is precisely where a wide
            // beam has the highest leverage on level-0 recall. Keep this
            // invariant uniform instead of hiding a second build policy behind
            // the level number.
            let search_results = self.search_on_level(
                point_id,
                candidates,
                level,
                self.construction_beam_for_level(level),
                storage,
                distance,
                &mut visited,
            );
            let m_limit = self.hnsw_m.get_m(level);
            if self.use_heuristic {
                let mut selected = LinksContainer::with_capacity(m_limit);
                selected.fill_from_sorted_with_heuristic(
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
                links_by_level[level] = selected.into_vec();
            } else {
                links_by_level[level] = search_results
                    .iter()
                    .take(m_limit)
                    .map(|point| point.idx)
                    .collect();
            }
            candidates = search_results;
        }

        Ok(FrozenPointProposal {
            point_id,
            links_by_level,
        })
    }

    /// Publish a wave of proposals computed from one frozen topology.
    ///
    /// Outgoing rows are installed first. Reciprocal mutations are then grouped
    /// by `(target, level)`: groups are independent and can run in parallel,
    /// while updates inside a group retain point-id order. Ready bits and entry
    /// points become visible only after every reciprocal mutation completes.
    pub(crate) fn publish_frozen_wave(
        &self,
        mut proposals: Vec<FrozenPointProposal>,
        storage: &dyn VectorStorage,
        distance: DistanceMetric,
        parallelism: usize,
    ) {
        proposals.sort_unstable_by_key(|proposal| proposal.point_id);
        let mut reciprocal = Vec::new();

        for proposal in &proposals {
            let point_idx = proposal.point_id as usize;
            for (level, neighbors) in proposal.links_by_level.iter().enumerate() {
                self.links_layers[point_idx][level]
                    .write()
                    .fill_from(neighbors.iter().copied());
                reciprocal.extend(neighbors.iter().copied().map(|target| ReciprocalLink {
                    target,
                    level,
                    source: proposal.point_id,
                }));
            }
        }

        reciprocal.sort_unstable();
        let mut groups = Vec::new();
        let mut start = 0;
        while start < reciprocal.len() {
            let key = (reciprocal[start].target, reciprocal[start].level);
            let mut end = start + 1;
            while end < reciprocal.len() && (reciprocal[end].target, reciprocal[end].level) == key {
                end += 1;
            }
            groups.push((start, end));
            start = end;
        }

        let publish_group = |(start, end): (usize, usize)| {
            let first = reciprocal[start];
            let m_limit = self.hnsw_m.get_m(first.level);
            let mut links = self.links_layers[first.target as usize][first.level].write();
            let mut items = ItemsBuffer::default();
            let mut distance_cache = if self.distance_cache_slots > 0 {
                Some(DistanceCache::new(self.distance_cache_slots))
            } else {
                None
            };
            for update in &reciprocal[start..end] {
                if self.use_heuristic {
                    links.connect_with_heuristic(
                        update.source,
                        update.target,
                        m_limit,
                        |target, candidate| {
                            self.score_with_optional_cache(
                                storage,
                                distance,
                                target,
                                candidate,
                                &mut distance_cache,
                            )
                        },
                        &mut items,
                    );
                } else {
                    links.connect(
                        update.source,
                        update.target,
                        m_limit,
                        |target, candidate| {
                            Self::score_indexed(storage, distance, target, candidate)
                        },
                    );
                }
            }
        };
        if parallelism > 1 {
            let chunk_size = groups.len().div_ceil(parallelism).max(1);
            groups
                .par_chunks(chunk_size)
                .for_each(|chunk| chunk.iter().copied().for_each(&publish_group));
        } else {
            groups.into_iter().for_each(publish_group);
        }

        let mut entry_points = self.entry_points.lock();
        for proposal in proposals {
            let point_idx = proposal.point_id as usize;
            self.ready_list.set_aliased(point_idx, true);
            entry_points.new_point(
                proposal.point_id,
                self.get_point_level(proposal.point_id),
                |_| true,
            );
        }
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
        visited: &mut BuildVisitedListHandle<'_>,
    ) -> Vec<ScoredPoint> {
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

    /// Score one unordered artifact point pair in a canonical operand order.
    ///
    /// Cosine scoring multiplies the dot product by two persisted inverse
    /// norms. Floating-point multiplication is not associative, so allowing
    /// call-site order to choose `(dot * norm_a) * norm_b` versus
    /// `(dot * norm_b) * norm_a` can change the last bit and, at a heuristic
    /// boundary, the durable topology. Every construction and repair path must
    /// use this function rather than calling `similarity_indexed` directly.
    #[inline]
    pub(crate) fn score_indexed(
        storage: &dyn VectorStorage,
        distance: DistanceMetric,
        mut left: PointOffset,
        mut right: PointOffset,
    ) -> ScoreType {
        if distance == DistanceMetric::Cosine && left > right {
            // Only cosine consumes point-owned norm factors in a left-
            // associative multiply. The other finite-valued metrics are
            // bitwise symmetric, so keep their multi-billion-call build loop
            // free of a random point-id compare and conditional swap.
            std::mem::swap(&mut left, &mut right);
        }
        storage.construction_similarity(distance, left, right)
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
    pub fn into_graph_data(self) -> paro_common::error::Result<(GraphLinks, EntryPoints)> {
        let mut flattened_edges = Vec::with_capacity(self.links_layers.len());
        for point_levels in self.links_layers {
            let mut levels = Vec::with_capacity(point_levels.len());
            for level_lock in point_levels {
                levels.push(level_lock.into_inner().into_vec());
            }
            flattened_edges.push(levels);
        }

        let graph_links = GraphLinks::try_new_from_edges(flattened_edges, self.hnsw_m.m0)?;
        let entry_points = self.entry_points.into_inner();

        Ok((graph_links, entry_points))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_sampling_depends_on_point_identity_not_call_order() {
        let contract = HnswConfig::new(16, 96)
            .with_build_seed(0x1234_5678)
            .build_contract(DistanceMetric::Euclidean);
        let forward = GraphLayersBuilder::new_from_contract(128, &contract, true);
        let forward_levels = (0..128_u32)
            .map(|point| forward.random_layer_for_point(point))
            .collect::<Vec<_>>();
        let reverse = GraphLayersBuilder::new_from_contract(128, &contract, true);
        let mut reverse_levels = (0..128_u32)
            .rev()
            .map(|point| (point, reverse.random_layer_for_point(point)))
            .collect::<Vec<_>>();
        reverse_levels.sort_unstable_by_key(|(point, _)| *point);

        assert_eq!(
            forward_levels,
            reverse_levels
                .into_iter()
                .map(|(_, level)| level)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn construction_beam_is_uniform_across_all_layers() {
        let contract = HnswConfig::new(16, 96).build_contract(DistanceMetric::Euclidean);
        let builder = GraphLayersBuilder::new_from_contract(1, &contract, true);

        assert_eq!(builder.construction_beam_for_level(0), 96);
        assert_eq!(builder.construction_beam_for_level(1), 96);
        assert_eq!(builder.construction_beam_for_level(31), 96);
    }
}
