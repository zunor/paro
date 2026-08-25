// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Graph Healer
//!
//! Reuses links from an existing graph and repairs edges that point to deleted
//! points by searching shortcut candidates through deleted subgraphs.

use super::builder::GraphLayersBuilder;
use super::graph::GraphLayers;
use super::links_container::{ItemsBuffer, LinksContainer};
use super::search_context::FixedLengthPriorityQueue;
use super::types::{HnswM, PointOffset, ScoredPoint};
use super::vector_storage::VectorStorage;
use super::visited_pool::VisitedPool;
use super::DistanceMetric;
use parking_lot::RwLock;
use paro_common::error::Result;

type LockedLinkContainer = RwLock<LinksContainer>;
type LockedLayersContainer = Vec<LockedLinkContainer>;

#[inline]
fn score_indexed(
    storage: &dyn VectorStorage,
    distance: DistanceMetric,
    left: PointOffset,
    right: PointOffset,
) -> f32 {
    if distance == DistanceMetric::Cosine {
        let norms = storage
            .cosine_inverse_norms()
            .unwrap_or_else(|| unreachable!("cosine healer storage is prepared once"));
        distance.similarity_indexed(
            storage.get_vector(left),
            storage.get_vector(right),
            norms.value(left),
            norms.value(right),
        )
    } else {
        distance.similarity(storage.get_vector(left), storage.get_vector(right))
    }
}

/// Repairs an old graph and migrates surviving points into a new builder.
pub struct GraphLayersHealer<'a> {
    links_layers: Vec<LockedLayersContainer>,
    to_heal: Vec<(PointOffset, usize)>,
    old_to_new: &'a [Option<PointOffset>],
    hnsw_m: HnswM,
    ef_construct: usize,
    visited_pool: VisitedPool,
}

impl<'a> GraphLayersHealer<'a> {
    pub fn new(
        graph_layers: &GraphLayers,
        old_to_new: &'a [Option<PointOffset>],
        ef_construct: usize,
    ) -> Result<Self> {
        let mut to_heal = Vec::new();
        let mut links_layers = Vec::with_capacity(graph_layers.links.num_points());

        for point_idx in 0..graph_layers.links.num_points() {
            let point_id = point_idx as PointOffset;
            let num_levels = graph_layers.links.num_levels(point_id)?;
            let mut point_layers = Vec::with_capacity(num_levels);

            for level in 0..num_levels {
                let level_m = graph_layers.hnsw_m.get_m(level);
                let mut container = LinksContainer::with_capacity(level_m);
                if let Some(level_links) = graph_layers.links.links_on_level(point_id, level)? {
                    container.fill_from(level_links.iter().copied().take(level_m));
                }

                if old_to_new.get(point_idx).copied().flatten().is_some()
                    && container.iter().any(|neighbor| {
                        old_to_new
                            .get(neighbor as usize)
                            .copied()
                            .flatten()
                            .is_none()
                    })
                {
                    to_heal.push((point_id, level));
                }

                point_layers.push(RwLock::new(container));
            }

            links_layers.push(point_layers);
        }

        Ok(Self {
            links_layers,
            to_heal,
            old_to_new,
            hnsw_m: graph_layers.hnsw_m,
            ef_construct,
            visited_pool: VisitedPool::new(),
        })
    }

    fn point_deleted(&self, point: PointOffset) -> bool {
        self.old_to_new
            .get(point as usize)
            .copied()
            .flatten()
            .is_none()
    }

    /// Greedy DFS over deleted points and collects reachable non-deleted border points.
    fn search_shortcuts_on_level(
        &self,
        offset: PointOffset,
        level: usize,
        storage: &dyn VectorStorage,
        distance: DistanceMetric,
    ) -> Vec<ScoredPoint> {
        if self.ef_construct == 0
            || offset as usize >= self.links_layers.len()
            || level >= self.links_layers[offset as usize].len()
        {
            return Vec::new();
        }

        let mut visited = self.visited_pool.get(self.links_layers.len());
        let mut nearest = FixedLengthPriorityQueue::<ScoredPoint>::new(self.ef_construct);

        let mut pending = Vec::new();
        let mut neighbours = Vec::with_capacity(self.hnsw_m.get_m(level).saturating_mul(2));

        visited.check_and_update_visited(offset);
        {
            let links = self.links_layers[offset as usize][level].read();
            for point in links.iter() {
                if self.point_deleted(point) {
                    let score = score_indexed(storage, distance, offset, point);
                    pending.push(ScoredPoint { idx: point, score });
                } else {
                    visited.check_and_update_visited(point);
                }
            }
        }

        while let Some(candidate) = pending.pop() {
            if nearest.len() == nearest.capacity
                && nearest
                    .min_element()
                    .is_some_and(|min| candidate.score < min.score)
            {
                continue;
            }

            if visited.check_and_update_visited(candidate.idx) {
                continue;
            }

            neighbours.clear();
            self.links_layers[candidate.idx as usize][level]
                .read()
                .iter()
                .filter(|link| !visited.check(*link))
                .for_each(|link| neighbours.push(link));

            for idx in neighbours.iter().copied() {
                let score = score_indexed(storage, distance, offset, idx);
                if self.point_deleted(idx) {
                    pending.push(ScoredPoint { idx, score });
                } else {
                    nearest.push(ScoredPoint { idx, score });
                }
            }
        }

        nearest.into_sorted_vec()
    }

    fn heal_point_on_level(
        &self,
        offset: PointOffset,
        level: usize,
        storage: &dyn VectorStorage,
        distance: DistanceMetric,
    ) {
        let level_m = self.hnsw_m.get_m(level);
        if level_m == 0 {
            return;
        }

        let mut valid_links = Vec::with_capacity(level_m);
        valid_links.extend(
            self.links_layers[offset as usize][level]
                .read()
                .iter()
                .filter(|idx| !self.point_deleted(*idx)),
        );
        valid_links.truncate(level_m);

        let shortcuts = self.search_shortcuts_on_level(offset, level, storage, distance);
        let mut container = LinksContainer::with_capacity(level_m);
        let scorer = |a: PointOffset, b: PointOffset| score_indexed(storage, distance, a, b);
        container.fill_from_sorted_with_heuristic(
            shortcuts.into_iter(),
            level_m.saturating_sub(valid_links.len()),
            scorer,
        );

        for &link in &valid_links {
            if container.len() >= level_m {
                break;
            }
            if container.iter().any(|existing| existing == link) {
                continue;
            }
            container.push(link);
        }

        let repaired = container.into_vec();
        self.links_layers[offset as usize][level]
            .write()
            .fill_from(repaired.iter().copied());

        // Keep backlinks consistent after shortcut insertion.
        let mut items = ItemsBuffer::default();
        for other_point in repaired {
            if self.point_deleted(other_point) {
                continue;
            }

            let mut other_container = self.links_layers[other_point as usize][level].write();
            if !other_container.iter().any(|link| link == offset) {
                other_container.connect_with_heuristic(
                    offset,
                    other_point,
                    level_m,
                    scorer,
                    &mut items,
                );
            }
        }
    }

    /// Heal all marked point/level pairs.
    pub fn heal(&mut self, storage: &dyn VectorStorage, distance: DistanceMetric) {
        for (offset, level) in std::mem::take(&mut self.to_heal) {
            if self.point_deleted(offset) {
                continue;
            }
            self.heal_point_on_level(offset, level, storage, distance);
        }
    }

    /// Save surviving points and repaired links into a pre-allocated builder.
    pub fn save_into_builder(self, builder: &GraphLayersBuilder) {
        for (old_offset, layers) in self.links_layers.into_iter().enumerate() {
            let Some(new_offset) = self.old_to_new.get(old_offset).copied().flatten() else {
                continue;
            };

            let links_by_level = layers
                .into_iter()
                .map(|layer| {
                    layer
                        .into_inner()
                        .into_vec()
                        .into_iter()
                        .filter_map(|link| self.old_to_new.get(link as usize).copied().flatten())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            builder.add_new_point(new_offset, links_by_level);
        }
    }

    #[cfg(test)]
    fn pending_count(&self) -> usize {
        self.to_heal.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::builder::GraphLayersBuilder;
    use crate::index::hnsw::persistence::HnswIndex;
    use crate::index::hnsw::types::{HnswConfig, SearchParams};
    use crate::index::hnsw::vector_storage::{InMemoryVectorStorage, VectorStorage};
    use crate::index::hnsw::{IndexedVectorStorage, VisitedPool};
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use std::cmp::max;
    use std::sync::Arc;

    fn make_storage(vectors: &[Vec<f32>]) -> Arc<InMemoryVectorStorage> {
        let dim = vectors.first().map(|v| v.len()).unwrap_or(0);
        let mut flat = Vec::with_capacity(vectors.len() * dim);
        for v in vectors {
            assert_eq!(v.len(), dim);
            flat.extend_from_slice(v);
        }
        Arc::new(InMemoryVectorStorage::new(flat, dim))
    }

    fn make_sift_like_vectors(
        seed: u64,
        num_vectors: usize,
        dim: usize,
        num_clusters: usize,
    ) -> Vec<Vec<f32>> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut centroids = Vec::with_capacity(num_clusters);
        for _ in 0..num_clusters {
            let mut centroid = Vec::with_capacity(dim);
            for _ in 0..dim {
                centroid.push(rng.gen_range(0.0..1.0));
            }
            centroids.push(centroid);
        }

        let mut vectors = Vec::with_capacity(num_vectors);
        for i in 0..num_vectors {
            let centroid = &centroids[i % num_clusters];
            let mut vector = Vec::with_capacity(dim);
            for &base in centroid {
                let noise = rng.gen_range(-0.12f32..0.12f32);
                vector.push((base + noise).clamp(0.0f32, 1.0f32));
            }
            vectors.push(vector);
        }

        vectors
    }

    fn make_sift_like_queries(
        seed: u64,
        vectors: &[Vec<f32>],
        num_queries: usize,
        jitter: f32,
    ) -> Vec<Vec<f32>> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut queries = Vec::with_capacity(num_queries);
        for _ in 0..num_queries {
            let base_idx = rng.gen_range(0..vectors.len());
            let mut query = vectors[base_idx].clone();
            for v in &mut query {
                *v = (*v + rng.gen_range(-jitter..jitter)).clamp(0.0, 1.0);
            }
            queries.push(query);
        }
        queries
    }

    fn deterministic_levels(num_vectors: usize, m: usize, seed: u64) -> Vec<usize> {
        let mut rng = StdRng::seed_from_u64(seed);
        let level_factor = 1.0 / (max(m, 2) as f64).ln();
        (0..num_vectors)
            .map(|_| {
                let r = rng.gen_range(f64::EPSILON..1.0);
                ((-r.ln() * level_factor) as usize).min(6)
            })
            .collect()
    }

    fn build_index_with_levels(
        vectors: &[Vec<f32>],
        levels: &[usize],
        config: HnswConfig,
        distance: DistanceMetric,
    ) -> HnswIndex {
        assert_eq!(vectors.len(), levels.len());

        let storage = IndexedVectorStorage::prepare(make_storage(vectors), distance);
        let mut builder = GraphLayersBuilder::new_with_heuristic(vectors.len(), &config, true);
        for (idx, level) in levels.iter().copied().enumerate() {
            builder.set_levels(idx as u32, level);
        }
        for i in 0..vectors.len() {
            builder.insert_single_point(i as u32, storage.as_ref(), distance);
        }
        let (links, entry_points) = builder.into_graph_data();
        let graph = GraphLayers::new(links, entry_points, VisitedPool::new(), (&config).into());
        HnswIndex::new(config, graph, storage, distance)
    }

    fn brute_force_top_k_ids(
        vectors: &[Vec<f32>],
        query: &[f32],
        top_k: usize,
        distance: DistanceMetric,
    ) -> Vec<PointOffset> {
        let mut scored = vectors
            .iter()
            .enumerate()
            .map(|(idx, vector)| ScoredPoint {
                idx: idx as PointOffset,
                score: distance.similarity(query, vector),
            })
            .collect::<Vec<_>>();
        scored.sort_unstable_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.idx.cmp(&b.idx)));
        scored.truncate(top_k);
        scored.into_iter().map(|point| point.idx).collect()
    }

    fn average_recall_at_k(
        index: &HnswIndex,
        vectors: &[Vec<f32>],
        queries: &[Vec<f32>],
        top_k: usize,
        search_params: &SearchParams,
        distance: DistanceMetric,
    ) -> f32 {
        let mut total_recall = 0.0f32;
        for query in queries {
            let expected = brute_force_top_k_ids(vectors, query, top_k, distance);
            let actual = index.search_one(query, top_k, search_params, None).unwrap();
            let hits = actual
                .iter()
                .filter(|point| expected.contains(&point.idx))
                .count();
            total_recall += hits as f32 / top_k as f32;
        }
        total_recall / queries.len() as f32
    }

    fn largest_component_ratio(index: &HnswIndex) -> f32 {
        let n = index.graph.links.num_points();
        if n <= 1 {
            return 1.0;
        }

        let mut adjacency = vec![Vec::<usize>::new(); n];
        for point_id in 0..n as PointOffset {
            index
                .graph
                .links
                .for_each_link(point_id, 0, |neighbor| {
                    let a = point_id as usize;
                    let b = neighbor as usize;
                    adjacency[a].push(b);
                    adjacency[b].push(a);
                })
                .unwrap();
        }

        let mut visited = vec![false; n];
        let mut largest = 0usize;
        for start in 0..n {
            if visited[start] {
                continue;
            }

            let mut stack = vec![start];
            visited[start] = true;
            let mut size = 0usize;
            while let Some(node) = stack.pop() {
                size += 1;
                for &next in &adjacency[node] {
                    if !visited[next] {
                        visited[next] = true;
                        stack.push(next);
                    }
                }
            }
            largest = largest.max(size);
        }

        largest as f32 / n as f32
    }

    fn build_old_to_new_mapping(
        num_points: usize,
        delete_ratio: f32,
        seed: u64,
    ) -> (Vec<Option<PointOffset>>, Vec<PointOffset>) {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut order = (0..num_points).collect::<Vec<_>>();
        order.shuffle(&mut rng);

        let max_delete = num_points.saturating_sub(2);
        let delete_count = (((num_points as f32) * delete_ratio).round() as usize).min(max_delete);

        let mut deleted = vec![false; num_points];
        for idx in order.into_iter().take(delete_count) {
            deleted[idx] = true;
        }

        let mut old_to_new = vec![None; num_points];
        let mut survivors_old = Vec::with_capacity(num_points - delete_count);
        for (old_idx, is_deleted) in deleted.into_iter().enumerate() {
            if !is_deleted {
                let new_idx = survivors_old.len() as PointOffset;
                old_to_new[old_idx] = Some(new_idx);
                survivors_old.push(old_idx as PointOffset);
            }
        }

        (old_to_new, survivors_old)
    }

    #[test]
    fn test_heal_after_delete_ratios_preserves_connectivity_and_recall() {
        let num_vectors = 720;
        let dim = 64;
        let distance = DistanceMetric::Euclidean;
        let vectors = make_sift_like_vectors(42, num_vectors, dim, 48);
        let config = HnswConfig::new(16, 128)
            .with_plain_scan_threshold(0)
            .with_ef(160);
        let levels = deterministic_levels(num_vectors, config.m, 77);
        let base_index = build_index_with_levels(&vectors, &levels, config, distance);

        for (ratio, seed) in [(0.10f32, 11u64), (0.30f32, 22u64), (0.50f32, 33u64)] {
            let (old_to_new, survivors_old) = build_old_to_new_mapping(num_vectors, ratio, seed);

            let survivor_vectors = survivors_old
                .iter()
                .map(|&old_idx| vectors[old_idx as usize].clone())
                .collect::<Vec<_>>();
            let survivor_levels = survivors_old
                .iter()
                .map(|&old_idx| base_index.graph.links.point_level(old_idx).unwrap())
                .collect::<Vec<_>>();
            let survivor_queries = make_sift_like_queries(seed + 1000, &survivor_vectors, 64, 0.02);

            let rebuilt =
                build_index_with_levels(&survivor_vectors, &survivor_levels, config, distance);

            let mut healed_builder =
                GraphLayersBuilder::new_with_heuristic(survivor_vectors.len(), &config, true);
            for &old_idx in &survivors_old {
                let new_idx = old_to_new[old_idx as usize].expect("survivor must be remapped");
                healed_builder.set_levels(
                    new_idx,
                    base_index.graph.links.point_level(old_idx).unwrap(),
                );
            }

            let mut healer =
                GraphLayersHealer::new(&base_index.graph, &old_to_new, config.ef_construct)
                    .unwrap();
            assert!(
                healer.pending_count() > 0,
                "expected non-empty repair set for delete ratio {ratio}"
            );
            healer.heal(base_index.vector_storage.as_ref(), distance);
            healer.save_into_builder(&healed_builder);

            let survivor_storage: Arc<dyn VectorStorage> = make_storage(&survivor_vectors);
            let (links, entry_points) = healed_builder.into_graph_data();
            let healed_graph =
                GraphLayers::new(links, entry_points, VisitedPool::new(), (&config).into());
            let healed = HnswIndex::new(config, healed_graph, survivor_storage, distance);

            let search_params = SearchParams {
                ef: Some(config.ef),
                ..Default::default()
            };

            let healed_recall = average_recall_at_k(
                &healed,
                &survivor_vectors,
                &survivor_queries,
                10,
                &search_params,
                distance,
            );
            let rebuilt_recall = average_recall_at_k(
                &rebuilt,
                &survivor_vectors,
                &survivor_queries,
                10,
                &search_params,
                distance,
            );

            let healed_connectivity = largest_component_ratio(&healed);
            let rebuilt_connectivity = largest_component_ratio(&rebuilt);

            assert!(
                healed_connectivity >= 0.95,
                "delete_ratio={ratio:.2}: healed connectivity too low: {healed_connectivity:.3}"
            );
            assert!(
                healed_connectivity + 0.05 >= rebuilt_connectivity,
                "delete_ratio={ratio:.2}: healed connectivity regressed too much: healed={healed_connectivity:.3}, rebuilt={rebuilt_connectivity:.3}"
            );
            assert!(
                healed_recall + 0.08 >= rebuilt_recall,
                "delete_ratio={ratio:.2}: healed recall regressed too much: healed={healed_recall:.3}, rebuilt={rebuilt_recall:.3}"
            );
        }
    }
}
