// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Persistence
//!
//! Save, load, serialize, and search HNSW indexes.

use super::entry_points::EntryPoints;
use super::graph::GraphLayers;
use super::graph_links::GraphLinks;
use super::search_context::FixedLengthPriorityQueue;
use super::vector_storage::VectorStorage;
use super::visited_pool::VisitedPool;
use super::{
    BatchScorer, DistanceMetric, GraphLayersBuilder, HnswBuildStopCheck, HnswConfig,
    HnswSearchMode, PreparedQuery, ScoredPoint, SearchAlgorithm, SearchParams, VectorScorer,
};
use crate::statistics::{
    append_stats_trailer, HnswBatchTelemetry, HnswIndexStatistics, SearchTelemetry,
};
use paro_common::error;
use paro_common::error::Result;
use roaring::RoaringBitmap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// A high-level HNSW index structure that combines graph and storage.
pub struct HnswIndex {
    pub config: HnswConfig,
    pub graph: GraphLayers,
    pub vector_storage: Arc<dyn VectorStorage>,
    pub distance: DistanceMetric,
    single_telemetry: Mutex<SearchTelemetry>,
    batch_telemetry: Mutex<HnswBatchTelemetry>,
}

impl HnswIndex {
    pub fn new(
        config: HnswConfig,
        graph: GraphLayers,
        vector_storage: Arc<dyn VectorStorage>,
        distance: DistanceMetric,
    ) -> Self {
        Self {
            config,
            graph,
            vector_storage,
            distance,
            single_telemetry: Mutex::new(SearchTelemetry::default()),
            batch_telemetry: Mutex::new(HnswBatchTelemetry::default()),
        }
    }

    /// Build a new HNSW index from scratch.
    pub fn build(
        storage: Arc<dyn VectorStorage>,
        config: HnswConfig,
        distance: DistanceMetric,
    ) -> Self {
        Self::build_with_controls(storage, config, distance, None)
            .expect("HnswIndex::build without stop-check should not fail")
    }

    pub(crate) fn build_with_controls(
        storage: Arc<dyn VectorStorage>,
        config: HnswConfig,
        distance: DistanceMetric,
        stop_check: Option<&HnswBuildStopCheck>,
    ) -> Result<Self> {
        let num_vectors = storage.num_vectors();
        // Diverse neighbor selection is required for clustered vector sets;
        // nearest-only truncation forms disconnected local components.
        let mut builder = GraphLayersBuilder::new_parallel(num_vectors, &config, true);

        // Pre-allocate levels for all points.
        for i in 0..num_vectors {
            if i % 1024 == 0 && stop_check.is_some_and(|check| check.should_stop()) {
                return Err(error::query_canceled());
            }
            let level = builder.get_random_layer();
            builder.set_levels(i as u32, level);
        }

        // Heuristic insertion mutates existing neighbors. Keeping a single
        // publication order is the correctness baseline; a future parallel
        // builder must compute proposals against a frozen topology and publish
        // them behind a barrier instead of exposing half-built waves.
        for i in 0..num_vectors {
            if stop_check.is_some_and(|check| check.should_stop()) {
                return Err(error::query_canceled());
            }
            builder.link_new_point(
                i as u32,
                storage.get_vector(i as u32),
                storage.as_ref(),
                distance,
            );
        }

        let (links, entry_points) = builder.into_graph_data();
        let graph = GraphLayers::new(links, entry_points, VisitedPool::new(), (&config).into());
        Ok(Self::new(config, graph, storage, distance))
    }

    /// Save HNSW index to a directory.
    pub fn save(&self, directory: &Path) -> Result<()> {
        if !directory.exists() {
            fs::create_dir_all(directory).map_err(error::io)?;
        }

        // Save config as JSON.
        let config_path = directory.join("config.json");
        let config_json = serde_json::to_string_pretty(&self.config)
            .map_err(|e| error::serialization_error(e.to_string()))?;
        fs::write(config_path, config_json).map_err(error::io)?;

        // Save entry points as JSON.
        let entry_points_path = directory.join("entry_points.json");
        let entry_points_json = serde_json::to_string_pretty(&self.graph.entry_points)
            .map_err(|e| error::serialization_error(e.to_string()))?;
        fs::write(entry_points_path, entry_points_json).map_err(error::io)?;

        // Save graph links in binary form.
        let links_path = directory.join("graph_links.bin");
        self.graph.links.save(&links_path)?;

        Ok(())
    }

    /// Load HNSW index from a directory.
    pub fn load(
        directory: &Path,
        vector_storage: Arc<dyn VectorStorage>,
        distance: DistanceMetric,
    ) -> Result<Self> {
        // Load config.
        let config_path = directory.join("config.json");
        let config_json = fs::read_to_string(config_path).map_err(error::io)?;
        let config: HnswConfig = serde_json::from_str(&config_json)
            .map_err(|e| error::serialization_error(e.to_string()))?;

        // Load entry points.
        let entry_points_path = directory.join("entry_points.json");
        let entry_points_json = fs::read_to_string(entry_points_path).map_err(error::io)?;
        let entry_points: EntryPoints = serde_json::from_str(&entry_points_json)
            .map_err(|e| error::serialization_error(e.to_string()))?;

        // Load graph links.
        let links_path = directory.join("graph_links.bin");
        let links =
            GraphLinks::load_mmap(&links_path).or_else(|_| GraphLinks::load(&links_path))?;

        let graph = GraphLayers::new(links, entry_points, VisitedPool::new(), (&config).into());

        Ok(Self::new(config, graph, vector_storage, distance))
    }

    /// Serialize HNSW index to a byte vector for embedding in segments.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut data = Vec::new();

        // Serialize distance metric.
        data.push(match self.distance {
            DistanceMetric::Euclidean => 0,
            DistanceMetric::Cosine => 1,
            DistanceMetric::DotProduct => 2,
            DistanceMetric::Manhattan => 3,
        });

        // Serialize config as JSON.
        let config_json = serde_json::to_vec(&self.config)
            .map_err(|e| error::serialization_error(e.to_string()))?;
        data.extend_from_slice(&(config_json.len() as u32).to_le_bytes());
        data.extend_from_slice(&config_json);

        // Serialize entry points as JSON.
        let entry_points_json = serde_json::to_vec(&self.graph.entry_points)
            .map_err(|e| error::serialization_error(e.to_string()))?;
        data.extend_from_slice(&(entry_points_json.len() as u32).to_le_bytes());
        data.extend_from_slice(&entry_points_json);

        // Serialize graph links in binary form.
        self.graph.links.serialize(&mut data)?;

        // Append statistics trailer.
        let stats = HnswIndexStatistics::collect(self);
        append_stats_trailer(&mut data, &stats.to_bytes())?;

        Ok(data)
    }

    /// Deserialize HNSW index from a byte buffer.
    pub fn deserialize(data: &[u8], vector_storage: Arc<dyn VectorStorage>) -> Result<Self> {
        let mut offset = 0;

        // Deserialize distance metric.
        let distance_byte = data[offset];
        offset += 1;
        let distance = match distance_byte {
            0 => DistanceMetric::Euclidean,
            1 => DistanceMetric::Cosine,
            2 => DistanceMetric::DotProduct,
            3 => DistanceMetric::Manhattan,
            _ => return Err(error::serialization_error("Invalid distance metric byte")),
        };

        // Deserialize config.
        let config_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let config: HnswConfig = serde_json::from_slice(&data[offset..offset + config_len])
            .map_err(|e| error::serialization_error(e.to_string()))?;
        offset += config_len;

        // Deserialize entry points.
        let entry_points_len =
            u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let entry_points: EntryPoints =
            serde_json::from_slice(&data[offset..offset + entry_points_len])
                .map_err(|e| error::serialization_error(e.to_string()))?;
        offset += entry_points_len;

        // Deserialize graph links.
        let links = GraphLinks::deserialize(&data[offset..])?;

        let graph = GraphLayers::new(links, entry_points, VisitedPool::new(), (&config).into());

        Ok(Self::new(config, graph, vector_storage, distance))
    }

    /// Perform a vector search using this index.
    pub fn search_one(
        &self,
        query: &[f32],
        top_k: usize,
        params: &SearchParams,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> Result<Vec<ScoredPoint>> {
        self.search_one_with_mode(query, top_k, params, filter_bitmap, HnswSearchMode::Auto)
    }

    pub(crate) fn search_one_with_mode(
        &self,
        query: &[f32],
        top_k: usize,
        params: &SearchParams,
        filter_bitmap: Option<&RoaringBitmap>,
        mode: HnswSearchMode,
    ) -> Result<Vec<ScoredPoint>> {
        if top_k == 0 {
            return Ok(Vec::new());
        }

        let start = Instant::now();
        let pre_filter_count = self.graph.num_points() as u64;
        let post_filter_count = filter_bitmap.map(|bm| bm.len()).unwrap_or(pre_filter_count);

        let prepared_query = self.distance.prepare(query);
        let mut scorer = VectorScorer::new(&prepared_query, self.vector_storage.as_ref());
        let results = if self.should_use_plain_scan(filter_bitmap, mode) {
            self.plain_scan(top_k, &mut scorer, filter_bitmap)
        } else {
            let algorithm = self.choose_algorithm(params, filter_bitmap);
            let ef = params.ef.unwrap_or(self.config.ef);
            self.graph.search_one(
                top_k,
                ef,
                algorithm,
                &mut scorer,
                filter_bitmap,
                self.should_use_random_entry_point(params, filter_bitmap),
            )
        };

        let elapsed_us = start.elapsed().as_micros() as u64;
        self.single_telemetry.lock().unwrap().record(
            elapsed_us,
            pre_filter_count,
            post_filter_count,
        );

        Ok(results)
    }

    /// Perform batched vector search using one shared filter bitmap.
    pub fn search_many_prepared(
        &self,
        queries: &[PreparedQuery],
        top_k: usize,
        params: &SearchParams,
        filter_bitmap: Option<&RoaringBitmap>,
        mode: HnswSearchMode,
    ) -> Result<Vec<Vec<ScoredPoint>>> {
        if queries.is_empty() {
            return Ok(Vec::new());
        }
        if top_k == 0 {
            return Ok(vec![Vec::new(); queries.len()]);
        }

        self.validate_prepared_queries(queries)?;

        let start = Instant::now();
        let mut scorers: Vec<_> = queries
            .iter()
            .map(|query| VectorScorer::new(query, self.vector_storage.as_ref()))
            .collect();

        let results = if self.should_use_plain_scan(filter_bitmap, mode) {
            let batch_scorer = BatchScorer::new(scorers, top_k);
            let num_points = self.graph.num_points() as u32;
            match filter_bitmap {
                Some(bitmap) => batch_scorer.scan(bitmap.iter().filter(|&idx| idx < num_points)),
                None => batch_scorer.scan(0..num_points),
            }
        } else {
            let algorithm = self.choose_algorithm(params, filter_bitmap);
            let ef = params.ef.unwrap_or(self.config.ef);
            self.graph.search_many(
                top_k,
                ef,
                algorithm,
                &mut scorers,
                filter_bitmap,
                self.should_use_random_entry_point(params, filter_bitmap),
            )
        };

        let elapsed_us = start.elapsed().as_micros() as u64;
        self.batch_telemetry
            .lock()
            .unwrap()
            .record_batch(elapsed_us, queries.len());

        Ok(results)
    }

    /// Snapshot single-query search telemetry.
    pub fn search_telemetry(&self) -> SearchTelemetry {
        self.single_telemetry.lock().unwrap().clone()
    }

    /// Snapshot batched search telemetry.
    pub fn batch_search_telemetry(&self) -> HnswBatchTelemetry {
        self.batch_telemetry.lock().unwrap().clone()
    }

    fn choose_algorithm(
        &self,
        params: &SearchParams,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> SearchAlgorithm {
        if let (Some(acorn), Some(bitmap)) = (params.acorn, filter_bitmap) {
            if acorn.enable && self.config.m0 != 0 {
                let selectivity = bitmap.len() as f64 / self.graph.links.num_points() as f64;
                if selectivity
                    <= acorn
                        .max_selectivity
                        .unwrap_or(super::ACORN_MAX_SELECTIVITY_DEFAULT)
                {
                    return SearchAlgorithm::Acorn;
                }
            }
        }
        SearchAlgorithm::Hnsw
    }

    fn should_use_random_entry_point(
        &self,
        params: &SearchParams,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> bool {
        params.random_entry_point.unwrap_or(filter_bitmap.is_some())
    }

    fn validate_prepared_queries(&self, queries: &[PreparedQuery]) -> Result<()> {
        let expected_dim = self.vector_storage.vector_dim();
        for (idx, query) in queries.iter().enumerate() {
            if query.metric() != self.distance {
                return Err(error::invalid_input(format!(
                    "query[{idx}] prepared with {:?}, but index uses {:?}",
                    query.metric(),
                    self.distance
                )));
            }
            if query.as_slice().len() != expected_dim {
                return Err(error::invalid_input(format!(
                    "query[{idx}] dimension mismatch: expected {expected_dim}, got {}",
                    query.as_slice().len()
                )));
            }
        }
        Ok(())
    }

    fn should_use_plain_scan(
        &self,
        filter_bitmap: Option<&RoaringBitmap>,
        mode: HnswSearchMode,
    ) -> bool {
        match mode {
            HnswSearchMode::Exact => return true,
            HnswSearchMode::Graph => return false,
            HnswSearchMode::Auto => {}
        }
        let num_points = self.graph.num_points();
        if num_points <= self.config.plain_scan_threshold {
            return true;
        }

        let Some(bitmap) = filter_bitmap else {
            return false;
        };
        let candidate_count = bitmap
            .iter()
            .take_while(|&idx| idx < num_points as u32)
            .count();
        candidate_count <= self.config.filtered_plain_scan_threshold
    }

    fn plain_scan(
        &self,
        top_k: usize,
        scorer: &mut VectorScorer,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> Vec<ScoredPoint> {
        let mut best = FixedLengthPriorityQueue::new(top_k);
        let num_points = self.graph.num_points() as u32;

        match filter_bitmap {
            Some(bitmap) => {
                for idx in bitmap.iter() {
                    if idx >= num_points {
                        continue;
                    }
                    let score = scorer.score_point(idx);
                    best.push(ScoredPoint { idx, score });
                }
            }
            None => {
                for idx in 0..num_points {
                    let score = scorer.score_point(idx);
                    best.push(ScoredPoint { idx, score });
                }
            }
        }

        best.into_sorted_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::builder::DistanceProfileSnapshot;
    use crate::index::hnsw::{AcornParams, InMemoryVectorStorage, PointOffset};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use rayon::prelude::{IntoParallelIterator, ParallelIterator};
    use roaring::RoaringBitmap;
    use std::cmp::max;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn make_storage(vectors: &[Vec<f32>]) -> Arc<InMemoryVectorStorage> {
        let dim = vectors.first().map(|v| v.len()).unwrap_or(0);
        let mut flat = Vec::with_capacity(vectors.len() * dim);
        for v in vectors {
            assert_eq!(v.len(), dim);
            flat.extend_from_slice(v);
        }
        Arc::new(InMemoryVectorStorage::new(flat, dim))
    }

    fn prepare_queries(distance: DistanceMetric, queries: &[Vec<f32>]) -> Vec<PreparedQuery> {
        queries
            .iter()
            .map(|query| distance.prepare(query))
            .collect()
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
        use_heuristic: bool,
        parallel: bool,
    ) -> HnswIndex {
        build_index_with_levels_and_cache(
            vectors,
            levels,
            config,
            distance,
            use_heuristic,
            parallel,
            0,
            false,
        )
        .0
    }

    fn build_index_with_levels_and_cache(
        vectors: &[Vec<f32>],
        levels: &[usize],
        config: HnswConfig,
        distance: DistanceMetric,
        use_heuristic: bool,
        parallel: bool,
        distance_cache_slots: usize,
        profile_distances: bool,
    ) -> (HnswIndex, DistanceProfileSnapshot) {
        assert_eq!(vectors.len(), levels.len());

        let storage: Arc<dyn VectorStorage> = make_storage(vectors);
        let mut builder = GraphLayersBuilder::new_parallel(vectors.len(), &config, use_heuristic);
        builder.set_distance_cache_slots_for_benchmark(distance_cache_slots);
        builder.set_distance_profile_enabled_for_benchmark(profile_distances);

        for (idx, level) in levels.iter().copied().enumerate() {
            builder.set_levels(idx as u32, level);
        }

        if parallel {
            const WARM_START_POINTS: usize = 256;
            let warm_start_end = vectors.len().min(WARM_START_POINTS);
            for i in 0..warm_start_end {
                builder.link_new_point(
                    i as u32,
                    storage.get_vector(i as u32),
                    storage.as_ref(),
                    distance,
                );
            }

            if warm_start_end < vectors.len() {
                let builder_ref = &builder;
                let storage_ref = storage.as_ref();
                (warm_start_end..vectors.len())
                    .into_par_iter()
                    .for_each(|i| {
                        builder_ref.link_new_point(
                            i as u32,
                            storage_ref.get_vector(i as u32),
                            storage_ref,
                            distance,
                        );
                    });
            }
        } else {
            for i in 0..vectors.len() {
                builder.link_new_point(
                    i as u32,
                    storage.get_vector(i as u32),
                    storage.as_ref(),
                    distance,
                );
            }
        }

        let profile = builder.distance_profile_snapshot();
        let (links, entry_points) = builder.into_graph_data();
        let graph = GraphLayers::new(links, entry_points, VisitedPool::new(), (&config).into());
        (HnswIndex::new(config, graph, storage, distance), profile)
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

    fn average_overlap_at_k(
        lhs: &HnswIndex,
        rhs: &HnswIndex,
        queries: &[Vec<f32>],
        top_k: usize,
        search_params: &SearchParams,
    ) -> f32 {
        let mut total_overlap = 0.0f32;
        for query in queries {
            let lhs_result = lhs.search_one(query, top_k, search_params, None).unwrap();
            let rhs_result = rhs.search_one(query, top_k, search_params, None).unwrap();
            let overlap = lhs_result
                .iter()
                .filter(|point| rhs_result.iter().any(|other| other.idx == point.idx))
                .count();
            total_overlap += overlap as f32 / top_k as f32;
        }
        total_overlap / queries.len() as f32
    }

    fn assert_scored_points_exact(lhs: &[ScoredPoint], rhs: &[ScoredPoint]) {
        assert_eq!(lhs.len(), rhs.len());
        for (left, right) in lhs.iter().zip(rhs.iter()) {
            assert_eq!(left.idx, right.idx);
            assert!(
                (left.score - right.score).abs() <= 1e-6,
                "score mismatch for idx {}: left={}, right={}",
                left.idx,
                left.score,
                right.score
            );
        }
    }

    fn dominated_neighbor_ratio(
        index: &HnswIndex,
        vectors: &[Vec<f32>],
        distance: DistanceMetric,
    ) -> f32 {
        let mut dominated = 0usize;
        let mut total = 0usize;

        for point_id in 0..index.graph.links.num_points() as PointOffset {
            let mut neighbors = Vec::new();
            index
                .graph
                .links
                .for_each_link(point_id, 0, |neighbor| neighbors.push(neighbor));

            for (candidate_pos, &candidate) in neighbors.iter().enumerate() {
                total += 1;
                let candidate_score =
                    distance.similarity(&vectors[point_id as usize], &vectors[candidate as usize]);
                for (other_pos, &other) in neighbors.iter().enumerate() {
                    if candidate_pos == other_pos {
                        continue;
                    }
                    if distance.similarity(&vectors[candidate as usize], &vectors[other as usize])
                        > candidate_score
                    {
                        dominated += 1;
                        break;
                    }
                }
            }
        }

        dominated as f32 / total.max(1) as f32
    }

    #[test]
    fn test_hnsw_build() {
        let storage = make_storage(&[
            vec![0.0, 0.0],
            vec![1.0, 1.0],
            vec![2.0, 2.0],
            vec![3.0, 3.0],
        ]);
        let config = HnswConfig::new(8, 50).with_plain_scan_threshold(0);
        let index = HnswIndex::build(storage, config, DistanceMetric::DotProduct);

        assert_eq!(index.graph.num_points(), 4);
        assert!(!index.graph.entry_points.entry_points.is_empty());
    }

    #[test]
    fn build_is_byte_deterministic_for_same_seed() {
        let vectors = make_sift_like_vectors(0xabc, 256, 16, 12);
        let config = HnswConfig::new(12, 72)
            .with_plain_scan_threshold(0)
            .with_build_seed(0x1234_5678_9abc_def0)
            .with_build_random_entry_point(true);
        let first = HnswIndex::build(make_storage(&vectors), config, DistanceMetric::Euclidean)
            .serialize()
            .unwrap();
        let second = HnswIndex::build(make_storage(&vectors), config, DistanceMetric::Euclidean)
            .serialize()
            .unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn test_hnsw_search() {
        let storage = make_storage(&[vec![0.0], vec![1.0]]);
        let config = HnswConfig::new(8, 50)
            .with_plain_scan_threshold(0)
            .with_ef(50);
        let index = HnswIndex::build(storage, config, DistanceMetric::DotProduct);

        let params = SearchParams {
            ef: Some(50),
            ..Default::default()
        };
        let result = index.search_one(&[1.0], 1, &params, None).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].idx, 1);
    }

    #[test]
    #[serial_test::serial]
    fn test_hnsw_search_many_matches_search_one_hnsw_path() {
        let vectors = make_sift_like_vectors(7, 384, 24, 16);
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(16, 96)
            .with_plain_scan_threshold(0)
            .with_ef(96);
        let index = HnswIndex::build(storage, config, DistanceMetric::Euclidean);

        let mut filter = RoaringBitmap::new();
        for idx in 0..vectors.len() as u32 {
            if idx % 3 != 0 {
                filter.insert(idx);
            }
        }
        for entry in &index.graph.entry_points.entry_points {
            filter.insert(entry.point_id);
        }
        for entry in &index.graph.entry_points.extra_entry_points {
            filter.insert(entry.point_id);
        }

        let queries = make_sift_like_queries(77, &vectors, 8, 0.02);
        let prepared_queries = prepare_queries(DistanceMetric::Euclidean, &queries);
        let params = SearchParams {
            ef: Some(96),
            random_entry_point: Some(false),
            ..Default::default()
        };
        let top_k = 12;

        let batch = index
            .search_many_prepared(
                &prepared_queries,
                top_k,
                &params,
                Some(&filter),
                HnswSearchMode::Graph,
            )
            .unwrap();
        assert_eq!(batch.len(), queries.len());

        for (batch_result, query) in batch.iter().zip(queries.iter()) {
            let single = index
                .search_one(query, top_k, &params, Some(&filter))
                .unwrap();
            assert_scored_points_exact(batch_result, &single);
        }
    }

    #[test]
    fn test_hnsw_search_many_matches_search_one_full_scan_path() {
        let vectors = make_sift_like_vectors(9, 96, 12, 8);
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(8, 32)
            .with_plain_scan_threshold(10_000)
            .with_ef(64);
        let index = HnswIndex::build(storage, config, DistanceMetric::DotProduct);

        let queries = make_sift_like_queries(11, &vectors, 6, 0.01);
        let prepared_queries = prepare_queries(DistanceMetric::DotProduct, &queries);
        let params = SearchParams {
            ef: Some(64),
            ..Default::default()
        };
        let top_k = 10;

        let batch = index
            .search_many_prepared(
                &prepared_queries,
                top_k,
                &params,
                None,
                HnswSearchMode::Exact,
            )
            .unwrap();
        assert_eq!(batch.len(), queries.len());

        for (batch_result, query) in batch.iter().zip(queries.iter()) {
            let single = index.search_one(query, top_k, &params, None).unwrap();
            assert_scored_points_exact(batch_result, &single);
        }
    }

    #[test]
    fn test_hnsw_search_many_matches_search_one_full_scan_path_with_filter() {
        let vectors = make_sift_like_vectors(13, 120, 12, 8);
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(8, 32)
            .with_plain_scan_threshold(10_000)
            .with_ef(64);
        let index = HnswIndex::build(storage, config, DistanceMetric::DotProduct);

        let mut filter = RoaringBitmap::new();
        for idx in 0..vectors.len() as u32 {
            if idx % 2 == 1 {
                filter.insert(idx);
            }
        }

        let queries = make_sift_like_queries(19, &vectors, 6, 0.01);
        let prepared_queries = prepare_queries(DistanceMetric::DotProduct, &queries);
        let params = SearchParams {
            ef: Some(64),
            ..Default::default()
        };
        let top_k = 10;

        let batch = index
            .search_many_prepared(
                &prepared_queries,
                top_k,
                &params,
                Some(&filter),
                HnswSearchMode::Exact,
            )
            .unwrap();
        assert_eq!(batch.len(), queries.len());

        for (batch_result, query) in batch.iter().zip(queries.iter()) {
            let single = index
                .search_one(query, top_k, &params, Some(&filter))
                .unwrap();
            assert_scored_points_exact(batch_result, &single);
        }
    }

    #[test]
    fn test_hnsw_search_many_batch_size_one_matches_search_one() {
        let vectors = make_sift_like_vectors(17, 192, 16, 12);
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(16, 96)
            .with_plain_scan_threshold(0)
            .with_ef(96);
        let index = HnswIndex::build(storage, config, DistanceMetric::Euclidean);

        let query = make_sift_like_queries(23, &vectors, 1, 0.02)
            .into_iter()
            .next()
            .unwrap();
        let prepared_queries = vec![DistanceMetric::Euclidean.prepare(&query)];
        let params = SearchParams {
            ef: Some(96),
            random_entry_point: Some(false),
            ..Default::default()
        };
        let top_k = 8;

        let batch = index
            .search_many_prepared(
                &prepared_queries,
                top_k,
                &params,
                None,
                HnswSearchMode::Graph,
            )
            .unwrap();
        let single = index.search_one(&query, top_k, &params, None).unwrap();

        assert_eq!(batch.len(), 1);
        assert_scored_points_exact(&batch[0], &single);
    }

    #[test]
    fn test_hnsw_acorn_search() {
        let vectors: Vec<Vec<f32>> = (0..10).map(|i| vec![i as f32]).collect();
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(8, 50)
            .with_plain_scan_threshold(0)
            .with_ef(50);
        let index = HnswIndex::build(storage, config, DistanceMetric::DotProduct);

        let entry_id = index.graph.entry_points.entry_points[0].point_id;
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(entry_id);

        let params = SearchParams {
            ef: Some(50),
            acorn: Some(AcornParams {
                enable: true,
                max_selectivity: Some(0.4),
            }),
            random_entry_point: None,
        };
        let result = index.search_one(&[0.0], 1, &params, Some(&bitmap)).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].idx, entry_id);
    }

    #[test]
    fn test_hnsw_search_many_matches_search_one_acorn_path() {
        let vectors = make_sift_like_vectors(29, 320, 20, 12);
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(16, 96)
            .with_plain_scan_threshold(0)
            .with_ef(96);
        let index = HnswIndex::build(storage, config, DistanceMetric::Euclidean);

        let mut filter = RoaringBitmap::new();
        for idx in 0..vectors.len() as u32 {
            if idx % 4 == 0 {
                filter.insert(idx);
            }
        }
        for entry in &index.graph.entry_points.entry_points {
            filter.insert(entry.point_id);
        }
        for entry in &index.graph.entry_points.extra_entry_points {
            filter.insert(entry.point_id);
        }

        let queries = make_sift_like_queries(31, &vectors, 5, 0.03);
        let prepared_queries = prepare_queries(DistanceMetric::Euclidean, &queries);
        let params = SearchParams {
            ef: Some(96),
            acorn: Some(AcornParams {
                enable: true,
                max_selectivity: Some(0.5),
            }),
            random_entry_point: Some(false),
        };
        let top_k = 10;

        let batch = index
            .search_many_prepared(
                &prepared_queries,
                top_k,
                &params,
                Some(&filter),
                HnswSearchMode::Graph,
            )
            .unwrap();
        assert_eq!(batch.len(), queries.len());

        for (batch_result, query) in batch.iter().zip(queries.iter()) {
            let single = index
                .search_one(query, top_k, &params, Some(&filter))
                .unwrap();
            assert_scored_points_exact(batch_result, &single);
        }
    }

    #[test]
    fn test_random_entry_point_search_params_default_and_override() {
        let vectors: Vec<Vec<f32>> = (0..8).map(|i| vec![i as f32]).collect();
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(8, 32).with_plain_scan_threshold(0);
        let index = HnswIndex::build(storage, config, DistanceMetric::DotProduct);

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(0);

        assert!(!index.should_use_random_entry_point(&SearchParams::default(), None));
        assert!(index.should_use_random_entry_point(&SearchParams::default(), Some(&bitmap)));
        assert!(index.should_use_random_entry_point(
            &SearchParams {
                random_entry_point: Some(true),
                ..Default::default()
            },
            None
        ));
        assert!(!index.should_use_random_entry_point(
            &SearchParams {
                random_entry_point: Some(false),
                ..Default::default()
            },
            Some(&bitmap)
        ));
    }

    #[test]
    fn test_hnsw_with_delete() {
        let vectors: Vec<Vec<f32>> = (0..5).map(|i| vec![i as f32]).collect();
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(8, 50).with_plain_scan_threshold(100);
        let index = HnswIndex::build(storage, config, DistanceMetric::DotProduct);

        let mut all = RoaringBitmap::new();
        all.insert_range(0..5);
        all.remove(3);

        let params = SearchParams {
            ef: Some(10),
            ..Default::default()
        };
        let results = index.search_one(&[10.0], 5, &params, Some(&all)).unwrap();
        assert!(results.iter().all(|p| p.idx != 3));
    }

    #[test]
    fn test_hnsw_persistence() {
        let vectors: Vec<Vec<f32>> = (0..6).map(|i| vec![i as f32]).collect();
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(8, 50).with_plain_scan_threshold(100);
        let index = HnswIndex::build(storage.clone(), config, DistanceMetric::DotProduct);

        let params = SearchParams {
            ef: Some(10),
            ..Default::default()
        };
        let before = index.search_one(&[5.0], 1, &params, None).unwrap();

        let data = index.serialize().unwrap();
        let loaded = HnswIndex::deserialize(&data, storage).unwrap();
        let after = loaded.search_one(&[5.0], 1, &params, None).unwrap();

        assert_eq!(before[0].idx, after[0].idx);
    }

    #[test]
    fn test_hnsw_batch_telemetry_is_separate_from_single_query_telemetry() {
        let storage = make_storage(&[vec![0.0], vec![1.0], vec![2.0], vec![3.0]]);
        let config = HnswConfig::new(8, 32).with_plain_scan_threshold(100);
        let index = HnswIndex::build(storage, config, DistanceMetric::DotProduct);
        let params = SearchParams::default();

        let _ = index.search_one(&[1.0], 2, &params, None).unwrap();
        let _ = index.search_one(&[2.0], 2, &params, None).unwrap();

        let queries = vec![
            DistanceMetric::DotProduct.prepare(&[0.0]),
            DistanceMetric::DotProduct.prepare(&[1.0]),
            DistanceMetric::DotProduct.prepare(&[3.0]),
        ];
        let _ = index
            .search_many_prepared(&queries, 2, &params, None, HnswSearchMode::Exact)
            .unwrap();

        let single = index.search_telemetry();
        assert_eq!(single.search_count, 2);
        assert_eq!(single.pre_filter_count, 8);
        assert_eq!(single.post_filter_count, 8);

        let batch = index.batch_search_telemetry();
        assert_eq!(batch.batch_search_count, 1);
        assert_eq!(batch.batched_query_count, 3);
        assert_eq!(batch.batch_size_histogram, vec![0, 0, 1, 0, 0, 0, 0]);
    }

    #[test]
    fn test_search_many_prepared_rejects_metric_mismatch() {
        let storage = make_storage(&[vec![0.0, 0.0], vec![1.0, 1.0]]);
        let config = HnswConfig::new(8, 32).with_plain_scan_threshold(100);
        let index = HnswIndex::build(storage, config, DistanceMetric::DotProduct);

        let error = index
            .search_many_prepared(
                &[DistanceMetric::Cosine.prepare(&[1.0, 0.0])],
                1,
                &SearchParams::default(),
                None,
                HnswSearchMode::Exact,
            )
            .unwrap_err();

        assert!(
            error.to_string().contains("prepared with"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_search_many_prepared_rejects_dimension_mismatch() {
        let storage = make_storage(&[vec![0.0, 0.0], vec![1.0, 1.0]]);
        let config = HnswConfig::new(8, 32).with_plain_scan_threshold(100);
        let index = HnswIndex::build(storage, config, DistanceMetric::DotProduct);

        let error = index
            .search_many_prepared(
                &[PreparedQuery::new(
                    vec![1.0, 0.0, 0.0],
                    DistanceMetric::DotProduct,
                )],
                1,
                &SearchParams::default(),
                None,
                HnswSearchMode::Exact,
            )
            .unwrap_err();

        assert!(
            error.to_string().contains("dimension mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_filter_aware_plain_scan_uses_candidate_count() {
        let vectors = make_sift_like_vectors(41, 256, 16, 12);
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(16, 96)
            .with_plain_scan_threshold(0)
            .with_filtered_plain_scan_threshold(12)
            .with_ef(96);
        let index = HnswIndex::build(storage, config, DistanceMetric::Euclidean);

        let mut small_filter = RoaringBitmap::new();
        for idx in (0..vectors.len() as u32).step_by(32) {
            small_filter.insert(idx);
        }

        let mut large_filter = small_filter.clone();
        for idx in (1..vectors.len() as u32).step_by(8) {
            large_filter.insert(idx);
        }

        assert!(!index.should_use_plain_scan(None, HnswSearchMode::Auto));
        assert!(index.should_use_plain_scan(Some(&small_filter), HnswSearchMode::Auto));
        assert!(!index.should_use_plain_scan(Some(&large_filter), HnswSearchMode::Auto));
    }

    #[test]
    fn test_search_many_prepared_matches_search_one_for_large_segment_strong_filter() {
        let vectors = make_sift_like_vectors(43, 320, 20, 16);
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(16, 96)
            .with_plain_scan_threshold(0)
            .with_filtered_plain_scan_threshold(16)
            .with_ef(96);
        let index = HnswIndex::build(storage, config, DistanceMetric::Euclidean);

        let mut filter = RoaringBitmap::new();
        for idx in (0..vectors.len() as u32).step_by(40) {
            filter.insert(idx);
        }

        let queries = make_sift_like_queries(47, &vectors, 4, 0.02);
        let prepared_queries = prepare_queries(DistanceMetric::Euclidean, &queries);
        let params = SearchParams {
            ef: Some(96),
            random_entry_point: Some(false),
            ..Default::default()
        };
        let top_k = 6;

        let batch = index
            .search_many_prepared(
                &prepared_queries,
                top_k,
                &params,
                Some(&filter),
                HnswSearchMode::Exact,
            )
            .unwrap();
        assert_eq!(batch.len(), queries.len());

        for (batch_result, query) in batch.iter().zip(queries.iter()) {
            let single = index
                .search_one(query, top_k, &params, Some(&filter))
                .unwrap();
            assert_scored_points_exact(batch_result, &single);
        }
    }

    #[test]
    fn test_hnsw_directory_load_uses_mmap_graph_links() {
        let vectors: Vec<Vec<f32>> = (0..32).map(|i| vec![i as f32, (i % 7) as f32]).collect();
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(8, 50).with_plain_scan_threshold(0);
        let index = HnswIndex::build(storage.clone(), config, DistanceMetric::DotProduct);

        let temp_dir = TempDir::new().unwrap();
        index.save(temp_dir.path()).unwrap();

        let loaded = HnswIndex::load(temp_dir.path(), storage.clone(), DistanceMetric::DotProduct)
            .expect("load index from directory");
        assert!(loaded.graph.links.is_mmap_backed());

        let params = SearchParams {
            ef: Some(32),
            ..Default::default()
        };
        let before = index.search_one(&[31.0, 3.0], 5, &params, None).unwrap();
        let after = loaded.search_one(&[31.0, 3.0], 5, &params, None).unwrap();
        assert_eq!(before.len(), after.len());
        assert_eq!(before[0].idx, after[0].idx);
    }

    #[test]
    fn test_hnsw_build_parallel_over_warm_start() {
        // Use enough vectors to exercise the parallel build path after the serial prefix.
        let vectors: Vec<Vec<f32>> = (0..300).map(|i| vec![i as f32, (i % 7) as f32]).collect();
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(8, 50)
            .with_plain_scan_threshold(0)
            .with_ef(64);
        let index = HnswIndex::build(storage, config, DistanceMetric::DotProduct);

        assert_eq!(index.graph.num_points(), 300);
        assert!(!index.graph.entry_points.entry_points.is_empty());
    }

    #[test]
    fn test_heuristic_reduces_dominated_links() {
        let num_vectors = 512;
        let dim = 64;
        let vectors = make_sift_like_vectors(7, num_vectors, dim, 32);
        let config = HnswConfig::new(8, 64)
            .with_plain_scan_threshold(0)
            .with_ef(96);
        let levels = deterministic_levels(num_vectors, config.m, 99);

        let no_heuristic = build_index_with_levels(
            &vectors,
            &levels,
            config,
            DistanceMetric::Euclidean,
            false,
            false,
        );
        let with_heuristic = build_index_with_levels(
            &vectors,
            &levels,
            config,
            DistanceMetric::Euclidean,
            true,
            false,
        );

        let no_heuristic_ratio =
            dominated_neighbor_ratio(&no_heuristic, &vectors, DistanceMetric::Euclidean);
        let with_heuristic_ratio =
            dominated_neighbor_ratio(&with_heuristic, &vectors, DistanceMetric::Euclidean);

        assert!(
            with_heuristic_ratio < no_heuristic_ratio,
            "heuristic should reduce dominated links: with={with_heuristic_ratio:.4}, without={no_heuristic_ratio:.4}"
        );
    }

    #[test]
    fn test_parallel_build_matches_serial_build_quality() {
        let num_vectors = 640;
        let dim = 64;
        let vectors = make_sift_like_vectors(11, num_vectors, dim, 40);
        let config = HnswConfig::new(12, 96)
            .with_plain_scan_threshold(0)
            .with_ef(128);
        let levels = deterministic_levels(num_vectors, config.m, 123);
        let queries = make_sift_like_queries(2026, &vectors, 64, 0.02);

        let serial = build_index_with_levels(
            &vectors,
            &levels,
            config,
            DistanceMetric::Euclidean,
            true,
            false,
        );
        let parallel = build_index_with_levels(
            &vectors,
            &levels,
            config,
            DistanceMetric::Euclidean,
            true,
            true,
        );

        let search_params = SearchParams {
            ef: Some(128),
            ..Default::default()
        };
        let serial_recall = average_recall_at_k(
            &serial,
            &vectors,
            &queries,
            10,
            &search_params,
            DistanceMetric::Euclidean,
        );
        let parallel_recall = average_recall_at_k(
            &parallel,
            &vectors,
            &queries,
            10,
            &search_params,
            DistanceMetric::Euclidean,
        );
        let overlap = average_overlap_at_k(&serial, &parallel, &queries, 10, &search_params);

        assert!(
            (serial_recall - parallel_recall).abs() <= 0.03,
            "parallel and serial recall diverged too much: serial={serial_recall:.3}, parallel={parallel_recall:.3}"
        );
        assert!(
            overlap >= 0.90,
            "parallel and serial top-k overlap too low: overlap={overlap:.3}"
        );
    }

    #[test]
    fn test_sift_like_recall_at_10_is_above_94_percent() {
        let num_vectors = 1200;
        let dim = 128;
        let vectors = make_sift_like_vectors(42, num_vectors, dim, 48);
        let queries = make_sift_like_queries(43, &vectors, 100, 0.015);
        let config = HnswConfig::new(16, 200)
            .with_plain_scan_threshold(0)
            .with_ef(200);
        let index = HnswIndex::build(make_storage(&vectors), config, DistanceMetric::Euclidean);

        let search_params = SearchParams {
            ef: Some(200),
            ..Default::default()
        };
        let recall = average_recall_at_k(
            &index,
            &vectors,
            &queries,
            10,
            &search_params,
            DistanceMetric::Euclidean,
        );

        // HNSW graph construction has non-determinism; 0.94 allows for acceptable variance
        assert!(
            recall >= 0.94,
            "expected recall@10 >= 0.94, got {recall:.3}"
        );
    }
}
