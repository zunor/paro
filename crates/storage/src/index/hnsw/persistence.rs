// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Persistence
//!
//! Save, load, serialize, and search HNSW indexes.

use super::entry_points::EntryPoints;
use super::graph::GraphLayers;
use super::graph_links::GraphLinks;
use super::hnsw_builder::hnsw_build_pool;
use super::search_context::FixedLengthPriorityQueue;
use super::vector_storage::{CosineInverseNorms, IndexedVectorStorage, VectorStorage};
use super::visited_pool::VisitedPool;
#[cfg(test)]
use super::HnswConfig;
use super::{
    BatchScorer, DistanceMetric, GraphLayersBuilder, HnswBuildContract, HnswBuildStopCheck,
    HnswSearchFilter, HnswSearchPolicy, HnswSearchStrategy, PointOffset, PreparedQuery,
    ScoredPoint, SearchAlgorithm, SearchParams, VectorScorer, HNSW_BUILD_CONTRACT_VERSION,
};
use crate::statistics::{
    append_stats_trailer, HnswBatchTelemetry, HnswIndexStatistics, SearchTelemetry,
};
use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};
use paro_common::error;
use paro_common::error::Result;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const HNSW_ARTIFACT_MAGIC: [u8; 4] = *b"HNSW";
const HNSW_ARTIFACT_VERSION: u32 = 3;
const HNSW_ARTIFACT_HEADER_LEN: usize = 72;

/// O(1) keyed permutation of point ids used to decouple frozen-wave membership
/// from ingest order.
///
/// A balanced Feistel network permutes the next even-bit power-of-two domain.
/// Cycle walking restricts that permutation to `[0, len)`. Unlike an affine
/// permutation, this does not preserve arithmetic progressions, so a valid but
/// unlucky key cannot turn a wave back into a cluster of adjacent ingest ids.
#[derive(Debug, Clone, Copy)]
struct DeterministicPointOrder {
    seed: u64,
    len: u64,
    half_bits: u32,
    half_mask: u32,
}

impl DeterministicPointOrder {
    fn new(len: usize, seed: u64) -> Self {
        let len = len as u64;
        if len <= 1 {
            return Self {
                seed,
                len,
                half_bits: 0,
                half_mask: 0,
            };
        }
        let domain_bits = (u64::BITS - (len - 1).leading_zeros()).next_multiple_of(2);
        let half_bits = domain_bits / 2;
        Self {
            seed,
            len,
            half_bits,
            half_mask: ((1_u64 << half_bits) - 1) as u32,
        }
    }

    fn point_at(self, position: usize) -> PointOffset {
        debug_assert!((position as u64) < self.len);
        if self.len <= 1 {
            return 0;
        }
        let mut candidate = position as u32;
        loop {
            candidate = self.permute_domain(candidate);
            if u64::from(candidate) < self.len {
                return candidate;
            }
        }
    }

    fn permute_domain(self, value: u32) -> u32 {
        let mut left = value >> self.half_bits;
        let mut right = value & self.half_mask;
        for round in 0..6_u64 {
            let round_key =
                self.seed ^ 0x4857_4e53_5756_3400 ^ round.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let mixed = splitmix64(round_key ^ u64::from(right)) as u32 & self.half_mask;
            (left, right) = (right, left ^ mixed);
        }
        (left << self.half_bits) | right
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HnswArtifactCompatibility {
    Current,
    UnsupportedArtifactVersion(u32),
    UnsupportedBuildContractVersion(u32),
}

impl HnswArtifactCompatibility {
    pub fn rebuild_reason(self) -> Option<String> {
        match self {
            Self::Current => None,
            Self::UnsupportedArtifactVersion(version) => Some(format!(
                "HNSW artifact version {version} is not queryable (runtime expects {HNSW_ARTIFACT_VERSION}); rebuild the vector index"
            )),
            Self::UnsupportedBuildContractVersion(version) => Some(format!(
                "HNSW build contract version {version} is not queryable (runtime expects {HNSW_BUILD_CONTRACT_VERSION}); rebuild the vector index"
            )),
        }
    }
}

pub fn hnsw_artifact_compatibility(data: &[u8]) -> Result<HnswArtifactCompatibility> {
    if data.len() < HNSW_ARTIFACT_MAGIC.len() {
        return Err(error::data_corrupted(
            "HNSW artifact is truncated before its magic",
        ));
    }
    if data[..4] != HNSW_ARTIFACT_MAGIC {
        return Err(error::data_corrupted("invalid HNSW artifact magic"));
    }
    if data.len() < 8 {
        return Err(error::data_corrupted(
            "HNSW artifact is truncated after its magic",
        ));
    }
    let version = u32::from_le_bytes(data[4..8].try_into().expect("u32 width"));
    if version != HNSW_ARTIFACT_VERSION {
        return Ok(HnswArtifactCompatibility::UnsupportedArtifactVersion(
            version,
        ));
    }
    if data.len() < 16 {
        return Err(error::data_corrupted(
            "HNSW artifact is truncated before its build contract version",
        ));
    }
    let header_len = u32::from_le_bytes(data[8..12].try_into().expect("u32 width")) as usize;
    if header_len != HNSW_ARTIFACT_HEADER_LEN {
        return Err(error::data_corrupted(format!(
            "invalid HNSW artifact header length {header_len}, expected {HNSW_ARTIFACT_HEADER_LEN}"
        )));
    }
    let build_contract_version = u32::from_le_bytes(data[12..16].try_into().expect("u32 width"));
    if build_contract_version != HNSW_BUILD_CONTRACT_VERSION {
        return Ok(HnswArtifactCompatibility::UnsupportedBuildContractVersion(
            build_contract_version,
        ));
    }
    Ok(HnswArtifactCompatibility::Current)
}

const fn distance_tag(distance: DistanceMetric) -> u8 {
    match distance {
        DistanceMetric::Euclidean => 0,
        DistanceMetric::Cosine => 1,
        DistanceMetric::DotProduct => 2,
        DistanceMetric::Manhattan => 3,
    }
}

fn append_entry_points(data: &mut Vec<u8>, entries: &[super::EntryPoint]) -> Result<()> {
    for entry in entries {
        data.extend_from_slice(&entry.point_id.to_le_bytes());
        let level = u32::try_from(entry.level)
            .map_err(|_| error::out_of_range("HNSW entry-point level exceeds u32"))?;
        data.extend_from_slice(&level.to_le_bytes());
    }
    Ok(())
}

fn read_entry_points(
    data: &[u8],
    offset: &mut usize,
    count: usize,
) -> Result<Vec<super::EntryPoint>> {
    let encoded_len = count
        .checked_mul(2 * std::mem::size_of::<u32>())
        .ok_or_else(|| error::data_corrupted("HNSW entry-point table length overflow"))?;
    if data.len().saturating_sub(*offset) < encoded_len {
        return Err(error::data_corrupted(format!(
            "HNSW entry-point table is truncated: count={count}, remaining={}",
            data.len().saturating_sub(*offset)
        )));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let point_id = u32::from_le_bytes(
            take_artifact_bytes(data, offset, 4, "entry point id")?
                .try_into()
                .expect("u32 width"),
        );
        let level = u32::from_le_bytes(
            take_artifact_bytes(data, offset, 4, "entry point level")?
                .try_into()
                .expect("u32 width"),
        ) as usize;
        entries.push(super::EntryPoint { point_id, level });
    }
    Ok(entries)
}

fn take_artifact_bytes<'a>(
    data: &'a [u8],
    offset: &mut usize,
    len: usize,
    field: &str,
) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| error::data_corrupted(format!("HNSW {field} offset overflow")))?;
    let bytes = data.get(*offset..end).ok_or_else(|| {
        error::data_corrupted(format!(
            "HNSW artifact truncated while reading {field}: need {end} bytes, got {}",
            data.len()
        ))
    })?;
    *offset = end;
    Ok(bytes)
}

enum HnswArtifactBacking {
    Bytes(Bytes),
    Mmap {
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
    },
}

impl HnswArtifactBacking {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Mmap { mmap, offset, len } => &mmap[*offset..*offset + *len],
        }
    }

    fn inverse_norms(&self, offset: usize, len: usize) -> Result<CosineInverseNorms> {
        match self {
            Self::Bytes(bytes) => CosineInverseNorms::from_bytes(bytes.slice(offset..offset + len)),
            Self::Mmap {
                mmap,
                offset: artifact_offset,
                ..
            } => CosineInverseNorms::from_mmap_range(
                Arc::clone(mmap),
                artifact_offset.checked_add(offset).ok_or_else(|| {
                    error::data_corrupted("HNSW cosine norm artifact offset overflow")
                })?,
                len,
            ),
        }
    }

    fn graph_links(&self, offset: usize) -> Result<GraphLinks> {
        match self {
            Self::Bytes(bytes) => GraphLinks::deserialize_bytes(bytes.slice(offset..)),
            Self::Mmap {
                mmap,
                offset: artifact_offset,
                len,
            } => GraphLinks::deserialize_mmap_range(
                Arc::clone(mmap),
                artifact_offset
                    .checked_add(offset)
                    .ok_or_else(|| error::data_corrupted("HNSW graph artifact offset overflow"))?,
                len.checked_sub(offset).ok_or_else(|| {
                    error::data_corrupted("HNSW graph artifact offset exceeds artifact length")
                })?,
            ),
        }
    }
}

/// A high-level HNSW index structure that combines graph and storage.
pub struct HnswIndex {
    pub build_contract: HnswBuildContract,
    pub graph: GraphLayers,
    pub vector_storage: Arc<dyn VectorStorage>,
    single_telemetry: Mutex<SearchTelemetry>,
    batch_telemetry: Mutex<HnswBatchTelemetry>,
}

impl HnswIndex {
    pub fn try_new(
        build_contract: HnswBuildContract,
        graph: GraphLayers,
        vector_storage: Arc<dyn VectorStorage>,
    ) -> Result<Self> {
        build_contract.validate()?;
        let distance = build_contract.distance;
        let vector_storage = IndexedVectorStorage::prepare(vector_storage, distance);
        let index = Self {
            build_contract,
            graph,
            vector_storage,
            single_telemetry: Mutex::new(SearchTelemetry::default()),
            batch_telemetry: Mutex::new(HnswBatchTelemetry::default()),
        };
        index.validate_entry_points()?;
        Ok(index)
    }

    #[cfg(test)]
    pub fn new(
        config: HnswConfig,
        graph: GraphLayers,
        vector_storage: Arc<dyn VectorStorage>,
        distance: DistanceMetric,
    ) -> Self {
        Self::try_new(
            config
                .try_build_contract(distance)
                .expect("test HNSW configuration is valid"),
            graph,
            vector_storage,
        )
        .expect("test HNSW configuration is valid")
    }

    /// Build a new HNSW index from scratch.
    pub fn try_build(
        storage: Arc<dyn VectorStorage>,
        build_contract: HnswBuildContract,
    ) -> Result<Self> {
        let (pool, _) = hnsw_build_pool()?;
        Self::build_with_controls(storage, build_contract, Some(pool), None)
    }

    #[cfg(test)]
    pub fn build(
        storage: Arc<dyn VectorStorage>,
        config: HnswConfig,
        distance: DistanceMetric,
    ) -> Self {
        Self::try_build(
            storage,
            config
                .try_build_contract(distance)
                .expect("test HNSW configuration is valid"),
        )
        .expect("test HNSW configuration is valid")
    }

    pub(crate) fn build_with_controls(
        storage: Arc<dyn VectorStorage>,
        build_contract: HnswBuildContract,
        pool: Option<&rayon::ThreadPool>,
        stop_check: Option<&HnswBuildStopCheck>,
    ) -> Result<Self> {
        build_contract.validate()?;
        let distance = build_contract.distance;
        let storage = IndexedVectorStorage::prepare(storage, distance);
        let num_vectors = storage.num_vectors();
        if num_vectors > PointOffset::MAX as usize {
            return Err(error::configuration_limit_exceeded(
                "HNSW artifact exceeds the u32 point-id address space",
            ));
        }
        // Diverse neighbor selection is required for clustered vector sets;
        // nearest-only truncation forms disconnected local components.
        let visited_capacity = pool.map_or(1, rayon::ThreadPool::current_num_threads);
        let mut builder = GraphLayersBuilder::new_from_contract_with_visited_capacity(
            num_vectors,
            &build_contract,
            true,
            visited_capacity,
        );

        // Pre-allocate levels for all points.
        for i in 0..num_vectors {
            if i % 1024 == 0 && stop_check.is_some_and(|check| check.should_stop()) {
                return Err(error::query_canceled());
            }
            let point_id = i as PointOffset;
            let level = builder.random_layer_for_point(point_id);
            builder.set_levels(point_id, level);
        }

        let point_order = DeterministicPointOrder::new(num_vectors, build_contract.build_seed);
        let warmup_end = (build_contract.warmup_point_count as usize).min(num_vectors);
        for position in 0..warmup_end {
            if stop_check.is_some_and(|check| check.should_stop()) {
                return Err(error::query_canceled());
            }
            builder.insert_single_point(point_order.point_at(position), storage.as_ref(), distance);
        }

        if warmup_end < num_vectors {
            let wave_size = build_contract.proposal_wave_size as usize;
            for wave_start in (warmup_end..num_vectors).step_by(wave_size) {
                if stop_check.is_some_and(|check| check.should_stop()) {
                    return Err(error::query_canceled());
                }
                let wave_end = wave_start.saturating_add(wave_size).min(num_vectors);
                let entry_points = builder.snapshot_entry_points();
                let proposals = if let Some(pool) = pool {
                    pool.install(|| {
                        (wave_start..wave_end)
                            .into_par_iter()
                            .map(|position| {
                                builder.propose_new_point(
                                    point_order.point_at(position),
                                    &entry_points,
                                    storage.as_ref(),
                                    distance,
                                )
                            })
                            .collect::<Vec<_>>()
                    })
                } else {
                    (wave_start..wave_end)
                        .map(|position| {
                            builder.propose_new_point(
                                point_order.point_at(position),
                                &entry_points,
                                storage.as_ref(),
                                distance,
                            )
                        })
                        .collect::<Vec<_>>()
                };
                if let Some(pool) = pool {
                    pool.install(|| {
                        builder.publish_frozen_wave(proposals, storage.as_ref(), distance, true)
                    });
                } else {
                    builder.publish_frozen_wave(proposals, storage.as_ref(), distance, false);
                }
            }
        }

        let (links, entry_points) = builder.into_graph_data();
        let graph = GraphLayers::new(
            links,
            entry_points,
            VisitedPool::new(),
            (&build_contract).into(),
        );
        Self::try_new(build_contract, graph, storage)
    }

    /// Save HNSW index to a directory.
    pub fn save(&self, directory: &Path) -> Result<()> {
        // Publishing is the one place where a full O(E) semantic validation
        // is mandatory. Opens remain O(N) and lazy.
        self.verify_integrity()?;
        if !directory.exists() {
            fs::create_dir_all(directory).map_err(error::io)?;
        }

        // Save only the immutable graph construction contract. Search policy
        // belongs to the active definition and can change without a rebuild.
        let config_path = directory.join("config.json");
        let config_json = serde_json::to_string_pretty(&self.build_contract)
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

        if let Some(norms) = self.vector_storage.cosine_inverse_norms() {
            let norms_path = directory.join("cosine_inverse_norms.bin");
            let mut bytes = Vec::with_capacity(norms.len() * std::mem::size_of::<f32>());
            for norm in norms.iter() {
                bytes.extend_from_slice(&norm.to_le_bytes());
            }
            fs::write(norms_path, bytes).map_err(error::io)?;
        }

        Ok(())
    }

    /// Load HNSW index from a directory.
    pub fn load(directory: &Path, vector_storage: Arc<dyn VectorStorage>) -> Result<Self> {
        // Load config.
        let config_path = directory.join("config.json");
        let config_json = fs::read_to_string(config_path).map_err(error::io)?;
        let build_contract: HnswBuildContract = serde_json::from_str(&config_json)
            .map_err(|e| error::serialization_error(e.to_string()))?;
        build_contract.validate()?;
        let distance = build_contract.distance;

        // Load entry points.
        let entry_points_path = directory.join("entry_points.json");
        let entry_points_json = fs::read_to_string(entry_points_path).map_err(error::io)?;
        let entry_points: EntryPoints = serde_json::from_str(&entry_points_json)
            .map_err(|e| error::serialization_error(e.to_string()))?;

        // Load graph links.
        let links_path = directory.join("graph_links.bin");
        let links =
            GraphLinks::load_mmap(&links_path).or_else(|_| GraphLinks::load(&links_path))?;

        let vector_storage = if distance == DistanceMetric::Cosine {
            let norms_path = directory.join("cosine_inverse_norms.bin");
            let file = fs::File::open(norms_path).map_err(error::io)?;
            let norms = if file.metadata().map_err(error::io)?.len() == 0 {
                CosineInverseNorms::Owned(Arc::from([]))
            } else {
                let mmap = Arc::new(unsafe { MmapOptions::new().map(&file).map_err(error::io)? });
                CosineInverseNorms::from_mmap(mmap)?
            };
            IndexedVectorStorage::from_persisted_cosine_norms(vector_storage, norms)?
        } else {
            vector_storage
        };
        let graph = GraphLayers::new(
            links,
            entry_points,
            VisitedPool::new(),
            (&build_contract).into(),
        );

        let index = Self {
            build_contract,
            graph,
            vector_storage,
            single_telemetry: Mutex::new(SearchTelemetry::default()),
            batch_telemetry: Mutex::new(HnswBatchTelemetry::default()),
        };
        index.validate_entry_points()?;
        Ok(index)
    }

    /// Serialize HNSW index to a byte vector for embedding in segments.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        // Inline and sidecar generations are published from this envelope.
        // Run the expensive semantic verifier here, while the freshly built
        // graph is hot, instead of imposing O(E) work on every open.
        self.verify_integrity()?;
        let mut data = Vec::new();

        data.extend_from_slice(&HNSW_ARTIFACT_MAGIC);
        data.extend_from_slice(&HNSW_ARTIFACT_VERSION.to_le_bytes());
        data.extend_from_slice(&(HNSW_ARTIFACT_HEADER_LEN as u32).to_le_bytes());
        data.extend_from_slice(&self.build_contract.version.to_le_bytes());
        data.extend_from_slice(&self.build_contract.m.to_le_bytes());
        data.extend_from_slice(&self.build_contract.m0.to_le_bytes());
        data.extend_from_slice(&self.build_contract.ef_construct.to_le_bytes());
        data.push(distance_tag(self.build_contract.distance));
        data.extend_from_slice(&[0; 3]);
        data.extend_from_slice(&self.build_contract.build_seed.to_le_bytes());
        data.extend_from_slice(&self.build_contract.proposal_wave_size.to_le_bytes());
        data.extend_from_slice(&self.build_contract.warmup_point_count.to_le_bytes());
        let norm_count = self
            .vector_storage
            .cosine_inverse_norms()
            .map_or(0, CosineInverseNorms::len);
        let norms = self.vector_storage.cosine_inverse_norms();
        data.extend_from_slice(&(norm_count as u64).to_le_bytes());
        let primary_count = u32::try_from(self.graph.entry_points.entry_points.len())
            .map_err(|_| error::out_of_range("too many HNSW primary entry points"))?;
        let extra_count = u32::try_from(self.graph.entry_points.extra_entry_points.len())
            .map_err(|_| error::out_of_range("too many HNSW extra entry points"))?;
        data.extend_from_slice(&primary_count.to_le_bytes());
        data.extend_from_slice(&extra_count.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        debug_assert_eq!(data.len(), HNSW_ARTIFACT_HEADER_LEN);

        if let Some(norms) = norms {
            for norm in norms.iter() {
                data.extend_from_slice(&norm.to_le_bytes());
            }
        }

        append_entry_points(&mut data, &self.graph.entry_points.entry_points)?;
        append_entry_points(&mut data, &self.graph.entry_points.extra_entry_points)?;

        // Serialize graph links in binary form.
        self.graph.links.serialize(&mut data)?;

        // Append statistics trailer.
        let stats = HnswIndexStatistics::collect(self);
        append_stats_trailer(&mut data, &stats.to_bytes())?;

        Ok(data)
    }

    /// Deserialize HNSW index from a byte buffer.
    pub fn deserialize(data: &[u8], vector_storage: Arc<dyn VectorStorage>) -> Result<Self> {
        Self::deserialize_bytes(Bytes::copy_from_slice(data), vector_storage)
    }

    /// Deserialize an inline artifact while retaining its owned byte backing.
    /// Graph links and cosine norms become immutable slices of that backing,
    /// so open does not allocate memory proportional to the graph size.
    pub fn deserialize_bytes(data: Bytes, vector_storage: Arc<dyn VectorStorage>) -> Result<Self> {
        Self::deserialize_backing(HnswArtifactBacking::Bytes(data), vector_storage)
    }

    /// Deserialize a sidecar artifact directly over its package mmap.
    pub fn deserialize_mmap_range(
        mmap: Arc<Mmap>,
        artifact_offset: usize,
        artifact_len: usize,
        vector_storage: Arc<dyn VectorStorage>,
    ) -> Result<Self> {
        let end = artifact_offset
            .checked_add(artifact_len)
            .ok_or_else(|| error::data_corrupted("HNSW artifact mmap range overflow"))?;
        if end > mmap.len() {
            return Err(error::data_corrupted(
                "HNSW artifact mmap range exceeds package length",
            ));
        }
        Self::deserialize_backing(
            HnswArtifactBacking::Mmap {
                mmap,
                offset: artifact_offset,
                len: artifact_len,
            },
            vector_storage,
        )
    }

    fn deserialize_backing(
        backing: HnswArtifactBacking,
        vector_storage: Arc<dyn VectorStorage>,
    ) -> Result<Self> {
        let data = backing.as_bytes();
        match hnsw_artifact_compatibility(data)? {
            HnswArtifactCompatibility::Current => {}
            compatibility => {
                return Err(error::artifact_not_ready(
                    compatibility
                        .rebuild_reason()
                        .expect("non-current compatibility has a reason"),
                ))
            }
        }

        let mut offset = 8;
        let header_len = u32::from_le_bytes(
            take_artifact_bytes(data, &mut offset, 4, "header length")?
                .try_into()
                .expect("u32 width"),
        ) as usize;
        if header_len != HNSW_ARTIFACT_HEADER_LEN {
            return Err(error::data_corrupted(format!(
                "invalid HNSW artifact header length {header_len}, expected {HNSW_ARTIFACT_HEADER_LEN}"
            )));
        }
        let read_u32 = |data: &[u8], offset: &mut usize, field| -> Result<u32> {
            Ok(u32::from_le_bytes(
                take_artifact_bytes(data, offset, 4, field)?
                    .try_into()
                    .expect("u32 width"),
            ))
        };
        let build_contract = HnswBuildContract {
            version: read_u32(data, &mut offset, "build contract version")?,
            m: read_u32(data, &mut offset, "m")?,
            m0: read_u32(data, &mut offset, "m0")?,
            ef_construct: read_u32(data, &mut offset, "ef_construct")?,
            distance: {
                let tag = take_artifact_bytes(data, &mut offset, 1, "distance")?[0];
                DistanceMetric::from_u8(tag).ok_or_else(|| {
                    error::data_corrupted(format!("unknown HNSW distance tag {tag}"))
                })?
            },
            build_seed: {
                let padding = take_artifact_bytes(data, &mut offset, 3, "distance padding")?;
                if padding != [0, 0, 0] {
                    return Err(error::data_corrupted("HNSW distance padding must be zero"));
                }
                u64::from_le_bytes(
                    take_artifact_bytes(data, &mut offset, 8, "build seed")?
                        .try_into()
                        .expect("u64 width"),
                )
            },
            proposal_wave_size: read_u32(data, &mut offset, "proposal wave size")?,
            warmup_point_count: read_u32(data, &mut offset, "warm-up point count")?,
        };
        build_contract.validate()?;
        let distance = build_contract.distance;

        let norm_count = usize::try_from(u64::from_le_bytes(
            take_artifact_bytes(data, &mut offset, 8, "inverse norm count")?
                .try_into()
                .expect("u64 width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW inverse norm count exceeds usize"))?;
        let primary_count = read_u32(data, &mut offset, "primary entry point count")? as usize;
        let extra_count = read_u32(data, &mut offset, "extra entry point count")? as usize;
        if u64::from_le_bytes(
            take_artifact_bytes(data, &mut offset, 8, "reserved")?
                .try_into()
                .expect("u64 width"),
        ) != 0
        {
            return Err(error::data_corrupted(
                "HNSW artifact reserved header field must be zero",
            ));
        }
        debug_assert_eq!(offset, HNSW_ARTIFACT_HEADER_LEN);
        let norm_bytes = norm_count
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(|| error::data_corrupted("HNSW inverse norm byte length overflow"))?;
        let norm_start = offset;
        take_artifact_bytes(data, &mut offset, norm_bytes, "inverse norms")?;
        let inverse_norms = backing.inverse_norms(norm_start, norm_bytes)?;
        let vector_storage = match distance {
            DistanceMetric::Cosine => {
                IndexedVectorStorage::from_persisted_cosine_norms(vector_storage, inverse_norms)?
            }
            _ if norm_count == 0 => vector_storage,
            _ => {
                return Err(error::data_corrupted(
                    "non-cosine HNSW artifact contains cosine inverse norms",
                ))
            }
        };

        let entry_points = EntryPoints {
            entry_points: read_entry_points(data, &mut offset, primary_count)?,
            extra_entry_points: read_entry_points(data, &mut offset, extra_count)?,
        };

        // Deserialize graph links.
        let links = backing.graph_links(offset)?;

        let graph = GraphLayers::new(
            links,
            entry_points,
            VisitedPool::new(),
            (&build_contract).into(),
        );

        let index = Self {
            build_contract,
            graph,
            vector_storage,
            single_telemetry: Mutex::new(SearchTelemetry::default()),
            batch_telemetry: Mutex::new(HnswBatchTelemetry::default()),
        };
        index.validate_entry_points()?;
        Ok(index)
    }

    /// Perform a vector search under the caller's active query policy.
    pub(crate) fn search_one_with_policy_strategy(
        &self,
        query: &[f32],
        top_k: usize,
        params: &SearchParams,
        filter: HnswSearchFilter<'_>,
        policy: &HnswSearchPolicy,
        strategy: HnswSearchStrategy,
    ) -> Result<Vec<ScoredPoint>> {
        if top_k == 0 {
            return Ok(Vec::new());
        }

        let start = Instant::now();
        let pre_filter_count = self.graph.num_points() as u64;
        let filter_bitmap = filter.bitmap();
        let post_filter_count = filter_bitmap.map(|bm| bm.len()).unwrap_or(pre_filter_count);

        let prepared_query = self.build_contract.distance.prepare(query);
        let mut scorer = VectorScorer::new(&prepared_query, self.vector_storage.as_ref())?;
        let mut exact_scan = false;
        let mut masked_graph = false;
        let mut adaptive_graph = false;
        let mut predicate_refined = false;
        let mut exact_fallback = false;
        let results = if self.should_use_plain_scan(filter, policy, strategy) {
            exact_scan = true;
            self.plain_scan(top_k, &mut scorer, filter_bitmap)
        } else {
            let algorithm = Self::algorithm_for_strategy(filter, strategy)?;
            masked_graph = algorithm == SearchAlgorithm::MaskedTopK;
            adaptive_graph = algorithm == SearchAlgorithm::AdaptiveFilteredTopK;
            let ef = Self::effective_graph_ef(top_k, params, policy);
            let graph_result = self.graph.search_one(
                top_k,
                ef,
                algorithm,
                &mut scorer,
                filter_bitmap,
                Self::use_random_entry_point(params),
            );
            predicate_refined = graph_result.predicate_refined;
            let results = graph_result.points;
            if filter_bitmap.is_some()
                && results.len() < self.expected_filtered_rows(top_k, filter_bitmap)?
            {
                exact_fallback = true;
                self.plain_scan(top_k, &mut scorer, filter_bitmap)
            } else {
                results
            }
        };

        let elapsed_us = start.elapsed().as_micros() as u64;
        let mut telemetry = self.single_telemetry.lock().unwrap();
        telemetry.record(elapsed_us, pre_filter_count, post_filter_count);
        telemetry.record_hnsw_work(
            scorer.scored_point_count(),
            exact_scan,
            masked_graph,
            adaptive_graph,
            predicate_refined,
            exact_fallback,
        );

        Ok(results)
    }

    #[cfg(test)]
    pub(crate) fn search_one(
        &self,
        query: &[f32],
        top_k: usize,
        params: &SearchParams,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> Result<Vec<ScoredPoint>> {
        let policy = HnswSearchPolicy::default();
        let filter = filter_bitmap.map_or(HnswSearchFilter::None, HnswSearchFilter::Predicate);
        let matching_rows = filter
            .bitmap()
            .map_or(self.graph.num_points() as u64, RoaringBitmap::len);
        let strategy = HnswSearchStrategy::choose(
            filter.kind(),
            matching_rows,
            self.graph.num_points() as u64,
            policy,
        );
        self.search_one_with_policy_strategy(query, top_k, params, filter, &policy, strategy)
    }

    /// Perform batched vector search using one shared filter bitmap.
    pub fn search_many_prepared_with_policy(
        &self,
        queries: &[PreparedQuery],
        top_k: usize,
        params: &SearchParams,
        filter: HnswSearchFilter<'_>,
        policy: &HnswSearchPolicy,
        strategy: HnswSearchStrategy,
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
            .collect::<Result<Vec<_>>>()?;

        let filter_bitmap = filter.bitmap();
        let results = if self.should_use_plain_scan(filter, policy, strategy) {
            let batch_scorer = BatchScorer::new(scorers, top_k);
            let num_points = self.graph.num_points() as u32;
            match filter_bitmap {
                Some(bitmap) => batch_scorer.scan(bitmap.iter().filter(|&idx| idx < num_points)),
                None => batch_scorer.scan(0..num_points),
            }
        } else {
            let algorithm = Self::algorithm_for_strategy(filter, strategy)?;
            let ef = Self::effective_graph_ef(top_k, params, policy);
            let results = self.graph.search_many(
                top_k,
                ef,
                algorithm,
                &mut scorers,
                filter_bitmap,
                Self::use_random_entry_point(params),
            );
            let expected_rows = self.expected_filtered_rows(top_k, filter_bitmap)?;
            if filter_bitmap.is_some() {
                let mut results = results
                    .into_iter()
                    .map(|result| result.points)
                    .collect::<Vec<_>>();
                for (rows, scorer) in results.iter_mut().zip(scorers.iter_mut()) {
                    if rows.len() < expected_rows {
                        *rows = self.plain_scan(top_k, scorer, filter_bitmap);
                    }
                }
                results
            } else {
                results.into_iter().map(|result| result.points).collect()
            }
        };

        let elapsed_us = start.elapsed().as_micros() as u64;
        self.batch_telemetry
            .lock()
            .unwrap()
            .record_batch(elapsed_us, queries.len());

        Ok(results)
    }

    #[cfg(test)]
    pub(crate) fn search_many_prepared(
        &self,
        queries: &[PreparedQuery],
        top_k: usize,
        params: &SearchParams,
        filter_bitmap: Option<&RoaringBitmap>,
        strategy: HnswSearchStrategy,
    ) -> Result<Vec<Vec<ScoredPoint>>> {
        self.search_many_prepared_with_policy(
            queries,
            top_k,
            params,
            filter_bitmap.map_or(HnswSearchFilter::None, HnswSearchFilter::Predicate),
            &HnswSearchPolicy::default(),
            strategy,
        )
    }

    /// Snapshot single-query search telemetry.
    pub fn search_telemetry(&self) -> SearchTelemetry {
        self.single_telemetry.lock().unwrap().clone()
    }

    /// Snapshot batched search telemetry.
    pub fn batch_search_telemetry(&self) -> HnswBatchTelemetry {
        self.batch_telemetry.lock().unwrap().clone()
    }

    /// Explicit deep verifier for recovery/fsck tooling. Normal mmap open only
    /// validates the O(N) layout and checksum boundary.
    pub fn verify_integrity(&self) -> Result<()> {
        self.build_contract.validate()?;
        self.validate_entry_points()?;
        match (
            self.build_contract.distance,
            self.vector_storage.cosine_inverse_norms(),
        ) {
            (DistanceMetric::Cosine, Some(norms))
                if norms.len() == self.vector_storage.num_vectors() =>
            {
                if let Some((point, value)) = norms
                    .iter()
                    .enumerate()
                    .find(|(_, value)| !value.is_finite() || *value < 0.0)
                {
                    return Err(error::data_corrupted(format!(
                        "invalid cosine inverse norm {value} for HNSW point {point}"
                    )));
                }
            }
            (DistanceMetric::Cosine, _) => {
                return Err(error::data_corrupted(
                    "cosine HNSW artifact is missing per-point inverse norms",
                ))
            }
            (_, Some(_)) => {
                return Err(error::data_corrupted(
                    "non-cosine HNSW artifact contains cosine inverse norms",
                ))
            }
            (_, None) => {}
        }
        self.graph.links.verify_integrity()
    }

    fn validate_entry_points(&self) -> Result<()> {
        for entry in self
            .graph
            .entry_points
            .entry_points
            .iter()
            .chain(self.graph.entry_points.extra_entry_points.iter())
        {
            if entry.point_id as usize >= self.vector_storage.num_vectors() {
                return Err(error::data_corrupted(format!(
                    "HNSW entry point {} is outside vector cardinality {}",
                    entry.point_id,
                    self.vector_storage.num_vectors()
                )));
            }
            if entry.level >= self.graph.links.num_levels(entry.point_id) {
                return Err(error::data_corrupted(format!(
                    "HNSW entry point {} level {} exceeds its graph levels",
                    entry.point_id, entry.level
                )));
            }
        }
        Ok(())
    }

    fn algorithm_for_strategy(
        filter: HnswSearchFilter<'_>,
        strategy: HnswSearchStrategy,
    ) -> Result<SearchAlgorithm> {
        match (strategy, filter) {
            (HnswSearchStrategy::UnfilteredGraph, HnswSearchFilter::None)
            | (HnswSearchStrategy::MaskedGraph, HnswSearchFilter::None)
            | (HnswSearchStrategy::AdaptiveFilteredGraph, HnswSearchFilter::None) => {
                Ok(SearchAlgorithm::Hnsw)
            }
            (HnswSearchStrategy::MaskedGraph, HnswSearchFilter::Visibility(_)) => {
                Ok(SearchAlgorithm::MaskedTopK)
            }
            (HnswSearchStrategy::AdaptiveFilteredGraph, HnswSearchFilter::Predicate(_)) => {
                Ok(SearchAlgorithm::AdaptiveFilteredTopK)
            }
            (HnswSearchStrategy::ExactScan, _) => Err(error::internal(
                "exact HNSW strategy reached graph algorithm selection",
            )),
            (HnswSearchStrategy::UnfilteredGraph, _) => Err(error::internal(
                "unfiltered HNSW strategy received an admission bitmap",
            )),
            (HnswSearchStrategy::MaskedGraph, HnswSearchFilter::Predicate(_)) => Err(
                error::internal("predicate HNSW search must use adaptive graph execution"),
            ),
            (HnswSearchStrategy::AdaptiveFilteredGraph, HnswSearchFilter::Visibility(_)) => Err(
                error::internal("visibility-only HNSW search cannot use predicate refinement"),
            ),
        }
    }

    fn effective_graph_ef(top_k: usize, params: &SearchParams, policy: &HnswSearchPolicy) -> usize {
        policy.effective_ef(top_k, params.ef)
    }

    fn use_random_entry_point(params: &SearchParams) -> bool {
        params.random_entry_point.unwrap_or(false)
    }

    fn validate_prepared_queries(&self, queries: &[PreparedQuery]) -> Result<()> {
        let expected_dim = self.vector_storage.vector_dim();
        for (idx, query) in queries.iter().enumerate() {
            if query.metric() != self.build_contract.distance {
                return Err(error::invalid_input(format!(
                    "query[{idx}] prepared with {:?}, but index uses {:?}",
                    query.metric(),
                    self.build_contract.distance
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
        filter: HnswSearchFilter<'_>,
        policy: &HnswSearchPolicy,
        strategy: HnswSearchStrategy,
    ) -> bool {
        if strategy == HnswSearchStrategy::ExactScan {
            return true;
        }
        if self.graph.num_points() <= policy.plain_scan_threshold {
            return true;
        }

        match filter {
            HnswSearchFilter::None => false,
            HnswSearchFilter::Visibility(bitmap) | HnswSearchFilter::Predicate(bitmap) => {
                bitmap.len() <= policy.filtered_plain_scan_threshold as u64
            }
        }
    }

    fn expected_filtered_rows(
        &self,
        top_k: usize,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> Result<usize> {
        let Some(bitmap) = filter_bitmap else {
            return Ok(top_k.min(self.graph.num_points()));
        };
        if bitmap
            .max()
            .is_some_and(|point| point as usize >= self.graph.num_points())
        {
            return Err(error::data_corrupted(format!(
                "HNSW filter bitmap domain exceeds graph cardinality {}",
                self.graph.num_points()
            )));
        }
        Ok(top_k.min(bitmap.len() as usize))
    }

    fn plain_scan(
        &self,
        top_k: usize,
        scorer: &mut VectorScorer,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> Vec<ScoredPoint> {
        let num_points = self.graph.num_points() as u32;
        match filter_bitmap {
            Some(bitmap) => self.plain_scan_iter(
                top_k,
                scorer,
                bitmap.iter().take_while(|&idx| idx < num_points),
            ),
            None => self.plain_scan_iter(top_k, scorer, 0..num_points),
        }
    }

    fn plain_scan_iter(
        &self,
        top_k: usize,
        scorer: &mut VectorScorer,
        point_ids: impl Iterator<Item = PointOffset>,
    ) -> Vec<ScoredPoint> {
        const SCORE_BATCH: usize = crate::index::hnsw::batch_scorer::BATCH_SIZE;
        let mut best = FixedLengthPriorityQueue::new(top_k);
        let mut point_ids = point_ids;
        let mut chunk = [0; SCORE_BATCH];
        loop {
            let mut len = 0;
            while len < SCORE_BATCH {
                let Some(point_id) = point_ids.next() else {
                    break;
                };
                chunk[len] = point_id;
                len += 1;
            }
            if len == 0 {
                break;
            }
            for point in scorer.score_points_unfiltered(&chunk[..len]) {
                best.push(point);
            }
        }
        best.into_sorted_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::{HnswBuilder, HnswM, InMemoryVectorStorage, PointOffset};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
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
    ) -> HnswIndex {
        assert_eq!(vectors.len(), levels.len());

        let storage = IndexedVectorStorage::prepare(make_storage(vectors), distance);
        let mut builder =
            GraphLayersBuilder::new_with_heuristic(vectors.len(), &config, use_heuristic);

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
            .with_build_seed(0x1234_5678_9abc_def0);
        let first = HnswIndex::build(make_storage(&vectors), config, DistanceMetric::Euclidean)
            .serialize()
            .unwrap();
        let second = HnswIndex::build(make_storage(&vectors), config, DistanceMetric::Euclidean)
            .serialize()
            .unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn frozen_wave_point_order_is_a_seeded_bijection() {
        let len = 10_003;
        let first = DeterministicPointOrder::new(len, 7);
        let second = DeterministicPointOrder::new(len, 7);
        let other = DeterministicPointOrder::new(len, 8);
        let first_order = (0..len)
            .map(|position| first.point_at(position))
            .collect::<Vec<_>>();
        let second_order = (0..len)
            .map(|position| second.point_at(position))
            .collect::<Vec<_>>();
        let other_order = (0..len)
            .map(|position| other.point_at(position))
            .collect::<Vec<_>>();

        assert_eq!(first_order, second_order);
        assert_ne!(first_order, other_order);
        let mut sorted = first_order;
        sorted.sort_unstable();
        assert_eq!(sorted, (0..len as u32).collect::<Vec<_>>());
    }

    #[test]
    fn frozen_wave_point_order_breaks_ingest_locality() {
        for len in [5_000, 10_007] {
            for seed in 0..128 {
                let order = DeterministicPointOrder::new(len, seed);
                let first_wave = (0..64)
                    .map(|position| order.point_at(position))
                    .collect::<Vec<_>>();
                let min = first_wave.iter().copied().min().unwrap();
                let max = first_wave.iter().copied().max().unwrap();
                assert!(
                    max - min > len as u32 / 4,
                    "seed {seed} left a clustered first wave for {len} points"
                );
                let distinct_deltas = first_wave
                    .windows(2)
                    .map(|pair| pair[1].abs_diff(pair[0]))
                    .collect::<std::collections::BTreeSet<_>>();
                assert!(distinct_deltas.len() > 32);
            }
        }
    }

    #[test]
    fn frozen_wave_build_is_byte_deterministic_across_pool_widths() {
        let vectors = make_sift_like_vectors(0xdef, 4_352, 24, 16);
        let contract = HnswConfig::new(12, 72)
            .with_build_seed(0x0fed_cba9_8765_4321)
            .build_contract(DistanceMetric::Euclidean);
        let build = |width| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(width)
                .build()
                .unwrap();
            HnswIndex::build_with_controls(make_storage(&vectors), contract, Some(&pool), None)
                .unwrap()
                .serialize()
                .unwrap()
        };
        let width_2 = build(2);
        let width_7 = build(7);
        let width_16 = build(16);

        assert_eq!(width_2, width_7);
        assert_eq!(width_2, width_16);
    }

    #[test]
    fn frozen_wave_build_retains_sift_like_recall() {
        let vectors = make_sift_like_vectors(0x123, 2_048, 32, 24);
        let queries = make_sift_like_queries(0x456, &vectors, 64, 0.02);
        let config = HnswConfig::new(16, 96)
            .with_plain_scan_threshold(0)
            .with_ef(96);
        let index = HnswBuilder::new()
            .build(
                make_storage(&vectors),
                config.build_contract(DistanceMetric::Euclidean),
            )
            .unwrap();
        let recall = average_recall_at_k(
            &index,
            &vectors,
            &queries,
            10,
            &SearchParams {
                ef: Some(96),
                ..Default::default()
            },
            DistanceMetric::Euclidean,
        );

        assert!(
            recall >= 0.94,
            "expected recall@10 >= 0.94, got {recall:.3}"
        );
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
        };
        let top_k = 12;

        let batch = index
            .search_many_prepared(
                &prepared_queries,
                top_k,
                &params,
                Some(&filter),
                HnswSearchStrategy::MaskedGraph,
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
                HnswSearchStrategy::ExactScan,
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
                HnswSearchStrategy::ExactScan,
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
        };
        let top_k = 8;

        let batch = index
            .search_many_prepared(
                &prepared_queries,
                top_k,
                &params,
                None,
                HnswSearchStrategy::UnfilteredGraph,
            )
            .unwrap();
        let single = index.search_one(&query, top_k, &params, None).unwrap();

        assert_eq!(batch.len(), 1);
        assert_scored_points_exact(&batch[0], &single);
    }

    #[test]
    fn test_hnsw_filtered_topk_search() {
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
            random_entry_point: None,
        };
        let result = index.search_one(&[0.0], 1, &params, Some(&bitmap)).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].idx, entry_id);
    }

    #[test]
    fn filtered_algorithm_keeps_navigation_separate_from_result_admission() {
        let filter = RoaringBitmap::from_iter([0]);
        assert_eq!(
            HnswIndex::algorithm_for_strategy(
                HnswSearchFilter::Predicate(&filter),
                HnswSearchStrategy::AdaptiveFilteredGraph,
            )
            .unwrap(),
            SearchAlgorithm::AdaptiveFilteredTopK
        );
        assert_eq!(
            HnswIndex::algorithm_for_strategy(
                HnswSearchFilter::Visibility(&filter),
                HnswSearchStrategy::MaskedGraph,
            )
            .unwrap(),
            SearchAlgorithm::MaskedTopK
        );
        assert!(HnswIndex::algorithm_for_strategy(
            HnswSearchFilter::Predicate(&filter),
            HnswSearchStrategy::MaskedGraph,
        )
        .is_err());
        assert_eq!(
            HnswIndex::algorithm_for_strategy(
                HnswSearchFilter::None,
                HnswSearchStrategy::UnfilteredGraph,
            )
            .unwrap(),
            SearchAlgorithm::Hnsw
        );
        assert!(HnswIndex::algorithm_for_strategy(
            HnswSearchFilter::Visibility(&filter),
            HnswSearchStrategy::AdaptiveFilteredGraph,
        )
        .is_err());
    }

    #[test]
    fn adaptive_filtered_graph_refines_from_observed_admissions() {
        let vectors = make_sift_like_vectors(101, 512, 24, 16);
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(16, 96)
            .with_plain_scan_threshold(0)
            .with_filtered_plain_scan_threshold(0)
            .with_ef(96);
        let index = HnswIndex::build(storage, config, DistanceMetric::Euclidean);
        let query = &vectors[7];
        let params = SearchParams {
            ef: Some(96),
            random_entry_point: Some(false),
        };
        let policy = HnswSearchPolicy {
            ef_search: 96,
            plain_scan_threshold: 0,
            filtered_plain_scan_threshold: 0,
        };

        let broad = RoaringBitmap::from_iter((0..vectors.len() as u32).filter(|idx| idx % 4 != 0));
        let broad_rows = index
            .search_one_with_policy_strategy(
                query,
                10,
                &params,
                HnswSearchFilter::Predicate(&broad),
                &policy,
                HnswSearchStrategy::AdaptiveFilteredGraph,
            )
            .unwrap();
        assert_eq!(broad_rows.len(), 10);
        let broad_telemetry = index.search_telemetry();
        assert_eq!(broad_telemetry.hnsw_adaptive_graph_count, 1);
        assert_eq!(broad_telemetry.hnsw_predicate_refinement_count, 0);

        let selective = RoaringBitmap::from_iter((0..vectors.len() as u32).step_by(64));
        let selective_rows = index
            .search_one_with_policy_strategy(
                query,
                6,
                &params,
                HnswSearchFilter::Predicate(&selective),
                &policy,
                HnswSearchStrategy::AdaptiveFilteredGraph,
            )
            .unwrap();
        assert_eq!(selective_rows.len(), 6);
        let telemetry = index.search_telemetry();
        assert_eq!(telemetry.hnsw_adaptive_graph_count, 2);
        assert_eq!(telemetry.hnsw_predicate_refinement_count, 1);
    }

    #[test]
    fn test_hnsw_search_many_matches_search_one_filtered_topk_path() {
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
            random_entry_point: Some(false),
        };
        let top_k = 10;

        let batch = index
            .search_many_prepared(
                &prepared_queries,
                top_k,
                &params,
                Some(&filter),
                HnswSearchStrategy::AdaptiveFilteredGraph,
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
        assert!(!HnswIndex::use_random_entry_point(&SearchParams::default()));
        assert!(HnswIndex::use_random_entry_point(&SearchParams {
            random_entry_point: Some(true),
            ..Default::default()
        }));
        assert!(!HnswIndex::use_random_entry_point(&SearchParams {
            random_entry_point: Some(false),
            ..Default::default()
        }));
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
    fn embedded_artifact_requires_the_versioned_envelope() {
        let vectors = vec![vec![0.0], vec![1.0]];
        let index = HnswIndex::build(
            make_storage(&vectors),
            HnswConfig::new(8, 32),
            DistanceMetric::Euclidean,
        );
        let bytes = index.serialize().unwrap();
        assert_eq!(&bytes[..HNSW_ARTIFACT_MAGIC.len()], &HNSW_ARTIFACT_MAGIC);

        let mut legacy = bytes.clone();
        legacy[0] = 0;
        assert!(HnswIndex::deserialize(&legacy, make_storage(&vectors))
            .err()
            .expect("legacy envelope must fail")
            .to_string()
            .contains("artifact magic"));

        let mut unknown = bytes;
        unknown[4..8].copy_from_slice(&(HNSW_ARTIFACT_VERSION + 1).to_le_bytes());
        assert!(HnswIndex::deserialize(&unknown, make_storage(&vectors))
            .err()
            .expect("unknown envelope version must fail")
            .to_string()
            .contains("rebuild the vector index"));
    }

    #[test]
    fn embedded_artifact_rejects_stale_build_contract_before_open() {
        let vectors = vec![vec![0.0], vec![1.0]];
        let index = HnswIndex::build(
            make_storage(&vectors),
            HnswConfig::new(8, 32),
            DistanceMetric::Euclidean,
        );
        let mut bytes = index.serialize().unwrap();
        bytes[12..16].copy_from_slice(&(HNSW_BUILD_CONTRACT_VERSION - 1).to_le_bytes());

        assert_eq!(
            hnsw_artifact_compatibility(&bytes).unwrap(),
            HnswArtifactCompatibility::UnsupportedBuildContractVersion(
                HNSW_BUILD_CONTRACT_VERSION - 1
            )
        );
        let error = HnswIndex::deserialize(&bytes, make_storage(&vectors))
            .err()
            .expect("stale topology algorithm must require rebuild");
        assert!(error.to_string().contains("build contract version"));
        assert!(error.to_string().contains("rebuild the vector index"));
    }

    #[test]
    fn cosine_inverse_norms_are_persisted_with_the_index_artifact() {
        let vectors = vec![vec![3.0, 4.0], vec![0.0, 0.0], vec![1.0, 0.0]];
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(8, 32).with_plain_scan_threshold(0);
        let index = HnswIndex::build(storage, config, DistanceMetric::Cosine);

        let norms = index
            .vector_storage
            .cosine_inverse_norms()
            .expect("cosine index preprocessing");
        assert!((norms.value(0) - 0.2).abs() < 1e-6);
        assert_eq!(norms.value(1), 0.0);
        assert_eq!(norms.value(2), 1.0);

        let bytes = index.serialize().unwrap();
        let restored = HnswIndex::deserialize(&bytes, make_storage(&vectors)).unwrap();
        assert!(restored.graph.links.is_bytes_backed());
        assert!(restored
            .vector_storage
            .cosine_inverse_norms()
            .unwrap()
            .is_bytes_backed());
        assert_eq!(
            restored
                .vector_storage
                .cosine_inverse_norms()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            norms.iter().collect::<Vec<_>>()
        );
        let result = restored
            .search_one(&[1.0, 0.0], 3, &SearchParams::default(), None)
            .unwrap();
        assert_eq!(result[0].idx, 2);
        assert_eq!(result[2].idx, 1);
    }

    #[test]
    fn sidecar_mmap_range_keeps_graph_and_norms_zero_copy() {
        let vectors = vec![vec![3.0, 4.0], vec![1.0, 0.0], vec![0.0, 1.0]];
        let storage: Arc<dyn VectorStorage> = make_storage(&vectors);
        let index = HnswIndex::build(
            Arc::clone(&storage),
            HnswConfig::new(8, 32),
            DistanceMetric::Cosine,
        );
        let artifact = index.serialize().unwrap();
        let prefix_len = 19;
        let mut package = vec![0xA5; prefix_len];
        package.extend_from_slice(&artifact);
        package.extend_from_slice(&[0x5A; 11]);

        let temp_dir = TempDir::new().unwrap();
        let package_path = temp_dir.path().join("sidecar.pkg");
        fs::write(&package_path, package).unwrap();
        let file = fs::File::open(package_path).unwrap();
        let mmap = Arc::new(unsafe { MmapOptions::new().map(&file).unwrap() });
        let restored =
            HnswIndex::deserialize_mmap_range(mmap, prefix_len, artifact.len(), storage).unwrap();

        assert!(restored.graph.links.is_mmap_backed());
        assert!(restored
            .vector_storage
            .cosine_inverse_norms()
            .expect("cosine norms")
            .is_mmap_backed());
    }

    #[test]
    fn metric_preprocessing_does_not_leak_between_artifact_contracts() {
        let vectors = vec![vec![3.0, 4.0], vec![1.0, 0.0]];
        let cosine_storage =
            IndexedVectorStorage::prepare(make_storage(&vectors), DistanceMetric::Cosine);
        assert!(cosine_storage.cosine_inverse_norms().is_some());

        let dot_index = HnswIndex::build(
            cosine_storage,
            HnswConfig::new(8, 32),
            DistanceMetric::DotProduct,
        );
        assert!(dot_index.vector_storage.cosine_inverse_norms().is_none());
        dot_index.verify_integrity().unwrap();
    }

    #[test]
    fn serialize_rejects_semantically_invalid_graph_before_publish() {
        let links = GraphLinks::new_from_edges(vec![vec![vec![1]], vec![vec![0]]]);
        let mut encoded = Vec::new();
        links.serialize(&mut encoded).unwrap();
        let first_link_offset = 64 + (2 + 1) * std::mem::size_of::<u64>();
        encoded[first_link_offset..first_link_offset + 4].copy_from_slice(&99_u32.to_le_bytes());
        let invalid_links = GraphLinks::deserialize(encoded.as_slice()).unwrap();
        let graph = GraphLayers::new(
            invalid_links,
            EntryPoints {
                entry_points: vec![super::super::EntryPoint {
                    point_id: 0,
                    level: 0,
                }],
                extra_entry_points: Vec::new(),
            },
            VisitedPool::new(),
            HnswM::new(8),
        );
        let index = HnswIndex::new(
            HnswConfig::new(8, 32),
            graph,
            make_storage(&[vec![0.0], vec![1.0]]),
            DistanceMetric::Euclidean,
        );

        assert!(index
            .serialize()
            .unwrap_err()
            .to_string()
            .contains("out of bounds"));
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
            .search_many_prepared(&queries, 2, &params, None, HnswSearchStrategy::ExactScan)
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
                HnswSearchStrategy::ExactScan,
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
                HnswSearchStrategy::ExactScan,
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
        let policy = config.search_policy();
        let index = HnswIndex::build(storage, config, DistanceMetric::Euclidean);

        let mut small_filter = RoaringBitmap::new();
        for idx in (0..vectors.len() as u32).step_by(32) {
            small_filter.insert(idx);
        }

        let mut large_filter = small_filter.clone();
        for idx in (1..vectors.len() as u32).step_by(8) {
            large_filter.insert(idx);
        }

        assert!(!index.should_use_plain_scan(
            HnswSearchFilter::None,
            &policy,
            HnswSearchStrategy::UnfilteredGraph
        ));
        assert!(index.should_use_plain_scan(
            HnswSearchFilter::Predicate(&small_filter),
            &policy,
            HnswSearchStrategy::ExactScan
        ));
        assert!(index.should_use_plain_scan(
            HnswSearchFilter::Predicate(&small_filter),
            &policy,
            HnswSearchStrategy::AdaptiveFilteredGraph
        ));
        assert!(!index.should_use_plain_scan(
            HnswSearchFilter::Predicate(&large_filter),
            &policy,
            HnswSearchStrategy::AdaptiveFilteredGraph
        ));
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
        };
        let top_k = 6;

        let batch = index
            .search_many_prepared(
                &prepared_queries,
                top_k,
                &params,
                Some(&filter),
                HnswSearchStrategy::ExactScan,
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
    fn test_hnsw_directory_load_uses_mmap_graph_and_norms() {
        let vectors: Vec<Vec<f32>> = (0..32).map(|i| vec![i as f32, (i % 7) as f32]).collect();
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(8, 50).with_plain_scan_threshold(0);
        let index = HnswIndex::build(storage.clone(), config, DistanceMetric::Cosine);

        let temp_dir = TempDir::new().unwrap();
        index.save(temp_dir.path()).unwrap();

        let loaded =
            HnswIndex::load(temp_dir.path(), storage.clone()).expect("load index from directory");
        assert!(loaded.graph.links.is_mmap_backed());
        assert!(loaded
            .vector_storage
            .cosine_inverse_norms()
            .expect("cosine norms")
            .is_mmap_backed());

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
    fn test_heuristic_reduces_dominated_links() {
        let num_vectors = 512;
        let dim = 64;
        let vectors = make_sift_like_vectors(7, num_vectors, dim, 32);
        let config = HnswConfig::new(8, 64)
            .with_plain_scan_threshold(0)
            .with_ef(96);
        let levels = deterministic_levels(num_vectors, config.m, 99);

        let no_heuristic =
            build_index_with_levels(&vectors, &levels, config, DistanceMetric::Euclidean, false);
        let with_heuristic =
            build_index_with_levels(&vectors, &levels, config, DistanceMetric::Euclidean, true);

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
