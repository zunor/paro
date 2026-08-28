// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Persistence
//!
//! Save, load, serialize, and search HNSW indexes.

use super::artifact_integrity::{
    append_integrity_table_streaming, ArtifactIntegrity, ArtifactIntegrityBacking,
    IntegrityDescriptor,
};
use super::entry_points::EntryPoints;
use super::graph::{GraphLayers, GraphSearchLimits, PredicatePartitionSeeds};
use super::graph_links::GraphLinks;
use super::hnsw_builder::{
    hnsw_build_pool, hnsw_current_build_parallelism, HnswForegroundQueryGuard,
};
use super::predicate_scan::{
    PredicateScanBuildBlock, PredicateScanBuildColumn, PredicateScanLayout,
};
use super::search_context::ScanTopK;
use super::vector_storage::{
    prepare_build_vector_storage, ArtifactVectorStorage, CosineInverseNorms, I16BuildBacking,
    IndexedVectorStorage, PointRemappedBuildVectorStorage, SymmetricI16BuildVectorStorage,
    VectorStorage,
};
use super::{
    BatchScorer, DistanceMetric, GraphLayersBuilder, GraphVectorScorer, HnswBuildContract,
    HnswBuildStopCheck, HnswBuildVectorEncoding, HnswDistanceCostModel, HnswExactScanKind,
    HnswExactScanWorkload, HnswFilterTopologyContract, HnswSearchFilter, HnswSearchOutcome,
    HnswSearchPath, HnswSearchPolicy, HnswSearchResult, HnswSearchStrategy, PointOffset,
    PreparedQuery, ScoreType, ScoredPoint, SearchAlgorithm, SearchParams, VectorScorer,
    HNSW_BUILD_CONTRACT_VERSION,
};
#[cfg(test)]
use super::{HnswConfig, HnswSegmentSearchInput};
use crate::index::{ExactRowPartitions, ExactRowSet};
use crate::metrics::storage_metrics;
use crate::search::segment_dispatch::map_search_tasks;
use crate::search::{ResourceBudget, SearchWorkBudget};
use crate::statistics::{
    split_stats_trailer, write_stats_trailer, HnswBatchTelemetry, HnswIndexStatistics,
    SearchTelemetry,
};
use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};
use paro_common::error;
use paro_common::error::Result;
use rayon::prelude::*;
#[cfg(test)]
use roaring::RoaringBitmap;
use std::fs;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const HNSW_ARTIFACT_MAGIC: [u8; 4] = *b"HNSW";
/// Version 19 aligns compact routing-code rows to a cache-line boundary inside
/// every envelope. Version 18 adds the chunk-authenticated integrity hierarchy.
/// Version 17 persists compact routing codes beside canonical f32 vectors so
/// graph navigation and graph construction use the same metric image while
/// exact re-ranking retains SQL-visible f32 semantics.
/// Version 16 persists the construction vector encoding in the build contract.
/// Version 15 binds artifacts to canonical unordered point-pair scoring during
/// construction and repair, including cosine inverse-norm multiplication.
/// Version 14 binds every fixed level-0 record to its durable degree capacity;
/// observed data can no longer change artifact width.
/// Version 13 replaces level-0 CSR offsets with fixed-stride adjacency records.
/// Degree and neighbors now share one mmap stream, so graph expansion no
/// longer pays an independent random offset-table lookup. Version 11's
/// canonical predicate dictionary keys remain part of the format. Earlier
/// graph layouts are intentionally rejected rather than translated at open.
pub const HNSW_ARTIFACT_FORMAT_VERSION: u32 = 19;
const HNSW_ARTIFACT_VERSION: u32 = HNSW_ARTIFACT_FORMAT_VERSION;
pub(crate) const HNSW_ARTIFACT_HEADER_LEN: usize = 208;
const HNSW_NORM_COUNT_FIELD: usize = 48;
const HNSW_VECTOR_COUNT_FIELD: usize = 56;
const HNSW_VECTOR_DIM_FIELD: usize = 64;
const HNSW_VECTOR_ENCODING_FIELD: usize = 68;
const HNSW_PRIMARY_COUNT_FIELD: usize = 72;
const HNSW_EXTRA_COUNT_FIELD: usize = 76;
const HNSW_INTEGRITY_OFFSET_FIELD: usize = 176;
const HNSW_INTEGRITY_LEN_FIELD: usize = 184;
const HNSW_ARTIFACT_LEN_FIELD: usize = 192;
const HNSW_INTEGRITY_CHECKSUM_FIELD: usize = 200;
const HNSW_HEADER_CHECKSUM_FIELD: usize = 204;
const HNSW_VECTOR_ENCODING_F32_LE: u32 = 1;

fn build_vector_encoding_tag(encoding: HnswBuildVectorEncoding) -> u8 {
    match encoding {
        HnswBuildVectorEncoding::ExactF32 => 0,
        HnswBuildVectorEncoding::SymmetricI16 { .. } => 1,
    }
}

fn build_vector_encoding_from_tag(
    tag: u8,
    routing_dimensions: u16,
) -> Result<HnswBuildVectorEncoding> {
    match tag {
        0 if routing_dimensions == 0 => Ok(HnswBuildVectorEncoding::ExactF32),
        0 => Err(error::data_corrupted(
            "exact-f32 HNSW artifact declares compact routing dimensions",
        )),
        1 => HnswBuildVectorEncoding::symmetric_i16(u32::from(routing_dimensions))
            .map_err(|err| error::data_corrupted(err.to_string())),
        _ => Err(error::data_corrupted(format!(
            "unknown HNSW build vector encoding tag {tag}"
        ))),
    }
}

#[derive(Debug)]
struct ExactScanLaneResult {
    points: Vec<ScoredPoint>,
    scored_points: u64,
}

#[derive(Debug, Clone, Copy)]
struct CoveringScanRange<'a> {
    range: super::predicate_scan::PredicateScanRangeRef<'a>,
    first_row: usize,
    len: usize,
}

#[derive(Debug, Clone, Copy)]
struct ExactPostingRange<'a> {
    posting: &'a roaring::RoaringBitmap,
    point_base: PointOffset,
    first_rank: u32,
    len: u32,
}

#[derive(Debug, Clone, Copy)]
enum ExactPhysicalRange<'a> {
    Covering(CoveringScanRange<'a>),
    Posting(ExactPostingRange<'a>),
    Dense { first_point: PointOffset, len: u32 },
}

impl ExactPhysicalRange<'_> {
    fn len(self) -> u64 {
        match self {
            Self::Covering(range) => range.len as u64,
            Self::Posting(range) => u64::from(range.len),
            Self::Dense { len, .. } => u64::from(len),
        }
    }

    fn slice(self, first: u64, len: u64) -> Result<Self> {
        match self {
            Self::Covering(range) => Ok(Self::Covering(CoveringScanRange {
                range: range.range,
                first_row: range
                    .first_row
                    .checked_add(usize::try_from(first).map_err(|_| {
                        error::out_of_range("covering scan slice offset exceeds usize")
                    })?)
                    .ok_or_else(|| error::data_corrupted("covering scan slice offset overflow"))?,
                len: usize::try_from(len)
                    .map_err(|_| error::out_of_range("covering scan slice exceeds usize"))?,
            })),
            Self::Posting(range) => Ok(Self::Posting(ExactPostingRange {
                posting: range.posting,
                point_base: range.point_base,
                first_rank: range
                    .first_rank
                    .checked_add(u32::try_from(first).map_err(|_| {
                        error::out_of_range("posting scan slice offset exceeds u32")
                    })?)
                    .ok_or_else(|| error::data_corrupted("posting scan slice offset overflow"))?,
                len: u32::try_from(len)
                    .map_err(|_| error::out_of_range("posting scan slice exceeds u32"))?,
            })),
            Self::Dense { first_point, .. } => {
                Ok(Self::Dense {
                    first_point: first_point
                        .checked_add(u32::try_from(first).map_err(|_| {
                            error::out_of_range("dense scan slice offset exceeds u32")
                        })?)
                        .ok_or_else(|| error::data_corrupted("dense scan slice offset overflow"))?,
                    len: u32::try_from(len)
                        .map_err(|_| error::out_of_range("dense scan slice exceeds u32"))?,
                })
            }
        }
    }
}

#[derive(Debug, Default)]
struct ExactScanLane<'a> {
    ranges: Vec<ExactPhysicalRange<'a>>,
}

#[derive(Debug, Default)]
struct ExactPhysicalScanPlan<'a> {
    ranges: Vec<ExactPhysicalRange<'a>>,
    covering_rows: u64,
    base_rows: u64,
}

impl ExactPhysicalScanPlan<'_> {
    fn row_count(&self) -> u64 {
        self.covering_rows.saturating_add(self.base_rows)
    }

    fn kind(&self) -> HnswExactScanKind {
        match (self.covering_rows, self.base_rows) {
            (0, _) => HnswExactScanKind::BaseVectors,
            (_, 0) => HnswExactScanKind::PredicateCovering,
            (_, _) => HnswExactScanKind::Hybrid,
        }
    }
}

struct ExactScanResult {
    points: Vec<ScoredPoint>,
    kind: HnswExactScanKind,
}

/// Durable byte alignment for an mmap-backed HNSW envelope. Every nested
/// typed region is aligned relative to the envelope, so its file offset must
/// preserve the same base alignment.
pub const HNSW_ARTIFACT_ALIGNMENT: usize = 64;

fn aligned_offset(offset: usize, alignment: usize) -> Result<usize> {
    offset
        .checked_add(alignment - 1)
        .map(|value| value / alignment * alignment)
        .ok_or_else(|| error::out_of_range("HNSW artifact alignment overflow"))
}

fn alignment_padding(offset: usize, alignment: usize) -> Result<usize> {
    Ok(aligned_offset(offset, alignment)?.saturating_sub(offset))
}

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

fn current_artifact_header(data: &[u8]) -> Result<&[u8]> {
    match hnsw_artifact_compatibility(data)? {
        HnswArtifactCompatibility::Current => {}
        compatibility => {
            return Err(error::artifact_not_ready(
                compatibility
                    .rebuild_reason()
                    .expect("non-current compatibility has a reason"),
            ));
        }
    }
    let header = data.get(..HNSW_ARTIFACT_HEADER_LEN).ok_or_else(|| {
        error::data_corrupted("HNSW artifact is truncated inside its fixed header")
    })?;
    let expected = u32::from_le_bytes(
        header[HNSW_HEADER_CHECKSUM_FIELD..HNSW_HEADER_CHECKSUM_FIELD + 4]
            .try_into()
            .expect("header checksum width"),
    );
    if crc32c::crc32c(&header[..HNSW_HEADER_CHECKSUM_FIELD]) != expected {
        return Err(error::data_corrupted(
            "HNSW artifact fixed header checksum mismatch",
        ));
    }
    Ok(header)
}

fn decode_build_contract(header: &[u8]) -> Result<(HnswBuildContract, usize)> {
    let mut offset = 12;
    let read_u32 = |offset: &mut usize, field| -> Result<u32> {
        Ok(u32::from_le_bytes(
            take_artifact_bytes(header, offset, 4, field)?
                .try_into()
                .expect("u32 width"),
        ))
    };
    let mut contract = HnswBuildContract {
        version: read_u32(&mut offset, "build contract version")?,
        m: read_u32(&mut offset, "m")?,
        m0: read_u32(&mut offset, "m0")?,
        ef_construct: read_u32(&mut offset, "ef_construct")?,
        distance: {
            let tag = take_artifact_bytes(header, &mut offset, 1, "distance")?[0];
            DistanceMetric::from_u8(tag)
                .ok_or_else(|| error::data_corrupted(format!("unknown HNSW distance tag {tag}")))?
        },
        vector_encoding: {
            let encoding = take_artifact_bytes(header, &mut offset, 1, "build vector encoding")?[0];
            let routing_dimensions = u16::from_le_bytes(
                take_artifact_bytes(header, &mut offset, 2, "build routing dimensions")?
                    .try_into()
                    .expect("u16 width"),
            );
            build_vector_encoding_from_tag(encoding, routing_dimensions)?
        },
        build_seed: {
            u64::from_le_bytes(
                take_artifact_bytes(header, &mut offset, 8, "build seed")?
                    .try_into()
                    .expect("u64 width"),
            )
        },
        proposal_wave_size: read_u32(&mut offset, "proposal wave size")?,
        warmup_point_count: read_u32(&mut offset, "warm-up point count")?,
        filter_topology: HnswFilterTopologyContract::default(),
    };
    // Skip counts that precede the filter-topology contract.
    take_artifact_bytes(header, &mut offset, 8, "inverse norm count")?;
    take_artifact_bytes(header, &mut offset, 8, "vector count")?;
    take_artifact_bytes(header, &mut offset, 4, "vector dimension")?;
    take_artifact_bytes(header, &mut offset, 4, "vector encoding")?;
    take_artifact_bytes(header, &mut offset, 4, "primary entry point count")?;
    take_artifact_bytes(header, &mut offset, 4, "extra entry point count")?;
    let filter_version = read_u32(&mut offset, "filter-topology version")?;
    let filter_column_count = read_u32(&mut offset, "filter column count")?;
    let mut filter_column_ids = [0; super::MAX_HNSW_FILTER_COLUMNS];
    for column_id in &mut filter_column_ids {
        *column_id = read_u32(&mut offset, "filter column id")?;
    }
    contract.filter_topology = HnswFilterTopologyContract {
        version: filter_version,
        column_count: filter_column_count,
        column_ids: filter_column_ids,
        target_block_rows: read_u32(&mut offset, "filter block rows")?,
        m: read_u32(&mut offset, "filter m")?,
    };
    contract.validate()?;
    Ok((contract, offset))
}

/// Inspect the authenticated fixed header without touching graph payload
/// pages. `None` means the artifact is structurally recognizable but belongs
/// to an unsupported artifact/build-contract generation and must be rebuilt.
pub(crate) fn hnsw_artifact_build_contract(data: &[u8]) -> Result<Option<HnswBuildContract>> {
    if !matches!(
        hnsw_artifact_compatibility(data)?,
        HnswArtifactCompatibility::Current
    ) {
        return Ok(None);
    }
    let (contract, _) = decode_build_contract(current_artifact_header(data)?)?;
    Ok(Some(contract))
}

const fn distance_tag(distance: DistanceMetric) -> u8 {
    match distance {
        DistanceMetric::Euclidean => 0,
        DistanceMetric::Cosine => 1,
        DistanceMetric::DotProduct => 2,
        DistanceMetric::Manhattan => 3,
    }
}

fn write_f32_slice<W: Write>(writer: &mut W, values: &[f32]) -> Result<()> {
    #[cfg(target_endian = "little")]
    {
        writer.write_all(bytemuck::cast_slice(values))?;
    }
    #[cfg(not(target_endian = "little"))]
    {
        write_f32_iter(writer, values.iter().copied())?;
    }
    Ok(())
}

fn write_f32_iter<W: Write>(writer: &mut W, values: impl Iterator<Item = f32>) -> Result<()> {
    const BUFFER_VALUES: usize = 16 * 1024;
    let mut buffer = vec![0u8; BUFFER_VALUES * std::mem::size_of::<f32>()];
    let mut count = 0usize;
    for value in values {
        buffer[count * 4..count * 4 + 4].copy_from_slice(&value.to_le_bytes());
        count += 1;
        if count == BUFFER_VALUES {
            writer.write_all(&buffer)?;
            count = 0;
        }
    }
    if count != 0 {
        writer.write_all(&buffer[..count * std::mem::size_of::<f32>()])?;
    }
    Ok(())
}

fn append_entry_points<W: Write>(mut writer: W, entries: &[super::EntryPoint]) -> Result<()> {
    for entry in entries {
        writer.write_all(&entry.point_id.to_le_bytes())?;
        let level = u32::try_from(entry.level)
            .map_err(|_| error::out_of_range("HNSW entry-point level exceeds u32"))?;
        writer.write_all(&level.to_le_bytes())?;
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

fn append_predicate_entry_points(
    mut writer: impl Write,
    entries: &[super::PredicateEntryPoint],
) -> Result<()> {
    for entry in entries {
        writer.write_all(&entry.column_id.to_le_bytes())?;
        writer.write_all(&entry.point_id.to_le_bytes())?;
        let level = u32::try_from(entry.level)
            .map_err(|_| error::out_of_range("HNSW predicate entry-point level exceeds u32"))?;
        writer.write_all(&level.to_le_bytes())?;
    }
    Ok(())
}

fn read_predicate_entry_points(
    data: &[u8],
    offset: &mut usize,
    count: usize,
) -> Result<Box<[super::PredicateEntryPoint]>> {
    const ENTRY_BYTES: usize = 3 * std::mem::size_of::<u32>();
    let encoded_len = count
        .checked_mul(ENTRY_BYTES)
        .ok_or_else(|| error::data_corrupted("HNSW predicate entry-point table length overflow"))?;
    if data.len().saturating_sub(*offset) < encoded_len {
        return Err(error::data_corrupted(format!(
            "HNSW predicate entry-point table is truncated: count={count}, remaining={}",
            data.len().saturating_sub(*offset)
        )));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let column_id = u32::from_le_bytes(
            take_artifact_bytes(data, offset, 4, "predicate entry column")?
                .try_into()
                .expect("u32 width"),
        );
        let point_id = u32::from_le_bytes(
            take_artifact_bytes(data, offset, 4, "predicate entry point id")?
                .try_into()
                .expect("u32 width"),
        );
        let level = u32::from_le_bytes(
            take_artifact_bytes(data, offset, 4, "predicate entry point level")?
                .try_into()
                .expect("u32 width"),
        ) as usize;
        entries.push(super::PredicateEntryPoint {
            column_id,
            point_id,
            level,
        });
    }
    Ok(entries.into_boxed_slice())
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

    fn integrity_backing(&self) -> ArtifactIntegrityBacking {
        match self {
            Self::Bytes(bytes) => ArtifactIntegrityBacking::Bytes(bytes.clone()),
            Self::Mmap { mmap, offset, len } => ArtifactIntegrityBacking::Mmap {
                mmap: Arc::clone(mmap),
                offset: *offset,
                len: *len,
            },
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

    fn i16_routing_backing(&self, offset: usize, len: usize) -> Result<I16BuildBacking> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| error::data_corrupted("HNSW routing-code range overflow"))?;
        match self {
            Self::Bytes(bytes) => {
                if end > bytes.len() {
                    return Err(error::data_corrupted(
                        "HNSW routing-code range exceeds byte backing",
                    ));
                }
                Ok(I16BuildBacking::Bytes(bytes.slice(offset..end)))
            }
            Self::Mmap {
                mmap,
                offset: artifact_offset,
                len: artifact_len,
            } => {
                if end > *artifact_len {
                    return Err(error::data_corrupted(
                        "HNSW routing-code range exceeds mmap backing",
                    ));
                }
                Ok(I16BuildBacking::MmapRange {
                    mmap: Arc::clone(mmap),
                    offset: artifact_offset.checked_add(offset).ok_or_else(|| {
                        error::data_corrupted("HNSW routing-code mmap offset overflow")
                    })?,
                    len,
                })
            }
        }
    }

    fn vector_storage(
        &self,
        offset: usize,
        len: usize,
        dimension: usize,
        count: usize,
    ) -> Result<Arc<dyn VectorStorage>> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| error::data_corrupted("HNSW vector artifact range overflow"))?;
        match self {
            Self::Bytes(bytes) => {
                let vector_bytes = bytes.get(offset..end).ok_or_else(|| {
                    error::data_corrupted("HNSW vector artifact range exceeds byte backing")
                })?;
                ArtifactVectorStorage::from_bytes(vector_bytes, dimension, count)
            }
            Self::Mmap {
                mmap,
                offset: artifact_offset,
                len: artifact_len,
            } => {
                if end > *artifact_len {
                    return Err(error::data_corrupted(
                        "HNSW vector artifact range exceeds mmap backing",
                    ));
                }
                ArtifactVectorStorage::from_mmap_range(
                    Arc::clone(mmap),
                    artifact_offset.checked_add(offset).ok_or_else(|| {
                        error::data_corrupted("HNSW vector artifact offset overflow")
                    })?,
                    len,
                    dimension,
                    count,
                )
            }
        }
    }

    fn graph_links(
        &self,
        offset: usize,
        len: usize,
        integrity: Arc<ArtifactIntegrity>,
    ) -> Result<GraphLinks> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| error::data_corrupted("HNSW graph artifact range overflow"))?;
        match self {
            Self::Bytes(bytes) => {
                if end > bytes.len() {
                    return Err(error::data_corrupted(
                        "HNSW graph artifact range exceeds byte backing",
                    ));
                }
                GraphLinks::deserialize_bytes_with_integrity(
                    bytes.slice(offset..end),
                    integrity,
                    offset,
                )
            }
            Self::Mmap {
                mmap,
                offset: artifact_offset,
                len: artifact_len,
            } => GraphLinks::deserialize_mmap_range_with_integrity(
                Arc::clone(mmap),
                artifact_offset
                    .checked_add(offset)
                    .ok_or_else(|| error::data_corrupted("HNSW graph artifact offset overflow"))?,
                if end <= *artifact_len {
                    len
                } else {
                    return Err(error::data_corrupted(
                        "HNSW graph artifact range exceeds mmap backing",
                    ));
                },
                integrity,
                offset,
            ),
        }
    }

    fn predicate_scan(
        &self,
        offset: usize,
        len: usize,
        integrity: Arc<ArtifactIntegrity>,
    ) -> Result<PredicateScanLayout> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| error::data_corrupted("HNSW predicate scan range overflow"))?;
        match self {
            Self::Bytes(bytes) => {
                if end > bytes.len() {
                    return Err(error::data_corrupted(
                        "HNSW predicate scan range exceeds byte backing",
                    ));
                }
                PredicateScanLayout::deserialize_bytes_with_integrity(
                    bytes.slice(offset..end),
                    integrity,
                    offset,
                )
            }
            Self::Mmap {
                mmap,
                offset: artifact_offset,
                len: artifact_len,
            } => PredicateScanLayout::deserialize_mmap_range_with_integrity(
                Arc::clone(mmap),
                artifact_offset.checked_add(offset).ok_or_else(|| {
                    error::data_corrupted("HNSW predicate scan mmap offset overflow")
                })?,
                if end <= *artifact_len {
                    len
                } else {
                    return Err(error::data_corrupted(
                        "HNSW predicate scan range exceeds mmap backing",
                    ));
                },
                integrity,
                offset,
            ),
        }
    }
}

/// A high-level HNSW index structure that combines graph and storage.
pub struct HnswIndex {
    pub build_contract: HnswBuildContract,
    pub graph: GraphLayers,
    pub vector_storage: Arc<dyn VectorStorage>,
    pub(crate) predicate_scan: Option<PredicateScanLayout>,
    persisted_statistics: Option<HnswIndexStatistics>,
    _artifact_integrity: Option<Arc<ArtifactIntegrity>>,
    integrity_scheduled: AtomicBool,
    single_telemetry: Mutex<SearchTelemetry>,
    batch_telemetry: Mutex<HnswBatchTelemetry>,
}

/// Complete deterministic block partition for one configured scalar column.
/// Every segment-local point must occur exactly once across `blocks`.
#[derive(Debug, Clone)]
pub struct HnswFilterColumnBlocks {
    pub column_id: u32,
    pub blocks: Vec<HnswFilterBlock>,
}

#[derive(Debug, Clone)]
pub struct HnswFilterBlock {
    pub dictionary_ordinals: Box<[u32]>,
    pub dictionary_values: Box<[Option<bytes::Bytes>]>,
    pub ordinal_row_counts: Box<[u32]>,
    pub ordinal_fingerprints: Box<[u64]>,
    pub point_ids: Box<[PointOffset]>,
}

/// Deterministic predicate-local graph build input. Columns are aligned with
/// the durable filter-topology contract. Blocks from different columns may
/// overlap, but each column independently partitions the full point domain.
#[derive(Debug, Clone, Default)]
pub struct HnswFilterBlocks {
    pub columns: Vec<HnswFilterColumnBlocks>,
}

struct BuiltPredicateGraph {
    links: GraphLinks,
    entry_points: Box<[super::PredicateEntryPoint]>,
    scan_layout: PredicateScanLayout,
}

struct PreparedPredicateBlock {
    scan_block: PredicateScanBuildBlock,
    graph_point_ids: Arc<[PointOffset]>,
    local_contract: HnswBuildContract,
}

struct BuiltPredicateBlock {
    scan_block: PredicateScanBuildBlock,
    graph_point_ids: Arc<[PointOffset]>,
    local: HnswIndex,
}

impl HnswIndex {
    /// Bind transient search workspaces to the instance-wide buffer pool.
    /// Loaded artifacts may be shared by many cursors, so binding is
    /// idempotent and rejects accidental cross-instance reuse.
    pub fn bind_search_buffer_pool(
        &self,
        buffer_pool: Arc<crate::buffer::BufferPool>,
    ) -> Result<()> {
        self.graph.bind_search_buffer_pool(buffer_pool)
    }

    pub(crate) fn exact_scan_workload(
        &self,
        filter: HnswSearchFilter<'_>,
    ) -> HnswExactScanWorkload {
        filter.exact_scan_workload(self.graph.num_points() as u64, |column_id| {
            self.predicate_scan
                .as_ref()
                .is_some_and(|layout| layout.has_column(column_id))
        })
    }

    pub(crate) fn persisted_statistics(&self) -> Option<&HnswIndexStatistics> {
        self.persisted_statistics.as_ref()
    }

    /// Read the statistics embedded in a newly serialized HNSW artifact.
    ///
    /// The integrity table follows the statistics trailer in artifact v10, so
    /// generic "trailer at end of buffer" parsing is no longer valid. Writers
    /// use the artifact's authenticated fixed header to find the protected
    /// payload boundary instead of duplicating the envelope layout.
    pub(crate) fn serialized_statistics(data: &[u8]) -> Result<HnswIndexStatistics> {
        let header = current_artifact_header(data)?;
        let integrity_offset = usize::try_from(u64::from_le_bytes(
            header[HNSW_INTEGRITY_OFFSET_FIELD..HNSW_INTEGRITY_OFFSET_FIELD + 8]
                .try_into()
                .expect("integrity offset width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW integrity offset exceeds usize"))?;
        let payload = data.get(..integrity_offset).ok_or_else(|| {
            error::data_corrupted("HNSW statistics boundary exceeds artifact length")
        })?;
        let (statistics, _) = split_stats_trailer(payload);
        HnswIndexStatistics::from_bytes(statistics.ok_or_else(|| {
            error::data_corrupted("HNSW artifact is missing its statistics trailer")
        })?)
    }

    pub fn try_new(
        build_contract: HnswBuildContract,
        graph: GraphLayers,
        vector_storage: Arc<dyn VectorStorage>,
    ) -> Result<Self> {
        Self::try_new_with_predicate_scan(build_contract, graph, vector_storage, None)
    }

    fn try_new_with_predicate_scan(
        build_contract: HnswBuildContract,
        graph: GraphLayers,
        vector_storage: Arc<dyn VectorStorage>,
        predicate_scan: Option<PredicateScanLayout>,
    ) -> Result<Self> {
        build_contract.validate()?;
        let distance = build_contract.distance;
        let vector_storage = IndexedVectorStorage::prepare(vector_storage, distance);
        let index = Self {
            build_contract,
            graph,
            vector_storage,
            predicate_scan,
            persisted_statistics: None,
            _artifact_integrity: None,
            integrity_scheduled: AtomicBool::new(false),
            single_telemetry: Mutex::new(SearchTelemetry::default()),
            batch_telemetry: Mutex::new(HnswBatchTelemetry::default()),
        };
        index.validate_artifact_contract()?;
        Ok(index)
    }

    pub(crate) fn artifact_integrity(&self) -> Option<Arc<ArtifactIntegrity>> {
        self._artifact_integrity.as_ref().map(Arc::clone)
    }

    /// Whether authenticated bytes have proved this secondary artifact
    /// corrupt. Callers may quarantine it and fall back to immutable base
    /// vectors; table readability must not depend on a rebuildable index.
    pub(crate) fn integrity_failed(&self) -> bool {
        self._artifact_integrity
            .as_ref()
            .is_some_and(|integrity| integrity.is_corrupt())
    }

    pub(crate) fn try_mark_integrity_scheduled(&self) -> bool {
        self._artifact_integrity
            .as_ref()
            .is_some_and(|integrity| !integrity.is_fully_verified())
            && self
                .integrity_scheduled
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }

    pub(crate) fn clear_integrity_scheduled(&self) {
        self.integrity_scheduled.store(false, Ordering::Release);
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

    pub(crate) fn try_build_in_workspace(
        storage: Arc<dyn VectorStorage>,
        build_contract: HnswBuildContract,
        workspace_dir: &Path,
    ) -> Result<Self> {
        let (pool, _) = hnsw_build_pool()?;
        Self::build_with_controls_and_filter_blocks_in_workspace(
            storage,
            build_contract,
            HnswFilterBlocks::default(),
            Some(pool),
            None,
            Some(workspace_dir),
        )
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
        Self::build_with_controls_and_filter_blocks(
            storage,
            build_contract,
            HnswFilterBlocks::default(),
            pool,
            stop_check,
        )
    }

    pub(crate) fn build_with_controls_and_filter_blocks(
        storage: Arc<dyn VectorStorage>,
        build_contract: HnswBuildContract,
        filter_blocks: HnswFilterBlocks,
        pool: Option<&rayon::ThreadPool>,
        stop_check: Option<&HnswBuildStopCheck>,
    ) -> Result<Self> {
        Self::build_with_controls_and_filter_blocks_in_workspace(
            storage,
            build_contract,
            filter_blocks,
            pool,
            stop_check,
            None,
        )
    }

    pub(crate) fn build_with_controls_and_filter_blocks_in_workspace(
        storage: Arc<dyn VectorStorage>,
        build_contract: HnswBuildContract,
        filter_blocks: HnswFilterBlocks,
        pool: Option<&rayon::ThreadPool>,
        stop_check: Option<&HnswBuildStopCheck>,
        workspace_dir: Option<&Path>,
    ) -> Result<Self> {
        let parallelism = pool.map_or(1, rayon::ThreadPool::current_num_threads);
        Self::build_with_controls_and_filter_blocks_in_workspace_with_parallelism(
            storage,
            build_contract,
            filter_blocks,
            pool,
            parallelism,
            stop_check,
            workspace_dir,
        )
    }

    pub(crate) fn build_with_controls_and_filter_blocks_in_workspace_with_parallelism(
        storage: Arc<dyn VectorStorage>,
        build_contract: HnswBuildContract,
        filter_blocks: HnswFilterBlocks,
        pool: Option<&rayon::ThreadPool>,
        parallelism: usize,
        stop_check: Option<&HnswBuildStopCheck>,
        workspace_dir: Option<&Path>,
    ) -> Result<Self> {
        build_contract.validate()?;
        if !build_contract.filter_topology.is_enabled() && !filter_blocks.columns.is_empty() {
            return Err(error::invalid_input(
                "HNSW filter blocks require an enabled filter-topology contract",
            ));
        }
        let distance = build_contract.distance;
        let storage = IndexedVectorStorage::prepare(storage, distance);
        let storage = prepare_build_vector_storage(
            storage,
            build_contract.vector_encoding,
            build_contract.build_seed,
            workspace_dir,
        )?;
        Self::build_prepared_with_controls_and_filter_blocks(
            storage,
            build_contract,
            filter_blocks,
            pool,
            parallelism,
            stop_check,
        )
    }

    /// Build from a storage whose construction metric has already been
    /// prepared for `build_contract`.
    ///
    /// Predicate-local graphs project point ids onto the generation's routing
    /// space and must enter here. Sending them through the public preparation
    /// boundary again would derive block-local dimensions/scales and silently
    /// construct a different metric from the parent graph.
    fn build_prepared_with_controls_and_filter_blocks(
        storage: Arc<dyn VectorStorage>,
        build_contract: HnswBuildContract,
        filter_blocks: HnswFilterBlocks,
        pool: Option<&rayon::ThreadPool>,
        parallelism: usize,
        stop_check: Option<&HnswBuildStopCheck>,
    ) -> Result<Self> {
        let distance = build_contract.distance;
        let num_vectors = storage.num_vectors();
        if num_vectors > PointOffset::MAX as usize {
            return Err(error::configuration_limit_exceeded(
                "HNSW artifact exceeds the u32 point-id address space",
            ));
        }
        // Diverse neighbor selection is required for clustered vector sets;
        // nearest-only truncation forms disconnected local components.
        let parallelism = parallelism
            .max(1)
            .min(pool.map_or(1, rayon::ThreadPool::current_num_threads));
        let visited_capacity = parallelism;
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
            builder.insert_single_point(
                point_order.point_at(position),
                storage.as_ref(),
                distance,
            )?;
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
                    let positions = (wave_start..wave_end).collect::<Vec<_>>();
                    let wave_parallelism = hnsw_current_build_parallelism(parallelism);
                    let chunk_size = positions.len().div_ceil(wave_parallelism).max(1);
                    pool.install(|| {
                        positions
                            .par_chunks(chunk_size)
                            .map(|chunk| {
                                chunk
                                    .iter()
                                    .map(|&position| {
                                        builder.propose_new_point(
                                            point_order.point_at(position),
                                            &entry_points,
                                            storage.as_ref(),
                                            distance,
                                        )
                                    })
                                    .collect::<Result<Vec<_>>>()
                            })
                            .collect::<Result<Vec<Vec<_>>>>()
                            .map(|chunks| chunks.into_iter().flatten().collect())
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
                        .collect::<Result<Vec<_>>>()
                }?;
                if let Some(pool) = pool {
                    pool.install(|| {
                        builder.publish_frozen_wave(
                            proposals,
                            storage.as_ref(),
                            distance,
                            hnsw_current_build_parallelism(parallelism),
                        )
                    });
                } else {
                    builder.publish_frozen_wave(proposals, storage.as_ref(), distance, 1);
                }
            }
        }

        let (links, entry_points) = builder.into_graph_data()?;
        let predicate_graph = if build_contract.filter_topology.is_enabled() {
            Some(Self::build_predicate_links(
                &storage,
                &build_contract,
                &links,
                filter_blocks,
                pool,
                parallelism,
                stop_check,
            )?)
        } else {
            None
        };
        let (graph, predicate_scan) = match predicate_graph {
            Some(predicate_graph) => (
                GraphLayers::new_with_predicate_links(
                    links,
                    predicate_graph.links,
                    predicate_graph.entry_points,
                    entry_points,
                    (&build_contract).into(),
                ),
                Some(predicate_graph.scan_layout),
            ),
            None => (
                GraphLayers::new(links, entry_points, (&build_contract).into()),
                None,
            ),
        };
        Self::try_new_with_predicate_scan(build_contract, graph, storage, predicate_scan)
    }

    fn build_predicate_links(
        storage: &Arc<dyn VectorStorage>,
        build_contract: &HnswBuildContract,
        base_links: &GraphLinks,
        filter_blocks: HnswFilterBlocks,
        pool: Option<&rayon::ThreadPool>,
        parallelism: usize,
        stop_check: Option<&HnswBuildStopCheck>,
    ) -> Result<BuiltPredicateGraph> {
        let mut merged = vec![Vec::<Vec<PointOffset>>::new(); storage.num_vectors()];
        let mut predicate_entry_points = Vec::new();
        let mut scan_columns = Vec::with_capacity(filter_blocks.columns.len());
        let dimension = storage.vector_dim();
        let filter_m = build_contract.filter_topology.m;
        let expected_columns = build_contract.filter_topology.columns();
        let actual_columns = filter_blocks
            .columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>();
        if actual_columns != expected_columns {
            return Err(error::invalid_input(format!(
                "HNSW filter-block columns {:?} differ from durable contract {:?}",
                actual_columns, expected_columns
            )));
        }

        let mut global_block_index = 0usize;
        for column in filter_blocks.columns {
            let column_id = column.column_id;
            let mut block_by_point = vec![u32::MAX; storage.num_vectors()];
            let mut scan_blocks = Vec::with_capacity(column.blocks.len());
            let mut prepared_blocks = Vec::with_capacity(column.blocks.len());
            for (block_position, block) in column.blocks.into_iter().enumerate() {
                let scan_block = PredicateScanBuildBlock {
                    dictionary_ordinals: block.dictionary_ordinals,
                    dictionary_values: block.dictionary_values,
                    ordinal_row_counts: block.ordinal_row_counts,
                    ordinal_fingerprints: block.ordinal_fingerprints,
                    row_ids: block.point_ids,
                };
                // Predicate topology and covering scans have different
                // physical locality requirements. Keep graph-local point ids
                // in stable segment-row order so changing scan encoding never
                // perturbs topology quality; keep the scan copy in exact
                // ordinal-run order so every posting is contiguous.
                let mut graph_point_ids = scan_block.row_ids.to_vec();
                graph_point_ids.sort_unstable();
                let graph_point_ids: Arc<[PointOffset]> = Arc::from(graph_point_ids);
                let point_ids = graph_point_ids.as_ref();
                if stop_check.is_some_and(HnswBuildStopCheck::should_stop) {
                    return Err(error::query_canceled());
                }
                if point_ids.is_empty() {
                    return Err(error::invalid_input(
                        "HNSW filter blocks must not contain empty partitions",
                    ));
                }
                let block_index = u32::try_from(block_position).map_err(|_| {
                    error::configuration_limit_exceeded(
                        "HNSW filter topology exceeds the u32 block-id space",
                    )
                })?;
                for &point_id in point_ids.iter() {
                    if point_id as usize >= storage.num_vectors() {
                        return Err(error::data_corrupted(
                            "HNSW filter block point id exceeds vector cardinality",
                        ));
                    }
                    if std::mem::replace(&mut block_by_point[point_id as usize], block_index)
                        != u32::MAX
                    {
                        return Err(error::data_corrupted(format!(
                            "HNSW filter column {} contains duplicate point {}",
                            column_id, point_id
                        )));
                    }
                }

                let mut local_contract = *build_contract;
                local_contract.m = filter_m;
                local_contract.m0 = filter_m.saturating_mul(2);
                local_contract.ef_construct = local_contract.ef_construct.max(filter_m);
                local_contract.build_seed = splitmix64(
                    build_contract.build_seed
                        ^ 0x5052_4544_4943_4154
                        ^ u64::from(column_id).rotate_left(17)
                        ^ global_block_index.saturating_add(block_position) as u64,
                );
                local_contract.filter_topology = Default::default();
                prepared_blocks.push(PreparedPredicateBlock {
                    scan_block,
                    graph_point_ids,
                    local_contract,
                });
            }
            global_block_index = global_block_index.saturating_add(prepared_blocks.len());

            Self::for_each_built_predicate_block(
                storage,
                prepared_blocks,
                build_contract.filter_topology.target_block_rows,
                pool,
                parallelism,
                stop_check,
                |built| {
                    let BuiltPredicateBlock {
                        scan_block,
                        graph_point_ids,
                        local,
                    } = built;
                    let point_ids = graph_point_ids.as_ref();
                    for (local_point, &global_point) in point_ids.iter().enumerate() {
                        let local_point = local_point as PointOffset;
                        let levels = local.graph.links.num_levels(local_point)?;
                        let global_levels = &mut merged[global_point as usize];
                        if global_levels.len() < levels {
                            global_levels.resize_with(levels, Vec::new);
                        }
                        for (level, global_links) in
                            global_levels.iter_mut().take(levels).enumerate()
                        {
                            local.graph.links.for_each_link(
                                local_point,
                                level,
                                |local_neighbor| {
                                    global_links.push(point_ids[local_neighbor as usize]);
                                },
                            )?;
                        }
                    }
                    for entry in &local.graph.entry_points.entry_points {
                        predicate_entry_points.push(super::PredicateEntryPoint {
                            column_id,
                            point_id: point_ids[entry.point_id as usize],
                            level: entry.level,
                        });
                    }
                    scan_blocks.push(scan_block);
                    Ok(())
                },
            )?;
            if block_by_point.contains(&u32::MAX) {
                return Err(error::data_corrupted(format!(
                    "HNSW filter column {} does not cover the complete vector domain",
                    column_id
                )));
            }
            scan_columns.push(PredicateScanBuildColumn {
                column_id,
                blocks: scan_blocks,
            });

            // Local block graphs make selective predicates dense, but a range
            // spanning several blocks would otherwise be a disjoint union and
            // could only reach blocks discovered by the base routing beam.
            // Reuse a bounded number of deterministic base-graph edges that
            // cross block boundaries. Exact row admission still decides which
            // of these bridges are traversable for a concrete predicate.
            // This keeps equality filters inside their local graph while range
            // filters obtain a connected, vector-aware predicate surface.
            let cross_block_degree = usize::try_from(filter_m).unwrap_or(usize::MAX);
            for point in 0..storage.num_vectors() {
                let point_id = point as PointOffset;
                let point_block = block_by_point[point];
                let levels = &mut merged[point];
                if levels.is_empty() {
                    levels.push(Vec::new());
                }
                let links = &mut levels[0];
                let mut added = 0usize;
                base_links.for_each_link(point_id, 0, |neighbor| {
                    if added < cross_block_degree
                        && block_by_point[neighbor as usize] != point_block
                    {
                        links.push(neighbor);
                        added += 1;
                    }
                })?;
            }
        }

        for levels in &mut merged {
            for links in levels {
                links.sort_unstable();
                links.dedup();
            }
        }
        predicate_entry_points
            .sort_unstable_by_key(|entry| (entry.column_id, entry.point_id, entry.level));
        Ok(BuiltPredicateGraph {
            // Each configured predicate column contributes at most 2*filter_m
            // local links plus filter_m deterministic cross-block links. The
            // merged multi-column graph therefore owns an explicit 3*m*C
            // capacity; persist that contract, never an observed maximum.
            links: GraphLinks::try_new_from_edges(
                merged,
                build_contract.filter_topology.merged_level0_stride()?,
            )?,
            entry_points: predicate_entry_points.into_boxed_slice(),
            scan_layout: PredicateScanLayout::from_build_columns(
                dimension,
                storage.num_vectors(),
                scan_columns,
                Arc::clone(storage),
            )?,
        })
    }

    fn for_each_built_predicate_block<F>(
        storage: &Arc<dyn VectorStorage>,
        prepared_blocks: Vec<PreparedPredicateBlock>,
        target_block_rows: u32,
        pool: Option<&rayon::ThreadPool>,
        parallelism: usize,
        stop_check: Option<&HnswBuildStopCheck>,
        mut visit: F,
    ) -> Result<()>
    where
        F: FnMut(BuiltPredicateBlock) -> Result<()>,
    {
        let oversized_rows = usize::try_from(target_block_rows)
            .unwrap_or(usize::MAX)
            .saturating_mul(2);
        let mut remaining = prepared_blocks.into_iter().peekable();
        while let Some(next) = remaining.peek() {
            let worker_count = hnsw_current_build_parallelism(parallelism)
                .min(pool.map_or(1, rayon::ThreadPool::current_num_threads))
                .max(1);
            if worker_count == 1 || next.graph_point_ids.len() > oversized_rows {
                let block = remaining.next().expect("peeked predicate block exists");
                visit(Self::build_predicate_block(
                    storage,
                    block,
                    pool,
                    worker_count,
                    stop_check,
                )?)?;
                continue;
            }

            // Normal blocks are independent immutable subgraphs. Build one
            // bounded wave with a single worker per block, then merge in
            // durable block order. This exposes the serial warm-up prefix of
            // every local graph to process-wide work stealing without nested
            // Rayon pools or retaining every local graph until the column is
            // complete. A posting that cannot be split and grows beyond twice
            // the contract target is built alone with point-level parallelism.
            let mut wave = Vec::with_capacity(worker_count);
            while wave.len() < worker_count
                && remaining
                    .peek()
                    .is_some_and(|block| block.graph_point_ids.len() <= oversized_rows)
            {
                wave.push(remaining.next().expect("peeked predicate block exists"));
            }
            if wave.len() == 1 {
                visit(Self::build_predicate_block(
                    storage,
                    wave.pop().expect("single predicate block exists"),
                    pool,
                    worker_count,
                    stop_check,
                )?)?;
            } else if let Some(pool) = pool {
                let wave_results = pool.install(|| {
                    wave.into_par_iter()
                        .map(|block| {
                            Self::build_predicate_block(storage, block, None, 1, stop_check)
                        })
                        .collect::<Result<Vec<_>>>()
                })?;
                for result in wave_results {
                    visit(result)?;
                }
            } else {
                return Err(error::internal(
                    "parallel HNSW predicate-block wave requires a build pool",
                ));
            }
        }
        Ok(())
    }

    fn build_predicate_block(
        storage: &Arc<dyn VectorStorage>,
        prepared: PreparedPredicateBlock,
        pool: Option<&rayon::ThreadPool>,
        parallelism: usize,
        stop_check: Option<&HnswBuildStopCheck>,
    ) -> Result<BuiltPredicateBlock> {
        if stop_check.is_some_and(HnswBuildStopCheck::should_stop) {
            return Err(error::query_canceled());
        }
        let local = Self::build_prepared_with_controls_and_filter_blocks(
            Arc::new(PointRemappedBuildVectorStorage::try_new(
                Arc::clone(storage),
                Arc::clone(&prepared.graph_point_ids),
            )?),
            prepared.local_contract,
            HnswFilterBlocks::default(),
            pool,
            parallelism,
            stop_check,
        )?;
        Ok(BuiltPredicateBlock {
            scan_block: prepared.scan_block,
            graph_point_ids: prepared.graph_point_ids,
            local,
        })
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
        let predicate_links_path = directory.join("predicate_graph_links.bin");
        let predicate_entry_points_path = directory.join("predicate_entry_points.json");
        let predicate_scan_path = directory.join("predicate_scan.bin");
        if let Some(predicate_links) = &self.graph.predicate_links {
            predicate_links.save(&predicate_links_path)?;
            let encoded = serde_json::to_string_pretty(&self.graph.predicate_entry_points)
                .map_err(|e| error::serialization_error(e.to_string()))?;
            fs::write(&predicate_entry_points_path, encoded).map_err(error::io)?;
            self.predicate_scan
                .as_ref()
                .ok_or_else(|| {
                    error::data_corrupted("predicate graph is missing its covering scan layout")
                })?
                .save(&predicate_scan_path)?;
        } else {
            if predicate_links_path.exists() {
                fs::remove_file(predicate_links_path).map_err(error::io)?;
            }
            if predicate_entry_points_path.exists() {
                fs::remove_file(predicate_entry_points_path).map_err(error::io)?;
            }
            if predicate_scan_path.exists() {
                fs::remove_file(predicate_scan_path).map_err(error::io)?;
            }
        }

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
        let predicate_graph = if build_contract.filter_topology.is_enabled() {
            let path = directory.join("predicate_graph_links.bin");
            let links = GraphLinks::load_mmap(&path).or_else(|_| GraphLinks::load(&path))?;
            let entries = fs::read_to_string(directory.join("predicate_entry_points.json"))
                .map_err(error::io)?;
            let entry_points = serde_json::from_str::<Vec<super::PredicateEntryPoint>>(&entries)
                .map_err(|e| error::serialization_error(e.to_string()))?
                .into_boxed_slice();
            let scan_layout =
                PredicateScanLayout::load_mmap(&directory.join("predicate_scan.bin"))?;
            Some((links, entry_points, scan_layout))
        } else {
            None
        };

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
        let (graph, predicate_scan) = match predicate_graph {
            Some((predicate_links, predicate_entry_points, scan_layout)) => (
                GraphLayers::new_with_predicate_links(
                    links,
                    predicate_links,
                    predicate_entry_points,
                    entry_points,
                    (&build_contract).into(),
                ),
                Some(scan_layout),
            ),
            None => (
                GraphLayers::new(links, entry_points, (&build_contract).into()),
                None,
            ),
        };

        Self::try_new_with_predicate_scan(build_contract, graph, vector_storage, predicate_scan)
    }

    /// Serialize HNSW index to a byte vector for embedding in segments.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut cursor = Cursor::new(Vec::new());
        self.serialize_into_seekable(&mut cursor, 0)?;
        Ok(cursor.into_inner())
    }

    /// Stream one self-contained HNSW envelope into an already positioned
    /// seekable artifact file. Only compact metadata/checksum tables are kept
    /// in memory; vector, predicate-scan, and graph payloads are written from
    /// their immutable source regions.
    pub(crate) fn serialize_into_seekable<RW: Read + Write + Seek>(
        &self,
        writer: &mut RW,
        artifact_start: u64,
    ) -> Result<u64> {
        // Inline and sidecar generations are published from this envelope.
        // Run the expensive semantic verifier here, while the freshly built
        // graph is hot, instead of imposing O(E) work on every open.
        self.verify_integrity()?;
        let predicate_scan = match (
            self.build_contract.filter_topology.is_enabled(),
            self.predicate_scan.as_ref(),
        ) {
            (true, Some(layout)) => Some(layout),
            (false, None) => None,
            (true, None) => {
                return Err(error::data_corrupted(
                    "enabled HNSW filter topology is missing its covering scan layout",
                ))
            }
            (false, Some(_)) => {
                return Err(error::data_corrupted(
                    "HNSW covering scan layout requires an enabled filter topology",
                ))
            }
        };
        let predicate_scan_len =
            predicate_scan.map_or(0, PredicateScanLayout::serialized_size_bytes);
        let mut header = Vec::with_capacity(HNSW_ARTIFACT_HEADER_LEN);
        header.extend_from_slice(&HNSW_ARTIFACT_MAGIC);
        header.extend_from_slice(&HNSW_ARTIFACT_VERSION.to_le_bytes());
        header.extend_from_slice(&(HNSW_ARTIFACT_HEADER_LEN as u32).to_le_bytes());
        header.extend_from_slice(&self.build_contract.version.to_le_bytes());
        header.extend_from_slice(&self.build_contract.m.to_le_bytes());
        header.extend_from_slice(&self.build_contract.m0.to_le_bytes());
        header.extend_from_slice(&self.build_contract.ef_construct.to_le_bytes());
        header.push(distance_tag(self.build_contract.distance));
        header.push(build_vector_encoding_tag(
            self.build_contract.vector_encoding,
        ));
        header.extend_from_slice(
            &self
                .build_contract
                .vector_encoding
                .routing_dimensions()
                .unwrap_or(0)
                .to_le_bytes(),
        );
        header.extend_from_slice(&self.build_contract.build_seed.to_le_bytes());
        header.extend_from_slice(&self.build_contract.proposal_wave_size.to_le_bytes());
        header.extend_from_slice(&self.build_contract.warmup_point_count.to_le_bytes());
        let norm_count = self
            .vector_storage
            .cosine_inverse_norms()
            .map_or(0, CosineInverseNorms::len);
        let norms = self.vector_storage.cosine_inverse_norms();
        header.extend_from_slice(&(norm_count as u64).to_le_bytes());
        let vector_count = u64::try_from(self.vector_storage.num_vectors())
            .map_err(|_| error::out_of_range("HNSW vector count exceeds u64"))?;
        let vector_dim = u32::try_from(self.vector_storage.vector_dim())
            .map_err(|_| error::out_of_range("HNSW vector dimension exceeds u32"))?;
        header.extend_from_slice(&vector_count.to_le_bytes());
        header.extend_from_slice(&vector_dim.to_le_bytes());
        header.extend_from_slice(&HNSW_VECTOR_ENCODING_F32_LE.to_le_bytes());
        let primary_count = u32::try_from(self.graph.entry_points.entry_points.len())
            .map_err(|_| error::out_of_range("too many HNSW primary entry points"))?;
        let extra_count = u32::try_from(self.graph.entry_points.extra_entry_points.len())
            .map_err(|_| error::out_of_range("too many HNSW extra entry points"))?;
        header.extend_from_slice(&primary_count.to_le_bytes());
        header.extend_from_slice(&extra_count.to_le_bytes());
        let filter_topology = self.build_contract.filter_topology;
        header.extend_from_slice(&filter_topology.version.to_le_bytes());
        header.extend_from_slice(&filter_topology.column_count.to_le_bytes());
        for column_id in filter_topology.column_ids {
            header.extend_from_slice(&column_id.to_le_bytes());
        }
        header.extend_from_slice(&filter_topology.target_block_rows.to_le_bytes());
        header.extend_from_slice(&filter_topology.m.to_le_bytes());
        let routing = self.vector_storage.i16_routing_view();
        let routing_metadata_len = routing.map_or(0, |view| {
            view.selected_dimensions
                .len()
                .saturating_mul(std::mem::size_of::<u32>() + std::mem::size_of::<f32>())
        });
        let routing_code_len = routing.map_or(0, |view| view.codes.len());
        header.extend_from_slice(
            &u64::try_from(routing_metadata_len)
                .map_err(|_| error::out_of_range("HNSW routing metadata length exceeds u64"))?
                .to_le_bytes(),
        );
        header.extend_from_slice(
            &u64::try_from(routing_code_len)
                .map_err(|_| error::out_of_range("HNSW routing-code length exceeds u64"))?
                .to_le_bytes(),
        );
        let base_graph_len = self.graph.links.serialized_size_bytes();
        let predicate_graph_len = self
            .graph
            .predicate_links
            .as_ref()
            .map_or(0, GraphLinks::serialized_size_bytes);
        header.extend_from_slice(&base_graph_len.to_le_bytes());
        header.extend_from_slice(&predicate_graph_len.to_le_bytes());
        let predicate_entry_count = u64::try_from(self.graph.predicate_entry_points.len())
            .map_err(|_| error::out_of_range("too many HNSW predicate entry points"))?;
        header.extend_from_slice(&predicate_entry_count.to_le_bytes());
        header.extend_from_slice(
            &u64::try_from(predicate_scan_len)
                .map_err(|_| error::out_of_range("HNSW predicate scan length exceeds u64"))?
                .to_le_bytes(),
        );
        header.extend_from_slice(&[0; 32]);
        debug_assert_eq!(header.len(), HNSW_ARTIFACT_HEADER_LEN);

        writer.seek(SeekFrom::Start(artifact_start))?;
        writer.write_all(&header)?;
        let vector_padding = alignment_padding(HNSW_ARTIFACT_HEADER_LEN, HNSW_ARTIFACT_ALIGNMENT)?;
        writer.write_all(&[0; HNSW_ARTIFACT_ALIGNMENT][..vector_padding])?;
        self.vector_storage
            .try_for_each_contiguous_chunk(&mut |vectors| write_f32_slice(writer, vectors))?;
        if let Some(norms) = norms {
            write_f32_iter(writer, norms.iter())?;
        }
        if let Some(routing) = routing {
            for &source_dimension in routing.selected_dimensions {
                writer.write_all(
                    &u32::try_from(source_dimension)
                        .map_err(|_| {
                            error::out_of_range("HNSW routing source dimension exceeds u32")
                        })?
                        .to_le_bytes(),
                )?;
            }
            write_f32_iter(writer, routing.scales.iter().copied())?;
            let relative_position = usize::try_from(
                writer
                    .stream_position()?
                    .checked_sub(artifact_start)
                    .ok_or_else(|| error::internal("HNSW writer moved before artifact start"))?,
            )
            .map_err(|_| error::out_of_range("HNSW artifact offset exceeds usize"))?;
            let padding = alignment_padding(relative_position, HNSW_ARTIFACT_ALIGNMENT)?;
            writer.write_all(&[0; HNSW_ARTIFACT_ALIGNMENT][..padding])?;
            writer.write_all(routing.codes)?;
            if let Some(norms) = routing.inverse_norms {
                write_f32_iter(writer, norms.iter())?;
            }
        }

        append_entry_points(&mut *writer, &self.graph.entry_points.entry_points)?;
        append_entry_points(&mut *writer, &self.graph.entry_points.extra_entry_points)?;
        append_predicate_entry_points(&mut *writer, &self.graph.predicate_entry_points)?;

        let relative_position = usize::try_from(
            writer
                .stream_position()?
                .checked_sub(artifact_start)
                .ok_or_else(|| error::internal("HNSW writer moved before artifact start"))?,
        )
        .map_err(|_| error::out_of_range("HNSW artifact position exceeds usize"))?;
        let predicate_scan_offset = aligned_offset(relative_position, 8)?;
        writer.write_all(&[0; 8][..predicate_scan_offset - relative_position])?;
        if let Some(predicate_scan) = predicate_scan {
            predicate_scan.serialize_into(&mut *writer)?;
        }

        self.graph.links.serialize(&mut *writer)?;
        if let Some(predicate_links) = &self.graph.predicate_links {
            predicate_links.serialize(&mut *writer)?;
        }

        let stats = HnswIndexStatistics::collect(self)?;
        write_stats_trailer(&mut *writer, &stats.to_bytes())?;
        let protected_end = usize::try_from(
            writer
                .stream_position()?
                .checked_sub(artifact_start)
                .ok_or_else(|| error::internal("HNSW writer moved before artifact start"))?,
        )
        .map_err(|_| error::out_of_range("HNSW artifact position exceeds usize"))?;
        let integrity = append_integrity_table_streaming(
            writer,
            artifact_start,
            HNSW_ARTIFACT_HEADER_LEN,
            protected_end,
        )?;
        header[HNSW_INTEGRITY_OFFSET_FIELD..HNSW_INTEGRITY_OFFSET_FIELD + 8].copy_from_slice(
            &u64::try_from(integrity.offset)
                .map_err(|_| error::out_of_range("HNSW integrity offset exceeds u64"))?
                .to_le_bytes(),
        );
        header[HNSW_INTEGRITY_LEN_FIELD..HNSW_INTEGRITY_LEN_FIELD + 8].copy_from_slice(
            &u64::try_from(integrity.len)
                .map_err(|_| error::out_of_range("HNSW integrity length exceeds u64"))?
                .to_le_bytes(),
        );
        header[HNSW_ARTIFACT_LEN_FIELD..HNSW_ARTIFACT_LEN_FIELD + 8].copy_from_slice(
            &u64::try_from(integrity.artifact_len)
                .map_err(|_| error::out_of_range("HNSW artifact length exceeds u64"))?
                .to_le_bytes(),
        );
        header[HNSW_INTEGRITY_CHECKSUM_FIELD..HNSW_INTEGRITY_CHECKSUM_FIELD + 4]
            .copy_from_slice(&integrity.checksum.to_le_bytes());
        let header_checksum = crc32c::crc32c(&header[..HNSW_HEADER_CHECKSUM_FIELD]);
        header[HNSW_HEADER_CHECKSUM_FIELD..HNSW_HEADER_CHECKSUM_FIELD + 4]
            .copy_from_slice(&header_checksum.to_le_bytes());
        writer.seek(SeekFrom::Start(artifact_start))?;
        writer.write_all(&header)?;
        let artifact_len = u64::try_from(integrity.artifact_len)
            .map_err(|_| error::out_of_range("HNSW artifact length exceeds u64"))?;
        writer.seek(SeekFrom::Start(
            artifact_start
                .checked_add(artifact_len)
                .ok_or_else(|| error::out_of_range("HNSW artifact end offset overflow"))?,
        ))?;
        Ok(artifact_len)
    }

    /// Deserialize HNSW index from a byte buffer.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        Self::deserialize_bytes(Bytes::copy_from_slice(data))
    }

    /// Deserialize an inline artifact while retaining its owned byte backing.
    /// Graph links and cosine norms become immutable slices of that backing,
    /// so open does not allocate memory proportional to the graph size.
    pub fn deserialize_bytes(data: Bytes) -> Result<Self> {
        Self::deserialize_backing(HnswArtifactBacking::Bytes(data))
    }

    /// Deserialize a sidecar artifact directly over its package mmap.
    pub fn deserialize_mmap_range(
        mmap: Arc<Mmap>,
        artifact_offset: usize,
        artifact_len: usize,
    ) -> Result<Self> {
        let end = artifact_offset
            .checked_add(artifact_len)
            .ok_or_else(|| error::data_corrupted("HNSW artifact mmap range overflow"))?;
        if end > mmap.len() {
            return Err(error::data_corrupted(
                "HNSW artifact mmap range exceeds package length",
            ));
        }
        Self::deserialize_backing(HnswArtifactBacking::Mmap {
            mmap,
            offset: artifact_offset,
            len: artifact_len,
        })
    }

    fn deserialize_backing(backing: HnswArtifactBacking) -> Result<Self> {
        let data = backing.as_bytes();
        let header = current_artifact_header(data)?;
        let expected_header_checksum = u32::from_le_bytes(
            header[HNSW_HEADER_CHECKSUM_FIELD..HNSW_HEADER_CHECKSUM_FIELD + 4]
                .try_into()
                .expect("header checksum width"),
        );
        let (build_contract, mut offset) = decode_build_contract(header)?;
        let read_u32 = |data: &[u8], offset: &mut usize, field| -> Result<u32> {
            Ok(u32::from_le_bytes(
                take_artifact_bytes(data, offset, 4, field)?
                    .try_into()
                    .expect("u32 width"),
            ))
        };
        let norm_count = usize::try_from(u64::from_le_bytes(
            header[HNSW_NORM_COUNT_FIELD..HNSW_NORM_COUNT_FIELD + 8]
                .try_into()
                .expect("u64 width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW inverse norm count exceeds usize"))?;
        let vector_count = usize::try_from(u64::from_le_bytes(
            header[HNSW_VECTOR_COUNT_FIELD..HNSW_VECTOR_COUNT_FIELD + 8]
                .try_into()
                .expect("u64 width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW vector count exceeds usize"))?;
        let vector_dim = usize::try_from(u32::from_le_bytes(
            header[HNSW_VECTOR_DIM_FIELD..HNSW_VECTOR_DIM_FIELD + 4]
                .try_into()
                .expect("u32 width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW vector dimension exceeds usize"))?;
        let vector_encoding = u32::from_le_bytes(
            header[HNSW_VECTOR_ENCODING_FIELD..HNSW_VECTOR_ENCODING_FIELD + 4]
                .try_into()
                .expect("u32 width"),
        );
        if vector_encoding != HNSW_VECTOR_ENCODING_F32_LE {
            return Err(error::data_corrupted(format!(
                "unsupported HNSW vector encoding {vector_encoding}"
            )));
        }
        let primary_count = u32::from_le_bytes(
            header[HNSW_PRIMARY_COUNT_FIELD..HNSW_PRIMARY_COUNT_FIELD + 4]
                .try_into()
                .expect("u32 width"),
        ) as usize;
        let extra_count = u32::from_le_bytes(
            header[HNSW_EXTRA_COUNT_FIELD..HNSW_EXTRA_COUNT_FIELD + 4]
                .try_into()
                .expect("u32 width"),
        ) as usize;
        let routing_metadata_len = usize::try_from(u64::from_le_bytes(
            take_artifact_bytes(data, &mut offset, 8, "routing metadata length")?
                .try_into()
                .expect("u64 width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW routing metadata length exceeds usize"))?;
        let routing_code_len = usize::try_from(u64::from_le_bytes(
            take_artifact_bytes(data, &mut offset, 8, "routing-code length")?
                .try_into()
                .expect("u64 width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW routing-code length exceeds usize"))?;
        let base_graph_len = usize::try_from(u64::from_le_bytes(
            take_artifact_bytes(data, &mut offset, 8, "base graph length")?
                .try_into()
                .expect("u64 width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW base graph length exceeds usize"))?;
        let predicate_graph_len = usize::try_from(u64::from_le_bytes(
            take_artifact_bytes(data, &mut offset, 8, "predicate graph length")?
                .try_into()
                .expect("u64 width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW predicate graph length exceeds usize"))?;
        let predicate_entry_count = usize::try_from(u64::from_le_bytes(
            take_artifact_bytes(data, &mut offset, 8, "predicate entry-point count")?
                .try_into()
                .expect("u64 width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW predicate entry-point count exceeds usize"))?;
        let predicate_scan_len = usize::try_from(u64::from_le_bytes(
            take_artifact_bytes(data, &mut offset, 8, "predicate scan length")?
                .try_into()
                .expect("u64 width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW predicate scan length exceeds usize"))?;
        let integrity_offset = usize::try_from(u64::from_le_bytes(
            take_artifact_bytes(data, &mut offset, 8, "integrity table offset")?
                .try_into()
                .expect("u64 width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW integrity offset exceeds usize"))?;
        let integrity_len = usize::try_from(u64::from_le_bytes(
            take_artifact_bytes(data, &mut offset, 8, "integrity table length")?
                .try_into()
                .expect("u64 width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW integrity length exceeds usize"))?;
        let artifact_len = usize::try_from(u64::from_le_bytes(
            take_artifact_bytes(data, &mut offset, 8, "artifact length")?
                .try_into()
                .expect("u64 width"),
        ))
        .map_err(|_| error::data_corrupted("HNSW artifact length exceeds usize"))?;
        let integrity_checksum = read_u32(data, &mut offset, "integrity table checksum")?;
        let header_checksum = read_u32(data, &mut offset, "header checksum")?;
        if header_checksum != expected_header_checksum {
            return Err(error::data_corrupted(
                "HNSW header checksum field changed while decoding",
            ));
        }
        debug_assert_eq!(offset, HNSW_ARTIFACT_HEADER_LEN);
        let integrity = ArtifactIntegrity::open(
            backing.integrity_backing(),
            IntegrityDescriptor {
                offset: integrity_offset,
                len: integrity_len,
                checksum: integrity_checksum,
                artifact_len,
            },
        )?;
        build_contract.validate()?;
        if build_contract.filter_topology.is_enabled() != (predicate_graph_len != 0) {
            return Err(error::data_corrupted(
                "HNSW predicate graph presence does not match its filter-topology contract",
            ));
        }
        if !build_contract.filter_topology.is_enabled() && predicate_entry_count != 0 {
            return Err(error::data_corrupted(
                "HNSW predicate entry points require an enabled filter topology",
            ));
        }
        if build_contract.filter_topology.is_enabled() != (predicate_scan_len != 0) {
            return Err(error::data_corrupted(
                "HNSW covering scan presence does not match its filter-topology contract",
            ));
        }
        let distance = build_contract.distance;
        let vector_bytes = vector_count
            .checked_mul(vector_dim)
            .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
            .ok_or_else(|| error::data_corrupted("HNSW vector byte length overflow"))?;
        let norm_bytes = norm_count
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(|| error::data_corrupted("HNSW inverse norm byte length overflow"))?;
        let routing_norm_bytes = match build_contract.vector_encoding {
            HnswBuildVectorEncoding::ExactF32 => {
                if routing_metadata_len != 0 || routing_code_len != 0 {
                    return Err(error::data_corrupted(
                        "exact-f32 HNSW artifact must not contain compact routing metadata",
                    ));
                }
                0
            }
            HnswBuildVectorEncoding::SymmetricI16 { routing_dimensions } => {
                let encoded_dimension = usize::from(routing_dimensions.get()).next_multiple_of(16);
                let expected_codes = vector_count
                    .checked_mul(encoded_dimension)
                    .and_then(|values| values.checked_mul(std::mem::size_of::<i16>()))
                    .ok_or_else(|| error::data_corrupted("HNSW routing-code length overflow"))?;
                if routing_code_len != expected_codes {
                    return Err(error::data_corrupted(format!(
                        "HNSW routing-code length mismatch: expected {expected_codes}, got {routing_code_len}"
                    )));
                }
                let expected_metadata = usize::from(routing_dimensions.get())
                    .checked_mul(std::mem::size_of::<u32>() + std::mem::size_of::<f32>())
                    .ok_or_else(|| {
                        error::data_corrupted("HNSW routing metadata length overflow")
                    })?;
                if routing_metadata_len != expected_metadata {
                    return Err(error::data_corrupted(format!(
                        "HNSW routing metadata length mismatch: expected {expected_metadata}, got {routing_metadata_len}"
                    )));
                }
                if distance == DistanceMetric::Cosine {
                    vector_count
                        .checked_mul(std::mem::size_of::<f32>())
                        .ok_or_else(|| {
                            error::data_corrupted("HNSW routing norm byte length overflow")
                        })?
                } else {
                    0
                }
            }
        };
        let entry_bytes = primary_count
            .checked_add(extra_count)
            .and_then(|count| count.checked_mul(2 * std::mem::size_of::<u32>()))
            .and_then(|bytes| {
                predicate_entry_count
                    .checked_mul(3 * std::mem::size_of::<u32>())
                    .and_then(|predicate_bytes| bytes.checked_add(predicate_bytes))
            })
            .ok_or_else(|| error::data_corrupted("HNSW entry metadata length overflow"))?;
        let vector_padding = alignment_padding(HNSW_ARTIFACT_HEADER_LEN, HNSW_ARTIFACT_ALIGNMENT)?;
        let routing_code_padding = if routing_code_len == 0 {
            0
        } else {
            alignment_padding(
                HNSW_ARTIFACT_HEADER_LEN
                    .checked_add(vector_padding)
                    .and_then(|bytes| bytes.checked_add(vector_bytes))
                    .and_then(|bytes| bytes.checked_add(norm_bytes))
                    .and_then(|bytes| bytes.checked_add(routing_metadata_len))
                    .ok_or_else(|| error::data_corrupted("HNSW routing-code offset overflow"))?,
                HNSW_ARTIFACT_ALIGNMENT,
            )?
        };
        integrity.verify_range(
            HNSW_ARTIFACT_HEADER_LEN,
            vector_padding
                .checked_add(vector_bytes)
                .and_then(|bytes| bytes.checked_add(norm_bytes))
                .and_then(|bytes| bytes.checked_add(routing_metadata_len))
                .and_then(|bytes| bytes.checked_add(routing_code_padding))
                .and_then(|bytes| bytes.checked_add(routing_code_len))
                .and_then(|bytes| bytes.checked_add(routing_norm_bytes))
                .and_then(|bytes| bytes.checked_add(entry_bytes))
                .ok_or_else(|| error::data_corrupted("HNSW metadata length overflow"))?,
        )?;
        let vector_alignment =
            take_artifact_bytes(data, &mut offset, vector_padding, "vector alignment")?;
        if vector_alignment.iter().any(|byte| *byte != 0) {
            return Err(error::data_corrupted(
                "HNSW vector alignment padding is not zeroed",
            ));
        }
        let vector_start = offset;
        take_artifact_bytes(data, &mut offset, vector_bytes, "embedded vectors")?;
        let vector_storage =
            backing.vector_storage(vector_start, vector_bytes, vector_dim, vector_count)?;
        let norm_start = offset;
        take_artifact_bytes(data, &mut offset, norm_bytes, "inverse norms")?;
        let inverse_norms = backing.inverse_norms(norm_start, norm_bytes)?;
        let mut vector_storage = match distance {
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
        let routing_metadata =
            take_artifact_bytes(data, &mut offset, routing_metadata_len, "routing metadata")?;
        let routing_dimensions = build_contract
            .vector_encoding
            .routing_dimensions()
            .map_or(0, usize::from);
        let (routing_dimension_bytes, routing_scale_bytes) = routing_metadata
            .split_at(routing_dimensions.saturating_mul(std::mem::size_of::<u32>()));
        let selected_dimensions = routing_dimension_bytes
            .chunks_exact(std::mem::size_of::<u32>())
            .map(|bytes| u32::from_le_bytes(bytes.try_into().expect("u32 width")) as usize)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let routing_scales = routing_scale_bytes
            .chunks_exact(std::mem::size_of::<f32>())
            .map(|bytes| f32::from_le_bytes(bytes.try_into().expect("f32 width")))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let routing_padding =
            take_artifact_bytes(data, &mut offset, routing_code_padding, "routing alignment")?;
        if routing_padding.iter().any(|byte| *byte != 0) {
            return Err(error::data_corrupted(
                "HNSW routing-code alignment padding is not zeroed",
            ));
        }
        let routing_start = offset;
        take_artifact_bytes(data, &mut offset, routing_code_len, "routing codes")?;
        let routing_codes = backing.i16_routing_backing(routing_start, routing_code_len)?;
        let routing_norm_start = offset;
        take_artifact_bytes(
            data,
            &mut offset,
            routing_norm_bytes,
            "routing inverse norms",
        )?;
        let routing_inverse_norms = if routing_norm_bytes == 0 {
            None
        } else {
            Some(backing.inverse_norms(routing_norm_start, routing_norm_bytes)?)
        };
        if matches!(
            build_contract.vector_encoding,
            HnswBuildVectorEncoding::SymmetricI16 { .. }
        ) {
            vector_storage = SymmetricI16BuildVectorStorage::from_persisted(
                vector_storage,
                routing_codes,
                selected_dimensions,
                routing_scales,
                routing_inverse_norms,
            )?;
        }

        /*
         * Vector and metric metadata are now entirely artifact-owned. Keep
         * entry-point parsing after that ownership boundary so no caller can
         * accidentally pair a graph with unrelated base-segment vectors.
         */
        let entry_points = EntryPoints {
            entry_points: read_entry_points(data, &mut offset, primary_count)?,
            extra_entry_points: read_entry_points(data, &mut offset, extra_count)?,
        };
        let predicate_entry_points =
            read_predicate_entry_points(data, &mut offset, predicate_entry_count)?;
        let predicate_scan_offset = aligned_offset(offset, 8)?;
        if data
            .get(offset..predicate_scan_offset)
            .is_none_or(|padding| padding.iter().any(|byte| *byte != 0))
        {
            return Err(error::data_corrupted(
                "HNSW predicate scan alignment padding is invalid",
            ));
        }
        offset = predicate_scan_offset;
        let predicate_scan = if predicate_scan_len == 0 {
            None
        } else {
            let layout =
                backing.predicate_scan(offset, predicate_scan_len, Arc::clone(&integrity))?;
            offset = offset
                .checked_add(predicate_scan_len)
                .ok_or_else(|| error::data_corrupted("HNSW predicate scan offset overflow"))?;
            Some(layout)
        };

        // Deserialize base and predicate-local graph links from exact envelope
        // ranges. Statistics bytes are never exposed to either graph parser.
        let links = backing.graph_links(offset, base_graph_len, Arc::clone(&integrity))?;
        offset = offset
            .checked_add(base_graph_len)
            .ok_or_else(|| error::data_corrupted("HNSW base graph offset overflow"))?;
        let predicate_links = if predicate_graph_len == 0 {
            None
        } else {
            let links = backing.graph_links(offset, predicate_graph_len, Arc::clone(&integrity))?;
            offset = offset
                .checked_add(predicate_graph_len)
                .ok_or_else(|| error::data_corrupted("HNSW predicate graph offset overflow"))?;
            Some(links)
        };

        if integrity_offset < 8 {
            return Err(error::data_corrupted(
                "HNSW integrity table leaves no statistics trailer",
            ));
        }
        integrity.verify_range(integrity_offset - 8, 8)?;
        let (statistics_bytes, graph_payload) = split_stats_trailer(&data[..integrity_offset]);
        let statistics_bytes = statistics_bytes.ok_or_else(|| {
            error::data_corrupted("HNSW artifact is missing its statistics trailer")
        })?;
        if graph_payload.len() != offset {
            return Err(error::data_corrupted(format!(
                "HNSW payload length mismatch: graph ends at {offset}, statistics begin at {}",
                graph_payload.len()
            )));
        }
        let statistics_start = graph_payload.len();
        integrity.verify_range(statistics_start, integrity_offset - statistics_start)?;
        let persisted_statistics = HnswIndexStatistics::from_bytes(statistics_bytes)?;

        let graph = match predicate_links {
            Some(predicate_links) => GraphLayers::new_with_predicate_links(
                links,
                predicate_links,
                predicate_entry_points,
                entry_points,
                (&build_contract).into(),
            ),
            None => GraphLayers::new(links, entry_points, (&build_contract).into()),
        };

        let mut index = Self::try_new_with_predicate_scan(
            build_contract,
            graph,
            vector_storage,
            predicate_scan,
        )?;
        index.persisted_statistics = Some(persisted_statistics);
        index._artifact_integrity = Some(integrity);
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
        budget: &ResourceBudget,
    ) -> Result<HnswSearchResult> {
        if top_k == 0 {
            return Ok(HnswSearchResult {
                points: Vec::new(),
                scored_points: 0,
                outcome: HnswSearchOutcome::new(HnswSearchPath::ExactScan(
                    HnswExactScanKind::BaseVectors,
                )),
            });
        }

        let _foreground_query = HnswForegroundQueryGuard::enter();
        let start = Instant::now();
        let pre_filter_count = self.graph.num_points() as u64;
        let filter_row_set = filter.row_set();
        let predicate_topology_available =
            filter.predicate_topology_available(&self.build_contract.filter_topology);
        let post_filter_count = filter_row_set
            .map(ExactRowSet::len)
            .unwrap_or(pre_filter_count);

        let prepared_query = self.build_contract.distance.prepare(query);
        let mut exact_scorer = VectorScorer::new(&prepared_query, self.vector_storage.as_ref())?;
        let mut graph_scored_points = 0_u64;
        let (points, outcome) = if Self::should_use_plain_scan(strategy) {
            let exact = self.plain_scan(top_k, &mut exact_scorer, filter_row_set, budget)?;
            (
                exact.points,
                HnswSearchOutcome::new(HnswSearchPath::ExactScan(exact.kind)),
            )
        } else {
            let algorithm = Self::algorithm_for_strategy(filter, strategy)?;
            let path = match algorithm {
                SearchAlgorithm::Hnsw => HnswSearchPath::UnfilteredGraph,
                SearchAlgorithm::MaskedTopK => HnswSearchPath::MaskedGraph,
                SearchAlgorithm::AdaptiveFilteredTopK => HnswSearchPath::AdaptiveGraph,
            };
            let widths = Self::effective_graph_widths(top_k, params, policy);
            let ef = widths.ef;
            let predicate_seed_rows = if predicate_topology_available {
                Some(filter_row_set.expect("predicate topology requires an exact row set"))
            } else {
                None
            };
            let mut predicate_partition_seeds =
                PredicatePartitionSeeds::new(predicate_seed_rows, ef.saturating_mul(2));
            let admission = filter_row_set.map(ExactRowSet::admission);
            let mut graph_scorer =
                GraphVectorScorer::new(&prepared_query, self.vector_storage.as_ref())?;
            let rerank_window = if graph_scorer.uses_compact_routing() {
                widths.rerank_window
            } else {
                top_k
            };
            let limits = GraphSearchLimits::try_new(top_k, rerank_window, ef)?;
            let graph_result = self.graph.search_one(
                limits,
                algorithm,
                &mut graph_scorer,
                admission.as_ref(),
                &mut predicate_partition_seeds,
                filter.predicate_columns(),
                predicate_topology_available,
                Self::use_random_entry_point(params),
                budget,
            )?;
            graph_scored_points = graph_scorer.scored_point_count();
            let mut results = graph_result.points;
            if graph_scorer.uses_compact_routing() {
                for point in &mut results {
                    point.score = exact_scorer.score_point(point.idx);
                }
                results.sort_unstable_by(|left, right| right.cmp(left));
                results.truncate(top_k);
            }
            if filter_row_set.is_some()
                && results.len() < self.expected_filtered_rows(top_k, filter_row_set)?
            {
                {
                    let exact =
                        self.plain_scan(top_k, &mut exact_scorer, filter_row_set, budget)?;
                    (
                        exact.points,
                        HnswSearchOutcome::new(path)
                            .with_predicate_admission(graph_result.predicate_admission)
                            .with_predicate_topology(graph_result.predicate_topology_used)
                            .with_predicate_refinement(graph_result.predicate_refined)
                            .with_exact_fallback(exact.kind),
                    )
                }
            } else {
                (
                    results,
                    HnswSearchOutcome::new(path)
                        .with_predicate_admission(graph_result.predicate_admission)
                        .with_predicate_topology(graph_result.predicate_topology_used)
                        .with_predicate_refinement(graph_result.predicate_refined),
                )
            }
        };

        let scored_points = exact_scorer
            .scored_point_count()
            .saturating_add(graph_scored_points);

        let elapsed = start.elapsed();
        let elapsed_us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        let mut telemetry = self.single_telemetry.lock().unwrap();
        telemetry.record(elapsed_us, pre_filter_count, post_filter_count);
        telemetry.record_hnsw_work(scored_points, outcome);
        storage_metrics().record_search_hnsw_work(scored_points, outcome);

        Ok(HnswSearchResult {
            points,
            scored_points,
            outcome,
        })
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
        let filter = filter_bitmap.map_or(HnswSearchFilter::None, |bitmap| {
            HnswSearchFilter::predicate(bitmap, &[])
        });
        let matching_rows = filter
            .row_set()
            .map_or(self.graph.num_points() as u64, ExactRowSet::len);
        let widths = policy.effective_widths(top_k, params.ef, params.rerank_window);
        let strategy = HnswSearchStrategy::choose(HnswSegmentSearchInput {
            objective: params.objective,
            filter_kind: filter.kind(),
            matching_rows,
            total_rows: self.graph.num_points() as u64,
            top_k,
            effective_ef: widths.ef,
            rerank_window: widths.rerank_window,
            vector_dimension: u32::try_from(self.vector_storage.vector_dim())
                .map_err(|_| error::out_of_range("HNSW vector dimension exceeds u32"))?,
            vector_encoding: self.build_contract.vector_encoding,
            exact_scan_workload: self.exact_scan_workload(filter),
            cost_profile: policy.distance_cost,
        });
        let budget = crate::search::ResourceBudget::default();
        self.search_one_with_policy_strategy(
            query, top_k, params, filter, &policy, strategy, &budget,
        )
        .map(|result| result.points)
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
        budget: &ResourceBudget,
    ) -> Result<Vec<HnswSearchResult>> {
        if queries.is_empty() {
            return Ok(Vec::new());
        }
        if top_k == 0 {
            return Ok((0..queries.len())
                .map(|_| HnswSearchResult {
                    points: Vec::new(),
                    scored_points: 0,
                    outcome: HnswSearchOutcome::new(HnswSearchPath::ExactScan(
                        HnswExactScanKind::BaseVectors,
                    )),
                })
                .collect());
        }

        self.validate_prepared_queries(queries)?;

        let _foreground_query = HnswForegroundQueryGuard::enter();
        let start = Instant::now();
        let mut exact_scorers: Vec<_> = queries
            .iter()
            .map(|query| VectorScorer::new(query, self.vector_storage.as_ref()))
            .collect::<Result<Vec<_>>>()?;

        let filter_row_set = filter.row_set();
        let predicate_topology_available =
            filter.predicate_topology_available(&self.build_contract.filter_topology);
        let results: Vec<HnswSearchResult> = if Self::should_use_plain_scan(strategy) {
            let batch_scorer = BatchScorer::new(exact_scorers, top_k);
            let num_points = self.graph.num_points() as u32;
            let scored = match filter_row_set {
                Some(row_set) => batch_scorer.scan_with_work(
                    row_set
                        .materialize()
                        .into_iter()
                        .filter(|&idx| idx < num_points),
                    budget.work.as_ref(),
                )?,
                None => batch_scorer.scan_with_work(0..num_points, budget.work.as_ref())?,
            };
            scored
                .into_iter()
                .map(|result| HnswSearchResult {
                    points: result.points,
                    scored_points: result.scored_points,
                    outcome: HnswSearchOutcome::new(HnswSearchPath::ExactScan(
                        HnswExactScanKind::BaseVectors,
                    )),
                })
                .collect()
        } else {
            let algorithm = Self::algorithm_for_strategy(filter, strategy)?;
            let path = match algorithm {
                SearchAlgorithm::Hnsw => HnswSearchPath::UnfilteredGraph,
                SearchAlgorithm::MaskedTopK => HnswSearchPath::MaskedGraph,
                SearchAlgorithm::AdaptiveFilteredTopK => HnswSearchPath::AdaptiveGraph,
            };
            let widths = Self::effective_graph_widths(top_k, params, policy);
            let ef = widths.ef;
            let predicate_seed_rows = if predicate_topology_available {
                Some(filter_row_set.expect("predicate topology requires an exact row set"))
            } else {
                None
            };
            let mut predicate_partition_seeds =
                PredicatePartitionSeeds::new(predicate_seed_rows, ef.saturating_mul(2));
            let admission = filter_row_set.map(ExactRowSet::admission);
            let mut graph_scorers = queries
                .iter()
                .map(|query| GraphVectorScorer::new(query, self.vector_storage.as_ref()))
                .collect::<Result<Vec<_>>>()?;
            let uses_compact_routing = graph_scorers
                .first()
                .is_some_and(GraphVectorScorer::uses_compact_routing);
            if graph_scorers
                .iter()
                .any(|scorer| scorer.uses_compact_routing() != uses_compact_routing)
            {
                return Err(error::data_corrupted(
                    "HNSW batch scorers disagree on the artifact routing representation",
                ));
            }
            let rerank_window = if uses_compact_routing {
                widths.rerank_window
            } else {
                top_k
            };
            let limits = GraphSearchLimits::try_new(top_k, rerank_window, ef)?;
            let results = self.graph.search_many(
                limits,
                algorithm,
                &mut graph_scorers,
                admission.as_ref(),
                &mut predicate_partition_seeds,
                filter.predicate_columns(),
                predicate_topology_available,
                Self::use_random_entry_point(params),
                budget,
            )?;
            let expected_rows = self.expected_filtered_rows(top_k, filter_row_set)?;
            results
                .into_iter()
                .zip(graph_scorers.iter())
                .zip(exact_scorers.iter_mut())
                .map(
                    |((graph_result, graph_scorer), exact_scorer)| -> Result<HnswSearchResult> {
                        let mut outcome = HnswSearchOutcome::new(path)
                            .with_predicate_admission(graph_result.predicate_admission)
                            .with_predicate_topology(graph_result.predicate_topology_used)
                            .with_predicate_refinement(graph_result.predicate_refined);
                        let mut points = graph_result.points;
                        if uses_compact_routing {
                            for point in &mut points {
                                point.score = exact_scorer.score_point(point.idx);
                            }
                            points.sort_unstable_by(|left, right| right.cmp(left));
                            points.truncate(top_k);
                        }
                        let points = if filter_row_set.is_some() && points.len() < expected_rows {
                            let exact =
                                self.plain_scan(top_k, exact_scorer, filter_row_set, budget)?;
                            outcome = outcome.with_exact_fallback(exact.kind);
                            exact.points
                        } else {
                            points
                        };
                        Ok(HnswSearchResult {
                            points,
                            scored_points: graph_scorer
                                .scored_point_count()
                                .saturating_add(exact_scorer.scored_point_count()),
                            outcome,
                        })
                    },
                )
                .collect::<Result<Vec<_>>>()?
        };

        {
            let mut telemetry = self.single_telemetry.lock().unwrap();
            for result in &results {
                telemetry.record_hnsw_work(result.scored_points, result.outcome);
                storage_metrics().record_search_hnsw_work(result.scored_points, result.outcome);
            }
        }

        let elapsed = start.elapsed();
        let elapsed_us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
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
        let budget = crate::search::ResourceBudget::default();
        self.search_many_prepared_with_policy(
            queries,
            top_k,
            params,
            filter_bitmap.map_or(HnswSearchFilter::None, |bitmap| {
                HnswSearchFilter::predicate(bitmap, &[])
            }),
            &HnswSearchPolicy::default(),
            strategy,
            &budget,
        )
        .map(|results| results.into_iter().map(|result| result.points).collect())
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
        if let Some(integrity) = &self._artifact_integrity {
            integrity.verify_all()?;
        }
        self.validate_artifact_contract()?;
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
        self.graph.links.verify_integrity()?;
        match (
            self.build_contract.filter_topology.is_enabled(),
            self.graph.predicate_links.as_ref(),
        ) {
            (true, Some(predicate_links)) => {
                if predicate_links.num_points() != self.graph.links.num_points() {
                    return Err(error::data_corrupted(
                        "HNSW predicate graph cardinality differs from base graph",
                    ));
                }
                predicate_links.verify_integrity()?
            }
            (false, None) => {}
            _ => {
                return Err(error::data_corrupted(
                    "HNSW predicate graph presence differs from filter-topology contract",
                ))
            }
        }
        if let Some(layout) = &self.predicate_scan {
            layout.verify_integrity()?;
        }
        Ok(())
    }

    /// Run an explicit O(N + E) structural qualification scan.
    ///
    /// This is deliberately not part of artifact open or ordinary search.
    /// Callers must supply a governed budget large enough for the retained
    /// indegree image and temporary reachability/component workspaces.
    pub fn graph_diagnostics(
        &self,
        budget: &ResourceBudget,
    ) -> Result<super::HnswGraphDiagnostics> {
        super::HnswGraphDiagnostics::analyze(&self.graph.links, &self.graph.entry_points, budget)
    }

    fn validate_artifact_contract(&self) -> Result<()> {
        self.build_contract.validate()?;
        if self.graph.num_points() != self.vector_storage.num_vectors() {
            return Err(error::data_corrupted(format!(
                "HNSW graph cardinality {} differs from vector cardinality {}",
                self.graph.num_points(),
                self.vector_storage.num_vectors()
            )));
        }
        if self.graph.links.level0_stride() != self.build_contract.m0 as usize {
            return Err(error::data_corrupted(format!(
                "HNSW level-0 record stride {} differs from build-contract m0 {}",
                self.graph.links.level0_stride(),
                self.build_contract.m0
            )));
        }
        if let Some(predicate_links) = &self.graph.predicate_links {
            let expected = self.build_contract.filter_topology.merged_level0_stride()?;
            if predicate_links.level0_stride() != expected {
                return Err(error::data_corrupted(format!(
                    "HNSW predicate level-0 record stride {} differs from topology capacity {expected}",
                    predicate_links.level0_stride()
                )));
            }
        }
        match (
            self.build_contract.filter_topology.is_enabled(),
            self.predicate_scan.as_ref(),
        ) {
            (true, Some(layout)) => layout.validate_contract(
                self.vector_storage.vector_dim(),
                self.graph.num_points(),
                self.build_contract.filter_topology.columns(),
            )?,
            (false, None) => {}
            (true, None) => {
                return Err(error::data_corrupted(
                    "HNSW filter topology is missing its predicate covering scan layout",
                ))
            }
            (false, Some(_)) => {
                return Err(error::data_corrupted(
                    "HNSW predicate covering scan layout requires a filter topology",
                ))
            }
        }
        self.validate_entry_points()
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
            if entry.level >= self.graph.links.num_levels(entry.point_id)? {
                return Err(error::data_corrupted(format!(
                    "HNSW entry point {} level {} exceeds its graph levels",
                    entry.point_id, entry.level
                )));
            }
        }
        let topology = &self.build_contract.filter_topology;
        if !topology.is_enabled() {
            if !self.graph.predicate_entry_points.is_empty() {
                return Err(error::data_corrupted(
                    "HNSW artifact without filter topology contains predicate entry points",
                ));
            }
            return Ok(());
        }
        let predicate_links = self.graph.predicate_links.as_ref().ok_or_else(|| {
            error::data_corrupted("HNSW filter topology is missing predicate graph links")
        })?;
        if self.vector_storage.num_vectors() != 0
            && topology.columns().iter().any(|column_id| {
                !self
                    .graph
                    .predicate_entry_points
                    .iter()
                    .any(|entry| entry.column_id == *column_id)
            })
        {
            return Err(error::data_corrupted(
                "HNSW predicate entry points do not cover every configured filter column",
            ));
        }
        if self.graph.predicate_entry_points.windows(2).any(|entries| {
            (entries[0].column_id, entries[0].point_id, entries[0].level)
                >= (entries[1].column_id, entries[1].point_id, entries[1].level)
        }) {
            return Err(error::data_corrupted(
                "HNSW predicate entry points must be strictly ordered",
            ));
        }
        for entry in &self.graph.predicate_entry_points {
            if !topology.columns().contains(&entry.column_id) {
                return Err(error::data_corrupted(format!(
                    "HNSW predicate entry column {} is absent from the filter topology",
                    entry.column_id
                )));
            }
            if entry.point_id as usize >= self.vector_storage.num_vectors() {
                return Err(error::data_corrupted(format!(
                    "HNSW predicate entry point {} is outside vector cardinality {}",
                    entry.point_id,
                    self.vector_storage.num_vectors()
                )));
            }
            if entry.level >= predicate_links.num_levels(entry.point_id)? {
                return Err(error::data_corrupted(format!(
                    "HNSW predicate entry point {} level {} exceeds its graph levels",
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
            (HnswSearchStrategy::AdaptiveFilteredGraph, HnswSearchFilter::Predicate { .. }) => {
                Ok(SearchAlgorithm::AdaptiveFilteredTopK)
            }
            (HnswSearchStrategy::ExactScan, _) => Err(error::internal(
                "exact HNSW strategy reached graph algorithm selection",
            )),
            (HnswSearchStrategy::UnfilteredGraph, _) => Err(error::internal(
                "unfiltered HNSW strategy received an admission bitmap",
            )),
            (HnswSearchStrategy::MaskedGraph, HnswSearchFilter::Predicate { .. }) => Err(
                error::internal("predicate HNSW search must use adaptive graph execution"),
            ),
            (HnswSearchStrategy::AdaptiveFilteredGraph, HnswSearchFilter::Visibility(_)) => Err(
                error::internal("visibility-only HNSW search cannot use predicate refinement"),
            ),
        }
    }

    fn effective_graph_widths(
        top_k: usize,
        params: &SearchParams,
        policy: &HnswSearchPolicy,
    ) -> super::HnswSearchWidths {
        policy.effective_widths(top_k, params.ef, params.rerank_window)
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

    fn should_use_plain_scan(strategy: HnswSearchStrategy) -> bool {
        strategy == HnswSearchStrategy::ExactScan
    }

    fn expected_filtered_rows(
        &self,
        top_k: usize,
        filter_row_set: Option<&dyn ExactRowSet>,
    ) -> Result<usize> {
        let Some(row_set) = filter_row_set else {
            return Ok(top_k.min(self.graph.num_points()));
        };
        if row_set.domain_len() > self.graph.num_points() {
            return Err(error::data_corrupted(format!(
                "HNSW filter row-set domain exceeds graph cardinality {}",
                self.graph.num_points()
            )));
        }
        Ok(top_k.min(row_set.len() as usize))
    }

    fn plain_scan(
        &self,
        top_k: usize,
        scorer: &mut VectorScorer,
        filter_row_set: Option<&dyn ExactRowSet>,
        budget: &ResourceBudget,
    ) -> Result<ExactScanResult> {
        let num_points = self.graph.num_points() as u32;
        match filter_row_set {
            Some(row_set) => {
                if row_set.domain_len() > self.graph.num_points() {
                    return Err(error::data_corrupted(format!(
                        "HNSW exact row-set domain {} exceeds graph cardinality {}",
                        row_set.domain_len(),
                        self.graph.num_points()
                    )));
                }
                self.exact_physical_scan(top_k, scorer, row_set, budget)
            }
            None => Ok(ExactScanResult {
                points: self.plain_scan_iter(top_k, scorer, 0..num_points, budget.work.as_ref())?,
                kind: HnswExactScanKind::BaseVectors,
            }),
        }
    }

    fn exact_physical_scan(
        &self,
        top_k: usize,
        scorer: &mut VectorScorer,
        row_set: &dyn ExactRowSet,
        budget: &ResourceBudget,
    ) -> Result<ExactScanResult> {
        let row_count = row_set.len();
        let lane_count =
            HnswDistanceCostModel::exact_scan_parallelism(row_count, budget.parallelism_slots);
        let source_count = row_set.physical_partitions().len();
        let descriptor_count = source_count.saturating_add(lane_count.saturating_sub(1));
        let descriptor_bytes = descriptor_count
            .saturating_mul(std::mem::size_of::<ExactPhysicalRange<'_>>())
            .saturating_mul(2)
            .saturating_add(lane_count.saturating_mul(std::mem::size_of::<ExactScanLane<'_>>()));
        let scorer_scratch_bytes = lane_count.saturating_mul(
            crate::index::hnsw::batch_scorer::BATCH_SIZE
                .saturating_mul(std::mem::size_of::<ScoreType>())
                .saturating_add(
                    top_k
                        .saturating_mul(std::mem::size_of::<ScoredPoint>())
                        .saturating_mul(2),
                ),
        );
        let _scan_reservation =
            budget.try_reserve_memory(descriptor_bytes.saturating_add(scorer_scratch_bytes))?;
        let plan = self.plan_exact_physical_scan(row_set)?;
        let kind = plan.kind();
        let lanes = Self::plan_exact_scan_lanes(&plan.ranges, row_count, lane_count)?;
        let prepared_scoring = scorer.prepared_scoring();
        let results = map_search_tasks(
            &lanes,
            budget.parallelism_slots,
            |_, lane| -> Result<ExactScanLaneResult> {
                let mut local_scorer = prepared_scoring.scorer();
                let mut best = ScanTopK::new(top_k);
                for range in &lane.ranges {
                    match *range {
                        ExactPhysicalRange::Covering(range) => self.scan_covering_range_into(
                            &mut best,
                            &mut local_scorer,
                            range,
                            budget.work.as_ref(),
                        )?,
                        ExactPhysicalRange::Posting(range) => {
                            let first =
                                range.posting.select(range.first_rank).ok_or_else(|| {
                                    error::data_corrupted(
                                        "exact row-set posting rank exceeds its cardinality",
                                    )
                                })?;
                            self.scan_exact_points_into(
                                &mut best,
                                &mut local_scorer,
                                range
                                    .posting
                                    .range(first..)
                                    .take(range.len as usize)
                                    .map(|point_id| range.point_base + point_id),
                                budget.work.as_ref(),
                            )?;
                        }
                        ExactPhysicalRange::Dense { first_point, len } => {
                            let end = first_point.checked_add(len).ok_or_else(|| {
                                error::data_corrupted("dense exact scan range overflow")
                            })?;
                            self.scan_exact_points_into(
                                &mut best,
                                &mut local_scorer,
                                first_point..end,
                                budget.work.as_ref(),
                            )?;
                        }
                    }
                }
                Ok(ExactScanLaneResult {
                    points: best.into_sorted_vec(),
                    scored_points: local_scorer.scored_point_count(),
                })
            },
        )?;

        let mut best = ScanTopK::new(top_k);
        let mut scored_points = 0u64;
        for result in results {
            scored_points = scored_points.saturating_add(result.scored_points);
            for point in result.points {
                best.push(point);
            }
        }
        scorer.add_scored_point_count(scored_points);
        Ok(ExactScanResult {
            points: best.into_sorted_vec(),
            kind,
        })
    }

    fn plan_exact_physical_scan<'a>(
        &'a self,
        row_set: &'a dyn ExactRowSet,
    ) -> Result<ExactPhysicalScanPlan<'a>> {
        let point_count = u32::try_from(self.graph.num_points())
            .map_err(|_| error::out_of_range("HNSW point count exceeds u32"))?;
        let mut pending = vec![(row_set.physical_partitions(), 0..point_count)];
        let mut plan = ExactPhysicalScanPlan::default();
        while let Some((partitions, point_range)) = pending.pop() {
            match partitions {
                ExactRowPartitions::Dense(rows) => {
                    let available = point_range.end.saturating_sub(point_range.start);
                    if rows > available {
                        return Err(error::data_corrupted(
                            "dense exact row-set exceeds its physical partition",
                        ));
                    }
                    if rows != 0 {
                        plan.ranges.push(ExactPhysicalRange::Dense {
                            first_point: point_range.start,
                            len: rows,
                        });
                        plan.base_rows = plan.base_rows.saturating_add(u64::from(rows));
                    }
                }
                ExactRowPartitions::Single(posting) => {
                    if !posting.is_empty() {
                        plan.ranges
                            .push(ExactPhysicalRange::Posting(ExactPostingRange {
                                posting,
                                point_base: point_range.start,
                                first_rank: 0,
                                len: u32::try_from(posting.len()).map_err(|_| {
                                    error::data_corrupted(
                                        "exact row-set posting cardinality exceeds u32",
                                    )
                                })?,
                            }));
                        plan.base_rows = plan.base_rows.saturating_add(posting.len());
                    }
                }
                ExactRowPartitions::OrdinalSelection(row_set) => {
                    let covering = self
                        .predicate_scan
                        .as_ref()
                        .map(|layout| {
                            layout.selected_ranges_for_partition(
                                row_set.column_id(),
                                point_range.clone(),
                                row_set.selected_postings(),
                            )
                        })
                        .transpose()?
                        .flatten();
                    if let Some(ranges) = covering {
                        for range in ranges {
                            let rows = range.row_ids().len();
                            if rows != 0 {
                                plan.ranges
                                    .push(ExactPhysicalRange::Covering(CoveringScanRange {
                                        range,
                                        first_row: 0,
                                        len: rows,
                                    }));
                                plan.covering_rows = plan.covering_rows.saturating_add(rows as u64);
                            }
                        }
                    } else {
                        for posting in row_set.selected_postings() {
                            if posting.rows().is_empty() {
                                continue;
                            }
                            plan.ranges
                                .push(ExactPhysicalRange::Posting(ExactPostingRange {
                                    posting: posting.rows(),
                                    point_base: point_range.start,
                                    first_rank: 0,
                                    len: u32::try_from(posting.rows().len()).map_err(|_| {
                                        error::data_corrupted(
                                            "exact ordinal posting cardinality exceeds u32",
                                        )
                                    })?,
                                }));
                            plan.base_rows = plan.base_rows.saturating_add(posting.rows().len());
                        }
                    }
                }
                ExactRowPartitions::Partitioned(partitioned) => {
                    let mut parts = partitioned.physical_parts().collect::<Vec<_>>();
                    for (local_range, part) in parts.drain(..).rev() {
                        let start = point_range
                            .start
                            .checked_add(local_range.start)
                            .ok_or_else(|| {
                                error::data_corrupted("exact partition start overflow")
                            })?;
                        let end = point_range
                            .start
                            .checked_add(local_range.end)
                            .ok_or_else(|| error::data_corrupted("exact partition end overflow"))?;
                        if end > point_range.end {
                            return Err(error::data_corrupted(
                                "nested exact partition exceeds its parent domain",
                            ));
                        }
                        pending.push((part.physical_partitions(), start..end));
                    }
                }
            }
        }
        if plan.row_count() != row_set.len() {
            return Err(error::data_corrupted(format!(
                "exact physical scan planned {} rows, expected {}",
                plan.row_count(),
                row_set.len()
            )));
        }
        Ok(plan)
    }

    fn plan_exact_scan_lanes<'a>(
        ranges: &[ExactPhysicalRange<'a>],
        row_count: u64,
        lane_count: usize,
    ) -> Result<Vec<ExactScanLane<'a>>> {
        let mut lanes = (0..lane_count)
            .map(|_| ExactScanLane::default())
            .collect::<Vec<_>>();
        let target_rows = row_count.div_ceil(lane_count as u64);
        let mut lane = 0usize;
        let mut rows_in_lane = 0u64;
        let mut observed_rows = 0u64;
        for &range in ranges {
            let mut first_row = 0u64;
            let range_rows = range.len();
            observed_rows = observed_rows.saturating_add(range_rows);
            while first_row < range_rows {
                let available = target_rows.saturating_sub(rows_in_lane).max(1);
                let take = available.min(range_rows - first_row);
                lanes[lane].ranges.push(range.slice(first_row, take)?);
                first_row += take;
                rows_in_lane = rows_in_lane.saturating_add(take);
                if rows_in_lane >= target_rows && lane + 1 < lane_count {
                    lane += 1;
                    rows_in_lane = 0;
                }
            }
        }
        if observed_rows != row_count {
            return Err(error::data_corrupted(format!(
                "exact scan lane cardinality mismatch: expected {row_count}, got {observed_rows}"
            )));
        }
        if lanes.iter().any(|lane| lane.ranges.is_empty()) {
            return Err(error::internal(
                "exact scan lane planner produced an empty worker lane",
            ));
        }
        Ok(lanes)
    }

    fn scan_covering_range_into(
        &self,
        best: &mut ScanTopK<ScoredPoint>,
        scorer: &mut VectorScorer,
        range: CoveringScanRange<'_>,
        work: &SearchWorkBudget,
    ) -> Result<()> {
        const SCORE_BATCH: usize = crate::index::hnsw::batch_scorer::BATCH_SIZE;
        let end_row = range
            .first_row
            .checked_add(range.len)
            .ok_or_else(|| error::data_corrupted("predicate covering row range overflow"))?;
        let row_ids = range
            .range
            .row_ids()
            .get(range.first_row..end_row)
            .ok_or_else(|| error::data_corrupted("predicate covering row range exceeds block"))?;
        let dimension = self.vector_storage.vector_dim();
        let vector_start = range
            .first_row
            .checked_mul(dimension)
            .ok_or_else(|| error::data_corrupted("predicate covering vector range overflow"))?;
        let vector_end = end_row
            .checked_mul(dimension)
            .ok_or_else(|| error::data_corrupted("predicate covering vector range overflow"))?;
        let vectors = range
            .range
            .vectors()
            .get(vector_start..vector_end)
            .ok_or_else(|| {
                error::data_corrupted("predicate covering vector range exceeds block")
            })?;

        for (rows, vectors) in row_ids
            .chunks(SCORE_BATCH)
            .zip(vectors.chunks(SCORE_BATCH.saturating_mul(dimension)))
        {
            work.check_and_consume(rows.len())?;
            for point in scorer.score_covering_contiguous(rows, vectors) {
                best.push(point);
            }
        }
        Ok(())
    }

    fn scan_exact_points_into(
        &self,
        best: &mut ScanTopK<ScoredPoint>,
        scorer: &mut VectorScorer,
        point_ids: impl Iterator<Item = PointOffset>,
        work: &SearchWorkBudget,
    ) -> Result<()> {
        const SCORE_BATCH: usize = crate::index::hnsw::batch_scorer::BATCH_SIZE;
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
            work.check_and_consume(len)?;
            for point in scorer.score_points_unfiltered(&chunk[..len]) {
                best.push(point);
            }
        }
        Ok(())
    }

    fn plain_scan_iter(
        &self,
        top_k: usize,
        scorer: &mut VectorScorer,
        point_ids: impl Iterator<Item = PointOffset>,
        work: &SearchWorkBudget,
    ) -> Result<Vec<ScoredPoint>> {
        let mut best = ScanTopK::new(top_k);
        self.scan_exact_points_into(&mut best, scorer, point_ids, work)?;
        Ok(best.into_sorted_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::{
        HnswBuilder, HnswM, HnswSearchObjective, InMemoryVectorStorage, PointOffset,
    };
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

    fn test_dictionary_values(ordinals: &[u16]) -> Box<[Option<Bytes>]> {
        ordinals
            .iter()
            .map(|ordinal| Some(Bytes::copy_from_slice(&ordinal.to_le_bytes())))
            .collect()
    }

    #[test]
    fn covering_scan_lane_planner_splits_one_large_contiguous_run_without_gaps() {
        const ROWS: u32 = 40_000;
        let posting = Arc::new(RoaringBitmap::from_iter(0..ROWS));
        let layout = PredicateScanLayout::from_build_columns(
            1,
            ROWS as usize,
            vec![PredicateScanBuildColumn {
                column_id: 7,
                blocks: vec![PredicateScanBuildBlock {
                    dictionary_ordinals: vec![0].into_boxed_slice(),
                    dictionary_values: test_dictionary_values(&[0]),
                    ordinal_row_counts: vec![ROWS].into_boxed_slice(),
                    ordinal_fingerprints: vec![crate::index::bitmap::posting_fingerprint(&posting)]
                        .into_boxed_slice(),
                    row_ids: (0..ROWS).collect::<Vec<_>>().into_boxed_slice(),
                }],
            }],
            Arc::new(InMemoryVectorStorage::new(vec![0.0; ROWS as usize], 1)),
        )
        .and_then(|layout| layout.serialize())
        .and_then(|bytes| PredicateScanLayout::deserialize_bytes(Bytes::from(bytes)))
        .unwrap();
        let exact_postings = [crate::index::ExactOrdinalPosting::new(0, posting)];
        let ranges = layout
            .selected_ranges_for_partition(7, 0..ROWS, &exact_postings)
            .unwrap()
            .unwrap();
        let ranges = ranges
            .into_iter()
            .map(|range| {
                ExactPhysicalRange::Covering(CoveringScanRange {
                    first_row: 0,
                    len: range.row_ids().len(),
                    range,
                })
            })
            .collect::<Vec<_>>();
        let lanes = HnswIndex::plan_exact_scan_lanes(&ranges, u64::from(ROWS), 3).unwrap();

        assert_eq!(lanes.len(), 3);
        assert!(lanes.iter().all(|lane| !lane.ranges.is_empty()));
        assert_eq!(
            lanes
                .iter()
                .flat_map(|lane| lane.ranges.iter())
                .map(|range| range.len())
                .sum::<u64>(),
            u64::from(ROWS)
        );
        let starts = lanes
            .iter()
            .flat_map(|lane| lane.ranges.iter())
            .map(|range| match range {
                ExactPhysicalRange::Covering(range) => range.first_row,
                _ => panic!("expected covering range"),
            })
            .collect::<Vec<_>>();
        assert_eq!(starts, vec![0, 13_334, 26_668]);
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
            builder
                .insert_single_point(i as u32, storage.as_ref(), distance)
                .unwrap();
        }

        let (links, entry_points) = builder.into_graph_data().unwrap();
        let graph = GraphLayers::new(links, entry_points, (&config).into());
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
                .for_each_link(point_id, 0, |neighbor| neighbors.push(neighbor))
                .unwrap();

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
        let config = HnswConfig::new(8, 50);
        let index = HnswIndex::build(storage, config, DistanceMetric::DotProduct);

        assert_eq!(index.graph.num_points(), 4);
        assert!(!index.graph.entry_points.entry_points.is_empty());
    }

    #[test]
    fn build_is_byte_deterministic_for_same_seed() {
        let vectors = make_sift_like_vectors(0xabc, 256, 16, 12);
        let config = HnswConfig::new(12, 72).with_build_seed(0x1234_5678_9abc_def0);
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
        for distance in [DistanceMetric::Euclidean, DistanceMetric::Cosine] {
            let mut contract = HnswConfig::new(12, 72)
                .with_build_seed(0x0fed_cba9_8765_4321)
                .build_contract(distance);
            contract.vector_encoding = HnswBuildVectorEncoding::symmetric_i16(24).unwrap();
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
            let width_1 = build(1);
            let width_2 = build(2);
            let width_8 = build(8);

            assert_eq!(width_1, width_2, "distance={distance:?}");
            assert_eq!(width_1, width_8, "distance={distance:?}");

            let shared_pool = rayon::ThreadPoolBuilder::new()
                .num_threads(8)
                .build()
                .unwrap();
            let build_with_grant = |parallelism| {
                HnswIndex::build_with_controls_and_filter_blocks_in_workspace_with_parallelism(
                    make_storage(&vectors),
                    contract,
                    HnswFilterBlocks::default(),
                    Some(&shared_pool),
                    parallelism,
                    None,
                    None,
                )
                .unwrap()
                .serialize()
                .unwrap()
            };
            assert_eq!(width_1, build_with_grant(2), "distance={distance:?}");
            assert_eq!(width_1, build_with_grant(5), "distance={distance:?}");
        }
    }

    #[test]
    fn predicate_block_scheduler_is_byte_deterministic_across_pool_widths() {
        const POINTS: usize = 2_048;
        const BLOCK_ROWS: usize = 256;
        let vectors = make_sift_like_vectors(0x517a, POINTS, 24, 16);
        let mut contract = HnswConfig::new(12, 72)
            .with_build_seed(0x6d91_e31a_472b_850f)
            .build_contract(DistanceMetric::Euclidean);
        contract.vector_encoding = HnswBuildVectorEncoding::symmetric_i16(24).unwrap();
        contract.filter_topology =
            HnswFilterTopologyContract::from_columns(&[7], BLOCK_ROWS as u32, 4).unwrap();
        let make_blocks = || HnswFilterBlocks {
            columns: vec![HnswFilterColumnBlocks {
                column_id: 7,
                blocks: (0..POINTS / BLOCK_ROWS)
                    .map(|block| {
                        let point_ids = (block * BLOCK_ROWS..(block + 1) * BLOCK_ROWS)
                            .map(|point| point as PointOffset)
                            .collect::<Vec<_>>();
                        HnswFilterBlock {
                            dictionary_ordinals: vec![block as u32].into_boxed_slice(),
                            dictionary_values: test_dictionary_values(&[block as u16]),
                            ordinal_row_counts: vec![BLOCK_ROWS as u32].into_boxed_slice(),
                            ordinal_fingerprints: vec![crate::index::bitmap::posting_fingerprint(
                                &RoaringBitmap::from_iter(point_ids.iter().copied()),
                            )]
                            .into_boxed_slice(),
                            point_ids: point_ids.into_boxed_slice(),
                        }
                    })
                    .collect(),
            }],
        };
        let build = |width| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(width)
                .build()
                .unwrap();
            HnswIndex::build_with_controls_and_filter_blocks(
                make_storage(&vectors),
                contract,
                make_blocks(),
                Some(&pool),
                None,
            )
            .unwrap()
            .serialize()
            .unwrap()
        };

        let width_1 = build(1);
        let width_2 = build(2);
        let width_8 = build(8);
        assert_eq!(width_1, width_2);
        assert_eq!(width_1, width_8);
    }

    #[test]
    fn frozen_wave_build_retains_sift_like_recall() {
        let vectors = make_sift_like_vectors(0x123, 2_048, 32, 24);
        let queries = make_sift_like_queries(0x456, &vectors, 64, 0.02);
        let config = HnswConfig::new(16, 96).with_ef(96);
        let mut contract = config.build_contract(DistanceMetric::Euclidean);
        contract.vector_encoding = HnswBuildVectorEncoding::symmetric_i16(32).unwrap();
        let index = HnswBuilder::new()
            .build(make_storage(&vectors), contract)
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
        let config = HnswConfig::new(8, 50).with_ef(50);
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
    fn predicate_topology_rejects_missing_or_incomplete_column_partitions() {
        let vectors = vec![vec![0.0], vec![1.0], vec![2.0], vec![3.0]];
        let mut contract = HnswConfig::new(4, 16).build_contract(DistanceMetric::Euclidean);
        contract.filter_topology = HnswFilterTopologyContract::from_columns(&[7], 2, 2).unwrap();

        let missing = HnswBuilder::new()
            .build(make_storage(&vectors), contract)
            .err()
            .expect("enabled topology requires its configured column partition");
        assert!(missing.to_string().contains("filter-block columns"));

        let incomplete = HnswBuilder::new()
            .build_with_filter_blocks(
                make_storage(&vectors),
                contract,
                HnswFilterBlocks {
                    columns: vec![HnswFilterColumnBlocks {
                        column_id: 7,
                        blocks: vec![HnswFilterBlock {
                            dictionary_ordinals: vec![0].into_boxed_slice(),
                            dictionary_values: test_dictionary_values(&[0]),
                            ordinal_row_counts: vec![3].into_boxed_slice(),
                            ordinal_fingerprints: vec![crate::index::bitmap::posting_fingerprint(
                                &RoaringBitmap::from_iter([0, 1, 2]),
                            )]
                            .into_boxed_slice(),
                            point_ids: vec![0, 1, 2].into_boxed_slice(),
                        }],
                    }],
                },
            )
            .err()
            .expect("each configured column must cover every vector exactly once");
        assert!(incomplete.to_string().contains("complete vector domain"));
    }

    #[test]
    fn predicate_topology_persists_tagged_hierarchical_block_entries() {
        let vectors = make_sift_like_vectors(0x7711, 256, 16, 16);
        let mut contract = HnswConfig::new(12, 72).build_contract(DistanceMetric::Euclidean);
        contract.filter_topology =
            HnswFilterTopologyContract::from_columns(&[7, 8], 64, 4).unwrap();
        let blocks = (0..4)
            .map(|block| {
                let point_ids = ((block * 64)..((block + 1) * 64))
                    .map(|point| point as PointOffset)
                    .collect::<Vec<_>>();
                HnswFilterBlock {
                    dictionary_ordinals: vec![block as u32].into_boxed_slice(),
                    dictionary_values: test_dictionary_values(&[block as u16]),
                    ordinal_row_counts: vec![64].into_boxed_slice(),
                    ordinal_fingerprints: vec![crate::index::bitmap::posting_fingerprint(
                        &RoaringBitmap::from_iter(point_ids.iter().copied()),
                    )]
                    .into_boxed_slice(),
                    point_ids: point_ids.into_boxed_slice(),
                }
            })
            .collect::<Vec<_>>();
        let interleaved_blocks = (0..4)
            .map(|block| {
                let point_ids = (block..256)
                    .step_by(4)
                    .map(|point| point as PointOffset)
                    .collect::<Vec<_>>();
                HnswFilterBlock {
                    dictionary_ordinals: vec![block as u32].into_boxed_slice(),
                    dictionary_values: test_dictionary_values(&[block as u16]),
                    ordinal_row_counts: vec![64].into_boxed_slice(),
                    ordinal_fingerprints: vec![crate::index::bitmap::posting_fingerprint(
                        &RoaringBitmap::from_iter(point_ids.iter().copied()),
                    )]
                    .into_boxed_slice(),
                    point_ids: point_ids.into_boxed_slice(),
                }
            })
            .collect();
        let index = HnswBuilder::new()
            .build_with_filter_blocks(
                make_storage(&vectors),
                contract,
                HnswFilterBlocks {
                    columns: vec![
                        HnswFilterColumnBlocks {
                            column_id: 7,
                            blocks,
                        },
                        HnswFilterColumnBlocks {
                            column_id: 8,
                            blocks: interleaved_blocks,
                        },
                    ],
                },
            )
            .unwrap();

        assert_eq!(index.graph.predicate_entry_points.len(), 8);
        assert!(index.graph.predicate_entry_points.iter().all(|entry| {
            [7, 8].contains(&entry.column_id)
                && index
                    .graph
                    .predicate_links
                    .as_ref()
                    .is_some_and(|links| entry.level < links.num_levels(entry.point_id).unwrap())
        }));

        let artifact = index.serialize().unwrap();
        let restored = HnswIndex::deserialize(&artifact).unwrap();
        assert_eq!(
            restored.graph.predicate_entry_points,
            index.graph.predicate_entry_points
        );

        let admitted = RoaringBitmap::from_iter(0..128);
        let budget = ResourceBudget::default();
        let result = restored
            .search_one_with_policy_strategy(
                &vectors[100],
                1,
                &SearchParams {
                    ef: Some(64),
                    ..Default::default()
                },
                HnswSearchFilter::predicate(&admitted, &[7]),
                &HnswSearchPolicy {
                    ..HnswSearchPolicy::default()
                },
                HnswSearchStrategy::AdaptiveFilteredGraph,
                &budget,
            )
            .unwrap();
        assert_eq!(result.points[0].idx, 100);

        let ordinal_rows = crate::index::OrdinalRowSet::new(
            7,
            (0..256)
                .map(|point| (point / 64) as u16)
                .collect::<Vec<_>>()
                .into(),
            vec![0b11].into_boxed_slice(),
            false,
            128,
            vec![
                crate::index::ExactOrdinalPosting::new(
                    0,
                    Arc::new(RoaringBitmap::from_iter(0..64)),
                ),
                crate::index::ExactOrdinalPosting::new(
                    1,
                    Arc::new(RoaringBitmap::from_iter(64..128)),
                ),
            ]
            .into_boxed_slice(),
        );
        let exact = restored
            .search_one_with_policy_strategy(
                &vectors[100],
                5,
                &SearchParams::default(),
                HnswSearchFilter::predicate(&ordinal_rows, &[7]),
                &HnswSearchPolicy::default(),
                HnswSearchStrategy::ExactScan,
                &budget,
            )
            .unwrap();
        assert_eq!(
            exact.outcome.path,
            HnswSearchPath::ExactScan(HnswExactScanKind::PredicateCovering)
        );
        assert_eq!(exact.scored_points, 128);
        assert_eq!(exact.points[0].idx, 100);
        assert!(exact.points.iter().all(|point| point.idx < 128));

        let covered_part: Arc<dyn ExactRowSet> = Arc::new(crate::index::OrdinalRowSet::new(
            7,
            (0..128)
                .map(|point| (point / 64) as u16)
                .collect::<Vec<_>>()
                .into(),
            vec![0b11].into_boxed_slice(),
            false,
            128,
            vec![
                crate::index::ExactOrdinalPosting::new(
                    0,
                    Arc::new(RoaringBitmap::from_iter(0..64)),
                ),
                crate::index::ExactOrdinalPosting::new(
                    1,
                    Arc::new(RoaringBitmap::from_iter(64..128)),
                ),
            ]
            .into_boxed_slice(),
        ));
        let base_part: Arc<dyn ExactRowSet> = Arc::new(RoaringBitmap::from_iter(0..128));
        let mixed_rows = crate::index::PartitionExactRowSet::try_new(vec![
            (0..128, covered_part),
            (128..256, base_part),
        ])
        .unwrap();
        let mixed_filter = HnswSearchFilter::predicate(&mixed_rows, &[7]);
        assert_eq!(
            restored.exact_scan_workload(mixed_filter),
            HnswExactScanWorkload {
                sequential_rows: 128,
                indexed_base_rows: 128,
            }
        );
        let mixed = restored
            .search_one_with_policy_strategy(
                &vectors[100],
                5,
                &SearchParams::default(),
                mixed_filter,
                &HnswSearchPolicy::default(),
                HnswSearchStrategy::ExactScan,
                &budget,
            )
            .unwrap();
        assert_eq!(
            mixed.outcome.path,
            HnswSearchPath::ExactScan(HnswExactScanKind::Hybrid)
        );
        assert_eq!(mixed.scored_points, 256);
        assert_eq!(mixed.points[0].idx, 100);
    }

    #[test]
    #[serial_test::serial]
    fn test_hnsw_search_many_matches_search_one_hnsw_path() {
        let vectors = make_sift_like_vectors(7, 384, 24, 16);
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(16, 96).with_ef(96);
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
            rerank_window: None,
            objective: HnswSearchObjective::CostOptimized,
            random_entry_point: Some(false),
        };
        let top_k = 12;

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
    fn test_hnsw_search_many_matches_search_one_full_scan_path() {
        let vectors = make_sift_like_vectors(9, 96, 12, 8);
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(8, 32).with_ef(64);
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
        let config = HnswConfig::new(8, 32).with_ef(64);
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
        let config = HnswConfig::new(16, 96).with_ef(96);
        let index = HnswIndex::build(storage, config, DistanceMetric::Euclidean);

        let query = make_sift_like_queries(23, &vectors, 1, 0.02)
            .into_iter()
            .next()
            .unwrap();
        let prepared_queries = vec![DistanceMetric::Euclidean.prepare(&query)];
        let params = SearchParams {
            ef: Some(96),
            rerank_window: None,
            objective: HnswSearchObjective::CostOptimized,
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
        let config = HnswConfig::new(8, 50).with_ef(50);
        let index = HnswIndex::build(storage, config, DistanceMetric::DotProduct);

        let entry_id = index.graph.entry_points.entry_points[0].point_id;
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(entry_id);

        let params = SearchParams {
            ef: Some(50),
            rerank_window: None,
            objective: HnswSearchObjective::CostOptimized,
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
                HnswSearchFilter::predicate(&filter, &[]),
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
            HnswSearchFilter::predicate(&filter, &[]),
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
        let config = HnswConfig::new(16, 96).with_ef(96);
        let index = HnswIndex::build(storage, config, DistanceMetric::Euclidean);
        let query = &vectors[7];
        let params = SearchParams {
            ef: Some(96),
            rerank_window: None,
            objective: HnswSearchObjective::CostOptimized,
            random_entry_point: Some(false),
        };
        let policy = HnswSearchPolicy {
            ef_search: 96,
            ..HnswSearchPolicy::default()
        };
        let budget = crate::search::ResourceBudget::default();

        let broad = RoaringBitmap::from_iter((0..vectors.len() as u32).filter(|idx| idx % 4 != 0));
        let broad_rows = index
            .search_one_with_policy_strategy(
                query,
                10,
                &params,
                HnswSearchFilter::predicate(&broad, &[]),
                &policy,
                HnswSearchStrategy::AdaptiveFilteredGraph,
                &budget,
            )
            .unwrap();
        assert_eq!(broad_rows.len(), 10);
        let broad_telemetry = index.search_telemetry();
        assert_eq!(broad_telemetry.hnsw_adaptive_graph_count, 1);
        assert_eq!(broad_telemetry.hnsw_predicate_refinement_count, 0);
        assert_eq!(broad_telemetry.hnsw_deferred_beam_admission_count, 1);

        // The admission window, not an impossible 1.5*K target, governs the
        // decision when K approaches ef. This used to refine unconditionally
        // for K >= 67 with ef=96/100 even when nearly every row matched.
        let almost_all = RoaringBitmap::from_iter(1..vectors.len() as u32);
        let large_k = index
            .search_one_with_policy_strategy(
                query,
                67,
                &params,
                HnswSearchFilter::predicate(&almost_all, &[]),
                &policy,
                HnswSearchStrategy::AdaptiveFilteredGraph,
                &budget,
            )
            .unwrap();
        assert_eq!(large_k.len(), 67);
        assert!(!large_k.outcome.predicate_refined);

        let selective = RoaringBitmap::from_iter((0..vectors.len() as u32).step_by(64));
        let selective_rows = index
            .search_one_with_policy_strategy(
                query,
                6,
                &params,
                HnswSearchFilter::predicate(&selective, &[]),
                &policy,
                HnswSearchStrategy::AdaptiveFilteredGraph,
                &budget,
            )
            .unwrap();
        assert_eq!(selective_rows.len(), 6);
        let telemetry = index.search_telemetry();
        assert_eq!(telemetry.hnsw_adaptive_graph_count, 3);
        assert_eq!(telemetry.hnsw_predicate_refinement_count, 1);
    }

    #[test]
    fn test_hnsw_search_many_matches_search_one_filtered_topk_path() {
        let vectors = make_sift_like_vectors(29, 320, 20, 12);
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(16, 96).with_ef(96);
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
            rerank_window: None,
            objective: HnswSearchObjective::CostOptimized,
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
        let config = HnswConfig::new(8, 50);
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
        let config = HnswConfig::new(8, 50);
        let index = HnswIndex::build(storage.clone(), config, DistanceMetric::DotProduct);

        let params = SearchParams {
            ef: Some(10),
            ..Default::default()
        };
        let before = index.search_one(&[5.0], 1, &params, None).unwrap();

        let data = index.serialize().unwrap();
        let loaded = HnswIndex::deserialize(&data).unwrap();
        let after = loaded.search_one(&[5.0], 1, &params, None).unwrap();

        assert_eq!(before[0].idx, after[0].idx);
    }

    #[test]
    fn compact_routing_image_survives_artifact_roundtrip_and_exactly_rescores() {
        let vectors = make_sift_like_vectors(91, 256, 24, 16);
        let storage = make_storage(&vectors);
        let mut contract = HnswConfig::new(16, 96)
            .with_ef(96)
            .build_contract(DistanceMetric::Euclidean);
        contract.vector_encoding = HnswBuildVectorEncoding::symmetric_i16(8).unwrap();
        contract.validate().unwrap();
        let index = HnswIndex::build_with_controls(storage, contract, None, None).unwrap();
        assert!(index.vector_storage.i16_routing_view().is_some());

        let artifact = index.serialize().unwrap();
        let restored = HnswIndex::deserialize(&artifact).unwrap();
        let routing = restored
            .vector_storage
            .i16_routing_view()
            .expect("routing image must be byte-backed by the artifact");
        assert_eq!(routing.selected_dimensions.len(), 8);
        assert_eq!(routing.codes.len(), 256 * 16 * std::mem::size_of::<i16>());

        let params = SearchParams {
            ef: Some(96),
            ..Default::default()
        };
        let result = restored.search_one(&vectors[37], 1, &params, None).unwrap();
        assert_eq!(result[0].idx, 37);
        assert_eq!(result[0].score, 0.0);
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
        assert!(HnswIndex::deserialize(&legacy)
            .err()
            .expect("legacy envelope must fail")
            .to_string()
            .contains("artifact magic"));

        let mut unknown = bytes;
        unknown[4..8].copy_from_slice(&(HNSW_ARTIFACT_VERSION + 1).to_le_bytes());
        assert!(HnswIndex::deserialize(&unknown)
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
        let error = HnswIndex::deserialize(&bytes)
            .err()
            .expect("stale topology algorithm must require rebuild");
        assert!(error.to_string().contains("build contract version"));
        assert!(error.to_string().contains("rebuild the vector index"));
    }

    #[test]
    fn cosine_inverse_norms_are_persisted_with_the_index_artifact() {
        let vectors = vec![vec![3.0, 4.0], vec![0.0, 0.0], vec![1.0, 0.0]];
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(8, 32);
        let index = HnswIndex::build(storage, config, DistanceMetric::Cosine);

        let norms = index
            .vector_storage
            .cosine_inverse_norms()
            .expect("cosine index preprocessing");
        assert!((norms.value(0) - 0.2).abs() < 1e-6);
        assert_eq!(norms.value(1), 0.0);
        assert_eq!(norms.value(2), 1.0);

        let bytes = index.serialize().unwrap();
        let restored = HnswIndex::deserialize(&bytes).unwrap();
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
    fn sidecar_mmap_range_keeps_vectors_graph_and_norms_zero_copy() {
        let vectors = vec![vec![3.0, 4.0], vec![1.0, 0.0], vec![0.0, 1.0]];
        let storage: Arc<dyn VectorStorage> = make_storage(&vectors);
        let index = HnswIndex::build(
            Arc::clone(&storage),
            HnswConfig::new(8, 32),
            DistanceMetric::Cosine,
        );
        let artifact = index.serialize().unwrap();
        let prefix_len = HNSW_ARTIFACT_ALIGNMENT;
        let mut package = vec![0xA5; prefix_len];
        package.extend_from_slice(&artifact);
        package.extend_from_slice(&[0x5A; 11]);

        let temp_dir = TempDir::new().unwrap();
        let package_path = temp_dir.path().join("sidecar.pkg");
        fs::write(&package_path, package).unwrap();
        let file = fs::File::open(package_path).unwrap();
        let mmap = Arc::new(unsafe { MmapOptions::new().map(&file).unwrap() });
        let restored = HnswIndex::deserialize_mmap_range(mmap, prefix_len, artifact.len()).unwrap();

        assert!(restored.graph.links.is_mmap_backed());
        assert!(restored.vector_storage.is_mmap_backed());
        assert!(restored
            .vector_storage
            .cosine_inverse_norms()
            .expect("cosine norms")
            .is_mmap_backed());
    }

    #[test]
    fn seekable_envelope_writer_is_byte_identical_at_nonzero_offset() {
        let vectors = vec![vec![3.0, 4.0], vec![1.0, 0.0], vec![0.0, 1.0]];
        let index = HnswIndex::build(
            make_storage(&vectors),
            HnswConfig::new(8, 32),
            DistanceMetric::Cosine,
        );
        let expected = index.serialize().unwrap();
        let artifact_start = 64u64;
        let mut cursor = Cursor::new(Vec::new());
        let len = index
            .serialize_into_seekable(&mut cursor, artifact_start)
            .unwrap();

        assert_eq!(len as usize, expected.len());
        assert_eq!(
            &cursor.get_ref()[artifact_start as usize..artifact_start as usize + len as usize],
            expected.as_slice()
        );
        HnswIndex::deserialize(
            &cursor.get_ref()[artifact_start as usize..artifact_start as usize + len as usize],
        )
        .unwrap();
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
        let links = GraphLinks::try_new_from_edges(vec![vec![vec![1]], vec![vec![0]]], 16).unwrap();
        let mut encoded = Vec::new();
        links.serialize(&mut encoded).unwrap();
        // GraphLinks v5 starts with a 64-byte header followed immediately by
        // the first sentinel-terminated fixed link record.
        let first_link_offset = 64;
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
        let config = HnswConfig::new(8, 32);
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
        let config = HnswConfig::new(8, 32);
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
        let config = HnswConfig::new(8, 32);
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
    fn exact_scan_execution_uses_the_lowered_segment_strategy_only() {
        assert!(HnswIndex::should_use_plain_scan(
            HnswSearchStrategy::ExactScan
        ));
        assert!(!HnswIndex::should_use_plain_scan(
            HnswSearchStrategy::UnfilteredGraph
        ));
        assert!(!HnswIndex::should_use_plain_scan(
            HnswSearchStrategy::AdaptiveFilteredGraph
        ));
    }

    #[test]
    fn exact_scan_lane_planner_splits_postings_without_materializing_row_ids() {
        let first = RoaringBitmap::from_iter(0..10_000);
        let second = RoaringBitmap::from_iter(20_000..50_000);
        let postings = [
            crate::index::ExactOrdinalPosting::new(0, Arc::new(first)),
            crate::index::ExactOrdinalPosting::new(1, Arc::new(second)),
        ];
        let ranges = postings
            .iter()
            .map(|posting| {
                ExactPhysicalRange::Posting(ExactPostingRange {
                    posting: posting.rows(),
                    point_base: 0,
                    first_rank: 0,
                    len: posting.rows().len() as u32,
                })
            })
            .collect::<Vec<_>>();
        let lanes = HnswIndex::plan_exact_scan_lanes(&ranges, 40_000, 3).unwrap();

        assert_eq!(lanes.len(), 3);
        let lane_sizes = lanes
            .iter()
            .map(|lane| lane.ranges.iter().map(|range| range.len()).sum::<u64>())
            .collect::<Vec<_>>();
        assert_eq!(lane_sizes, vec![13_334, 13_334, 13_332]);

        let mut reconstructed = RoaringBitmap::new();
        for lane in lanes {
            for range in lane.ranges {
                let ExactPhysicalRange::Posting(range) = range else {
                    panic!("expected posting range")
                };
                let first = range.posting.select(range.first_rank).unwrap();
                reconstructed.extend(range.posting.range(first..).take(range.len as usize));
            }
        }
        assert_eq!(reconstructed.len(), 40_000);
        assert!(reconstructed.contains(0));
        assert!(!reconstructed.contains(10_000));
        assert!(reconstructed.contains(49_999));
    }

    #[test]
    fn parallel_exact_scan_merges_lane_topk_and_telemetry() {
        const ROWS: usize = 65_536;
        let vectors = (0..ROWS).map(|row| vec![row as f32]).collect::<Vec<_>>();
        let links = GraphLinks::try_new_from_edges(vec![vec![Vec::new()]; ROWS], 16).unwrap();
        let graph = GraphLayers::new(
            links,
            EntryPoints {
                entry_points: vec![super::super::EntryPoint {
                    point_id: 0,
                    level: 0,
                }],
                extra_entry_points: Vec::new(),
            },
            HnswM::new(8),
        );
        let config = HnswConfig::new(8, 32);
        let policy = config.search_policy();
        let index = HnswIndex::new(
            config,
            graph,
            make_storage(&vectors),
            DistanceMetric::DotProduct,
        );
        let filter = RoaringBitmap::from_iter((0..ROWS as u32).step_by(2));
        let budget = ResourceBudget::standalone(64 << 20, 1024, 4);

        let result = index
            .search_one_with_policy_strategy(
                &[1.0],
                5,
                &SearchParams::default(),
                HnswSearchFilter::predicate(&filter, &[]),
                &policy,
                HnswSearchStrategy::ExactScan,
                &budget,
            )
            .unwrap();

        assert_eq!(result.scored_points, filter.len());
        assert_eq!(
            result
                .points
                .iter()
                .map(|point| point.idx)
                .collect::<Vec<_>>(),
            vec![65_534, 65_532, 65_530, 65_528, 65_526]
        );
    }

    #[test]
    fn test_search_many_prepared_matches_search_one_for_large_segment_strong_filter() {
        let vectors = make_sift_like_vectors(43, 320, 20, 16);
        let storage = make_storage(&vectors);
        let config = HnswConfig::new(16, 96).with_ef(96);
        let index = HnswIndex::build(storage, config, DistanceMetric::Euclidean);

        let mut filter = RoaringBitmap::new();
        for idx in (0..vectors.len() as u32).step_by(40) {
            filter.insert(idx);
        }

        let queries = make_sift_like_queries(47, &vectors, 4, 0.02);
        let prepared_queries = prepare_queries(DistanceMetric::Euclidean, &queries);
        let params = SearchParams {
            ef: Some(96),
            rerank_window: None,
            objective: HnswSearchObjective::CostOptimized,
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
        let config = HnswConfig::new(8, 50);
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
        let config = HnswConfig::new(8, 64).with_ef(96);
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
        let config = HnswConfig::new(16, 200).with_ef(200);
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
