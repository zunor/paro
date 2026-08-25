// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Compact binary codec for search manifest fragments.
//!
//! This codec is intentionally explicit instead of serde-driven. Manifest open is
//! on the search query hot path, so binary-v2 keeps stable field order, small
//! enum tags, and little-endian scalar encoding without reflection-style map
//! dispatch. Binary-v2 stores generation-owned multi-segment partition
//! coverage for every artifact.

use paro_common::error::{self as paro_error, Result};

use crate::search::artifact::{ArtifactFileId, ArtifactLocation, SegmentPagePointer};
use crate::search::capability::{
    ArtifactSegmentRef, ArtifactSegmentSpan, CoverageState, SearchArtifactRef, SearchIndexKind,
    SearchPartitionCoverage,
};
use crate::search::inline_sink::{
    FullTextStatsDelta, HnswStatsDelta, SearchStatsDelta, SparseStatsDelta,
};
use crate::search::stats::{
    BuildWatermarks, CatchUpBacklogTier, ExecutionModes, FullTextProviderStats,
    GenerationMaintenanceState, GenerationRecoveryState, GenerationStats, HnswProviderStats,
    MaintenancePriority, SearchArtifactStats, SearchExecutionMode, SearchProviderStats,
    SparseProviderStats,
};
use crate::search::tail::{TailEntryId, TailMutationKind, TailPendingEntry, TailRowImageRef};

use super::{
    GenerationManifestRoot, ManifestCodecFamily, ManifestCodecKind, ManifestDelta,
    ManifestDeltaEntry, ManifestFileRef, ManifestShard,
};

const MAGIC: &[u8; 4] = b"PMB2";
const ROOT_FRAGMENT: u8 = 1;
const SHARD_FRAGMENT: u8 = 2;
const DELTA_FRAGMENT: u8 = 3;

pub(crate) trait BinaryManifestFragment: Sized {
    const FRAGMENT_TAG: u8;

    fn encode_binary(&self, writer: &mut BinaryWriter) -> Result<()>;
    fn decode_binary(reader: &mut BinaryReader<'_>) -> Result<Self>;
}

pub(crate) fn encode_binary_manifest_fragment<T: BinaryManifestFragment>(
    value: &T,
) -> Result<Vec<u8>> {
    let mut writer = BinaryWriter::new();
    writer.bytes(MAGIC);
    writer.u8(T::FRAGMENT_TAG);
    value.encode_binary(&mut writer)?;
    Ok(writer.finish())
}

pub(crate) fn decode_binary_manifest_fragment<T: BinaryManifestFragment>(
    bytes: &[u8],
) -> Result<T> {
    let mut reader = BinaryReader::new(bytes);
    reader.expect_magic(MAGIC)?;
    let fragment_tag = reader.u8()?;
    if fragment_tag != T::FRAGMENT_TAG {
        return Err(paro_error::serialization_error(format!(
            "decode binary search manifest fragment: expected tag {}, got {}",
            T::FRAGMENT_TAG,
            fragment_tag
        )));
    }
    let decoded = T::decode_binary(&mut reader)?;
    reader.finish()?;
    Ok(decoded)
}

pub(crate) fn encode_binary_root_fragment(
    root: &GenerationManifestRoot,
    materialized_state: Option<&ManifestShard>,
) -> Result<Vec<u8>> {
    let mut writer = BinaryWriter::new();
    writer.bytes(MAGIC);
    writer.u8(ROOT_FRAGMENT);
    root.encode_binary(&mut writer)?;
    match materialized_state {
        Some(state) => {
            writer.bool(true);
            state.encode_binary(&mut writer)?;
        }
        None => writer.bool(false),
    }
    Ok(writer.finish())
}

pub(crate) fn decode_binary_root_fragment(
    bytes: &[u8],
) -> Result<(GenerationManifestRoot, Option<ManifestShard>)> {
    let mut reader = BinaryReader::new(bytes);
    reader.expect_magic(MAGIC)?;
    let fragment_tag = reader.u8()?;
    if fragment_tag != ROOT_FRAGMENT {
        return Err(paro_error::serialization_error(format!(
            "decode binary search manifest root: expected tag {}, got {}",
            ROOT_FRAGMENT, fragment_tag
        )));
    }
    let root = GenerationManifestRoot::decode_binary(&mut reader)?;
    let materialized_state = if reader.bool()? {
        Some(ManifestShard::decode_binary(&mut reader)?)
    } else {
        None
    };
    reader.finish()?;
    Ok((root, materialized_state))
}

pub(crate) struct BinaryWriter {
    bytes: Vec<u8>,
}

impl BinaryWriter {
    fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(256),
        }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn bytes(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn usize(&mut self, value: usize) -> Result<()> {
        let value = u64::try_from(value).map_err(|_| {
            paro_error::serialization_error("encode binary search manifest: usize overflow")
        })?;
        self.u64(value);
        Ok(())
    }

    fn f32(&mut self, value: f32) {
        self.u32(value.to_bits());
    }

    fn f64(&mut self, value: f64) {
        self.u64(value.to_bits());
    }

    fn string(&mut self, value: &str) -> Result<()> {
        let len = u32::try_from(value.len()).map_err(|_| {
            paro_error::serialization_error("encode binary search manifest: string too large")
        })?;
        self.u32(len);
        self.bytes(value.as_bytes());
        Ok(())
    }

    fn len(&mut self, len: usize) -> Result<()> {
        let len = u32::try_from(len).map_err(|_| {
            paro_error::serialization_error("encode binary search manifest: vec too large")
        })?;
        self.u32(len);
        Ok(())
    }
}

pub(crate) struct BinaryReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BinaryReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_magic(&mut self, magic: &[u8]) -> Result<()> {
        let actual = self.take(magic.len())?;
        if actual != magic {
            return Err(paro_error::serialization_error(
                "decode binary search manifest fragment: missing PMB2 magic",
            ));
        }
        Ok(())
    }

    fn finish(&self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(paro_error::serialization_error(format!(
                "decode binary search manifest fragment: {} trailing bytes",
                self.bytes.len() - self.offset
            )));
        }
        Ok(())
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            paro_error::serialization_error("decode binary search manifest: offset overflow")
        })?;
        let slice = self.bytes.get(self.offset..end).ok_or_else(|| {
            paro_error::serialization_error("decode binary search manifest: truncated payload")
        })?;
        self.offset = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn bool(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(paro_error::serialization_error(format!(
                "decode binary search manifest: invalid bool tag {other}"
            ))),
        }
    }

    fn u32(&mut self) -> Result<u32> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(bytes))
    }

    fn i64(&mut self) -> Result<i64> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(i64::from_le_bytes(bytes))
    }

    fn usize(&mut self) -> Result<usize> {
        usize::try_from(self.u64()?).map_err(|_| {
            paro_error::serialization_error("decode binary search manifest: usize overflow")
        })
    }

    fn f32(&mut self) -> Result<f32> {
        Ok(f32::from_bits(self.u32()?))
    }

    fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.u64()?))
    }

    fn string(&mut self) -> Result<String> {
        let len = self.len()?;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|err| {
            paro_error::serialization_error(format!(
                "decode binary search manifest: invalid utf8 string: {err}"
            ))
        })
    }

    fn len(&mut self) -> Result<usize> {
        Ok(self.u32()? as usize)
    }
}

impl BinaryManifestFragment for GenerationManifestRoot {
    const FRAGMENT_TAG: u8 = ROOT_FRAGMENT;

    fn encode_binary(&self, writer: &mut BinaryWriter) -> Result<()> {
        writer.u64(self.definition_id);
        writer.u64(self.generation_id);
        writer.u64(self.build_epoch);
        writer.i64(self.build_snapshot_version);
        writer.u64(self.indexed_through_ts);
        writer.u64(self.config_fingerprint);
        encode_coverage_state(writer, &self.coverage)?;
        encode_generation_stats(writer, &self.generation_stats)?;
        writer.u64(self.next_tail_entry_id.0);
        encode_execution_modes(writer, &self.execution_modes)?;
        encode_maintenance_state(writer, &self.maintenance_state)?;
        writer.u64(self.root_version);
        writer.u64(self.checksum);
        encode_vec(writer, &self.shard_files, encode_manifest_file_ref)?;
        encode_vec(writer, &self.recent_delta_files, encode_manifest_file_ref)?;
        encode_manifest_file_ref_option(writer, self.materialized_state_file.as_ref())
    }

    fn decode_binary(reader: &mut BinaryReader<'_>) -> Result<Self> {
        Ok(Self {
            definition_id: reader.u64()?,
            generation_id: reader.u64()?,
            build_epoch: reader.u64()?,
            build_snapshot_version: reader.i64()?,
            indexed_through_ts: reader.u64()?,
            config_fingerprint: reader.u64()?,
            coverage: decode_coverage_state(reader)?,
            generation_stats: decode_generation_stats(reader)?,
            next_tail_entry_id: TailEntryId(reader.u64()?),
            execution_modes: decode_execution_modes(reader)?,
            maintenance_state: decode_maintenance_state(reader)?,
            root_version: reader.u64()?,
            checksum: reader.u64()?,
            shard_files: decode_vec(reader, decode_manifest_file_ref)?,
            recent_delta_files: decode_vec(reader, decode_manifest_file_ref)?,
            materialized_state_file: decode_manifest_file_ref_option(reader)?,
        })
    }
}

impl BinaryManifestFragment for ManifestShard {
    const FRAGMENT_TAG: u8 = SHARD_FRAGMENT;

    fn encode_binary(&self, writer: &mut BinaryWriter) -> Result<()> {
        encode_vec(writer, &self.artifact_refs, encode_artifact_ref)?;
        encode_vec(writer, &self.tail_pending_entries, encode_tail_entry)
    }

    fn decode_binary(reader: &mut BinaryReader<'_>) -> Result<Self> {
        Ok(Self {
            artifact_refs: decode_vec(reader, decode_artifact_ref)?,
            tail_pending_entries: decode_vec(reader, decode_tail_entry)?,
        })
    }
}

impl BinaryManifestFragment for ManifestDelta {
    const FRAGMENT_TAG: u8 = DELTA_FRAGMENT;

    fn encode_binary(&self, writer: &mut BinaryWriter) -> Result<()> {
        encode_vec(writer, &self.entries, encode_delta_entry)
    }

    fn decode_binary(reader: &mut BinaryReader<'_>) -> Result<Self> {
        Ok(Self {
            entries: decode_vec(reader, decode_delta_entry)?,
        })
    }
}

fn encode_vec<T>(
    writer: &mut BinaryWriter,
    values: &[T],
    encode: fn(&mut BinaryWriter, &T) -> Result<()>,
) -> Result<()> {
    writer.len(values.len())?;
    for value in values {
        encode(writer, value)?;
    }
    Ok(())
}

fn decode_vec<T>(
    reader: &mut BinaryReader<'_>,
    decode: fn(&mut BinaryReader<'_>) -> Result<T>,
) -> Result<Vec<T>> {
    let len = reader.len()?;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(decode(reader)?);
    }
    Ok(values)
}

fn encode_manifest_file_ref(writer: &mut BinaryWriter, file: &ManifestFileRef) -> Result<()> {
    writer.string(&file.file_name)?;
    encode_codec_kind(writer, file.codec);
    Ok(())
}

fn decode_manifest_file_ref(reader: &mut BinaryReader<'_>) -> Result<ManifestFileRef> {
    Ok(ManifestFileRef {
        file_name: reader.string()?,
        codec: decode_codec_kind(reader)?,
    })
}

fn encode_manifest_file_ref_option(
    writer: &mut BinaryWriter,
    file: Option<&ManifestFileRef>,
) -> Result<()> {
    match file {
        Some(file) => {
            writer.bool(true);
            encode_manifest_file_ref(writer, file)
        }
        None => {
            writer.bool(false);
            Ok(())
        }
    }
}

fn decode_manifest_file_ref_option(
    reader: &mut BinaryReader<'_>,
) -> Result<Option<ManifestFileRef>> {
    if reader.bool()? {
        Ok(Some(decode_manifest_file_ref(reader)?))
    } else {
        Ok(None)
    }
}

fn encode_codec_kind(writer: &mut BinaryWriter, codec: ManifestCodecKind) {
    writer.u8(match codec.family {
        ManifestCodecFamily::JsonDebug => 0,
        ManifestCodecFamily::Binary => 1,
    });
    writer.u32(codec.version);
}

fn decode_codec_kind(reader: &mut BinaryReader<'_>) -> Result<ManifestCodecKind> {
    let family = match reader.u8()? {
        0 => ManifestCodecFamily::JsonDebug,
        1 => ManifestCodecFamily::Binary,
        other => {
            return Err(paro_error::serialization_error(format!(
                "decode binary search manifest: invalid codec family tag {other}"
            )))
        }
    };
    Ok(ManifestCodecKind {
        family,
        version: reader.u32()?,
    })
}

fn encode_coverage_state(writer: &mut BinaryWriter, coverage: &CoverageState) -> Result<()> {
    match coverage {
        CoverageState::Complete => writer.u8(0),
        CoverageState::TailPending {
            pending_rowsets,
            pending_segments,
            pending_rows,
            exact_tail_merge,
        } => {
            writer.u8(1);
            writer.usize(*pending_rowsets)?;
            writer.usize(*pending_segments)?;
            writer.u64(*pending_rows);
            writer.bool(*exact_tail_merge);
        }
    }
    Ok(())
}

fn decode_coverage_state(reader: &mut BinaryReader<'_>) -> Result<CoverageState> {
    match reader.u8()? {
        0 => Ok(CoverageState::Complete),
        1 => Ok(CoverageState::TailPending {
            pending_rowsets: reader.usize()?,
            pending_segments: reader.usize()?,
            pending_rows: reader.u64()?,
            exact_tail_merge: reader.bool()?,
        }),
        other => Err(paro_error::serialization_error(format!(
            "decode binary search manifest: invalid coverage tag {other}"
        ))),
    }
}

fn encode_generation_stats(writer: &mut BinaryWriter, stats: &GenerationStats) -> Result<()> {
    writer.u64(stats.indexed_rows);
    writer.usize(stats.artifact_count)?;
    encode_provider_stats_option(writer, stats.provider_stats.as_ref())
}

fn decode_generation_stats(reader: &mut BinaryReader<'_>) -> Result<GenerationStats> {
    Ok(GenerationStats {
        indexed_rows: reader.u64()?,
        artifact_count: reader.usize()?,
        provider_stats: decode_provider_stats_option(reader)?,
    })
}

fn encode_artifact_stats(writer: &mut BinaryWriter, stats: &SearchArtifactStats) -> Result<()> {
    writer.u64(stats.row_count);
    writer.u64(stats.bytes_on_disk);
    encode_provider_stats_option(writer, stats.provider_stats.as_ref())
}

fn decode_artifact_stats(reader: &mut BinaryReader<'_>) -> Result<SearchArtifactStats> {
    Ok(SearchArtifactStats {
        row_count: reader.u64()?,
        bytes_on_disk: reader.u64()?,
        provider_stats: decode_provider_stats_option(reader)?,
    })
}

fn encode_provider_stats_option(
    writer: &mut BinaryWriter,
    stats: Option<&SearchProviderStats>,
) -> Result<()> {
    match stats {
        Some(stats) => {
            writer.bool(true);
            encode_provider_stats(writer, stats)
        }
        None => {
            writer.bool(false);
            Ok(())
        }
    }
}

fn decode_provider_stats_option(
    reader: &mut BinaryReader<'_>,
) -> Result<Option<SearchProviderStats>> {
    if reader.bool()? {
        Ok(Some(decode_provider_stats(reader)?))
    } else {
        Ok(None)
    }
}

fn encode_provider_stats(writer: &mut BinaryWriter, stats: &SearchProviderStats) -> Result<()> {
    match stats {
        SearchProviderStats::FullText(stats) => {
            writer.u8(0);
            encode_fulltext_provider_stats(writer, stats)
        }
        SearchProviderStats::Sparse(stats) => {
            writer.u8(1);
            encode_sparse_provider_stats(writer, stats);
            Ok(())
        }
        SearchProviderStats::Hnsw(stats) => {
            writer.u8(2);
            encode_hnsw_provider_stats(writer, stats);
            Ok(())
        }
    }
}

fn decode_provider_stats(reader: &mut BinaryReader<'_>) -> Result<SearchProviderStats> {
    match reader.u8()? {
        0 => Ok(SearchProviderStats::FullText(
            decode_fulltext_provider_stats(reader)?,
        )),
        1 => Ok(SearchProviderStats::Sparse(decode_sparse_provider_stats(
            reader,
        )?)),
        2 => Ok(SearchProviderStats::Hnsw(decode_hnsw_provider_stats(
            reader,
        )?)),
        other => Err(paro_error::serialization_error(format!(
            "decode binary search manifest: invalid provider stats tag {other}"
        ))),
    }
}

fn encode_fulltext_provider_stats(
    writer: &mut BinaryWriter,
    stats: &FullTextProviderStats,
) -> Result<()> {
    writer.u32(stats.total_docs);
    writer.u64(stats.total_terms);
    writer.f32(stats.avg_doc_length);
    writer.u32(stats.unique_terms);
    writer.u64(stats.total_postings);
    writer.u32(stats.max_posting_list_len);
    writer.u32(stats.min_posting_list_len);
    writer.f32(stats.bm25_k1);
    writer.f32(stats.bm25_b);
    writer.string(&stats.tokenizer)
}

fn decode_fulltext_provider_stats(reader: &mut BinaryReader<'_>) -> Result<FullTextProviderStats> {
    Ok(FullTextProviderStats {
        total_docs: reader.u32()?,
        total_terms: reader.u64()?,
        avg_doc_length: reader.f32()?,
        unique_terms: reader.u32()?,
        total_postings: reader.u64()?,
        max_posting_list_len: reader.u32()?,
        min_posting_list_len: reader.u32()?,
        bm25_k1: reader.f32()?,
        bm25_b: reader.f32()?,
        tokenizer: reader.string()?,
    })
}

fn encode_sparse_provider_stats(writer: &mut BinaryWriter, stats: &SparseProviderStats) {
    writer.u64(stats.row_count);
    writer.u64(stats.nnz);
    writer.u64(stats.posting_fanout);
    writer.u64(stats.unique_dimensions);
    writer.f32(stats.avg_vector_nnz);
    writer.f64(stats.l2_norm_sum);
    writer.f32(stats.max_l2_norm);
}

fn decode_sparse_provider_stats(reader: &mut BinaryReader<'_>) -> Result<SparseProviderStats> {
    Ok(SparseProviderStats {
        row_count: reader.u64()?,
        nnz: reader.u64()?,
        posting_fanout: reader.u64()?,
        unique_dimensions: reader.u64()?,
        avg_vector_nnz: reader.f32()?,
        l2_norm_sum: reader.f64()?,
        max_l2_norm: reader.f32()?,
    })
}

fn encode_hnsw_provider_stats(writer: &mut BinaryWriter, stats: &HnswProviderStats) {
    writer.u64(stats.vector_count);
    writer.u32(stats.dimension);
    writer.u32(stats.max_level);
    writer.u32(stats.m);
    writer.u32(stats.ef_construction);
    writer.u64(stats.graph_memory_bytes);
    writer.u64(stats.vector_storage_bytes);
    writer.u64(stats.total_graph_links);
    writer.u64(stats.level0_graph_links);
    writer.f32(stats.avg_level0_degree);
    writer.u32(stats.max_level0_degree);
}

fn decode_hnsw_provider_stats(reader: &mut BinaryReader<'_>) -> Result<HnswProviderStats> {
    Ok(HnswProviderStats {
        vector_count: reader.u64()?,
        dimension: reader.u32()?,
        max_level: reader.u32()?,
        m: reader.u32()?,
        ef_construction: reader.u32()?,
        graph_memory_bytes: reader.u64()?,
        vector_storage_bytes: reader.u64()?,
        total_graph_links: reader.u64()?,
        level0_graph_links: reader.u64()?,
        avg_level0_degree: reader.f32()?,
        max_level0_degree: reader.u32()?,
    })
}

fn encode_execution_modes(writer: &mut BinaryWriter, modes: &ExecutionModes) -> Result<()> {
    let modes = modes.iter().copied().collect::<Vec<_>>();
    writer.len(modes.len())?;
    for mode in modes {
        writer.u8(match mode {
            SearchExecutionMode::Exact => 0,
            SearchExecutionMode::ExactTailMerge => 1,
            SearchExecutionMode::ApproxTopK => 2,
            SearchExecutionMode::ExactFallback => 3,
        });
    }
    Ok(())
}

fn decode_execution_modes(reader: &mut BinaryReader<'_>) -> Result<ExecutionModes> {
    let len = reader.len()?;
    let mut modes = Vec::with_capacity(len);
    for _ in 0..len {
        modes.push(match reader.u8()? {
            0 => SearchExecutionMode::Exact,
            1 => SearchExecutionMode::ExactTailMerge,
            2 => SearchExecutionMode::ApproxTopK,
            3 => SearchExecutionMode::ExactFallback,
            other => {
                return Err(paro_error::serialization_error(format!(
                    "decode binary search manifest: invalid execution mode tag {other}"
                )))
            }
        });
    }
    Ok(ExecutionModes::new(modes))
}

fn encode_maintenance_state(
    writer: &mut BinaryWriter,
    state: &GenerationMaintenanceState,
) -> Result<()> {
    encode_build_watermarks(writer, &state.build_watermarks);
    encode_recovery_state(writer, &state.recovery)?;
    writer.u64(state.tombstone_rows);
    writer.u32(state.tombstone_ratio_millis);
    Ok(())
}

fn decode_maintenance_state(reader: &mut BinaryReader<'_>) -> Result<GenerationMaintenanceState> {
    Ok(GenerationMaintenanceState {
        build_watermarks: decode_build_watermarks(reader)?,
        recovery: decode_recovery_state(reader)?,
        tombstone_rows: reader.u64()?,
        tombstone_ratio_millis: reader.u32()?,
    })
}

fn encode_build_watermarks(writer: &mut BinaryWriter, watermarks: &BuildWatermarks) {
    writer.i64(watermarks.snapshot_version);
    writer.i64(watermarks.replay_watermark);
    writer.i64(watermarks.cutover_watermark);
}

fn decode_build_watermarks(reader: &mut BinaryReader<'_>) -> Result<BuildWatermarks> {
    Ok(BuildWatermarks {
        snapshot_version: reader.i64()?,
        replay_watermark: reader.i64()?,
        cutover_watermark: reader.i64()?,
    })
}

fn encode_recovery_state(writer: &mut BinaryWriter, state: &GenerationRecoveryState) -> Result<()> {
    match state.catch_up_build_epoch {
        Some(epoch) => {
            writer.bool(true);
            writer.u64(epoch);
        }
        None => writer.bool(false),
    }
    writer.len(state.superseded_build_epochs.len())?;
    for epoch in &state.superseded_build_epochs {
        writer.u64(*epoch);
    }
    writer.usize(state.tail_pending_rowsets)?;
    writer.u64(state.tail_pending_rows);
    encode_backlog_tier(writer, state.backlog_tier);
    encode_maintenance_priority(writer, state.priority);
    writer.usize(state.rowset_rate_limit)?;
    writer.u64(state.row_rate_limit);
    Ok(())
}

fn decode_recovery_state(reader: &mut BinaryReader<'_>) -> Result<GenerationRecoveryState> {
    let catch_up_build_epoch = if reader.bool()? {
        Some(reader.u64()?)
    } else {
        None
    };
    let len = reader.len()?;
    let mut superseded_build_epochs = Vec::with_capacity(len);
    for _ in 0..len {
        superseded_build_epochs.push(reader.u64()?);
    }
    Ok(GenerationRecoveryState {
        catch_up_build_epoch,
        superseded_build_epochs,
        tail_pending_rowsets: reader.usize()?,
        tail_pending_rows: reader.u64()?,
        backlog_tier: decode_backlog_tier(reader)?,
        priority: decode_maintenance_priority(reader)?,
        rowset_rate_limit: reader.usize()?,
        row_rate_limit: reader.u64()?,
    })
}

fn encode_backlog_tier(writer: &mut BinaryWriter, tier: CatchUpBacklogTier) {
    writer.u8(match tier {
        CatchUpBacklogTier::Healthy => 0,
        CatchUpBacklogTier::Elevated => 1,
        CatchUpBacklogTier::Degraded => 2,
    });
}

fn decode_backlog_tier(reader: &mut BinaryReader<'_>) -> Result<CatchUpBacklogTier> {
    match reader.u8()? {
        0 => Ok(CatchUpBacklogTier::Healthy),
        1 => Ok(CatchUpBacklogTier::Elevated),
        2 => Ok(CatchUpBacklogTier::Degraded),
        other => Err(paro_error::serialization_error(format!(
            "decode binary search manifest: invalid backlog tier tag {other}"
        ))),
    }
}

fn encode_maintenance_priority(writer: &mut BinaryWriter, priority: MaintenancePriority) {
    writer.u8(match priority {
        MaintenancePriority::Idle => 0,
        MaintenancePriority::Opportunistic => 1,
        MaintenancePriority::Elevated => 2,
        MaintenancePriority::Critical => 3,
    });
}

fn decode_maintenance_priority(reader: &mut BinaryReader<'_>) -> Result<MaintenancePriority> {
    match reader.u8()? {
        0 => Ok(MaintenancePriority::Idle),
        1 => Ok(MaintenancePriority::Opportunistic),
        2 => Ok(MaintenancePriority::Elevated),
        3 => Ok(MaintenancePriority::Critical),
        other => Err(paro_error::serialization_error(format!(
            "decode binary search manifest: invalid maintenance priority tag {other}"
        ))),
    }
}

fn encode_delta_entry(writer: &mut BinaryWriter, entry: &ManifestDeltaEntry) -> Result<()> {
    match entry {
        ManifestDeltaEntry::AddArtifact(artifact) => {
            writer.u8(0);
            encode_artifact_ref(writer, artifact)
        }
        ManifestDeltaEntry::RemoveArtifact(coverage) => {
            writer.u8(1);
            encode_partition_coverage(writer, coverage)
        }
        ManifestDeltaEntry::UpsertTail(entry) => {
            writer.u8(2);
            encode_tail_entry(writer, entry)
        }
        ManifestDeltaEntry::CoverTail(entry_id) => {
            writer.u8(3);
            writer.u64(entry_id.0);
            Ok(())
        }
        ManifestDeltaEntry::StatsDelta(delta) => {
            writer.u8(4);
            encode_stats_delta(writer, delta)
        }
    }
}

fn decode_delta_entry(reader: &mut BinaryReader<'_>) -> Result<ManifestDeltaEntry> {
    match reader.u8()? {
        0 => Ok(ManifestDeltaEntry::AddArtifact(decode_artifact_ref(
            reader,
        )?)),
        1 => Ok(ManifestDeltaEntry::RemoveArtifact(
            decode_partition_coverage(reader)?,
        )),
        2 => Ok(ManifestDeltaEntry::UpsertTail(decode_tail_entry(reader)?)),
        3 => Ok(ManifestDeltaEntry::CoverTail(TailEntryId(reader.u64()?))),
        4 => Ok(ManifestDeltaEntry::StatsDelta(decode_stats_delta(reader)?)),
        other => Err(paro_error::serialization_error(format!(
            "decode binary search manifest: invalid delta entry tag {other}"
        ))),
    }
}

fn encode_artifact_ref(writer: &mut BinaryWriter, artifact: &SearchArtifactRef) -> Result<()> {
    writer.u64(artifact.definition_id);
    writer.u64(artifact.generation_id);
    encode_partition_coverage(writer, &artifact.coverage)?;
    writer.u32(artifact.column_id);
    encode_index_kind(writer, artifact.kind);
    writer.u32(artifact.provider_variant);
    writer.u32(artifact.artifact_format_version);
    encode_artifact_location(writer, &artifact.location);
    encode_artifact_stats(writer, &artifact.stats)?;
    writer.u64(artifact.checksum);
    Ok(())
}

fn decode_artifact_ref(reader: &mut BinaryReader<'_>) -> Result<SearchArtifactRef> {
    Ok(SearchArtifactRef {
        definition_id: reader.u64()?,
        generation_id: reader.u64()?,
        coverage: decode_partition_coverage(reader)?,
        column_id: reader.u32()?,
        kind: decode_index_kind(reader)?,
        provider_variant: reader.u32()?,
        artifact_format_version: reader.u32()?,
        location: decode_artifact_location(reader)?,
        stats: decode_artifact_stats(reader)?,
        checksum: reader.u64()?,
    })
}

fn encode_artifact_segment_ref(writer: &mut BinaryWriter, segment: &ArtifactSegmentRef) {
    writer.u64(segment.rowset_id);
    writer.u32(segment.segment_id);
}

fn decode_artifact_segment_ref(reader: &mut BinaryReader<'_>) -> Result<ArtifactSegmentRef> {
    Ok(ArtifactSegmentRef {
        rowset_id: reader.u64()?,
        segment_id: reader.u32()?,
    })
}

fn encode_partition_coverage(
    writer: &mut BinaryWriter,
    coverage: &SearchPartitionCoverage,
) -> Result<()> {
    writer.len(coverage.segments().len())?;
    for span in coverage.segments() {
        encode_artifact_segment_ref(writer, &span.segment);
        writer.u64(span.row_count);
    }
    Ok(())
}

fn decode_partition_coverage(reader: &mut BinaryReader<'_>) -> Result<SearchPartitionCoverage> {
    let span_count = reader.len()?;
    let mut spans = Vec::with_capacity(span_count);
    for _ in 0..span_count {
        spans.push(ArtifactSegmentSpan {
            segment: decode_artifact_segment_ref(reader)?,
            row_count: reader.u64()?,
        });
    }
    SearchPartitionCoverage::try_new(spans).map_err(|err| {
        paro_error::serialization_error(format!(
            "decode binary search manifest partition coverage: {err}"
        ))
    })
}

fn encode_index_kind(writer: &mut BinaryWriter, kind: SearchIndexKind) {
    writer.u8(match kind {
        SearchIndexKind::Hnsw => 0,
        SearchIndexKind::Sparse => 1,
        SearchIndexKind::FullText => 2,
    });
}

fn decode_index_kind(reader: &mut BinaryReader<'_>) -> Result<SearchIndexKind> {
    match reader.u8()? {
        0 => Ok(SearchIndexKind::Hnsw),
        1 => Ok(SearchIndexKind::Sparse),
        2 => Ok(SearchIndexKind::FullText),
        other => Err(paro_error::serialization_error(format!(
            "decode binary search manifest: invalid search kind tag {other}"
        ))),
    }
}

fn encode_artifact_location(writer: &mut BinaryWriter, location: &ArtifactLocation) {
    match location {
        ArtifactLocation::Inline { page } => {
            writer.u8(0);
            encode_segment_page_pointer(writer, page);
        }
        ArtifactLocation::SidecarArtifactFile {
            file_id,
            offset,
            len,
            checksum,
        } => {
            writer.u8(1);
            encode_artifact_file_id(writer, file_id);
            writer.u64(*offset);
            writer.u64(*len);
            writer.u64(*checksum);
        }
    }
}

fn decode_artifact_location(reader: &mut BinaryReader<'_>) -> Result<ArtifactLocation> {
    match reader.u8()? {
        0 => Ok(ArtifactLocation::Inline {
            page: decode_segment_page_pointer(reader)?,
        }),
        1 => Ok(ArtifactLocation::SidecarArtifactFile {
            file_id: decode_artifact_file_id(reader)?,
            offset: reader.u64()?,
            len: reader.u64()?,
            checksum: reader.u64()?,
        }),
        other => Err(paro_error::serialization_error(format!(
            "decode binary search manifest: invalid artifact location tag {other}"
        ))),
    }
}

fn encode_segment_page_pointer(writer: &mut BinaryWriter, page: &SegmentPagePointer) {
    writer.u64(page.rowset_id);
    writer.u32(page.segment_id);
    writer.u32(page.column_id);
    writer.u64(page.page_offset);
    writer.u64(page.page_len);
    writer.u64(page.checksum);
}

fn decode_segment_page_pointer(reader: &mut BinaryReader<'_>) -> Result<SegmentPagePointer> {
    Ok(SegmentPagePointer {
        rowset_id: reader.u64()?,
        segment_id: reader.u32()?,
        column_id: reader.u32()?,
        page_offset: reader.u64()?,
        page_len: reader.u64()?,
        checksum: reader.u64()?,
    })
}

fn encode_artifact_file_id(writer: &mut BinaryWriter, file_id: &ArtifactFileId) {
    writer.u64(file_id.definition_id);
    writer.u64(file_id.generation_id);
    writer.u32(file_id.package_index);
}

fn decode_artifact_file_id(reader: &mut BinaryReader<'_>) -> Result<ArtifactFileId> {
    Ok(ArtifactFileId {
        definition_id: reader.u64()?,
        generation_id: reader.u64()?,
        package_index: reader.u32()?,
    })
}

fn encode_tail_entry(writer: &mut BinaryWriter, entry: &TailPendingEntry) -> Result<()> {
    writer.u64(entry.entry_id.0);
    writer.u64(entry.rowset_id);
    writer.len(entry.segment_ids.len())?;
    for segment_id in &entry.segment_ids {
        writer.u32(*segment_id);
    }
    encode_tail_mutation_kind(writer, entry.mutation);
    writer.u64(entry.row_count);
    writer.u64(entry.byte_count);
    encode_tail_row_image_ref(writer, entry.row_image_ref.as_ref())
}

fn decode_tail_entry(reader: &mut BinaryReader<'_>) -> Result<TailPendingEntry> {
    let entry_id = TailEntryId(reader.u64()?);
    let rowset_id = reader.u64()?;
    let segment_count = reader.len()?;
    let mut segment_ids = Vec::with_capacity(segment_count);
    for _ in 0..segment_count {
        segment_ids.push(reader.u32()?);
    }
    Ok(TailPendingEntry {
        entry_id,
        rowset_id,
        segment_ids,
        mutation: decode_tail_mutation_kind(reader)?,
        row_count: reader.u64()?,
        byte_count: reader.u64()?,
        row_image_ref: decode_tail_row_image_ref(reader)?,
    })
}

fn encode_tail_mutation_kind(writer: &mut BinaryWriter, mutation: TailMutationKind) {
    writer.u8(match mutation {
        TailMutationKind::Append => 0,
        TailMutationKind::Replace => 1,
        TailMutationKind::Delete => 2,
    });
}

fn decode_tail_mutation_kind(reader: &mut BinaryReader<'_>) -> Result<TailMutationKind> {
    match reader.u8()? {
        0 => Ok(TailMutationKind::Append),
        1 => Ok(TailMutationKind::Replace),
        2 => Ok(TailMutationKind::Delete),
        other => Err(paro_error::serialization_error(format!(
            "decode binary search manifest: invalid tail mutation tag {other}"
        ))),
    }
}

fn encode_tail_row_image_ref(
    writer: &mut BinaryWriter,
    row_image_ref: Option<&TailRowImageRef>,
) -> Result<()> {
    match row_image_ref {
        None => writer.u8(0),
        Some(TailRowImageRef::WholeRowset) => writer.u8(1),
        Some(TailRowImageRef::PartialRowset {
            touched_columns,
            base_rowids_segments,
        }) => {
            writer.u8(2);
            writer.len(touched_columns.len())?;
            for column_id in touched_columns {
                writer.u32(*column_id);
            }
            writer.len(base_rowids_segments.len())?;
            for segment_id in base_rowids_segments {
                writer.u32(*segment_id);
            }
        }
    }
    Ok(())
}

fn decode_tail_row_image_ref(reader: &mut BinaryReader<'_>) -> Result<Option<TailRowImageRef>> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(TailRowImageRef::WholeRowset)),
        2 => {
            let touched_len = reader.len()?;
            let mut touched_columns = Vec::with_capacity(touched_len);
            for _ in 0..touched_len {
                touched_columns.push(reader.u32()?);
            }
            let base_len = reader.len()?;
            let mut base_rowids_segments = Vec::with_capacity(base_len);
            for _ in 0..base_len {
                base_rowids_segments.push(reader.u32()?);
            }
            Ok(Some(TailRowImageRef::PartialRowset {
                touched_columns,
                base_rowids_segments,
            }))
        }
        other => Err(paro_error::serialization_error(format!(
            "decode binary search manifest: invalid tail row image tag {other}"
        ))),
    }
}

fn encode_stats_delta(writer: &mut BinaryWriter, delta: &SearchStatsDelta) -> Result<()> {
    match delta {
        SearchStatsDelta::FullText(delta) => {
            writer.u8(0);
            encode_fulltext_provider_stats(writer, &delta.stats)
        }
        SearchStatsDelta::Sparse(delta) => {
            writer.u8(1);
            writer.u64(delta.row_count);
            writer.u64(delta.nnz);
            writer.u64(delta.posting_fanout);
            writer.u64(delta.unique_dimensions);
            writer.f64(delta.l2_norm_sum);
            writer.f32(delta.max_l2_norm);
            Ok(())
        }
        SearchStatsDelta::Hnsw(delta) => {
            writer.u8(2);
            writer.u64(delta.vector_count);
            writer.u32(delta.dimension);
            writer.u32(delta.max_level);
            writer.u32(delta.m);
            writer.u32(delta.ef_construction);
            writer.u64(delta.graph_memory_bytes);
            writer.u64(delta.vector_storage_bytes);
            writer.u64(delta.total_graph_links);
            writer.u64(delta.level0_graph_links);
            writer.f32(delta.avg_level0_degree);
            writer.u32(delta.max_level0_degree);
            Ok(())
        }
    }
}

fn decode_stats_delta(reader: &mut BinaryReader<'_>) -> Result<SearchStatsDelta> {
    match reader.u8()? {
        0 => Ok(SearchStatsDelta::FullText(FullTextStatsDelta {
            stats: decode_fulltext_provider_stats(reader)?,
        })),
        1 => Ok(SearchStatsDelta::Sparse(SparseStatsDelta {
            row_count: reader.u64()?,
            nnz: reader.u64()?,
            posting_fanout: reader.u64()?,
            unique_dimensions: reader.u64()?,
            l2_norm_sum: reader.f64()?,
            max_l2_norm: reader.f32()?,
        })),
        2 => Ok(SearchStatsDelta::Hnsw(HnswStatsDelta {
            vector_count: reader.u64()?,
            dimension: reader.u32()?,
            max_level: reader.u32()?,
            m: reader.u32()?,
            ef_construction: reader.u32()?,
            graph_memory_bytes: reader.u64()?,
            vector_storage_bytes: reader.u64()?,
            total_graph_links: reader.u64()?,
            level0_graph_links: reader.u64()?,
            avg_level0_degree: reader.f32()?,
            max_level0_degree: reader.u32()?,
        })),
        other => Err(paro_error::serialization_error(format!(
            "decode binary search manifest: invalid stats delta tag {other}"
        ))),
    }
}
