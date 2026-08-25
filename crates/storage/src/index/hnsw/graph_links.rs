// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Persistent HNSW adjacency storage.
//!
//! Version 4 stores each hot level-0 adjacency as one fixed-stride link record.
//! Unused slots contain `u32::MAX`, which cannot be a valid point id. For the
//! standard M0=32 contract a record is exactly 128 bytes: two aligned cache
//! lines instead of version 3's 132-byte degree-plus-links record that drifted
//! across cache-line boundaries. Upper levels remain delta-varint encoded.

use super::artifact_integrity::ArtifactIntegrity;
use super::types::PointOffset;
use bytes::Bytes;
use memmap2::Mmap;
use paro_common::error as paro_error;
use paro_common::error::Result;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::ptr::NonNull;
use std::sync::Arc;

const GRAPH_LINKS_MAGIC: u32 = u32::from_le_bytes(*b"HGLK");
const GRAPH_LINKS_VERSION_SENTINEL_LEVEL0_V4: u32 = 4;
const GRAPH_LINKS_HEADER_LEN: usize = 64;
const U64_BYTES: usize = std::mem::size_of::<u64>();
const POINT_BYTES: usize = std::mem::size_of::<PointOffset>();

#[derive(Debug)]
pub enum GraphLinksData {
    Ram(Vec<u8>),
    Bytes(Bytes),
    Mmap(Arc<Mmap>),
    MmapSlice {
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
    },
}

impl GraphLinksData {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Ram(data) => data,
            Self::Bytes(data) => data,
            Self::Mmap(mmap) => mmap,
            Self::MmapSlice { mmap, offset, len } => &mmap[*offset..*offset + *len],
        }
    }
}

#[derive(Debug)]
pub struct GraphLinks {
    data: GraphLinksData,
    integrity: Option<GraphIntegrityRange>,
    native_level0: Option<NativeLevel0View>,
    serialized_len_bytes: usize,
    point_count: usize,
    level0_link_count: usize,
    level0_records_offset: usize,
    level0_record_words: usize,
    level0_stride: usize,
    upper_offsets_offset: usize,
    upper_payload_offset: usize,
    upper_payload_len: usize,
}

#[derive(Debug, Clone)]
struct GraphIntegrityRange {
    verifier: Arc<ArtifactIntegrity>,
    artifact_offset: usize,
}

impl GraphIntegrityRange {
    #[inline(always)]
    fn verify(&self, local_offset: usize, len: usize) -> Result<()> {
        if self.verifier.is_fully_verified() {
            return Ok(());
        }
        self.verifier.verify_range(
            self.artifact_offset
                .checked_add(local_offset)
                .ok_or_else(|| paro_error::data_corrupted("GraphLinks integrity range overflow"))?,
            len,
        )
    }
}

/// Query-scoped view of immutable graph links.
///
/// Artifact authentication is a property of the complete immutable backing,
/// not of an individual edge expansion.  A view snapshots that state once at
/// the search boundary: while a background sweep is still running, random
/// reads continue to authenticate the ranges they expose; after the sweep has
/// published success, the hot graph loop carries no integrity-state load or
/// checksum-range arithmetic. Structural validation (sentinel padding and
/// neighbor-id bounds) remains enabled at publication.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GraphLinksReadView<'a> {
    links: &'a GraphLinks,
    integrity: Option<&'a GraphIntegrityRange>,
}

/// Point identity proven to belong to the graph/vector domain of a search.
///
/// The constructor remains private to this module. Graph traversal creates a
/// value only after validating the durable link target against `point_count`,
/// and `GraphLinks::search_view` first proves that the graph and scorer expose
/// the same cardinality. Hot consumers can therefore reuse this capability
/// instead of independently rechecking the same point id before every visited
/// lookup and vector prefetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct GraphPoint(PointOffset);

impl GraphPoint {
    #[inline(always)]
    pub(crate) fn offset(self) -> PointOffset {
        self.0
    }

    #[inline(always)]
    pub(crate) fn index(self) -> usize {
        self.0 as usize
    }
}

impl GraphLinksReadView<'_> {
    #[inline]
    pub(crate) fn for_each_link<F>(&self, point_id: PointOffset, level: usize, f: F) -> Result<()>
    where
        F: FnMut(GraphPoint),
    {
        self.links
            .for_each_link_with_integrity(point_id, level, self.integrity, f)
    }

    /// Prefetch only a point that has entered the expansion frontier.
    /// Discovery-time prefetching every neighbor pollutes caches because most
    /// scored points never become candidates. Frontier admission is the first
    /// boundary where a future level-0 record read is likely enough to justify
    /// bringing its first two cache lines in flight.
    #[inline]
    pub(crate) fn prefetch_level0_record(&self, point_id: PointOffset) {
        self.links.prefetch_level0_record(point_id);
    }
}

/// Typed little-endian view into the immutable backing owned by `GraphLinks`.
/// Pointers are established only after bytemuck validates alignment and exact
/// element widths. Vec, Bytes, and mmap allocations keep their address while
/// their owning handle moves, so the view remains valid for the owner's life.
#[derive(Debug, Clone, Copy)]
struct NativeLevel0View {
    records: NonNull<u32>,
    records_len: usize,
}

// The pointed-to bytes are immutable and owned by GraphLinks. Sharing these
// read-only views has the same thread-safety as sharing the backing itself.
unsafe impl Send for NativeLevel0View {}
unsafe impl Sync for NativeLevel0View {}

impl NativeLevel0View {
    fn from_bytes(bytes: &[u8], layout: ParsedLayout) -> Option<Self> {
        #[cfg(target_endian = "little")]
        {
            let records_len = layout
                .point_count
                .checked_mul(layout.level0_record_words)?
                .checked_mul(POINT_BYTES)?;
            let records_end = layout.level0_records_offset.checked_add(records_len)?;
            let records = bytemuck::try_cast_slice::<u8, u32>(
                bytes.get(layout.level0_records_offset..records_end)?,
            )
            .ok()?;
            Some(Self {
                records: NonNull::new(records.as_ptr().cast_mut())?,
                records_len: records.len(),
            })
        }
        #[cfg(not(target_endian = "little"))]
        {
            let _ = (bytes, layout);
            None
        }
    }

    #[inline(always)]
    fn records(&self) -> &[u32] {
        // SAFETY: construction validated alignment and length against the
        // immutable backing retained by the enclosing GraphLinks.
        unsafe { std::slice::from_raw_parts(self.records.as_ptr(), self.records_len) }
    }
}

impl Default for GraphLinks {
    fn default() -> Self {
        Self::new_from_edges(Vec::new())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct GraphLinksDegreeSummary {
    pub total_links: u64,
    pub level0_links: u64,
    pub max_level0_degree: u32,
    pub avg_level0_degree: f32,
}

#[derive(Debug, Clone, Copy)]
struct ParsedLayout {
    serialized_len_bytes: usize,
    point_count: usize,
    level0_link_count: usize,
    level0_records_offset: usize,
    level0_record_words: usize,
    level0_stride: usize,
    upper_offsets_offset: usize,
    upper_payload_offset: usize,
    upper_payload_len: usize,
}

impl GraphLinks {
    #[inline]
    fn prefetch_level0_record(&self, point_id: PointOffset) {
        const PREFETCH_BYTES: usize = 2 * 64;

        let point = point_id as usize;
        if point >= self.point_count {
            return;
        }
        let Some(record_word) = point.checked_mul(self.level0_record_words) else {
            return;
        };
        let Some(record_byte) = record_word
            .checked_mul(POINT_BYTES)
            .and_then(|offset| self.level0_records_offset.checked_add(offset))
        else {
            return;
        };
        let record_bytes = self.level0_record_words.saturating_mul(POINT_BYTES);
        let Some(bytes) = self
            .data
            .as_bytes()
            .get(record_byte..record_byte.saturating_add(record_bytes.min(PREFETCH_BYTES)))
        else {
            return;
        };
        paro_common::prefetch::read_l1(bytes);
    }

    pub fn new_from_edges(edges: Vec<Vec<Vec<PointOffset>>>) -> Self {
        let serialized = Self::encode_edges(edges);
        let layout = Self::parse_layout(&serialized, None)
            .expect("newly encoded GraphLinks layout must be valid");
        Self::from_data(GraphLinksData::Ram(serialized), layout, None)
    }

    fn from_data(
        data: GraphLinksData,
        layout: ParsedLayout,
        integrity: Option<GraphIntegrityRange>,
    ) -> Self {
        let native_level0 = NativeLevel0View::from_bytes(data.as_bytes(), layout);
        Self {
            data,
            integrity,
            native_level0,
            serialized_len_bytes: layout.serialized_len_bytes,
            point_count: layout.point_count,
            level0_link_count: layout.level0_link_count,
            level0_records_offset: layout.level0_records_offset,
            level0_record_words: layout.level0_record_words,
            level0_stride: layout.level0_stride,
            upper_offsets_offset: layout.upper_offsets_offset,
            upper_payload_offset: layout.upper_payload_offset,
            upper_payload_len: layout.upper_payload_len,
        }
    }

    fn encode_edges(mut edges: Vec<Vec<Vec<PointOffset>>>) -> Vec<u8> {
        let point_count = edges.len();
        let mut upper_offsets = Vec::with_capacity(point_count.saturating_add(1));
        let mut upper_payload = Vec::new();
        upper_offsets.push(0u64);
        let mut level0_link_count = 0usize;
        let mut level0_stride = 0usize;

        for point_edges in &mut edges {
            for links in point_edges.iter_mut() {
                links.sort_unstable();
            }
            let level0_degree = point_edges.first().map_or(0, Vec::len);
            level0_link_count = level0_link_count
                .checked_add(level0_degree)
                .expect("GraphLinks level-0 link count overflow");
            level0_stride = level0_stride.max(level0_degree);
            let num_levels = point_edges.len();
            Self::encode_varint(num_levels as u64, &mut upper_payload);
            for links in point_edges.iter().skip(1) {
                Self::encode_varint(links.len() as u64, &mut upper_payload);
                let mut previous = 0u32;
                for (index, &link) in links.iter().enumerate() {
                    let delta = if index == 0 {
                        u64::from(link)
                    } else {
                        u64::from(link - previous)
                    };
                    Self::encode_varint(delta, &mut upper_payload);
                    previous = link;
                }
            }
            upper_offsets.push(upper_payload.len() as u64);
        }

        let level0_record_words = level0_stride;
        let level0_records_bytes = point_count
            .checked_mul(level0_record_words)
            .and_then(|words| words.checked_mul(POINT_BYTES))
            .expect("GraphLinks level-0 record size overflow");
        let records_end = GRAPH_LINKS_HEADER_LEN
            .checked_add(level0_records_bytes)
            .expect("GraphLinks level-0 record offset overflow");
        let upper_offsets_offset = records_end
            .checked_add(U64_BYTES - 1)
            .expect("GraphLinks upper offset alignment overflow")
            & !(U64_BYTES - 1);
        let upper_offsets_bytes = upper_offsets
            .len()
            .checked_mul(U64_BYTES)
            .expect("GraphLinks upper offset size overflow");
        let serialized_len = upper_offsets_offset
            .checked_add(upper_offsets_bytes)
            .and_then(|len| len.checked_add(upper_payload.len()))
            .expect("GraphLinks serialized size overflow");

        let mut out = Vec::with_capacity(serialized_len);
        out.extend_from_slice(&GRAPH_LINKS_MAGIC.to_le_bytes());
        out.extend_from_slice(&GRAPH_LINKS_VERSION_SENTINEL_LEVEL0_V4.to_le_bytes());
        out.extend_from_slice(&(point_count as u64).to_le_bytes());
        out.extend_from_slice(&(level0_link_count as u64).to_le_bytes());
        out.extend_from_slice(&(upper_payload.len() as u64).to_le_bytes());
        out.extend_from_slice(&(serialized_len as u64).to_le_bytes());
        out.extend_from_slice(
            &u32::try_from(level0_stride)
                .expect("GraphLinks level-0 stride exceeds u32")
                .to_le_bytes(),
        );
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&0u64.to_le_bytes());
        out.extend_from_slice(&0u64.to_le_bytes());
        debug_assert_eq!(out.len(), GRAPH_LINKS_HEADER_LEN);

        for point_edges in &edges {
            let level0_links = point_edges.first().map_or(&[][..], Vec::as_slice);
            for &link in level0_links {
                out.extend_from_slice(&link.to_le_bytes());
            }
            for _ in level0_links.len()..level0_stride {
                out.extend_from_slice(&PointOffset::MAX.to_le_bytes());
            }
        }
        out.resize(upper_offsets_offset, 0);
        for offset in upper_offsets {
            out.extend_from_slice(&offset.to_le_bytes());
        }
        out.extend_from_slice(&upper_payload);
        debug_assert_eq!(out.len(), serialized_len);
        out
    }

    #[inline]
    pub fn for_each_link<F>(&self, point_id: PointOffset, level: usize, mut f: F) -> Result<()>
    where
        F: FnMut(PointOffset),
    {
        let integrity = self
            .integrity
            .as_ref()
            .filter(|integrity| !integrity.verifier.is_fully_verified());
        self.for_each_link_with_integrity(point_id, level, integrity, |point| f(point.offset()))
    }

    /// Bind graph traversal to a scorer/vector domain and snapshot artifact
    /// authentication state once for the complete logical search.
    ///
    /// This is the sole constructor for [`GraphLinksReadView`]. A successful
    /// result makes every [`GraphPoint`] yielded by the view valid for a vector
    /// storage with `expected_point_count` rows.
    /// `CHUNK_VALID` is terminal for immutable artifact bytes, so eliding
    /// subsequent checksum checks cannot race with mutation or invalidation.
    #[inline]
    pub(crate) fn search_view(
        &self,
        expected_point_count: usize,
    ) -> Result<GraphLinksReadView<'_>> {
        if self.point_count != expected_point_count {
            return Err(paro_error::data_corrupted(format!(
                "GraphLinks point count {} differs from search vector domain {expected_point_count}",
                self.point_count
            )));
        }
        let integrity = self
            .integrity
            .as_ref()
            .filter(|integrity| !integrity.verifier.is_fully_verified());
        Ok(GraphLinksReadView {
            links: self,
            integrity,
        })
    }

    #[inline]
    fn for_each_link_with_integrity<F>(
        &self,
        point_id: PointOffset,
        level: usize,
        integrity: Option<&GraphIntegrityRange>,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(GraphPoint),
    {
        if level == 0 {
            self.for_each_level0_link(point_id, integrity, f)
        } else {
            self.for_each_upper_link(point_id, level, integrity, &mut f)
        }
    }

    #[inline]
    fn for_each_level0_link<F>(
        &self,
        point_id: PointOffset,
        integrity: Option<&GraphIntegrityRange>,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(GraphPoint),
    {
        let point = usize::try_from(point_id)
            .map_err(|_| paro_error::data_corrupted("GraphLinks point id exceeds usize"))?;
        if point >= self.point_count {
            return Ok(());
        }
        let record_word = point
            .checked_mul(self.level0_record_words)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks level-0 record overflow"))?;
        let record_byte = self
            .level0_records_offset
            .checked_add(record_word.checked_mul(POINT_BYTES).ok_or_else(|| {
                paro_error::data_corrupted("GraphLinks level-0 byte offset overflow")
            })?)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks level-0 record overflow"))?;
        let record_bytes = self.level0_record_words.saturating_mul(POINT_BYTES);
        if let Some(integrity) = integrity {
            integrity.verify(record_byte, record_bytes)?;
        }

        if let Some(native) = self.native_level0 {
            let records = native.records();
            let links_end = record_word + self.level0_stride;
            for &link in &records[record_word..links_end] {
                if link == PointOffset::MAX {
                    break;
                }
                if link as usize >= self.point_count {
                    return Err(paro_error::data_corrupted(format!(
                        "GraphLinks level-0 link target {link} exceeds point count {}",
                        self.point_count
                    )));
                }
                f(GraphPoint(link));
            }
            return Ok(());
        }

        let bytes = self.data.as_bytes();
        let links_end = record_byte
            .checked_add(record_bytes)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks level-0 link overflow"))?;
        let link_bytes = bytes.get(record_byte..links_end).ok_or_else(|| {
            paro_error::data_corrupted("GraphLinks level-0 record exceeds backing")
        })?;
        // The fallback retains exact-width decoding for an unaligned backing
        // or a big-endian host.
        for raw in link_bytes.chunks_exact(POINT_BYTES) {
            let link =
                PointOffset::from_le_bytes(raw.try_into().expect("level-0 link chunk width"));
            if link == PointOffset::MAX {
                break;
            }
            if link as usize >= self.point_count {
                return Err(paro_error::data_corrupted(format!(
                    "GraphLinks level-0 link target {link} exceeds point count {}",
                    self.point_count
                )));
            }
            f(GraphPoint(link));
        }
        Ok(())
    }

    fn for_each_upper_link<F>(
        &self,
        point_id: PointOffset,
        level: usize,
        integrity: Option<&GraphIntegrityRange>,
        f: &mut F,
    ) -> Result<()>
    where
        F: FnMut(GraphPoint),
    {
        let Some(payload) = self.upper_point_payload(point_id, integrity)? else {
            return Ok(());
        };
        let mut cursor = 0usize;
        let Some(num_levels) = Self::decode_varint_checked(payload, &mut cursor)
            .and_then(|levels| usize::try_from(levels).ok())
        else {
            return Err(paro_error::data_corrupted(
                "GraphLinks upper payload has an invalid level count",
            ));
        };
        if level >= num_levels {
            return Ok(());
        }

        for current_level in 1..num_levels {
            let Some(count) = Self::decode_varint_checked(payload, &mut cursor)
                .and_then(|count| usize::try_from(count).ok())
            else {
                return Err(paro_error::data_corrupted(
                    "GraphLinks upper payload has an invalid link count",
                ));
            };
            let mut previous = 0u32;
            for link_index in 0..count {
                let Some(delta) = Self::decode_varint_checked(payload, &mut cursor)
                    .and_then(|delta| u32::try_from(delta).ok())
                else {
                    return Err(paro_error::data_corrupted(
                        "GraphLinks upper payload has an invalid link delta",
                    ));
                };
                let Some(link) = (if link_index == 0 {
                    Some(delta)
                } else {
                    previous.checked_add(delta)
                }) else {
                    return Err(paro_error::data_corrupted(
                        "GraphLinks upper payload link delta overflows",
                    ));
                };
                if link as usize >= self.point_count {
                    return Err(paro_error::data_corrupted(format!(
                        "GraphLinks upper link target {link} exceeds point count {}",
                        self.point_count
                    )));
                }
                previous = link;
                if current_level == level {
                    f(GraphPoint(link));
                }
            }
            if current_level == level {
                return Ok(());
            }
        }
        Ok(())
    }

    #[inline]
    fn upper_point_payload(
        &self,
        point_id: PointOffset,
        integrity: Option<&GraphIntegrityRange>,
    ) -> Result<Option<&[u8]>> {
        let point = usize::try_from(point_id)
            .map_err(|_| paro_error::data_corrupted("GraphLinks point id exceeds usize"))?;
        if point >= self.point_count {
            return Ok(None);
        }
        let offset_byte = self
            .upper_offsets_offset
            .checked_add(point.checked_mul(U64_BYTES).ok_or_else(|| {
                paro_error::data_corrupted("GraphLinks upper offset index overflow")
            })?)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks upper offset range overflow"))?;
        if let Some(integrity) = integrity {
            integrity.verify(offset_byte, 2 * U64_BYTES)?;
        }
        let bytes = self.data.as_bytes();
        let start = usize::try_from(Self::read_layout_u64(bytes, offset_byte)).map_err(|_| {
            paro_error::data_corrupted("GraphLinks upper start offset exceeds usize")
        })?;
        let end = usize::try_from(Self::read_layout_u64(bytes, offset_byte + U64_BYTES))
            .map_err(|_| paro_error::data_corrupted("GraphLinks upper end offset exceeds usize"))?;
        self.validate_offset_pair(start, end, self.upper_payload_len, "upper")?;
        let payload_start = self
            .upper_payload_offset
            .checked_add(start)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks upper payload overflow"))?;
        let payload_end = self
            .upper_payload_offset
            .checked_add(end)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks upper payload overflow"))?;
        if let Some(integrity) = integrity {
            integrity.verify(payload_start, payload_end - payload_start)?;
        }
        Ok(Some(bytes.get(payload_start..payload_end).ok_or_else(
            || paro_error::data_corrupted("GraphLinks upper payload exceeds backing"),
        )?))
    }

    #[inline]
    fn validate_offset_pair(
        &self,
        start: usize,
        end: usize,
        payload_len: usize,
        label: &str,
    ) -> Result<()> {
        if start > end || end > payload_len {
            return Err(paro_error::data_corrupted(format!(
                "GraphLinks {label} offset pair is not monotonic/in bounds: start={start}, end={end}, limit={payload_len}"
            )));
        }
        Ok(())
    }

    pub fn num_levels(&self, point_id: PointOffset) -> Result<usize> {
        let integrity = self
            .integrity
            .as_ref()
            .filter(|integrity| !integrity.verifier.is_fully_verified());
        let Some(payload) = self.upper_point_payload(point_id, integrity)? else {
            return Ok(0);
        };
        let mut cursor = 0usize;
        let levels = Self::decode_varint_checked(payload, &mut cursor)
            .and_then(|levels| usize::try_from(levels).ok())
            .ok_or_else(|| {
                paro_error::data_corrupted("GraphLinks upper payload has an invalid level count")
            })?;
        if levels == 0 {
            return Err(paro_error::data_corrupted(
                "GraphLinks point has no level-0 adjacency",
            ));
        }
        Ok(levels)
    }

    pub fn point_level(&self, point_id: PointOffset) -> Result<usize> {
        Ok(self.num_levels(point_id)?.saturating_sub(1))
    }

    pub fn links_on_level(
        &self,
        point_id: PointOffset,
        level: usize,
    ) -> Result<Option<Vec<PointOffset>>> {
        if level >= self.num_levels(point_id)? {
            return Ok(None);
        }
        let mut links = Vec::new();
        self.for_each_link(point_id, level, |neighbor| links.push(neighbor))?;
        Ok(Some(links))
    }

    pub fn degree_summary(&self) -> Result<GraphLinksDegreeSummary> {
        let mut total_links = 0u64;
        let mut level0_links = 0u64;
        let mut max_level0_degree = 0u32;
        for point_id in 0..self.num_points() as PointOffset {
            for level in 0..self.num_levels(point_id)? {
                let mut degree = 0u32;
                self.for_each_link(point_id, level, |_| degree = degree.saturating_add(1))?;
                total_links = total_links.saturating_add(u64::from(degree));
                if level == 0 {
                    level0_links = level0_links.saturating_add(u64::from(degree));
                    max_level0_degree = max_level0_degree.max(degree);
                }
            }
        }
        let avg_level0_degree = if self.point_count == 0 {
            0.0
        } else {
            level0_links as f32 / self.point_count as f32
        };
        Ok(GraphLinksDegreeSummary {
            total_links,
            level0_links,
            max_level0_degree,
            avg_level0_degree,
        })
    }

    fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
        while value >= 0x80 {
            out.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    fn decode_varint_checked(bytes: &[u8], cursor: &mut usize) -> Option<u64> {
        let mut result = 0u64;
        let mut shift = 0u32;
        for _ in 0..10 {
            let byte = *bytes.get(*cursor)?;
            *cursor += 1;
            let payload = u64::from(byte & 0x7f);
            if shift == 63 && payload > 1 {
                return None;
            }
            result |= payload << shift;
            if byte & 0x80 == 0 {
                return Some(result);
            }
            shift += 7;
        }
        None
    }

    #[inline(always)]
    fn read_u32_at(bytes: &[u8], start: usize) -> Option<u32> {
        let raw = bytes.get(start..start.checked_add(4)?)?;
        Some(u32::from_le_bytes(raw.try_into().ok()?))
    }

    #[inline(always)]
    fn read_u64_at(bytes: &[u8], start: usize) -> Option<u64> {
        let raw = bytes.get(start..start.checked_add(8)?)?;
        Some(u64::from_le_bytes(raw.try_into().ok()?))
    }

    #[inline(always)]
    fn read_layout_u64(bytes: &[u8], start: usize) -> u64 {
        u64::from_le_bytes(
            bytes[start..start + U64_BYTES]
                .try_into()
                .expect("validated GraphLinks offset width"),
        )
    }

    fn read_u32(bytes: &[u8], start: usize, field: &str) -> Result<u32> {
        Self::read_u32_at(bytes, start)
            .ok_or_else(|| paro_error::data_corrupted(format!("GraphLinks missing field: {field}")))
    }

    fn read_u64(bytes: &[u8], start: usize, field: &str) -> Result<u64> {
        Self::read_u64_at(bytes, start)
            .ok_or_else(|| paro_error::data_corrupted(format!("GraphLinks missing field: {field}")))
    }

    fn checked_usize(value: u64, field: &str) -> Result<usize> {
        usize::try_from(value)
            .map_err(|_| paro_error::data_corrupted(format!("GraphLinks {field} overflow")))
    }

    fn validate_offset_table(
        bytes: &[u8],
        table_offset: usize,
        point_count: usize,
        payload_len: usize,
        label: &str,
    ) -> Result<()> {
        let mut previous = 0u64;
        for point in 0..=point_count {
            let byte_offset = table_offset + point * U64_BYTES;
            let current = Self::read_u64(bytes, byte_offset, label)?;
            if current < previous || current > payload_len as u64 {
                return Err(paro_error::data_corrupted(format!(
                    "GraphLinks {label} is not monotonic/in bounds at point {point}: previous={previous}, current={current}, limit={payload_len}"
                )));
            }
            previous = current;
        }
        if previous != payload_len as u64 {
            return Err(paro_error::data_corrupted(format!(
                "GraphLinks {label} final offset mismatch: {previous} != {payload_len}"
            )));
        }
        Ok(())
    }

    fn validate_upper_payloads(
        bytes: &[u8],
        point_count: usize,
        upper_offsets_offset: usize,
        upper_payload_offset: usize,
    ) -> Result<()> {
        for point in 0..point_count {
            let start = Self::read_u64(
                bytes,
                upper_offsets_offset + point * U64_BYTES,
                "upper offset",
            )? as usize;
            let end = Self::read_u64(
                bytes,
                upper_offsets_offset + (point + 1) * U64_BYTES,
                "upper offset",
            )? as usize;
            let payload = &bytes[upper_payload_offset + start..upper_payload_offset + end];
            let mut cursor = 0usize;
            let num_levels = Self::decode_varint_checked(payload, &mut cursor)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| {
                    paro_error::data_corrupted(format!(
                        "GraphLinks invalid upper level count for point {point}"
                    ))
                })?;
            if num_levels == 0 {
                return Err(paro_error::data_corrupted(format!(
                    "GraphLinks point {point} has no level-0 adjacency"
                )));
            }
            for level in 1..num_levels {
                let count = Self::decode_varint_checked(payload, &mut cursor)
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or_else(|| {
                        paro_error::data_corrupted(format!(
                            "GraphLinks invalid upper link count for point {point} level {level}"
                        ))
                    })?;
                let mut previous = 0u32;
                for link_index in 0..count {
                    let delta = Self::decode_varint_checked(payload, &mut cursor)
                        .and_then(|value| u32::try_from(value).ok())
                        .ok_or_else(|| {
                            paro_error::data_corrupted(format!(
                                "GraphLinks invalid upper link for point {point} level {level}"
                            ))
                        })?;
                    previous = if link_index == 0 {
                        delta
                    } else {
                        previous.checked_add(delta).ok_or_else(|| {
                            paro_error::data_corrupted(format!(
                                "GraphLinks upper link delta overflow for point {point} level {level}"
                            ))
                        })?
                    };
                    if previous as usize >= point_count {
                        return Err(paro_error::data_corrupted(format!(
                            "GraphLinks upper link target {} out of bounds for point {point} level {level}",
                            previous
                        )));
                    }
                }
            }
            if cursor != payload.len() {
                return Err(paro_error::data_corrupted(format!(
                    "GraphLinks upper payload has trailing bytes for point {point}: consumed={cursor}, len={}",
                    payload.len()
                )));
            }
        }
        Ok(())
    }

    fn validate_level0_records(
        bytes: &[u8],
        point_count: usize,
        expected_link_count: usize,
        records_offset: usize,
        stride: usize,
    ) -> Result<()> {
        let record_words = stride;
        let mut observed_link_count = 0usize;
        for point in 0..point_count {
            let record = records_offset
                .checked_add(
                    point
                        .checked_mul(record_words)
                        .and_then(|word| word.checked_mul(POINT_BYTES))
                        .ok_or_else(|| {
                            paro_error::data_corrupted("GraphLinks level-0 record overflow")
                        })?,
                )
                .ok_or_else(|| paro_error::data_corrupted("GraphLinks level-0 record overflow"))?;
            let mut padding = false;
            for link_index in 0..stride {
                let link =
                    Self::read_u32(bytes, record + link_index * POINT_BYTES, "level-0 link")?;
                if link == PointOffset::MAX {
                    padding = true;
                    continue;
                }
                if padding {
                    return Err(paro_error::data_corrupted(format!(
                        "GraphLinks level-0 record has a link after sentinel padding at point {point}, slot {link_index}"
                    )));
                }
                if link as usize >= point_count {
                    return Err(paro_error::data_corrupted(format!(
                        "GraphLinks level-0 link target {link} out of bounds at point {point}, link {link_index}"
                    )));
                }
                observed_link_count = observed_link_count.checked_add(1).ok_or_else(|| {
                    paro_error::data_corrupted("GraphLinks level-0 link count overflow")
                })?;
            }
        }
        if observed_link_count != expected_link_count {
            return Err(paro_error::data_corrupted(format!(
                "GraphLinks level-0 link count mismatch: declared={expected_link_count}, observed={observed_link_count}"
            )));
        }
        Ok(())
    }

    fn parse_layout(bytes: &[u8], integrity: Option<&GraphIntegrityRange>) -> Result<ParsedLayout> {
        if bytes.len() < GRAPH_LINKS_HEADER_LEN {
            return Err(paro_error::data_corrupted(
                "GraphLinks file too small for version-4 header",
            ));
        }
        if let Some(integrity) = integrity {
            integrity.verify(0, GRAPH_LINKS_HEADER_LEN)?;
        }
        let marker = Self::read_u32(bytes, 0, "marker")?;
        if marker != GRAPH_LINKS_MAGIC {
            return Err(paro_error::data_corrupted(
                "legacy GraphLinks payloads are no longer supported",
            ));
        }
        let version = Self::read_u32(bytes, 4, "version")?;
        if version != GRAPH_LINKS_VERSION_SENTINEL_LEVEL0_V4 {
            return Err(paro_error::data_corrupted(format!(
                "unknown GraphLinks version: {version} (expected {GRAPH_LINKS_VERSION_SENTINEL_LEVEL0_V4})"
            )));
        }

        let point_count =
            Self::checked_usize(Self::read_u64(bytes, 8, "point_count")?, "point_count")?;
        let level0_link_count = Self::checked_usize(
            Self::read_u64(bytes, 16, "level0_link_count")?,
            "level0_link_count",
        )?;
        let upper_payload_len = Self::checked_usize(
            Self::read_u64(bytes, 24, "upper_payload_len")?,
            "upper_payload_len",
        )?;
        let declared_serialized_len = Self::checked_usize(
            Self::read_u64(bytes, 32, "serialized_len")?,
            "serialized_len",
        )?;
        let level0_stride = Self::read_u32(bytes, 40, "level0_stride")? as usize;
        if Self::read_u32(bytes, 44, "reserved")? != 0 {
            return Err(paro_error::data_corrupted(
                "GraphLinks reserved header fields must be zero",
            ));
        }
        for offset in [48usize, 56] {
            if Self::read_u64(bytes, offset, "reserved")? != 0 {
                return Err(paro_error::data_corrupted(
                    "GraphLinks reserved header fields must be zero",
                ));
            }
        }

        let offsets_count = point_count
            .checked_add(1)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks point count overflow"))?;
        let offsets_bytes = offsets_count
            .checked_mul(U64_BYTES)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks offsets size overflow"))?;
        if point_count == 0 && (level0_stride != 0 || level0_link_count != 0) {
            return Err(paro_error::data_corrupted(
                "empty GraphLinks cannot declare a level-0 stride or links",
            ));
        }
        let level0_capacity = point_count
            .checked_mul(level0_stride)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks level-0 capacity overflow"))?;
        if level0_link_count > level0_capacity {
            return Err(paro_error::data_corrupted(format!(
                "GraphLinks level-0 link count {level0_link_count} exceeds fixed-record capacity {level0_capacity}"
            )));
        }
        let level0_record_words = level0_stride;
        let level0_records_bytes = point_count
            .checked_mul(level0_record_words)
            .and_then(|words| words.checked_mul(POINT_BYTES))
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks level-0 size overflow"))?;
        let level0_records_offset = GRAPH_LINKS_HEADER_LEN;
        let records_end = level0_records_offset
            .checked_add(level0_records_bytes)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks level-0 offset overflow"))?;
        let upper_offsets_offset = records_end
            .checked_add(U64_BYTES - 1)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks upper offset overflow"))?
            & !(U64_BYTES - 1);
        let upper_payload_offset = upper_offsets_offset
            .checked_add(offsets_bytes)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks payload offset overflow"))?;
        let serialized_len_bytes = upper_payload_offset
            .checked_add(upper_payload_len)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks payload length overflow"))?;

        if declared_serialized_len != serialized_len_bytes {
            return Err(paro_error::data_corrupted(format!(
                "GraphLinks serialized length mismatch: declared={declared_serialized_len} calculated={serialized_len_bytes}"
            )));
        }
        if serialized_len_bytes > bytes.len() {
            return Err(paro_error::data_corrupted(format!(
                "GraphLinks data truncated: need {serialized_len_bytes} bytes, got {}",
                bytes.len()
            )));
        }

        if bytes[records_end..upper_offsets_offset]
            .iter()
            .any(|&byte| byte != 0)
        {
            return Err(paro_error::data_corrupted(
                "GraphLinks level-0 alignment padding must be zero",
            ));
        }

        // Authenticated artifacts were fully verified before publication.
        // Opening them validates only the upper-level boundary offsets; each
        // fixed level-0 record is authenticated and range-checked immediately
        // before use. Standalone opens validate the compact upper-offset table
        // but deliberately do not fault every fixed-record page.
        if let Some(integrity) = integrity {
            integrity.verify(upper_offsets_offset, U64_BYTES)?;
            integrity.verify(upper_offsets_offset + point_count * U64_BYTES, U64_BYTES)?;
        }
        let first_upper = Self::read_u64(bytes, upper_offsets_offset, "first upper offset")?;
        let last_upper = Self::read_u64(
            bytes,
            upper_offsets_offset + point_count * U64_BYTES,
            "last upper offset",
        )?;
        if first_upper != 0 || last_upper != upper_payload_len as u64 {
            return Err(paro_error::data_corrupted(
                "GraphLinks upper payload boundary mismatch",
            ));
        }
        if integrity.is_none() {
            Self::validate_offset_table(
                bytes,
                upper_offsets_offset,
                point_count,
                upper_payload_len,
                "upper offset",
            )?;
        }
        Ok(ParsedLayout {
            serialized_len_bytes,
            point_count,
            level0_link_count,
            level0_records_offset,
            level0_record_words,
            level0_stride,
            upper_offsets_offset,
            upper_payload_offset,
            upper_payload_len,
        })
    }

    pub fn serialize<W: Write>(&self, mut writer: W) -> Result<()> {
        if let Some(integrity) = &self.integrity {
            integrity.verify(0, self.serialized_len_bytes)?;
        }
        writer.write_all(&self.data.as_bytes()[..self.serialized_len_bytes])?;
        Ok(())
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let file = File::create(path)?;
        self.serialize(BufWriter::new(file))
    }

    pub fn deserialize<R: Read>(mut reader: R) -> Result<Self> {
        let mut serialized = Vec::new();
        reader.read_to_end(&mut serialized)?;
        let layout = Self::parse_layout(&serialized, None)?;
        serialized.truncate(layout.serialized_len_bytes);
        Ok(Self::from_data(
            GraphLinksData::Ram(serialized),
            layout,
            None,
        ))
    }

    /// Open graph links over an owned byte view without copying the graph.
    pub fn deserialize_bytes(serialized: Bytes) -> Result<Self> {
        let layout = Self::parse_layout(&serialized, None)?;
        let serialized = serialized.slice(..layout.serialized_len_bytes);
        Ok(Self::from_data(
            GraphLinksData::Bytes(serialized),
            layout,
            None,
        ))
    }

    pub(crate) fn deserialize_bytes_with_integrity(
        serialized: Bytes,
        verifier: Arc<ArtifactIntegrity>,
        artifact_offset: usize,
    ) -> Result<Self> {
        let integrity = GraphIntegrityRange {
            verifier,
            artifact_offset,
        };
        let layout = Self::parse_layout(&serialized, Some(&integrity))?;
        let serialized = serialized.slice(..layout.serialized_len_bytes);
        Ok(Self::from_data(
            GraphLinksData::Bytes(serialized),
            layout,
            Some(integrity),
        ))
    }

    pub fn load(path: &Path) -> Result<Self> {
        Self::deserialize(File::open(path)?)
    }

    pub fn load_mmap(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = Arc::new(unsafe { memmap2::MmapOptions::new().map(&file)? });
        #[cfg(unix)]
        {
            let _ = mmap.advise(memmap2::Advice::Random);
        }
        let layout = Self::parse_layout(&mmap, None)?;
        Ok(Self::from_data(GraphLinksData::Mmap(mmap), layout, None))
    }

    /// Open graph links over a validated range of a shared mmap package.
    pub fn deserialize_mmap_range(mmap: Arc<Mmap>, offset: usize, len: usize) -> Result<Self> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| paro_common::error::data_corrupted("HNSW mmap range overflow"))?;
        let serialized = mmap.get(offset..end).ok_or_else(|| {
            paro_common::error::data_corrupted("HNSW mmap range exceeds package length")
        })?;
        #[cfg(unix)]
        {
            let _ = mmap.advise_range(memmap2::Advice::Random, offset, len);
        }
        let layout = Self::parse_layout(serialized, None)?;
        Ok(Self::from_data(
            GraphLinksData::MmapSlice {
                mmap,
                offset,
                len: layout.serialized_len_bytes,
            },
            layout,
            None,
        ))
    }

    pub(crate) fn deserialize_mmap_range_with_integrity(
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
        verifier: Arc<ArtifactIntegrity>,
        artifact_offset: usize,
    ) -> Result<Self> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| paro_error::data_corrupted("HNSW mmap range overflow"))?;
        let serialized = mmap
            .get(offset..end)
            .ok_or_else(|| paro_error::data_corrupted("HNSW mmap range exceeds package length"))?;
        #[cfg(unix)]
        {
            // Fixed adjacency records and upper-level records are addressed by
            // graph point id rather than file order.
            let _ = mmap.advise_range(memmap2::Advice::Random, offset, len);
        }
        let integrity = GraphIntegrityRange {
            verifier,
            artifact_offset,
        };
        let layout = Self::parse_layout(serialized, Some(&integrity))?;
        Ok(Self::from_data(
            GraphLinksData::MmapSlice {
                mmap,
                offset,
                len: layout.serialized_len_bytes,
            },
            layout,
            Some(integrity),
        ))
    }

    /// Perform an explicit O(E) integrity scan of every graph link and upper
    /// payload. Authenticated segment/sidecar opens are O(1); standalone graph
    /// opens validate only the compact upper-offset table. Fixed level-0
    /// records validate targets lazily when searched.
    /// Standalone recovery tooling can opt into this deeper semantic scan
    /// without forcing every mmap open to fault every graph page.
    pub fn verify_integrity(&self) -> Result<()> {
        if let Some(integrity) = &self.integrity {
            integrity.verify(0, self.serialized_len_bytes)?;
        }
        let bytes = self.data.as_bytes();
        Self::validate_offset_table(
            bytes,
            self.upper_offsets_offset,
            self.point_count,
            self.upper_payload_len,
            "upper offset",
        )?;
        Self::validate_level0_records(
            bytes,
            self.point_count,
            self.level0_link_count,
            self.level0_records_offset,
            self.level0_stride,
        )?;
        Self::validate_upper_payloads(
            bytes,
            self.point_count,
            self.upper_offsets_offset,
            self.upper_payload_offset,
        )
    }

    #[inline]
    pub fn num_points(&self) -> usize {
        self.point_count
    }

    /// Durable fixed-record capacity. This is header metadata, not a scan of
    /// observed degrees, and is therefore safe to validate at artifact open.
    pub(crate) fn level0_stride(&self) -> usize {
        self.level0_stride
    }

    pub fn is_mmap_backed(&self) -> bool {
        matches!(
            self.data,
            GraphLinksData::Mmap(_) | GraphLinksData::MmapSlice { .. }
        )
    }

    pub fn is_bytes_backed(&self) -> bool {
        matches!(self.data, GraphLinksData::Bytes(_))
    }

    pub fn serialized_size_bytes(&self) -> u64 {
        self.serialized_len_bytes as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::artifact_integrity::{
        append_integrity_table, ArtifactIntegrity, ArtifactIntegrityBacking,
    };
    use crate::statistics::append_stats_trailer;
    use std::fs::File;
    use tempfile::TempDir;

    fn sample_edges() -> Vec<Vec<Vec<PointOffset>>> {
        vec![
            vec![vec![2, 1], vec![3]],
            vec![vec![2, 0]],
            vec![vec![1], vec![0]],
            vec![vec![0]],
        ]
    }

    fn collect_links(graph: &GraphLinks, point: PointOffset, level: usize) -> Vec<PointOffset> {
        let mut links = Vec::new();
        graph
            .for_each_link(point, level, |neighbor| links.push(neighbor))
            .unwrap();
        links
    }

    #[test]
    fn legacy_payload_is_rejected() {
        let bytes = 1u32.to_le_bytes();
        let error = GraphLinks::deserialize(bytes.as_slice()).unwrap_err();
        assert!(error.to_string().contains("version-4 header"));
    }

    #[test]
    fn stale_graph_versions_are_rejected() {
        for version in [1_u32, 2, 3] {
            let mut bytes = vec![0u8; GRAPH_LINKS_HEADER_LEN];
            bytes[0..4].copy_from_slice(&GRAPH_LINKS_MAGIC.to_le_bytes());
            bytes[4..8].copy_from_slice(&version.to_le_bytes());
            let error = GraphLinks::deserialize(bytes.as_slice()).unwrap_err();
            assert!(error.to_string().contains("unknown GraphLinks version"));
        }
    }

    #[test]
    fn fixed_record_roundtrip_uses_plain_level0_and_compressed_upper_levels() {
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut bytes = Vec::new();
        links.serialize(&mut bytes).unwrap();

        assert_eq!(
            u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            GRAPH_LINKS_MAGIC
        );
        assert_eq!(
            u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            GRAPH_LINKS_VERSION_SENTINEL_LEVEL0_V4
        );
        let layout = GraphLinks::parse_layout(&bytes, None).unwrap();
        assert_eq!(
            u32::from_le_bytes(
                bytes[layout.level0_records_offset..layout.level0_records_offset + 4]
                    .try_into()
                    .unwrap()
            ),
            1
        );
        assert_eq!(
            u32::from_le_bytes(
                bytes[layout.level0_records_offset + POINT_BYTES
                    ..layout.level0_records_offset + 2 * POINT_BYTES]
                    .try_into()
                    .unwrap()
            ),
            2
        );

        let restored = GraphLinks::deserialize(bytes.as_slice()).unwrap();
        assert_eq!(restored.num_points(), 4);
        assert_eq!(collect_links(&restored, 0, 0), vec![1, 2]);
        assert_eq!(collect_links(&restored, 1, 0), vec![0, 2]);
        assert_eq!(collect_links(&restored, 0, 1), vec![3]);
        assert_eq!(collect_links(&restored, 2, 1), vec![0]);
    }

    #[test]
    fn deserialize_ignores_stats_trailer() {
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut bytes = Vec::new();
        links.serialize(&mut bytes).unwrap();
        let graph_len = bytes.len();
        append_stats_trailer(&mut bytes, &[7, 7, 7]).unwrap();

        let restored = GraphLinks::deserialize(bytes.as_slice()).unwrap();
        assert_eq!(restored.serialized_size_bytes(), graph_len as u64);
        assert_eq!(collect_links(&restored, 3, 0), vec![0]);
    }

    #[test]
    fn unknown_version_reports_error() {
        let mut bytes = vec![0u8; GRAPH_LINKS_HEADER_LEN];
        bytes[0..4].copy_from_slice(&GRAPH_LINKS_MAGIC.to_le_bytes());
        bytes[4..8].copy_from_slice(&999u32.to_le_bytes());
        let error = GraphLinks::deserialize(bytes.as_slice()).unwrap_err();
        assert!(error.to_string().contains("unknown GraphLinks version"));
    }

    #[test]
    fn mmap_load_matches_ram_without_decoded_level0_copy() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("graph_links.bin");
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut file = File::create(&file_path).unwrap();
        links.serialize(&mut file).unwrap();
        drop(file);

        let ram = GraphLinks::load(&file_path).unwrap();
        let mmap = GraphLinks::load_mmap(&file_path).unwrap();
        assert!(!ram.is_mmap_backed());
        assert!(mmap.is_mmap_backed());
        assert_eq!(ram.num_points(), mmap.num_points());

        for point in 0..ram.num_points() as PointOffset {
            assert_eq!(
                ram.num_levels(point).unwrap(),
                mmap.num_levels(point).unwrap()
            );
            for level in 0..ram.num_levels(point).unwrap() {
                assert_eq!(
                    collect_links(&ram, point, level),
                    collect_links(&mmap, point, level)
                );
            }
        }
    }

    #[test]
    fn corrupt_level0_target_is_rejected_when_record_is_accessed() {
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut bytes = Vec::new();
        links.serialize(&mut bytes).unwrap();
        let layout = GraphLinks::parse_layout(&bytes, None).unwrap();
        let corrupt = (layout.point_count as u32).to_le_bytes();
        let offset = layout.level0_records_offset + layout.level0_record_words * POINT_BYTES;
        bytes[offset..offset + POINT_BYTES].copy_from_slice(&corrupt);

        let restored = GraphLinks::deserialize(bytes.as_slice()).unwrap();
        let error = restored.for_each_link(1, 0, |_| {}).unwrap_err();
        assert!(error.to_string().contains("link target"));
    }

    #[test]
    fn deep_verification_rejects_truncated_upper_varint() {
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut bytes = Vec::new();
        links.serialize(&mut bytes).unwrap();
        let layout = GraphLinks::parse_layout(&bytes, None).unwrap();
        let point0_end = GraphLinks::read_u64(
            &bytes,
            layout.upper_offsets_offset + U64_BYTES,
            "point 0 upper end",
        )
        .unwrap() as usize;
        bytes[layout.upper_payload_offset + point0_end - 1] = 0x80;

        let restored = GraphLinks::deserialize(bytes.as_slice()).unwrap();
        let error = restored.verify_integrity().unwrap_err();
        assert!(error.to_string().contains("invalid upper link"));
    }

    #[test]
    fn deep_verification_rejects_out_of_bounds_level0_link() {
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut bytes = Vec::new();
        links.serialize(&mut bytes).unwrap();
        let layout = GraphLinks::parse_layout(&bytes, None).unwrap();
        bytes[layout.level0_records_offset + POINT_BYTES
            ..layout.level0_records_offset + 2 * POINT_BYTES]
            .copy_from_slice(&(layout.point_count as u32).to_le_bytes());

        let restored = GraphLinks::deserialize(bytes.as_slice()).unwrap();
        let error = restored.verify_integrity().unwrap_err();
        assert!(error.to_string().contains("level-0 link target"));
    }

    #[test]
    fn mmap_integrity_defers_graph_payload_checks_until_access() {
        const POINTS: usize = 1_024;
        const DEGREE: usize = 16;
        const ARTIFACT_HEADER: usize = 176;

        let edges = (0..POINTS)
            .map(|point| {
                vec![(1..=DEGREE)
                    .map(|delta| ((point + delta) % POINTS) as PointOffset)
                    .collect::<Vec<_>>()]
            })
            .collect::<Vec<_>>();
        let links = GraphLinks::new_from_edges(edges);
        let mut graph_bytes = Vec::new();
        links.serialize(&mut graph_bytes).unwrap();
        let layout = GraphLinks::parse_layout(&graph_bytes, None).unwrap();

        let mut artifact = vec![0_u8; ARTIFACT_HEADER];
        artifact.extend_from_slice(&graph_bytes);
        let descriptor = append_integrity_table(&mut artifact, ARTIFACT_HEADER).unwrap();

        let point = POINTS / 2;
        let corrupt_offset = ARTIFACT_HEADER
            + layout.level0_records_offset
            + point * layout.level0_record_words * POINT_BYTES
            + POINT_BYTES;
        artifact[corrupt_offset] ^= 1;

        let backing = Bytes::from(artifact);
        let verifier =
            ArtifactIntegrity::open(ArtifactIntegrityBacking::Bytes(backing.clone()), descriptor)
                .unwrap();
        let graph = GraphLinks::deserialize_bytes_with_integrity(
            backing.slice(ARTIFACT_HEADER..descriptor.offset),
            Arc::clone(&verifier),
            ARTIFACT_HEADER,
        )
        .unwrap();

        assert!(verifier.verified_chunk_count() < verifier.chunk_count());
        let error = graph
            .for_each_link(point as PointOffset, 0, |_| {})
            .unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn authenticated_open_defers_interior_record_authentication() {
        const POINTS: usize = 4_096;
        const ARTIFACT_HEADER: usize = 176;

        let edges = (0..POINTS)
            .map(|point| vec![vec![((point + 1) % POINTS) as PointOffset]])
            .collect::<Vec<_>>();
        let links = GraphLinks::new_from_edges(edges);
        let mut graph_bytes = Vec::new();
        links.serialize(&mut graph_bytes).unwrap();
        let layout = GraphLinks::parse_layout(&graph_bytes, None).unwrap();

        let target = POINTS / 2;
        let target_offset =
            layout.level0_records_offset + target * layout.level0_record_words * POINT_BYTES;

        let mut artifact = vec![0_u8; ARTIFACT_HEADER];
        artifact.extend_from_slice(&graph_bytes);
        let descriptor = append_integrity_table(&mut artifact, ARTIFACT_HEADER).unwrap();
        // Authenticate the original graph, then mutate one interior record as
        // if immutable storage were corrupted after publication.
        artifact[ARTIFACT_HEADER + target_offset] ^= 1;

        let backing = Bytes::from(artifact);
        let verifier =
            ArtifactIntegrity::open(ArtifactIntegrityBacking::Bytes(backing.clone()), descriptor)
                .unwrap();
        let graph = GraphLinks::deserialize_bytes_with_integrity(
            backing.slice(ARTIFACT_HEADER..descriptor.offset),
            Arc::clone(&verifier),
            ARTIFACT_HEADER,
        )
        .unwrap();

        assert!(verifier.verified_chunk_count() < verifier.chunk_count());
        let error = graph
            .for_each_link(target as PointOffset, 0, |_| {})
            .unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn authenticated_access_rejects_invalid_target_without_panicking() {
        const POINTS: usize = 4_096;
        const ARTIFACT_HEADER: usize = 176;

        let edges = (0..POINTS)
            .map(|point| vec![vec![((point + 1) % POINTS) as PointOffset]])
            .collect::<Vec<_>>();
        let links = GraphLinks::new_from_edges(edges);
        let mut graph_bytes = Vec::new();
        links.serialize(&mut graph_bytes).unwrap();
        let layout = GraphLinks::parse_layout(&graph_bytes, None).unwrap();
        let target = POINTS / 2;
        let target_offset =
            layout.level0_records_offset + target * layout.level0_record_words * POINT_BYTES;
        graph_bytes[target_offset..target_offset + POINT_BYTES]
            .copy_from_slice(&(layout.point_count as u32).to_le_bytes());

        let mut artifact = vec![0_u8; ARTIFACT_HEADER];
        artifact.extend_from_slice(&graph_bytes);
        let descriptor = append_integrity_table(&mut artifact, ARTIFACT_HEADER).unwrap();
        let backing = Bytes::from(artifact);
        let verifier =
            ArtifactIntegrity::open(ArtifactIntegrityBacking::Bytes(backing.clone()), descriptor)
                .unwrap();
        let graph = GraphLinks::deserialize_bytes_with_integrity(
            backing.slice(ARTIFACT_HEADER..descriptor.offset),
            verifier,
            ARTIFACT_HEADER,
        )
        .unwrap();

        let error = graph
            .for_each_link(target as PointOffset, 0, |_| {})
            .unwrap_err();
        assert!(error.to_string().contains("link target"));
    }
}
