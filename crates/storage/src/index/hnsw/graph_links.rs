// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Persistent HNSW adjacency storage.
//!
//! Version 2 stores the hot level-0 graph as plain little-endian CSR and keeps
//! only upper levels delta-varint encoded. Both offset tables and link payloads
//! remain in the serialized backing bytes, so an mmap-backed index does not
//! allocate a decoded graph copy when opened.

use super::types::PointOffset;
use bytes::Bytes;
use memmap2::Mmap;
use paro_common::error as paro_error;
use paro_common::error::Result;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

const GRAPH_LINKS_MAGIC: u32 = u32::from_le_bytes(*b"HGLK");
const GRAPH_LINKS_VERSION_HYBRID_CSR_V2: u32 = 2;
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
    serialized_len_bytes: usize,
    point_count: usize,
    level0_link_count: usize,
    level0_offsets_offset: usize,
    level0_links_offset: usize,
    upper_offsets_offset: usize,
    upper_payload_offset: usize,
    upper_payload_len: usize,
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
    level0_offsets_offset: usize,
    level0_links_offset: usize,
    upper_offsets_offset: usize,
    upper_payload_offset: usize,
    upper_payload_len: usize,
}

impl GraphLinks {
    pub fn new_from_edges(edges: Vec<Vec<Vec<PointOffset>>>) -> Self {
        let serialized = Self::encode_edges(edges);
        let layout =
            Self::parse_layout(&serialized).expect("newly encoded GraphLinks layout must be valid");
        Self::from_data(GraphLinksData::Ram(serialized), layout)
    }

    fn from_data(data: GraphLinksData, layout: ParsedLayout) -> Self {
        Self {
            data,
            serialized_len_bytes: layout.serialized_len_bytes,
            point_count: layout.point_count,
            level0_link_count: layout.level0_link_count,
            level0_offsets_offset: layout.level0_offsets_offset,
            level0_links_offset: layout.level0_links_offset,
            upper_offsets_offset: layout.upper_offsets_offset,
            upper_payload_offset: layout.upper_payload_offset,
            upper_payload_len: layout.upper_payload_len,
        }
    }

    fn encode_edges(edges: Vec<Vec<Vec<PointOffset>>>) -> Vec<u8> {
        let point_count = edges.len();
        let mut level0_offsets = Vec::with_capacity(point_count.saturating_add(1));
        let mut level0_links = Vec::new();
        let mut upper_offsets = Vec::with_capacity(point_count.saturating_add(1));
        let mut upper_payload = Vec::new();
        level0_offsets.push(0u64);
        upper_offsets.push(0u64);

        for point_edges in edges {
            let num_levels = point_edges.len();
            Self::encode_varint(num_levels as u64, &mut upper_payload);
            for (level, mut links) in point_edges.into_iter().enumerate() {
                links.sort_unstable();
                if level == 0 {
                    level0_links.extend_from_slice(&links);
                    continue;
                }
                Self::encode_varint(links.len() as u64, &mut upper_payload);
                let mut previous = 0u32;
                for (index, link) in links.into_iter().enumerate() {
                    let delta = if index == 0 {
                        u64::from(link)
                    } else {
                        u64::from(link - previous)
                    };
                    Self::encode_varint(delta, &mut upper_payload);
                    previous = link;
                }
            }
            level0_offsets.push(level0_links.len() as u64);
            upper_offsets.push(upper_payload.len() as u64);
        }

        let level0_offsets_bytes = level0_offsets.len().saturating_mul(U64_BYTES);
        let level0_links_bytes = level0_links.len().saturating_mul(POINT_BYTES);
        let upper_offsets_bytes = upper_offsets.len().saturating_mul(U64_BYTES);
        let serialized_len = GRAPH_LINKS_HEADER_LEN
            .saturating_add(level0_offsets_bytes)
            .saturating_add(level0_links_bytes)
            .saturating_add(upper_offsets_bytes)
            .saturating_add(upper_payload.len());

        let mut out = Vec::with_capacity(serialized_len);
        out.extend_from_slice(&GRAPH_LINKS_MAGIC.to_le_bytes());
        out.extend_from_slice(&GRAPH_LINKS_VERSION_HYBRID_CSR_V2.to_le_bytes());
        out.extend_from_slice(&(point_count as u64).to_le_bytes());
        out.extend_from_slice(&(level0_links.len() as u64).to_le_bytes());
        out.extend_from_slice(&(upper_payload.len() as u64).to_le_bytes());
        out.extend_from_slice(&(serialized_len as u64).to_le_bytes());
        out.extend_from_slice(&0u64.to_le_bytes());
        out.extend_from_slice(&0u64.to_le_bytes());
        out.extend_from_slice(&0u64.to_le_bytes());
        debug_assert_eq!(out.len(), GRAPH_LINKS_HEADER_LEN);

        for offset in level0_offsets {
            out.extend_from_slice(&offset.to_le_bytes());
        }
        for link in level0_links {
            out.extend_from_slice(&link.to_le_bytes());
        }
        for offset in upper_offsets {
            out.extend_from_slice(&offset.to_le_bytes());
        }
        out.extend_from_slice(&upper_payload);
        debug_assert_eq!(out.len(), serialized_len);
        out
    }

    #[inline]
    pub fn for_each_link<F>(&self, point_id: PointOffset, level: usize, mut f: F)
    where
        F: FnMut(PointOffset),
    {
        if level == 0 {
            self.for_each_level0_link(point_id, f);
        } else {
            self.for_each_upper_link(point_id, level, &mut f);
        }
    }

    #[inline]
    fn for_each_level0_link<F>(&self, point_id: PointOffset, mut f: F)
    where
        F: FnMut(PointOffset),
    {
        let Some((start, end)) = self.level0_range(point_id) else {
            return;
        };
        let bytes = self.data.as_bytes();
        let start_byte = self.level0_links_offset + start * POINT_BYTES;
        let end_byte = self.level0_links_offset + end * POINT_BYTES;
        #[cfg(target_endian = "little")]
        if let Ok(links) = bytemuck::try_cast_slice::<u8, PointOffset>(&bytes[start_byte..end_byte])
        {
            for &link in links {
                f(link);
            }
            return;
        }
        // Offset-table validation makes this a single checked slice operation;
        // aligned little-endian backings use the native slice above. The
        // portable fallback retains exact-width decoding for an unaligned
        // Bytes view or a big-endian host.
        for raw in bytes[start_byte..end_byte].chunks_exact(POINT_BYTES) {
            f(PointOffset::from_le_bytes(
                raw.try_into().expect("level-0 link chunk width"),
            ));
        }
    }

    fn for_each_upper_link<F>(&self, point_id: PointOffset, level: usize, f: &mut F)
    where
        F: FnMut(PointOffset),
    {
        let Some(payload) = self.upper_point_payload(point_id) else {
            return;
        };
        let mut cursor = 0usize;
        let Some(num_levels) = Self::decode_varint_checked(payload, &mut cursor)
            .and_then(|levels| usize::try_from(levels).ok())
        else {
            return;
        };
        if level >= num_levels {
            return;
        }

        for current_level in 1..num_levels {
            let Some(count) = Self::decode_varint_checked(payload, &mut cursor)
                .and_then(|count| usize::try_from(count).ok())
            else {
                return;
            };
            let mut previous = 0u32;
            for link_index in 0..count {
                let Some(delta) = Self::decode_varint_checked(payload, &mut cursor)
                    .and_then(|delta| u32::try_from(delta).ok())
                else {
                    return;
                };
                let Some(link) = (if link_index == 0 {
                    Some(delta)
                } else {
                    previous.checked_add(delta)
                }) else {
                    return;
                };
                previous = link;
                if current_level == level {
                    f(link);
                }
            }
            if current_level == level {
                return;
            }
        }
    }

    #[inline]
    fn level0_range(&self, point_id: PointOffset) -> Option<(usize, usize)> {
        let point = usize::try_from(point_id).ok()?;
        if point >= self.point_count {
            return None;
        }
        let bytes = self.data.as_bytes();
        #[cfg(target_endian = "little")]
        if let Ok(offsets) = bytemuck::try_cast_slice::<u8, u64>(
            &bytes[self.level0_offsets_offset..self.level0_links_offset],
        ) {
            let start = offsets[point] as usize;
            let end = offsets[point + 1] as usize;
            debug_assert!(start <= end && end <= self.level0_link_count);
            return Some((start, end));
        }
        let start =
            Self::read_layout_u64(bytes, self.level0_offsets_offset + point * U64_BYTES) as usize;
        let end = Self::read_layout_u64(bytes, self.level0_offsets_offset + (point + 1) * U64_BYTES)
            as usize;
        debug_assert!(start <= end && end <= self.level0_link_count);
        Some((start, end))
    }

    #[inline]
    fn upper_point_payload(&self, point_id: PointOffset) -> Option<&[u8]> {
        let point = usize::try_from(point_id).ok()?;
        if point >= self.point_count {
            return None;
        }
        let bytes = self.data.as_bytes();
        let start =
            Self::read_layout_u64(bytes, self.upper_offsets_offset + point * U64_BYTES) as usize;
        let end = Self::read_layout_u64(bytes, self.upper_offsets_offset + (point + 1) * U64_BYTES)
            as usize;
        debug_assert!(start <= end && end <= self.upper_payload_len);
        Some(&bytes[self.upper_payload_offset + start..self.upper_payload_offset + end])
    }

    pub fn num_levels(&self, point_id: PointOffset) -> usize {
        let Some(payload) = self.upper_point_payload(point_id) else {
            return 0;
        };
        let mut cursor = 0usize;
        Self::decode_varint_checked(payload, &mut cursor)
            .and_then(|levels| usize::try_from(levels).ok())
            .unwrap_or(0)
    }

    pub fn point_level(&self, point_id: PointOffset) -> usize {
        self.num_levels(point_id).saturating_sub(1)
    }

    pub fn links_on_level(&self, point_id: PointOffset, level: usize) -> Option<Vec<PointOffset>> {
        if level >= self.num_levels(point_id) {
            return None;
        }
        let mut links = Vec::new();
        self.for_each_link(point_id, level, |neighbor| links.push(neighbor));
        Some(links)
    }

    pub fn degree_summary(&self) -> GraphLinksDegreeSummary {
        let mut total_links = 0u64;
        let mut level0_links = 0u64;
        let mut max_level0_degree = 0u32;
        for point_id in 0..self.num_points() as PointOffset {
            for level in 0..self.num_levels(point_id) {
                let mut degree = 0u32;
                self.for_each_link(point_id, level, |_| degree = degree.saturating_add(1));
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
        GraphLinksDegreeSummary {
            total_links,
            level0_links,
            max_level0_degree,
            avg_level0_degree,
        }
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

    fn validate_level0_links(
        bytes: &[u8],
        point_count: usize,
        link_count: usize,
        links_offset: usize,
    ) -> Result<()> {
        for link_index in 0..link_count {
            let link = Self::read_u32(
                bytes,
                links_offset + link_index * POINT_BYTES,
                "level-0 link",
            )?;
            if link as usize >= point_count {
                return Err(paro_error::data_corrupted(format!(
                    "GraphLinks level-0 link target {link} out of bounds at link {link_index}"
                )));
            }
        }
        Ok(())
    }

    fn parse_layout(bytes: &[u8]) -> Result<ParsedLayout> {
        if bytes.len() < GRAPH_LINKS_HEADER_LEN {
            return Err(paro_error::data_corrupted(
                "GraphLinks file too small for version-2 header",
            ));
        }
        let marker = Self::read_u32(bytes, 0, "marker")?;
        if marker != GRAPH_LINKS_MAGIC {
            return Err(paro_error::data_corrupted(
                "legacy GraphLinks payloads are no longer supported",
            ));
        }
        let version = Self::read_u32(bytes, 4, "version")?;
        if version != GRAPH_LINKS_VERSION_HYBRID_CSR_V2 {
            return Err(paro_error::data_corrupted(format!(
                "unknown GraphLinks version: {version} (expected {GRAPH_LINKS_VERSION_HYBRID_CSR_V2})"
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
        for offset in [40usize, 48, 56] {
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
        let level0_links_bytes = level0_link_count
            .checked_mul(POINT_BYTES)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks level-0 size overflow"))?;
        let level0_offsets_offset = GRAPH_LINKS_HEADER_LEN;
        let level0_links_offset = level0_offsets_offset
            .checked_add(offsets_bytes)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks level-0 offset overflow"))?;
        let upper_offsets_offset = level0_links_offset
            .checked_add(level0_links_bytes)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks upper offset overflow"))?;
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

        let first_level0 = Self::read_u64(bytes, level0_offsets_offset, "first level-0 offset")?;
        let last_level0 = Self::read_u64(
            bytes,
            level0_offsets_offset + point_count * U64_BYTES,
            "last level-0 offset",
        )?;
        if first_level0 != 0 || last_level0 != level0_link_count as u64 {
            return Err(paro_error::data_corrupted(
                "GraphLinks level-0 CSR boundary mismatch",
            ));
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
        Self::validate_offset_table(
            bytes,
            level0_offsets_offset,
            point_count,
            level0_link_count,
            "level-0 offset",
        )?;
        Self::validate_offset_table(
            bytes,
            upper_offsets_offset,
            point_count,
            upper_payload_len,
            "upper offset",
        )?;
        Ok(ParsedLayout {
            serialized_len_bytes,
            point_count,
            level0_link_count,
            level0_offsets_offset,
            level0_links_offset,
            upper_offsets_offset,
            upper_payload_offset,
            upper_payload_len,
        })
    }

    pub fn serialize<W: Write>(&self, mut writer: W) -> Result<()> {
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
        let layout = Self::parse_layout(&serialized)?;
        serialized.truncate(layout.serialized_len_bytes);
        Ok(Self::from_data(GraphLinksData::Ram(serialized), layout))
    }

    /// Open graph links over an owned byte view without copying the graph.
    pub fn deserialize_bytes(serialized: Bytes) -> Result<Self> {
        let layout = Self::parse_layout(&serialized)?;
        let serialized = serialized.slice(..layout.serialized_len_bytes);
        Ok(Self::from_data(GraphLinksData::Bytes(serialized), layout))
    }

    pub fn load(path: &Path) -> Result<Self> {
        Self::deserialize(File::open(path)?)
    }

    pub fn load_mmap(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = Arc::new(unsafe { memmap2::MmapOptions::new().map(&file)? });
        let layout = Self::parse_layout(&mmap)?;
        Ok(Self::from_data(GraphLinksData::Mmap(mmap), layout))
    }

    /// Open graph links over a validated range of a shared mmap package.
    pub fn deserialize_mmap_range(mmap: Arc<Mmap>, offset: usize, len: usize) -> Result<Self> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| paro_common::error::data_corrupted("HNSW mmap range overflow"))?;
        let serialized = mmap.get(offset..end).ok_or_else(|| {
            paro_common::error::data_corrupted("HNSW mmap range exceeds package length")
        })?;
        let layout = Self::parse_layout(serialized)?;
        Ok(Self::from_data(
            GraphLinksData::MmapSlice {
                mmap,
                offset,
                len: layout.serialized_len_bytes,
            },
            layout,
        ))
    }

    /// Perform an explicit O(E) integrity scan of every graph link and upper
    /// payload. Normal open validates the O(N) offset tables; segment and
    /// sidecar readers additionally validate their enclosing checksums.
    /// Standalone recovery tooling can opt into this deeper semantic scan
    /// without forcing every mmap open to fault every graph page.
    pub fn verify_integrity(&self) -> Result<()> {
        let bytes = self.data.as_bytes();
        Self::validate_level0_links(
            bytes,
            self.point_count,
            self.level0_link_count,
            self.level0_links_offset,
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
        graph.for_each_link(point, level, |neighbor| links.push(neighbor));
        links
    }

    #[test]
    fn legacy_payload_is_rejected() {
        let bytes = 1u32.to_le_bytes();
        let error = GraphLinks::deserialize(bytes.as_slice()).unwrap_err();
        assert!(error.to_string().contains("version-2 header"));
    }

    #[test]
    fn old_compressed_version_is_rejected() {
        let mut bytes = vec![0u8; GRAPH_LINKS_HEADER_LEN];
        bytes[0..4].copy_from_slice(&GRAPH_LINKS_MAGIC.to_le_bytes());
        bytes[4..8].copy_from_slice(&1u32.to_le_bytes());
        let error = GraphLinks::deserialize(bytes.as_slice()).unwrap_err();
        assert!(error.to_string().contains("unknown GraphLinks version"));
    }

    #[test]
    fn hybrid_roundtrip_uses_plain_level0_and_compressed_upper_levels() {
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut bytes = Vec::new();
        links.serialize(&mut bytes).unwrap();

        assert_eq!(
            u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            GRAPH_LINKS_MAGIC
        );
        assert_eq!(
            u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            GRAPH_LINKS_VERSION_HYBRID_CSR_V2
        );
        let layout = GraphLinks::parse_layout(&bytes).unwrap();
        assert_eq!(
            u32::from_le_bytes(
                bytes[layout.level0_links_offset..layout.level0_links_offset + 4]
                    .try_into()
                    .unwrap()
            ),
            1
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
            assert_eq!(ram.num_levels(point), mmap.num_levels(point));
            for level in 0..ram.num_levels(point) {
                assert_eq!(
                    collect_links(&ram, point, level),
                    collect_links(&mmap, point, level)
                );
            }
        }
    }

    #[test]
    fn middle_level0_offset_corruption_is_rejected_at_open() {
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut bytes = Vec::new();
        links.serialize(&mut bytes).unwrap();
        let layout = GraphLinks::parse_layout(&bytes).unwrap();
        let corrupt = (layout.level0_link_count as u64 + 1).to_le_bytes();
        let offset = layout.level0_offsets_offset + U64_BYTES;
        bytes[offset..offset + U64_BYTES].copy_from_slice(&corrupt);

        let error = GraphLinks::deserialize(bytes.as_slice()).unwrap_err();
        assert!(error.to_string().contains("level-0 offset"));
    }

    #[test]
    fn deep_verification_rejects_truncated_upper_varint() {
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut bytes = Vec::new();
        links.serialize(&mut bytes).unwrap();
        let layout = GraphLinks::parse_layout(&bytes).unwrap();
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
        let layout = GraphLinks::parse_layout(&bytes).unwrap();
        bytes[layout.level0_links_offset..layout.level0_links_offset + POINT_BYTES]
            .copy_from_slice(&(layout.point_count as u32).to_le_bytes());

        let restored = GraphLinks::deserialize(bytes.as_slice()).unwrap();
        let error = restored.verify_integrity().unwrap_err();
        assert!(error.to_string().contains("level-0 link target"));
    }
}
