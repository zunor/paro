// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Graph Links
//!
//! Efficient storage for HNSW graph edges.

use super::types::PointOffset;
use memmap2::Mmap;
use paro_common::error as paro_error;
use paro_common::error::Result;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

const GRAPH_LINKS_MAGIC: u32 = u32::from_le_bytes(*b"HGLK");
const GRAPH_LINKS_VERSION_COMPRESSED_V1: u32 = 1;
const GRAPH_LINKS_COMPRESSED_HEADER_LEN: usize = 24;

/// Backing storage for flattened graph-link data.
#[derive(Debug)]
pub enum GraphLinksData {
    Ram(Vec<u8>),
    Mmap(Arc<Mmap>),
}

impl Default for GraphLinksData {
    fn default() -> Self {
        Self::Ram(Vec::new())
    }
}

impl GraphLinksData {
    fn as_bytes(&self) -> &[u8] {
        match self {
            GraphLinksData::Ram(data) => data.as_slice(),
            GraphLinksData::Mmap(mmap) => &mmap[..],
        }
    }
}

/// Memory-efficient storage for HNSW graph links.
///
/// Payload layout (`CompressedV1`):
/// [num_levels(varint), level_0_count(varint), level_0_links(sorted-delta varint), ...]
///
/// Link payload can be backed by RAM bytes or mmap bytes.
#[derive(Debug, Default)]
pub struct GraphLinks {
    data: GraphLinksData,
    data_offset_bytes: usize,
    data_len_bytes: usize,
    /// Start byte offset for each point in the compressed payload.
    offsets: Vec<usize>,
    /// Decoded CSR for level 0, the only level used by the best-first search
    /// loop. Upper levels stay compressed because they are touched sparsely.
    level0_offsets: Vec<usize>,
    level0_links: Vec<PointOffset>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct GraphLinksDegreeSummary {
    pub total_links: u64,
    pub level0_links: u64,
    pub max_level0_degree: u32,
    pub avg_level0_degree: f32,
}

#[derive(Debug)]
struct ParsedLayout {
    payload_offset: usize,
    payload_len: usize,
    offsets: Vec<usize>,
}

impl GraphLinks {
    fn validate_compressed_offsets(offsets: &[usize], data_len_bytes: usize) -> Result<()> {
        if offsets.is_empty() {
            if data_len_bytes != 0 {
                return Err(paro_error::data_corrupted(
                    "GraphLinks compressed payload must be empty when point_count=0",
                ));
            }
            return Ok(());
        }

        if offsets[0] != 0 {
            return Err(paro_error::data_corrupted(
                "GraphLinks compressed offsets must start from zero",
            ));
        }

        let mut prev = 0usize;
        for (idx, &offset) in offsets.iter().enumerate() {
            if offset >= data_len_bytes {
                return Err(paro_error::data_corrupted(format!(
                    "GraphLinks compressed offset out of range: point={} offset={} data_len={}",
                    idx, offset, data_len_bytes
                )));
            }
            if idx > 0 && offset <= prev {
                return Err(paro_error::data_corrupted(format!(
                    "GraphLinks compressed offsets must be strictly increasing: point={} prev={} current={}",
                    idx, prev, offset
                )));
            }
            prev = offset;
        }
        Ok(())
    }

    /// Create new GraphLinks from building edges.
    /// `edges` is expected to be `Vec<Vec<Vec<PointOffset>>>` where:
    /// edges[point_idx][level] = neighbors
    pub fn new_from_edges(edges: Vec<Vec<Vec<PointOffset>>>) -> Self {
        let (offsets, links) = Self::encode_edges(edges);
        let data_len_bytes = links.len();

        let mut result = Self {
            data: GraphLinksData::Ram(links),
            data_offset_bytes: 0,
            data_len_bytes,
            offsets,
            level0_offsets: Vec::new(),
            level0_links: Vec::new(),
        };
        result.populate_level0_cache();
        result
    }

    fn encode_edges(edges: Vec<Vec<Vec<PointOffset>>>) -> (Vec<usize>, Vec<u8>) {
        let mut payload = Vec::new();
        let mut offsets = Vec::with_capacity(edges.len());

        for point_edges in edges {
            offsets.push(payload.len());
            Self::encode_varint(point_edges.len() as u64, &mut payload);

            for mut level_links in point_edges {
                level_links.sort_unstable();
                Self::encode_varint(level_links.len() as u64, &mut payload);

                let mut previous = 0u32;
                for (idx, link) in level_links.into_iter().enumerate() {
                    let delta = if idx == 0 {
                        u64::from(link)
                    } else {
                        u64::from(link - previous)
                    };
                    Self::encode_varint(delta, &mut payload);
                    previous = link;
                }
            }
        }

        (offsets, payload)
    }

    /// Iterate over links of a point at a specific level.
    pub fn for_each_link<F>(&self, point_id: PointOffset, level: usize, mut f: F)
    where
        F: FnMut(PointOffset),
    {
        if level == 0 && self.for_each_cached_level0_link(point_id, &mut f) {
            return;
        }
        self.for_each_compressed_link(point_id, level, f);
    }

    fn for_each_cached_level0_link<F>(&self, point_id: PointOffset, f: &mut F) -> bool
    where
        F: FnMut(PointOffset),
    {
        let point = point_id as usize;
        let Some((&start, &end)) = self
            .level0_offsets
            .get(point)
            .zip(self.level0_offsets.get(point + 1))
        else {
            return false;
        };
        let Some(links) = self.level0_links.get(start..end) else {
            return false;
        };
        for &neighbor in links {
            f(neighbor);
        }
        true
    }

    fn populate_level0_cache(&mut self) {
        let mut offsets = Vec::with_capacity(self.offsets.len().saturating_add(1));
        let mut links = Vec::new();
        offsets.push(0);
        for point_id in 0..self.offsets.len() as PointOffset {
            self.for_each_compressed_link(point_id, 0, |neighbor| links.push(neighbor));
            offsets.push(links.len());
        }
        self.level0_offsets = offsets;
        self.level0_links = links;
    }

    /// Number of levels for a given point.
    pub fn num_levels(&self, point_id: PointOffset) -> usize {
        let Some(&start_offset) = self.offsets.get(point_id as usize) else {
            return 0;
        };
        let payload = self.links_bytes();
        let mut cursor = start_offset;
        let Some(levels) = Self::decode_varint_checked(payload, &mut cursor) else {
            return 0;
        };
        usize::try_from(levels).unwrap_or(0)
    }

    /// Highest level index for a point.
    pub fn point_level(&self, point_id: PointOffset) -> usize {
        self.num_levels(point_id).saturating_sub(1)
    }

    /// Returns links for a point/level.
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
                self.for_each_link(point_id, level, |_| {
                    degree = degree.saturating_add(1);
                });
                total_links = total_links.saturating_add(u64::from(degree));
                if level == 0 {
                    level0_links = level0_links.saturating_add(u64::from(degree));
                    max_level0_degree = max_level0_degree.max(degree);
                }
            }
        }
        let avg_level0_degree = if self.num_points() == 0 {
            0.0
        } else {
            level0_links as f32 / self.num_points() as f32
        };
        GraphLinksDegreeSummary {
            total_links,
            level0_links,
            max_level0_degree,
            avg_level0_degree,
        }
    }

    fn links_bytes(&self) -> &[u8] {
        let start = self.data_offset_bytes;
        let end = start + self.data_len_bytes;
        &self.data.as_bytes()[start..end]
    }

    fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
        while value >= 0x80 {
            out.push((value as u8 & 0x7F) | 0x80);
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

            let chunk = u64::from(byte & 0x7F);
            if shift == 63 && chunk > 1 {
                return None;
            }
            result |= chunk << shift;

            if (byte & 0x80) == 0 {
                return Some(result);
            }
            shift += 7;
        }
        None
    }

    fn decode_varint_strict(bytes: &[u8], cursor: &mut usize, context: &str) -> Result<u64> {
        Self::decode_varint_checked(bytes, cursor).ok_or_else(|| {
            paro_error::data_corrupted(format!(
                "GraphLinks {}: invalid/truncated varint at byte offset {}",
                context, cursor
            ))
        })
    }

    fn decode_compressed_point_end(
        payload: &[u8],
        start: usize,
        point_idx: usize,
    ) -> Result<usize> {
        let mut cursor = start;
        let num_levels_u64 = Self::decode_varint_strict(
            payload,
            &mut cursor,
            &format!("point={} levels", point_idx),
        )?;
        let num_levels = usize::try_from(num_levels_u64).map_err(|_| {
            paro_error::data_corrupted(format!(
                "GraphLinks point={} levels value too large: {}",
                point_idx, num_levels_u64
            ))
        })?;

        for level in 0..num_levels {
            let count_u64 = Self::decode_varint_strict(
                payload,
                &mut cursor,
                &format!("point={} level={} count", point_idx, level),
            )?;
            let count = usize::try_from(count_u64).map_err(|_| {
                paro_error::data_corrupted(format!(
                    "GraphLinks point={} level={} count too large: {}",
                    point_idx, level, count_u64
                ))
            })?;

            let mut previous = 0u32;
            for link_idx in 0..count {
                let delta_u64 = Self::decode_varint_strict(
                    payload,
                    &mut cursor,
                    &format!(
                        "point={} level={} link={} delta",
                        point_idx, level, link_idx
                    ),
                )?;
                if delta_u64 > u32::MAX as u64 {
                    return Err(paro_error::data_corrupted(format!(
                        "GraphLinks point={} level={} link={} delta out of range: {}",
                        point_idx, level, link_idx, delta_u64
                    )));
                }
                let delta = delta_u64 as u32;
                previous = if link_idx == 0 {
                    delta
                } else {
                    previous.checked_add(delta).ok_or_else(|| {
                        paro_error::data_corrupted(format!(
                            "GraphLinks point={} level={} link={} delta overflow",
                            point_idx, level, link_idx
                        ))
                    })?
                };
            }
        }
        Ok(cursor)
    }

    fn validate_compressed_payload(offsets: &[usize], payload: &[u8]) -> Result<()> {
        if offsets.is_empty() {
            if payload.is_empty() {
                return Ok(());
            }
            return Err(paro_error::data_corrupted(
                "GraphLinks compressed payload must be empty when point_count=0",
            ));
        }

        for point_idx in 0..offsets.len() {
            let start = offsets[point_idx];
            let end = Self::decode_compressed_point_end(payload, start, point_idx)?;
            let expected_end = if point_idx + 1 < offsets.len() {
                offsets[point_idx + 1]
            } else {
                payload.len()
            };
            if end != expected_end {
                return Err(paro_error::data_corrupted(format!(
                    "GraphLinks compressed point range mismatch: point={} decoded_end={} expected_end={}",
                    point_idx, end, expected_end
                )));
            }
        }

        Ok(())
    }

    fn for_each_compressed_link<F>(&self, point_id: PointOffset, level: usize, mut f: F)
    where
        F: FnMut(PointOffset),
    {
        let Some(&start) = self.offsets.get(point_id as usize) else {
            return;
        };
        let payload = self.links_bytes();
        let mut cursor = start;
        let Some(num_levels_u64) = Self::decode_varint_checked(payload, &mut cursor) else {
            return;
        };
        let Ok(num_levels) = usize::try_from(num_levels_u64) else {
            return;
        };
        if level >= num_levels {
            return;
        }

        for current_level in 0..num_levels {
            let Some(count_u64) = Self::decode_varint_checked(payload, &mut cursor) else {
                return;
            };
            let Ok(count) = usize::try_from(count_u64) else {
                return;
            };

            let mut previous = 0u32;
            if current_level == level {
                for link_idx in 0..count {
                    let Some(delta_u64) = Self::decode_varint_checked(payload, &mut cursor) else {
                        return;
                    };
                    if delta_u64 > u32::MAX as u64 {
                        return;
                    }
                    let delta = delta_u64 as u32;
                    let value = if link_idx == 0 {
                        delta
                    } else {
                        match previous.checked_add(delta) {
                            Some(v) => v,
                            None => return,
                        }
                    };
                    previous = value;
                    f(value);
                }
                return;
            }

            for link_idx in 0..count {
                let Some(delta_u64) = Self::decode_varint_checked(payload, &mut cursor) else {
                    return;
                };
                if delta_u64 > u32::MAX as u64 {
                    return;
                }
                let delta = delta_u64 as u32;
                previous = if link_idx == 0 {
                    delta
                } else {
                    match previous.checked_add(delta) {
                        Some(v) => v,
                        None => return,
                    }
                };
            }
        }
    }

    fn build_compressed_payload(&self) -> Result<(Vec<usize>, Vec<u8>)> {
        let mut payload = Vec::new();
        let mut offsets = Vec::with_capacity(self.num_points());

        for point in 0..self.num_points() as PointOffset {
            offsets.push(payload.len());

            let num_levels = self.num_levels(point);
            Self::encode_varint(num_levels as u64, &mut payload);

            for level in 0..num_levels {
                let mut links = self.links_on_level(point, level).unwrap_or_default();
                links.sort_unstable();
                Self::encode_varint(links.len() as u64, &mut payload);
                let mut previous = 0u32;
                for (idx, link) in links.into_iter().enumerate() {
                    let delta = if idx == 0 {
                        link as u64
                    } else {
                        u64::from(link - previous)
                    };
                    Self::encode_varint(delta, &mut payload);
                    previous = link;
                }
            }
        }

        Ok((offsets, payload))
    }

    fn serialize_bytes(&self) -> Result<Vec<u8>> {
        let (offsets, payload) = self.build_compressed_payload()?;

        let point_count_u32 = u32::try_from(offsets.len())
            .map_err(|_| paro_error::out_of_range("GraphLinks point_count overflow"))?;
        let payload_len_u32 = u32::try_from(payload.len())
            .map_err(|_| paro_error::out_of_range("GraphLinks compressed payload overflow"))?;
        let offsets_count_u32 = u32::try_from(offsets.len())
            .map_err(|_| paro_error::out_of_range("GraphLinks offsets_count overflow"))?;

        let mut out = Vec::with_capacity(
            GRAPH_LINKS_COMPRESSED_HEADER_LEN + offsets.len() * 8 + payload.len(),
        );
        out.extend_from_slice(&GRAPH_LINKS_MAGIC.to_le_bytes());
        out.extend_from_slice(&GRAPH_LINKS_VERSION_COMPRESSED_V1.to_le_bytes());
        out.extend_from_slice(&point_count_u32.to_le_bytes());
        out.extend_from_slice(&payload_len_u32.to_le_bytes());
        out.extend_from_slice(&offsets_count_u32.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes()); // reserved flags

        for offset in offsets {
            out.extend_from_slice(&(offset as u64).to_le_bytes());
        }
        out.extend_from_slice(&payload);
        Ok(out)
    }

    fn read_u32(bytes: &[u8], start: usize, field: &str) -> Result<u32> {
        let end = start
            .checked_add(4)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks header overflow"))?;
        let raw = bytes.get(start..end).ok_or_else(|| {
            paro_error::data_corrupted(format!("GraphLinks missing field: {}", field))
        })?;
        Ok(u32::from_le_bytes(raw.try_into().unwrap()))
    }

    fn read_u64(bytes: &[u8], start: usize, field: &str) -> Result<u64> {
        let end = start
            .checked_add(8)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks offset overflow"))?;
        let raw = bytes.get(start..end).ok_or_else(|| {
            paro_error::data_corrupted(format!("GraphLinks missing field: {}", field))
        })?;
        Ok(u64::from_le_bytes(raw.try_into().unwrap()))
    }

    fn parse_compressed_v1_layout(bytes: &[u8]) -> Result<ParsedLayout> {
        if bytes.len() < GRAPH_LINKS_COMPRESSED_HEADER_LEN {
            return Err(paro_error::data_corrupted(
                "GraphLinks compressed header truncated",
            ));
        }

        let version = Self::read_u32(bytes, 4, "version")?;
        if version != GRAPH_LINKS_VERSION_COMPRESSED_V1 {
            return Err(paro_error::data_corrupted(format!(
                "unknown GraphLinks version: {} (expected {})",
                version, GRAPH_LINKS_VERSION_COMPRESSED_V1
            )));
        }

        let point_count = Self::read_u32(bytes, 8, "point_count")? as usize;
        let payload_len = Self::read_u32(bytes, 12, "payload_len")? as usize;
        let offsets_count = Self::read_u32(bytes, 16, "offsets_count")? as usize;
        let reserved_flags = Self::read_u32(bytes, 20, "reserved_flags")?;
        if reserved_flags != 0 {
            return Err(paro_error::data_corrupted(format!(
                "GraphLinks compressed reserved flags must be zero, got {}",
                reserved_flags
            )));
        }
        if offsets_count != point_count {
            return Err(paro_error::data_corrupted(format!(
                "GraphLinks offsets_count mismatch: expected {}, got {}",
                point_count, offsets_count
            )));
        }

        let offsets_bytes_len = point_count
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks offsets length overflow"))?;
        let payload_offset = GRAPH_LINKS_COMPRESSED_HEADER_LEN
            .checked_add(offsets_bytes_len)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks payload offset overflow"))?;
        let payload_end = payload_offset
            .checked_add(payload_len)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks payload range overflow"))?;
        if payload_end > bytes.len() {
            return Err(paro_error::data_corrupted(format!(
                "GraphLinks compressed data truncated: need {} bytes, got {} bytes",
                payload_end,
                bytes.len()
            )));
        }

        let mut offsets = Vec::with_capacity(point_count);
        for i in 0..point_count {
            let start = GRAPH_LINKS_COMPRESSED_HEADER_LEN + i * 8;
            let offset = Self::read_u64(bytes, start, "compressed offset")?;
            let offset = usize::try_from(offset)
                .map_err(|_| paro_error::data_corrupted("GraphLinks compressed offset overflow"))?;
            offsets.push(offset);
        }

        Self::validate_compressed_offsets(&offsets, payload_len)?;
        Self::validate_compressed_payload(&offsets, &bytes[payload_offset..payload_end])?;

        Ok(ParsedLayout {
            payload_offset,
            payload_len,
            offsets,
        })
    }

    fn parse_layout(bytes: &[u8]) -> Result<ParsedLayout> {
        if bytes.len() < GRAPH_LINKS_COMPRESSED_HEADER_LEN {
            return Err(paro_error::data_corrupted(
                "GraphLinks file too small for header",
            ));
        }
        let marker = Self::read_u32(bytes, 0, "marker")?;
        if marker == GRAPH_LINKS_MAGIC {
            return Self::parse_compressed_v1_layout(bytes);
        }
        Err(paro_error::data_corrupted(
            "legacy GraphLinks payloads are no longer supported",
        ))
    }

    /// Save graph links to a writer.
    pub fn serialize<W: Write>(&self, mut writer: W) -> Result<()> {
        let bytes = self.serialize_bytes()?;
        writer.write_all(&bytes)?;
        Ok(())
    }

    /// Save graph links to a file.
    pub fn save(&self, path: &Path) -> Result<()> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        self.serialize(writer)
    }

    /// Load graph links from a reader.
    pub fn deserialize<R: Read>(mut reader: R) -> Result<Self> {
        let mut serialized = Vec::new();
        reader.read_to_end(&mut serialized)?;
        let layout = Self::parse_layout(&serialized)?;
        let payload =
            serialized[layout.payload_offset..layout.payload_offset + layout.payload_len].to_vec();

        let mut result = Self {
            data: GraphLinksData::Ram(payload),
            data_offset_bytes: 0,
            data_len_bytes: layout.payload_len,
            offsets: layout.offsets,
            level0_offsets: Vec::new(),
            level0_links: Vec::new(),
        };
        result.populate_level0_cache();
        Ok(result)
    }

    /// Load graph links from a file.
    pub fn load(path: &Path) -> Result<Self> {
        Self::deserialize(File::open(path)?)
    }

    /// Load graph links from a file by mmap-ing link payload.
    pub fn load_mmap(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        let mmap = Arc::new(mmap);
        let bytes = &mmap[..];
        let layout = Self::parse_layout(bytes)?;

        let mut result = Self {
            data: GraphLinksData::Mmap(mmap),
            data_offset_bytes: layout.payload_offset,
            data_len_bytes: layout.payload_len,
            offsets: layout.offsets,
            level0_offsets: Vec::new(),
            level0_links: Vec::new(),
        };
        result.populate_level0_cache();
        Ok(result)
    }

    pub fn num_points(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_mmap_backed(&self) -> bool {
        matches!(self.data, GraphLinksData::Mmap(_))
    }

    /// Serialized size in bytes (as produced by `serialize`).
    pub fn serialized_size_bytes(&self) -> u64 {
        self.serialize_bytes()
            .map(|bytes| bytes.len() as u64)
            .unwrap_or(0)
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

    fn collect_links(g: &GraphLinks, point: PointOffset, level: usize) -> Vec<PointOffset> {
        let mut links = Vec::new();
        g.for_each_link(point, level, |n| links.push(n));
        links
    }

    #[test]
    fn legacy_payload_is_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());

        let err = GraphLinks::deserialize(bytes.as_slice()).unwrap_err();
        assert!(
            err.to_string().contains("legacy GraphLinks payloads"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn compressed_roundtrip_uses_versioned_header_and_sorted_delta() {
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut bytes = Vec::new();
        links.serialize(&mut bytes).unwrap();

        assert_eq!(
            u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            GRAPH_LINKS_MAGIC
        );
        assert_eq!(
            u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            GRAPH_LINKS_VERSION_COMPRESSED_V1
        );

        let restored = GraphLinks::deserialize(bytes.as_slice()).unwrap();
        assert_eq!(restored.num_points(), 4);
        assert_eq!(collect_links(&restored, 0, 0), vec![1, 2]);
        assert_eq!(collect_links(&restored, 1, 0), vec![0, 2]);
        assert_eq!(collect_links(&restored, 2, 1), vec![0]);
    }

    #[test]
    fn compressed_deserialize_ignores_stats_trailer() {
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut bytes = Vec::new();
        links.serialize(&mut bytes).unwrap();

        append_stats_trailer(&mut bytes, &[7, 7, 7]).unwrap();
        let restored = GraphLinks::deserialize(bytes.as_slice()).unwrap();
        assert_eq!(restored.num_points(), 4);
        assert_eq!(collect_links(&restored, 0, 0), vec![1, 2]);
        assert_eq!(collect_links(&restored, 3, 0), vec![0]);
    }

    #[test]
    fn compressed_unknown_version_reports_error() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&GRAPH_LINKS_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&999u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());

        let err = GraphLinks::deserialize(bytes.as_slice()).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("unknown GraphLinks version"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn mmap_load_matches_ram_load() {
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
}
