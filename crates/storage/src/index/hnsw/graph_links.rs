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
const GRAPH_LINKS_WRITE_FORMAT_ENV: &str = "PARO_HNSW_GRAPH_LINKS_WRITE_FORMAT";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GraphLinksEncoding {
    LegacyU32,
    CompressedVarintDeltaV1,
}

impl Default for GraphLinksEncoding {
    fn default() -> Self {
        Self::LegacyU32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphLinksWriteFormat {
    Legacy,
    CompressedV1,
}

impl GraphLinksWriteFormat {
    fn from_env() -> Self {
        match std::env::var(GRAPH_LINKS_WRITE_FORMAT_ENV)
            .ok()
            .map(|value| value.to_ascii_lowercase())
            .as_deref()
        {
            Some("legacy" | "v0" | "plain") => Self::Legacy,
            _ => Self::CompressedV1,
        }
    }
}

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
/// Legacy payload layout (`LegacyU32`):
/// [num_levels, level_0_count, level_0_links..., level_1_count, ...] with u32 values.
///
/// Compressed payload layout (`CompressedVarintDeltaV1`):
/// [num_levels(varint), level_0_count(varint), level_0_links(sorted-delta varint), ...]
///
/// Link payload can be backed by RAM bytes or mmap bytes.
#[derive(Debug, Default)]
pub struct GraphLinks {
    data: GraphLinksData,
    encoding: GraphLinksEncoding,
    data_offset_bytes: usize,
    data_len_bytes: usize,
    /// Start offset for each point in the payload.
    /// Legacy format uses u32 element offsets.
    /// Compressed format uses byte offsets.
    offsets: Vec<usize>,
}

#[derive(Debug)]
struct ParsedLayout {
    encoding: GraphLinksEncoding,
    payload_offset: usize,
    payload_len: usize,
    offsets: Vec<usize>,
}

impl GraphLinks {
    fn validate_legacy_offsets(offsets: &[usize], data_len_u32: usize) -> Result<()> {
        for (idx, &offset) in offsets.iter().enumerate() {
            if offset >= data_len_u32 {
                return Err(paro_error::data_corrupted(format!(
                    "GraphLinks offset out of range: point={} offset={} data_len={}",
                    idx, offset, data_len_u32
                )));
            }
        }
        Ok(())
    }

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
        let mut links = Vec::new();
        let mut offsets = Vec::with_capacity(edges.len());
        let mut link_u32_len = 0usize;

        for point_edges in edges {
            offsets.push(link_u32_len);

            let num_levels = point_edges.len();
            links.extend_from_slice(&(num_levels as u32).to_le_bytes());
            link_u32_len += 1;

            for level_links in point_edges {
                links.extend_from_slice(&(level_links.len() as u32).to_le_bytes());
                link_u32_len += 1;
                for link in level_links {
                    links.extend_from_slice(&link.to_le_bytes());
                    link_u32_len += 1;
                }
            }
        }

        Self {
            data: GraphLinksData::Ram(links),
            encoding: GraphLinksEncoding::LegacyU32,
            data_offset_bytes: 0,
            data_len_bytes: link_u32_len * std::mem::size_of::<u32>(),
            offsets,
        }
    }

    /// Iterate over links of a point at a specific level.
    pub fn for_each_link<F>(&self, point_id: PointOffset, level: usize, mut f: F)
    where
        F: FnMut(PointOffset),
    {
        match self.encoding {
            GraphLinksEncoding::LegacyU32 => {
                let Some((links_start, count)) = self.level_slice_start_legacy(point_id, level)
                else {
                    return;
                };
                for i in 0..count {
                    f(self.read_legacy_u32_at(links_start + i));
                }
            }
            GraphLinksEncoding::CompressedVarintDeltaV1 => {
                self.for_each_compressed_link(point_id, level, f);
            }
        }
    }

    /// Number of levels for a given point.
    pub fn num_levels(&self, point_id: PointOffset) -> usize {
        let Some(&start_offset) = self.offsets.get(point_id as usize) else {
            return 0;
        };
        match self.encoding {
            GraphLinksEncoding::LegacyU32 => self.read_legacy_u32_at(start_offset) as usize,
            GraphLinksEncoding::CompressedVarintDeltaV1 => {
                let payload = self.links_bytes();
                let mut cursor = start_offset;
                let Some(levels) = Self::decode_varint_checked(payload, &mut cursor) else {
                    return 0;
                };
                usize::try_from(levels).unwrap_or(0)
            }
        }
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

    fn level_slice_start_legacy(
        &self,
        point_id: PointOffset,
        level: usize,
    ) -> Option<(usize, usize)> {
        let start_offset = *self.offsets.get(point_id as usize)?;
        let num_levels = self.read_legacy_u32_at(start_offset) as usize;
        if level >= num_levels {
            return None;
        }

        let mut current_pos = start_offset + 1;
        for _ in 0..level {
            let count = self.read_legacy_u32_at(current_pos) as usize;
            current_pos += 1 + count;
        }

        let count = self.read_legacy_u32_at(current_pos) as usize;
        let links_start = current_pos + 1;
        Some((links_start, count))
    }

    fn legacy_data_len_u32(&self) -> usize {
        debug_assert_eq!(self.data_len_bytes % std::mem::size_of::<u32>(), 0);
        self.data_len_bytes / std::mem::size_of::<u32>()
    }

    fn read_legacy_u32_at(&self, idx: usize) -> u32 {
        debug_assert_eq!(self.encoding, GraphLinksEncoding::LegacyU32);
        debug_assert!(idx < self.legacy_data_len_u32());
        let byte_offset = self
            .data_offset_bytes
            .saturating_add(idx.saturating_mul(std::mem::size_of::<u32>()));
        let bytes = self.data.as_bytes();
        let chunk = &bytes[byte_offset..byte_offset + 4];
        u32::from_le_bytes(chunk.try_into().expect("u32 chunk"))
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

    fn build_legacy_payload(&self) -> Result<(Vec<usize>, Vec<u8>, usize)> {
        let mut payload = Vec::new();
        let mut offsets = Vec::with_capacity(self.num_points());
        let mut payload_len_u32 = 0usize;

        for point in 0..self.num_points() as PointOffset {
            offsets.push(payload_len_u32);

            let num_levels = self.num_levels(point);
            let num_levels_u32 = u32::try_from(num_levels).map_err(|_| {
                paro_error::out_of_range(format!(
                    "GraphLinks point {} levels exceed u32: {}",
                    point, num_levels
                ))
            })?;
            payload.extend_from_slice(&num_levels_u32.to_le_bytes());
            payload_len_u32 += 1;

            for level in 0..num_levels {
                let links = self.links_on_level(point, level).unwrap_or_default();
                let level_count_u32 = u32::try_from(links.len()).map_err(|_| {
                    paro_error::out_of_range(format!(
                        "GraphLinks point {} level {} links exceed u32: {}",
                        point,
                        level,
                        links.len()
                    ))
                })?;
                payload.extend_from_slice(&level_count_u32.to_le_bytes());
                payload_len_u32 += 1;
                for link in links {
                    payload.extend_from_slice(&link.to_le_bytes());
                    payload_len_u32 += 1;
                }
            }
        }

        Ok((offsets, payload, payload_len_u32))
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

    fn serialize_legacy_bytes(&self) -> Result<Vec<u8>> {
        let (offsets, payload, payload_len_u32) = self.build_legacy_payload()?;

        let point_count_u32 = u32::try_from(offsets.len())
            .map_err(|_| paro_error::out_of_range("GraphLinks point_count overflow"))?;
        let payload_len_u32 = u32::try_from(payload_len_u32)
            .map_err(|_| paro_error::out_of_range("GraphLinks payload_len overflow"))?;

        let mut out = Vec::with_capacity(8 + offsets.len() * 8 + payload.len());
        out.extend_from_slice(&point_count_u32.to_le_bytes());
        out.extend_from_slice(&payload_len_u32.to_le_bytes());
        for offset in offsets {
            out.extend_from_slice(&(offset as u64).to_le_bytes());
        }
        out.extend_from_slice(&payload);
        Ok(out)
    }

    fn serialize_compressed_v1_bytes(&self) -> Result<Vec<u8>> {
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

    fn serialize_bytes_for_format(&self, format: GraphLinksWriteFormat) -> Result<Vec<u8>> {
        match format {
            GraphLinksWriteFormat::Legacy => self.serialize_legacy_bytes(),
            GraphLinksWriteFormat::CompressedV1 => self.serialize_compressed_v1_bytes(),
        }
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

    fn parse_legacy_layout(bytes: &[u8], point_count: usize) -> Result<ParsedLayout> {
        if bytes.len() < 8 {
            return Err(paro_error::data_corrupted(
                "GraphLinks file too small for legacy header",
            ));
        }

        let payload_len_u32 = Self::read_u32(bytes, 4, "legacy payload_len")? as usize;
        let offsets_bytes_len = point_count
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks offsets length overflow"))?;
        let payload_offset = 8usize
            .checked_add(offsets_bytes_len)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks payload offset overflow"))?;
        let payload_len = payload_len_u32
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks payload length overflow"))?;
        let payload_end = payload_offset
            .checked_add(payload_len)
            .ok_or_else(|| paro_error::data_corrupted("GraphLinks payload range overflow"))?;
        if payload_end > bytes.len() {
            return Err(paro_error::data_corrupted(format!(
                "GraphLinks legacy data truncated: need {} bytes, got {} bytes",
                payload_end,
                bytes.len()
            )));
        }

        let mut offsets = Vec::with_capacity(point_count);
        for i in 0..point_count {
            let start = 8 + i * 8;
            let offset = Self::read_u64(bytes, start, "legacy offset")?;
            let offset = usize::try_from(offset)
                .map_err(|_| paro_error::data_corrupted("GraphLinks legacy offset overflow"))?;
            offsets.push(offset);
        }
        Self::validate_legacy_offsets(&offsets, payload_len_u32)?;

        Ok(ParsedLayout {
            encoding: GraphLinksEncoding::LegacyU32,
            payload_offset,
            payload_len,
            offsets,
        })
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
            encoding: GraphLinksEncoding::CompressedVarintDeltaV1,
            payload_offset,
            payload_len,
            offsets,
        })
    }

    fn parse_layout(bytes: &[u8]) -> Result<ParsedLayout> {
        if bytes.len() < 8 {
            return Err(paro_error::data_corrupted(
                "GraphLinks file too small for header",
            ));
        }
        let marker = Self::read_u32(bytes, 0, "marker")?;
        if marker == GRAPH_LINKS_MAGIC {
            Self::parse_compressed_v1_layout(bytes)
        } else {
            Self::parse_legacy_layout(bytes, marker as usize)
        }
    }

    /// Save graph links to a writer using explicit wire format.
    pub fn serialize_with_format<W: Write>(
        &self,
        mut writer: W,
        format: GraphLinksWriteFormat,
    ) -> Result<()> {
        let bytes = self.serialize_bytes_for_format(format)?;
        writer.write_all(&bytes)?;
        Ok(())
    }

    /// Save graph links to a writer.
    ///
    /// Default is compressed v1. If compressed serialization fails, it falls back
    /// to legacy format for rollback safety.
    pub fn serialize<W: Write>(&self, mut writer: W) -> Result<()> {
        let preferred = GraphLinksWriteFormat::from_env();
        match self.serialize_bytes_for_format(preferred) {
            Ok(bytes) => {
                writer.write_all(&bytes)?;
                Ok(())
            }
            Err(_) if preferred == GraphLinksWriteFormat::CompressedV1 => {
                let bytes = self.serialize_bytes_for_format(GraphLinksWriteFormat::Legacy)?;
                writer.write_all(&bytes)?;
                Ok(())
            }
            Err(err) => Err(err),
        }
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

        Ok(Self {
            data: GraphLinksData::Ram(payload),
            encoding: layout.encoding,
            data_offset_bytes: 0,
            data_len_bytes: layout.payload_len,
            offsets: layout.offsets,
        })
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

        Ok(Self {
            data: GraphLinksData::Mmap(mmap),
            encoding: layout.encoding,
            data_offset_bytes: layout.payload_offset,
            data_len_bytes: layout.payload_len,
            offsets: layout.offsets,
        })
    }

    pub fn num_points(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_mmap_backed(&self) -> bool {
        matches!(self.data, GraphLinksData::Mmap(_))
    }

    /// Serialized size in bytes (as produced by `serialize`).
    pub fn serialized_size_bytes(&self) -> u64 {
        let preferred = GraphLinksWriteFormat::from_env();
        if let Ok(bytes) = self.serialize_bytes_for_format(preferred) {
            return bytes.len() as u64;
        }
        self.serialize_bytes_for_format(GraphLinksWriteFormat::Legacy)
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
    fn deserialize_compat_with_existing_binary_layout() {
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut bytes = Vec::new();
        links
            .serialize_with_format(&mut bytes, GraphLinksWriteFormat::Legacy)
            .unwrap();

        let restored = GraphLinks::deserialize(bytes.as_slice()).unwrap();
        assert_eq!(restored.num_points(), 4);
        assert_eq!(collect_links(&restored, 0, 0), vec![2, 1]);
        assert_eq!(collect_links(&restored, 0, 1), vec![3]);
        assert_eq!(collect_links(&restored, 2, 1), vec![0]);
    }

    #[test]
    fn compressed_roundtrip_uses_versioned_header_and_sorted_delta() {
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut bytes = Vec::new();
        links
            .serialize_with_format(&mut bytes, GraphLinksWriteFormat::CompressedV1)
            .unwrap();

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
        links
            .serialize_with_format(&mut bytes, GraphLinksWriteFormat::CompressedV1)
            .unwrap();

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
    fn explicit_legacy_serialize_supports_rollback_path() {
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut bytes = Vec::new();
        links
            .serialize_with_format(&mut bytes, GraphLinksWriteFormat::Legacy)
            .unwrap();
        let marker = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        assert_ne!(marker, GRAPH_LINKS_MAGIC);
    }

    #[test]
    fn mmap_load_matches_ram_load() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("graph_links.bin");
        let links = GraphLinks::new_from_edges(sample_edges());
        let mut file = File::create(&file_path).unwrap();
        links
            .serialize_with_format(&mut file, GraphLinksWriteFormat::CompressedV1)
            .unwrap();
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
