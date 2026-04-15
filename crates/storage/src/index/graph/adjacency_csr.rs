// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Compressed sparse row adjacency index for graph traversal.

use memmap2::Mmap;
use paro_common::error as paro_error;
use paro_common::error::Result;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

const ADJACENCY_CSR_MAGIC: u32 = u32::from_le_bytes(*b"ACSR");
const ADJACENCY_CSR_VERSION_V1: u32 = 1;
const ADJACENCY_CSR_HEADER_LEN: usize = 48;

/// Optional backing source for CSR payload.
#[derive(Debug)]
pub enum CSRData {
    Ram(Vec<u8>),
    Mmap(Arc<Mmap>),
}

impl Default for CSRData {
    fn default() -> Self {
        Self::Ram(Vec::new())
    }
}

#[derive(Debug, Clone, Copy)]
struct ParsedLayout {
    num_vertices: u32,
    num_edges: u64,
    offsets_start: usize,
    offsets_len: usize,
    neighbors_start: usize,
    neighbors_len: usize,
    edge_rowids_start: usize,
    edge_rowids_len: usize,
}

/// CSR adjacency structure used by SQL/PGQ graph expand.
///
/// Runtime query path stores offsets / neighbors / edge rowids in typed vectors
/// for O(1) random access and slice-based reads.
#[derive(Debug, Default)]
pub struct AdjacencyCSR {
    num_vertices: u32,
    num_edges: u64,
    offsets: Vec<u64>,
    neighbor_ids: Vec<u32>,
    edge_rowids: Vec<u64>,
    backing: CSRData,
}

impl AdjacencyCSR {
    /// Build CSR from `(src_local_id, dst_local_id, edge_rowid)` tuples.
    pub fn build(edges: &mut [(u32, u32, u64)], num_vertices: u32) -> Self {
        edges.sort_unstable_by(|lhs, rhs| (lhs.0, lhs.1, lhs.2).cmp(&(rhs.0, rhs.1, rhs.2)));

        let vertex_count = num_vertices as usize;
        let edge_count = edges.len();
        let mut offsets = vec![0u64; vertex_count + 1];

        for &(src, dst, _) in edges.iter() {
            assert!(
                src < num_vertices,
                "AdjacencyCSR::build: src_local_id {} out of range [0, {})",
                src,
                num_vertices
            );
            assert!(
                dst < num_vertices,
                "AdjacencyCSR::build: dst_local_id {} out of range [0, {})",
                dst,
                num_vertices
            );
            offsets[src as usize + 1] += 1;
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }

        let mut neighbor_ids = Vec::with_capacity(edge_count);
        let mut edge_rowids = Vec::with_capacity(edge_count);
        for &(_, dst, edge_rowid) in edges.iter() {
            neighbor_ids.push(dst);
            edge_rowids.push(edge_rowid);
        }

        Self::from_parts(
            num_vertices,
            offsets,
            neighbor_ids,
            edge_rowids,
            CSRData::default(),
        )
        .expect("AdjacencyCSR::build produced invalid internal state")
    }

    /// Build a reverse (CSC) adjacency from `(src, dst, edge_rowid)` tuples.
    ///
    /// This swaps source and destination so that the resulting CSR is indexed
    /// by the original destination vertex, enabling backward traversal.
    pub fn build_reverse(edges: &[(u32, u32, u64)], num_vertices: u32) -> Self {
        let mut reversed: Vec<(u32, u32, u64)> = edges
            .iter()
            .map(|&(src, dst, rowid)| (dst, src, rowid))
            .collect();
        Self::build(&mut reversed, num_vertices)
    }

    /// Number of vertices in this CSR.
    pub fn num_vertices(&self) -> u32 {
        self.num_vertices
    }

    /// Number of edges in this CSR.
    pub fn num_edges(&self) -> u64 {
        self.num_edges
    }

    /// Returns whether this instance was loaded by `load_mmap`.
    pub fn is_mmap_backed(&self) -> bool {
        matches!(self.backing, CSRData::Mmap(_))
    }

    /// Get neighbors of vertex `v`.
    pub fn neighbors(&self, v: u32) -> &[u32] {
        let Some((start, end)) = self.vertex_edge_range(v) else {
            return &[];
        };
        &self.neighbor_ids[start..end]
    }

    /// Get outgoing degree of vertex `v`.
    pub fn degree(&self, v: u32) -> u32 {
        let Some((start, end)) = self.vertex_edge_range(v) else {
            return 0;
        };
        (end - start) as u32
    }

    /// Get the `i`th outgoing edge rowid of vertex `v`.
    pub fn edge_rowid(&self, v: u32, i: u32) -> u64 {
        let Some((start, end)) = self.vertex_edge_range(v) else {
            return 0;
        };
        let idx = start + i as usize;
        if idx >= end {
            return 0;
        }
        self.edge_rowids[idx]
    }

    /// Get all outgoing edge rowids of vertex `v`.
    pub fn edge_rowids_for(&self, v: u32) -> &[u64] {
        let Some((start, end)) = self.vertex_edge_range(v) else {
            return &[];
        };
        &self.edge_rowids[start..end]
    }

    /// Serialize to writer.
    ///
    /// Layout:
    /// `header | offsets(u64[]) | neighbors(delta-varint) | edge_rowids(u64[])`
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        let neighbors_payload = self.encode_neighbors_payload()?;
        let offsets_bytes = usize_mul_to_u64(self.offsets.len(), std::mem::size_of::<u64>())?;
        let neighbor_bytes = u64::try_from(neighbors_payload.len())
            .map_err(|_| paro_error::out_of_range("AdjacencyCSR neighbors payload overflow"))?;
        let edge_rowids_bytes =
            usize_mul_to_u64(self.edge_rowids.len(), std::mem::size_of::<u64>())?;

        writer.write_all(&ADJACENCY_CSR_MAGIC.to_le_bytes())?;
        writer.write_all(&ADJACENCY_CSR_VERSION_V1.to_le_bytes())?;
        writer.write_all(&self.num_vertices.to_le_bytes())?;
        writer.write_all(&0u32.to_le_bytes())?; // reserved flags
        writer.write_all(&self.num_edges.to_le_bytes())?;
        writer.write_all(&offsets_bytes.to_le_bytes())?;
        writer.write_all(&neighbor_bytes.to_le_bytes())?;
        writer.write_all(&edge_rowids_bytes.to_le_bytes())?;

        for offset in &self.offsets {
            writer.write_all(&offset.to_le_bytes())?;
        }
        writer.write_all(&neighbors_payload)?;
        for edge_rowid in &self.edge_rowids {
            writer.write_all(&edge_rowid.to_le_bytes())?;
        }
        Ok(())
    }

    /// Deserialize from reader.
    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
        let mut serialized = Vec::new();
        reader.read_to_end(&mut serialized)?;
        Self::from_serialized_bytes(&serialized, CSRData::default())
    }

    /// Load and parse CSR from mmap-ed file.
    pub fn load_mmap(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        let mmap = Arc::new(mmap);
        Self::from_serialized_bytes(&mmap[..], CSRData::Mmap(Arc::clone(&mmap)))
    }

    /// Estimated in-memory footprint in bytes.
    pub fn memory_usage(&self) -> usize {
        let in_memory = self.offsets.len() * std::mem::size_of::<u64>()
            + self.neighbor_ids.len() * std::mem::size_of::<u32>()
            + self.edge_rowids.len() * std::mem::size_of::<u64>();
        match &self.backing {
            CSRData::Ram(data) => in_memory + data.len(),
            CSRData::Mmap(mmap) => in_memory + mmap.len(),
        }
    }

    fn from_serialized_bytes(serialized: &[u8], backing: CSRData) -> Result<Self> {
        let layout = Self::parse_layout(serialized)?;
        let offsets_bytes =
            &serialized[layout.offsets_start..layout.offsets_start + layout.offsets_len];
        let neighbors_bytes =
            &serialized[layout.neighbors_start..layout.neighbors_start + layout.neighbors_len];
        let edge_rowids_bytes = &serialized
            [layout.edge_rowids_start..layout.edge_rowids_start + layout.edge_rowids_len];

        let offsets = decode_u64_array(offsets_bytes, "offsets")?;
        let edge_rowids = decode_u64_array(edge_rowids_bytes, "edge_rowids")?;
        let neighbor_ids = Self::decode_neighbors_payload(
            layout.num_vertices,
            layout.num_edges,
            &offsets,
            neighbors_bytes,
        )?;

        Self::from_parts(
            layout.num_vertices,
            offsets,
            neighbor_ids,
            edge_rowids,
            backing,
        )
    }

    fn from_parts(
        num_vertices: u32,
        offsets: Vec<u64>,
        neighbor_ids: Vec<u32>,
        edge_rowids: Vec<u64>,
        backing: CSRData,
    ) -> Result<Self> {
        let num_edges = u64::try_from(neighbor_ids.len())
            .map_err(|_| paro_error::out_of_range("AdjacencyCSR num_edges overflow"))?;
        if edge_rowids.len() != neighbor_ids.len() {
            return Err(paro_error::invalid_input(format!(
                "AdjacencyCSR edge count mismatch: neighbors={} edge_rowids={}",
                neighbor_ids.len(),
                edge_rowids.len()
            )));
        }
        Self::validate_offsets(num_vertices, num_edges, &offsets)?;
        for (idx, &neighbor) in neighbor_ids.iter().enumerate() {
            if neighbor >= num_vertices {
                return Err(paro_error::invalid_input(format!(
                    "AdjacencyCSR neighbor out of range: idx={} neighbor={} num_vertices={}",
                    idx, neighbor, num_vertices
                )));
            }
        }

        Ok(Self {
            num_vertices,
            num_edges,
            offsets,
            neighbor_ids,
            edge_rowids,
            backing,
        })
    }

    fn parse_layout(serialized: &[u8]) -> Result<ParsedLayout> {
        if serialized.len() < ADJACENCY_CSR_HEADER_LEN {
            return Err(paro_error::data_corrupted(
                "AdjacencyCSR: file too small for header",
            ));
        }

        let magic = read_u32(serialized, 0, "magic")?;
        if magic != ADJACENCY_CSR_MAGIC {
            return Err(paro_error::data_corrupted(format!(
                "AdjacencyCSR: invalid magic {:08X} (expected {:08X})",
                magic, ADJACENCY_CSR_MAGIC
            )));
        }

        let version = read_u32(serialized, 4, "version")?;
        if version != ADJACENCY_CSR_VERSION_V1 {
            return Err(paro_error::data_corrupted(format!(
                "AdjacencyCSR: unsupported version {} (expected {})",
                version, ADJACENCY_CSR_VERSION_V1
            )));
        }

        let num_vertices = read_u32(serialized, 8, "num_vertices")?;
        let _reserved_flags = read_u32(serialized, 12, "reserved_flags")?;
        let num_edges = read_u64(serialized, 16, "num_edges")?;
        let offsets_len = usize_from_u64(read_u64(serialized, 24, "offsets_len")?, "offsets_len")?;
        let neighbors_len =
            usize_from_u64(read_u64(serialized, 32, "neighbors_len")?, "neighbors_len")?;
        let edge_rowids_len = usize_from_u64(
            read_u64(serialized, 40, "edge_rowids_len")?,
            "edge_rowids_len",
        )?;

        let expected_offsets_len = usize_mul(
            num_vertices as usize + 1,
            std::mem::size_of::<u64>(),
            "expected_offsets_len",
        )?;
        if offsets_len != expected_offsets_len {
            return Err(paro_error::data_corrupted(format!(
                "AdjacencyCSR: offsets length mismatch: expected {} got {}",
                expected_offsets_len, offsets_len
            )));
        }

        let expected_edge_rowids_len = usize_mul(
            usize_from_u64(num_edges, "num_edges")?,
            std::mem::size_of::<u64>(),
            "expected_edge_rowids_len",
        )?;
        if edge_rowids_len != expected_edge_rowids_len {
            return Err(paro_error::data_corrupted(format!(
                "AdjacencyCSR: edge_rowids length mismatch: expected {} got {}",
                expected_edge_rowids_len, edge_rowids_len
            )));
        }

        let offsets_start = ADJACENCY_CSR_HEADER_LEN;
        let neighbors_start = offsets_start
            .checked_add(offsets_len)
            .ok_or_else(|| paro_error::data_corrupted("AdjacencyCSR: neighbors start overflow"))?;
        let edge_rowids_start = neighbors_start.checked_add(neighbors_len).ok_or_else(|| {
            paro_error::data_corrupted("AdjacencyCSR: edge_rowids start overflow")
        })?;
        let total_len = edge_rowids_start
            .checked_add(edge_rowids_len)
            .ok_or_else(|| paro_error::data_corrupted("AdjacencyCSR: payload length overflow"))?;

        if total_len > serialized.len() {
            return Err(paro_error::data_corrupted(format!(
                "AdjacencyCSR: truncated file, need {} bytes but only {} available",
                total_len,
                serialized.len()
            )));
        }

        Ok(ParsedLayout {
            num_vertices,
            num_edges,
            offsets_start,
            offsets_len,
            neighbors_start,
            neighbors_len,
            edge_rowids_start,
            edge_rowids_len,
        })
    }

    fn validate_offsets(num_vertices: u32, num_edges: u64, offsets: &[u64]) -> Result<()> {
        let expected_len = num_vertices as usize + 1;
        if offsets.len() != expected_len {
            return Err(paro_error::invalid_input(format!(
                "AdjacencyCSR offsets length mismatch: expected {} got {}",
                expected_len,
                offsets.len()
            )));
        }
        if offsets.first().copied().unwrap_or(1) != 0 {
            return Err(paro_error::invalid_input(
                "AdjacencyCSR offsets must start with 0",
            ));
        }

        let mut prev = 0u64;
        for (idx, &value) in offsets.iter().enumerate() {
            if value < prev {
                return Err(paro_error::invalid_input(format!(
                    "AdjacencyCSR offsets not monotonic: idx={} prev={} current={}",
                    idx, prev, value
                )));
            }
            if value > num_edges {
                return Err(paro_error::invalid_input(format!(
                    "AdjacencyCSR offset out of range: idx={} offset={} num_edges={}",
                    idx, value, num_edges
                )));
            }
            prev = value;
        }

        if offsets.last().copied().unwrap_or(0) != num_edges {
            return Err(paro_error::invalid_input(format!(
                "AdjacencyCSR last offset mismatch: expected {} got {}",
                num_edges,
                offsets.last().copied().unwrap_or(0)
            )));
        }
        Ok(())
    }

    fn encode_neighbors_payload(&self) -> Result<Vec<u8>> {
        Self::validate_offsets(self.num_vertices, self.num_edges, &self.offsets)?;
        if self.neighbor_ids.len() != self.edge_rowids.len() {
            return Err(paro_error::invalid_input(format!(
                "AdjacencyCSR edge count mismatch: neighbors={} edge_rowids={}",
                self.neighbor_ids.len(),
                self.edge_rowids.len()
            )));
        }

        let mut payload = Vec::new();
        for vertex in 0..self.num_vertices as usize {
            let start = usize_from_u64(self.offsets[vertex], "offset start")?;
            let end = usize_from_u64(self.offsets[vertex + 1], "offset end")?;
            let mut prev_neighbor = 0u32;
            for edge_idx in start..end {
                let neighbor = *self.neighbor_ids.get(edge_idx).ok_or_else(|| {
                    paro_error::invalid_input(format!(
                        "AdjacencyCSR neighbor index out of range: {}",
                        edge_idx
                    ))
                })?;
                if neighbor >= self.num_vertices {
                    return Err(paro_error::invalid_input(format!(
                        "AdjacencyCSR neighbor out of range during encode: vertex={} neighbor={}",
                        vertex, neighbor
                    )));
                }

                let delta = if edge_idx == start {
                    u64::from(neighbor)
                } else {
                    if neighbor < prev_neighbor {
                        return Err(paro_error::invalid_input(format!(
                            "AdjacencyCSR neighbors must be sorted per vertex for delta encoding: vertex={} prev={} current={}",
                            vertex, prev_neighbor, neighbor
                        )));
                    }
                    u64::from(neighbor - prev_neighbor)
                };
                encode_varint(delta, &mut payload);
                prev_neighbor = neighbor;
            }
        }
        Ok(payload)
    }

    fn decode_neighbors_payload(
        num_vertices: u32,
        num_edges: u64,
        offsets: &[u64],
        payload: &[u8],
    ) -> Result<Vec<u32>> {
        Self::validate_offsets(num_vertices, num_edges, offsets)?;

        let edge_count = usize_from_u64(num_edges, "num_edges")?;
        let mut cursor = 0usize;
        let mut neighbors = Vec::with_capacity(edge_count);

        for vertex in 0..num_vertices as usize {
            let start = usize_from_u64(offsets[vertex], "offset start")?;
            let end = usize_from_u64(offsets[vertex + 1], "offset end")?;
            let mut prev_neighbor = 0u32;

            for edge_idx in start..end {
                let delta = decode_varint(payload, &mut cursor, "neighbor delta")?;
                if delta > u32::MAX as u64 {
                    return Err(paro_error::data_corrupted(format!(
                        "AdjacencyCSR: neighbor delta out of range at vertex={} edge_idx={} delta={}",
                        vertex, edge_idx, delta
                    )));
                }
                let delta = delta as u32;
                let neighbor = if edge_idx == start {
                    delta
                } else {
                    prev_neighbor.checked_add(delta).ok_or_else(|| {
                        paro_error::data_corrupted(format!(
                            "AdjacencyCSR: neighbor delta overflow at vertex={} edge_idx={}",
                            vertex, edge_idx
                        ))
                    })?
                };

                if neighbor >= num_vertices {
                    return Err(paro_error::data_corrupted(format!(
                        "AdjacencyCSR: neighbor out of range at vertex={} edge_idx={} neighbor={} num_vertices={}",
                        vertex, edge_idx, neighbor, num_vertices
                    )));
                }

                neighbors.push(neighbor);
                prev_neighbor = neighbor;
            }
        }

        if neighbors.len() != edge_count {
            return Err(paro_error::data_corrupted(format!(
                "AdjacencyCSR: decoded edge count mismatch: expected {} got {}",
                edge_count,
                neighbors.len()
            )));
        }
        if cursor != payload.len() {
            return Err(paro_error::data_corrupted(format!(
                "AdjacencyCSR: neighbors payload has trailing bytes: decoded={} payload={}",
                cursor,
                payload.len()
            )));
        }
        Ok(neighbors)
    }

    fn vertex_edge_range(&self, v: u32) -> Option<(usize, usize)> {
        if v >= self.num_vertices {
            return None;
        }
        let start = *self.offsets.get(v as usize)? as usize;
        let end = *self.offsets.get(v as usize + 1)? as usize;
        Some((start, end))
    }
}

fn decode_u64_array(bytes: &[u8], field: &str) -> Result<Vec<u64>> {
    if !bytes.len().is_multiple_of(std::mem::size_of::<u64>()) {
        return Err(paro_error::data_corrupted(format!(
            "AdjacencyCSR: {} length is not aligned to u64: {}",
            field,
            bytes.len()
        )));
    }

    let mut values = Vec::with_capacity(bytes.len() / std::mem::size_of::<u64>());
    for chunk in bytes.chunks_exact(std::mem::size_of::<u64>()) {
        values.push(u64::from_le_bytes(chunk.try_into().unwrap()));
    }
    Ok(values)
}

fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7F) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_varint(bytes: &[u8], cursor: &mut usize, context: &str) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;

    for _ in 0..10 {
        let byte = *bytes.get(*cursor).ok_or_else(|| {
            paro_error::data_corrupted(format!(
                "AdjacencyCSR: unexpected end of varint while reading {} at offset {}",
                context, cursor
            ))
        })?;
        *cursor += 1;

        let chunk = u64::from(byte & 0x7F);
        if shift == 63 && chunk > 1 {
            return Err(paro_error::data_corrupted(format!(
                "AdjacencyCSR: varint overflow while reading {} at offset {}",
                context, cursor
            )));
        }
        result |= chunk << shift;
        if (byte & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }

    Err(paro_error::data_corrupted(format!(
        "AdjacencyCSR: varint too long while reading {} at offset {}",
        context, cursor
    )))
}

fn read_u32(bytes: &[u8], start: usize, field: &str) -> Result<u32> {
    let end = start
        .checked_add(4)
        .ok_or_else(|| paro_error::data_corrupted("AdjacencyCSR: header overflow"))?;
    let raw = bytes
        .get(start..end)
        .ok_or_else(|| paro_error::data_corrupted(format!("AdjacencyCSR: missing {}", field)))?;
    Ok(u32::from_le_bytes(raw.try_into().unwrap()))
}

fn read_u64(bytes: &[u8], start: usize, field: &str) -> Result<u64> {
    let end = start
        .checked_add(8)
        .ok_or_else(|| paro_error::data_corrupted("AdjacencyCSR: header overflow"))?;
    let raw = bytes
        .get(start..end)
        .ok_or_else(|| paro_error::data_corrupted(format!("AdjacencyCSR: missing {}", field)))?;
    Ok(u64::from_le_bytes(raw.try_into().unwrap()))
}

fn usize_from_u64(value: u64, field: &str) -> Result<usize> {
    usize::try_from(value)
        .map_err(|_| paro_error::out_of_range(format!("AdjacencyCSR: {} overflow", field)))
}

fn usize_mul(lhs: usize, rhs: usize, field: &str) -> Result<usize> {
    lhs.checked_mul(rhs)
        .ok_or_else(|| paro_error::out_of_range(format!("AdjacencyCSR: {} overflow", field)))
}

fn usize_mul_to_u64(lhs: usize, rhs: usize) -> Result<u64> {
    let bytes = usize_mul(lhs, rhs, "byte length")?;
    u64::try_from(bytes).map_err(|_| paro_error::out_of_range("AdjacencyCSR: byte length overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::TempDir;

    fn assert_csr_equivalent(lhs: &AdjacencyCSR, rhs: &AdjacencyCSR) {
        assert_eq!(lhs.num_vertices(), rhs.num_vertices());
        assert_eq!(lhs.num_edges(), rhs.num_edges());
        for vertex in 0..lhs.num_vertices() {
            assert_eq!(lhs.degree(vertex), rhs.degree(vertex));
            assert_eq!(lhs.neighbors(vertex), rhs.neighbors(vertex));
            assert_eq!(lhs.edge_rowids_for(vertex), rhs.edge_rowids_for(vertex));
            for i in 0..lhs.degree(vertex) {
                assert_eq!(lhs.edge_rowid(vertex, i), rhs.edge_rowid(vertex, i));
            }
        }
    }

    #[test]
    fn build_empty_graph() {
        let mut edges: Vec<(u32, u32, u64)> = Vec::new();
        let csr = AdjacencyCSR::build(&mut edges, 0);

        assert_eq!(csr.num_vertices(), 0);
        assert_eq!(csr.num_edges(), 0);
        assert_eq!(csr.degree(0), 0);
        assert_eq!(csr.neighbors(0), &[] as &[u32]);
        assert_eq!(csr.edge_rowids_for(0), &[] as &[u64]);
    }

    #[test]
    fn build_single_vertex_graph() {
        let mut edges = vec![(0, 0, 7)];
        let csr = AdjacencyCSR::build(&mut edges, 1);

        assert_eq!(csr.num_vertices(), 1);
        assert_eq!(csr.num_edges(), 1);
        assert_eq!(csr.degree(0), 1);
        assert_eq!(csr.neighbors(0), &[0]);
        assert_eq!(csr.edge_rowids_for(0), &[7]);
        assert_eq!(csr.edge_rowid(0, 0), 7);
    }

    #[test]
    fn build_with_isolated_vertices() {
        let mut edges = vec![(0, 1, 11)];
        let csr = AdjacencyCSR::build(&mut edges, 4);

        assert_eq!(csr.degree(0), 1);
        assert_eq!(csr.degree(1), 0);
        assert_eq!(csr.degree(2), 0);
        assert_eq!(csr.degree(3), 0);
        assert_eq!(csr.neighbors(0), &[1]);
        assert_eq!(csr.neighbors(2), &[] as &[u32]);
    }

    #[test]
    fn build_with_self_loop_and_multi_edges() {
        let mut edges = vec![
            (2, 2, 200),
            (0, 1, 102),
            (0, 0, 100),
            (0, 0, 101),
            (1, 1, 150),
        ];
        let csr = AdjacencyCSR::build(&mut edges, 3);

        assert_eq!(csr.num_edges(), 5);
        assert_eq!(csr.neighbors(0), &[0, 0, 1]);
        assert_eq!(csr.edge_rowids_for(0), &[100, 101, 102]);
        assert_eq!(csr.neighbors(1), &[1]);
        assert_eq!(csr.edge_rowids_for(1), &[150]);
        assert_eq!(csr.neighbors(2), &[2]);
        assert_eq!(csr.edge_rowids_for(2), &[200]);
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let mut edges = vec![
            (0, 1, 10),
            (0, 3, 11),
            (1, 2, 20),
            (2, 2, 30),
            (3, 0, 40),
            (3, 1, 41),
        ];
        let csr = AdjacencyCSR::build(&mut edges, 5);

        let mut bytes = Vec::new();
        csr.serialize(&mut bytes).unwrap();

        let mut cursor = Cursor::new(bytes);
        let restored = AdjacencyCSR::deserialize(&mut cursor).unwrap();
        assert!(!restored.is_mmap_backed());
        assert_csr_equivalent(&csr, &restored);
    }

    #[test]
    fn mmap_load_matches_ram() {
        let mut edges = vec![
            (0, 1, 10),
            (0, 3, 11),
            (1, 2, 20),
            (2, 2, 30),
            (3, 0, 40),
            (3, 1, 41),
        ];
        let csr = AdjacencyCSR::build(&mut edges, 5);

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("graph.csr");
        {
            let mut file = File::create(&path).unwrap();
            csr.serialize(&mut file).unwrap();
        }

        let mut reader = File::open(&path).unwrap();
        let ram = AdjacencyCSR::deserialize(&mut reader).unwrap();
        let mmap = AdjacencyCSR::load_mmap(&path).unwrap();

        assert!(!ram.is_mmap_backed());
        assert!(mmap.is_mmap_backed());
        assert_csr_equivalent(&ram, &mmap);
    }

    #[test]
    fn large_graph_build_smoke() {
        let num_vertices = 100_000u32;
        let num_edges = 1_000_000usize;
        let mut edges = Vec::with_capacity(num_edges);
        for i in 0..num_edges {
            let src = (i % num_vertices as usize) as u32;
            let dst = ((i * 31 + 7) % num_vertices as usize) as u32;
            edges.push((src, dst, i as u64));
        }

        let csr = AdjacencyCSR::build(&mut edges, num_vertices);
        assert_eq!(csr.num_vertices(), num_vertices);
        assert_eq!(csr.num_edges(), num_edges as u64);
        assert!(csr.degree(7) > 0);
        assert_eq!(csr.neighbors(0).len() as u32, csr.degree(0));
    }

    #[test]
    fn build_reverse_swaps_src_dst() {
        // Forward: 0→1 (rowid 10), 0→2 (rowid 20), 1→2 (rowid 30)
        let edges = vec![(0u32, 1u32, 10u64), (0, 2, 20), (1, 2, 30)];
        let reverse = AdjacencyCSR::build_reverse(&edges, 3);

        assert_eq!(reverse.num_vertices(), 3);
        assert_eq!(reverse.num_edges(), 3);

        // Vertex 0 has no incoming edges in forward → no neighbors in reverse
        assert_eq!(reverse.neighbors(0), &[] as &[u32]);
        // Vertex 1 has incoming from 0 → reverse neighbors = [0]
        assert_eq!(reverse.neighbors(1), &[0]);
        assert_eq!(reverse.edge_rowids_for(1), &[10]);
        // Vertex 2 has incoming from 0 and 1 → reverse neighbors = [0, 1]
        assert_eq!(reverse.neighbors(2), &[0, 1]);
        assert_eq!(reverse.edge_rowids_for(2), &[20, 30]);
    }

    #[test]
    fn build_reverse_empty() {
        let edges: Vec<(u32, u32, u64)> = Vec::new();
        let reverse = AdjacencyCSR::build_reverse(&edges, 0);
        assert_eq!(reverse.num_vertices(), 0);
        assert_eq!(reverse.num_edges(), 0);
    }

    #[test]
    fn build_reverse_self_loop() {
        let edges = vec![(0u32, 0u32, 42u64)];
        let reverse = AdjacencyCSR::build_reverse(&edges, 1);
        // Self-loop stays the same after swap
        assert_eq!(reverse.neighbors(0), &[0]);
        assert_eq!(reverse.edge_rowids_for(0), &[42]);
    }
}
