// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Vertex key/local-id/rowid mapping for graph projection indexes.

use paro_common::error as paro_error;
use paro_common::error::Result;
use std::collections::HashMap;
use std::io::{Read, Write};

const VERTEX_ID_MAP_MAGIC: u32 = u32::from_le_bytes(*b"VMAP");
const VERTEX_ID_MAP_VERSION_V1: u32 = 1;
const VERTEX_ID_MAP_KEY_ENCODING_INT64_ONLY: u32 = 1;
const VERTEX_ID_MAP_KEY_ENCODING_STRING_UTF8: u32 = 2;
const VERTEX_ID_MAP_KEY_ENCODING_COMPOSITE_BYTES: u32 = 3;
const VERTEX_ID_MAP_HEADER_LEN: usize = 32;

/// Dense local vertex id used by graph adjacency.
pub type LocalVertexId = u32;

/// Vertex key representation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VertexKey {
    Int64(i64),
    String(Box<str>),
    Composite(Box<[u8]>),
}

/// Bidirectional mapping between vertex key/local-id/rowid.
#[derive(Debug, Clone, Default)]
pub struct VertexIdMap {
    /// vertex primary key → local dense id
    key_to_local: HashMap<VertexKey, LocalVertexId>,
    /// local dense id → rowid
    local_to_rowid: Vec<u64>,
    /// rowid → local dense id
    rowid_to_local: HashMap<u64, LocalVertexId>,
    /// local dense id → vertex key
    local_to_key: Vec<VertexKey>,
    num_vertices: u32,
}

impl VertexIdMap {
    /// Build map from `(vertex_key, rowid)` in local-id order.
    pub fn build(keys_and_rowids: Vec<(VertexKey, u64)>) -> Self {
        let mut local_to_key = Vec::with_capacity(keys_and_rowids.len());
        let mut local_to_rowid = Vec::with_capacity(keys_and_rowids.len());

        for (key, rowid) in keys_and_rowids {
            local_to_key.push(key);
            local_to_rowid.push(rowid);
        }

        Self::from_local_vectors(local_to_key, local_to_rowid)
            .expect("VertexIdMap::build produced invalid internal state")
    }

    /// Lookup local id by vertex key.
    pub fn key_to_local(&self, key: &VertexKey) -> Option<LocalVertexId> {
        self.key_to_local.get(key).copied()
    }

    /// Lookup rowid by local id.
    pub fn local_to_rowid(&self, local_id: LocalVertexId) -> u64 {
        self.local_to_rowid
            .get(local_id as usize)
            .copied()
            .unwrap_or(0)
    }

    /// Dense local-id to rowid storage.
    pub fn local_to_rowids(&self) -> &[u64] {
        &self.local_to_rowid
    }

    /// Batch lookup rowids by local ids.
    #[inline]
    pub fn batch_local_to_rowid(&self, local_ids: &[u32]) -> Vec<u64> {
        let storage = &self.local_to_rowid;
        local_ids
            .iter()
            .map(|&lid| storage.get(lid as usize).copied().unwrap_or(0))
            .collect()
    }

    /// Lookup local id by rowid.
    pub fn rowid_to_local(&self, rowid: u64) -> Option<LocalVertexId> {
        self.rowid_to_local.get(&rowid).copied()
    }

    /// Number of vertices in this map.
    pub fn num_vertices(&self) -> u32 {
        self.num_vertices
    }

    /// Estimated in-memory footprint in bytes.
    pub fn memory_usage(&self) -> usize {
        let mut usage = std::mem::size_of::<Self>();
        usage = usage.saturating_add(
            self.local_to_rowid
                .capacity()
                .saturating_mul(std::mem::size_of::<u64>()),
        );
        usage = usage.saturating_add(
            self.local_to_key
                .iter()
                .map(vertex_key_heap_bytes)
                .sum::<usize>(),
        );
        usage = usage.saturating_add(
            self.local_to_key
                .capacity()
                .saturating_mul(std::mem::size_of::<VertexKey>()),
        );
        usage = usage.saturating_add(
            self.key_to_local
                .capacity()
                .saturating_mul(std::mem::size_of::<(VertexKey, LocalVertexId)>()),
        );
        usage = usage.saturating_add(
            self.rowid_to_local
                .capacity()
                .saturating_mul(std::mem::size_of::<(u64, LocalVertexId)>()),
        );
        usage
    }

    /// Serialize map to writer.
    ///
    /// Layout:
    /// `header | encoded_keys | rowid_u64[]`
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        let count = self.num_vertices as usize;
        if self.local_to_key.len() != count || self.local_to_rowid.len() != count {
            return Err(paro_error::invalid_input(
                "VertexIdMap: inconsistent internal vectors",
            ));
        }

        let key_encoding = detect_key_encoding(&self.local_to_key)?;
        let keys_bytes = encoded_keys_len(&self.local_to_key, key_encoding)?;
        let rowids_bytes = usize_mul_to_u64(count, std::mem::size_of::<u64>())?;

        writer.write_all(&VERTEX_ID_MAP_MAGIC.to_le_bytes())?;
        writer.write_all(&VERTEX_ID_MAP_VERSION_V1.to_le_bytes())?;
        writer.write_all(&key_encoding.to_le_bytes())?;
        writer.write_all(&self.num_vertices.to_le_bytes())?;
        writer.write_all(&keys_bytes.to_le_bytes())?;
        writer.write_all(&rowids_bytes.to_le_bytes())?;

        match key_encoding {
            VERTEX_ID_MAP_KEY_ENCODING_INT64_ONLY => {
                for key in &self.local_to_key {
                    let VertexKey::Int64(value) = key else {
                        return Err(paro_error::invalid_input(
                            "VertexIdMap: mixed key encodings are not supported",
                        ));
                    };
                    writer.write_all(&value.to_le_bytes())?;
                }
            }
            VERTEX_ID_MAP_KEY_ENCODING_STRING_UTF8 => {
                for key in &self.local_to_key {
                    let VertexKey::String(value) = key else {
                        return Err(paro_error::invalid_input(
                            "VertexIdMap: mixed key encodings are not supported",
                        ));
                    };
                    let len = u32::try_from(value.len()).map_err(|_| {
                        paro_error::out_of_range("VertexIdMap: string key length overflow")
                    })?;
                    writer.write_all(&len.to_le_bytes())?;
                    writer.write_all(value.as_bytes())?;
                }
            }
            VERTEX_ID_MAP_KEY_ENCODING_COMPOSITE_BYTES => {
                for key in &self.local_to_key {
                    let VertexKey::Composite(value) = key else {
                        return Err(paro_error::invalid_input(
                            "VertexIdMap: mixed key encodings are not supported",
                        ));
                    };
                    let len = u32::try_from(value.len()).map_err(|_| {
                        paro_error::out_of_range("VertexIdMap: composite key length overflow")
                    })?;
                    writer.write_all(&len.to_le_bytes())?;
                    writer.write_all(value.as_ref())?;
                }
            }
            _ => {
                return Err(paro_error::invalid_input(format!(
                    "VertexIdMap: unsupported key encoding {}",
                    key_encoding
                )));
            }
        }

        for rowid in &self.local_to_rowid {
            writer.write_all(&rowid.to_le_bytes())?;
        }
        Ok(())
    }

    /// Deserialize map from reader.
    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Self::from_serialized_bytes(&bytes)
    }

    fn from_local_vectors(local_to_key: Vec<VertexKey>, local_to_rowid: Vec<u64>) -> Result<Self> {
        if local_to_key.len() != local_to_rowid.len() {
            return Err(paro_error::invalid_input(format!(
                "VertexIdMap: key/rowid length mismatch: keys={} rowids={}",
                local_to_key.len(),
                local_to_rowid.len()
            )));
        }

        let num_vertices = u32::try_from(local_to_key.len())
            .map_err(|_| paro_error::out_of_range("VertexIdMap: num_vertices overflow"))?;
        let key_encoding = detect_key_encoding(&local_to_key)?;

        let mut key_to_local = HashMap::with_capacity(local_to_key.len());
        let mut rowid_to_local = HashMap::with_capacity(local_to_rowid.len());

        for (idx, key) in local_to_key.iter().enumerate() {
            validate_key_encoding(key, key_encoding)?;
            let local_id = idx as LocalVertexId;

            if key_to_local.insert(key.clone(), local_id).is_some() {
                return Err(paro_error::invalid_input(format!(
                    "VertexIdMap: duplicate vertex key at local_id={}",
                    local_id
                )));
            }

            let rowid = local_to_rowid[idx];
            if rowid_to_local.insert(rowid, local_id).is_some() {
                return Err(paro_error::invalid_input(format!(
                    "VertexIdMap: duplicate rowid={} in mapping",
                    rowid
                )));
            }
        }

        Ok(Self {
            key_to_local,
            local_to_rowid,
            rowid_to_local,
            local_to_key,
            num_vertices,
        })
    }

    fn from_serialized_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < VERTEX_ID_MAP_HEADER_LEN {
            return Err(paro_error::data_corrupted(
                "VertexIdMap: file too small for header",
            ));
        }

        let magic = read_u32(bytes, 0, "magic")?;
        if magic != VERTEX_ID_MAP_MAGIC {
            return Err(paro_error::data_corrupted(format!(
                "VertexIdMap: invalid magic {:08X} (expected {:08X})",
                magic, VERTEX_ID_MAP_MAGIC
            )));
        }

        let version = read_u32(bytes, 4, "version")?;
        if version != VERTEX_ID_MAP_VERSION_V1 {
            return Err(paro_error::data_corrupted(format!(
                "VertexIdMap: unsupported version {} (expected {})",
                version, VERTEX_ID_MAP_VERSION_V1
            )));
        }

        let key_encoding = read_u32(bytes, 8, "key_encoding")?;
        if key_encoding != VERTEX_ID_MAP_KEY_ENCODING_INT64_ONLY
            && key_encoding != VERTEX_ID_MAP_KEY_ENCODING_STRING_UTF8
            && key_encoding != VERTEX_ID_MAP_KEY_ENCODING_COMPOSITE_BYTES
        {
            return Err(paro_error::data_corrupted(format!(
                "VertexIdMap: unsupported key_encoding {}",
                key_encoding
            )));
        }

        let num_vertices = read_u32(bytes, 12, "num_vertices")? as usize;
        let keys_len = usize_from_u64(read_u64(bytes, 16, "keys_len")?, "keys_len")?;
        let rowids_len = usize_from_u64(read_u64(bytes, 24, "rowids_len")?, "rowids_len")?;

        if key_encoding == VERTEX_ID_MAP_KEY_ENCODING_INT64_ONLY {
            let expected_keys_len = usize_mul(
                num_vertices,
                std::mem::size_of::<i64>(),
                "expected_keys_len",
            )?;
            if keys_len != expected_keys_len {
                return Err(paro_error::data_corrupted(format!(
                    "VertexIdMap: keys length mismatch: expected {} got {}",
                    expected_keys_len, keys_len
                )));
            }
        }

        let expected_rowids_len = usize_mul(
            num_vertices,
            std::mem::size_of::<u64>(),
            "expected_rowids_len",
        )?;
        if rowids_len != expected_rowids_len {
            return Err(paro_error::data_corrupted(format!(
                "VertexIdMap: rowids length mismatch: expected {} got {}",
                expected_rowids_len, rowids_len
            )));
        }

        let keys_start = VERTEX_ID_MAP_HEADER_LEN;
        let rowids_start = keys_start
            .checked_add(keys_len)
            .ok_or_else(|| paro_error::data_corrupted("VertexIdMap: rowids start overflow"))?;
        let total_len = rowids_start
            .checked_add(rowids_len)
            .ok_or_else(|| paro_error::data_corrupted("VertexIdMap: payload length overflow"))?;
        if total_len > bytes.len() {
            return Err(paro_error::data_corrupted(format!(
                "VertexIdMap: truncated payload, need {} bytes got {} bytes",
                total_len,
                bytes.len()
            )));
        }

        let keys_slice = &bytes[keys_start..keys_start + keys_len];
        let rowids_slice = &bytes[rowids_start..rowids_start + rowids_len];

        let mut local_to_key = Vec::with_capacity(num_vertices);
        match key_encoding {
            VERTEX_ID_MAP_KEY_ENCODING_INT64_ONLY => {
                for chunk in keys_slice.chunks_exact(std::mem::size_of::<i64>()) {
                    let value = i64::from_le_bytes(chunk.try_into().unwrap());
                    local_to_key.push(VertexKey::Int64(value));
                }
            }
            VERTEX_ID_MAP_KEY_ENCODING_STRING_UTF8 => {
                let mut offset = 0usize;
                while offset < keys_slice.len() {
                    let len = read_u32(keys_slice, offset, "string_key_len")? as usize;
                    offset = offset.checked_add(4).ok_or_else(|| {
                        paro_error::data_corrupted("VertexIdMap: string key offset overflow")
                    })?;
                    let end = offset.checked_add(len).ok_or_else(|| {
                        paro_error::data_corrupted("VertexIdMap: string key end overflow")
                    })?;
                    let raw = keys_slice.get(offset..end).ok_or_else(|| {
                        paro_error::data_corrupted("VertexIdMap: truncated string key payload")
                    })?;
                    let value = std::str::from_utf8(raw).map_err(|err| {
                        paro_error::data_corrupted(format!(
                            "VertexIdMap: invalid UTF-8 in string key: {}",
                            err
                        ))
                    })?;
                    local_to_key.push(VertexKey::String(value.into()));
                    offset = end;
                }
                if local_to_key.len() != num_vertices {
                    return Err(paro_error::data_corrupted(format!(
                        "VertexIdMap: decoded {} string keys but expected {}",
                        local_to_key.len(),
                        num_vertices
                    )));
                }
            }
            VERTEX_ID_MAP_KEY_ENCODING_COMPOSITE_BYTES => {
                let mut offset = 0usize;
                while offset < keys_slice.len() {
                    let len = read_u32(keys_slice, offset, "composite_key_len")? as usize;
                    offset = offset.checked_add(4).ok_or_else(|| {
                        paro_error::data_corrupted("VertexIdMap: composite key offset overflow")
                    })?;
                    let end = offset.checked_add(len).ok_or_else(|| {
                        paro_error::data_corrupted("VertexIdMap: composite key end overflow")
                    })?;
                    let raw = keys_slice.get(offset..end).ok_or_else(|| {
                        paro_error::data_corrupted("VertexIdMap: truncated composite key payload")
                    })?;
                    local_to_key.push(VertexKey::Composite(raw.into()));
                    offset = end;
                }
                if local_to_key.len() != num_vertices {
                    return Err(paro_error::data_corrupted(format!(
                        "VertexIdMap: decoded {} composite keys but expected {}",
                        local_to_key.len(),
                        num_vertices
                    )));
                }
            }
            _ => unreachable!(),
        }

        let mut local_to_rowid = Vec::with_capacity(num_vertices);
        for chunk in rowids_slice.chunks_exact(std::mem::size_of::<u64>()) {
            let value = u64::from_le_bytes(chunk.try_into().unwrap());
            local_to_rowid.push(value);
        }

        Self::from_local_vectors(local_to_key, local_to_rowid)
    }
}

fn read_u32(bytes: &[u8], start: usize, field: &str) -> Result<u32> {
    let end = start
        .checked_add(4)
        .ok_or_else(|| paro_error::data_corrupted("VertexIdMap: header overflow"))?;
    let raw = bytes
        .get(start..end)
        .ok_or_else(|| paro_error::data_corrupted(format!("VertexIdMap: missing {}", field)))?;
    Ok(u32::from_le_bytes(raw.try_into().unwrap()))
}

fn read_u64(bytes: &[u8], start: usize, field: &str) -> Result<u64> {
    let end = start
        .checked_add(8)
        .ok_or_else(|| paro_error::data_corrupted("VertexIdMap: header overflow"))?;
    let raw = bytes
        .get(start..end)
        .ok_or_else(|| paro_error::data_corrupted(format!("VertexIdMap: missing {}", field)))?;
    Ok(u64::from_le_bytes(raw.try_into().unwrap()))
}

fn usize_from_u64(value: u64, field: &str) -> Result<usize> {
    usize::try_from(value)
        .map_err(|_| paro_error::out_of_range(format!("VertexIdMap: {} overflow", field)))
}

fn usize_mul(lhs: usize, rhs: usize, field: &str) -> Result<usize> {
    lhs.checked_mul(rhs)
        .ok_or_else(|| paro_error::out_of_range(format!("VertexIdMap: {} overflow", field)))
}

fn usize_mul_to_u64(lhs: usize, rhs: usize) -> Result<u64> {
    let bytes = usize_mul(lhs, rhs, "byte length")?;
    u64::try_from(bytes).map_err(|_| paro_error::out_of_range("VertexIdMap: byte length overflow"))
}

fn vertex_key_heap_bytes(key: &VertexKey) -> usize {
    match key {
        VertexKey::Int64(_) => 0,
        VertexKey::String(value) => value.len(),
        VertexKey::Composite(value) => value.len(),
    }
}

fn detect_key_encoding(keys: &[VertexKey]) -> Result<u32> {
    let mut encoding = None;
    for key in keys {
        let current = match key {
            VertexKey::Int64(_) => VERTEX_ID_MAP_KEY_ENCODING_INT64_ONLY,
            VertexKey::String(_) => VERTEX_ID_MAP_KEY_ENCODING_STRING_UTF8,
            VertexKey::Composite(_) => VERTEX_ID_MAP_KEY_ENCODING_COMPOSITE_BYTES,
        };
        if let Some(existing) = encoding {
            if existing != current {
                return Err(paro_error::invalid_input(
                    "VertexIdMap: mixed key encodings are not supported",
                ));
            }
        } else {
            encoding = Some(current);
        }
    }
    Ok(encoding.unwrap_or(VERTEX_ID_MAP_KEY_ENCODING_INT64_ONLY))
}

fn validate_key_encoding(key: &VertexKey, encoding: u32) -> Result<()> {
    match (encoding, key) {
        (VERTEX_ID_MAP_KEY_ENCODING_INT64_ONLY, VertexKey::Int64(_))
        | (VERTEX_ID_MAP_KEY_ENCODING_STRING_UTF8, VertexKey::String(_))
        | (VERTEX_ID_MAP_KEY_ENCODING_COMPOSITE_BYTES, VertexKey::Composite(_)) => Ok(()),
        (VERTEX_ID_MAP_KEY_ENCODING_INT64_ONLY, _)
        | (VERTEX_ID_MAP_KEY_ENCODING_STRING_UTF8, _)
        | (VERTEX_ID_MAP_KEY_ENCODING_COMPOSITE_BYTES, _) => Err(paro_error::invalid_input(
            "VertexIdMap: mixed key encodings are not supported",
        )),
        _ => Err(paro_error::invalid_input(format!(
            "VertexIdMap: unsupported key encoding {}",
            encoding
        ))),
    }
}

fn encoded_keys_len(keys: &[VertexKey], encoding: u32) -> Result<u64> {
    match encoding {
        VERTEX_ID_MAP_KEY_ENCODING_INT64_ONLY => {
            usize_mul_to_u64(keys.len(), std::mem::size_of::<i64>())
        }
        VERTEX_ID_MAP_KEY_ENCODING_STRING_UTF8 => {
            let bytes = keys.iter().try_fold(0usize, |acc, key| match key {
                VertexKey::String(value) => acc
                    .checked_add(4)
                    .and_then(|sum| sum.checked_add(value.len()))
                    .ok_or_else(|| {
                        paro_error::out_of_range("VertexIdMap: string key bytes overflow")
                    }),
                _ => Err(paro_error::invalid_input(
                    "VertexIdMap: mixed key encodings are not supported",
                )),
            })?;
            u64::try_from(bytes)
                .map_err(|_| paro_error::out_of_range("VertexIdMap: key bytes overflow"))
        }
        VERTEX_ID_MAP_KEY_ENCODING_COMPOSITE_BYTES => {
            let bytes = keys.iter().try_fold(0usize, |acc, key| match key {
                VertexKey::Composite(value) => acc
                    .checked_add(4)
                    .and_then(|sum| sum.checked_add(value.len()))
                    .ok_or_else(|| {
                        paro_error::out_of_range("VertexIdMap: composite key bytes overflow")
                    }),
                _ => Err(paro_error::invalid_input(
                    "VertexIdMap: mixed key encodings are not supported",
                )),
            })?;
            u64::try_from(bytes)
                .map_err(|_| paro_error::out_of_range("VertexIdMap: key bytes overflow"))
        }
        _ => Err(paro_error::invalid_input(format!(
            "VertexIdMap: unsupported key encoding {}",
            encoding
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn empty_mapping() {
        let map = VertexIdMap::build(Vec::new());
        assert_eq!(map.num_vertices(), 0);
        assert_eq!(map.key_to_local(&VertexKey::Int64(1)), None);
        assert_eq!(map.rowid_to_local(1), None);
    }

    #[test]
    fn single_vertex_mapping() {
        let map = VertexIdMap::build(vec![(VertexKey::Int64(42), 1001)]);
        assert_eq!(map.num_vertices(), 1);
        assert_eq!(map.key_to_local(&VertexKey::Int64(42)), Some(0));
        assert_eq!(map.local_to_rowid(0), 1001);
        assert_eq!(map.rowid_to_local(1001), Some(0));
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let map = VertexIdMap::build(vec![
            (VertexKey::Int64(1), 100),
            (VertexKey::Int64(2), 200),
            (VertexKey::Int64(3), 300),
        ]);

        let mut bytes = Vec::new();
        map.serialize(&mut bytes).unwrap();

        let mut cursor = Cursor::new(bytes);
        let restored = VertexIdMap::deserialize(&mut cursor).unwrap();
        assert_eq!(restored.num_vertices(), 3);
        assert_eq!(restored.key_to_local(&VertexKey::Int64(1)), Some(0));
        assert_eq!(restored.key_to_local(&VertexKey::Int64(2)), Some(1));
        assert_eq!(restored.key_to_local(&VertexKey::Int64(3)), Some(2));
        assert_eq!(restored.local_to_rowid(0), 100);
        assert_eq!(restored.local_to_rowid(1), 200);
        assert_eq!(restored.local_to_rowid(2), 300);
        assert_eq!(restored.rowid_to_local(100), Some(0));
        assert_eq!(restored.rowid_to_local(200), Some(1));
        assert_eq!(restored.rowid_to_local(300), Some(2));
    }

    #[test]
    fn serialize_deserialize_string_roundtrip() {
        let map = VertexIdMap::build(vec![
            (VertexKey::String("alpha".into()), 100),
            (VertexKey::String("beta".into()), 200),
        ]);

        let mut bytes = Vec::new();
        map.serialize(&mut bytes).unwrap();

        let mut cursor = Cursor::new(bytes);
        let restored = VertexIdMap::deserialize(&mut cursor).unwrap();
        assert_eq!(
            restored.key_to_local(&VertexKey::String("alpha".into())),
            Some(0)
        );
        assert_eq!(
            restored.key_to_local(&VertexKey::String("beta".into())),
            Some(1)
        );
        assert_eq!(restored.local_to_rowid(1), 200);
    }

    #[test]
    fn serialize_deserialize_composite_roundtrip() {
        let map = VertexIdMap::build(vec![
            (VertexKey::Composite(vec![1, 2, 3].into_boxed_slice()), 100),
            (
                VertexKey::Composite(vec![4, 5, 6, 7].into_boxed_slice()),
                200,
            ),
        ]);

        let mut bytes = Vec::new();
        map.serialize(&mut bytes).unwrap();

        let mut cursor = Cursor::new(bytes);
        let restored = VertexIdMap::deserialize(&mut cursor).unwrap();
        assert_eq!(
            restored.key_to_local(&VertexKey::Composite(vec![1, 2, 3].into_boxed_slice())),
            Some(0)
        );
        assert_eq!(
            restored.key_to_local(&VertexKey::Composite(vec![4, 5, 6, 7].into_boxed_slice())),
            Some(1)
        );
        assert_eq!(restored.local_to_rowid(1), 200);
    }

    #[test]
    fn large_mapping_smoke() {
        let num_vertices = 1_000_000usize;
        let mut input = Vec::with_capacity(num_vertices);
        for i in 0..num_vertices {
            input.push((VertexKey::Int64(i as i64), i as u64 * 2 + 1));
        }

        let map = VertexIdMap::build(input);
        assert_eq!(map.num_vertices(), num_vertices as u32);
        assert_eq!(map.key_to_local(&VertexKey::Int64(0)), Some(0));
        assert_eq!(map.key_to_local(&VertexKey::Int64(123_456)), Some(123_456));
        assert_eq!(
            map.key_to_local(&VertexKey::Int64((num_vertices - 1) as i64)),
            Some((num_vertices - 1) as u32)
        );
        assert_eq!(map.rowid_to_local(1), Some(0));
        assert_eq!(map.rowid_to_local(246_913), Some(123_456));
    }

    #[test]
    fn batch_local_to_rowid_basic() {
        let map = VertexIdMap::build(vec![
            (VertexKey::Int64(10), 100),
            (VertexKey::Int64(20), 200),
            (VertexKey::Int64(30), 300),
        ]);
        let result = map.batch_local_to_rowid(&[0, 2, 1]);
        assert_eq!(result, vec![100, 300, 200]);
    }

    #[test]
    fn batch_local_to_rowid_out_of_range() {
        let map = VertexIdMap::build(vec![(VertexKey::Int64(1), 10)]);
        let result = map.batch_local_to_rowid(&[0, 5, 100]);
        assert_eq!(result, vec![10, 0, 0]);
    }

    #[test]
    fn batch_local_to_rowid_empty() {
        let map = VertexIdMap::build(vec![(VertexKey::Int64(1), 10)]);
        let result = map.batch_local_to_rowid(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn missing_key_returns_none() {
        let map = VertexIdMap::build(vec![
            (VertexKey::Int64(10), 100),
            (VertexKey::Int64(20), 200),
        ]);
        assert_eq!(map.key_to_local(&VertexKey::Int64(99)), None);
        assert_eq!(map.rowid_to_local(999), None);
    }

    #[test]
    fn build_rejects_mixed_key_encodings() {
        let err = VertexIdMap::from_local_vectors(
            vec![VertexKey::Int64(1), VertexKey::String("abc".into())],
            vec![1, 2],
        )
        .unwrap_err();
        assert!(err.to_string().contains("mixed key encodings"));
    }
}
