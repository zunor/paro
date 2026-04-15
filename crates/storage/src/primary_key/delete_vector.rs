//! DeleteVector - versioned per-segment delete bitmaps with persistence helpers.

use crate::metrics::storage_metrics;
use paro_common::error::{self as paro_error, Result};
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

const DELETE_VECTOR_MAGIC: [u8; 4] = *b"PDV2";
const DELETE_VECTOR_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Default)]
pub struct DeleteVector {
    version: i64,
    bitmap: RoaringBitmap,
}

impl DeleteVector {
    pub fn new() -> Self {
        Self::with_version(0)
    }

    pub fn with_version(version: i64) -> Self {
        Self {
            version,
            bitmap: RoaringBitmap::new(),
        }
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    pub fn set_version(&mut self, version: i64) {
        self.version = version;
    }

    pub fn bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    pub fn mark_deleted(&mut self, row_id: u32) {
        if self.bitmap.insert(row_id) {
            storage_metrics().inc_delete_vector_entries(1);
        }
    }

    pub fn extend<I>(&mut self, row_ids: I)
    where
        I: IntoIterator<Item = u32>,
    {
        for row_id in row_ids {
            self.mark_deleted(row_id);
        }
    }

    pub fn add_dels_as_new_version(&self, row_ids: &[u32], version: i64) -> Self {
        let mut next = self.clone();
        next.version = version;
        next.extend(row_ids.iter().copied());
        next
    }

    pub fn is_deleted(&self, row_id: u32) -> bool {
        self.bitmap.contains(row_id)
    }

    pub fn cardinality(&self) -> u64 {
        self.bitmap.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.bitmap.iter()
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.bitmap
            .serialize_into(&mut buf)
            .map_err(|e| paro_error::serialization_error(e.to_string()))?;
        Ok(buf)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        use std::io::Cursor;
        let mut cursor = Cursor::new(bytes);
        let bitmap = RoaringBitmap::deserialize_from(&mut cursor)
            .map_err(|e| paro_error::serialization_error(e.to_string()))?;
        Ok(Self { version: 0, bitmap })
    }

    pub fn save_to_dir(&self, dir: impl AsRef<Path>, segment_id: u32) -> Result<PathBuf> {
        let mut chain = VersionedDeleteVector::new();
        chain.insert_version(self.clone());
        chain.save_to_dir(dir, segment_id)
    }

    pub fn load_from_dir(dir: impl AsRef<Path>, segment_id: u32) -> Result<Option<Self>> {
        Ok(VersionedDeleteVector::load_from_dir(dir, segment_id)?
            .latest()
            .cloned())
    }

    pub fn load_from_dir_at_version(
        dir: impl AsRef<Path>,
        segment_id: u32,
        version: i64,
    ) -> Result<Option<Self>> {
        Ok(VersionedDeleteVector::load_from_dir(dir, segment_id)?.latest_at(version))
    }

    pub fn load_versioned_from_dir(
        dir: impl AsRef<Path>,
        segment_id: u32,
    ) -> Result<VersionedDeleteVector> {
        VersionedDeleteVector::load_from_dir(dir, segment_id)
    }

    pub fn file_name(segment_id: u32) -> String {
        format!("segment_{}.delvec", segment_id)
    }
}

#[derive(Debug, Clone, Default)]
pub struct VersionedDeleteVector {
    versions: Vec<DeleteVector>,
}

impl VersionedDeleteVector {
    pub fn new() -> Self {
        Self {
            versions: Vec::new(),
        }
    }

    pub fn from_versions(mut versions: Vec<DeleteVector>) -> Self {
        versions.sort_by_key(|dv| dv.version());
        versions.dedup_by_key(|dv| dv.version());
        Self { versions }
    }

    pub fn versions(&self) -> &[DeleteVector] {
        &self.versions
    }

    pub fn latest(&self) -> Option<&DeleteVector> {
        self.versions.last()
    }

    pub fn latest_at(&self, version: i64) -> Option<DeleteVector> {
        self.versions
            .iter()
            .rev()
            .find(|dv| dv.version() <= version)
            .cloned()
    }

    pub fn insert_version(&mut self, delete_vector: DeleteVector) {
        match self
            .versions
            .binary_search_by_key(&delete_vector.version(), |dv| dv.version())
        {
            Ok(idx) => self.versions[idx] = delete_vector,
            Err(idx) => self.versions.insert(idx, delete_vector),
        }
    }

    pub fn add_dels_as_new_version(&mut self, row_ids: &[u32], version: i64) -> DeleteVector {
        let base = self
            .latest_at(version)
            .or_else(|| self.latest().cloned())
            .unwrap_or_else(|| DeleteVector::with_version(version));
        let next = base.add_dels_as_new_version(row_ids, version);
        self.insert_version(next.clone());
        next
    }

    pub fn gc_versions_older_than(&mut self, min_visible_version: i64) -> usize {
        if self.versions.len() <= 1 {
            return 0;
        }

        let keep_anchor = self
            .versions
            .iter()
            .rposition(|dv| dv.version() <= min_visible_version);
        let mut retained = Vec::with_capacity(self.versions.len());

        for (idx, dv) in self.versions.iter().cloned().enumerate() {
            if Some(idx) == keep_anchor || dv.version() >= min_visible_version {
                retained.push(dv);
            }
        }

        if retained.is_empty() {
            if let Some(last) = self.versions.last().cloned() {
                retained.push(last);
            }
        }

        let removed = self.versions.len().saturating_sub(retained.len());
        self.versions = retained;
        removed
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&DELETE_VECTOR_MAGIC);
        buf.extend_from_slice(&DELETE_VECTOR_FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.versions.len() as u32).to_le_bytes());
        for delete_vector in &self.versions {
            let payload = delete_vector.to_bytes()?;
            buf.extend_from_slice(&delete_vector.version().to_le_bytes());
            buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buf.extend_from_slice(&payload);
        }
        Ok(buf)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 8 || bytes.get(0..4) != Some(&DELETE_VECTOR_MAGIC) {
            let mut legacy = DeleteVector::from_bytes(bytes)?;
            legacy.set_version(0);
            return Ok(Self::from_versions(vec![legacy]));
        }

        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        if version != DELETE_VECTOR_FORMAT_VERSION {
            return Err(paro_error::data_corrupted(format!(
                "unsupported delete vector format version {}",
                version
            )));
        }
        if bytes.len() < 12 {
            return Err(paro_error::data_corrupted(
                "delete vector file truncated before version count",
            ));
        }

        let count = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let mut offset = 12usize;
        let mut versions = Vec::with_capacity(count);
        for _ in 0..count {
            if offset + 12 > bytes.len() {
                return Err(paro_error::data_corrupted(
                    "delete vector file truncated while reading entry header",
                ));
            }
            let dv_version = i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let payload_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + payload_len > bytes.len() {
                return Err(paro_error::data_corrupted(
                    "delete vector file truncated while reading entry payload",
                ));
            }
            let mut delete_vector = DeleteVector::from_bytes(&bytes[offset..offset + payload_len])?;
            delete_vector.set_version(dv_version);
            versions.push(delete_vector);
            offset += payload_len;
        }
        Ok(Self::from_versions(versions))
    }

    pub fn save_to_dir(&self, dir: impl AsRef<Path>, segment_id: u32) -> Result<PathBuf> {
        let path = dir.as_ref().join(DeleteVector::file_name(segment_id));
        let bytes = self.to_bytes()?;
        fs::write(&path, bytes)
            .map_err(|e| paro_error::io_error(format!("write delvec {:?}: {}", path, e)))?;
        Ok(path)
    }

    pub fn load_from_dir(dir: impl AsRef<Path>, segment_id: u32) -> Result<Self> {
        let path = dir.as_ref().join(DeleteVector::file_name(segment_id));
        if !path.exists() {
            return Ok(Self::new());
        }
        let data = fs::read(&path)
            .map_err(|e| paro_error::io_error(format!("read delvec {:?}: {}", path, e)))?;
        Self::from_bytes(&data)
    }
}

/// Snapshot of delete vectors keyed by (rowset_id, segment_id).
#[derive(Debug, Default, Clone)]
pub struct DeleteVectorSnapshot {
    by_rowset: HashMap<u64, HashMap<u32, DeleteVector>>,
}

impl DeleteVectorSnapshot {
    pub fn new() -> Self {
        Self {
            by_rowset: HashMap::new(),
        }
    }

    pub fn insert(&mut self, rowset_id: u64, segment_id: u32, dv: DeleteVector) {
        self.by_rowset
            .entry(rowset_id)
            .or_default()
            .insert(segment_id, dv);
    }

    pub fn get(&self, rowset_id: u64, segment_id: u32) -> Option<&DeleteVector> {
        self.by_rowset
            .get(&rowset_id)
            .and_then(|m| m.get(&segment_id))
    }

    pub fn segments_in_rowset(&self, rowset_id: u64) -> usize {
        self.by_rowset.get(&rowset_id).map(|m| m.len()).unwrap_or(0)
    }

    pub fn total_deleted_rows(&self) -> u64 {
        self.by_rowset
            .values()
            .map(|m| m.values().map(|dv| dv.cardinality()).sum::<u64>())
            .sum()
    }

    pub fn total_delete_vectors(&self) -> u32 {
        self.by_rowset.values().map(|m| m.len() as u32).sum()
    }

    /// Apply stats to a RowsetMeta for the given rowset.
    pub fn apply_to_rowset_meta(&self, rowset_id: u64, meta: &mut crate::rowset::RowsetMeta) {
        if let Some(map) = self.by_rowset.get(&rowset_id) {
            let num_vectors = map.len() as u32;
            let num_deleted: u64 = map.values().map(|dv| dv.cardinality()).sum();
            meta.set_delete_info(num_vectors, num_deleted);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::storage_metrics;
    use tempfile::tempdir;

    #[test]
    fn mark_and_check() {
        storage_metrics().reset_for_tests();
        let mut dv = DeleteVector::with_version(7);
        dv.mark_deleted(5);
        assert!(dv.is_deleted(5));
        assert!(!dv.is_deleted(6));
        assert_eq!(dv.cardinality(), 1);
        assert_eq!(dv.version(), 7);
    }

    #[test]
    fn serialize_roundtrip() {
        storage_metrics().reset_for_tests();
        let mut dv = DeleteVector::new();
        dv.mark_deleted(1);
        dv.mark_deleted(1000);
        let bytes = dv.to_bytes().unwrap();
        let restored = DeleteVector::from_bytes(&bytes).unwrap();
        assert!(restored.is_deleted(1));
        assert!(restored.is_deleted(1000));
        assert_eq!(restored.cardinality(), 2);
    }

    #[test]
    fn save_and_load() {
        storage_metrics().reset_for_tests();
        let dir = tempdir().unwrap();
        let mut chain = VersionedDeleteVector::new();
        chain.add_dels_as_new_version(&[7, 8], 3);
        let path = chain.save_to_dir(dir.path(), 3).unwrap();
        assert!(path.exists());
        let loaded = DeleteVector::load_from_dir(dir.path(), 3).unwrap().unwrap();
        assert!(loaded.is_deleted(7));
        assert!(loaded.is_deleted(8));
        assert_eq!(loaded.version(), 3);
    }

    #[test]
    fn batch_delete_persistence_roundtrip() {
        storage_metrics().reset_for_tests();
        let dir = tempdir().unwrap();
        let mut chain = VersionedDeleteVector::new();

        let row_ids = [0u32, 1, 1, 7, 42, 1_024, 65_535];
        chain.add_dels_as_new_version(&row_ids, 9);

        let expected = vec![0u32, 1, 7, 42, 1_024, 65_535];
        let loaded = chain
            .save_to_dir(dir.path(), 9)
            .and_then(|_| DeleteVector::load_from_dir(dir.path(), 9))
            .unwrap()
            .expect("delete vector should exist");
        assert_eq!(loaded.cardinality(), expected.len() as u64);
        assert_eq!(loaded.iter().collect::<Vec<_>>(), expected);
        assert_eq!(loaded.version(), 9);
    }

    #[test]
    fn version_chain_selects_latest_visible_snapshot() {
        storage_metrics().reset_for_tests();
        let mut chain = VersionedDeleteVector::new();
        chain.add_dels_as_new_version(&[1, 2], 3);
        chain.add_dels_as_new_version(&[9], 5);

        let at_v3 = chain.latest_at(3).expect("snapshot at v3");
        assert!(at_v3.is_deleted(1));
        assert!(at_v3.is_deleted(2));
        assert!(!at_v3.is_deleted(9));

        let at_v5 = chain.latest_at(5).expect("snapshot at v5");
        assert!(at_v5.is_deleted(9));
        assert_eq!(at_v5.cardinality(), 3);
    }

    #[test]
    fn version_chain_gc_keeps_anchor_version() {
        storage_metrics().reset_for_tests();
        let mut chain = VersionedDeleteVector::new();
        chain.add_dels_as_new_version(&[1], 1);
        chain.add_dels_as_new_version(&[2], 3);
        chain.add_dels_as_new_version(&[3], 7);

        assert_eq!(chain.gc_versions_older_than(5), 1);
        let versions: Vec<i64> = chain.versions().iter().map(|dv| dv.version()).collect();
        assert_eq!(versions, vec![3, 7]);
    }

    #[test]
    fn version_chain_persistence_roundtrip_preserves_all_versions() {
        storage_metrics().reset_for_tests();
        let dir = tempdir().unwrap();
        let mut chain = VersionedDeleteVector::new();
        chain.add_dels_as_new_version(&[1, 2], 3);
        chain.add_dels_as_new_version(&[7], 5);
        chain.add_dels_as_new_version(&[11], 9);

        chain.save_to_dir(dir.path(), 42).unwrap();
        let loaded = VersionedDeleteVector::load_from_dir(dir.path(), 42).unwrap();
        let versions: Vec<i64> = loaded.versions().iter().map(|dv| dv.version()).collect();
        assert_eq!(versions, vec![3, 5, 9]);
        assert_eq!(
            loaded.latest_at(5).unwrap().iter().collect::<Vec<_>>(),
            vec![1, 2, 7]
        );
        assert_eq!(
            loaded.latest().unwrap().iter().collect::<Vec<_>>(),
            vec![1, 2, 7, 11]
        );
    }

    #[test]
    fn version_chain_latest_at_returns_none_before_first_version() {
        storage_metrics().reset_for_tests();
        let mut chain = VersionedDeleteVector::new();
        chain.add_dels_as_new_version(&[9], 4);
        assert!(chain.latest_at(3).is_none());
    }

    #[test]
    fn snapshot_counts() {
        storage_metrics().reset_for_tests();
        let mut snap = DeleteVectorSnapshot::new();
        let mut dv1 = DeleteVector::new();
        dv1.mark_deleted(1);
        let mut dv2 = DeleteVector::new();
        dv2.mark_deleted(10);
        dv2.mark_deleted(11);
        snap.insert(100, 0, dv1);
        snap.insert(100, 1, dv2);
        assert_eq!(snap.total_deleted_rows(), 3);
        assert_eq!(snap.total_delete_vectors(), 2);
        assert_eq!(snap.segments_in_rowset(100), 2);
        assert!(snap.get(100, 1).unwrap().is_deleted(11));
    }
}
