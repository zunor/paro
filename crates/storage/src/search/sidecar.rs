// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use memmap2::Mmap;
use paro_common::error::{self as paro_error, Result};

use crate::metrics::storage_metrics;

use super::artifact::{ArtifactFileId, ArtifactLocation};
use super::capability::SearchIndexKind;
use super::stats::{SearchDefinitionId, SearchGenerationId};

static SIDECAR_STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(1);
pub const SIDECAR_PACKAGE_CODEC: &str = "scar-v1";

/// Search sidecar package store.
///
/// The long-lived identity is `ArtifactFileId`; physical paths stay behind this
/// store so manifest shards can keep referencing old packages after shard
/// compaction or repack.
#[derive(Debug, Clone)]
pub struct SidecarArtifactStore {
    table_data_dir: PathBuf,
}

impl SidecarArtifactStore {
    pub fn new(table_data_dir: impl Into<PathBuf>) -> Self {
        Self {
            table_data_dir: table_data_dir.into(),
        }
    }

    pub fn default_shard_file_id(
        definition_id: SearchDefinitionId,
        generation_id: SearchGenerationId,
    ) -> ArtifactFileId {
        ArtifactFileId {
            definition_id,
            generation_id,
            package_index: 0,
        }
    }

    pub fn package_relative_path(file_id: ArtifactFileId) -> PathBuf {
        PathBuf::from("search_registry")
            .join("definitions")
            .join(file_id.definition_id.to_string())
            .join("sidecars")
            .join(format!("g{}", file_id.generation_id))
            .join(format!("package_{}.scar", file_id.package_index))
    }

    pub fn package_path(&self, file_id: ArtifactFileId) -> PathBuf {
        self.table_data_dir
            .join(Self::package_relative_path(file_id))
    }

    pub fn remove_package(&self, file_id: ArtifactFileId) {
        let _ = fs::remove_file(self.package_path(file_id));
    }

    pub fn create_package_writer(&self, file_id: ArtifactFileId) -> Result<SidecarPackageWriter> {
        SidecarPackageWriter::create(self.table_data_dir.clone(), file_id)
    }

    pub fn create_next_package_writer(
        &self,
        definition_id: SearchDefinitionId,
        generation_id: SearchGenerationId,
    ) -> Result<SidecarPackageWriter> {
        let mut package_index = 0u32;
        loop {
            let file_id = ArtifactFileId {
                definition_id,
                generation_id,
                package_index,
            };
            let path = self.package_path(file_id);
            if !path.exists() {
                return self.create_package_writer(file_id);
            }
            package_index = package_index.checked_add(1).ok_or_else(|| {
                paro_error::out_of_range(format!(
                    "search sidecar package index overflow for definition {} generation {}",
                    definition_id, generation_id
                ))
            })?;
        }
    }

    pub fn read_artifact(&self, location: &ArtifactLocation) -> Result<Vec<u8>> {
        let (file_id, offset, len, checksum) = sidecar_location_parts(location)?;
        let path = self.package_path(file_id);
        let mut file = File::open(&path).map_err(|err| {
            paro_error::io_error(format!(
                "open search sidecar package {} for {:?}: {}",
                path.display(),
                file_id,
                err
            ))
        })?;
        file.seek(SeekFrom::Start(offset)).map_err(|err| {
            paro_error::io_error(format!(
                "seek search sidecar package {} to {}: {}",
                path.display(),
                offset,
                err
            ))
        })?;

        let len_usize = usize::try_from(len).map_err(|_| {
            paro_error::invalid_input(format!(
                "search sidecar artifact length {} does not fit in usize",
                len
            ))
        })?;
        let mut bytes = vec![0u8; len_usize];
        file.read_exact(&mut bytes).map_err(|err| {
            paro_error::io_error(format!(
                "read search sidecar artifact {}:{}+{}: {}",
                path.display(),
                offset,
                len,
                err
            ))
        })?;
        verify_sidecar_checksum(file_id, checksum, &bytes)?;
        Ok(bytes)
    }

    pub fn mmap_artifact(&self, location: &ArtifactLocation) -> Result<SidecarMappedArtifact> {
        let (file_id, offset, len, checksum) = sidecar_location_parts(location)?;
        let package = self.mmap_package(file_id)?;
        package.artifact(offset, len, checksum)
    }

    pub fn mmap_package(&self, file_id: ArtifactFileId) -> Result<SidecarMappedPackage> {
        let path = self.package_path(file_id);
        let file = File::open(&path).map_err(|err| {
            paro_error::io_error(format!(
                "open search sidecar package {} for {:?}: {}",
                path.display(),
                file_id,
                err
            ))
        })?;
        let mmap = unsafe {
            memmap2::MmapOptions::new().map(&file).map_err(|err| {
                paro_error::io_error(format!(
                    "mmap search sidecar package {} for {:?}: {}",
                    path.display(),
                    file_id,
                    err
                ))
            })?
        };
        Ok(SidecarMappedPackage {
            file_id,
            mmap: Arc::new(mmap),
        })
    }
}

#[derive(Debug)]
pub struct SidecarMappedPackage {
    file_id: ArtifactFileId,
    mmap: Arc<Mmap>,
}

impl SidecarMappedPackage {
    pub fn file_id(&self) -> ArtifactFileId {
        self.file_id
    }

    pub fn pinned_bytes(&self) -> usize {
        self.mmap.len()
    }

    pub fn artifact(&self, offset: u64, len: u64, checksum: u64) -> Result<SidecarMappedArtifact> {
        let offset = usize::try_from(offset).map_err(|_| {
            paro_error::invalid_input(format!(
                "search sidecar artifact offset {} does not fit in usize",
                offset
            ))
        })?;
        let len = usize::try_from(len).map_err(|_| {
            paro_error::invalid_input(format!(
                "search sidecar artifact length {} does not fit in usize",
                len
            ))
        })?;
        let end = offset.checked_add(len).ok_or_else(|| {
            paro_error::invalid_input(format!(
                "search sidecar artifact range overflows usize for {:?}",
                self.file_id
            ))
        })?;
        if end > self.mmap.len() {
            return Err(paro_error::data_corrupted(format!(
                "search sidecar artifact range {}..{} exceeds package length {} for {:?}",
                offset,
                end,
                self.mmap.len(),
                self.file_id
            )));
        }
        verify_sidecar_checksum(self.file_id, checksum, &self.mmap[offset..end])?;
        Ok(SidecarMappedArtifact {
            package: Arc::clone(&self.mmap),
            offset,
            len,
        })
    }
}

#[derive(Debug)]
pub struct SidecarPackageWriter {
    file_id: ArtifactFileId,
    final_path: PathBuf,
    staging_path: PathBuf,
    file: Option<File>,
    offset: u64,
    committed: bool,
}

impl SidecarPackageWriter {
    fn create(table_data_dir: PathBuf, file_id: ArtifactFileId) -> Result<Self> {
        let final_path = table_data_dir.join(SidecarArtifactStore::package_relative_path(file_id));
        if final_path.exists() {
            return Err(paro_error::invalid_input(format!(
                "search sidecar package already exists: {}",
                final_path.display()
            )));
        }
        let parent = final_path.parent().ok_or_else(|| {
            paro_error::internal(format!(
                "search sidecar package {} has no parent",
                final_path.display()
            ))
        })?;
        fs::create_dir_all(parent).map_err(|err| {
            paro_error::io_error(format!(
                "create search sidecar package parent {}: {}",
                parent.display(),
                err
            ))
        })?;

        let staging_path = staging_path_for(&final_path);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging_path)
            .map_err(|err| {
                paro_error::io_error(format!(
                    "create search sidecar staging package {}: {}",
                    staging_path.display(),
                    err
                ))
            })?;

        Ok(Self {
            file_id,
            final_path,
            staging_path,
            file: Some(file),
            offset: 0,
            committed: false,
        })
    }

    pub fn file_id(&self) -> ArtifactFileId {
        self.file_id
    }

    pub fn final_path(&self) -> &Path {
        &self.final_path
    }

    pub fn staging_path(&self) -> &Path {
        &self.staging_path
    }

    pub fn append_artifact(&mut self, bytes: &[u8]) -> Result<ArtifactLocation> {
        let len = u64::try_from(bytes.len()).map_err(|_| {
            paro_error::invalid_input("search sidecar artifact length does not fit in u64")
        })?;
        let offset = self.offset;
        let file = self.file.as_mut().ok_or_else(|| {
            paro_error::internal("cannot append to finalized search sidecar package")
        })?;
        file.write_all(bytes).map_err(|err| {
            paro_error::io_error(format!(
                "write search sidecar artifact {}:{}+{}: {}",
                self.staging_path.display(),
                offset,
                len,
                err
            ))
        })?;
        self.offset = self.offset.saturating_add(len);
        Ok(ArtifactLocation::SidecarArtifactFile {
            file_id: self.file_id,
            offset,
            len,
            checksum: seahash::hash(bytes),
        })
    }

    pub fn finalize(mut self) -> Result<PathBuf> {
        if let Some(mut file) = self.file.take() {
            file.flush().map_err(|err| {
                paro_error::io_error(format!(
                    "flush search sidecar package {}: {}",
                    self.staging_path.display(),
                    err
                ))
            })?;
            file.sync_all().map_err(|err| {
                paro_error::io_error(format!(
                    "sync search sidecar package {}: {}",
                    self.staging_path.display(),
                    err
                ))
            })?;
        }
        if let Some(parent) = self.final_path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                paro_error::io_error(format!(
                    "create search sidecar package parent {}: {}",
                    parent.display(),
                    err
                ))
            })?;
        }
        fs::rename(&self.staging_path, &self.final_path).map_err(|err| {
            let _ = fs::remove_file(&self.staging_path);
            paro_error::io_error(format!(
                "commit search sidecar package {} -> {}: {}",
                self.staging_path.display(),
                self.final_path.display(),
                err
            ))
        })?;
        self.committed = true;
        Ok(self.final_path.clone())
    }

    pub fn abort(mut self) {
        self.file.take();
        let _ = fs::remove_file(&self.staging_path);
        self.committed = true;
    }

    pub fn bytes_written(&self) -> u64 {
        self.offset
    }
}

impl Drop for SidecarPackageWriter {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.file.take();
            let _ = fs::remove_file(&self.staging_path);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SidecarReaderCacheKey {
    pub checksum: u64,
    pub artifact_format_version: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct SidecarReaderRequest<'a> {
    pub location: &'a ArtifactLocation,
    pub artifact_format_version: u32,
    pub provider: SearchIndexKind,
    pub codec: &'static str,
}

#[derive(Debug)]
pub struct SidecarCachedArtifact {
    key: SidecarReaderCacheKey,
    mapped: SidecarMappedArtifact,
}

impl SidecarCachedArtifact {
    pub fn key(&self) -> SidecarReaderCacheKey {
        self.key
    }

    pub fn bytes(&self) -> &[u8] {
        self.mapped.bytes()
    }

    pub fn is_mmap_backed(&self) -> bool {
        true
    }

    pub fn pinned_bytes(&self) -> usize {
        self.mapped.pinned_bytes()
    }
}

#[derive(Debug)]
pub struct SidecarMappedArtifact {
    package: Arc<Mmap>,
    offset: usize,
    len: usize,
}

impl SidecarMappedArtifact {
    pub fn bytes(&self) -> &[u8] {
        &self.package[self.offset..self.offset + self.len]
    }

    pub fn artifact_len(&self) -> usize {
        self.len
    }

    pub fn pinned_bytes(&self) -> usize {
        self.package.len()
    }
}

#[derive(Debug)]
pub struct SidecarReaderCache {
    store: SidecarArtifactStore,
    packages: Mutex<BTreeMap<ArtifactFileId, Arc<SidecarMappedPackage>>>,
    entries: Mutex<BTreeMap<SidecarReaderCacheKey, Arc<SidecarCachedArtifact>>>,
}

impl SidecarReaderCache {
    pub fn new(store: SidecarArtifactStore) -> Self {
        Self {
            store,
            packages: Mutex::new(BTreeMap::new()),
            entries: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn open(&self, request: SidecarReaderRequest<'_>) -> Result<Arc<SidecarCachedArtifact>> {
        storage_metrics()
            .record_search_sidecar_reader_format_dispatch(request.provider, request.codec);
        let key = sidecar_reader_cache_key(request.location, request.artifact_format_version)?;

        if let Some(cached) = self
            .entries
            .lock()
            .expect("search sidecar reader cache lock poisoned")
            .get(&key)
            .cloned()
        {
            storage_metrics()
                .record_search_sidecar_reader_cache_hit(request.provider, request.codec);
            return Ok(cached);
        }

        storage_metrics().record_search_sidecar_reader_cache_miss(request.provider, request.codec);
        let (file_id, offset, len, checksum) = sidecar_location_parts(request.location)?;
        let package = self.open_package(file_id, request.provider, request.codec)?;
        let mapped = package.artifact(offset, len, checksum)?;
        let cached = Arc::new(SidecarCachedArtifact { key, mapped });

        let mut guard = self
            .entries
            .lock()
            .expect("search sidecar reader cache lock poisoned");
        Ok(guard.entry(key).or_insert_with(|| cached.clone()).clone())
    }

    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .expect("search sidecar reader cache lock poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn package_count(&self) -> usize {
        self.packages
            .lock()
            .expect("search sidecar package cache lock poisoned")
            .len()
    }

    fn open_package(
        &self,
        file_id: ArtifactFileId,
        provider: SearchIndexKind,
        codec: &'static str,
    ) -> Result<Arc<SidecarMappedPackage>> {
        let mut guard = self
            .packages
            .lock()
            .expect("search sidecar package cache lock poisoned");
        if let Some(package) = guard.get(&file_id).cloned() {
            return Ok(package);
        }

        storage_metrics().record_search_sidecar_reader_open(provider, codec);
        let package = Arc::new(self.store.mmap_package(file_id)?);
        storage_metrics().add_search_sidecar_reader_mmap_bytes(
            provider,
            codec,
            package.pinned_bytes() as u64,
        );
        guard.insert(file_id, Arc::clone(&package));
        Ok(package)
    }
}

fn staging_path_for(final_path: &Path) -> PathBuf {
    let sequence = SIDECAR_STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let file_name = final_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("package.scar");
    final_path.with_file_name(format!("{file_name}.staging-{sequence}"))
}

fn sidecar_reader_cache_key(
    location: &ArtifactLocation,
    artifact_format_version: u32,
) -> Result<SidecarReaderCacheKey> {
    let ArtifactLocation::SidecarArtifactFile { checksum, .. } = location else {
        return Err(paro_error::invalid_input(
            "expected sidecar artifact file location",
        ));
    };
    Ok(SidecarReaderCacheKey {
        checksum: *checksum,
        artifact_format_version,
    })
}

fn sidecar_location_parts(location: &ArtifactLocation) -> Result<(ArtifactFileId, u64, u64, u64)> {
    let ArtifactLocation::SidecarArtifactFile {
        file_id,
        offset,
        len,
        checksum,
    } = location
    else {
        return Err(paro_error::invalid_input(
            "expected sidecar artifact file location",
        ));
    };
    Ok((*file_id, *offset, *len, *checksum))
}

fn verify_sidecar_checksum(file_id: ArtifactFileId, checksum: u64, bytes: &[u8]) -> Result<()> {
    let actual = seahash::hash(bytes);
    if actual != checksum {
        return Err(paro_error::data_corrupted(format!(
            "search sidecar artifact checksum mismatch for {:?}: expected {}, got {}",
            file_id, checksum, actual
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::storage_metrics;

    #[test]
    fn sidecar_package_writer_appends_offsets_and_reads_by_location() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = SidecarArtifactStore::new(temp_dir.path());
        let file_id = SidecarArtifactStore::default_shard_file_id(7, 3);
        let mut writer = store.create_package_writer(file_id).unwrap();

        let first = writer.append_artifact(b"alpha").unwrap();
        let second = writer.append_artifact(b"beta-gamma").unwrap();
        let staging_path = writer.staging_path().to_path_buf();
        let final_path = writer.final_path().to_path_buf();

        assert_eq!(writer.bytes_written(), 15);
        assert!(staging_path.exists());
        writer.finalize().unwrap();
        assert!(final_path.exists());
        assert!(!staging_path.exists());

        assert_eq!(store.read_artifact(&first).unwrap(), b"alpha");
        assert_eq!(store.read_artifact(&second).unwrap(), b"beta-gamma");

        match (&first, &second) {
            (
                ArtifactLocation::SidecarArtifactFile {
                    file_id: first_file,
                    offset: first_offset,
                    len: first_len,
                    ..
                },
                ArtifactLocation::SidecarArtifactFile {
                    file_id: second_file,
                    offset: second_offset,
                    len: second_len,
                    ..
                },
            ) => {
                assert_eq!(*first_file, file_id);
                assert_eq!(*second_file, file_id);
                assert_eq!(store.package_path(*first_file), final_path);
                assert_eq!(*first_offset, 0);
                assert_eq!(*first_len, 5);
                assert_eq!(*second_offset, 5);
                assert_eq!(*second_len, 10);
                assert!(!SidecarArtifactStore::package_relative_path(file_id).is_absolute());
            }
            _ => panic!("expected sidecar locations"),
        }
    }

    #[test]
    fn sidecar_package_writer_drop_cleans_staging_without_final_package() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = SidecarArtifactStore::new(temp_dir.path());
        let file_id = ArtifactFileId {
            definition_id: 11,
            generation_id: 4,
            package_index: 2,
        };
        let final_path;
        let staging_path;
        {
            let mut writer = store.create_package_writer(file_id).unwrap();
            writer.append_artifact(b"orphan").unwrap();
            final_path = writer.final_path().to_path_buf();
            staging_path = writer.staging_path().to_path_buf();
            assert!(staging_path.exists());
        }

        assert!(!staging_path.exists());
        assert!(!final_path.exists());
    }

    #[test]
    fn sidecar_package_reader_rejects_checksum_mismatch() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = SidecarArtifactStore::new(temp_dir.path());
        let file_id = SidecarArtifactStore::default_shard_file_id(9, 1);
        let mut writer = store.create_package_writer(file_id).unwrap();
        let location = writer.append_artifact(b"stable").unwrap();
        let final_path = writer.final_path().to_path_buf();
        writer.finalize().unwrap();
        fs::write(&final_path, b"mutate").unwrap();

        let err = store.read_artifact(&location).unwrap_err();
        assert!(err.to_string().contains("checksum mismatch"));
    }

    #[test]
    #[serial_test::serial]
    fn sidecar_reader_cache_uses_content_checksum_and_format_version_identity() {
        let _metrics_guard = crate::metrics::storage_metrics_test_guard();
        const TEST_CODEC: &str = "test-cache-content-identity";
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = SidecarArtifactStore::new(temp_dir.path());

        let mut writer = store
            .create_package_writer(SidecarArtifactStore::default_shard_file_id(17, 1))
            .unwrap();
        let first_location = writer.append_artifact(b"cache-me").unwrap();
        writer.finalize().unwrap();

        let second_file_id = ArtifactFileId {
            definition_id: 17,
            generation_id: 1,
            package_index: 1,
        };
        let mut second_writer = store.create_package_writer(second_file_id).unwrap();
        let second_location = second_writer.append_artifact(b"cache-me").unwrap();
        let second_path = second_writer.final_path().to_path_buf();
        second_writer.finalize().unwrap();
        fs::remove_file(&second_path).unwrap();

        let cache = SidecarReaderCache::new(store.clone());
        let first = cache
            .open(SidecarReaderRequest {
                location: &first_location,
                artifact_format_version: 1,
                provider: SearchIndexKind::FullText,
                codec: TEST_CODEC,
            })
            .unwrap();
        let second = cache
            .open(SidecarReaderRequest {
                location: &second_location,
                artifact_format_version: 1,
                provider: SearchIndexKind::FullText,
                codec: TEST_CODEC,
            })
            .unwrap();

        assert!(std::sync::Arc::ptr_eq(&first, &second));
        assert_eq!(first.bytes(), b"cache-me");
        assert_eq!(cache.len(), 1);

        let err = cache
            .open(SidecarReaderRequest {
                location: &second_location,
                artifact_format_version: 2,
                provider: SearchIndexKind::FullText,
                codec: TEST_CODEC,
            })
            .unwrap_err();
        assert!(err.to_string().contains("open search sidecar package"));

        let snapshot = storage_metrics().snapshot();
        let series = snapshot
            .search_sidecar_reader_by_key
            .iter()
            .find(|series| {
                series.key.provider == SearchIndexKind::FullText && series.key.codec == TEST_CODEC
            })
            .expect("content identity test sidecar reader metrics");
        assert_eq!(series.key.provider, SearchIndexKind::FullText);
        assert_eq!(series.key.codec, TEST_CODEC);
        assert_eq!(series.counters.format_dispatch_total, 3);
        assert_eq!(series.counters.cache_misses_total, 2);
        assert_eq!(series.counters.cache_hits_total, 1);
        assert_eq!(series.counters.open_count_total, 2);
        assert_eq!(series.counters.mmap_bytes, 8);
    }

    #[test]
    #[serial_test::serial]
    fn sidecar_reader_cache_reuses_package_mmap_for_multiple_artifacts() {
        let _metrics_guard = crate::metrics::storage_metrics_test_guard();
        const TEST_CODEC: &str = "test-cache-package-reuse";
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = SidecarArtifactStore::new(temp_dir.path());
        let file_id = SidecarArtifactStore::default_shard_file_id(23, 5);
        let mut writer = store.create_package_writer(file_id).unwrap();
        let first_location = writer.append_artifact(b"first").unwrap();
        let second_location = writer.append_artifact(b"second-artifact").unwrap();
        writer.finalize().unwrap();

        let cache = SidecarReaderCache::new(store);
        let first = cache
            .open(SidecarReaderRequest {
                location: &first_location,
                artifact_format_version: 1,
                provider: SearchIndexKind::Sparse,
                codec: TEST_CODEC,
            })
            .unwrap();
        let second = cache
            .open(SidecarReaderRequest {
                location: &second_location,
                artifact_format_version: 1,
                provider: SearchIndexKind::Sparse,
                codec: TEST_CODEC,
            })
            .unwrap();
        let first_again = cache
            .open(SidecarReaderRequest {
                location: &first_location,
                artifact_format_version: 1,
                provider: SearchIndexKind::Sparse,
                codec: TEST_CODEC,
            })
            .unwrap();

        assert_eq!(first.bytes(), b"first");
        assert_eq!(second.bytes(), b"second-artifact");
        assert!(std::sync::Arc::ptr_eq(&first, &first_again));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.package_count(), 1);

        let snapshot = storage_metrics().snapshot();
        let series = snapshot
            .search_sidecar_reader_by_key
            .iter()
            .find(|series| {
                series.key.provider == SearchIndexKind::Sparse && series.key.codec == TEST_CODEC
            })
            .expect("package reuse test sidecar reader metrics");
        assert_eq!(series.key.provider, SearchIndexKind::Sparse);
        assert_eq!(series.counters.format_dispatch_total, 3);
        assert_eq!(series.counters.cache_misses_total, 2);
        assert_eq!(series.counters.cache_hits_total, 1);
        assert_eq!(series.counters.open_count_total, 1);
        assert_eq!(series.counters.mmap_bytes, 20);
    }
}
