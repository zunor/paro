// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::hash::Hasher;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arc_swap::ArcSwap;
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
            .join("generations")
            .join(format!("g{}", file_id.generation_id))
            .join("sidecars")
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
        package.artifact(
            offset,
            len,
            checksum,
            SidecarIntegrityPolicy::EnvelopeChecksum,
        )
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

    pub fn artifact(
        &self,
        offset: u64,
        len: u64,
        checksum: u64,
        integrity: SidecarIntegrityPolicy,
    ) -> Result<SidecarMappedArtifact> {
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
        if integrity == SidecarIntegrityPolicy::EnvelopeChecksum {
            verify_sidecar_checksum(self.file_id, checksum, &self.mmap[offset..end])?;
        }
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
    workspace_dir: PathBuf,
    staging_path: PathBuf,
    file: Option<File>,
    offset: u64,
    committed: bool,
}

impl SidecarPackageWriter {
    const ARTIFACT_ALIGNMENT: u64 = 64;

    pub(crate) fn workspace_dir(&self) -> &Path {
        &self.workspace_dir
    }

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

        let staging_root = table_data_dir
            .join("_staged")
            .join("search-sidecar")
            .join(file_id.definition_id.to_string())
            .join(format!("g{}", file_id.generation_id));
        fs::create_dir_all(&staging_root).map_err(|err| {
            paro_error::io_error(format!(
                "create search sidecar staging root {}: {}",
                staging_root.display(),
                err
            ))
        })?;
        let workspace_dir = create_sidecar_workspace(&staging_root, file_id.package_index)?;
        let staging_path = workspace_dir.join("package.scar");
        let file = OpenOptions::new()
            .read(true)
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
            workspace_dir,
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
        let file = self.file.as_mut().ok_or_else(|| {
            paro_error::internal("cannot append to finalized search sidecar package")
        })?;
        let offset = self
            .offset
            .checked_add(Self::ARTIFACT_ALIGNMENT - 1)
            .map(|value| value / Self::ARTIFACT_ALIGNMENT * Self::ARTIFACT_ALIGNMENT)
            .ok_or_else(|| paro_error::out_of_range("search sidecar artifact alignment"))?;
        let padding = usize::try_from(offset - self.offset).map_err(|_| {
            paro_error::out_of_range("search sidecar artifact padding exceeds usize")
        })?;
        if padding != 0 {
            const ZEROES: [u8; SidecarPackageWriter::ARTIFACT_ALIGNMENT as usize] =
                [0; SidecarPackageWriter::ARTIFACT_ALIGNMENT as usize];
            file.write_all(&ZEROES[..padding]).map_err(|err| {
                paro_error::io_error(format!(
                    "align search sidecar artifact {}:{}+{}: {}",
                    self.staging_path.display(),
                    self.offset,
                    padding,
                    err
                ))
            })?;
        }
        file.write_all(bytes).map_err(|err| {
            paro_error::io_error(format!(
                "write search sidecar artifact {}:{}+{}: {}",
                self.staging_path.display(),
                offset,
                len,
                err
            ))
        })?;
        self.offset = offset.saturating_add(len);
        Ok(ArtifactLocation::SidecarArtifactFile {
            file_id: self.file_id,
            offset,
            len,
            checksum: seahash::hash(bytes),
        })
    }

    pub(crate) fn append_streamed_artifact(
        &mut self,
        write_artifact: impl FnOnce(&mut File, u64) -> Result<()>,
    ) -> Result<ArtifactLocation> {
        let file = self.file.as_mut().ok_or_else(|| {
            paro_error::internal("cannot append to finalized search sidecar package")
        })?;
        let offset = self
            .offset
            .checked_add(Self::ARTIFACT_ALIGNMENT - 1)
            .map(|value| value / Self::ARTIFACT_ALIGNMENT * Self::ARTIFACT_ALIGNMENT)
            .ok_or_else(|| paro_error::out_of_range("search sidecar artifact alignment"))?;
        let padding = usize::try_from(offset - self.offset).map_err(|_| {
            paro_error::out_of_range("search sidecar artifact padding exceeds usize")
        })?;
        if padding != 0 {
            const ZEROES: [u8; SidecarPackageWriter::ARTIFACT_ALIGNMENT as usize] =
                [0; SidecarPackageWriter::ARTIFACT_ALIGNMENT as usize];
            file.seek(SeekFrom::Start(self.offset))?;
            file.write_all(&ZEROES[..padding])?;
        }
        file.seek(SeekFrom::Start(offset))?;
        write_artifact(file, offset)?;
        let end = file.stream_position()?;
        let len = end.checked_sub(offset).ok_or_else(|| {
            paro_error::internal("streamed search sidecar writer moved before artifact start")
        })?;

        file.seek(SeekFrom::Start(offset))?;
        let mut remaining = len;
        let mut hasher = seahash::SeaHasher::new();
        let mut buffer = vec![0u8; 64 * 1024];
        while remaining != 0 {
            let read_len = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(|_| paro_error::out_of_range("sidecar hash chunk exceeds usize"))?;
            file.read_exact(&mut buffer[..read_len])?;
            hasher.write(&buffer[..read_len]);
            remaining -= read_len as u64;
        }
        let checksum = hasher.finish();
        file.seek(SeekFrom::Start(end))?;
        self.offset = end;
        Ok(ArtifactLocation::SidecarArtifactFile {
            file_id: self.file_id,
            offset,
            len,
            checksum,
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
            let _ = fs::remove_dir_all(&self.workspace_dir);
            paro_error::io_error(format!(
                "commit search sidecar package {} -> {}: {}",
                self.staging_path.display(),
                self.final_path.display(),
                err
            ))
        })?;
        let _ = fs::remove_dir_all(&self.workspace_dir);
        self.committed = true;
        Ok(self.final_path.clone())
    }

    pub fn abort(mut self) {
        self.file.take();
        let _ = fs::remove_dir_all(&self.workspace_dir);
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
            let _ = fs::remove_dir_all(&self.workspace_dir);
        }
    }
}

/// Integrity owner for a sidecar artifact.
///
/// Most providers rely on the package manifest checksum. Random-access HNSW
/// artifacts instead authenticate their own fixed header and lazily verify
/// payload chunks before use; eagerly hashing that entire mmap range would
/// destroy their cold-open contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SidecarIntegrityPolicy {
    EnvelopeChecksum,
    SelfValidatingArtifact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SidecarReaderCacheKey {
    pub file_id: ArtifactFileId,
    pub offset: u64,
    pub len: u64,
    pub checksum: u64,
    pub artifact_format_version: u32,
    pub integrity: SidecarIntegrityPolicy,
}

/// Identity for a typed, immutable provider reader derived from one sidecar
/// artifact. The physical segment identity is part of this key because an
/// HNSW graph can be byte-identical while its external base-vector storage is
/// not. Content identity alone is therefore insufficient for decoded readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DecodedSidecarArtifactKey {
    pub sidecar: SidecarReaderCacheKey,
    pub provider: SearchIndexKind,
}

/// One immutable provider reader lookup.
///
/// The physical identity is derivable from the manifest location, so a hot
/// lookup must not open (or even lock) the lower-level mmap cache merely to
/// discover its decoded-reader key. Provider readers are the normal query
/// surface; the sidecar mapping is only a cold-miss construction dependency.
#[derive(Debug, Clone, Copy)]
pub struct DecodedSidecarReaderRequest<'a> {
    pub sidecar: SidecarReaderRequest<'a>,
}

impl DecodedSidecarReaderRequest<'_> {
    fn key(self) -> Result<DecodedSidecarArtifactKey> {
        Ok(DecodedSidecarArtifactKey {
            sidecar: sidecar_reader_cache_key(
                self.sidecar.location,
                self.sidecar.artifact_format_version,
                self.sidecar.integrity,
            )?,
            provider: self.sidecar.provider,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SidecarReaderRequest<'a> {
    pub location: &'a ArtifactLocation,
    pub artifact_format_version: u32,
    pub provider: SearchIndexKind,
    pub codec: &'static str,
    pub integrity: SidecarIntegrityPolicy,
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

    pub(crate) fn mmap_range(&self) -> (Arc<Mmap>, usize, usize) {
        self.mapped.mmap_range()
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

    fn mmap_range(&self) -> (Arc<Mmap>, usize, usize) {
        (Arc::clone(&self.package), self.offset, self.len)
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
        let key = sidecar_reader_cache_key(
            request.location,
            request.artifact_format_version,
            request.integrity,
        )?;

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
        let mapped = package.artifact(offset, len, checksum, request.integrity)?;
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

    fn evict_packages(&self, file_ids: &BTreeSet<ArtifactFileId>) {
        if file_ids.is_empty() {
            return;
        }
        self.entries
            .lock()
            .expect("search sidecar reader cache lock poisoned")
            .retain(|key, _| !file_ids.contains(&key.file_id));
        self.packages
            .lock()
            .expect("search sidecar package cache lock poisoned")
            .retain(|file_id, _| !file_ids.contains(file_id));
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

/// Table-scoped owner for immutable sidecar mappings and provider readers.
///
/// Query cursors borrow this runtime through an `Arc`; they never create their
/// own mmap/cache namespace. Generation retirement evicts physical packages
/// only after read leases are gone, while an in-flight cursor can safely keep
/// its typed reader alive through its own `Arc`.
pub struct SearchReaderRuntime {
    sidecars: SidecarReaderCache,
    decoded: ArcSwap<BTreeMap<DecodedSidecarArtifactKey, Arc<dyn Any + Send + Sync>>>,
    decoded_update: Mutex<()>,
    buffer_pool: OnceLock<Arc<crate::buffer::BufferPool>>,
    hnsw_integrity_scheduler: OnceLock<Arc<crate::index::hnsw::HnswIntegrityScheduler>>,
}

impl std::fmt::Debug for SearchReaderRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchReaderRuntime")
            .field("sidecar_entries", &self.sidecars.len())
            .field("sidecar_packages", &self.sidecars.package_count())
            .field("decoded_entries", &self.decoded.load().len())
            .finish()
    }
}

impl SearchReaderRuntime {
    pub fn new(store: SidecarArtifactStore) -> Self {
        Self {
            sidecars: SidecarReaderCache::new(store),
            decoded: ArcSwap::from_pointee(BTreeMap::new()),
            decoded_update: Mutex::new(()),
            buffer_pool: OnceLock::new(),
            hnsw_integrity_scheduler: OnceLock::new(),
        }
    }

    /// Bind long-lived provider readers to the same process memory governor
    /// used by ordinary table reads. A table runtime belongs to one instance;
    /// accepting a different pool later would silently split its accounting.
    pub(crate) fn bind_buffer_pool(
        &self,
        buffer_pool: Option<Arc<crate::buffer::BufferPool>>,
    ) -> Result<()> {
        let Some(buffer_pool) = buffer_pool else {
            return Ok(());
        };
        if let Some(existing) = self.buffer_pool.get() {
            return if Arc::ptr_eq(existing, &buffer_pool) {
                Ok(())
            } else {
                Err(paro_error::internal(
                    "search reader runtime cannot move between buffer pools",
                ))
            };
        }
        if let Err(buffer_pool) = self.buffer_pool.set(buffer_pool) {
            if !self
                .buffer_pool
                .get()
                .is_some_and(|existing| Arc::ptr_eq(existing, &buffer_pool))
            {
                return Err(paro_error::internal(
                    "concurrent search reader buffer-pool binding",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn buffer_pool(&self) -> Option<Arc<crate::buffer::BufferPool>> {
        self.buffer_pool.get().cloned()
    }

    pub(crate) fn bind_hnsw_integrity_scheduler(
        &self,
        scheduler: Option<Arc<crate::index::hnsw::HnswIntegrityScheduler>>,
    ) -> Result<()> {
        let Some(scheduler) = scheduler else {
            return Ok(());
        };
        if let Some(existing) = self.hnsw_integrity_scheduler.get() {
            return if Arc::ptr_eq(existing, &scheduler) {
                Ok(())
            } else {
                Err(paro_error::internal(
                    "search reader runtime cannot move between HNSW integrity schedulers",
                ))
            };
        }
        if let Err(scheduler) = self.hnsw_integrity_scheduler.set(scheduler) {
            if !self
                .hnsw_integrity_scheduler
                .get()
                .is_some_and(|existing| Arc::ptr_eq(existing, &scheduler))
            {
                return Err(paro_error::internal(
                    "concurrent HNSW integrity-scheduler binding",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn schedule_hnsw_integrity_verification(
        &self,
        index: &Arc<crate::index::hnsw::HnswIndex>,
    ) {
        if let Some(scheduler) = self.hnsw_integrity_scheduler.get() {
            scheduler.schedule(index);
        }
    }

    pub fn open_sidecar(
        &self,
        request: SidecarReaderRequest<'_>,
    ) -> Result<Arc<SidecarCachedArtifact>> {
        self.sidecars.open(request)
    }

    /// Return a typed provider reader without touching the mmap cache on a hot
    /// lookup. The decoded map is immutable between rare generation changes,
    /// so readers use an `ArcSwap` snapshot and never contend with one another.
    /// Cold construction happens outside the publication lock.
    pub fn get_or_try_open_decoded<T, F>(
        &self,
        request: DecodedSidecarReaderRequest<'_>,
        build: F,
    ) -> Result<Option<Arc<T>>>
    where
        T: Any + Send + Sync,
        F: FnOnce(&SidecarCachedArtifact) -> Result<Option<T>>,
    {
        let key = request.key()?;
        if let Some(existing) = self.lookup_decoded::<T>(key)? {
            return Ok(Some(existing));
        }

        let cached = self.sidecars.open(request.sidecar)?;
        let Some(candidate) = build(cached.as_ref())? else {
            return Ok(None);
        };
        let candidate = Arc::new(candidate);
        let erased_candidate: Arc<dyn Any + Send + Sync> = candidate.clone();
        let _update = self
            .decoded_update
            .lock()
            .map_err(|_| paro_error::internal("search decoded-reader update lock poisoned"))?;
        if let Some(existing) = self.lookup_decoded::<T>(key)? {
            return Ok(Some(existing));
        }
        let current = self.decoded.load_full();
        let mut updated = (*current).clone();
        updated.insert(key, erased_candidate);
        self.decoded.store(Arc::new(updated));
        Ok(Some(candidate))
    }

    fn lookup_decoded<T>(&self, key: DecodedSidecarArtifactKey) -> Result<Option<Arc<T>>>
    where
        T: Any + Send + Sync,
    {
        let Some(existing) = self.decoded.load().get(&key).cloned() else {
            return Ok(None);
        };
        Arc::downcast::<T>(existing).map(Some).map_err(|_| {
            paro_error::internal("search decoded-reader cache type does not match provider")
        })
    }

    pub(crate) fn evict_packages(&self, file_ids: &BTreeSet<ArtifactFileId>) {
        if file_ids.is_empty() {
            return;
        }
        if let Ok(_update) = self.decoded_update.lock() {
            let current = self.decoded.load_full();
            let mut updated = (*current).clone();
            updated.retain(|key, _| !file_ids.contains(&key.sidecar.file_id));
            self.decoded.store(Arc::new(updated));
        }
        self.sidecars.evict_packages(file_ids);
    }

    #[cfg(test)]
    fn decoded_len(&self) -> usize {
        self.decoded.load().len()
    }
}

fn create_sidecar_workspace(staging_root: &Path, package_index: u32) -> Result<PathBuf> {
    loop {
        let sequence = SIDECAR_STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let workspace = staging_root.join(format!(
            "package-{package_index}-process-{}-{sequence}",
            std::process::id()
        ));
        match fs::create_dir(&workspace) {
            Ok(()) => return Ok(workspace),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(paro_error::io_error(format!(
                    "create search sidecar workspace {}: {}",
                    workspace.display(),
                    error
                )));
            }
        }
    }
}

fn sidecar_reader_cache_key(
    location: &ArtifactLocation,
    artifact_format_version: u32,
    integrity: SidecarIntegrityPolicy,
) -> Result<SidecarReaderCacheKey> {
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
    Ok(SidecarReaderCacheKey {
        file_id: *file_id,
        offset: *offset,
        len: *len,
        checksum: *checksum,
        artifact_format_version,
        integrity,
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
    fn self_validating_artifact_does_not_share_an_unverified_cache_entry() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = SidecarArtifactStore::new(temp_dir.path());
        let file_id = SidecarArtifactStore::default_shard_file_id(91, 3);
        let mut writer = store.create_package_writer(file_id).unwrap();
        let location = writer.append_artifact(b"self-validating").unwrap();
        let path = writer.final_path().to_path_buf();
        writer.finalize().unwrap();

        let (_, offset, _, _) = sidecar_location_parts(&location).unwrap();
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(b"S").unwrap();
        file.sync_all().unwrap();

        let cache = SidecarReaderCache::new(store);
        let self_validating = cache
            .open(SidecarReaderRequest {
                location: &location,
                artifact_format_version: 8,
                provider: SearchIndexKind::Hnsw,
                codec: SIDECAR_PACKAGE_CODEC,
                integrity: SidecarIntegrityPolicy::SelfValidatingArtifact,
            })
            .unwrap();
        assert_eq!(self_validating.bytes(), b"Self-validating");

        let error = cache
            .open(SidecarReaderRequest {
                location: &location,
                artifact_format_version: 8,
                provider: SearchIndexKind::Hnsw,
                codec: SIDECAR_PACKAGE_CODEC,
                integrity: SidecarIntegrityPolicy::EnvelopeChecksum,
            })
            .unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
        assert_eq!(cache.len(), 1);
    }

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

        assert_eq!(writer.bytes_written(), 74);
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
                assert_eq!(*second_offset, SidecarPackageWriter::ARTIFACT_ALIGNMENT);
                assert_eq!(*second_len, 10);
                assert!(!SidecarArtifactStore::package_relative_path(file_id).is_absolute());
            }
            _ => panic!("expected sidecar locations"),
        }
    }

    #[test]
    fn sidecar_package_writer_streams_aligned_artifact_and_derives_its_length() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = SidecarArtifactStore::new(temp_dir.path());
        let file_id = SidecarArtifactStore::default_shard_file_id(8, 4);
        let mut writer = store.create_package_writer(file_id).unwrap();
        writer.append_artifact(b"prefix").unwrap();
        let expected = (0..128 * 1024 + 17)
            .map(|position| (position % 251) as u8)
            .collect::<Vec<_>>();

        let location = writer
            .append_streamed_artifact(|file, offset| {
                assert_eq!(file.stream_position().unwrap(), offset);
                for chunk in expected.chunks(4093) {
                    file.write_all(chunk)?;
                }
                Ok(())
            })
            .unwrap();
        writer.finalize().unwrap();

        let ArtifactLocation::SidecarArtifactFile {
            offset,
            len,
            checksum,
            ..
        } = location
        else {
            panic!("expected streamed sidecar location");
        };
        assert_eq!(offset % SidecarPackageWriter::ARTIFACT_ALIGNMENT, 0);
        assert_eq!(len, expected.len() as u64);
        assert_eq!(checksum, seahash::hash(&expected));
        assert_eq!(store.read_artifact(&location).unwrap(), expected);
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
        let workspace_dir;
        {
            let mut writer = store.create_package_writer(file_id).unwrap();
            writer.append_artifact(b"orphan").unwrap();
            final_path = writer.final_path().to_path_buf();
            staging_path = writer.staging_path().to_path_buf();
            workspace_dir = writer.workspace_dir().to_path_buf();
            assert!(staging_path.exists());
        }

        assert!(!staging_path.exists());
        assert!(!workspace_dir.exists());
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
    fn sidecar_reader_cache_uses_physical_artifact_and_format_identity() {
        let _metrics_guard = crate::metrics::storage_metrics_test_guard();
        const TEST_CODEC: &str = "test-cache-physical-identity";
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
        second_writer.finalize().unwrap();

        let cache = SidecarReaderCache::new(store.clone());
        let first = cache
            .open(SidecarReaderRequest {
                location: &first_location,
                artifact_format_version: 1,
                provider: SearchIndexKind::FullText,
                codec: TEST_CODEC,
                integrity: SidecarIntegrityPolicy::EnvelopeChecksum,
            })
            .unwrap();
        let second = cache
            .open(SidecarReaderRequest {
                location: &second_location,
                artifact_format_version: 1,
                provider: SearchIndexKind::FullText,
                codec: TEST_CODEC,
                integrity: SidecarIntegrityPolicy::EnvelopeChecksum,
            })
            .unwrap();

        assert!(!std::sync::Arc::ptr_eq(&first, &second));
        assert_eq!(first.bytes(), b"cache-me");
        assert_eq!(second.bytes(), b"cache-me");
        assert_eq!(cache.len(), 2);

        let second_other_format = cache
            .open(SidecarReaderRequest {
                location: &second_location,
                artifact_format_version: 2,
                provider: SearchIndexKind::FullText,
                codec: TEST_CODEC,
                integrity: SidecarIntegrityPolicy::EnvelopeChecksum,
            })
            .unwrap();
        assert!(!std::sync::Arc::ptr_eq(&second, &second_other_format));
        assert_eq!(cache.len(), 3);

        let snapshot = storage_metrics().snapshot();
        let series = snapshot
            .search_sidecar_reader_by_key
            .iter()
            .find(|series| {
                series.key.provider == SearchIndexKind::FullText && series.key.codec == TEST_CODEC
            })
            .expect("physical identity test sidecar reader metrics");
        assert_eq!(series.key.provider, SearchIndexKind::FullText);
        assert_eq!(series.key.codec, TEST_CODEC);
        assert_eq!(series.counters.format_dispatch_total, 3);
        assert_eq!(series.counters.cache_misses_total, 3);
        assert_eq!(series.counters.cache_hits_total, 0);
        assert_eq!(series.counters.open_count_total, 2);
        assert_eq!(series.counters.mmap_bytes, 16);
    }

    #[test]
    #[serial_test::serial]
    fn reader_runtime_reuses_typed_reader_and_evicts_with_physical_package() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        const TEST_CODEC: &str = "test-typed-reader-cache";
        let _metrics_guard = crate::metrics::storage_metrics_test_guard();
        storage_metrics().reset_for_tests();
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = SidecarArtifactStore::new(temp_dir.path());
        let file_id = SidecarArtifactStore::default_shard_file_id(31, 7);
        let mut writer = store.create_package_writer(file_id).unwrap();
        let location = writer.append_artifact(b"typed-reader").unwrap();
        writer.finalize().unwrap();

        let runtime = SearchReaderRuntime::new(store);
        let request = DecodedSidecarReaderRequest {
            sidecar: SidecarReaderRequest {
                location: &location,
                artifact_format_version: 3,
                provider: SearchIndexKind::Hnsw,
                codec: TEST_CODEC,
                integrity: SidecarIntegrityPolicy::SelfValidatingArtifact,
            },
        };
        let builds = AtomicUsize::new(0);
        let first = runtime
            .get_or_try_open_decoded(request, |_| {
                builds.fetch_add(1, Ordering::Relaxed);
                Ok(Some(String::from("decoded")))
            })
            .unwrap()
            .unwrap();
        let second = runtime
            .get_or_try_open_decoded(request, |_| {
                builds.fetch_add(1, Ordering::Relaxed);
                Ok(Some(String::from("must-not-run")))
            })
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(builds.load(Ordering::Relaxed), 1);
        assert_eq!(runtime.decoded_len(), 1);
        // The typed hit bypasses both locks in the lower-level mmap cache.
        assert_eq!(runtime.sidecars.len(), 1);
        assert_eq!(runtime.sidecars.package_count(), 1);
        let snapshot = storage_metrics().snapshot();
        let series = snapshot
            .search_sidecar_reader_by_key
            .iter()
            .find(|series| {
                series.key.provider == SearchIndexKind::Hnsw && series.key.codec == TEST_CODEC
            })
            .expect("typed reader physical-cache metrics");
        assert_eq!(series.counters.format_dispatch_total, 1);
        assert_eq!(series.counters.cache_misses_total, 1);
        assert_eq!(series.counters.cache_hits_total, 0);
        assert_eq!(series.counters.open_count_total, 1);

        runtime.evict_packages(&BTreeSet::from([file_id]));
        assert_eq!(runtime.decoded_len(), 0);
        assert!(runtime.sidecars.is_empty());
        assert_eq!(runtime.sidecars.package_count(), 0);
        assert_eq!(first.as_str(), "decoded");
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
                integrity: SidecarIntegrityPolicy::EnvelopeChecksum,
            })
            .unwrap();
        let second = cache
            .open(SidecarReaderRequest {
                location: &second_location,
                artifact_format_version: 1,
                provider: SearchIndexKind::Sparse,
                codec: TEST_CODEC,
                integrity: SidecarIntegrityPolicy::EnvelopeChecksum,
            })
            .unwrap();
        let first_again = cache
            .open(SidecarReaderRequest {
                location: &first_location,
                artifact_format_version: 1,
                provider: SearchIndexKind::Sparse,
                codec: TEST_CODEC,
                integrity: SidecarIntegrityPolicy::EnvelopeChecksum,
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
        assert_eq!(series.counters.mmap_bytes, 79);
    }
}
