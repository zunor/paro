// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Segment-backed WAL handle used by transactions and checkpoint coordination.

use crate::compaction::publish::record::CompactionPublishRecord;
use crate::wal::wal_entry::WalHeaderMetadata;
use crate::wal::wal_reader::WalReader;
use crate::wal::wal_write_state::WalWriteState;
use crate::wal::wal_writer::{WalInitState, WalWriter};
use paro_common::error::{self as paro_error, Result};
use paro_journal::segments::{
    should_rotate_after_flush, SegmentCatalog, SegmentCatalogStore, DEFAULT_SEGMENT_ROTATION_BYTES,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

struct WriteAheadLogState {
    catalog: SegmentCatalog,
    active_writer: Arc<WalWriter>,
    sealed_size_bytes: u64,
}

/// Segment-backed write-ahead log for durability.
pub struct WriteAheadLog {
    seed_path: PathBuf,
    catalog_store: SegmentCatalogStore,
    rotation_bytes: u64,
    state: Mutex<WriteAheadLogState>,
    header_metadata: WalHeaderMetadata,
}

impl WriteAheadLog {
    /// Create a new WAL at the specified seed path.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::new_with_header_metadata(path, WalHeaderMetadata::default())
    }

    /// Create a new WAL with explicit header metadata.
    pub fn new_with_header_metadata<P: AsRef<Path>>(
        path: P,
        header_metadata: WalHeaderMetadata,
    ) -> Result<Self> {
        Self::with_state_and_start_lsn(path, WalInitState::Uninitialized, header_metadata, 1)
    }

    /// Create a WAL with a specific initial state.
    pub fn with_state<P: AsRef<Path>>(path: P, state: WalInitState) -> Result<Self> {
        Self::with_state_and_header_metadata(path, state, WalHeaderMetadata::default())
    }

    /// Create a WAL with explicit state and header metadata.
    pub fn with_state_and_header_metadata<P: AsRef<Path>>(
        path: P,
        state: WalInitState,
        header_metadata: WalHeaderMetadata,
    ) -> Result<Self> {
        Self::with_state_and_start_lsn(path, state, header_metadata, 1)
    }

    /// Create a WAL with an explicit initial LSN floor used when a fresh segment
    /// catalog must be bootstrapped.
    pub fn with_state_and_start_lsn<P: AsRef<Path>>(
        path: P,
        state: WalInitState,
        header_metadata: WalHeaderMetadata,
        initial_start_lsn: u64,
    ) -> Result<Self> {
        let seed_path = path.as_ref().to_path_buf();
        let catalog_store = SegmentCatalogStore::from_seed_path(&seed_path);
        let catalog = catalog_store.load_or_create(initial_start_lsn)?;
        let active_segment_path = catalog_store
            .layout()
            .segment_path(catalog.active_segment_id);
        let actual_metadata =
            Self::load_active_header_metadata(&catalog_store, &catalog)?.unwrap_or(header_metadata);
        let active_writer = Arc::new(WalWriter::with_header_metadata(
            &active_segment_path,
            state,
            actual_metadata,
        ));
        let sealed_size_bytes = Self::sealed_size_bytes(&catalog_store, &catalog)?;

        Ok(Self {
            seed_path,
            catalog_store,
            rotation_bytes: DEFAULT_SEGMENT_ROTATION_BYTES,
            state: Mutex::new(WriteAheadLogState {
                catalog,
                active_writer,
                sealed_size_bytes,
            }),
            header_metadata: actual_metadata,
        })
    }

    pub fn exists_for_seed<P: AsRef<Path>>(path: P) -> bool {
        SegmentCatalogStore::from_seed_path(path).exists()
    }

    fn load_active_header_metadata(
        store: &SegmentCatalogStore,
        catalog: &SegmentCatalog,
    ) -> Result<Option<WalHeaderMetadata>> {
        let Some(active) = catalog.active_segment() else {
            return Ok(None);
        };
        let segment_path = store.layout().segment_path(active.segment_id);
        let Some(mut reader) = WalReader::open(&segment_path)? else {
            return Ok(None);
        };
        reader.ensure_header_read()?;
        Ok(Some(reader.header_metadata()))
    }

    fn sealed_size_bytes(store: &SegmentCatalogStore, catalog: &SegmentCatalog) -> Result<u64> {
        let mut total = 0u64;
        for segment in &catalog.segments {
            if segment.segment_id == catalog.active_segment_id {
                continue;
            }
            let segment_path = store.layout().segment_path(segment.segment_id);
            total = total.saturating_add(
                segment_path
                    .metadata()
                    .map(|metadata| metadata.len())
                    .unwrap_or(0),
            );
        }
        Ok(total)
    }

    fn active_writer(&self) -> Arc<WalWriter> {
        Arc::clone(&self.state.lock().unwrap().active_writer)
    }

    pub fn begin_write(&self) -> WalWriteState {
        WalWriteState::new(self.active_writer())
    }

    pub fn flush(&self) -> Result<()> {
        self.active_writer().flush()
    }

    pub fn note_flushed_lsn(&self, flushed_lsn: u64) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let active_size = state.active_writer.file_size();
        if !should_rotate_after_flush(active_size, self.rotation_bytes) {
            return Ok(());
        }

        let Some(active_entry) = state.catalog.active_segment_mut() else {
            return Err(paro_error::internal(
                "segment catalog has no active segment during rotation",
            ));
        };
        active_entry.sealed_end_lsn = Some(flushed_lsn);
        let next_segment = state
            .catalog
            .append_rotated_segment(flushed_lsn.saturating_add(1));
        self.catalog_store.save(&state.catalog)?;
        state.sealed_size_bytes = Self::sealed_size_bytes(&self.catalog_store, &state.catalog)?;
        state.active_writer = Arc::new(WalWriter::with_header_metadata(
            self.catalog_store
                .layout()
                .segment_path(next_segment.segment_id),
            WalInitState::NoWal,
            self.header_metadata,
        ));
        Ok(())
    }

    pub fn write_rowset_commit(
        &self,
        tablet_id: u64,
        rowset_id: u64,
        start_version: i64,
        end_version: i64,
        rowset_path: &str,
    ) -> Result<()> {
        self.active_writer().write_rowset_commit(
            tablet_id,
            rowset_id,
            start_version,
            end_version,
            rowset_path,
        )
    }

    pub fn write_compaction_publish(&self, record: &CompactionPublishRecord) -> Result<()> {
        self.active_writer().write_compaction_publish(record)
    }

    pub fn truncate(&self, size: u64) -> Result<()> {
        self.active_writer().truncate(size)
    }

    pub fn file_size(&self) -> u64 {
        let state = self.state.lock().unwrap();
        state
            .sealed_size_bytes
            .saturating_add(state.active_writer.file_size())
    }

    pub fn total_written(&self) -> u64 {
        self.file_size()
    }

    pub fn path(&self) -> &Path {
        &self.seed_path
    }

    pub fn is_initialized(&self) -> bool {
        self.active_writer().is_initialized()
    }

    pub fn writer(&self) -> Arc<WalWriter> {
        self.active_writer()
    }

    pub fn header_metadata(&self) -> WalHeaderMetadata {
        self.header_metadata
    }

    pub fn segment_catalog_snapshot(&self) -> SegmentCatalog {
        self.state.lock().unwrap().catalog.clone()
    }

    pub fn segment_id_for_lsn(&self, lsn: u64) -> Result<u64> {
        let state = self.state.lock().unwrap();
        state
            .catalog
            .segment_for_replay_lsn(lsn)
            .map(|segment| segment.segment_id)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "segment catalog at {} has no segment for replay lsn {}",
                    self.catalog_store.layout().catalog_path().display(),
                    lsn
                ))
            })
    }

    pub fn catalog_path(&self) -> &Path {
        self.catalog_store.layout().catalog_path()
    }

    pub fn segments_dir(&self) -> &Path {
        self.catalog_store.layout().segments_dir()
    }
}

impl std::fmt::Debug for WriteAheadLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteAheadLog")
            .field("path", &self.seed_path)
            .field("catalog_path", &self.catalog_path())
            .field("initialized", &self.is_initialized())
            .field("file_size", &self.file_size())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::test_support::write_flushed_create_schema_txn;
    use tempfile::tempdir;

    #[test]
    fn test_write_ahead_log_create() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::new(&path).unwrap();
        assert!(!wal.is_initialized());
        assert_eq!(wal.file_size(), 0);
        assert!(wal.catalog_path().exists());
    }

    #[test]
    fn test_write_ahead_log_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::new(&path).unwrap();

        write_flushed_create_schema_txn(wal.writer().as_ref(), "default", "test_schema", 1, 100)
            .unwrap();
        wal.note_flushed_lsn(1).unwrap();

        assert!(wal.is_initialized());
        assert!(wal.file_size() > 0);
        assert_eq!(wal.segment_id_for_lsn(1).unwrap(), 1);
    }

    #[test]
    fn rotate_after_flush_publishes_new_segment_to_catalog() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::with_state_and_start_lsn(
            &path,
            WalInitState::Uninitialized,
            WalHeaderMetadata::default(),
            9,
        )
        .unwrap();

        write_flushed_create_schema_txn(wal.writer().as_ref(), "default", "schema1", 1, 100)
            .unwrap();
        {
            let state = wal.state.lock().unwrap();
            state
                .active_writer
                .truncate(DEFAULT_SEGMENT_ROTATION_BYTES)
                .unwrap();
        }
        wal.note_flushed_lsn(9).unwrap();

        let catalog = wal.segment_catalog_snapshot();
        assert_eq!(catalog.segments.len(), 2);
        assert_eq!(catalog.active_segment_id, 2);
        assert_eq!(
            catalog
                .segments
                .iter()
                .find(|segment| segment.segment_id == 1)
                .unwrap()
                .sealed_end_lsn,
            Some(9)
        );
        assert_eq!(wal.segment_id_for_lsn(10).unwrap(), 2);
    }
}
