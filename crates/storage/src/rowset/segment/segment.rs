// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Segment
//!
//! Core Segment structure managing column readers and runtime state.

use super::segment_delete_vector::CachedDeleteVector;
use super::segment_format::{ColumnMeta, SegmentFooter};
use super::segment_indexes::{SegmentIndexStats, SegmentIndexes};
use super::segment_iterator::SegmentIterator;
use crate::buffer::{PageCache, Prefetcher};
use crate::codec::physical_layout::fixed_row_width;
use crate::index::short_key::ShortKeyIndexDecoder;
use crate::metrics::storage_metrics;
use crate::rowset::column::{
    ColumnBatch, ColumnIterator, ColumnReader, ColumnReaderMeta, ColumnReaderOptions,
    SharedColumnReader,
};
use crate::rowset::page::CompressionType;
use crate::rowset::page_reader::PageReader;
use crate::rowset::scan_cost::ScanAccessCostModel;
use crate::rowset::segment_statistics::SegmentStatistics;
use crate::tablet::{ColumnId, TabletSchemaRef};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use paro_common::error::{self as paro_error, Result};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};

/// Segment options for reading.
#[derive(Debug, Clone)]
pub struct SegmentOptions {
    pub verify_checksum: bool,
    pub compression: CompressionType,
    pub column_ids: Option<Vec<ColumnId>>,
    pub predicates: Vec<()>,
    pub page_cache: Option<Arc<PageCache>>,
    pub cache_decompressed: bool,
    pub cache_decoded: bool,
    pub parallel_decompressor: Option<crate::compression::ParallelDecompressor>,
    pub scan_access_cost: ScanAccessCostModel,
}

impl Default for SegmentOptions {
    fn default() -> Self {
        Self {
            verify_checksum: true,
            compression: CompressionType::Lz4,
            column_ids: None,
            predicates: Vec::new(),
            page_cache: None,
            cache_decompressed: false,
            cache_decoded: false,
            parallel_decompressor: None,
            scan_access_cost: ScanAccessCostModel::default(),
        }
    }
}

impl SegmentOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_verify_checksum(mut self, verify: bool) -> Self {
        self.verify_checksum = verify;
        self
    }

    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_columns(mut self, column_ids: Vec<ColumnId>) -> Self {
        self.column_ids = Some(column_ids);
        self
    }

    pub fn with_page_cache(mut self, cache: Arc<PageCache>) -> Self {
        self.page_cache = Some(cache);
        self
    }

    pub fn with_cache_decompressed(mut self, enable: bool) -> Self {
        self.cache_decompressed = enable;
        self
    }

    pub fn with_cache_decoded(mut self, enable: bool) -> Self {
        self.cache_decoded = enable;
        self
    }

    pub fn with_parallel_decompressor(
        mut self,
        decompressor: crate::compression::ParallelDecompressor,
    ) -> Self {
        self.parallel_decompressor = Some(decompressor);
        self
    }

    pub fn with_scan_access_cost(mut self, model: ScanAccessCostModel) -> Self {
        self.scan_access_cost = model;
        self
    }

    pub(crate) fn runtime_equivalent(&self, other: &Self) -> bool {
        self.verify_checksum == other.verify_checksum
            && self.compression == other.compression
            && self.column_ids == other.column_ids
            && self.predicates.len() == other.predicates.len()
            && self.cache_decompressed == other.cache_decompressed
            && self.cache_decoded == other.cache_decoded
            && self.scan_access_cost == other.scan_access_cost
            && match (&self.page_cache, &other.page_cache) {
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                (None, None) => true,
                _ => false,
            }
            && match (&self.parallel_decompressor, &other.parallel_decompressor) {
                (Some(left), Some(right)) => left.runtime_equivalent(right),
                (None, None) => true,
                _ => false,
            }
    }
}

/// Segment metadata (lightweight, for management).
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub segment_id: u32,
    pub num_rows: u64,
    pub file_size: u64,
    pub num_columns: u32,
}

impl SegmentMeta {
    pub fn new(segment_id: u32) -> Self {
        Self {
            segment_id,
            num_rows: 0,
            file_size: 0,
            num_columns: 0,
        }
    }
}

pub struct Segment {
    pub(super) tablet_id: u64,
    pub(super) rowset_id: u64,
    pub(super) rowset_gen: u64,
    pub(super) segment_id: u32,
    pub(super) file_path: PathBuf,
    pub(super) schema: TabletSchemaRef,
    pub(super) footer: SegmentFooter,
    pub(super) meta: SegmentMeta,
    pub(super) statistics: Option<SegmentStatistics>,
    pub(super) column_readers: RwLock<HashMap<ColumnId, Arc<SharedColumnReader<PositionedFile>>>>,
    /// One immutable file handle per loaded segment. Column iterators own only
    /// a logical cursor and use positioned reads, so independent scan morsels
    /// neither reopen the file nor share a mutable OS seek position.
    pub(super) shared_file: Arc<Mutex<Option<Arc<File>>>>,
    pub(super) short_key_index_decoder: Arc<RwLock<Option<Arc<ShortKeyIndexDecoder>>>>,
    pub(super) indexes: Arc<SegmentIndexes>,
    pub(super) index_stats: Arc<SegmentIndexStats>,
    pub(super) options: SegmentOptions,
    pub(super) page_reader: PageReader,
    pub(super) delete_vector_cache: Arc<ArcSwapOption<CachedDeleteVector>>,
    #[cfg(test)]
    pub(super) delete_vector_load_requests: AtomicU64,
}

/// An independently seekable cursor over a shared immutable segment file.
///
/// `File::try_clone` duplicates the descriptor but may retain the same open
/// file description and therefore the same seek offset. Analytical scans use
/// many column iterators concurrently, so the cursor must live in userspace
/// and every read must name its physical offset explicitly.
#[derive(Debug)]
pub struct PositionedFile {
    file: Arc<File>,
    position: u64,
}

impl PositionedFile {
    pub(super) fn new(file: Arc<File>) -> Self {
        Self { file, position: 0 }
    }

    fn position_with_delta(base: u64, delta: i64) -> std::io::Result<u64> {
        if delta >= 0 {
            base.checked_add(delta as u64)
        } else {
            base.checked_sub(delta.unsigned_abs())
        }
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid seek"))
    }

    #[cfg(unix)]
    fn read_positioned(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        use std::os::unix::fs::FileExt;
        self.file.read_at(buf, self.position)
    }

    #[cfg(windows)]
    fn read_positioned(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        use std::os::windows::fs::FileExt;
        self.file.seek_read(buf, self.position)
    }
}

impl Clone for PositionedFile {
    fn clone(&self) -> Self {
        Self {
            file: Arc::clone(&self.file),
            position: self.position,
        }
    }
}

impl Read for PositionedFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.read_positioned(buf)?;
        self.position = self.position.checked_add(read as u64).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "file position overflow")
        })?;
        Ok(read)
    }
}

impl Seek for PositionedFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.position = match pos {
            SeekFrom::Start(position) => position,
            SeekFrom::Current(delta) => Self::position_with_delta(self.position, delta)?,
            SeekFrom::End(delta) => Self::position_with_delta(self.file.metadata()?.len(), delta)?,
        };
        Ok(self.position)
    }
}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Segment")
            .field("segment_id", &self.segment_id)
            .field("tablet_id", &self.tablet_id)
            .field("rowset_id", &self.rowset_id)
            .field("rowset_gen", &self.rowset_gen)
            .field("file_path", &self.file_path)
            .field("num_rows", &self.footer.num_rows)
            .field("num_columns", &self.footer.column_metas.len())
            .finish()
    }
}

impl Segment {
    /// Create a query-owned runtime view over this segment's immutable
    /// structure. The footer, indexes, mmap-backed vector artifacts and file
    /// descriptor are shared; page readers and column-reader caches are scoped
    /// to the supplied runtime resources.
    pub(crate) fn runtime_view(&self, options: SegmentOptions) -> Self {
        let page_reader = PageReader::new(
            crate::rowset::page_reader::PageReaderContext::new(
                self.tablet_id,
                self.rowset_id,
                self.rowset_gen,
                self.segment_id,
            ),
            options.page_cache.clone(),
            crate::rowset::page_reader::PageReaderOptions {
                cache_decompressed: options.cache_decompressed,
                cache_decoded: options.cache_decoded,
                parallel_decompressor: options.parallel_decompressor.clone(),
            },
        );
        Self {
            tablet_id: self.tablet_id,
            rowset_id: self.rowset_id,
            rowset_gen: self.rowset_gen,
            segment_id: self.segment_id,
            file_path: self.file_path.clone(),
            schema: self.schema.clone(),
            footer: self.footer.clone(),
            meta: self.meta.clone(),
            statistics: self.statistics.clone(),
            column_readers: RwLock::new(HashMap::new()),
            shared_file: Arc::clone(&self.shared_file),
            short_key_index_decoder: Arc::clone(&self.short_key_index_decoder),
            indexes: Arc::clone(&self.indexes),
            index_stats: Arc::clone(&self.index_stats),
            options,
            page_reader,
            delete_vector_cache: Arc::clone(&self.delete_vector_cache),
            #[cfg(test)]
            delete_vector_load_requests: AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    pub(crate) fn shares_structural_state_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared_file, &other.shared_file)
            && Arc::ptr_eq(&self.indexes, &other.indexes)
            && Arc::ptr_eq(&self.index_stats, &other.index_stats)
            && Arc::ptr_eq(&self.delete_vector_cache, &other.delete_vector_cache)
    }

    #[cfg(test)]
    pub(crate) fn uses_page_cache(&self, cache: Option<&Arc<PageCache>>) -> bool {
        match (&self.options.page_cache, cache) {
            (Some(actual), Some(expected)) => Arc::ptr_eq(actual, expected),
            (None, None) => true,
            _ => false,
        }
    }

    fn shared_file_reader(&self) -> Result<PositionedFile> {
        let mut shared = self.shared_file.lock().map_err(|_| {
            paro_error::internal(format!(
                "segment {} shared file lock is poisoned",
                self.segment_id
            ))
        })?;
        if shared.is_none() {
            let file = File::open(&self.file_path).map_err(|error| {
                paro_error::io_error(format!(
                    "Failed to open segment file {:?}: {error}",
                    self.file_path
                ))
            })?;
            storage_metrics().record_segment_file_open();
            *shared = Some(Arc::new(file));
        }
        Ok(PositionedFile::new(Arc::clone(
            shared.as_ref().expect("shared segment file initialized"),
        )))
    }

    /// Get segment-level statistics if available.
    pub fn statistics(&self) -> Option<&SegmentStatistics> {
        self.statistics.as_ref()
    }

    /// Set segment-level statistics (used by writers).
    pub fn set_statistics(&mut self, stats: SegmentStatistics) {
        self.statistics = Some(stats);
    }

    pub(super) fn rowset_path(&self) -> Result<&Path> {
        self.file_path
            .parent()
            .ok_or_else(|| paro_error::internal("Segment file path has no parent"))
    }

    pub fn segment_id(&self) -> u32 {
        self.segment_id
    }

    pub fn num_rows(&self) -> u64 {
        self.footer.num_rows
    }

    pub fn num_columns(&self) -> usize {
        self.footer.column_metas.len()
    }

    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    pub fn schema(&self) -> &TabletSchemaRef {
        &self.schema
    }

    pub fn footer(&self) -> &SegmentFooter {
        &self.footer
    }

    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    pub fn is_empty(&self) -> bool {
        self.footer.num_rows == 0
    }

    pub fn mem_usage(&self) -> usize {
        let base_size = std::mem::size_of::<Self>()
            + self.footer.column_metas.len() * std::mem::size_of::<ColumnMeta>();
        let readers_size = self.column_readers.read().unwrap().len() * 1024;
        let short_key_size = self
            .short_key_index_decoder
            .read()
            .unwrap()
            .as_ref()
            .map(|decoder| decoder.mem_usage())
            .unwrap_or(0);
        base_size + readers_size + short_key_size
    }

    pub fn data_size(&self) -> u64 {
        self.footer
            .column_metas
            .iter()
            .map(|m| m.data_page_pointer.size as u64)
            .sum()
    }

    pub fn index_size(&self) -> u64 {
        let column_index_size: u64 = self
            .footer
            .column_metas
            .iter()
            .map(|m| {
                let ordinal = m.ordinal_index_pointer.size as u64;
                let zonemap = m.zonemap_index_pointer.size as u64;
                let dict = m.dict_page_pointer.map(|p| p.size as u64).unwrap_or(0);
                let bloom = m.bloom_filter_pointer.map(|p| p.size as u64).unwrap_or(0);
                let bitmap = m.bitmap_index_pointer.map(|p| p.size as u64).unwrap_or(0);
                let hnsw = m.hnsw_index_pointer.map(|p| p.size as u64).unwrap_or(0);
                let sparse = m.sparse_index_pointer.map(|p| p.size as u64).unwrap_or(0);
                let fulltext = m.fulltext_index_pointer.map(|p| p.size as u64).unwrap_or(0);
                ordinal + zonemap + dict + bloom + bitmap + hnsw + sparse + fulltext
            })
            .sum();

        let short_key_size = self
            .footer
            .short_key_index_pointer
            .map(|p| p.size as u64)
            .unwrap_or(0);

        column_index_size + short_key_size
    }

    pub fn file_size(&self) -> u64 {
        self.meta.file_size
    }

    pub fn get_column_meta(&self, column_id: ColumnId) -> Option<&ColumnMeta> {
        self.footer
            .column_metas
            .iter()
            .find(|m| m.column_id == column_id)
    }

    pub fn column_metas(&self) -> &[ColumnMeta] {
        &self.footer.column_metas
    }

    pub fn read_by_rowids(
        &self,
        column_ids: &[ColumnId],
        row_offsets: &[u32],
    ) -> Result<Vec<(ColumnId, ColumnBatch)>> {
        if row_offsets.is_empty() {
            return Ok(Vec::new());
        }

        let ordinals: Vec<u64> = row_offsets.iter().map(|&o| o as u64).collect();
        // A late materialization batch is commonly a small TopN result.  In
        // that shape each column performs only a handful of page-local point
        // reads, so dispatching one Rayon job per column costs more than the
        // decode itself.  Keep small batches on the query worker and reserve
        // column-parallel reads for enough independent cell work to amortize
        // scheduler handoff.  This is deliberately based on total work, not a
        // query or schema special case.
        let cell_count = column_ids.len().saturating_mul(row_offsets.len());
        if column_ids.len() > 1 && cell_count >= paro_common::vector::VECTOR_SIZE {
            return column_ids
                .par_iter()
                .map(|&col_id| {
                    let mut iter = self.new_column_iterator(col_id)?;
                    let batch = iter.read_by_rowids(&ordinals)?;
                    Ok((col_id, batch))
                })
                .collect();
        }
        column_ids
            .iter()
            .map(|&col_id| {
                let mut iter = self.new_column_iterator(col_id)?;
                let batch = iter.read_by_rowids(&ordinals)?;
                Ok((col_id, batch))
            })
            .collect()
    }

    pub fn new_column_iterator(
        &self,
        column_id: ColumnId,
    ) -> Result<Box<dyn ColumnIterator + Send + Sync>> {
        self.new_column_iterator_with_prefetcher(column_id, None)
    }

    pub fn new_column_iterator_with_prefetcher(
        &self,
        column_id: ColumnId,
        prefetcher: Option<Arc<Prefetcher>>,
    ) -> Result<Box<dyn ColumnIterator + Send + Sync>> {
        {
            let readers = self.column_readers.read().unwrap();
            if let Some(reader) = readers.get(&column_id) {
                return reader.new_iterator(
                    self.shared_file_reader()?,
                    prefetcher,
                    Some(self.file_path.clone()),
                );
            }
        }

        let mut readers = self.column_readers.write().unwrap();
        if let Some(reader) = readers.get(&column_id) {
            return reader.new_iterator(
                self.shared_file_reader()?,
                prefetcher,
                Some(self.file_path.clone()),
            );
        }

        let col_meta = self.get_column_meta(column_id).ok_or_else(|| {
            paro_error::invalid_input(format!("Column {} not found in segment", column_id))
        })?;

        let file = self.shared_file_reader()?;

        let logical_type = self
            .schema
            .column_by_id(column_id)
            .map(|col| &col.logical_type);
        let reader_meta = ColumnReaderMeta {
            column_id: col_meta.column_id,
            num_rows: col_meta.num_rows,
            encoding: col_meta.encoding,
            compression: col_meta.compression,
            field_type: col_meta.field_type,
            data_page_pointer: col_meta.data_page_pointer,
            ordinal_index_pointer: col_meta.ordinal_index_pointer,
            zonemap_index_pointer: col_meta.zonemap_index_pointer,
            dict_page_pointer: col_meta.dict_page_pointer,
            is_nullable: col_meta.is_nullable,
            null_count: col_meta.null_count,
            type_size: logical_type
                .and_then(|logical_type| fixed_row_width(logical_type).ok())
                .or_else(|| col_meta.field_type.size()),
        };

        let reader_opts = ColumnReaderOptions {
            verify_checksum: self.options.verify_checksum,
            compression: col_meta.compression,
        };

        let mut column_reader = ColumnReader::create(
            reader_meta,
            file.clone(),
            reader_opts,
            self.page_reader.clone(),
            prefetcher.clone(),
            Some(self.file_path.clone()),
        )?;

        let _ = column_reader.new_iterator()?;
        let shared_reader = column_reader.into_shared();
        readers.insert(column_id, shared_reader.clone());
        shared_reader.new_iterator(file, prefetcher, Some(self.file_path.clone()))
    }

    pub fn load_short_key_index(&self) -> Result<()> {
        if self.short_key_index_decoder.read().unwrap().is_some() {
            return Ok(());
        }

        if let Some(ptr) = self.footer.short_key_index_pointer {
            let footer = self.footer.short_key_index_footer.as_ref().ok_or_else(|| {
                paro_error::data_corrupted("Short key footer missing for short key page")
            })?;
            let mut file = self.shared_file_reader()?;
            file.seek(SeekFrom::Start(ptr.offset)).map_err(|e| {
                paro_error::io_error(format!("Failed to seek to short key index: {}", e))
            })?;

            let mut buf = vec![0u8; ptr.size as usize];
            file.read_exact(&mut buf).map_err(|e| {
                paro_error::io_error(format!("Failed to read short key index: {}", e))
            })?;

            let decoder = ShortKeyIndexDecoder::parse(&Bytes::from(buf), footer)?;
            *self.short_key_index_decoder.write().unwrap() = Some(Arc::new(decoder));
        }

        Ok(())
    }

    pub fn short_key_index(&self) -> Result<Option<Arc<ShortKeyIndexDecoder>>> {
        self.load_short_key_index()?;
        Ok(self.short_key_index_decoder.read().unwrap().clone())
    }

    pub fn new_iterator(&self) -> Result<SegmentIterator> {
        let column_ids: Vec<ColumnId> = if let Some(ids) = &self.options.column_ids {
            ids.clone()
        } else {
            self.footer
                .column_metas
                .iter()
                .map(|m| m.column_id)
                .collect()
        };
        SegmentIterator::new(self, column_ids)
    }

    pub fn new_iterator_with_columns(&self, column_ids: Vec<ColumnId>) -> Result<SegmentIterator> {
        SegmentIterator::new(self, column_ids)
    }
}

pub type SegmentSharedPtr = Arc<Segment>;
