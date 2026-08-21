// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::segment::{Segment, SegmentMeta, SegmentOptions};
use super::segment_format::{ColumnMeta, SegmentFooter};
use super::segment_indexes::{
    SegmentIndexStats, SegmentIndexes, SegmentPredicateIndexes, SegmentSearchIndexes,
};
use crate::index::fulltext::text_index::FullTextIndex;
use crate::index::hnsw::HnswIndex;
use crate::index::sparse::SparseVectorIndex;
use crate::index::{
    BitmapIndex, BloomFilterIndex, IndexConstraintType, MmapVectorStorage, PageRange,
};
use crate::metrics::storage_metrics;
use crate::rowset::column::OrdinalIndexReader;
use crate::rowset::encoding::PLAIN_PAGE_HEADER_SIZE;
use crate::rowset::page::{CompressionType, PagePointer, PageReadOptions};
use crate::rowset::page_reader::{PageReader, PageReaderContext, PageReaderOptions};
use crate::rowset::segment_statistics::SegmentStatistics;
use crate::statistics::{
    split_stats_trailer, FullTextIndexStatistics, HnswIndexStatistics, SparseIndexStatistics,
};
use crate::tablet::{ColumnId, TabletSchemaRef};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

impl Segment {
    pub fn open(
        segment_id: u32,
        file_path: impl Into<PathBuf>,
        schema: TabletSchemaRef,
        options: SegmentOptions,
        tablet_id: u64,
        rowset_id: u64,
        rowset_gen: u64,
    ) -> Result<Self> {
        let file_path = file_path.into();
        let mut file = File::open(&file_path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to open segment file {:?}: {}",
                file_path, e
            ))
        })?;
        storage_metrics().record_segment_file_open();

        let file_size = file
            .metadata()
            .map_err(|e| paro_error::io_error(format!("Failed to get file metadata: {}", e)))?
            .len();

        file.seek(SeekFrom::End(-4))
            .map_err(|e| paro_error::io_error(format!("Failed to seek to footer size: {}", e)))?;

        let mut footer_size_buf = [0u8; 4];
        file.read_exact(&mut footer_size_buf)
            .map_err(|e| paro_error::io_error(format!("Failed to read footer size: {}", e)))?;
        let footer_size = u32::from_le_bytes(footer_size_buf) as usize;

        file.seek(SeekFrom::End(-(footer_size as i64)))
            .map_err(|e| paro_error::io_error(format!("Failed to seek to footer: {}", e)))?;

        let mut footer_buf = vec![0u8; footer_size - 4];
        file.read_exact(&mut footer_buf)
            .map_err(|e| paro_error::io_error(format!("Failed to read footer: {}", e)))?;

        let footer = SegmentFooter::deserialize(&footer_buf)?;
        let meta = SegmentMeta {
            segment_id,
            num_rows: footer.num_rows,
            file_size,
            num_columns: footer.column_metas.len() as u32,
        };

        let page_reader = PageReader::new(
            PageReaderContext::new(tablet_id, rowset_id, rowset_gen, segment_id),
            options.page_cache.clone(),
            PageReaderOptions {
                cache_decompressed: options.cache_decompressed,
                cache_decoded: options.cache_decoded,
                parallel_decompressor: options.parallel_decompressor.clone(),
            },
        );

        let bloom_filters = Self::load_bloom_filter_indexes(
            &mut file,
            &page_reader,
            segment_id,
            &footer,
            &schema,
            &options,
        )?;
        let bitmap_indexes = Self::load_bitmap_indexes(
            &mut file,
            &page_reader,
            segment_id,
            &footer,
            &schema,
            &options,
        )?;
        let (hnsw_indexes, hnsw_stats) = Self::load_hnsw_indexes(
            &mut file,
            &page_reader,
            &file_path,
            &footer,
            &schema,
            &options,
        )?;
        let (sparse_indexes, sparse_stats) =
            Self::load_sparse_indexes(&mut file, &page_reader, &footer, &options)?;
        let (fulltext_indexes, fulltext_stats) =
            Self::load_fulltext_indexes(&mut file, &page_reader, &footer, &options)?;
        let statistics =
            SegmentStatistics::from_column_metas(&footer.column_metas, footer.num_rows);

        Ok(Self {
            tablet_id,
            rowset_id,
            rowset_gen,
            segment_id,
            file_path,
            schema,
            footer,
            meta,
            statistics,
            column_readers: RwLock::new(HashMap::new()),
            shared_file: Mutex::new(Some(Arc::new(file))),
            short_key_index_decoder: RwLock::new(None),
            indexes: SegmentIndexes {
                predicate: SegmentPredicateIndexes {
                    bloom_filters,
                    bitmap_indexes,
                    runtime_art_indexes: RwLock::new(HashMap::new()),
                },
                search: SegmentSearchIndexes {
                    hnsw_indexes,
                    sparse_indexes,
                    fulltext_indexes,
                    runtime_fulltext_indexes: RwLock::new(HashMap::new()),
                },
            },
            index_stats: SegmentIndexStats {
                hnsw_stats,
                sparse_stats,
                fulltext_stats,
                runtime_fulltext_stats: RwLock::new(HashMap::new()),
            },
            options,
            page_reader,
            delete_vector_cache: arc_swap::ArcSwapOption::empty(),
            #[cfg(test)]
            delete_vector_load_requests: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub fn from_footer(
        segment_id: u32,
        file_path: impl Into<PathBuf>,
        schema: TabletSchemaRef,
        footer: SegmentFooter,
        options: SegmentOptions,
        tablet_id: u64,
        rowset_id: u64,
        rowset_gen: u64,
    ) -> Self {
        let file_path = file_path.into();
        let meta = SegmentMeta {
            segment_id,
            num_rows: footer.num_rows,
            file_size: 0,
            num_columns: footer.column_metas.len() as u32,
        };

        let page_reader = PageReader::new(
            PageReaderContext::new(tablet_id, rowset_id, rowset_gen, segment_id),
            options.page_cache.clone(),
            PageReaderOptions {
                cache_decompressed: options.cache_decompressed,
                cache_decoded: options.cache_decoded,
                parallel_decompressor: options.parallel_decompressor.clone(),
            },
        );

        Self {
            tablet_id,
            rowset_id,
            rowset_gen,
            segment_id,
            file_path,
            schema,
            statistics: SegmentStatistics::from_column_metas(&footer.column_metas, footer.num_rows),
            footer,
            meta,
            column_readers: RwLock::new(HashMap::new()),
            shared_file: Mutex::new(None),
            short_key_index_decoder: RwLock::new(None),
            indexes: SegmentIndexes::default(),
            index_stats: SegmentIndexStats::default(),
            options,
            page_reader,
            delete_vector_cache: arc_swap::ArcSwapOption::empty(),
            #[cfg(test)]
            delete_vector_load_requests: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn load_index_page_bodies_parallel<R: Read + Seek>(
        reader: &mut R,
        page_reader: &PageReader,
        pages: &[(PagePointer, CompressionType)],
        verify_checksum: bool,
    ) -> Result<Vec<bytes::Bytes>> {
        if pages.is_empty() {
            return Ok(Vec::new());
        }

        let opts: Vec<PageReadOptions> = pages
            .iter()
            .map(|(pointer, compression)| {
                PageReadOptions::new(*pointer)
                    .with_verify_checksum(verify_checksum)
                    .with_codec(*compression)
            })
            .collect();

        let loaded = page_reader.read_pages_parallel(reader, &opts)?;
        Ok(loaded
            .into_iter()
            .map(|(body, _footer, _uncompressed_size)| body)
            .collect())
    }

    fn build_page_ranges_from_ordinal_body(
        meta: &ColumnMeta,
        body: &bytes::Bytes,
    ) -> Result<Vec<PageRange>> {
        let mut ordinal = OrdinalIndexReader::from_bytes(body)?;
        ordinal.set_num_rows(meta.num_rows);

        let entries = ordinal.entries();
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut ranges = Vec::with_capacity(entries.len());
        for idx in 0..entries.len() {
            let start = entries[idx].first_ordinal;
            let end = if idx + 1 < entries.len() {
                entries[idx + 1].first_ordinal
            } else {
                meta.num_rows
            };
            let start_u32 = u32::try_from(start)
                .map_err(|_| paro_error::out_of_range("PageRange start exceeds u32 range"))?;
            let end_u32 = u32::try_from(end)
                .map_err(|_| paro_error::out_of_range("PageRange end exceeds u32 range"))?;
            if start_u32 < end_u32 {
                ranges.push(PageRange::new(start_u32, end_u32));
            }
        }
        Ok(ranges)
    }

    fn load_bloom_filter_indexes(
        file: &mut File,
        page_reader: &PageReader,
        segment_id: u32,
        footer: &SegmentFooter,
        schema: &TabletSchemaRef,
        options: &SegmentOptions,
    ) -> Result<HashMap<ColumnId, std::sync::Arc<BloomFilterIndex>>> {
        let mut indexes = HashMap::new();
        let bloom_metas: Vec<&ColumnMeta> = footer
            .column_metas
            .iter()
            .filter(|meta| meta.bloom_filter_pointer.is_some())
            .collect();
        if bloom_metas.is_empty() {
            return Ok(indexes);
        }

        let bloom_pages: Vec<(PagePointer, CompressionType)> = bloom_metas
            .iter()
            .map(|meta| {
                (
                    meta.bloom_filter_pointer
                        .expect("bloom pointer checked by filter"),
                    meta.compression,
                )
            })
            .collect();
        let ordinal_pages: Vec<(PagePointer, CompressionType)> = bloom_metas
            .iter()
            .map(|meta| (meta.ordinal_index_pointer, meta.compression))
            .collect();
        let bloom_bodies = Self::load_index_page_bodies_parallel(
            file,
            page_reader,
            &bloom_pages,
            options.verify_checksum,
        )?;
        let ordinal_bodies = Self::load_index_page_bodies_parallel(
            file,
            page_reader,
            &ordinal_pages,
            options.verify_checksum,
        )?;

        for ((meta, body), ordinal_body) in bloom_metas
            .into_iter()
            .zip(bloom_bodies.into_iter())
            .zip(ordinal_bodies.into_iter())
        {
            let column = schema.column_by_id(meta.column_id).ok_or_else(|| {
                paro_error::invalid_input(format!(
                    "Column ID {} not found for bloom filter index",
                    meta.column_id
                ))
            })?;
            let page_ranges = Self::build_page_ranges_from_ordinal_body(meta, &ordinal_body)?;
            let index = BloomFilterIndex::from_bytes(
                format!("bloom_{}_{}", segment_id, meta.column_id),
                IndexConstraintType::None,
                vec![meta.column_id],
                vec![column.logical_type.clone()],
                body,
                page_ranges,
            )?;
            indexes.insert(meta.column_id, std::sync::Arc::new(index));
        }
        Ok(indexes)
    }

    fn load_bitmap_indexes(
        file: &mut File,
        page_reader: &PageReader,
        segment_id: u32,
        footer: &SegmentFooter,
        schema: &TabletSchemaRef,
        options: &SegmentOptions,
    ) -> Result<HashMap<ColumnId, std::sync::Arc<BitmapIndex>>> {
        let mut indexes = HashMap::new();
        let bitmap_metas: Vec<&ColumnMeta> = footer
            .column_metas
            .iter()
            .filter(|meta| meta.bitmap_index_pointer.is_some())
            .collect();
        if bitmap_metas.is_empty() {
            return Ok(indexes);
        }

        let bitmap_pages: Vec<(PagePointer, CompressionType)> = bitmap_metas
            .iter()
            .map(|meta| {
                (
                    meta.bitmap_index_pointer
                        .expect("bitmap pointer checked by filter"),
                    meta.compression,
                )
            })
            .collect();
        let bitmap_bodies = Self::load_index_page_bodies_parallel(
            file,
            page_reader,
            &bitmap_pages,
            options.verify_checksum,
        )?;

        for (meta, body) in bitmap_metas.into_iter().zip(bitmap_bodies.into_iter()) {
            let column = schema.column_by_id(meta.column_id).ok_or_else(|| {
                paro_error::invalid_input(format!(
                    "Column ID {} not found for bitmap index",
                    meta.column_id
                ))
            })?;
            let index = BitmapIndex::from_bytes(
                format!("bitmap_{}_{}", segment_id, meta.column_id),
                IndexConstraintType::None,
                vec![meta.column_id],
                vec![column.logical_type.clone()],
                body,
            )?;
            indexes.insert(meta.column_id, std::sync::Arc::new(index));
        }
        Ok(indexes)
    }

    fn load_hnsw_indexes<R: Read + Seek>(
        reader: &mut R,
        page_reader: &PageReader,
        file_path: &Path,
        footer: &SegmentFooter,
        schema: &TabletSchemaRef,
        options: &SegmentOptions,
    ) -> Result<(
        HashMap<ColumnId, std::sync::Arc<HnswIndex>>,
        HashMap<ColumnId, HnswIndexStatistics>,
    )> {
        let mut indexes = HashMap::new();
        let mut stats_map = HashMap::new();
        let hnsw_metas: Vec<&ColumnMeta> = footer
            .column_metas
            .iter()
            .filter(|meta| meta.hnsw_index_pointer.is_some())
            .collect();
        if hnsw_metas.is_empty() {
            return Ok((indexes, stats_map));
        }

        let hnsw_pages: Vec<(PagePointer, CompressionType)> = hnsw_metas
            .iter()
            .map(|meta| {
                (
                    meta.hnsw_index_pointer
                        .expect("hnsw pointer checked by filter"),
                    meta.compression,
                )
            })
            .collect();
        let hnsw_bodies = Self::load_index_page_bodies_parallel(
            reader,
            page_reader,
            &hnsw_pages,
            options.verify_checksum,
        )?;

        for (meta, body) in hnsw_metas.into_iter().zip(hnsw_bodies.into_iter()) {
            let col = schema.column_by_id(meta.column_id).ok_or_else(|| {
                paro_error::column_not_found(format!(
                    "Column {} not found in schema",
                    meta.column_id
                ))
            })?;

            let dim = if let LogicalType::Array(_, d) = col.logical_type {
                d
            } else {
                return Err(paro_error::data_corrupted(format!(
                    "HNSW index on non-vector column {} with type {:?}",
                    meta.column_id, col.logical_type
                )));
            };

            let vector_storage = std::sync::Arc::new(MmapVectorStorage::open_range(
                file_path,
                meta.data_page_pointer.offset + PLAIN_PAGE_HEADER_SIZE as u64,
                meta.num_rows * dim as u64 * 4,
                dim,
            )?);

            let (stats_bytes, _) = split_stats_trailer(body.as_ref());
            let index = HnswIndex::deserialize(body.as_ref(), vector_storage)?;
            let stats = if let Some(bytes) = stats_bytes {
                HnswIndexStatistics::from_bytes(bytes)
                    .unwrap_or_else(|_| HnswIndexStatistics::collect(&index))
            } else {
                HnswIndexStatistics::collect(&index)
            };
            stats_map.insert(meta.column_id, stats);
            indexes.insert(meta.column_id, std::sync::Arc::new(index));
        }
        Ok((indexes, stats_map))
    }

    fn load_sparse_indexes<R: Read + Seek>(
        reader: &mut R,
        page_reader: &PageReader,
        footer: &SegmentFooter,
        options: &SegmentOptions,
    ) -> Result<(
        HashMap<ColumnId, std::sync::Arc<SparseVectorIndex>>,
        HashMap<ColumnId, SparseIndexStatistics>,
    )> {
        let mut indexes = HashMap::new();
        let mut stats_map = HashMap::new();
        let sparse_metas: Vec<&ColumnMeta> = footer
            .column_metas
            .iter()
            .filter(|meta| meta.sparse_index_pointer.is_some())
            .collect();
        if sparse_metas.is_empty() {
            return Ok((indexes, stats_map));
        }

        let sparse_pages: Vec<(PagePointer, CompressionType)> = sparse_metas
            .iter()
            .map(|meta| {
                (
                    meta.sparse_index_pointer
                        .expect("sparse pointer checked by filter"),
                    meta.compression,
                )
            })
            .collect();
        let sparse_bodies = Self::load_index_page_bodies_parallel(
            reader,
            page_reader,
            &sparse_pages,
            options.verify_checksum,
        )?;

        for (meta, body) in sparse_metas.into_iter().zip(sparse_bodies.into_iter()) {
            let (stats_bytes, _) = split_stats_trailer(body.as_ref());
            let index = SparseVectorIndex::deserialize(body.as_ref())?;
            let stats = if let Some(bytes) = stats_bytes {
                SparseIndexStatistics::from_bytes(bytes)
                    .unwrap_or_else(|_| SparseIndexStatistics::collect(&index))
            } else {
                SparseIndexStatistics::collect(&index)
            };
            stats_map.insert(meta.column_id, stats);
            indexes.insert(meta.column_id, std::sync::Arc::new(index));
        }
        Ok((indexes, stats_map))
    }

    fn load_fulltext_indexes<R: Read + Seek>(
        reader: &mut R,
        page_reader: &PageReader,
        footer: &SegmentFooter,
        options: &SegmentOptions,
    ) -> Result<(
        HashMap<ColumnId, std::sync::Arc<FullTextIndex>>,
        HashMap<ColumnId, FullTextIndexStatistics>,
    )> {
        let mut indexes = HashMap::new();
        let mut stats_map = HashMap::new();
        let fulltext_metas: Vec<&ColumnMeta> = footer
            .column_metas
            .iter()
            .filter(|meta| meta.fulltext_index_pointer.is_some())
            .collect();
        if fulltext_metas.is_empty() {
            return Ok((indexes, stats_map));
        }

        let fulltext_pages: Vec<(PagePointer, CompressionType)> = fulltext_metas
            .iter()
            .map(|meta| {
                (
                    meta.fulltext_index_pointer
                        .expect("fulltext pointer checked by filter"),
                    meta.compression,
                )
            })
            .collect();
        let fulltext_bodies = Self::load_index_page_bodies_parallel(
            reader,
            page_reader,
            &fulltext_pages,
            options.verify_checksum,
        )?;

        for (meta, body) in fulltext_metas.into_iter().zip(fulltext_bodies.into_iter()) {
            let (stats_bytes, _) = split_stats_trailer(body.as_ref());
            let index = FullTextIndex::deserialize(body.as_ref())?;
            let stats = if let Some(bytes) = stats_bytes {
                FullTextIndexStatistics::from_bytes(bytes)
                    .unwrap_or_else(|_| FullTextIndexStatistics::collect(&index))
            } else {
                FullTextIndexStatistics::collect(&index)
            };
            stats_map.insert(meta.column_id, stats);
            indexes.insert(meta.column_id, std::sync::Arc::new(index));
        }
        Ok((indexes, stats_map))
    }
}
