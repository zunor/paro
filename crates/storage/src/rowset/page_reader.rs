// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PageReader - unified page read path with PageCache integration.
//!
//! PageReader replaces direct PageIO::read_and_decompress_page calls and
//! provides cache-aware page loading with optional decompressed caching.

use std::io::{Read, Seek};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use paro_common::error::{self as paro_error, Result};
use tracing::trace;

use crate::buffer::{PageCache, PageContentKind, PageKey};
use crate::compression::{ParallelDecompressTask, ParallelDecompressor};
use crate::rowset::page::{PageFooter, PageIO, PagePointer, PageReadOptions};

const SLOW_PAGE_IO_THRESHOLD: Duration = Duration::from_millis(8);
const SLOW_PAGE_DECOMPRESS_THRESHOLD: Duration = Duration::from_millis(8);
const SLOW_PAGE_FALLBACK_THRESHOLD: Duration = Duration::from_millis(12);

/// Page reader context used for PageKey construction and version isolation.
#[derive(Debug, Clone)]
pub struct PageReaderContext {
    pub tablet_id: u64,
    pub rowset_id: u64,
    pub rowset_gen: u64,
    pub segment_id: u32,
}

impl PageReaderContext {
    pub fn new(tablet_id: u64, rowset_id: u64, rowset_gen: u64, segment_id: u32) -> Self {
        Self {
            tablet_id,
            rowset_id,
            rowset_gen,
            segment_id,
        }
    }
}

/// Page reader options.
#[derive(Debug, Clone)]
pub struct PageReaderOptions {
    pub cache_decompressed: bool,
    pub parallel_decompressor: Option<ParallelDecompressor>,
}

impl Default for PageReaderOptions {
    fn default() -> Self {
        Self {
            cache_decompressed: false,
            parallel_decompressor: None,
        }
    }
}

/// PageReader with optional cache and decompressed caching policy.
#[derive(Clone)]
pub struct PageReader {
    cache: Option<Arc<PageCache>>,
    context: PageReaderContext,
    options: PageReaderOptions,
}

impl PageReader {
    pub fn new(
        context: PageReaderContext,
        cache: Option<Arc<PageCache>>,
        options: PageReaderOptions,
    ) -> Self {
        Self {
            cache,
            context,
            options,
        }
    }

    /// Read a page with cache integration.
    pub fn read_page<R: Read + Seek>(
        &self,
        reader: &mut R,
        opts: &PageReadOptions,
    ) -> Result<(Bytes, PageFooter, u32)> {
        let key = self.make_key(opts.page_pointer);

        // If no cache, fall back to direct PageIO path (or allocator-aware decompressor).
        if self.cache.is_none() {
            if let Some(decompressor) = &self.options.parallel_decompressor {
                let io_start = Instant::now();
                let raw = PageIO::read_page_bytes(reader, opts)?;
                let io_elapsed = io_start.elapsed();
                if io_elapsed >= SLOW_PAGE_IO_THRESHOLD {
                    self.trace_slow_io(&key, io_elapsed, "direct");
                }
                let (footer, uncompressed_size, body_size) =
                    PageIO::parse_page_footer(&raw, opts.verify_checksum)?;
                let decompress_start = Instant::now();
                let body = decompressor.decompress_one(
                    &raw[..body_size],
                    uncompressed_size as usize,
                    opts.codec,
                )?;
                let decompress_elapsed = decompress_start.elapsed();
                if decompress_elapsed >= SLOW_PAGE_DECOMPRESS_THRESHOLD {
                    self.trace_slow_decompress(
                        &key,
                        decompress_elapsed,
                        body_size,
                        uncompressed_size,
                    );
                }
                return Ok((body, footer, uncompressed_size));
            }
            let fallback_start = Instant::now();
            let result = PageIO::read_and_decompress_page(reader, opts);
            let fallback_elapsed = fallback_start.elapsed();
            if fallback_elapsed >= SLOW_PAGE_FALLBACK_THRESHOLD {
                trace!(
                    tablet_id = self.context.tablet_id,
                    rowset_id = self.context.rowset_id,
                    rowset_gen = self.context.rowset_gen,
                    segment_id = self.context.segment_id,
                    page_offset = key.page_offset,
                    page_size = key.page_size,
                    elapsed_ms = fallback_elapsed.as_secs_f64() * 1000.0,
                    "slow page read+decompress fallback path",
                );
            }
            return result;
        }

        // If decompressed cache is enabled, try it first.
        if self.options.cache_decompressed {
            if let Some(body) = self.lookup_cached(&key, PageContentKind::Decompressed) {
                let raw = self.read_raw_page(reader, opts, &key)?;
                let (footer, uncompressed_size, _) =
                    PageIO::parse_page_footer(&raw, opts.verify_checksum)?;
                return Ok((body, footer, uncompressed_size));
            }
        }

        let raw = self.read_raw_page(reader, opts, &key)?;
        let (footer, uncompressed_size, body_size) =
            PageIO::parse_page_footer(&raw, opts.verify_checksum)?;
        let decompress_start = Instant::now();
        let body = if let Some(decompressor) = &self.options.parallel_decompressor {
            decompressor.decompress_one(
                &raw[..body_size],
                uncompressed_size as usize,
                opts.codec,
            )?
        } else {
            PageIO::decompress_page_body(&raw[..body_size], uncompressed_size, opts.codec)?
        };
        let decompress_elapsed = decompress_start.elapsed();
        if decompress_elapsed >= SLOW_PAGE_DECOMPRESS_THRESHOLD {
            self.trace_slow_decompress(&key, decompress_elapsed, body_size, uncompressed_size);
        }

        if self.options.cache_decompressed {
            if let Some(cache) = &self.cache {
                let _ = cache.insert(key, PageContentKind::Decompressed, body.to_vec());
            }
        }

        Ok((body, footer, uncompressed_size))
    }

    fn make_key(&self, pointer: PagePointer) -> PageKey {
        PageKey::new(
            self.context.tablet_id,
            self.context.rowset_id,
            self.context.rowset_gen,
            self.context.segment_id,
            pointer.offset,
            pointer.size,
        )
    }

    /// Build a PageKey for the given pointer.
    pub fn page_key(&self, pointer: PagePointer) -> PageKey {
        self.make_key(pointer)
    }

    fn lookup_cached(&self, key: &PageKey, kind: PageContentKind) -> Option<Bytes> {
        let cache = self.cache.as_ref()?;
        let handle = cache.lookup(key, kind)?;
        let data = handle.data()?;
        Some(Bytes::copy_from_slice(data))
    }

    fn read_raw_page<R: Read + Seek>(
        &self,
        reader: &mut R,
        opts: &PageReadOptions,
        key: &PageKey,
    ) -> Result<Vec<u8>> {
        let io_start = Instant::now();
        if let Some(cache) = &self.cache {
            let handle = cache.get_or_load(*key, PageContentKind::Compressed, || {
                PageIO::read_page_bytes(reader, opts)
            })?;
            let data = handle
                .data()
                .ok_or_else(|| paro_error::internal("page cache data missing"))?;
            let io_elapsed = io_start.elapsed();
            if io_elapsed >= SLOW_PAGE_IO_THRESHOLD {
                self.trace_slow_io(key, io_elapsed, "cache");
            }
            return Ok(data.to_vec());
        }

        let raw = PageIO::read_page_bytes(reader, opts)?;
        let io_elapsed = io_start.elapsed();
        if io_elapsed >= SLOW_PAGE_IO_THRESHOLD {
            self.trace_slow_io(key, io_elapsed, "direct");
        }
        Ok(raw)
    }

    fn trace_slow_io(&self, key: &PageKey, elapsed: Duration, source: &'static str) {
        trace!(
            source,
            tablet_id = self.context.tablet_id,
            rowset_id = self.context.rowset_id,
            rowset_gen = self.context.rowset_gen,
            segment_id = self.context.segment_id,
            page_offset = key.page_offset,
            page_size = key.page_size,
            elapsed_ms = elapsed.as_secs_f64() * 1000.0,
            "slow page io path",
        );
    }

    fn trace_slow_decompress(
        &self,
        key: &PageKey,
        elapsed: Duration,
        compressed_bytes: usize,
        uncompressed_bytes: u32,
    ) {
        trace!(
            tablet_id = self.context.tablet_id,
            rowset_id = self.context.rowset_id,
            rowset_gen = self.context.rowset_gen,
            segment_id = self.context.segment_id,
            page_offset = key.page_offset,
            page_size = key.page_size,
            compressed_bytes,
            uncompressed_bytes,
            elapsed_ms = elapsed.as_secs_f64() * 1000.0,
            "slow page decompress path",
        );
    }

    /// Read multiple pages and decompress bodies in parallel.
    ///
    /// Results preserve the input order.
    pub fn read_pages_parallel<R: Read + Seek>(
        &self,
        reader: &mut R,
        opts: &[PageReadOptions],
    ) -> Result<Vec<(Bytes, PageFooter, u32)>> {
        if opts.is_empty() {
            return Ok(Vec::new());
        }

        let Some(decompressor) = &self.options.parallel_decompressor else {
            let mut output = Vec::with_capacity(opts.len());
            for opt in opts {
                output.push(self.read_page(reader, opt)?);
            }
            return Ok(output);
        };

        let mut results: Vec<Option<(Bytes, PageFooter, u32)>> = vec![None; opts.len()];
        let mut pending = Vec::new();

        for (idx, opt) in opts.iter().enumerate() {
            let key = self.make_key(opt.page_pointer);

            if self.options.cache_decompressed {
                if let Some(body) = self.lookup_cached(&key, PageContentKind::Decompressed) {
                    let raw = self.read_raw_page(reader, opt, &key)?;
                    let (footer, uncompressed_size, _) =
                        PageIO::parse_page_footer(&raw, opt.verify_checksum)?;
                    results[idx] = Some((body, footer, uncompressed_size));
                    continue;
                }
            }

            let raw = self.read_raw_page(reader, opt, &key)?;
            let (footer, uncompressed_size, body_size) =
                PageIO::parse_page_footer(&raw, opt.verify_checksum)?;
            pending.push(PendingPage {
                idx,
                key,
                raw,
                footer,
                uncompressed_size,
                body_size,
                codec: opt.codec,
            });
        }

        if !pending.is_empty() {
            let tasks: Vec<ParallelDecompressTask<'_>> = pending
                .iter()
                .map(|page| ParallelDecompressTask {
                    body: &page.raw[..page.body_size],
                    uncompressed_size: page.uncompressed_size as usize,
                    codec: page.codec,
                })
                .collect();

            let bodies = decompressor.decompress_batch(&tasks)?;
            for (page, body) in pending.into_iter().zip(bodies.into_iter()) {
                if self.options.cache_decompressed {
                    if let Some(cache) = &self.cache {
                        let _ =
                            cache.insert(page.key, PageContentKind::Decompressed, body.to_vec());
                    }
                }
                results[page.idx] = Some((body, page.footer, page.uncompressed_size));
            }
        }

        let mut output = Vec::with_capacity(opts.len());
        for slot in results {
            match slot {
                Some(entry) => output.push(entry),
                None => return Err(paro_error::internal("parallel page read missing result")),
            }
        }

        Ok(output)
    }
}

struct PendingPage {
    idx: usize,
    key: PageKey,
    raw: Vec<u8>,
    footer: PageFooter,
    uncompressed_size: u32,
    body_size: usize,
    codec: Option<crate::rowset::page::CompressionType>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::{Lz4BlockCompression, ParallelDecompressor};
    use crate::rowset::page::{
        CompressionType, DataPageFooter, NullEncoding, PageFooter, PageIO, PageReadOptions,
        DEFAULT_MIN_SPACE_SAVING,
    };
    use paro_common::allocator::default_allocator;
    use std::io::Cursor;
    use std::sync::Arc;

    fn make_data_footer(first_ordinal: u64, num_values: u64) -> PageFooter {
        PageFooter::Data(DataPageFooter {
            first_ordinal,
            num_values,
            nullmap_size: 0,
            corresponding_element_ordinal: None,
            format_version: 1,
            null_encoding: NullEncoding::BitShuffle,
        })
    }

    #[test]
    fn page_reader_falls_back_without_cache() {
        let mut buffer = Cursor::new(Vec::new());
        let footer = make_data_footer(0, 4);
        let body = vec![1u8, 2, 3, 4];
        let pointer = PageIO::write_page(&mut buffer, &body, &footer, body.len() as u32).unwrap();

        let ctx = PageReaderContext::new(1, 1, 1, 0);
        let reader = PageReader::new(ctx, None, PageReaderOptions::default());

        let opts = PageReadOptions::new(pointer).with_verify_checksum(true);
        buffer.set_position(0);
        let (read_body, _, _) = reader.read_page(&mut buffer, &opts).unwrap();
        assert_eq!(read_body.as_ref(), body.as_slice());
    }

    #[test]
    fn page_reader_parallel_batch_preserves_input_order() {
        let mut buffer = Cursor::new(Vec::new());
        let codec = Lz4BlockCompression::new();

        let body_a = vec![11u8; 4096];
        let body_b = vec![22u8; 4096];
        let body_c = vec![33u8; 4096];
        let ptr_a = PageIO::compress_and_write_page(
            Some(&codec),
            DEFAULT_MIN_SPACE_SAVING,
            &mut buffer,
            &body_a,
            &make_data_footer(10, body_a.len() as u64),
        )
        .unwrap();
        let ptr_b = PageIO::compress_and_write_page(
            Some(&codec),
            DEFAULT_MIN_SPACE_SAVING,
            &mut buffer,
            &body_b,
            &make_data_footer(20, body_b.len() as u64),
        )
        .unwrap();
        let ptr_c = PageIO::compress_and_write_page(
            Some(&codec),
            DEFAULT_MIN_SPACE_SAVING,
            &mut buffer,
            &body_c,
            &make_data_footer(30, body_c.len() as u64),
        )
        .unwrap();

        let ctx = PageReaderContext::new(1, 2, 3, 4);
        let reader = PageReader::new(
            ctx,
            None,
            PageReaderOptions {
                cache_decompressed: false,
                parallel_decompressor: Some(
                    ParallelDecompressor::new(Arc::new(default_allocator())).with_max_threads(4),
                ),
            },
        );

        let opts = vec![
            PageReadOptions::new(ptr_c).with_codec(CompressionType::Lz4),
            PageReadOptions::new(ptr_a).with_codec(CompressionType::Lz4),
            PageReadOptions::new(ptr_b).with_codec(CompressionType::Lz4),
        ];

        buffer.set_position(0);
        let result = reader.read_pages_parallel(&mut buffer, &opts).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0.as_ref(), body_c.as_slice());
        assert_eq!(result[1].0.as_ref(), body_a.as_slice());
        assert_eq!(result[2].0.as_ref(), body_b.as_slice());

        let f0 = match &result[0].1 {
            PageFooter::Data(footer) => footer.first_ordinal,
            _ => panic!("expected data footer"),
        };
        let f1 = match &result[1].1 {
            PageFooter::Data(footer) => footer.first_ordinal,
            _ => panic!("expected data footer"),
        };
        let f2 = match &result[2].1 {
            PageFooter::Data(footer) => footer.first_ordinal,
            _ => panic!("expected data footer"),
        };
        assert_eq!((f0, f1, f2), (30, 10, 20));
    }

    #[test]
    fn page_reader_parallel_batch_error_matches_single_page_error() {
        let mut buffer = Cursor::new(Vec::new());
        let codec = Lz4BlockCompression::new();

        let good_body = vec![7u8; 2048];
        let bad_body = vec![9u8; 2048];
        let good_ptr = PageIO::compress_and_write_page(
            Some(&codec),
            DEFAULT_MIN_SPACE_SAVING,
            &mut buffer,
            &good_body,
            &make_data_footer(0, good_body.len() as u64),
        )
        .unwrap();
        let bad_ptr = PageIO::compress_and_write_page(
            Some(&codec),
            DEFAULT_MIN_SPACE_SAVING,
            &mut buffer,
            &bad_body,
            &make_data_footer(1, bad_body.len() as u64),
        )
        .unwrap();

        let mut raw = buffer.into_inner();
        raw[bad_ptr.offset as usize] = raw[bad_ptr.offset as usize].wrapping_add(1);

        let ctx = PageReaderContext::new(1, 2, 3, 4);
        let reader = PageReader::new(
            ctx,
            None,
            PageReaderOptions {
                cache_decompressed: false,
                parallel_decompressor: Some(
                    ParallelDecompressor::new(Arc::new(default_allocator())).with_max_threads(4),
                ),
            },
        );

        let batch_opts = vec![
            PageReadOptions::new(good_ptr)
                .with_verify_checksum(false)
                .with_codec(CompressionType::Lz4),
            PageReadOptions::new(bad_ptr)
                .with_verify_checksum(false)
                .with_codec(CompressionType::Lz4),
        ];
        let mut batch_cursor = Cursor::new(raw.clone());
        let batch_err = reader
            .read_pages_parallel(&mut batch_cursor, &batch_opts)
            .unwrap_err()
            .to_string();

        let single_opt = PageReadOptions::new(bad_ptr)
            .with_verify_checksum(false)
            .with_codec(CompressionType::Lz4);
        let mut single_cursor = Cursor::new(raw);
        let single_err = reader
            .read_page(&mut single_cursor, &single_opt)
            .unwrap_err()
            .to_string();

        assert!(
            batch_err.contains("uncompressed size mismatch"),
            "unexpected error: {}",
            batch_err
        );
        assert_eq!(batch_err, single_err);
    }
}
