// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::index::fulltext::text_index::{FullTextIndex, FullTextIndexConfig};
use crate::index::fulltext::tokenizer::TokenizerKind;
use crate::index::sparse::SparseIndexBuilder;
use crate::rowset::page::{
    CompressionType, IndexPageFooter, IndexPageType, Lz4Codec, NoCompressionCodec, PageFooter,
    PageIO, ZstdCodec,
};
use crate::rowset::segment::SegmentSharedPtr;
use crate::rowset::{RowsetSharedPtr, SparseVector};
use crate::tablet::ColumnId;
use paro_common::error::{self as paro_error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FullTextWriteBinding {
    pub(crate) column_id: ColumnId,
    pub(crate) config: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SparseWriteBinding {
    pub(crate) column_id: ColumnId,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SearchWritePlan {
    pub(crate) fulltext: Vec<FullTextWriteBinding>,
    pub(crate) sparse: Vec<SparseWriteBinding>,
}

impl SearchWritePlan {
    pub(crate) fn is_empty(&self) -> bool {
        self.fulltext.is_empty() && self.sparse.is_empty()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let mut fulltext_configs = BTreeMap::<ColumnId, String>::new();
        for binding in &self.fulltext {
            let normalized = TokenizerKind::from_config(&binding.config)?
                .config_name()
                .to_string();
            match fulltext_configs.entry(binding.column_id) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(normalized);
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    if !entry.get().eq_ignore_ascii_case(&normalized) {
                        return Err(paro_error::invalid_input(format!(
                            "multiple fulltext tokenizer configs for column {} are not supported by the current durable write path",
                            binding.column_id
                        )));
                    }
                }
            }
        }

        let mut sparse_columns = BTreeSet::new();
        for binding in &self.sparse {
            sparse_columns.insert(binding.column_id);
        }
        if sparse_columns.len() != self.sparse.len() {
            return Err(paro_error::invalid_input(
                "duplicate sparse write bindings are not supported",
            ));
        }

        Ok(())
    }
}

pub(crate) fn materialize_rowset_artifacts(
    rowset: &RowsetSharedPtr,
    plan: &SearchWritePlan,
) -> Result<()> {
    if plan.is_empty() {
        return Ok(());
    }
    plan.validate()?;

    rowset.load()?;
    let mut changed = false;
    for segment in rowset.segments() {
        changed |= materialize_segment_artifacts(&segment, plan)?;
    }

    if !changed {
        return Ok(());
    }

    rowset.reload()?;
    let segments = rowset.segments();
    let data_size = segments.iter().map(|segment| segment.data_size()).sum();
    let index_size = segments.iter().map(|segment| segment.index_size()).sum();
    rowset.update_stats(rowset.num_rows(), data_size, index_size);
    Ok(())
}

fn materialize_segment_artifacts(
    segment: &SegmentSharedPtr,
    plan: &SearchWritePlan,
) -> Result<bool> {
    let mut footer = segment.footer().clone();
    let mut page_specs = Vec::new();

    for binding in &plan.fulltext {
        let Some(meta) = footer
            .column_metas
            .iter_mut()
            .find(|meta| meta.column_id == binding.column_id)
        else {
            continue;
        };
        if meta.fulltext_index_pointer.is_some() {
            continue;
        }

        let bytes = build_fulltext_index_bytes(segment, binding.column_id, &binding.config)?;
        if bytes.is_empty() {
            continue;
        }
        page_specs.push(IndexPageSpec {
            column_id: binding.column_id,
            kind: SearchIndexPageKind::FullText,
            compression: meta.compression,
            num_entries: segment.num_rows() as u32,
            bytes,
        });
    }

    for binding in &plan.sparse {
        let Some(meta) = footer
            .column_metas
            .iter_mut()
            .find(|meta| meta.column_id == binding.column_id)
        else {
            continue;
        };
        if meta.sparse_index_pointer.is_some() {
            continue;
        }

        let bytes = build_sparse_index_bytes(segment, binding.column_id)?;
        if bytes.is_empty() {
            continue;
        }
        page_specs.push(IndexPageSpec {
            column_id: binding.column_id,
            kind: SearchIndexPageKind::Sparse,
            compression: meta.compression,
            num_entries: segment.num_rows() as u32,
            bytes,
        });
    }

    if page_specs.is_empty() {
        return Ok(false);
    }

    patch_segment_footer(segment, &mut footer, &page_specs)?;
    Ok(true)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchIndexPageKind {
    Sparse,
    FullText,
}

#[derive(Debug, Clone)]
struct IndexPageSpec {
    column_id: ColumnId,
    kind: SearchIndexPageKind,
    compression: CompressionType,
    num_entries: u32,
    bytes: Vec<u8>,
}

fn build_fulltext_index_bytes(
    segment: &SegmentSharedPtr,
    column_id: ColumnId,
    config: &str,
) -> Result<Vec<u8>> {
    let tokenizer_kind = TokenizerKind::from_config(config)?;
    let mut index =
        FullTextIndex::new_with_tokenizer_kind(tokenizer_kind, FullTextIndexConfig::default());
    let mut iter = segment.new_column_iterator(column_id)?;

    for row_id in 0..segment.num_rows() {
        iter.seek_to_ordinal(row_id)?;
        let (_rowids, batch) = iter.next_batch(1)?;
        let Some(text_bytes) = batch.varlen_row(0)? else {
            continue;
        };
        let text = std::str::from_utf8(text_bytes.as_ref()).map_err(|err| {
            paro_error::data_corrupted(format!(
                "durable fulltext write decode error at column {} row {}: {}",
                column_id, row_id, err
            ))
        })?;
        index.add_document(row_id as u32, text)?;
    }

    index.serialize()
}

fn build_sparse_index_bytes(segment: &SegmentSharedPtr, column_id: ColumnId) -> Result<Vec<u8>> {
    let mut builder = SparseIndexBuilder::new();
    let mut iter = segment.new_column_iterator(column_id)?;

    for row_id in 0..segment.num_rows() {
        iter.seek_to_ordinal(row_id)?;
        let (_rowids, batch) = iter.next_batch(1)?;
        let Some(vector_bytes) = batch.varlen_row(0)? else {
            continue;
        };
        let text = std::str::from_utf8(vector_bytes.as_ref()).map_err(|err| {
            paro_error::data_corrupted(format!(
                "durable sparse write decode error at column {} row {}: {}",
                column_id, row_id, err
            ))
        })?;
        let vector = SparseVector::parse(text).map_err(|err| {
            paro_error::data_corrupted(format!(
                "durable sparse write parse error at column {} row {}: {}",
                column_id, row_id, err
            ))
        })?;
        builder.add(row_id as u32, &vector)?;
    }

    builder.build().serialize()
}

fn patch_segment_footer(
    segment: &SegmentSharedPtr,
    footer: &mut crate::rowset::segment::SegmentFooter,
    pages: &[IndexPageSpec],
) -> Result<()> {
    let file_path = segment.file_path().to_path_buf();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .map_err(|err| {
            paro_error::io_error(format!(
                "open segment {} for search patch: {}",
                file_path.display(),
                err
            ))
        })?;

    let file_size = file
        .metadata()
        .map_err(|err| {
            paro_error::io_error(format!(
                "stat segment {} for search patch: {}",
                file_path.display(),
                err
            ))
        })?
        .len();
    if file_size < 4 {
        return Err(paro_error::data_corrupted(format!(
            "segment {} missing footer size trailer",
            file_path.display()
        )));
    }

    file.seek(SeekFrom::End(-4)).map_err(|err| {
        paro_error::io_error(format!(
            "seek segment {} footer size: {}",
            file_path.display(),
            err
        ))
    })?;
    let mut footer_size_buf = [0u8; 4];
    file.read_exact(&mut footer_size_buf).map_err(|err| {
        paro_error::io_error(format!(
            "read segment {} footer size: {}",
            file_path.display(),
            err
        ))
    })?;
    let footer_size = u32::from_le_bytes(footer_size_buf) as u64;
    let footer_offset = file_size.checked_sub(footer_size).ok_or_else(|| {
        paro_error::data_corrupted(format!(
            "segment {} footer size {} exceeds file length {}",
            file_path.display(),
            footer_size,
            file_size
        ))
    })?;

    file.set_len(footer_offset).map_err(|err| {
        paro_error::io_error(format!(
            "truncate old footer from segment {}: {}",
            file_path.display(),
            err
        ))
    })?;
    file.seek(SeekFrom::Start(footer_offset)).map_err(|err| {
        paro_error::io_error(format!(
            "seek segment {} to footer offset {}: {}",
            file_path.display(),
            footer_offset,
            err
        ))
    })?;

    for page in pages {
        let pointer = write_index_page(&mut file, page.compression, &page.bytes, page.num_entries)?;
        let meta = footer
            .column_metas
            .iter_mut()
            .find(|meta| meta.column_id == page.column_id)
            .ok_or_else(|| {
                paro_error::column_not_found(format!(
                    "column {} missing from segment {}",
                    page.column_id,
                    segment.segment_id()
                ))
            })?;
        match page.kind {
            SearchIndexPageKind::Sparse => meta.sparse_index_pointer = Some(pointer),
            SearchIndexPageKind::FullText => meta.fulltext_index_pointer = Some(pointer),
        }
    }

    let footer_bytes = footer.serialize();
    file.write_all(&footer_bytes).map_err(|err| {
        paro_error::io_error(format!(
            "write patched footer for segment {}: {}",
            file_path.display(),
            err
        ))
    })?;
    let outer_footer_size = (footer_bytes.len() + 4) as u32;
    file.write_all(&outer_footer_size.to_le_bytes())
        .map_err(|err| {
            paro_error::io_error(format!(
                "write footer size for patched segment {}: {}",
                file_path.display(),
                err
            ))
        })?;
    file.flush().map_err(|err| {
        paro_error::io_error(format!(
            "flush patched segment {}: {}",
            file_path.display(),
            err
        ))
    })?;
    Ok(())
}

fn write_index_page(
    file: &mut File,
    compression: CompressionType,
    body: &[u8],
    num_entries: u32,
) -> Result<crate::rowset::page::PagePointer> {
    let footer = PageFooter::Index(IndexPageFooter {
        num_entries,
        page_type: IndexPageType::Leaf,
    });

    match compression {
        CompressionType::None => PageIO::compress_and_write_page(
            Some(&NoCompressionCodec),
            crate::rowset::page::DEFAULT_MIN_SPACE_SAVING,
            file,
            body,
            &footer,
        ),
        CompressionType::Lz4 => PageIO::compress_and_write_page(
            Some(&Lz4Codec),
            crate::rowset::page::DEFAULT_MIN_SPACE_SAVING,
            file,
            body,
            &footer,
        ),
        CompressionType::Zstd => PageIO::compress_and_write_page(
            Some(&ZstdCodec::default()),
            crate::rowset::page::DEFAULT_MIN_SPACE_SAVING,
            file,
            body,
            &footer,
        ),
    }
}
